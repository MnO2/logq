//! Bounded full-scan gzip decoding and worker-local JSON aggregation.
#[cfg(feature = "bench-internals")]
use super::{WorkerTiming, probe::WorkerProbeReport};
#[cfg(feature = "bench-internals")]
use crate::common::types::Value;
use crate::common::types::Variables;
#[cfg(feature = "bench-internals")]
use crate::execution::batch::BatchToRowAdapter;
use crate::execution::batch::{BatchSchema, BatchStream, ColumnBatch, ColumnType, PrecomputedBatchStream};
use crate::execution::batch_groupby::{BatchGroupByOperator, PartialAggregateState, output_names};
use crate::execution::json_batch_scan::JsonBatchScanOperator;
use crate::execution::memory::{MemoryReservation, MemoryTracker};
#[cfg(feature = "bench-internals")]
use crate::execution::types::{Aggregate, CountAggregate, Expression, Named, SumAggregate};
use crate::execution::types::{CreateStreamError, NamedAggregate, StreamError, StreamResult};
use crate::functions::FunctionRegistry;
use crate::syntax::ast::PathExpr;
#[cfg(feature = "bench-internals")]
use crate::syntax::ast::PathSegment;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
#[cfg(feature = "bench-internals")]
use std::path::Path;
use std::path::PathBuf;
#[cfg(feature = "bench-internals")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
#[cfg(feature = "bench-internals")]
use std::time::Instant;

const GZIP_CHUNK_BYTES: usize = 256 * 1024;
type ReaderFactory = Box<dyn FnOnce() -> StreamResult<Box<dyn Read>> + Send>;
type WorkerWrapper = dyn Fn(Box<dyn BatchStream>) -> Box<dyn BatchStream> + Send + Sync;

pub(crate) struct GzipAggregateStream {
    reader_factory: Option<ReaderFactory>,
    parser_workers: usize,
    chunk_bytes: usize,
    fields: Vec<String>,
    keys: Vec<PathExpr>,
    aggregates: Vec<NamedAggregate>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    memory: MemoryTracker,
    wrapper: Arc<WorkerWrapper>,
    schema: BatchSchema,
    cancelled: Arc<AtomicBool>,
    output_memory: MemoryReservation,
    #[cfg(feature = "bench-internals")]
    metrics: Option<Arc<ProbeMetrics>>,
}

impl GzipAggregateStream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        path: PathBuf,
        parser_workers: usize,
        fields: Vec<String>,
        keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
        memory: MemoryTracker,
    ) -> Self {
        Self::from_reader(
            Box::new(move || {
                let file = File::open(path).map_err(|_| StreamError::Get(CreateStreamError::Io))?;
                Ok(Box::new(flate2::read::GzDecoder::new(file)))
            }),
            parser_workers,
            fields,
            keys,
            aggregates,
            variables,
            registry,
            memory,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_reader(
        reader_factory: ReaderFactory,
        parser_workers: usize,
        fields: Vec<String>,
        keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
        memory: MemoryTracker,
    ) -> Self {
        assert!(parser_workers > 0);
        assert!(BatchGroupByOperator::supports_parallel(&keys, &aggregates));
        let names = output_names(&keys, &aggregates);
        Self {
            reader_factory: Some(reader_factory),
            parser_workers,
            chunk_bytes: GZIP_CHUNK_BYTES,
            fields,
            keys,
            aggregates,
            variables,
            registry,
            output_memory: MemoryReservation::new(memory.clone()),
            memory,
            wrapper: Arc::new(|child| child),
            schema: BatchSchema {
                types: vec![ColumnType::Mixed; names.len()],
                names,
            },
            cancelled: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "bench-internals")]
            metrics: None,
        }
    }

    pub(crate) fn map_workers<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(Box<dyn BatchStream>) -> Box<dyn BatchStream> + Send + Sync + 'static,
    {
        self.wrapper = Arc::new(wrapper);
        self
    }

    fn consume(&self, reader_factory: ReaderFactory) -> StreamResult<(ColumnBatch, MemoryReservation)> {
        let threads = self.parser_workers;
        let schema = BatchSchema {
            names: self.fields.clone(),
            types: vec![ColumnType::Mixed; self.fields.len()],
        };
        let fields = Arc::new(self.fields.clone());
        let aggregates = Arc::new(self.aggregates.clone());
        let registry = self.registry.clone();
        let memory = self.memory.clone();
        let mut pipeline = Pipeline {
            cancelled: self.cancelled.clone(),
            inputs: Vec::new(),
            outputs: VecDeque::new(),
            workers: Vec::new(),
            producer: None,
        };
        for worker in 0..threads {
            let (input, receiver) = mpsc::sync_channel::<StreamResult<Chunk>>(1);
            let (sender, output) = mpsc::sync_channel(1);
            pipeline.inputs.push(input);
            pipeline.outputs.push_back(output);
            let cancelled = pipeline.cancelled.clone();
            let fields = fields.clone();
            let aggregates = aggregates.clone();
            let registry = registry.clone();
            let memory = memory.clone();
            let keys = self.keys.clone();
            let variables = self.variables.clone();
            let wrapper = self.wrapper.clone();
            #[cfg(feature = "bench-internals")]
            let timing = self
                .metrics
                .as_ref()
                .filter(|metrics| metrics.instrument)
                .map(|metrics| metrics.workers[worker].clone());
            let handle = std::thread::Builder::new()
                .name(format!("logq-gzip-parse-{worker}"))
                .spawn(move || {
                    #[cfg(feature = "bench-internals")]
                    let start = timing.as_ref().map(|_| Instant::now());
                    while !cancelled.load(Ordering::Relaxed) {
                        #[cfg(feature = "bench-internals")]
                        let receive_start = timing.as_ref().map(|_| Instant::now());
                        let received = receiver.recv();
                        #[cfg(feature = "bench-internals")]
                        if let (Some(start), Some(timing)) = (receive_start, &timing) {
                            timing
                                .input_wait_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        let Ok(input) = received else {
                            break;
                        };
                        #[cfg(feature = "bench-internals")]
                        if let Some(timing) = &timing {
                            timing.tasks.fetch_add(1, Ordering::Relaxed);
                        }
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let chunk = input?;
                            BatchGroupByOperator::new(
                                wrapper(Box::new(JsonBatchScanOperator::new(
                                    Box::new(ChunkReader {
                                        cursor: io::Cursor::new(chunk.data),
                                        _reservation: chunk.reservation,
                                        cancelled: cancelled.clone(),
                                        terminal_read_error: chunk.terminal_read_error,
                                    }),
                                    fields.as_ref().clone(),
                                ))),
                                keys.clone(),
                                aggregates.as_ref().clone(),
                                variables.clone(),
                                registry.clone(),
                            )
                            .with_memory_tracker(memory.clone())
                            .consume_partial()
                        }))
                        .unwrap_or_else(|_| Err(StreamError::General("gzip parser worker panicked".into())));
                        let failed = result.is_err();
                        #[cfg(feature = "bench-internals")]
                        let send_start = timing.as_ref().map(|_| Instant::now());
                        let sent = sender.send(result).is_ok();
                        #[cfg(feature = "bench-internals")]
                        if let (Some(start), Some(timing)) = (send_start, &timing) {
                            timing
                                .send_wait_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        if !sent || failed {
                            break;
                        }
                    }
                    #[cfg(feature = "bench-internals")]
                    if let (Some(start), Some(timing)) = (start, &timing) {
                        timing
                            .elapsed_ns
                            .store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                })
                .map_err(|error| StreamError::General(format!("could not start gzip parser: {error}")))?;
            pipeline.workers.push(handle);
        }
        let inputs = std::mem::take(&mut pipeline.inputs);
        let cancelled = pipeline.cancelled.clone();
        #[cfg(feature = "bench-internals")]
        let stats = self.metrics.as_ref().map(|metrics| metrics.producer.clone());
        let producer_memory = memory.clone();
        #[cfg(feature = "bench-internals")]
        let instrument = self.metrics.as_ref().is_some_and(|metrics| metrics.instrument);
        let chunk_bytes = self.chunk_bytes;
        pipeline.producer = Some(
            std::thread::Builder::new()
                .name("logq-gzip-decode".into())
                .spawn(move || {
                    #[cfg(feature = "bench-internals")]
                    let start = instrument.then(Instant::now);
                    let reader = std::panic::catch_unwind(std::panic::AssertUnwindSafe(reader_factory))
                        .unwrap_or_else(|_| Err(StreamError::General("gzip decoder worker panicked".into())));
                    let mut reader = match reader {
                        Ok(reader) => BufReader::with_capacity(64 * 1024, reader),
                        Err(error) => {
                            let _ = inputs[0].send(Err(error));
                            return;
                        }
                    };
                    let mut task = 0usize;
                    loop {
                        let next = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            read_chunk(&mut reader, chunk_bytes, &producer_memory, &cancelled)
                        }))
                        .unwrap_or_else(|_| Err(StreamError::General("gzip decoder worker panicked".into())));
                        let input = match next {
                            Ok(Some(chunk)) => {
                                #[cfg(feature = "bench-internals")]
                                if let Some(stats) = &stats {
                                    stats.bytes.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
                                    stats.chunks.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(chunk)
                            }
                            Ok(None) => break,
                            Err(error) => Err(error),
                        };
                        let failed = input.is_err();
                        let terminal = input.as_ref().is_ok_and(|chunk| chunk.terminal_read_error);
                        #[cfg(feature = "bench-internals")]
                        let send_start = instrument.then(Instant::now);
                        let sent = inputs[task % inputs.len()].send(input).is_ok();
                        #[cfg(feature = "bench-internals")]
                        if let (Some(start), Some(stats)) = (send_start, &stats) {
                            stats
                                .send_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        if !sent || failed || terminal {
                            break;
                        }
                        task += 1;
                    }
                    #[cfg(feature = "bench-internals")]
                    if let (Some(start), Some(stats)) = (start, &stats) {
                        stats
                            .elapsed_ns
                            .store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                })
                .map_err(|error| StreamError::General(format!("could not start gzip decoder: {error}")))?,
        );
        let mut merged: Option<PartialAggregateState> = None;
        while let Some(output) = pipeline.outputs.front() {
            match output.recv() {
                Ok(Ok(state)) => {
                    if let Some(merged) = merged.as_mut() {
                        merged.merge(state)?;
                    } else {
                        merged = Some(state);
                    }
                    let output = pipeline.outputs.pop_front().unwrap();
                    pipeline.outputs.push_back(output);
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    pipeline.outputs.pop_front();
                }
            }
        }
        pipeline.stop();
        let state = match merged {
            Some(state) => state,
            None => BatchGroupByOperator::new(
                (self.wrapper)(Box::new(PrecomputedBatchStream::new(vec![], schema))),
                self.keys.clone(),
                aggregates.as_ref().clone(),
                self.variables.clone(),
                registry,
            )
            .with_memory_tracker(memory)
            .consume_partial()?,
        };
        state.finish()
    }
}

impl BatchStream for GzipAggregateStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        self.output_memory.resize(0)?;
        if self.cancelled.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let Some(factory) = self.reader_factory.take() else {
            return Ok(None);
        };
        // consume owns its supervisor: every return path drops queues and joins
        // all worker/producer threads before this blocking aggregate returns.
        let (batch, reservation) = self.consume(factory)?;
        self.output_memory = reservation;
        Ok((batch.len != 0).then_some(batch))
    }
    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

#[cfg(feature = "bench-internals")]
struct ProbeMetrics {
    workers: Vec<Arc<WorkerTiming>>,
    producer: Arc<ProducerStats>,
    instrument: bool,
}

struct Chunk {
    data: Vec<u8>,
    reservation: MemoryReservation,
    terminal_read_error: bool,
}

struct ChunkReader {
    cursor: io::Cursor<Vec<u8>>,
    _reservation: MemoryReservation,
    cancelled: Arc<AtomicBool>,
    terminal_read_error: bool,
}

impl Read for ChunkReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let input = self.fill_buf()?;
        let len = input.len().min(output.len());
        output[..len].copy_from_slice(&input[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl BufRead for ChunkReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.cancelled.load(Ordering::Relaxed) {
            Ok(&[])
        } else if self.terminal_read_error && self.cursor.position() == self.cursor.get_ref().len() as u64 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gzip input failed after decoded prefix",
            ))
        } else {
            self.cursor.fill_buf()
        }
    }
    fn consume(&mut self, bytes: usize) {
        self.cursor.consume(bytes);
    }
}

/// One input slot and one partial-result slot per worker bound queued work.
/// Inputs are retained here until the producer owns them so spawn failures also
/// drop senders before joining workers that are waiting to receive input.
struct Pipeline {
    cancelled: Arc<AtomicBool>,
    inputs: Vec<mpsc::SyncSender<StreamResult<Chunk>>>,
    outputs: VecDeque<mpsc::Receiver<StreamResult<PartialAggregateState>>>,
    workers: Vec<JoinHandle<()>>,
    producer: Option<JoinHandle<()>>,
}

impl Pipeline {
    fn stop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        // A blocked worker send must fail before joining; its dropped input
        // receiver then releases a producer blocked on that worker's queue.
        self.outputs.clear();
        self.inputs.clear();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        if let Some(producer) = self.producer.take() {
            let _ = producer.join();
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(feature = "bench-internals")]
#[derive(Default)]
struct ProducerStats {
    bytes: AtomicU64,
    chunks: AtomicU64,
    elapsed_ns: AtomicU64,
    send_ns: AtomicU64,
}

fn read_chunk(
    reader: &mut impl BufRead,
    target: usize,
    memory: &MemoryTracker,
    cancelled: &AtomicBool,
) -> StreamResult<Option<Chunk>> {
    let mut data = Vec::new();
    let mut reservation = MemoryReservation::new(memory.clone());
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(_) if !data.is_empty() => {
                // Preserve decoded rows, but expose the read error at their
                // actual end instead of creating a successful partial batch.
                return Ok(Some(Chunk {
                    data,
                    reservation,
                    terminal_read_error: true,
                }));
            }
            Err(_) => return Err(StreamError::Reader),
        };
        if available.is_empty() {
            return Ok((!data.is_empty()).then_some(Chunk {
                data,
                reservation,
                terminal_read_error: false,
            }));
        }
        let remaining = target.saturating_sub(data.len());
        let end = if remaining <= available.len() {
            let from = remaining.saturating_sub(1);
            memchr::memchr(b'\n', &available[from..]).map(|offset| from + offset + 1)
        } else {
            None
        };
        let take = end.unwrap_or(available.len());
        let needed = data.len().checked_add(take).ok_or(StreamError::MemoryBudgetExceeded)?;
        if needed > data.capacity() {
            let capacity = data.capacity().saturating_mul(2).max(target).max(needed);
            reservation.resize(capacity)?;
            data.reserve_exact(capacity - data.len());
        }
        data.extend_from_slice(&available[..take]);
        reader.consume(take);
        if end.is_some() {
            return Ok(Some(Chunk {
                data,
                reservation,
                terminal_read_error: false,
            }));
        }
    }
}

#[cfg(feature = "bench-internals")]
pub struct JsonGzipProbeConfig {
    pub threads: usize,
    pub chunk_bytes: usize,
    pub sum_field: Option<String>,
    pub max_memory: usize,
    pub instrument_workers: bool,
}

#[cfg(feature = "bench-internals")]
#[derive(Debug, serde::Serialize)]
pub struct JsonGzipProbeReport {
    pub count: i32,
    pub sum: Option<f32>,
    pub elapsed_ns: u64,
    pub decoded_bytes: u64,
    pub chunks: u64,
    pub workers_used: usize,
    pub producer_busy_ns: Option<u64>,
    pub producer_send_wait_ns: Option<u64>,
    pub workers: Vec<WorkerProbeReport>,
}

#[cfg(feature = "bench-internals")]
pub fn profile_json_gzip(
    path: &Path,
    config: JsonGzipProbeConfig,
) -> Result<JsonGzipProbeReport, Box<dyn std::error::Error>> {
    if !path.metadata()?.is_file() {
        return Err("gzip probe requires an immutable regular file".into());
    }
    let file = File::open(path)?;
    let memory = MemoryTracker::new(Some(config.max_memory));
    // Header reads/decoder construction happen inside the timed producer.
    Ok(run_profile(
        Box::new(move || Ok(Box::new(flate2::read::GzDecoder::new(file)))),
        config,
        memory,
    )?)
}

#[cfg(all(test, feature = "bench-internals"))]
fn run_pipeline(
    reader: impl Read + Send + 'static,
    config: JsonGzipProbeConfig,
    memory: MemoryTracker,
) -> StreamResult<JsonGzipProbeReport> {
    run_profile(Box::new(move || Ok(Box::new(reader))), config, memory)
}

#[cfg(feature = "bench-internals")]
fn run_profile(
    factory: ReaderFactory,
    config: JsonGzipProbeConfig,
    memory: MemoryTracker,
) -> StreamResult<JsonGzipProbeReport> {
    if config.chunk_bytes == 0 {
        return Err(StreamError::General("chunk size must be positive".into()));
    }
    let start = Instant::now();
    let threads = if config.threads == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        config.threads
    };
    let fields: Vec<_> = config.sum_field.iter().cloned().collect();
    let mut aggregates = vec![NamedAggregate::new(
        Aggregate::Count(CountAggregate::new(), Named::Star),
        Some("count".into()),
    )];
    if let Some(field) = &config.sum_field {
        aggregates.push(NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(
                    Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(field.clone())])),
                    None,
                ),
            ),
            Some("sum".into()),
        ));
    }
    let instrument = config.instrument_workers;
    let metrics = Arc::new(ProbeMetrics {
        workers: (0..threads).map(|_| Arc::new(WorkerTiming::default())).collect(),
        producer: Arc::new(ProducerStats::default()),
        instrument,
    });
    let mut stream = GzipAggregateStream::from_reader(
        factory,
        threads,
        fields,
        vec![],
        aggregates,
        Variables::new(),
        Arc::new(FunctionRegistry::new()),
        memory,
    );
    stream.chunk_bytes = config.chunk_bytes;
    stream.metrics = Some(metrics.clone());
    let batch = stream.next_batch()?.expect("ungrouped output");
    let count = match BatchToRowAdapter::extract_value(&batch.columns[0], 0) {
        Value::Int(value) => value,
        _ => unreachable!("count result"),
    };
    let sum = if config.sum_field.is_some() {
        match BatchToRowAdapter::extract_value(&batch.columns[1], 0) {
            Value::Float(value) => Some(value.0),
            Value::Null => None,
            _ => unreachable!("sum result"),
        }
    } else {
        None
    };
    drop(batch);
    drop(stream);
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    let producer_stats = &metrics.producer;
    let timings = &metrics.workers;
    let send_wait = producer_stats.send_ns.load(Ordering::Relaxed);
    let workers = if instrument {
        timings
            .iter()
            .enumerate()
            .map(|(worker, timing)| {
                let send_wait_ns = timing.send_wait_ns.load(Ordering::Relaxed);
                let input_wait_ns = timing.input_wait_ns.load(Ordering::Relaxed);
                WorkerProbeReport {
                    worker,
                    tasks: timing.tasks.load(Ordering::Relaxed),
                    busy_ns: timing
                        .elapsed_ns
                        .load(Ordering::Relaxed)
                        .saturating_sub(send_wait_ns)
                        .saturating_sub(input_wait_ns),
                    send_wait_ns,
                    input_wait_ns,
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(JsonGzipProbeReport {
        count,
        sum,
        elapsed_ns,
        decoded_bytes: producer_stats.bytes.load(Ordering::Relaxed),
        chunks: producer_stats.chunks.load(Ordering::Relaxed),
        workers_used: threads,
        producer_busy_ns: instrument.then(|| {
            producer_stats
                .elapsed_ns
                .load(Ordering::Relaxed)
                .saturating_sub(send_wait)
        }),
        producer_send_wait_ns: instrument.then_some(send_wait),
        workers,
    })
}

#[cfg(all(test, feature = "bench-internals"))]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    fn compressed(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn config(threads: usize, chunk_bytes: usize) -> JsonGzipProbeConfig {
        JsonGzipProbeConfig {
            threads,
            chunk_bytes,
            sum_field: Some("n".into()),
            max_memory: 8 * 1024 * 1024,
            instrument_workers: true,
        }
    }

    #[test]
    fn gzip_pipeline_preserves_long_lines_crlf_unterminated_rows_and_numeric_partials() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let data = format!(
            "{{\"n\":100000000}}\r\n{{\"n\":1,\"large\":\"{}\"}}\n{{\"n\":-100000000}}\n{{\"n\":null}}\n{{}}",
            "繁體中文 ".repeat(2048)
        );
        std::fs::write(file.path(), compressed(data.as_bytes())).unwrap();
        for threads in [1, 4] {
            for chunk_bytes in [11, 64 * 1024] {
                let report = profile_json_gzip(file.path(), config(threads, chunk_bytes)).unwrap();
                assert_eq!(report.count, 5);
                assert_eq!(report.sum, Some(1.0));
                assert_eq!(report.decoded_bytes, data.len() as u64);
                assert!(report.chunks > 0);
            }
        }
    }

    #[test]
    fn gzip_pipeline_rejects_late_crc_truncation_and_ignored_invalid_json() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let bytes = compressed("{\"n\":1}\n".repeat(30_000).as_bytes());
        let mut corrupt = bytes.clone();
        let footer = corrupt.len() - 8;
        corrupt[footer] ^= 1;
        for data in [
            corrupt,
            bytes[..bytes.len() - 3].to_vec(),
            compressed(b"{\"ignored\": [0,]}\n"),
        ] {
            std::fs::write(file.path(), data).unwrap();
            assert!(profile_json_gzip(file.path(), config(4, 1024)).is_err());
        }
    }

    fn completes_with_released_memory(
        reader: impl Read + Send + 'static,
        config: JsonGzipProbeConfig,
    ) -> StreamResult<JsonGzipProbeReport> {
        let (sender, receiver) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let memory = MemoryTracker::new(Some(config.max_memory));
            let result = run_pipeline(reader, config, memory.clone());
            sender.send((result, memory.used())).unwrap();
        });
        let (result, used) = receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("all parser/producer workers must stop, including blocked queue senders");
        handle.join().unwrap();
        assert_eq!(used, 0, "queued chunks and partial state charges must be released");
        result
    }

    #[test]
    fn gzip_pipeline_budget_and_earlier_parser_error_cancel_queued_work() {
        let mut small_budget = config(4, 11);
        small_budget.max_memory = 8192;
        let large_row = format!("{{\"n\":1,\"ignored\":\"{}\"}}\n", "x".repeat(32 * 1024));
        assert!(matches!(
            completes_with_released_memory(io::Cursor::new(large_row.into_bytes()), small_budget),
            Err(StreamError::MemoryBudgetExceeded)
        ));

        let data = format!("{{\"n\":\"invalid SUM input\"}}\n{}", "{\"n\":1}\n".repeat(30_000));
        let mut bytes = compressed(data.as_bytes());
        let footer = bytes.len() - 8;
        bytes[footer] ^= 1;
        assert!(
            matches!(
                completes_with_released_memory(flate2::read::GzDecoder::new(io::Cursor::new(bytes)), config(4, 1024)),
                Err(StreamError::Aggregate)
            ),
            "the earlier parser/aggregate failure precedes the late decoder CRC error"
        );
    }

    #[test]
    fn gzip_pipeline_preserves_same_chunk_data_before_late_crc_or_io_error() {
        fn sequential_error(reader: impl Read + 'static) -> StreamError {
            BatchGroupByOperator::new(
                Box::new(JsonBatchScanOperator::new(
                    Box::new(BufReader::with_capacity(64 * 1024, reader)),
                    vec!["n".into()],
                )),
                vec![],
                vec![NamedAggregate::new(
                    Aggregate::Sum(
                        SumAggregate::new(),
                        Named::Expression(
                            Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("n".into())])),
                            None,
                        ),
                    ),
                    Some("sum".into()),
                )],
                Variables::new(),
                Arc::new(FunctionRegistry::new()),
            )
            .consume_partial()
            .err()
            .unwrap()
        }
        // A partial scanner batch must still surface its terminal read error
        // before SUM; a complete batch can execute SUM before that read.
        for rows in [
            1,
            crate::execution::batch::BATCH_SIZE - 1,
            crate::execution::batch::BATCH_SIZE,
        ] {
            let data = format!("{{\"n\":\"invalid SUM input\"}}\n{}", "{\"n\":1}\n".repeat(rows - 1));
            let mut bytes = compressed(data.as_bytes());
            let footer = bytes.len() - 8;
            bytes[footer] ^= 1;
            let expected = sequential_error(flate2::read::GzDecoder::new(io::Cursor::new(bytes.clone())));
            assert_eq!(
                expected,
                if rows < crate::execution::batch::BATCH_SIZE {
                    StreamError::Reader
                } else {
                    StreamError::Aggregate
                }
            );
            let actual = completes_with_released_memory(
                flate2::read::GzDecoder::new(io::Cursor::new(bytes)),
                config(4, 64 * 1024),
            );
            assert_eq!(
                actual.err().unwrap(),
                expected,
                "same-chunk error order with {rows} rows"
            );
        }
        struct ErrorAfterPrefix(io::Cursor<Vec<u8>>);
        impl Read for ErrorAfterPrefix {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                if self.0.position() == self.0.get_ref().len() as u64 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "injected trailing error"));
                }
                self.0.read(output)
            }
        }
        for data in [
            b"{\"n\":\"invalid SUM input\"}\n".to_vec(),
            b"{\"n\":1}\n".to_vec(),
            format!(
                "{{\"n\":\"invalid SUM input\"}}\n{}",
                "{\"n\":1}\n".repeat(crate::execution::batch::BATCH_SIZE - 1)
            )
            .into_bytes(),
        ] {
            let expected = sequential_error(ErrorAfterPrefix(io::Cursor::new(data.clone())));
            let result = completes_with_released_memory(ErrorAfterPrefix(io::Cursor::new(data)), config(4, 64 * 1024));
            assert_eq!(
                result.err().unwrap(),
                expected,
                "the deferred read error must neither discard rows nor be swallowed"
            );
        }
    }

    #[test]
    fn gzip_pipeline_decoder_panic_cancels_waiting_parsers() {
        struct PanickingReader;
        impl Read for PanickingReader {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                panic!("injected decoder panic");
            }
        }
        assert!(
            matches!(completes_with_released_memory(PanickingReader, config(4, 1024)), Err(StreamError::General(message)) if message == "gzip decoder worker panicked")
        );
    }

    #[test]
    fn gzip_pipeline_uninstrumented_empty_input_returns_sql_empty_aggregate() {
        let mut config = config(4, 1024);
        config.instrument_workers = false;
        let report = completes_with_released_memory(io::Cursor::new(Vec::<u8>::new()), config).unwrap();
        assert_eq!(report.count, 0);
        assert_eq!(report.sum, None);
        assert_eq!(report.chunks, 0);
        assert_eq!(report.decoded_bytes, 0);
        assert_eq!(report.producer_busy_ns, None);
        assert_eq!(report.producer_send_wait_ns, None);
        assert!(report.workers.is_empty());
    }
}
