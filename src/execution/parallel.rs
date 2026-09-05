#[cfg(test)]
use crate::common::types::Value;
use crate::common::types::Variables;
use crate::execution::batch::*;
use crate::execution::batch_groupby::{BatchGroupByOperator, PartialAggregateState, output_names};
use crate::execution::batch_scan::BatchScanOperator;
use crate::execution::log_schema::LogSchema;
use crate::execution::memory::{MemoryReservation, MemoryTracker, estimate_batch};
#[cfg(test)]
use crate::execution::types::{AvgAggregate, CountAggregate, MaxAggregate, MinAggregate, SumAggregate};
use crate::execution::types::{Formula, NamedAggregate, StreamError, StreamResult};
use crate::functions::FunctionRegistry;
use crate::syntax::ast::PathExpr;
use memmap2::MmapOptions;
#[cfg(test)]
use ordered_float::OrderedFloat;
#[cfg(test)]
use rayon::prelude::*;
use std::cell::RefCell;
#[cfg(test)]
use std::cmp;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufRead, Read};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;

pub(crate) const PARALLEL_THRESHOLD: u64 = 16 * 1024 * 1024; // 16MB
const PARALLEL_QUEUE_CAPACITY: usize = 2;
const PARALLEL_TASK_BYTES: usize = 256 * 1024;
const MMAP_READ_WINDOW: usize = 1024 * 1024;

/// A reader over one newline-aligned range of a shared mapping. Cancellation is
/// checked while reading, including when a selective filter emits no batches.
struct MmapRangeReader {
    mmap: Arc<memmap2::Mmap>,
    range: Range<usize>,
    cancelled: Arc<AtomicBool>,
}

impl Read for MmapRangeReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let input = self.fill_buf()?;
        let len = output.len().min(input.len());
        output[..len].copy_from_slice(&input[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl BufRead for MmapRangeReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.cancelled.load(AtomicOrdering::Relaxed) {
            return Ok(&[]);
        }
        let end = self.range.start.saturating_add(MMAP_READ_WINDOW).min(self.range.end);
        Ok(&self.mmap[self.range.start..end])
    }

    fn consume(&mut self, amount: usize) {
        self.range.start += amount.min(self.range.len());
    }
}

enum ScanMessage {
    Batch(ColumnBatch, MemoryReservation),
    Partial(PartialAggregateState),
    TaskFinished,
    Finished(StreamResult<()>),
}

struct AggregateSpec {
    keys: Vec<PathExpr>,
    aggregates: Vec<NamedAggregate>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
}

type ScanFactory = dyn Fn(usize, Arc<AtomicBool>) -> Box<dyn BatchStream> + Send + Sync;
type CountFactory = dyn Fn(usize, Arc<AtomicBool>) -> StreamResult<i64> + Send + Sync;

struct PendingWorkers {
    count: usize,
    tasks: usize,
    make_stream: Arc<ScanFactory>,
    make_aggregate_stream: Option<Arc<ScanFactory>>,
    count_rows: Option<(String, Arc<CountFactory>)>,
}

/// Parallel scan with bounded per-worker queues, drained in small task order.
/// At most (queue capacity + one producer batch) per worker is retained, in
/// addition to the batch held by the consumer and scanner scratch buffers.
pub(crate) struct ParallelBatchStream {
    receivers: RefCell<VecDeque<Receiver<ScanMessage>>>,
    workers: RefCell<Vec<JoinHandle<()>>>,
    pending: RefCell<Option<PendingWorkers>>,
    cancelled: Arc<AtomicBool>,
    schema: BatchSchema,
    remaining: Option<usize>,
    memory: MemoryTracker,
    aggregate: Option<Arc<AggregateSpec>>,
}

impl ParallelBatchStream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        mmap: memmap2::Mmap,
        num_threads: usize,
        schema: LogSchema,
        projected_fields: Vec<usize>,
        filter_field_indices: Vec<usize>,
        pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
        row_limit: Option<usize>,
    ) -> io::Result<Self> {
        let batch_schema = BatchSchema {
            names: projected_fields
                .iter()
                .map(|&index| schema.field_name(index).to_string())
                .collect(),
            types: projected_fields
                .iter()
                .map(|&index| crate::execution::batch_scan::datatype_to_column_type(&schema.field_type(index)))
                .collect(),
        };
        Self::from_mmap(mmap, num_threads, batch_schema, row_limit, move |reader| {
            Box::new(BatchScanOperator::new(
                Box::new(reader),
                schema.clone(),
                projected_fields.clone(),
                filter_field_indices.clone(),
                pushed_predicate.clone(),
            ))
        })
    }

    pub(crate) fn new_json(
        mmap: memmap2::Mmap,
        num_threads: usize,
        fields: Vec<String>,
        row_limit: Option<usize>,
    ) -> io::Result<Self> {
        let mut seen = std::collections::HashSet::new();
        let fields: Vec<String> = fields.into_iter().filter(|field| seen.insert(field.clone())).collect();
        let batch_schema = BatchSchema {
            names: fields.clone(),
            types: vec![ColumnType::Mixed; fields.len()],
        };
        Self::from_mmap(mmap, num_threads, batch_schema, row_limit, move |reader| {
            Box::new(crate::execution::json_batch_scan::JsonBatchScanOperator::new(
                Box::new(reader),
                fields.clone(),
            ))
        })
    }

    /// Framing-only ungrouped COUNT(*) keeps i64 partial counts in workers.
    /// A pushed predicate still parses its required fields in each worker.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_count(
        mmap: memmap2::Mmap,
        num_threads: usize,
        schema: LogSchema,
        filter_field_indices: Vec<usize>,
        pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
        output_name: String,
        registry: Arc<FunctionRegistry>,
        memory: MemoryTracker,
    ) -> io::Result<ParallelAggregateStream> {
        let mmap = Arc::new(mmap);
        let threads = if num_threads == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            num_threads
        };
        let workers = threads.min(mmap.len());
        let mut stream = Self::spawn_tasks(
            workers,
            workers,
            BatchSchema {
                names: vec![],
                types: vec![],
            },
            None,
            |_, _| unreachable!("framed COUNT uses its count factory"),
        )?;
        if let Some(pending) = stream.pending.get_mut() {
            pending.count_rows = Some((
                output_name.clone(),
                Arc::new(move |worker, cancelled| {
                    let point = |index: usize| mmap.len() / workers * index + index.min(mmap.len() % workers);
                    let range = Self::aligned_range(&mmap, point(worker), point(worker + 1));
                    let reader = MmapRangeReader {
                        mmap: mmap.clone(),
                        range,
                        cancelled,
                    };
                    BatchScanOperator::new(
                        Box::new(reader),
                        schema.clone(),
                        vec![],
                        filter_field_indices.clone(),
                        pushed_predicate.clone(),
                    )
                    .consume_count()
                }),
            ));
        }
        use crate::execution::types::{Aggregate, CountAggregate, Named};
        Ok(stream.into_aggregate(
            vec![],
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some(output_name),
            )],
            Variables::new(),
            registry,
            memory,
        ))
    }

    /// Byte-grid tasks find their own adjacent newline boundaries lazily. No
    /// whole-file line index or queue of all tasks is retained. A line crossing
    /// several grid cells belongs to its first cell; later cells are empty.
    fn aligned_range(data: &[u8], start: usize, end: usize) -> Range<usize> {
        let boundary = |offset: usize| {
            if offset == 0 || offset >= data.len() {
                return offset.min(data.len());
            }
            memchr::memchr(b'\n', &data[offset - 1..]).map_or(data.len(), |pos| offset + pos)
        };
        boundary(start)..boundary(end)
    }

    fn from_mmap<F>(
        mmap: memmap2::Mmap,
        num_threads: usize,
        schema: BatchSchema,
        row_limit: Option<usize>,
        make_scanner: F,
    ) -> io::Result<Self>
    where
        F: Fn(MmapRangeReader) -> Box<dyn BatchStream> + Send + Sync + 'static,
    {
        let mmap = Arc::new(mmap);
        let threads = if num_threads == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            num_threads
        };
        let workers = threads.min(mmap.len()).max(1);
        let task_bytes = mmap.len().div_ceil(workers).clamp(1, PARALLEL_TASK_BYTES);
        let tasks = mmap.len().div_ceil(task_bytes);
        let workers = workers.min(tasks);
        let make_scanner = Arc::new(make_scanner);
        let scan_mmap = mmap.clone();
        let scan_factory = make_scanner.clone();
        let mut stream = Self::spawn_tasks(workers, tasks, schema, row_limit, move |task, cancelled| {
            let range = Self::aligned_range(&scan_mmap, task * task_bytes, (task + 1) * task_bytes);
            scan_factory(MmapRangeReader {
                mmap: scan_mmap.clone(),
                range,
                cancelled,
            })
        })?;
        if let Some(pending) = stream.pending.get_mut() {
            // An aggregate drains a whole range without sending row batches,
            // so its workers can retain one state each and run concurrently.
            pending.make_aggregate_stream = Some(Arc::new(move |worker, cancelled| {
                let point = |index: usize| mmap.len() / workers * index + index.min(mmap.len() % workers);
                let range = Self::aligned_range(&mmap, point(worker), point(worker + 1));
                make_scanner(MmapRangeReader {
                    mmap: mmap.clone(),
                    range,
                    cancelled,
                })
            }));
        }
        Ok(stream)
    }

    #[cfg(test)]
    fn spawn_workers<F>(
        num_workers: usize,
        schema: BatchSchema,
        row_limit: Option<usize>,
        make_stream: F,
    ) -> io::Result<Self>
    where
        F: Fn(usize, Arc<AtomicBool>) -> Box<dyn BatchStream> + Send + Sync + 'static,
    {
        Self::spawn_tasks(num_workers, num_workers, schema, row_limit, make_stream)
    }

    fn spawn_tasks<F>(
        num_workers: usize,
        num_tasks: usize,
        schema: BatchSchema,
        row_limit: Option<usize>,
        make_stream: F,
    ) -> io::Result<Self>
    where
        F: Fn(usize, Arc<AtomicBool>) -> Box<dyn BatchStream> + Send + Sync + 'static,
    {
        let stream = Self {
            receivers: RefCell::new(VecDeque::new()),
            workers: RefCell::new(Vec::new()),
            pending: RefCell::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
            schema,
            remaining: row_limit,
            memory: MemoryTracker::default(),
            aggregate: None,
        };
        if row_limit == Some(0) {
            return Ok(stream);
        }
        *stream.pending.borrow_mut() = Some(PendingWorkers {
            count: num_workers,
            tasks: num_tasks,
            make_stream: Arc::new(make_stream),
            make_aggregate_stream: None,
            count_rows: None,
        });
        Ok(stream)
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.memory = memory;
        self
    }

    /// Install worker-local filter/projection operators before the first pull.
    pub(crate) fn map_workers<F>(mut self, output_schema: BatchSchema, wrapper: F) -> Self
    where
        F: Fn(Box<dyn BatchStream>) -> Box<dyn BatchStream> + Send + Sync + 'static,
    {
        assert!(
            self.workers.borrow().is_empty(),
            "worker transforms must be installed before scanning"
        );
        if let Some(pending) = self.pending.get_mut().take() {
            let wrapper = Arc::new(wrapper);
            let aggregate_wrapper = wrapper.clone();
            *self.pending.get_mut() = Some(PendingWorkers {
                count: pending.count,
                tasks: pending.tasks,
                count_rows: pending.count_rows,
                make_stream: Arc::new(move |task, cancelled| wrapper((pending.make_stream)(task, cancelled))),
                make_aggregate_stream: pending.make_aggregate_stream.map(|factory| {
                    Arc::new(move |worker, cancelled| aggregate_wrapper(factory(worker, cancelled))) as Arc<ScanFactory>
                }),
            });
        }
        self.schema = output_schema;
        self
    }

    pub(crate) fn into_aggregate(
        mut self,
        keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
        memory: MemoryTracker,
    ) -> ParallelAggregateStream {
        assert!(
            self.workers.borrow().is_empty(),
            "aggregation must be installed before scanning"
        );
        assert!(
            BatchGroupByOperator::supports_parallel(&keys, &aggregates),
            "non-mergeable parallel aggregation"
        );
        let names = output_names(&keys, &aggregates);
        let schema = BatchSchema {
            types: vec![ColumnType::Mixed; names.len()],
            names,
        };
        if let Some(pending) = self.pending.get_mut() {
            if let Some(factory) = pending.make_aggregate_stream.take() {
                pending.make_stream = factory;
                pending.tasks = pending.count;
            }
        }
        self.memory = memory;
        self.remaining = None;
        self.aggregate = Some(Arc::new(AggregateSpec {
            keys,
            aggregates,
            variables,
            registry,
        }));
        ParallelAggregateStream {
            scan: self,
            schema,
            consumed: false,
            output_memory: MemoryReservation::default(),
        }
    }

    fn start_workers(&mut self) -> io::Result<()> {
        let Some(pending) = self.pending.borrow_mut().take() else {
            return Ok(());
        };
        for worker in 0..pending.count {
            let (sender, receiver) = mpsc::sync_channel(PARALLEL_QUEUE_CAPACITY);
            let make_stream = pending.make_stream.clone();
            let cancelled = self.cancelled.clone();
            let memory = self.memory.clone();
            let aggregate = self.aggregate.clone();
            let tasks = pending.tasks;
            let count = pending.count;
            let count_rows = pending.count_rows.clone();
            let handle = std::thread::Builder::new()
                .name(format!("logq-scan-{worker}"))
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        for task in (worker..tasks).step_by(count) {
                            if cancelled.load(AtomicOrdering::Relaxed) {
                                break;
                            }
                            if let Some((name, factory)) = count_rows.as_ref() {
                                let count = factory(task, cancelled.clone())?;
                                let state = PartialAggregateState::count_star(count, name.clone(), memory.clone())?;
                                if cancelled.load(AtomicOrdering::Relaxed)
                                    || sender.send(ScanMessage::Partial(state)).is_err()
                                {
                                    break;
                                }
                            } else {
                                let scanner = make_stream(task, cancelled.clone());
                                Self::run_worker(scanner, &sender, &cancelled, &memory, aggregate.as_deref())?;
                            }
                            if cancelled.load(AtomicOrdering::Relaxed)
                                || sender.send(ScanMessage::TaskFinished).is_err()
                            {
                                break;
                            }
                        }
                        Ok(())
                    }))
                    .unwrap_or_else(|_| Err(StreamError::General("parallel scan worker panicked".into())));
                    if !cancelled.load(AtomicOrdering::Relaxed) {
                        let _ = sender.send(ScanMessage::Finished(result));
                    }
                })?;
            self.receivers.borrow_mut().push_back(receiver);
            self.workers.borrow_mut().push(handle);
        }
        Ok(())
    }

    fn run_worker(
        mut scanner: Box<dyn BatchStream>,
        sender: &SyncSender<ScanMessage>,
        cancelled: &AtomicBool,
        memory: &MemoryTracker,
        aggregate: Option<&AggregateSpec>,
    ) -> StreamResult<()> {
        if let Some(spec) = aggregate {
            if cancelled.load(AtomicOrdering::Relaxed) {
                return Ok(());
            }
            let state = BatchGroupByOperator::new(
                scanner,
                spec.keys.clone(),
                spec.aggregates.clone(),
                spec.variables.clone(),
                spec.registry.clone(),
            )
            .with_memory_tracker(memory.clone())
            .consume_partial()?;
            if !cancelled.load(AtomicOrdering::Relaxed) {
                let _ = sender.send(ScanMessage::Partial(state));
            }
            return Ok(());
        }
        while !cancelled.load(AtomicOrdering::Relaxed) {
            let Some(batch) = scanner.next_batch()? else {
                break;
            };
            let mut reservation = MemoryReservation::new(memory.clone());
            if reservation.is_enabled() {
                reservation.add(estimate_batch(&batch))?;
            }
            if cancelled.load(AtomicOrdering::Relaxed) || sender.send(ScanMessage::Batch(batch, reservation)).is_err() {
                break;
            }
        }
        Ok(())
    }

    fn stop(&self) {
        self.cancelled.store(true, AtomicOrdering::Relaxed);
        self.pending.borrow_mut().take();
        // Disconnect every queue before joining: a later chunk may be blocked
        // behind its full queue while the consumer is still in an earlier one.
        self.receivers.borrow_mut().clear();
        for worker in self.workers.borrow_mut().drain(..) {
            let _ = worker.join();
        }
    }
}

impl BatchStream for ParallelBatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if let Err(error) = self.start_workers() {
            self.stop();
            return Err(StreamError::General(format!(
                "could not start parallel scan worker: {error}"
            )));
        }
        loop {
            let message = self.receivers.borrow().front().map(Receiver::recv);
            match message {
                Some(Ok(ScanMessage::Batch(mut batch, _reservation))) => {
                    if let Some(remaining) = self.remaining {
                        let active = batch.selection.count_active(batch.len);
                        if active >= remaining {
                            if active > remaining {
                                let mut selected = crate::simd::bitmap::Bitmap::all_unset(batch.len);
                                let mut kept = 0;
                                for row in 0..batch.len {
                                    if batch.selection.is_active(row, batch.len) {
                                        if kept == remaining {
                                            break;
                                        }
                                        selected.set(row);
                                        kept += 1;
                                    }
                                }
                                batch.selection = crate::simd::selection::SelectionVector::Bitmap(selected);
                            }
                            self.remaining = Some(0);
                            self.stop();
                        } else {
                            self.remaining = Some(remaining - active);
                        }
                    }
                    return Ok(Some(batch));
                }
                Some(Ok(ScanMessage::TaskFinished)) => {
                    let mut receivers = self.receivers.borrow_mut();
                    let receiver = receivers.pop_front().unwrap();
                    receivers.push_back(receiver);
                }
                Some(Ok(ScanMessage::Finished(Ok(())))) => {
                    self.receivers.borrow_mut().pop_front();
                }
                Some(Ok(ScanMessage::Finished(Err(error)))) => {
                    self.stop();
                    return Err(error);
                }
                Some(Ok(ScanMessage::Partial(_))) => {
                    self.stop();
                    return Err(StreamError::General(
                        "partial aggregation requires its merge consumer".into(),
                    ));
                }
                Some(Err(_)) => {
                    self.stop();
                    return Err(StreamError::General(
                        "parallel scan worker disconnected without completing".into(),
                    ));
                }
                None => {
                    self.stop();
                    return Ok(None);
                }
            }
        }
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.stop();
    }
}

impl Drop for ParallelBatchStream {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Worker-local aggregation sends mergeable states, preserving SUM/AVG f64
/// accumulators until the final result is produced on the consumer thread.
pub(crate) struct ParallelAggregateStream {
    scan: ParallelBatchStream,
    schema: BatchSchema,
    consumed: bool,
    output_memory: MemoryReservation,
}

impl ParallelAggregateStream {
    fn consume(&mut self) -> StreamResult<Option<ColumnBatch>> {
        self.scan
            .start_workers()
            .map_err(|error| StreamError::General(format!("could not start parallel aggregate worker: {error}")))?;
        let mut merged: Option<PartialAggregateState> = None;
        loop {
            let message = self.scan.receivers.borrow().front().map(Receiver::recv);
            match message {
                Some(Ok(ScanMessage::Partial(state))) => {
                    if let Some(merged) = merged.as_mut() {
                        merged.merge(state)?;
                    } else {
                        merged = Some(state);
                    }
                }
                Some(Ok(ScanMessage::TaskFinished)) => {
                    let mut receivers = self.scan.receivers.borrow_mut();
                    let receiver = receivers.pop_front().unwrap();
                    receivers.push_back(receiver);
                }
                Some(Ok(ScanMessage::Finished(Ok(())))) => {
                    self.scan.receivers.borrow_mut().pop_front();
                }
                Some(Ok(ScanMessage::Finished(Err(error)))) => return Err(error),
                Some(Ok(ScanMessage::Batch(_, _))) => {
                    return Err(StreamError::General(
                        "aggregate worker emitted unaggregated rows".into(),
                    ));
                }
                Some(Err(_)) => {
                    return Err(StreamError::General(
                        "parallel aggregate worker disconnected without completing".into(),
                    ));
                }
                None => break,
            }
        }
        self.scan.stop();
        let state = match merged {
            Some(state) => state,
            None => {
                let spec = self.scan.aggregate.as_ref().unwrap();
                BatchGroupByOperator::new(
                    Box::new(PrecomputedBatchStream::new(vec![], self.scan.schema.clone())),
                    spec.keys.clone(),
                    spec.aggregates.clone(),
                    spec.variables.clone(),
                    spec.registry.clone(),
                )
                .with_memory_tracker(self.scan.memory.clone())
                .consume_partial()?
            }
        };
        let (batch, reservation) = state.finish()?;
        self.output_memory = reservation;
        if batch.len == 0 {
            self.output_memory.resize(0)?;
            return Ok(None);
        }
        Ok(Some(batch))
    }
}

impl BatchStream for ParallelAggregateStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.consumed {
            self.output_memory.resize(0)?;
            return Ok(None);
        }
        self.consumed = true;
        let result = self.consume();
        if result.is_err() {
            self.scan.stop();
        }
        result
    }
    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.scan.close();
    }
}

pub(crate) enum ScanStrategy {
    Mmap(memmap2::Mmap),
    Sequential,
}

pub(crate) fn choose_strategy(path: &Path) -> ScanStrategy {
    if crate::execution::datasource::path_is_gzip(path).unwrap_or(true) {
        return ScanStrategy::Sequential;
    }
    let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);

    if file_size < PARALLEL_THRESHOLD || file_size == 0 {
        return ScanStrategy::Sequential;
    }

    #[cfg(target_pointer_width = "64")]
    {
        if let Ok(mmap) = File::open(path).and_then(|f| unsafe { MmapOptions::new().map(&f) }) {
            return ScanStrategy::Mmap(mmap);
        }
    }

    ScanStrategy::Sequential
}

/// Split a byte slice into chunks along newline boundaries.
#[cfg(test)]
pub(crate) fn split_chunks(data: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    if data.is_empty() || num_chunks == 0 {
        return vec![];
    }

    let chunk_size = data.len() / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for i in 0..num_chunks {
        if start >= data.len() {
            break;
        }

        if i == num_chunks - 1 {
            chunks.push(&data[start..]);
            break;
        }

        let raw_end = cmp::min(start + chunk_size, data.len());

        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1,
            None => data.len(),
        };

        chunks.push(&data[start..end]);
        start = end;
    }

    while chunks.last().is_some_and(|c| c.is_empty()) {
        chunks.pop();
    }

    chunks
}

#[cfg(test)]
fn collect_results<T>(results: Vec<StreamResult<T>>) -> StreamResult<Vec<T>> {
    let mut collected = Vec::with_capacity(results.len());
    for result in results {
        collected.push(result?);
    }
    Ok(collected)
}

#[cfg(test)]
pub(crate) fn parallel_scan_chunks(
    data: &[u8],
    num_threads: usize,
    schema: &LogSchema,
    projected_fields: &[usize],
    filter_field_indices: &[usize],
    pushed_predicate: &Option<(Formula, Variables, Arc<FunctionRegistry>)>,
) -> StreamResult<Vec<Vec<ColumnBatch>>> {
    parallel_scan_chunks_limited(
        data,
        num_threads,
        schema,
        projected_fields,
        filter_field_indices,
        pushed_predicate,
        None,
    )
}

/// Like `parallel_scan_chunks` but with an optional row limit.
/// When set, workers stop early once `row_limit` total active rows have been
/// collected across all chunks. This avoids scanning the full file for LIMIT queries.
#[cfg(test)]
pub(crate) fn parallel_scan_chunks_limited(
    data: &[u8],
    num_threads: usize,
    schema: &LogSchema,
    projected_fields: &[usize],
    filter_field_indices: &[usize],
    pushed_predicate: &Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    row_limit: Option<usize>,
) -> StreamResult<Vec<Vec<ColumnBatch>>> {
    let chunks = split_chunks(data, num_threads);
    if chunks.is_empty() {
        return Ok(vec![]);
    }

    let global_count = std::sync::atomic::AtomicUsize::new(0);
    let limit = row_limit.unwrap_or(usize::MAX);

    let partial_results: Vec<StreamResult<Vec<ColumnBatch>>> = chunks
        .par_iter()
        .map(|chunk| {
            let reader: Box<dyn BufRead> = Box::new(io::Cursor::new(chunk.to_vec()));
            let mut scanner = BatchScanOperator::new(
                reader,
                schema.clone(),
                projected_fields.to_vec(),
                filter_field_indices.to_vec(),
                pushed_predicate.clone(),
            );
            let mut batches = Vec::new();
            while let Some(batch) = scanner.next_batch()? {
                let active = batch.selection.count_active(batch.len);
                batches.push(batch);
                // Check if we've collectively found enough rows
                let prev = global_count.fetch_add(active, std::sync::atomic::Ordering::Relaxed);
                if prev + active >= limit {
                    break;
                }
            }
            Ok(batches)
        })
        .collect();

    collect_results(partial_results)
}

// ---------------------------------------------------------------------------
// Parallel aggregation merge functions
// ---------------------------------------------------------------------------

/// Merge two CountAggregates by summing counts per key.
#[cfg(test)]
pub(crate) fn merge_count(a: &CountAggregate, b: &CountAggregate) -> CountAggregate {
    let mut merged = a.clone();
    for (key, &count) in b.counts.iter() {
        *merged.counts.entry(key.clone()).or_insert(0) += count;
    }
    merged
}

/// Merge two SumAggregates by summing values per key.
#[cfg(test)]
pub(crate) fn merge_sum(a: &SumAggregate, b: &SumAggregate) -> SumAggregate {
    let mut merged = a.clone();
    for (key, &sum) in b.sums.iter() {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat(0.0f64));
        *entry = OrderedFloat(entry.into_inner() + sum.into_inner());
    }
    merged
}

/// Merge two AvgAggregates by combining (sum, count) pairs per key.
#[cfg(test)]
pub(crate) fn merge_avg(a: &AvgAggregate, b: &AvgAggregate) -> AvgAggregate {
    let mut merged = a.clone();
    for (key, &sum) in b.sums.iter() {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat(0.0f64));
        *entry = OrderedFloat(entry.into_inner() + sum.into_inner());
    }
    for (key, &count) in b.counts.iter() {
        *merged.counts.entry(key.clone()).or_insert(0) += count;
    }
    merged
}

/// Merge two MinAggregates by taking the minimum value per key.
#[cfg(test)]
pub(crate) fn merge_min(a: &MinAggregate, b: &MinAggregate) -> MinAggregate {
    let mut merged = a.clone();
    for (key, value) in b.mins.iter() {
        match merged.mins.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let should_replace = match (e.get(), value) {
                    (Value::Int(i1), Value::Int(i2)) => i2 < i1,
                    (Value::Float(f1), Value::Float(f2)) => f2 < f1,
                    _ => false,
                };
                if should_replace {
                    e.insert(value.clone());
                }
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
            }
        }
    }
    merged
}

/// Merge two MaxAggregates by taking the maximum value per key.
#[cfg(test)]
pub(crate) fn merge_max(a: &MaxAggregate, b: &MaxAggregate) -> MaxAggregate {
    let mut merged = a.clone();
    for (key, value) in b.maxs.iter() {
        match merged.maxs.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let should_replace = match (e.get(), value) {
                    (Value::Int(i1), Value::Int(i2)) => i2 > i1,
                    (Value::Float(f1), Value::Float(f2)) => f2 > f1,
                    _ => false,
                };
                if should_replace {
                    e.insert(value.clone());
                }
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
            }
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// K-way merge for parallel ORDER BY
// ---------------------------------------------------------------------------

/// An entry in the k-way merge heap. Compared by sort key bytes, then
/// chunk index and row index as tiebreakers for stable ordering.
#[cfg(test)]
struct MergeEntry {
    key: Vec<u8>,
    chunk_idx: usize,
    row_idx: usize,
    record: Variables,
}

#[cfg(test)]
impl Eq for MergeEntry {}

#[cfg(test)]
impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.chunk_idx == other.chunk_idx && self.row_idx == other.row_idx
    }
}

#[cfg(test)]
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.chunk_idx.cmp(&other.chunk_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}

#[cfg(test)]
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Perform a k-way merge of pre-sorted chunks. Each chunk is a Vec of
/// `(sort_key_bytes, record)` pairs sorted in ascending key order.
/// Returns records in globally-sorted order, optionally limited to `limit`.
#[cfg(test)]
pub(crate) fn kway_merge(chunks: Vec<Vec<(Vec<u8>, Variables)>>, limit: Option<usize>) -> Vec<Variables> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();

    // Iterators to track current position within each chunk.
    let mut iters: Vec<std::vec::IntoIter<(Vec<u8>, Variables)>> = chunks.into_iter().map(|c| c.into_iter()).collect();

    // Seed the heap with the first element from each chunk.
    for (chunk_idx, iter) in iters.iter_mut().enumerate() {
        if let Some((key, record)) = iter.next() {
            heap.push(Reverse(MergeEntry {
                key,
                chunk_idx,
                row_idx: 0,
                record,
            }));
        }
    }

    let cap = limit.unwrap_or(usize::MAX);
    let mut result: Vec<Variables> = Vec::with_capacity(std::cmp::min(cap, 1024));

    while let Some(Reverse(entry)) = heap.pop() {
        let chunk_idx = entry.chunk_idx;
        result.push(entry.record);

        if result.len() >= cap {
            break;
        }

        // Push the next element from the same chunk.
        if let Some((key, record)) = iters[chunk_idx].next() {
            heap.push(Reverse(MergeEntry {
                key,
                chunk_idx,
                row_idx: entry.row_idx + 1,
                record,
            }));
        }
    }

    result
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::execution::types::StreamError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    fn mmap_input(data: &[u8]) -> memmap2::Mmap {
        let mut mmap = memmap2::MmapMut::map_anon(data.len()).unwrap();
        mmap.copy_from_slice(data);
        mmap.make_read_only().unwrap()
    }

    fn scan_input(data: &[u8], workers: usize, limit: Option<usize>) -> ParallelBatchStream {
        ParallelBatchStream::new(
            mmap_input(data),
            workers,
            LogSchema::from_format("squid"),
            vec![1],
            vec![],
            None,
            limit,
        )
        .unwrap()
    }

    fn squid_lines(rows: usize) -> Vec<u8> {
        (0..rows)
            .map(|i| format!("1 {i:08} host status 0 GET url rfc peer type"))
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes()
    }

    fn collect_elapsed(stream: &mut ParallelBatchStream) -> Vec<i32> {
        let mut values = Vec::new();
        while let Some(batch) = stream.next_batch().unwrap() {
            for row in 0..batch.len {
                if batch.selection.is_active(row, batch.len) {
                    match BatchToRowAdapter::extract_value(&batch.columns[0], row) {
                        Value::String(v) => values.push(v.parse().unwrap()),
                        _ => panic!("expected elapsed string"),
                    }
                }
            }
        }
        values
    }

    #[test]
    fn lazy_task_ranges_cover_each_line_once_even_when_lines_span_many_tasks() {
        let data = b"\nshort\r\na-much-longer-line-with-no-internal-breaks\nlast";
        for task_bytes in 1..=data.len() + 1 {
            let ranges: Vec<_> = (0..data.len().div_ceil(task_bytes))
                .map(|task| ParallelBatchStream::aligned_range(data, task * task_bytes, (task + 1) * task_bytes))
                .collect();
            let actual: Vec<_> = ranges
                .into_iter()
                .flat_map(|range| data[range].iter().copied())
                .collect();
            assert_eq!(actual, data);
        }
    }

    #[test]
    fn streaming_scan_preserves_order_and_unterminated_last_line() {
        let rows = BATCH_SIZE * 64 + 17;
        for workers in [1, 2, 4, 8] {
            let mut stream = scan_input(&squid_lines(rows), workers, None);
            assert_eq!(collect_elapsed(&mut stream), (0..rows as i32).collect::<Vec<_>>());
        }
    }

    #[test]
    fn streaming_limit_keeps_first_rows_and_closes_workers() {
        let mut stream = scan_input(&squid_lines(BATCH_SIZE * 20), 4, Some(7));
        assert_eq!(collect_elapsed(&mut stream), (0..7).collect::<Vec<_>>());
        assert!(stream.workers.borrow().is_empty());
    }

    #[test]
    fn streaming_filtered_limit_keeps_first_matches_across_chunks() {
        use crate::execution::types::{Expression, Relation};
        use crate::syntax::ast::{PathExpr, PathSegment};

        let first_match = BATCH_SIZE * 3;
        let predicate = Formula::Predicate(
            Relation::GreaterEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "elapsed".into(),
            )]))),
            Box::new(Expression::Constant(Value::String(format!("{first_match:08}").into()))),
        );
        let mut stream = ParallelBatchStream::new(
            mmap_input(&squid_lines(BATCH_SIZE * 8)),
            4,
            LogSchema::from_format("squid"),
            vec![1],
            vec![1],
            Some((predicate, Variables::new(), Arc::new(FunctionRegistry::new()))),
            Some(7),
        )
        .unwrap();
        assert_eq!(
            collect_elapsed(&mut stream),
            (first_match as i32..first_match as i32 + 7).collect::<Vec<_>>()
        );
    }

    #[test]
    fn mmap_reader_observes_cancellation_before_another_batch_is_emitted() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut reader = MmapRangeReader {
            mmap: Arc::new(mmap_input(b"first\nsecond")),
            range: 0..12,
            cancelled: cancelled.clone(),
        };
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "first\n");
        cancelled.store(true, AtomicOrdering::Relaxed);
        line.clear();
        assert_eq!(reader.read_line(&mut line).unwrap(), 0);
    }

    struct ProbeStream {
        calls: Arc<Vec<AtomicUsize>>,
        alive: Arc<AtomicUsize>,
        progress: mpsc::Sender<usize>,
        worker: usize,
        remaining: usize,
        error: bool,
        schema: BatchSchema,
    }

    impl Drop for ProbeStream {
        fn drop(&mut self) {
            self.alive.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl BatchStream for ProbeStream {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            if self.error {
                return Err(StreamError::General("injected scan failure".into()));
            }
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            self.calls[self.worker].fetch_add(1, Ordering::SeqCst);
            let _ = self.progress.send(self.worker);
            Ok(Some(ColumnBatch {
                columns: vec![],
                names: vec![],
                selection: crate::simd::selection::SelectionVector::All,
                len: 1,
            }))
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    #[test]
    fn streaming_small_tasks_keep_worker_count_and_bound_reordering() {
        let workers = 3;
        let tasks = 18;
        let (started_tx, started_rx) = mpsc::channel();
        let mut stream = ParallelBatchStream::spawn_tasks(
            workers,
            tasks,
            BatchSchema {
                names: vec!["i".into()],
                types: vec![ColumnType::Mixed],
            },
            None,
            move |task, _| {
                started_tx.send((task, std::thread::current().id())).unwrap();
                let batch = ColumnBatch {
                    columns: vec![TypedColumn::Mixed {
                        data: vec![Value::Int(task as i32)],
                        null: crate::simd::bitmap::Bitmap::all_set(1),
                        missing: crate::simd::bitmap::Bitmap::all_set(1),
                    }],
                    names: vec!["i".into()],
                    selection: crate::simd::selection::SelectionVector::All,
                    len: 1,
                };
                Box::new(PrecomputedBatchStream::new(
                    vec![batch],
                    BatchSchema {
                        names: vec!["i".into()],
                        types: vec![ColumnType::Mixed],
                    },
                ))
            },
        )
        .unwrap();
        stream.start_workers().unwrap();
        let initial: Vec<_> = (0..workers * 2)
            .map(|_| started_rx.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        assert_eq!(
            initial
                .iter()
                .map(|(_, id)| *id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            workers
        );
        assert!(
            started_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "future tasks must stay bounded while consumer is idle"
        );
        let mut actual = Vec::new();
        while let Some(batch) = stream.next_batch().unwrap() {
            for row in 0..batch.len {
                actual.push(BatchToRowAdapter::extract_value(&batch.columns[0], row));
            }
        }
        assert_eq!(
            actual,
            (0..tasks).map(|task| Value::Int(task as i32)).collect::<Vec<_>>()
        );
        assert!(stream.workers.borrow().is_empty());
    }

    #[test]
    fn production_scan_schedules_more_tasks_than_workers_for_large_inputs() {
        let stream = scan_input(&squid_lines(BATCH_SIZE * 64), 4, None);
        let pending = stream.pending.borrow();
        let pending = pending.as_ref().unwrap();
        assert_eq!(pending.count, 4);
        assert!(pending.tasks > pending.count * 2);
    }

    #[test]
    fn streaming_backpressure_bounds_work_and_drop_joins_exact_workers() {
        for explicit_close in [false, true] {
            let num_workers = 3;
            let memory = MemoryTracker::new(Some(1_000_000));
            let calls = Arc::new((0..num_workers).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let alive = Arc::new(AtomicUsize::new(0));
            let (progress_tx, progress_rx) = mpsc::channel();
            let (started_tx, started_rx) = mpsc::channel();
            let calls_worker = calls.clone();
            let alive_worker = alive.clone();
            let mut stream = ParallelBatchStream::spawn_workers(
                num_workers,
                BatchSchema {
                    names: vec![],
                    types: vec![],
                },
                None,
                move |worker, _| {
                    alive_worker.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(std::thread::current().id()).unwrap();
                    Box::new(ProbeStream {
                        calls: calls_worker.clone(),
                        alive: alive_worker.clone(),
                        progress: progress_tx.clone(),
                        worker,
                        remaining: 100,
                        error: false,
                        schema: BatchSchema {
                            names: vec![],
                            types: vec![],
                        },
                    })
                },
            )
            .unwrap()
            .with_memory_tracker(memory.clone());
            assert!(started_rx.try_recv().is_err());
            stream.start_workers().unwrap();
            let ids = (0..num_workers)
                .map(|_| started_rx.recv_timeout(Duration::from_secs(5)).unwrap())
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(ids.len(), num_workers);
            // Each queue can hold two batches, with at most one more owned by its blocked producer.
            for _ in 0..num_workers * (PARALLEL_QUEUE_CAPACITY + 1) {
                progress_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            assert!(progress_rx.recv_timeout(Duration::from_millis(50)).is_err());
            for count in calls.iter() {
                assert_eq!(count.load(Ordering::SeqCst), PARALLEL_QUEUE_CAPACITY + 1);
            }
            assert!(
                memory.used() > 0,
                "queued and producer-owned batches must remain charged"
            );
            if explicit_close {
                stream.close();
                assert_eq!(alive.load(Ordering::SeqCst), 0);
            }
            drop(stream);
            assert_eq!(alive.load(Ordering::SeqCst), 0);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn streaming_worker_error_cancels_blocked_peers() {
        let calls = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let alive = Arc::new(AtomicUsize::new(0));
        let alive_worker = alive.clone();
        let (progress, _) = mpsc::channel();
        let mut stream = ParallelBatchStream::spawn_workers(
            3,
            BatchSchema {
                names: vec![],
                types: vec![],
            },
            None,
            move |worker, _| {
                alive_worker.fetch_add(1, Ordering::SeqCst);
                Box::new(ProbeStream {
                    calls: calls.clone(),
                    alive: alive_worker.clone(),
                    progress: progress.clone(),
                    worker,
                    remaining: 100,
                    error: worker == 0,
                    schema: BatchSchema {
                        names: vec![],
                        types: vec![],
                    },
                })
            },
        )
        .unwrap();
        assert!(
            matches!(stream.next_batch(), Err(StreamError::General(message)) if message == "injected scan failure")
        );
        assert_eq!(alive.load(Ordering::SeqCst), 0);
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn streaming_worker_panic_is_reported_as_an_error() {
        let mut stream = ParallelBatchStream::spawn_workers(
            1,
            BatchSchema {
                names: vec![],
                types: vec![],
            },
            None,
            |_, _| panic!("injected worker panic"),
        )
        .unwrap();
        assert!(matches!(stream.next_batch(), Err(StreamError::General(message)) if message.contains("panicked")));
    }

    #[test]
    fn streaming_zero_limit_starts_no_workers() {
        let mut stream = scan_input(&squid_lines(10), 8, Some(0));
        assert!(stream.workers.borrow().is_empty());
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn parallel_scan_budget_failure_cancels_workers_and_releases_all_charges() {
        let memory = crate::execution::memory::MemoryTracker::new(Some(1));
        let mut stream = scan_input(&squid_lines(BATCH_SIZE * 8), 4, None).with_memory_tracker(memory.clone());
        assert!(matches!(stream.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(memory.used(), 0);
        assert!(stream.workers.borrow().is_empty());
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn parallel_json_preserves_order_projection_and_dynamic_values() {
        let rows = BATCH_SIZE * 5 + 7;
        let data = (0..rows)
            .map(|i| format!(r#"{{"i":{i},"payload":{{"nested":[{i}]}},"unused":"ignored"}}"#))
            .collect::<Vec<_>>()
            .join("\n");
        let mut stream = ParallelBatchStream::new_json(
            mmap_input(data.as_bytes()),
            4,
            vec!["i".into(), "payload".into(), "i".into(), "absent".into()],
            None,
        )
        .unwrap();
        assert_eq!(stream.schema().names, vec!["i", "payload", "absent"]);
        let mut seen = 0;
        while let Some(batch) = stream.next_batch().unwrap() {
            for row in 0..batch.len {
                assert_eq!(
                    BatchToRowAdapter::extract_value(&batch.columns[0], row),
                    Value::Int(seen)
                );
                assert!(matches!(
                    BatchToRowAdapter::extract_value(&batch.columns[1], row),
                    Value::Object(_)
                ));
                assert_eq!(BatchToRowAdapter::extract_value(&batch.columns[2], row), Value::Missing);
                seen += 1;
            }
        }
        assert_eq!(seen as usize, rows);
    }

    #[test]
    fn parallel_worker_transform_runs_before_batches_leave_workers() {
        let rows = BATCH_SIZE * 8;
        let mut stream = scan_input(&squid_lines(rows), 4, None).map_workers(
            BatchSchema {
                names: vec!["elapsed".into()],
                types: vec![ColumnType::Utf8],
            },
            |child| Box::new(crate::execution::batch_limit::BatchLimitOperator::new(child, 1)),
        );
        // A local LIMIT belongs to each worker, unlike the stream's global limit.
        let results = collect_elapsed(&mut stream);
        assert_eq!(results.len(), 4);
        assert!(results.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn parallel_framed_count_handles_blank_utf8_and_last_line() {
        let input = "row\n \t\r\n\u{2003}\nother\r\n".repeat(20_000) + "last";
        for threads in [1, 4] {
            let memory = MemoryTracker::new(Some(128 * 1024));
            let mut stream = ParallelBatchStream::new_count(
                mmap_input(input.as_bytes()),
                threads,
                LogSchema::from_format("elb"),
                vec![],
                None,
                "n".into(),
                Arc::new(FunctionRegistry::new()),
                memory.clone(),
            )
            .unwrap();
            assert!(stream.scan.workers.borrow().is_empty());
            let batch = stream.next_batch().unwrap().unwrap();
            assert_eq!(batch.names, ["n"]);
            assert_eq!(
                BatchToRowAdapter::extract_value(&batch.columns[0], 0),
                Value::Int(40_001)
            );
            assert!(stream.next_batch().unwrap().is_none());
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn parallel_framed_count_propagates_invalid_utf8_and_budget_error() {
        for invalid_utf8 in [true, false] {
            let mut input = b"row\n".repeat(20_000);
            if invalid_utf8 {
                input.push(0xff);
            }
            let memory = MemoryTracker::new(Some(if invalid_utf8 { 128 * 1024 } else { 1 }));
            let mut stream = ParallelBatchStream::new_count(
                mmap_input(&input),
                4,
                LogSchema::from_format("elb"),
                vec![],
                None,
                "n".into(),
                Arc::new(FunctionRegistry::new()),
                memory.clone(),
            )
            .unwrap();
            if invalid_utf8 {
                assert!(matches!(stream.next_batch(), Err(StreamError::Reader)));
            } else {
                assert!(matches!(stream.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
            }
            assert!(stream.next_batch().unwrap().is_none());
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn parallel_framed_count_applies_predicate_in_each_worker() {
        use crate::execution::types::{Expression, Relation};
        use crate::syntax::ast::PathSegment;
        let rows = BATCH_SIZE * 8;
        let first_match = BATCH_SIZE * 3;
        let registry = Arc::new(FunctionRegistry::new());
        let predicate = Formula::Predicate(
            Relation::GreaterEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "elapsed".into(),
            )]))),
            Box::new(Expression::Constant(Value::String(format!("{first_match:08}").into()))),
        );
        let mut stream = ParallelBatchStream::new_count(
            mmap_input(&squid_lines(rows)),
            4,
            LogSchema::from_format("squid"),
            vec![1],
            Some((predicate, Variables::new(), registry.clone())),
            "n".into(),
            registry,
            MemoryTracker::default(),
        )
        .unwrap();
        let output = stream.next_batch().unwrap().unwrap();
        assert_eq!(
            BatchToRowAdapter::extract_value(&output.columns[0], 0),
            Value::Int((rows - first_match) as i32)
        );
    }

    #[test]
    fn parallel_partial_groups_merge_counts_and_f64_numeric_states() {
        use crate::execution::types::{Aggregate, Expression, Named, NamedAggregate};
        use crate::syntax::ast::{PathExpr, PathSegment};
        let path = |name: &str| PathExpr::new(vec![PathSegment::AttrName(name.into())]);
        let variable = || Named::Expression(Expression::Variable(path("x")), None);
        let aggregates = vec![
            NamedAggregate::new(Aggregate::Sum(SumAggregate::new(), variable()), Some("sum".into())),
            NamedAggregate::new(Aggregate::Avg(AvgAggregate::new(), variable()), Some("avg".into())),
            NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("count".into()),
            ),
        ];
        let data = (0..15_000)
            .map(|i| {
                let x = match i % 3 {
                    0 => 100_000_000,
                    1 => 1,
                    _ => -100_000_000,
                };
                format!("{{\"g\":{},\"x\":{x}}}\n", i % 2)
            })
            .collect::<String>();
        let memory = MemoryTracker::new(Some(128 * 1024));
        let scan =
            ParallelBatchStream::new_json(mmap_input(data.as_bytes()), 4, vec!["g".into(), "x".into()], None).unwrap();
        let mut grouped = scan.into_aggregate(
            vec![path("g")],
            aggregates,
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
            memory.clone(),
        );
        let batch = grouped.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 2);
        for row in 0..2 {
            assert_eq!(
                BatchToRowAdapter::extract_value(&batch.columns[1], row),
                Value::Float(OrderedFloat(2500.0))
            );
            assert_eq!(
                BatchToRowAdapter::extract_value(&batch.columns[2], row),
                Value::Float(OrderedFloat(1.0 / 3.0))
            );
            assert_eq!(
                BatchToRowAdapter::extract_value(&batch.columns[3], row),
                Value::Int(7500)
            );
        }
        assert!(grouped.next_batch().unwrap().is_none());
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn parallel_partial_budget_error_cancels_all_workers() {
        use crate::execution::types::{Aggregate, Named, NamedAggregate};
        let memory = MemoryTracker::new(Some(1));
        let scan = ParallelBatchStream::new_json(mmap_input(b"{\"x\":1}\n{\"x\":2}\n"), 2, vec![], None).unwrap();
        let mut grouped = scan.into_aggregate(
            vec![],
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("n".into()),
            )],
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
            memory.clone(),
        );
        assert!(matches!(grouped.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(memory.used(), 0);
        assert!(grouped.next_batch().unwrap().is_none());
    }

    #[test]
    fn parallel_json_propagates_malformed_input_but_limit_can_stop_before_it() {
        let mut data = "{\"i\":1}\n".repeat(BATCH_SIZE * 8);
        data.push_str("malformed");
        let mut limited =
            ParallelBatchStream::new_json(mmap_input(data.as_bytes()), 4, vec!["i".into()], Some(3)).unwrap();
        let batch = limited.next_batch().unwrap().unwrap();
        assert_eq!(batch.selection.count_active(batch.len), 3);
        assert!(limited.next_batch().unwrap().is_none());

        let mut all = ParallelBatchStream::new_json(mmap_input(data.as_bytes()), 4, vec!["i".into()], None).unwrap();
        loop {
            match all.next_batch() {
                Ok(Some(_)) => {}
                Err(StreamError::Reader) => break,
                _ => panic!("malformed input must fail"),
            }
        }
        assert!(all.workers.borrow().is_empty());
        assert!(all.next_batch().unwrap().is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Tuple;

    #[test]
    fn test_split_chunks_basic() {
        let data = b"line1\nline2\nline3\nline4\n";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
        for chunk in &chunks {
            assert!(chunk.ends_with(b"\n"), "each chunk should end with newline");
        }
    }

    #[test]
    fn test_split_chunks_empty() {
        let data = b"";
        let chunks = split_chunks(data, 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_zero_chunks() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_more_chunks_than_lines() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 10);
        assert!(chunks.len() <= 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_chunk() {
        let data = b"line1\nline2\nline3\n";
        let chunks = split_chunks(data, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data.as_slice());
    }

    #[test]
    fn test_split_chunks_no_trailing_newline() {
        let data = b"line1\nline2";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_line() {
        let data = b"single_line\n";
        let chunks = split_chunks(data, 4);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_scan_strategy_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("small.log");
        std::fs::write(&file_path, "hello world\n").unwrap();
        let strategy = choose_strategy(&file_path);
        assert!(matches!(strategy, ScanStrategy::Sequential));
    }

    #[test]
    fn test_scan_strategy_nonexistent() {
        let strategy = choose_strategy(std::path::Path::new("/tmp/nonexistent_logq_test_file.log"));
        assert!(matches!(strategy, ScanStrategy::Sequential));
    }

    #[test]
    fn test_parallel_scan_basic() {
        // Build 20 lines of squid-format data (10 whitespace-separated fields each)
        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(
                "ts{} {} host{} status{} {} GET url{} rfc{} peer{} type{}\n",
                i,
                i,
                i,
                i,
                i * 100,
                i,
                i,
                i,
                i
            ));
        }

        let schema = LogSchema::from_format("squid");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();

        let results = parallel_scan_chunks(data.as_bytes(), 2, &schema, &all_fields, &[], &None).unwrap();

        let total_rows: usize = results
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.len)
            .sum();

        assert_eq!(total_rows, 20);
    }

    // -------------------------------------------------------------------
    // Step 11: Merge function tests
    // -------------------------------------------------------------------

    fn key(s: &str) -> Option<Tuple> {
        Some(vec![Value::String(s.to_string().into())])
    }

    #[test]
    fn test_merge_count() {
        let mut a = CountAggregate::new();
        a.counts.insert(key("x"), 3);
        a.counts.insert(key("y"), 5);

        let mut b = CountAggregate::new();
        b.counts.insert(key("y"), 7);
        b.counts.insert(key("z"), 2);

        let merged = merge_count(&a, &b);
        assert_eq!(merged.counts.get(&key("x")), Some(&3));
        assert_eq!(merged.counts.get(&key("y")), Some(&12));
        assert_eq!(merged.counts.get(&key("z")), Some(&2));
    }

    #[test]
    fn test_merge_sum() {
        let mut a = SumAggregate::new();
        a.sums.insert(key("x"), OrderedFloat(1.0f64));
        a.sums.insert(key("y"), OrderedFloat(2.5f64));

        let mut b = SumAggregate::new();
        b.sums.insert(key("y"), OrderedFloat(3.5f64));
        b.sums.insert(key("z"), OrderedFloat(4.0f64));

        let merged = merge_sum(&a, &b);
        assert_eq!(merged.sums.get(&key("x")), Some(&OrderedFloat(1.0f64)));
        assert_eq!(merged.sums.get(&key("y")), Some(&OrderedFloat(6.0f64)));
        assert_eq!(merged.sums.get(&key("z")), Some(&OrderedFloat(4.0f64)));
    }

    #[test]
    fn test_merge_avg() {
        let mut a = AvgAggregate::new();
        a.sums.insert(key("x"), OrderedFloat(10.0f64));
        a.counts.insert(key("x"), 2);
        a.sums.insert(key("y"), OrderedFloat(6.0f64));
        a.counts.insert(key("y"), 3);

        let mut b = AvgAggregate::new();
        b.sums.insert(key("y"), OrderedFloat(14.0f64));
        b.counts.insert(key("y"), 7);
        b.sums.insert(key("z"), OrderedFloat(9.0f64));
        b.counts.insert(key("z"), 3);

        let merged = merge_avg(&a, &b);
        // x: only in a
        assert_eq!(merged.sums.get(&key("x")), Some(&OrderedFloat(10.0f64)));
        assert_eq!(merged.counts.get(&key("x")), Some(&2));
        // y: merged
        assert_eq!(merged.sums.get(&key("y")), Some(&OrderedFloat(20.0f64)));
        assert_eq!(merged.counts.get(&key("y")), Some(&10));
        // z: only in b
        assert_eq!(merged.sums.get(&key("z")), Some(&OrderedFloat(9.0f64)));
        assert_eq!(merged.counts.get(&key("z")), Some(&3));
    }

    #[test]
    fn test_merge_min() {
        let mut a = MinAggregate::new();
        a.mins.insert(key("x"), Value::Int(10));
        a.mins.insert(key("y"), Value::Int(3));

        let mut b = MinAggregate::new();
        b.mins.insert(key("y"), Value::Int(5));
        b.mins.insert(key("z"), Value::Int(1));

        let merged = merge_min(&a, &b);
        assert_eq!(merged.mins.get(&key("x")), Some(&Value::Int(10)));
        assert_eq!(merged.mins.get(&key("y")), Some(&Value::Int(3)));
        assert_eq!(merged.mins.get(&key("z")), Some(&Value::Int(1)));
    }

    #[test]
    fn test_merge_max() {
        let mut a = MaxAggregate::new();
        a.maxs.insert(key("x"), Value::Int(10));
        a.maxs.insert(key("y"), Value::Int(3));

        let mut b = MaxAggregate::new();
        b.maxs.insert(key("y"), Value::Int(5));
        b.maxs.insert(key("z"), Value::Int(1));

        let merged = merge_max(&a, &b);
        assert_eq!(merged.maxs.get(&key("x")), Some(&Value::Int(10)));
        assert_eq!(merged.maxs.get(&key("y")), Some(&Value::Int(5)));
        assert_eq!(merged.maxs.get(&key("z")), Some(&Value::Int(1)));
    }

    // -------------------------------------------------------------------
    // Step 12: K-way merge tests
    // -------------------------------------------------------------------

    fn make_record(name: &str) -> Variables {
        let mut vars = Variables::new();
        vars.insert("name".to_string(), Value::String(name.to_string().into()));
        vars
    }

    #[test]
    fn test_kway_merge_sorted_chunks() {
        // Two chunks with interleaved sorted keys
        let chunk1 = vec![
            (b"a".to_vec(), make_record("a1")),
            (b"c".to_vec(), make_record("c1")),
            (b"e".to_vec(), make_record("e1")),
        ];
        let chunk2 = vec![
            (b"b".to_vec(), make_record("b2")),
            (b"d".to_vec(), make_record("d2")),
            (b"f".to_vec(), make_record("f2")),
        ];

        let result = kway_merge(vec![chunk1, chunk2], None);
        let names: Vec<&str> = result
            .iter()
            .map(|r| match r.get("name").unwrap() {
                Value::String(s) => s.as_str(),
                _ => panic!("expected string"),
            })
            .collect();
        assert_eq!(names, vec!["a1", "b2", "c1", "d2", "e1", "f2"]);
    }

    #[test]
    fn test_kway_merge_with_limit() {
        let chunk1 = vec![(b"a".to_vec(), make_record("a1")), (b"c".to_vec(), make_record("c1"))];
        let chunk2 = vec![(b"b".to_vec(), make_record("b2")), (b"d".to_vec(), make_record("d2"))];

        let result = kway_merge(vec![chunk1, chunk2], Some(2));
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result
            .iter()
            .map(|r| match r.get("name").unwrap() {
                Value::String(s) => s.as_str(),
                _ => panic!("expected string"),
            })
            .collect();
        assert_eq!(names, vec!["a1", "b2"]);
    }

    #[test]
    fn test_kway_merge_empty() {
        let result = kway_merge(vec![], None);
        assert!(result.is_empty());

        let result = kway_merge(vec![vec![], vec![]], None);
        assert!(result.is_empty());
    }
}
