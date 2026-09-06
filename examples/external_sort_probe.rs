//! Experimental fixed-schema external sort. This is not a production spill path.
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::{NamedTempFile, TempDir, TempPath};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const BUFFER_BYTES: usize = 8192;
const EXACT_ORACLE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, clap::Args, Serialize)]
struct Config {
    /// Maximum retained row/vector bytes in one sortable run (excludes input buffers).
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    run_bytes: usize,
    /// Maximum input runs open in one merge, from 2 through 64.
    #[arg(long, default_value_t = 8)]
    fan_in: usize,
    /// Maximum number of initial runs; bounds run metadata and temporary files.
    #[arg(long, default_value_t = 4096)]
    max_runs: usize,
    /// Maximum physical JSONL line bytes, including a newline if present.
    #[arg(long, default_value_t = 1024 * 1024)]
    max_record_bytes: usize,
    /// Maximum live temporary run bytes, including old/new runs during a merge.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    disk_bytes: u64,
    /// Existing directory under which a private temporary directory is created.
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    key: i32,
    payload: String,
    sequence: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputRecord {
    key: i32,
    payload: String,
}

#[derive(Default, Debug, PartialEq, Eq, Serialize)]
struct Fingerprint {
    rows: u64,
    sum: u64,
    squared_sum: u64,
    xor: u64,
}

impl Fingerprint {
    fn observe(&mut self, record: &Record) {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in record
            .key
            .to_le_bytes()
            .into_iter()
            .chain(record.sequence.to_le_bytes())
            .chain(record.payload.as_bytes().iter().copied())
        {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
        self.rows += 1;
        self.sum = self.sum.wrapping_add(hash);
        self.squared_sum = self.squared_sum.wrapping_add(hash.wrapping_mul(hash));
        self.xor ^= hash;
    }
}

#[derive(Default, Serialize)]
struct Report {
    input_bytes: u64,
    initial_runs: usize,
    runs_written: usize,
    merge_passes: usize,
    max_open_input_runs: usize,
    bytes_written: u64,
    live_disk_bytes: u64,
    peak_disk_bytes: u64,
    peak_retained_estimate: usize,
    run_generation_ns: u128,
    merge_ns: u128,
    validation_ns: u128,
    sort_ns: u128,
    total_ns: u128,
    input_fingerprint: Fingerprint,
    output_fingerprint: Fingerprint,
}

impl Report {
    fn reserve_disk(&mut self, bytes: u64, limit: u64) -> Result<()> {
        let live = self
            .live_disk_bytes
            .checked_add(bytes)
            .ok_or("temporary disk size overflow")?;
        if live > limit {
            return Err("temporary disk quota exceeded (old and new runs count together)".into());
        }
        self.live_disk_bytes = live;
        self.peak_disk_bytes = self.peak_disk_bytes.max(live);
        self.bytes_written = self
            .bytes_written
            .checked_add(bytes)
            .ok_or("written byte count overflow")?;
        Ok(())
    }

    fn retain(&mut self, bytes: usize) {
        self.peak_retained_estimate = self.peak_retained_estimate.max(bytes);
    }
}

struct Run {
    path: TempPath,
    bytes: u64,
}

struct Outcome {
    final_run: Option<Run>,
    max_record_bytes: usize,
    report: Report,
    // Keep the private directory alive until all output/oracle readers finish.
    _scratch: TempDir,
}

fn check_config(config: &Config) -> Result<()> {
    if !(2..=64).contains(&config.fan_in) {
        return Err("fan-in must be between 2 and 64".into());
    }
    if config.max_runs == 0 || config.max_runs > 1_000_000 {
        return Err("max-runs must be between 1 and 1000000".into());
    }
    if config.max_record_bytes == 0 || config.max_record_bytes > 64 * 1024 * 1024 {
        return Err("max-record-bytes must be between 1 and 67108864".into());
    }
    if config.run_bytes < size_of::<Record>() || config.disk_bytes == 0 {
        return Err("run/disk budgets must hold at least one record header".into());
    }
    Ok(())
}

fn read_line(reader: &mut impl BufRead, line: &mut Vec<u8>, max: usize) -> Result<bool> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |position| position + 1);
        if count > max.saturating_sub(line.len()) {
            return Err("record exceeds max-record-bytes".into());
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

fn encoded_bytes(record: &Record) -> u64 {
    16 + record.payload.len() as u64
}

fn write_record(writer: &mut impl Write, record: &Record) -> Result<()> {
    let length = u32::try_from(record.payload.len())?;
    writer.write_all(&record.key.to_le_bytes())?;
    writer.write_all(&record.sequence.to_le_bytes())?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(record.payload.as_bytes())?;
    Ok(())
}

fn read_record(reader: &mut impl Read, max_record_bytes: usize) -> Result<Option<Record>> {
    let mut header = [0_u8; 16];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let length = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    if length > max_record_bytes {
        return Err("run payload exceeds max-record-bytes".into());
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload)?;
    Ok(Some(Record {
        key: i32::from_le_bytes(header[..4].try_into().unwrap()),
        sequence: u64::from_le_bytes(header[4..12].try_into().unwrap()),
        payload: String::from_utf8(payload)?,
    }))
}

fn spill(mut rows: Vec<Record>, scratch: &Path, config: &Config, report: &mut Report) -> Result<Run> {
    rows.sort_unstable_by_key(|record| (record.key, record.sequence));
    let bytes = rows.iter().map(encoded_bytes).sum();
    report.reserve_disk(bytes, config.disk_bytes)?;
    let mut file = NamedTempFile::new_in(scratch)?;
    {
        let mut writer = BufWriter::with_capacity(BUFFER_BYTES, file.as_file_mut());
        for record in &rows {
            write_record(&mut writer, record)?;
        }
        writer.flush()?;
    }
    report.runs_written += 1;
    Ok(Run {
        path: file.into_temp_path(),
        bytes,
    })
}

#[derive(Eq, PartialEq)]
struct MergeItem {
    record: Record,
    source: usize,
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.record.key, self.record.sequence, self.source)
            .cmp(&(other.record.key, other.record.sequence, other.source))
            .then_with(|| self.record.payload.cmp(&other.record.payload))
    }
}
impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge(runs: &[Run], scratch: &Path, config: &Config, report: &mut Report, metadata_bytes: usize) -> Result<Run> {
    debug_assert!(runs.len() <= config.fan_in);
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::with_capacity(runs.len());
    let mut payload_bytes = 0usize;
    for (source, run) in runs.iter().enumerate() {
        let mut reader = BufReader::with_capacity(BUFFER_BYTES, File::open(&run.path)?);
        if let Some(record) = read_record(&mut reader, config.max_record_bytes)? {
            payload_bytes += record.payload.capacity();
            heap.push(Reverse(MergeItem { record, source }));
        }
        readers.push(reader);
    }
    report.max_open_input_runs = report.max_open_input_runs.max(readers.len());
    let fixed = metadata_bytes
        + readers.len() * BUFFER_BYTES
        + BUFFER_BYTES
        + readers.capacity() * size_of::<BufReader<File>>()
        + heap.capacity() * size_of::<MergeItem>();
    report.retain(fixed + payload_bytes);
    let mut file = NamedTempFile::new_in(scratch)?;
    let mut bytes = 0u64;
    {
        let mut writer = BufWriter::with_capacity(BUFFER_BYTES, file.as_file_mut());
        while let Some(Reverse(item)) = heap.pop() {
            let size = encoded_bytes(&item.record);
            report.reserve_disk(size, config.disk_bytes)?;
            write_record(&mut writer, &item.record)?;
            bytes += size;
            let source = item.source;
            payload_bytes -= item.record.payload.capacity();
            // Drop the consumed payload before allocating its replacement.
            drop(item);
            if let Some(record) = read_record(&mut readers[source], config.max_record_bytes)? {
                payload_bytes += record.payload.capacity();
                heap.push(Reverse(MergeItem { record, source }));
            }
            report.retain(fixed + payload_bytes);
        }
        writer.flush()?;
    }
    report.runs_written += 1;
    Ok(Run {
        path: file.into_temp_path(),
        bytes,
    })
}

fn run_metadata_bytes(runs: &[Run], capacity: usize) -> usize {
    capacity * size_of::<Run>() + runs.iter().map(|run| run.path.as_os_str().len()).sum::<usize>()
}

fn external_sort(input: &Path, config: &Config) -> Result<Outcome> {
    check_config(config)?;
    if !input.metadata()?.is_file() {
        return Err("input must be an immutable regular JSONL file".into());
    }
    let scratch = if let Some(parent) = &config.scratch_dir {
        tempfile::tempdir_in(parent)?
    } else {
        tempfile::tempdir()?
    };
    let mut reader = BufReader::with_capacity(BUFFER_BYTES, File::open(input)?);
    let mut report = Report::default();
    report.retain(BUFFER_BYTES);
    let start = Instant::now();
    let mut runs = Vec::new();
    let mut run_path_bytes = 0usize;
    let mut rows: Vec<Record> = Vec::new();
    let mut payload_bytes = 0usize;
    let mut line = Vec::new();
    while read_line(&mut reader, &mut line, config.max_record_bytes)? {
        report.input_bytes += line.len() as u64;
        let parsed: InputRecord = serde_json::from_slice(&line)?;
        let record = Record {
            key: parsed.key,
            payload: parsed.payload,
            sequence: report.input_fingerprint.rows,
        };
        let payload = record.payload.capacity();
        let proposed_capacity = if rows.len() == rows.capacity() {
            rows.capacity().saturating_mul(2).max(1)
        } else {
            rows.capacity()
        };
        let proposed = proposed_capacity
            .saturating_mul(size_of::<Record>())
            .saturating_add(payload_bytes)
            .saturating_add(payload);
        report.retain(
            rows.capacity() * size_of::<Record>()
                + payload_bytes
                + payload
                + line.capacity()
                + 2 * BUFFER_BYTES
                + runs.capacity() * size_of::<Run>()
                + run_path_bytes,
        );
        if proposed > config.run_bytes && !rows.is_empty() {
            if runs.len() >= config.max_runs {
                return Err("initial run quota exceeded".into());
            }
            let run = spill(std::mem::take(&mut rows), scratch.path(), config, &mut report)?;
            run_path_bytes += run.path.as_os_str().len();
            runs.push(run);
            payload_bytes = 0;
        }
        let capacity = if rows.len() == rows.capacity() {
            rows.capacity().saturating_mul(2).max(1)
        } else {
            rows.capacity()
        };
        if capacity
            .saturating_mul(size_of::<Record>())
            .saturating_add(payload_bytes)
            .saturating_add(payload)
            > config.run_bytes
        {
            return Err("single record exceeds run-bytes".into());
        }
        if rows.capacity() < capacity {
            rows.try_reserve_exact(capacity - rows.len())?;
        }
        if rows
            .capacity()
            .saturating_mul(size_of::<Record>())
            .saturating_add(payload_bytes)
            .saturating_add(payload)
            > config.run_bytes
        {
            return Err("allocated run capacity exceeds run-bytes".into());
        }
        report.input_fingerprint.observe(&record);
        payload_bytes += payload;
        rows.push(record);
        report.retain(
            rows.capacity() * size_of::<Record>()
                + payload_bytes
                + line.capacity()
                + 2 * BUFFER_BYTES
                + runs.capacity() * size_of::<Run>()
                + run_path_bytes,
        );
    }
    if !rows.is_empty() {
        if runs.len() >= config.max_runs {
            return Err("initial run quota exceeded".into());
        }
        runs.push(spill(rows, scratch.path(), config, &mut report)?);
    }
    drop(line);
    drop(reader);
    report.initial_runs = runs.len();
    report.run_generation_ns = start.elapsed().as_nanos();
    let merge_start = Instant::now();
    while runs.len() > 1 {
        let metadata = run_metadata_bytes(&runs, runs.capacity());
        let mut next = Vec::with_capacity(runs.len().div_ceil(config.fan_in));
        let mut pending = runs.into_iter();
        loop {
            let group: Vec<_> = pending.by_ref().take(config.fan_in).collect();
            if group.is_empty() {
                break;
            }
            if group.len() == 1 {
                next.extend(group);
                continue;
            }
            let merged = merge(
                &group,
                scratch.path(),
                config,
                &mut report,
                metadata + (next.capacity() + group.capacity()) * size_of::<Run>(),
            )?;
            let removed: u64 = group.iter().map(|run| run.bytes).sum();
            drop(group);
            report.live_disk_bytes -= removed;
            next.push(merged);
        }
        runs = next;
        report.merge_passes += 1;
    }
    report.merge_ns = merge_start.elapsed().as_nanos();
    report.sort_ns = start.elapsed().as_nanos();
    let mut outcome = Outcome {
        final_run: runs.pop(),
        max_record_bytes: config.max_record_bytes,
        report,
        _scratch: scratch,
    };
    let validation_start = Instant::now();
    let mut actual = Fingerprint::default();
    let mut previous = None;
    read_sorted(&outcome, |record| {
        let key = (record.key, record.sequence);
        if previous.is_some_and(|previous| previous > key) {
            return Err("merged output is not sorted stably".into());
        }
        previous = Some(key);
        actual.observe(&record);
        Ok(())
    })?;
    if actual != outcome.report.input_fingerprint {
        return Err("output bag fingerprint mismatch".into());
    }
    outcome.report.output_fingerprint = actual;
    outcome.report.validation_ns = validation_start.elapsed().as_nanos();
    outcome.report.total_ns = start.elapsed().as_nanos();
    Ok(outcome)
}

fn read_sorted(outcome: &Outcome, mut visit: impl FnMut(Record) -> Result<()>) -> Result<()> {
    if let Some(run) = &outcome.final_run {
        let mut reader = BufReader::with_capacity(BUFFER_BYTES, File::open(&run.path)?);
        while let Some(record) = read_record(&mut reader, outcome.max_record_bytes)? {
            visit(record)?;
        }
    }
    Ok(())
}

fn write_output(outcome: &Outcome, output: &Path) -> Result<u64> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut file = NamedTempFile::new_in(parent)?;
    {
        let mut writer = BufWriter::with_capacity(BUFFER_BYTES, file.as_file_mut());
        read_sorted(outcome, |record| {
            serde_json::to_writer(&mut writer, &record)?;
            writeln!(writer)?;
            Ok(())
        })?;
        writer.flush()?;
    }
    let bytes = file.as_file().metadata()?.len();
    file.persist_noclobber(output)?;
    Ok(bytes)
}

fn exact_oracle(input: &Path, config: &Config, outcome: &Outcome, logq: Option<&Path>) -> Result<serde_json::Value> {
    let mut reader = BufReader::new(File::open(input)?);
    let mut line = Vec::new();
    let mut expected = Vec::new();
    let mut bytes = 0u64;
    let mut oracle_input = NamedTempFile::new_in(outcome._scratch.path())?;
    while read_line(&mut reader, &mut line, config.max_record_bytes)? {
        bytes += line.len() as u64;
        if bytes > EXACT_ORACLE_BYTES {
            return Err("exact in-memory validation is limited to 16 MiB; use --output with an external streaming oracle for large inputs".into());
        }
        let parsed: InputRecord = serde_json::from_slice(&line)?;
        let record = Record {
            key: parsed.key,
            payload: parsed.payload,
            sequence: expected.len() as u64,
        };
        serde_json::to_writer(oracle_input.as_file_mut(), &record)?;
        writeln!(oracle_input.as_file_mut())?;
        expected.push(record);
    }
    expected.sort_by_key(|record| (record.key, record.sequence));
    let mut position = 0;
    read_sorted(outcome, |record| {
        if expected.get(position) != Some(&record) {
            return Err(format!("in-memory oracle mismatch at row {position}").into());
        }
        position += 1;
        Ok(())
    })?;
    if position != expected.len() {
        return Err("in-memory oracle row-count mismatch".into());
    }
    if let Some(binary) = logq {
        let stdout = NamedTempFile::new_in(outcome._scratch.path())?;
        let stderr = NamedTempFile::new_in(outcome._scratch.path())?;
        let mut child = Command::new(binary)
            .args([
                "query",
                "select key, payload, sequence from it order by key asc, sequence asc",
                "--table",
                "it:jsonl=stdin",
                "--threads",
                "1",
                "--output",
                "ndjson",
            ])
            .stdin(File::open(oracle_input.path())?)
            .stdout(Stdio::from(stdout.as_file().try_clone()?))
            .stderr(Stdio::from(stderr.as_file().try_clone()?))
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("logq oracle timed out after 60 seconds".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        if !status.success() {
            return Err(format!("logq oracle failed with {status}").into());
        }
        let mut reader = BufReader::new(File::open(stdout.path())?);
        let mut position = 0;
        while read_line(&mut reader, &mut line, config.max_record_bytes + 128)? {
            let record: Record = serde_json::from_slice(&line)?;
            if expected.get(position) != Some(&record) {
                return Err(format!("logq oracle mismatch at row {position}").into());
            }
            position += 1;
        }
        if position != expected.len() {
            return Err("logq oracle row-count mismatch".into());
        }
    }
    Ok(
        serde_json::json!({"in_memory":"exact match", "logq":if logq.is_some(){"exact match"}else{"not requested"}, "rows":expected.len()}),
    )
}

#[derive(Parser)]
struct Args {
    input: PathBuf,
    #[command(flatten)]
    config: Config,
    /// Write complete sorted NDJSON with original sequence numbers, without replacing an existing file. Outside sort timing/quota.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Check exact in-memory output on a fixture up to 16 MiB. Excluded from timing; do not use this process for RSS measurements.
    #[arg(long)]
    validate: bool,
    /// Also compare with a logq executable using the sequential stdin row path.
    #[arg(long, requires = "validate")]
    logq: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.output.as_ref().is_some_and(|path| path.exists()) {
        return Err("output already exists; choose a new path".into());
    }
    if args.validate && args.input.metadata()?.len() > EXACT_ORACLE_BYTES {
        return Err("exact in-memory validation is limited to 16 MiB; use --output with an external streaming oracle for large inputs".into());
    }
    let outcome = external_sort(&args.input, &args.config)?;
    let oracle = if args.validate {
        Some(exact_oracle(&args.input, &args.config, &outcome, args.logq.as_deref())?)
    } else {
        None
    };
    let output_start = Instant::now();
    let output_bytes = args
        .output
        .as_ref()
        .map(|path| write_output(&outcome, path))
        .transpose()?;
    let output_ns = args.output.as_ref().map(|_| output_start.elapsed().as_nanos());
    println!(
        "{}",
        serde_json::json!({
            "version":1, "status":"complete", "config":args.config, "report":outcome.report, "exact_oracle":oracle,
            "output":args.output, "output_bytes":output_bytes, "output_ns":output_ns,
            "timing_scope":"sort_ns includes parsing, run generation, sorting, temporary writes and bounded merges; total_ns additionally includes streaming monotonic and bag checks; setup, optional exact oracles and final NDJSON formatting/write are excluded",
            "memory_scope":"peak_retained_estimate counts run vector/payload storage, line/read/write buffers, merge heap and run metadata; conservative application estimate, not allocator statistics or RSS; fan-in and max-record-bytes separately bound merge memory",
            "validation_scope":"normal mode verifies stable key/sequence ordering and count plus sum/squared-sum/xor of per-record FNV64 hashes; probabilistic fingerprints are not a full equality proof; use --output for an independent large-input oracle",
            "contract":"input is exactly {key:Int32,payload:String}; sequence is original zero-based row index; no NULL/MISSING, mixed types, expressions, SQL integration or production spill; temporary quota excludes optional final output and exact-oracle files",
            "rss_measurement_eligible":!args.validate,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Cursor;

    fn config(scratch: &Path) -> Config {
        Config {
            run_bytes: 256,
            fan_in: 2,
            max_runs: 128,
            max_record_bytes: 1024,
            disk_bytes: 1_000_000,
            scratch_dir: Some(scratch.to_path_buf()),
        }
    }

    fn fixture(input: &Path) -> Vec<Record> {
        let expected: Vec<_> = (0..97)
            .map(|sequence| Record {
                key: (sequence % 9) as i32 - 4,
                payload: format!("雪\"line\\{sequence}"),
                sequence,
            })
            .collect();
        let text: String = expected
            .iter()
            .map(|r| serde_json::json!({"key":r.key,"payload":r.payload}).to_string() + "\n")
            .collect();
        fs::write(input, text).unwrap();
        expected
    }

    #[test]
    fn bounded_runs_merge_stably_and_cleanup_after_success() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.jsonl");
        let scratch = dir.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let mut expected = fixture(&input);
        expected.sort_by_key(|r| (r.key, r.sequence));
        let outcome = external_sort(&input, &config(&scratch)).unwrap();
        let mut actual = Vec::new();
        read_sorted(&outcome, |record| {
            actual.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(actual, expected);
        assert!(outcome.report.initial_runs > 2);
        assert!(outcome.report.merge_passes > 1);
        assert!(outcome.report.max_open_input_runs <= 2);
        assert!(outcome.report.peak_disk_bytes <= config(&scratch).disk_bytes);
        assert_eq!(outcome.report.input_fingerprint, outcome.report.output_fingerprint);
        exact_oracle(&input, &config(&scratch), &outcome, None).unwrap();
        drop(outcome);
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }

    #[test]
    fn invalid_input_and_resource_limits_cleanup_private_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.jsonl");
        let scratch = dir.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        for invalid in [
            "[]\n",
            "{\"key\":1,\"payload\":null}\n",
            "{\"key\":2147483648,\"payload\":\"x\"}\n",
            "{\"key\":1,\"key\":2,\"payload\":\"x\"}\n",
            "{\"key\":1,\"payload\":\"x\",\"extra\":1}\n",
            "{\"key\":1,\"payload\":\"truncated",
        ] {
            fs::write(&input, invalid).unwrap();
            assert!(external_sort(&input, &config(&scratch)).is_err(), "{invalid}");
            assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        }
        let expected = fixture(&input);
        for invalid in [
            Config {
                fan_in: 1,
                ..config(&scratch)
            },
            Config {
                fan_in: 65,
                ..config(&scratch)
            },
            Config {
                max_runs: 1,
                ..config(&scratch)
            },
            Config {
                disk_bytes: 80,
                ..config(&scratch)
            },
            Config {
                max_record_bytes: 8,
                ..config(&scratch)
            },
        ] {
            assert!(external_sort(&input, &invalid).is_err(), "{invalid:?}");
            assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        }
        let initial_disk: u64 = expected.iter().map(encoded_bytes).sum();
        let error = external_sort(
            &input,
            &Config {
                disk_bytes: initial_disk,
                ..config(&scratch)
            },
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("disk quota"), "{error}");
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        // A parse failure after several completed runs also cleans every run.
        let mut late_error = fs::read_to_string(&input).unwrap();
        late_error.push_str("{bad}\n");
        fs::write(&input, late_error).unwrap();
        assert!(external_sort(&input, &config(&scratch)).is_err());
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        fs::write(
            &input,
            serde_json::json!({"key":0,"payload":"x".repeat(512)}).to_string(),
        )
        .unwrap();
        assert!(external_sort(&input, &config(&scratch)).is_err());
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }

    #[test]
    fn empty_and_unterminated_final_input_lines_are_supported() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.jsonl");
        for text in ["", "{\"key\":-1,\"payload\":\"last\"}"] {
            fs::write(&input, text).unwrap();
            let outcome = external_sort(&input, &config(dir.path())).unwrap();
            let mut actual = Vec::new();
            read_sorted(&outcome, |record| {
                actual.push(record);
                Ok(())
            })
            .unwrap();
            assert_eq!(actual.len(), usize::from(!text.is_empty()));
        }
    }

    #[test]
    fn internal_run_reader_rejects_truncation_and_overlarge_payloads() {
        assert!(read_record(&mut Cursor::new(vec![0; 7]), 1024).is_err());
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_i32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&2048_u32.to_le_bytes());
        assert!(read_record(&mut Cursor::new(&bytes), 1024).is_err());
        bytes[12..16].copy_from_slice(&2_u32.to_le_bytes());
        bytes.push(b'a');
        assert!(read_record(&mut Cursor::new(&bytes), 1024).is_err());
        let record = Record {
            key: i32::MIN,
            payload: "雪\n\"".into(),
            sequence: u64::MAX,
        };
        let mut bytes = Vec::new();
        write_record(&mut bytes, &record).unwrap();
        let mut reader = Cursor::new(bytes);
        assert_eq!(read_record(&mut reader, 1024).unwrap(), Some(record));
        assert_eq!(read_record(&mut reader, 1024).unwrap(), None);
    }

    #[test]
    fn output_is_complete_and_never_overwrites_existing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.jsonl");
        let mut expected = fixture(&input);
        expected.sort_by_key(|record| (record.key, record.sequence));
        let outcome = external_sort(&input, &config(dir.path())).unwrap();
        let output = dir.path().join("sorted.jsonl");
        let bytes = write_output(&outcome, &output).unwrap();
        assert_eq!(bytes, output.metadata().unwrap().len());
        let text = fs::read_to_string(&output).unwrap();
        let actual: Vec<Record> = text.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(actual, expected);
        assert!(write_output(&outcome, &output).is_err());
        assert_eq!(fs::read_to_string(&output).unwrap(), text);
    }
}
