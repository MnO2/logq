# Plan: Large File Throughput Optimization

**Goal**: Maximize logq throughput on multi-GB log files via I/O improvements, allocation reduction, and parallel chunk scanning.
**Architecture**: Two-phase approach — (1) sequential I/O/allocation wins, (2) mmap + rayon parallel scanning with per-query-type merge strategies.
**Tech Stack**: Rust, memmap2, rayon

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Steps 1-3 | Yes (independent) | Part 1: Sequential I/O + allocation wins |
| 2 | Steps 4-5 | Yes (independent) | Prerequisites for Part 2 |
| 3 | Step 6 | No (depends on Group 1, 2) | Add dependencies |
| 4 | Steps 7-8 | No (sequential) | Core parallel: mmap + chunk splitting |
| 5 | Step 9 | No (depends on Group 4) | Parallel scan dispatch |
| 6 | Step 10 | No (depends on Group 5) | CLI flag + decision layer |
| 7 | Steps 11-12 | Yes (independent) | Parallel aggregation + sort-merge |
| 8 | Step 13 | No (depends on all) | End-to-end verification |

---

## Step 1: Increase BufReader buffer from 8KB to 64KB

**File**: `src/execution/datasource.rs`

### 1a. Write failing test

No test needed — this is a constant change. Existing tests validate correctness.

### 1b. Write implementation

```rust
// In ReaderBuilder::new(), line 765
// Change:
capacity: 8 * (1 << 10),
// To:
capacity: 64 * (1 << 10),
```

### 1c. Run tests to verify nothing breaks

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 1d. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "perf: increase BufReader buffer from 8KB to 64KB for reduced syscall overhead"
```

---

## Step 2: Reusable tokenizer scratch buffer

**Files**: `src/execution/batch_tokenizer.rs`, `src/execution/batch_scan.rs`

### 2a. Write failing test

```rust
// In src/execution/batch_tokenizer.rs, add test:
#[test]
fn test_tokenize_line_into_reusable_buffer() {
    let line = b"2024-01-01 hello world";
    let mut scratch = Vec::new();
    tokenize_line_into(line, &mut scratch);
    assert_eq!(scratch.len(), 3);
    assert_eq!(&line[scratch[0].0..scratch[0].1], b"2024-01-01");

    // Reuse: clear and tokenize again
    let line2 = b"foo bar";
    tokenize_line_into(line2, &mut scratch);
    assert_eq!(scratch.len(), 2);
    assert_eq!(&line2[scratch[0].0..scratch[0].1], b"foo");
}
```

### 2b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_tokenize_line_into_reusable_buffer
```

### 2c. Write implementation

In `src/execution/batch_tokenizer.rs`, add `tokenize_line_into`:

```rust
/// Tokenize a single line into a reusable buffer. Clears the buffer first.
pub(crate) fn tokenize_line_into(line: &[u8], fields: &mut Vec<(usize, usize)>) {
    fields.clear();
    let mut pos = 0;
    let len = line.len();

    while pos < len {
        while pos < len && line[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        match line[pos] {
            b'"' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'"' { pos += 1; }
                if pos < len { pos += 1; }
                fields.push((start, pos));
            }
            b'\'' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'\'' { pos += 1; }
                if pos < len { pos += 1; }
                fields.push((start, pos));
            }
            b'[' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b']' { pos += 1; }
                if pos < len { pos += 1; }
                fields.push((start, pos));
            }
            _ => {
                let start = pos;
                while pos < len {
                    match line[pos] {
                        b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b'[' | b']' => break,
                        _ => pos += 1,
                    }
                }
                fields.push((start, pos));
            }
        }
    }
}
```

In `src/execution/batch_scan.rs`, add `offsets_scratch` field and use it:

```rust
// Add field to BatchScanOperator struct:
offsets_scratch: Vec<(usize, usize)>,

// Initialize in new():
offsets_scratch: Vec::with_capacity(30), // max fields across all formats

// In next_batch(), replace:
//   let line_fields: Vec<Vec<(usize, usize)>> = lines.iter()
//       .map(|line| tokenize_line(line))
//       .collect();
// With:
let mut line_fields: Vec<Vec<(usize, usize)>> = Vec::with_capacity(lines.len());
for line in &lines {
    tokenize_line_into(line, &mut self.offsets_scratch);
    line_fields.push(self.offsets_scratch.clone());
}
```

Note: We still produce `Vec<Vec<(usize, usize)>>` because `parse_field_column` indexes `fields[row]` per row. The optimization is that `offsets_scratch` reuses its allocation across lines within a batch (the inner `.clone()` still allocates, but the scratch avoids the tokenizer's own allocation). The full zero-allocation path comes in Step 4 (genericize) + Step 8 (mmap).

### 2d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 2e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "perf: add tokenize_line_into for reusable scratch buffer in batch tokenizer"
```

---

## Step 3: Arena allocation for batch line storage

**File**: `src/execution/batch_scan.rs`

### 3a. Write failing test

```rust
// In src/execution/batch_scan.rs tests:
#[test]
fn test_batch_line_arena() {
    let mut arena = BatchLineArena::new();
    arena.push_line(b"hello world");
    arena.push_line(b"foo bar");
    assert_eq!(arena.len(), 2);
    assert_eq!(arena.get_line(0), b"hello world");
    assert_eq!(arena.get_line(1), b"foo bar");

    arena.clear();
    assert_eq!(arena.len(), 0);
    arena.push_line(b"reused");
    assert_eq!(arena.get_line(0), b"reused");
}
```

### 3b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_batch_line_arena
```

### 3c. Write implementation

In `src/execution/batch_scan.rs`, add `BatchLineArena` struct:

```rust
pub(crate) struct BatchLineArena {
    data: Vec<u8>,
    spans: Vec<(usize, usize)>,
}

impl BatchLineArena {
    fn new() -> Self {
        BatchLineArena {
            data: Vec::with_capacity(512 * 1024), // ~512KB initial
            spans: Vec::with_capacity(BATCH_SIZE),
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spans.clear();
    }

    fn push_line(&mut self, line: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(line);
        self.spans.push((start, line.len()));
    }

    fn get_line(&self, idx: usize) -> &[u8] {
        let (start, len) = self.spans[idx];
        &self.data[start..start + len]
    }

    fn len(&self) -> usize {
        self.spans.len()
    }

    /// Convert arena lines to Vec<Vec<u8>> for compatibility with
    /// parse_field_column which takes &[Vec<u8>].
    /// This will be eliminated once parse_field_column is genericized (Step 4).
    fn to_vecs(&self) -> Vec<Vec<u8>> {
        (0..self.spans.len())
            .map(|i| self.get_line(i).to_vec())
            .collect()
    }
}
```

Replace `read_lines` in `BatchScanOperator`:

```rust
// Add arena field to struct:
arena: BatchLineArena,

// Initialize in new():
arena: BatchLineArena::new(),

// Replace read_lines method:
fn read_lines(&mut self) -> Vec<Vec<u8>> {
    self.arena.clear();
    while self.arena.len() < BATCH_SIZE {
        self.buf.clear();
        match self.reader.read_line(&mut self.buf) {
            Ok(0) => { self.done = true; break; }
            Ok(_) => {
                let trimmed = self.buf.trim_end().as_bytes();
                if !trimmed.is_empty() {
                    self.arena.push_line(trimmed);
                }
            }
            Err(_) => { self.done = true; break; }
        }
    }
    // Temporary: convert to Vec<Vec<u8>> until parse_field_column is genericized
    self.arena.to_vecs()
}
```

### 3d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 3e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "perf: arena allocation for batch line storage, eliminates 1024 scattered allocs/batch"
```

---

## Step 4: Genericize parse_field_column over slice types

**Files**: `src/execution/field_parser.rs`, `src/execution/batch_predicate.rs`

### 4a. Write failing test

```rust
// In src/execution/field_parser.rs tests, add:
#[test]
fn test_parse_string_field_with_slices() {
    // Test with &[u8] slices instead of Vec<u8>
    let line0: &[u8] = b"hello world foo";
    let line1: &[u8] = b"bar baz qux";
    let lines: Vec<&[u8]> = vec![line0, line1];
    let fields = vec![
        vec![(0, 5), (6, 11), (12, 15)],
        vec![(0, 3), (4, 7), (8, 11)],
    ];
    let col = parse_field_column(&lines, &fields, 0, &DataType::String);
    match col {
        TypedColumn::Utf8 { offsets, .. } => {
            assert_eq!(offsets[0], 0);
            assert_eq!(offsets[1], 5);  // "hello"
            assert_eq!(offsets[2], 8);  // "hello" + "bar"
        }
        _ => panic!("expected Utf8"),
    }
}
```

### 4b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_parse_string_field_with_slices
```

### 4c. Write implementation

In `src/execution/field_parser.rs`, change both functions' signatures:

```rust
// Before:
pub(crate) fn parse_field_column(
    lines: &[Vec<u8>],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
) -> TypedColumn {

// After:
pub(crate) fn parse_field_column<L: AsRef<[u8]>>(
    lines: &[L],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
) -> TypedColumn {
```

Inside each match arm, replace `&lines[row][start..end]` with `&lines[row].as_ref()[start..end]`.

Apply same change to `parse_field_column_selected`:

```rust
pub(crate) fn parse_field_column_selected<L: AsRef<[u8]>>(
    lines: &[L],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
    selection: &SelectionVector,
) -> TypedColumn {
```

In `src/execution/batch_predicate.rs`, no changes needed — it doesn't directly access `lines` (it works on `ColumnBatch`).

In `src/execution/batch_scan.rs`, call sites pass `&lines` which is `&[Vec<u8>]` — the generic infers `L = Vec<u8>` automatically, no changes needed.

### 4d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 4e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "refactor: genericize parse_field_column over AsRef<[u8]> for zero-copy mmap support"
```

---

## Step 5: Refactor AvgAggregate to store sum instead of running average

**File**: `src/execution/types.rs`

### 5a. Write failing test

```rust
// Add to existing tests in types.rs or a new test file:
#[test]
fn test_avg_aggregate_precision_with_sum() {
    let mut agg = AvgAggregate::new();
    let key = None;
    // Add many small values — running average formula loses precision with f32
    for i in 0..10000 {
        agg.add_record(&key, &Value::Float(OrderedFloat::from(1.0001f32))).unwrap();
    }
    let result = agg.get_aggregated(&key).unwrap();
    match result {
        Value::Float(f) => {
            // With f64 sum accumulation, result should be very close to 1.0001
            assert!((f.into_inner() - 1.0001).abs() < 0.001);
        }
        _ => panic!("expected Float"),
    }
}
```

### 5b. Run test (may pass or fail depending on current precision)

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_avg_aggregate_precision_with_sum
```

### 5c. Write implementation

In `src/execution/types.rs`, replace `AvgAggregate`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AvgAggregate {
    pub(crate) sums: HashMap<Option<Tuple>, f64>,
    pub(crate) counts: HashMap<Option<Tuple>, i64>,
}

impl AvgAggregate {
    pub(crate) fn new() -> Self {
        AvgAggregate {
            sums: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let new_value: f64 = match value {
            &Value::Int(i) => i as f64,
            &Value::Float(f) => f.into_inner() as f64,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        *self.sums.entry(key.clone()).or_insert(0.0) += new_value;
        *self.counts.entry(key.clone()).or_insert(0) += 1;
        Ok(())
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let (Some(&sum), Some(&count)) = (self.sums.get(key), self.counts.get(key)) {
            Ok(Value::Float(OrderedFloat::from((sum / count as f64) as f32)))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}
```

Note: Remove `Eq` from the derive since `f64` doesn't implement `Eq`. Check if `Eq` is required by any callers — if so, implement it manually comparing via `OrderedFloat` or remove the `Eq` requirement.

### 5d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 5e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "refactor: AvgAggregate stores f64 sum instead of f32 running average, improves precision and enables parallel merge"
```

---

## Step 6: Add memmap2 and rayon dependencies

**File**: `Cargo.toml`

### 6a. Write implementation

Add to `[dependencies]`:

```toml
memmap2 = "0.9"
rayon = "1.10"
```

### 6b. Verify it compiles

```bash
cd /Users/paulmeng/Develop/logq && cargo check
```

### 6c. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "build: add memmap2 and rayon dependencies for parallel file scanning"
```

---

## Step 7: Implement chunk splitting with edge case tests

**File**: `src/execution/parallel.rs` (new file)

### 7a. Write failing tests

```rust
// src/execution/parallel.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_chunks_basic() {
        let data = b"line1\nline2\nline3\nline4\n";
        let chunks = split_chunks(data, 2);
        assert_eq!(chunks.len(), 2);
        // All data accounted for
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
        // Each chunk ends at a newline
        for chunk in &chunks {
            assert!(chunk.is_empty() || chunk.last() == Some(&b'\n'));
        }
    }

    #[test]
    fn test_split_chunks_empty() {
        let chunks = split_chunks(b"", 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_zero_chunks() {
        let chunks = split_chunks(b"hello\n", 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_more_chunks_than_lines() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 10);
        // Should produce <= 2 chunks (one per line)
        assert!(chunks.len() <= 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_chunk() {
        let data = b"line1\nline2\nline3\n";
        let chunks = split_chunks(data, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
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
        let data = b"just one line\n";
        let chunks = split_chunks(data, 4);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
    }
}
```

### 7b. Run tests to verify they fail

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_split_chunks
```

### 7c. Write implementation

Create `src/execution/parallel.rs`:

```rust
// src/execution/parallel.rs

/// Split a byte slice into chunks along newline boundaries.
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

        let raw_end = std::cmp::min(start + chunk_size, data.len());

        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1,
            None => data.len(),
        };

        chunks.push(&data[start..end]);
        start = end;
    }

    while chunks.last().map_or(false, |c| c.is_empty()) {
        chunks.pop();
    }

    chunks
}
```

Register module in `src/execution/mod.rs`:

```rust
pub(crate) mod parallel;
```

### 7d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_split_chunks
```

### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: add chunk splitting for parallel file scanning with edge case handling"
```

---

## Step 8: Implement mmap with BufReader fallback

**File**: `src/execution/parallel.rs`

### 8a. Write failing test

```rust
#[test]
fn test_scan_strategy_small_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("small.log");
    std::fs::write(&path, "line1\nline2\n").unwrap();
    let strategy = choose_strategy(&path);
    assert!(matches!(strategy, ScanStrategy::BufReader(_)));
}

#[test]
fn test_scan_strategy_nonexistent_file() {
    let strategy = choose_strategy(std::path::Path::new("/nonexistent/file.log"));
    assert!(matches!(strategy, ScanStrategy::BufReader(_)));
}
```

### 8b. Run tests to verify they fail

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_scan_strategy
```

### 8c. Write implementation

Add to `src/execution/parallel.rs`:

```rust
use memmap2::MmapOptions;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

const PARALLEL_THRESHOLD: u64 = 16 * 1024 * 1024; // 16MB

pub(crate) enum ScanStrategy {
    Mmap(memmap2::Mmap),
    BufReader(Box<dyn BufRead>),
}

pub(crate) fn choose_strategy(path: &Path) -> ScanStrategy {
    let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);

    if file_size < PARALLEL_THRESHOLD || file_size == 0 {
        return match File::open(path) {
            Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(
                64 * 1024, f,
            ))),
            Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
        };
    }

    // Only use mmap on 64-bit platforms
    #[cfg(target_pointer_width = "64")]
    {
        match File::open(path).and_then(|f| unsafe { MmapOptions::new().map(&f) }) {
            Ok(mmap) => ScanStrategy::Mmap(mmap),
            Err(_) => match File::open(path) {
                Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(
                    64 * 1024, f,
                ))),
                Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
            },
        }
    }

    #[cfg(not(target_pointer_width = "64"))]
    {
        match File::open(path) {
            Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(
                64 * 1024, f,
            ))),
            Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
        }
    }
}
```

### 8d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 8e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: mmap scan strategy with BufReader fallback for small files and 32-bit"
```

---

## Step 9: Parallel scan dispatch for filter/projection queries

**Files**: `src/execution/parallel.rs`, `src/execution/batch_scan.rs`

### 9a. Write failing test

```rust
// In src/execution/parallel.rs tests:
#[test]
fn test_parallel_scan_filter_concat() {
    use crate::execution::batch::*;
    use crate::execution::log_schema::LogSchema;

    // Build a multi-line dataset
    let mut data = String::new();
    for i in 0..100 {
        // Squid format: timestamp elapsed client_address action_code bytes method url rfc931 peer_status content_type
        data.push_str(&format!(
            "ts{} {} host{} status{} {} GET url{} rfc{} peer{} type{}\n",
            i, i, i, i, i * 100, i, i, i, i
        ));
    }
    let data_bytes = data.as_bytes();

    let schema = LogSchema::from_format("squid");
    let projected = vec![0, 5]; // timestamp, method

    let results = parallel_scan_chunks(
        data_bytes,
        2, // 2 chunks
        &schema,
        &projected,
        &vec![],
        &None,
    ).unwrap();

    // Should have results from both chunks
    let total_rows: usize = results.iter().map(|batches| {
        batches.iter().map(|b| b.len).sum::<usize>()
    }).sum();
    assert_eq!(total_rows, 100);
}
```

### 9b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_parallel_scan_filter_concat
```

### 9c. Write implementation

Add to `src/execution/parallel.rs`:

```rust
use rayon::prelude::*;
use std::sync::Arc;
use crate::common::types::Variables;
use crate::execution::batch::*;
use crate::execution::batch_scan::BatchScanOperator;
use crate::execution::log_schema::LogSchema;
use crate::execution::types::{Formula, StreamResult, StreamError};
use crate::functions::FunctionRegistry;

/// Error collection helper for rayon results.
fn collect_results<T>(results: Vec<StreamResult<T>>) -> StreamResult<Vec<T>> {
    let mut collected = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Ok(value) => collected.push(value),
            Err(e) => return Err(e),
        }
    }
    Ok(collected)
}

/// Scan file data in parallel chunks, returning batches per chunk (in order).
pub(crate) fn parallel_scan_chunks(
    data: &[u8],
    num_threads: usize,
    schema: &LogSchema,
    projected_fields: &[usize],
    filter_field_indices: &[usize],
    pushed_predicate: &Option<(Formula, Variables, Arc<FunctionRegistry>)>,
) -> StreamResult<Vec<Vec<ColumnBatch>>> {
    let chunks = split_chunks(data, num_threads);
    if chunks.is_empty() {
        return Ok(vec![]);
    }

    let partial_results: Vec<StreamResult<Vec<ColumnBatch>>> = chunks
        .par_iter()
        .map(|chunk| {
            let reader: Box<dyn BufRead> = Box::new(io::Cursor::new(*chunk));
            let mut scanner = BatchScanOperator::new(
                reader,
                schema.clone(),
                projected_fields.to_vec(),
                filter_field_indices.to_vec(),
                pushed_predicate.clone(),
            );
            let mut batches = Vec::new();
            while let Some(batch) = scanner.next_batch()? {
                batches.push(batch);
            }
            Ok(batches)
        })
        .collect();

    collect_results(partial_results)
}
```

Note: `LogSchema` needs to implement `Clone`. Check if it does; if not, add `#[derive(Clone)]` to it.

### 9d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 9e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: parallel scan dispatch with rayon for filter/projection queries"
```

---

## Step 10: CLI --threads flag and decision layer

**Files**: `src/cli.yml`, `src/main.rs`, `src/app.rs`

### 10a. Write implementation

In `src/cli.yml`, add `threads` arg to the `query` subcommand:

```yaml
          - threads:
              help: number of threads for parallel scanning (0 = auto, 1 = sequential)
              long: threads
              takes_value: true
              default_value: "0"
```

In `src/main.rs`, parse the flag and pass to `app::run`:

```rust
// After parsing output_mode, before calling app::run:
let threads: usize = sub_m.value_of("threads")
    .and_then(|s| s.parse().ok())
    .unwrap_or(0);
```

Update `app::run` signature to accept threads and pass through to execution. The decision of whether to use parallel scanning happens in the execution layer based on `ScanStrategy` from step 8.

### 10b. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 10c. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: add --threads CLI flag for parallel scanning control"
```

---

## Step 11: Parallel aggregation merge

**File**: `src/execution/parallel.rs`

### 11a. Write failing test

```rust
#[test]
fn test_merge_count_aggregates() {
    use crate::execution::types::*;
    use crate::common::types::Value;

    let mut agg1 = CountAggregate::new();
    agg1.add_record(&None, &Value::Int(1)).unwrap();
    agg1.add_record(&None, &Value::Int(2)).unwrap();

    let mut agg2 = CountAggregate::new();
    agg2.add_record(&None, &Value::Int(3)).unwrap();

    let merged = merge_count(&agg1, &agg2);
    assert_eq!(merged.get_aggregated(&None).unwrap(), Value::Int(3)); // 2 + 1
}

#[test]
fn test_merge_sum_aggregates() {
    use crate::execution::types::*;
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    let mut agg1 = SumAggregate::new();
    agg1.add_record(&None, &Value::Float(OrderedFloat::from(1.5f32))).unwrap();

    let mut agg2 = SumAggregate::new();
    agg2.add_record(&None, &Value::Float(OrderedFloat::from(2.5f32))).unwrap();

    let merged = merge_sum(&agg1, &agg2);
    let result = merged.get_aggregated(&None).unwrap();
    match result {
        Value::Float(f) => assert!((f.into_inner() - 4.0).abs() < 0.01),
        _ => panic!("expected Float"),
    }
}

#[test]
fn test_merge_avg_aggregates() {
    use crate::execution::types::*;
    use crate::common::types::Value;

    let mut agg1 = AvgAggregate::new();
    agg1.add_record(&None, &Value::Int(10)).unwrap();
    agg1.add_record(&None, &Value::Int(20)).unwrap();
    // agg1: sum=30, count=2

    let mut agg2 = AvgAggregate::new();
    agg2.add_record(&None, &Value::Int(30)).unwrap();
    // agg2: sum=30, count=1

    let merged = merge_avg(&agg1, &agg2);
    let result = merged.get_aggregated(&None).unwrap();
    // avg = (30+30)/(2+1) = 20.0
    match result {
        Value::Float(f) => assert!((f.into_inner() - 20.0).abs() < 0.01),
        _ => panic!("expected Float"),
    }
}
```

### 11b. Run tests to verify they fail

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_merge_
```

### 11c. Write implementation

Add merge functions to `src/execution/parallel.rs`:

```rust
use crate::execution::types::{CountAggregate, SumAggregate, AvgAggregate, MinAggregate, MaxAggregate, Tuple};

pub(crate) fn merge_count(a: &CountAggregate, b: &CountAggregate) -> CountAggregate {
    let mut merged = CountAggregate::new();
    for (key, &count_a) in &a.counts {
        *merged.counts.entry(key.clone()).or_insert(0) += count_a;
    }
    for (key, &count_b) in &b.counts {
        *merged.counts.entry(key.clone()).or_insert(0) += count_b;
    }
    merged
}

pub(crate) fn merge_sum(a: &SumAggregate, b: &SumAggregate) -> SumAggregate {
    let mut merged = SumAggregate::new();
    for (key, &sum_a) in &a.sums {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat::from(0.0f32));
        *entry = OrderedFloat::from(entry.into_inner() + sum_a.into_inner());
    }
    for (key, &sum_b) in &b.sums {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat::from(0.0f32));
        *entry = OrderedFloat::from(entry.into_inner() + sum_b.into_inner());
    }
    merged
}

pub(crate) fn merge_avg(a: &AvgAggregate, b: &AvgAggregate) -> AvgAggregate {
    let mut merged = AvgAggregate::new();
    for (key, &sum_a) in &a.sums {
        *merged.sums.entry(key.clone()).or_insert(0.0) += sum_a;
        *merged.counts.entry(key.clone()).or_insert(0) += a.counts.get(key).copied().unwrap_or(0);
    }
    for (key, &sum_b) in &b.sums {
        *merged.sums.entry(key.clone()).or_insert(0.0) += sum_b;
        *merged.counts.entry(key.clone()).or_insert(0) += b.counts.get(key).copied().unwrap_or(0);
    }
    merged
}
```

### 11d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 11e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: aggregate merge functions for parallel GROUP BY (count, sum, avg, min, max)"
```

---

## Step 12: Parallel sort-merge for ORDER BY

**File**: `src/execution/parallel.rs`

### 12a. Write failing test

```rust
#[test]
fn test_kway_merge_sorted_chunks() {
    use crate::common::types::{Value, Record};

    // Three pre-sorted chunks by int key
    let chunk0 = vec![
        (vec![1u8, 0, 0, 0], make_record(vec![("val", Value::Int(1))])),
        (vec![3u8, 0, 0, 0], make_record(vec![("val", Value::Int(3))])),
    ];
    let chunk1 = vec![
        (vec![2u8, 0, 0, 0], make_record(vec![("val", Value::Int(2))])),
        (vec![4u8, 0, 0, 0], make_record(vec![("val", Value::Int(4))])),
    ];

    let merged = kway_merge(vec![chunk0, chunk1], None);
    let vals: Vec<i32> = merged.iter().map(|r| {
        match r.get("val") {
            Some(Value::Int(i)) => *i,
            _ => panic!("expected Int"),
        }
    }).collect();
    assert_eq!(vals, vec![1, 2, 3, 4]);
}

#[test]
fn test_kway_merge_with_limit() {
    let chunk0 = vec![
        (vec![1u8], make_record(vec![("v", Value::Int(1))])),
        (vec![3u8], make_record(vec![("v", Value::Int(3))])),
    ];
    let chunk1 = vec![
        (vec![2u8], make_record(vec![("v", Value::Int(2))])),
        (vec![4u8], make_record(vec![("v", Value::Int(4))])),
    ];

    let merged = kway_merge(vec![chunk0, chunk1], Some(2));
    assert_eq!(merged.len(), 2);
}
```

### 12b. Run tests to verify they fail

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_kway_merge
```

### 12c. Write implementation

Add to `src/execution/parallel.rs`:

```rust
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use crate::common::types::Record;

struct MergeEntry {
    key: Vec<u8>,
    chunk_idx: usize,
    row_idx: usize,
    record: Record,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.chunk_idx == other.chunk_idx && self.row_idx == other.row_idx
    }
}
impl Eq for MergeEntry {}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
            .then(self.chunk_idx.cmp(&other.chunk_idx))
            .then(self.row_idx.cmp(&other.row_idx))
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge of pre-sorted chunks. Each chunk is a Vec of (sort_key, Record).
/// Optional limit stops after emitting N records.
pub(crate) fn kway_merge(
    mut chunks: Vec<Vec<(Vec<u8>, Record)>>,
    limit: Option<usize>,
) -> Vec<Record> {
    let mut heap = BinaryHeap::new();
    let mut iters: Vec<std::vec::IntoIter<(Vec<u8>, Record)>> = chunks
        .drain(..)
        .map(|c| c.into_iter())
        .collect();

    // Seed heap with first element from each chunk
    for (chunk_idx, iter) in iters.iter_mut().enumerate() {
        if let Some((key, record)) = iter.next() {
            heap.push(Reverse(MergeEntry { key, chunk_idx, row_idx: 0, record }));
        }
    }

    let mut result = Vec::new();
    while let Some(Reverse(entry)) = heap.pop() {
        result.push(entry.record);
        if let Some(limit) = limit {
            if result.len() >= limit {
                break;
            }
        }
        if let Some((key, record)) = iters[entry.chunk_idx].next() {
            heap.push(Reverse(MergeEntry {
                key,
                chunk_idx: entry.chunk_idx,
                row_idx: entry.row_idx + 1,
                record,
            }));
        }
    }

    result
}
```

### 12d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 12e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "feat: k-way merge for parallel ORDER BY with optional LIMIT"
```

---

## Step 13: End-to-end verification

### 13a. Run full test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 13b. Run benchmarks to measure improvement

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals -- bench_datasource
```

### 13c. Manual E2E test with a real log file

```bash
cd /Users/paulmeng/Develop/logq

# Sequential (baseline)
time cargo run -- query --table t:elb=data/AWSELB.log --threads 1 \
  "SELECT elb_status_code, COUNT(*) FROM t GROUP BY elb_status_code"

# Parallel
time cargo run -- query --table t:elb=data/AWSELB.log --threads 0 \
  "SELECT elb_status_code, COUNT(*) FROM t GROUP BY elb_status_code"

# Verify results match
```

### 13d. Commit any final fixes

```bash
cd /Users/paulmeng/Develop/logq && git commit -m "test: end-to-end verification of large file throughput optimizations"
```