# Large File Throughput Optimization Design (Final)

**Date**: 2026-04-06
**Goal**: Maximize logq throughput on multi-GB log files, regardless of query shape
**Inspiration**: Velox's I/O layer, memory management, and multi-Driver execution model

## Changes from v2

This revision addresses review feedback from `large-file-throughput-design-review-2.md`.

**C1 (AVG parallel merge correctness)**: `AvgAggregate` stores a running average (`OrderedFloat<f32>`), not a sum. Merging running averages introduces additional f32 precision loss. Added a prerequisite refactoring step (Section 2.0.2) to change `AvgAggregate` to store `(sum: f64, count: i64)` instead of `(avg: OrderedFloat<f32>, count: i64)`. This enables correct `(sum_a + sum_b, count_a + count_b)` merging and also improves precision for sequential execution. Updated Section 2.3.3 accordingly.

**C2 (parse_field_column signature incompatibility)**: `parse_field_column` takes `&[Vec<u8>]` but the mmap path needs `&[&[u8]]`. Added a prerequisite refactoring step (Section 2.0.1) to genericize `parse_field_column` and `tokenize_line` over `AsRef<[u8]>`. This touches `batch_tokenizer.rs`, `field_parser.rs`, `batch_scan.rs`, and `batch_predicate.rs` (~80-120 lines). Effort estimate revised.

**C3 (Rayon error handling)**: `rayon::ParallelIterator::collect` does not support `Result`-collecting like `std::iter::Iterator`. Replaced pseudocode with a pattern that compiles: `.collect::<Vec<StreamResult<_>>>()` followed by explicit error checking. Added error propagation strategy (Section 2.3.4): errors are collected per-chunk, first error is returned, remaining chunks run to completion (rayon does not support cancellation without `scope` + panics).

**S2 (DISTINCT)**: Added DISTINCT to activation criteria — falls back to sequential.

**S3 (LIMIT coordination)**: Added `compare_exchange` per-batch budget check for tighter overshoot bounds.

**S5 (BinaryHeap)**: Specified `BinaryHeap<Reverse<...>>` for k-way merge.

---

## Motivation

logq's current execution is single-threaded with an 8KB I/O buffer, and the batch path allocates per-line temporary buffers that are discarded each batch. For large files (hundreds of MB to multi-GB), these add up: excessive syscalls, thousands of short-lived allocations per batch, and zero utilization of multiple cores.

This design addresses throughput with two complementary strategies:
1. **I/O and allocation improvements** (low effort, immediate gains)
2. **Parallel chunk scanning** (higher effort, near-linear core scaling)

## Part 1: I/O and Allocation Layer

### 1.1 Larger BufReader Buffer

**Current**: `DEFAULT_CAPACITY = 8KB` in `datasource.rs:764-765`.

**Change**: Increase to 64KB.

```rust
const DEFAULT_CAPACITY: usize = 64 * (1 << 10); // 65536
```

**Rationale**: Sequential log file reads benefit from larger buffers. At 8KB, each MB of log data requires ~125 `read()` syscalls at ~1-2us each. At 64KB, that drops to ~16 syscalls. This is a conservative starting point; benchmarking against 128KB and 256KB should be done before finalizing, since modern OS readahead (128-256KB on Linux, adaptive on macOS) may shift the optimal point. The change is trivial regardless.

**Impact**: 10-20% I/O reduction on scan-dominated queries.
**Risk**: Near zero. Only cost is 56KB more heap per open file.

### 1.2 Reusable Tokenizer Scratch Buffers

**Current**: `tokenize_line()` returns a new `Vec<(usize, usize)>` per line. In the batch path (1024 lines/batch), this means 1024 Vec allocations + deallocations per batch.

**Change**: Pre-allocate a reusable offsets buffer in `BatchScanOperator`:

```rust
struct BatchScanOperator {
    // existing fields...
    offsets_scratch: Vec<(usize, usize)>, // reusable per-line
}
```

On each `tokenize_line()` call:
1. `offsets_scratch.clear()` — resets length to 0, keeps capacity
2. Tokenizer pushes offsets into the scratch buffer
3. `parse_field_column()` reads from the scratch buffer by index

The max number of fields per log format is known at schema time (ELB ~15, ALB ~29), so after the first line the Vec never reallocates again. Eliminates **2048 alloc/dealloc operations per batch**.

**Risk**: Very low. Mechanical refactor — tokenizer fills a provided Vec instead of returning a new one.

### 1.3 Arena Allocation for Batch Line Storage

**Current**: `read_lines()` copies each trimmed line into an individual `Vec<u8>`. For a 1024-line batch with ~300-byte average lines, that's 1024 separate heap allocations totaling ~300KB of scattered data.

**Change**: Use a contiguous byte arena per batch:

```rust
struct BatchLineArena {
    data: Vec<u8>,              // contiguous byte storage
    spans: Vec<(usize, usize)>, // (start_offset, length) per line
}

impl BatchLineArena {
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
}
```

After the first batch, `data` has capacity for ~300KB and never reallocates again. Replaces **1024 allocations per batch with 0** (amortized). Improves cache locality — all line bytes are contiguous in memory, benefiting the tokenization pass.

Inspired by Velox's `HashStringAllocator` — arena-style allocation with pool reuse instead of per-item system allocator calls.

**Phasing note**: This arena is the primary allocation strategy for the **sequential BufReader path**. When mmap is introduced in Part 2, the parallel path reads lines directly from the mmap slice, making the arena unnecessary for that path. However, the arena **remains in use** for the following fallback cases:
- **stdin input**: Cannot be mmap'd; must use BufReader + arena.
- **Small files** (< 16MB): Stay on the sequential BufReader path by design.
- **mmap failure**: If mmap fails (permissions, address space, etc.), the BufReader + arena path is the fallback.

The arena is therefore not wasted work — it improves the sequential path that always remains available.

**Risk**: Low. Tokenizer and field parsers already operate on `&[u8]` slices.

## Part 2: Parallel Chunk Scanning

### 2.0 Prerequisite Refactoring

These changes are required before the parallel path can be implemented.

#### 2.0.1 Genericize parse_field_column Over Slice Types

**Problem**: `parse_field_column` in `field_parser.rs` takes `lines: &[Vec<u8>]`. The mmap zero-copy path needs to pass `&[&[u8]]` (slices into the mmap). These types are incompatible.

**Solution**: Genericize over `AsRef<[u8]>`:

```rust
// Before:
pub fn parse_field_column(lines: &[Vec<u8>], offsets: &[(usize, usize)], ...) -> TypedColumn

// After:
pub fn parse_field_column<L: AsRef<[u8]>>(lines: &[L], offsets: &[(usize, usize)], ...) -> TypedColumn
```

Inside the function, `lines[row]` becomes `lines[row].as_ref()`, which works for both `Vec<u8>` (existing) and `&[u8]` (mmap).

**Scope**: This requires changes in:
- `field_parser.rs`: 6 match arms in `parse_field_column`, plus `parse_field_column_selected` — each `lines[row]` access becomes `lines[row].as_ref()`
- `batch_tokenizer.rs`: `tokenize_line` already takes `&[u8]`, no change needed
- `batch_scan.rs`: Call sites pass `&self.lines` which is `&[Vec<u8>]` — no change needed (generic infers correctly)
- `batch_predicate.rs`: `evaluate_batch_predicate` takes `lines: &[Vec<u8>]` — genericize similarly

**Effort**: ~80-120 lines of changes across 4 files. Purely mechanical — add type parameter, replace direct indexing with `.as_ref()`.

**Risk**: Very low. No behavioral change for the existing path. The generic is monomorphized at compile time — no runtime cost.

#### 2.0.2 Refactor AvgAggregate to Store Sum Instead of Running Average

**Problem**: `AvgAggregate` (types.rs:1090-1132) stores a running average in `averages: HashMap<..., OrderedFloat<f32>>`. The `add_record` method computes `(avg * count + new_value) / new_count`. For parallel merge, we need `(sum, count)` pairs, not `(avg, count)`, because reconstructing sum from `avg * count` with f32 loses precision (especially for large counts where the running average has already accumulated rounding errors).

**Solution**: Change `AvgAggregate` to store `(sum: f64, count: i64)`:

```rust
// Before:
pub struct AvgAggregate {
    averages: HashMap<Option<Tuple>, OrderedFloat<f32>>,
    counts: HashMap<Option<Tuple>, i64>,
}

// After:
pub struct AvgAggregate {
    sums: HashMap<Option<Tuple>, f64>,
    counts: HashMap<Option<Tuple>, i64>,
}
```

The `add_record` method becomes a simple `sum += value` instead of the running average formula. The `get_aggregated` method computes `sum / count` at the end.

**Benefits**:
- Enables correct parallel merge: `(sum_a + sum_b, count_a + count_b)`
- Improves precision even in sequential mode — f64 sum accumulation has ~15 digits of precision vs f32's ~7
- Simpler code — removes the running average update formula

**Effort**: ~30 lines changed in `types.rs`. Update `add_record`, `get_aggregated`, and the struct definition.

**Risk**: Very low. The output type remains `Value::Float(OrderedFloat<f32>)` from `get_aggregated` — downstream behavior is unchanged. Precision can only improve.

### 2.1 Memory-Mapped I/O

**Change**: For the parallel path, replace `BufReader::read_line()` with `memmap2` for zero-copy file access:

```rust
use memmap2::MmapOptions;

let file = File::open(path)?;
let mmap = unsafe { MmapOptions::new().map(&file)? };
// mmap acts as &[u8] over the entire file
```

Within each chunk, lines are found by scanning for `\n` in the mmap slice. The tokenizer and field parsers operate on `&[u8]` / `&str` (after the 2.0.1 refactoring), so they work directly on mmap'd memory without any intermediate copy.

This eliminates the entire `read_line()` → `Vec<u8>` copy chain for the parallel path.

**Dependency**: `memmap2` — stable, widely used Rust crate (~200 lines). Safe for read-only mappings of files not concurrently modified.

**Risk**: Low-medium. Edge cases handled explicitly (see Section 2.1.1).

#### 2.1.1 Mmap Error Handling and Fallback Strategy

**Fallback architecture**: Mmap is attempted first; on any failure, the system falls back to the sequential BufReader + arena path (Part 1).

```rust
enum ScanStrategy {
    Mmap(memmap2::Mmap),
    BufReader(Box<dyn BufRead>),
}

fn choose_strategy(path: &Path, file_size: u64) -> ScanStrategy {
    // 1. Size gate: skip mmap for small files
    if file_size < PARALLEL_THRESHOLD {
        return ScanStrategy::BufReader(open_bufreader(path));
    }

    // 2. Attempt mmap
    match File::open(path).and_then(|f| unsafe { MmapOptions::new().map(&f) }) {
        Ok(mmap) => ScanStrategy::Mmap(mmap),
        Err(_) => {
            // mmap failed (permissions, address space, etc.) — fall back
            ScanStrategy::BufReader(open_bufreader(path))
        }
    }
}
```

**SIGBUS risk**: On Unix systems, if the underlying file is truncated or deleted while the mmap is active, accessing the now-invalid pages triggers a SIGBUS signal. SIGBUS is process-fatal and cannot be caught as a Rust `Result` error. This design **assumes files are not modified during query execution**, which is the expected use case (logq queries completed log files). This assumption is documented in the `--help` text for `--threads`.

**Mitigations**:
- The mmap is created with read-only permissions (`Mmap::map`, not `MmapMut`), reducing the risk surface.
- File size is checked before mmap creation. If the file is empty, the BufReader path is used (no benefit from mmap on empty files).
- On 32-bit platforms, files larger than ~2GB could exhaust virtual address space. A compile-time or runtime check gates mmap to 64-bit targets only (logq's primary target). On 32-bit, the BufReader fallback is used automatically.

**stdin is excluded**: `DataSource::Stdin` has no file descriptor suitable for mmap. Stdin always uses the sequential BufReader + arena path. This is enforced in the activation criteria (Section 2.5).

### 2.2 Chunk Splitting

Split the file into N chunks along newline boundaries:

```rust
fn split_chunks(data: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    if data.is_empty() || num_chunks == 0 {
        return vec![];
    }

    let chunk_size = data.len() / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for i in 0..num_chunks {
        // Guard: if we've consumed all data, stop producing chunks.
        // This handles num_chunks > number_of_lines gracefully —
        // we just produce fewer chunks than requested.
        if start >= data.len() {
            break;
        }

        if i == num_chunks - 1 {
            // Last chunk: take everything remaining
            chunks.push(&data[start..]);
            break;
        }

        let raw_end = std::cmp::min(start + chunk_size, data.len());

        // Find the next newline at or after raw_end to avoid splitting mid-line.
        // Search only within the remaining data to prevent OOB access.
        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1, // include the newline in this chunk
            None => data.len(),              // no newline found; take rest of file
        };

        chunks.push(&data[start..end]);
        start = end;
    }

    // Remove any trailing empty chunks (defensive)
    while chunks.last().map_or(false, |c| c.is_empty()) {
        chunks.pop();
    }

    chunks
}
```

**Edge cases handled**:
- `data.is_empty()`: Returns empty vec, no chunks produced.
- `num_chunks == 0`: Returns empty vec.
- `num_chunks > number_of_lines`: The `start >= data.len()` guard stops producing chunks once data is exhausted. Result: fewer chunks than requested, each containing at least one line.
- `num_chunks == 1`: The `i == num_chunks - 1` branch takes everything; equivalent to no splitting.
- File with no trailing newline: The `None` branch in newline search consumes to `data.len()`, correctly including the last line without a terminator.
- `chunk_size == 0` (file smaller than num_chunks): After the first chunk consumes everything up to the first newline (or EOF), `start >= data.len()` terminates the loop.

Each chunk is a `&[u8]` slice — zero allocation, zero copy. Line-boundary adjustment scans at most one line length (~300 bytes) per chunk boundary. Chunk size target: ~16MB (large enough to amortize thread overhead, small enough for even distribution).

### 2.3 Parallel Execution with Rayon

Each chunk runs the scan+filter pipeline independently on a rayon thread pool.

#### 2.3.1 Send/Sync Audit for Rayon Closures

Rayon's `par_iter().map()` requires the closure and all captured values to be `Send`. The following audit covers every type that flows into the per-chunk closure:

| Type | Send? | Sync? | Notes |
|------|-------|-------|-------|
| `&[u8]` (mmap chunk slice) | Yes | Yes | Immutable reference to `Mmap` which is `Send + Sync` |
| `LogSchema` | Yes | Yes | Contains `&'static Vec<String>` and `&'static Vec<DataType>` — static references are `Send + Sync` |
| `Formula` | Yes | Yes | Enum of `Clone + Eq` types. All variants contain `Expression`, `Relation`, `Value`, `PathExpr` — all composed of `String`, `i32`, `OrderedFloat`, `bool`, `Box`, `Vec`. No `Rc`, no `Cell`, no raw pointers. |
| `Expression` | Yes | Yes | Same reasoning as Formula. The `Subquery(Box<Node>)` variant contains a `Node` which is also composed entirely of `Send + Sync` types. |
| `Value` | Yes | Yes | Enum of `i32`, `OrderedFloat<f32>`, `bool`, `String`, `chrono::DateTime`, `Box<HttpRequest>`, `Box<Host>`, `LinkedHashMap<String, Value>`, `Vec<Value>`. All `Send + Sync`. |
| `Variables` (`LinkedHashMap<String, Value>`) | Yes | Yes | `LinkedHashMap` is `Send + Sync` when K and V are. |
| `Arc<FunctionRegistry>` | Yes | Yes | `FunctionRegistry` contains `HashMap<String, FunctionDef>`. `FunctionDef.func` is `Box<dyn Fn(&[Value]) -> ... + Send + Sync>` — already bounded. `Arc<T: Send + Sync>` is `Send + Sync`. |
| `BatchScanOperator` | N/A | N/A | **Not shared across threads.** In the parallel path, each thread constructs its own `BatchScanOperator` from a `Cursor<&[u8]>` chunk slice. `Cursor<&[u8]>` implements `BufRead + Send`. The existing `Box<dyn BufRead>` in `BatchScanOperator` does not need a `Send` bound because the operator is constructed thread-locally. |
| `thread_local!` caches | N/A | N/A | `LIKE_REGEX_CACHE` (types.rs:353) and `REGEX_CACHE` (regexp.rs:10) are `thread_local!` with `RefCell<LruCache>`. Each rayon worker thread gets its own TLS instance. No cross-thread sharing. **Not a blocker.** |

**Required code changes for Send/Sync**:
- The parallel path does **not** share `BatchScanOperator` instances across threads. Each chunk gets a freshly constructed scanner with `reader: Box::new(Cursor::new(chunk))`. No existing trait objects need `Send` bounds added.
- The `Formula`, `Variables`, and `Arc<FunctionRegistry>` are cloned (or arc-cloned) into each closure. All are already `Send + Sync` by composition.
- **No blockers identified.** The main effort is structural: building the parallel dispatch layer, not retrofitting Send bounds.

#### 2.3.2 LIMIT Query Coordination

Each thread checks a global `AtomicUsize` budget before producing each batch, using `compare_exchange` for tighter coordination:

```rust
let remaining = Arc::new(AtomicUsize::new(limit));

// Inside each thread's loop:
loop {
    let budget = remaining.load(Ordering::Relaxed);
    if budget == 0 {
        break; // Another thread satisfied the LIMIT
    }
    
    let batch = match scanner.next_batch()? {
        Some(b) => b,
        None => break,
    };
    
    let batch_rows = batch.num_rows();
    // Atomically claim rows from the budget
    let claimed = remaining.fetch_sub(
        batch_rows.min(budget), Ordering::Relaxed
    ).min(batch_rows);
    
    if claimed > 0 {
        batches.push(batch); // May contain more rows than claimed
    }
    if budget <= batch_rows {
        break; // Budget exhausted
    }
}
```

**Overshoot bound**: With this approach, overshoot is limited to at most `num_threads` batches (one in-flight batch per thread at the moment the budget hits zero), rather than being unbounded. The existing `LimitStream` (or equivalent truncation) still enforces the exact LIMIT after collection.

#### 2.3.3 Parallel Aggregation Merge

**Aggregation queries** (GROUP BY):

```rust
let partial_results: Vec<StreamResult<HashMap<GroupKey, Accumulators>>> = chunks
    .par_iter()
    .map(|chunk| {
        let reader = Box::new(Cursor::new(*chunk));
        let mut scanner = BatchScanOperator::new(
            reader, schema.clone(), fields.clone(),
            filter_fields.clone(), predicate.clone(),
        );
        let mut local_agg = HashMap::new();
        while let Some(batch) = scanner.next_batch()? {
            update_aggregates(&mut local_agg, &batch);
        }
        Ok(local_agg)
    })
    .collect(); // Vec<StreamResult<...>>, not Result<Vec<...>>

// Explicit error handling (see 2.3.4)
let partial_aggregates = collect_results(partial_results)?;

let final_agg = partial_aggregates.into_iter().reduce(merge_aggregates);
```

Each thread maintains local accumulators; a single-threaded merge combines them. The merge strategy depends on the aggregate function:

**Directly parallelizable** (associative + commutative merge):

| Aggregate | Merge Operation | Notes |
|-----------|----------------|-------|
| `COUNT` | `count_a + count_b` | Trivial |
| `SUM` | `sum_a + sum_b` | Trivial |
| `MIN` | `min(min_a, min_b)` | Trivial |
| `MAX` | `max(max_a, max_b)` | Trivial |
| `APPROX_COUNT_DISTINCT` | `hll_a.merge(hll_b)` | HyperLogLog supports merge natively |

**Parallelizable with carrier state**:

| Aggregate | Merge Operation | Notes |
|-----------|----------------|-------|
| `AVG` | `(sum_a + sum_b, count_a + count_b)` → `total_sum / total_count` | Requires 2.0.2 refactoring to store `(sum: f64, count: i64)` instead of running average. Merge is then trivial addition of sums and counts. |
| `APPROX_PERCENTILE` | `tdigest_a.merge(tdigest_b)` | t-digest supports merge natively. The `tdigest` crate's `TDigest::merge_digests` handles this. |

**Require chunk ordering**:

| Aggregate | Merge Operation | Notes |
|-----------|----------------|-------|
| `FIRST` | Take `first` from the lowest-index chunk that has a value for this group key | Chunks are processed in order (chunk 0, 1, 2, ...). The merge iterates partial results in chunk order and keeps the first non-empty value per group key. |
| `LAST` | Take `last` from the highest-index chunk that has a value for this group key | Same as FIRST but iterates in reverse chunk order. |

**Not parallelizable** (requires all records):

| Aggregate | Strategy | Notes |
|-----------|----------|-------|
| `PERCENTILE_DISC` | Each chunk collects all matching values into a `Vec<Value>`. The merge concatenates all per-chunk Vecs, then sorts and selects the percentile value. | Exact percentile requires the full sorted dataset. Memory cost: O(matching_rows). |
| `GROUP_AS` | Each chunk collects records into a `Vec`. The merge concatenates all per-chunk Vecs in chunk order. | Must preserve original ordering. |

For typical GROUP BY cardinalities (hundreds to thousands of groups), the merge cost is negligible relative to scanning.

#### 2.3.4 Error Propagation Strategy

Rayon's `ParallelIterator` does not support `Result`-collecting the way `std::iter::Iterator::collect::<Result<Vec<_>, E>>()` does. The parallel path uses this pattern:

```rust
fn collect_results<T>(results: Vec<StreamResult<T>>) -> StreamResult<Vec<T>> {
    let mut collected = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Ok(value) => collected.push(value),
            Err(e) => return Err(e), // Return first error encountered
        }
    }
    Ok(collected)
}
```

**Behavior on error**:
- All chunks run to completion (rayon does not support cancellation of in-flight work without panics).
- After all chunks finish, results are checked in chunk order.
- The first error encountered is returned to the caller.
- Remaining chunk results (successful or failed) are dropped.

**Rationale**: Log file scan errors are typically malformed lines, which are rare. Running remaining chunks to completion wastes minimal work in practice. The alternative — using `rayon::scope` with panic-based cancellation — adds complexity and is not warranted for the expected error frequency.

**Future enhancement**: If early cancellation becomes important (e.g., for permission errors that affect all chunks), a shared `AtomicBool` flag can be checked at the top of each batch loop iteration, allowing threads to bail out cooperatively.

**Dependency**: `rayon` — standard Rust parallelism library with work-stealing thread pool.

### 2.4 ORDER BY with Parallel Sort-Merge

**Phase 1 — Parallel local sort**: Each thread scans its chunk, collects matching records, and sorts locally using the existing `prefix_sort.rs` infrastructure. Each thread gets its own `PrefixSortEncoder` and sorted buffer.

**Phase 2 — K-way merge**: Merge pre-sorted chunks using `std::collections::BinaryHeap` with `Reverse` wrapper for min-heap behavior:

```rust
use std::cmp::Reverse;
use std::collections::BinaryHeap;

struct MergeEntry {
    key: Vec<u8>,        // prefix-sort encoded key
    chunk_idx: usize,    // tiebreaker for stability
    row_idx: usize,      // position within chunk's sorted output
    record: Record,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
            .then(self.chunk_idx.cmp(&other.chunk_idx))
            .then(self.row_idx.cmp(&other.row_idx))
    }
}

// Initialize heap with first record from each chunk
let mut heap = BinaryHeap::new();
for (chunk_idx, chunk_records) in sorted_chunks.iter_mut().enumerate() {
    if let Some((key, record)) = chunk_records.next() {
        heap.push(Reverse(MergeEntry { key, chunk_idx, row_idx: 0, record }));
    }
}

// Pull records in sorted order
while let Some(Reverse(entry)) = heap.pop() {
    emit(entry.record);
    if let Some((key, record)) = sorted_chunks[entry.chunk_idx].next() {
        heap.push(Reverse(MergeEntry {
            key, chunk_idx: entry.chunk_idx,
            row_idx: entry.row_idx + 1, record,
        }));
    }
}
```

Complexity: O(N log K) where K is number of chunks (typically 4-8).

**With ORDER BY ... LIMIT N**: Each thread keeps only top-N locally (partial sort / selection), reducing memory from O(total_rows) to O(N x K). The merge only pulls N records from the heap, then stops.

**Stability**: Chunk index + row index used as tiebreaker to preserve original file ordering for equal sort keys.

**String key tie-breaking**: When the prefix-sort key uses a truncated string prefix and two entries have identical prefix bytes, the `Ord` implementation falls back to `chunk_idx.cmp` + `row_idx.cmp`, which preserves stability but does not resolve the actual string comparison. For correctness, if the sort key contains strings, ties on the prefix must fall back to full `Record` comparison using the original sort key expressions. This matches the existing behavior in `prefix_sort.rs` which already handles this case for sequential sort.

### 2.5 Activation Criteria

```
Input is stdin?    → Sequential (BufReader + arena, no mmap possible)

File size < 16MB?  → Sequential (BufReader + arena)
                      Overhead of mmap + thread spawn > benefit

mmap fails?        → Sequential (BufReader + arena fallback)

File size >= 16MB  → Parallel chunked scan (mmap + rayon)
  AND mmap OK?
  │
  ├── Filter/Project only     → concat results, truncate to LIMIT if needed
  ├── GROUP BY                → merge partial aggregates (see 2.3.3)
  ├── ORDER BY                → k-way merge of sorted chunks
  ├── ORDER BY ... LIMIT N    → partial sort + k-way merge
  ├── DISTINCT                → sequential (requires global dedup state)
  └── JOIN                    → sequential (right side needs full access)
```

**stdin is always sequential**: `DataSource::Stdin` cannot be memory-mapped. The BufReader + arena path handles stdin. No parallel scanning is attempted.

**DISTINCT stays sequential**: Each chunk would produce local distinct sets, but merging requires deduplication across chunks (union of sets). While straightforward in principle, DISTINCT queries on log files are uncommon enough that the parallelization benefit doesn't justify the added complexity. Can be added as a follow-up.

**JOINs stay sequential**: Parallelizing joins requires broadcasting or partitioning both sides by join key — significant complexity for a rare log-query use case.

**JSONL stays sequential**: The batch path already excludes JSONL (`execution/types.rs:599`). Parallel JSONL could be added as a follow-up.

**16MB threshold**: This is a heuristic starting point. The optimal threshold depends on query selectivity, core count, and I/O speed. It is implemented as a constant (`PARALLEL_THRESHOLD`) that can be tuned via benchmarking and is overridable with `--threads 1` (force sequential). Per-query-type threshold tuning (e.g., higher threshold for aggregation queries due to per-thread accumulator overhead) is a follow-up.

### 2.6 CLI Surface

A `--threads N` flag with default `0` (auto-detect = number of cores). `--threads 1` forces sequential execution for reproducibility or debugging.

```
logq --table t:elb=access.log --threads 4 \
  "SELECT status, COUNT(*) FROM t GROUP BY status"
```

## Summary

| # | Change | Effort | Expected Impact | Risk |
|---|--------|--------|----------------|------|
| 1 | BufReader 8KB → 64KB | ~5 lines | 10-20% I/O reduction | Near zero |
| 2 | Reusable tokenizer scratch buffers | ~60 lines | Eliminate 1024 allocs/batch | Low |
| 3 | Arena allocation for batch lines | ~100 lines | Eliminate 1024 allocs/batch + cache locality | Low |
| 4 | Genericize parse_field_column (2.0.1) | ~100 lines | Enable zero-copy mmap path | Very low |
| 5 | Refactor AvgAggregate to store sum (2.0.2) | ~30 lines | Enable correct parallel AVG merge + precision improvement | Very low |
| 6 | Memory-mapped I/O + fallback | ~150 lines | Eliminate all read_line copies (parallel path) | Low-medium |
| 7 | Chunk splitting + edge cases | ~80 lines (incl. tests) | Correct parallel workload distribution | Low |
| 8 | Parallel scan dispatch (rayon) + error handling | ~350 lines | Near-linear core scaling (3-6x) | Medium |
| 9 | Parallel aggregation merge | ~250 lines | Same scaling for GROUP BY | Medium |
| 10 | Parallel sort-merge for ORDER BY | ~200 lines | Same scaling + LIMIT optimization | Medium |
| 11 | Decision layer + --threads flag | ~100 lines | Correct path selection | Low |
| 12 | Tests for parallel paths | ~250 lines | Correctness verification | Low |

**Total**: ~1675 lines of new/modified code.
**New dependencies**: `memmap2`, `rayon`.
**Unchanged**: Parser, AST, desugarer, logical planner, optimizer, function registry, output formatting, JSONL path, JOIN operators.

## Implementation Order

Recommended sequence (each step is independently shippable and benchmarkable):

1. **Part 1** (items 1-3): BufReader buffer, scratch buffers, arena — immediate sequential gains
2. **Prerequisites** (items 4-5): Genericize parse_field_column, refactor AvgAggregate — unblocks Part 2
3. **Core parallel** (items 6-8): Mmap, chunk splitting, rayon dispatch — filter/project parallelism
4. **Parallel operators** (items 9-10): Aggregation merge, sort-merge — GROUP BY and ORDER BY parallelism
5. **Polish** (items 11-12): Decision layer, CLI flag, tests
