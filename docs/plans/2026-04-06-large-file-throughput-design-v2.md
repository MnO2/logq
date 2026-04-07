# Large File Throughput Optimization Design (v2)

**Date**: 2026-04-06
**Goal**: Maximize logq throughput on multi-GB log files, regardless of query shape
**Inspiration**: Velox's I/O layer, memory management, and multi-Driver execution model

## Changes from v1

This revision addresses review feedback from `large-file-throughput-design-review-1.md`.

**C1 (chunk splitting OOB bug)**: Rewrote `split_chunks` algorithm with guards against `start >= data.len()`, degenerate `num_chunks > line_count`, and empty/overlapping chunks. Added explicit edge-case handling.

**C2 (Send bounds for rayon)**: Added Section 2.3.1 with a full audit of types flowing into rayon closures. `Formula`, `Expression`, `Value`, `LogSchema`, `FunctionRegistry` are all `Send + Sync` today. `BatchScanOperator` holds `Box<dyn BufRead>` which is not `Send`-bounded; however, the parallel path constructs a new scanner per chunk from an `&[u8]` slice (via `Cursor`), so no existing `dyn BufRead` crosses thread boundaries. The `thread_local!` caches in `regexp.rs` and `types.rs` are not blockers -- each thread gets its own TLS instance. Effort estimate revised upward.

**C3 (mmap vs arena contradiction)**: Clarified phasing. The arena (1.3) is a stepping-stone for the sequential BufReader path. When mmap is introduced in Part 2, it replaces the arena for the parallel path. The arena remains useful for the BufReader fallback (stdin, small files, mmap failure).

**C4 (SIGBUS / mmap error handling)**: Added Section 2.1.1 with a fallback strategy: attempt mmap, fall back to BufReader on failure. Documented SIGBUS risk and static-file assumption. Added pre-scan file size check.

**S2 (non-associative aggregates)**: Added Section 2.3.3 documenting which aggregates support parallel merge and which require special handling.

**S3 (LIMIT overshoot)**: Documented that output truncation is needed after parallel collection (Section 2.3.2).

**S6 (stdin)**: Explicitly excluded stdin from mmap/parallel path in activation criteria (Section 2.5).

**S7 (effort estimates)**: Revised upward to 1200-1500 lines total.

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
1. `offsets_scratch.clear()` -- resets length to 0, keeps capacity
2. Tokenizer pushes offsets into the scratch buffer
3. `parse_field_column()` reads from the scratch buffer by index

The max number of fields per log format is known at schema time (ELB ~15, ALB ~29), so after the first line the Vec never reallocates again. Eliminates **2048 alloc/dealloc operations per batch**.

**Risk**: Very low. Mechanical refactor -- tokenizer fills a provided Vec instead of returning a new one.

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

After the first batch, `data` has capacity for ~300KB and never reallocates again. Replaces **1024 allocations per batch with 0** (amortized). Improves cache locality -- all line bytes are contiguous in memory, benefiting the tokenization pass.

Inspired by Velox's `HashStringAllocator` -- arena-style allocation with pool reuse instead of per-item system allocator calls.

**Phasing note**: This arena is the primary allocation strategy for the **sequential BufReader path**. When mmap is introduced in Part 2, the parallel path reads lines directly from the mmap slice, making the arena unnecessary for that path. However, the arena **remains in use** for the following fallback cases:
- **stdin input**: Cannot be mmap'd; must use BufReader + arena.
- **Small files** (< 16MB): Stay on the sequential BufReader path by design.
- **mmap failure**: If mmap fails (permissions, address space, etc.), the BufReader + arena path is the fallback.

The arena is therefore not wasted work -- it improves the sequential path that always remains available.

**Risk**: Low. Tokenizer and field parsers already operate on `&[u8]` slices.

## Part 2: Parallel Chunk Scanning

### 2.1 Memory-Mapped I/O

**Change**: For the parallel path, replace `BufReader::read_line()` with `memmap2` for zero-copy file access:

```rust
use memmap2::MmapOptions;

let file = File::open(path)?;
let mmap = unsafe { MmapOptions::new().map(&file)? };
// mmap acts as &[u8] over the entire file
```

Within each chunk, lines are found by scanning for `\n` in the mmap slice. The tokenizer and field parsers already operate on `&[u8]` / `&str`, so they work directly on mmap'd memory without any intermediate copy.

This eliminates the entire `read_line()` -> `Vec<u8>` copy chain for the parallel path. Note that `parse_field_column` currently takes `&[Vec<u8>]` (not `&[&[u8]]`), so the signature will need a small adjustment to accept either owned or borrowed slices for the zero-copy mmap path.

**Dependency**: `memmap2` -- stable, widely used Rust crate (~200 lines). Safe for read-only mappings of files not concurrently modified.

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
            // mmap failed (permissions, address space, etc.) -- fall back
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
        // This handles num_chunks > number_of_lines gracefully --
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

Each chunk is a `&[u8]` slice -- zero allocation, zero copy. Line-boundary adjustment scans at most one line length (~300 bytes) per chunk boundary. Chunk size target: ~16MB (large enough to amortize thread overhead, small enough for even distribution).

### 2.3 Parallel Execution with Rayon

Each chunk runs the scan+filter pipeline independently on a rayon thread pool.

#### 2.3.1 Send/Sync Audit for Rayon Closures

Rayon's `par_iter().map()` requires the closure and all captured values to be `Send`. The following audit covers every type that flows into the per-chunk closure:

| Type | Send? | Sync? | Notes |
|------|-------|-------|-------|
| `&[u8]` (mmap chunk slice) | Yes | Yes | Immutable reference to `Mmap` which is `Send + Sync` |
| `LogSchema` | Yes | Yes | Contains `&'static Vec<String>` and `&'static Vec<DataType>` -- static references are `Send + Sync` |
| `Formula` | Yes | Yes | Enum of `Clone + Eq` types. All variants contain `Expression`, `Relation`, `Value`, `PathExpr` -- all composed of `String`, `i32`, `OrderedFloat`, `bool`, `Box`, `Vec`. No `Rc`, no `Cell`, no raw pointers. |
| `Expression` | Yes | Yes | Same reasoning as Formula. The `Subquery(Box<Node>)` variant contains a `Node` which is also composed entirely of `Send + Sync` types. |
| `Value` | Yes | Yes | Enum of `i32`, `OrderedFloat<f32>`, `bool`, `String`, `chrono::DateTime`, `Box<HttpRequest>`, `Box<Host>`, `LinkedHashMap<String, Value>`, `Vec<Value>`. All `Send + Sync`. |
| `Variables` (`LinkedHashMap<String, Value>`) | Yes | Yes | `LinkedHashMap` is `Send + Sync` when K and V are. |
| `Arc<FunctionRegistry>` | Yes | Yes | `FunctionRegistry` contains `HashMap<String, FunctionDef>`. `FunctionDef.func` is `Box<dyn Fn(&[Value]) -> ... + Send + Sync>` -- already bounded. `Arc<T: Send + Sync>` is `Send + Sync`. |
| `BatchScanOperator` | N/A | N/A | **Not shared across threads.** In the parallel path, each thread constructs its own `BatchScanOperator` from a `Cursor<&[u8]>` chunk slice. `Cursor<&[u8]>` implements `BufRead + Send`. The existing `Box<dyn BufRead>` in `BatchScanOperator` does not need a `Send` bound because the operator is constructed thread-locally. |
| `thread_local!` caches | N/A | N/A | `LIKE_REGEX_CACHE` (types.rs:353) and `REGEX_CACHE` (regexp.rs:10) are `thread_local!` with `RefCell<LruCache>`. Each rayon worker thread gets its own TLS instance. No cross-thread sharing. **Not a blocker.** |

**Required code changes for Send/Sync**:
- The parallel path does **not** share `BatchScanOperator` instances across threads. Each chunk gets a freshly constructed scanner with `reader: Box::new(Cursor::new(chunk))`. No existing trait objects need `Send` bounds added.
- The `Formula`, `Variables`, and `Arc<FunctionRegistry>` are cloned (or arc-cloned) into each closure. All are already `Send + Sync` by composition.
- **No blockers identified.** The main effort is structural: building the parallel dispatch layer, not retrofitting Send bounds.

**Filter/projection queries** (no aggregation, no ordering):

```rust
use rayon::prelude::*;

let results: Vec<Vec<ColumnBatch>> = chunks
    .par_iter()
    .map(|chunk| {
        // Each thread builds its own scanner -- no Send issues
        let reader = Box::new(Cursor::new(*chunk));
        let mut scanner = BatchScanOperator::new(
            reader, schema.clone(), fields.clone(),
            filter_fields.clone(), predicate.clone(),
        );
        let mut batches = Vec::new();
        while let Some(batch) = scanner.next_batch()? {
            batches.push(batch);
        }
        Ok(batches)
    })
    .collect::<StreamResult<Vec<_>>>()?;

// Flatten in chunk order -- preserves original file ordering
let all_batches = results.into_iter().flatten();
```

#### 2.3.2 LIMIT Query Coordination

Each thread tracks a local count. A global `AtomicUsize` budget coordinates early termination -- when enough rows are found, remaining threads bail out.

**Overshoot note**: Due to the concurrent nature of the budget check, total collected rows may exceed the LIMIT by up to `(num_threads - 1) * batch_size` rows. This is acceptable because the existing `LimitStream` (or an equivalent truncation step) sits on top of the parallel output and enforces the exact LIMIT. The parallel budget is a **best-effort early termination optimization**, not a correctness guarantee. Output must always be truncated to the exact LIMIT after collection.

#### 2.3.3 Parallel Aggregation Merge

**Aggregation queries** (GROUP BY):

```rust
let partial_aggregates: Vec<HashMap<GroupKey, Accumulators>> = chunks
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
    .collect::<StreamResult<Vec<_>>>()?;

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
| `AVG` | Carry `(sum, count)` pairs; final result is `total_sum / total_count` | Cannot merge averages directly. Each partial accumulator must store sum and count separately. |
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

**Dependency**: `rayon` -- standard Rust parallelism library with work-stealing thread pool.

### 2.4 ORDER BY with Parallel Sort-Merge

**Phase 1 -- Parallel local sort**: Each thread scans its chunk, collects matching records, and sorts locally using the existing `prefix_sort.rs` infrastructure. Each thread gets its own `PrefixSortEncoder` and sorted buffer.

**Phase 2 -- K-way merge**: Merge pre-sorted chunks using a min-heap. O(N log K) where K is number of chunks (typically 4-8). This is new code, integrated at the `Node::OrderBy` level in `execution/types.rs` where the parallel vs. sequential decision is made.

**With ORDER BY ... LIMIT N**: Each thread keeps only top-N locally (partial sort / selection), reducing memory from O(total_rows) to O(N x K). The merge only pulls N records from the heap.

**Stability**: Chunk index is used as a tiebreaker to preserve original file ordering for equal sort keys.

### 2.5 Activation Criteria

```
Input is stdin?    --> Sequential (BufReader + arena, no mmap possible)

File size < 16MB?  --> Sequential (BufReader + arena)
                       Overhead of mmap + thread spawn > benefit

mmap fails?        --> Sequential (BufReader + arena fallback)

File size >= 16MB  --> Parallel chunked scan (mmap + rayon)
  AND mmap OK?
  |
  +-- Filter/Project only     -> concat results, truncate to LIMIT if needed
  +-- GROUP BY                -> merge partial aggregates (see 2.3.3)
  +-- ORDER BY                -> k-way merge of sorted chunks
  +-- ORDER BY ... LIMIT N    -> partial sort + k-way merge
  +-- JOIN                    -> sequential (right side needs full access)
```

**stdin is always sequential**: `DataSource::Stdin` cannot be memory-mapped. The BufReader + arena path handles stdin. No parallel scanning is attempted.

**JOINs stay sequential**: Parallelizing joins requires broadcasting or partitioning both sides by join key -- significant complexity for a rare log-query use case.

**JSONL stays sequential**: The batch path already excludes JSONL (`execution/types.rs:599`). Parallel JSONL could be added as a follow-up.

**16MB threshold**: This is a heuristic starting point. The optimal threshold depends on query selectivity, core count, and I/O speed. It is implemented as a constant (`PARALLEL_THRESHOLD`) that can be tuned via benchmarking and is overriddable with `--threads 1` (force sequential). Future work could make it adaptive.

### 2.6 CLI Surface

A `--threads N` flag with default `0` (auto-detect = number of cores). `--threads 1` forces sequential execution for reproducibility or debugging.

```
logq --table t:elb=access.log --threads 4 \
  "SELECT status, COUNT(*) FROM t GROUP BY status"
```

## Summary

| # | Change | Effort | Expected Impact | Risk |
|---|--------|--------|----------------|------|
| 1 | BufReader 8KB -> 64KB | ~5 lines (incl. benchmark harness) | 10-20% I/O reduction | Near zero |
| 2 | Reusable tokenizer scratch buffers | ~60 lines | Eliminate 1024 allocs/batch | Low |
| 3 | Arena allocation for batch lines | ~100 lines | Eliminate 1024 allocs/batch + cache locality | Low |
| 4 | Memory-mapped I/O + fallback | ~150 lines | Eliminate all read_line copies (parallel path) | Low-medium |
| 5 | Chunk splitting + edge cases | ~80 lines (incl. tests) | Correct parallel workload distribution | Low |
| 6 | Parallel scan dispatch (rayon) | ~300 lines | Near-linear core scaling (3-6x) | Medium |
| 7 | Parallel aggregation merge | ~200 lines | Same scaling for GROUP BY | Medium |
| 8 | Parallel sort-merge for ORDER BY | ~200 lines | Same scaling + LIMIT optimization | Medium |
| 9 | Decision layer + --threads flag | ~100 lines | Correct path selection (stdin, size, mmap) | Low |
| 10 | Tests for parallel paths | ~200 lines | Correctness verification | Low |

**Total**: ~1400 lines of new/modified code.
**New dependencies**: `memmap2`, `rayon`.
**Unchanged**: Parser, AST, desugarer, logical planner, optimizer, function registry, output formatting, JSONL path, JOIN operators.
