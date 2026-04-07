# Large File Throughput Optimization Design

**Date**: 2026-04-06
**Goal**: Maximize logq throughput on multi-GB log files, regardless of query shape
**Inspiration**: Velox's I/O layer, memory management, and multi-Driver execution model

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

**Rationale**: Sequential log file reads benefit from larger buffers. At 8KB, each MB of log data requires ~125 `read()` syscalls at ~1-2us each. At 64KB, that drops to ~16 syscalls. This matches typical OS readahead page sizes and is the sweet spot before diminishing returns.

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

**Risk**: Low. Tokenizer and field parsers already operate on `&[u8]` slices.

## Part 2: Parallel Chunk Scanning

### 2.1 Memory-Mapped I/O

**Change**: Replace `BufReader::read_line()` with `memmap2` for zero-copy file access:

```rust
use memmap2::MmapOptions;

let file = File::open(path)?;
let mmap = unsafe { MmapOptions::new().map(&file)? };
// mmap acts as &[u8] over the entire file
```

Within each chunk, lines are found by scanning for `\n` in the mmap slice. The tokenizer and field parsers already operate on `&[u8]` / `&str`, so they work directly on mmap'd memory without any intermediate copy.

This eliminates the entire `read_line()` -> `Vec<u8>` copy chain. Combined with the arena from Section 1.3, per-line allocation is replaced by direct slices into the mmap.

**Dependency**: `memmap2` — stable, widely used Rust crate (~200 lines). Safe for read-only mappings of files not concurrently modified (logq's use case).

**Risk**: Low-medium. Edge cases: empty files, files with no trailing newline (last chunk may not end with `\n`).

### 2.2 Chunk Splitting

Split the file into N chunks along newline boundaries:

```rust
fn split_chunks(data: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    let chunk_size = data.len() / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for i in 0..num_chunks {
        let raw_end = if i == num_chunks - 1 {
            data.len()
        } else {
            start + chunk_size
        };
        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1,
            None => data.len(),
        };
        chunks.push(&data[start..end]);
        start = end;
    }
    chunks
}
```

Each chunk is a `&[u8]` slice — zero allocation, zero copy. Line-boundary adjustment scans at most one line length (~300 bytes) per chunk boundary. Chunk size target: ~16MB (large enough to amortize thread overhead, small enough for even distribution).

### 2.3 Parallel Execution with Rayon

Each chunk runs the scan+filter pipeline independently on a rayon thread pool.

**Filter/projection queries** (no aggregation, no ordering):

```rust
use rayon::prelude::*;

let results: Vec<Vec<ColumnBatch>> = chunks
    .par_iter()
    .map(|chunk| {
        let mut scanner = BatchScanOperator::from_slice(chunk, schema, predicate);
        let mut batches = Vec::new();
        while let Some(batch) = scanner.next_batch()? {
            batches.push(batch);
        }
        batches
    })
    .collect()?;

// Flatten in chunk order — preserves original file ordering
let all_batches = results.into_iter().flatten();
```

**Aggregation queries** (GROUP BY):

```rust
let partial_aggregates: Vec<HashMap<GroupKey, Accumulators>> = chunks
    .par_iter()
    .map(|chunk| {
        let mut scanner = BatchScanOperator::from_slice(chunk, schema, predicate);
        let mut local_agg = HashMap::new();
        while let Some(batch) = scanner.next_batch()? {
            update_aggregates(&mut local_agg, &batch);
        }
        local_agg
    })
    .collect()?;

let final_agg = partial_aggregates.into_iter().reduce(merge_aggregates);
```

Each thread maintains local accumulators; a single-threaded merge combines them (sum += sum, count += count, min = min(min, min)). For typical GROUP BY cardinalities (hundreds to thousands of groups), the merge is negligible.

**LIMIT queries**: Each thread tracks a local count. A global `AtomicUsize` budget coordinates early termination — when enough rows are found, remaining threads bail out.

**Dependency**: `rayon` — standard Rust parallelism library with work-stealing thread pool.

### 2.4 ORDER BY with Parallel Sort-Merge

**Phase 1 — Parallel local sort**: Each thread scans its chunk, collects matching records, and sorts locally using the existing `prefix_sort.rs` infrastructure.

**Phase 2 — K-way merge**: Merge pre-sorted chunks using a min-heap. O(N log K) where K is number of chunks (typically 4-8).

**With ORDER BY ... LIMIT N**: Each thread keeps only top-N locally (partial sort / selection), reducing memory from O(total_rows) to O(N x K). The merge only pulls N records from the heap.

**Stability**: Chunk index is used as a tiebreaker to preserve original file ordering for equal sort keys.

### 2.5 Activation Criteria

```
File size < 16MB?  --> Sequential (batch path as-is)
                       Overhead of mmap + thread spawn > benefit

File size >= 16MB? --> Parallel chunked scan
  |
  +-- Filter/Project only     -> concat results
  +-- GROUP BY                -> merge partial aggregates
  +-- ORDER BY                -> k-way merge of sorted chunks
  +-- ORDER BY ... LIMIT N    -> partial sort + k-way merge
  +-- JOIN                    -> sequential (right side needs full access)
```

**JOINs stay sequential**: Parallelizing joins requires broadcasting or partitioning both sides by join key — significant complexity for a rare log-query use case.

**JSONL stays sequential**: The batch path already excludes JSONL. Parallel JSONL could be added as a follow-up.

### 2.6 CLI Surface

A `--threads N` flag with default `0` (auto-detect = number of cores). `--threads 1` forces sequential execution for reproducibility or debugging.

```
logq --table t:elb=access.log --threads 4 \
  "SELECT status, COUNT(*) FROM t GROUP BY status"
```

## Summary

| # | Change | Effort | Expected Impact | Risk |
|---|--------|--------|----------------|------|
| 1 | BufReader 8KB -> 64KB | 1 line | 10-20% I/O reduction | Near zero |
| 2 | Reusable tokenizer scratch buffers | ~50 lines | Eliminate 1024 allocs/batch | Low |
| 3 | Arena allocation for batch lines | ~80 lines | Eliminate 1024 allocs/batch + cache locality | Low |
| 4 | Memory-mapped I/O | ~100 lines | Eliminate all read_line copies | Low-medium |
| 5 | Parallel chunk scanning (rayon) | ~200 lines | Near-linear core scaling (3-6x) | Medium |
| 6 | Parallel aggregation merge | ~100 lines | Same scaling for GROUP BY | Medium |
| 7 | Parallel sort-merge for ORDER BY | ~150 lines | Same scaling + LIMIT optimization | Medium |
| 8 | Decision layer + --threads flag | ~60 lines | Correct path selection | Low |

**Total**: ~750 lines of new/modified code.
**New dependencies**: `memmap2`, `rayon`.
**Unchanged**: Parser, AST, desugarer, logical planner, optimizer, function registry, output formatting, JSONL path, JOIN operators.
