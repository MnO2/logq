# SIMD-Accelerated Physical Plan for logq

**Date:** 2026-04-05
**Status:** Draft
**Inspiration:** Velox execution engine SIMD patterns

## Overview

Introduce a hybrid columnar batch execution model to logq's physical plan layer, enabling SIMD acceleration across all hot paths: data source parsing, filtering, projection, and aggregation. The existing Volcano-style operator tree is preserved, but the unit of data flow changes from a single `Record` to a `ColumnBatch` of ~1024 rows in columnar layout.

### Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Execution model | Hybrid columnar batches inside existing operator tree | Gets most SIMD benefit without full engine rewrite |
| SIMD strategy | `std::arch` intrinsics (stable Rust) | No nightly dependency; full control over codegen |
| Architecture support | x86_64 (SSE2, AVX2), aarch64 (NEON), scalar fallback | Covers all practical deployment targets |
| Type system | Schema inference at scan time | Structured formats have known schemas; JSONL inferred |
| Batch size | 1024 rows | 4KB per i32 column fits L1 cache |

## Section 1: Core Data Structures

Three new types replace `Record` as the unit of data flow between operators.

### ColumnBatch

A batch of up to 1024 rows in columnar layout:

```rust
pub struct ColumnBatch {
    columns: Vec<TypedColumn>,   // one per projected column
    names: Vec<String>,          // column names, parallel to `columns`
    selection: SelectionVector,  // which rows are "active"
    len: usize,                  // number of logical rows (before selection)
}
```

### TypedColumn

A typed array with null tracking:

```rust
pub enum TypedColumn {
    Int32(Vec<i32>, BitVec),        // data + null bitmap
    Float32(Vec<f32>, BitVec),
    Boolean(Vec<bool>, BitVec),
    Utf8(Vec<u8>, Vec<u32>, BitVec), // concatenated bytes + offset array + nulls
    DateTime(Vec<i64>, BitVec),      // epoch micros
    Mixed(Vec<Value>, BitVec),       // fallback for heterogeneous columns
}
```

The string representation (concatenated bytes + offsets) mirrors Arrow/Velox, keeping string data contiguous for SIMD scanning. The `Mixed` variant is the escape hatch for JSONL columns that have inconsistent types.

### SelectionVector

Tracks active rows through the pipeline, avoiding data copies when filtering:

```rust
pub enum SelectionVector {
    All,                    // all rows active (no filtering yet)
    Bitmap(BitVec),         // dense bitmask -- good for high selectivity
    Indices(Vec<u32>),      // sparse index list -- good for low selectivity
}
```

Batch size of 1024 balances SIMD utilization against L1 cache residency -- a batch of 1024 `i32` values is 4KB, fitting comfortably in L1.

## Section 2: Batch Execution Model

The `RecordStream` trait is replaced by a `BatchStream` trait. The Volcano pull model stays -- operators still call `.next()` on their children -- but now each call returns a batch instead of a single row.

### New Trait

```rust
pub trait BatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>>;
    fn schema(&self) -> &BatchSchema;  // column names + types, known after first batch
    fn close(&self);
}
```

### Operator Mapping

Each existing stream gets a batch counterpart with `Operator` suffix:

| Current (row) | New (batch) | SIMD opportunity |
|---|---|---|
| `LogFileStream` | `BatchScanOperator` | SIMD tokenization, parallel field parsing |
| `FilterStream` | `BatchFilterOperator` | SIMD predicate evaluation, selection vector update |
| `MapStream` | `BatchProjectOperator` | SIMD expression evaluation (arithmetic, casts) |
| `GroupByStream` | `BatchGroupByOperator` | SIMD hashing, vectorized aggregate accumulators |
| `OrderByStream` | `BatchOrderByOperator` | Vectorized comparison keys, SIMD radix sort |
| `LimitStream` | `BatchLimitOperator` | Trivial -- truncate selection vector |
| `DistinctStream` | `BatchDistinctOperator` | SIMD hash dedup |
| `CrossJoinStream` | `BatchCrossJoinOperator` | Batch-level nested loop |
| `LeftJoinStream` | `BatchLeftJoinOperator` | SIMD hash probe (Velox-style tag matching) |

### Selection Vector Propagation

Filters don't physically remove rows from the batch -- they update the `SelectionVector`. Downstream operators only process active rows. This avoids expensive compaction and is how Velox handles it. Compaction only happens when the active ratio drops below ~25%, at which point a new dense batch is materialized.

### Fallback Path

The `Mixed` column type and any operator that encounters unsupported types falls back to a per-row loop over the batch, calling the existing scalar `Value`-based logic. This means no correctness risk -- SIMD accelerates the common case, scalar handles the rest.

## Section 3: Architecture Dispatch Layer

A thin abstraction that selects the best available instruction set at compile time.

### Module Structure

```
src/simd/
  mod.rs          // public API -- re-exports the best backend
  scalar.rs       // fallback: plain loops, always available
  x86_64_sse2.rs  // SSE2 baseline (128-bit, all x86_64 CPUs)
  x86_64_avx2.rs  // AVX2 fast path (256-bit, most CPUs since ~2015)
  aarch64_neon.rs  // ARM NEON (128-bit, all aarch64 including Apple Silicon)
```

### Dispatch via cfg + target_feature

```rust
// src/simd/mod.rs
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
pub use x86_64_avx2 as backend;

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
pub use x86_64_sse2 as backend;

#[cfg(target_arch = "aarch64")]
pub use aarch64_neon as backend;

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub use scalar as backend;
```

### Backend Trait

Each backend implements the same trait:

```rust
pub trait SimdOps {
    const LANES_I32: usize;  // 4 (SSE2/NEON), 8 (AVX2)
    const LANES_I64: usize;  // 2 (SSE2/NEON), 4 (AVX2)

    fn filter_range_i32(data: &[i32], min: i32, max: i32, out: &mut BitVec);
    fn filter_eq_i32(data: &[i32], value: i32, out: &mut BitVec);
    fn hash_crc32(data: &[i32], seed: u32, out: &mut [u32]);
    fn sum_i32(data: &[i32], selection: &SelectionVector) -> i64;
    fn gather_i32(data: &[i32], indices: &[u32], out: &mut [i32]);
    fn str_contains(haystack: &[u8], offsets: &[u32], needle: &[u8], out: &mut BitVec);
    fn memcmp_batch(a: &[u8], b: &[u8], len: usize) -> bool;
    // ... more kernels added as needed
}
```

The scalar backend implements every function with plain loops, guaranteeing correctness on all platforms. The SIMD backends override with intrinsics wrapped in `unsafe` blocks, each gated by `#[target_feature(enable = "avx2")]` or equivalent.

## Section 4: SIMD Kernels -- Filtering

### BatchFilterOperator Core Loop

```rust
impl BatchStream for BatchFilterOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(mut batch) = self.child.next_batch()? {
            self.predicate.evaluate_batch(&mut batch)?;
            if batch.selection.any_active() {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}
```

### Predicate-to-Kernel Mapping

| Predicate pattern | SIMD kernel | What it does |
|---|---|---|
| `col >= 500` | `filter_range_i32(data, 500, i32::MAX, bitmask)` | Broadcast constant, compare all lanes, write bitmask |
| `col = 'GET'` | `str_eq_batch(bytes, offsets, needle, bitmask)` | SIMD string compare with early-exit on length mismatch |
| `col LIKE '%/api%'` | `str_contains(bytes, offsets, needle, bitmask)` | Velox-style simdStrstr -- broadcast first+last char, scan in SIMD-width blocks |
| `col IN (200, 301, 404)` | `filter_in_i32(data, &hashset, bitmask)` | SIMD hash + gather from lookup table |
| `a AND b` | `bitmask_and(left, right)` | Bitwise AND of two selection bitmasks |
| `a OR b` | `bitmask_or(left, right)` | Bitwise OR |

### Compound Predicate Short-Circuit

Evaluate the left side first, then pass the narrowed `SelectionVector` to the right side. If the left side eliminates 90% of rows, the right side only touches 10%.

### Fallback

If a predicate involves `TypedColumn::Mixed` or an unsupported expression shape (e.g. `function(col) > value`), it falls back to per-row scalar evaluation over the batch.

## Section 5: SIMD Kernels -- Data Source Parsing

### BatchScanOperator Pipeline

```
Read 1024 lines into line buffer
        |
        v
  SIMD Tokenization (find delimiters)
        |
        v
  Typed Field Parsing (populate TypedColumns)
        |
        v
  Emit ColumnBatch
```

### SIMD Tokenization

The current `LogTokenizer` scans bytes one at a time. The SIMD version processes 16/32 bytes at a time:

- Broadcast the delimiter byte (e.g. `' '` for space-delimited formats)
- Compare against a register-width chunk: `_mm256_cmpeq_epi8(chunk, delim)`
- Extract bitmask with `_mm256_movemask_epi8` -- each set bit is a delimiter position
- Walk set bits with `_tzcnt_u32` to extract field boundaries

For quoted fields, a state machine tracks inside/outside quotes, but the delimiter scan between quotes is still SIMD-accelerated.

### Typed Field Parsing

For structured formats (ELB, ALB, S3, Squid), the schema is fixed so each field index maps to a known type:

| Field type | Parsing strategy |
|---|---|
| Integer | SIMD digit detection + scalar `atoi` |
| Float | Scalar `fast_float` crate |
| DateTime | Existing `parse_utc_timestamp` byte-level parser, applied per batch |
| String | Zero-copy: store offset into the line buffer, avoid allocation |
| Host/HttpRequest | Scalar parse into typed struct, store in `Mixed` column |

### Zero-Copy Strings

Instead of allocating a `String` per field per row, the `Utf8` column stores byte offsets into the shared line buffer. The buffer lives as long as the batch.

### JSONL

Uses `simd-json` crate for SIMD-accelerated JSON parsing, then distributes parsed values into typed columns based on the inferred schema.

## Section 6: SIMD Kernels -- Aggregation & Hashing

### BatchGroupByOperator Pipeline

```
Consume all batches from child
        |
        v
  SIMD Hash Computation (per batch)
        |
        v
  Hash Table Probe & Insert (Velox-style tag matching)
        |
        v
  Vectorized Aggregate Update
        |
        v
  Emit result batches
```

### SIMD Hash Computation

- **Integer keys:** Hardware CRC32 via `_mm_crc32_u32` (x86 SSE4.2) or `crc32cw` (ARM). Process 4-8 keys per iteration. For multi-column keys, chain hashes.
- **String keys:** SIMD-accelerated inner hash loop -- process 16/32 bytes per iteration.
- **Combined hash array:** Results written to `Vec<u32>` feeding the hash table probe.

### Hash Table -- Velox-Style Tag Matching

Adopted from Velox's `HashTable.cpp` Swiss Table pattern:

- Separate **tag array** (1 byte per slot, 7-bit hash tag + high bit marker) from **payload array** (group index)
- Probe loads 16 tags at once via `_mm_loadu_si128`, compares against broadcast wanted tag with `_mm_cmpeq_epi8`, extracts bitmask with `_mm_movemask_epi8`
- Each probe checks 16 slots in 3 instructions
- On match, verify the actual key; on empty, insert new group

### Vectorized Aggregate Accumulators

Flat arrays indexed by group ID, replacing `HashMap<key, accumulator>`:

| Aggregate | Storage | SIMD update |
|---|---|---|
| `COUNT` | `Vec<i64>` | Gather group counts, add 1, scatter back |
| `SUM(int)` | `Vec<i64>` | Gather group sums, add values, scatter back |
| `SUM(float)` | `Vec<f64>` | Same, with f64 accumulators |
| `MIN/MAX` | `Vec<Value>` | Gather, SIMD min/max for integers, scalar for others |
| `AVG` | `Vec<(f64, i64)>` | Update sum and count arrays separately |

### Low-Cardinality Fast Path

When the number of groups < 256, group IDs fit in `u8` and accumulator arrays fit in L1 cache, making the gather-update-scatter pattern extremely fast.

## Section 7: SIMD Kernels -- Projection & Expression Evaluation

### BatchProjectOperator Approach

Each `Expression` gets an `evaluate_column` method that returns a `TypedColumn`. Evaluation is bottom-up -- leaf expressions produce columns, operators combine them.

### Arithmetic Example

```rust
fn add_i32(a: &[i32], b: &[i32], out: &mut [i32]) {
    for chunk in 0..a.len() / LANES_I32 {
        let va = _mm256_loadu_si256(a[chunk * 8..]);
        let vb = _mm256_loadu_si256(b[chunk * 8..]);
        let vr = _mm256_add_epi32(va, vb);
        _mm256_storeu_si256(out[chunk * 8..], vr);
    }
    // scalar tail for remaining elements
}
```

### Expression-to-Kernel Mapping

| Expression | SIMD kernel |
|---|---|
| `col + const` | Broadcast constant, SIMD add across column |
| `col * col` | SIMD multiply, pairwise |
| `CAST(col AS FLOAT)` | SIMD int-to-float conversion (`_mm256_cvtepi32_ps`) |
| `UPPER(col)` | SIMD ASCII case flip: OR with `0x20` mask for bytes in `a-z` range |
| `SUBSTRING(col, start, len)` | Compute new offsets array arithmetically, no data copy |
| `Variable(name)` | Direct reference to existing column -- zero cost |
| `Constant(v)` | Broadcast into a column, or pass as scalar to binary ops |

### Selection Vector Awareness

All kernels check the `SelectionVector` before processing. For `All`, the tight SIMD loop runs without branching. For `Bitmap`, the kernel ANDs the null bitmap with the selection bitmap and skips fully-zero 64-bit words. For `Indices`, the kernel uses gather to load only active elements.

### Null Propagation

Follows SQL semantics -- `NULL + 5 = NULL`. Implemented by ORing the null bitmaps of both operands to produce the result's null bitmap. This is a single `bitmask_or` per 64 rows.

### Fallback

Expressions involving `TypedColumn::Mixed`, unported `Function` calls, or `Subquery` nodes fall back to row-at-a-time evaluation by iterating the batch and calling existing `expression_value()`.

## Section 8: Migration Strategy

### Phase 1 -- Foundation (no behavior change)

- Add `src/simd/` module with architecture dispatch and scalar fallback
- Add `ColumnBatch`, `TypedColumn`, `SelectionVector` types in `src/execution/batch.rs`
- Add `BatchStream` trait alongside existing `RecordStream`
- Write unit tests for SIMD kernels against scalar reference implementations

### Phase 2 -- Adapters (enables incremental migration)

```rust
// Converts BatchStream -> RecordStream (for unconverted downstream operators)
struct BatchToRowAdapter { inner: Box<dyn BatchStream> }

// Converts RecordStream -> BatchStream (for unconverted upstream operators)
struct RowToBatchAdapter { inner: Box<dyn RecordStream>, schema: BatchSchema }
```

With adapters, the plan tree becomes:
```
BatchScanOperator -> BatchFilterOperator -> BatchToRowAdapter -> OrderByStream
```

### Phase 3 -- Operators, highest impact first

| Priority | Operator | Rationale |
|---|---|---|
| 1 | `BatchScanOperator` | Entry point, biggest cost center |
| 2 | `BatchFilterOperator` | Second biggest cost center |
| 3 | `BatchProjectOperator` | Completes SELECT-FROM-WHERE path |
| 4 | `BatchGroupByOperator` | Unlocks SIMD hashing + vectorized aggregation |
| 5 | `BatchLimitOperator` | Trivial -- truncate selection vector |
| 6 | `BatchOrderByOperator` | Vectorized sort keys |
| 7 | `BatchDistinctOperator` | Reuses GroupBy's hash table |
| 8 | Join operators | Lowest priority -- log queries rarely join |

### Phase 4 -- Remove adapters

Once all operators are converted, remove `RecordStream`, `Record`, and the adapter layer.

### Performance Targets

- Phase 2 complete (scan + filter): 3-5x on filter-heavy queries
- Phase 3 complete (+ project + groupby): 5-10x on aggregation queries
- Phase 4 complete (full batch): 10x+ on large log files

Benchmarks in `benches/bench_execution.rs` track progress at each phase.
