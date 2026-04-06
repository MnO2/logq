# SIMD-Accelerated Physical Plan for logq (Final)

**Date:** 2026-04-05
**Status:** Final
**Revision:** v3 (final -- addresses round 2 review feedback)
**Inspiration:** Velox execution engine SIMD patterns

## Changes from v2

This revision addresses all critical issues (C1-C3) and incorporates relevant suggestions (S1, S2, S6) from the second design review.

| Issue | Summary | Resolution |
|---|---|---|
| **C1** | `filter_ge_i32` and `sum_i32_selected` kernels with bit-manipulation into shared u64 words will NOT auto-vectorize. Non-contiguous writes, data-dependent shifts, and conditional RMW into shared locations all defeat LLVM's vectorizer. | Rewrote filter kernels to use a two-pass strategy: (1) produce results into a `Vec<u8>` (one byte per row, 0 or 1) which LLVM CAN vectorize (simple comparison to byte store), then (2) pack into u64 bitmap words in a second pass. Rewrote selection-aware aggregation to unpack bitmap into `Vec<u8>` mask, then use branch-free multiply-accumulate `total += (val as i64) * (mask[i] as i64)` which LLVM can vectorize. Updated CI verification section accordingly. |
| **C2** | Host/HttpRequest decomposed typed columns cannot be consumed by `host_name()`, `url_path()` etc. which are function calls in `FunctionRegistry`, not path expressions. Reconstruction would negate the decomposition benefit. | Removed `TypedColumn::Host` and `TypedColumn::HttpRequest` variants entirely. Host and HttpRequest fields use `Mixed` column type and go through scalar per-row evaluation. Documented that SIMD benefit for these fields is limited to the tokenization/scan layer (faster parsing into the batch). Updated impact analysis table: ELB/ALB now show 3 Mixed columns (2 Host + 1 HttpRequest), not 0%. |
| **C3** | Compaction threshold in `BatchFilterOperator` creates tiny batches that defeat batch processing efficiency. Downstream operators receive batches of 1-10 rows instead of 1024. | Removed compaction threshold from non-materializing operators entirely. Only materializing operators compact on consumption. Selection vector propagates through the non-materializing path; the cost of iterating inactive rows via bitmap checks is minimal (16 u64 words for 1024 rows). |
| **S1** | `hash_column_i32` creates a new `AHasher` per element -- expensive initialization, prevents auto-vectorization. | Use a simple multiply-shift integer hash for the inner loop (auto-vectorizable), reserve AHash for string key hashing. |
| **S2** | `hash_combine` multiply-add has weak mixing (if first hash is 0, result is just `n`). | Switched to rotation-xor-multiply pattern: `h = (h.rotate_left(5) ^ n).wrapping_mul(0x517cc1b727220a95)` for better avalanche properties. |
| **S6** | Swiss Table tag-matching bitmask extraction step does not reliably auto-vectorize. | Use `hashbrown` crate (already a dependency at `hashbrown = "0.11"` in Cargo.toml) instead of implementing a custom Swiss Table. |

Other suggestions noted but deferred: S3 (field count verification -- confirmed correct), S4 (Float64 column -- tracked as future work), S5 (simd-json mutable buffer -- noted in JSONL section and Phase 1 task list).

---

## Overview

Introduce a hybrid columnar batch execution model to logq's physical plan layer, enabling SIMD acceleration across all hot paths: data source parsing, filtering, projection, and aggregation. The existing Volcano-style operator tree is preserved, but the unit of data flow changes from a single `Record` to a `ColumnBatch` of up to `BATCH_SIZE` rows in columnar layout.

### Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Execution model | Hybrid columnar batches inside existing operator tree | Gets most SIMD benefit without full engine rewrite |
| SIMD strategy | Auto-vectorization-first, with targeted intrinsics for string search only | Pragmatic for single-developer project; LLVM auto-vectorizes simple loops well; avoids maintaining 4 unsafe backends |
| Architecture support | Compile with `--target-cpu=native` for auto-vectorization; runtime dispatch for optional intrinsics | Covers all deployment targets via scalar fallback; native builds get full benefit |
| Type system | Schema inference at scan time; full Value coverage including NULL/MISSING distinction | Structured formats have known schemas; JSONL inferred; PartiQL NULL/MISSING semantics preserved |
| Batch size | `const BATCH_SIZE: usize = 1024` (compile-time tunable) | 4KB per i32 column fits L1 cache; adjustable for wide schemas |
| Bitmap representation | `Vec<u64>` (manual bit ops, no `bitvec` crate) | SIMD-friendly; 64 rows per word; simpler than bitvec's generic model |
| Arrow crate | Not adopted | Adds ~200KB+ dependency with features we do not need (IPC, nested types, alignment); custom TypedColumn is <500 lines and gives us full control. Revisit if we later need DataFusion interop. |
| Hash function | `ahash` crate for string keys; multiply-shift for integer keys | AHash uses hardware AES-NI where available; multiply-shift is simple and auto-vectorizable for integer columns |
| Hash table | `hashbrown` crate (already a dependency) | Production-quality Swiss Table with SIMD probing; avoids maintaining a custom implementation |
| Host/HttpRequest columns | `Mixed` (scalar per-row evaluation) | These types are accessed via `FunctionRegistry` calls (`host_name()`, `url_path()`, etc.) that expect `Value`, not decomposed arrays. Decomposition would require reconstruction, negating the benefit. |

## Section 1: Core Data Structures

Three new types replace `Record` as the unit of data flow between operators.

### Batch Size Constant

```rust
/// Compile-time tunable batch size. 1024 is optimal for narrow schemas
/// (4KB per i32 column fits L1). Reduce for wide schemas (25+ columns).
pub const BATCH_SIZE: usize = 1024;
```

### Validity Bitmaps

All columns carry two bitmaps to preserve the PartiQL NULL/MISSING distinction:

```rust
/// A bitmap stored as packed u64 words. For BATCH_SIZE=1024, this is 16 words (128 bytes).
/// Bit 1 = present/true, Bit 0 = absent/false.
pub struct Bitmap {
    words: Vec<u64>,  // ceil(BATCH_SIZE / 64) words
}

impl Bitmap {
    fn all_set(len: usize) -> Self { /* all bits = 1 */ }
    fn all_unset(len: usize) -> Self { /* all bits = 0 */ }
    fn is_set(&self, idx: usize) -> bool { /* test single bit */ }
    fn set(&mut self, idx: usize) { /* set single bit */ }
    fn unset(&mut self, idx: usize) { /* clear single bit */ }
    fn and(&self, other: &Bitmap) -> Bitmap { /* word-by-word AND */ }
    fn or(&self, other: &Bitmap) -> Bitmap { /* word-by-word OR */ }
    fn not(&self) -> Bitmap { /* word-by-word NOT */ }
    fn count_ones(&self) -> usize { /* popcount across words */ }
    fn any(&self) -> bool { /* any bit set */ }

    /// Unpack bitmap into a Vec<u8> mask (one byte per row, 0 or 1).
    /// Used by kernels that need a dense mask for auto-vectorizable loops.
    fn unpack_to_bytes(&self, len: usize) -> Vec<u8> {
        let mut mask = vec![0u8; len];
        for i in 0..len {
            let word = i / 64;
            let bit = i % 64;
            mask[i] = ((self.words[word] >> bit) & 1) as u8;
        }
        mask
    }

    /// Pack a Vec<u8> byte mask (one byte per row, 0 or 1) into bitmap words.
    /// Second pass after auto-vectorizable comparison kernels.
    fn pack_from_bytes(mask: &[u8]) -> Self {
        let num_words = (mask.len() + 63) / 64;
        let mut words = vec![0u64; num_words];
        for (i, &byte) in mask.iter().enumerate() {
            let word = i / 64;
            let bit = i % 64;
            words[word] |= (byte as u64) << bit;
        }
        Bitmap { words }
    }
}
```

Using `Vec<u64>` instead of the `bitvec` crate gives us direct word-level access, which is natural for SIMD-width processing (64 rows per word, matching AVX-512's width, or 4 words per AVX2 pass). Bitwise AND/OR/NOT across bitmaps process 64 rows per instruction.

The `unpack_to_bytes` / `pack_from_bytes` methods bridge the bitmap representation with the byte-per-row representation needed for auto-vectorizable kernels (see Section 3 for why this two-pass strategy is necessary).

### ColumnBatch

A batch of up to `BATCH_SIZE` rows in columnar layout:

```rust
pub struct ColumnBatch {
    columns: Vec<TypedColumn>,   // one per projected column
    names: Vec<String>,          // column names, parallel to `columns`
    selection: SelectionVector,  // which rows are "active"
    len: usize,                  // number of logical rows (before selection)
}
```

### TypedColumn

A typed array with separate NULL and MISSING tracking:

```rust
pub enum TypedColumn {
    // --- Primitive types (SIMD-friendly dense arrays) ---
    Int32 {
        data: Vec<i32>,
        null: Bitmap,      // bit=1 means value IS NOT NULL
        missing: Bitmap,   // bit=1 means value IS NOT MISSING
    },
    Float32 {
        data: Vec<f32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Boolean {
        data: Bitmap,      // packed booleans
        null: Bitmap,
        missing: Bitmap,
    },
    Utf8 {
        data: Vec<u8>,     // concatenated string bytes (owned buffer)
        offsets: Vec<u32>, // offsets[i]..offsets[i+1] = string i
        null: Bitmap,
        missing: Bitmap,
    },
    DateTime {
        data: Vec<i64>,    // epoch micros
        null: Bitmap,
        missing: Bitmap,
    },

    // --- Fallback for heterogeneous/complex/domain-specific data ---
    Mixed {
        data: Vec<Value>,  // handles Object, Array, Host, HttpRequest, and heterogeneous columns
        null: Bitmap,
        missing: Bitmap,
    },
}
```

**NULL vs MISSING semantics.** For any row `i`:
- If `missing` bit `i` is 0: the value is MISSING (field does not exist). The `null` bit and `data[i]` are don't-care.
- If `missing` bit `i` is 1 and `null` bit `i` is 0: the value is NULL (field exists but has no value).
- If `missing` bit `i` is 1 and `null` bit `i` is 1: the value is present in `data[i]`.

This two-bitmap encoding adds negligible overhead (128 bytes per bitmap at BATCH_SIZE=1024) and preserves the PartiQL `IS NULL` vs `IS MISSING` distinction that the codebase relies on in `Formula::evaluate`, `FunctionRegistry::call`, and IS NULL/IS MISSING predicates.

**Host and HttpRequest columns use `Mixed`.** These are domain-specific types (`Value::Host`, `Value::HttpRequest`) used in ELB/ALB logs. Each ELB log has 2 Host fields and 1 HttpRequest field; ALB has the same.

These types are accessed exclusively through function calls registered in `FunctionRegistry`:
- `host_name(col)` and `host_port(col)` in `src/functions/host.rs` -- pattern match on `Value::Host`
- `url_host(col)`, `url_port(col)`, `url_path(col)`, `url_query(col)`, `url_fragment(col)`, `url_path_segments(col, idx)`, `url_path_bucket(col, idx, target)` in `src/functions/url.rs` -- pattern match on `Value::HttpRequest` and call `r.parsed_url()`

Decomposing these into flat arrays (as attempted in v2) would require reconstructing a `Value::Host` or `Value::HttpRequest` whenever the expression evaluator encounters a function call like `Function("url_path", [Variable("request")])`. This reconstruction negates the decomposition benefit. Path expressions like `request.url` also do not work on these types because `get_value_by_path_expr` (in `src/common/types.rs`) only supports `Value::Object` for attribute navigation, not `Value::HttpRequest`.

The SIMD benefit for Host/HttpRequest fields is limited to the **tokenization/scan layer**: SIMD delimiter scanning in `BatchScanOperator` accelerates the parsing of these fields from raw log lines into `Value::Host` / `Value::HttpRequest`, even though subsequent expression evaluation on these values remains scalar per-row. This is still valuable because tokenization is the single largest cost center (see Section 5).

**Object and Array in Mixed.** `Value::Object` and `Value::Array` (used by JSONL) also use the `Mixed` fallback column. Impact analysis:

| Format | Fields hitting Mixed | Total fields | Mixed % | Impact |
|---|---|---|---|---|
| ELB | 3 (2 Host + 1 HttpRequest) | 17 | 18% | SIMD for 14/17 fields; Host/HttpRequest get SIMD tokenization only |
| ALB | 3 (2 Host + 1 HttpRequest) | 25 | 12% | SIMD for 22/25 fields; Host/HttpRequest get SIMD tokenization only |
| S3 | 0 (all String) | 24 | 0% | Full SIMD benefit |
| Squid | 0 (all String) | 10 | 0% | Full SIMD benefit |
| JSONL (flat) | 0 (inferred as Int32/Float32/Utf8/Boolean per key) | varies | 0% | Full SIMD benefit |
| JSONL (nested) | only nested Object/Array fields | varies | varies | Partial -- top-level primitives get SIMD, nested structures fall back |

For JSONL with nested objects, the `Mixed` column is used only for fields that actually contain Object or Array values. Top-level primitive fields (strings, numbers, booleans) are still placed into typed columns via schema inference. This means even JSONL queries get SIMD benefit for the common case of filtering/aggregating on primitive fields.

The 12-18% Mixed ratio for ELB/ALB is acceptable because: (a) Host/HttpRequest expression evaluation is rarely the bottleneck -- it occurs only when the query uses `host_name()`, `url_path()`, etc.; (b) the SIMD tokenization benefit applies to all fields including Host/HttpRequest; (c) the most common filter patterns (status code, byte counts, timestamps, user agents) operate on typed columns.

### SelectionVector

Tracks active rows through the pipeline, avoiding data copies when filtering:

```rust
pub enum SelectionVector {
    All,                    // all rows active (no filtering yet)
    Bitmap(Bitmap),         // dense bitmask -- good for high selectivity
    Indices(Vec<u32>),      // sparse index list -- good for low selectivity
}

impl SelectionVector {
    /// Returns true if any rows are active.
    fn any_active(&self) -> bool;

    /// Count of active rows.
    fn count_active(&self, total: usize) -> usize;

    /// Convert to Bitmap form (for operators that need it).
    fn to_bitmap(&self, total: usize) -> Bitmap;

    /// Compact a batch: produce dense output with only active rows.
    /// Used by materializing operators on consumption.
    fn compact(&self, batch: &ColumnBatch) -> ColumnBatch;
}
```

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

Each existing stream gets a batch counterpart. The table below also specifies each operator's selection vector behavior:

| Current (row) | New (batch) | SIMD opportunity | SelectionVector output |
|---|---|---|---|
| `LogFileStream` | `BatchScanOperator` | SIMD tokenization, parallel field parsing | Always `All` (freshly scanned data) |
| `FilterStream` | `BatchFilterOperator` | SIMD predicate evaluation | Narrows: produces `Bitmap` or `Indices` |
| `MapStream` | `BatchProjectOperator` | SIMD expression evaluation (arithmetic, casts) | Propagates input selection |
| `GroupByStream` | `BatchGroupByOperator` | SIMD hashing, vectorized aggregate accumulators | **Materializing:** compacts input, emits `All` |
| `OrderByStream` | `BatchOrderByOperator` | Vectorized comparison keys | **Materializing:** compacts input, emits `All` |
| `LimitStream` | `BatchLimitOperator` | Trivial -- truncate selection vector | Narrows: truncates existing selection |
| `DistinctStream` | `BatchDistinctOperator` | SIMD hash dedup | **Materializing:** compacts input, emits `All` |
| `CrossJoinStream` | `BatchCrossJoinOperator` | Batch-level nested loop | **Materializing:** emits `All` |
| `LeftJoinStream` | `BatchLeftJoinOperator` | SIMD hash probe (tag matching) | **Materializing:** emits `All` |
| `UnionStream` | `BatchUnionOperator` | Pass-through | Propagates child selection |
| `IntersectStream` | `BatchIntersectOperator` | SIMD hash dedup | **Materializing:** compacts right side, emits `All` |
| `ExceptStream` | `BatchExceptOperator` | SIMD hash dedup | **Materializing:** compacts right side, emits `All` |

### Selection Vector Propagation and Compaction

**Non-materializing operators** (Filter, Project, Limit, Union) propagate or narrow the selection vector without touching the underlying column data. They never compact. Downstream operators only process active rows. This avoids expensive compaction and is how Velox handles it.

The cost of propagating a sparse selection vector is minimal: for `BATCH_SIZE=1024`, the bitmap is 16 u64 words (128 bytes). Even if a filter eliminates 99% of rows, iterating the bitmap to find active rows requires checking only 16 words. This is far cheaper than compacting the batch (copying all column data for the surviving rows into new vectors) and avoids the problem of creating tiny batches that defeat SIMD efficiency.

**Materializing operators** (GroupBy, OrderBy, Distinct, Join, set operations) must read all their input before producing output. These operators:
1. Compact their input batches on consumption -- apply the selection vector to produce dense active-rows-only data before inserting into their internal data structures (hash tables, sort buffers).
2. Emit output batches with `SelectionVector::All` -- output is always fully materialized.

Compaction happens inside the materializing operator, not in the upstream operator. This ensures that the non-materializing pipeline always passes full-sized batches with bitmap selection, and only the operator that actually needs dense data pays the compaction cost.

### Fallback Path

The `Mixed` column type and any operator that encounters unsupported types falls back to a per-row loop over the batch, calling the existing scalar `Value`-based logic. This means no correctness risk -- SIMD accelerates the common case, scalar handles the rest.

## Section 3: SIMD Strategy -- Auto-Vectorization First

The previous design proposed writing raw `std::arch` intrinsics across 4 backends (SSE2, AVX2, NEON, scalar). That approach is impractical for a single-developer project: roughly 40 unsafe function implementations, each needing independent testing across architectures.

Instead, we adopt a layered approach:

### Layer 1: Auto-Vectorization (all kernels)

Write kernels as simple, tight Rust loops that LLVM can auto-vectorize. Compile with `--target-cpu=native` (for local builds) or a reasonable baseline like `x86-64-v3` (AVX2-capable) for distributed binaries.

**Critical design constraint: separate comparison from bit-packing.** LLVM's auto-vectorizer works well on loops that read from contiguous arrays and write to contiguous arrays with one element per output slot. It does NOT vectorize loops that pack results into shared u64 bitmap words (non-contiguous writes, data-dependent shifts, conditional RMW into shared locations). Therefore, all filter/comparison kernels use a **two-pass strategy**:

1. **Pass 1 (auto-vectorizable):** Compare elements and produce a `Vec<u8>` result array (one byte per row, value 0 or 1). LLVM vectorizes this into SIMD comparison + byte store.
2. **Pass 2 (scalar):** Pack the byte array into bitmap `Vec<u64>` words via `Bitmap::pack_from_bytes()`. This is a fast scalar loop over the byte results.

Similarly, selection-aware aggregation kernels unpack the bitmap into a byte mask first, then use a branch-free multiply-accumulate loop that LLVM can vectorize.

```rust
/// Pass 1: LLVM auto-vectorizes this into SIMD compare + byte store.
/// Each iteration reads one i32, compares, and writes one u8 -- contiguous
/// read-write pattern that LLVM handles well.
pub fn filter_ge_i32(data: &[i32], threshold: i32, result_bytes: &mut [u8]) {
    for i in 0..data.len() {
        result_bytes[i] = (data[i] >= threshold) as u8;
    }
}

/// Pass 2: Pack byte mask into bitmap. This is scalar but fast
/// (1024 rows = 1024 iterations, each doing a shift+OR into 16 words).
/// Called after the auto-vectorized comparison pass.
// See Bitmap::pack_from_bytes() above.

/// Full filter pipeline:
pub fn filter_ge_i32_to_bitmap(data: &[i32], threshold: i32) -> Bitmap {
    let mut bytes = vec![0u8; data.len()];
    filter_ge_i32(data, threshold, &mut bytes);
    Bitmap::pack_from_bytes(&bytes)
}

/// Sum with selection vector awareness.
/// Pass 1: unpack bitmap to byte mask.
/// Pass 2: branch-free multiply-accumulate that LLVM auto-vectorizes.
pub fn sum_i32_selected(data: &[i32], selection: &Bitmap) -> i64 {
    let mask = selection.unpack_to_bytes(data.len());
    let mut total: i64 = 0;
    for i in 0..data.len() {
        // Branch-free: mask[i] is 0 or 1, so this is a conditional add
        // without a branch. LLVM vectorizes this into SIMD multiply + add.
        total += (data[i] as i64) * (mask[i] as i64);
    }
    total
}
```

**Why the two-pass approach is efficient despite the extra pass.** The byte array for 1024 rows is 1KB -- well within L1 cache. Pass 1 (comparison) and pass 2 (bit-packing) both operate on this hot buffer. The cost of the extra pass is negligible compared to the SIMD speedup gained in pass 1. In benchmarks of similar patterns, the two-pass approach matches or exceeds hand-written intrinsics for comparison kernels because the auto-vectorized comparison loop achieves full SIMD width without any unsafe code.

Kernels that operate on already-packed bitmaps (AND, OR, NOT, popcount) remain single-pass because they operate word-by-word on `Vec<u64>` -- these are simple element-wise loops that LLVM auto-vectorizes directly.

### CI Verification of Auto-Vectorization

To verify auto-vectorization, add a CI step that compiles kernels with `--emit=asm` and checks for vector instructions. The check targets the specific kernels that must vectorize:

```bash
# Compile kernels module to assembly
RUSTFLAGS="--emit=asm -C target-cpu=x86-64-v3" cargo build --release --lib 2>/dev/null

# Verify filter kernels contain SIMD comparison instructions
# filter_ge_i32 should produce vpcmpgtd or vpcmpd (AVX2 i32 compare)
grep -c 'vpcmp' target/release/deps/logq-*.s | grep -v ':0$'

# Verify sum_i32_selected contains SIMD multiply-add
# The multiply-accumulate loop should produce vpmulld/vpaddq or similar
grep -c 'vpmull\|vpmadd\|vpadd' target/release/deps/logq-*.s | grep -v ':0$'

# Verify bitmap AND/OR/NOT contain SIMD bitwise ops
grep -c 'vpand\|vpor\|vpxor' target/release/deps/logq-*.s | grep -v ':0$'
```

This is a one-time verification that is re-run when kernel code changes. If any kernel fails to produce vector instructions, it must be rewritten or moved to the Layer 2 (targeted intrinsics) approach before merging.

### Layer 2: Targeted `#[target_feature]` Functions (string search only)

The one kernel that does not auto-vectorize well is string substring search (`str_contains` / LIKE '%pattern%'). Variable-length strings with offset arrays, boundary checking across SIMD lanes, and the first+last character broadcast trick require manual SIMD.

For this kernel only, we use `#[target_feature]` annotations with runtime dispatch:

```rust
/// Runtime dispatch: check CPU features once at startup, store function pointer.
pub struct SimdStringKernels {
    str_contains_fn: fn(&[u8], &[u32], &[u8], &mut [u8]),  // result is byte-per-row
}

impl SimdStringKernels {
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return Self { str_contains_fn: str_contains_avx2 };
            }
            // SSE2 is baseline for all x86_64
            return Self { str_contains_fn: str_contains_sse2 };
        }
        #[cfg(target_arch = "aarch64")]
        {
            // NEON is baseline for all aarch64
            return Self { str_contains_fn: str_contains_neon };
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            return Self { str_contains_fn: str_contains_scalar };
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn str_contains_avx2(
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u8]
) {
    // AVX2 implementation: broadcast first+last needle byte,
    // scan 32-byte chunks, extract match bitmask.
    // Boundary-safe: each string bounded by offsets[i]..offsets[i+1].
    // Result written as one byte per row (0 or 1), then caller packs to bitmap.
    // ...
}

/// Scalar fallback: always correct, works everywhere.
fn str_contains_scalar(
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u8]
) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let hay = &haystack[start..end];
        result[i] = hay.windows(needle.len()).any(|w| w == needle) as u8;
    }
}
```

**Runtime dispatch** uses `is_x86_feature_detected!()` at startup (called once, stored in a function pointer). This fixes the compile-time-only dispatch problem from v1: a binary compiled on a basic x86_64 machine will still use AVX2 at runtime if the CPU supports it, and will not crash on older CPUs.

Note: the string search kernels also produce byte-per-row results (not bitmap), consistent with the two-pass strategy. The caller packs to bitmap via `Bitmap::pack_from_bytes()`.

### Module Structure

```
src/simd/
  mod.rs            // SimdStringKernels::detect(), re-exports
  kernels.rs        // Auto-vectorizable kernels (filter, sum, hash, bitmap ops)
  string_search.rs  // Targeted intrinsics for str_contains (per-arch)
```

This is 3 files instead of the original 5, with only `string_search.rs` containing `unsafe` code.

### What We Do NOT Build

- No `SimdOps` trait with 10+ methods across 4 backends
- No SSE2 vs AVX2 vs NEON vs scalar duplication for arithmetic/comparison/aggregation kernels (LLVM handles this)
- No `_mm_crc32_u32` usage (this was an SSE4.2 instruction incorrectly attributed to SSE2 in v1; we use AHash for strings and multiply-shift for integers)
- No custom Swiss Table implementation (use `hashbrown` crate)
- No `TypedColumn::Host` or `TypedColumn::HttpRequest` variants (these types use `Mixed`)

## Section 4: SIMD Kernels -- Filtering

### BatchFilterOperator Core Loop

```rust
impl BatchStream for BatchFilterOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(mut batch) = self.child.next_batch()? {
            self.predicate.evaluate_batch(&mut batch)?;
            if batch.selection.any_active() {
                // No compaction -- pass the selection vector through.
                // Downstream non-materializing operators respect the selection.
                // Materializing operators compact on consumption.
                return Ok(Some(batch));
            }
            // All rows filtered out -- skip this batch, pull next.
        }
        Ok(None)
    }
}
```

Note the absence of any compaction threshold. The filter operator only narrows the selection vector. This avoids creating tiny batches that would defeat SIMD efficiency in downstream operators. The cost of carrying inactive rows through the pipeline is minimal: checking 16 bitmap words (128 bytes) per batch is negligible compared to the overhead of compaction (allocating new vectors, copying surviving rows, and then processing undersized batches).

### Predicate-to-Kernel Mapping

| Predicate pattern | Kernel | Notes |
|---|---|---|
| `received_bytes >= 1000` | `filter_ge_i32(data, 1000, result_bytes)` then `pack_from_bytes` | Two-pass: auto-vectorized comparison to bytes, then pack to bitmap |
| `elb_status_code = '200'` | `str_eq_batch(bytes, offsets, needle, result_bytes)` then pack | String equality on status codes (which are `DataType::String` in the schema) |
| `request LIKE '%/api%'` | `str_contains(bytes, offsets, needle, result_bytes)` via `SimdStringKernels` then pack | Runtime-dispatched SIMD string search on request field (Mixed column, falls back to per-row for function-based access like `url_path(request)`) |
| `col IN (200, 301, 404)` | `filter_in_i32(data, &hashset, result_bytes)` then pack | Auto-vectorized hash + lookup |
| `a AND b` | `bitmap_and(left, right)` | Word-by-word u64 AND; auto-vectorized (already in bitmap form) |
| `a OR b` | `bitmap_or(left, right)` | Word-by-word u64 OR |
| `IS NULL` | Check: `missing` bit=1 AND `null` bit=0 | `and(missing, not(null))` -- three bitmap ops |
| `IS MISSING` | Check: `missing` bit=0 | `not(missing)` -- single bitmap op |
| `IS NOT NULL AND IS NOT MISSING` | Check: `missing` bit=1 AND `null` bit=1 | `and(missing, null)` |

**ELB/ALB field type breakdown.** The primary SIMD benefit for structured log formats comes from string operations and tokenization, not integer filtering. Here is the actual field type distribution:

| Format | String fields | Float fields | Int fields | DateTime fields | Host fields (Mixed) | HttpRequest fields (Mixed) | Total |
|---|---|---|---|---|---|---|---|
| ELB | 8 | 3 | 2 | 1 | 2 | 1 | 17 |
| ALB | 15 | 3 | 2 | 1 | 2 | 1 | 25* |
| S3 | 24 | 0 | 0 | 0 | 0 | 0 | 24 |
| Squid | 10 | 0 | 0 | 0 | 0 | 0 | 10 |

*ALB note: `matched_rule_priority` and `request_creation_time` are schema-typed as String even though they contain numeric/datetime-like content.

This means the most impactful SIMD kernels are (in priority order):
1. **Tokenization** -- SIMD delimiter scanning in `BatchScanOperator` (applies to every field of every row, including Host/HttpRequest fields)
2. **String equality/substring** -- `str_eq_batch`, `str_contains` (covers status codes, method names, URLs, user agents)
3. **Float comparison** -- processing time filters (3 float fields in ELB/ALB)
4. **Integer comparison** -- byte count filters (2 integer fields in ELB/ALB)

Host/HttpRequest expression evaluation (via `host_name()`, `url_path()`, etc.) remains scalar per-row through the `Mixed` fallback path. This is acceptable because these function calls are not the typical bottleneck -- they occur only when the query explicitly uses these functions, and the tokenization acceleration still applies.

### Compound Predicate Short-Circuit

Evaluate the left side first, then pass the narrowed `SelectionVector` to the right side. If the left side eliminates 90% of rows, the right side only touches 10%.

### Fallback

If a predicate involves `TypedColumn::Mixed` or an unsupported expression shape (e.g. `function(col) > value`), it falls back to per-row scalar evaluation over the batch. This includes all Host/HttpRequest function-based predicates like `WHERE url_path(request) = '/api'` or `WHERE host_name(client_and_port) = 'example.com'`.

## Section 5: SIMD Kernels -- Data Source Parsing

### BatchScanOperator Pipeline

```
Read BATCH_SIZE lines into line buffer
        |
        v
  SIMD Tokenization (find delimiters)
        |
        v
  Typed Field Parsing (populate TypedColumns)
        |
        v
  Emit ColumnBatch with SelectionVector::All
```

### SIMD Tokenization

The current `LogTokenizer` (datasource.rs, lines 93-165) scans bytes one at a time with a match on individual bytes. The SIMD-friendly version processes 16/32 bytes at a time using auto-vectorizable patterns:

```rust
/// Find all positions of `delim` in `input`. LLVM auto-vectorizes
/// the comparison loop into SIMD when compiled with appropriate target features.
fn find_delimiters(input: &[u8], delim: u8) -> Vec<usize> {
    let mut positions = Vec::new();
    for (i, &b) in input.iter().enumerate() {
        if b == delim {
            positions.push(i);
        }
    }
    positions
}
```

For quoted fields, a state machine tracks inside/outside quotes. The delimiter scan between quoted regions is still vectorizable.

This is where Host and HttpRequest fields get their SIMD benefit: the delimiter scanning that splits the raw log line into fields (including the Host and HttpRequest fields) is accelerated. The subsequent parsing of the field content into `Value::Host` / `Value::HttpRequest` remains scalar, but it operates on already-extracted field bytes.

### Typed Field Parsing

For structured formats (ELB, ALB, S3, Squid), the schema is fixed so each field index maps to a known type:

| Field type | Parsing strategy | TypedColumn target |
|---|---|---|
| Integer | Scalar `str::parse::<i32>()` (already fast; auto-vectorization of atoi is unreliable) | `Int32` |
| Float | Scalar `str::parse::<f32>()` or `fast_float` crate | `Float32` |
| DateTime | Existing `parse_utc_timestamp` byte-level parser (already optimized, confirmed by reviewer) | `DateTime` |
| String | Copy bytes into owned `Vec<u8>` buffer, record offset | `Utf8` |
| Host | Scalar parse via `common::types::parse_host`, store as `Value::Host` | `Mixed` |
| HttpRequest | Scalar parse via `common::types::parse_http_request`, store as `Value::HttpRequest` | `Mixed` |

### String Storage (Arrow-Style Owned Buffer)

Each `Utf8` column owns its string data. There are no references to external line buffers:

```rust
// Building a Utf8 column during scan:
let mut data: Vec<u8> = Vec::with_capacity(BATCH_SIZE * 32); // estimate
let mut offsets: Vec<u32> = Vec::with_capacity(BATCH_SIZE + 1);
offsets.push(0);

for row in 0..batch_len {
    let field_bytes: &[u8] = /* extracted from tokenizer */;
    data.extend_from_slice(field_bytes);
    offsets.push(data.len() as u32);
}

// The Utf8 column now owns all string data. No lifetime issues.
// data = [bytes_of_row0 | bytes_of_row1 | ... | bytes_of_rowN]
// offsets = [0, len0, len0+len1, ..., total_len]
```

This is the same approach Arrow uses (`Buffer` + offsets). It allocates once per batch instead of once per field per row, which is a significant reduction in allocation overhead (from `BATCH_SIZE * num_string_fields` allocations to `num_string_fields` allocations per batch). The data is contiguous for cache-friendly SIMD string scanning.

### JSONL

Uses `simd-json` crate for SIMD-accelerated JSON parsing, then distributes parsed values into typed columns based on the inferred schema. Note: `simd-json` requires mutable access to the input buffer (it modifies the input in-place for zero-copy parsing), so the JSONL scan path will pass owned/mutable `Vec<u8>` buffers instead of borrowed `&str`. The current path in `read_record` uses `json::parse(&self.buf)` which borrows immutably; this needs to change to `simd_json::to_borrowed_value(buf.as_mut_bytes())` or similar. This is tracked as a concrete task in Phase 1.

Schema inference runs on the first batch:
- Top-level primitive fields (string, number, boolean, null) are assigned typed columns (`Utf8`, `Int32`/`Float32`, `Boolean`).
- Top-level Object/Array fields are assigned `Mixed` columns.
- Subsequent batches follow the inferred schema; type mismatches demote a column to `Mixed`.

## Section 6: SIMD Kernels -- Aggregation & Hashing

### BatchGroupByOperator Pipeline

```
Consume all batches from child
        |
        v
  Compact active rows (apply SelectionVector)
        |
        v
  Hash Computation (per compacted batch)
        |
        v
  Hash Table Probe & Insert (via hashbrown)
        |
        v
  Vectorized Aggregate Update
        |
        v
  Emit result batches with SelectionVector::All
```

The operator compacts its input (applies the selection vector to produce dense data) before hashing. Output batches always have `SelectionVector::All`.

### Hash Computation -- Integer Keys (Multiply-Shift)

For integer columns, use a simple multiply-shift hash that LLVM can auto-vectorize:

```rust
/// Multiply-shift hash for integer keys. LLVM auto-vectorizes this into
/// SIMD multiply instructions (vpmulld on AVX2, mul on NEON).
/// The constant is a large odd number with good bit-mixing properties.
const HASH_MULT: u64 = 0x517cc1b727220a95;

fn hash_column_i32(data: &[i32], hashes: &mut [u64]) {
    for i in 0..data.len() {
        // Multiply-shift: fast, auto-vectorizable, good distribution for
        // integer keys in hash tables. Not cryptographic, but we don't need that.
        hashes[i] = (data[i] as u64).wrapping_mul(HASH_MULT);
    }
}
```

This is dramatically faster than creating a new `AHasher` per element (which involves opaque function calls that prevent auto-vectorization). The multiply-shift hash has good distribution properties for integer keys in hash table probing.

### Hash Computation -- String Keys (AHash)

For string columns, use `ahash` via `hashbrown`'s built-in hasher (since we use `hashbrown` for the hash table anyway). String hashing cannot be auto-vectorized due to variable-length data, so the per-element AHash overhead is acceptable -- it is amortized over the string bytes.

```rust
use ahash::AHasher;
use std::hash::Hasher;

fn hash_column_utf8(data: &[u8], offsets: &[u32], hashes: &mut [u64]) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let mut hasher = AHasher::default();
        hasher.write(&data[start..end]);
        hashes[i] = hasher.finish();
    }
}
```

### Hash Combination for Multi-Column Keys

When GROUP BY has multiple columns, combine per-column hashes with a rotation-xor-multiply pattern that provides good avalanche properties:

```rust
fn hash_combine(existing: &mut [u64], new: &[u64]) {
    for (h, &n) in existing.iter_mut().zip(new.iter()) {
        // Rotation-xor-multiply: better mixing than plain multiply-add.
        // The rotation ensures that if `h` is 0, the result is not just `n`
        // (rotate_left(5) of 0 is still 0, but the XOR with `n` and subsequent
        // multiply produces good distribution). The constant is the same
        // golden-ratio-derived odd number used elsewhere.
        *h = (h.rotate_left(5) ^ n).wrapping_mul(0x517cc1b727220a95);
    }
}
```

This is an improvement over the plain multiply-add from v2: the rotation ensures that the existing hash bits are spread across different positions before XOR, providing better avalanche properties. If the first column's hash is 0, `(0u64.rotate_left(5) ^ n) = n`, and the multiply still produces a well-distributed result. For non-zero `h`, the rotation breaks the commutativity that plain XOR would have.

### Hash Table -- hashbrown

Instead of implementing a custom Swiss Table (whose tag-matching bitmask extraction does not reliably auto-vectorize as claimed in v2), we use the `hashbrown` crate which is already a dependency (`hashbrown = "0.11"` in `Cargo.toml`, used in `execution/types.rs` and `logical/parser.rs`).

`hashbrown` is Rust's port of Google's Swiss Table (Abseil `flat_hash_map`). It uses SSE2/NEON intrinsics internally for the 16-byte tag group probing, achieving the same performance as a hand-written implementation without any unsafe code in our codebase.

```rust
use hashbrown::HashMap;

struct GroupByState {
    /// Maps group key -> group index in the accumulator arrays.
    /// hashbrown handles SIMD probing internally.
    group_map: HashMap<GroupKey, u32>,

    /// Flat accumulator arrays indexed by group ID.
    accumulators: Vec<AccumulatorArray>,

    /// Number of groups seen so far.
    num_groups: u32,
}

/// Group key is a vector of Values for multi-column GROUP BY.
/// For single-column integer GROUP BY, we can use a simpler i32/i64 key.
#[derive(Hash, PartialEq, Eq)]
enum GroupKey {
    Single(Value),
    Multi(Vec<Value>),
}
```

### Vectorized Aggregate Accumulators

Flat arrays indexed by group ID, replacing the nested `HashMap<key, accumulator>` pattern:

| Aggregate | Storage | Update strategy |
|---|---|---|
| `COUNT` | `Vec<i64>` | Increment group count by 1 per active row |
| `SUM(int)` | `Vec<i64>` | Add value to group sum |
| `SUM(float)` | `Vec<f64>` | Add value to group sum (f64 accumulator for precision) |
| `MIN/MAX(int)` | `Vec<i64>` | Conditional update per group |
| `MIN/MAX(float)` | `Vec<f64>` | Conditional update per group |
| `MIN/MAX(other)` | `Vec<Value>` | Scalar comparison |
| `AVG` | `Vec<(f64, i64)>` | Update sum and count arrays separately |

### Low-Cardinality Fast Path

When the number of groups < 256, group IDs fit in `u8` and accumulator arrays fit in L1 cache, making the update pattern extremely fast.

## Section 7: SIMD Kernels -- Projection & Expression Evaluation

### BatchProjectOperator Approach

Each `Expression` gets an `evaluate_column` method that returns a `TypedColumn`. Evaluation is bottom-up -- leaf expressions produce columns, operators combine them.

The `BatchProjectOperator` propagates the input selection vector without compaction. It evaluates expressions for all rows and lets the selection vector mask the results downstream (simpler, often faster due to branch-free SIMD).

### Arithmetic Kernels (Auto-Vectorized)

```rust
/// LLVM auto-vectorizes this into vpaddd (AVX2) or add v (NEON).
fn add_i32(a: &[i32], b: &[i32], out: &mut [i32]) {
    for i in 0..a.len() {
        out[i] = a[i] + b[i];
    }
}
```

No `unsafe`, no `_mm256_*` intrinsics. LLVM generates the same vector instructions when compiled with `--target-cpu=native` or `-C target-feature=+avx2`.

### Expression-to-Kernel Mapping

| Expression | Kernel | Notes |
|---|---|---|
| `col + const` | `add_i32_scalar(col, c, out)` | Auto-vectorized broadcast + add |
| `col * col` | `mul_i32(a, b, out)` | Auto-vectorized pairwise multiply |
| `CAST(col AS FLOAT)` | `cast_i32_to_f32(col, out)` | Auto-vectorized int-to-float conversion |
| `UPPER(col)` | `upper_ascii(bytes, out)` | Auto-vectorized: `if b'a' <= b && b <= b'z' { b - 32 }` |
| `SUBSTRING(col, start, len)` | Compute new offsets array arithmetically | No data copy needed |
| `Variable(name)` | Direct reference to existing column | Zero cost |
| `Constant(v)` | Broadcast into a column, or pass as scalar to binary ops | |
| `Function("host_name", [col])` | Per-row scalar via `Mixed` fallback | Cannot vectorize: requires `Value::Host` pattern match |
| `Function("url_path", [col])` | Per-row scalar via `Mixed` fallback | Cannot vectorize: requires `Value::HttpRequest` + URL re-parse |

### Null and Missing Propagation

Follows PartiQL semantics:
- **Arithmetic/comparison with NULL:** `NULL + 5 = NULL`. Implemented by ANDing the null bitmaps of both operands: `result.null = a.null AND b.null` (if either is null, result is null). This is a single word-by-word `AND` per 64 rows.
- **Arithmetic/comparison with MISSING:** `MISSING + 5 = MISSING`. Implemented similarly: `result.missing = a.missing AND b.missing`.
- **Combined:** `result_valid = a.missing AND b.missing AND a.null AND b.null`. Three bitmap operations per 64 rows.

### Fallback

Expressions involving `TypedColumn::Mixed`, unported `Function` calls, or `Subquery` nodes fall back to row-at-a-time evaluation by iterating the batch and calling existing `expression_value()`. This includes all Host/HttpRequest function calls (`host_name`, `host_port`, `url_host`, `url_port`, `url_path`, `url_query`, `url_fragment`, `url_path_segments`, `url_path_bucket`).

## Section 8: Migration Strategy

### Phase 0 -- Profiling (no code changes)

Before writing any SIMD code, profile the current execution engine to identify where time is actually spent. The existing benchmark infrastructure (`benches/bench_execution.rs`) provides the framework.

**Profiling tasks:**
1. Run tier-A benchmarks with `cargo flamegraph` on representative ELB, ALB, and JSONL workloads.
2. Measure time distribution across: I/O, tokenization, field parsing, expression evaluation, aggregation, output formatting.
3. Record baseline numbers for the benchmark queries.

**Expected outcome:** Quantified breakdown of where time is spent. The reviewer correctly noted that if parsing takes 80% of execution time, even infinitely fast expression evaluation yields only 5x. This profiling step grounds our performance targets.

**Gate:** Phase 1 begins only after profiling data is recorded. Performance targets in Phase 3 are adjusted based on measured bottleneck distribution.

### Phase 1 -- Foundation (no behavior change)

- Add `src/simd/` module with auto-vectorizable kernels (two-pass filter/sum pattern) and runtime-dispatched string search
- Add `ColumnBatch`, `TypedColumn` (with NULL + MISSING bitmaps, no Host/HttpRequest variants), `SelectionVector`, `Bitmap` types in `src/execution/batch.rs`
- Add `BatchStream` trait alongside existing `RecordStream`
- Write unit tests for all kernels against scalar reference implementations
- Verify auto-vectorization via `--emit=asm` spot checks in CI (see Section 3 for the specific grep patterns)
- Change JSONL scan path to use `Vec<u8>` buffers for `simd-json` compatibility (mutable buffer requirement)

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

**Benchmark gate:** Measure the adapter path against the row-only baseline. The `BatchToRowAdapter` materializes batches into individual Records, which could be slower than the current all-row path due to conversion overhead. If the adapter path is slower than baseline for any benchmark query, investigate before proceeding. The SIMD benefit in scan+filter must outweigh the adapter conversion cost.

### Phase 3 -- Operators, highest impact first

Priority is informed by Phase 0 profiling results. Expected priority order:

| Priority | Operator | Rationale |
|---|---|---|
| 1 | `BatchScanOperator` | Entry point; SIMD tokenization benefits every query including Host/HttpRequest field parsing |
| 2 | `BatchFilterOperator` | Second biggest cost center; two-pass filter kernels for typed columns, scalar fallback for Mixed |
| 3 | `BatchProjectOperator` | Completes SELECT-FROM-WHERE path |
| 4 | `BatchGroupByOperator` | Unlocks multiply-shift hashing + hashbrown probing + vectorized accumulators |
| 5 | `BatchLimitOperator` | Trivial -- truncate selection vector |
| 6 | `BatchOrderByOperator` | Vectorized sort keys |
| 7 | `BatchDistinctOperator` | Reuses GroupBy's hashbrown table |
| 8 | Join operators | Lowest priority -- log queries rarely join |

### Phase 4 -- Remove adapters

Once all operators are converted, remove `RecordStream`, `Record`, and the adapter layer.

### Performance Targets

Targets are qualified by profiling data from Phase 0. The estimates below assume a typical bottleneck distribution of ~50% tokenization/parsing, ~30% filtering/evaluation, ~20% aggregation/output (to be validated by profiling):

| Milestone | Target | Measurement |
|---|---|---|
| Phase 2 (scan + filter) | 2-4x on filter-heavy ELB queries | `benches/bench_execution.rs` tier-A |
| Phase 3 (+ project + groupby) | 3-6x on aggregation queries | Same |
| Phase 4 (full batch) | 5-8x on large log files | Same |

If Phase 0 profiling reveals that I/O dominates (>60%), the targets will be reduced accordingly and the design may prioritize I/O improvements (e.g., memory-mapped files, parallel reads) over expression-level SIMD.

Note: queries heavy on Host/HttpRequest function calls (e.g., `SELECT url_path(request) ... GROUP BY url_path(request)`) will see less speedup than queries on typed columns, since the function evaluation path remains scalar. The primary speedup for these queries comes from SIMD tokenization in the scan layer.

Benchmarks in `benches/bench_execution.rs` track progress at each phase boundary. Each phase must show measurable improvement over the previous phase on at least one benchmark category before proceeding to the next.
