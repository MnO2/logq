# SIMD-Accelerated Physical Plan for logq (v4)

**Date:** 2026-04-06
**Status:** Draft
**Revision:** v4 (incorporates 5 Velox-inspired optimizations identified via adversarial review of v3)
**Inspiration:** Velox execution engine SIMD patterns

## Changes from v3 (Final)

This revision adds five new optimizations inspired by deeper study of Velox's execution engine patterns. None of the v3 design is removed -- these are additive improvements layered on top of the existing architecture.

| ID | Optimization | Summary | Section |
|---|---|---|---|
| **G1** | Lazy field parsing (two-phase scan) | Defer parsing of non-filter fields until after predicate evaluation; skip entirely for filtered rows | Section 9 |
| **G2** | Adaptive filter reordering | Reorder compound predicate conjuncts by estimated cost at plan time so cheapest filters run first | Section 10 |
| **G3** | Padded vectors | `PaddedVec<T>` wrapper with 32-byte tail padding for tail-safe SIMD loads/stores | Section 11 |
| **G4** | Direct bitmask extraction | Optional `simd-intrinsics` cargo feature using `std::simd::Mask::to_bitmask()` to eliminate the two-pass filter strategy | Section 12 |
| **G5** | Filter result caching for low-cardinality strings | Cache filter results per unique string value within a batch; avoid redundant comparisons on status codes, methods, etc. | Section 13 |

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
| Scan strategy | Two-phase: tokenize + filter fields first, then parse projected fields for survivors only | Avoids parsing non-filter fields for rows that will be discarded (inspired by Velox LazyVector) |
| Filter ordering | Static cost-model reordering at plan time | Cheap predicates first reduces work for expensive ones; ~20 lines in planner |
| Vector storage | `PaddedVec<T>` with 32-byte tail padding | Eliminates tail-handling complexity in all SIMD kernels, especially Layer 2 intrinsics |
| Bitmask extraction | Two-pass by default; optional single-pass via `simd-intrinsics` feature flag | Stable Rust compatibility by default; nightly users can opt into 20-30% faster filters |
| String filter caching | Per-batch dedup + cache for low-cardinality string filters | Reduces filter evaluations from N rows to K unique values for status codes, methods, etc. |

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

### PaddedVec (NEW in v4)

All column data vectors use `PaddedVec<T>` instead of plain `Vec<T>` to guarantee tail-safe SIMD access:

```rust
const SIMD_PADDING: usize = 32; // bytes -- covers AVX2 (32B) and NEON (16B)

/// A Vec<T> with guaranteed padding bytes past the logical end.
/// SIMD kernels can perform full-width loads/stores at the tail
/// without bounds checking or scalar epilogues.
pub struct PaddedVec<T> {
    inner: Vec<T>,
    // Invariant: capacity >= len + SIMD_PADDING / size_of::<T>()
    // Padding bytes are always zeroed.
}

impl<T: Copy + Default> PaddedVec<T> {
    pub fn with_len(len: usize) -> Self {
        let pad_elements = SIMD_PADDING / std::mem::size_of::<T>();
        let mut inner = vec![T::default(); len + pad_elements];
        inner.truncate(len);
        // Padding is already zeroed from vec! initialization
        Self { inner }
    }

    pub fn from_vec(mut v: Vec<T>) -> Self {
        let pad_elements = SIMD_PADDING / std::mem::size_of::<T>();
        v.reserve(pad_elements);
        // Zero the padding region
        unsafe {
            let ptr = v.as_mut_ptr().add(v.len());
            std::ptr::write_bytes(ptr, 0, pad_elements);
        }
        Self { inner: v }
    }
}

impl<T> std::ops::Deref for PaddedVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] { &self.inner }
}

impl<T> std::ops::DerefMut for PaddedVec<T> {
    fn deref_mut(&mut self) -> &mut [T] { &mut self.inner }
}
```

Cost: 32 extra bytes per column per batch (8 extra i32 elements). For a 17-column ELB schema, that's 544 bytes per batch -- negligible.

Benefit: eliminates scalar tail loops in Layer 2 intrinsic kernels (`string_search.rs`) and any future hand-written SIMD. For Layer 1 auto-vectorized kernels, LLVM already generates tail handling, but `PaddedVec` makes the generated code simpler (no masked tail operations needed).

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

A typed array with separate NULL and MISSING tracking. All data vectors use `PaddedVec<T>`:

```rust
pub enum TypedColumn {
    // --- Primitive types (SIMD-friendly dense arrays) ---
    Int32 {
        data: PaddedVec<i32>,
        null: Bitmap,      // bit=1 means value IS NOT NULL
        missing: Bitmap,   // bit=1 means value IS NOT MISSING
    },
    Float32 {
        data: PaddedVec<f32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Boolean {
        data: Bitmap,      // packed booleans
        null: Bitmap,
        missing: Bitmap,
    },
    Utf8 {
        data: PaddedVec<u8>,  // concatenated string bytes (owned buffer)
        offsets: PaddedVec<u32>, // offsets[i]..offsets[i+1] = string i
        null: Bitmap,
        missing: Bitmap,
    },
    DateTime {
        data: PaddedVec<i64>,  // epoch micros
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
| `LogFileStream` | `BatchScanOperator` | SIMD tokenization, parallel field parsing, **lazy field parsing (G1)** | Always `All` (freshly scanned data) |
| `FilterStream` | `BatchFilterOperator` | SIMD predicate evaluation, **filter caching (G5)**, **adaptive reordering (G2)** | Narrows: produces `Bitmap` or `Indices` |
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

### Layer 1.5: Direct Bitmask Extraction (NEW in v4, optional)

When built with the `simd-intrinsics` cargo feature (requires nightly Rust), filter kernels use `std::simd::Mask::to_bitmask()` to eliminate the two-pass strategy entirely. This is the Rust equivalent of Velox's `toBitMask` (which maps to hardware `movemask` on x86, AND+horizontal-add on NEON). See Section 12 for details.

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
    // PaddedVec guarantees safe overread at the tail of haystack.
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

Note: the string search kernels also produce byte-per-row results (not bitmap), consistent with the two-pass strategy. The caller packs to bitmap via `Bitmap::pack_from_bytes()`. The `PaddedVec` backing the `Utf8` column's `data` and `offsets` fields guarantees that SIMD overreads at the tail are safe.

### Module Structure

```
src/simd/
  mod.rs            // SimdStringKernels::detect(), re-exports
  kernels.rs        // Auto-vectorizable kernels (filter, sum, hash, bitmap ops)
  string_search.rs  // Targeted intrinsics for str_contains (per-arch)
  padded_vec.rs     // PaddedVec<T> type (NEW in v4)
  direct_bitmask.rs // Optional std::simd-based bitmask extraction (NEW in v4, behind feature flag)
```

This is 5 files, with only `string_search.rs` and optionally `direct_bitmask.rs` containing `unsafe` code.

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
| `elb_status_code = '200'` | `str_eq_batch(bytes, offsets, needle, result_bytes)` then pack | String equality on status codes (which are `DataType::String` in the schema). **Benefits from filter caching (G5)** -- only ~15 unique status codes |
| `request LIKE '%/api%'` | `str_contains(bytes, offsets, needle, result_bytes)` via `SimdStringKernels` then pack | Runtime-dispatched SIMD string search on request field (Mixed column, falls back to per-row for function-based access like `url_path(request)`) |
| `col IN (200, 301, 404)` | `filter_in_i32(data, &hashset, result_bytes)` then pack | Auto-vectorized hash + lookup |
| `a AND b` | `bitmap_and(left, right)` | Word-by-word u64 AND; auto-vectorized (already in bitmap form). **Conjuncts reordered by cost (G2)** |
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
2. **String equality/substring** -- `str_eq_batch`, `str_contains` (covers status codes, method names, URLs, user agents). **Filter caching (G5) amplifies this for low-cardinality fields.**
3. **Float comparison** -- processing time filters (3 float fields in ELB/ALB)
4. **Integer comparison** -- byte count filters (2 integer fields in ELB/ALB)

Host/HttpRequest expression evaluation (via `host_name()`, `url_path()`, etc.) remains scalar per-row through the `Mixed` fallback path. This is acceptable because these function calls are not the typical bottleneck -- they occur only when the query explicitly uses these functions, and the tokenization acceleration still applies.

### Compound Predicate Short-Circuit

Evaluate the left side first, then pass the narrowed `SelectionVector` to the right side. If the left side eliminates 90% of rows, the right side only touches 10%. **Conjuncts are reordered at plan time by estimated cost (Section 10) so the cheapest predicate runs first.**

### Fallback

If a predicate involves `TypedColumn::Mixed` or an unsupported expression shape (e.g. `function(col) > value`), it falls back to per-row scalar evaluation over the batch. This includes all Host/HttpRequest function-based predicates like `WHERE url_path(request) = '/api'` or `WHERE host_name(client_and_port) = 'example.com'`.

## Section 5: SIMD Kernels -- Data Source Parsing

### BatchScanOperator Pipeline (Updated in v4)

The scan operator now implements a **two-phase pipeline** inspired by Velox's LazyVector and predicate pushdown patterns (see Section 9 for full details):

```
Read BATCH_SIZE lines into line buffer
        |
        v
  Phase 1: SIMD Tokenization (find ALL field delimiters)
        |
        v
  Phase 1: Parse FILTER fields only into TypedColumns
        |
        v
  Phase 1: Evaluate pushed-down predicate (if any)
        |
        v
  Phase 2: Parse PROJECTED fields for surviving rows only
        |
        v
  Emit ColumnBatch with SelectionVector from Phase 1
```

When no filter is pushed down (e.g., `SELECT * FROM elb`), the two phases collapse into a single pass that parses all fields -- no overhead from the lazy mechanism.

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

**Lazy field parsing (G1):** In the two-phase scan, only fields referenced in the WHERE clause are parsed in Phase 1. The remaining projected fields are parsed in Phase 2 only for surviving rows. See Section 9 for the full mechanism.

### String Storage (Arrow-Style Owned Buffer)

Each `Utf8` column owns its string data. There are no references to external line buffers:

```rust
// Building a Utf8 column during scan:
let mut data: PaddedVec<u8> = PaddedVec::with_capacity(BATCH_SIZE * 32); // estimate
let mut offsets: PaddedVec<u32> = PaddedVec::with_capacity(BATCH_SIZE + 1);
offsets.push(0);

for row in 0..batch_len {
    let field_bytes: &[u8] = /* extracted from tokenizer */;
    data.extend_from_slice(field_bytes);
    offsets.push(data.len() as u32);
}

// The Utf8 column now owns all string data. No lifetime issues.
// data = [bytes_of_row0 | bytes_of_row1 | ... | bytes_of_rowN]
// offsets = [0, len0, len0+len1, ..., total_len]
// PaddedVec guarantees 32 bytes of safe overread past the end.
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
- Add `PaddedVec<T>` in `src/simd/padded_vec.rs` **(NEW in v4)**
- Add `ColumnBatch`, `TypedColumn` (with NULL + MISSING bitmaps, no Host/HttpRequest variants, using `PaddedVec`), `SelectionVector`, `Bitmap` types in `src/execution/batch.rs`
- Add `BatchStream` trait alongside existing `RecordStream`
- Add `FilterCache` for string predicate caching in `src/simd/filter_cache.rs` **(NEW in v4)**
- Add predicate cost model and conjunct reordering in `src/logical/optimizer.rs` **(NEW in v4)**
- Write unit tests for all kernels against scalar reference implementations
- Verify auto-vectorization via `--emit=asm` spot checks in CI (see Section 3 for the specific grep patterns)
- Change JSONL scan path to use `Vec<u8>` buffers for `simd-json` compatibility (mutable buffer requirement)
- Optional: add `simd-intrinsics` feature flag with `std::simd` direct bitmask extraction **(NEW in v4)**

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
| 1 | `BatchScanOperator` **with two-phase lazy parsing (G1)** | Entry point; SIMD tokenization + lazy field parsing benefits every query. Selective queries skip parsing non-filter fields for discarded rows. |
| 2 | `BatchFilterOperator` **with filter caching (G5) and reordered predicates (G2)** | Second biggest cost center; two-pass filter kernels for typed columns, string filter caching for low-cardinality fields, scalar fallback for Mixed |
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
| Phase 2 (scan + filter) | 3-6x on filter-heavy ELB queries (improved from 2-4x by G1+G5) | `benches/bench_execution.rs` tier-A |
| Phase 3 (+ project + groupby) | 4-8x on aggregation queries | Same |
| Phase 4 (full batch) | 6-10x on large log files (improved from 5-8x by cumulative G1-G5 impact) | Same |

If Phase 0 profiling reveals that I/O dominates (>60%), the targets will be reduced accordingly and the design may prioritize I/O improvements (e.g., memory-mapped files, parallel reads) over expression-level SIMD.

Note: queries heavy on Host/HttpRequest function calls (e.g., `SELECT url_path(request) ... GROUP BY url_path(request)`) will see less speedup than queries on typed columns, since the function evaluation path remains scalar. The primary speedup for these queries comes from SIMD tokenization in the scan layer.

Benchmarks in `benches/bench_execution.rs` track progress at each phase boundary. Each phase must show measurable improvement over the previous phase on at least one benchmark category before proceeding to the next.

---

## Section 9: Lazy Field Parsing -- Two-Phase Scan (NEW in v4)

*Inspired by Velox's `LazyVector`, `SelectiveColumnReader`, and predicate pushdown into the I/O layer.*

### Motivation

In the v3 design, `BatchScanOperator` parses ALL fields for ALL rows into typed columns, then `BatchFilterOperator` discards non-matching rows via the selection vector. For a query like `SELECT url FROM elb WHERE status_code = '200'`, this means:
- All 17 ELB fields are parsed for every row
- ~80% of parsed data is discarded (assuming 20% match rate for status 200)
- Expensive Host/HttpRequest parsing runs for rows that will never be seen

Velox avoids this by pushing predicates into the column reader and using LazyVector to defer non-filter column materialization.

### Design

The `BatchScanOperator` accepts an optional **pushed-down predicate** from the planner. When present, the scan pipeline splits into two phases:

```rust
pub struct BatchScanOperator {
    reader: Box<dyn RecordRead>,
    schema: LogSchema,                    // field names, types, positions
    pushed_predicate: Option<PushedPredicate>,  // from planner
    projected_fields: Vec<usize>,         // field indices needed in output
    // ...
}

/// A predicate that can be evaluated during scan, before full field parsing.
pub struct PushedPredicate {
    /// Field indices that the predicate references (must be parsed in Phase 1).
    filter_field_indices: Vec<usize>,
    /// The predicate expression to evaluate on the filter fields.
    predicate: Box<Formula>,
}
```

**Phase 1 — Tokenize + Parse Filter Fields + Evaluate Predicate:**

```rust
// 1. Read BATCH_SIZE lines, SIMD-tokenize each to find field boundaries.
//    Store raw byte ranges per field per row (no allocation, just offsets).
let line_fields: Vec<Vec<(usize, usize)>> = tokenize_batch(&lines);

// 2. Parse ONLY the fields referenced by the pushed predicate.
let mut filter_columns: Vec<TypedColumn> = Vec::new();
for &field_idx in &self.pushed_predicate.filter_field_indices {
    filter_columns.push(parse_field_column(
        &lines, &line_fields, field_idx, &self.schema
    ));
}

// 3. Evaluate the predicate on the filter columns → SelectionVector.
let selection = self.pushed_predicate.predicate
    .evaluate_on_columns(&filter_columns)?;
```

**Phase 2 — Parse Remaining Projected Fields (survivors only):**

```rust
// 4. For surviving rows only, parse the remaining projected fields.
let mut all_columns: Vec<TypedColumn> = filter_columns;
for &field_idx in &self.projected_fields {
    if self.pushed_predicate.filter_field_indices.contains(&field_idx) {
        continue; // already parsed in Phase 1
    }
    // Parse only for rows where selection is active.
    all_columns.push(parse_field_column_selected(
        &lines, &line_fields, field_idx, &self.schema, &selection
    ));
}
```

**`parse_field_column_selected`** skips parsing for inactive rows:

```rust
fn parse_field_column_selected(
    lines: &[Vec<u8>],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    schema: &LogSchema,
    selection: &SelectionVector,
) -> TypedColumn {
    match schema.field_type(field_idx) {
        FieldType::String => {
            let mut data = PaddedVec::<u8>::new();
            let mut offsets = PaddedVec::<u32>::new();
            offsets.push(0);
            for row in 0..lines.len() {
                if selection.is_active(row) {
                    let (start, end) = fields[row][field_idx];
                    data.extend_from_slice(&lines[row][start..end]);
                }
                offsets.push(data.len() as u32);
            }
            TypedColumn::Utf8 { data, offsets, null: ..., missing: ... }
        }
        FieldType::Host => {
            // Host parsing is expensive -- skip entirely for inactive rows
            let mut values = Vec::with_capacity(lines.len());
            for row in 0..lines.len() {
                if selection.is_active(row) {
                    let (start, end) = fields[row][field_idx];
                    values.push(parse_host(&lines[row][start..end]));
                } else {
                    values.push(Value::Missing); // placeholder, never read
                }
            }
            TypedColumn::Mixed { data: values, null: ..., missing: ... }
        }
        // ... other types
    }
}
```

### Predicate Pushdown from Planner

The planner identifies pushable predicates during logical-to-physical translation:

```rust
// In PhysicalPlanCreator::create_scan_with_filter():
fn try_push_predicate(
    filter: &Formula,
    schema: &LogSchema,
) -> Option<PushedPredicate> {
    // A predicate is pushable if ALL referenced columns are schema fields
    // (not computed expressions, not function calls on Mixed columns).
    let referenced_fields = filter.referenced_field_indices(schema);
    if referenced_fields.is_empty() {
        return None;
    }

    // Check that no referenced field requires Mixed-column function evaluation.
    // E.g., `status_code = '200'` is pushable (Utf8 column).
    //        `url_path(request) = '/api'` is NOT pushable (function on Mixed).
    for &idx in &referenced_fields {
        if schema.field_type(idx).is_mixed_function_access() {
            return None; // fall back to post-scan filter
        }
    }

    Some(PushedPredicate {
        filter_field_indices: referenced_fields,
        predicate: filter.clone().into(),
    })
}
```

When the predicate is partially pushable (e.g., `WHERE status_code = '200' AND url_path(request) = '/api'`), the pushable conjuncts are pushed and the non-pushable ones remain as a post-scan `BatchFilterOperator`.

### Impact Analysis

| Scenario | Fields parsed (v3) | Fields parsed (v4) | Savings |
|---|---|---|---|
| `SELECT * FROM elb WHERE status = '200'` (20% match) | 17 * N rows | 1 * N + 17 * 0.2N = 4.4N | 74% |
| `SELECT url FROM elb WHERE status = '200'` (20% match) | 17 * N | 1 * N + 1 * 0.2N = 1.2N | 93% |
| `SELECT * FROM elb` (no filter) | 17 * N | 17 * N (no change) | 0% |
| `SELECT * FROM elb WHERE url_path(request) = '/api'` | 17 * N | 17 * N (not pushable) | 0% |

The biggest wins come from selective queries that project few columns -- the most common log analysis pattern.

## Section 10: Adaptive Filter Reordering (NEW in v4)

*Inspired by Velox's `ScanSpec::newRead()` adaptive column reordering by selectivity.*

### Motivation

Velox reorders filter columns at runtime based on measured selectivity. For logq, a simpler static cost model suffices -- log query predicates have predictable cost characteristics.

### Design

During logical-to-physical translation, compound AND predicates are reordered so the cheapest conjuncts evaluate first. This is a ~20-line addition to the planner.

```rust
/// Estimated cost of evaluating a predicate, used for reordering AND conjuncts.
/// Lower cost = evaluate first (to eliminate rows cheaply before expensive predicates).
fn predicate_cost(formula: &Formula, schema: &LogSchema) -> u32 {
    match formula {
        // Boolean/NULL checks are bitmap-only -- essentially free
        Formula::IsNull(_) | Formula::IsMissing(_) => 1,

        // Integer/float comparisons -- one SIMD pass
        Formula::Comparison(op, left, right)
            if is_numeric_column(left, schema) || is_numeric_column(right, schema) => 10,

        // String equality -- hash or byte compare, but benefits from filter cache
        Formula::Comparison(Eq, left, right)
            if is_string_column(left, schema) => 20,

        // String LIKE with prefix (e.g., LIKE 'abc%') -- prefix check, fast
        Formula::Like(_, pattern) if pattern.starts_with_literal() => 30,

        // String LIKE with substring (e.g., LIKE '%abc%') -- SIMD string search
        Formula::Like(_, _) => 50,

        // IN list -- hash set lookup per row
        Formula::InList(_, _) => 40,

        // Function calls on Mixed columns -- scalar per-row, expensive
        Formula::Comparison(_, left, right)
            if involves_function_call(left) || involves_function_call(right) => 100,

        // Anything else
        _ => 60,
    }
}

/// Reorder AND conjuncts by ascending cost.
fn reorder_conjuncts(conjuncts: &mut Vec<Formula>, schema: &LogSchema) {
    conjuncts.sort_by_key(|f| predicate_cost(f, schema));
}
```

This is called once during plan construction, not at runtime. The reordered conjuncts feed into both:
1. **Predicate pushdown (Section 9):** The cheapest pushable conjunct becomes the pushed predicate in the scan.
2. **BatchFilterOperator:** Remaining conjuncts evaluate in cost order with short-circuit via selection vector narrowing.

### Interaction with Filter Caching (G5)

Filter caching (Section 13) makes string equality predicates effectively as cheap as integer comparisons for low-cardinality fields. The cost model could be refined to account for this, but the static ordering is already a good approximation -- string equality is ranked at cost 20, close to integer comparison at 10.

## Section 11: Padded Vectors -- Design Details (NEW in v4)

*Inspired by Velox's `raw_vector<T>` with `simd::kPadding` and `AlignedBuffer` padding strategy.*

### Rationale

Velox allocates 32-64 extra bytes past every SIMD-participating buffer. This eliminates scalar tail loops in hot SIMD code -- functions can do full-width loads/stores at the last element, overwriting the padding harmlessly. Examples in Velox: `memEqualUnsafe`, `processFixedFilter`, `indicesOfSetBits`, `simd::transpose`.

For logq, the primary benefit is in Layer 2 intrinsic kernels (`string_search.rs`), where manual SIMD loads at the tail of the haystack or offset array would otherwise require bounds checking. It also simplifies any future intrinsic kernels.

For Layer 1 auto-vectorized kernels, LLVM already handles tails, but `PaddedVec` allows LLVM to generate simpler tail code (no masked operations needed when the compiler can prove the overread is safe via the allocation size).

### Implementation

See `PaddedVec<T>` definition in Section 1. Key properties:

- **Zero padding bytes:** Padding region is always zeroed, so SIMD loads into the padding produce deterministic (zero) values. This matters for sum/count kernels where garbage in the padding could corrupt results.
- **No alignment guarantee:** Unlike Velox's `raw_vector` (which aligns to `kPadding`), `PaddedVec` uses Rust's default allocator alignment. Rust's `Vec<i32>` is 4-byte aligned, `Vec<i64>` is 8-byte aligned. We use `load_unaligned` everywhere (same as Velox's practice -- even Velox uses unaligned loads despite aligned allocations, because working offsets within buffers are not aligned).
- **Deref to `[T]`:** The logical slice excludes padding. Iterators, indexing, and `len()` all see only the logical data. SIMD kernels that need to overread must use `inner.as_ptr()` + raw pointer arithmetic.

### Usage in TypedColumn

All data vectors in `TypedColumn` variants (except `Mixed`, which uses `Vec<Value>`) are `PaddedVec<T>`. This includes:
- `Int32::data: PaddedVec<i32>`
- `Float32::data: PaddedVec<f32>`
- `Utf8::data: PaddedVec<u8>` and `Utf8::offsets: PaddedVec<u32>`
- `DateTime::data: PaddedVec<i64>`

`Bitmap::words` remains `Vec<u64>` because bitmap operations are word-by-word and `BATCH_SIZE=1024` produces exactly 16 words -- no tail issue.

## Section 12: Direct Bitmask Extraction (NEW in v4)

*Inspired by Velox's `toBitMask` which uses hardware `movemask` instructions to convert SIMD comparison results to integer bitmasks in a single instruction.*

### Motivation

The v3 two-pass strategy (compare to `Vec<u8>`, then pack to bitmap) exists because LLVM cannot auto-vectorize bitmap packing. Velox avoids this entirely with architecture-specific `movemask` intrinsics.

Rust's `std::simd` (nightly, experimental) provides `Mask::to_bitmask()` — the direct equivalent. On x86, this compiles to `movemask`. On ARM, it uses the AND+horizontal-add pattern (same as Velox's NEON path).

### Design

A cargo feature flag `simd-intrinsics` gates the single-pass path:

```toml
# Cargo.toml
[features]
default = []
simd-intrinsics = []  # Requires nightly Rust. Enables std::simd for direct bitmask extraction.
```

```rust
// src/simd/kernels.rs

/// Filter: compare i32 column against threshold, produce bitmap.
/// Default path: two-pass (auto-vectorized compare to bytes, then pack).
#[cfg(not(feature = "simd-intrinsics"))]
pub fn filter_ge_i32_to_bitmap(data: &[i32], threshold: i32) -> Bitmap {
    let mut bytes = vec![0u8; data.len()];
    filter_ge_i32(data, threshold, &mut bytes);
    Bitmap::pack_from_bytes(&bytes)
}

/// Optimized path: single-pass using std::simd::Mask::to_bitmask().
/// Eliminates the intermediate Vec<u8> buffer entirely.
#[cfg(feature = "simd-intrinsics")]
pub fn filter_ge_i32_to_bitmap(data: &[i32], threshold: i32) -> Bitmap {
    use std::simd::*;

    const LANES: usize = 8; // i32x8 = 256 bits (AVX2)
    let thresh = Simd::<i32, LANES>::splat(threshold);
    let num_words = (data.len() + 63) / 64;
    let mut words = vec![0u64; num_words];

    let chunks = data.chunks_exact(LANES);
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let v = Simd::<i32, LANES>::from_slice(chunk);
        let mask = v.simd_ge(thresh);
        let bits = mask.to_bitmask() as u64; // Single movemask instruction!

        let word_idx = (chunk_idx * LANES) / 64;
        let bit_offset = (chunk_idx * LANES) % 64;
        words[word_idx] |= bits << bit_offset;
    }

    // Handle remainder (< LANES elements) with scalar fallback
    let base = data.len() - remainder.len();
    for (i, &val) in remainder.iter().enumerate() {
        if val >= threshold {
            let pos = base + i;
            words[pos / 64] |= 1u64 << (pos % 64);
        }
    }

    Bitmap { words }
}
```

### Trade-offs

| Aspect | Two-pass (default) | Direct bitmask (`simd-intrinsics`) |
|---|---|---|
| Rust version | Stable | Nightly only |
| Unsafe code | None | None (std::simd is safe) |
| Intermediate buffer | 1KB Vec<u8> per kernel call | None |
| Instructions per batch | ~2048 (compare) + ~1024 (pack) | ~128 (compare+movemask) |
| Estimated speedup | Baseline | 20-30% faster filter evaluation |
| Portability | All platforms | x86-64 + aarch64 (std::simd coverage) |

### When to Adopt

This is a Phase 1 optional item. Add the feature flag and alternative implementations during foundation work. Enable in benchmarks to measure actual impact. If profiling (Phase 0) shows filtering is <20% of total time, the 20-30% filter speedup translates to <6% overall — not worth the nightly dependency for most users.

## Section 13: Filter Result Caching for Low-Cardinality Strings (NEW in v4)

*Inspired by Velox's `SelectiveColumnReader::filterCache_` which caches filter results per dictionary entry.*

### Motivation

Log files have many low-cardinality string fields:

| Field | Typical cardinality | Example values |
|---|---|---|
| `status_code` / `elb_status_code` | 10-20 | "200", "301", "404", "500" |
| `request_method` | 5-8 | "GET", "POST", "PUT", "DELETE" |
| `ssl_protocol` | 3-5 | "TLSv1.2", "TLSv1.3", "-" |
| `ssl_cipher` | 10-15 | "ECDHE-RSA-AES128-GCM-SHA256", ... |
| `type` (ALB) | 3 | "h2", "http", "https" |

For a 1M-row log file, a filter like `WHERE status_code = '200'` runs the string comparison 1M times. With caching, it runs ~15 times (once per unique value), then the rest are hash lookups.

Velox achieves this via dictionary encoding — each column has a dictionary, and `filterCache_` maps dictionary entry IDs to filter results. logq doesn't have dictionaries, but we can build an equivalent per-batch cache.

### Design

```rust
/// Caches filter results per unique string value within a batch.
/// Used by BatchFilterOperator for string equality/comparison predicates.
pub struct StringFilterCache {
    /// Maps string bytes → filter result (true = pass, false = fail).
    /// Uses a small HashMap since cardinality is typically <100.
    cache: HashMap<Vec<u8>, bool>,
}

impl StringFilterCache {
    pub fn new() -> Self {
        Self { cache: HashMap::with_capacity(32) }
    }

    /// Evaluate a string filter on a Utf8 column, caching results per unique value.
    /// Returns a Bitmap of matching rows.
    pub fn evaluate_cached(
        &mut self,
        data: &[u8],
        offsets: &[u32],
        filter_fn: &dyn Fn(&[u8]) -> bool,
        len: usize,
    ) -> Bitmap {
        self.cache.clear();
        let mut result_bytes = vec![0u8; len];

        for i in 0..len {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let field = &data[start..end];

            let passed = self.cache
                .entry(field.to_vec())
                .or_insert_with(|| filter_fn(field));

            result_bytes[i] = *passed as u8;
        }

        Bitmap::pack_from_bytes(&result_bytes)
    }
}
```

### Optimized Two-Pass Variant

The per-row `entry().or_insert_with()` approach above involves a hash lookup per row. For maximum throughput, a two-pass variant deduplicates first:

```rust
/// Two-pass cached filter evaluation:
/// Pass 1: Deduplicate — scan column, collect unique values, test each once.
/// Pass 2: Broadcast — for each row, look up cached result.
pub fn evaluate_cached_two_pass(
    data: &[u8],
    offsets: &[u32],
    filter_fn: &dyn Fn(&[u8]) -> bool,
    len: usize,
) -> Bitmap {
    // Pass 1: Collect unique values and their filter results.
    let mut cache: HashMap<&[u8], bool> = HashMap::with_capacity(32);
    for i in 0..len {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let field = &data[start..end];
        cache.entry(field).or_insert_with(|| filter_fn(field));
    }

    // Pass 2: Look up each row's result from the cache.
    let mut result_bytes = vec![0u8; len];
    for i in 0..len {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        result_bytes[i] = *cache.get(&data[start..end]).unwrap() as u8;
    }

    Bitmap::pack_from_bytes(&result_bytes)
}
```

The two-pass variant uses borrowed `&[u8]` keys (no allocation per row) since both passes operate on the same batch data. Pass 1 does K hash inserts + K filter evaluations (K = cardinality). Pass 2 does N hash lookups (N = batch size). For K=15 and N=1024, this is 15 filter evaluations + 1039 hash lookups instead of 1024 filter evaluations.

### When to Use Caching

Not all string filters benefit from caching. The decision is based on predicate type:

| Predicate | Cache? | Rationale |
|---|---|---|
| String equality (`status = '200'`) | **Yes** | Low cardinality, expensive string compare |
| String IN list (`status IN ('200', '301')`) | **Yes** | Same as equality |
| LIKE with prefix (`LIKE 'GET%'`) | **Yes** | Low cardinality for method-like fields |
| LIKE with substring (`LIKE '%api%'`) | **Maybe** | High-cardinality fields (URLs) won't benefit; skip if unique count > batch_size / 4 |
| Function predicates (`url_path(x) = '/api'`) | **Yes** | Function evaluation is expensive; cache per unique input value |

The cache is created per-batch and cleared between batches. No cross-batch state, no memory growth concern.

### Cardinality Adaptive Behavior

For the two-pass variant, after Pass 1, check the cache size. If `cache.len() > len / 4` (more than 25% unique values), the caching overhead exceeds the benefit — fall back to direct evaluation for this batch:

```rust
if cache.len() > len / 4 {
    // High cardinality — caching not beneficial. Direct evaluation.
    return direct_filter_evaluation(data, offsets, filter_fn, len);
}
```

This ensures that high-cardinality columns (URLs, user agents) don't pay caching overhead.

---

## Appendix: Combined Optimization Flow

Here is how all five optimizations interact in a typical selective query:

```
Query: SELECT url, status FROM elb WHERE status = '200' AND user_agent LIKE '%bot%'

1. PLANNER (G2: Adaptive Reordering)
   - Cost model: status equality (20) < user_agent LIKE (50)
   - Reorder: evaluate status first, then user_agent
   - Identify pushable predicate: status = '200' → push into scan

2. BATCH SCAN OPERATOR (G1: Lazy Field Parsing)
   Phase 1:
   - SIMD tokenize 1024 lines → field byte ranges
   - Parse only field #12 (elb_status_code) into Utf8 column
   - Evaluate: status = '200'
     → (G5: Filter Cache) 15 unique status codes → 15 comparisons, 1009 cache hits
     → SelectionVector: ~200 rows active (20% match rate)
   Phase 2:
   - Parse field #3 (url) for 200 surviving rows only (not 1024!)
   - Parse field #12 (status) already done
   - Skip parsing 15 other fields entirely

3. BATCH FILTER OPERATOR (remaining predicate)
   - Evaluate: user_agent LIKE '%bot%' on 200 active rows
   - (user_agent was already parsed in Phase 2 since it's projected? No — it's
     not in SELECT. If not projected, it wasn't parsed. Need to handle this.)
   - CORRECTION: user_agent is not in SELECT but IS in WHERE. The planner
     must include non-pushed WHERE fields in the projected set for Phase 2.
   - After filter: ~20 rows active (10% of status=200 rows are bots)

4. BATCH PROJECT OPERATOR
   - Emit url and status columns with SelectionVector (20 active rows)
   - No compaction (non-materializing)

5. OUTPUT
   - Iterate 20 active rows, format output.

Result: parsed 1*1024 + 2*200 = 1424 field parses instead of 17*1024 = 17408.
That's 12x fewer field parses. Combined with SIMD tokenization and filter
caching, the total speedup is substantial.
```

**Important correction noted above:** When a WHERE predicate references a field that is NOT in the SELECT list, the planner must ensure that field is included in the Phase 2 projected set (parsed for surviving rows). The field is then dropped after filtering, before output. This is standard predicate handling — the existing planner already adds filter-referenced columns to the scan's output.
