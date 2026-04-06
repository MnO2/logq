# SIMD-Accelerated Physical Plan for logq (v2)

**Date:** 2026-04-05
**Status:** Draft
**Revision:** v2 (addresses review feedback)
**Inspiration:** Velox execution engine SIMD patterns

## Changes from Previous Version

This revision addresses all critical issues (C1-C5) and incorporates relevant suggestions (S1, S4, S5, S7, S8) from the first design review.

| Issue | Summary | Resolution |
|---|---|---|
| **C1** | TypedColumn missing MISSING bitmap; no coverage for Host, HttpRequest, Object, Array | Added separate `missing` bitmap to all column variants. Added `Host` and `HttpRequest` typed columns for ELB/ALB fast path. Object/Array remain in `Mixed` with documented impact analysis. |
| **C2** | `std::arch` intrinsics impractical for a single-developer 19K-line project | Switched to auto-vectorization-first strategy with `#[target_feature]` annotations. Targeted intrinsics only for string search kernel. Removed 4-backend architecture. Fixed CRC32/SSE4.2 mistake. Added runtime dispatch. |
| **C3** | Zero-copy string lifetimes create unsound self-referential struct | Adopted Arrow-style owned `Vec<u8>` buffer + offsets within each `Utf8` column (no external references). |
| **C4** | SelectionVector interaction with materializing operators unspecified | Documented compaction rules: materializing operators compact input, always emit `SelectionVector::All`. Enumerated every operator's behavior. |
| **C5** | Status code example misleading (strings, not integers); ELB fields mostly strings | Fixed example to use `received_bytes`. Added field type breakdown for ELB/ALB showing string dominance. Adjusted prioritization to emphasize string/tokenization SIMD. |
| **S1** | Consider Arrow crate | Decided against: too heavy for this project's needs. Custom TypedColumn is simpler and avoids pulling in the Arrow dependency graph. Revisit if/when the project outgrows the custom approach. |
| **S4** | BitVec crate selection | Switched from `bitvec` crate to `Vec<u64>` with manual bit manipulation for SIMD-friendly 64-row-at-a-time processing. |
| **S5** | Batch size hardcoded at 1024 | Made batch size a compile-time constant (`const BATCH_SIZE: usize = 1024`) so it can be tuned. |
| **S7** | CRC32 is a poor hash function for probing | Switched to `ahash` (AHash) for hash table probing -- fast, well-distributed, and hardware-accelerated where available. |
| **S8** | 10x performance target ungrounded | Added profiling step (Phase 0) before any implementation. Performance targets now qualified by measured bottleneck distribution. |

Other suggestions noted but deferred: S2 (Float64 column) tracked as future work; S3 (simd-json mutable buffer) noted in JSONL section; S6 (adapter overhead) addressed by adding benchmark gates at each phase boundary.

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
| Hash function | `ahash` crate (AHash) | Fast, well-distributed, uses hardware AES-NI where available; suitable for hash table probing unlike CRC32 |

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
}
```

Using `Vec<u64>` instead of the `bitvec` crate gives us direct word-level access, which is natural for SIMD-width processing (64 rows per word, matching AVX-512's width, or 4 words per AVX2 pass). Bitwise AND/OR/NOT across bitmaps process 64 rows per instruction.

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

    // --- Domain-specific structured types (ELB/ALB fast path) ---
    Host {
        // Decomposed for efficient access: hostname as Utf8 + port as u16
        hostnames: Vec<u8>,       // concatenated hostname bytes
        hostname_offsets: Vec<u32>,
        ports: Vec<u16>,
        null: Bitmap,
        missing: Bitmap,
    },
    HttpRequest {
        // Decomposed: method (enum tag) + url (Utf8) + version (enum tag)
        methods: Vec<u8>,          // enum tag: 0=GET,1=POST,2=DELETE,3=HEAD,4=PUT,5=PATCH
        urls: Vec<u8>,             // concatenated URL bytes
        url_offsets: Vec<u32>,
        versions: Vec<u8>,         // enum tag: 0=HTTP/1.0,1=HTTP/1.1,2=HTTP/2.0
        null: Bitmap,
        missing: Bitmap,
    },

    // --- Fallback for heterogeneous/complex data ---
    Mixed {
        data: Vec<Value>,  // handles Object, Array, and heterogeneous columns
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

**Host and HttpRequest columns.** These are the domain-specific types from `Value::Host` and `Value::HttpRequest`. ELB logs have 2 Host fields and 1 HttpRequest field; ALB logs have 2 Host fields and 1 HttpRequest field. Rather than dropping these into `Mixed` (which would negate SIMD benefit for logq's primary use case), we decompose them into flat arrays:

- `Host` is decomposed into hostname (Utf8-style) + port (u16 array). This enables SIMD-accelerated hostname string matching and port range filtering.
- `HttpRequest` is decomposed into method (u8 enum tag) + URL (Utf8-style) + version (u8 enum tag). This enables SIMD filtering on method (integer equality on enum tags), URL substring matching, and version filtering.

Path expressions that access sub-fields (e.g., `request.url`, `client_and_port.hostname`) can read directly from the decomposed arrays without reconstructing the composite type.

**Object and Array in Mixed.** `Value::Object` and `Value::Array` (used by JSONL) remain in the `Mixed` fallback column. Impact analysis:

| Format | Fields hitting Mixed | Total fields | Mixed % | Impact |
|---|---|---|---|---|
| ELB | 0 (Host/HttpRequest now typed) | 17 | 0% | Full SIMD benefit |
| ALB | 0 (Host/HttpRequest now typed) | 25 | 0% | Full SIMD benefit |
| S3 | 0 (all String) | 24 | 0% | Full SIMD benefit |
| Squid | 0 (all String) | 10 | 0% | Full SIMD benefit |
| JSONL (flat) | 0 (inferred as Int32/Float32/Utf8/Boolean per key) | varies | 0% | Full SIMD benefit |
| JSONL (nested) | only nested Object/Array fields | varies | varies | Partial -- top-level primitives get SIMD, nested structures fall back |

For JSONL with nested objects, the `Mixed` column is used only for fields that actually contain Object or Array values. Top-level primitive fields (strings, numbers, booleans) are still placed into typed columns via schema inference. This means even JSONL queries get SIMD benefit for the common case of filtering/aggregating on primitive fields.

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
    /// Used by materializing operators.
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

**Non-materializing operators** (Filter, Project, Limit, Union) propagate or narrow the selection vector without touching the underlying column data. Downstream operators only process active rows. This avoids expensive compaction and is how Velox handles it.

**Materializing operators** (GroupBy, OrderBy, Distinct, Join, set operations) must read all their input before producing output. These operators:
1. Compact their input batches on consumption -- apply the selection vector to produce dense active-rows-only data before inserting into their internal data structures (hash tables, sort buffers).
2. Emit output batches with `SelectionVector::All` -- output is always fully materialized.

**Compaction threshold for non-materializing path.** If a filter reduces the active ratio below ~25% (fewer than `BATCH_SIZE / 4` active rows), the `BatchFilterOperator` compacts the batch into a new dense batch before passing it downstream. This avoids pathological iteration overhead when most rows are inactive. The compaction copies only the active rows into a new, smaller batch and resets the selection to `All`.

### Fallback Path

The `Mixed` column type and any operator that encounters unsupported types falls back to a per-row loop over the batch, calling the existing scalar `Value`-based logic. This means no correctness risk -- SIMD accelerates the common case, scalar handles the rest.

## Section 3: SIMD Strategy -- Auto-Vectorization First

The previous design proposed writing raw `std::arch` intrinsics across 4 backends (SSE2, AVX2, NEON, scalar). That approach is impractical for a single-developer project: roughly 40 unsafe function implementations, each needing independent testing across architectures.

Instead, we adopt a layered approach:

### Layer 1: Auto-Vectorization (all kernels)

Write kernels as simple, tight Rust loops that LLVM can auto-vectorize. Compile with `--target-cpu=native` (for local builds) or a reasonable baseline like `x86-64-v3` (AVX2-capable) for distributed binaries.

```rust
/// LLVM auto-vectorizes this into SIMD instructions when compiled with
/// --target-cpu=native or -C target-feature=+avx2.
pub fn filter_ge_i32(data: &[i32], threshold: i32, result: &mut [u64]) {
    for (i, &val) in data.iter().enumerate() {
        let word = i / 64;
        let bit = i % 64;
        if val >= threshold {
            result[word] |= 1u64 << bit;
        }
    }
}

/// Sum with selection vector awareness.
pub fn sum_i32_selected(data: &[i32], selection: &[u64]) -> i64 {
    let mut total: i64 = 0;
    for (i, &val) in data.iter().enumerate() {
        let word = i / 64;
        let bit = i % 64;
        if (selection[word] >> bit) & 1 == 1 {
            total += val as i64;
        }
    }
    total
}
```

Many of the proposed kernels (integer comparison, addition, sum, min/max, bitmap AND/OR/NOT) auto-vectorize well. The compiler generates SSE2/AVX2 on x86_64 and NEON on aarch64 without any unsafe code.

To verify auto-vectorization, add a CI step that compiles kernels with `--emit=asm` and checks for vector instructions (e.g., `vpcmpgtd`, `vpaddd` on x86_64). This is a one-time verification, not an ongoing maintenance burden.

### Layer 2: Targeted `#[target_feature]` Functions (string search only)

The one kernel that does not auto-vectorize well is string substring search (`str_contains` / LIKE '%pattern%'). Variable-length strings with offset arrays, boundary checking across SIMD lanes, and the first+last character broadcast trick require manual SIMD.

For this kernel only, we use `#[target_feature]` annotations with runtime dispatch:

```rust
/// Runtime dispatch: check CPU features once at startup, store function pointer.
pub struct SimdStringKernels {
    str_contains_fn: fn(&[u8], &[u32], &[u8], &mut [u64]),
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
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u64]
) {
    // AVX2 implementation: broadcast first+last needle byte,
    // scan 32-byte chunks, extract match bitmask.
    // Boundary-safe: each string bounded by offsets[i]..offsets[i+1].
    // ...
}

/// Scalar fallback: always correct, works everywhere.
fn str_contains_scalar(
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u64]
) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let hay = &haystack[start..end];
        let word = i / 64;
        let bit = i % 64;
        if hay.windows(needle.len()).any(|w| w == needle) {
            result[word] |= 1u64 << bit;
        }
    }
}
```

**Runtime dispatch** uses `is_x86_feature_detected!()` at startup (called once, stored in a function pointer). This fixes the compile-time-only dispatch problem from v1: a binary compiled on a basic x86_64 machine will still use AVX2 at runtime if the CPU supports it, and will not crash on older CPUs.

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
- No `_mm_crc32_u32` usage (this was an SSE4.2 instruction incorrectly attributed to SSE2 in v1; we use AHash instead)

## Section 4: SIMD Kernels -- Filtering

### BatchFilterOperator Core Loop

```rust
impl BatchStream for BatchFilterOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(mut batch) = self.child.next_batch()? {
            self.predicate.evaluate_batch(&mut batch)?;
            if batch.selection.any_active() {
                // Compact if active ratio < 25%
                let active = batch.selection.count_active(batch.len);
                if active < batch.len / 4 {
                    batch = batch.selection.compact(&batch);
                }
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}
```

### Predicate-to-Kernel Mapping

| Predicate pattern | Kernel | Notes |
|---|---|---|
| `received_bytes >= 1000` | `filter_ge_i32(data, 1000, bitmask)` | Auto-vectorized; works on the actual integer fields (received_bytes, sent_bytes) |
| `elb_status_code = '200'` | `str_eq_batch(bytes, offsets, needle, bitmask)` | String equality on status codes (which are `DataType::String` in the schema) |
| `request.url LIKE '%/api%'` | `str_contains(bytes, offsets, needle, bitmask)` via `SimdStringKernels` | Runtime-dispatched SIMD string search on HttpRequest.urls |
| `request.http_method = 'GET'` | `filter_eq_u8(methods, 0u8, bitmask)` | Integer equality on HttpRequest method enum tag (0=GET) |
| `col IN (200, 301, 404)` | `filter_in_i32(data, &hashset, bitmask)` | Auto-vectorized hash + lookup |
| `a AND b` | `bitmap_and(left, right)` | Word-by-word u64 AND; auto-vectorized |
| `a OR b` | `bitmap_or(left, right)` | Word-by-word u64 OR |
| `IS NULL` | Check: `missing` bit=1 AND `null` bit=0 | `and(missing, not(null))` -- three bitmap ops |
| `IS MISSING` | Check: `missing` bit=0 | `not(missing)` -- single bitmap op |
| `IS NOT NULL AND IS NOT MISSING` | Check: `missing` bit=1 AND `null` bit=1 | `and(missing, null)` |

**ELB/ALB field type breakdown.** The primary SIMD benefit for structured log formats comes from string operations and tokenization, not integer filtering. Here is the actual field type distribution:

| Format | String fields | Float fields | Int fields | DateTime fields | Host fields | HttpRequest fields | Total |
|---|---|---|---|---|---|---|---|
| ELB | 8 | 3 | 2 | 1 | 2 | 1 | 17 |
| ALB | 15 | 3 | 2 | 1 | 2 | 1 | 25* |
| S3 | 24 | 0 | 0 | 0 | 0 | 0 | 24 |
| Squid | 10 | 0 | 0 | 0 | 0 | 0 | 10 |

*ALB note: `matched_rule_priority` and `request_creation_time` are schema-typed as String even though they contain numeric/datetime-like content.

This means the most impactful SIMD kernels are (in priority order):
1. **Tokenization** -- SIMD delimiter scanning in `BatchScanOperator` (applies to every field of every row)
2. **String equality/substring** -- `str_eq_batch`, `str_contains` (covers status codes, method names, URLs, user agents)
3. **Float comparison** -- processing time filters (3 float fields in ELB/ALB)
4. **Integer comparison** -- byte count filters (2 integer fields in ELB/ALB)
5. **Host/HttpRequest decomposed access** -- enum tag comparison, hostname/URL string matching

### Compound Predicate Short-Circuit

Evaluate the left side first, then pass the narrowed `SelectionVector` to the right side. If the left side eliminates 90% of rows, the right side only touches 10%.

### Fallback

If a predicate involves `TypedColumn::Mixed` or an unsupported expression shape (e.g. `function(col) > value`), it falls back to per-row scalar evaluation over the batch.

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

### Typed Field Parsing

For structured formats (ELB, ALB, S3, Squid), the schema is fixed so each field index maps to a known type:

| Field type | Parsing strategy | TypedColumn target |
|---|---|---|
| Integer | Scalar `str::parse::<i32>()` (already fast; auto-vectorization of atoi is unreliable) | `Int32` |
| Float | Scalar `str::parse::<f32>()` or `fast_float` crate | `Float32` |
| DateTime | Existing `parse_utc_timestamp` byte-level parser (already optimized, confirmed by reviewer) | `DateTime` |
| String | Copy bytes into owned `Vec<u8>` buffer, record offset | `Utf8` |
| Host | Scalar parse via `common::types::parse_host`, decompose into hostname bytes + port | `Host` |
| HttpRequest | Scalar parse via `common::types::parse_http_request`, decompose into method tag + URL bytes + version tag | `HttpRequest` |

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

Uses `simd-json` crate for SIMD-accelerated JSON parsing, then distributes parsed values into typed columns based on the inferred schema. Note: `simd-json` requires mutable access to the input buffer (it modifies the input in-place for zero-copy parsing), so the JSONL scan path will pass owned/mutable `Vec<u8>` buffers instead of borrowed `&str`.

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
  Hash Table Probe & Insert
        |
        v
  Vectorized Aggregate Update
        |
        v
  Emit result batches with SelectionVector::All
```

The operator compacts its input (applies the selection vector to produce dense data) before hashing. Output batches always have `SelectionVector::All`.

### Hash Computation with AHash

The previous design proposed hardware CRC32 (`_mm_crc32_u32`, an SSE4.2 instruction incorrectly attributed to SSE2). CRC32 has poor distribution properties for hash table probing, leading to high collision rates.

Instead, we use the `ahash` crate (AHash):
- Uses AES-NI instructions when available (runtime-detected), achieving near-CRC32 throughput
- Excellent distribution properties -- designed for hash tables
- Falls back to a fast software implementation on CPUs without AES-NI
- Zero unsafe code in our codebase (the crate handles it internally)

```rust
use ahash::AHasher;
use std::hash::{Hash, Hasher};

fn hash_column_i32(data: &[i32], hashes: &mut [u64]) {
    for (i, &val) in data.iter().enumerate() {
        let mut hasher = AHasher::default();
        val.hash(&mut hasher);
        hashes[i] = hasher.finish();
    }
}

// For multi-column keys, chain hashes:
fn hash_combine(existing: &mut [u64], new: &[u64]) {
    for (h, &n) in existing.iter_mut().zip(new.iter()) {
        *h = h.wrapping_mul(0x517cc1b727220a95).wrapping_add(n);
    }
}
```

### Hash Table -- Swiss Table Pattern

Adopted from Velox's `HashTable.cpp` Swiss Table pattern:

- Separate **tag array** (1 byte per slot, 7-bit hash tag + high bit marker) from **payload array** (group index)
- Probe loads 16 tags at once, compares against the wanted tag, extracts match bitmask
- Each probe checks 16 slots in a few instructions
- On match, verify the actual key; on empty, insert new group

The tag-matching inner loop is auto-vectorizable (it is a byte-comparison loop over 16 consecutive bytes), so we do not need manual SIMD intrinsics here.

### Vectorized Aggregate Accumulators

Flat arrays indexed by group ID, replacing `HashMap<key, accumulator>`:

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

The `BatchProjectOperator` propagates the input selection vector without compaction. It only evaluates expressions for active rows (via selection-aware kernels) or evaluates all rows and lets the selection vector mask the results (simpler, often faster due to branch-free SIMD).

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

### Null and Missing Propagation

Follows PartiQL semantics:
- **Arithmetic/comparison with NULL:** `NULL + 5 = NULL`. Implemented by ORing the null bitmaps of both operands: `result.null = a.null AND b.null` (if either is null, result is null). This is a single word-by-word `AND` per 64 rows.
- **Arithmetic/comparison with MISSING:** `MISSING + 5 = MISSING`. Implemented similarly: `result.missing = a.missing AND b.missing`.
- **Combined:** `result_valid = a.missing AND b.missing AND a.null AND b.null`. Three bitmap operations per 64 rows.

### Fallback

Expressions involving `TypedColumn::Mixed`, unported `Function` calls, or `Subquery` nodes fall back to row-at-a-time evaluation by iterating the batch and calling existing `expression_value()`.

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

- Add `src/simd/` module with auto-vectorizable kernels and runtime-dispatched string search
- Add `ColumnBatch`, `TypedColumn` (with NULL + MISSING bitmaps), `SelectionVector`, `Bitmap` types in `src/execution/batch.rs`
- Add `BatchStream` trait alongside existing `RecordStream`
- Write unit tests for all kernels against scalar reference implementations
- Verify auto-vectorization via `--emit=asm` spot checks in CI

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
| 1 | `BatchScanOperator` | Entry point; SIMD tokenization benefits every query |
| 2 | `BatchFilterOperator` | Second biggest cost center; string comparison SIMD |
| 3 | `BatchProjectOperator` | Completes SELECT-FROM-WHERE path |
| 4 | `BatchGroupByOperator` | Unlocks AHash-based vectorized aggregation |
| 5 | `BatchLimitOperator` | Trivial -- truncate selection vector |
| 6 | `BatchOrderByOperator` | Vectorized sort keys |
| 7 | `BatchDistinctOperator` | Reuses GroupBy's hash table |
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

Benchmarks in `benches/bench_execution.rs` track progress at each phase boundary. Each phase must show measurable improvement over the previous phase on at least one benchmark category before proceeding to the next.
