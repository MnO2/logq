# Hash Join Optimization Design (v2)

**Date:** 2026-04-07
**Goal:** Replace logq's O(|L| x |R| x disk I/O) nested-loop joins with hash joins, inspired by velox's execution engine. Deliver a phased upgrade from basic hash join to bloom-filter-enhanced, batch-integrated join execution.

---

## Changes from previous version

This revision addresses all critical issues and incorporates suggestions from the design review.

### Critical fixes

| # | Issue | Resolution |
|---|-------|------------|
| C1 | `Vec<Value>` as HashMap key is a performance trap | Specialized `JoinKey` enum: single-string keys hash raw bytes via `ahash`; single-integer keys use `i32` directly; multi-column/exotic keys fall back to `Vec<Value>`. See Section 1.3. |
| C2 | Design claims SIMD intrinsics that do not exist in the codebase | Phase 2 no longer claims a custom SIMD tag-based hash table. Instead, `hashbrown::HashMap` (which uses SSE2/NEON Swiss Table probing internally) is adopted for Phase 1 and retained as the primary hash table for Phase 2. An optional explicit-intrinsics path using `std::arch` is described as a future exploration, not a baseline. See Section 2.1. |
| C3 | Build side hardcoded to right | Added build-side selection heuristic for INNER JOIN (build from smaller side, using file size as proxy). LEFT JOIN always builds right (semantics require it). RIGHT JOIN implemented by swapping inputs and running as LEFT JOIN. See Section 1.4. |
| C4 | Equi-predicate extraction at wrong layer | Added a proper logical plan optimizer pass (`optimize_joins`) that rewrites `LeftJoin`/`CrossJoin` into a new `HashJoin` logical `Node` variant before `physical()` is called. `physical()` remains a 1:1 structural translation. See Section 1.1. |
| C5 | No memory limit for build side | Added configurable memory budget (`--join-memory-limit`, default 512 MB) with a clear error when exceeded. Spill-to-disk noted as future work. See Section 1.6. |
| C6 | i32 range overflow in Array mode | Range computation specified as `i64` arithmetic: `(max as i64) - (min as i64) < 2_000_000`. See Section 3.2. |

### Incorporated suggestions

| # | Suggestion | Resolution |
|---|-----------|------------|
| S1 | Move materialize-once fallback to Step 1 | Done. It is now Step 1 in the implementation order -- the highest-ROI, zero-risk change ships first. See Section 1.8 and the Implementation Order table. |
| S2 | Bloom filter hash quality requirements | Bloom filter specified to use `ahash` (AES-NI accelerated) rather than the existing multiply-shift kernel hash. See Section 2.2. |
| S3 | NormalizedKey endianness-aware packing | Specified big-endian byte packing for integer keys and zero-padded fixed-length prefix for strings. Mixed-type packing rules documented. See Section 3.2. |
| S4 | Use `hashbrown::HashMap` for Phase 1 | Done. `hashbrown` is already a dependency (`Cargo.toml`), used throughout the codebase. The custom SIMD hash table from v1 Phase 2 is removed; `hashbrown` provides Swiss Table SIMD probing internally. See Section 1.3. |
| S5 | Dynamic filter pushdown backward communication | Specified `Arc<Mutex<Option<DynamicFilter>>>` shared reference between join operator and upstream scan, set after build completes. Scan checks the slot before each batch. See Section 4.2. |
| S6 | Keep nested-loop behind a flag for regression testing | Added `--force-nested-loop` CLI flag. Both paths remain available; correctness tests run both and diff output. See Section 5. |
| S7 | RIGHT JOIN implementation strategy | RIGHT JOIN implemented by swapping left/right inputs and running as LEFT JOIN, then reversing column order in output. See Section 1.5. |

---

## Background

logq currently supports CROSS JOIN and LEFT JOIN via nested-loop streams (`CrossJoinStream`, `LeftJoinStream` in `execution/stream.rs`). The right side is re-opened and re-parsed from disk for every left row. For any non-trivial data sizes, this is prohibitively slow.

velox (Meta's C++ execution engine) implements a sophisticated hash join with adaptive hash modes, SIMD tag-based probing, bloom filter pushdown, dynamic filter pushdown, parallel build, and spill-to-disk. This design adapts the most impactful techniques for logq's Rust codebase and log-querying use case.

---

## Phase 1: Basic Hash Join

### 1.1 Logical Plan Optimizer Pass for Join Rewriting

Add a new optimizer pass `optimize_joins` in `logical/optimizer.rs` that transforms the logical plan tree before `physical()` is called. This pass:

1. Walks the logical `Node` tree.
2. When it encounters a `LeftJoin(left, right, condition)` or `CrossJoin(left, right)` with a downstream `Filter` containing equi-predicates:
   - Calls `extract_equi_predicates(condition)` to separate equality predicates (`left_field = right_field`) from residual predicates.
   - If equi-keys are found, rewrites the node into a new `HashJoin` logical node variant.
   - If no equi-keys are found, leaves the node unchanged (nested-loop path).
3. For `CrossJoin + Filter(equi_condition, CrossJoin(...))`, the pass detects the pattern and folds the filter into the join, producing `HashJoin` with `join_type: Inner`.

**New logical Node variant:**

```rust
// In logical/types.rs
pub(crate) enum Node {
    // ... existing variants ...
    HashJoin {
        left: Box<Node>,
        right: Box<Node>,
        equi_keys: Vec<(String, String)>,  // (left_field, right_field)
        residual: Option<Box<Formula>>,
        join_type: JoinType,               // Inner, Left, Right
    },
}
```

**Equi-predicate extraction** (`extract_equi_predicates` in `logical/optimizer.rs`):

Input: a `Formula` representing the ON condition.
Output: `(Vec<(String, String)>, Option<Formula>)` -- equi-key pairs (left_field, right_field) and optional residual.

The extraction walks the formula tree, collecting `Equal(FieldAccess(left), FieldAccess(right))` conjuncts from top-level ANDs. Each field reference is checked against the left vs. right input schemas to ensure the pair spans both sides. Anything that is not a cross-side field equality is kept as residual.

**Why a separate pass instead of doing this in `physical()`:** The `physical()` method in `logical/types.rs` performs a 1:1 structural translation from logical to physical nodes. Adding predicate inspection and conditional operator selection there would break that clean separation. A dedicated optimizer pass keeps the architecture clean: optimizer rewrites the plan, then `physical()` translates it structurally.

### 1.2 INNER JOIN Syntax

Add `Inner` and `Right` to the `JoinType` enum in `syntax/ast.rs`. Update the parser in `syntax/parser.rs` to recognize:
- `[INNER] JOIN ... ON ...` (INNER is optional when bare `JOIN` is used)
- `RIGHT [OUTER] JOIN ... ON ...`

### 1.3 HashJoinStream with Specialized Key Hashing

New struct in `execution/stream.rs`. Uses `hashbrown::HashMap` (already a crate dependency used throughout the codebase) for consistency and because hashbrown internally uses Swiss Table with SSE2/NEON SIMD probing.

**Key specialization via `JoinKey` enum:**

The v1 design's `HashMap<Vec<Value>, Vec<Record>>` is a performance trap. `Value` is an 11-variant enum whose derived `Hash` must recursively hash nested `Object` and `Array` variants. For the dominant use case (joining on a single string or integer field), this is orders of magnitude slower than necessary.

```rust
/// Specialized join key for efficient hashing.
/// Single-column keys avoid Vec allocation and Value enum dispatch.
enum JoinKey {
    /// Single string key: hash raw bytes via ahash.
    SingleString(String),
    /// Single integer key: use i32 directly, zero-cost hash.
    SingleInt(i32),
    /// Multi-column or exotic types: fall back to Vec<Value>.
    /// This path is slower but handles arbitrary key combinations.
    Composite(Vec<Value>),
}
```

Key extraction from a `Record`:

```rust
fn extract_key(record: &Record, key_fields: &[String]) -> JoinKey {
    if key_fields.len() == 1 {
        match record.get(&key_fields[0]) {
            Some(Value::String(s)) => JoinKey::SingleString(s.clone()),
            Some(Value::Int(i)) => JoinKey::SingleInt(*i),
            _ => JoinKey::Composite(vec![record.get(&key_fields[0]).cloned()
                                         .unwrap_or(Value::Missing)]),
        }
    } else {
        JoinKey::Composite(
            key_fields.iter()
                .map(|f| record.get(f).cloned().unwrap_or(Value::Missing))
                .collect()
        )
    }
}
```

`JoinKey` implements `Hash` and `Eq` manually: `SingleString` hashes the raw `&[u8]` bytes; `SingleInt` hashes the `i32` directly; `Composite` hashes each `Value` in sequence. The three variants are given distinct hash prefixes to avoid cross-variant collisions.

**HashJoinStream struct:**

```rust
struct HashJoinStream {
    left: Box<dyn RecordStream>,
    hash_table: hashbrown::HashMap<JoinKey, Vec<Record>>,  // build side
    join_type: JoinType,
    equi_keys: Vec<(String, String)>,       // (left_field, right_field)
    left_key_fields: Vec<String>,           // pre-extracted left field names
    right_key_fields: Vec<String>,          // pre-extracted right field names
    residual: Option<Formula>,
    // iteration state
    current_left: Option<Record>,
    current_matches: Vec<Record>,
    match_index: usize,
    matched: bool,                          // for LEFT JOIN NULL padding
    right_field_names: Vec<String>,         // for NULL padding
    build_side_bytes: usize,                // tracked memory usage
    memory_limit: usize,                    // configurable budget
}
```

**Execution:**

1. On first call to `next()`, fully materialize the build side into the `hashbrown::HashMap`. Key is `JoinKey` extracted from build-side equi-join columns. Value is `Vec<Record>` of all rows with that key. Track `build_side_bytes` during materialization (see Section 1.6).
2. For each probe-side row, extract the join key, look up in the hash map.
3. For each match, merge left + right records. Apply residual filter if present.
4. For LEFT JOIN, if no match found, emit NULL-padded right side.

### 1.4 Build-Side Selection Heuristic

The v1 design always builds the hash table from the right side. This is correct for LEFT JOIN (semantics require iterating all left rows and NULL-padding unmatched ones, so the right side must be the build side). But for INNER JOIN, building from the larger side wastes memory and destroys performance.

**Heuristic for INNER JOIN:**

Before constructing `HashJoinStream`, estimate relative sizes of left and right inputs:

1. If both inputs are file scans (`DataSource::File`), use file size as a proxy for row count. Build from the smaller file.
2. If one input is a file scan and the other is a derived query (filter, subquery), assume the derived query is smaller (it has been filtered). Build from the derived side.
3. If both are derived, build from the right side (default).

When swapping is needed, the optimizer pass swaps the left/right children and transposes the equi-key pairs (swap each `(left_field, right_field)` to `(right_field, left_field)`).

**For LEFT JOIN:** Always build right. No swapping. If the right side is the large table, the user must rewrite their query or accept the memory cost.

**For RIGHT JOIN:** See Section 1.5.

### 1.5 RIGHT JOIN Implementation

RIGHT JOIN is implemented by input swapping:

1. The optimizer pass rewrites `RightJoin(left, right, condition)` as `HashJoin { left: right, right: left, equi_keys: transposed, join_type: Left }`.
2. After the join produces output rows, a post-processing step reverses the column order so that the original left-side columns appear first and the original right-side columns appear second.
3. This reuse of LEFT JOIN logic means RIGHT JOIN gets correct NULL-padding behavior for free -- the original left side (now probe side) gets NULL-padded when unmatched.

This approach avoids duplicating NULL-padding logic and is how most production databases implement RIGHT JOIN.

### 1.6 Memory Budget for Build Side

Materializing the entire build side into memory with no budget will OOM when joining two large log files.

**Configuration:**
- CLI flag: `--join-memory-limit <MB>` (default: 512 MB).
- Environment variable: `LOGQ_JOIN_MEMORY_LIMIT_MB`.

**Enforcement:**

During the build phase, track cumulative memory usage by estimating the size of each `Record` inserted into the hash table (sum of field value sizes: 4 bytes per `Int`, string length + 24 bytes per `String`, etc., plus `HashMap` overhead estimate of 64 bytes per entry).

When `build_side_bytes` exceeds the configured limit:

```
Error: Build side of join exceeded memory limit of 512 MB.
  The right side of the join contains too many rows to fit in memory.
  Suggestions:
  - Add filters to reduce the right side before the join.
  - For INNER JOIN, rewrite the query so the smaller table is on the right.
  - Increase the limit with --join-memory-limit <MB>.
```

**Spill-to-disk:** Deferred to a future phase. The design acknowledges that for joining two multi-GB log files, spill-to-disk (partitioned hash join with temporary files) will eventually be needed. The memory budget with clear error messaging is the Phase 1 solution.

### 1.7 Physical Plan Wiring

In `logical/types.rs` (`Node::physical()`), add a branch for the new `HashJoin` logical node that performs a 1:1 translation to `execution::Node::HashJoin { left, right, equi_keys, residual, join_type }`. No predicate inspection happens here -- that was already done by the optimizer pass.

In `execution/types.rs` (`Node::get()`), construct `HashJoinStream` from the `HashJoin` physical node, passing the configured memory limit.

### 1.8 Materialize-Once Fallback for Nested-Loop Joins

**This is Step 1 in the implementation order** (moved up from Step 5 per review feedback).

For non-equi joins that must remain nested-loop, fix the existing `CrossJoinStream` and `LeftJoinStream` to materialize the right side into a `Vec<Record>` once on first iteration, instead of re-creating the stream from disk per left row. This eliminates the disk I/O multiplier immediately.

```rust
// In CrossJoinStream / LeftJoinStream:
// Before (current): self.right_node.get(...) called per left row
// After: self.right_rows: Option<Vec<Record>>, populated on first iteration
```

This is a low-risk structural change that immediately improves all join performance, even before equi-predicate extraction or hash join logic is ready. It has zero dependencies on any other step.

### 1.9 Complexity

- Build: O(|B|) time and space, where B is the build side.
- Probe: O(|P|) amortized (O(|P| x matches_per_key) worst case for many-to-many).
- vs. current: O(|L| x |R| x disk I/O).

---

## Phase 2: Bloom Filter + Hash Table Optimization

### 2.1 Hash Table Strategy

**v1 proposed** a custom SIMD tag-based hash table with explicit `u8x16` intrinsics, claiming it could leverage "existing SIMD infrastructure" in `simd/kernels.rs`. This was incorrect: the existing SIMD infrastructure consists entirely of scalar loops that rely on LLVM auto-vectorization. There are no explicit SIMD intrinsics anywhere in the codebase.

**v2 approach:** Use `hashbrown::HashMap` as the hash table for all phases. `hashbrown` is Rust's port of Google's Swiss Table (Abseil `flat_hash_map`). It uses SSE2 intrinsics on x86-64 and NEON intrinsics on ARM internally for the 16-byte tag group probing. This gives us the same SIMD-accelerated probing that the v1 design tried to build from scratch, without any unsafe code or portability burden in our codebase.

This decision is consistent with the SIMD physical plan design (`2026-04-06-simd-physical-plan-design-final.md`), which already concluded that `hashbrown` should be used instead of a custom Swiss Table.

**Future exploration (not Phase 2 scope):** If benchmarking shows that `hashbrown`'s general-purpose hash table is a bottleneck for join-specific workloads, a specialized join hash table could be built using `std::arch` intrinsics (e.g., `_mm_cmpeq_epi8` on x86-64, `vceqq_u8` on ARM). This would require:
- Feature-gated modules (`#[cfg(target_arch = "x86_64")]` and `#[cfg(target_arch = "aarch64")]`)
- A scalar fallback for other architectures
- Careful benchmarking to justify the complexity

This is noted as potential Phase 5 work, not a Phase 2 deliverable.

### 2.2 Bloom Filter (`simd/bloom_filter.rs`)

Split-block bloom filter design (matching velox):
- 64-bit blocks.
- 4 hash functions derived from 24 bits of the hash code (4 groups of 6 bits each selecting a bit position within the 64-bit block).
- Block selection via bits 24+ of the hash code.
- Target: under 2% false positive rate at 8 bits per entry.

**Hash quality requirement:** The bloom filter must use a high-quality hash function. The existing `hash_column_i32` in `simd/kernels.rs` uses a simple multiplicative hash (`wrapping_mul(HASH_MULT)`) that does not distribute well across upper bits. The bloom filter uses `ahash` (AES-NI accelerated on x86-64, fallback on other architectures) for all key types. This is the same hasher used internally by `hashbrown`, so hashes computed for hash table insertion can be reused for bloom filter insertion (single hash computation, dual use).

```rust
struct BloomFilter {
    blocks: Vec<u64>,
    num_bits: usize,
}

impl BloomFilter {
    /// Create a bloom filter sized for the expected number of entries.
    /// Uses 8 bits per entry for ~2% false positive rate.
    fn new(expected_entries: usize) -> Self;

    /// Insert a pre-computed ahash hash value.
    fn insert(&mut self, hash: u64);

    /// Test membership using a pre-computed ahash hash value.
    fn might_contain(&self, hash: u64) -> bool;
}
```

### 2.3 Bloom Filter Pushdown to Scan

After `HashJoinStream` (or `BatchHashJoinOperator`) builds the hash table, it constructs a `BloomFilter` from the build-side key hashes.

The bloom filter is injected into the upstream scan pipeline. In the batch path:

```
BatchScan -> [BloomFilterProbe] -> BatchFilter -> BatchProject -> BatchHashJoinProbe
```

`BloomFilterProbe` is a lightweight batch operator that:
1. Hashes the join key column of each batch using `ahash`.
2. Tests each hash against the bloom filter.
3. Updates the `SelectionVector` to exclude non-matching rows.

This eliminates rows before they reach the join, which is especially valuable for logq's primary use case: large log file joined against a small dimension table.

---

## Phase 3: Adaptive Hash Mode Selection

### 3.1 Key Statistics Collection

During the build phase, collect statistics on join key columns:
- `min` and `max` values (for integer keys, stored as `i64` to avoid overflow)
- `distinct_count` (via a `HashSet` capped at 100K entries)
- `total_key_bits` (sum of bit widths of all key columns)

This adds negligible overhead since every build-side row is already iterated during build.

### 3.2 Three Hash Modes

**Array mode:**
- Condition: single integer key, range < 2M (computed in `i64` arithmetic to avoid overflow), density (`distinct_count / range`) > 0.1.
- Range computation: `let range: i64 = (max as i64) - (min as i64);` then `range < 2_000_000`. This handles the full `i32` range without overflow (`i32::MAX as i64 - i32::MIN as i64 = 4_294_967_295`, which correctly exceeds 2M and disqualifies Array mode).
- Implementation: `Vec<Option<RowIndex>>` indexed by `(value as i64 - min as i64) as usize`.
- Probe: single array dereference, no hashing.
- Ideal for: HTTP status codes, port numbers, small error code tables.

**NormalizedKey mode:**
- Condition: all key columns fit in 64 bits or fewer total.
- Implementation: pack all key columns into a single `u64` stored alongside each row. Probe compares the `u64` first; full key comparison only on match.
- **Packing rules (endianness-aware, big-endian byte order for consistent ordering):**
  - `i32`: cast to `u32` via XOR with `0x80000000` (flip sign bit for order preservation), then store as 4 big-endian bytes.
  - `u16`/`u32`: store as big-endian bytes.
  - `bool`: 1 byte (0x00 or 0x01).
  - Short strings (up to N bytes where N = remaining bits / 8): store length as 1 byte, then up to N-1 bytes of content, zero-padded. Strings longer than the available space disqualify NormalizedKey mode for that key set.
- Columns are packed left-to-right into the `u64` in declaration order, most significant byte first.
- Ideal for: multi-column joins like `(host_id u32, status u16)`.

**Hash mode (hashbrown):**
- Condition: fallback for string keys, high-cardinality keys, or keys exceeding 64 bits.
- Implementation: `hashbrown::HashMap` with `JoinKey` specialization (Phase 1's table, retained).
- Ideal for: joining on URL paths, log messages, or other string fields.

### 3.3 Decision Logic

```
collect key stats during build
  |
  +-- single integer key AND (max as i64 - min as i64) < 2M AND density > 0.1 -> Array mode
  +-- all keys fit in <= 64 bits total (no variable-length strings)             -> NormalizedKey mode
  +-- otherwise                                                                 -> Hash mode (hashbrown)
```

The mode is selected after the build phase completes (since statistics are needed). For Array and NormalizedKey modes, the build-side data is re-indexed into the optimized structure. This is a one-time cost amortized over all probe-side rows.

---

## Phase 4: Batch-Level Integration + Dynamic Filter Pushdown

### 4.1 BatchHashJoinOperator (`execution/batch_join.rs`)

A new `BatchStream` implementation for hash joins in the columnar pipeline.

**Build phase:** Same as row-based -- materialize build side into the adaptive hash table. The build side is typically small, so row-based build is acceptable.

**Probe phase:** Operates on `ColumnBatch` from the probe side:
1. Extract the join key column as a typed slice (e.g., `&[i32]` or string offsets).
2. Hash all keys in the batch using `ahash`.
3. Probe the hash table for each row, producing a match index vector.
4. Gather matched build-side columns into new `TypedColumn`s using the match indices.
5. Combine probe batch columns + gathered build columns into an output `ColumnBatch`.
6. Update the `SelectionVector` to exclude non-matching rows (INNER JOIN) or mark NULL-padded rows (LEFT JOIN).

This keeps the entire pipeline columnar:
```
BatchScan -> BloomFilter -> BatchHashJoinProbe -> BatchFilter -> BatchProject
```

### 4.2 Dynamic Filter Pushdown

After the hash table is built, push knowledge about the build-side key domain back to the scan operator.

**Backward communication mechanism:** The join operator and the upstream scan operator share an `Arc<Mutex<Option<DynamicFilter>>>`. The flow is:

1. During plan construction, an `Arc<Mutex<Option<DynamicFilter>>>` is created and shared between the `BatchHashJoinOperator` and the upstream `BatchScanOperator` (or `BatchFilterOperator`).
2. The scan operator is constructed first (as is normal in the top-down pipeline construction). It holds a reference to the shared slot but initially finds `None`.
3. On first call to `BatchHashJoinOperator::next_batch()`, the build phase executes. After building the hash table and collecting key statistics, the operator constructs a `DynamicFilter` and writes it into the shared slot.
4. On subsequent calls, the scan operator checks the slot before processing each batch. If a `DynamicFilter` is present, it applies it.

```rust
enum DynamicFilter {
    /// Integer key with known range. Inject `key >= min AND key <= max`.
    Range { field: String, min: i64, max: i64 },
    /// Few distinct values. Inject `key IN (v1, v2, ...)`.
    InList { field: String, values: Vec<Value> },
    /// Many distinct values. Use bloom filter test.
    Bloom { field: String, filter: Arc<BloomFilter> },
}
```

**Filter selection:**
- If build side has integer keys with `distinct_count <= 100`: emit `InList`.
- If build side has integer keys with known min/max and `range < 1M`: emit `Range`.
- Otherwise: emit `Bloom`.

Combined with the existing `FilterCache` for low-cardinality string columns, the `InList` filter is very effective for small dimension tables.

---

## Edge Cases and Error Handling

- **NULL join keys:** Follow SQL semantics -- NULL never equals NULL. During build, skip rows with NULL/Missing keys. During probe, NULL keys produce no match (LEFT JOIN emits NULL-padded build side).
- **Empty build side:** Build produces empty hash table. INNER JOIN produces zero rows. LEFT JOIN emits every probe-side row with NULL-padded build columns. Short-circuit: skip probe entirely.
- **Empty probe side:** Stream finishes immediately.
- **Duplicate keys on build side:** Hash table maps each key to a list of row indexes. During probe, all matches are emitted. Handles many-to-many correctly.
- **Mixed equi + non-equi predicates:** Equi part drives hash lookup. Non-equi part becomes residual filter applied after hash probe.
- **Fallback to nested-loop:** If no equi-join predicate detected, use materialize-once nested-loop (Section 1.8). Available behind `--force-nested-loop` flag for all joins.
- **Build side exceeds memory limit:** Clear error with actionable suggestions (see Section 1.6). No silent OOM.
- **RIGHT JOIN with non-equi predicates:** Handled via the swap-to-LEFT-JOIN rewrite (Section 1.5); residual predicates apply identically after the swap.

---

## Testing Strategy

- **Correctness:** Parameterized tests comparing hash join output against nested-loop join output for identical queries. Cover: CROSS, LEFT, INNER (new), RIGHT (new), empty inputs, NULL keys, duplicate keys, multi-column keys, mixed predicates.
- **Regression flag:** The nested-loop path is retained behind `--force-nested-loop`. Correctness tests run each join query twice (hash join and nested-loop) and assert identical output (modulo row ordering for INNER JOIN). This is especially important for LEFT JOIN NULL-padding behavior, which is subtle.
- **Mode selection:** Unit tests verifying Array mode for small integer ranges, NormalizedKey for multi-column fits-in-64-bits, Hash mode otherwise.
- **Key specialization:** Unit tests verifying that `JoinKey::SingleString` and `JoinKey::SingleInt` paths are exercised for single-column joins, and `JoinKey::Composite` for multi-column joins.
- **Memory limit:** Test that exceeding the configured memory budget produces a clear error, not an OOM.
- **Performance benchmarks:** Join 1M-row log file against dimension tables of 10, 1K, and 100K rows. Measure throughput for each hash mode vs. nested-loop baseline.
- **Bloom filter accuracy:** Unit test false positive rate under 3% for expected configurations. Verify that `ahash` is used (not the multiply-shift kernel hash).
- **i64 overflow test:** Test Array mode decision logic with `i32::MIN` and `i32::MAX` keys to verify no overflow in range computation.
- **RIGHT JOIN:** Verify output column order matches SQL semantics (original left columns first, original right columns second) even though internally the inputs are swapped.

---

## Implementation Order

| Step | What | Files | Depends On |
|------|------|-------|------------|
| 1 | **Materialize-once fallback for nested-loop** (highest ROI, zero risk) | `execution/stream.rs` | -- |
| 2 | Add `Inner`, `Right` to `JoinType` enum; parse `JOIN`, `RIGHT JOIN` | `syntax/ast.rs`, `syntax/parser.rs` | -- |
| 3 | Equi-join predicate extraction | `logical/optimizer.rs` | -- |
| 4 | `HashJoin` logical node variant + optimizer pass (`optimize_joins`) | `logical/types.rs`, `logical/optimizer.rs` | 3 |
| 5 | `JoinKey` enum with specialized hashing | `execution/stream.rs` (or new `execution/join.rs`) | -- |
| 6 | `HashJoinStream` with memory budget | `execution/stream.rs` | 2, 4, 5 |
| 7 | Physical plan wiring (logical HashJoin -> physical HashJoin -> stream) | `logical/types.rs`, `execution/types.rs` | 4, 6 |
| 8 | Build-side selection heuristic for INNER JOIN | `logical/optimizer.rs` | 4 |
| 9 | RIGHT JOIN via input swap | `logical/optimizer.rs` | 4 |
| 10 | `--force-nested-loop` flag + regression tests | `main.rs`, tests | 7 |
| 11 | Bloom filter | `simd/bloom_filter.rs` (new) | -- |
| 12 | Bloom filter pushdown into scan | `execution/batch_scan.rs` | 11 |
| 13 | Adaptive mode selection (Array, NormalizedKey, Hash) | `execution/join.rs` or `execution/stream.rs` | 6 |
| 14 | `BatchHashJoinOperator` | `execution/batch_join.rs` (new) | 6, 13 |
| 15 | Dynamic filter pushdown with `Arc<Mutex<Option<DynamicFilter>>>` | `execution/batch_scan.rs`, `execution/batch_join.rs` | 14 |

Steps 1-10 = Phase 1 (biggest impact, all critical fixes addressed).
Steps 11-12 = Phase 2 (bloom filter).
Steps 13-15 = Phase 3/4 (adaptive modes + batch integration).

The materialize-once fallback (Step 1) ships first as it has zero dependencies, zero risk, and immediately eliminates the disk I/O multiplier for all existing join queries.

---

## Non-Goals and Future Work

- **Spill-to-disk:** Not in scope. The memory budget with clear error messaging is the Phase 1 solution. Partitioned hash join with temporary files is future work for multi-GB-to-multi-GB joins.
- **Parallel build:** Not in scope. logq's current execution model is single-threaded. Parallel hash table build would require concurrent `HashMap` construction (e.g., per-partition tables merged post-build).
- **Custom SIMD hash table:** `hashbrown` provides Swiss Table SIMD probing internally. A custom implementation using `std::arch` intrinsics is only justified if benchmarking reveals `hashbrown` as a bottleneck for join-specific workloads. This is potential Phase 5 work.
- **FULL OUTER JOIN:** Not in scope for this design. Can be implemented as a combination of LEFT JOIN + anti-join of the right side.
