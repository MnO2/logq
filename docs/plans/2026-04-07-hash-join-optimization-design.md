# Hash Join Optimization Design

**Date:** 2026-04-07
**Goal:** Replace logq's O(|L| x |R| x disk I/O) nested-loop joins with hash joins, inspired by velox's execution engine. Deliver a phased upgrade from basic hash join to SIMD-optimized, bloom-filter-enhanced, batch-integrated join execution.

---

## Background

logq currently supports CROSS JOIN and LEFT JOIN via nested-loop streams (`CrossJoinStream`, `LeftJoinStream` in `execution/stream.rs`). The right side is re-opened and re-parsed from disk for every left row. For any non-trivial data sizes, this is prohibitively slow.

velox (Meta's C++ execution engine) implements a sophisticated hash join with three adaptive hash modes, SIMD tag-based probing, bloom filter pushdown, dynamic filter pushdown, parallel build, and spill-to-disk. This design adapts the most impactful techniques for logq's Rust codebase and log-querying use case.

---

## Phase 1: Basic Hash Join

### 1.1 Equi-Join Predicate Extraction

When converting a `LeftJoin` or `CrossJoin + Filter` to a physical plan, inspect the ON/WHERE condition for equality predicates of the form `left_field = right_field`. If found, extract those as **hash keys** and keep remaining predicates as a **residual filter**.

Implementation location: `logical/optimizer.rs` (new function `extract_equi_predicates`).

Input: a `Formula` representing the ON condition.
Output: `(Vec<(FieldRef, FieldRef)>, Option<Formula>)` — equi-key pairs and optional residual.

The extraction walks the formula tree, collecting `Equal(FieldAccess(left), FieldAccess(right))` conjuncts from top-level ANDs. Anything that isn't a simple field equality is kept as residual.

### 1.2 INNER JOIN Syntax

Add `Inner` to the `JoinType` enum in `syntax/ast.rs`. Update the parser in `syntax/parser.rs` to recognize `[INNER] JOIN ... ON ...` syntax. INNER JOIN is semantically a hash join with no NULL padding — the simplest join type.

### 1.3 HashJoinStream

New struct in `execution/stream.rs`:

```rust
struct HashJoinStream {
    left: Box<dyn RecordStream>,
    hash_table: HashMap<Vec<Value>, Vec<Record>>,  // build side
    join_type: JoinType,                            // Inner, Left, Cross
    equi_keys: Vec<(String, String)>,               // (left_field, right_field)
    residual: Option<Formula>,                      // non-equi predicates
    // iteration state
    current_left: Option<Record>,
    current_matches: Vec<Record>,
    match_index: usize,
    matched: bool,                                  // for LEFT JOIN NULL padding
    right_field_names: Vec<String>,                  // for NULL padding
}
```

Execution:
1. On first call to `next()`, fully materialize the right side into the `HashMap`. Key is `Vec<Value>` of the right-side equi-join columns. Value is `Vec<Record>` of all rows with that key.
2. For each left row, extract the join key columns, look up in the hash map.
3. For each match, merge left + right records. Apply residual filter if present.
4. For LEFT JOIN, if no match found, emit NULL-padded right side.

### 1.4 Physical Plan Wiring

In `logical/types.rs` (`Node::physical()`), when converting `LeftJoin` or `CrossJoin`:
- Call `extract_equi_predicates` on the condition.
- If equi-keys are found, emit `execution::Node::HashJoin { left, right, equi_keys, residual, join_type }`.
- If no equi-keys, fall back to existing nested-loop nodes.

In `execution/types.rs` (`Node::get()`), construct `HashJoinStream` from the `HashJoin` node.

### 1.5 Materialize-Once Fallback

For non-equi joins that must remain nested-loop, fix the existing `CrossJoinStream` and `LeftJoinStream` to materialize the right side into a `Vec<Record>` once on first iteration, instead of re-creating the stream from disk per left row. This is a one-line structural change to the existing streams and immediately improves all join performance.

### 1.6 Complexity

- Build: O(|R|) time and space.
- Probe: O(|L|) amortized (O(|L| x matches_per_key) worst case for many-to-many).
- vs. current: O(|L| x |R| x disk I/O).

---

## Phase 2: SIMD Tag-Based Hash Table + Bloom Filter

### 2.1 Custom Hash Table (`simd/hash_table.rs`)

A purpose-built hash table optimized for join workloads.

**Bucket layout (128 bytes, 2 cache lines):**
- 16 tag bytes: 7-bit hash tag + high bit set as occupancy marker
- 16 row pointers: indexes into a flat `Vec<JoinRow>` arena
- Padding to 128 bytes

**Probing:**
- Compute hash of probe key.
- Extract 7-bit tag from hash.
- Load 16 tags from the target bucket via SIMD (`u8x16` equality check).
- For each tag match, perform full key comparison.
- If no match in bucket, linear-probe to next bucket.

**Integration with logq's SIMD infra:** Uses existing `PaddedVec` for aligned storage and `Bitmap` for match extraction. Tag comparison leverages auto-vectorization patterns already used in `simd/kernels.rs`.

**Row arena:** All build-side rows are stored in a contiguous `Vec<JoinRow>` arena. Row pointers in buckets are `u32` indexes into this arena (not raw pointers), keeping the bucket compact and Rust-safe.

**Hash function:** Use `ahash` (AES-NI accelerated) or `xxhash-rust` for high-throughput hashing. Both are well-maintained Rust crates with excellent performance.

### 2.2 Bloom Filter (`simd/bloom_filter.rs`)

Split-block bloom filter design (matching velox):
- 64-bit blocks.
- 4 hash functions derived from 24 bits of the hash code (4 groups of 6 bits each selecting a bit position).
- Block selection via bits 24+ of the hash code.
- Target: ~2% false positive rate at 8 bits per entry.

API:
```rust
struct BloomFilter {
    blocks: Vec<u64>,
    num_bits: usize,
}

impl BloomFilter {
    fn new(expected_entries: usize) -> Self;
    fn insert(&mut self, hash: u64);
    fn might_contain(&self, hash: u64) -> bool;
}
```

### 2.3 Bloom Filter Pushdown to Scan

After `HashJoinStream` (or `BatchHashJoinOperator`) builds the hash table, it constructs a `BloomFilter` from the build-side keys.

The bloom filter is injected into the upstream scan pipeline. In the batch path:

```
BatchScan → [BloomFilterProbe] → BatchFilter → BatchProject → BatchHashJoinProbe
```

`BloomFilterProbe` is a lightweight batch operator that:
1. Hashes the join key column of each batch.
2. Tests each hash against the bloom filter.
3. Updates the `SelectionVector` to exclude non-matching rows.

This eliminates rows before they reach the join, which is especially valuable for logq's primary use case: large log file joined against a small dimension table.

---

## Phase 3: Adaptive Hash Mode Selection

### 3.1 Key Statistics Collection

During the build phase, collect statistics on join key columns:
- `min` and `max` values (for integer keys)
- `distinct_count` (via a `HashSet` capped at 100K entries)
- `total_key_bits` (sum of bit widths of all key columns)

This adds negligible overhead since every right-side row is already iterated during build.

### 3.2 Three Hash Modes

**Array mode:**
- Condition: single integer key, `max - min < 2M`, density (`distinct_count / range`) > 0.1.
- Implementation: `Vec<Option<RowIndex>>` indexed by `value - min`.
- Probe: single array dereference, no hashing.
- Ideal for: HTTP status codes, port numbers, small error code tables.

**NormalizedKey mode:**
- Condition: all key columns fit in ≤ 64 bits total.
- Implementation: pack all key columns into a single `u64` stored alongside each row. Probe compares the `u64` first; full key comparison only on match.
- Ideal for: multi-column joins like `(host_id u32, status u16)`.

**Hash mode (SIMD tag-based):**
- Condition: fallback for string keys, high-cardinality keys, or keys exceeding 64 bits.
- Implementation: Phase 2's SIMD bucket layout.
- Ideal for: joining on URL paths, log messages, or other string fields.

### 3.3 Decision Logic

```
collect key stats during build
  │
  ├─ single integer key AND range < 2M AND density > 0.1 → Array mode
  ├─ all keys fit in ≤ 64 bits total                      → NormalizedKey mode
  └─ otherwise                                             → Hash mode (SIMD)
```

---

## Phase 4: Batch-Level Integration + Dynamic Filter Pushdown

### 4.1 BatchHashJoinOperator (`execution/batch_join.rs`)

A new `BatchStream` implementation for hash joins in the columnar pipeline.

**Build phase:** Same as row-based — materialize right side into the adaptive hash table. The right side is typically small, so row-based build is acceptable.

**Probe phase:** Operates on `ColumnBatch` from the left side:
1. Extract the join key column as a typed slice (e.g., `&[i32]` or string offsets).
2. Hash all keys in the batch using existing SIMD kernels (`hash_column_i32`, `hash_combine`).
3. Probe the hash table for each row, producing a match index vector.
4. Gather matched right-side columns into new `TypedColumn`s using the match indices.
5. Combine left batch columns + gathered right columns into an output `ColumnBatch`.
6. Update the `SelectionVector` to exclude non-matching rows (INNER JOIN) or mark NULL-padded rows (LEFT JOIN).

This keeps the entire pipeline columnar:
```
BatchScan → BloomFilter → BatchHashJoinProbe → BatchFilter → BatchProject
```

### 4.2 Dynamic Filter Pushdown

After the hash table is built, push knowledge about the build-side key domain back to the scan operator:

- **Value range filter:** If build side has integer keys with known min/max, inject `key >= min AND key <= max` into the scan's predicate.
- **Distinct value filter:** If build side has few distinct values (e.g., 50 error codes), inject an `IN (v1, v2, ...)` filter. Combined with the existing `FilterCache` for low-cardinality string columns, this is very effective.
- **Bloom filter:** Fallback when distinct values are too numerous for an IN-list.

Implementation: `BatchHashJoinOperator::build()` returns a `DynamicFilter` enum that the plan executor injects into the upstream `BatchScanOperator` or `BatchFilterOperator`.

---

## Edge Cases and Error Handling

- **NULL join keys:** Follow SQL semantics — NULL never equals NULL. During build, skip rows with NULL/Missing keys. During probe, NULL keys produce no match (LEFT JOIN emits NULL-padded right side).
- **Empty right side:** Build produces empty hash table. INNER JOIN produces zero rows. LEFT JOIN emits every left row with NULL-padded right columns. Short-circuit: skip probe entirely.
- **Empty left side:** Stream finishes immediately.
- **Duplicate keys on build side:** Hash table maps each key to a list of row indexes. During probe, all matches are emitted. Handles many-to-many correctly.
- **Mixed equi + non-equi predicates:** Equi part drives hash lookup. Non-equi part becomes residual filter applied after hash probe.
- **Fallback to nested-loop:** If no equi-join predicate detected, use materialize-once nested-loop (Phase 1.5).

---

## Testing Strategy

- **Correctness:** Parameterized tests comparing hash join output against nested-loop join output for identical queries. Cover: CROSS, LEFT, INNER (new), empty inputs, NULL keys, duplicate keys, multi-column keys, mixed predicates.
- **Mode selection:** Unit tests verifying Array mode for small integer ranges, NormalizedKey for multi-column fits-in-64-bits, Hash mode otherwise.
- **Performance benchmarks:** Join 1M-row log file against dimension tables of 10, 1K, and 100K rows. Measure throughput for each hash mode vs. nested-loop baseline.
- **Bloom filter accuracy:** Unit test false positive rate under 3% for expected configurations.

---

## Implementation Order

| Step | What | Files | Depends On |
|------|------|-------|------------|
| 1 | Add `Inner` to `JoinType` enum | `syntax/ast.rs`, `syntax/parser.rs` | — |
| 2 | Equi-join predicate extraction | `logical/optimizer.rs` | — |
| 3 | Basic `HashJoinStream` | `execution/stream.rs` | 1, 2 |
| 4 | Wire up logical → physical → stream | `logical/types.rs`, `execution/types.rs` | 2, 3 |
| 5 | Materialize-once fallback for nested-loop | `execution/stream.rs` | — |
| 6 | SIMD tag-based hash table | `simd/hash_table.rs` (new) | — |
| 7 | Bloom filter | `simd/bloom_filter.rs` (new) | 6 |
| 8 | Bloom filter pushdown into scan | `execution/batch_scan.rs` | 7 |
| 9 | Adaptive mode selection | `simd/hash_table.rs` | 6 |
| 10 | `BatchHashJoinOperator` | `execution/batch_join.rs` (new) | 6, 9 |
| 11 | Dynamic filter pushdown | `execution/batch_scan.rs`, `execution/batch_join.rs` | 10 |
| 12 | RIGHT JOIN support | `syntax/ast.rs`, `syntax/parser.rs`, `execution/stream.rs` | 3 |

Steps 1-5 = Phase 1 (biggest impact). Steps 6-9 = Phase 2 (SIMD + bloom). Steps 10-12 = Phase 3/4 (batch + new join types).

The SIMD hash table (step 6) is a standalone module reusable for `DISTINCT`, `GROUP BY`, and `INTERSECT`/`EXCEPT` in the future.
