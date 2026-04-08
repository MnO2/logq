# Hash Join Optimization Design (Final)

**Date:** 2026-04-07
**Goal:** Replace logq's O(|L| x |R| x disk I/O) nested-loop joins with hash joins, inspired by velox's execution engine. Deliver a phased upgrade from basic hash join to bloom-filter-enhanced, batch-integrated join execution.

---

## Changes from v2

This revision addresses all critical issues and incorporates suggestions from the round 2 design review.

### Critical fixes

| # | Issue | Resolution |
|---|-------|------------|
| C1 | Equi-predicate extraction cannot determine left vs. right field membership -- no schema propagation exists in the logical plan | Replaced the optimizer-pass approach with parser-level extraction (option (c) from the reviewer). Equi-predicates are now extracted in `build_from_node()` in `logical/parser.rs`, where `TableReference` alias and table name information is still available. The `HashJoin` node is emitted directly by the parser. The optimizer module is no longer involved in join rewriting. For `CrossJoin + WHERE` (comma-separated tables), extraction happens in `parse_query()` where both the `FromClause` table references and the `WHERE` expression are available. See Section 1.1. |
| C2 | `extract_key` pseudocode uses `record.get(&String)` but `Record::get` takes `&PathExpr` | Changed `equi_keys` from `Vec<(String, String)>` to `Vec<(PathExpr, PathExpr)>`. Key extraction uses `Record::get_ref` (which performs a direct `LinkedHashMap` lookup for single-segment paths) on the hot path. Added a new `Record::get_field_value(&str) -> Option<&Value>` convenience method that directly accesses the internal `LinkedHashMap` without `PathExpr` construction overhead. See Sections 1.1 and 1.3. |

### Incorporated suggestions

| # | Suggestion | Resolution |
|---|-----------|------------|
| S1 | Optimizer pass has no integration point | Now moot. Join rewriting happens in the parser, not in an optimizer pass. No integration point needed. |
| S2 | `ahash` is not a direct dependency | Added `ahash = "0.7"` (matching the `hashbrown 0.11` transitive dependency in `Cargo.lock`) as an explicit `[dependency]` in `Cargo.toml`. This ensures hash compatibility for the "single hash computation, dual use" claim between `hashbrown` and the bloom filter. See Section 2.2. |
| S4 | RIGHT JOIN column reordering for `SELECT *` needs specification | Specified: the `HashJoinStream` records `original_left_field_count` during build. After producing each merged record (which has swapped order), the `reorder_for_right_join()` method splits the merged `LinkedHashMap` at the recorded count and reassembles with the second half first. Field name collisions are handled by the existing `Record::merge` behavior (right-side overwrites). See Section 1.5. |
| S6 | `Vec<Record>` per hash bucket wastes 24 bytes overhead for single-element buckets | Changed the hash table value type to `SmallVec<[Record; 1]>` (from the `smallvec` crate). Single-element buckets (the common case for dimension table lookups with unique keys) are inlined without heap allocation. Multi-match keys spill to the heap as before. `smallvec = "1"` added to `Cargo.toml`. See Section 1.3. |
| S7 | `CrossJoin + Filter` pattern detection may be fragile due to `Map` interposition | Scoped precisely: the `CrossJoin + WHERE` pattern is detected only at the `parse_query()` level in `logical/parser.rs`, not as a tree-pattern-match on the `Node` tree. At that point, the `WHERE` clause has not yet been wrapped around any `Map` node, so there is no interposition problem. The parser checks the `FromClause` for multi-table references, and if the `WHERE` contains equi-predicates spanning both table aliases, it emits a `HashJoin` node directly. See Section 1.1. |

### Suggestions acknowledged but not adopted

| # | Suggestion | Reason |
|---|-----------|--------|
| S3 | Memory estimation imprecision -- consider allocator tracking | Acknowledged. The coarse estimation (per-record size sampling from the first 100 rows, extrapolated) is sufficient for Phase 1. The goal is preventing OOM, not precise accounting. The 512 MB default provides a wide margin. |
| S5 | Bloom filter false positive rate too optimistic at 8 bits/entry | Acknowledged. Adjusted target to "under 3% false positive rate" for 8 bits per entry with 4 hash functions in a split-block layout. This matches theoretical expectations for block-partitioned bloom filters. |

---

## Background

logq currently supports CROSS JOIN and LEFT JOIN via nested-loop streams (`CrossJoinStream`, `LeftJoinStream` in `execution/stream.rs`). The right side is re-opened and re-parsed from disk for every left row. For any non-trivial data sizes, this is prohibitively slow.

velox (Meta's C++ execution engine) implements a sophisticated hash join with adaptive hash modes, SIMD tag-based probing, bloom filter pushdown, dynamic filter pushdown, parallel build, and spill-to-disk. This design adapts the most impactful techniques for logq's Rust codebase and log-querying use case.

---

## Phase 1: Basic Hash Join

### 1.1 Parser-Level Equi-Predicate Extraction and HashJoin Node Emission

**Why at the parser, not a separate optimizer pass:** The v2 design proposed an `optimize_joins` pass in `logical/optimizer.rs` that would walk the `Node` tree and rewrite `LeftJoin`/`CrossJoin` into `HashJoin`. The review correctly identified that this approach fails because the logical `Node` tree has no schema propagation -- there is no way to determine which side of the join a field reference belongs to by inspecting the `Node` tree alone. The `Node` enum carries no `output_schema()` method, and field references are resolved at runtime via merged `Variables` maps.

However, at the parser level in `logical/parser.rs`, table alias and table name information is available through the `TableReference` and `FromClause` structures. The `build_from_node()` function has access to each table's `as_clause` (alias) and `path_expr` (table name). This information is exactly what is needed to determine whether a field reference like `a.status` belongs to the left or right side of the join.

**Two extraction sites in `logical/parser.rs`:**

**Site 1: Explicit JOIN syntax** (`build_from_node()`, handling `FromClause::Join`)

When processing `FromClause::Join { left, right, join_type: Left, condition: Some(on_expr) }`:

1. Collect the table alias (or table name if no alias) for the right `TableReference`. For the left side, recursively collect all aliases from the `FromClause` subtree.
2. Call `extract_equi_predicates(on_expr, &left_aliases, &right_alias)` to split the ON condition.
3. If equi-keys are found, emit `Node::HashJoin { ... }`. Otherwise, emit `Node::LeftJoin { ... }` (nested-loop fallback).

```rust
// In build_from_node(), replacing the current JoinType::Left branch:
JoinType::Left => {
    let on_expr = condition.as_ref().expect("LEFT JOIN requires ON condition");
    let left_aliases = collect_aliases(left);
    let right_alias = get_alias(right);  // as_clause or base table name

    let (equi_keys, residual) = extract_equi_predicates_from_ast(
        on_expr, &left_aliases, &right_alias
    );

    if equi_keys.is_empty() {
        // No equi-predicates found; fall back to nested-loop
        let formula = parse_logic(ctx, on_expr)?;
        Ok(types::Node::LeftJoin(Box::new(left_node), Box::new(right_node), formula))
    } else {
        let residual_formula = residual.map(|r| parse_logic(ctx, &r)).transpose()?;
        Ok(types::Node::HashJoin {
            left: Box::new(left_node),
            right: Box::new(right_node),
            equi_keys,  // Vec<(PathExpr, PathExpr)>
            residual: residual_formula,
            join_type: LogicalJoinType::Left,
        })
    }
}
```

**Site 2: Comma-separated tables with WHERE** (`parse_query()`)

When `from_clause` is `FromClause::Tables` with 2+ table references and a `WHERE` clause exists:

1. After building the `CrossJoin` node via `build_from_node()`, but before wrapping with `Filter`, inspect the WHERE expression.
2. Collect table aliases from all `TableReference` entries. For 2-table joins, identify left (first) and right (second) aliases.
3. Call `extract_equi_predicates_from_ast(where_expr, &left_aliases, &right_alias)`.
4. If equi-keys are found, replace the `CrossJoin` node with a `HashJoin` node (join_type: Inner) and wrap only the residual (if any) in a `Filter`.

This detection is scoped precisely to the `parse_query()` function, where both the `FromClause` and the `WHERE` expression are available in the same scope. At this point, the `WHERE` has not yet been applied as a `Filter` node wrapping a `Map` node, so the interposition problem identified in S7 does not arise.

```rust
// In parse_query(), after building root from from_clause, before applying WHERE:
if let Some(where_expr) = &query.where_expr_opt {
    if let Some((cross_left, cross_right)) = try_unwrap_cross_join(&root) {
        let left_aliases = collect_aliases_from_from_clause(&query.from_clause, 0);
        let right_alias = collect_alias_from_from_clause(&query.from_clause, 1);

        let (equi_keys, residual) = extract_equi_predicates_from_ast(
            &where_expr.expr, &left_aliases, &right_alias
        );

        if !equi_keys.is_empty() {
            root = types::Node::HashJoin {
                left: Box::new(cross_left),
                right: Box::new(cross_right),
                equi_keys,
                residual: residual.map(|r| parse_logic(&parsing_context, &r))
                                  .transpose()?,
                join_type: LogicalJoinType::Inner,
            };
            // Skip the normal WHERE wrapping below -- residual already handled
            where_consumed = true;
        }
    }
}
// Existing WHERE wrapping only if not consumed:
if !where_consumed {
    if let Some(where_expr) = query.where_expr_opt {
        let filter_formula = parse_logic(&parsing_context, &where_expr.expr)?;
        root = types::Node::Filter(filter_formula, Box::new(root));
    }
}
```

**Equi-predicate extraction function** (`extract_equi_predicates_from_ast`):

Operates on `ast::Expression` (not `logical::Formula`), before `parse_logic` is called. This preserves the raw `PathExpr` information from the AST.

```rust
fn extract_equi_predicates_from_ast(
    expr: &ast::Expression,
    left_aliases: &HashSet<String>,
    right_alias: &str,
) -> (Vec<(PathExpr, PathExpr)>, Option<ast::Expression>)
```

Algorithm:
1. Flatten top-level AND conjuncts from the AST expression.
2. For each conjunct, check if it is `BinaryOperator(Equal, Column(path_l), Column(path_r))`.
3. For each equality, check the first segment of each `PathExpr`:
   - If `path_l.path_segments[0]` is an `AttrName` matching a left alias, and `path_r.path_segments[0]` matches the right alias (or vice versa), classify as an equi-predicate.
   - Extract the full `PathExpr` for each side (preserving multi-segment paths like `a.request.method`).
4. Collect equi-predicates as `Vec<(PathExpr, PathExpr)>` (left-side path, right-side path).
5. Remaining conjuncts become the residual, rebuilt as an AND tree of `ast::Expression`.

**Handling queries without table aliases:**

When tables have no aliases (e.g., `FROM access_log LEFT JOIN users ON status = user_id`), field references are bare names without a table qualifier prefix. In this case, the extraction cannot determine which side a field belongs to. The function returns empty equi-keys, and the join falls back to nested-loop. This is the correct conservative behavior.

For the extraction to work, queries must use table aliases or qualified field names:
- `FROM access_log AS a LEFT JOIN users AS u ON a.status = u.user_id` -- works
- `FROM access_log, users WHERE access_log.status = users.user_id` -- works (table name as implicit alias)
- `FROM access_log LEFT JOIN users ON status = user_id` -- falls back to nested-loop

This limitation is documented in user-facing output when `--force-nested-loop` is not set and a join falls back to nested-loop due to unqualified field references.

**New logical Node variant:**

```rust
// In logical/types.rs
pub(crate) enum Node {
    // ... existing variants ...
    HashJoin {
        left: Box<Node>,
        right: Box<Node>,
        equi_keys: Vec<(PathExpr, PathExpr)>,  // (left_path, right_path)
        residual: Option<Box<Formula>>,
        join_type: LogicalJoinType,             // Inner, Left, Right
    },
}

pub(crate) enum LogicalJoinType {
    Inner,
    Left,
    Right,
}
```

Note: `equi_keys` uses `PathExpr` (not `String`) to match the `Record` API. The `PathExpr` values retain the full path from the AST, including the table alias prefix (e.g., `a.status` is `PathExpr { path_segments: [AttrName("a"), AttrName("status")] }`).

### 1.2 INNER JOIN and RIGHT JOIN Syntax

Add `Inner` and `Right` to the `JoinType` enum in `syntax/ast.rs`. Update the parser in `syntax/parser.rs` to recognize:
- `[INNER] JOIN ... ON ...` (INNER is optional when bare `JOIN` is used)
- `RIGHT [OUTER] JOIN ... ON ...`

```rust
// In syntax/ast.rs
pub(crate) enum JoinType {
    Cross,
    Left,
    Inner,
    Right,
}
```

### 1.3 HashJoinStream with Specialized Key Hashing and Direct Field Access

New struct in `execution/stream.rs`. Uses `hashbrown::HashMap` (already a crate dependency used throughout the codebase) for consistency and because hashbrown internally uses Swiss Table with SSE2/NEON SIMD probing.

**New `Record::get_field_value` method for hot-path key extraction:**

The existing `Record::get(&PathExpr)` traverses the full `get_value_by_path_expr` code path. The existing `Record::get_ref(&PathExpr)` is better (direct `LinkedHashMap` lookup for single-segment paths) but still requires constructing a `PathExpr` at the call site. For the join hot path, add a zero-overhead field access method:

```rust
// In execution/stream.rs, impl Record:
/// Direct field access by name. Bypasses PathExpr construction.
/// For the join key extraction hot path only.
#[inline]
pub(crate) fn get_field_value(&self, field_name: &str) -> Option<&Value> {
    self.variables.get(field_name)
}
```

This method directly calls `LinkedHashMap::get`, avoiding all `PathExpr` overhead. It works because join key fields are always simple names (the alias-qualified path like `a.status` is resolved to the bare field name `status` at bind time via `Record::alias()`).

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
        match record.get_field_value(&key_fields[0]) {
            Some(Value::String(s)) => JoinKey::SingleString(s.clone()),
            Some(Value::Int(i)) => JoinKey::SingleInt(*i),
            Some(other) => JoinKey::Composite(vec![other.clone()]),
            None => JoinKey::Composite(vec![Value::Missing]),
        }
    } else {
        JoinKey::Composite(
            key_fields.iter()
                .map(|f| record.get_field_value(f)
                               .cloned()
                               .unwrap_or(Value::Missing))
                .collect()
        )
    }
}
```

Note: `key_fields` is `Vec<String>` (bare field names extracted from the `PathExpr` equi-keys during `HashJoinStream` construction), not `Vec<PathExpr>`. The `HashJoinStream` constructor resolves each `PathExpr` to its terminal field name at construction time, since after `Record::alias()` has run, fields are accessible by their bare names.

`JoinKey` implements `Hash` and `Eq` manually: `SingleString` hashes the raw `&[u8]` bytes; `SingleInt` hashes the `i32` directly; `Composite` hashes each `Value` in sequence. The three variants are given distinct hash prefixes to avoid cross-variant collisions.

**HashJoinStream struct:**

```rust
struct HashJoinStream {
    left: Box<dyn RecordStream>,
    hash_table: hashbrown::HashMap<JoinKey, SmallVec<[Record; 1]>>,  // build side
    join_type: LogicalJoinType,
    equi_keys: Vec<(PathExpr, PathExpr)>,      // original PathExpr pairs from logical node
    left_key_fields: Vec<String>,              // bare field names, pre-resolved
    right_key_fields: Vec<String>,             // bare field names, pre-resolved
    residual: Option<Formula>,
    // iteration state
    current_left: Option<Record>,
    current_matches: SmallVec<[Record; 1]>,
    match_index: usize,
    matched: bool,                             // for LEFT JOIN NULL padding
    right_field_names: Vec<String>,            // for NULL padding
    build_side_bytes: usize,                   // tracked memory usage
    memory_limit: usize,                       // configurable budget
    // RIGHT JOIN support
    is_right_join_swap: bool,                  // true if this was originally a RIGHT JOIN
    original_left_field_count: usize,          // for column reordering after swap
}
```

The value type is `SmallVec<[Record; 1]>` (from the `smallvec` crate) instead of `Vec<Record>`. For unique keys (the common case when joining a log file against a dimension table), the single `Record` is stored inline in the `SmallVec` without a separate heap allocation, saving 24 bytes per entry. For duplicate keys, `SmallVec` spills to the heap automatically.

**Execution:**

1. On first call to `next()`, fully materialize the build side into the `hashbrown::HashMap`. Key is `JoinKey` extracted from build-side equi-join columns via `get_field_value`. Value is `SmallVec<[Record; 1]>` of all rows with that key. Track `build_side_bytes` during materialization (see Section 1.6).
2. For each probe-side row, extract the join key using `get_field_value`, look up in the hash map.
3. For each match, merge left + right records. Apply residual filter if present.
4. For LEFT JOIN, if no match found, emit NULL-padded right side.

### 1.4 Build-Side Selection Heuristic

The v1 design always builds the hash table from the right side. This is correct for LEFT JOIN (semantics require iterating all left rows and NULL-padding unmatched ones, so the right side must be the build side). But for INNER JOIN, building from the larger side wastes memory and destroys performance.

**Heuristic for INNER JOIN:**

Before constructing `HashJoinStream`, estimate relative sizes of left and right inputs:

1. If both inputs are file scans (`DataSource::File`), use file size as a proxy for row count. Build from the smaller file.
2. If one input is a file scan and the other is a derived query (filter, subquery), assume the derived query is smaller (it has been filtered). Build from the derived side.
3. If both are derived, build from the right side (default).

When swapping is needed, the parser swaps the left/right children and transposes the equi-key pairs (swap each `(left_path, right_path)` to `(right_path, left_path)`).

**For LEFT JOIN:** Always build right. No swapping. If the right side is the large table, the user must rewrite their query or accept the memory cost.

**For RIGHT JOIN:** See Section 1.5.

### 1.5 RIGHT JOIN Implementation

RIGHT JOIN is implemented by input swapping:

1. The parser rewrites `RightJoin(left, right, condition)` as `HashJoin { left: right, right: left, equi_keys: transposed, join_type: Left }` and sets a flag `is_right_join_swap: true` on the resulting node.
2. During `HashJoinStream` construction, when `is_right_join_swap` is true, the stream records `original_left_field_count` -- the number of fields in the original left table (which is now the build/right side after swapping). This count is determined from the first build-side record during materialization.
3. After each merged output record is produced, if `is_right_join_swap` is true, `reorder_for_right_join()` rearranges the `LinkedHashMap` entries:
   - The merged record has fields in order: [swapped-left-fields (original right), swapped-right-fields (original left)].
   - The method splits at `original_left_field_count` from the end and reassembles as: [original-left-fields, original-right-fields].
4. For `SELECT *`, this ensures output column order matches SQL semantics (original left columns first, original right columns second).

**Column reordering implementation:**

```rust
fn reorder_for_right_join(record: Record, original_left_field_count: usize) -> Record {
    let tuples: Vec<(String, Value)> = record.into_tuples();
    let split_point = tuples.len() - original_left_field_count;
    let (right_fields, left_fields) = tuples.split_at(split_point);
    let mut variables = LinkedHashMap::with_capacity(tuples.len());
    // Original left fields first (they were the right/build side after swap)
    for (k, v) in left_fields {
        variables.insert(k.clone(), v.clone());
    }
    // Original right fields second (they were the left/probe side after swap)
    for (k, v) in right_fields {
        variables.insert(k.clone(), v.clone());
    }
    Record::new_with_variables(variables)
}
```

**Edge case -- field name collisions:** When both sides have a field with the same name, the existing `Record::merge` behavior silently overwrites (right side wins). After the RIGHT JOIN swap, the "right side" in merge is the original left table. This matches SQL semantics for RIGHT JOIN: the left table's value should be used for shared column names when the right table is NULL-padded. This behavior is acceptable and consistent with how LEFT JOIN handles collisions.

This approach avoids duplicating NULL-padding logic and is how most production databases implement RIGHT JOIN.

### 1.6 Memory Budget for Build Side

Materializing the entire build side into memory with no budget will OOM when joining two large log files.

**Configuration:**
- CLI flag: `--join-memory-limit <MB>` (default: 512 MB).
- Environment variable: `LOGQ_JOIN_MEMORY_LIMIT_MB`.

**Enforcement:**

During the build phase, track cumulative memory usage by sampling the size of the first 100 `Record` entries (using the sum of field name lengths + field value sizes: 4 bytes per `Int`, string length + 24 bytes per `String`, etc.) to compute a `bytes_per_row` estimate. For each subsequent row, add `bytes_per_row` plus 1 byte `SmallVec`/`hashbrown` control overhead to `build_side_bytes`.

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

In `logical/types.rs` (`Node::physical()`), add a branch for the new `HashJoin` logical node that performs a 1:1 translation to `execution::Node::HashJoin { left, right, equi_keys, residual, join_type, is_right_join_swap }`. No predicate inspection happens here -- that was already done by the parser.

In `execution/types.rs` (`Node::get()`), construct `HashJoinStream` from the `HashJoin` physical node, passing the configured memory limit.

### 1.8 Materialize-Once Fallback for Nested-Loop Joins

**This is Step 1 in the implementation order** (moved up from Step 5 per round 1 review feedback).

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

**Approach:** Use `hashbrown::HashMap` as the hash table for all phases. `hashbrown` is Rust's port of Google's Swiss Table (Abseil `flat_hash_map`). It uses SSE2 intrinsics on x86-64 and NEON intrinsics on ARM internally for the 16-byte tag group probing. This gives us the same SIMD-accelerated probing that the v1 design tried to build from scratch, without any unsafe code or portability burden in our codebase.

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
- Target: under 3% false positive rate at 8 bits per entry (adjusted from v2's 2% target per reviewer feedback on split-block theoretical limits).

**Hash function and dependency:** The bloom filter uses `ahash` (AES-NI accelerated on x86-64, fallback on other architectures) for all key types. `ahash = "0.7"` is added as an explicit dependency in `Cargo.toml`, matching the version used transitively by `hashbrown 0.11` (confirmed in `Cargo.lock` lines 6-14: `ahash 0.7.4`). This version match ensures hash values are compatible between `hashbrown`'s internal hashing and the bloom filter, enabling the "single hash computation, dual use" optimization.

To reuse hash values, the `hashbrown::HashMap` is constructed with an explicit `ahash::RandomState` hasher, and the `raw_entry` API is used to access the pre-computed hash:

```rust
use ahash::RandomState;

let hasher = RandomState::new();
let hash_table: hashbrown::HashMap<JoinKey, SmallVec<[Record; 1]>, RandomState> =
    hashbrown::HashMap::with_hasher(hasher.clone());

// During build: compute hash once, insert into both hash table and bloom filter
let hash = hasher.hash_one(&key);
hash_table.raw_entry_mut().from_hash(hash, |k| k == &key)
    .or_insert(key, SmallVec::new());
bloom_filter.insert(hash);
```

```rust
struct BloomFilter {
    blocks: Vec<u64>,
    num_bits: usize,
}

impl BloomFilter {
    /// Create a bloom filter sized for the expected number of entries.
    /// Uses 8 bits per entry for ~3% false positive rate with split-block layout.
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
- **Duplicate keys on build side:** Hash table maps each key to a `SmallVec<[Record; 1]>`. During probe, all matches are emitted. Handles many-to-many correctly.
- **Mixed equi + non-equi predicates:** Equi part drives hash lookup. Non-equi part becomes residual filter applied after hash probe.
- **Fallback to nested-loop:** If no equi-join predicate detected (including unqualified field references without table aliases), use materialize-once nested-loop (Section 1.8). Available behind `--force-nested-loop` flag for all joins.
- **Build side exceeds memory limit:** Clear error with actionable suggestions (see Section 1.6). No silent OOM.
- **RIGHT JOIN with non-equi predicates:** Handled via the swap-to-LEFT-JOIN rewrite (Section 1.5); residual predicates apply identically after the swap.
- **Unqualified field references in ON/WHERE:** When table aliases are not present, equi-predicate extraction cannot determine field ownership. The join falls back to nested-loop with a diagnostic message suggesting the user add table aliases.

---

## Dependency Changes

```toml
# Added to [dependencies] in Cargo.toml:
ahash = "0.7"       # Explicit dependency matching hashbrown 0.11's transitive ahash version.
                     # Required for direct hash computation (bloom filter, hash reuse).
smallvec = "1"       # Inline single-element hash buckets without heap allocation.
```

---

## Testing Strategy

- **Correctness:** Parameterized tests comparing hash join output against nested-loop join output for identical queries. Cover: CROSS, LEFT, INNER (new), RIGHT (new), empty inputs, NULL keys, duplicate keys, multi-column keys, mixed predicates.
- **Regression flag:** The nested-loop path is retained behind `--force-nested-loop`. Correctness tests run each join query twice (hash join and nested-loop) and assert identical output (modulo row ordering for INNER JOIN). This is especially important for LEFT JOIN NULL-padding behavior, which is subtle.
- **Equi-predicate extraction:** Unit tests in `logical/parser.rs` verifying extraction from AST expressions with table aliases. Test cases:
  - `a.x = b.y` -- extracted as equi-key.
  - `a.x = b.y AND a.z > 10` -- equi-key `(a.x, b.y)`, residual `a.z > 10`.
  - `a.x = a.y` -- same-side reference, not extracted (remains residual).
  - `x = y` (no aliases) -- not extracted, falls back to nested-loop.
  - `a.x = b.y AND a.w = b.v` -- two equi-key pairs extracted.
- **PathExpr-based key storage:** Verify that `equi_keys` stores `PathExpr` values and that `HashJoinStream` correctly resolves them to bare field names for `get_field_value` calls.
- **Mode selection:** Unit tests verifying Array mode for small integer ranges, NormalizedKey for multi-column fits-in-64-bits, Hash mode otherwise.
- **Key specialization:** Unit tests verifying that `JoinKey::SingleString` and `JoinKey::SingleInt` paths are exercised for single-column joins, and `JoinKey::Composite` for multi-column joins.
- **Memory limit:** Test that exceeding the configured memory budget produces a clear error, not an OOM.
- **Performance benchmarks:** Join 1M-row log file against dimension tables of 10, 1K, and 100K rows. Measure throughput for each hash mode vs. nested-loop baseline.
- **Bloom filter accuracy:** Unit test false positive rate under 4% for expected configurations (8 bits/entry, 4 hash functions, split-block). Verify that `ahash` is used (not the multiply-shift kernel hash).
- **i64 overflow test:** Test Array mode decision logic with `i32::MIN` and `i32::MAX` keys to verify no overflow in range computation.
- **RIGHT JOIN column order:** Verify output column order matches SQL semantics (original left columns first, original right columns second) even though internally the inputs are swapped. Test with `SELECT *` and with explicit column lists.
- **SmallVec behavior:** Verify single-element buckets do not heap-allocate (via assertion on `SmallVec::spilled()`). Verify multi-element buckets work correctly.

---

## Implementation Order

| Step | What | Files | Depends On |
|------|------|-------|------------|
| 1 | **Materialize-once fallback for nested-loop** (highest ROI, zero risk) | `execution/stream.rs` | -- |
| 2 | Add `Inner`, `Right` to `JoinType` enum; parse `JOIN`, `RIGHT JOIN` | `syntax/ast.rs`, `syntax/parser.rs` | -- |
| 3 | Add `ahash = "0.7"` and `smallvec = "1"` to `Cargo.toml` | `Cargo.toml` | -- |
| 4 | Add `Record::get_field_value(&str)` method | `execution/stream.rs` | -- |
| 5 | `HashJoin` logical node variant + `LogicalJoinType` enum | `logical/types.rs` | -- |
| 6 | Equi-predicate extraction from AST at parser level | `logical/parser.rs` | 2, 5 |
| 7 | `JoinKey` enum with specialized hashing | `execution/stream.rs` (or new `execution/join.rs`) | 3 |
| 8 | `HashJoinStream` with memory budget + SmallVec buckets | `execution/stream.rs` | 3, 4, 7 |
| 9 | Physical plan wiring (logical HashJoin -> physical HashJoin -> stream) | `logical/types.rs`, `execution/types.rs` | 5, 6, 8 |
| 10 | Build-side selection heuristic for INNER JOIN | `logical/parser.rs` | 6 |
| 11 | RIGHT JOIN via input swap + column reordering | `logical/parser.rs`, `execution/stream.rs` | 6, 8 |
| 12 | `--force-nested-loop` flag + regression tests | `main.rs`, tests | 9 |
| 13 | Bloom filter | `simd/bloom_filter.rs` (new) | 3 |
| 14 | Bloom filter pushdown into scan | `execution/batch_scan.rs` | 13 |
| 15 | Adaptive mode selection (Array, NormalizedKey, Hash) | `execution/join.rs` or `execution/stream.rs` | 8 |
| 16 | `BatchHashJoinOperator` | `execution/batch_join.rs` (new) | 8, 15 |
| 17 | Dynamic filter pushdown with `Arc<Mutex<Option<DynamicFilter>>>` | `execution/batch_scan.rs`, `execution/batch_join.rs` | 16 |

Steps 1-12 = Phase 1 (biggest impact, all critical fixes addressed).
Steps 13-14 = Phase 2 (bloom filter).
Steps 15-17 = Phase 3/4 (adaptive modes + batch integration).

The materialize-once fallback (Step 1) ships first as it has zero dependencies, zero risk, and immediately eliminates the disk I/O multiplier for all existing join queries.

---

## Non-Goals and Future Work

- **Spill-to-disk:** Not in scope. The memory budget with clear error messaging is the Phase 1 solution. Partitioned hash join with temporary files is future work for multi-GB-to-multi-GB joins.
- **Parallel build:** Not in scope. logq's current execution model is single-threaded. Parallel hash table build would require concurrent `HashMap` construction (e.g., per-partition tables merged post-build).
- **Custom SIMD hash table:** `hashbrown` provides Swiss Table SIMD probing internally. A custom implementation using `std::arch` intrinsics is only justified if benchmarking reveals `hashbrown` as a bottleneck for join-specific workloads. This is potential Phase 5 work.
- **FULL OUTER JOIN:** Not in scope for this design. Can be implemented as a combination of LEFT JOIN + anti-join of the right side.
- **Schema propagation infrastructure:** The parser-level extraction approach sidesteps the need for full schema propagation in the logical plan. If future optimizer passes need schema information (e.g., for cost-based join ordering with 3+ tables), a schema inference pass would be needed at that point. This is out of scope for the current design.
