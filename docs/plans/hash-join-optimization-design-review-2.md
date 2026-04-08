VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design is a substantial improvement over v1, with all six critical issues from round 1 addressed at the specification level. The architecture is cleaner, the `JoinKey` specialization is well-thought-out, the SIMD claims have been corrected, and the memory budget fills a real gap. However, there are two remaining critical issues: (1) the equi-predicate extraction design does not specify how to determine which side of the join a field reference belongs to, given that the logical plan has no schema propagation, and (2) the `extract_key` pseudocode has a type mismatch with the actual `Record` API that reveals a deeper design gap around field access by string name.

## Critical Issues (must fix)

### C1. Equi-predicate extraction cannot determine left vs. right field membership -- no schema propagation exists

The design says `extract_equi_predicates` will "check each field reference against the left vs. right input schemas to ensure the pair spans both sides." However, **the logical plan has no schema propagation mechanism**. The `Node` enum does not carry output schema information. There is no `output_schema()` method on `Node`. The bindings attached to `DataSource` nodes carry alias information, but there is no infrastructure to propagate which field names are produced by each subtree.

In the current codebase, field references are resolved at runtime by looking up keys in the merged `Variables` map (a `LinkedHashMap<String, Value>`). At the logical plan level, there is no way to statically determine whether `PathExpr("a.x")` comes from the left child or the right child of a join.

This is not a trivial gap. To make `extract_equi_predicates` work, the design needs to specify one of:
- (a) A schema inference pass that computes the output field set for each `Node` subtree, so the optimizer can check left-schema vs. right-schema membership. This is significant new infrastructure.
- (b) A naming convention approach: if tables have aliases (e.g., `a` and `b`), then `a.x` belongs to the left side and `b.y` to the right. This is simpler but the design must specify it works only when table aliases are present and how it handles the unaliased case.
- (c) Extraction at the parser level (`logical/parser.rs`) where table alias information is still available, rather than as a post-hoc optimizer pass on the `Node` tree.

Without this, the core Phase 1 join rewrite cannot be implemented.

### C2. `extract_key` pseudocode uses `record.get(&String)` but `Record::get` takes `&PathExpr` -- design needs a field access plan

The design's `extract_key` function (Section 1.3) calls `record.get(&key_fields[0])` where `key_fields` is `Vec<String>`. But in the actual codebase (`src/execution/stream.rs` line 36), `Record::get` takes `&ast::PathExpr`, not `&String`. The `Record` stores fields in a `LinkedHashMap<String, Value>` internally, but its public API always goes through `PathExpr`.

This matters because:
1. The `equi_keys: Vec<(String, String)>` in the `HashJoin` node would need to be `Vec<(PathExpr, PathExpr)>` to work with the existing `Record` API.
2. Alternatively, a new `Record::get_by_name(&str) -> Option<&Value>` method could be added (the internal `variables.get(name)` call exists in `get_ref` for single-segment paths, but is not exposed as a standalone API).
3. The `JoinKey` extraction needs to be efficient, so going through the full `get_value_by_path_expr` for every row is wasteful when the field name is known to be a single-segment path. The design should specify using `Record::get_ref` (which does a direct `HashMap` lookup for single-segment paths) or adding a dedicated `Record::get_field(&str)` method.

This is a critical issue because the key extraction path is the hottest code in the join -- it runs for every row on both build and probe sides. Getting the API wrong here would negate the performance gains from `JoinKey` specialization.

## Suggestions (nice to have)

### S1. The optimizer pass has no integration point -- specify where `optimize_joins` is called

The design says to add `optimize_joins` in `logical/optimizer.rs`, but the current optimizer module is dead code -- `reorder_and_conjuncts` is defined but never called outside of tests. The existing pipeline in `app.rs` goes directly from `logical::parser::parse_query_top()` to `node.physical()` with no optimizer pass in between.

The design should specify the exact integration point. Presumably this would be:
```rust
let node = logical::parser::parse_query_top(q, data_sources, registry.clone())?;
let node = logical::optimizer::optimize_joins(node);  // NEW
let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;
```

This is straightforward but the design should call it out explicitly, especially since it would be the first time optimizer passes are actually wired into the pipeline.

### S2. `ahash` is not a direct dependency -- clarify how to use it

The design specifies `ahash` for bloom filter hashing and says "hashes computed for hash table insertion can be reused for bloom filter insertion." However, `ahash` is currently only a transitive dependency via `hashbrown 0.11`. To use `ahash` directly for computing hashes in user code, it needs to be added as an explicit dependency in `Cargo.toml`.

Additionally, `hashbrown 0.11` uses `ahash 0.7.x`. If the design wants to directly call `ahash::AHasher` and have hash values be compatible with `hashbrown`'s internal hashing, the version must match. The design should specify adding `ahash = "0.7"` (matching the lock file) to `Cargo.toml` `[dependencies]`.

Alternatively, `hashbrown::HashMap` can be constructed with `hashbrown::HashMap::with_hasher(ahash::RandomState::new())` to get an explicit hasher, or the design can use `hashbrown`'s `raw_entry` API to extract hash values. This detail matters for the "single hash computation, dual use" claim.

### S3. Memory estimation for build side is imprecise -- consider using allocator tracking

The design estimates Record size as "sum of field value sizes: 4 bytes per Int, string length + 24 bytes per String, etc., plus HashMap overhead estimate of 64 bytes per entry." This will be inaccurate because:
- `Record` contains a `LinkedHashMap<String, Value>`, which has per-entry overhead of ~80-96 bytes (doubly-linked list node + hash entry + key `String` heap allocation of 24 bytes + alignment).
- `Value` is an 11-variant enum whose size is dominated by the largest variant (`Object(Box<LinkedHashMap<String, Value>>)`), so each `Value` takes ~24 bytes regardless of variant (due to enum alignment).
- The `hashbrown::HashMap` overhead per entry is ~1 byte (control byte) + 8 bytes (pointer/index), not 64 bytes.

The combined underestimate of `Record` overhead and overestimate of `HashMap` overhead may roughly cancel, but the design should acknowledge the imprecision and consider a simpler approach: track `build_side_count` (number of rows) and use a conservative bytes-per-row estimate based on sampling the first N rows, or use `std::mem::size_of_val` on the first few records to calibrate.

### S4. RIGHT JOIN column reordering needs specification for `SELECT *`

The design says RIGHT JOIN is implemented by swapping inputs and running as LEFT JOIN, then "a post-processing step reverses the column order." But it does not specify how this post-processing step works. In the current codebase, `Record::merge` concatenates left and right fields in order. After swapping, the original right columns would appear first and original left columns second. The "reversal" needs to know which fields came from which original side.

For `SELECT *`, the output column order matters. The design should specify:
- How the `HashJoinStream` tracks which fields are from the "original left" vs "original right" after the swap.
- Whether this is done by recording the field count from each side during build, then reordering in the merge step.
- Edge case: what if both sides have a field with the same name? The current `Record::merge` silently overwrites.

### S5. Bloom filter false positive rate target may be too optimistic

The design targets "under 2% false positive rate at 8 bits per entry." A split-block bloom filter with 4 hash functions at 8 bits per entry theoretically achieves ~2.3-3% false positive rate (worse than a standard bloom filter at the same parameters due to block locality). The 2% target may require 10-12 bits per entry. The design should either adjust the target to ~3% or increase the bits-per-entry allocation.

### S6. Consider whether `Vec<Record>` per hash bucket is the right data structure

The design stores `HashMap<JoinKey, Vec<Record>>` which means for unique keys (the common case for dimension table lookups), every entry has a `Vec` with a single `Record`. Each `Vec` has 24 bytes of overhead (ptr + len + cap) even for a single element. For 100K unique keys, that's 2.4 MB of wasted `Vec` overhead.

An alternative is `HashMap<JoinKey, SmallVec<[Record; 1]>>` (from the `smallvec` crate) which inlines a single element without heap allocation, only spilling to heap for duplicate keys. Or the design could use a two-level structure: `HashMap<JoinKey, u32>` mapping to an index in a flat `Vec<Record>` with a separate run-length table for duplicates.

This is an optimization suggestion, not a blocker.

### S7. `CrossJoin + Filter(equi_condition)` pattern detection may be fragile

The design says the optimizer detects `CrossJoin + Filter(equi_condition, CrossJoin(...))` and folds the filter into a `HashJoin` with `join_type: Inner`. However, the current codebase generates `CrossJoin` from comma-separated tables in FROM (e.g., `FROM a, b WHERE a.x = b.x`), not from explicit `CROSS JOIN` syntax. The filter node containing the WHERE clause is created separately in `parse_query` (around line 708 of `logical/parser.rs`), wrapping the entire from-clause node.

The pattern match would need to handle the case where there are multiple filter levels (e.g., `Filter(complex_predicate, Map(projections, CrossJoin(left, right)))`). The `Map` node between `Filter` and `CrossJoin` may prevent the pattern from matching. The design should specify whether the optimizer handles this interposition or whether the optimizer needs to look through `Map` nodes.

## Verified Claims (things I confirmed are correct)

1. **`hashbrown` is used throughout the codebase.** Confirmed in 6 source files: `stream.rs`, `types.rs`, `filter_cache.rs`, `field_analysis.rs`, `parser.rs` (syntax), and `parser.rs` (logical). The claim is accurate.

2. **Current join implementation re-creates the right stream per left row.** Confirmed at `stream.rs` lines 698-701 (`CrossJoinStream`) and lines 807+ (`LeftJoinStream`): both call `self.right_node.get(...)` inside the main loop. This is indeed the O(|L| x |R| x disk I/O) behavior the design targets.

3. **`JoinType` enum currently only has `Cross` and `Left`.** Confirmed at `syntax/ast.rs` lines 26-29. Adding `Inner` and `Right` is indeed needed.

4. **The SIMD infrastructure has no explicit intrinsics.** Confirmed in `simd/kernels.rs`: all functions are plain `for` loops relying on LLVM auto-vectorization. The v2 design correctly acknowledges this and no longer claims otherwise.

5. **`Value` derives `Hash`.** Confirmed at `common/types.rs` line 14. The 11-variant enum includes `Object(Box<LinkedHashMap<...>>)` and `Array(Vec<Value>)`, making derived `Hash` expensive for nested types, validating the `JoinKey` specialization motivation.

6. **The optimizer module is never invoked in the actual pipeline.** Confirmed: `reorder_and_conjuncts` exists only in `logical/optimizer.rs` and is only called in tests. No code in `app.rs`, `logical/parser.rs`, or `execution/` imports or calls optimizer functions. The optimizer is dead code.

7. **`physical()` performs 1:1 structural translation.** Confirmed in `logical/types.rs` lines 34-151: every `Node` variant maps directly to a corresponding `execution::Node` variant with no conditional logic or predicate inspection. The v2 design correctly places join rewriting in a separate optimizer pass rather than in `physical()`.

8. **`ahash 0.7.4` is in the dependency tree via `hashbrown 0.11`.** Confirmed in `Cargo.lock` lines 6-14. It is a transitive dependency, not a direct one.

9. **The `Record` type uses `LinkedHashMap<String, Value>` internally.** Confirmed at `stream.rs` line 15. The `merge` method (line 109) clones the left side and inserts right-side entries. Field name collision during merge will silently overwrite.

10. **The batch pipeline (`BatchStream`, `ColumnBatch`, etc.) has no join support.** Confirmed: `try_get_batch` in `execution/types.rs` handles `Filter`, `Limit`, `Map`, and `GroupBy` but has no branches for `CrossJoin` or `LeftJoin` -- these fall through to row-based execution.

## Previous Issues Resolved

### C1 (Vec<Value> as HashMap key): RESOLVED
The `JoinKey` enum with `SingleString`, `SingleInt`, and `Composite` variants directly addresses this. The specialization avoids `Value` enum dispatch for the dominant single-column case. The manual `Hash` implementation with distinct hash prefixes per variant is correctly specified to prevent cross-variant collisions.

### C2 (Nonexistent SIMD intrinsics): RESOLVED
The v2 design correctly acknowledges that no explicit SIMD intrinsics exist in the codebase and adopts `hashbrown::HashMap` (which provides Swiss Table SIMD probing internally) instead of proposing a custom SIMD hash table. The custom table is correctly deferred to "potential Phase 5 work."

### C3 (Build side hardcoded to right): RESOLVED
The build-side selection heuristic for INNER JOIN (build from smaller side using file size as proxy) is well-specified. LEFT JOIN correctly always builds right. RIGHT JOIN via input swap to LEFT JOIN is a clean solution.

### C4 (Equi-predicate extraction at wrong layer): PARTIALLY RESOLVED
The design correctly separates the optimizer pass from `physical()`. However, as noted in C1 above, the mechanism for determining left vs. right field membership is unspecified due to the absence of schema propagation infrastructure.

### C5 (No memory limit for build side): RESOLVED
The configurable memory budget with `--join-memory-limit` flag, environment variable fallback, and clear error message with actionable suggestions is well-specified. Spill-to-disk is correctly deferred.

### C6 (i32 range overflow in Array mode): RESOLVED
The range computation is now specified as `i64` arithmetic with the explicit formula `(max as i64) - (min as i64) < 2_000_000`. The design even includes the verification that `i32::MAX - i32::MIN` as `i64` correctly exceeds 2M.
