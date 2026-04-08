VERDICT: NEEDS_REVISION

## Summary Assessment

The design correctly identifies the core performance problem (right-side re-parsing per left row) and proposes a reasonable phased approach. However, it contains several critical issues: the proposed `HashMap<Vec<Value>, Vec<Record>>` key type has serious performance problems due to `Value`'s complex `Hash` derive; the design references nonexistent SIMD infrastructure for the hash table ("u8x16 equality check"); the build-vs-probe side choice is hardcoded to always build right, which is wrong when the right side is the large log file; and the equi-predicate extraction operates at the wrong plan layer.

## Critical Issues (must fix)

### 1. `Vec<Value>` as HashMap key is a performance trap

The design proposes `HashMap<Vec<Value>, Vec<Record>>` for the hash table. `Value` is an 11-variant enum (in `src/common/types.rs` line 15-27) that includes `Object(Box<LinkedHashMap<String, Value>>)` and `Array(Vec<Value>)`. The derived `Hash` implementation for these nested types is extremely slow -- it must recursively hash every element of potentially deep structures. For the primary use case (joining on a simple string or integer field), creating a `Vec<Value>` allocation per lookup and then hashing through the `Value` enum's derived implementation is orders of magnitude slower than hashing the raw bytes directly.

**Fix:** For Phase 1, at minimum specialize the single-key case. When there is exactly one equi-key and it is a `Value::String`, hash the string bytes directly (via `ahash` or the crate's existing hash infrastructure). When it is `Value::Int(i32)`, use the integer directly. Only fall back to `Vec<Value>` hashing for multi-column or exotic-type keys. This is also critical for Phase 2's SIMD hash table to be viable -- it needs typed key access, not `Value` enum dispatch.

### 2. No SIMD vector intrinsics exist in the codebase -- design claims otherwise

The design states (Section 2.1): "Tag comparison leverages auto-vectorization patterns already used in `simd/kernels.rs`." And: "Load 16 tags from the target bucket via SIMD (`u8x16` equality check)."

Examining `src/simd/kernels.rs`, the existing SIMD infrastructure consists entirely of **scalar loops** that rely on LLVM auto-vectorization (e.g., `filter_ge_i32` is a plain `for` loop over `&[i32]`). There are no explicit SIMD intrinsics (`u8x16`, `_mm_cmpeq_epi8`, `std::simd`, etc.) anywhere in the codebase. The `PaddedVec` provides tail padding for safe overshoot reads, not for explicit SIMD loads.

A `u8x16` equality check for 16 tag bytes is a fundamentally different operation from what the existing auto-vectorizable kernels do. The design must either: (a) acknowledge that this requires introducing explicit SIMD intrinsics (which adds a portability burden -- ARM vs x86), or (b) redesign the tag-based probing to use a scalar loop that LLVM can auto-vectorize (which is less reliable for the 16-byte tag-match pattern). This is the centerpiece of Phase 2 and it is built on a false claim about existing infrastructure.

### 3. Build side is always the right side -- wrong for the primary use case

The design always builds the hash table from the right side of the join. But the design's own stated primary use case is: "large log file joined against a small dimension table." In SQL, users naturally write `FROM logs LEFT JOIN dim_table ON ...` -- the left side is the large log file and the right side is the small dimension table. This happens to work for LEFT JOIN.

However, for INNER JOIN (which Phase 1 introduces), if a user writes `FROM dim_table INNER JOIN logs ON ...` (small table on the left), the design would build the hash table from `logs` (the right side -- the large file), consuming massive memory and destroying performance. The correct approach is to always build from the smaller side. At minimum, the design should: (a) document that the build side is always right and rely on the optimizer to swap sides, or (b) add a heuristic to swap left/right when the right side appears to be a large file scan.

### 4. Equi-predicate extraction is at the wrong layer

The design says to add `extract_equi_predicates` to `logical/optimizer.rs`, but then wire it during `Node::physical()` in `logical/types.rs`. Looking at the code, `logical/optimizer.rs` currently only contains predicate reordering utilities (`extract_conjuncts`, `reorder_and_conjuncts`). The optimizer does not perform plan transformations -- it only operates on formulas.

The `physical()` method in `logical/types.rs` performs a 1:1 structural translation from logical to physical nodes. It does not inspect predicates to choose different physical operators. Adding equi-predicate-based join selection here would break the clean separation: the `physical()` method would need to parse `Formula` structures to find field references, determine which fields belong to left vs. right, and conditionally emit different physical node types.

**Fix:** Either: (a) add a proper logical plan optimizer pass that rewrites `LeftJoin(l, r, condition)` into `HashJoin(l, r, equi_keys, residual)` as a new logical `Node` variant before `physical()` is called, or (b) do the rewrite in the physical planner explicitly as a recognized pattern, but acknowledge this is plan-level optimization happening during physical plan creation, and explain why it is in `physical()` rather than a separate optimizer pass.

### 5. No memory limit or spill-to-disk for build side

The design materializes the entire right side into memory (`HashMap` or custom hash table) with no memory budget. The "Edge Cases" section mentions "Empty right side" but says nothing about a right side with millions of rows. For logq's use case (joining two log files), both sides could be multi-GB. Without a memory limit, this will OOM.

**Fix:** At minimum, add a memory budget parameter and a clear error message when the build side exceeds it (e.g., "Right side of join exceeds memory limit of N MB; consider rewriting query with smaller table on right"). Spill-to-disk can be deferred to a later phase, but the design must at least acknowledge the constraint and provide graceful failure.

### 6. `Value::Int` is `i32` -- Array mode range check of `max - min < 2M` can overflow

The design proposes Array mode when `max - min < 2M` for single integer keys. `Value::Int` wraps `i32` (line 16 of `common/types.rs`). If `max = i32::MAX` and `min = i32::MIN`, then `max - min` overflows `i32`. Even as `i64`, the range would be ~4 billion, far exceeding the 2M threshold, but the overflow in the subtraction must be handled. The design does not specify the arithmetic type for this computation.

**Fix:** Specify that range computation uses `i64` arithmetic: `(max as i64) - (min as i64) < 2_000_000`.

## Suggestions (nice to have)

### 1. Phase 1.5 (materialize-once fallback) should be Step 1, not Step 5

The design calls this "a one-line structural change" that "immediately improves all join performance." It has zero risk and zero dependency on any other step. Moving it to Step 1 in the implementation order means every join query benefits from day one, even before equi-predicate extraction or hash join logic is ready. The current ordering buries the highest-ROI change.

### 2. Bloom filter bit math uses 24 bits but design says "4 groups of 6 bits"

4 groups of 6 bits = 24 bits for bit-position selection. But the design also says "Block selection via bits 24+ of the hash code." This means the bloom filter requires at least 30+ useful bits from the hash. The existing `hash_column_i32` in `simd/kernels.rs` uses a simple multiplicative hash (`(data[i] as u64).wrapping_mul(HASH_MULT)`) that may not distribute well across the upper bits. Consider specifying that the bloom filter uses a high-quality hash (e.g., `ahash`) rather than the existing kernel hash.

### 3. NormalizedKey mode needs endianness-aware packing

The design says "pack all key columns into a single u64." For this to work as a hash table key, the packing must be deterministic and order-preserving within the key. The design does not specify byte order or how variable-length types (strings shorter than 8 bytes) are packed. For integer-only multi-column keys this is straightforward, but mixed types need explicit specification.

### 4. Consider `hashbrown::HashMap` instead of `std::collections::HashMap`

The codebase already uses `hashbrown::HashMap` in multiple places (e.g., `stream.rs` line 7, `execution/types.rs` line 15 via import). The design's Phase 1 `HashJoinStream` should use `hashbrown::HashMap` for consistency and because hashbrown uses SwissTable internally, which already provides many of the performance benefits the design tries to achieve with the custom SIMD hash table in Phase 2. This might make Phase 2's custom hash table unnecessary for all but the most extreme workloads -- worth benchmarking before building.

### 5. Dynamic filter pushdown (Phase 4.2) needs a mechanism to inject filters into already-constructed scan operators

The design says `BatchHashJoinOperator::build()` returns a `DynamicFilter` that "the plan executor injects into the upstream `BatchScanOperator`." But the batch pipeline is currently constructed top-down: the scan is created first, then wrapped by downstream operators. By the time the hash join builds its table, the scan operator is already running. The design needs to specify a concrete mechanism for this backward communication -- e.g., an `Arc<Mutex<Option<DynamicFilter>>>` shared between scan and join, or a two-pass construction where the join builds first from a separate stream.

### 6. Testing strategy should include a regression test against the existing nested-loop implementation

The design mentions "parameterized tests comparing hash join output against nested-loop join output" but should be explicit: keep the nested-loop path available behind a flag (e.g., `--force-nested-loop`) so that correctness can be validated by running both paths on the same query and diffing output. This is especially important for LEFT JOIN NULL-padding behavior, which is subtle.

### 7. RIGHT JOIN (Step 12) is under-specified

Step 12 mentions "RIGHT JOIN support" but gives no design details. RIGHT JOIN is typically implemented by swapping left and right inputs and running a LEFT JOIN, then reversing the column order. If the design plans this approach, it should say so. If not, it needs its own section.

## Verified Claims (things I confirmed are correct)

1. **Current join implementation re-creates the right stream per left row.** Confirmed in `CrossJoinStream::next()` (stream.rs line 698-700) and `LeftJoinStream::next()` (stream.rs line 813-816): both call `self.right_node.get(...)` inside the main loop, which re-opens the file and re-parses from scratch for every left row. This is indeed O(|L| x |R| x disk I/O).

2. **`Value` derives `Hash`.** Confirmed at `common/types.rs` line 14: `#[derive(PartialEq, Eq, Hash, Clone, Debug)]`. The derived `Hash` will work for `HashMap` keys, though with the performance caveats noted above.

3. **`PaddedVec` and `Bitmap` exist and are functional.** Confirmed in `src/simd/padded_vec.rs` and `src/simd/bitmap.rs`. Both are well-tested with comprehensive unit tests.

4. **`hash_column_i32` and `hash_combine` kernels exist.** Confirmed in `simd/kernels.rs` lines 114-124. These are auto-vectorizable scalar loops, not explicit SIMD intrinsics.

5. **The batch pipeline (`BatchStream` trait, `ColumnBatch`, `BatchScanOperator`, etc.) exists and is operational.** Confirmed in `execution/batch.rs`, `execution/batch_scan.rs`, and `execution/types.rs` (`try_get_batch`). The batch pipeline currently handles `Filter`, `Limit`, `Map/Project`, and `GroupBy` but has no join support (`try_get_batch` returns `None` for `CrossJoin` and `LeftJoin` -- they fall through to row-based).

6. **The `JoinType` enum currently only has `Cross` and `Left`.** Confirmed in `syntax/ast.rs` line 26-29. Adding `Inner` is indeed needed.

7. **The logical plan `Node` enum has `CrossJoin` and `LeftJoin` variants.** Confirmed in `logical/types.rs` lines 28-29 and `execution/types.rs` lines 558-559. The physical plan mirrors this structure 1:1.

8. **The optimizer (`logical/optimizer.rs`) currently only does predicate reordering.** Confirmed -- it contains `extract_conjuncts`, `rebuild_conjunction`, `predicate_cost`, and `reorder_and_conjuncts`. There are no plan-level transformations.

9. **`SelectionVector` is a working abstraction.** Confirmed in `simd/selection.rs` -- supports `All` (no filtering) and `Bitmap` modes, with `any_active`, `count_active`, `is_active`, and `to_bitmap` methods. This is ready to use for bloom filter integration.
