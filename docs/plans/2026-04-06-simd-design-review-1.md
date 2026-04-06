VERDICT: NEEDS_REVISION

## Summary Assessment

The five v4 optimizations (G1-G5) are individually well-motivated and the Velox inspirations are appropriate. However, several design issues need correction before implementation: the `PaddedVec<T>` API is incomplete (missing the mutation methods the design itself uses two sections later), the lazy parsing design has an unresolved gap around projected-field determination that the Appendix self-corrects but the main design never incorporates, and the predicate cost model references `Formula` variant names that do not exist in the codebase.

## Critical Issues (must fix)

### C1. PaddedVec<T> API is incomplete -- design uses methods it does not define

Section 1 defines `PaddedVec<T>` with only `with_len`, `from_vec`, `Deref`, and `DerefMut`. But Section 5 (line ~631) uses `PaddedVec::with_capacity(...)`, `offsets.push(0)`, and `data.extend_from_slice(...)`. These are `Vec` methods that are NOT available through `Deref<Target=[T]>` or `DerefMut`. The `Deref` impl returns `&[T]`, which does not expose `push`, `extend_from_slice`, `with_capacity`, or any allocating methods.

This is not just a documentation gap -- any mutation that changes the Vec's length (push, extend, etc.) can consume the reserved padding capacity, silently breaking the SIMD safety invariant. If you push `pad_elements` items after `from_vec`, the padding is entirely consumed and SIMD overreads become undefined behavior.

**Fix:** Either (a) implement custom `push`, `extend_from_slice`, `with_capacity` methods on `PaddedVec` that maintain the padding invariant (re-reserve if capacity drops below `len + pad_elements`), or (b) make `PaddedVec` an immutable-after-construction type and use a separate `PaddedVecBuilder` for the mutable construction phase that seals padding at the end. Option (b) is cleaner -- it makes the invariant structurally enforced rather than relying on every caller to avoid breaking it.

### C2. Lazy parsing projected-field set does not account for non-pushed WHERE columns

Section 9 defines `projected_fields: Vec<usize>` as the fields needed in output. But when a WHERE clause has both pushable and non-pushable conjuncts (e.g., `WHERE status = '200' AND user_agent LIKE '%bot%'`), the non-pushed predicate runs in `BatchFilterOperator` downstream and needs `user_agent` as a parsed column. If `user_agent` is not in SELECT, it would not be in `projected_fields`, so it would never be parsed.

The Appendix (line ~1409) catches this exact problem and notes "the planner must include non-pushed WHERE fields in the projected set for Phase 2" -- but the actual design in Section 9 never incorporates this correction. The `BatchScanOperator` struct only has `pushed_predicate` and `projected_fields`; there is no `filter_residual_fields` or equivalent.

**Fix:** Add a `residual_filter_fields: Vec<usize>` field to `BatchScanOperator` (or merge them into `projected_fields` at plan time). Update the Phase 2 logic to parse these fields for surviving rows. Update the `try_push_predicate` function to also return the field indices needed by non-pushed conjuncts.

### C3. Predicate cost model uses non-existent Formula variant names

Section 10's `predicate_cost` function pattern-matches on `Formula::Comparison`, `Formula::Like`, and `Formula::InList`. None of these exist in the codebase:

- The logical `Formula` (in `src/logical/types.rs`) uses `Formula::Predicate(Relation, Box<Expression>, Box<Expression>)`, not `Formula::Comparison`.
- It uses `Formula::In(...)`, not `Formula::InList`.
- AND is `Formula::InfixOperator(LogicInfixOp::And, left, right)`, not `Formula::And`.

Since the cost model runs at plan time (logical-to-physical translation), it operates on the *logical* `Formula`. The `reorder_conjuncts` function would need to flatten `InfixOperator(And, ...)` trees into a list of conjuncts, reorder them, then reconstruct the tree. The design presents this as a simple `sort_by_key` on a `Vec<Formula>`, but extracting conjuncts from a nested binary tree and rebuilding it is more involved.

**Fix:** Rewrite the cost model to match the actual logical `Formula` enum variants. Show the conjunct extraction/reconstruction logic explicitly (it is ~15-20 lines, not trivial).

### C4. Appendix combined flow has wrong field indices

The Appendix example says "Parse only field #12 (elb_status_code)". According to the actual ELB schema in `src/execution/datasource.rs` (line 193-238), `elb_status_code` is at index **7**, not 12. Index 12 is `user_agent`. This undermines confidence in the example's accuracy.

Later the Appendix says "Parse field #3 (url)" but there is no field named `url` in the ELB schema. The closest is `request` at index 11 (which is an `HttpRequest`, not a plain URL string). If the query says `SELECT url FROM elb`, it would fail because `url` is not a valid ELB column name.

**Fix:** Use correct field indices and valid column names in the example. A corrected example might be: `SELECT user_agent, elb_status_code FROM elb WHERE elb_status_code = '200'` with field indices 7 (elb_status_code) and 12 (user_agent).

## Suggestions (nice to have)

### S1. PaddedVec with_len truncation is fragile

`PaddedVec::with_len` creates `vec![T::default(); len + pad_elements]` then calls `inner.truncate(len)`. While `truncate` does not release capacity in current Rust, this is an implementation detail -- the Rust documentation says "Note that this method has no effect on the allocated capacity of the vector." This is stable behavior, but the design should add a debug assertion: `debug_assert!(self.inner.capacity() >= self.inner.len() + pad_elements)` after construction to catch any future surprises.

### S2. Filter cache's HashMap<Vec<u8>, bool> allocates per unique string

The `StringFilterCache` (first variant, line ~1278) uses `HashMap<Vec<u8>, bool>` which allocates a new `Vec<u8>` for each unique string key via `field.to_vec()`. For the two-pass variant (line ~1332), borrowed `&[u8]` keys avoid allocation. The design should recommend the two-pass variant as the default and mark the owned-key variant as the naive baseline shown for clarity only.

### S3. Filter cache cardinality threshold of 25% may be too generous

The adaptive threshold `cache.len() > len / 4` (25% unique values) means that for BATCH_SIZE=1024, caching is still used with up to 256 unique values. At that point you have 256 hash insertions + 1024 hash lookups = 1280 hash operations, vs 1024 direct filter evaluations. For cheap filters (short string equality), the hash operations may actually be slower than direct comparison. Consider a lower threshold like 5-10%, or make it dependent on the estimated filter cost.

### S4. Two-phase scan stores full line buffers in memory

The `tokenize_batch` function (Section 9, Phase 1) returns `Vec<Vec<(usize, usize)>>` -- a vector of byte ranges per field per row. This requires keeping all 1024 raw line buffers (`lines: Vec<Vec<u8>>`) alive in memory through both Phase 1 and Phase 2, since Phase 2 references them by byte range. For a batch of 1024 lines at ~500 bytes each, that is ~512KB of raw line data plus the offset vectors. This is within L2 cache and unlikely to be a problem in practice, but it should be documented as a memory trade-off of lazy parsing vs the v3 approach (which can discard each line after parsing).

### S5. Direct bitmask extraction (G4) lane count is hardcoded

Section 12's `filter_ge_i32_to_bitmap` hardcodes `const LANES: usize = 8` (i32x8 = AVX2). On aarch64 with NEON, the natural lane count is 4 (i32x4 = 128-bit). The `std::simd` API supports different lane counts, but using 8 on a NEON machine would either be emulated (2x 128-bit ops) or fail to compile. Either use a platform-conditional lane count or let the compiler choose via `Simd::<i32, N>` with a generic `N`.

### S6. ELB status codes are strings, not integers

The design's predicate table (Section 4) shows `elb_status_code = '200'` and correctly notes these are `DataType::String` in the schema. However, the filter caching discussion (Section 13) mentions "~15 unique status codes" which is reasonable for HTTP status codes. Worth noting explicitly that since these are strings (not integers), the SIMD integer comparison kernels do NOT apply to status code filters -- they go through the string equality path. The design does handle this correctly but could be clearer.

### S7. ALB string field count is wrong in the table

Section 4's field type breakdown table says ALB has 15 String fields. Counting the actual `AWS_ALB_DATATYPES` array in `src/execution/datasource.rs` (lines 240-269), ALB has **16** String fields (`type`, `elb`, `elb_status_code`, `target_status_code`, `user_agent`, `ssl_cipher`, `ssl_protocol`, `target_group_arn`, `trace_id`, `domain_name`, `chosen_cert_arn`, `matched_rule_priority`, `request_creation_time`, `action_executed`, `redirect_url`, `error_reason`). With 16 strings, the total is 16+3+2+1+2+1 = 25, which matches. The table should say 16 String fields.

### S8. Consider whether G2 reordering should also apply to OR predicates

The design only discusses reordering AND conjuncts. For OR predicates, the order also matters for short-circuiting (true dominates in OR). If the cheapest-to-evaluate disjunct is likely to return true, evaluating it first saves work. The same cost model could apply, though the benefit is smaller since OR short-circuits on true rather than false.

## Verified Claims (things confirmed correct from codebase)

1. **ELB has 17 fields** -- Confirmed. `ClassicLoadBalancerLogField::len()` returns 17 (line 475), and `AWS_ELB_DATATYPES` has 17 entries.

2. **ALB has 25 fields** -- Confirmed. `ApplicationLoadBalancerLogField::len()` returns 25 (line 563), and `AWS_ALB_DATATYPES` has 25 entries.

3. **S3 has 24 fields, all String** -- Confirmed. `S3Field::len()` returns 24 (line 649), and `AWS_S3_DATATYPES` is 24 entries of `DataType::String`.

4. **Squid has 10 fields, all String** -- Confirmed. `SquidLogField::len()` returns 10 (line 707), and `SQUID_DATATYPES` is 10 entries of `DataType::String`.

5. **ELB has 2 Host fields and 1 HttpRequest field** -- Confirmed. `AWS_ELB_DATATYPES` has Host at indices 2,3 and HttpRequest at index 11.

6. **ELB field type breakdown: 8 String, 3 Float, 2 Int, 1 DateTime, 2 Host, 1 HttpRequest** -- Confirmed by counting `AWS_ELB_DATATYPES`.

7. **hashbrown is already a dependency** -- Confirmed. `Cargo.toml` has `hashbrown = "0.11"`, and it is used in `execution/types.rs`, `logical/parser.rs`, and `syntax/parser.rs`.

8. **Host/HttpRequest accessed via FunctionRegistry** -- Confirmed. `src/functions/host.rs` registers `host_name` and `host_port` which pattern-match on `Value::Host`. `src/functions/url.rs` registers `url_host`, `url_port`, `url_path`, `url_query`, `url_fragment`, `url_path_segments`, `url_path_bucket` which pattern-match on `Value::HttpRequest`.

9. **`get_value_by_path_expr` only supports `Value::Object` for attribute navigation** -- Confirmed. `apply_path_to_value` in `src/common/types.rs` matches `AttrName` against `Value::Object` only; `Value::Host` and `Value::HttpRequest` would return `Value::Missing`.

10. **LogTokenizer scans bytes one at a time** -- Confirmed. `LogTokenizer::next()` in `src/execution/datasource.rs` (lines 98-165) iterates `bytes[self.pos]` one byte at a time.

11. **JSONL uses `json::parse(&self.buf)` with immutable borrow** -- Confirmed. Line 936 of `datasource.rs` shows `json::parse(&self.buf)`.

12. **`parse_utc_timestamp` is already an optimized byte-level parser** -- Confirmed. Lines 20-64 of `datasource.rs` show a hand-written parser that avoids chrono's full parse_from_rfc3339 on the fast path.

13. **Current project uses edition 2018** -- Confirmed. `Cargo.toml` has `edition = "2018"`. Note: `std::simd` (used in G4) requires nightly Rust regardless of edition, which the design correctly notes.

14. **No existing OrderByStream struct** -- The design's operator mapping table lists `OrderByStream` as the current row-based operator, but ORDER BY is actually implemented inline in `Node::OrderBy`'s `create_stream` method (line 623 of `types.rs`) as a collect-and-sort into `InMemoryStream`. This is minor since the batch counterpart would be new code regardless.
