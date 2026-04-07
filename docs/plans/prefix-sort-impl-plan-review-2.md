VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 plan correctly fixes all 5 critical issues from round 1, and the core implementation code is sound. However, the doc comment on `encode_f32_to` carries over an incorrect claim from the design doc about +Infinity encoding, and the Object/Array ordering test has a subtle logical flaw that makes it less useful than intended. One additional critical issue exists: the design doc's float encoding table is inconsistent with the implementation formula, and the plan has propagated one of these inconsistencies into a doc comment while fixing another.

## Critical Issues (must fix)

### C1. `encode_f32_to` doc comment has wrong value for +Infinity encoding

Step 2c (line 198) doc comment states:

```
/// NaN -> 0xFFFFFFFF, +Inf -> 0xFFFFFFFE (via sign-flip of 0x7F800000),
/// -Inf -> 0x007FFFFF (via bit-flip of 0xFF800000).
```

The `+Inf -> 0xFFFFFFFE` claim is incorrect. Tracing through the implementation:
- `f32::INFINITY.to_bits() = 0x7F800000`
- Not NaN, not negative (sign bit clear)
- `0x7F800000 ^ 0x80000000 = 0xFF800000`
- So `+Inf -> 0xFF800000`, not `0xFFFFFFFE`

The `0xFFFFFFFE` value was copied from the design doc's conceptual table, which describes an aspirational encoding where +Infinity is "one less than NaN." However, the actual sign-flip formula produces `0xFF800000`, which is still correctly ordered (less than NaN's `0xFFFFFFFF`, greater than `f32::MAX`'s `0xFF7FFFFF`). The implementation code is correct; only the comment is wrong.

This is critical because an incorrect doc comment can mislead future maintainers or reviewers into thinking the implementation has a bug when it does not. The values for +Infinity and -Infinity were explicitly called out in this comment, so they must be accurate.

**Fix:** Change the comment to:
```
/// NaN -> 0xFFFFFFFF, +Inf -> 0xFF800000 (via sign-flip of 0x7F800000),
/// -Inf -> 0x007FFFFF (via bit-flip of 0xFF800000).
```

Note: the design doc (`prefix-sort-design-final.md` Section 3) also has this error in its float encoding table (`+Infinity -> 0xFFFFFFFE` and `-Infinity -> 0x00000000`). The plan correctly fixed `-Infinity` in the test assertion (C1 from round 1) but did not address the design doc table errors. A follow-up should fix the design doc table, but that is outside the scope of this plan review.

## Suggestions (nice to have)

### S1. The Object/Array ordering test comment about Null could be confusing

In `test_encode_value_object_array_ordering` (Step 5a, line 459), the comment says:

```
// Null sorts before Object (both have null_byte 0xFF, but Null type_tag 0x00 < Object 0x08)
```

This is factually correct and the test assertion is correct. However, a reader unfamiliar with the encoding might find it surprising that Null sorts before Object when both have the "null" marker byte. Consider expanding the comment to clarify that within the `0xFF` group, the type_tag serves as a secondary discriminant, and Null's `0x00` is intentionally lowest.

### S2. Property test does not exercise string prefix fallback for multi-key scenario

In `test_prefix_sort_matches_direct_sort_int` (Step 9a), the string values are short (e.g., `"str_42"`, 6 bytes) and fit entirely within the 16-byte prefix. This means the string fallback tie-breaking path is never exercised in the multi-key property test. Consider using longer strings (e.g., `format!("shared_prefix_pad_{:04}", rng.gen_range(0..50))`) that exceed 16 bytes and share a common prefix to verify the fallback path in the property-based equivalence test.

### S3. Phase 3 buffer read of row index is redundant

In Step 6c Phase 3 (line 920), the code reads `row_idx` from the buffer for each sorted index:
```rust
let row_idx = u32::from_be_bytes(
    buffer[idx * entry_w + key_w..idx * entry_w + key_w + 4].try_into().unwrap()
) as usize;
```

Since entry `i` always has row index `i` (written in Phase 1 at line 863), `row_idx == idx` always. The buffer read is technically correct but unnecessary -- `opt_records[idx].take().unwrap()` would produce the same result. This is not a bug (the code is correct either way), but it adds unnecessary work and may confuse readers who expect the row index to differ from the entry index.

### S4. `-0.0` encodes differently from `+0.0`, diverging from design doc

The design doc table says both `-0.0` and `+0.0` encode to `0x80000000`. The implementation encodes:
- `-0.0` (bits `0x80000000`, negative): `!0x80000000 = 0x7FFFFFFF`
- `+0.0` (bits `0x00000000`, non-negative): `0x00000000 ^ 0x80000000 = 0x80000000`

The ordering test uses `<=` (line 157) so this passes, and the sort order is still correct (negative floats including `-0.0` sort before positive floats). However, this means `-0.0` and `+0.0` are treated as distinct values in the sort, unlike IEEE 754 and `OrderedFloat` where they compare as equal. In practice this is harmless for sorting (the relative order of "equal" elements is unspecified with `sort_unstable`), but it's worth noting the divergence from the design doc.

## Verified Claims (things you confirmed are correct)

1. **C1 fix (NEG_INFINITY encoding) is correct.** `f32::NEG_INFINITY.to_bits() = 0xFF800000`. Negative (sign bit set). `!0xFF800000 = 0x007FFFFF`. Big-endian bytes: `[0x00, 0x7F, 0xFF, 0xFF]`. The test assertion at line 169 is correct. This sorts before all negative finite values (e.g., `-f32::MAX = 0xFF7FFFFF -> !0xFF7FFFFF = 0x00800000`, and `0x007FFFFF < 0x00800000`). Correct.

2. **C2 fix (cargo test command) is correct.** The revised command `cargo test --lib "test_elb_order_by|test_elb_limit_with_order|test_alb_order_by|test_integration_distinct"` uses a single regex filter argument with `|` as OR separator, which is valid `cargo test` syntax. All four test functions exist in `src/app.rs` (lines 950, 1186, 1198, 1237).

3. **C3 fix (array.rs compare_values note) is correct.** The plan now explicitly states that `src/functions/array.rs` has a separate `compare_values` function with different semantics (Int/Float cross-type comparison, string fallback for mixed types) that must not be touched. Verified: the `array.rs` function (line 27) is independently defined and only called from within `array.rs` (line 67, for `array_sort`). Removing `compare_values` from `types.rs` does not affect it.

4. **C4 fix (Object/Array test) verifies the behavioral change.** The new `test_encode_value_object_array_ordering` test (Step 5a, lines 435-464) verifies: (a) typed values sort before Null, (b) Null sorts before Object, (c) Object sorts before Array. This covers the deterministic ordering of Object and Array values described in the design doc Section 6.

5. **C5 fix (benchmark generators) are correct.** `generate_url_records` creates strings with a 25-byte shared prefix (`"https://example.com/path/"`) that exceeds the 16-byte default prefix length, forcing all comparisons through the string fallback path. `generate_int_with_nulls_records` generates 20% null values as specified.

6. **S6 fix (pre-epoch DateTime test) is correct.** The test includes `1960-01-01` which has a negative Unix timestamp (`-315619200`). The sign-flip encoding `(-315619200 as u64) ^ 0x8000000000000000` produces a value less than the encoding for `2020-01-01` (positive timestamp). The assertion `buf0 < buf1` verifies this.

7. **`compare_values` in `types.rs` is safe to remove.** After Step 7b's integration change, the only call site (line 857) is replaced. The `array.rs` version is an independent function. No other code in `types.rs` or elsewhere references `types::compare_values`.

8. **`InMemoryStream::new` accepts `VecDeque<Record>`.** Confirmed at `src/execution/stream.rs` line 356. The `sort` method's return type `VecDeque<Record>` is compatible.

9. **`Ordering` derives `PartialEq`.** Confirmed at `src/execution/types.rs` line 123: `#[derive(Debug, PartialEq, Eq, Clone)]`. The comparison `orderings[k] == Ordering::Desc` in the sort comparator compiles.

10. **`Record` derives `Clone`.** Confirmed at `src/execution/stream.rs` line 13. The property tests' `records.clone()` compiles.

11. **`rand` crate is available.** Confirmed in `Cargo.toml` line 42: `rand = "0.8"` in `[dev-dependencies]`.

12. **Chrono API compatibility.** The lock file shows chrono 0.4.19. The `FixedOffset::east()`, `.ymd()`, and `.and_hms()` methods are available in this version (deprecated starting in 0.4.23).

13. **Benchmark file structure follows existing patterns.** Existing benchmarks (e.g., `bench_execution.rs`) use `mod helpers;`, `use logq::bench_internals::*;`, and `criterion_group!`/`criterion_main!` macros. The new `bench_sort.rs` follows the same pattern.

14. **`bench_internals` exports cover benchmark needs.** `PrefixSortEncoder` will be added in Step 8b. `Record`, `Value`, `PathExpr`, `PathSegment`, and `Ordering` are already exported. The `ordered_float` crate is re-exported through `Value::Float(OrderedFloat<f32>)`.

15. **The `bench_udf` bench entry does NOT have `required-features = ["bench-internals"]`** (Cargo.toml line 61), confirming that the `bench-internals` feature is optional and only needed for benchmarks that access internal APIs. The new `bench_sort` entry correctly includes `required-features = ["bench-internals"]`.
