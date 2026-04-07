VERDICT: NEEDS_REVISION

## Summary Assessment

The plan is well-structured with correct dependency ordering and mostly sound Rust code, but contains two bugs that will cause test failures (wrong f32 encoding assertion, invalid cargo test command), a duplicate-and-delete code strategy that ignores a second call site, and incomplete benchmark coverage relative to the design spec.

## Critical Issues (must fix)

### C1. Step 2a `test_encode_f32_special_values` has wrong expected value for NEG_INFINITY

The test asserts:
```rust
encode_f32_to(f32::NEG_INFINITY, &mut buf);
assert_eq!(buf, [0x00, 0x00, 0x00, 0x00]);
```

But `f32::NEG_INFINITY.to_bits() = 0xFF800000`. The implementation flips all bits for negative values: `!0xFF800000 = 0x007FFFFF`. The correct assertion should be:
```rust
assert_eq!(buf, [0x00, 0x7F, 0xFF, 0xFF]);
```

This means the test will fail even with the correct implementation, breaking the TDD cycle. The ordering test (Step 2a `test_encode_f32_ordering`) would still pass since `0x007FFFFF < 0x80000000`, but the specific values test would not.

Note: the design doc's table also has this wrong (`-Infinity -> 0x00000000`), but the implementation formula is correct. The test just needs to match the actual formula output.

### C2. Step 7a test command is invalid syntax

The command:
```bash
cargo test --lib test_elb_order_by_timestamp test_elb_limit_with_order test_alb_order_by_received_bytes test_integration_distinct_and_order_by
```

This fails with `error: unexpected argument 'test_elb_limit_with_order' found`. `cargo test` accepts only a single test name filter. Fix: either run separate commands for each test, or use a regex filter like `cargo test --lib "test_elb_order_by|test_alb_order_by|test_integration_distinct"`.

### C3. Step 7b removes `compare_values` from `types.rs` but it is NOT the only call site

The plan says to "remove the now-unused `compare_values` function from `types.rs` (lines 541-566) since it's been duplicated into `prefix_sort.rs`." After the integration change, `compare_values` in `types.rs` is indeed no longer called from `types.rs` itself. Removing it is correct.

However, **there is a separate `compare_values` function in `src/functions/array.rs` (line 27)** that has the same name but different semantics (it handles Int/Float cross-type comparison and falls back to string comparison for mixed types). The plan's description is misleading -- it says "duplicated into `prefix_sort.rs`" but the `prefix_sort.rs` version is actually a different function from the `array.rs` one. The `types.rs` removal is safe because the `array.rs` function is independently defined, but the plan should explicitly note that the `array.rs` `compare_values` is a separate function and is NOT affected.

This is critical because an implementer might incorrectly think the plan wants them to also update or remove the `array.rs` version, or might get confused about why there are "two copies" of a function with the same name.

### C4. Step 5c `encode_value` treats Object/Array inconsistently with the design doc

In Step 5c, `Object` and `Array` set `slot[0] = NULL_BYTE_NULL` (0xFF) which is correct per the design. But the `test_encode_value_type_ordering` test in Step 5a does NOT test Object or Array values at all. Since this is an intentional behavioral change from the current code (design Section 6: "Object and Array Values"), there should be at least one test verifying that Objects and Arrays sort after all typed non-null values in the new deterministic order. Without this, the behavioral change is unverified.

### C5. Benchmarks missing multiple scenarios required by the design

The design doc (Section 7) specifies 8 benchmark scenarios plus a threshold sweep, string prefix length sweep, and an extract-and-index comparison baseline. The plan's Step 10b implements only 3 scenarios (`sort_int`, `sort_string`, `sort_multi_key`). Missing:

- `sort_string_urls` (worst case for prefix collisions -- this is important for validating the design's string prefix approach)
- `sort_mixed_type` (mixed Int/String column)
- `sort_with_nulls` (Int with 20% nulls)
- Threshold sweep
- String prefix length sweep
- Extract-and-index alternative baseline

While some of these could be deferred, the `sort_string_urls` scenario is specifically designed to stress-test the prefix collision fallback path and is critical for validating that the optimization actually helps for real-world URL-heavy log data.

## Suggestions (nice to have)

### S1. Duplication of `compare_values` into `prefix_sort.rs` should use a shared module function

The plan copies `compare_values` into `prefix_sort.rs` and removes it from `types.rs`. A cleaner approach would be to extract it into a shared location (e.g., `common/types.rs` or keep it in `types.rs` as `pub(crate)`) and import it from both `types.rs` (for any future use) and `prefix_sort.rs`. This avoids maintaining two copies that could drift. However, since the `types.rs` copy is being deleted (not kept alongside), this is a minor concern -- there will only be one copy after the change.

### S2. Step 10b's `PrefixSortEncoder { threshold: usize::MAX, ..Default::default() }` to force direct sort

Using `threshold: usize::MAX` to force the direct-sort path in benchmarks is a pragmatic hack. It works correctly but is a bit opaque. A clearer API would be a named constructor like `PrefixSortEncoder::direct_only()`. However, this is purely cosmetic and the current approach works fine for benchmarking purposes. It does expose internal implementation details (threshold semantics) to benchmark code, which is acceptable for a `bench-internals` feature gate.

### S3. Step 8b only exports `PrefixSortEncoder` but benchmarks may need `direct_sort`

The benchmark code (Step 10b) only uses `PrefixSortEncoder` with different threshold values to switch between prefix and direct sort, so `direct_sort` does not need to be exported. This is confirmed correct. However, if future benchmarks want to benchmark the raw `direct_sort` function independently (without the `PrefixSortEncoder` wrapper overhead), an export would be needed. Not required now.

### S4. Property tests could be more thorough

Step 9's property tests cover Int+String (with Desc ordering) and Int+Null cases. Consider also testing:
- Float values (especially NaN, Infinity edge cases)
- DateTime values
- All-string columns with varying prefix collision rates
- Mixed types in the same column (since this is a behavioral change)

### S5. Task sizing

Most tasks are appropriately sized at 2-5 minutes. Steps 5 and 6 are on the larger side -- Step 5 has a substantial `encode_value` implementation with 10 match arms, and Step 6 has the three-phase sort algorithm. These might take 8-10 minutes each for careful implementation. Consider splitting Step 6 into "6a: direct_sort fallback" and "6b: prefix sort phases" if strict 5-minute adherence is desired.

### S6. `encode_datetime` test uses only positive timestamps

Step 3a's `test_encode_datetime_ordering` tests three dates in 2020-2026, all with positive Unix timestamps. Consider adding a date before the Unix epoch (negative timestamp) to verify the sign-flip encoding works for DateTime, since the same `i64 ^ 0x8000000000000000` trick is used.

## Verified Claims (things you confirmed are correct)

1. **Borrow checker concern (Step 6c) is not an issue.** The sort closure in Phase 2 captures `records` by shared reference (`&records`). After `sort_unstable_by` returns, the shared borrow ends. Phase 3's `records.into_iter()` then consumes `records` by move. Rust's lexical lifetime rules allow this because the borrow scope of the closure is contained within the `sort_unstable_by` call.

2. **`compare_values` in `types.rs` is safe to remove.** After Step 7b's integration change, the only caller of `compare_values` in `types.rs` (the `Node::OrderBy` match arm at line 857) is replaced. The function in `src/functions/array.rs:27` is a completely separate function with the same name, not a reference to the `types.rs` one.

3. **`rand` crate is in `dev-dependencies`.** Confirmed at `Cargo.toml` line 42: `rand = "0.8"`. The property tests in Step 9 can use it.

4. **File paths are accurate.** `src/execution/types.rs`, `src/execution/mod.rs`, `src/lib.rs`, `src/execution/stream.rs` all exist. `src/execution/prefix_sort.rs` is a new file with the correct parent directory. The `benches/` directory exists with an existing `helpers/` module.

5. **Test commands are correct (except C2).** `cargo test --lib prefix_sort` correctly filters to tests in the `prefix_sort` module. The `--lib` flag limits to library tests. The filter string matches the module path.

6. **`bench_internals` exports cover what the benchmarks need.** `PrefixSortEncoder` (added in Step 8b), `Record`, `Value`, `PathExpr`, `PathSegment`, and `Ordering` are all exported or will be exported through `bench_internals`.

7. **Chrono API compatibility.** The project uses `chrono = "0.4"` which resolves to v0.4.19. The `FixedOffset::east()` and `.ymd().and_hms()` methods are not deprecated in this version and will compile.

8. **`Record` implements `Clone`.** Confirmed at `src/execution/stream.rs` line 13: `#[derive(Debug, PartialEq, Eq, Clone)]`. This is needed for the property test in Step 9 which calls `records.clone()`.

9. **Dependency groups avoid file conflicts.** Group 1-3 all write to `prefix_sort.rs`. Groups 1 steps (1-4) are independent and can be parallelized since they write to different sections of the same new file. Group 4 (Step 7) touches `types.rs`. Group 5 (Step 8) touches `mod.rs` and `lib.rs`. Group 6 touches `bench_sort.rs` (new) and `Cargo.toml`. No conflicts between groups.

10. **The `bench_sort.rs` benchmark correctly references the `helpers` module.** Existing benchmarks use `mod helpers;` (e.g., `bench_execution.rs` line 10), and the `helpers/` directory exists with `mod.rs`, `queries.rs`, and `synthetic.rs`. The new benchmark follows the same pattern.
