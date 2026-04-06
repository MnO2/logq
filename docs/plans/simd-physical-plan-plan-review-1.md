VERDICT: NEEDS_REVISION

## Summary Assessment

The plan is well-structured and covers the Phase 1 and Phase 2 foundation work from the design document. However, there are several critical issues: the plan uses `git commit` while the project uses Sapling (`sl`), the `simd-json` version is outdated (0.14 vs current 0.17), key API mismatches with the codebase (visibility, type constructors), a wrong type in `SelectionVector::Indices` tests, a step ordering problem that prevents test-first development from working, and the `RowToBatchAdapter` has a significant correctness issue with type-mismatched values.

## Critical Issues (must fix)

### C1: Version Control -- `git commit` should be `sl commit`

The project uses Sapling (sl), confirmed by `sl log` producing output and `sl --version` returning `Sapling 4.4.2`. While a `.git` directory exists (Sapling can use git under the hood), all commit messages in the history use `sl commit`. Every commit step in the plan uses `git commit` and should be changed to `sl commit` (or at minimum verified that `git commit` works with the Sapling-managed git repo).

### C2: `simd-json` version is outdated

The plan specifies `simd-json = "0.14"` but the current latest is `0.17.0`. Version 0.14 is roughly 2 years old. The API may have changed between versions. Use `simd-json = "0.17"` instead. (Note: `ahash = "0.8"` is fine -- 0.8.12 is the latest 0.8.x release.)

### C3: Step ordering prevents TDD -- module wiring must come before tests

Steps 2 and 7 write tests in `src/execution/batch.rs` and `src/simd/kernels.rs`, but these modules are not wired into the crate until Steps 6 and 10. The tests cannot compile (let alone fail with a test failure) until `pub mod batch;` is added to `src/execution/mod.rs` and `pub mod simd;` is added to `src/lib.rs`. The "run test to verify it fails" steps (2b, 7b, 9b) will fail with compilation errors like "file not found in module tree" rather than test failures.

**Fix:** Move module wiring (Steps 6 and 10) to occur immediately after creating the new files, before writing tests. Alternatively, restructure so Step 2 includes creating the file AND adding `pub mod batch;` to `mod.rs`, and Step 7 includes creating `src/simd/mod.rs`, `src/simd/kernels.rs`, and adding `pub mod simd;` to `lib.rs`.

### C4: `SelectionVector::Indices` type mismatch in test

Step 4's test uses:
```rust
let sv = SelectionVector::Indices(vec![0, 5, 10]);
```

But the implementation defines `Indices(Vec<u32>)`. Rust integer literals default to `i32`, so this will fail to compile. The test needs `vec![0u32, 5, 10]`.

### C5: `Record::new_with_variables` and `Record::get_ref` are `pub(crate)`, not `pub`

In `src/execution/stream.rs`, both `new_with_variables` and `get_ref` are declared `pub(crate)`. Since `batch.rs` is within the same crate, `pub(crate)` should work. However, the `BatchToRowAdapter` test in Step 11 uses `get_ref` which returns `Option<&Value>` -- this is fine for same-crate access. **But** the `into_variables` method is also `pub(crate)`, and the `RowToBatchAdapter` in Step 12 calls `record.into_variables()`. This is acceptable since `batch.rs` is within the `execution` module (same crate). This item is actually correct -- leaving it documented for completeness.

**However**, the `BatchToRowAdapter` test (Step 11) compares `Value::Int(10)` against a value extracted from a column. The `Value::Int` variant in `src/common/types.rs` is `Int(i32)`. The plan stores `vec![10, 20, 30]` as `Vec<i32>` in a `TypedColumn::Int32` and converts back via `Value::Int(data[idx])`. This is correct.

### C6: `RowToBatchAdapter` silently drops type-mismatched values

In Step 12's `ColumnBuilder::push`, when a value doesn't match the column type (e.g., a `Value::String` pushed to an `Int32` column), the code falls through to:
```rust
(_, v) => {
    // Type mismatch -- store in mixed as fallback
    self.mixed_data.push(v);
}
```

But the column is still built as `Int32` in `finish()`, using `self.int32_data` which is now shorter than expected (it's missing the entry for the mismatched row). The `null` and `missing` bitmaps say the row is present (`true`, `true`) but `int32_data` has no value at that index. This creates a vector length mismatch -- `int32_data` will have fewer elements than `null`/`missing` bitmap entries, causing index-out-of-bounds panics when accessing later rows.

**Fix:** On type mismatch, either (a) push a default value into the typed data vector and mark the row as NULL or MISSING, or (b) convert the whole column to `Mixed`. Option (a) is simpler.

### C7: `ColumnBuilder::push` Utf8 control flow is fragile and has a bug

The `push` method for Utf8 has a problematic pattern:
```rust
(ColumnType::Utf8, Value::String(v)) => {
    self.utf8_data.extend_from_slice(v.as_bytes());
    self.utf8_offsets.push(self.utf8_data.len() as u32);
    return; // offset already pushed
}
```

The `return` statement exits the entire `push` method, skipping the final block that pushes duplicate offsets for NULL/MISSING values on Utf8 columns. But there's a second check after the match:
```rust
if self.col_type == ColumnType::Utf8 { return; } // already handled
```

This second check is dead code for the success case (already returned above) and only fires for the type-mismatch fallback case `(_, v) => { self.mixed_data.push(v); }`. In the mismatch case, it returns early but the mismatch case pushed to `mixed_data` not `utf8_data`, so the Utf8 offsets are now inconsistent. This entire control flow needs to be rewritten more clearly.

### C8: `RowToBatchAdapter` test doesn't verify Utf8 column data is stored correctly for roundtrip

The test creates an `InMemoryStream` with `vec![r].into_iter().collect()` but `collect()` on `Vec<Record>::into_iter()` produces a `Vec<Record>`, not a `VecDeque<Record>`. The `InMemoryStream::new` expects a `VecDeque<Record>`. This will fail to compile.

**Fix:** Use `VecDeque::from(vec![r])` or `let records: VecDeque<Record> = vec![r].into_iter().collect();` (VecDeque implements FromIterator).

Actually, looking more carefully at `InMemoryStream::new`, it takes `VecDeque<Record>`. The test code `vec![r].into_iter().collect()` will work because VecDeque implements `FromIterator`, and Rust can infer the target type from the function signature. But this requires `InMemoryStream::new` to accept `VecDeque<Record>`, which it does. So this is actually fine since `.collect()` can produce a VecDeque. **Retracted** -- not a critical issue.

### C9: Design coverage gap -- Phase 1 JSONL `simd-json` migration task missing from plan

The design document's Phase 1 explicitly includes: "Change JSONL scan path to use `Vec<u8>` buffers for `simd-json` compatibility (mutable buffer requirement)." The plan does not include any task for this. While the plan adds `simd-json` as a dependency in Step 1, no step actually modifies the JSONL parsing code to use it. This is a Phase 1 deliverable per the design.

### C10: `ColumnType` does not derive `PartialEq` for `ColumnBuilder` type comparison

In Step 12, `ColumnBuilder::push` uses:
```rust
if self.col_type == ColumnType::Utf8 { return; }
```

This requires `ColumnType: PartialEq`. Step 5 defines `ColumnType` with `#[derive(Clone, Debug, PartialEq, Eq)]` which is correct. However, this comparison is used inside a match arm that already matched on `self.col_type` -- so the check is redundant and indicates a code smell. Not strictly a bug but indicates the logic should be restructured.

## Suggestions (nice to have)

### S1: Task sizing -- Steps 11 and 12 are too large

Steps 11 (`BatchToRowAdapter`) and 12 (`RowToBatchAdapter`) are significantly larger than 2-5 minutes. Step 12 alone includes a `ColumnBuilder` struct with 8 fields, a `push` method handling 6 type variants with NULL/MISSING, a `push_default` method, a `finish` method, and the full `RowToBatchAdapter` with `BatchStream` implementation. This is more like 15-20 minutes of work. Consider breaking Step 12 into: (a) ColumnBuilder struct and tests, (b) RowToBatchAdapter using ColumnBuilder.

### S2: Missing `SelectionVector::compact` implementation

The design document specifies `SelectionVector::compact(&self, batch: &ColumnBatch) -> ColumnBatch` as part of the API. The plan does not implement this method. While it's primarily needed for Phase 3 (materializing operators), it's part of the core data structure design and could be added alongside `SelectionVector` in Step 4.

### S3: `Bitmap::not()` doesn't track length -- can produce invalid high bits

`Bitmap::not()` inverts all bits in all words, including the padding bits in the last word. For a 65-element bitmap (2 words), the second word's bits 1-63 will all become 1 after `not()`, making `count_ones()` return 127 instead of the expected complementary count. This doesn't cause a crash but produces wrong counts. Consider tracking `len` in `Bitmap` and masking the last word in `not()` and `count_ones()`.

### S4: The `test_batch_to_row_adapter` test does not verify selection vector filtering

The test uses `SelectionVector::All`. Consider adding a test with `SelectionVector::Bitmap` that filters out some rows, verifying that the adapter skips inactive rows.

### S5: `hash_column_i32` test for distinctness is fragile

The test `test_hash_column_i32_distinct` asserts that 4 different small integers produce 4 different hashes. While the multiply-shift hash constant is likely to produce distinct outputs for inputs 0-3, this is not guaranteed by any hash function property. The test is testing an implementation detail rather than a contract. Consider weakening the assertion or documenting why it holds.

### S6: Consider using `edition = "2021"` 

The project uses `edition = "2018"`. While not strictly needed for this plan, the new code uses `2018`-compatible syntax. However, `edition = "2021"` would enable cleaner closures and pattern matching. This is orthogonal to the SIMD plan but worth noting.

### S7: `BatchStream` trait not object-safe verification

The plan uses `Box<dyn BatchStream>` in the adapters. The `BatchStream` trait has `fn schema(&self) -> &BatchSchema` which returns a reference, so this is fine for object safety. Verified correct.

### S8: Missing `hash_column_utf8` implementation

The design document specifies `hash_column_utf8` using AHash for string keys (Section 6). The plan's kernel steps (7-8) implement `hash_column_i32` and `hash_combine` but not `hash_column_utf8`. While this is primarily needed for Phase 3 GroupBy, the design puts it in the kernel module. Consider adding it in Phase 1.

## Verified Claims (things I confirmed are correct)

1. **File paths are accurate.** `src/execution/mod.rs` exists with `pub mod datasource; pub mod stream; pub mod types;`. The `src/simd/` directory and `src/execution/batch.rs` do not yet exist (correct -- they are new files). Parent directories exist.

2. **`hashbrown` is already a dependency.** Confirmed at `hashbrown = "0.11"` in Cargo.toml, used in `src/execution/types.rs`.

3. **`linked-hash-map` is available.** Confirmed in both `[dependencies]` and `[dev-dependencies]` in Cargo.toml. The plan's `BatchToRowAdapter` uses `LinkedHashMap` which is correct.

4. **`ordered-float` is available.** Confirmed at version `2.8` in Cargo.toml. The `OrderedFloat(data[idx])` constructor in the plan's `typed_column_get_value` is correct (OrderedFloat wraps f32).

5. **`Value::Int(i32)` type is correct.** The plan correctly uses `i32` for `TypedColumn::Int32` matching `Value::Int(i32)`.

6. **`Record::new_with_variables` and `into_variables` are accessible.** Both are `pub(crate)` which is accessible from `batch.rs` within the same crate.

7. **`RecordStream` trait signature is correct.** The plan's `BatchToRowAdapter` correctly implements `fn next(&mut self) -> StreamResult<Option<Record>>` and `fn close(&self)`.

8. **`ahash = "0.8"` version is reasonable.** Latest 0.8.x is 0.8.12; specifying "0.8" will pick it up.

9. **`PathExpr::new` and `PathSegment::AttrName` are public.** Confirmed in `src/syntax/ast.rs` -- both are `pub`.

10. **`cargo test --lib execution::batch::tests` is the correct test command.** The project uses `cargo test` (not buck), confirmed by Cargo.toml and project structure.

11. **`InMemoryStream::new` accepts `VecDeque<Record>`.** Confirmed in stream.rs.

12. **`chrono::FixedOffset::east_opt` exists.** Confirmed the codebase already uses it in `src/execution/datasource.rs`. The `chrono = "0.4"` dependency includes this method.

13. **`criterion` is already a dev dependency.** Confirmed at version `"0.3"` in Cargo.toml.

14. **Design Phase 1 and Phase 2 structural coverage.** The plan covers: Bitmap, TypedColumn, SelectionVector, ColumnBatch, BatchSchema, ColumnType, BatchStream trait, SIMD kernels (filter, arithmetic, hash, string), BatchToRowAdapter, RowToBatchAdapter. This aligns well with the design's Phase 1 and Phase 2.

15. **Dependency group file conflicts.** Checked the "Files Touched" column: Group 2 touches 3 independent new files; Group 3 has Steps 3-5 sequential on batch.rs and Step 8 parallel on kernels.rs; Group 4 touches mod.rs files. No conflicts detected within parallel groups.
