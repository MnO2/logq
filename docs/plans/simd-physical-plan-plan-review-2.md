VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 plan addressed most of the 10 critical issues from Review 1 well, but introduces several new problems: the `typed_column_get_value` function does not handle two `Value` variants that exist in the codebase (`HttpRequest`, `Host`), the `Bitmap::or()` method silently truncates when bitmaps have different lengths, the `ColumnBuilder` has an unreachable `Value::Null` handling inconsistency in the `Mixed` column type, the JSONL simd-json migration step is too vague to implement correctly, and `AHasher::default()` requires the `AHasher` to implement `Default` which is not guaranteed in all `ahash` 0.8.x versions (the stable API is `RandomState`).

## Critical Issues (must fix)

### C1: `typed_column_get_value` is missing `HttpRequest` and `Host` Value variants

The `Value` enum in `src/common/types.rs` has 11 variants:
```
Int, Float, Boolean, String, Null, DateTime, HttpRequest, Host, Missing, Object, Array
```

The `TypedColumn` enum only covers `Int32`, `Float32`, `Boolean`, `Utf8`, `DateTime`, and `Mixed`. The `typed_column_get_value` function (Step 10) handles all `TypedColumn` variants but when values are stored in `Mixed`, it calls `data[idx].clone()`. This is fine for values that were stored as `Mixed`.

However, the real problem is the `ColumnBuilder::push` method (Step 11). The match for type compatibility only recognizes these pairs:
```rust
(ColumnType::Int32, Value::Int(_)) => true,
(ColumnType::Float32, Value::Float(_)) => true,
(ColumnType::Boolean, Value::Boolean(_)) => true,
(ColumnType::Utf8, Value::String(_)) => true,
(ColumnType::DateTime, Value::DateTime(_)) => true,
(ColumnType::Mixed, _) => true,
_ => false,
```

For a `ColumnType::Mixed` column, `Value::HttpRequest`, `Value::Host`, `Value::Object`, and `Value::Array` are all accepted (the `(ColumnType::Mixed, _)` arm). This is correct. But for any non-Mixed column type, these values will be treated as type mismatches and stored as NULL with a default value. While this is not incorrect per se, it means that `RowToBatchAdapter` -> `BatchToRowAdapter` roundtrip will lose `HttpRequest`, `Host`, `Object`, and `Array` values if the schema specifies a specific type. The plan does not document this lossy behavior.

More critically, `typed_column_get_value` for `TypedColumn::DateTime` converts microsecond timestamps back to `DateTime` using `FixedOffset::east_opt(0)`, which means all timezone information is lost -- the original `DateTime<FixedOffset>` might have been in any timezone, but it comes back as UTC. This is a data fidelity bug for the roundtrip path.

**Fix:** At minimum, document the lossy behavior. For DateTime, consider storing the timezone offset alongside the timestamp (e.g., as a separate `Vec<i32>` for UTC offset seconds), or storing as `i64` microseconds from epoch (which is timezone-neutral) and reconstructing with the original offset.

### C2: `Bitmap::or()` silently truncates when bitmaps have different lengths

The `or()` method uses `zip()`:
```rust
pub fn or(&self, other: &Bitmap) -> Bitmap {
    let len = self.len.max(other.len);
    let words = self.words.iter().zip(other.words.iter())
        .map(|(&a, &b)| a | b).collect();
    Bitmap { words, len }
}
```

`zip()` stops at the shorter iterator. So if `self` has 2 words (128 bits) and `other` has 1 word (64 bits), the result will have only 1 word but `len = 128`. This creates an inconsistency: `len` says 128 but there is only 1 word, so `is_set(65)` will panic with index out of bounds.

The same issue exists for `and()`, though it is less dangerous since `and()` uses `min(len)` (but still truncates words).

**Fix:** For `or()`, use `zip_longest` or manually handle the case where one bitmap has more words than the other. For example:
```rust
pub fn or(&self, other: &Bitmap) -> Bitmap {
    let len = self.len.max(other.len);
    let max_words = self.words.len().max(other.words.len());
    let mut words = vec![0u64; max_words];
    for i in 0..max_words {
        let a = self.words.get(i).copied().unwrap_or(0);
        let b = other.words.get(i).copied().unwrap_or(0);
        words[i] = a | b;
    }
    Bitmap { words, len }
}
```

Similarly for `and()`:
```rust
pub fn and(&self, other: &Bitmap) -> Bitmap {
    let len = self.len.min(other.len);
    let min_words = self.words.len().min(other.words.len());
    let words: Vec<u64> = self.words[..min_words].iter()
        .zip(other.words[..min_words].iter())
        .map(|(&a, &b)| a & b).collect();
    Bitmap { words, len }
}
```

### C3: `AHasher::default()` is not the stable public API

The plan uses `AHasher::default()` in `hash_column_utf8`:
```rust
let mut hasher = AHasher::default();
hasher.write(&data[start..end]);
hashes[i] = hasher.finish();
```

In `ahash` 0.8.x, `AHasher` does implement `Default`, but the documentation explicitly notes this creates a hasher with fixed keys (zero keys), which is fine for non-adversarial use but is not the recommended API. More importantly, creating a new `AHasher` per element is wasteful. The previous design review (simd-physical-plan-design-review-2.md) already flagged this exact issue:

> "Creating a new `AHasher` per element is expensive (AHasher initialization involves multiple state words)."

**Fix:** Use `ahash::RandomState` and call `hash_one()`, or create a single `AHasher` and clone it, or use `BuildHasher::build_hasher()` once and reset per element. For example:
```rust
use ahash::RandomState;
pub fn hash_column_utf8(data: &[u8], offsets: &[u32], hashes: &mut [u64]) {
    let state = RandomState::with_seeds(0, 0, 0, 0); // deterministic
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        hashes[i] = state.hash_one(&data[start..end]);
    }
}
```

### C4: JSONL simd-json migration step (Step 14) is too vague to implement

Step 14 says to "Locate the JSONL record reading path" and "Change it to use `simd_json::to_borrowed_value` or `simd_json::to_owned_value`", but provides no actual implementation code. Every other step in the plan provides complete code. Step 14 only provides a minimal test that tests simd-json's own API (not the integration) and hand-waves the actual conversion:

1. The existing `json_to_data_model` function converts `json::JsonValue` to `Value`. A new converter from `simd_json::BorrowedValue` (or `OwnedValue`) to `Value` is needed. This function does not exist anywhere in the plan.
2. The `json` crate's `JsonError` is in the `ReaderError` enum. Replacing `json::parse` with `simd_json::to_owned_value` requires changing the error type from `json::JsonError` to `simd_json::Error`.
3. The test `test_jsonl_parse_with_simd_json` only tests that simd-json can parse a JSON string -- it does not test the integration with the existing `Record` / `Value` types.
4. The buffer ownership semantics are different: `simd-json` mutates the input buffer, meaning `self.buf` (a `String`) would need to be converted to `Vec<u8>` via `unsafe { self.buf.as_bytes_mut() }` or by using a separate byte buffer. The plan does not address this.

**Fix:** Provide complete implementation code for Step 14, including:
- A `simd_json_to_data_model` function converting `simd_json::OwnedValue` to `Value`
- The modified `read_record` method for the JSONL path
- Error type changes in `ReaderError`
- Buffer handling (e.g., `let mut bytes = std::mem::take(&mut self.buf).into_bytes();`)

### C5: `ColumnBuilder` handles `Value::Missing` and `Value::Null` for `Mixed` columns incorrectly

When the column type is `Mixed`, the `push` method handles `Value::Missing` and `Value::Null` in Phase 1 before checking the type:

```rust
Value::Missing => {
    self.missing.push(false);
    self.null.push(false);
    None  // typed_value = None
}
Value::Null => {
    self.missing.push(true);
    self.null.push(false);
    None  // typed_value = None
}
```

Then in Phase 2, `None` goes to `push_default()`, which for `Mixed` pushes `Value::Null`:
```rust
ColumnType::Mixed => self.mixed_data.push(Value::Null),
```

This means for a `Mixed` column:
- `Value::Missing` -> bitmap says MISSING, but `mixed_data` gets `Value::Null` as placeholder. OK, since the bitmap will prevent reading this slot.
- `Value::Null` -> bitmap says NULL, and `mixed_data` gets `Value::Null` as placeholder. OK.

This is actually correct because the bitmaps are checked first in `typed_column_get_value`. However, it does waste a slot in `mixed_data` for values that will never be read. This is not a correctness bug but is wasteful. **Retracted as critical** -- this is a minor efficiency concern.

## Suggestions (nice to have)

### S1: `Bitmap::any()` does not mask padding bits in the last word

```rust
pub fn any(&self) -> bool {
    self.words.iter().any(|&w| w != 0)
}
```

If `all_set(65)` is created and then `not()` is called, the `not()` correctly masks the last word. But if a bitmap is constructed manually (e.g., via direct word manipulation), the padding bits in the last word could be non-zero, causing `any()` to return `true` when logically all valid bits are unset. Since the plan only uses `any()` through the public API (which always masks properly via `not()`), this is not a bug in practice, but defensive masking would be safer.

### S2: `SelectionVector::Indices` linear search is O(n) per row in `BatchToRowAdapter`

The `materialize_batch` method checks:
```rust
SelectionVector::Indices(indices) => indices.contains(&(row_idx as u32)),
```

This is `O(n * m)` where `n` is the batch size and `m` is the number of selected indices. For large selections this is quadratic. Consider converting to a `HashSet` or sorting + binary search, or converting to a `Bitmap` once before the loop.

### S3: The `count_ones()` method double-counts when `len` is an exact multiple of 64

Looking at the implementation:
```rust
let full_words = if self.len % 64 == 0 { self.words.len() } else { self.words.len() - 1 };
let mut count: usize = self.words[..full_words].iter()
    .map(|w| w.count_ones() as usize).sum();
if self.len % 64 != 0 {
    ...
}
```

When `len % 64 == 0`, `full_words == self.words.len()`, so all words are counted in the first pass. The second `if` block is skipped because `len % 64 == 0`. This is correct. When `len % 64 != 0`, it counts all words except the last in the first pass, then counts the masked last word. This is also correct. No issue here -- just confirming.

### S4: Consider adding a `Bitmap::len()` accessor

The `len` field is `pub(crate)` but tests access `bm.len` directly. A `pub fn len(&self) -> usize` method would be more idiomatic Rust.

### S5: `RowToBatchAdapter` consumes record ownership via `into_variables()` which then does `vars.get(name).cloned()`

The `into_variables()` returns an owned `LinkedHashMap`, but then each value is looked up with `.get(name).cloned()`. This clones the value even though the map is owned and could use `.remove(name)`. Consider using `vars.remove(name).unwrap_or(Value::Missing)` to avoid cloning.

### S6: Step 14 test should be an integration test, not a unit test

The test `test_jsonl_parse_with_simd_json` tests simd-json's own API, not the integration. It will pass even if the actual JSONL parsing code is never changed. The test should parse a JSONL line through the actual `Reader` / `RecordRead` implementation and verify the resulting `Record`.

## Verified Claims (things I confirmed are correct)

1. **C1 from Review 1 (sl commit):** All commit commands in v2 now correctly use `sl commit`. Verified throughout all 15 steps.

2. **C2 from Review 1 (simd-json version):** Updated to `simd-json = "0.17"`. Correct.

3. **C3 from Review 1 (module wiring order):** Step 2 now wires `pub mod batch;` into `src/execution/mod.rs` before writing tests. Step 6 creates the `src/simd/` module tree and wires it into `src/lib.rs` before Steps 7-9 write SIMD code. The ordering is now correct.

4. **C4 from Review 1 (SelectionVector::Indices u32):** Fixed to `vec![0u32, 5, 10]` in Step 4 test. Correct.

5. **C6 from Review 1 (ColumnBuilder type mismatch):** Fixed. On type mismatch, the code now pushes a default value to the typed data vector and marks the row as NULL (`missing=true, null=false`). The `int32_data`, `float32_data`, etc. vectors will always have the same length as the bitmap. Verified correct.

6. **C7 from Review 1 (Utf8 control flow):** Rewritten with a clean two-phase approach. No early returns, no dead code. The `typed_value` is either `Some(value)` or `None`, and Phase 2 handles each case cleanly. Verified correct.

7. **C9 from Review 1 (JSONL simd-json migration):** Added as Step 14. The step exists but is underspecified (see C4 above).

8. **C10 from Review 1 (redundant Utf8 check):** Removed as part of the C7 rewrite. The new two-phase approach has no redundant checks. Correct.

9. **S3 from Review 1 (Bitmap::not padding):** Added `len` field to `Bitmap`. The `not()` method now masks the last word. The `count_ones()` method masks the last partial word. The `all_set()` constructor pre-masks the last word. All verified correct.

10. **S4 from Review 1 (BatchToRowAdapter selection test):** Added `test_batch_to_row_adapter_with_selection` using `SelectionVector::Bitmap`. The test correctly verifies that only rows 1 and 3 (values 20 and 40) are emitted. Correct.

11. **S8 from Review 1 (hash_column_utf8):** Added in Step 9. Implementation uses AHash. Present and functional (though has the `AHasher::default()` concern noted in C3).

12. **Step dependencies are sound.** Verified the ordering: Step 1 (deps) -> Step 2 (batch module + wiring) -> Steps 3-5 (batch types) -> Step 6 (simd module + wiring) -> Steps 7-9 (kernels, parallel where possible) -> Steps 10-12 (adapters, sequential) -> Step 13 (integration) -> Step 14 (JSONL migration) -> Step 15 (final). All dependency constraints are met.

13. **Tests will compile.** With the module wiring happening in Steps 2 and 6, all test files are properly connected to the crate. The test code correctly references `crate::execution::batch::*`, `crate::common::types::Value`, `crate::syntax::ast::*`, and `super::stream::RecordStream`. All paths verified against the actual module structure.

14. **`StreamResult` type is correctly imported.** Step 5 adds `use super::types::StreamResult;` which resolves to `execution::types::StreamResult`. This is the correct path.

15. **`Bitmap` edge cases for len=0, 63, 64, 65:**
    - `len=0`: `all_set(0)` creates 0 words, `count_ones()` returns 0 (early return from `is_empty()`). `all_unset(0)` works similarly. Correct.
    - `len=63`: 1 word, `remainder=63`, last word masked to `(1<<63)-1 = 0x7FFFFFFFFFFFFFFF`. Correct.
    - `len=64`: 1 word, `remainder=0`, no masking needed. `count_ones()` counts the full word. Correct.
    - `len=65`: 2 words, `remainder=1`, last word masked to `(1<<1)-1 = 1`. Only bit 0 of word 1 is valid. Correct.

16. **`chrono::FixedOffset::east_opt(0)` is available.** The codebase already uses this in `src/execution/datasource.rs`. The `chrono = "0.4"` dependency supports it.

17. **`Bitmap` test `test_bitmap_not_masks_high_bits` is correct.** Creates a 65-element all-unset bitmap (2 words, all zeros), negates it. The `not()` masks the last word to `(1<<1)-1 = 1`, so only bits 0-64 are set. `count_ones()` counts 64 (from word 0 = all ones) + 1 (from word 1, bit 0 only) = 65. Correct.

18. **`Bitmap` test `test_bitmap_count_ones_ignores_padding` is correct.** Creates `all_set(65)`. The `all_set` constructor pre-masks the last word to `(1<<1)-1 = 1`. So `count_ones()` returns 64 + 1 = 65. Correct.
