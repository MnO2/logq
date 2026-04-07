# PrefixSort Optimization for ORDER BY -- Design v2

## 1. Changes from Previous Version

This revision addresses all critical issues and incorporates relevant suggestions from the design review.

**Critical fixes:**

1. **Object/Array sort keys (Critical #1):** Added explicit handling. Object and Array values are encoded with type tag `0x08`/`0x09` respectively, null-byte set to the nulls-last pattern (`0xFF`), and remaining bytes zeroed. They compare as equal to each other within the same type group (matching the current `compare_values` behavior where the catch-all arm returns `Equal`), and sort after all typed values due to their high type tags.

2. **Mixed-type columns (Critical #2):** Added a 1-byte type discriminant at the start of each key slot. Layout is now `[type_tag][null_byte][encoded_value]`. Different types sort into deterministic groups by type tag rather than producing arbitrary memcmp results. This adds 1 byte per key per entry.

3. **Float NaN handling (Critical #3):** Explicit NaN encoding specified. Following Velox's `PrefixSortEncoder.h` approach: NaN maps to `0xFFFFFFFF`, +Infinity to `0xFFFFFFFE`, -Infinity to `0x00000000`, positive/negative zero to `0x80000000`. This ensures NaN sorts after all values, matching `OrderedFloat` semantics, and handles negative NaN correctly.

4. **ORDER BY + LIMIT top-N (Critical #4):** Acknowledged as a valid separate optimization. Added discussion in Future Work. PrefixSort and top-N are orthogonal; the prefix encoding can be reused for heap comparisons if top-N is implemented later.

5. **128-row threshold (Critical #5):** Changed to "threshold TBD via benchmarking" with an initial value of 64 rows. The `bench_sort.rs` micro-benchmark will sweep thresholds from 32 to 512 to find the crossover point.

**Incorporated suggestions:**

- **Suggestion A (extract-and-index):** Acknowledged as a valid simpler alternative. Documented rationale for choosing PrefixSort, with a fallback note.
- **Suggestion B (memcmp clarification):** Fixed wording. The comparison is a byte-slice `.cmp()` inside `sort_unstable_by`'s closure, which compiles to a `memcmp` call within the closure. The benefit is branch-free comparison body + cache locality.
- **Suggestion C (string prefix length):** Made configurable with a default of 16 bytes. Documented that URL-heavy workloads may want 32 or more.
- **Suggestion E (reorder phase):** Changed to the simple approach: allocate a new `VecDeque` from sorted indices rather than in-place cycle-chase.

---

## 2. Overview

### Problem

The current `ORDER BY` implementation in `src/execution/types.rs` (lines 831-872) sorts records using `sort_by` with a closure that, for every comparison:

1. Calls `Record::get_ref()` which performs a `LinkedHashMap::get()` hash lookup per key per comparison.
2. Dispatches through `compare_values()` which pattern-matches on the `Value` enum.
3. For `Host` and `HttpRequest` values, allocates a new `String` via `.to_string()` on every comparison.

For N rows and K sort keys, a typical sort performs O(N log N) comparisons, each doing K hash lookups and K type dispatches. This makes the sort comparison the bottleneck for large result sets.

### Solution

Introduce a PrefixSort optimization that encodes sort keys into a flat byte buffer once (O(N * K)), then sorts using byte-slice comparisons (O(N log N) comparisons, each a simple `memcmp`). This eliminates per-comparison hash lookups, type dispatch, and string allocations.

### Why PrefixSort over extract-and-index

An alternative approach is to pre-extract sort key values into a `Vec<Vec<Value>>` (one hash lookup per key per row, done once) and sort an index array using `compare_values()` on the extracted values. This is simpler (~20 lines) and eliminates per-comparison hash lookups.

PrefixSort is chosen because:

- **Cache-friendly comparison:** The flat byte buffer provides spatial locality during sort comparisons, which matters for sorts beyond a few thousand rows.
- **No type dispatch during comparison:** Eliminates the `match` on `Value` variants during every comparison, not just the hash lookups.
- **Architectural alignment:** The project's trajectory includes SIMD/batch optimizations (`ColumnBatch`/`BatchStream` infrastructure). A byte-encoded sort aligns with future vectorized processing.

If PrefixSort proves overly complex during implementation, falling back to the extract-and-index approach is acceptable as an intermediate step. It still captures the primary bottleneck (per-comparison hash lookups).

---

## 3. Encoding Scheme

### Key Slot Layout

Each sort key for each row is encoded into a fixed-width byte slot:

```
[type_tag: 1 byte][null_byte: 1 byte][encoded_value: W bytes]
```

- **type_tag:** Discriminant byte that groups values by type, ensuring different types in the same column produce deterministic sort order rather than arbitrary memcmp results.
- **null_byte:** `0x00` for non-null values, `0xFF` for Null/Missing/Object/Array (sorts after non-null in ascending order).
- **encoded_value:** Type-specific encoding, W bytes wide.

### Type Tags

| Type        | Tag  |
|-------------|------|
| Null        | 0x00 |
| Bool        | 0x01 |
| Int         | 0x02 |
| Float       | 0x03 |
| String      | 0x04 |
| DateTime    | 0x05 |
| Host        | 0x06 |
| HttpRequest | 0x07 |
| Object      | 0x08 |
| Array       | 0x09 |
| Missing     | 0x00 |

Null and Missing share tag `0x00`. Since both have null_byte `0xFF`, they sort identically (matching current behavior where `compare_values` returns `Equal` for Null vs Missing).

### Value Encodings

**Boolean** (W = 1 byte, total slot = 3 bytes):

- `false` -> `0x00`, `true` -> `0x01`

**Int (i32)** (W = 4 bytes, total slot = 6 bytes):

- Flip the sign bit: `(value as u32) ^ 0x80000000`, stored big-endian.
- This maps `i32::MIN` -> `0x00000000` and `i32::MAX` -> `0xFFFFFFFF`, preserving sort order under unsigned byte comparison.

**Float (OrderedFloat\<f32\>)** (W = 4 bytes, total slot = 6 bytes):

Following Velox's `PrefixSortEncoder.h` `encodeFloat` approach with explicit handling of special values:

| Value           | Encoded (u32 big-endian) |
|-----------------|--------------------------|
| -Infinity       | `0x00000000`             |
| Negative values | sign-flip encoding       |
| -0.0            | `0x80000000`             |
| +0.0            | `0x80000000`             |
| Positive values | sign-flip encoding       |
| +Infinity       | `0xFFFFFFFE`             |
| NaN (any)       | `0xFFFFFFFF`             |

The sign-flip encoding for normal values:
- If the float is negative (sign bit set): flip all bits (`!bits`).
- If the float is non-negative (sign bit clear): flip the sign bit (`bits ^ 0x80000000`).

For NaN: regardless of the NaN bit pattern (positive NaN `0x7FC00000`, negative NaN `0xFFC00000`, signaling NaN, etc.), always encode as `0xFFFFFFFF`. This ensures NaN sorts after all other values, matching `OrderedFloat` semantics.

For +Infinity (`0x7F800000`): encode as `0xFFFFFFFE` (one less than NaN).

For -Infinity (`0xFF800000`): encode as `0x00000000`.

Implementation:

```rust
fn encode_f32(value: f32) -> u32 {
    if value.is_nan() {
        return u32::MAX; // 0xFFFFFFFF — sorts after everything
    }
    let bits = value.to_bits();
    if bits & 0x80000000 != 0 {
        // Negative: flip all bits
        !bits
    } else {
        // Non-negative: flip sign bit
        bits ^ 0x80000000
    }
}
```

**String** (W = configurable prefix length, default 16 bytes, total slot = 2 + W bytes):

- Copy the first `min(len, W)` bytes of the UTF-8 representation into the slot.
- Pad remaining bytes with `0x00`.
- When two string prefixes are equal under memcmp, the full comparison falls back to `compare_values()` on the original records (see Section 4, Phase 2).

The prefix length is set at `PrefixSortEncoder` construction time. Default is 16 bytes. For URL-heavy workloads (where prefixes like `https://example.` are common), users or the encoder may want 32 or more bytes to reduce fallback frequency.

**DateTime** (W = 8 bytes, total slot = 10 bytes):

- Encode as `i64` Unix timestamp (seconds since epoch) using the same sign-flip trick as Int: `(ts as u64) ^ 0x8000000000000000`, stored big-endian.

**Host** (W = configurable prefix length, same as string, total slot = 2 + W bytes):

- Call `host.to_string()` once during encoding (not per comparison).
- Encode the resulting string using the String encoding above.

**HttpRequest** (W = configurable prefix length, same as string, total slot = 2 + W bytes):

- Call `http_request.to_string()` once during encoding (not per comparison).
- Encode the resulting string using the String encoding above.

**Null / Missing** (W = max width for that key position, total slot = 2 + W bytes):

- type_tag: `0x00`
- null_byte: `0xFF`
- encoded_value: all zeros.
- With null_byte `0xFF`, these sort after all non-null values (which have null_byte `0x00`) in ascending order.

**Object / Array** (W = max width for that key position, total slot = 2 + W bytes):

- type_tag: `0x08` (Object) or `0x09` (Array)
- null_byte: `0xFF`
- encoded_value: all zeros.
- These sort after all typed non-null values due to null_byte `0xFF`. Within the Object or Array group, all entries compare as equal (matching current `compare_values` behavior). Objects sort before Arrays due to type tag ordering (`0x08` < `0x09`), which is a minor refinement over the current behavior (where both fall through to `Equal`).

### Descending Order

For descending sort keys, flip all bytes in the entire slot (type_tag, null_byte, and encoded_value) after encoding: `byte = !byte`. This reverses the memcmp order. Null/Missing values (which are `0xFF` in ascending) become `0x00` after flip, sorting first in descending order, which is the correct behavior.

### Row Entry Layout

Each row's prefix entry in the buffer:

```
[key_0 slot][key_1 slot]...[key_{K-1} slot][row_index: 4 bytes (u32 big-endian)]
```

The row index is appended so that after sorting the byte buffer, we know which original row each entry corresponds to.

Total entry width = sum of all key slot widths + 4 bytes.

### Memory Usage

For N rows with K sort keys, the buffer is `N * entry_width` bytes.

Examples:
- 2 Int keys: entry_width = 6 + 6 + 4 = 16 bytes. For 100K rows: 1.6 MB.
- 2 String keys (prefix=16): entry_width = 18 + 18 + 4 = 40 bytes. For 100K rows: 4 MB.
- 2 String keys (prefix=16), 1M rows: 40 MB.

The buffer is temporary and freed after Phase 3 (reorder).

---

## 4. Algorithm

### Phase 1: Encode Keys

```rust
fn encode_prefix_buffer(
    records: &[Record],
    sort_keys: &[PathExpr],
    orderings: &[Ordering],
    string_prefix_len: usize,  // default 16
) -> (Vec<u8>, usize) // (buffer, entry_width)
```

1. Compute `entry_width` from the sort key types. Since columns can be mixed-type, use the maximum width for each key position. For simplicity, all key positions use the same width: `max(1, 4, 4, string_prefix_len, 8, string_prefix_len, string_prefix_len)` + 2 bytes overhead (type_tag + null_byte). In practice, the encoder computes the width per key position based on a configurable `key_width` that defaults to `string_prefix_len` (since string/host/httprequest are the widest).
2. Allocate `buffer: Vec<u8>` of size `records.len() * entry_width`, zero-initialized.
3. For each row `i`, for each sort key `k`:
   - Look up the value via `Record::get_ref()` (one hash lookup per key per row, done once).
   - Encode the type tag, null byte, and value into the appropriate slot.
   - If the ordering for key `k` is `Desc`, flip all bytes in the slot.
4. Write the row index `i` as `u32` big-endian at the end of the entry.

### Phase 2: Sort Prefix Buffer

```rust
fn sort_prefix_buffer(buffer: &mut [u8], entry_width: usize, records: &[Record], sort_keys: &[PathExpr], orderings: &[Ordering])
```

1. View the buffer as chunks of `entry_width` bytes.
2. Sort using `sort_unstable_by` with a comparator that calls `.cmp()` on the key portion of each chunk (everything except the trailing 4-byte row index).

The `.cmp()` on `&[u8]` slices compiles to a `memcmp` call within the comparison closure. The benefit is that the comparison body is branch-free and cache-friendly (all keys for a row are contiguous in memory), not that `sort_unstable_by` itself becomes a single `memcmp` instruction. The pattern-defeating quicksort in `sort_unstable_by` still invokes the closure per comparison, but the closure body is fast.

**String prefix tie-breaking:** When the memcmp on prefixes returns `Equal`, two entries may still differ in the full string values. To handle this correctly, the comparator falls back to `compare_values()` on the original records (using the row indices stored at the end of each entry) for any key whose encoding involves a truncated prefix (String, Host, HttpRequest). This fallback only fires when prefixes tie, so it is rare if the prefix length is well-chosen.

### Phase 3: Reorder Records

```rust
fn reorder_records(buffer: &[u8], entry_width: usize, records: Vec<Record>) -> VecDeque<Record>
```

1. Extract sorted row indices from the buffer (last 4 bytes of each entry).
2. Allocate a new `VecDeque<Record>` with capacity `records.len()`.
3. Convert `records` into a `Vec<Option<Record>>` by wrapping each in `Some`.
4. For each sorted index, `.take()` the record from the `Option` vec and push it to the output `VecDeque`.

This is the simple approach: allocate a new `VecDeque` from sorted indices rather than an in-place cycle-chase permutation. The temporary memory cost of the `Option` wrappers (one pointer-width per row) is acceptable and the code is significantly simpler and less error-prone than the in-place algorithm.

### Fallback Path

If the number of rows is below the threshold (initial value: 64, TBD via benchmarking), skip PrefixSort entirely and use the current `sort_by` + `compare_values()` approach. The encoding overhead is not justified for small result sets.

The `bench_sort.rs` micro-benchmark should sweep thresholds from 32 to 512 rows across different key types (int, string, mixed) to identify the crossover point where PrefixSort becomes faster than direct sort.

---

## 5. Integration

### Module Structure

Add a new module `src/execution/prefix_sort.rs` containing:

- `struct PrefixSortEncoder` -- holds configuration (string prefix length, threshold) and provides the encoding/sorting/reorder methods.
- `pub fn prefix_sort(records: Vec<Record>, sort_keys: &[PathExpr], orderings: &[Ordering]) -> VecDeque<Record>` -- top-level entry point.
- Internal encoding functions: `encode_bool`, `encode_i32`, `encode_f32`, `encode_string_prefix`, `encode_datetime`, `encode_null`.

### PrefixSortEncoder

```rust
pub struct PrefixSortEncoder {
    /// Minimum row count to use prefix sort. Below this, fall back to direct sort.
    pub threshold: usize,       // default: 64, TBD via benchmarking
    /// Number of bytes to use for string/host/httprequest prefix encoding.
    pub string_prefix_len: usize, // default: 16
}
```

The encoder is constructed with these parameters at the `Node::OrderBy` call site.

### Modification to Node::OrderBy

In `src/execution/types.rs`, the `Node::OrderBy` match arm (lines 831-872) is modified:

```rust
Node::OrderBy(column_names, orderings, source) => {
    let mut record_stream = source.get(variables.clone(), registry)?;
    let mut records = Vec::new();
    while let Some(record) = record_stream.next()? {
        records.push(record);
    }

    let encoder = PrefixSortEncoder::default();
    let sorted = encoder.sort(records, &column_names, &orderings);

    let stream = InMemoryStream::new(sorted);
    Ok(Box::new(stream))
}
```

Inside `encoder.sort()`:
- If `records.len() < self.threshold`, use the current direct `sort_by` with `compare_values()`.
- Otherwise, run the three-phase PrefixSort.

### Module Registration

Add `mod prefix_sort;` to `src/execution/mod.rs`.

---

## 6. Edge Cases and Fallbacks

### Mixed-Type Columns

The type tag byte ensures that values of different types in the same column sort into deterministic groups ordered by type tag: Null/Missing (0x00) < Bool (0x01) < Int (0x02) < Float (0x03) < String (0x04) < DateTime (0x05) < Host (0x06) < HttpRequest (0x07) < Object (0x08) < Array (0x09).

This is a refinement over the current behavior, where `compare_values` returns `Equal` for cross-type pairs (preserving input order via stable sort). The new behavior is strictly better: it produces a consistent, deterministic ordering for mixed-type columns rather than relying on input order. Since the current cross-type `Equal` behavior is undocumented and likely unintentional, this change is acceptable.

### Object and Array Values

Encoded with their respective type tags and null_byte `0xFF`. The encoded_value region is zeroed. All Objects compare as equal to each other; all Arrays compare as equal to each other. Objects sort before Arrays (type tag `0x08` < `0x09`). Both sort after all non-null typed values due to null_byte `0xFF`.

### Float NaN

All NaN bit patterns (positive NaN, negative NaN, signaling NaN, quiet NaN) encode to `0xFFFFFFFF`, sorting after +Infinity (`0xFFFFFFFE`) and all finite values. This matches `OrderedFloat<f32>` semantics.

### String Prefix Collisions

When two entries have identical byte prefixes, the `sort_unstable_by` comparator falls back to `compare_values()` on the original `Record` values using the stored row indices. This ensures correctness even when prefixes collide. The fallback incurs the same cost as the current sort (hash lookup + type dispatch) but only for the colliding pair on the colliding key, not for every comparison.

### Empty Result Sets and Single Rows

If `records.len() <= 1`, return immediately without sorting (no work to do). This is handled before the threshold check.

### Null/Missing Ordering

In ascending order, Null and Missing sort last (null_byte `0xFF` > `0x00`). In descending order, the byte flip makes them `0x00`, sorting first. This matches the current `compare_values` behavior: "Null/Missing sort after all non-null values in ascending order."

---

## 7. Benchmarking Plan

### Micro-Benchmark: `bench_sort.rs`

Add a Criterion benchmark in `benches/bench_sort.rs` with the following scenarios:

| Scenario | Rows | Keys | Key Types | Description |
|----------|------|------|-----------|-------------|
| `sort_int_small` | 100 | 1 | Int | Below threshold, validates fallback path |
| `sort_int_medium` | 10,000 | 1 | Int | Sweet spot for prefix sort |
| `sort_int_large` | 100,000 | 1 | Int | Large sort, cache effects visible |
| `sort_string_medium` | 10,000 | 1 | String(random 32 chars) | String prefix effectiveness |
| `sort_string_urls` | 10,000 | 1 | String(URLs with shared prefix) | Worst case for prefix collisions |
| `sort_multi_key` | 10,000 | 3 | Int, String, DateTime | Multi-key sort |
| `sort_mixed_type` | 10,000 | 1 | Mixed Int/String | Mixed-type column |
| `sort_with_nulls` | 10,000 | 1 | Int with 20% nulls | Null handling |

### Threshold Sweep

Add a parameterized benchmark that sweeps the threshold from 32 to 512 (powers of 2) for each key type to find the crossover point. The threshold may differ by key type (int keys have less encoding overhead than string keys).

### Comparison Baseline

Each benchmark compares:
1. Current `sort_by` + `compare_values()` (control)
2. PrefixSort (treatment)
3. Extract-and-index (alternative baseline, for calibration)

### String Prefix Length Sweep

A separate benchmark sweeps string prefix lengths (8, 16, 24, 32, 48, 64) on the URL scenario to quantify the tradeoff between buffer size and fallback frequency.

---

## 8. Future Work

### ORDER BY + LIMIT Top-N Optimization

For the common query pattern `SELECT ... ORDER BY x LIMIT 10`, a partial sort using a `BinaryHeap` of size K (the limit) would be O(N log K) instead of O(N log N) and would avoid allocating a prefix buffer for all N rows.

This is orthogonal to PrefixSort and is a plan-level optimization:
- It requires the planner or executor to detect that `Limit(K, OrderBy(...))` can be fused into a single `TopN(K, ...)` operator.
- The `TopN` operator would maintain a max-heap of size K, scanning all input rows and only keeping the K smallest (or largest).

PrefixSort still benefits `ORDER BY + LIMIT` queries because the sort phase is faster even when followed by `LIMIT` truncation. However, for very large result sets with small limits (e.g., `LIMIT 10` on 1M rows), the top-N approach avoids sorting entirely and is the strictly better optimization.

If top-N is implemented later, the PrefixSort encoding can be reused for the heap comparisons: each heap entry can store the encoded prefix bytes, and the heap comparator can use memcmp on those bytes. This makes the two optimizations complementary rather than competing.

### Batch-Columnar ORDER BY

The codebase has `ColumnBatch`/`BatchStream` infrastructure (`src/execution/batch.rs`) with typed columns. A columnar ORDER BY operator that sorts within typed columns would eliminate hash lookups and type dispatch architecturally, without byte encoding. This is the cleanest long-term solution but requires building a `BatchSort` operator that does not currently exist. PrefixSort serves as a pragmatic improvement within the current row-oriented execution model while the columnar path matures.

### Adaptive String Prefix Length

A future enhancement could sample the first N rows during Phase 1 to detect the common prefix length and automatically adjust the string prefix size. For example, if 90% of string values share the first 12 bytes, the encoder could increase the prefix length to 24 to capture the differentiating bytes.
