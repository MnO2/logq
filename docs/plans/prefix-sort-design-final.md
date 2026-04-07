# PrefixSort Optimization for ORDER BY -- Final Design

## 1. Changes from v2

This final revision addresses all critical issues from review round 2.

**Fix 1 -- Null ordering (Critical #1, #2):** Reversed the slot layout from `[type_tag][null_byte][value]` to `[null_byte][type_tag][value]`. In the v2 layout, memcmp resolved on the type_tag byte first, causing Null/Missing (type_tag `0x00`) to sort BEFORE all non-null values -- the opposite of the intended nulls-last behavior. With the corrected layout, the null_byte is compared first: non-null values have `0x00` and Null/Missing/Object/Array have `0xFF`, so nulls sort last in ascending order as intended. The DESC byte-flip is also correct under this layout. All encoding rules, examples, and edge case descriptions have been updated.

**Fix 2 -- String tie-breaking specificity (Critical #3):** When memcmp returns Equal for the entire key portion, the comparator now explicitly decodes the type_tag byte from each entry at each key position (at offset `key_position_offset + 1`, since null_byte is at offset 0). If either entry has type_tag `0x04` (String), `0x06` (Host), or `0x07` (HttpRequest) at that position, the comparator falls back to `compare_values()` on the original records for that key. Otherwise, Equal is final. This avoids unnecessary fallback calls for non-string types whose encoded values happen to be identical.

**Fix 3 -- Index-based sorting (Critical #4):** Replaced the under-specified "view buffer as chunks and `sort_unstable_by`" with sorting a `Vec<usize>` index array. Rust's `sort_unstable_by` requires `&mut [T]` where `T` is a fixed-size type; it cannot sort variable-width byte chunks in a flat buffer. The comparator indexes into the flat buffer to compare the key portions of the chunks identified by each index. The flat buffer is still allocated contiguously for cache locality during comparison reads.

**Fix 4 -- Object/Array semantic change (Suggestion B):** Explicitly called out that Objects and Arrays now sort deterministically -- after all typed non-null values, with Object before Array -- rather than preserving input order via stable sort. The previous behavior where `compare_values()` returned `Equal` for these types against other types was undocumented and likely accidental. The new deterministic ordering is an intentional behavioral improvement.

**Additional (Suggestion D):** Noted that the fallback threshold could be a function of `entry_width` (which depends on key count and types), not just row count. A single Int key has 8-byte entries while 3 String keys at prefix length 32 produce 106-byte entries; the encoding overhead differs substantially.

---

## 2. Overview

### Problem

The current `ORDER BY` implementation in `src/execution/types.rs` (lines 831-872) sorts records using `sort_by` with a closure that, for every comparison:

1. Calls `Record::get_ref()` which performs a `LinkedHashMap::get()` hash lookup per key per comparison.
2. Dispatches through `compare_values()` which pattern-matches on the `Value` enum.
3. For `Host` and `HttpRequest` values, allocates a new `String` via `.to_string()` on every comparison.

For N rows and K sort keys, a typical sort performs O(N log N) comparisons, each doing K hash lookups and K type dispatches. This makes the sort comparison the bottleneck for large result sets.

### Solution

Introduce a PrefixSort optimization that encodes sort keys into a flat byte buffer once (O(N * K)), then sorts an index array using byte-slice comparisons (O(N log N) comparisons, each a simple `memcmp`). This eliminates per-comparison hash lookups, type dispatch, and string allocations.

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
[null_byte: 1 byte][type_tag: 1 byte][encoded_value: W bytes]
```

- **null_byte:** `0x00` for non-null typed values, `0xFF` for Null/Missing/Object/Array. This is the first byte so that memcmp resolves the null-vs-non-null distinction immediately: non-null values (starting with `0x00`) sort before null-like values (starting with `0xFF`) in ascending order.
- **type_tag:** Discriminant byte that groups values by type within the null/non-null partition, ensuring different types in the same column produce deterministic sort order.
- **encoded_value:** Type-specific encoding, W bytes wide.

### Type Tags

| Type        | null_byte | Tag  |
|-------------|-----------|------|
| Bool        | 0x00      | 0x01 |
| Int         | 0x00      | 0x02 |
| Float       | 0x00      | 0x03 |
| String      | 0x00      | 0x04 |
| DateTime    | 0x00      | 0x05 |
| Host        | 0x00      | 0x06 |
| HttpRequest | 0x00      | 0x07 |
| Null        | 0xFF      | 0x00 |
| Missing     | 0xFF      | 0x00 |
| Object      | 0xFF      | 0x08 |
| Array       | 0xFF      | 0x09 |

Null and Missing share null_byte `0xFF` and type_tag `0x00`. Since both encode identically, they sort as equal (matching current behavior where `compare_values` returns `Equal` for Null vs Missing).

Object and Array have null_byte `0xFF`, so they sort after all typed non-null values. Within the `0xFF` group, Object (type_tag `0x08`) sorts before Array (type_tag `0x09`). Null/Missing (type_tag `0x00`) sort before Object and Array within this group.

Ascending sort order across all types:

```
Bool(0x01) < Int(0x02) < Float(0x03) < String(0x04) < DateTime(0x05) < Host(0x06) < HttpRequest(0x07)
    < Null/Missing(0x00) < Object(0x08) < Array(0x09)
```

The left group has null_byte `0x00`; the right group (from Null onward) has null_byte `0xFF`.

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
        return u32::MAX; // 0xFFFFFFFF -- sorts after everything
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

Note: the prefix length is in bytes, not characters. For ASCII strings, 16 bytes equals 16 characters. For multi-byte UTF-8 strings, 16 bytes may cut mid-character (e.g., 4-byte emoji characters yield only 4 characters in 16 bytes). This does not cause correctness issues -- UTF-8's byte ordering preserves codepoint ordering, and cutting mid-character still produces a valid comparison prefix -- but non-ASCII strings may have less effective prefix discrimination.

**DateTime** (W = 8 bytes, total slot = 10 bytes):

- Encode as `i64` Unix timestamp (seconds since epoch) using the same sign-flip trick as Int: `(ts as u64) ^ 0x8000000000000000`, stored big-endian.

**Host** (W = configurable prefix length, same as string, total slot = 2 + W bytes):

- Call `host.to_string()` once during encoding (not per comparison).
- Encode the resulting string using the String encoding above.

**HttpRequest** (W = configurable prefix length, same as string, total slot = 2 + W bytes):

- Call `http_request.to_string()` once during encoding (not per comparison).
- Encode the resulting string using the String encoding above.

**Null / Missing** (W = max width for that key position, total slot = 2 + W bytes):

- null_byte: `0xFF`
- type_tag: `0x00`
- encoded_value: all zeros.
- With null_byte `0xFF`, these sort after all non-null values (which have null_byte `0x00`) in ascending order.

Concrete example:
- Null entry for an Int-width key: `[0xFF][0x00][0x00 0x00 0x00 0x00]`
- Int(5) entry:                     `[0x00][0x02][0x80 0x00 0x00 0x05]`
- memcmp: first byte `0xFF > 0x00`, result is Greater. Null sorts after Int(5). Correct.

**Object / Array** (W = max width for that key position, total slot = 2 + W bytes):

- null_byte: `0xFF`
- type_tag: `0x08` (Object) or `0x09` (Array)
- encoded_value: all zeros.
- These sort after all typed non-null values due to null_byte `0xFF`. Within the `0xFF` group: Null/Missing (type_tag `0x00`) sort first, then Object (`0x08`), then Array (`0x09`).

This is an intentional behavioral change from the current implementation. The current `compare_values()` returns `Equal` for Object vs. any non-null/non-missing value (catch-all arm), which means Object and Array ordering relative to typed values was determined by `sort_by`'s stability -- effectively preserving input order. The new behavior deterministically places Objects and Arrays after typed values, Object before Array. This is strictly better: it produces a consistent, reproducible ordering rather than depending on input arrival order. The previous behavior was undocumented and likely accidental.

### Descending Order

For descending sort keys, flip all bytes in the entire slot (null_byte, type_tag, and encoded_value) after encoding: `byte = !byte`. This reverses the memcmp order.

Example with corrected layout:
- Null in ASC: `[0xFF][0x00][0x00...]` -- sorts last among all values (correct).
- Null in DESC after flip: `[0x00][0xFF][0xFF...]` -- sorts first (correct: DESC reverses order).
- Int(5) in ASC: `[0x00][0x02][0x80 0x00 0x00 0x05]` -- sorts before Null (correct).
- Int(5) in DESC after flip: `[0xFF][0xFD][0x7F 0xFF 0xFF 0xFA]` -- sorts after Null's `[0x00][0xFF]...` (correct: DESC Int after Null means original ASC Int was before Null).

### Row Entry Layout

Each row's prefix entry in the buffer:

```
[key_0 slot][key_1 slot]...[key_{K-1} slot][row_index: 4 bytes (u32 big-endian)]
```

The row index is appended so that after sorting, we know which original row each entry corresponds to.

Total entry width = sum of all key slot widths + 4 bytes.

### Memory Usage

For N rows with K sort keys, the buffer is `N * entry_width` bytes, plus `N * size_of::<usize>()` bytes for the index array.

Examples:
- 2 Int keys: entry_width = 6 + 6 + 4 = 16 bytes. For 100K rows: 1.6 MB buffer + 0.8 MB indices.
- 2 String keys (prefix=16): entry_width = 18 + 18 + 4 = 40 bytes. For 100K rows: 4 MB buffer + 0.8 MB indices.
- 2 String keys (prefix=16), 1M rows: 40 MB buffer + 8 MB indices.

The buffer and index array are temporary and freed after Phase 3 (reorder).

Since columns can be mixed-type, each key position uses the maximum width across all possible types: `max(1, 4, 4, string_prefix_len, 8, string_prefix_len, string_prefix_len)` + 2 bytes overhead (null_byte + type_tag). With the default `string_prefix_len` of 16, this is 18 bytes per key slot. An Int key (which only needs 6 bytes) wastes 12 bytes per entry. This is a known tradeoff: per-key-position width computation would require a pre-scan of all rows to determine the actual types present, adding complexity for marginal space savings.

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

1. Compute `entry_width` from the sort key configuration. Each key slot is `max_value_width + 2` bytes (null_byte + type_tag + encoded_value). The `max_value_width` is `max(1, 4, 4, string_prefix_len, 8, string_prefix_len, string_prefix_len)`, which simplifies to `max(8, string_prefix_len)`. Add 4 bytes for the trailing row index. Total: `K * (max_value_width + 2) + 4`.
2. Allocate `buffer: Vec<u8>` of size `records.len() * entry_width`, zero-initialized.
3. For each row `i`, for each sort key `k`:
   - Look up the value via `Record::get_ref()` (one hash lookup per key per row, done once).
   - Encode the null_byte, type_tag, and value into the appropriate slot.
   - If the ordering for key `k` is `Desc`, flip all bytes in the slot.
4. Write the row index `i` as `u32` big-endian at the end of the entry.

### Phase 2: Sort via Index Array

```rust
fn sort_by_prefix(
    buffer: &[u8],
    entry_width: usize,
    key_width: usize,           // entry_width - 4 (excludes row index)
    key_slot_offsets: &[usize],  // byte offset of each key slot within an entry
    key_slot_width: usize,       // width of each key slot (null_byte + type_tag + value)
    records: &[Record],
    sort_keys: &[PathExpr],
    orderings: &[Ordering],
) -> Vec<usize>
```

1. Create an index array: `let mut indices: Vec<usize> = (0..records.len()).collect();`
2. Sort the index array using `indices.sort_unstable_by(|&a, &b| { ... })` where the comparator:
   - Computes the byte offset of each entry: `a * entry_width` and `b * entry_width`.
   - Compares the key portion (first `key_width` bytes) of each entry using `buffer[a_off..a_off + key_width].cmp(&buffer[b_off..b_off + key_width])`.
   - If the key comparison returns `Equal`, performs string tie-breaking (see below).
   - Otherwise, returns the comparison result.

**String prefix tie-breaking (when memcmp returns Equal):**

When the byte comparison of the entire key portion returns `Equal`, the two entries may still differ in full string/host/httprequest values whose prefixes were truncated. The comparator performs the following for each key position `k`:

1. Compute the offset of key slot `k` within entry `a`: `slot_offset_a = a * entry_width + key_slot_offsets[k]`.
2. Read the type_tag byte at `slot_offset_a + 1` (offset +1 because null_byte is at offset 0). Similarly for entry `b`.
3. If the ordering for key `k` is `Desc`, the type_tag byte was flipped during encoding. Un-flip it: `type_tag = !raw_byte`.
4. If either entry's type_tag at this key position is `0x04` (String), `0x06` (Host), or `0x07` (HttpRequest), fall back to `compare_values()` on the original records for this key:
   - Decode row indices from the trailing 4 bytes of each entry.
   - Look up `records[row_a].get_ref(sort_keys[k])` and `records[row_b].get_ref(sort_keys[k])`.
   - Call `compare_values()` on the two values.
   - If the result is not `Equal`, return it (applying `Ordering::reverse()` if `Desc`).
5. If neither entry has a string-type tag at this position, `Equal` is final for this key -- move to the next key position.
6. If all key positions resolve to `Equal`, return `std::cmp::Ordering::Equal`.

This logic ensures:
- Non-string types (Int, Bool, Float, DateTime) that encode identically are truly equal; no fallback needed.
- String-type values that share a prefix are correctly ordered by their full values.
- The fallback only fires when prefixes actually tie, which is rare if the prefix length is well-chosen.

Note: `records` must not be moved or mutated during the sort. This is naturally enforced by the borrow checker since `sort_unstable_by` takes `&mut [usize]` on the index array (not on records or the buffer) and the closure captures `&records` and `&buffer` as shared references.

**Why an index array rather than sorting the buffer directly:**

Rust's `[T]::sort_unstable_by` operates on `&mut [T]` where each element `T` is a fixed-size type known at compile time. It cannot sort a `&mut [u8]` buffer treating every `entry_width` bytes as a single element, because `entry_width` is a runtime value. Sorting a `Vec<usize>` index array is the simplest correct approach. The flat buffer is still allocated contiguously, so the comparison reads during the sort enjoy cache locality. The index array adds `N * size_of::<usize>()` bytes of memory, which is modest compared to the prefix buffer itself.

### Phase 3: Reorder Records

```rust
fn reorder_records(
    buffer: &[u8],
    entry_width: usize,
    sorted_indices: &[usize],
    records: Vec<Record>,
) -> VecDeque<Record>
```

1. For each index `i` in `sorted_indices`, read the row index from the buffer: the last 4 bytes of the entry at `buffer[sorted_indices[i] * entry_width ..]` decoded as `u32` big-endian.
2. Allocate a new `VecDeque<Record>` with capacity `records.len()`.
3. Convert `records` into a `Vec<Option<Record>>` by wrapping each in `Some`.
4. For each sorted row index, `.take()` the record from the `Option` vec and push it to the output `VecDeque`.

This is the simple approach: allocate a new `VecDeque` from sorted indices rather than an in-place cycle-chase permutation. The temporary memory cost of the `Option` wrappers (one pointer-width per row) is acceptable and the code is significantly simpler and less error-prone than the in-place algorithm.

### Fallback Path

If the number of rows is below the threshold, skip PrefixSort entirely and use the current `sort_by` + `compare_values()` approach. The encoding overhead is not justified for small result sets.

The initial threshold is 64 rows, to be refined via benchmarking. The crossover point likely depends not just on row count but also on `entry_width`, which varies with the number of sort keys and their types. A single Int key has entry_width of 8 bytes (low encoding overhead), while 3 String keys at prefix length 32 have entry_width of 106 bytes (high encoding overhead). The threshold could therefore be a function of `entry_width` rather than a fixed row count -- for example, `threshold = base_threshold * (reference_width / entry_width)` where larger entries raise the crossover point. The `bench_sort.rs` benchmark sweep (see Section 7) will determine the right formula.

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
    pub threshold: usize,         // default: 64, TBD via benchmarking
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

The null_byte + type_tag pair ensures that values of different types in the same column sort into deterministic groups. Non-null typed values (null_byte `0x00`) sort first, ordered by type_tag: Bool (0x01) < Int (0x02) < Float (0x03) < String (0x04) < DateTime (0x05) < Host (0x06) < HttpRequest (0x07). Then null-like values (null_byte `0xFF`): Null/Missing (0x00) < Object (0x08) < Array (0x09).

This is a refinement over the current behavior, where `compare_values` returns `Equal` for cross-type pairs (preserving input order via stable sort). The new behavior is strictly better: it produces a consistent, deterministic ordering for mixed-type columns rather than relying on input order. Since the current cross-type `Equal` behavior is undocumented and likely unintentional, this change is acceptable.

### Object and Array Values

Encoded with null_byte `0xFF` and their respective type tags (`0x08` Object, `0x09` Array). The encoded_value region is zeroed. All Objects compare as equal to each other; all Arrays compare as equal to each other. Objects sort before Arrays (type_tag `0x08` < `0x09`). Both sort after all non-null typed values due to null_byte `0xFF`.

This is an intentional behavioral change. The current `compare_values()` returns `Equal` when comparing Object or Array against other non-null types, which made their relative ordering depend on `sort_by` stability and input order. The new behavior is deterministic and reproducible: Objects and Arrays always sort after typed values, and Object always sorts before Array. This is documented as an intentional improvement.

### Float NaN

All NaN bit patterns (positive NaN, negative NaN, signaling NaN, quiet NaN) encode to `0xFFFFFFFF`, sorting after +Infinity (`0xFFFFFFFE`) and all finite values. This matches `OrderedFloat<f32>` semantics.

### String Prefix Collisions

When two entries have identical byte prefixes for a string-type key, the `sort_unstable_by` comparator falls back to `compare_values()` on the original `Record` values using the stored row indices. This ensures correctness even when prefixes collide. The fallback incurs the same cost as the current sort (hash lookup + type dispatch) but only for the colliding pair on the colliding key, not for every comparison.

### Empty Result Sets and Single Rows

If `records.len() <= 1`, return immediately without sorting (no work to do). This is handled before the threshold check.

### Null/Missing Ordering

In ascending order, Null and Missing sort after all typed non-null values: their null_byte `0xFF` is greater than non-null's `0x00`, so memcmp resolves on the first byte. In descending order, the byte flip makes the null_byte `0x00` (sorting first) and non-null's null_byte becomes `0xFF` (sorting last). This matches the current `compare_values` behavior: "Null/Missing sort after all non-null values in ascending order."

Concrete ascending example:
- Int(5):  `[0x00][0x02][0x80 0x00 0x00 0x05]` -- null_byte `0x00`
- Null:    `[0xFF][0x00][0x00 0x00 0x00 0x00]` -- null_byte `0xFF`
- memcmp: `0x00 < 0xFF`, so Int(5) < Null. Int sorts first; Null sorts last. Correct.

Concrete descending example (after byte flip):
- Int(5):  `[0xFF][0xFD][0x7F 0xFF 0xFF 0xFA]` -- flipped null_byte `0xFF`
- Null:    `[0x00][0xFF][0xFF 0xFF 0xFF 0xFF]` -- flipped null_byte `0x00`
- memcmp: `0xFF > 0x00`, so Int(5) > Null. Int sorts last; Null sorts first. Correct (DESC reverses order).

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

Add a parameterized benchmark that sweeps the threshold from 32 to 512 (powers of 2) for each key type to find the crossover point. The threshold may differ by key type (int keys have less encoding overhead than string keys). The sweep should be parameterized by scenario (not just row count) to determine whether the threshold should be a function of `entry_width`.

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
