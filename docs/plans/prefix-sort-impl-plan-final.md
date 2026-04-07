# Plan: PrefixSort Optimization for ORDER BY (v2)

**Goal**: Replace logq's per-comparison hash-lookup sort with a PrefixSort that encodes sort keys into a flat byte buffer for memcmp-based comparison, achieving 2-4x speedup on ORDER BY queries.
**Architecture**: New `src/execution/prefix_sort.rs` module. Three-phase algorithm: encode keys into byte buffer, sort index array via memcmp, reorder records. Falls back to direct sort for small result sets.
**Tech Stack**: Rust, criterion (benchmarks), cargo test
**Design Doc**: `docs/plans/prefix-sort-design-final.md`

---

## Changes from previous version

| Issue | Type | Fix |
|-------|------|-----|
| C1 | Critical | Fixed `test_encode_f32_special_values` in Step 2a: NEG_INFINITY assertion changed from `[0x00, 0x00, 0x00, 0x00]` to `[0x00, 0x7F, 0xFF, 0xFF]`. The implementation correctly computes `!0xFF800000 = 0x007FFFFF`, big-endian `[0x00, 0x7F, 0xFF, 0xFF]`. Also updated the doc comment on `encode_f32_to` to reflect the accurate encoding for -Inf. |
| C2 | Critical | Fixed Step 7a's `cargo test` command to use a regex filter: `cargo test --lib "test_elb_order_by\|test_elb_limit_with_order\|test_alb_order_by\|test_integration_distinct"`. The previous command passed multiple positional arguments which is invalid syntax. |
| C3 | Critical | Added explicit note in Step 7b that `src/functions/array.rs` has a separate `compare_values` function with different semantics (handles Int/Float cross-type comparison) that is unrelated and must NOT be touched. |
| C4 | Critical | Added `test_encode_value_object_array_ordering` test in Step 5a verifying that Object and Array sort after all typed non-null values, and that Object sorts before Array. |
| C5 | Critical | Added `sort_string_urls` and `sort_with_nulls` benchmark scenarios to Step 10b. |
| S6 | Suggestion | Added pre-epoch DateTime test (`1960-01-01`) in Step 3a to verify sign-flip encoding for negative timestamps. |

---

## Task Dependencies

| Group | Steps | Can Parallelize | Description |
|-------|-------|-----------------|-------------|
| 1 | Steps 1-4 | Yes (independent) | Encoding functions (no dependencies between types) |
| 2 | Step 5 | No (depends on Group 1) | PrefixSortEncoder struct + layout computation |
| 3 | Step 6 | No (depends on Group 2) | Sort + reorder (Phase 2 & 3) |
| 4 | Step 7 | No (depends on Group 3) | Integration into Node::OrderBy |
| 5 | Step 8 | No (depends on Group 4) | Expose in bench_internals + add module |
| 6 | Steps 9-10 | Yes (independent) | Property tests + benchmarks |
| 7 | Step 11 | No (depends on all) | End-to-end verification |

---

## Step 1: encode_i32 and encode_bool

**File**: `src/execution/prefix_sort.rs` (new)

### 1a. Write failing test

```rust
// At bottom of src/execution/prefix_sort.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_i32_ordering() {
        let mut buf_neg = [0u8; 4];
        let mut buf_zero = [0u8; 4];
        let mut buf_pos = [0u8; 4];
        let mut buf_max = [0u8; 4];
        let mut buf_min = [0u8; 4];

        encode_i32(i32::MIN, &mut buf_min);
        encode_i32(-1, &mut buf_neg);
        encode_i32(0, &mut buf_zero);
        encode_i32(1, &mut buf_pos);
        encode_i32(i32::MAX, &mut buf_max);

        assert!(buf_min < buf_neg);
        assert!(buf_neg < buf_zero);
        assert!(buf_zero < buf_pos);
        assert!(buf_pos < buf_max);
    }

    #[test]
    fn test_encode_i32_specific_values() {
        let mut buf = [0u8; 4];
        encode_i32(i32::MIN, &mut buf);
        assert_eq!(buf, [0x00, 0x00, 0x00, 0x00]);

        encode_i32(0, &mut buf);
        assert_eq!(buf, [0x80, 0x00, 0x00, 0x00]);

        encode_i32(i32::MAX, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_bool() {
        let mut buf_f = [0u8; 1];
        let mut buf_t = [0u8; 1];
        encode_bool(false, &mut buf_f);
        encode_bool(true, &mut buf_t);
        assert!(buf_f < buf_t);
        assert_eq!(buf_f[0], 0x00);
        assert_eq!(buf_t[0], 0x01);
    }
}
```

### 1b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 1c. Write implementation

```rust
// src/execution/prefix_sort.rs — top of file

/// Encode i32 into 4 bytes, big-endian, with sign bit flipped for unsigned sort order.
/// i32::MIN -> 0x00000000, 0 -> 0x80000000, i32::MAX -> 0xFFFFFFFF.
#[inline]
fn encode_i32(value: i32, dest: &mut [u8]) {
    let unsigned = (value as u32) ^ 0x80000000;
    dest[..4].copy_from_slice(&unsigned.to_be_bytes());
}

/// Encode bool into 1 byte: false -> 0x00, true -> 0x01.
#[inline]
fn encode_bool(value: bool, dest: &mut [u8]) {
    dest[0] = value as u8;
}
```

### 1d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 2: encode_f32

**File**: `src/execution/prefix_sort.rs`

### 2a. Write failing test

```rust
// Add to the tests module in prefix_sort.rs

    #[test]
    fn test_encode_f32_ordering() {
        let cases: Vec<f32> = vec![
            f32::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1000.0,
            f32::INFINITY,
            f32::NAN,
        ];
        let mut prev = [0u8; 4];
        encode_f32_to(cases[0], &mut prev);
        for &val in &cases[1..] {
            let mut curr = [0u8; 4];
            encode_f32_to(val, &mut curr);
            assert!(prev <= curr, "failed: prev {:?} should <= curr {:?} for value {}", prev, curr, val);
            prev = curr;
        }
    }

    #[test]
    fn test_encode_f32_special_values() {
        let mut buf = [0u8; 4];

        // NEG_INFINITY: bits = 0xFF800000, negative so !0xFF800000 = 0x007FFFFF,
        // big-endian = [0x00, 0x7F, 0xFF, 0xFF]
        encode_f32_to(f32::NEG_INFINITY, &mut buf);
        assert_eq!(buf, [0x00, 0x7F, 0xFF, 0xFF]);

        encode_f32_to(0.0, &mut buf);
        assert_eq!(buf, [0x80, 0x00, 0x00, 0x00]);

        encode_f32_to(f32::NAN, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_f32_neg_nan() {
        let neg_nan = f32::from_bits(0xFFC00000); // negative NaN
        let mut buf = [0u8; 4];
        encode_f32_to(neg_nan, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]); // same as positive NaN
    }
```

### 2b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 2c. Write implementation

```rust
/// Encode f32 into 4 bytes for sort-preserving unsigned comparison.
/// NaN -> 0xFFFFFFFF, +Inf -> 0xFF800000 (via sign-flip of 0x7F800000),
/// -Inf -> 0x007FFFFF (via bit-flip of 0xFF800000).
/// Positive values: flip sign bit. Negative values: flip all bits.
#[inline]
fn encode_f32_to(value: f32, dest: &mut [u8]) {
    let encoded = if value.is_nan() {
        u32::MAX
    } else {
        let bits = value.to_bits();
        if bits & 0x80000000 != 0 {
            !bits // negative: flip all bits
        } else {
            bits ^ 0x80000000 // non-negative: flip sign bit
        }
    };
    dest[..4].copy_from_slice(&encoded.to_be_bytes());
}
```

### 2d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 3: encode_datetime

**File**: `src/execution/prefix_sort.rs`

### 3a. Write failing test

```rust
    #[test]
    fn test_encode_datetime_ordering() {
        use chrono::{TimeZone, FixedOffset};
        let utc = FixedOffset::east(0);

        // Pre-epoch date (negative timestamp) to verify sign-flip for i64
        let t0 = utc.ymd(1960, 1, 1).and_hms(0, 0, 0);
        let t1 = utc.ymd(2020, 1, 1).and_hms(0, 0, 0);
        let t2 = utc.ymd(2025, 6, 15).and_hms(12, 0, 0);
        let t3 = utc.ymd(2026, 1, 1).and_hms(0, 0, 0);

        let mut buf0 = [0u8; 8];
        let mut buf1 = [0u8; 8];
        let mut buf2 = [0u8; 8];
        let mut buf3 = [0u8; 8];

        encode_datetime(&t0, &mut buf0);
        encode_datetime(&t1, &mut buf1);
        encode_datetime(&t2, &mut buf2);
        encode_datetime(&t3, &mut buf3);

        assert!(buf0 < buf1, "pre-epoch date should sort before post-epoch date");
        assert!(buf1 < buf2);
        assert!(buf2 < buf3);
    }
```

### 3b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 3c. Write implementation

```rust
use chrono;

/// Encode DateTime as i64 epoch seconds, sign-flipped big-endian.
#[inline]
fn encode_datetime(dt: &chrono::DateTime<chrono::FixedOffset>, dest: &mut [u8]) {
    let secs = dt.timestamp();
    let unsigned = (secs as u64) ^ 0x8000000000000000;
    dest[..8].copy_from_slice(&unsigned.to_be_bytes());
}
```

### 3d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 4: encode_string_prefix

**File**: `src/execution/prefix_sort.rs`

### 4a. Write failing test

```rust
    #[test]
    fn test_encode_string_prefix_basic() {
        let mut buf = [0xFFu8; 16];
        encode_string_prefix("hello", &mut buf, 16);
        assert_eq!(&buf[..5], b"hello");
        assert_eq!(&buf[5..], &[0u8; 11]); // zero-padded
    }

    #[test]
    fn test_encode_string_prefix_truncation() {
        let long = "abcdefghijklmnopqrstuvwxyz";
        let mut buf = [0u8; 16];
        encode_string_prefix(long, &mut buf, 16);
        assert_eq!(&buf, b"abcdefghijklmnop"); // first 16 bytes
    }

    #[test]
    fn test_encode_string_prefix_ordering() {
        let mut buf_a = [0u8; 16];
        let mut buf_b = [0u8; 16];
        encode_string_prefix("apple", &mut buf_a, 16);
        encode_string_prefix("banana", &mut buf_b, 16);
        assert!(buf_a < buf_b);
    }

    #[test]
    fn test_encode_string_prefix_empty() {
        let mut buf = [0xFFu8; 16];
        encode_string_prefix("", &mut buf, 16);
        assert_eq!(&buf, &[0u8; 16]);
    }
```

### 4b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 4c. Write implementation

```rust
/// Encode a string into a fixed-width prefix. Copies the first min(len, prefix_len)
/// bytes of the UTF-8 representation and zero-pads the rest.
#[inline]
fn encode_string_prefix(s: &str, dest: &mut [u8], prefix_len: usize) {
    let bytes = s.as_bytes();
    let copy_len = bytes.len().min(prefix_len);
    dest[..copy_len].copy_from_slice(&bytes[..copy_len]);
    if copy_len < prefix_len {
        dest[copy_len..prefix_len].fill(0);
    }
}
```

### 4d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 5: PrefixSortEncoder — layout computation and encode_value

**File**: `src/execution/prefix_sort.rs`

### 5a. Write failing test

```rust
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_encode_value_null_sorts_last() {
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        let mut null_slot = vec![0u8; slot_width];
        let mut int_slot = vec![0u8; slot_width];

        encoder.encode_value(&Value::Null, &mut null_slot, false);
        encoder.encode_value(&Value::Int(42), &mut int_slot, false);

        assert!(int_slot < null_slot, "Int should sort before Null in ASC");
    }

    #[test]
    fn test_encode_value_desc_reverses() {
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        let mut asc_slot = vec![0u8; slot_width];
        let mut desc_slot = vec![0u8; slot_width];

        encoder.encode_value(&Value::Int(42), &mut asc_slot, false);
        encoder.encode_value(&Value::Int(42), &mut desc_slot, true);

        // Every byte should be flipped
        for i in 0..slot_width {
            assert_eq!(asc_slot[i], !desc_slot[i]);
        }
    }

    #[test]
    fn test_encode_value_missing_equals_null() {
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        let mut null_slot = vec![0u8; slot_width];
        let mut missing_slot = vec![0u8; slot_width];

        encoder.encode_value(&Value::Null, &mut null_slot, false);
        encoder.encode_value(&Value::Missing, &mut missing_slot, false);

        assert_eq!(null_slot, missing_slot);
    }

    #[test]
    fn test_encode_value_type_ordering() {
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        let values = vec![
            Value::Boolean(true),
            Value::Int(1),
            Value::Float(OrderedFloat::from(1.0f32)),
            Value::String("a".to_string()),
            Value::Null,
        ];

        let mut prev = vec![0u8; slot_width];
        encoder.encode_value(&values[0], &mut prev, false);
        for val in &values[1..] {
            let mut curr = vec![0u8; slot_width];
            encoder.encode_value(val, &mut curr, false);
            assert!(prev < curr, "type ordering failed for {:?}", val);
            prev = curr;
        }
    }

    #[test]
    fn test_encode_value_object_array_ordering() {
        // Verify Object and Array sort after all typed non-null values,
        // and that Object sorts before Array.
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        // A representative typed non-null value
        let mut string_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::String("zzz".to_string()), &mut string_slot, false);

        // Null/Missing
        let mut null_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Null, &mut null_slot, false);

        // Object
        let mut obj_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Object(Default::default()), &mut obj_slot, false);

        // Array
        let mut arr_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Array(vec![]), &mut arr_slot, false);

        // All typed non-null values sort before Null
        assert!(string_slot < null_slot, "typed values should sort before Null");
        // Null sorts before Object (both have null_byte 0xFF, but Null type_tag 0x00 < Object 0x08)
        assert!(null_slot < obj_slot, "Null should sort before Object");
        // Object sorts before Array (type_tag 0x08 < 0x09)
        assert!(obj_slot < arr_slot, "Object should sort before Array");
        // Transitively: typed values < Null < Object < Array
    }
```

### 5b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 5c. Write implementation

```rust
use crate::common::types::Value;

const TYPE_TAG_NULL: u8 = 0x00;
const TYPE_TAG_BOOL: u8 = 0x01;
const TYPE_TAG_INT: u8 = 0x02;
const TYPE_TAG_FLOAT: u8 = 0x03;
const TYPE_TAG_STRING: u8 = 0x04;
const TYPE_TAG_DATETIME: u8 = 0x05;
const TYPE_TAG_HOST: u8 = 0x06;
const TYPE_TAG_HTTP_REQUEST: u8 = 0x07;
const TYPE_TAG_OBJECT: u8 = 0x08;
const TYPE_TAG_ARRAY: u8 = 0x09;

const NULL_BYTE_NON_NULL: u8 = 0x00;
const NULL_BYTE_NULL: u8 = 0xFF;

/// String-type tags that need fallback tie-breaking.
const STRING_TYPE_TAGS: [u8; 3] = [TYPE_TAG_STRING, TYPE_TAG_HOST, TYPE_TAG_HTTP_REQUEST];

pub struct PrefixSortEncoder {
    pub threshold: usize,
    pub string_prefix_len: usize,
}

impl Default for PrefixSortEncoder {
    fn default() -> Self {
        PrefixSortEncoder {
            threshold: 64,
            string_prefix_len: 16,
        }
    }
}

impl PrefixSortEncoder {
    /// Width of the encoded value portion (max across all types).
    fn max_value_width(&self) -> usize {
        // Bool=1, Int=4, Float=4, DateTime=8, String/Host/HttpRequest=string_prefix_len
        self.string_prefix_len.max(8)
    }

    /// Total width of one key slot: null_byte + type_tag + value.
    pub fn slot_width(&self) -> usize {
        2 + self.max_value_width()
    }

    /// Total entry width for K sort keys: K * slot_width + 4 (row index).
    pub fn entry_width(&self, num_keys: usize) -> usize {
        num_keys * self.slot_width() + 4
    }

    /// Key portion width (everything except the trailing row index).
    pub fn key_width(&self, num_keys: usize) -> usize {
        num_keys * self.slot_width()
    }

    /// Encode a single Value into a key slot. Applies DESC flip if descending.
    pub fn encode_value(&self, value: &Value, slot: &mut [u8], descending: bool) {
        let max_w = self.max_value_width();
        // Zero out the slot first
        slot[..2 + max_w].fill(0);

        match value {
            Value::Null | Value::Missing => {
                slot[0] = NULL_BYTE_NULL;
                slot[1] = TYPE_TAG_NULL;
                // value bytes remain zeroed
            }
            Value::Boolean(b) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_BOOL;
                encode_bool(*b, &mut slot[2..]);
            }
            Value::Int(i) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_INT;
                encode_i32(*i, &mut slot[2..]);
            }
            Value::Float(f) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_FLOAT;
                encode_f32_to(f.into_inner(), &mut slot[2..]);
            }
            Value::String(s) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_STRING;
                encode_string_prefix(s, &mut slot[2..], self.string_prefix_len);
            }
            Value::DateTime(dt) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_DATETIME;
                encode_datetime(dt, &mut slot[2..]);
            }
            Value::Host(h) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_HOST;
                let s = h.to_string();
                encode_string_prefix(&s, &mut slot[2..], self.string_prefix_len);
            }
            Value::HttpRequest(r) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_HTTP_REQUEST;
                let s = r.to_string();
                encode_string_prefix(&s, &mut slot[2..], self.string_prefix_len);
            }
            Value::Object(_) => {
                slot[0] = NULL_BYTE_NULL;
                slot[1] = TYPE_TAG_OBJECT;
            }
            Value::Array(_) => {
                slot[0] = NULL_BYTE_NULL;
                slot[1] = TYPE_TAG_ARRAY;
            }
        }

        if descending {
            for byte in slot[..2 + max_w].iter_mut() {
                *byte = !*byte;
            }
        }
    }
}
```

### 5d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 6: sort function — encode, sort index array, reorder

**File**: `src/execution/prefix_sort.rs`

### 6a. Write failing test

```rust
    use crate::execution::stream::Record;
    use crate::execution::types::Ordering;
    use crate::syntax::ast::{PathExpr, PathSegment};

    fn make_record(field_names: &[String], values: Vec<Value>) -> Record {
        Record::new(&field_names.to_vec(), values)
    }

    fn path(name: &str) -> PathExpr {
        PathExpr::new(vec![PathSegment::AttrName(name.to_string())])
    }

    #[test]
    fn test_prefix_sort_int_asc() {
        let fields = vec!["x".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::Int(30)]),
            make_record(&fields, vec![Value::Int(10)]),
            make_record(&fields, vec![Value::Int(20)]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() }; // force prefix sort
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals, vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_prefix_sort_int_desc() {
        let fields = vec!["x".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::Int(10)]),
            make_record(&fields, vec![Value::Int(30)]),
            make_record(&fields, vec![Value::Int(20)]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Desc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals, vec![Value::Int(30), Value::Int(20), Value::Int(10)]);
    }

    #[test]
    fn test_prefix_sort_with_nulls() {
        let fields = vec!["x".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::Null]),
            make_record(&fields, vec![Value::Int(1)]),
            make_record(&fields, vec![Value::Missing]),
            make_record(&fields, vec![Value::Int(2)]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        // Non-null first, then null/missing last
        assert_eq!(vals[0], Value::Int(1));
        assert_eq!(vals[1], Value::Int(2));
        // Last two are Null and Missing (equal, order between them doesn't matter)
        assert!(matches!(vals[2], Value::Null | Value::Missing));
        assert!(matches!(vals[3], Value::Null | Value::Missing));
    }

    #[test]
    fn test_prefix_sort_string_with_prefix_collision() {
        let fields = vec!["x".to_string()];
        // These share the first 16 bytes "abcdefghijklmnop" but differ after
        let records = vec![
            make_record(&fields, vec![Value::String("abcdefghijklmnopXYZ".to_string())]),
            make_record(&fields, vec![Value::String("abcdefghijklmnopABC".to_string())]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals[0], Value::String("abcdefghijklmnopABC".to_string()));
        assert_eq!(vals[1], Value::String("abcdefghijklmnopXYZ".to_string()));
    }

    #[test]
    fn test_prefix_sort_multi_key() {
        let fields = vec!["a".to_string(), "b".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::Int(1), Value::Int(30)]),
            make_record(&fields, vec![Value::Int(2), Value::Int(10)]),
            make_record(&fields, vec![Value::Int(1), Value::Int(10)]),
        ];
        let keys = vec![path("a"), path("b")];
        let orderings = vec![Ordering::Asc, Ordering::Asc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<(Value, Value)> = result.iter()
            .map(|r| (r.get(&path("a")), r.get(&path("b"))))
            .collect();
        assert_eq!(vals, vec![
            (Value::Int(1), Value::Int(10)),
            (Value::Int(1), Value::Int(30)),
            (Value::Int(2), Value::Int(10)),
        ]);
    }

    #[test]
    fn test_prefix_sort_fallback_below_threshold() {
        let fields = vec!["x".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::Int(3)]),
            make_record(&fields, vec![Value::Int(1)]),
            make_record(&fields, vec![Value::Int(2)]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        // Default threshold=64, so 3 records should use fallback
        let encoder = PrefixSortEncoder::default();
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }
```

### 6b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

### 6c. Write implementation

```rust
use crate::execution::stream::Record;
use crate::execution::types::Ordering;
use crate::syntax::ast::PathExpr;
use std::collections::VecDeque;

/// Compare two Values by reference for sorting. Returns Ordering assuming ascending.
/// Null/Missing sort after all non-null values in ascending order.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use ordered_float::OrderedFloat;
    match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
        (Value::Float(f1), Value::Float(f2)) => f1.cmp(f2),
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
        (Value::DateTime(dt1), Value::DateTime(dt2)) => dt1.cmp(dt2),
        (Value::Host(h1), Value::Host(h2)) => {
            let s1 = h1.to_string();
            let s2 = h2.to_string();
            s1.cmp(&s2)
        }
        (Value::HttpRequest(h1), Value::HttpRequest(h2)) => {
            let s1 = h1.to_string();
            let s2 = h2.to_string();
            s1.cmp(&s2)
        }
        (Value::Null, Value::Null)
        | (Value::Missing, Value::Missing)
        | (Value::Null, Value::Missing)
        | (Value::Missing, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) | (Value::Missing, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) | (_, Value::Missing) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Equal,
    }
}

/// Fallback: direct sort using compare_values (used for small result sets).
fn direct_sort(
    records: &mut Vec<Record>,
    sort_keys: &[PathExpr],
    orderings: &[Ordering],
) {
    records.sort_by(|a, b| {
        for idx in 0..sort_keys.len() {
            let key = &sort_keys[idx];
            let ordering = &orderings[idx];
            let a_owned;
            let b_owned;
            let a_ref = match a.get_ref(key) {
                Some(v) => v,
                None => { a_owned = a.get(key); &a_owned }
            };
            let b_ref = match b.get_ref(key) {
                Some(v) => v,
                None => { b_owned = b.get(key); &b_owned }
            };
            let cmp_result = compare_values(a_ref, b_ref);
            let ordered = match ordering {
                Ordering::Asc => cmp_result,
                Ordering::Desc => cmp_result.reverse(),
            };
            if ordered != std::cmp::Ordering::Equal {
                return ordered;
            }
        }
        std::cmp::Ordering::Equal
    });
}

impl PrefixSortEncoder {
    /// Main entry point: sort records by the given keys and orderings.
    /// Returns a VecDeque of sorted records.
    pub fn sort(
        &self,
        mut records: Vec<Record>,
        sort_keys: &[PathExpr],
        orderings: &[Ordering],
    ) -> VecDeque<Record> {
        if records.len() <= 1 {
            return VecDeque::from(records);
        }

        if records.len() < self.threshold {
            direct_sort(&mut records, sort_keys, orderings);
            return VecDeque::from(records);
        }

        // Phase 1: Encode keys into prefix buffer
        let num_keys = sort_keys.len();
        let slot_w = self.slot_width();
        let entry_w = self.entry_width(num_keys);
        let key_w = self.key_width(num_keys);
        let n = records.len();

        let mut buffer = vec![0u8; n * entry_w];

        for i in 0..n {
            let entry_offset = i * entry_w;
            for k in 0..num_keys {
                let slot_offset = entry_offset + k * slot_w;
                let val = records[i].get(&sort_keys[k]);
                let descending = orderings[k] == Ordering::Desc;
                self.encode_value(&val, &mut buffer[slot_offset..slot_offset + slot_w], descending);
            }
            // Write row index as u32 big-endian
            let idx_offset = entry_offset + key_w;
            buffer[idx_offset..idx_offset + 4].copy_from_slice(&(i as u32).to_be_bytes());
        }

        // Phase 2: Sort index array
        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_unstable_by(|&a, &b| {
            let a_off = a * entry_w;
            let b_off = b * entry_w;
            let a_key = &buffer[a_off..a_off + key_w];
            let b_key = &buffer[b_off..b_off + key_w];

            let ord = a_key.cmp(b_key);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }

            // Tie-breaking: check if any key position has a string-type tag
            let row_a = u32::from_be_bytes(
                buffer[a_off + key_w..a_off + key_w + 4].try_into().unwrap()
            ) as usize;
            let row_b = u32::from_be_bytes(
                buffer[b_off + key_w..b_off + key_w + 4].try_into().unwrap()
            ) as usize;

            for k in 0..num_keys {
                let slot_a = a_off + k * slot_w;
                let mut tag_a = buffer[slot_a + 1]; // type_tag is at offset 1
                if orderings[k] == Ordering::Desc {
                    tag_a = !tag_a; // un-flip for DESC
                }
                let slot_b = b_off + k * slot_w;
                let mut tag_b = buffer[slot_b + 1];
                if orderings[k] == Ordering::Desc {
                    tag_b = !tag_b;
                }

                if STRING_TYPE_TAGS.contains(&tag_a) || STRING_TYPE_TAGS.contains(&tag_b) {
                    let va = records[row_a].get(&sort_keys[k]);
                    let vb = records[row_b].get(&sort_keys[k]);
                    let cmp = compare_values(&va, &vb);
                    let ordered = match orderings[k] {
                        Ordering::Asc => cmp,
                        Ordering::Desc => cmp.reverse(),
                    };
                    if ordered != std::cmp::Ordering::Equal {
                        return ordered;
                    }
                }
            }

            std::cmp::Ordering::Equal
        });

        // Phase 3: Reorder records
        let mut opt_records: Vec<Option<Record>> = records.into_iter().map(Some).collect();
        let mut result = VecDeque::with_capacity(n);
        for &idx in &indices {
            let row_idx = u32::from_be_bytes(
                buffer[idx * entry_w + key_w..idx * entry_w + key_w + 4].try_into().unwrap()
            ) as usize;
            result.push_back(opt_records[row_idx].take().unwrap());
        }

        result
    }
}
```

### 6d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 7: Integration into Node::OrderBy

**File**: `src/execution/types.rs`

### 7a. Write test (existing tests serve as integration tests)

No new test needed -- the existing ORDER BY tests in `src/app.rs` (test_elb_order_by_timestamp, test_elb_limit_with_order, test_alb_order_by_received_bytes, test_integration_distinct_and_order_by) will validate the integration. Run them first to confirm they pass before the change:

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib "test_elb_order_by|test_elb_limit_with_order|test_alb_order_by|test_integration_distinct"
```

### 7b. Modify Node::OrderBy

Replace lines 831-872 in `src/execution/types.rs`:

```rust
// Old:
            Node::OrderBy(column_names, orderings, source) => {
                let mut record_stream = source.get(variables.clone(), registry)?;
                let mut records = Vec::new();

                while let Some(record) = record_stream.next()? {
                    records.push(record);
                }

                records.sort_by(|a, b| {
                    // ... old comparison logic ...
                });

                let stream = InMemoryStream::new(VecDeque::from(records));
                Ok(Box::new(stream))
            }

// New:
            Node::OrderBy(column_names, orderings, source) => {
                let mut record_stream = source.get(variables.clone(), registry)?;
                let mut records = Vec::new();

                while let Some(record) = record_stream.next()? {
                    records.push(record);
                }

                let encoder = super::prefix_sort::PrefixSortEncoder::default();
                let sorted = encoder.sort(records, column_names, orderings);

                let stream = InMemoryStream::new(sorted);
                Ok(Box::new(stream))
            }
```

Also remove the now-unused `compare_values` function from `types.rs` (lines 541-566) since it's been duplicated into `prefix_sort.rs`.

**IMPORTANT**: `src/functions/array.rs` contains a separate `compare_values` function (line 27) with different semantics -- it handles Int/Float cross-type comparison and falls back to string comparison for mixed types. This is a completely independent function that happens to share the same name. It is NOT affected by this change and must NOT be modified or removed.

### 7c. Run all existing tests to verify nothing breaks

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

---

## Step 8: Module registration and bench_internals export

**Files**: `src/execution/mod.rs`, `src/lib.rs`

### 8a. Add module to execution/mod.rs

Add this line to `src/execution/mod.rs`:

```rust
pub mod prefix_sort;
```

### 8b. Export PrefixSortEncoder in bench_internals

Add to the `bench_internals` module in `src/lib.rs`:

```rust
    // PrefixSort
    pub use crate::execution::prefix_sort::PrefixSortEncoder;
```

### 8c. Verify everything compiles and tests pass

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

---

## Step 9: Property-based equivalence test

**File**: `src/execution/prefix_sort.rs` (add to tests module)

### 9a. Write the property test

```rust
    #[test]
    fn test_prefix_sort_matches_direct_sort_int() {
        // Generate random records and verify prefix sort produces the same
        // ordering as the direct sort_by approach.
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(12345);

        let fields = vec!["x".to_string(), "y".to_string()];
        let mut records: Vec<Record> = (0..500).map(|_| {
            make_record(&fields, vec![
                Value::Int(rng.gen_range(-1000..1000)),
                Value::String(format!("str_{}", rng.gen_range(0..50))),
            ])
        }).collect();

        let keys = vec![path("x"), path("y")];
        let orderings = vec![Ordering::Asc, Ordering::Desc];

        // Direct sort (control)
        let mut direct = records.clone();
        direct_sort(&mut direct, &keys, &orderings);
        let direct_vals: Vec<(Value, Value)> = direct.iter()
            .map(|r| (r.get(&path("x")), r.get(&path("y"))))
            .collect();

        // Prefix sort (treatment)
        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let prefix_result = encoder.sort(records, &keys, &orderings);
        let prefix_vals: Vec<(Value, Value)> = prefix_result.iter()
            .map(|r| (r.get(&path("x")), r.get(&path("y"))))
            .collect();

        assert_eq!(direct_vals, prefix_vals);
    }

    #[test]
    fn test_prefix_sort_matches_direct_sort_with_nulls() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(99999);

        let fields = vec!["x".to_string()];
        let mut records: Vec<Record> = (0..200).map(|_| {
            let val = if rng.gen_bool(0.2) {
                Value::Null
            } else {
                Value::Int(rng.gen_range(0..100))
            };
            make_record(&fields, vec![val])
        }).collect();

        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let mut direct = records.clone();
        direct_sort(&mut direct, &keys, &orderings);
        let direct_vals: Vec<Value> = direct.iter().map(|r| r.get(&path("x"))).collect();

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let prefix_result = encoder.sort(records, &keys, &orderings);
        let prefix_vals: Vec<Value> = prefix_result.iter().map(|r| r.get(&path("x"))).collect();

        assert_eq!(direct_vals, prefix_vals);
    }
```

### 9b. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib prefix_sort
```

---

## Step 10: Benchmarks

**File**: `benches/bench_sort.rs` (new)

### 10a. Add bench entry to Cargo.toml

Append to `Cargo.toml`:

```toml
[[bench]]
name = "bench_sort"
harness = false
required-features = ["bench-internals"]
```

### 10b. Write benchmark file

```rust
// benches/bench_sort.rs
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use std::collections::VecDeque;

use logq::bench_internals::*;
use ordered_float::OrderedFloat;
use rand::prelude::*;

mod helpers;

/// Generate records with a single Int column for sort benchmarking.
fn generate_int_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        Record::new(&field_names, vec![Value::Int(rng.gen_range(0..1_000_000))])
    }).collect()
}

/// Generate records with a single String column.
fn generate_string_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        let s: String = (0..32).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
        Record::new(&field_names, vec![Value::String(s)])
    }).collect()
}

/// Generate records with URL strings that share a long common prefix.
/// This is the worst case for prefix collisions and exercises the fallback path.
fn generate_url_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        // Shared prefix "https://example.com/path/" is 25 bytes, exceeding
        // the default 16-byte prefix length -- forces fallback tie-breaking.
        let suffix: String = (0..16).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
        let url = format!("https://example.com/path/{}", suffix);
        Record::new(&field_names, vec![Value::String(url)])
    }).collect()
}

/// Generate records with a single Int column where 20% of values are Null.
fn generate_int_with_nulls_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        let val = if rng.gen_bool(0.2) {
            Value::Null
        } else {
            Value::Int(rng.gen_range(0..1_000_000))
        };
        Record::new(&field_names, vec![val])
    }).collect()
}

/// Generate records with multiple sort keys.
fn generate_multi_key_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    (0..count).map(|_| {
        Record::new(&field_names, vec![
            Value::Int(rng.gen_range(0..100)),
            Value::String(format!("str_{}", rng.gen_range(0..1000))),
            Value::Int(rng.gen_range(0..10000)),
        ])
    }).collect()
}

fn path(name: &str) -> PathExpr {
    PathExpr::new(vec![PathSegment::AttrName(name.to_string())])
}

fn bench_sort_int(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (100, "100"),
        (1_000, "1K"),
        (10_000, "10K"),
        (100_000, "100K"),
    ];

    let mut group = c.benchmark_group("sort_int");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];

    for &(size, label) in sizes {
        if size >= 100_000 {
            group.measurement_time(Duration::from_secs(10));
            group.sample_size(20);
        }

        group.bench_with_input(BenchmarkId::new("prefix", label), &size, |b, &sz| {
            b.iter_batched(
                || generate_int_records(sz),
                |records| {
                    let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                    black_box(encoder.sort(records, &keys, &orderings))
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("direct", label), &size, |b, &sz| {
            b.iter_batched(
                || generate_int_records(sz),
                |records| {
                    let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                    black_box(encoder.sort(records, &keys, &orderings))
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_sort_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_string");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_string_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_string_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_string_urls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_string_urls");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_url_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_url_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_int_with_nulls_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_int_with_nulls_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_multi_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_multi_key");
    let keys = vec![path("a"), path("b"), path("c")];
    let orderings = vec![Ordering::Asc, Ordering::Desc, Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_multi_key_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_multi_key_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_sort_int, bench_sort_string, bench_sort_string_urls, bench_sort_with_nulls, bench_sort_multi_key);
criterion_main!(benches);
```

### 10c. Verify benchmark compiles

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --bench bench_sort --features bench-internals -- --test
```

---

## Step 11: End-to-end verification

### 11a. Run full test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 11b. Run ORDER BY specific integration tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib "test_elb_order_by|test_elb_limit_with_order|test_alb_order_by|test_integration_distinct"
```

### 11c. Run existing E3 benchmark (ORDER BY regression gate)

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --bench bench_execution --features bench-internals -- E3_filter_orderby
```

### 11d. Run new sort benchmarks

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --bench bench_sort --features bench-internals
```

### 11e. Verify correct behavior with a manual query

```bash
cd /Users/paulmeng/Develop/logq && echo 'Test data not required — rely on cargo test results above'
```

All tests passing + benchmark results showing improvement = done.
