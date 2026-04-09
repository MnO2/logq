use chrono;
use std::collections::VecDeque;
use std::convert::TryInto;

use crate::common::types::Value;
use crate::execution::stream::Record;
use crate::execution::types::Ordering;
use crate::syntax::ast::PathExpr;

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

/// Encode DateTime as i64 epoch seconds, sign-flipped big-endian.
#[inline]
fn encode_datetime(dt: &chrono::DateTime<chrono::FixedOffset>, dest: &mut [u8]) {
    let secs = dt.timestamp();
    let unsigned = (secs as u64) ^ 0x8000000000000000;
    dest[..8].copy_from_slice(&unsigned.to_be_bytes());
}

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

/// Compare two Values by reference for sorting. Returns Ordering assuming ascending.
/// Null/Missing sort after all non-null values in ascending order.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
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
        slot[..2 + max_w].fill(0);

        match value {
            Value::Null | Value::Missing => {
                slot[0] = NULL_BYTE_NULL;
                slot[1] = TYPE_TAG_NULL;
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
                let val_owned;
                let val = match records[i].get_ref(&sort_keys[k]) {
                    Some(v) => v,
                    None => { val_owned = records[i].get(&sort_keys[k]); &val_owned }
                };
                let descending = orderings[k] == Ordering::Desc;
                self.encode_value(val, &mut buffer[slot_offset..slot_offset + slot_w], descending);
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
                let mut tag_a = buffer[slot_a + 1];
                if orderings[k] == Ordering::Desc {
                    tag_a = !tag_a;
                }
                let slot_b = b_off + k * slot_w;
                let mut tag_b = buffer[slot_b + 1];
                if orderings[k] == Ordering::Desc {
                    tag_b = !tag_b;
                }

                if STRING_TYPE_TAGS.contains(&tag_a) || STRING_TYPE_TAGS.contains(&tag_b) {
                    let va_owned;
                    let va = match records[row_a].get_ref(&sort_keys[k]) {
                        Some(v) => v,
                        None => { va_owned = records[row_a].get(&sort_keys[k]); &va_owned }
                    };
                    let vb_owned;
                    let vb = match records[row_b].get_ref(&sort_keys[k]) {
                        Some(v) => v,
                        None => { vb_owned = records[row_b].get(&sort_keys[k]); &vb_owned }
                    };
                    let cmp = compare_values(va, vb);
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

#[cfg(test)]
mod tests {
    use super::*;

    // ---- encode_i32 tests ----

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

    // ---- encode_bool tests ----

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

    // ---- encode_f32_to tests ----

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
            assert!(
                prev <= curr,
                "failed: prev {:?} should <= curr {:?} for value {}",
                prev,
                curr,
                val
            );
            prev = curr;
        }
    }

    #[test]
    fn test_encode_f32_special_values() {
        let mut buf = [0u8; 4];

        encode_f32_to(f32::NEG_INFINITY, &mut buf);
        assert_eq!(buf, [0x00, 0x7F, 0xFF, 0xFF]);

        encode_f32_to(0.0, &mut buf);
        assert_eq!(buf, [0x80, 0x00, 0x00, 0x00]);

        encode_f32_to(f32::NAN, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_f32_neg_nan() {
        let neg_nan = f32::from_bits(0xFFC00000);
        let mut buf = [0u8; 4];
        encode_f32_to(neg_nan, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    // ---- encode_datetime tests ----

    #[test]
    fn test_encode_datetime_ordering() {
        use chrono::{FixedOffset, TimeZone};

        let utc = FixedOffset::east(0);

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

        assert!(
            buf0 < buf1,
            "pre-epoch date should sort before post-epoch date"
        );
        assert!(buf1 < buf2);
        assert!(buf2 < buf3);
    }

    // ---- encode_string_prefix tests ----

    #[test]
    fn test_encode_string_prefix_basic() {
        let mut buf = [0xFFu8; 16];
        encode_string_prefix("hello", &mut buf, 16);
        assert_eq!(&buf[..5], b"hello");
        assert_eq!(&buf[5..], &[0u8; 11]);
    }

    #[test]
    fn test_encode_string_prefix_truncation() {
        let long = "abcdefghijklmnopqrstuvwxyz";
        let mut buf = [0u8; 16];
        encode_string_prefix(long, &mut buf, 16);
        assert_eq!(&buf, b"abcdefghijklmnop");
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

    // ---- PrefixSortEncoder / encode_value tests ----

    use crate::common::types::Value;
    use crate::execution::stream::Record;
    use crate::execution::types::Ordering;
    use crate::syntax::ast::{PathExpr, PathSegment};
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
            Value::String("a".to_string().into()),
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
        let encoder = PrefixSortEncoder::default();
        let slot_width = encoder.slot_width();

        let mut string_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::String("zzz".to_string().into()), &mut string_slot, false);

        let mut null_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Null, &mut null_slot, false);

        let mut obj_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Object(Default::default()), &mut obj_slot, false);

        let mut arr_slot = vec![0u8; slot_width];
        encoder.encode_value(&Value::Array(vec![]), &mut arr_slot, false);

        assert!(string_slot < null_slot, "typed values should sort before Null");
        assert!(null_slot < obj_slot, "Null should sort before Object");
        assert!(obj_slot < arr_slot, "Object should sort before Array");
    }

    // ---- sort function tests ----

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

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
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
        assert_eq!(vals[0], Value::Int(1));
        assert_eq!(vals[1], Value::Int(2));
        assert!(matches!(vals[2], Value::Null | Value::Missing));
        assert!(matches!(vals[3], Value::Null | Value::Missing));
    }

    #[test]
    fn test_prefix_sort_string_with_prefix_collision() {
        let fields = vec!["x".to_string()];
        let records = vec![
            make_record(&fields, vec![Value::String("abcdefghijklmnopXYZ".to_string().into())]),
            make_record(&fields, vec![Value::String("abcdefghijklmnopABC".to_string().into())]),
        ];
        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals[0], Value::String("abcdefghijklmnopABC".to_string().into()));
        assert_eq!(vals[1], Value::String("abcdefghijklmnopXYZ".to_string().into()));
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

        let encoder = PrefixSortEncoder::default();
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<Value> = result.iter().map(|r| r.get(&path("x"))).collect();
        assert_eq!(vals, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[test]
    fn test_compare_values_basic() {
        assert_eq!(compare_values(&Value::Int(1), &Value::Int(2)), std::cmp::Ordering::Less);
        assert_eq!(compare_values(&Value::Int(2), &Value::Int(1)), std::cmp::Ordering::Greater);
        assert_eq!(compare_values(&Value::Int(1), &Value::Int(1)), std::cmp::Ordering::Equal);
        assert_eq!(compare_values(&Value::Null, &Value::Int(1)), std::cmp::Ordering::Greater);
        assert_eq!(compare_values(&Value::Int(1), &Value::Null), std::cmp::Ordering::Less);
        assert_eq!(compare_values(&Value::Null, &Value::Missing), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_prefix_sort_matches_direct_sort_int() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(12345);

        let fields = vec!["x".to_string(), "y".to_string()];
        let records: Vec<Record> = (0..500).map(|_| {
            make_record(&fields, vec![
                Value::Int(rng.gen_range(-1000..1000)),
                Value::String(format!("str_{}", rng.gen_range(0..50)).into()),
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
        let records: Vec<Record> = (0..200).map(|_| {
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
}
