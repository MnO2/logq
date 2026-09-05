use chrono;
use std::collections::VecDeque;
use std::convert::TryInto;

use crate::common::types::Value;
use crate::execution::stream::Record;
use crate::execution::types::Ordering;
use crate::syntax::ast::PathExpr;

/// Encode bool into 1 byte: false -> 0x00, true -> 0x01.
#[inline]
fn encode_bool(value: bool, dest: &mut [u8]) {
    dest[0] = value as u8;
}

/// Both public numeric types are exact in f64. One encoding permits numeric
/// comparison across types without rounding large integers through f32.
fn encode_number(value: f64, dest: &mut [u8]) {
    let encoded = if value.is_nan() {
        u64::MAX
    } else {
        // OrderedFloat considers signed zero equal, so preserve input ties.
        let bits = if value == 0.0 {
            0.0f64.to_bits()
        } else {
            value.to_bits()
        };
        if bits & (1 << 63) != 0 { !bits } else { bits ^ (1 << 63) }
    };
    dest[..8].copy_from_slice(&encoded.to_be_bytes());
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
const TYPE_TAG_STRING: u8 = 0x04;
const TYPE_TAG_DATETIME: u8 = 0x05;
const TYPE_TAG_HOST: u8 = 0x06;
const TYPE_TAG_HTTP_REQUEST: u8 = 0x07;
const TYPE_TAG_OBJECT: u8 = 0x08;
const TYPE_TAG_ARRAY: u8 = 0x09;

const NULL_BYTE_NON_NULL: u8 = 0x00;
const NULL_BYTE_NULL: u8 = 0xFF;

fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Boolean(_) => TYPE_TAG_BOOL,
        Value::Int(_) | Value::Float(_) => TYPE_TAG_INT,
        Value::String(_) => TYPE_TAG_STRING,
        Value::DateTime(_) => TYPE_TAG_DATETIME,
        Value::Host(_) => TYPE_TAG_HOST,
        Value::HttpRequest(_) => TYPE_TAG_HTTP_REQUEST,
        Value::Object(_) => TYPE_TAG_OBJECT,
        Value::Array(_) => TYPE_TAG_ARRAY,
        Value::Null | Value::Missing => u8::MAX,
    }
}

/// Compare two Values by reference for sorting. Returns Ordering assuming ascending.
/// Null/Missing sort after all non-null values in ascending order.
pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
        (Value::Float(f1), Value::Float(f2)) => f1.cmp(f2),
        (Value::Int(i), Value::Float(f)) => {
            ordered_float::OrderedFloat(*i as f64).cmp(&ordered_float::OrderedFloat(f.into_inner() as f64))
        }
        (Value::Float(f), Value::Int(i)) => {
            ordered_float::OrderedFloat(f.into_inner() as f64).cmp(&ordered_float::OrderedFloat(*i as f64))
        }
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
        // Same-type complex values remain ties. Different type families must
        // have a consistent order to keep sorting and heap comparison transitive.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

fn compare_records(a: &Record, b: &Record, sort_keys: &[PathExpr], orderings: &[Ordering]) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let a_owned;
        let b_owned;
        let a_ref = match a.get_ref(key) {
            Some(value) => value,
            None => {
                a_owned = a.get(key);
                &a_owned
            }
        };
        let b_ref = match b.get_ref(key) {
            Some(value) => value,
            None => {
                b_owned = b.get(key);
                &b_owned
            }
        };
        let ordering = orderings.get(idx).copied().unwrap_or(Ordering::Asc);
        let result = match ordering {
            Ordering::Asc => compare_values(a_ref, b_ref),
            Ordering::Desc => compare_values(a_ref, b_ref).reverse(),
        };
        if result != std::cmp::Ordering::Equal {
            return result;
        }
    }
    std::cmp::Ordering::Equal
}

/// Fallback: direct sort using compare_values (used for small result sets).
fn direct_sort(records: &mut [Record], sort_keys: &[PathExpr], orderings: &[Ordering]) {
    records.sort_by(|a, b| compare_records(a, b, sort_keys, orderings));
}

struct RankedRecord {
    record: Record,
    ordinal: usize,
    estimated_bytes: usize,
}

fn compare_ranked(
    a: &RankedRecord,
    b: &RankedRecord,
    sort_keys: &[PathExpr],
    orderings: &[Ordering],
) -> std::cmp::Ordering {
    compare_records(&a.record, &b.record, sort_keys, orderings).then_with(|| a.ordinal.cmp(&b.ordinal))
}

/// Retains only the best `capacity` records in a max-heap while scanning.
pub(crate) struct BoundedTopN {
    heap: Vec<RankedRecord>,
    capacity: usize,
    sort_keys: Vec<PathExpr>,
    orderings: Vec<Ordering>,
    next_ordinal: usize,
    peak_retained: usize,
    estimated_bytes: usize,
    memory: crate::execution::memory::MemoryTracker,
}

impl BoundedTopN {
    pub(crate) fn new(capacity: usize, sort_keys: Vec<PathExpr>, orderings: Vec<Ordering>) -> Self {
        Self::new_with_memory_limit(capacity, sort_keys, orderings, None)
    }

    pub(crate) fn new_with_memory_limit(
        capacity: usize,
        sort_keys: Vec<PathExpr>,
        orderings: Vec<Ordering>,
        max_memory: Option<usize>,
    ) -> Self {
        Self::new_with_memory_tracker(
            capacity,
            sort_keys,
            orderings,
            crate::execution::memory::MemoryTracker::new(max_memory),
        )
    }

    pub(crate) fn new_with_memory_tracker(
        capacity: usize,
        sort_keys: Vec<PathExpr>,
        orderings: Vec<Ordering>,
        memory: crate::execution::memory::MemoryTracker,
    ) -> Self {
        Self {
            // LIMIT is user-controlled. Allocate only as rows are retained.
            heap: Vec::new(),
            capacity,
            sort_keys,
            orderings,
            next_ordinal: 0,
            peak_retained: 0,
            estimated_bytes: 0,
            memory,
        }
    }

    #[cfg(test)]
    pub(crate) fn push(&mut self, record: Record) {
        self.try_push(record)
            .expect("unlimited top-N memory tracker cannot fail");
    }

    pub(crate) fn try_push(&mut self, record: Record) -> crate::execution::types::StreamResult<()> {
        let estimated_bytes = crate::execution::memory::estimate_record(&record);
        let ranked = RankedRecord {
            record,
            ordinal: self.next_ordinal,
            estimated_bytes,
        };
        self.next_ordinal += 1;
        if self.capacity == 0 {
            return Ok(());
        }

        if self.heap.len() < self.capacity {
            self.memory.add(estimated_bytes)?;
            self.estimated_bytes = self.estimated_bytes.saturating_add(estimated_bytes);
            self.heap.push(ranked);
            self.sift_up(self.heap.len() - 1);
            self.peak_retained = self.peak_retained.max(self.heap.len());
        } else if compare_ranked(&ranked, &self.heap[0], &self.sort_keys, &self.orderings).is_lt() {
            self.memory.replace(self.heap[0].estimated_bytes, estimated_bytes)?;
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(self.heap[0].estimated_bytes)
                .saturating_add(estimated_bytes);
            self.heap[0] = ranked;
            self.sift_down(0);
        }
        Ok(())
    }

    /// The supplied keys must describe the record returned by `materialize`, in
    /// sort-key order. Rejected rows do not construct their payload; equal keys
    /// retain earlier input rows exactly as the eager heap does.
    pub(crate) fn try_push_lazy(
        &mut self,
        keys: &[Value],
        materialize: impl FnOnce() -> Record,
    ) -> crate::execution::types::StreamResult<bool> {
        debug_assert_eq!(keys.len(), self.sort_keys.len());
        let accepted = self.capacity > 0
            && (self.heap.len() < self.capacity || {
                let worst = &self.heap[0].record;
                let mut ordering = std::cmp::Ordering::Equal;
                for (index, (key, path)) in keys.iter().zip(&self.sort_keys).enumerate() {
                    let owned;
                    let other = if let Some(value) = worst.get_ref(path) {
                        value
                    } else {
                        owned = worst.get(path);
                        &owned
                    };
                    ordering = compare_values(key, other);
                    if self.orderings.get(index) == Some(&Ordering::Desc) {
                        ordering = ordering.reverse();
                    }
                    if !ordering.is_eq() {
                        break;
                    }
                }
                ordering.is_lt()
            });
        if accepted {
            self.try_push(materialize())?;
        } else {
            self.next_ordinal += 1;
        }
        Ok(accepted)
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
            .saturating_add(self.heap.capacity() * std::mem::size_of::<RankedRecord>())
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if !compare_ranked(&self.heap[index], &self.heap[parent], &self.sort_keys, &self.orderings).is_gt() {
                break;
            }
            self.heap.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let mut larger = left;
            if right < self.heap.len()
                && compare_ranked(&self.heap[right], &self.heap[left], &self.sort_keys, &self.orderings).is_gt()
            {
                larger = right;
            }
            if !compare_ranked(&self.heap[larger], &self.heap[index], &self.sort_keys, &self.orderings).is_gt() {
                break;
            }
            self.heap.swap(index, larger);
            index = larger;
        }
    }

    #[cfg(test)]
    pub(crate) fn peak_retained(&self) -> usize {
        self.peak_retained
    }

    pub(crate) fn finish(mut self) -> VecDeque<Record> {
        self.heap
            .sort_by(|a, b| compare_ranked(a, b, &self.sort_keys, &self.orderings));
        self.heap.into_iter().map(|ranked| ranked.record).collect()
    }
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
                encode_number(*i as f64, &mut slot[2..]);
            }
            Value::Float(f) => {
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_INT;
                encode_number(f.into_inner() as f64, &mut slot[2..]);
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
                slot[0] = NULL_BYTE_NON_NULL;
                slot[1] = TYPE_TAG_OBJECT;
            }
            Value::Array(_) => {
                slot[0] = NULL_BYTE_NON_NULL;
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
    pub fn sort(&self, mut records: Vec<Record>, sort_keys: &[PathExpr], orderings: &[Ordering]) -> VecDeque<Record> {
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

        for (i, record) in records.iter().enumerate().take(n) {
            let entry_offset = i * entry_w;
            for (k, key) in sort_keys.iter().enumerate() {
                let slot_offset = entry_offset + k * slot_w;
                let val_owned;
                let val = match record.get_ref(key) {
                    Some(v) => v,
                    None => {
                        val_owned = record.get(key);
                        &val_owned
                    }
                };
                let descending = orderings.get(k) == Some(&Ordering::Desc);
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
            // Resolve each prefix collision before consulting the next key:
            // a long string or subsecond timestamp can still decide ordering.
            for (k, key) in sort_keys.iter().enumerate() {
                let a_slot = &buffer[a_off + k * slot_w..a_off + (k + 1) * slot_w];
                let b_slot = &buffer[b_off + k * slot_w..b_off + (k + 1) * slot_w];
                let encoded_order = a_slot.cmp(b_slot);
                if !encoded_order.is_eq() {
                    return encoded_order;
                }
                let mut tag = a_slot[1];
                if orderings.get(k) == Some(&Ordering::Desc) {
                    tag = !tag;
                }
                if !matches!(
                    tag,
                    TYPE_TAG_STRING | TYPE_TAG_DATETIME | TYPE_TAG_HOST | TYPE_TAG_HTTP_REQUEST
                ) {
                    // Numeric, boolean and nullish encodings are complete.
                    continue;
                }
                let direction = orderings.get(k).copied().unwrap_or(Ordering::Asc);
                let full_order = compare_records(&records[a], &records[b], std::slice::from_ref(key), &[direction]);
                if !full_order.is_eq() {
                    return full_order;
                }
            }
            // The row index makes equal keys stable even with unstable sorting.
            a.cmp(&b)
        });

        // Phase 3: Reorder records
        let mut opt_records: Vec<Option<Record>> = records.into_iter().map(Some).collect();
        let mut result = VecDeque::with_capacity(n);
        for &idx in &indices {
            let row_idx = u32::from_be_bytes(
                buffer[idx * entry_w + key_w..idx * entry_w + key_w + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            result.push_back(opt_records[row_idx].take().unwrap());
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    // ---- encode_datetime tests ----

    #[test]
    fn test_encode_datetime_ordering() {
        use chrono::{FixedOffset, TimeZone};

        let utc = FixedOffset::east_opt(0).unwrap();

        let t0 = utc.with_ymd_and_hms(1960, 1, 1, 0, 0, 0).unwrap();
        let t1 = utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let t2 = utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap();
        let t3 = utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

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
    fn sort_strategies_agree_on_mixed_numbers_nulls_and_stable_ties() {
        let values = vec![
            Value::Null,
            Value::Int(3),
            Value::Float(OrderedFloat(1.5)),
            Value::Missing,
            Value::Float(OrderedFloat(-0.0)),
            Value::Int(0),
            Value::Float(OrderedFloat(0.0)),
            Value::Int(16_777_217),
            Value::Float(OrderedFloat(16_777_216.0)),
            Value::Int(16_777_216),
        ];
        let records: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(i, value)| Record::new(&["x".into(), "id".into()], vec![value, Value::Int(i as i32)]))
            .collect();
        let keys = vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])];
        for (direction, expected) in [
            (Ordering::Asc, vec![4, 5, 6, 2, 1, 8, 9, 7, 0, 3]),
            (Ordering::Desc, vec![0, 3, 7, 8, 9, 1, 2, 4, 5, 6]),
        ] {
            for threshold in [0, usize::MAX] {
                let output = PrefixSortEncoder {
                    threshold,
                    ..Default::default()
                }
                .sort(records.clone(), &keys, &[direction]);
                let ids: Vec<_> = output
                    .iter()
                    .map(|row| row.get_field_value("id").unwrap().clone())
                    .collect();
                assert_eq!(
                    ids,
                    expected.iter().copied().map(Value::Int).collect::<Vec<_>>(),
                    "threshold {threshold}, direction {direction:?}"
                );
            }
            let mut top = BoundedTopN::new(8, keys.clone(), vec![direction]);
            for record in records.clone() {
                top.push(record);
            }
            let ids: Vec<_> = top
                .finish()
                .iter()
                .map(|row| row.get_field_value("id").unwrap().clone())
                .collect();
            assert_eq!(ids, expected[..8].iter().copied().map(Value::Int).collect::<Vec<_>>());
        }
    }

    #[test]
    fn prefix_collisions_resolve_first_key_before_secondary_keys() {
        use chrono::{FixedOffset, TimeZone};
        let utc = FixedOffset::east_opt(0).unwrap();
        let cases = [
            (
                Value::String("same-prefix-b".into()),
                Value::String("same-prefix-a".into()),
            ),
            (
                Value::DateTime(utc.timestamp_opt(1, 2).unwrap()),
                Value::DateTime(utc.timestamp_opt(1, 1).unwrap()),
            ),
        ];
        let keys = vec![
            PathExpr::new(vec![PathSegment::AttrName("x".into())]),
            PathExpr::new(vec![PathSegment::AttrName("id".into())]),
        ];
        for (later, earlier) in cases {
            let records = vec![
                Record::new(&["x".into(), "id".into()], vec![later, Value::Int(0)]),
                Record::new(&["x".into(), "id".into()], vec![earlier, Value::Int(1)]),
            ];
            let output = PrefixSortEncoder {
                threshold: 0,
                string_prefix_len: 4,
            }
            .sort(records, &keys, &[Ordering::Asc, Ordering::Asc]);
            assert_eq!(output[0].get_field_value("id"), Some(&Value::Int(1)));
        }
    }

    #[test]
    fn encoded_sort_keeps_input_order_for_equal_keys() {
        let records: Vec<_> = (0..256)
            .map(|i| Record::new(&["x".into(), "id".into()], vec![Value::Int(i % 3), Value::Int(i)]))
            .collect();
        let keys = vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])];
        let output = PrefixSortEncoder::default().sort(records, &keys, &[Ordering::Asc]);
        for group in 0..3 {
            let ids: Vec<_> = output
                .iter()
                .filter(|row| row.get_field_value("x") == Some(&Value::Int(group)))
                .map(|row| row.get_field_value("id").unwrap().clone())
                .collect();
            assert_eq!(
                ids,
                (0..256).filter(|i| i % 3 == group).map(Value::Int).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn bounded_top_n_never_retains_more_than_its_limit() {
        let key = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut top_n = BoundedTopN::new(3, vec![key], vec![Ordering::Asc]);
        for value in (1..=1_000).rev() {
            top_n.push(Record::new(&["x".to_string()], vec![Value::Int(value)]));
        }

        assert_eq!(top_n.peak_retained(), 3);
        let values: Vec<_> = top_n
            .finish()
            .into_iter()
            .map(|record| record.get_field_value("x").unwrap().clone())
            .collect();
        assert_eq!(values, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[test]
    fn lazy_top_n_materializes_only_admitted_payloads_and_keeps_stable_ties() {
        let key = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut top_n = BoundedTopN::new(2, vec![key], vec![Ordering::Asc]);
        let mut materialized = 0;
        for (value, label) in [(1, "first"), (1, "second"), (2, "worse"), (0, "best"), (1, "late tie")] {
            top_n
                .try_push_lazy(&[Value::Int(value)], || {
                    materialized += 1;
                    Record::new(
                        &["x".into(), "label".into()],
                        vec![Value::Int(value), Value::String(label.into())],
                    )
                })
                .unwrap();
        }
        assert_eq!(materialized, 3);
        let records = top_n.finish();
        assert_eq!(records[0].get_field_value("label"), Some(&Value::String("best".into())));
        assert_eq!(
            records[1].get_field_value("label"),
            Some(&Value::String("first".into()))
        );
    }

    #[test]
    fn zero_top_n_does_not_materialize_and_large_limits_do_not_preallocate() {
        let mut zero = BoundedTopN::new(0, vec![], vec![]);
        assert!(
            !zero
                .try_push_lazy(&[], || panic!("zero LIMIT evaluated payload"))
                .unwrap()
        );
        let large = BoundedTopN::new(usize::MAX, vec![], vec![]);
        assert!(large.finish().is_empty());
    }

    #[test]
    fn bounded_top_n_matches_full_sort_for_multiple_keys_and_directions() {
        let fields = ["x".to_string(), "y".to_string()];
        let records: Vec<_> = (0..100)
            .map(|value| Record::new(&fields, vec![Value::Int(value % 7), Value::Int(100 - value)]))
            .collect();
        let keys = vec![
            PathExpr::new(vec![PathSegment::AttrName("x".to_string())]),
            PathExpr::new(vec![PathSegment::AttrName("y".to_string())]),
        ];

        for orderings in [vec![Ordering::Asc, Ordering::Desc], vec![Ordering::Desc, Ordering::Asc]] {
            let expected: Vec<_> = PrefixSortEncoder::default()
                .sort(records.clone(), &keys, &orderings)
                .into_iter()
                .take(11)
                .collect();
            let mut top_n = BoundedTopN::new(11, keys.clone(), orderings);
            for record in records.clone() {
                top_n.push(record);
            }
            assert_eq!(top_n.finish().into_iter().collect::<Vec<_>>(), expected);
        }
    }

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

        let values = [
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
            assert!(prev <= curr, "type ordering failed for {:?}", val);
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
        assert!(obj_slot < null_slot, "Null should sort after Object");
        assert!(obj_slot < arr_slot, "Object should sort before Array");
        assert!(arr_slot < null_slot, "Null should sort after Array");
    }

    // ---- sort function tests ----

    fn make_record(field_names: &[String], values: Vec<Value>) -> Record {
        Record::new(field_names, values)
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

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
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

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
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

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
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

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
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

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
        let result = encoder.sort(records, &keys, &orderings);

        let vals: Vec<(Value, Value)> = result.iter().map(|r| (r.get(&path("a")), r.get(&path("b")))).collect();
        assert_eq!(
            vals,
            vec![
                (Value::Int(1), Value::Int(10)),
                (Value::Int(1), Value::Int(30)),
                (Value::Int(2), Value::Int(10)),
            ]
        );
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
        assert_eq!(
            compare_values(&Value::Int(2), &Value::Int(1)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Int(1), &Value::Int(1)),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Null, &Value::Int(1)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(compare_values(&Value::Int(1), &Value::Null), std::cmp::Ordering::Less);
        assert_eq!(compare_values(&Value::Null, &Value::Missing), std::cmp::Ordering::Equal);
    }

    proptest! {
        #[test]
        fn prop_nullish_values_sort_last_ascending_and_first_descending(value in -1_000_000i32..=1_000_000) {
            let value = Value::Int(value);
            for nullish in [Value::Null, Value::Missing] {
                prop_assert_eq!(compare_values(&value, &nullish), std::cmp::Ordering::Less);
                prop_assert_eq!(compare_values(&nullish, &value), std::cmp::Ordering::Greater);
                prop_assert_eq!(compare_values(&nullish, &value).reverse(), std::cmp::Ordering::Less);
            }
        }
    }

    #[test]
    fn test_prefix_sort_matches_direct_sort_int() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(12345);

        let fields = vec!["x".to_string(), "y".to_string()];
        let records: Vec<Record> = (0..500)
            .map(|_| {
                make_record(
                    &fields,
                    vec![
                        Value::Int(rng.gen_range(-1000..1000)),
                        Value::String(format!("str_{}", rng.gen_range(0..50)).into()),
                    ],
                )
            })
            .collect();

        let keys = vec![path("x"), path("y")];
        let orderings = vec![Ordering::Asc, Ordering::Desc];

        // Direct sort (control)
        let mut direct = records.clone();
        direct_sort(&mut direct, &keys, &orderings);
        let direct_vals: Vec<(Value, Value)> = direct.iter().map(|r| (r.get(&path("x")), r.get(&path("y")))).collect();

        // Prefix sort (treatment)
        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
        let prefix_result = encoder.sort(records, &keys, &orderings);
        let prefix_vals: Vec<(Value, Value)> = prefix_result
            .iter()
            .map(|r| (r.get(&path("x")), r.get(&path("y"))))
            .collect();

        assert_eq!(direct_vals, prefix_vals);
    }

    #[test]
    fn test_prefix_sort_matches_direct_sort_with_nulls() {
        use rand::prelude::*;
        let mut rng = StdRng::seed_from_u64(99999);

        let fields = vec!["x".to_string()];
        let records: Vec<Record> = (0..200)
            .map(|_| {
                let val = if rng.gen_bool(0.2) {
                    Value::Null
                } else {
                    Value::Int(rng.gen_range(0..100))
                };
                make_record(&fields, vec![val])
            })
            .collect();

        let keys = vec![path("x")];
        let orderings = vec![Ordering::Asc];

        let mut direct = records.clone();
        direct_sort(&mut direct, &keys, &orderings);
        let direct_vals: Vec<Value> = direct.iter().map(|r| r.get(&path("x"))).collect();

        let encoder = PrefixSortEncoder {
            threshold: 0,
            ..Default::default()
        };
        let prefix_result = encoder.sort(records, &keys, &orderings);
        let prefix_vals: Vec<Value> = prefix_result.iter().map(|r| r.get(&path("x"))).collect();

        assert_eq!(direct_vals, prefix_vals);
    }
}
