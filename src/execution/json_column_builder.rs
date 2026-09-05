//! JSON scalar destinations. Homogeneous columns never own per-row `Value`s;
//! a genuinely heterogeneous column upgrades to dynamic values on demand.

use crate::common::types::Value;
use crate::execution::batch::{BATCH_SIZE, TypedColumn};
use crate::execution::field_parser::try_dict_encode;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use ordered_float::OrderedFloat;

enum ColumnData {
    Empty,
    Int(Vec<i32>),
    Float(Vec<f32>),
    Boolean(Bitmap),
    Utf8 { data: Vec<u8>, offsets: Vec<u32> },
    Mixed(Vec<Value>),
}

pub(crate) struct JsonColumnBuilder {
    data: ColumnData,
    null: Bitmap,
    missing: Bitmap,
    len: usize,
}

impl JsonColumnBuilder {
    pub(crate) fn new() -> Self {
        Self {
            data: ColumnData::Empty,
            null: Bitmap::all_set(BATCH_SIZE),
            missing: Bitmap::all_unset(BATCH_SIZE),
            len: 0,
        }
    }

    pub(crate) fn begin_row(&mut self) {
        debug_assert!(self.len < BATCH_SIZE);
        self.len += 1;
        match &mut self.data {
            ColumnData::Int(data) => data.push(0),
            ColumnData::Float(data) => data.push(0.0),
            ColumnData::Utf8 { data, offsets } => offsets.push(data.len() as u32),
            ColumnData::Mixed(data) => data.push(Value::Missing),
            ColumnData::Empty | ColumnData::Boolean(_) => {}
        }
    }

    fn mark_present(&mut self) {
        self.null.set(self.len - 1);
        self.missing.set(self.len - 1);
    }

    pub(crate) fn put_null(&mut self) {
        let row = self.len - 1;
        match &mut self.data {
            ColumnData::Utf8 { data, offsets } => {
                // A duplicate key replaces the current row's string in place.
                data.truncate(offsets[row] as usize);
                offsets[row + 1] = data.len() as u32;
            }
            ColumnData::Mixed(data) => data[row] = Value::Null,
            _ => {}
        }
        self.null.unset(row);
        self.missing.set(row);
    }

    pub(crate) fn put_int(&mut self, value: i32) {
        if matches!(self.data, ColumnData::Empty) {
            let mut data = Vec::with_capacity(BATCH_SIZE + 8);
            data.resize(self.len, 0);
            self.data = ColumnData::Int(data);
        }
        if let ColumnData::Int(data) = &mut self.data {
            data[self.len - 1] = value;
            self.mark_present();
        } else {
            self.put_value(Value::Int(value));
        }
    }

    pub(crate) fn put_float(&mut self, value: f32) {
        if matches!(self.data, ColumnData::Empty) {
            let mut data = Vec::with_capacity(BATCH_SIZE + 8);
            data.resize(self.len, 0.0);
            self.data = ColumnData::Float(data);
        }
        if let ColumnData::Float(data) = &mut self.data {
            data[self.len - 1] = value;
            self.mark_present();
        } else {
            self.put_value(Value::Float(OrderedFloat(value)));
        }
    }

    pub(crate) fn put_bool(&mut self, value: bool) {
        if matches!(self.data, ColumnData::Empty) {
            self.data = ColumnData::Boolean(Bitmap::all_unset(BATCH_SIZE));
        }
        if let ColumnData::Boolean(data) = &mut self.data {
            if value {
                data.set(self.len - 1);
            } else {
                data.unset(self.len - 1);
            }
            self.mark_present();
        } else {
            self.put_value(Value::Boolean(value));
        }
    }

    pub(crate) fn put_str(&mut self, value: &str) {
        if matches!(self.data, ColumnData::Empty) {
            let mut offsets = Vec::with_capacity(BATCH_SIZE + 9);
            offsets.resize(self.len + 1, 0);
            self.data = ColumnData::Utf8 {
                data: Vec::with_capacity(BATCH_SIZE * value.len().min(64) + 32),
                offsets,
            };
        }
        if let ColumnData::Utf8 { data, offsets } = &mut self.data {
            let row = self.len - 1;
            data.truncate(offsets[row] as usize);
            data.extend_from_slice(value.as_bytes());
            offsets[row + 1] = data.len() as u32;
            self.mark_present();
        } else {
            self.put_value(Value::String(value.into()));
        }
    }

    /// Objects/arrays and type changes keep the existing dynamic semantics.
    pub(crate) fn put_value(&mut self, value: Value) {
        if !matches!(self.data, ColumnData::Mixed(_)) {
            let old = std::mem::replace(&mut self.data, ColumnData::Empty);
            let mut values = Vec::with_capacity(BATCH_SIZE);
            for row in 0..self.len {
                values.push(if !self.missing.is_set(row) {
                    Value::Missing
                } else if !self.null.is_set(row) {
                    Value::Null
                } else {
                    match &old {
                        ColumnData::Int(data) => Value::Int(data[row]),
                        ColumnData::Float(data) => Value::Float(OrderedFloat(data[row])),
                        ColumnData::Boolean(data) => Value::Boolean(data.is_set(row)),
                        ColumnData::Utf8 { data, offsets } => Value::String(
                            std::str::from_utf8(&data[offsets[row] as usize..offsets[row + 1] as usize])
                                .expect("JSON strings are valid UTF-8")
                                .into(),
                        ),
                        ColumnData::Empty => unreachable!("empty column has no present values"),
                        ColumnData::Mixed(_) => unreachable!(),
                    }
                });
            }
            self.data = ColumnData::Mixed(values);
        }
        let ColumnData::Mixed(values) = &mut self.data else {
            unreachable!()
        };
        values[self.len - 1] = value;
        self.mark_present();
    }

    pub(crate) fn finish(self, dictionary: bool) -> TypedColumn {
        let null = finish_bitmap(self.null, self.len);
        let missing = finish_bitmap(self.missing, self.len);
        let column = match self.data {
            ColumnData::Empty => TypedColumn::Mixed {
                data: (0..self.len)
                    .map(|row| {
                        if missing.is_set(row) {
                            Value::Null
                        } else {
                            Value::Missing
                        }
                    })
                    .collect(),
                null,
                missing,
            },
            ColumnData::Int(data) => TypedColumn::Int32 {
                data: PaddedVec::from_vec(data),
                null,
                missing,
            },
            ColumnData::Float(data) => TypedColumn::Float32 {
                data: PaddedVec::from_vec(data),
                null,
                missing,
            },
            ColumnData::Boolean(data) => TypedColumn::Boolean {
                data: finish_bitmap(data, self.len),
                null,
                missing,
            },
            ColumnData::Utf8 { mut data, offsets } => {
                // A large duplicate replaced by NULL/a short string leaves its
                // old capacity behind. Reclaim only substantial, mostly unused
                // arenas so normal batches retain their amortized allocation.
                const RECLAIM_MIN_CAPACITY: usize = 256 * 1024;
                const SIMD_PADDING: usize = 32;
                if data.capacity() >= RECLAIM_MIN_CAPACITY && data.len() < data.capacity() / 4 {
                    data.shrink_to(data.len().saturating_add(SIMD_PADDING));
                }
                // A single oversized string may fill capacity exactly. Reserve
                // padding without Vec::reserve geometrically doubling a large
                // payload merely to append PaddedVec's 32-byte SIMD tail.
                data.reserve_exact(SIMD_PADDING);
                TypedColumn::Utf8 {
                    data: PaddedVec::from_vec(data),
                    offsets: PaddedVec::from_vec(offsets),
                    null,
                    missing,
                }
            }
            // Duplicate keys can introduce a temporary type change. Re-check
            // final values so overwritten types do not change the batch schema.
            ColumnData::Mixed(values) => crate::execution::json_batch_scan::typed_column(values),
        };
        if dictionary { try_dict_encode(column) } else { column }
    }
}

fn finish_bitmap(mut bitmap: Bitmap, len: usize) -> Bitmap {
    bitmap.words.truncate(len.div_ceil(64));
    if len % 64 != 0 {
        if let Some(last) = bitmap.words.last_mut() {
            *last &= (1u64 << (len % 64)) - 1;
        }
    }
    bitmap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::BatchToRowAdapter;
    use crate::execution::json_reader::parse_columns;
    use std::collections::HashMap;

    #[test]
    fn scalar_json_visitors_write_final_storage_without_dynamic_staging() {
        let fields = HashMap::from([("n".to_owned(), 0), ("s".to_owned(), 1)]);
        let mut columns = vec![JsonColumnBuilder::new(), JsonColumnBuilder::new()];
        let payload = "long unescaped payload ".repeat(4);
        let line = format!("{{\"n\":7,\"s\":\"{payload}\"}}");
        for _ in 0..100 {
            parse_columns(&line, &fields, &mut columns).unwrap();
        }
        let ColumnData::Int(data) = &columns[0].data else {
            panic!("integers must stay in primitive storage while parsing");
        };
        let numeric_pointer = data.as_ptr();
        let ColumnData::Utf8 { data, offsets } = &columns[1].data else {
            panic!("strings must be appended directly to the byte arena");
        };
        assert_eq!(data.len(), payload.len() * 100);
        assert_eq!(offsets.len(), 101);
        let string_pointer = data.as_ptr();
        let mut columns = columns.into_iter();
        let TypedColumn::Int32 { data, .. } = columns.next().unwrap().finish(false) else {
            panic!()
        };
        assert_eq!(
            data.as_ptr(),
            numeric_pointer,
            "sealing must retain the numeric allocation"
        );
        let TypedColumn::Utf8 { data, .. } = columns.next().unwrap().finish(false) else {
            panic!()
        };
        assert_eq!(data.as_ptr(), string_pointer, "sealing must retain the string arena");
    }

    #[test]
    fn duplicate_strings_replace_arena_suffix_and_null_reclaims_it() {
        let mut builder = JsonColumnBuilder::new();
        builder.begin_row();
        builder.put_str("retained");
        builder.begin_row();
        builder.put_str(&"discarded".repeat(1000));
        builder.put_str("replacement");
        let ColumnData::Utf8 { data, .. } = &builder.data else {
            panic!()
        };
        assert_eq!(data, b"retainedreplacement");
        builder.put_null();
        let ColumnData::Utf8 { data, .. } = &builder.data else {
            panic!()
        };
        assert_eq!(data, b"retained");
        builder.begin_row();
        let column = builder.finish(false);
        assert_eq!(
            BatchToRowAdapter::extract_value(&column, 0),
            Value::String("retained".into())
        );
        assert_eq!(BatchToRowAdapter::extract_value(&column, 1), Value::Null);
        assert_eq!(BatchToRowAdapter::extract_value(&column, 2), Value::Missing);
    }

    #[test]
    fn partial_batch_masks_hide_unused_capacity() {
        for len in [1, 63, 64, 65, BATCH_SIZE - 1, BATCH_SIZE] {
            let mut builder = JsonColumnBuilder::new();
            for _ in 0..len {
                builder.begin_row();
                builder.put_bool(true);
            }
            let TypedColumn::Boolean { data, null, missing } = builder.finish(false) else {
                panic!()
            };
            assert_eq!(data.count_ones(), len);
            assert_eq!(null.count_ones(), len);
            assert_eq!(missing.count_ones(), len);
        }
    }

    #[test]
    fn overwritten_large_strings_release_unused_retained_arena() {
        for replacement in [None, Some("tiny")] {
            let mut builder = JsonColumnBuilder::new();
            builder.begin_row();
            builder.put_str("retained");
            builder.begin_row();
            builder.put_str(&"x".repeat(2 * 1024 * 1024));
            match replacement {
                Some(value) => builder.put_str(value),
                None => builder.put_null(),
            }
            let column = builder.finish(false);
            let TypedColumn::Utf8 { data, .. } = &column else {
                panic!("expected string storage");
            };
            assert!(
                data.capacity() < 64 * 1024,
                "discarded duplicate retained {} bytes",
                data.capacity()
            );
            assert_eq!(
                BatchToRowAdapter::extract_value(&column, 0),
                Value::String("retained".into())
            );
            assert_eq!(
                BatchToRowAdapter::extract_value(&column, 1),
                replacement.map_or(Value::Null, |value| Value::String(value.into()))
            );
        }
    }

    #[test]
    fn sealing_large_string_reserves_padding_without_doubling_payload_capacity() {
        let value = "x".repeat(2 * 1024 * 1024);
        let mut builder = JsonColumnBuilder::new();
        builder.begin_row();
        builder.put_str(&value);
        let column = builder.finish(false);
        let TypedColumn::Utf8 { data, .. } = &column else {
            panic!("expected string storage");
        };
        assert!(
            data.capacity() < value.len() + 64 * 1024,
            "SIMD padding grew {} bytes into {} retained bytes",
            value.len(),
            data.capacity()
        );
        assert_eq!(&data[..], value.as_bytes());
    }
}
