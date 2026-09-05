use crate::common::types::Value;
use crate::execution::batch::{BATCH_SIZE, BatchSchema, BatchStream, ColumnBatch, ColumnType, TypedColumn};
use crate::execution::json_reader::parse_columns;
use crate::execution::types::{StreamError, StreamResult};
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVecBuilder;
use crate::simd::selection::SelectionVector;
use std::collections::HashMap;
use std::io::BufRead;

/// A bounded JSONL scan with query-local root names and no whole-file inference.
/// The schema stays dynamic; homogeneous batches use primitive column storage.
pub(crate) struct JsonBatchScanOperator {
    reader: Box<dyn BufRead>,
    schema: BatchSchema,
    field_indices: HashMap<String, usize>,
    line: String,
    done: bool,
}

impl JsonBatchScanOperator {
    pub(crate) fn new(reader: Box<dyn BufRead>, fields: Vec<String>) -> Self {
        let mut field_indices = HashMap::with_capacity(fields.len());
        let mut names = Vec::with_capacity(fields.len());
        for field in fields {
            if !field_indices.contains_key(&field) {
                field_indices.insert(field.clone(), names.len());
                names.push(field);
            }
        }
        Self {
            reader,
            schema: BatchSchema {
                types: vec![ColumnType::Mixed; names.len()],
                names,
            },
            field_indices,
            line: String::with_capacity(512),
            done: false,
        }
    }
}

impl BatchStream for JsonBatchScanOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }
        let mut values: Vec<Vec<Value>> = (0..self.schema.names.len())
            .map(|_| Vec::with_capacity(BATCH_SIZE))
            .collect();
        let mut len = 0;
        while len < BATCH_SIZE {
            self.line.clear();
            if self.reader.read_line(&mut self.line).map_err(|_| StreamError::Reader)? == 0 {
                self.done = true;
                break;
            }
            parse_columns(&self.line, &self.field_indices, &mut values).map_err(|_| StreamError::Reader)?;
            len += 1;
        }
        if len == 0 {
            return Ok(None);
        }
        Ok(Some(ColumnBatch {
            columns: values.into_iter().map(typed_column).collect(),
            names: self.schema.names.clone(),
            selection: SelectionVector::All,
            len,
        }))
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    fn close(&self) {}
}

fn typed_column(values: Vec<Value>) -> TypedColumn {
    let len = values.len();
    let mut null = Bitmap::all_set(len);
    let mut missing = Bitmap::all_set(len);
    let mut kind = None;
    let mut string_bytes = 0;
    for (row, value) in values.iter().enumerate() {
        let value_kind = match value {
            Value::Null => {
                null.unset(row);
                continue;
            }
            Value::Missing => {
                missing.unset(row);
                continue;
            }
            Value::Int(_) => ColumnType::Int32,
            Value::Float(_) => ColumnType::Float32,
            Value::Boolean(_) => ColumnType::Boolean,
            Value::String(value) => {
                string_bytes += value.len();
                ColumnType::Utf8
            }
            _ => ColumnType::Mixed,
        };
        kind = Some(match kind {
            None => value_kind,
            Some(previous) if previous == value_kind => previous,
            _ => ColumnType::Mixed,
        });
    }
    match kind.unwrap_or(ColumnType::Mixed) {
        ColumnType::Int32 => {
            // Include SIMD tail padding in the allocation made before filling.
            let mut data = PaddedVecBuilder::with_capacity(len + 8);
            for value in values {
                data.push(match value {
                    Value::Int(value) => value,
                    _ => 0,
                });
            }
            TypedColumn::Int32 {
                data: data.seal(),
                null,
                missing,
            }
        }
        ColumnType::Float32 => {
            let mut data = PaddedVecBuilder::with_capacity(len + 8);
            for value in values {
                data.push(match value {
                    Value::Float(value) => value.0,
                    _ => 0.0,
                });
            }
            TypedColumn::Float32 {
                data: data.seal(),
                null,
                missing,
            }
        }
        ColumnType::Boolean => {
            let mut data = Bitmap::all_unset(len);
            for (row, value) in values.into_iter().enumerate() {
                if matches!(value, Value::Boolean(true)) {
                    data.set(row);
                }
            }
            TypedColumn::Boolean { data, null, missing }
        }
        ColumnType::Utf8 => {
            let mut data = PaddedVecBuilder::with_capacity(string_bytes + 32);
            let mut offsets = PaddedVecBuilder::with_capacity(len + 9);
            offsets.push(0);
            for value in values {
                if let Value::String(value) = value {
                    data.extend_from_slice(value.as_bytes());
                }
                offsets.push(data.len() as u32);
            }
            TypedColumn::Utf8 {
                data: data.seal(),
                offsets: offsets.seal(),
                null,
                missing,
            }
        }
        _ => TypedColumn::Mixed {
            data: values,
            null,
            missing,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::batch::{BATCH_SIZE, BatchToRowAdapter};
    use crate::execution::datasource::{ReaderBuilder, RecordRead};
    use ordered_float::OrderedFloat;
    use std::io::{BufReader, Cursor, Write};

    fn scanner(input: impl Into<Vec<u8>>, fields: &[&str]) -> JsonBatchScanOperator {
        JsonBatchScanOperator::new(
            Box::new(Cursor::new(input.into())),
            fields.iter().map(|field| (*field).to_string()).collect(),
        )
    }

    #[test]
    fn test_json_batch_primitive_columns_preserve_null_and_missing() {
        let mut scan = scanner(
            b"{\"n\":7,\"s\":\"hello\",\"b\":true,\"f\":1.5}\n{\"n\":null,\"s\":null,\"b\":null,\"f\":null}\n{}"
                .to_vec(),
            &["n", "s", "b", "f"],
        );
        assert_eq!(scan.schema().types, vec![ColumnType::Mixed; 4]);
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 3);
        assert!(matches!(batch.columns[0], TypedColumn::Int32 { .. }));
        assert!(matches!(batch.columns[1], TypedColumn::Utf8 { .. }));
        assert!(matches!(batch.columns[2], TypedColumn::Boolean { .. }));
        assert!(matches!(batch.columns[3], TypedColumn::Float32 { .. }));
        let expected = [
            Value::Int(7),
            Value::String("hello".into()),
            Value::Boolean(true),
            Value::Float(OrderedFloat(1.5)),
        ];
        for (column, expected) in batch.columns.iter().zip(expected) {
            assert_eq!(BatchToRowAdapter::extract_value(column, 0), expected);
            assert_eq!(BatchToRowAdapter::extract_value(column, 1), Value::Null);
            assert_eq!(BatchToRowAdapter::extract_value(column, 2), Value::Missing);
        }
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_json_batch_mixed_types_are_not_coerced() {
        let mut scan = scanner(
            "{\"x\":1}\n{\"x\":1.0}\n{\"x\":true}\n{\"x\":\"1\"}\n{\"x\":null}\n{}",
            &["x"],
        );
        let batch = scan.next_batch().unwrap().unwrap();
        assert!(matches!(batch.columns[0], TypedColumn::Mixed { .. }));
        for (row, expected) in [
            Value::Int(1),
            Value::Float(OrderedFloat(1.0)),
            Value::Boolean(true),
            Value::String("1".into()),
            Value::Null,
            Value::Missing,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(BatchToRowAdapter::extract_value(&batch.columns[0], row), expected);
        }
    }

    #[test]
    fn test_json_batch_type_can_change_between_batches() {
        let mut input = "{\"x\":1}\n".repeat(BATCH_SIZE);
        input.push_str("{\"x\":\"later\"}");
        let mut scan = scanner(input, &["x"]);
        let first = scan.next_batch().unwrap().unwrap();
        assert_eq!(first.len, BATCH_SIZE);
        assert!(matches!(first.columns[0], TypedColumn::Int32 { .. }));
        let second = scan.next_batch().unwrap().unwrap();
        assert_eq!(second.len, 1);
        assert!(matches!(second.columns[0], TypedColumn::Utf8 { .. }));
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_json_batch_count_has_rows_without_columns() {
        let mut scan = scanner("{\"ignored\":[1,true,null]}\n".repeat(BATCH_SIZE + 1), &[]);
        let first = scan.next_batch().unwrap().unwrap();
        assert!(first.columns.is_empty());
        assert_eq!(first.len, BATCH_SIZE);
        let last = scan.next_batch().unwrap().unwrap();
        assert_eq!(last.len, 1);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_json_batch_validation_matches_projected_reader() {
        for input in [
            r#"{"x":1,"ignored":1e9999}"#,
            r#"{"x":1,"ignored":"\uD800"}"#,
            r#"{"x":1,"ignored":{"\uD800":0}}"#,
            r#"{"x":1,"ignored":[1,]}"#,
            r#"{"x":1} trailing"#,
            "[]",
            " \n",
        ] {
            for fields in [&["x"][..], &[][..]] {
                let mut scan = scanner(input, fields);
                assert!(matches!(scan.next_batch(), Err(StreamError::Reader)), "{input}");
            }
        }
        let mut scan = scanner(vec![b'{', b'"', b'x', b'"', b':', b'"', 0xff, b'"', b'}'], &["x"]);
        assert!(matches!(scan.next_batch(), Err(StreamError::Reader)));
    }

    #[test]
    fn test_json_batch_matches_reader_for_escaped_keys_duplicates_and_nested_values() {
        let input =
            r#"{"omit":0,"\u0078":{"a":1,"b":2,"a":3},"x":[null,{"a":1,"b":2,"a":4}],"f":18446744073709551615}"#;
        let fields = vec!["x".to_string(), "f".to_string(), "absent".to_string()];
        let mut reader = ReaderBuilder::new("jsonl".into())
            .with_required_fields(fields.clone())
            .with_reader(input.as_bytes())
            .unwrap();
        let expected = reader.read_record().unwrap().unwrap().into_variables();
        let mut scan = scanner(input, &["x", "f", "absent"]);
        let batch = scan.next_batch().unwrap().unwrap();
        for (name, column) in batch.names.iter().zip(&batch.columns) {
            assert_eq!(
                BatchToRowAdapter::extract_value(column, 0),
                expected.get(name).cloned().unwrap_or(Value::Missing)
            );
        }
    }

    #[test]
    fn test_json_batch_reads_gzip_bufread_and_empty_input() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"{\"x\":1}\r\n{\"x\":2}").unwrap();
        let decoder = flate2::read::GzDecoder::new(Cursor::new(encoder.finish().unwrap()));
        let mut scan = JsonBatchScanOperator::new(Box::new(BufReader::new(decoder)), vec!["x".into()]);
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 2);
        assert_eq!(BatchToRowAdapter::extract_value(&batch.columns[0], 1), Value::Int(2));
        assert!(scanner(Vec::new(), &[]).next_batch().unwrap().is_none());
    }
}
