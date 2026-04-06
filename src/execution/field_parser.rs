use crate::execution::batch::TypedColumn;
use crate::execution::datasource::DataType;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
use crate::simd::selection::SelectionVector;
use crate::common::types::Value;

/// Parse a single field column from all rows in the batch.
pub(crate) fn parse_field_column(
    lines: &[Vec<u8>],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
) -> TypedColumn {
    let len = lines.len();
    match datatype {
        DataType::String => {
            let mut data_builder = PaddedVecBuilder::<u8>::new();
            let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(len + 1);
            offsets_builder.push(0);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row][start..end];
                    let bytes = strip_quotes(raw);
                    data_builder.extend_from_slice(bytes);
                }
                offsets_builder.push(data_builder.len() as u32);
            }
            TypedColumn::Utf8 {
                data: data_builder.seal(),
                offsets: offsets_builder.seal(),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::Integral => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("0");
                    match s.parse::<i32>() {
                        Ok(v) => data.push(v),
                        Err(_) => { data.push(0); null_bm.unset(row); }
                    }
                } else {
                    data.push(0);
                    null_bm.unset(row);
                }
            }
            TypedColumn::Int32 {
                data: PaddedVec::from_vec(data),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::Float => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("0");
                    match s.parse::<f32>() {
                        Ok(v) => data.push(v),
                        Err(_) => { data.push(0.0); null_bm.unset(row); }
                    }
                } else {
                    data.push(0.0);
                    null_bm.unset(row);
                }
            }
            TypedColumn::Float32 {
                data: PaddedVec::from_vec(data),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::DateTime => {
            // Store as Mixed -- DateTime parsing is complex and varied
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row][start..end];
                    let s = std::str::from_utf8(strip_quotes(raw)).unwrap_or("");
                    match crate::execution::datasource::parse_utc_timestamp(s) {
                        Ok(dt) => data.push(Value::DateTime(dt)),
                        Err(_) => data.push(Value::Null),
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
        DataType::Host => {
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("-");
                    if s == "-" {
                        data.push(Value::Null);
                    } else {
                        match crate::common::types::parse_host(s) {
                            Ok(host) => data.push(Value::Host(Box::new(host))),
                            Err(_) => data.push(Value::Null),
                        }
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
        DataType::HttpRequest => {
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row][start..end];
                    let s = std::str::from_utf8(strip_quotes(raw)).unwrap_or("");
                    match crate::common::types::parse_http_request(s) {
                        Ok(req) => data.push(Value::HttpRequest(Box::new(req))),
                        Err(_) => data.push(Value::Null),
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
    }
}

/// Parse a field column only for active rows in the selection vector.
/// Inactive rows get zero-length entries (strings) or zero values (numbers).
pub(crate) fn parse_field_column_selected(
    lines: &[Vec<u8>],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
    selection: &SelectionVector,
) -> TypedColumn {
    let len = lines.len();
    match datatype {
        DataType::String => {
            let mut data_builder = PaddedVecBuilder::<u8>::new();
            let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(len + 1);
            offsets_builder.push(0);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if selection.is_active(row, len) && field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let bytes = strip_quotes(&lines[row][start..end]);
                    data_builder.extend_from_slice(bytes);
                }
                offsets_builder.push(data_builder.len() as u32);
            }
            TypedColumn::Utf8 {
                data: data_builder.seal(),
                offsets: offsets_builder.seal(),
                null: null_bm,
                missing: missing_bm,
            }
        }
        _ => parse_field_column(lines, fields, field_idx, datatype),
    }
}

/// Strip surrounding quotes or brackets from a byte slice.
fn strip_quotes(raw: &[u8]) -> &[u8] {
    if raw.len() >= 2 {
        match (raw[0], raw[raw.len() - 1]) {
            (b'"', b'"') | (b'\'', b'\'') | (b'[', b']') => &raw[1..raw.len() - 1],
            _ => raw,
        }
    } else {
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string_field() {
        let lines: Vec<Vec<u8>> = vec![
            b"hello world foo".to_vec(),
            b"bar baz qux".to_vec(),
        ];
        let fields = vec![
            vec![(0, 5), (6, 11), (12, 15)],
            vec![(0, 3), (4, 7), (8, 11)],
        ];
        let col = parse_field_column(&lines, &fields, 0, &DataType::String);
        match col {
            TypedColumn::Utf8 { offsets, .. } => {
                assert_eq!(offsets[0], 0);
                assert_eq!(offsets[1], 5);  // "hello"
                assert_eq!(offsets[2], 8);  // "hello" + "bar"
            }
            _ => panic!("expected Utf8"),
        }
    }

    #[test]
    fn test_parse_int_field() {
        let lines: Vec<Vec<u8>> = vec![
            b"100 hello".to_vec(),
            b"200 world".to_vec(),
        ];
        let fields = vec![
            vec![(0, 3), (4, 9)],
            vec![(0, 3), (4, 9)],
        ];
        let col = parse_field_column(&lines, &fields, 0, &DataType::Integral);
        match col {
            TypedColumn::Int32 { data, .. } => {
                assert_eq!(data[0], 100);
                assert_eq!(data[1], 200);
            }
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn test_parse_selected_skips_inactive() {
        let lines: Vec<Vec<u8>> = vec![
            b"aaa".to_vec(),
            b"bbb".to_vec(),
            b"ccc".to_vec(),
        ];
        let fields = vec![
            vec![(0, 3)],
            vec![(0, 3)],
            vec![(0, 3)],
        ];
        let mut sel_bm = Bitmap::all_unset(3);
        sel_bm.set(0);
        sel_bm.set(2);
        let sel = SelectionVector::Bitmap(sel_bm);
        let col = parse_field_column_selected(&lines, &fields, 0, &DataType::String, &sel);
        match col {
            TypedColumn::Utf8 { data, offsets, .. } => {
                assert_eq!(offsets.len(), 4); // 3 rows + 1
                let s0 = &data[offsets[0] as usize..offsets[1] as usize];
                assert_eq!(s0, b"aaa");
                assert_eq!(offsets[1], offsets[2]); // row 1 inactive
                let s2 = &data[offsets[2] as usize..offsets[3] as usize];
                assert_eq!(s2, b"ccc");
            }
            _ => panic!("expected Utf8"),
        }
    }

    #[test]
    fn test_strip_quotes() {
        assert_eq!(strip_quotes(b"\"hello\""), b"hello");
        assert_eq!(strip_quotes(b"'hello'"), b"hello");
        assert_eq!(strip_quotes(b"[hello]"), b"hello");
        assert_eq!(strip_quotes(b"hello"), b"hello");
        assert_eq!(strip_quotes(b"x"), b"x");
        assert_eq!(strip_quotes(b""), b"");
    }
}
