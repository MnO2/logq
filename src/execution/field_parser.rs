use crate::execution::batch::TypedColumn;
use crate::execution::datasource::DataType;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
use crate::simd::selection::SelectionVector;
use crate::common::types::Value;
use hashbrown::HashMap;

/// Fast i32 parsing from ASCII bytes without UTF-8 validation or allocator.
#[inline]
fn parse_i32_fast(bytes: &[u8]) -> Option<i32> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    if start >= bytes.len() {
        return None;
    }
    let mut n: i32 = 0;
    for &b in &bytes[start..] {
        if b < b'0' || b > b'9' {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as i32)?;
    }
    Some(if neg { -n } else { n })
}

/// Fast f32 parsing using the fast-float crate.
#[inline]
fn parse_f32_fast(bytes: &[u8]) -> Option<f32> {
    fast_float::parse::<f32, &[u8]>(bytes).ok()
}

/// Parse a single field column from all rows in the batch.
pub(crate) fn parse_field_column<L: AsRef<[u8]>>(
    lines: &[L],
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
                    let raw = &lines[row].as_ref()[start..end];
                    let bytes = strip_quotes(raw);
                    data_builder.extend_from_slice(bytes);
                }
                offsets_builder.push(data_builder.len() as u32);
            }
            try_dict_encode(TypedColumn::Utf8 {
                data: data_builder.seal(),
                offsets: offsets_builder.seal(),
                null: null_bm,
                missing: missing_bm,
            })
        }
        DataType::Integral => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let bytes = &lines[row].as_ref()[start..end];
                    match parse_i32_fast(bytes) {
                        Some(v) => data.push(v),
                        None => { data.push(0); null_bm.unset(row); }
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
                    let bytes = &lines[row].as_ref()[start..end];
                    match parse_f32_fast(bytes) {
                        Some(v) => data.push(v),
                        None => { data.push(0.0); null_bm.unset(row); }
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
                    let raw = &lines[row].as_ref()[start..end];
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
                    let s = std::str::from_utf8(&lines[row].as_ref()[start..end]).unwrap_or("-");
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
                    let raw = &lines[row].as_ref()[start..end];
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
pub(crate) fn parse_field_column_selected<L: AsRef<[u8]>>(
    lines: &[L],
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
                    let bytes = strip_quotes(&lines[row].as_ref()[start..end]);
                    data_builder.extend_from_slice(bytes);
                }
                offsets_builder.push(data_builder.len() as u32);
            }
            try_dict_encode(TypedColumn::Utf8 {
                data: data_builder.seal(),
                offsets: offsets_builder.seal(),
                null: null_bm,
                missing: missing_bm,
            })
        }
        DataType::Integral => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(0);
                    null_bm.unset(row);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let bytes = &lines[row].as_ref()[start..end];
                    match parse_i32_fast(bytes) {
                        Some(v) => data.push(v),
                        None => { data.push(0); null_bm.unset(row); }
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
                if !selection.is_active(row, len) {
                    data.push(0.0);
                    null_bm.unset(row);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let bytes = &lines[row].as_ref()[start..end];
                    match parse_f32_fast(bytes) {
                        Some(v) => data.push(v),
                        None => { data.push(0.0); null_bm.unset(row); }
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
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row].as_ref()[start..end];
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
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row].as_ref()[start..end]).unwrap_or("-");
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
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row].as_ref()[start..end];
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

/// Attempt to dictionary-encode a Utf8 column.
/// If the number of unique values is less than half the row count (and <= 4096),
/// returns a DictUtf8 column; otherwise returns the original Utf8 unchanged.
fn try_dict_encode(col: TypedColumn) -> TypedColumn {
    let (data, offsets, null, missing) = match col {
        TypedColumn::Utf8 { data, offsets, null, missing } => (data, offsets, null, missing),
        other => return other,
    };
    let len = offsets.len() - 1;
    if len == 0 {
        return TypedColumn::Utf8 { data, offsets, null, missing };
    }

    // Phase 1: determine unique strings and assign codes.
    // Use borrowed slices in a block so the borrow is dropped before we move `data`.
    let result = {
        let mut dict: HashMap<&[u8], u16> = HashMap::with_capacity(32);
        let mut dict_spans: Vec<(usize, usize)> = Vec::new();
        let mut codes: Vec<u16> = Vec::with_capacity(len);
        let mut too_high = false;

        for i in 0..len {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let field = &data[start..end];
            let next_code = dict.len() as u16;
            let code = *dict.entry(field).or_insert_with(|| {
                dict_spans.push((start, end));
                next_code
            });
            codes.push(code);

            if dict.len() > 4096 || dict.len() * 2 > len {
                too_high = true;
                break;
            }
        }

        if too_high {
            None
        } else {
            Some((dict_spans, codes))
        }
    }; // dict (with borrows into data) is dropped here

    match result {
        None => TypedColumn::Utf8 { data, offsets, null, missing },
        Some((dict_spans, codes)) => {
            let mut dict_data_builder = PaddedVecBuilder::<u8>::new();
            let mut dict_offsets_builder = PaddedVecBuilder::<u32>::with_capacity(dict_spans.len() + 1);
            dict_offsets_builder.push(0);
            for &(start, end) in &dict_spans {
                dict_data_builder.extend_from_slice(&data[start..end]);
                dict_offsets_builder.push(dict_data_builder.len() as u32);
            }
            TypedColumn::DictUtf8 {
                dict_data: dict_data_builder.seal(),
                dict_offsets: dict_offsets_builder.seal(),
                codes: PaddedVec::from_vec(codes),
                null,
                missing,
            }
        }
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
    fn test_parse_int_selected_skips_inactive() {
        let lines: Vec<Vec<u8>> = vec![
            b"100".to_vec(),
            b"200".to_vec(),
            b"300".to_vec(),
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
        let col = parse_field_column_selected(&lines, &fields, 0, &DataType::Integral, &sel);
        match col {
            TypedColumn::Int32 { data, null, .. } => {
                assert_eq!(data[0], 100);
                assert_eq!(data[1], 0);   // inactive row gets default 0
                assert_eq!(data[2], 300);
                assert!(null.is_set(0));   // active, parsed ok
                assert!(!null.is_set(1));  // inactive, null unset
                assert!(null.is_set(2));   // active, parsed ok
            }
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn test_parse_float_selected_skips_inactive() {
        let lines: Vec<Vec<u8>> = vec![
            b"1.5".to_vec(),
            b"2.5".to_vec(),
            b"3.5".to_vec(),
        ];
        let fields = vec![
            vec![(0, 3)],
            vec![(0, 3)],
            vec![(0, 3)],
        ];
        let mut sel_bm = Bitmap::all_unset(3);
        sel_bm.set(0);
        let sel = SelectionVector::Bitmap(sel_bm);
        let col = parse_field_column_selected(&lines, &fields, 0, &DataType::Float, &sel);
        match col {
            TypedColumn::Float32 { data, null, .. } => {
                assert_eq!(data[0], 1.5);
                assert_eq!(data[1], 0.0);  // inactive row gets default 0.0
                assert_eq!(data[2], 0.0);  // inactive row gets default 0.0
                assert!(null.is_set(0));    // active, parsed ok
                assert!(!null.is_set(1));   // inactive, null unset
                assert!(!null.is_set(2));   // inactive, null unset
            }
            _ => panic!("expected Float32"),
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

    #[test]
    fn test_parse_string_field_with_slices() {
        let lines: Vec<&[u8]> = vec![b"hello world foo" as &[u8], b"bar baz qux" as &[u8]];
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
    fn test_dict_encode_low_cardinality() {
        // 8 rows with only 2 unique values → should dict-encode
        let lines: Vec<Vec<u8>> = vec![
            b"200".to_vec(), b"404".to_vec(), b"200".to_vec(), b"200".to_vec(),
            b"404".to_vec(), b"200".to_vec(), b"404".to_vec(), b"200".to_vec(),
        ];
        let fields: Vec<Vec<(usize, usize)>> = lines.iter().map(|l| vec![(0, l.len())]).collect();
        let col = parse_field_column(&lines, &fields, 0, &DataType::String);
        match &col {
            TypedColumn::DictUtf8 { dict_data, dict_offsets, codes, .. } => {
                // 2 unique dictionary entries
                assert_eq!(dict_offsets.len() - 1, 2);
                // Verify codes map correctly
                let code_200 = codes[0]; // first row is "200"
                let code_404 = codes[1]; // second row is "404"
                assert_ne!(code_200, code_404);
                assert_eq!(codes[2], code_200); // "200"
                assert_eq!(codes[4], code_404); // "404"
                // Verify dictionary entries
                for c in 0..2 {
                    let s = dict_offsets[c] as usize;
                    let e = dict_offsets[c + 1] as usize;
                    let entry = &dict_data[s..e];
                    assert!(entry == b"200" || entry == b"404");
                }
            }
            _ => panic!("expected DictUtf8 for low-cardinality data, got Utf8"),
        }

        // Verify extract_value round-trips correctly
        use crate::execution::batch::BatchToRowAdapter;
        for (i, expected) in ["200","404","200","200","404","200","404","200"].iter().enumerate() {
            match BatchToRowAdapter::extract_value(&col, i) {
                Value::String(s) => assert_eq!(s, *expected, "row {i}"),
                other => panic!("row {i}: expected String, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_dict_encode_high_cardinality_stays_utf8() {
        // All unique values → cardinality = 100% → should NOT dict-encode
        let lines: Vec<Vec<u8>> = (0..10).map(|i| format!("val_{i}").into_bytes()).collect();
        let fields: Vec<Vec<(usize, usize)>> = lines.iter().map(|l| vec![(0, l.len())]).collect();
        let col = parse_field_column(&lines, &fields, 0, &DataType::String);
        assert!(matches!(col, TypedColumn::Utf8 { .. }), "high cardinality should stay Utf8");
    }
}
