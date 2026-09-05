// src/execution/batch_scan.rs

use crate::common::types::Variables;
use crate::execution::batch::*;
use crate::execution::batch_predicate::evaluate_batch_predicate;
use crate::execution::batch_tokenizer::tokenize_line_into;
use crate::execution::datasource::DataType;
use crate::execution::field_parser::{parse_field_column, parse_field_column_selected};
use crate::execution::log_schema::LogSchema;
use crate::execution::types::{Formula, StreamError, StreamResult};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVecBuilder;
use crate::simd::selection::SelectionVector;
use std::io::BufRead;
use std::sync::Arc;

/// Scan-time aggregation mode. When set, the scan operator accumulates
/// aggregates directly during scanning without constructing full column batches,
/// then emits a single-row result batch at the end.
#[derive(Debug, Clone)]
pub(crate) enum ScanAggregation {
    /// COUNT(*) — count all rows passing the filter.
    CountStar,
}

/// Arena-style storage for batch lines. Stores all line bytes in a single
/// contiguous buffer with a spans index, avoiding per-line heap allocation.
pub(crate) struct BatchLineArena {
    data: Vec<u8>,
    spans: Vec<(usize, usize)>,
}

impl BatchLineArena {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(BATCH_SIZE * 256),
            spans: Vec::with_capacity(BATCH_SIZE),
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spans.clear();
    }

    fn push_line(&mut self, line: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(line);
        self.spans.push((start, self.data.len()));
    }

    #[cfg(test)]
    fn get_line(&self, idx: usize) -> &[u8] {
        let (start, end) = self.spans[idx];
        &self.data[start..end]
    }

    fn len(&self) -> usize {
        self.spans.len()
    }

    /// Return borrowed slices into the arena buffer, avoiding per-line heap allocation.
    fn to_slices(&self) -> Vec<&[u8]> {
        self.spans.iter().map(|&(start, end)| &self.data[start..end]).collect()
    }
}

/// Batch scan operator that reads lines from a BufRead source,
/// tokenizes them, and parses into columnar ColumnBatches.
pub(crate) struct BatchScanOperator {
    reader: Box<dyn BufRead>,
    schema: LogSchema,
    projected_fields: Vec<usize>,
    filter_field_indices: Vec<usize>,
    pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    batch_schema: BatchSchema,
    done: bool,
    buf: String,
    offsets_scratch: Vec<(usize, usize)>,
    field_offsets: Vec<(usize, usize)>,
    field_spans: Vec<(usize, usize)>,
    arena: BatchLineArena,
    /// Optional scan-time aggregation. When set, the operator accumulates
    /// the aggregate during scanning and emits a single-row result.
    scan_aggregation: Option<ScanAggregation>,
}

impl BatchScanOperator {
    pub fn new(
        reader: Box<dyn BufRead>,
        schema: LogSchema,
        projected_fields: Vec<usize>,
        filter_field_indices: Vec<usize>,
        pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    ) -> Self {
        let batch_schema = BatchSchema {
            names: projected_fields
                .iter()
                .map(|&i| schema.field_name(i).to_string())
                .collect(),
            types: projected_fields
                .iter()
                .map(|&i| datatype_to_column_type(&schema.field_type(i)))
                .collect(),
        };
        Self {
            reader,
            schema,
            projected_fields,
            filter_field_indices,
            pushed_predicate,
            batch_schema,
            done: false,
            buf: String::with_capacity(512),
            offsets_scratch: Vec::with_capacity(30),
            field_offsets: Vec::with_capacity(BATCH_SIZE * 30),
            field_spans: Vec::with_capacity(BATCH_SIZE),
            arena: BatchLineArena::new(),
            scan_aggregation: None,
        }
    }

    pub fn with_scan_aggregation(mut self, agg: ScanAggregation, output_schema: BatchSchema) -> Self {
        self.scan_aggregation = Some(agg);
        self.batch_schema = output_schema;
        self
    }

    fn read_lines(&mut self) -> StreamResult<()> {
        self.arena.clear();
        while self.arena.len() < BATCH_SIZE {
            self.buf.clear();
            match self.reader.read_line(&mut self.buf) {
                Ok(0) => {
                    self.done = true;
                    break;
                }
                Ok(_) => {
                    let trimmed = self.buf.trim_end().as_bytes();
                    if !trimmed.is_empty() {
                        self.arena.push_line(trimmed);
                    }
                }
                Err(_) => {
                    self.done = true;
                    return Err(StreamError::Reader);
                }
            }
        }
        Ok(())
    }

    /// Reuse one offsets arena for the whole batch, rather than allocating an
    /// owned Vec for every line. Field parsers borrow each row's range.
    fn tokenize_lines(&mut self) {
        self.field_offsets.clear();
        self.field_spans.clear();
        for &(start, end) in &self.arena.spans {
            tokenize_line_into(&self.arena.data[start..end], &mut self.offsets_scratch);
            let first = self.field_offsets.len();
            self.field_offsets.extend_from_slice(&self.offsets_scratch);
            self.field_spans.push((first, self.field_offsets.len()));
        }
    }

    /// Count framed nonblank UTF-8 lines without tokenization or a line arena.
    /// Complete lines are inspected in the reader's buffer; only lines split
    /// across buffers need temporary storage.
    fn count_unfiltered_rows(&mut self) -> StreamResult<i64> {
        fn nonblank(line: &[u8]) -> StreamResult<bool> {
            let text = std::str::from_utf8(line).map_err(|_| StreamError::Reader)?;
            Ok(!text.trim_end().is_empty())
        }

        let mut count = 0;
        let mut partial = Vec::new();
        loop {
            let input = self.reader.fill_buf().map_err(|_| StreamError::Reader)?;
            if input.is_empty() {
                break;
            }
            let consumed = input.len();
            let mut start = 0;
            for end in memchr::memchr_iter(b'\n', input) {
                let line = &input[start..end];
                let present = if partial.is_empty() {
                    nonblank(line)?
                } else {
                    partial.extend_from_slice(line);
                    let present = nonblank(&partial)?;
                    partial.clear();
                    present
                };
                count += i64::from(present);
                start = end + 1;
            }
            partial.extend_from_slice(&input[start..]);
            self.reader.consume(consumed);
        }
        if !partial.is_empty() {
            count += i64::from(nonblank(&partial)?);
        }
        Ok(count)
    }

    /// Consume COUNT(*) into mergeable i64 state without narrowing it into
    /// the public Int32 output representation at each parallel worker.
    pub(crate) fn consume_count(&mut self) -> StreamResult<i64> {
        if self.done {
            return Ok(0);
        }
        self.scan_aggregation = None;
        let count = if self.pushed_predicate.is_none() {
            self.done = true;
            self.count_unfiltered_rows()?
        } else {
            let mut count: i64 = 0;
            while !self.done {
                self.read_lines()?;
                if self.arena.len() == 0 {
                    break;
                }
                self.tokenize_lines();
                let lines = self.arena.to_slices();
                let len = lines.len();
                let line_fields: Vec<&[(usize, usize)]> = self
                    .field_spans
                    .iter()
                    .map(|&(start, end)| &self.field_offsets[start..end])
                    .collect();
                let mut filter_columns = Vec::with_capacity(self.filter_field_indices.len());
                let mut filter_names = Vec::with_capacity(self.filter_field_indices.len());
                for &field_idx in &self.filter_field_indices {
                    let datatype = self.schema.field_type(field_idx);
                    filter_columns.push(parse_field_column(&lines, &line_fields, field_idx, &datatype));
                    filter_names.push(self.schema.field_name(field_idx).to_string());
                }
                let filter_batch = ColumnBatch {
                    columns: filter_columns,
                    names: filter_names,
                    selection: SelectionVector::All,
                    len,
                };
                let (formula, variables, registry) = self.pushed_predicate.as_ref().unwrap();
                count += evaluate_batch_predicate(formula, &filter_batch, variables, registry)?.count_ones() as i64;
            }
            count
        };
        self.done = true;
        Ok(count)
    }

    /// Consume all input in aggregation mode, returning a single-row result batch.
    fn next_batch_aggregated(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }
        let count = self.consume_count()?;
        let mut builder = PaddedVecBuilder::<i32>::with_capacity(1);
        builder.push(count as i32);
        let column = TypedColumn::Int32 {
            data: builder.seal(),
            null: Bitmap::all_set(1),
            missing: Bitmap::all_set(1),
        };
        Ok(Some(ColumnBatch {
            columns: vec![column],
            names: self.batch_schema.names.clone(),
            selection: SelectionVector::All,
            len: 1,
        }))
    }
}

impl BatchStream for BatchScanOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // Scan-time aggregation: consume all input, return single-row result
        if self.scan_aggregation.is_some() {
            return self.next_batch_aggregated();
        }

        loop {
            if self.done {
                return Ok(None);
            }

            self.read_lines()?;
            if self.arena.len() == 0 {
                return Ok(None);
            }

            self.tokenize_lines();
            let lines = self.arena.to_slices();
            let len = lines.len();
            let line_fields: Vec<&[(usize, usize)]> = self
                .field_spans
                .iter()
                .map(|&(start, end)| &self.field_offsets[start..end])
                .collect();

            if let Some((ref formula, ref variables, ref registry)) = self.pushed_predicate {
                // === Two-phase scan ===

                // Phase 1: Parse only filter fields, evaluate predicate
                let mut filter_columns = Vec::with_capacity(self.filter_field_indices.len());
                let mut filter_names = Vec::with_capacity(self.filter_field_indices.len());
                for &field_idx in &self.filter_field_indices {
                    let datatype = self.schema.field_type(field_idx);
                    let col = parse_field_column(&lines, &line_fields, field_idx, &datatype);
                    filter_columns.push(col);
                    filter_names.push(self.schema.field_name(field_idx).to_string());
                }

                let filter_batch = ColumnBatch {
                    columns: filter_columns,
                    names: filter_names,
                    selection: SelectionVector::All,
                    len,
                };

                let bitmap = evaluate_batch_predicate(formula, &filter_batch, variables, registry)?;

                // If all rows filtered out, skip to next batch
                if bitmap.count_ones() == 0 {
                    continue;
                }

                // Phase 2: Parse remaining projected fields with selection
                let selection = SelectionVector::Bitmap(bitmap);

                // Decompose the filter batch to reclaim its columns
                let ColumnBatch {
                    columns: filter_cols,
                    names: _filter_names_vec,
                    ..
                } = filter_batch;
                let mut filter_col_map: Vec<Option<TypedColumn>> = filter_cols.into_iter().map(Some).collect();

                let mut columns = Vec::with_capacity(self.projected_fields.len());
                let mut names = Vec::with_capacity(self.projected_fields.len());
                for &field_idx in &self.projected_fields {
                    // Check if this field was already parsed in Phase 1
                    let reuse_pos = self.filter_field_indices.iter().position(|&fi| fi == field_idx);
                    if let Some(pos) = reuse_pos {
                        // Reuse the column from Phase 1
                        columns.push(filter_col_map[pos].take().unwrap());
                    } else {
                        // Parse with selection (skip inactive rows)
                        let datatype = self.schema.field_type(field_idx);
                        let col = parse_field_column_selected(&lines, &line_fields, field_idx, &datatype, &selection);
                        columns.push(col);
                    }
                    names.push(self.schema.field_name(field_idx).to_string());
                }

                return Ok(Some(ColumnBatch {
                    columns,
                    names,
                    selection,
                    len,
                }));
            } else {
                // === Single-phase scan (no predicate) ===
                let mut columns = Vec::with_capacity(self.projected_fields.len());
                let mut names = Vec::with_capacity(self.projected_fields.len());
                for &field_idx in &self.projected_fields {
                    let datatype = self.schema.field_type(field_idx);
                    let col = parse_field_column(&lines, &line_fields, field_idx, &datatype);
                    columns.push(col);
                    names.push(self.schema.field_name(field_idx).to_string());
                }

                return Ok(Some(ColumnBatch {
                    columns,
                    names,
                    selection: SelectionVector::All,
                    len,
                }));
            }
        }
    }

    fn schema(&self) -> &BatchSchema {
        &self.batch_schema
    }

    fn close(&self) {}
}

pub(crate) fn datatype_to_column_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::String => ColumnType::Utf8,
        DataType::Integral => ColumnType::Int32,
        DataType::Float => ColumnType::Float32,
        DataType::DateTime => ColumnType::Mixed,
        DataType::Host => ColumnType::Mixed,
        DataType::HttpRequest => ColumnType::Mixed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{Value, Variables};
    use crate::execution::types::{Expression, Formula, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};
    use std::io::Cursor;
    use std::sync::Arc;

    struct FailingAfterData(Cursor<Vec<u8>>);

    impl std::io::Read for FailingAfterData {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let input = self.fill_buf()?;
            let len = input.len().min(output.len());
            output[..len].copy_from_slice(&input[..len]);
            self.consume(len);
            Ok(len)
        }
    }

    impl BufRead for FailingAfterData {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            let input = self.0.fill_buf()?;
            if input.is_empty() {
                Err(std::io::Error::other("injected read failure"))
            } else {
                Ok(input)
            }
        }
        fn consume(&mut self, amount: usize) {
            self.0.consume(amount);
        }
    }

    fn count_scan(reader: Box<dyn BufRead>) -> BatchScanOperator {
        BatchScanOperator::new(reader, LogSchema::from_format("squid"), vec![], vec![], None).with_scan_aggregation(
            ScanAggregation::CountStar,
            BatchSchema {
                names: vec!["n".into()],
                types: vec![ColumnType::Int32],
            },
        )
    }

    #[test]
    fn consume_count_keeps_i64_state_without_output_column() {
        let mut scan = count_scan(Box::new(Cursor::new(b"one\ntwo\nlast".to_vec())));
        assert_eq!(scan.consume_count().unwrap(), 3i64);
        assert!(scan.next_batch().unwrap().is_none());
        assert!(scan.arena.spans.is_empty());
        assert!(scan.field_offsets.is_empty());
    }

    #[test]
    fn scan_propagates_io_failure_instead_of_returning_partial_batch() {
        let reader = Box::new(FailingAfterData(Cursor::new(b"first row\n".to_vec())));
        let mut scan = BatchScanOperator::new(reader, LogSchema::from_format("squid"), vec![0], vec![], None);
        assert!(matches!(
            scan.next_batch(),
            Err(crate::execution::types::StreamError::Reader)
        ));
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn count_scan_propagates_io_failure_instead_of_partial_count() {
        let mut scan = count_scan(Box::new(FailingAfterData(Cursor::new(b"first row\n".to_vec()))));
        assert!(matches!(
            scan.next_batch(),
            Err(crate::execution::types::StreamError::Reader)
        ));
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn count_framing_preserves_blank_lines_utf8_crlf_and_unterminated_rows() {
        let data = "\n \t\r\n\u{2003}\nfirst row\r\n第二 行\nunterminated";
        for buffer_size in [1, 2, 3, 7, 64] {
            let reader = std::io::BufReader::with_capacity(buffer_size, Cursor::new(data.as_bytes().to_vec()));
            let mut scan = count_scan(Box::new(reader));
            let batch = scan.next_batch().unwrap().unwrap();
            assert_eq!(BatchToRowAdapter::extract_value(&batch.columns[0], 0), Value::Int(3));
            assert!(scan.next_batch().unwrap().is_none());
        }
    }

    #[test]
    fn count_framing_rejects_invalid_utf8_like_the_row_reader() {
        for data in [b"valid\n\xff\n".as_slice(), b"valid\n\xff".as_slice()] {
            let mut scan = count_scan(Box::new(Cursor::new(data.to_vec())));
            assert!(matches!(
                scan.next_batch(),
                Err(crate::execution::types::StreamError::Reader)
            ));
        }
    }

    #[test]
    fn scan_offsets_remain_correct_across_batches_with_different_widths() {
        let mut data = "first \"quoted field\" tail\n".repeat(BATCH_SIZE);
        data.push_str("last\nnext [bracket field] trailing");
        let mut scan = BatchScanOperator::new(
            Box::new(Cursor::new(data.into_bytes())),
            LogSchema::from_format("squid"),
            vec![0, 1, 2],
            vec![],
            None,
        );
        let first = scan.next_batch().unwrap().unwrap();
        assert_eq!(
            BatchToRowAdapter::extract_value(&first.columns[1], BATCH_SIZE - 1),
            Value::String("quoted field".into())
        );
        let second = scan.next_batch().unwrap().unwrap();
        assert_eq!(second.len, 2);
        assert_eq!(BatchToRowAdapter::extract_value(&second.columns[1], 0), Value::Null);
        assert_eq!(
            BatchToRowAdapter::extract_value(&second.columns[1], 1),
            Value::String("bracket field".into())
        );
        assert_eq!(
            BatchToRowAdapter::extract_value(&second.columns[2], 1),
            Value::String("trailing".into())
        );
    }

    #[test]
    fn test_batch_scan_simple() {
        let data = b"hello world foo\nbar baz qux\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(reader, schema, all_fields, vec![], None);

        let batch = scan.next_batch().unwrap();
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.len, 2);
    }

    #[test]
    fn test_batch_scan_empty() {
        let data = b"";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(reader, schema, all_fields, vec![], None);

        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_scan_respects_batch_size() {
        // Create more than BATCH_SIZE lines
        let mut data = String::new();
        for i in 0..BATCH_SIZE + 10 {
            data.push_str(&format!("line{} data{}\n", i, i));
        }
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.into_bytes()));
        let schema = LogSchema::from_format("squid");
        let fields: Vec<usize> = vec![0]; // just first field
        let mut scan = BatchScanOperator::new(reader, schema, fields, vec![], None);

        let batch1 = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch1.len, BATCH_SIZE);

        let batch2 = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch2.len, 10);

        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_scan_with_real_elb_data() {
        let path = std::path::PathBuf::from("data/AWSELB.log");
        if !path.exists() {
            return;
        }

        let file = std::fs::File::open(&path).unwrap();
        let reader: Box<dyn BufRead> = Box::new(std::io::BufReader::new(file));
        let schema = LogSchema::from_format("elb");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(reader, schema, all_fields, vec![], None);

        let mut total_rows = 0;
        while let Some(batch) = scan.next_batch().unwrap() {
            total_rows += batch.len;
            assert!(batch.len <= BATCH_SIZE);
        }
        assert!(total_rows > 0);
    }

    #[test]
    fn test_two_phase_scan_no_predicate_unchanged() {
        // With no pushed predicate, behavior should be identical to before
        let data = b"hello world foo\nbar baz qux\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected: Vec<usize> = vec![0, 1];
        let mut scan = BatchScanOperator::new(reader, schema, projected, vec![], None);

        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 2);
        assert_eq!(batch.columns.len(), 2);
        assert_eq!(batch.names, vec!["timestamp", "elapsed"]);
        assert!(matches!(batch.selection, SelectionVector::All));
    }

    #[test]
    fn test_two_phase_scan_with_pushed_predicate() {
        // Squid format: field 5 = "method" (all String type)
        // Lines: 3 rows, method is field index 5
        // We'll filter on method == "GET"
        let data = b"ts1 1 host1 status1 100 GET url1 rfc1 peer1 type1\n\
                      ts2 2 host2 status2 200 POST url2 rfc2 peer2 type2\n\
                      ts3 3 host3 status3 300 GET url3 rfc3 peer3 type3\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        // Project fields 0 (timestamp), 5 (method), 6 (url)
        let projected = vec![0, 5, 6];
        // Filter on field 5 (method)
        let filter_fields = vec![5];

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "method".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let mut scan = BatchScanOperator::new(
            reader,
            schema,
            projected,
            filter_fields,
            Some((formula, Variables::new(), registry)),
        );

        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 3); // all 3 rows present in batch
        assert_eq!(batch.columns.len(), 3); // 3 projected columns
        assert_eq!(batch.names, vec!["timestamp", "method", "url"]);

        // Selection bitmap should mark rows 0 and 2 as active (GET rows)
        match &batch.selection {
            SelectionVector::Bitmap(bm) => {
                assert_eq!(bm.count_ones(), 2);
                assert!(bm.is_set(0)); // GET
                assert!(!bm.is_set(1)); // POST - filtered out
                assert!(bm.is_set(2)); // GET
            }
            _ => panic!("expected Bitmap selection"),
        }
    }

    #[test]
    fn test_two_phase_scan_all_filtered_skips_batch() {
        // All rows have method "POST", filter for "GET" => all filtered out
        let data = b"ts1 1 host1 status1 100 POST url1 rfc1 peer1 type1\n\
                      ts2 2 host2 status2 200 POST url2 rfc2 peer2 type2\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected = vec![0, 5];
        let filter_fields = vec![5];

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "method".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let mut scan = BatchScanOperator::new(
            reader,
            schema,
            projected,
            filter_fields,
            Some((formula, Variables::new(), registry)),
        );

        // All rows filtered => should return None (no more data)
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_line_arena() {
        let mut arena = BatchLineArena::new();

        // Push lines and verify
        arena.push_line(b"hello world");
        arena.push_line(b"foo bar baz");
        assert_eq!(arena.len(), 2);
        assert_eq!(arena.get_line(0), b"hello world");
        assert_eq!(arena.get_line(1), b"foo bar baz");

        // to_slices: borrowed slices into arena buffer
        let slices = arena.to_slices();
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0], b"hello world");
        assert_eq!(slices[1], b"foo bar baz");

        // Clear and reuse
        arena.clear();
        assert_eq!(arena.len(), 0);

        arena.push_line(b"reused line");
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.get_line(0), b"reused line");
    }

    #[test]
    fn test_scan_aggregation_count_star_no_predicate() {
        // 3 lines, no predicate => COUNT(*) = 3
        let data = b"ts1 1 host1 status1 100 GET url1 rfc1 peer1 type1\n\
                      ts2 2 host2 status2 200 POST url2 rfc2 peer2 type2\n\
                      ts3 3 host3 status3 300 GET url3 rfc3 peer3 type3\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected = vec![0];
        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(reader, schema, projected, vec![], None)
            .with_scan_aggregation(ScanAggregation::CountStar, output_schema);

        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 1);
        assert_eq!(batch.columns.len(), 1);
        // Extract the count value
        match &batch.columns[0] {
            TypedColumn::Int32 { data, .. } => assert_eq!(data[0], 3),
            _ => panic!("expected Int32 column"),
        }
        // Second call should return None
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_scan_aggregation_count_star_with_predicate() {
        // 3 lines, filter method == "GET" => COUNT(*) = 2
        let data = b"ts1 1 host1 status1 100 GET url1 rfc1 peer1 type1\n\
                      ts2 2 host2 status2 200 POST url2 rfc2 peer2 type2\n\
                      ts3 3 host3 status3 300 GET url3 rfc3 peer3 type3\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected = vec![5]; // method field
        let filter_fields = vec![5];

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "method".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(
            reader,
            schema,
            projected,
            filter_fields,
            Some((formula, Variables::new(), registry)),
        )
        .with_scan_aggregation(ScanAggregation::CountStar, output_schema);

        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 1);
        match &batch.columns[0] {
            TypedColumn::Int32 { data, .. } => assert_eq!(data[0], 2),
            _ => panic!("expected Int32 column"),
        }
    }

    #[test]
    fn test_scan_aggregation_count_star_all_filtered() {
        // All rows have POST, filter for GET => COUNT(*) = 0
        let data = b"ts1 1 host1 status1 100 POST url1 rfc1 peer1 type1\n\
                      ts2 2 host2 status2 200 POST url2 rfc2 peer2 type2\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected = vec![5];
        let filter_fields = vec![5];

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "method".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(
            reader,
            schema,
            projected,
            filter_fields,
            Some((formula, Variables::new(), registry)),
        )
        .with_scan_aggregation(ScanAggregation::CountStar, output_schema);

        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 1);
        match &batch.columns[0] {
            TypedColumn::Int32 { data, .. } => assert_eq!(data[0], 0),
            _ => panic!("expected Int32 column"),
        }
    }

    #[test]
    fn test_scan_aggregation_empty_input() {
        let data = b"";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let projected = vec![0];
        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(reader, schema, projected, vec![], None)
            .with_scan_aggregation(ScanAggregation::CountStar, output_schema);

        // Empty input should still return a result batch with count=0
        let batch = scan.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 1);
        match &batch.columns[0] {
            TypedColumn::Int32 { data, .. } => assert_eq!(data[0], 0),
            _ => panic!("expected Int32 column"),
        }
    }
}
