// src/execution/batch_scan.rs

use std::io::BufRead;
use std::sync::Arc;
use crate::common::types::Variables;
use crate::execution::batch::*;
use crate::execution::batch_predicate::evaluate_batch_predicate;
use crate::execution::batch_tokenizer::{tokenize_line, tokenize_line_into};
use crate::execution::field_parser::{parse_field_column, parse_field_column_selected};
use crate::execution::log_schema::LogSchema;
use crate::execution::datasource::DataType;
use crate::execution::types::{Formula, StreamResult};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVecBuilder;
use crate::simd::selection::SelectionVector;

/// Scan-time aggregation mode. When set, the scan operator accumulates
/// aggregates directly during scanning without constructing full column batches,
/// then emits a single-row result batch at the end.
#[derive(Debug, Clone)]
pub(crate) enum ScanAggregation {
    /// COUNT(*) — count all rows passing the filter.
    CountStar,
    /// COUNT(column) — count non-null/non-missing values in the given field index.
    CountColumn(usize),
    /// SUM(column) — sum values in an integer/float field.
    SumColumn(usize),
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

    fn get_line(&self, idx: usize) -> &[u8] {
        let (start, end) = self.spans[idx];
        &self.data[start..end]
    }

    fn len(&self) -> usize {
        self.spans.len()
    }

    /// Return borrowed slices into the arena buffer, avoiding per-line heap allocation.
    fn to_slices(&self) -> Vec<&[u8]> {
        self.spans.iter()
            .map(|&(start, end)| &self.data[start..end])
            .collect()
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
            names: projected_fields.iter()
                .map(|&i| schema.field_name(i).to_string())
                .collect(),
            types: projected_fields.iter()
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
            arena: BatchLineArena::new(),
            scan_aggregation: None,
        }
    }

    pub fn with_scan_aggregation(mut self, agg: ScanAggregation, output_schema: BatchSchema) -> Self {
        self.scan_aggregation = Some(agg);
        self.batch_schema = output_schema;
        self
    }

    fn read_lines(&mut self) {
        self.arena.clear();
        while self.arena.len() < BATCH_SIZE {
            self.buf.clear();
            match self.reader.read_line(&mut self.buf) {
                Ok(0) => { self.done = true; break; }
                Ok(_) => {
                    let trimmed = self.buf.trim_end().as_bytes();
                    if !trimmed.is_empty() {
                        self.arena.push_line(trimmed);
                    }
                }
                Err(_) => { self.done = true; break; }
            }
        }
    }

    /// Consume all input in aggregation mode, returning a single-row result batch.
    /// This avoids constructing projected columns — only filter fields are parsed
    /// (if a predicate is pushed), and the aggregate is accumulated directly.
    fn next_batch_aggregated(&mut self, agg: ScanAggregation) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }
        // Clear aggregation mode so subsequent calls return None
        self.scan_aggregation = None;

        let mut count: i64 = 0;
        let mut sum: f64 = 0.0;

        loop {
            self.read_lines();
            if self.arena.len() == 0 {
                break;
            }

            let lines = self.arena.to_slices();
            let len = lines.len();

            // Tokenize all lines
            let mut line_fields: Vec<Vec<(usize, usize)>> = Vec::with_capacity(len);
            for line in &lines {
                tokenize_line_into(line, &mut self.offsets_scratch);
                line_fields.push(self.offsets_scratch.clone());
            }

            if let Some((ref formula, ref variables, ref registry)) = self.pushed_predicate {
                // Parse only filter fields for predicate evaluation
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

                match &agg {
                    ScanAggregation::CountStar => {
                        count += bitmap.count_ones() as i64;
                    }
                    ScanAggregation::CountColumn(field_idx) => {
                        // Parse the target column to check for nulls/missing
                        let datatype = self.schema.field_type(*field_idx);
                        let col = parse_field_column(&lines, &line_fields, *field_idx, &datatype);
                        let valid = col.validity_bitmap(len);
                        let active = bitmap.and(&valid);
                        count += active.count_ones() as i64;
                    }
                    ScanAggregation::SumColumn(field_idx) => {
                        let datatype = self.schema.field_type(*field_idx);
                        let col = parse_field_column(&lines, &line_fields, *field_idx, &datatype);
                        // Sum active, valid values
                        match &col {
                            TypedColumn::Int32 { data, null, missing, .. } => {
                                for i in 0..len {
                                    if bitmap.is_set(i) && null.is_set(i) && missing.is_set(i) {
                                        sum += data[i] as f64;
                                    }
                                }
                            }
                            TypedColumn::Float32 { data, null, missing, .. } => {
                                for i in 0..len {
                                    if bitmap.is_set(i) && null.is_set(i) && missing.is_set(i) {
                                        sum += data[i] as f64;
                                    }
                                }
                            }
                            _ => {
                                // Non-numeric column — sum is 0
                            }
                        }
                    }
                }
            } else {
                // No predicate — all rows active
                match &agg {
                    ScanAggregation::CountStar => {
                        count += len as i64;
                    }
                    ScanAggregation::CountColumn(field_idx) => {
                        let datatype = self.schema.field_type(*field_idx);
                        let col = parse_field_column(&lines, &line_fields, *field_idx, &datatype);
                        let valid = col.validity_bitmap(len);
                        count += valid.count_ones() as i64;
                    }
                    ScanAggregation::SumColumn(field_idx) => {
                        let datatype = self.schema.field_type(*field_idx);
                        let col = parse_field_column(&lines, &line_fields, *field_idx, &datatype);
                        match &col {
                            TypedColumn::Int32 { data, null, missing, .. } => {
                                for i in 0..len {
                                    if null.is_set(i) && missing.is_set(i) {
                                        sum += data[i] as f64;
                                    }
                                }
                            }
                            TypedColumn::Float32 { data, null, missing, .. } => {
                                for i in 0..len {
                                    if null.is_set(i) && missing.is_set(i) {
                                        sum += data[i] as f64;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        // Build single-row result batch
        match agg {
            ScanAggregation::CountStar | ScanAggregation::CountColumn(_) => {
                let mut builder = PaddedVecBuilder::<i32>::with_capacity(1);
                builder.push(count as i32);
                let data = builder.seal();
                let null_bm = Bitmap::all_set(1);
                let missing_bm = Bitmap::all_set(1);
                let col = TypedColumn::Int32 { data, null: null_bm, missing: missing_bm };
                Ok(Some(ColumnBatch {
                    columns: vec![col],
                    names: self.batch_schema.names.clone(),
                    selection: SelectionVector::All,
                    len: 1,
                }))
            }
            ScanAggregation::SumColumn(_) => {
                let mut builder = PaddedVecBuilder::<f32>::with_capacity(1);
                builder.push(sum as f32);
                let data = builder.seal();
                let null_bm = Bitmap::all_set(1);
                let missing_bm = Bitmap::all_set(1);
                let col = TypedColumn::Float32 { data, null: null_bm, missing: missing_bm };
                Ok(Some(ColumnBatch {
                    columns: vec![col],
                    names: self.batch_schema.names.clone(),
                    selection: SelectionVector::All,
                    len: 1,
                }))
            }
        }
    }
}

impl BatchStream for BatchScanOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // Scan-time aggregation: consume all input, return single-row result
        if let Some(ref agg) = self.scan_aggregation {
            return self.next_batch_aggregated(agg.clone());
        }

        loop {
            if self.done {
                return Ok(None);
            }

            self.read_lines();
            if self.arena.len() == 0 {
                return Ok(None);
            }

            let lines = self.arena.to_slices();
            let len = lines.len();

            // Tokenize all lines using reusable scratch buffer
            let mut line_fields: Vec<Vec<(usize, usize)>> = Vec::with_capacity(len);
            for line in &lines {
                tokenize_line_into(line, &mut self.offsets_scratch);
                line_fields.push(self.offsets_scratch.clone());
            }

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
                let ColumnBatch { columns: filter_cols, names: _filter_names_vec, .. } = filter_batch;
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
                        let col = parse_field_column_selected(
                            &lines, &line_fields, field_idx, &datatype, &selection,
                        );
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
    use std::io::Cursor;
    use std::sync::Arc;
    use crate::common::types::{Value, Variables};
    use crate::execution::types::{Expression, Formula, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};

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
        if !path.exists() { return; }

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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("method".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let mut scan = BatchScanOperator::new(
            reader, schema, projected, filter_fields,
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
                assert!(bm.is_set(0));   // GET
                assert!(!bm.is_set(1));  // POST - filtered out
                assert!(bm.is_set(2));   // GET
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("method".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let mut scan = BatchScanOperator::new(
            reader, schema, projected, filter_fields,
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("method".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(
            reader, schema, projected, filter_fields,
            Some((formula, Variables::new(), registry)),
        ).with_scan_aggregation(ScanAggregation::CountStar, output_schema);

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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("method".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("GET".to_string().into()))),
        );

        let output_schema = BatchSchema {
            names: vec!["_count".to_string()],
            types: vec![crate::execution::batch::ColumnType::Int32],
        };
        let mut scan = BatchScanOperator::new(
            reader, schema, projected, filter_fields,
            Some((formula, Variables::new(), registry)),
        ).with_scan_aggregation(ScanAggregation::CountStar, output_schema);

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
