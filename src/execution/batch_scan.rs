// src/execution/batch_scan.rs

use std::io::BufRead;
use crate::execution::batch::*;
use crate::execution::batch_tokenizer::tokenize_line;
use crate::execution::field_parser::parse_field_column;
use crate::execution::log_schema::LogSchema;
use crate::execution::datasource::DataType;
use crate::execution::types::StreamResult;
use crate::simd::selection::SelectionVector;

/// Batch scan operator that reads lines from a BufRead source,
/// tokenizes them, and parses into columnar ColumnBatches.
pub(crate) struct BatchScanOperator {
    reader: Box<dyn BufRead>,
    schema: LogSchema,
    projected_fields: Vec<usize>,
    batch_schema: BatchSchema,
    done: bool,
    buf: String,
}

impl BatchScanOperator {
    pub fn new(
        reader: Box<dyn BufRead>,
        schema: LogSchema,
        projected_fields: Vec<usize>,
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
            batch_schema,
            done: false,
            buf: String::with_capacity(512),
        }
    }

    fn read_lines(&mut self) -> Vec<Vec<u8>> {
        let mut lines = Vec::with_capacity(BATCH_SIZE);
        while lines.len() < BATCH_SIZE {
            self.buf.clear();
            match self.reader.read_line(&mut self.buf) {
                Ok(0) => { self.done = true; break; }
                Ok(_) => {
                    let trimmed = self.buf.trim_end().as_bytes().to_vec();
                    if !trimmed.is_empty() {
                        lines.push(trimmed);
                    }
                }
                Err(_) => { self.done = true; break; }
            }
        }
        lines
    }
}

impl BatchStream for BatchScanOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }

        let lines = self.read_lines();
        if lines.is_empty() {
            return Ok(None);
        }

        let len = lines.len();

        // Tokenize all lines
        let line_fields: Vec<Vec<(usize, usize)>> = lines.iter()
            .map(|line| tokenize_line(line))
            .collect();

        // Parse projected fields into columns
        let mut columns = Vec::with_capacity(self.projected_fields.len());
        let mut names = Vec::with_capacity(self.projected_fields.len());
        for &field_idx in &self.projected_fields {
            let datatype = self.schema.field_type(field_idx);
            let col = parse_field_column(&lines, &line_fields, field_idx, &datatype);
            columns.push(col);
            names.push(self.schema.field_name(field_idx).to_string());
        }

        Ok(Some(ColumnBatch {
            columns,
            names,
            selection: SelectionVector::All,
            len,
        }))
    }

    fn schema(&self) -> &BatchSchema {
        &self.batch_schema
    }

    fn close(&self) {}
}

fn datatype_to_column_type(dt: &DataType) -> ColumnType {
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

    #[test]
    fn test_batch_scan_simple() {
        let data = b"hello world foo\nbar baz qux\n";
        let reader: Box<dyn BufRead> = Box::new(Cursor::new(data.to_vec()));
        let schema = LogSchema::from_format("squid");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(reader, schema, all_fields);

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
        let mut scan = BatchScanOperator::new(reader, schema, all_fields);

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
        let mut scan = BatchScanOperator::new(reader, schema, fields);

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
        let mut scan = BatchScanOperator::new(reader, schema, all_fields);

        let mut total_rows = 0;
        while let Some(batch) = scan.next_batch().unwrap() {
            total_rows += batch.len;
            assert!(batch.len <= BATCH_SIZE);
        }
        assert!(total_rows > 0);
    }
}
