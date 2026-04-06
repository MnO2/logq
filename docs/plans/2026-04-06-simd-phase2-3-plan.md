# Plan: SIMD Phase 2-3 — Adapters + Batch Operators

**Goal**: Wire Phase 1 foundation types into the actual execution pipeline, replacing row-at-a-time processing with columnar batch processing for scan, filter, project, and limit operators, with adapters enabling incremental migration.

**Architecture**: Adapters (`BatchToRowAdapter`, `RowToBatchAdapter`) bridge batch and row pipelines so operators can be migrated one at a time. `BatchScanOperator` replaces `LogFileStream` with two-phase lazy parsing. `BatchFilterOperator` evaluates predicates on typed columns using SIMD kernels. The physical `Node::get()` method is extended to produce batch pipelines when possible, falling back to row pipelines via adapters for unconverted operators.

**Tech Stack**: Rust (edition 2018, stable), criterion benchmarks.

**Design Reference**: `docs/plans/2026-04-06-simd-physical-plan-design-final.md`

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Steps 1-2 | Yes | Phase 2: BatchToRowAdapter + RowToBatchAdapter |
| 2 | Step 3 | No | Phase 2: Adapter integration test. Depends on Group 1. |
| 3 | Step 4 | No | Phase 3: LogSchema type. Independent foundation. |
| 4 | Step 5 | No | Phase 3: Batch tokenizer. Depends on Group 3. |
| 5 | Step 6 | No | Phase 3: Field parsing into TypedColumns. Depends on Group 4. |
| 6 | Step 7 | No | Phase 3: BatchScanOperator. Depends on Groups 4-5. |
| 7 | Step 8 | No | Phase 3: Batch predicate evaluation. Depends on Group 6. |
| 8 | Step 9 | No | Phase 3: BatchFilterOperator. Depends on Group 7. |
| 9 | Step 10 | No | Phase 3: BatchProjectOperator. Depends on Group 8. |
| 10 | Step 11 | No | Phase 3: BatchLimitOperator. Depends on Group 8. |
| 11 | Step 12 | No | Phase 3: Wire into Node::get(). Depends on Groups 9-10. |
| 12 | Step 13 | No | Phase 3: Benchmark comparison. Depends on Group 11. |
| 13 | Step 14 | No | End-to-end verification. Depends on all. |

---

## Phase 2: Adapters

### Step 1: Implement BatchToRowAdapter

**File**: `src/execution/batch.rs` (append)

#### 1a. Write failing tests

```rust
// Append to existing tests module in src/execution/batch.rs

#[test]
fn test_batch_to_row_adapter_empty() {
    use crate::execution::stream::RecordStream;
    struct NoBatches;
    impl BatchStream for NoBatches {
        fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
            Ok(None)
        }
        fn schema(&self) -> &BatchSchema {
            &BatchSchema { names: vec![], types: vec![] }
        }
        fn close(&self) {}
    }
    // Cannot use a temporary for schema - need to store it
    // Use the adapter
    let mut adapter = BatchToRowAdapter::new(Box::new(NoBatches));
    assert!(adapter.next().unwrap().is_none());
}

#[test]
fn test_batch_to_row_adapter_converts_int32() {
    use crate::execution::stream::{RecordStream, Record};
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::selection::SelectionVector;

    let col = TypedColumn::Int32 {
        data: PaddedVec::from_vec(vec![10, 20, 30]),
        null: Bitmap::all_set(3),
        missing: Bitmap::all_set(3),
    };
    let batch = ColumnBatch {
        columns: vec![col],
        names: vec!["x".to_string()],
        selection: SelectionVector::All,
        len: 3,
    };

    struct OneBatch { batch: Option<ColumnBatch> }
    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
            Ok(self.batch.take())
        }
        fn schema(&self) -> &BatchSchema {
            &BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] }
        }
        fn close(&self) {}
    }

    let mut adapter = BatchToRowAdapter::new(Box::new(OneBatch { batch: Some(batch) }));
    let r1 = adapter.next().unwrap().unwrap();
    assert_eq!(r1.to_variables()["x"], crate::common::types::Value::Int(10));
    let r2 = adapter.next().unwrap().unwrap();
    assert_eq!(r2.to_variables()["x"], crate::common::types::Value::Int(20));
    let r3 = adapter.next().unwrap().unwrap();
    assert_eq!(r3.to_variables()["x"], crate::common::types::Value::Int(30));
    assert!(adapter.next().unwrap().is_none());
}

#[test]
fn test_batch_to_row_adapter_respects_selection() {
    use crate::execution::stream::RecordStream;
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::selection::SelectionVector;

    let col = TypedColumn::Int32 {
        data: PaddedVec::from_vec(vec![10, 20, 30]),
        null: Bitmap::all_set(3),
        missing: Bitmap::all_set(3),
    };
    let mut sel = Bitmap::all_unset(3);
    sel.set(0); sel.set(2); // skip row 1
    let batch = ColumnBatch {
        columns: vec![col],
        names: vec!["x".to_string()],
        selection: SelectionVector::Bitmap(sel),
        len: 3,
    };

    struct OneBatch { batch: Option<ColumnBatch> }
    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
            Ok(self.batch.take())
        }
        fn schema(&self) -> &BatchSchema {
            &BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] }
        }
        fn close(&self) {}
    }

    let mut adapter = BatchToRowAdapter::new(Box::new(OneBatch { batch: Some(batch) }));
    let r1 = adapter.next().unwrap().unwrap();
    assert_eq!(r1.to_variables()["x"], crate::common::types::Value::Int(10));
    let r2 = adapter.next().unwrap().unwrap();
    assert_eq!(r2.to_variables()["x"], crate::common::types::Value::Int(30)); // skipped 20
    assert!(adapter.next().unwrap().is_none());
}
```

#### 1b. Run tests (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 1c. Write implementation

```rust
// Append to src/execution/batch.rs

use crate::execution::stream::{Record, RecordStream};
use crate::common::types::{Value, Variables};
use ordered_float::OrderedFloat;

/// Converts a BatchStream into a RecordStream by materializing each batch
/// into individual Records. Used to bridge batch operators with unconverted
/// row-based downstream operators.
pub struct BatchToRowAdapter {
    source: Box<dyn BatchStream>,
    current_batch: Option<ColumnBatch>,
    row_idx: usize,
}

impl BatchToRowAdapter {
    pub fn new(source: Box<dyn BatchStream>) -> Self {
        Self {
            source,
            current_batch: None,
            row_idx: 0,
        }
    }

    /// Extract a single Value from a TypedColumn at the given row index.
    fn extract_value(col: &TypedColumn, row: usize) -> Value {
        match col {
            TypedColumn::Int32 { data, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                Value::Int(data[row])
            }
            TypedColumn::Float32 { data, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                Value::Float(OrderedFloat::from(data[row]))
            }
            TypedColumn::Boolean { data, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                Value::Boolean(data.is_set(row))
            }
            TypedColumn::Utf8 { data, offsets, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let bytes = &data[start..end];
                Value::String(String::from_utf8_lossy(bytes).into_owned())
            }
            TypedColumn::DateTime { data, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                // Epoch seconds stored as i64
                use chrono::{TimeZone, Utc};
                let ts = data[row];
                let secs = ts / 1_000_000;
                let nanos = ((ts % 1_000_000) * 1000) as u32;
                let dt = Utc.timestamp(secs, nanos).with_timezone(&chrono::FixedOffset::east(0));
                Value::DateTime(dt)
            }
            TypedColumn::Mixed { data, null, missing } => {
                if !missing.is_set(row) { return Value::Missing; }
                if !null.is_set(row) { return Value::Null; }
                data[row].clone()
            }
        }
    }
}

impl RecordStream for BatchToRowAdapter {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        loop {
            // If we have a current batch, try to emit the next active row
            if let Some(ref batch) = self.current_batch {
                while self.row_idx < batch.len {
                    let row = self.row_idx;
                    self.row_idx += 1;

                    if !batch.selection.is_active(row, batch.len) {
                        continue;
                    }

                    // Build a Record from this row
                    let mut vars = Variables::with_capacity(batch.columns.len());
                    for (i, col) in batch.columns.iter().enumerate() {
                        let val = Self::extract_value(col, row);
                        vars.insert(batch.names[i].clone(), val);
                    }
                    return Ok(Some(Record::new_with_variables(vars)));
                }
                // Exhausted current batch
                self.current_batch = None;
            }

            // Pull next batch
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current_batch = Some(batch);
                    self.row_idx = 0;
                }
                None => return Ok(None),
            }
        }
    }

    fn close(&self) {
        self.source.close();
    }
}
```

#### 1d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 1e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchToRowAdapter converting BatchStream to RecordStream"
```

---

### Step 2: Implement RowToBatchAdapter

**File**: `src/execution/batch.rs` (append)

#### 2a. Write failing tests

```rust
#[test]
fn test_row_to_batch_adapter() {
    use crate::execution::stream::{Record, InMemoryStream};
    use std::collections::VecDeque;

    let records = vec![
        Record::new_with_variables({
            let mut v = Variables::new();
            v.insert("x".to_string(), Value::Int(1));
            v.insert("y".to_string(), Value::String("hello".to_string()));
            v
        }),
        Record::new_with_variables({
            let mut v = Variables::new();
            v.insert("x".to_string(), Value::Int(2));
            v.insert("y".to_string(), Value::String("world".to_string()));
            v
        }),
    ];
    let source = InMemoryStream::new(VecDeque::from(records));
    let schema = BatchSchema {
        names: vec!["x".to_string(), "y".to_string()],
        types: vec![ColumnType::Int32, ColumnType::Utf8],
    };
    let mut adapter = RowToBatchAdapter::new(Box::new(source), schema);
    let batch = adapter.next_batch().unwrap().unwrap();
    assert_eq!(batch.len, 2);
    assert_eq!(batch.columns.len(), 2);
    match &batch.columns[0] {
        TypedColumn::Int32 { data, .. } => {
            assert_eq!(data[0], 1);
            assert_eq!(data[1], 2);
        }
        _ => panic!("expected Int32"),
    }
    // Second call should return None
    assert!(adapter.next_batch().unwrap().is_none());
}
```

#### 2b. Write implementation

```rust
// Append to src/execution/batch.rs

use crate::simd::padded_vec::PaddedVecBuilder;

/// Converts a RecordStream into a BatchStream by buffering up to BATCH_SIZE
/// rows and packing them into columnar ColumnBatches.
pub struct RowToBatchAdapter {
    source: Box<dyn RecordStream>,
    schema: BatchSchema,
    done: bool,
}

impl RowToBatchAdapter {
    pub fn new(source: Box<dyn RecordStream>, schema: BatchSchema) -> Self {
        Self { source, schema, done: false }
    }
}

impl BatchStream for RowToBatchAdapter {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }

        // Collect up to BATCH_SIZE records
        let mut records = Vec::with_capacity(BATCH_SIZE);
        while records.len() < BATCH_SIZE {
            match self.source.next()? {
                Some(record) => records.push(record),
                None => { self.done = true; break; }
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        let len = records.len();
        let mut columns = Vec::with_capacity(self.schema.names.len());

        for (col_idx, col_type) in self.schema.types.iter().enumerate() {
            let col_name = &self.schema.names[col_idx];
            let col = match col_type {
                ColumnType::Int32 => {
                    let mut data = Vec::with_capacity(len);
                    let mut null_bm = Bitmap::all_set(len);
                    let mut missing_bm = Bitmap::all_set(len);
                    for (row, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::Int(v)) => data.push(*v),
                            Some(Value::Null) => { data.push(0); null_bm.unset(row); }
                            Some(Value::Missing) | None => { data.push(0); missing_bm.unset(row); }
                            _ => data.push(0), // type mismatch fallback
                        }
                    }
                    TypedColumn::Int32 {
                        data: PaddedVec::from_vec(data),
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
                ColumnType::Float32 => {
                    let mut data = Vec::with_capacity(len);
                    let mut null_bm = Bitmap::all_set(len);
                    let mut missing_bm = Bitmap::all_set(len);
                    for (row, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::Float(v)) => data.push(v.into_inner()),
                            Some(Value::Null) => { data.push(0.0); null_bm.unset(row); }
                            Some(Value::Missing) | None => { data.push(0.0); missing_bm.unset(row); }
                            _ => data.push(0.0),
                        }
                    }
                    TypedColumn::Float32 {
                        data: PaddedVec::from_vec(data),
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
                ColumnType::Utf8 => {
                    let mut data_builder = PaddedVecBuilder::<u8>::new();
                    let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(len + 1);
                    offsets_builder.push(0);
                    let mut null_bm = Bitmap::all_set(len);
                    let mut missing_bm = Bitmap::all_set(len);
                    for (row, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::String(s)) => {
                                data_builder.extend_from_slice(s.as_bytes());
                            }
                            Some(Value::Null) => { null_bm.unset(row); }
                            Some(Value::Missing) | None => { missing_bm.unset(row); }
                            _ => {} // type mismatch
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
                ColumnType::Mixed | ColumnType::Boolean | ColumnType::DateTime => {
                    // Fallback: store as Mixed
                    let mut data = Vec::with_capacity(len);
                    let null_bm = Bitmap::all_set(len);
                    let missing_bm = Bitmap::all_set(len);
                    for record in &records {
                        let val = record.to_variables().get(col_name)
                            .cloned().unwrap_or(Value::Missing);
                        data.push(val);
                    }
                    TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
                }
            };
            columns.push(col);
        }

        Ok(Some(ColumnBatch {
            columns,
            names: self.schema.names.clone(),
            selection: SelectionVector::All,
            len,
        }))
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    fn close(&self) {
        self.source.close();
    }
}
```

#### 2c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 2d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add RowToBatchAdapter converting RecordStream to BatchStream"
```

---

### Step 3: Adapter round-trip integration test

**File**: `src/execution/batch.rs` (append to tests)

#### 3a. Write test

```rust
#[test]
fn test_adapter_round_trip() {
    use crate::execution::stream::{Record, RecordStream, InMemoryStream};
    use std::collections::VecDeque;

    // Create 3 Records
    let records: Vec<Record> = (0..3).map(|i| {
        let mut v = Variables::new();
        v.insert("id".to_string(), Value::Int(i));
        v.insert("name".to_string(), Value::String(format!("item_{}", i)));
        Record::new_with_variables(v)
    }).collect();

    // Row -> Batch -> Row round trip
    let source = InMemoryStream::new(VecDeque::from(records));
    let schema = BatchSchema {
        names: vec!["id".to_string(), "name".to_string()],
        types: vec![ColumnType::Int32, ColumnType::Utf8],
    };
    let batch_stream = RowToBatchAdapter::new(Box::new(source), schema);
    let mut row_stream = BatchToRowAdapter::new(Box::new(batch_stream));

    for i in 0..3 {
        let record = row_stream.next().unwrap().unwrap();
        assert_eq!(record.to_variables()["id"], Value::Int(i));
        assert_eq!(record.to_variables()["name"], Value::String(format!("item_{}", i)));
    }
    assert!(row_stream.next().unwrap().is_none());
}
```

#### 3b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_adapter_round_trip
```

#### 3c. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add adapter round-trip integration test"
```

---

## Phase 3: Batch Operators

### Step 4: Implement LogSchema

**File (create)**: `src/execution/log_schema.rs`

This type abstracts the schema information for structured log formats so `BatchScanOperator` doesn't need to know about specific log formats.

#### 4a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datasource::DataType;

    #[test]
    fn test_log_schema_elb() {
        let schema = LogSchema::from_format("elb");
        assert_eq!(schema.field_count(), 17);
        assert_eq!(schema.field_name(0), "timestamp");
        assert_eq!(schema.field_type(7), DataType::String); // elb_status_code
    }

    #[test]
    fn test_field_index_by_name() {
        let schema = LogSchema::from_format("elb");
        assert_eq!(schema.field_index("elb_status_code"), Some(7));
        assert_eq!(schema.field_index("nonexistent"), None);
    }

    #[test]
    fn test_is_mixed_type() {
        let schema = LogSchema::from_format("elb");
        // Host fields at indices 2, 3
        assert!(schema.is_mixed_type(2)); // client_and_port (Host)
        assert!(schema.is_mixed_type(3)); // backend_and_port (Host)
        assert!(schema.is_mixed_type(11)); // request (HttpRequest)
        assert!(!schema.is_mixed_type(0)); // timestamp (DateTime)
        assert!(!schema.is_mixed_type(7)); // elb_status_code (String)
    }
}
```

#### 4b. Write implementation

```rust
// src/execution/log_schema.rs

use crate::execution::datasource::{
    DataType, LogFormat,
    ClassicLoadBalancerLogField, ApplicationLoadBalancerLogField,
    S3Field, SquidLogField,
};

/// Schema information for a structured log format.
/// Provides field names, types, and lookup methods.
pub(crate) struct LogSchema {
    field_names: &'static Vec<String>,
    datatypes: &'static Vec<DataType>,
    field_count: usize,
}

impl LogSchema {
    pub fn from_format(format_str: &str) -> Self {
        let format = LogFormat::from_str(format_str);
        let (field_names, datatypes, field_count) = format.field_info();
        Self { field_names, datatypes, field_count }
    }

    pub fn field_count(&self) -> usize {
        self.field_count
    }

    pub fn field_name(&self, idx: usize) -> &str {
        &self.field_names[idx]
    }

    pub fn field_type(&self, idx: usize) -> DataType {
        self.datatypes[idx].clone()
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.field_names.iter().position(|n| n == name)
    }

    /// Returns true if this field type requires scalar per-row parsing
    /// (Host, HttpRequest) and cannot be used in pushed-down predicates.
    pub fn is_mixed_type(&self, idx: usize) -> bool {
        matches!(self.datatypes[idx], DataType::Host | DataType::HttpRequest)
    }

    pub fn field_names(&self) -> &[String] {
        self.field_names
    }

    pub fn datatypes(&self) -> &[DataType] {
        self.datatypes
    }
}
```

Add `pub mod log_schema;` to `src/execution/mod.rs`.

Note: `LogFormat`, `DataType`, and `field_info()` are currently `pub(crate)` or private. You may need to adjust visibility of `LogFormat::from_str` and `LogFormat::field_info` to `pub(crate)` if not already.

#### 4c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::log_schema
```

#### 4d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add LogSchema for structured log format metadata"
```

---

### Step 5: Implement batch tokenizer

**File (create)**: `src/execution/batch_tokenizer.rs`

#### 5a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_line_simple() {
        let line = b"2024-01-01 hello world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        assert_eq!(&line[fields[0].0..fields[0].1], b"2024-01-01");
        assert_eq!(&line[fields[1].0..fields[1].1], b"hello");
        assert_eq!(&line[fields[2].0..fields[2].1], b"world");
    }

    #[test]
    fn test_tokenize_line_quoted() {
        let line = b"hello \"quoted string\" world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        // Quoted field includes the quotes
        assert_eq!(&line[fields[1].0..fields[1].1], b"\"quoted string\"");
    }

    #[test]
    fn test_tokenize_line_bracket() {
        let line = b"hello [bracketed] world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        assert_eq!(&line[fields[1].0..fields[1].1], b"[bracketed]");
    }

    #[test]
    fn test_tokenize_batch() {
        let lines: Vec<Vec<u8>> = vec![
            b"a b c".to_vec(),
            b"d e f".to_vec(),
        ];
        let result = tokenize_batch_lines(&lines);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[1].len(), 3);
    }
}
```

#### 5b. Write implementation

```rust
// src/execution/batch_tokenizer.rs

/// Tokenize a single line into field byte ranges (start, end).
/// Mirrors the existing LogTokenizer but works on &[u8] and returns
/// byte offsets instead of string slices.
pub(crate) fn tokenize_line(line: &[u8]) -> Vec<(usize, usize)> {
    let mut fields = Vec::with_capacity(20);
    let mut pos = 0;
    let len = line.len();

    while pos < len {
        // Skip whitespace
        while pos < len && line[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        match line[pos] {
            b'"' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'"' {
                    pos += 1;
                }
                if pos < len { pos += 1; } // skip closing quote
                fields.push((start, pos));
            }
            b'\'' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'\'' {
                    pos += 1;
                }
                if pos < len { pos += 1; }
                fields.push((start, pos));
            }
            b'[' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b']' {
                    pos += 1;
                }
                if pos < len { pos += 1; }
                fields.push((start, pos));
            }
            _ => {
                let start = pos;
                while pos < len {
                    match line[pos] {
                        b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b'[' | b']' => break,
                        _ => pos += 1,
                    }
                }
                fields.push((start, pos));
            }
        }
    }

    fields
}

/// Tokenize a batch of lines, returning field byte ranges for each line.
pub(crate) fn tokenize_batch_lines(lines: &[Vec<u8>]) -> Vec<Vec<(usize, usize)>> {
    lines.iter().map(|line| tokenize_line(line)).collect()
}
```

Add `pub mod batch_tokenizer;` to `src/execution/mod.rs`.

#### 5c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_tokenizer
```

#### 5d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add batch tokenizer for byte-level field extraction"
```

---

### Step 6: Implement field parsing into TypedColumns

**File (create)**: `src/execution/field_parser.rs`

#### 6a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datasource::DataType;
    use crate::simd::selection::SelectionVector;

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
        let mut sel_bm = crate::simd::bitmap::Bitmap::all_unset(3);
        sel_bm.set(0);
        sel_bm.set(2);
        let sel = SelectionVector::Bitmap(sel_bm);
        let col = parse_field_column_selected(&lines, &fields, 0, &DataType::String, &sel);
        match col {
            TypedColumn::Utf8 { data, offsets, .. } => {
                // Only active rows parsed, but offsets still have entries for all rows
                assert_eq!(offsets.len(), 4); // 3 rows + 1
                let s0 = &data[offsets[0] as usize..offsets[1] as usize];
                assert_eq!(s0, b"aaa");
                // row 1 is inactive, offset[1] == offset[2]
                assert_eq!(offsets[1], offsets[2]);
                let s2 = &data[offsets[2] as usize..offsets[3] as usize];
                assert_eq!(s2, b"ccc");
            }
            _ => panic!("expected Utf8"),
        }
    }
}
```

#### 6b. Write implementation

```rust
// src/execution/field_parser.rs

use crate::execution::batch::TypedColumn;
use crate::execution::datasource::DataType;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
use crate::simd::selection::SelectionVector;
use crate::common::types::Value;
use ordered_float::OrderedFloat;

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
                    // Strip surrounding quotes if present
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
            // Store as Mixed for now — DateTime parsing is complex
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("");
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
        // For non-string types, delegate to parse_field_column
        // (the selection filtering is less critical for numeric types)
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
```

Add `pub mod field_parser;` to `src/execution/mod.rs`.

Note: `parse_utc_timestamp`, `parse_host`, and `parse_http_request` need to be accessible. Check their visibility — they may need `pub(crate)`.

#### 6c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::field_parser
```

#### 6d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add field parser for converting tokenized fields into TypedColumns"
```

---

### Step 7: Implement BatchScanOperator

**File (create)**: `src/execution/batch_scan.rs`

#### 7a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::{BatchStream, ColumnBatch, TypedColumn};
    use crate::execution::datasource::ReaderBuilder;
    use std::path::PathBuf;

    #[test]
    fn test_batch_scan_reads_elb() {
        let path = PathBuf::from("data/AWSELB.log");
        if !path.exists() { return; } // skip if no test data

        let reader = ReaderBuilder::new("elb".to_string()).with_path(&path).unwrap();
        let schema = LogSchema::from_format("elb");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(
            Box::new(reader),
            schema,
            all_fields.clone(), // project all
            None,               // no pushed predicate
            vec![],             // no residual fields
        );

        let batch = scan.next_batch().unwrap();
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert!(batch.len > 0);
        assert!(batch.len <= crate::execution::batch::BATCH_SIZE);
        assert_eq!(batch.columns.len(), 17); // ELB has 17 fields
    }

    #[test]
    fn test_batch_scan_emits_multiple_batches() {
        let path = PathBuf::from("data/AWSELB.log");
        if !path.exists() { return; }

        let reader = ReaderBuilder::new("elb".to_string()).with_path(&path).unwrap();
        let schema = LogSchema::from_format("elb");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let mut scan = BatchScanOperator::new(
            Box::new(reader), schema, all_fields, None, vec![],
        );

        let mut total_rows = 0;
        while let Some(batch) = scan.next_batch().unwrap() {
            total_rows += batch.len;
        }
        assert!(total_rows > 0);
    }
}
```

#### 7b. Write implementation

```rust
// src/execution/batch_scan.rs

use std::io::{self, BufRead};
use crate::execution::batch::*;
use crate::execution::batch_tokenizer::tokenize_line;
use crate::execution::field_parser::{parse_field_column, parse_field_column_selected};
use crate::execution::log_schema::LogSchema;
use crate::execution::datasource::RecordRead;
use crate::execution::types::StreamResult;
use crate::simd::selection::SelectionVector;

/// Batch scan operator with optional two-phase lazy parsing.
/// Phase 1: Tokenize + parse filter fields + evaluate pushed predicate.
/// Phase 2: Parse projected + residual fields for surviving rows only.
pub(crate) struct BatchScanOperator {
    reader: Box<dyn io::BufRead>,
    schema: LogSchema,
    projected_fields: Vec<usize>,
    pushed_predicate: Option<PushedPredicate>,
    residual_filter_fields: Vec<usize>,
    done: bool,
    buf: String,
}

pub(crate) struct PushedPredicate {
    pub filter_field_indices: Vec<usize>,
    pub predicate: Box<crate::execution::types::Formula>,
}

impl BatchScanOperator {
    pub fn new(
        reader: Box<dyn io::BufRead>,
        schema: LogSchema,
        projected_fields: Vec<usize>,
        pushed_predicate: Option<PushedPredicate>,
        residual_filter_fields: Vec<usize>,
    ) -> Self {
        Self {
            reader,
            schema,
            projected_fields,
            pushed_predicate,
            residual_filter_fields,
            done: false,
            buf: String::with_capacity(512),
        }
    }

    /// Read up to BATCH_SIZE lines from the reader.
    fn read_lines(&mut self) -> Vec<Vec<u8>> {
        let mut lines = Vec::with_capacity(BATCH_SIZE);
        while lines.len() < BATCH_SIZE {
            self.buf.clear();
            match self.reader.read_line(&mut self.buf) {
                Ok(0) => { self.done = true; break; }
                Ok(_) => {
                    // Trim trailing newline
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

        // Phase 1: Tokenize all lines
        let line_fields: Vec<Vec<(usize, usize)>> = lines.iter()
            .map(|line| tokenize_line(line))
            .collect();

        // Determine which fields to parse
        // For now (no predicate pushdown wired yet), parse all projected fields
        let mut all_field_indices: Vec<usize> = self.projected_fields.clone();
        for &idx in &self.residual_filter_fields {
            if !all_field_indices.contains(&idx) {
                all_field_indices.push(idx);
            }
        }
        all_field_indices.sort();
        all_field_indices.dedup();

        // Parse columns
        let mut columns = Vec::with_capacity(all_field_indices.len());
        let mut names = Vec::with_capacity(all_field_indices.len());
        for &field_idx in &all_field_indices {
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
        // TODO: cache this
        &BatchSchema {
            names: self.projected_fields.iter()
                .map(|&i| self.schema.field_name(i).to_string())
                .collect(),
            types: self.projected_fields.iter()
                .map(|&i| datatype_to_column_type(&self.schema.field_type(i)))
                .collect(),
        }
    }

    fn close(&self) {}
}

fn datatype_to_column_type(dt: &crate::execution::datasource::DataType) -> ColumnType {
    match dt {
        crate::execution::datasource::DataType::String => ColumnType::Utf8,
        crate::execution::datasource::DataType::Integral => ColumnType::Int32,
        crate::execution::datasource::DataType::Float => ColumnType::Float32,
        crate::execution::datasource::DataType::DateTime => ColumnType::Mixed,
        crate::execution::datasource::DataType::Host => ColumnType::Mixed,
        crate::execution::datasource::DataType::HttpRequest => ColumnType::Mixed,
    }
}
```

Add `pub mod batch_scan;` to `src/execution/mod.rs`.

Note: The `schema()` method returns a reference to `BatchSchema` but needs to own the data. This will need adjustment — either store a `BatchSchema` field on the struct or change the trait signature. The implementer should resolve this.

#### 7c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_scan
```

#### 7d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchScanOperator with columnar field parsing"
```

---

### Step 8: Implement batch predicate evaluation

**File (create)**: `src/execution/batch_predicate.rs`

This evaluates a physical `Formula` against `ColumnBatch` columns, producing a `Bitmap` result.

#### 8a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::selection::SelectionVector;
    use crate::execution::types::{Formula, Expression, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::common::types::Value;

    fn make_test_batch() -> ColumnBatch {
        // 4 rows: status = ["200", "404", "200", "500"]
        let mut data_builder = crate::simd::padded_vec::PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = crate::simd::padded_vec::PaddedVecBuilder::<u32>::with_capacity(5);
        offsets_builder.push(0);
        for s in &["200", "404", "200", "500"] {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        let col = TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        ColumnBatch {
            columns: vec![col],
            names: vec!["status".to_string()],
            selection: SelectionVector::All,
            len: 4,
        }
    }

    #[test]
    fn test_evaluate_string_equality() {
        let batch = make_test_batch();
        // status = '200'
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        );
        let result = evaluate_batch_predicate(&formula, &batch, &Default::default(), &crate::functions::register_all().unwrap()).unwrap();
        assert_eq!(result.count_ones(), 2); // rows 0 and 2
        assert!(result.is_set(0));
        assert!(!result.is_set(1));
        assert!(result.is_set(2));
        assert!(!result.is_set(3));
    }
}
```

#### 8b. Write implementation

```rust
// src/execution/batch_predicate.rs

use crate::execution::batch::*;
use crate::execution::types::{Formula, Expression, Relation, StreamResult, StreamError};
use crate::simd::bitmap::Bitmap;
use crate::simd::kernels;
use crate::simd::filter_cache::evaluate_cached_two_pass;
use crate::common::types::{Value, Variables};
use crate::functions::FunctionRegistry;
use std::sync::Arc;
use crate::syntax::ast::{PathExpr, PathSegment};

/// Evaluate a physical Formula against a ColumnBatch, returning a Bitmap
/// of rows where the predicate is true.
pub(crate) fn evaluate_batch_predicate(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    match formula {
        Formula::Predicate(relation, left, right) => {
            evaluate_comparison(relation, left, right, batch, variables, registry)
        }
        Formula::And(left, right) => {
            let left_bm = evaluate_batch_predicate(left, batch, variables, registry)?;
            let right_bm = evaluate_batch_predicate(right, batch, variables, registry)?;
            Ok(left_bm.and(&right_bm))
        }
        Formula::Or(left, right) => {
            let left_bm = evaluate_batch_predicate(left, batch, variables, registry)?;
            let right_bm = evaluate_batch_predicate(right, batch, variables, registry)?;
            Ok(left_bm.or(&right_bm))
        }
        Formula::Not(inner) => {
            let bm = evaluate_batch_predicate(inner, batch, variables, registry)?;
            Ok(bm.not(batch.len))
        }
        Formula::IsNull(expr) => {
            evaluate_is_null(expr, batch, variables)
        }
        Formula::IsNotNull(expr) => {
            let bm = evaluate_is_null(expr, batch, variables)?;
            Ok(bm.not(batch.len))
        }
        // Fallback: evaluate row-by-row using scalar logic
        _ => {
            evaluate_scalar_fallback(formula, batch, variables, registry)
        }
    }
}

fn evaluate_comparison(
    relation: &Relation,
    left: &Expression,
    right: &Expression,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    // Fast path: Variable op Constant (most common filter pattern)
    if let (Expression::Variable(path), Expression::Constant(const_val)) = (left, right) {
        if let Some(col_name) = single_attr_name(path) {
            if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
                return evaluate_column_vs_constant(
                    &batch.columns[col_idx], relation, const_val, batch.len,
                );
            }
        }
    }
    // Also handle Constant op Variable (reversed)
    if let (Expression::Constant(const_val), Expression::Variable(path)) = (left, right) {
        if let Some(col_name) = single_attr_name(path) {
            if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
                let flipped = flip_relation(relation);
                return evaluate_column_vs_constant(
                    &batch.columns[col_idx], &flipped, const_val, batch.len,
                );
            }
        }
    }
    // Fallback to scalar
    evaluate_scalar_fallback(
        &Formula::Predicate(relation.clone(), Box::new(left.clone()), Box::new(right.clone())),
        batch, variables, registry,
    )
}

fn evaluate_column_vs_constant(
    col: &TypedColumn,
    relation: &Relation,
    constant: &Value,
    len: usize,
) -> StreamResult<Bitmap> {
    match (col, constant) {
        // String equality — use SIMD kernel with filter cache
        (TypedColumn::Utf8 { data, offsets, null, missing }, Value::String(needle))
            if matches!(relation, Relation::Equal) =>
        {
            let needle_bytes = needle.as_bytes();
            let bm = evaluate_cached_two_pass(
                data, offsets, &|field| field == needle_bytes, len,
            );
            // AND with non-null/non-missing mask
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // Int32 comparisons — use SIMD kernels
        (TypedColumn::Int32 { data, null, missing }, Value::Int(threshold)) => {
            let mut result_bytes = vec![0u8; len];
            match relation {
                Relation::Equal => kernels::filter_eq_i32(data, *threshold, &mut result_bytes),
                Relation::MoreThan => kernels::filter_gt_i32(data, *threshold, &mut result_bytes),
                Relation::LessThan => kernels::filter_lt_i32(data, *threshold, &mut result_bytes),
                Relation::GreaterEqual => kernels::filter_ge_i32(data, *threshold, &mut result_bytes),
                Relation::LessEqual => kernels::filter_le_i32(data, *threshold, &mut result_bytes),
                Relation::NotEqual => {
                    kernels::filter_eq_i32(data, *threshold, &mut result_bytes);
                    for b in result_bytes.iter_mut() { *b = 1 - *b; }
                }
            }
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // Float comparisons
        (TypedColumn::Float32 { data, null, missing }, Value::Float(threshold)) => {
            let t = threshold.into_inner();
            let mut result_bytes = vec![0u8; len];
            match relation {
                Relation::Equal => kernels::filter_eq_f32(data, t, &mut result_bytes),
                Relation::GreaterEqual => kernels::filter_ge_f32(data, t, &mut result_bytes),
                _ => {
                    // Simple scalar fallback for other float relations
                    for i in 0..len {
                        result_bytes[i] = match relation {
                            Relation::MoreThan => (data[i] > t) as u8,
                            Relation::LessThan => (data[i] < t) as u8,
                            Relation::LessEqual => (data[i] <= t) as u8,
                            Relation::NotEqual => (data[i] != t) as u8,
                            _ => unreachable!(),
                        };
                    }
                }
            }
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // Fallback for other type combinations
        _ => {
            // Row-by-row evaluation
            let mut result = Bitmap::all_unset(len);
            for row in 0..len {
                let val = crate::execution::batch::BatchToRowAdapter::extract_value(col, row);
                let matches = Value::compare_with_relation(&val, constant, relation);
                if matches == Some(true) {
                    result.set(row);
                }
            }
            Ok(result)
        }
    }
}

fn evaluate_is_null(
    expr: &Expression,
    batch: &ColumnBatch,
    _variables: &Variables,
) -> StreamResult<Bitmap> {
    if let Some(col_name) = expr_to_column_name(expr) {
        if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
            let (null, missing) = get_null_missing_bitmaps(&batch.columns[col_idx]);
            // IS NULL = missing bit 1 AND null bit 0
            return Ok(missing.and(&null.not(batch.len)));
        }
    }
    // Fallback: all false
    Ok(Bitmap::all_unset(batch.len))
}

fn evaluate_scalar_fallback(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    let mut result = Bitmap::all_unset(batch.len);
    for row in 0..batch.len {
        let mut row_vars = variables.clone();
        for (i, col) in batch.columns.iter().enumerate() {
            let val = crate::execution::batch::BatchToRowAdapter::extract_value(col, row);
            row_vars.insert(batch.names[i].clone(), val);
        }
        match formula.evaluate(&row_vars, registry) {
            Ok(Some(true)) => result.set(row),
            _ => {}
        }
    }
    Ok(result)
}

fn single_attr_name(path: &PathExpr) -> Option<&str> {
    let segments = path.segments();
    if segments.len() == 1 {
        if let PathSegment::AttrName(name) = &segments[0] {
            return Some(name);
        }
    }
    None
}

fn expr_to_column_name(expr: &Expression) -> Option<&str> {
    if let Expression::Variable(path) = expr {
        single_attr_name(path)
    } else {
        None
    }
}

fn flip_relation(r: &Relation) -> Relation {
    match r {
        Relation::Equal => Relation::Equal,
        Relation::NotEqual => Relation::NotEqual,
        Relation::MoreThan => Relation::LessThan,
        Relation::LessThan => Relation::MoreThan,
        Relation::GreaterEqual => Relation::LessEqual,
        Relation::LessEqual => Relation::GreaterEqual,
    }
}

fn get_null_missing_bitmaps(col: &TypedColumn) -> (&Bitmap, &Bitmap) {
    match col {
        TypedColumn::Int32 { null, missing, .. } => (null, missing),
        TypedColumn::Float32 { null, missing, .. } => (null, missing),
        TypedColumn::Boolean { null, missing, .. } => (null, missing),
        TypedColumn::Utf8 { null, missing, .. } => (null, missing),
        TypedColumn::DateTime { null, missing, .. } => (null, missing),
        TypedColumn::Mixed { null, missing, .. } => (null, missing),
    }
}
```

Add `pub mod batch_predicate;` to `src/execution/mod.rs`.

Note: The physical `Formula` enum (in `execution/types.rs`) uses `And`/`Or`/`Not` variant names, NOT `InfixOperator`/`PrefixOperator` like the logical Formula. The implementer should verify the exact variant names. Also, `Value::compare_with_relation` may not exist — the implementer may need to implement the comparison inline using `common::types::compare_values`.

#### 8c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_predicate
```

#### 8d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add batch predicate evaluation with SIMD kernel dispatch"
```

---

### Step 9: Implement BatchFilterOperator

**File (create)**: `src/execution/batch_filter.rs`

#### 9a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVecBuilder;
    use crate::simd::selection::SelectionVector;

    #[test]
    fn test_batch_filter_narrows_selection() {
        // Create a batch with status column: ["200", "404", "200", "500"]
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(5);
        offsets_builder.push(0);
        for s in &["200", "404", "200", "500"] {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        let col = TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["status".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };

        struct OneBatch { batch: Option<ColumnBatch> }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema {
                &BatchSchema { names: vec!["status".to_string()], types: vec![ColumnType::Utf8] }
            }
            fn close(&self) {}
        }

        use crate::execution::types::{Formula, Expression, Relation};
        use crate::syntax::ast::{PathExpr, PathSegment};
        use crate::common::types::Value;

        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        );

        let registry = std::sync::Arc::new(crate::functions::register_all().unwrap());
        let mut filter = BatchFilterOperator::new(
            Box::new(OneBatch { batch: Some(batch) }),
            formula,
            Default::default(),
            registry,
        );

        let result = filter.next_batch().unwrap().unwrap();
        // Selection should show only rows 0 and 2 active
        assert_eq!(result.selection.count_active(result.len), 2);
        assert!(result.selection.is_active(0, result.len));
        assert!(!result.selection.is_active(1, result.len));
        assert!(result.selection.is_active(2, result.len));
    }
}
```

#### 9b. Write implementation

```rust
// src/execution/batch_filter.rs

use crate::execution::batch::*;
use crate::execution::batch_predicate::evaluate_batch_predicate;
use crate::execution::types::{Formula, StreamResult};
use crate::simd::selection::SelectionVector;
use crate::common::types::Variables;
use crate::functions::FunctionRegistry;
use std::sync::Arc;

pub(crate) struct BatchFilterOperator {
    child: Box<dyn BatchStream>,
    formula: Formula,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
}

impl BatchFilterOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        formula: Formula,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        Self { child, formula, variables, registry }
    }
}

impl BatchStream for BatchFilterOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(mut batch) = self.child.next_batch()? {
            let result_bm = evaluate_batch_predicate(
                &self.formula, &batch, &self.variables, &self.registry,
            )?;

            // Combine with existing selection
            let new_selection = match batch.selection {
                SelectionVector::All => SelectionVector::Bitmap(result_bm),
                SelectionVector::Bitmap(ref existing) => {
                    SelectionVector::Bitmap(existing.and(&result_bm))
                }
            };

            batch.selection = new_selection;

            if batch.selection.any_active(batch.len) {
                return Ok(Some(batch));
            }
            // All rows filtered — skip batch, pull next
        }
        Ok(None)
    }

    fn schema(&self) -> &BatchSchema {
        self.child.schema()
    }

    fn close(&self) {
        self.child.close();
    }
}
```

Add `pub mod batch_filter;` to `src/execution/mod.rs`.

#### 9c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_filter
```

#### 9d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchFilterOperator with SIMD predicate evaluation"
```

---

### Step 10: Implement BatchProjectOperator

**File (create)**: `src/execution/batch_project.rs`

#### 10a. Write test

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::selection::SelectionVector;

    #[test]
    fn test_project_selects_columns() {
        let col_a = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![1, 2, 3]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let col_b = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col_a, col_b],
            names: vec!["a".to_string(), "b".to_string()],
            selection: SelectionVector::All,
            len: 3,
        };

        struct OneBatch { batch: Option<ColumnBatch> }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema {
                &BatchSchema {
                    names: vec!["a".to_string(), "b".to_string()],
                    types: vec![ColumnType::Int32, ColumnType::Int32],
                }
            }
            fn close(&self) {}
        }

        // Project only column "b"
        let mut proj = BatchProjectOperator::new(
            Box::new(OneBatch { batch: Some(batch) }),
            vec!["b".to_string()],
        );
        let result = proj.next_batch().unwrap().unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.names, vec!["b".to_string()]);
    }
}
```

#### 10b. Write implementation

```rust
// src/execution/batch_project.rs

use crate::execution::batch::*;
use crate::execution::types::StreamResult;

/// Projects (selects) a subset of columns from a ColumnBatch.
/// This is the simple case — column selection only, no expression evaluation.
pub(crate) struct BatchProjectOperator {
    child: Box<dyn BatchStream>,
    output_columns: Vec<String>,
    schema: BatchSchema,
}

impl BatchProjectOperator {
    pub fn new(child: Box<dyn BatchStream>, output_columns: Vec<String>) -> Self {
        let child_schema = child.schema();
        let types: Vec<ColumnType> = output_columns.iter()
            .filter_map(|name| {
                child_schema.names.iter().position(|n| n == name)
                    .map(|i| child_schema.types[i].clone())
            })
            .collect();
        let schema = BatchSchema {
            names: output_columns.clone(),
            types,
        };
        Self { child, output_columns, schema }
    }
}

impl BatchStream for BatchProjectOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        match self.child.next_batch()? {
            Some(batch) => {
                let mut new_columns = Vec::with_capacity(self.output_columns.len());
                let mut new_names = Vec::with_capacity(self.output_columns.len());

                for output_name in &self.output_columns {
                    if let Some(idx) = batch.names.iter().position(|n| n == output_name) {
                        // Move the column out of the batch
                        // Since we can't easily move out of a Vec by index,
                        // we'll need to take ownership. For now, use indices.
                        new_names.push(output_name.clone());
                    }
                }

                // Rebuild with only selected columns
                // We need to consume the batch's columns
                let ColumnBatch { columns, names, selection, len } = batch;
                let mut col_map: Vec<(String, TypedColumn)> = names.into_iter()
                    .zip(columns.into_iter()).collect();

                for output_name in &self.output_columns {
                    if let Some(pos) = col_map.iter().position(|(n, _)| n == output_name) {
                        let (name, col) = col_map.remove(pos);
                        new_columns.push(col);
                        new_names.push(name);
                    }
                }

                Ok(Some(ColumnBatch {
                    columns: new_columns,
                    names: new_names,
                    selection,
                    len,
                }))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    fn close(&self) {
        self.child.close();
    }
}
```

Add `pub mod batch_project;` to `src/execution/mod.rs`.

#### 10c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_project
```

#### 10d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchProjectOperator for column selection"
```

---

### Step 11: Implement BatchLimitOperator

**File (create)**: `src/execution/batch_limit.rs`

#### 11a. Write test

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::selection::SelectionVector;

    #[test]
    fn test_limit_truncates() {
        let batches: Vec<ColumnBatch> = (0..3).map(|i| {
            ColumnBatch {
                columns: vec![TypedColumn::Int32 {
                    data: PaddedVec::from_vec(vec![i; 10]),
                    null: Bitmap::all_set(10),
                    missing: Bitmap::all_set(10),
                }],
                names: vec!["x".to_string()],
                selection: SelectionVector::All,
                len: 10,
            }
        }).collect();

        struct MultiBatch { batches: Vec<ColumnBatch>, idx: usize }
        impl BatchStream for MultiBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                if self.idx < self.batches.len() {
                    self.idx += 1;
                    Ok(Some(self.batches.remove(0)))
                } else {
                    Ok(None)
                }
            }
            fn schema(&self) -> &BatchSchema {
                &BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] }
            }
            fn close(&self) {}
        }

        let mut limit = BatchLimitOperator::new(
            Box::new(MultiBatch { batches, idx: 0 }), 15,
        );

        // First batch: 10 rows, all pass (15 remaining -> 5)
        let b1 = limit.next_batch().unwrap().unwrap();
        assert_eq!(b1.selection.count_active(b1.len), 10);

        // Second batch: only 5 of 10 rows should be active
        let b2 = limit.next_batch().unwrap().unwrap();
        assert_eq!(b2.selection.count_active(b2.len), 5);

        // Third batch: should be None
        assert!(limit.next_batch().unwrap().is_none());
    }
}
```

#### 11b. Write implementation

```rust
// src/execution/batch_limit.rs

use crate::execution::batch::*;
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;

pub(crate) struct BatchLimitOperator {
    child: Box<dyn BatchStream>,
    remaining: usize,
}

impl BatchLimitOperator {
    pub fn new(child: Box<dyn BatchStream>, limit: u32) -> Self {
        Self { child, remaining: limit as usize }
    }
}

impl BatchStream for BatchLimitOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        match self.child.next_batch()? {
            Some(mut batch) => {
                let active = batch.selection.count_active(batch.len);

                if active <= self.remaining {
                    // Entire batch fits within limit
                    self.remaining -= active;
                    Ok(Some(batch))
                } else {
                    // Need to truncate: keep only `self.remaining` active rows
                    let mut keep = self.remaining;
                    let mut bm = batch.selection.to_bitmap(batch.len);
                    for row in 0..batch.len {
                        if bm.is_set(row) {
                            if keep == 0 {
                                bm.unset(row);
                            } else {
                                keep -= 1;
                            }
                        }
                    }
                    batch.selection = SelectionVector::Bitmap(bm);
                    self.remaining = 0;
                    Ok(Some(batch))
                }
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &BatchSchema {
        self.child.schema()
    }

    fn close(&self) {
        self.child.close();
    }
}
```

Add `pub mod batch_limit;` to `src/execution/mod.rs`.

#### 11c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_limit
```

#### 11d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchLimitOperator with selection vector truncation"
```

---

### Step 12: Wire batch operators into Node::get()

**File**: `src/execution/types.rs`

This step modifies the physical plan's `Node::get()` method to produce batch pipelines for DataSource→Filter→Map→Limit chains, falling back to row pipelines via `BatchToRowAdapter` for other operators.

#### 12a. Write integration test

```rust
// Add to existing tests in src/execution/types.rs or create a new test file

#[cfg(test)]
mod batch_integration_tests {
    use super::*;
    use crate::app;
    use std::sync::Arc;

    #[test]
    fn test_batch_pipeline_produces_same_results_as_row() {
        let path = std::path::PathBuf::from("data/AWSELB.log");
        if !path.exists() { return; }

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources = crate::common::types::DataSourceRegistry::new();
        // ... register data source and run both pipelines
        // Compare results row-by-row
    }
}
```

The full implementation of this step requires modifying `Node::get()` to detect when the leaf node is a `DataSource` with supported format, and building a batch pipeline chain (BatchScanOperator → BatchFilterOperator → BatchToRowAdapter) instead of the current row pipeline. The `BatchToRowAdapter` at the end ensures compatibility with unconverted operators.

#### 12b. Implementation sketch

In `Node::get()`, modify `Node::DataSource` case:

```rust
Node::DataSource(data_source, bindings) => match data_source {
    DataSource::File(path, file_format, _table_name) => {
        // Check if we can use batch pipeline
        let format = file_format.as_str();
        if !bindings.is_empty() || format == "jsonl" {
            // Fall back to row pipeline for JSONL and binding cases
            let reader = ReaderBuilder::new(file_format.clone()).with_path(path)?;
            let file_stream = LogFileStream::new(Box::new(reader));
            if !bindings.is_empty() {
                return Ok(Box::new(ProjectionStream::new(Box::new(file_stream), bindings.clone())));
            }
            return Ok(Box::new(file_stream));
        }

        // Use batch scan
        let schema = LogSchema::from_format(format);
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();
        let reader = std::io::BufReader::new(std::fs::File::open(path)?);
        let scan = BatchScanOperator::new(
            Box::new(reader), schema, all_fields, None, vec![],
        );
        // Wrap in adapter for row-based downstream
        Ok(Box::new(BatchToRowAdapter::new(Box::new(scan))))
    }
    // ... stdin case unchanged
}
```

And for `Node::Filter`, detect if child is a batch-capable DataSource and build:
```
BatchScanOperator → BatchFilterOperator → BatchToRowAdapter
```

This is complex enough that the implementer should handle it carefully, testing against all existing queries.

#### 12c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

#### 12d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: wire batch pipeline into Node::get() with BatchToRowAdapter fallback"
```

---

### Step 13: Benchmark comparison

#### 13a. Run benchmarks

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals 2>&1 | tee docs/plans/phase2-3-benchmarks.txt
```

#### 13b. Compare against Phase 0 baseline

Compare `docs/plans/phase2-3-benchmarks.txt` against `docs/plans/phase0-baseline.txt`.

Key metrics to compare:
- `profiling/scan_only`: should show improvement from columnar parsing
- `profiling/scan_filter`: should show major improvement from SIMD filter kernels
- `profiling/scan_filter_groupby`: should show improvement in scan+filter phases

#### 13c. Document results

Create `docs/plans/phase2-3-results.md` with before/after comparison.

#### 13d. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add docs/plans/phase2-3-* && git commit -m "docs: record Phase 2-3 benchmark results"
```

---

### Step 14: End-to-end verification

#### 14a. Run full test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test --all-features
```

Verify all existing tests pass — no regressions from batch pipeline changes.

#### 14b. Run representative queries manually

```bash
cd /Users/paulmeng/Develop/logq
# Scan only
./target/release/logq -f elb "SELECT * FROM elb LIMIT 10" data/AWSELB.log

# Filter
./target/release/logq -f elb "SELECT * FROM elb WHERE elb_status_code = '200' LIMIT 10" data/AWSELB.log

# Filter + GroupBy
./target/release/logq -f elb "SELECT elb_status_code, count(*) FROM elb WHERE elb_status_code = '200' GROUP BY elb_status_code" data/AWSELB.log
```

Verify output matches expectations.

#### 14c. Commit any fixes

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "phase 2-3: end-to-end verification complete"
```
