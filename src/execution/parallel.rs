use memmap2::MmapOptions;
use rayon::prelude::*;
use std::cmp;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use crate::common::types::{Tuple, Value, Variables};
use crate::execution::batch::*;
use crate::execution::batch_scan::BatchScanOperator;
use crate::execution::log_schema::LogSchema;
use crate::execution::types::{
    CountAggregate, SumAggregate, AvgAggregate, MinAggregate, MaxAggregate,
    Formula, StreamResult,
};
use crate::functions::FunctionRegistry;
use ordered_float::OrderedFloat;

pub(crate) const PARALLEL_THRESHOLD: u64 = 16 * 1024 * 1024; // 16MB

pub(crate) enum ScanStrategy {
    Mmap(memmap2::Mmap),
    BufReader(Box<dyn BufRead>),
}

pub(crate) fn choose_strategy(path: &Path) -> ScanStrategy {
    let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);

    if file_size < PARALLEL_THRESHOLD || file_size == 0 {
        return match File::open(path) {
            Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(64 * 1024, f))),
            Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
        };
    }

    #[cfg(target_pointer_width = "64")]
    {
        match File::open(path).and_then(|f| unsafe { MmapOptions::new().map(&f) }) {
            Ok(mmap) => return ScanStrategy::Mmap(mmap),
            Err(_) => {}
        }
    }

    match File::open(path) {
        Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(64 * 1024, f))),
        Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
    }
}

/// Split a byte slice into chunks along newline boundaries.
pub(crate) fn split_chunks(data: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    if data.is_empty() || num_chunks == 0 {
        return vec![];
    }

    let chunk_size = data.len() / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for i in 0..num_chunks {
        if start >= data.len() {
            break;
        }

        if i == num_chunks - 1 {
            chunks.push(&data[start..]);
            break;
        }

        let raw_end = cmp::min(start + chunk_size, data.len());

        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1,
            None => data.len(),
        };

        chunks.push(&data[start..end]);
        start = end;
    }

    while chunks.last().map_or(false, |c| c.is_empty()) {
        chunks.pop();
    }

    chunks
}

fn collect_results<T>(results: Vec<StreamResult<T>>) -> StreamResult<Vec<T>> {
    let mut collected = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Ok(value) => collected.push(value),
            Err(e) => return Err(e),
        }
    }
    Ok(collected)
}

pub(crate) fn parallel_scan_chunks(
    data: &[u8],
    num_threads: usize,
    schema: &LogSchema,
    projected_fields: &[usize],
    filter_field_indices: &[usize],
    pushed_predicate: &Option<(Formula, Variables, Arc<FunctionRegistry>)>,
) -> StreamResult<Vec<Vec<ColumnBatch>>> {
    parallel_scan_chunks_limited(
        data, num_threads, schema, projected_fields,
        filter_field_indices, pushed_predicate, None,
    )
}

/// Like `parallel_scan_chunks` but with an optional row limit.
/// When set, workers stop early once `row_limit` total active rows have been
/// collected across all chunks. This avoids scanning the full file for LIMIT queries.
pub(crate) fn parallel_scan_chunks_limited(
    data: &[u8],
    num_threads: usize,
    schema: &LogSchema,
    projected_fields: &[usize],
    filter_field_indices: &[usize],
    pushed_predicate: &Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    row_limit: Option<usize>,
) -> StreamResult<Vec<Vec<ColumnBatch>>> {
    let chunks = split_chunks(data, num_threads);
    if chunks.is_empty() {
        return Ok(vec![]);
    }

    let global_count = std::sync::atomic::AtomicUsize::new(0);
    let limit = row_limit.unwrap_or(usize::MAX);

    let partial_results: Vec<StreamResult<Vec<ColumnBatch>>> = chunks
        .par_iter()
        .map(|chunk| {
            let reader: Box<dyn BufRead> = Box::new(io::Cursor::new(chunk.to_vec()));
            let mut scanner = BatchScanOperator::new(
                reader,
                schema.clone(),
                projected_fields.to_vec(),
                filter_field_indices.to_vec(),
                pushed_predicate.clone(),
            );
            let mut batches = Vec::new();
            while let Some(batch) = scanner.next_batch()? {
                let active = batch.selection.count_active(batch.len);
                batches.push(batch);
                // Check if we've collectively found enough rows
                let prev = global_count.fetch_add(active, std::sync::atomic::Ordering::Relaxed);
                if prev + active >= limit {
                    break;
                }
            }
            Ok(batches)
        })
        .collect();

    collect_results(partial_results)
}

// ---------------------------------------------------------------------------
// Parallel aggregation merge functions
// ---------------------------------------------------------------------------

/// Merge two CountAggregates by summing counts per key.
pub(crate) fn merge_count(a: &CountAggregate, b: &CountAggregate) -> CountAggregate {
    let mut merged = a.clone();
    for (key, &count) in b.counts.iter() {
        *merged.counts.entry(key.clone()).or_insert(0) += count;
    }
    merged
}

/// Merge two SumAggregates by summing values per key.
pub(crate) fn merge_sum(a: &SumAggregate, b: &SumAggregate) -> SumAggregate {
    let mut merged = a.clone();
    for (key, &sum) in b.sums.iter() {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat(0.0f32));
        *entry = OrderedFloat(entry.into_inner() + sum.into_inner());
    }
    merged
}

/// Merge two AvgAggregates by combining (sum, count) pairs per key.
pub(crate) fn merge_avg(a: &AvgAggregate, b: &AvgAggregate) -> AvgAggregate {
    let mut merged = a.clone();
    for (key, &sum) in b.sums.iter() {
        let entry = merged.sums.entry(key.clone()).or_insert(OrderedFloat(0.0f64));
        *entry = OrderedFloat(entry.into_inner() + sum.into_inner());
    }
    for (key, &count) in b.counts.iter() {
        *merged.counts.entry(key.clone()).or_insert(0) += count;
    }
    merged
}

/// Merge two MinAggregates by taking the minimum value per key.
pub(crate) fn merge_min(a: &MinAggregate, b: &MinAggregate) -> MinAggregate {
    let mut merged = a.clone();
    for (key, value) in b.mins.iter() {
        match merged.mins.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let should_replace = match (e.get(), value) {
                    (Value::Int(i1), Value::Int(i2)) => i2 < i1,
                    (Value::Float(f1), Value::Float(f2)) => f2 < f1,
                    _ => false,
                };
                if should_replace {
                    e.insert(value.clone());
                }
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
            }
        }
    }
    merged
}

/// Merge two MaxAggregates by taking the maximum value per key.
pub(crate) fn merge_max(a: &MaxAggregate, b: &MaxAggregate) -> MaxAggregate {
    let mut merged = a.clone();
    for (key, value) in b.maxs.iter() {
        match merged.maxs.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let should_replace = match (e.get(), value) {
                    (Value::Int(i1), Value::Int(i2)) => i2 > i1,
                    (Value::Float(f1), Value::Float(f2)) => f2 > f1,
                    _ => false,
                };
                if should_replace {
                    e.insert(value.clone());
                }
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
            }
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// K-way merge for parallel ORDER BY
// ---------------------------------------------------------------------------

/// An entry in the k-way merge heap. Compared by sort key bytes, then
/// chunk index and row index as tiebreakers for stable ordering.
struct MergeEntry {
    key: Vec<u8>,
    chunk_idx: usize,
    row_idx: usize,
    record: Variables,
}

impl Eq for MergeEntry {}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.chunk_idx == other.chunk_idx
            && self.row_idx == other.row_idx
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.chunk_idx.cmp(&other.chunk_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Perform a k-way merge of pre-sorted chunks. Each chunk is a Vec of
/// `(sort_key_bytes, record)` pairs sorted in ascending key order.
/// Returns records in globally-sorted order, optionally limited to `limit`.
pub(crate) fn kway_merge(
    chunks: Vec<Vec<(Vec<u8>, Variables)>>,
    limit: Option<usize>,
) -> Vec<Variables> {
    let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();

    // Iterators to track current position within each chunk.
    let mut iters: Vec<std::vec::IntoIter<(Vec<u8>, Variables)>> = chunks
        .into_iter()
        .map(|c| c.into_iter())
        .collect();

    // Seed the heap with the first element from each chunk.
    for (chunk_idx, iter) in iters.iter_mut().enumerate() {
        if let Some((key, record)) = iter.next() {
            heap.push(Reverse(MergeEntry {
                key,
                chunk_idx,
                row_idx: 0,
                record,
            }));
        }
    }

    let cap = limit.unwrap_or(usize::MAX);
    let mut result: Vec<Variables> = Vec::with_capacity(std::cmp::min(cap, 1024));

    while let Some(Reverse(entry)) = heap.pop() {
        let chunk_idx = entry.chunk_idx;
        result.push(entry.record);

        if result.len() >= cap {
            break;
        }

        // Push the next element from the same chunk.
        if let Some((key, record)) = iters[chunk_idx].next() {
            heap.push(Reverse(MergeEntry {
                key,
                chunk_idx,
                row_idx: entry.row_idx + 1,
                record,
            }));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_chunks_basic() {
        let data = b"line1\nline2\nline3\nline4\n";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
        for chunk in &chunks {
            assert!(chunk.ends_with(b"\n"), "each chunk should end with newline");
        }
    }

    #[test]
    fn test_split_chunks_empty() {
        let data = b"";
        let chunks = split_chunks(data, 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_zero_chunks() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_more_chunks_than_lines() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 10);
        assert!(chunks.len() <= 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_chunk() {
        let data = b"line1\nline2\nline3\n";
        let chunks = split_chunks(data, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data.as_slice());
    }

    #[test]
    fn test_split_chunks_no_trailing_newline() {
        let data = b"line1\nline2";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_line() {
        let data = b"single_line\n";
        let chunks = split_chunks(data, 4);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_scan_strategy_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("small.log");
        std::fs::write(&file_path, "hello world\n").unwrap();
        let strategy = choose_strategy(&file_path);
        assert!(matches!(strategy, ScanStrategy::BufReader(_)));
    }

    #[test]
    fn test_scan_strategy_nonexistent() {
        let strategy = choose_strategy(std::path::Path::new("/tmp/nonexistent_logq_test_file.log"));
        assert!(matches!(strategy, ScanStrategy::BufReader(_)));
    }

    #[test]
    fn test_parallel_scan_basic() {
        // Build 20 lines of squid-format data (10 whitespace-separated fields each)
        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(
                "ts{} {} host{} status{} {} GET url{} rfc{} peer{} type{}\n",
                i, i, i, i, i * 100, i, i, i, i
            ));
        }

        let schema = LogSchema::from_format("squid");
        let all_fields: Vec<usize> = (0..schema.field_count()).collect();

        let results = parallel_scan_chunks(
            data.as_bytes(),
            2,
            &schema,
            &all_fields,
            &[],
            &None,
        )
        .unwrap();

        let total_rows: usize = results
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.len)
            .sum();

        assert_eq!(total_rows, 20);
    }

    // -------------------------------------------------------------------
    // Step 11: Merge function tests
    // -------------------------------------------------------------------

    fn key(s: &str) -> Option<Tuple> {
        Some(vec![Value::String(s.to_string())])
    }

    #[test]
    fn test_merge_count() {
        let mut a = CountAggregate::new();
        a.counts.insert(key("x"), 3);
        a.counts.insert(key("y"), 5);

        let mut b = CountAggregate::new();
        b.counts.insert(key("y"), 7);
        b.counts.insert(key("z"), 2);

        let merged = merge_count(&a, &b);
        assert_eq!(merged.counts.get(&key("x")), Some(&3));
        assert_eq!(merged.counts.get(&key("y")), Some(&12));
        assert_eq!(merged.counts.get(&key("z")), Some(&2));
    }

    #[test]
    fn test_merge_sum() {
        let mut a = SumAggregate::new();
        a.sums.insert(key("x"), OrderedFloat(1.0f32));
        a.sums.insert(key("y"), OrderedFloat(2.5f32));

        let mut b = SumAggregate::new();
        b.sums.insert(key("y"), OrderedFloat(3.5f32));
        b.sums.insert(key("z"), OrderedFloat(4.0f32));

        let merged = merge_sum(&a, &b);
        assert_eq!(merged.sums.get(&key("x")), Some(&OrderedFloat(1.0f32)));
        assert_eq!(merged.sums.get(&key("y")), Some(&OrderedFloat(6.0f32)));
        assert_eq!(merged.sums.get(&key("z")), Some(&OrderedFloat(4.0f32)));
    }

    #[test]
    fn test_merge_avg() {
        let mut a = AvgAggregate::new();
        a.sums.insert(key("x"), OrderedFloat(10.0f64));
        a.counts.insert(key("x"), 2);
        a.sums.insert(key("y"), OrderedFloat(6.0f64));
        a.counts.insert(key("y"), 3);

        let mut b = AvgAggregate::new();
        b.sums.insert(key("y"), OrderedFloat(14.0f64));
        b.counts.insert(key("y"), 7);
        b.sums.insert(key("z"), OrderedFloat(9.0f64));
        b.counts.insert(key("z"), 3);

        let merged = merge_avg(&a, &b);
        // x: only in a
        assert_eq!(merged.sums.get(&key("x")), Some(&OrderedFloat(10.0f64)));
        assert_eq!(merged.counts.get(&key("x")), Some(&2));
        // y: merged
        assert_eq!(merged.sums.get(&key("y")), Some(&OrderedFloat(20.0f64)));
        assert_eq!(merged.counts.get(&key("y")), Some(&10));
        // z: only in b
        assert_eq!(merged.sums.get(&key("z")), Some(&OrderedFloat(9.0f64)));
        assert_eq!(merged.counts.get(&key("z")), Some(&3));
    }

    #[test]
    fn test_merge_min() {
        let mut a = MinAggregate::new();
        a.mins.insert(key("x"), Value::Int(10));
        a.mins.insert(key("y"), Value::Int(3));

        let mut b = MinAggregate::new();
        b.mins.insert(key("y"), Value::Int(5));
        b.mins.insert(key("z"), Value::Int(1));

        let merged = merge_min(&a, &b);
        assert_eq!(merged.mins.get(&key("x")), Some(&Value::Int(10)));
        assert_eq!(merged.mins.get(&key("y")), Some(&Value::Int(3)));
        assert_eq!(merged.mins.get(&key("z")), Some(&Value::Int(1)));
    }

    #[test]
    fn test_merge_max() {
        let mut a = MaxAggregate::new();
        a.maxs.insert(key("x"), Value::Int(10));
        a.maxs.insert(key("y"), Value::Int(3));

        let mut b = MaxAggregate::new();
        b.maxs.insert(key("y"), Value::Int(5));
        b.maxs.insert(key("z"), Value::Int(1));

        let merged = merge_max(&a, &b);
        assert_eq!(merged.maxs.get(&key("x")), Some(&Value::Int(10)));
        assert_eq!(merged.maxs.get(&key("y")), Some(&Value::Int(5)));
        assert_eq!(merged.maxs.get(&key("z")), Some(&Value::Int(1)));
    }

    // -------------------------------------------------------------------
    // Step 12: K-way merge tests
    // -------------------------------------------------------------------

    fn make_record(name: &str) -> Variables {
        let mut vars = Variables::new();
        vars.insert("name".to_string(), Value::String(name.to_string()));
        vars
    }

    #[test]
    fn test_kway_merge_sorted_chunks() {
        // Two chunks with interleaved sorted keys
        let chunk1 = vec![
            (b"a".to_vec(), make_record("a1")),
            (b"c".to_vec(), make_record("c1")),
            (b"e".to_vec(), make_record("e1")),
        ];
        let chunk2 = vec![
            (b"b".to_vec(), make_record("b2")),
            (b"d".to_vec(), make_record("d2")),
            (b"f".to_vec(), make_record("f2")),
        ];

        let result = kway_merge(vec![chunk1, chunk2], None);
        let names: Vec<&str> = result
            .iter()
            .map(|r| match r.get("name").unwrap() {
                Value::String(s) => s.as_str(),
                _ => panic!("expected string"),
            })
            .collect();
        assert_eq!(names, vec!["a1", "b2", "c1", "d2", "e1", "f2"]);
    }

    #[test]
    fn test_kway_merge_with_limit() {
        let chunk1 = vec![
            (b"a".to_vec(), make_record("a1")),
            (b"c".to_vec(), make_record("c1")),
        ];
        let chunk2 = vec![
            (b"b".to_vec(), make_record("b2")),
            (b"d".to_vec(), make_record("d2")),
        ];

        let result = kway_merge(vec![chunk1, chunk2], Some(2));
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result
            .iter()
            .map(|r| match r.get("name").unwrap() {
                Value::String(s) => s.as_str(),
                _ => panic!("expected string"),
            })
            .collect();
        assert_eq!(names, vec!["a1", "b2"]);
    }

    #[test]
    fn test_kway_merge_empty() {
        let result = kway_merge(vec![], None);
        assert!(result.is_empty());

        let result = kway_merge(vec![vec![], vec![]], None);
        assert!(result.is_empty());
    }
}
