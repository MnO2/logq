//! Isolated, sequential aggregation phases over deterministic, already typed batches.
//! This is a diagnostic surface, not the production worker scheduler.
use super::BatchGroupByOperator;
use crate::common::types::{Value, Variables};
use crate::execution::batch::{
    BATCH_SIZE, BatchSchema, BatchToRowAdapter, ColumnBatch, ColumnType, PrecomputedBatchStream, TypedColumn,
};
use crate::execution::memory::MemoryTracker;
use crate::execution::stream::RecordStream;
use crate::execution::types::{
    Aggregate, AvgAggregate, CountAggregate, Expression, Named, NamedAggregate, SumAggregate,
};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use ordered_float::OrderedFloat;
use serde::Serialize;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug, Serialize)]
pub struct GroupProbeConfig {
    pub rows: usize,
    pub groups: usize,
    pub partitions: usize,
    pub skew: bool,
    pub nullable: bool,
    pub memory_limit: Option<usize>,
}

pub type GroupProbeReport = serde_json::Value;

/// Validate all values in an untimed run, then measure fresh, identical inputs.
/// Logical partitions run sequentially: this isolates costs, not worker scaling.
pub fn profile_group_phases(config: GroupProbeConfig) -> ProbeResult<GroupProbeReport> {
    validate_config(&config)?;
    let verified = run_phases(&config, true, &|| 0)?;
    let clock_start = Instant::now();
    let measured = run_phases(&config, false, &|| {
        clock_start.elapsed().as_nanos().min(u64::MAX as u128) as u64
    })?;
    if measured.output_rows != verified.output_rows || measured.format_bytes != verified.format_bytes {
        return Err("timed output row/byte counts differ from verified output".into());
    }
    Ok(serde_json::json!({
        "version": 1,
        "config": config,
        "query": "SELECT g, COUNT(*) AS n, COUNT(v) AS present, SUM(v) AS total, AVG(v) AS mean GROUP BY g",
        "input_definition": "typed i32 g and v; g=row%groups (skew: 90% g=0, remaining cycle g=1..groups-1); v=(row*17)%97-48; nullable: row%13=0 MISSING, else row%11=0 NULL; contiguous partitions",
        "input_rows": config.rows,
        "input_batches": measured.input_batches,
        "output_rows": measured.output_rows,
        "validation": "passed",
        "validation_scope": "every group/key/count/SUM/AVG checked against integer oracle in untimed preflight; timed run checks formatted row and byte counts, not a second full value oracle",
        "execution": "sequential logical partitions, not actual workers or a parallel wall-time prediction",
        "timing_scope": "input construction and operator setup excluded; local includes consuming/dropping typed input batches; merge includes donor disposal; finish includes output construction/state disposal; format includes real BatchToRowAdapter, CLI value conversion, NDJSON serialization and output drop to bounded memory sink, excludes OS output I/O",
        "memory_scope": "optional shared estimated operator-state budget; preparsed input/oracle and transient allocations are not a heap/RSS ceiling; no allocator instrumentation in these times",
        "local": measured.local,
        "merge": measured.merge,
        "local_ns": measured.local.iter().map(|part| part.elapsed_ns).sum::<u64>(),
        "merge_ns": measured.merge.iter().map(|part| part.elapsed_ns).sum::<u64>(),
        "finish_ns": measured.finish_ns,
        "format_ns": measured.format_ns,
        "format_bytes": measured.format_bytes,
    }))
}

fn validate_config(config: &GroupProbeConfig) -> ProbeResult<()> {
    if config.rows == 0
        || config.rows > i32::MAX as usize
        || config.groups == 0
        || config.groups > config.rows
        || config.partitions == 0
        || config.partitions > 4096
    {
        return Err("require 1 <= groups <= rows <= i32::MAX and 1 <= partitions <= 4096".into());
    }
    Ok(())
}

fn key_at(row: usize, config: &GroupProbeConfig) -> usize {
    if config.skew && config.groups > 1 {
        if row % 10 == 0 {
            1 + (row / 10) % (config.groups - 1)
        } else {
            0
        }
    } else {
        row % config.groups
    }
}

fn value_at(row: usize) -> i32 {
    ((row % 97 * 17) % 97) as i32 - 48
}

fn path(name: &str) -> PathExpr {
    PathExpr::new(vec![PathSegment::AttrName(name.into())])
}

fn input_schema() -> BatchSchema {
    BatchSchema {
        names: vec!["g".into(), "v".into()],
        types: vec![ColumnType::Int32; 2],
    }
}

struct PreparedPartition {
    operator: BatchGroupByOperator,
    rows: usize,
    batches: usize,
}

fn prepare(config: &GroupProbeConfig) -> Vec<PreparedPartition> {
    let memory = MemoryTracker::new(config.memory_limit);
    let registry = Arc::new(FunctionRegistry::new());
    (0..config.partitions)
        .map(|partition| {
            let start = config.rows / config.partitions * partition + partition.min(config.rows % config.partitions);
            let rows = config.rows / config.partitions + usize::from(partition < config.rows % config.partitions);
            let mut batches = Vec::new();
            for first in (start..start + rows).step_by(BATCH_SIZE) {
                let end = (first + BATCH_SIZE).min(start + rows);
                let len = end - first;
                let mut null = Bitmap::all_set(len);
                let mut missing = Bitmap::all_set(len);
                let mut keys = Vec::with_capacity(len);
                let mut values = Vec::with_capacity(len);
                for index in first..end {
                    keys.push(key_at(index, config) as i32);
                    values.push(value_at(index));
                    if config.nullable {
                        if index % 13 == 0 {
                            missing.unset(index - first);
                        } else if index % 11 == 0 {
                            null.unset(index - first);
                        }
                    }
                }
                batches.push(ColumnBatch {
                    names: input_schema().names,
                    columns: vec![
                        TypedColumn::Int32 {
                            data: PaddedVec::from_vec(keys),
                            null: Bitmap::all_set(len),
                            missing: Bitmap::all_set(len),
                        },
                        TypedColumn::Int32 {
                            data: PaddedVec::from_vec(values),
                            null,
                            missing,
                        },
                    ],
                    selection: SelectionVector::All,
                    len,
                });
            }
            let batch_count = batches.len();
            let value = Named::Expression(Expression::Variable(path("v")), Some("v".into()));
            let aggregates = vec![
                NamedAggregate::new(Aggregate::Count(CountAggregate::new(), Named::Star), Some("n".into())),
                NamedAggregate::new(
                    Aggregate::Count(CountAggregate::new(), value.clone()),
                    Some("present".into()),
                ),
                NamedAggregate::new(Aggregate::Sum(SumAggregate::new(), value.clone()), Some("total".into())),
                NamedAggregate::new(Aggregate::Avg(AvgAggregate::new(), value), Some("mean".into())),
            ];
            PreparedPartition {
                operator: BatchGroupByOperator::new(
                    Box::new(PrecomputedBatchStream::new(batches, input_schema())),
                    vec![path("g")],
                    aggregates,
                    Variables::new(),
                    registry.clone(),
                )
                .with_memory_tracker(memory.clone()),
                rows,
                batches: batch_count,
            }
        })
        .collect()
}

#[derive(Serialize)]
struct LocalPhase {
    partition: usize,
    rows: usize,
    groups: usize,
    batches: usize,
    elapsed_ns: u64,
}

#[derive(Serialize)]
struct MergePhase {
    donor_partition: usize,
    groups_before: usize,
    donor_groups: usize,
    groups_after: usize,
    elapsed_ns: u64,
}

struct PhaseRun {
    local: Vec<LocalPhase>,
    merge: Vec<MergePhase>,
    input_batches: usize,
    output_rows: usize,
    finish_ns: u64,
    format_ns: u64,
    format_bytes: u64,
}

fn run_phases(config: &GroupProbeConfig, verify: bool, clock: &impl Fn() -> u64) -> ProbeResult<PhaseRun> {
    // Prepare *all* partitions before the first timing span. No parsing or
    // synthetic input allocation is charged to any operator phase.
    let prepared = prepare(config);
    let mut states = Vec::with_capacity(prepared.len());
    let mut local = Vec::with_capacity(prepared.len());
    let mut merge = Vec::with_capacity(prepared.len().saturating_sub(1));
    for (partition, input) in prepared.into_iter().enumerate() {
        let start = clock();
        let state = input.operator.consume_partial()?;
        let elapsed_ns = clock().saturating_sub(start);
        local.push(LocalPhase {
            partition,
            rows: input.rows,
            batches: input.batches,
            groups: state.groups.len(),
            elapsed_ns,
        });
        states.push(state);
    }
    let mut states = states.into_iter();
    let mut merged = states.next().ok_or("no partitions")?;
    for (offset, donor) in states.enumerate() {
        let groups_before = merged.groups.len();
        let donor_groups = donor.groups.len();
        let start = clock();
        merged.merge(donor)?;
        let elapsed_ns = clock().saturating_sub(start);
        merge.push(MergePhase {
            donor_partition: offset + 1,
            groups_before,
            donor_groups,
            groups_after: merged.groups.len(),
            elapsed_ns,
        });
    }
    let start = clock();
    let (batch, reservation) = merged.finish()?;
    let finish_ns = clock().saturating_sub(start);
    if verify {
        validate_batch(&batch, config)?;
    }
    let output_rows = batch.len;
    let mut sink = CountingSink::default();
    let start = clock();
    let formatted_rows = format_batch(batch, &mut sink)?;
    drop(reservation);
    let format_ns = clock().saturating_sub(start);
    if formatted_rows != output_rows {
        return Err("formatter skipped output rows".into());
    }
    Ok(PhaseRun {
        input_batches: local.iter().map(|part| part.batches).sum(),
        local,
        merge,
        output_rows,
        finish_ns,
        format_ns,
        format_bytes: sink.bytes,
    })
}

#[derive(Default, Clone)]
struct ExpectedGroup {
    count: i32,
    present: i32,
    sum: i64,
}

fn validate_batch(batch: &ColumnBatch, config: &GroupProbeConfig) -> ProbeResult<()> {
    let mut expected = vec![ExpectedGroup::default(); config.groups];
    for row in 0..config.rows {
        let group = &mut expected[key_at(row, config)];
        group.count += 1;
        if !config.nullable || (row % 13 != 0 && row % 11 != 0) {
            group.present += 1;
            group.sum += i64::from(value_at(row));
        }
    }
    if batch.names != ["g", "n", "present", "total", "mean"]
        || batch.columns.len() != 5
        || batch.len != expected.iter().filter(|group| group.count != 0).count()
    {
        return Err("incorrect group output schema/cardinality".into());
    }
    let mut seen = vec![false; config.groups];
    for row in 0..batch.len {
        let Value::Int(key) = BatchToRowAdapter::extract_value(&batch.columns[0], row) else {
            return Err("group key is not an integer".into());
        };
        let key = usize::try_from(key)?;
        if key >= expected.len() || seen[key] {
            return Err("incorrect/duplicate group key".into());
        }
        seen[key] = true;
        let group = &expected[key];
        let sum = if group.present == 0 {
            Value::Null
        } else {
            Value::Float(OrderedFloat(group.sum as f32))
        };
        let mean = if group.present == 0 {
            Value::Null
        } else {
            Value::Float(OrderedFloat((group.sum as f64 / f64::from(group.present)) as f32))
        };
        for (column, expected) in
            batch.columns[1..]
                .iter()
                .zip([Value::Int(group.count), Value::Int(group.present), sum, mean])
        {
            if BatchToRowAdapter::extract_value(column, row) != expected {
                return Err(format!("incorrect aggregate for group {key}").into());
            }
        }
    }
    if expected
        .iter()
        .zip(seen)
        .any(|(group, seen)| (group.count != 0) != seen)
    {
        return Err("missing or phantom group in output".into());
    }
    Ok(())
}

#[derive(Default)]
struct CountingSink {
    bytes: u64,
}

impl Write for CountingSink {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        std::hint::black_box(buffer);
        self.bytes = self
            .bytes
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| io::Error::other("output byte count overflow"))?;
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn format_batch(batch: ColumnBatch, target: &mut impl Write) -> ProbeResult<usize> {
    let schema = BatchSchema {
        names: batch.names.clone(),
        types: vec![ColumnType::Mixed; batch.names.len()],
    };
    let mut records = BatchToRowAdapter::new(Box::new(PrecomputedBatchStream::new(vec![batch], schema)));
    let mut writer = io::BufWriter::new(target);
    let mut rows = 0;
    while let Some(record) = records.next()? {
        crate::app::write_json_record(&mut writer, &record)?;
        writeln!(writer)?;
        rows += 1;
    }
    writer.flush()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(rows: usize, groups: usize, partitions: usize) -> GroupProbeConfig {
        GroupProbeConfig {
            rows,
            groups,
            partitions,
            skew: false,
            nullable: true,
            memory_limit: None,
        }
    }

    #[test]
    fn group_probe_validates_every_group_across_uneven_partitions() {
        let report = profile_group_phases(config(103, 9, 4)).unwrap();
        assert_eq!(report["validation"], "passed");
        assert_eq!(report["output_rows"], 9);
        assert_eq!(report["input_rows"], 103);
        let local = report["local"].as_array().unwrap();
        assert_eq!(local.len(), 4);
        assert_eq!(
            local.iter().map(|part| part["rows"].as_u64().unwrap()).sum::<u64>(),
            103
        );
        assert_eq!(report["merge"].as_array().unwrap().len(), 3);
        assert!(report["format_bytes"].as_u64().unwrap() > 0);
    }

    #[test]
    fn group_probe_reports_actual_skew_cardinality_and_empty_partitions() {
        let mut skew = config(100, 100, 3);
        skew.skew = true;
        assert_eq!(profile_group_phases(skew).unwrap()["output_rows"], 11);
        let small = profile_group_phases(config(3, 3, 5)).unwrap();
        assert_eq!(small["output_rows"], 3);
        assert_eq!(
            small["local"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|p| p["rows"] == 0)
                .count(),
            2
        );
    }

    #[test]
    fn group_probe_rejects_invalid_configuration_and_memory_failure() {
        for bad in [config(0, 9, 1), config(9, 0, 1), config(9, 9, 0), config(9, 10, 1)] {
            assert!(profile_group_phases(bad).is_err());
        }
        let mut limited = config(100, 100, 4);
        limited.memory_limit = Some(1);
        assert!(
            profile_group_phases(limited)
                .unwrap_err()
                .to_string()
                .contains("memory")
        );
    }

    #[test]
    fn group_probe_clock_only_charges_explicit_phase_spans() {
        let ticks = std::cell::Cell::new(0u64);
        let run = run_phases(&config(103, 9, 4), true, &|| {
            ticks.set(ticks.get() + 10);
            ticks.get()
        })
        .unwrap();
        assert_eq!(ticks.get(), 180); // 4 local + 3 merge + finish + format, 2 readings each.
        assert!(run.local.iter().all(|phase| phase.elapsed_ns == 10));
        assert!(run.merge.iter().all(|phase| phase.elapsed_ns == 10));
        assert_eq!((run.finish_ns, run.format_ns), (10, 10));
    }

    #[test]
    fn group_probe_oracle_rejects_phantom_group_with_matching_cardinality() {
        let mut cfg = config(100, 100, 1);
        cfg.skew = true;
        let prepared = prepare(&cfg).pop().unwrap();
        let (mut batch, _reservation) = prepared.operator.consume_partial().unwrap().finish().unwrap();
        validate_batch(&batch, &cfg).unwrap();
        for (column, replacement) in
            batch
                .columns
                .iter_mut()
                .zip([Value::Int(99), Value::Int(0), Value::Int(0), Value::Null, Value::Null])
        {
            let TypedColumn::Mixed { data, .. } = column else {
                panic!("current finish output is Mixed")
            };
            data[0] = replacement;
        }
        assert!(
            validate_batch(&batch, &cfg).is_err(),
            "one real group was replaced by a zero-count phantom"
        );
    }

    #[test]
    fn group_probe_serializes_real_rows_and_propagates_writer_errors() {
        let make_batch = || {
            prepare(&config(5, 2, 1))
                .pop()
                .unwrap()
                .operator
                .consume_partial()
                .unwrap()
                .finish()
                .unwrap()
        };
        let (batch, _reservation) = make_batch();
        let mut output = Vec::new();
        assert_eq!(format_batch(batch, &mut output).unwrap(), 2);
        let actual: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            actual,
            vec![
                serde_json::json!({"g": 0, "n": 3, "present": 2, "total": 6, "mean": 3}),
                serde_json::json!({"g": 1, "n": 2, "present": 2, "total": -28, "mean": -14}),
            ]
        );
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("probe writer failed"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert!(
            format_batch(make_batch().0, &mut BrokenWriter)
                .unwrap_err()
                .to_string()
                .contains("probe writer failed")
        );
    }
}
