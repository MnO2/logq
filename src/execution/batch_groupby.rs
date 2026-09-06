// Batch aggregation keeps one set of shared accumulator states per group. Keys
// are encoded from columns directly, so dictionary strings and existing groups
// do not require a row map or a cloned Value tuple.
use crate::common::types::{Value, Variables, get_value_by_path_expr_scoped};
use crate::execution::batch::{BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, ColumnType, TypedColumn};
use crate::execution::memory::{MemoryReservation, MemoryTracker, estimate_batch, estimate_value};
use crate::execution::types::{
    AccumulatorKind, AccumulatorState, AggregateDef, Expression, ExtractionStrategy, NamedAggregate, StreamResult,
};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use ordered_float::OrderedFloat;
use std::mem::size_of;
use std::sync::Arc;

#[cfg(any(feature = "bench-internals", test))]
#[path = "group_probe.rs"]
pub mod probe;

pub(crate) struct BatchGroupByOperator {
    child: Box<dyn BatchStream>,
    group_keys: Vec<PathExpr>,
    aggregates: Vec<NamedAggregate>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    consumed: bool,
    schema: BatchSchema,
    memory: MemoryTracker,
    output_memory: MemoryReservation,
}

impl BatchGroupByOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        group_keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let names = output_names(&group_keys, &aggregates);
        Self {
            child,
            group_keys,
            aggregates,
            variables,
            registry,
            consumed: false,
            schema: BatchSchema {
                types: vec![ColumnType::Mixed; names.len()],
                names,
            },
            memory: MemoryTracker::default(),
            output_memory: MemoryReservation::default(),
        }
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.output_memory = MemoryReservation::new(memory.clone());
        self.memory = memory;
        self
    }

    /// MIN/MAX deliberately stay sequential for dynamic inputs: the existing
    /// scalar ordering only compares like Value variants, so a partial MIN of a
    /// heterogeneous chunk cannot in general be merged with an earlier chunk.
    pub(crate) fn supports_parallel(keys: &[PathExpr], aggregates: &[NamedAggregate]) -> bool {
        keys.iter().all(|path| simple_name(path).is_some())
            && aggregates.iter().all(|aggregate| {
                let def = AggregateDef::from_named_aggregate(aggregate);
                matches!(
                    def.kind,
                    AccumulatorKind::Count | AccumulatorKind::CountStar | AccumulatorKind::Sum | AccumulatorKind::Avg
                ) && match &def.extraction {
                    ExtractionStrategy::None => true,
                    ExtractionStrategy::Expression(Expression::Variable(path)) => simple_name(path).is_some(),
                    _ => false,
                }
            })
    }

    pub(crate) fn consume_partial(mut self) -> StreamResult<PartialAggregateState> {
        self.consume_state()
    }

    fn consume_state(&mut self) -> StreamResult<PartialAggregateState> {
        let defs: Vec<_> = self.aggregates.iter().map(AggregateDef::from_named_aggregate).collect();
        let mut state = PartialAggregateState::new(
            self.schema.names.clone(),
            &defs,
            self.group_keys.is_empty(),
            self.memory.clone(),
        )?;
        let mut key_bytes = Vec::new();
        let mut row_vars = Variables::new();
        while let Some(batch) = self.child.next_batch()? {
            let sources: Vec<_> = defs
                .iter()
                .map(|def| resolve_extraction(&def.extraction, &batch, &self.variables))
                .collect();
            let keys: Vec<_> = self
                .group_keys
                .iter()
                .map(|path| resolve_key(path, &batch, &self.variables))
                .collect();
            if keys.is_empty() && sources.iter().all(InputSource::is_direct) {
                accumulate_ungrouped(
                    &mut state.groups[0].accumulators,
                    &sources,
                    &batch,
                    &mut state.reservation,
                )?;
                continue;
            }
            let needs_row = sources.iter().any(|source| !source.is_direct())
                || keys.iter().any(|source| matches!(source, KeySource::Path(_)));
            let selected = batch.selection.to_bitmap(batch.len);
            for row in 0..batch.len {
                if !selected.is_set(row) {
                    continue;
                }
                if needs_row {
                    fill_row(&batch, row, &mut row_vars);
                }
                key_bytes.clear();
                for source in &keys {
                    encode_key(source, &batch, row, &row_vars, &self.variables, &mut key_bytes);
                }
                let group_index = if keys.is_empty() {
                    0
                } else if let Some(index) = state.index.get(key_bytes.as_slice()) {
                    *index
                } else {
                    let key = keys
                        .iter()
                        .map(|source| key_value(source, &batch, row, &row_vars, &self.variables))
                        .collect();
                    state.insert_group(key_bytes.clone(), key, &defs)?
                };
                let group = &mut state.groups[group_index];
                for (accumulator, source) in group.accumulators.iter_mut().zip(&sources) {
                    let previous = if state.reservation.is_enabled() {
                        estimate_accumulator(accumulator)
                    } else {
                        0
                    };
                    match source {
                        InputSource::None => accumulator.accumulate_row()?,
                        InputSource::Column(index) => accumulate_column(accumulator, &batch.columns[*index], row)?,
                        InputSource::Value(value) => accumulator.accumulate(value)?,
                        InputSource::Expression(expression) => accumulator.accumulate(
                            &expression.expression_value_impl(&row_vars, Some(&self.variables), &self.registry)?,
                        )?,
                        InputSource::RecordCapture => {
                            accumulator.accumulate(&Value::Object(Box::new(row_vars.clone())))?
                        }
                    }
                    update_charge(&mut state.reservation, accumulator, previous)?;
                }
            }
        }
        Ok(state)
    }
}

impl BatchStream for BatchGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.consumed {
            self.output_memory.resize(0)?;
            return Ok(None);
        }
        self.consumed = true;
        let state = match self.consume_state() {
            Ok(state) => state,
            Err(error) => {
                self.child.close();
                return Err(error);
            }
        };
        if state.groups.is_empty() {
            return Ok(None);
        }
        let (batch, reservation) = state.finish()?;
        self.output_memory = reservation;
        Ok(Some(batch))
    }
    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.child.close();
    }
}

struct RetainedGroup {
    key: Vec<Value>,
    accumulators: Vec<AccumulatorState>,
}

/// Worker output owns its memory charge until merging or final output transfers
/// the retained state. SUM/AVG remain f64 here; only finish converts to Value f32.
pub(crate) struct PartialAggregateState {
    index: hashbrown::HashMap<Vec<u8>, usize>,
    groups: Vec<RetainedGroup>,
    names: Vec<String>,
    reservation: MemoryReservation,
}

impl PartialAggregateState {
    pub(crate) fn count_star(count: i64, output_name: String, memory: MemoryTracker) -> StreamResult<Self> {
        let defs = [AggregateDef {
            kind: AccumulatorKind::CountStar,
            extraction: ExtractionStrategy::None,
            name: Some(output_name.clone()),
        }];
        let mut state = Self::new(vec![output_name], &defs, true, memory)?;
        state.groups[0].accumulators[0] = AccumulatorState::CountStar(count);
        Ok(state)
    }

    fn new(names: Vec<String>, defs: &[AggregateDef], ungrouped: bool, memory: MemoryTracker) -> StreamResult<Self> {
        let mut state = Self {
            index: hashbrown::HashMap::new(),
            groups: Vec::new(),
            names,
            reservation: MemoryReservation::new(memory),
        };
        if state.reservation.is_enabled() {
            state.reservation.add(
                size_of::<Self>()
                    + state
                        .names
                        .iter()
                        .map(|s| size_of::<String>() + s.capacity())
                        .sum::<usize>(),
            )?;
        }
        if ungrouped {
            state.insert_group(Vec::new(), Vec::new(), defs)?;
        }
        Ok(state)
    }

    fn insert_group(&mut self, bytes: Vec<u8>, key: Vec<Value>, defs: &[AggregateDef]) -> StreamResult<usize> {
        let group = RetainedGroup {
            key,
            accumulators: defs.iter().map(|def| AccumulatorState::new(&def.kind)).collect(),
        };
        if self.reservation.is_enabled() {
            self.reservation.add(estimate_group(&group, bytes.capacity()))?;
        }
        let index = self.groups.len();
        self.groups.push(group);
        self.index.insert(bytes, index);
        Ok(index)
    }

    pub(crate) fn merge(&mut self, mut later: Self) -> StreamResult<()> {
        // Move ownership; no finalized values or cloned group payloads cross the
        // queue. Release the donor charge before charging transferred storage.
        later.reservation.resize(0)?;
        let mut keys: Vec<Option<Vec<u8>>> = (0..later.groups.len()).map(|_| None).collect();
        for (key, index) in later.index {
            keys[index] = Some(key);
        }
        for (key, later_group) in keys.into_iter().zip(later.groups) {
            let key = key.unwrap();
            if let Some(index) = self.index.get(key.as_slice()).copied() {
                for (left, right) in self.groups[index].accumulators.iter_mut().zip(later_group.accumulators) {
                    merge_accumulator(left, right);
                }
            } else {
                if self.reservation.is_enabled() {
                    self.reservation.add(estimate_group(&later_group, key.capacity()))?;
                }
                let index = self.groups.len();
                self.groups.push(later_group);
                self.index.insert(key, index);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> StreamResult<(ColumnBatch, MemoryReservation)> {
        let len = self.groups.len();
        let mut columns: Vec<Vec<Value>> = (0..self.names.len()).map(|_| Vec::with_capacity(len)).collect();
        // Finalization can clone retained payload (e.g. GROUP AS); account for
        // output together with still-live state, then release the state charge.
        let retained_bytes = self.reservation.bytes();
        for mut group in self.groups.drain(..) {
            let key_len = group.key.len();
            for (column, value) in columns.iter_mut().zip(group.key) {
                column.push(value);
            }
            for (column, accumulator) in columns.iter_mut().skip(key_len).zip(&mut group.accumulators) {
                column.push(accumulator.finalize()?);
            }
        }
        let columns = columns
            .into_iter()
            .map(|data| {
                let mut null = Bitmap::all_set(len);
                let mut missing = Bitmap::all_set(len);
                for (row, value) in data.iter().enumerate() {
                    match value {
                        Value::Null => null.unset(row),
                        Value::Missing => missing.unset(row),
                        _ => {}
                    }
                }
                TypedColumn::Mixed { data, null, missing }
            })
            .collect();
        let batch = ColumnBatch {
            columns,
            names: self.names,
            selection: SelectionVector::All,
            len,
        };
        if self.reservation.is_enabled() {
            let output_bytes = estimate_batch(&batch);
            self.reservation.resize(retained_bytes.saturating_add(output_bytes))?;
            self.index.clear();
            self.reservation.resize(output_bytes)?;
        }
        Ok((batch, self.reservation))
    }
}

fn merge_accumulator(left: &mut AccumulatorState, right: AccumulatorState) {
    match (left, right) {
        (AccumulatorState::Count(a), AccumulatorState::Count(b))
        | (AccumulatorState::CountStar(a), AccumulatorState::CountStar(b)) => *a += b,
        (AccumulatorState::Sum(a), AccumulatorState::Sum(b)) => {
            if let Some(b) = b {
                *a = Some(a.map_or(b, |a| OrderedFloat(a.0 + b.0)));
            }
        }
        (
            AccumulatorState::Avg { sum, count },
            AccumulatorState::Avg {
                sum: other_sum,
                count: other_count,
            },
        ) => {
            *sum += other_sum;
            *count += other_count;
        }
        _ => unreachable!("parallel planning only permits Count/Sum/Avg"),
    }
}

pub(crate) fn output_names(keys: &[PathExpr], aggregates: &[NamedAggregate]) -> Vec<String> {
    let mut names = Vec::new();
    for key in keys {
        names.push(match key.path_segments.last() {
            Some(PathSegment::AttrName(name)) => name.clone(),
            _ => format!("_{}", names.len() + 1),
        });
    }
    for aggregate in aggregates {
        names.push(
            aggregate
                .name_opt
                .clone()
                .unwrap_or_else(|| format!("_{}", names.len() + 1)),
        );
    }
    names
}

fn simple_name(path: &PathExpr) -> Option<&str> {
    match path.path_segments.as_slice() {
        [PathSegment::AttrName(name)] => Some(name),
        _ => None,
    }
}

enum InputSource<'a> {
    None,
    Column(usize),
    Value(&'a Value),
    Expression(&'a Expression),
    RecordCapture,
}
impl InputSource<'_> {
    fn is_direct(&self) -> bool {
        !matches!(self, Self::Expression(_) | Self::RecordCapture)
    }
}
fn resolve_name<'a>(name: &str, batch: &ColumnBatch, variables: &'a Variables) -> InputSource<'a> {
    match batch.names.iter().rposition(|candidate| candidate == name) {
        Some(index) => InputSource::Column(index),
        None => InputSource::Value(variables.get(name).unwrap_or(&Value::Missing)),
    }
}
fn resolve_extraction<'a>(
    extraction: &'a ExtractionStrategy,
    batch: &ColumnBatch,
    variables: &'a Variables,
) -> InputSource<'a> {
    match extraction {
        ExtractionStrategy::None => InputSource::None,
        ExtractionStrategy::Expression(Expression::Variable(path)) if simple_name(path).is_some() => {
            resolve_name(simple_name(path).unwrap(), batch, variables)
        }
        ExtractionStrategy::Expression(expression) => InputSource::Expression(expression),
        ExtractionStrategy::ColumnLookup(name) => resolve_name(name, batch, variables),
        ExtractionStrategy::RecordCapture => InputSource::RecordCapture,
    }
}

enum KeySource<'a> {
    Column(usize),
    Value(&'a Value),
    Path(&'a PathExpr),
}
fn resolve_key<'a>(path: &'a PathExpr, batch: &ColumnBatch, variables: &'a Variables) -> KeySource<'a> {
    if let Some(name) = simple_name(path) {
        match resolve_name(name, batch, variables) {
            InputSource::Column(index) => KeySource::Column(index),
            InputSource::Value(value) => KeySource::Value(value),
            _ => unreachable!(),
        }
    } else {
        KeySource::Path(path)
    }
}
fn key_value(
    source: &KeySource<'_>,
    batch: &ColumnBatch,
    row: usize,
    variables: &Variables,
    scope: &Variables,
) -> Value {
    match source {
        KeySource::Column(index) => BatchToRowAdapter::extract_value(&batch.columns[*index], row),
        KeySource::Value(value) => (*value).clone(),
        KeySource::Path(path) => get_value_by_path_expr_scoped(path, 0, variables, Some(scope)),
    }
}
fn encode_key(
    source: &KeySource<'_>,
    batch: &ColumnBatch,
    row: usize,
    variables: &Variables,
    scope: &Variables,
    out: &mut Vec<u8>,
) {
    match source {
        KeySource::Column(index) => encode_column(&batch.columns[*index], row, out),
        KeySource::Value(value) => encode_value(value, out),
        KeySource::Path(_) => encode_value(&key_value(source, batch, row, variables, scope), out),
    }
}
fn fill_row(batch: &ColumnBatch, row: usize, variables: &mut Variables) {
    // Only fallback expressions need row materialization. Reuse map nodes for a
    // stable schema while removing old names if a child changes its schema.
    if variables.len() != batch.names.len() || variables.keys().zip(&batch.names).any(|(left, right)| left != right) {
        variables.clear();
    }
    for (name, column) in batch.names.iter().zip(&batch.columns) {
        let value = BatchToRowAdapter::extract_value(column, row);
        if let Some(slot) = variables.get_mut(name) {
            *slot = value;
        } else {
            variables.insert(name.clone(), value);
        }
    }
}

fn masks(column: &TypedColumn) -> (&Bitmap, &Bitmap) {
    match column {
        TypedColumn::Int32 { null, missing, .. }
        | TypedColumn::Float32 { null, missing, .. }
        | TypedColumn::Boolean { null, missing, .. }
        | TypedColumn::Utf8 { null, missing, .. }
        | TypedColumn::DictUtf8 { null, missing, .. }
        | TypedColumn::DateTime { null, missing, .. }
        | TypedColumn::Mixed { null, missing, .. } => (null, missing),
    }
}

fn accumulate_ungrouped(
    accumulators: &mut [AccumulatorState],
    sources: &[InputSource<'_>],
    batch: &ColumnBatch,
    reservation: &mut MemoryReservation,
) -> StreamResult<()> {
    let selected = batch.selection.to_bitmap(batch.len);
    for (state, source) in accumulators.iter_mut().zip(sources) {
        if let (AccumulatorState::CountStar(count), InputSource::None) = (&mut *state, source) {
            *count += selected.count_ones() as i64;
            continue;
        }
        if let InputSource::Column(index) = source {
            let column = &batch.columns[*index];
            let (null, missing) = masks(column);
            let active = selected.and(&null.and(missing));
            match (&mut *state, column) {
                (AccumulatorState::Count(count), column) if !matches!(column, TypedColumn::Mixed { .. }) => {
                    *count += active.count_ones() as i64;
                    continue;
                }
                (AccumulatorState::Sum(_) | AccumulatorState::Avg { .. }, TypedColumn::Int32 { data, .. }) => {
                    let count = active.count_ones() as i64;
                    let sum = crate::simd::kernels::sum_i32_selected(data, &active) as f64;
                    add_numeric_batch(state, sum, count);
                    continue;
                }
                (AccumulatorState::Sum(_) | AccumulatorState::Avg { .. }, TypedColumn::Float32 { data, .. }) => {
                    let count = active.count_ones() as i64;
                    // Multiplying by a zero mask lets inactive NaN/Inf poison
                    // the sum. Do not perform arithmetic on inactive values.
                    let sum = if count as usize == batch.len {
                        data.iter()
                            .take(batch.len)
                            .map(|value| *value as f64)
                            .reduce(|left, right| left + right)
                            .unwrap_or(0.0)
                    } else {
                        (0..batch.len)
                            .filter(|row| active.is_set(*row))
                            .map(|row| data[row] as f64)
                            .reduce(|left, right| left + right)
                            .unwrap_or(0.0)
                    };
                    add_numeric_batch(state, sum, count);
                    continue;
                }
                _ => {}
            }
        }
        for row in 0..batch.len {
            if !selected.is_set(row) {
                continue;
            }
            let before = if reservation.is_enabled() {
                estimate_accumulator(state)
            } else {
                0
            };
            match source {
                InputSource::None => state.accumulate_row()?,
                InputSource::Column(index) => accumulate_column(state, &batch.columns[*index], row)?,
                InputSource::Value(value) => state.accumulate(value)?,
                _ => unreachable!(),
            }
            update_charge(reservation, state, before)?;
        }
    }
    Ok(())
}
fn add_numeric_batch(state: &mut AccumulatorState, value: f64, rows: i64) {
    if rows == 0 {
        return;
    }
    match state {
        AccumulatorState::Sum(sum) => *sum = Some(OrderedFloat(sum.map_or(value, |sum| sum.0 + value))),
        AccumulatorState::Avg { sum, count } => {
            *sum += value;
            *count += rows;
        }
        _ => unreachable!(),
    }
}
fn accumulate_column(state: &mut AccumulatorState, column: &TypedColumn, row: usize) -> StreamResult<()> {
    let (null, missing) = masks(column);
    if !missing.is_set(row) {
        return Ok(());
    }
    if !null.is_set(row) {
        state.accumulate(&Value::Null)?;
        return Ok(());
    }
    if let TypedColumn::Mixed { data, .. } = column {
        state.accumulate(&data[row])?;
        return Ok(());
    }
    if let AccumulatorState::Count(count) = state {
        *count += 1;
        return Ok(());
    }
    match column {
        TypedColumn::Int32 { data, .. } => state.accumulate(&Value::Int(data[row]))?,
        TypedColumn::Float32 { data, .. } => state.accumulate(&Value::Float(OrderedFloat(data[row])))?,
        _ => state.accumulate(&BatchToRowAdapter::extract_value(column, row))?,
    }
    Ok(())
}
fn update_charge(
    reservation: &mut MemoryReservation,
    accumulator: &AccumulatorState,
    previous: usize,
) -> StreamResult<()> {
    if reservation.is_enabled() {
        reservation.resize(
            reservation
                .bytes()
                .saturating_sub(previous)
                .saturating_add(estimate_accumulator(accumulator)),
        )?;
    }
    Ok(())
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}
fn encode_float(value: f32, out: &mut Vec<u8>) {
    out.push(1);
    let normalized = if value == 0.0 {
        0
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    };
    out.extend_from_slice(&normalized.to_le_bytes());
}
fn encode_string(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(3);
    encode_bytes(String::from_utf8_lossy(bytes).as_bytes(), out);
}
fn encode_column(column: &TypedColumn, row: usize, out: &mut Vec<u8>) {
    let (null, missing) = masks(column);
    if !missing.is_set(row) {
        out.push(8);
        return;
    }
    if !null.is_set(row) {
        out.push(4);
        return;
    }
    match column {
        TypedColumn::Int32 { data, .. } => {
            out.push(0);
            out.extend_from_slice(&data[row].to_le_bytes());
        }
        TypedColumn::Float32 { data, .. } => encode_float(data[row], out),
        TypedColumn::Boolean { data, .. } => {
            out.push(2);
            out.push(data.is_set(row) as u8);
        }
        TypedColumn::Utf8 { data, offsets, .. } => {
            encode_string(&data[offsets[row] as usize..offsets[row + 1] as usize], out)
        }
        TypedColumn::DictUtf8 {
            dict_data,
            dict_offsets,
            codes,
            ..
        } => {
            let code = codes[row] as usize;
            encode_string(
                &dict_data[dict_offsets[code] as usize..dict_offsets[code + 1] as usize],
                out,
            );
        }
        TypedColumn::Mixed { data, .. } => encode_value(&data[row], out),
        TypedColumn::DateTime { .. } => encode_value(&BatchToRowAdapter::extract_value(column, row), out),
    }
}
fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(value) => {
            out.push(0);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => encode_float(value.0, out),
        Value::Boolean(value) => {
            out.push(2);
            out.push(*value as u8);
        }
        Value::String(value) => encode_string(value.as_bytes(), out),
        Value::Null => out.push(4),
        Value::DateTime(value) => {
            out.push(5);
            out.extend_from_slice(&value.timestamp().to_le_bytes());
            out.extend_from_slice(&value.timestamp_subsec_nanos().to_le_bytes());
        }
        Value::HttpRequest(value) => {
            out.push(6);
            encode_bytes(value.http_method.as_bytes(), out);
            encode_bytes(value.url_raw.as_bytes(), out);
            encode_bytes(value.http_version.as_bytes(), out);
        }
        Value::Host(value) => {
            out.push(7);
            encode_bytes(value.hostname.as_bytes(), out);
            out.extend_from_slice(&value.port.to_le_bytes());
        }
        Value::Missing => out.push(8),
        // LinkedHashMap equality includes insertion order, as does this key.
        Value::Object(values) => {
            out.push(9);
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for (name, value) in values.iter() {
                encode_bytes(name.as_bytes(), out);
                encode_value(value, out);
            }
        }
        Value::Array(values) => {
            out.push(10);
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for value in values {
                encode_value(value, out);
            }
        }
    }
}

pub(crate) fn estimate_accumulator(accumulator: &AccumulatorState) -> usize {
    let payload = match accumulator {
        AccumulatorState::Min(value)
        | AccumulatorState::Max(value)
        | AccumulatorState::First(value)
        | AccumulatorState::Last(value) => value.as_ref().map_or(0, estimate_value),
        AccumulatorState::GroupAs(values) | AccumulatorState::PercentileDisc { values, .. } => {
            values.capacity() * size_of::<Value>() + values.iter().map(estimate_value).sum::<usize>()
        }
        AccumulatorState::ApproxCountDistinct(_) => 512,
        AccumulatorState::ApproxPercentile { buffer, .. } => {
            100 * 32 + buffer.capacity() * size_of::<Value>() + buffer.iter().map(estimate_value).sum::<usize>()
        }
        _ => 0,
    };
    size_of::<AccumulatorState>() + payload
}
fn estimate_group(group: &RetainedGroup, key_capacity: usize) -> usize {
    // Includes conservative hash-table load factor and spare group-vector slots.
    96 + 2 * size_of::<RetainedGroup>()
        + key_capacity
        + group.key.capacity() * size_of::<Value>()
        + group.key.iter().map(estimate_value).sum::<usize>()
        + group.accumulators.iter().map(estimate_accumulator).sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::batch::{BatchSchema, BatchStream, ColumnBatch, ColumnType, TypedColumn};
    use crate::execution::types::{Aggregate, CountAggregate, Named, NamedAggregate, StreamResult};
    use crate::functions::FunctionRegistry;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVecBuilder;
    use crate::simd::selection::SelectionVector;
    use crate::syntax::ast::{PathExpr, PathSegment};
    use linked_hash_map::LinkedHashMap;
    use std::sync::Arc;

    /// A single-batch test helper stream.
    struct OneBatch {
        batch: Option<ColumnBatch>,
        schema: BatchSchema,
    }

    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batch.take())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    /// An empty test helper stream.
    struct EmptyStream {
        schema: BatchSchema,
    }

    impl BatchStream for EmptyStream {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(None)
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    /// Build a Utf8 TypedColumn from a slice of string values.
    fn build_utf8_column(values: &[&str]) -> TypedColumn {
        let n = values.len();
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(n + 1);
        offsets_builder.push(0);
        for s in values {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        }
    }

    #[test]
    fn test_batch_groupby_count_star() {
        // 4 rows: status = ["200", "200", "404", "200"]
        // Group by status, COUNT(*)
        // Expected: 2 groups, "200" -> 3, "404" -> 1
        let status_col = build_utf8_column(&["200", "200", "404", "200"]);
        let batch = ColumnBatch {
            columns: vec![status_col],
            names: vec!["status".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["status".to_string()],
            types: vec![ColumnType::Utf8],
        };
        let child = OneBatch {
            batch: Some(batch),
            schema,
        };

        let group_keys = vec![PathExpr::new(vec![PathSegment::AttrName("status".to_string())])];

        let count_agg = NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let mut op = BatchGroupByOperator::new(Box::new(child), group_keys, vec![count_agg], variables, registry);

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 2, "should have 2 groups");
        assert_eq!(result.columns.len(), 2, "should have 2 columns (status, cnt)");

        // Collect the results into a map for order-independent checking
        let mut group_counts: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
        for row in 0..result.len {
            let status_val = BatchToRowAdapter::extract_value(&result.columns[0], row);
            let count_val = BatchToRowAdapter::extract_value(&result.columns[1], row);
            let status_str = match status_val {
                Value::String(s) => s,
                other => panic!("expected String, got {:?}", other),
            };
            let count_int = match count_val {
                Value::Int(i) => i,
                other => panic!("expected Int, got {:?}", other),
            };
            group_counts.insert(status_str.to_string(), count_int);
        }

        assert_eq!(group_counts.get("200"), Some(&3));
        assert_eq!(group_counts.get("404"), Some(&1));

        // Second call should return None
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_groupby_empty_no_keys_returns_count_zero() {
        // Empty input stream, no group keys, COUNT(*)
        // Should return 1 row with COUNT = 0
        let schema = BatchSchema {
            names: vec![],
            types: vec![],
        };
        let child = EmptyStream { schema };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let mut op = BatchGroupByOperator::new(
            Box::new(child),
            vec![], // no group keys
            vec![count_agg],
            variables,
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1, "should have 1 row for empty-input aggregate");
        assert_eq!(result.columns.len(), 1, "should have 1 column (cnt)");

        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(0), "COUNT(*) on empty input should be 0");

        // Second call should return None
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_count_star() {
        // Ungrouped COUNT(*) over Int32 data -- should use column-direct fast path
        use crate::simd::padded_vec::PaddedVec;
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(crate::execution::types::CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![],
            vec![count_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(4));
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_sum() {
        use crate::execution::types::SumAggregate;
        use crate::simd::padded_vec::PaddedVec;

        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let sum_agg = NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(
                    Expression::Variable(PathExpr::new(vec![crate::syntax::ast::PathSegment::AttrName(
                        "x".to_string(),
                    )])),
                    Some("x".to_string()),
                ),
            ),
            Some("total".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![],
            vec![sum_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let sum_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(sum_val, Value::Float(OrderedFloat(100.0f32)));
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_with_selection() {
        use crate::simd::padded_vec::PaddedVec;

        // 4 rows, but only 2 active
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let mut sel = Bitmap::all_unset(4);
        sel.set(0);
        sel.set(2);
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::Bitmap(sel),
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(crate::execution::types::CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![],
            vec![count_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(2));
    }
    struct ManyBatches {
        batches: std::collections::VecDeque<ColumnBatch>,
        schema: BatchSchema,
    }
    impl BatchStream for ManyBatches {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batches.pop_front())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }
    fn mixed(values: Vec<Value>) -> TypedColumn {
        let n = values.len();
        TypedColumn::Mixed {
            data: values,
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        }
    }
    fn batches(columns: Vec<TypedColumn>) -> Box<dyn BatchStream> {
        Box::new(ManyBatches {
            batches: columns
                .into_iter()
                .map(|column| ColumnBatch {
                    len: match &column {
                        TypedColumn::Mixed { data, .. } => data.len(),
                        TypedColumn::Int32 { data, .. } => data.len(),
                        TypedColumn::Float32 { data, .. } => data.len(),
                        _ => unreachable!(),
                    },
                    names: vec!["x".into()],
                    columns: vec![column],
                    selection: SelectionVector::All,
                })
                .collect(),
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Mixed],
            },
        })
    }
    fn aggregate(kind: &str) -> NamedAggregate {
        use crate::execution::types::*;
        let named = Named::Expression(
            Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("x".into())])),
            None,
        );
        let aggregate = match kind {
            "count" => Aggregate::Count(CountAggregate::new(), named),
            "star" => Aggregate::Count(CountAggregate::new(), Named::Star),
            "sum" => Aggregate::Sum(SumAggregate::new(), named),
            "avg" => Aggregate::Avg(AvgAggregate::new(), named),
            "min" => Aggregate::Min(MinAggregate::new(), named),
            "max" => Aggregate::Max(MaxAggregate::new(), named),
            _ => unreachable!(),
        };
        NamedAggregate::new(aggregate, Some(kind.into()))
    }
    fn operator(child: Box<dyn BatchStream>, grouped: bool, kinds: &[&str]) -> BatchGroupByOperator {
        BatchGroupByOperator::new(
            child,
            if grouped {
                vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])]
            } else {
                vec![]
            },
            kinds.iter().map(|kind| aggregate(kind)).collect(),
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
        )
    }
    #[test]
    fn test_batch_groupby_typed_mixed_keys_are_identical() {
        use crate::simd::padded_vec::PaddedVec;
        let child = batches(vec![
            TypedColumn::Int32 {
                data: PaddedVec::from_vec(vec![1, 2]),
                null: Bitmap::all_set(2),
                missing: Bitmap::all_set(2),
            },
            mixed(vec![
                Value::Int(1),
                Value::Int(2),
                Value::String("1".into()),
                Value::Boolean(true),
            ]),
        ]);
        let result = operator(child, true, &["star"]).next_batch().unwrap().unwrap();
        assert_eq!(result.len, 4);
        for row in 0..2 {
            assert_eq!(BatchToRowAdapter::extract_value(&result.columns[1], row), Value::Int(2));
        }
    }
    #[test]
    fn test_batch_groupby_mixed_null_missing_match_accumulator_policy() {
        let child = batches(vec![mixed(vec![
            Value::Null,
            Value::Missing,
            Value::Int(2),
            Value::Int(1),
        ])]);
        let result = operator(child, false, &["count", "sum", "avg", "min", "max"])
            .next_batch()
            .unwrap()
            .unwrap();
        let actual: Vec<_> = result
            .columns
            .iter()
            .map(|column| BatchToRowAdapter::extract_value(column, 0))
            .collect();
        assert_eq!(
            actual,
            vec![
                Value::Int(2),
                Value::Float(OrderedFloat(3.0)),
                Value::Float(OrderedFloat(1.5)),
                Value::Null,
                Value::Null
            ]
        );
    }
    #[test]
    fn test_batch_groupby_invalid_numeric_values_are_errors() {
        for grouped in [false, true] {
            for kind in ["sum", "avg"] {
                let child = batches(vec![mixed(vec![Value::String("bad".into())])]);
                assert!(
                    operator(child, grouped, &[kind]).next_batch().is_err(),
                    "{kind} grouped={grouped}"
                );
            }
        }
    }
    #[test]
    fn test_batch_groupby_inactive_nan_does_not_poison_sum() {
        use crate::simd::padded_vec::PaddedVec;
        let mut selected = Bitmap::all_unset(3);
        selected.set(0);
        let child = OneBatch {
            batch: Some(ColumnBatch {
                columns: vec![TypedColumn::Float32 {
                    data: PaddedVec::from_vec(vec![2.0, f32::NAN, f32::INFINITY]),
                    null: Bitmap::all_set(3),
                    missing: Bitmap::all_set(3),
                }],
                names: vec!["x".into()],
                selection: SelectionVector::Bitmap(selected),
                len: 3,
            }),
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Float32],
            },
        };
        let result = operator(Box::new(child), false, &["sum", "avg"])
            .next_batch()
            .unwrap()
            .unwrap();
        for column in result.columns {
            assert_eq!(
                BatchToRowAdapter::extract_value(&column, 0),
                Value::Float(OrderedFloat(2.0))
            );
        }
    }
    #[test]
    fn test_batch_groupby_partial_sum_keeps_precision_and_encounter_order() {
        let mut first = operator(
            batches(vec![mixed(vec![Value::Int(16_777_216), Value::Int(1)])]),
            false,
            &["sum", "avg", "count", "star"],
        )
        .consume_partial()
        .unwrap();
        let second = operator(
            batches(vec![mixed(vec![Value::Int(-16_777_216)])]),
            false,
            &["sum", "avg", "count", "star"],
        )
        .consume_partial()
        .unwrap();
        first.merge(second).unwrap();
        let (result, _) = first.finish().unwrap();
        assert_eq!(
            BatchToRowAdapter::extract_value(&result.columns[0], 0),
            Value::Float(OrderedFloat(1.0))
        );
        assert_eq!(
            BatchToRowAdapter::extract_value(&result.columns[1], 0),
            Value::Float(OrderedFloat(1.0 / 3.0))
        );
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[2], 0), Value::Int(3));
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[3], 0), Value::Int(3));
        let mut first = operator(
            batches(vec![mixed(vec![Value::Int(3), Value::Int(1)])]),
            true,
            &["star"],
        )
        .consume_partial()
        .unwrap();
        first
            .merge(
                operator(
                    batches(vec![mixed(vec![Value::Int(2), Value::Int(1), Value::Int(4)])]),
                    true,
                    &["star"],
                )
                .consume_partial()
                .unwrap(),
            )
            .unwrap();
        let (result, _) = first.finish().unwrap();
        let actual: Vec<_> = (0..result.len)
            .map(|row| {
                (
                    BatchToRowAdapter::extract_value(&result.columns[0], row),
                    BatchToRowAdapter::extract_value(&result.columns[1], row),
                )
            })
            .collect();
        assert_eq!(
            actual,
            vec![
                (Value::Int(3), Value::Int(1)),
                (Value::Int(1), Value::Int(2)),
                (Value::Int(2), Value::Int(1)),
                (Value::Int(4), Value::Int(1))
            ]
        );
    }
    #[test]
    fn test_batch_groupby_memory_charges_groups_results_and_releases_errors() {
        use crate::execution::types::StreamError;
        let memory = MemoryTracker::new(Some(1200));
        let mut op = operator(batches(vec![mixed((0..50).map(Value::Int).collect())]), true, &["star"])
            .with_memory_tracker(memory.clone());
        assert!(matches!(op.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(memory.used(), 0);
        drop(op);
        assert_eq!(memory.used(), 0);

        let memory = MemoryTracker::new(Some(1_000_000));
        let mut first = operator(batches(vec![mixed(vec![Value::Int(1)])]), true, &["sum"])
            .with_memory_tracker(memory.clone())
            .consume_partial()
            .unwrap();
        let second = operator(batches(vec![mixed(vec![Value::Int(1), Value::Int(2)])]), true, &["sum"])
            .with_memory_tracker(memory.clone())
            .consume_partial()
            .unwrap();
        assert!(memory.used() > 0);
        first.merge(second).unwrap();
        let (batch, reservation) = first.finish().unwrap();
        assert_eq!(memory.used(), estimate_batch(&batch));
        assert!(memory.used() > 0);
        drop(reservation);
        assert_eq!(memory.used(), 0);
    }
    #[test]
    fn test_batch_groupby_fallback_expression_enforces_memory_limit() {
        use crate::execution::types::{CountAggregate, StreamError};
        let named = NamedAggregate::new(
            Aggregate::Count(
                CountAggregate::new(),
                Named::Expression(Expression::Constant(Value::Int(1)), None),
            ),
            Some("n".into()),
        );
        let memory = MemoryTracker::new(Some(1200));
        let mut op = BatchGroupByOperator::new(
            batches(vec![mixed((0..50).map(Value::Int).collect())]),
            vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])],
            vec![named],
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
        )
        .with_memory_tracker(memory.clone());
        assert!(matches!(op.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(memory.used(), 0);
    }
    #[test]
    fn test_batch_groupby_canonical_keys_match_value_equality() {
        let instant = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00.123456789Z").unwrap();
        let later_offset = chrono::DateTime::parse_from_rfc3339("2024-01-01T02:00:00.123456789+02:00").unwrap();
        let values = vec![
            Value::Int(1),
            Value::Float(OrderedFloat(1.0)),
            Value::Boolean(true),
            Value::String("1".into()),
            Value::Null,
            Value::Missing,
            Value::Float(OrderedFloat(0.0)),
            Value::Float(OrderedFloat(-0.0)),
            Value::Float(OrderedFloat(f32::NAN)),
            Value::Float(OrderedFloat(f32::from_bits(0x7fc00001))),
            Value::DateTime(instant),
            Value::DateTime(later_offset),
            Value::Array(vec![Value::String("ab".into()), Value::String("c".into())]),
            Value::Array(vec![Value::String("a".into()), Value::String("bc".into())]),
        ];
        for left in &values {
            for right in &values {
                let mut a = Vec::new();
                let mut b = Vec::new();
                encode_value(left, &mut a);
                encode_value(right, &mut b);
                assert_eq!(a == b, left == right, "{left:?} / {right:?}");
            }
        }
    }
    #[test]
    fn test_batch_groupby_parallel_gates_nonassociative_mixed_extrema() {
        assert!(BatchGroupByOperator::supports_parallel(
            &[],
            &[aggregate("count"), aggregate("sum"), aggregate("avg")]
        ));
        for kind in ["min", "max"] {
            assert!(!BatchGroupByOperator::supports_parallel(&[], &[aggregate(kind)]));
        }
    }
    #[test]
    fn test_batch_groupby_dictionary_utf8_and_mixed_keys_share_groups() {
        use crate::simd::padded_vec::PaddedVec;
        let string = "longer-than-inline-string";
        let dict = TypedColumn::DictUtf8 {
            dict_data: PaddedVec::from_vec(string.as_bytes().to_vec()),
            dict_offsets: PaddedVec::from_vec(vec![0, string.len() as u32]),
            codes: PaddedVec::from_vec(vec![0]),
            null: Bitmap::all_set(1),
            missing: Bitmap::all_set(1),
        };
        let columns = vec![
            dict,
            build_utf8_column(&[string]),
            mixed(vec![Value::String(string.into())]),
        ];
        let child = ManyBatches {
            batches: columns
                .into_iter()
                .map(|column| ColumnBatch {
                    columns: vec![column],
                    names: vec!["x".into()],
                    selection: SelectionVector::All,
                    len: 1,
                })
                .collect(),
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Mixed],
            },
        };
        let result = operator(Box::new(child), true, &["star"])
            .next_batch()
            .unwrap()
            .unwrap();
        assert_eq!(result.len, 1);
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[1], 0), Value::Int(3));
    }
    #[test]
    fn test_batch_groupby_nested_expressions_and_scoped_aggregate_values() {
        let mut object = Variables::new();
        object.insert("n".into(), Value::Int(7));
        let nested = PathExpr::new(vec![
            PathSegment::AttrName("x".into()),
            PathSegment::AttrName("n".into()),
        ]);
        let sum = NamedAggregate::new(
            Aggregate::Sum(
                crate::execution::types::SumAggregate::new(),
                Named::Expression(Expression::Variable(nested.clone()), None),
            ),
            Some("s".into()),
        );
        let mut op = BatchGroupByOperator::new(
            batches(vec![mixed(vec![
                Value::Object(Box::new(object.clone())),
                Value::Object(Box::new(object)),
            ])]),
            vec![nested],
            vec![sum],
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
        );
        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[0], 0), Value::Int(7));
        assert_eq!(
            BatchToRowAdapter::extract_value(&result.columns[1], 0),
            Value::Float(OrderedFloat(14.0))
        );

        let mut scope = Variables::new();
        scope.insert("x".into(), Value::Int(5));
        let empty_columns = OneBatch {
            batch: Some(ColumnBatch {
                columns: vec![],
                names: vec![],
                selection: SelectionVector::All,
                len: 3,
            }),
            schema: BatchSchema {
                names: vec![],
                types: vec![],
            },
        };
        let mut op = BatchGroupByOperator::new(
            Box::new(empty_columns),
            vec![],
            vec![aggregate("sum")],
            scope,
            Arc::new(FunctionRegistry::new()),
        );
        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(
            BatchToRowAdapter::extract_value(&result.columns[0], 0),
            Value::Float(OrderedFloat(15.0))
        );
    }
    #[test]
    fn test_batch_groupby_group_as_growth_is_budgeted_and_released() {
        let memory = MemoryTracker::new(Some(2048));
        let group_as = NamedAggregate::new(
            Aggregate::GroupAs(crate::execution::types::GroupAsAggregate::new(), Named::Star),
            Some("rows".into()),
        );
        let mut op = BatchGroupByOperator::new(
            batches(vec![mixed(
                (0..100).map(|_| Value::String("x".repeat(500).into())).collect(),
            )]),
            vec![],
            vec![group_as],
            Variables::new(),
            Arc::new(FunctionRegistry::new()),
        )
        .with_memory_tracker(memory.clone());
        assert!(matches!(
            op.next_batch(),
            Err(crate::execution::types::StreamError::MemoryBudgetExceeded)
        ));
        assert_eq!(memory.used(), 0);
    }
    #[test]
    fn test_batch_groupby_partial_count_stays_i64_until_finish() {
        let mut first =
            PartialAggregateState::count_star(i32::MAX as i64, "n".into(), MemoryTracker::default()).unwrap();
        first
            .merge(PartialAggregateState::count_star(2, "n".into(), MemoryTracker::default()).unwrap())
            .unwrap();
        assert!(
            matches!(first.groups[0].accumulators[0], AccumulatorState::CountStar(value) if value == i32::MAX as i64 + 2)
        );
    }
    #[test]
    fn test_batch_groupby_high_cardinality_partial_merge_matches_value_keys() {
        let memory = MemoryTracker::new(Some(32 * 1024 * 1024));
        let mut expected = std::collections::HashMap::<Value, i32>::new();
        let mut merged: Option<PartialAggregateState> = None;
        for worker in 0..4 {
            let values: Vec<_> = (0..4096)
                .map(|row| {
                    let index = worker * 4096 + row;
                    match index % 17 {
                        0 => Value::Null,
                        1 => Value::Missing,
                        2 => Value::String(format!("long dictionary-equivalent group {}", index % 300).into()),
                        3 => Value::Float(OrderedFloat((index % 2000) as f32)),
                        _ => Value::Int(index % 5000),
                    }
                })
                .collect();
            for value in &values {
                *expected.entry(value.clone()).or_default() += 1;
            }
            let child = batches(values.chunks(1024).map(|values| mixed(values.to_vec())).collect());
            let partial = operator(child, true, &["star"])
                .with_memory_tracker(memory.clone())
                .consume_partial()
                .unwrap();
            if let Some(merged) = merged.as_mut() {
                merged.merge(partial).unwrap();
            } else {
                merged = Some(partial);
            }
        }
        let (batch, reservation) = merged.unwrap().finish().unwrap();
        assert_eq!(batch.len, expected.len());
        for row in 0..batch.len {
            let key = BatchToRowAdapter::extract_value(&batch.columns[0], row);
            assert_eq!(
                BatchToRowAdapter::extract_value(&batch.columns[1], row),
                Value::Int(expected.remove(&key).unwrap())
            );
        }
        assert!(expected.is_empty());
        assert_eq!(memory.used(), estimate_batch(&batch));
        drop(reservation);
        assert_eq!(memory.used(), 0);
    }
    #[test]
    fn test_batch_groupby_sum_preserves_negative_zero_before_finalization() {
        use crate::simd::padded_vec::PaddedVec;
        for column in [
            mixed(vec![Value::Float(OrderedFloat(-0.0))]),
            TypedColumn::Float32 {
                data: PaddedVec::from_vec(vec![-0.0]),
                null: Bitmap::all_set(1),
                missing: Bitmap::all_set(1),
            },
        ] {
            let mut empty = operator(batches(vec![]), false, &["sum"]).consume_partial().unwrap();
            empty
                .merge(
                    operator(batches(vec![column]), false, &["sum"])
                        .consume_partial()
                        .unwrap(),
                )
                .unwrap();
            let (result, _) = empty.finish().unwrap();
            let Value::Float(value) = BatchToRowAdapter::extract_value(&result.columns[0], 0) else {
                panic!("expected float");
            };
            assert_eq!(value.0.to_bits(), (-0.0f32).to_bits());
        }
    }
    #[test]
    fn test_batch_groupby_error_closes_its_child() {
        struct ClosingChild {
            batch: Option<ColumnBatch>,
            schema: BatchSchema,
            closed: Arc<std::sync::atomic::AtomicBool>,
        }
        impl BatchStream for ClosingChild {
            fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema {
                &self.schema
            }
            fn close(&self) {
                self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let child = ClosingChild {
            batch: Some(ColumnBatch {
                columns: vec![mixed(vec![Value::String("invalid sum".into())])],
                names: vec!["x".into()],
                selection: SelectionVector::All,
                len: 1,
            }),
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Mixed],
            },
            closed: closed.clone(),
        };
        let mut op = operator(Box::new(child), false, &["sum"]);
        assert!(op.next_batch().is_err());
        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(op.next_batch().unwrap().is_none());
    }
}
