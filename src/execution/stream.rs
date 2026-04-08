use super::datasource::RecordRead;
use super::types::{AggregateDef, ExtractionStrategy, Expression, Formula, GroupState, LogicalJoinType, Named, NamedAggregate, Node, StreamResult};
use crate::common;
use crate::common::types::{Tuple, Value, VariableName, Variables};
use crate::functions::FunctionRegistry;
use crate::syntax::ast::{self, PathSegment};
use hashbrown::HashMap;
use linked_hash_map::LinkedHashMap;
use prettytable::Cell;
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Record {
    variables: LinkedHashMap<String, Value>,
}

impl Record {
    pub fn new(field_names: &Vec<VariableName>, data: Vec<Value>) -> Self {
        let mut variables = LinkedHashMap::with_capacity(field_names.len());
        for (i, v) in data.into_iter().enumerate() {
            variables.insert(field_names[i].clone(), v);
        }

        Record { variables }
    }

    pub(crate) fn new_with_variables(variables: Variables) -> Self {
        Record { variables }
    }

    pub(crate) fn into_variables(self) -> Variables {
        self.variables
    }

    pub(crate) fn get(&self, field_name: &ast::PathExpr) -> Value {
        common::types::get_value_by_path_expr(field_name, 0, &self.variables)
    }

    /// Get a reference to a value for a single-segment path. Returns None for
    /// multi-segment paths or missing values (caller must fall back to get()).
    #[inline]
    pub(crate) fn get_ref(&self, field_name: &ast::PathExpr) -> Option<&Value> {
        if field_name.path_segments.len() == 1 {
            if let ast::PathSegment::AttrName(ref name) = field_name.path_segments[0] {
                return self.variables.get(name);
            }
        }
        None
    }

    /// Direct field access by bare name. Bypasses PathExpr construction.
    /// For the join key extraction hot path.
    #[inline]
    pub(crate) fn get_field_value(&self, field_name: &str) -> Option<&Value> {
        self.variables.get(field_name)
    }

    pub(crate) fn alias(&mut self, bindings: &Vec<common::types::Binding>) {
        for binding in bindings.iter() {
            let val = common::types::get_value_by_path_expr(&binding.path_expr, 0, &self.variables);
            self.variables.insert(binding.name.clone(), val);
        }
    }

    pub(crate) fn project(&self, field_names: &[VariableName]) -> Record {
        let mut variables = Variables::with_capacity(field_names.len());
        for name in field_names {
            if let Some(v) = self.variables.get(name) {
                variables.insert(name.clone(), v.clone());
            }
        }
        Record::new_with_variables(variables)
    }

    pub(crate) fn get_many(&self, field_names: &[ast::PathExpr]) -> Vec<Value> {
        let mut ret = Vec::with_capacity(field_names.len());
        for name in field_names {
            let v = common::types::get_value_by_path_expr(name, 0, &self.variables);
            ret.push(v);
        }
        ret
    }

    pub fn to_variables(&self) -> &Variables {
        &self.variables as &Variables
    }

    pub fn to_tuples(&self) -> Vec<(VariableName, Value)> {
        self.variables.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn into_tuples(self) -> Vec<(VariableName, Value)> {
        self.variables.into_iter().collect()
    }

    pub(crate) fn to_row(&self) -> Vec<Cell> {
        self.variables
            .values()
            .map(|val| match val {
                Value::String(s) => Cell::new(&*s),
                Value::Int(i) => Cell::new(&*i.to_string()),
                Value::Float(f) => Cell::new(&*f.to_string()),
                Value::Boolean(b) => Cell::new(&*b.to_string()),
                Value::Null => Cell::new("<null>"),
                Value::DateTime(dt) => Cell::new(&*dt.to_string()),
                Value::HttpRequest(request) => Cell::new(&*request.to_string()),
                Value::Host(host) => Cell::new(&*host.to_string()),
                Value::Missing => Cell::new("<null>"),
                Value::Object(_) => Cell::new("{...}"),
                Value::Array(_) => Cell::new("[...]"),
            })
            .collect()
    }

    pub(crate) fn merge(&self, other: &Record) -> Record {
        let mut variables = self.variables.clone();
        for (k, v) in other.variables.iter() {
            variables.insert(k.clone(), v.clone());
        }
        Record { variables }
    }

    pub(crate) fn to_csv_record(&self) -> Vec<String> {
        self.variables
            .values()
            .map(|val| match val {
                Value::String(s) => s.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::Null => "<null>".to_string(),
                Value::DateTime(dt) => dt.to_string(),
                Value::HttpRequest(request) => request.to_string(),
                Value::Host(host) => host.to_string(),
                Value::Missing => "<null>".to_string(),
                Value::Object(_) => "{...}".to_string(),
                Value::Array(_) => "[...]".to_string(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum JoinKey {
    /// Single string key: hash raw bytes.
    SingleString(String),
    /// Single integer key: use i32 directly.
    SingleInt(i32),
    /// Multi-column or exotic types: fall back to Vec<Value>.
    Composite(Vec<Value>),
}

impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JoinKey::SingleString(a), JoinKey::SingleString(b)) => a == b,
            (JoinKey::SingleInt(a), JoinKey::SingleInt(b)) => a == b,
            (JoinKey::Composite(a), JoinKey::Composite(b)) => a == b,
            _ => false, // Different variants are never equal
        }
    }
}

impl Eq for JoinKey {}

impl std::hash::Hash for JoinKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first to prevent cross-variant collisions
        std::mem::discriminant(self).hash(state);
        match self {
            JoinKey::SingleString(s) => s.hash(state),
            JoinKey::SingleInt(i) => i.hash(state),
            JoinKey::Composite(vals) => vals.hash(state),
        }
    }
}

/// Extract a join key from a record using path expressions.
/// For single-column keys, specializes to SingleString or SingleInt.
/// NULL and Missing keys use Composite path (they won't match anything
/// since NULL != NULL in SQL semantics).
pub(crate) fn extract_key(record: &Record, key_fields: &[ast::PathExpr]) -> JoinKey {
    if key_fields.len() == 1 {
        match record.get(&key_fields[0]) {
            Value::String(s) => JoinKey::SingleString(s),
            Value::Int(i) => JoinKey::SingleInt(i),
            other => JoinKey::Composite(vec![other]),
        }
    } else {
        JoinKey::Composite(
            key_fields
                .iter()
                .map(|f| record.get(f))
                .collect(),
        )
    }
}

pub trait RecordStream {
    fn next(&mut self) -> StreamResult<Option<Record>>;
    fn close(&self);
}

pub struct MapStream {
    pub(crate) named_list: Vec<Named>,
    pub(crate) column_names: Vec<Option<String>>,
    pub(crate) variables: Variables,
    pub(crate) source: Box<dyn RecordStream>,
    pub(crate) registry: Arc<FunctionRegistry>,
    simple_projection: bool,
    is_star_only: bool,
    /// Pre-computed (source_field, output_column) pairs for simple_projection fast path
    projection_map: Vec<(String, String)>,
    /// True when all projection entries keep the same field name (no rename)
    projection_rename_free: bool,
}

impl MapStream {
    pub fn new(named_list: Vec<Named>, variables: Variables, source: Box<dyn RecordStream>, registry: Arc<FunctionRegistry>) -> Self {
        let column_names: Vec<Option<String>> = named_list.iter().enumerate().map(|(idx, named)| {
            match named {
                Named::Expression(_, name_opt) => {
                    Some(name_opt.clone().unwrap_or_else(|| format!("_{}", idx)))
                }
                Named::Star => None,
            }
        }).collect();
        // Pre-compute: can we use the move-based fast path?
        // True when all entries are simple single-segment Variable references
        // and variables is empty (no merge needed).
        let simple_projection = variables.is_empty() && named_list.iter().all(|n| matches!(n,
            Named::Expression(Expression::Variable(pe), _)
                if pe.path_segments.len() == 1 && matches!(&pe.path_segments[0], PathSegment::AttrName(_))
        ));
        // Also check for pure SELECT * (single Star, no extra variables)
        let is_star_only = variables.is_empty()
            && named_list.len() == 1
            && matches!(&named_list[0], Named::Star);
        // Pre-compute (source_field, output_column) pairs for simple_projection
        let projection_map: Vec<(String, String)> = if simple_projection {
            named_list.iter().enumerate().filter_map(|(idx, named)| {
                if let Named::Expression(Expression::Variable(pe), _) = named {
                    if let PathSegment::AttrName(ref field_name) = pe.path_segments[0] {
                        let out_name = column_names[idx].as_ref().unwrap().clone();
                        return Some((field_name.clone(), out_name));
                    }
                }
                None
            }).collect()
        } else {
            Vec::new()
        };
        let projection_rename_free = simple_projection
            && projection_map.iter().all(|(src, out)| src == out);
        MapStream {
            named_list,
            column_names,
            variables,
            source,
            registry,
            simple_projection,
            is_star_only,
            projection_map,
            projection_rename_free,
        }
    }
}

impl RecordStream for MapStream {
    fn close(&self) {
        self.source.close();
    }

    fn next(&mut self) -> StreamResult<Option<Record>> {
        if let Some(record) = self.source.next()? {
            // Fast path: SELECT * with no extra variables — pass through unchanged
            if self.is_star_only {
                return Ok(Some(record));
            }

            // Fast path: all expressions are simple variable projections, no merge needed.
            if self.simple_projection {
                if self.projection_rename_free {
                    // Zero-clone path: iterate source, keep matching entries by owned key
                    let map = &self.projection_map;
                    let mut out = Variables::with_capacity(map.len());
                    for (k, v) in record.into_variables().into_iter() {
                        if map.iter().any(|(src, _)| src == &k) {
                            out.insert(k, v);
                        }
                    }
                    return Ok(Some(Record::new_with_variables(out)));
                }
                // Move values out of source record instead of cloning.
                let mut source_vars = record.into_variables();
                let mut out = Variables::with_capacity(self.projection_map.len());
                for (src_field, out_name) in &self.projection_map {
                    let v = source_vars.remove(src_field).unwrap_or(Value::Missing);
                    out.insert(out_name.clone(), v);
                }
                return Ok(Some(Record::new_with_variables(out)));
            }

            let variables_owned;
            let variables = if self.variables.is_empty() {
                record.to_variables()
            } else {
                variables_owned = common::types::merge(&self.variables, record.to_variables());
                &variables_owned
            };

            let mut out = Variables::with_capacity(self.named_list.len());
            for (idx, named) in self.named_list.iter().enumerate() {
                match named {
                    Named::Expression(expr, _) => {
                        let name = self.column_names[idx].as_ref().unwrap().clone();
                        let v = expr.expression_value(variables, &self.registry)?;
                        out.insert(name, v);
                    }
                    Named::Star => {
                        for (k, v) in record.to_variables().iter() {
                            out.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            Ok(Some(Record::new_with_variables(out)))
        } else {
            Ok(None)
        }
    }
}

pub struct LimitStream {
    curr: u32,
    row_count: u32,
    source: Box<dyn RecordStream>,
}

impl LimitStream {
    pub fn new(row_count: u32, source: Box<dyn RecordStream>) -> Self {
        LimitStream {
            curr: 0,
            row_count,
            source,
        }
    }
}

impl RecordStream for LimitStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if self.curr >= self.row_count {
            return Ok(None);
        }

        while let Some(record) = self.source.next()? {
            if self.curr < self.row_count {
                self.curr += 1;
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn close(&self) {
        self.source.close();
    }
}

pub struct FilterStream {
    formula: Formula,
    variables: Variables,
    source: Box<dyn RecordStream>,
    registry: Arc<FunctionRegistry>,
}

impl FilterStream {
    pub fn new(formula: Formula, variables: Variables, source: Box<dyn RecordStream>, registry: Arc<FunctionRegistry>) -> Self {
        FilterStream {
            formula,
            variables,
            source,
            registry,
        }
    }
}

impl RecordStream for FilterStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        while let Some(record) = self.source.next()? {
            let predicate = if self.variables.is_empty() {
                self.formula.evaluate(record.to_variables(), &self.registry)?
            } else {
                let variables = common::types::merge(&self.variables, record.to_variables());
                self.formula.evaluate(&variables, &self.registry)?
            };

            if predicate == Some(true) {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn close(&self) {
        self.source.close();
    }
}

pub struct InMemoryStream {
    pub(crate) data: VecDeque<Record>,
}

impl InMemoryStream {
    pub fn new(data: VecDeque<Record>) -> InMemoryStream {
        InMemoryStream { data }
    }
}

impl RecordStream for InMemoryStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        Ok(self.data.pop_front())
    }

    fn close(&self) {}
}

pub struct GroupByStream {
    keys: Vec<ast::PathExpr>,
    variables: Variables,
    aggregate_defs: Vec<AggregateDef>,
    source: Box<dyn RecordStream>,
    group_iterator: Option<hashbrown::hash_map::IntoIter<Option<Tuple>, GroupState>>,
    registry: Arc<FunctionRegistry>,
}

impl GroupByStream {
    pub fn new(
        keys: Vec<ast::PathExpr>,
        variables: Variables,
        aggregates: Vec<NamedAggregate>,
        source: Box<dyn RecordStream>,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let aggregate_defs: Vec<AggregateDef> = aggregates
            .iter()
            .map(AggregateDef::from_named_aggregate)
            .collect();
        GroupByStream {
            keys,
            variables,
            aggregate_defs,
            source,
            group_iterator: None,
            registry,
        }
    }
}

impl RecordStream for GroupByStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if self.group_iterator.is_none() {
            let mut groups: HashMap<Option<Tuple>, GroupState> = HashMap::new();
            while let Some(record) = self.source.next()? {
                let variables_owned;
                let variables = if self.variables.is_empty() {
                    record.to_variables()
                } else {
                    variables_owned = common::types::merge(&self.variables, record.to_variables());
                    &variables_owned
                };

                let key = if self.keys.is_empty() {
                    None
                } else {
                    Some(record.get_many(&self.keys))
                };

                let missing = Value::Missing;
                let state = groups
                    .entry(key)
                    .or_insert_with(|| GroupState::new(&self.aggregate_defs));
                for (i, def) in self.aggregate_defs.iter().enumerate() {
                    match &def.extraction {
                        ExtractionStrategy::Expression(expr) => {
                            let val = expr.expression_value(&variables, &self.registry)?;
                            state.accumulators[i].accumulate(&val)?;
                        }
                        ExtractionStrategy::ColumnLookup(col_name) => {
                            let val = variables.get(col_name).unwrap_or(&missing);
                            state.accumulators[i].accumulate(val)?;
                        }
                        ExtractionStrategy::RecordCapture => {
                            let val =
                                Value::Object(Box::new(record.to_variables().clone()));
                            state.accumulators[i].accumulate(&val)?;
                        }
                        ExtractionStrategy::None => {
                            state.accumulators[i].accumulate_row()?;
                        }
                    }
                }
            }

            // Empty-input global aggregate: emit one row with defaults
            if groups.is_empty() && self.keys.is_empty() {
                let state = GroupState::new(&self.aggregate_defs);
                groups.insert(None, state);
            }

            self.group_iterator = Some(groups.into_iter());
        }

        let iter = self.group_iterator.as_mut().unwrap();
        if let Some((key, mut state)) = iter.next() {
            let mut values: Vec<Value> = Vec::new();
            let mut fields: Vec<VariableName> = Vec::new();

            if let Some(values_in_key) = &key {
                for (position_idx, k) in self.keys.iter().enumerate() {
                    match k.path_segments.last().unwrap() {
                        ast::PathSegment::AttrName(s) => {
                            fields.push(s.clone());
                        }
                        ast::PathSegment::ArrayIndex(_, _)
                        | ast::PathSegment::Wildcard
                        | ast::PathSegment::WildcardAttr => {
                            fields.push(format!("_{}", position_idx + 1));
                        }
                    }
                }

                for v in values_in_key {
                    values.push(v.clone());
                }
            }

            for (i, def) in self.aggregate_defs.iter().enumerate() {
                if let Some(ref field_name) = def.name {
                    fields.push(field_name.clone());
                } else {
                    let idx = fields.len() + 1;
                    fields.push(format!("_{}", idx));
                }
                let v = state.accumulators[i].finalize()?;
                values.push(v);
            }

            let record = Record::new(&fields, values);
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn close(&self) {
        self.source.close();
    }
}

pub(crate) struct ProjectionStream {
    pub(crate) source: Box<dyn RecordStream>,
    pub(crate) bindings: Vec<common::types::Binding>,
    produced_records: Option<Vec<Record>>,
    idx: usize,
}

impl ProjectionStream {
    pub(crate) fn new(source: Box<dyn RecordStream>, bindings: Vec<common::types::Binding>) -> Self {
        ProjectionStream {
            source,
            bindings,
            produced_records: None,
            idx: 0,
        }
    }
}

impl RecordStream for ProjectionStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        loop {
            if self.produced_records.is_none() {
                if let Some(mut record) = self.source.next()? {
                    record.alias(&self.bindings);
                    let binding_names: Vec<VariableName> = self.bindings.iter().map(|b| b.name.clone()).collect();
                    let projected_record = record.project(&binding_names);

                    let mut produced_records: Vec<Record> = vec![];
                    let mut results: Vec<Vec<Value>> = vec![];
                    let mut keys: Vec<String> = vec![];

                    let key_value_pairs: Vec<(String, Value)> = projected_record
                        .to_variables()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();

                    for (k, v) in key_value_pairs.iter() {
                        keys.push(k.clone());

                        let mut push_idx = false;
                        for binding in self.bindings.iter() {
                            if binding.name.eq(k) {
                                if let Some(idx_name) = binding.idx_name.as_ref() {
                                    keys.push(idx_name.clone());
                                    push_idx = true;
                                }
                            }
                        }

                        match v {
                            Value::Array(a) => {
                                let mut new_results = vec![];
                                for (i, e) in a.iter().enumerate() {
                                    let mut replica = results.clone();
                                    if replica.is_empty() {
                                        let mut v = vec![e.clone()];

                                        if push_idx {
                                            v.push(Value::Int(i as i32))
                                        }

                                        replica.push(v);
                                    } else {
                                        for row in replica.iter_mut() {
                                            row.push(e.clone());

                                            if push_idx {
                                                row.push(Value::Int(i as i32));
                                            }
                                        }
                                    }

                                    new_results.extend(replica.into_iter());
                                }

                                results = new_results;
                            }
                            _ => {
                                if results.is_empty() {
                                    let mut x = vec![v.clone()];
                                    if push_idx {
                                        x.push(Value::Missing);
                                    }

                                    results.push(x);
                                } else {
                                    for row in results.iter_mut() {
                                        row.push(v.clone());

                                        if push_idx {
                                            row.push(Value::Missing);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    for result in results.iter() {
                        let r = Record::new(&keys, result.clone());
                        produced_records.push(r);
                    }

                    if produced_records.is_empty() {
                        return Ok(None);
                    } else {
                        let r: Record = produced_records[0].clone();
                        self.produced_records = Some(produced_records);
                        self.idx = 1;

                        return Ok(Some(r));
                    }
                } else {
                    return Ok(None);
                }
            } else {
                let idx = self.idx;
                let records = self.produced_records.as_ref().unwrap();

                if idx < records.len() {
                    let record = records[idx].clone();
                    self.idx += 1;
                    return Ok(Some(record));
                } else {
                    self.produced_records = None;
                }
            }
        }
    }

    fn close(&self) {}
}

pub(crate) struct LogFileStream {
    pub(crate) reader: Box<dyn RecordRead>,
}

impl LogFileStream {
    pub(crate) fn new(reader: Box<dyn RecordRead>) -> Self {
        LogFileStream { reader }
    }
}

impl RecordStream for LogFileStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if let Some(record) = self.reader.read_record()? {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn close(&self) {}
}

pub(crate) struct CrossJoinStream {
    left: Box<dyn RecordStream>,
    right_node: Node,
    right_variables: Variables,
    current_left: Option<Record>,
    right_rows: Option<Vec<Record>>,
    right_index: usize,
    registry: Arc<FunctionRegistry>,
    threads: usize,
}

impl CrossJoinStream {
    pub(crate) fn new(left: Box<dyn RecordStream>, right_node: Node, right_variables: Variables, registry: Arc<FunctionRegistry>, threads: usize) -> Self {
        CrossJoinStream {
            left,
            right_node,
            right_variables,
            current_left: None,
            right_rows: None,
            right_index: 0,
            registry,
            threads,
        }
    }

    fn materialize_right(&mut self) -> StreamResult<()> {
        let mut right_stream = self.right_node.get(self.right_variables.clone(), self.registry.clone(), self.threads)
            .map_err(|e| super::types::StreamError::Get(e))?;
        let mut rows = Vec::new();
        while let Some(record) = right_stream.next()? {
            rows.push(record);
        }
        self.right_rows = Some(rows);
        Ok(())
    }
}

impl RecordStream for CrossJoinStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        // Materialize right side on first call
        if self.right_rows.is_none() {
            self.materialize_right()?;
        }

        let right_rows = self.right_rows.as_ref().unwrap();

        loop {
            // If we have a current left record, try to get next right record by index
            if self.current_left.is_some() && self.right_index < right_rows.len() {
                let left = self.current_left.as_ref().unwrap();
                let right_record = &right_rows[self.right_index];
                self.right_index += 1;
                let merged = left.merge(right_record);
                return Ok(Some(merged));
            }

            // Get next left record and reset right index
            match self.left.next()? {
                Some(left_record) => {
                    self.current_left = Some(left_record);
                    self.right_index = 0;
                }
                None => return Ok(None),
            }
        }
    }

    fn close(&self) {
        self.left.close();
    }
}

pub(crate) struct LeftJoinStream {
    left: Box<dyn RecordStream>,
    right_node: Node,
    right_variables: Variables,
    condition: Formula,
    current_left: Option<Record>,
    right_rows: Option<Vec<Record>>,
    right_index: usize,
    matched: bool,
    right_field_names: Option<Vec<String>>,
    registry: Arc<FunctionRegistry>,
    threads: usize,
}

impl LeftJoinStream {
    pub(crate) fn new(
        left: Box<dyn RecordStream>,
        right_node: Node,
        right_variables: Variables,
        condition: Formula,
        registry: Arc<FunctionRegistry>,
        threads: usize,
    ) -> Self {
        LeftJoinStream {
            left,
            right_node,
            right_variables,
            condition,
            current_left: None,
            right_rows: None,
            right_index: 0,
            matched: false,
            right_field_names: None,
            registry,
            threads,
        }
    }

    fn materialize_right(&mut self) -> StreamResult<()> {
        let mut right_stream = self.right_node.get(self.right_variables.clone(), self.registry.clone(), self.threads)
            .map_err(|e| super::types::StreamError::Get(e))?;
        let mut rows = Vec::new();
        while let Some(record) = right_stream.next()? {
            rows.push(record);
        }
        // Populate right_field_names from first record if available
        if !rows.is_empty() {
            self.right_field_names = Some(
                rows[0].to_variables().keys().cloned().collect(),
            );
        }
        self.right_rows = Some(rows);
        Ok(())
    }

    fn null_padded_right_record(&self) -> Record {
        let mut variables = common::types::Variables::default();
        if let Some(ref names) = self.right_field_names {
            for name in names {
                variables.insert(name.clone(), Value::Null);
            }
        }
        Record::new_with_variables(variables)
    }
}

impl RecordStream for LeftJoinStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        // Materialize right side on first call
        if self.right_rows.is_none() {
            self.materialize_right()?;
        }

        let right_rows = self.right_rows.as_ref().unwrap();

        loop {
            // If we have a current left record, try to get next right record by index
            if self.current_left.is_some() && self.right_index < right_rows.len() {
                let right_record = &right_rows[self.right_index];
                self.right_index += 1;

                let left = self.current_left.as_ref().unwrap();
                let merged = left.merge(right_record);

                // Evaluate ON condition
                let merged_vars = common::types::merge(&self.right_variables, merged.to_variables());
                let predicate = self.condition.evaluate(&merged_vars, &self.registry)?;

                if predicate == Some(true) {
                    self.matched = true;
                    return Ok(Some(merged));
                }
                // Condition didn't match; continue to next right row
                continue;
            }

            // If we just finished scanning right for a left row and found no match,
            // emit left row with NULL padding for right columns
            if self.current_left.is_some() && !self.matched {
                let left = self.current_left.take().unwrap();
                let null_right = self.null_padded_right_record();
                let merged = left.merge(&null_right);
                // Now fall through to get next left row on next call
                // But first return this result
                return Ok(Some(merged));
            }

            // Get next left record and reset right index
            match self.left.next()? {
                Some(left_record) => {
                    self.current_left = Some(left_record);
                    self.matched = false;
                    self.right_index = 0;
                }
                None => return Ok(None),
            }
        }
    }

    fn close(&self) {
        self.left.close();
    }
}

pub(crate) struct HashJoinStream {
    left: Box<dyn RecordStream>,
    hash_table: HashMap<JoinKey, SmallVec<[Record; 1]>>,
    join_type: LogicalJoinType,
    left_key_fields: Vec<ast::PathExpr>,
    right_key_fields: Vec<ast::PathExpr>,
    residual: Option<Formula>,
    registry: Arc<FunctionRegistry>,
    // Iteration state
    current_left: Option<Record>,
    current_matches: SmallVec<[Record; 1]>,
    match_index: usize,
    matched: bool,
    right_field_names: Vec<String>,
    built: bool,
    memory_limit: usize,
    build_input: Option<Box<dyn RecordStream>>,
}

impl HashJoinStream {
    pub(crate) fn new(
        left: Box<dyn RecordStream>,
        right: Box<dyn RecordStream>,
        left_key_fields: Vec<ast::PathExpr>,
        right_key_fields: Vec<ast::PathExpr>,
        residual: Option<Formula>,
        join_type: LogicalJoinType,
        memory_limit: usize,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        HashJoinStream {
            left,
            hash_table: HashMap::new(),
            join_type,
            left_key_fields,
            right_key_fields,
            residual,
            registry,
            current_left: None,
            current_matches: SmallVec::new(),
            match_index: 0,
            matched: false,
            right_field_names: Vec::new(),
            built: false,
            memory_limit,
            build_input: Some(right),
        }
    }

    fn build(&mut self) -> StreamResult<()> {
        let mut build_stream = self.build_input.take().unwrap();
        let mut approx_bytes: usize = 0;
        let mut first_record = true;

        while let Some(record) = build_stream.next()? {
            if first_record {
                self.right_field_names = record.to_variables().keys().cloned().collect();
                first_record = false;
            }

            // Check for NULL/Missing keys — skip them (NULL != NULL in SQL)
            let has_null_key = self.right_key_fields.iter().any(|f| {
                match record.get(f) {
                    Value::Null | Value::Missing => true,
                    _ => false,
                }
            });
            if has_null_key {
                continue;
            }

            // Approximate memory tracking: count field values
            let record_size: usize = record.to_variables().iter().map(|(k, v)| {
                k.len() + 24 + match v {
                    Value::String(s) => s.len() + 24,
                    Value::Int(_) => 4,
                    Value::Float(_) => 4,
                    Value::Boolean(_) => 1,
                    _ => 24,
                }
            }).sum();
            approx_bytes += record_size + 80; // 80 for LinkedHashMap per-entry overhead

            if approx_bytes > self.memory_limit {
                return Err(super::types::StreamError::General(format!(
                    "Hash join build side exceeded memory limit of {} MB. \
                     Consider using a smaller table on the build side, \
                     or increase the limit with --join-memory-limit.",
                    self.memory_limit / (1024 * 1024)
                )));
            }

            let key = extract_key(&record, &self.right_key_fields);
            self.hash_table.entry(key).or_insert_with(SmallVec::new).push(record);
        }

        self.built = true;
        Ok(())
    }

    fn null_padded_right_record(&self) -> Record {
        let mut variables = Variables::default();
        for name in &self.right_field_names {
            variables.insert(name.clone(), Value::Null);
        }
        Record::new_with_variables(variables)
    }
}

impl RecordStream for HashJoinStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if !self.built {
            self.build()?;
        }

        loop {
            // If we have pending matches from a previous probe, yield them
            if self.current_left.is_some() && self.match_index < self.current_matches.len() {
                let left = self.current_left.as_ref().unwrap();
                let right = &self.current_matches[self.match_index];
                self.match_index += 1;
                let merged = left.merge(right);

                // Apply residual filter if present
                if let Some(ref residual) = self.residual {
                    let vars = merged.to_variables().clone();
                    let predicate = residual.evaluate(&vars, &self.registry)?;
                    if predicate != Some(true) {
                        continue;
                    }
                }

                self.matched = true;
                return Ok(Some(merged));
            }

            // Finished scanning matches for current left row
            if self.current_left.is_some() && !self.matched {
                // LEFT JOIN: emit NULL-padded right side for unmatched left row
                if self.join_type == LogicalJoinType::Left {
                    let left = self.current_left.take().unwrap();
                    let null_right = self.null_padded_right_record();
                    return Ok(Some(left.merge(&null_right)));
                }
                // INNER JOIN: just move on to next left row
            }

            // Get next left row
            match self.left.next()? {
                Some(left_record) => {
                    // Check for NULL/Missing keys on probe side
                    let has_null_key = self.left_key_fields.iter().any(|f| {
                        match left_record.get(f) {
                            Value::Null | Value::Missing => true,
                            _ => false,
                        }
                    });

                    if has_null_key {
                        // NULL keys never match
                        if self.join_type == LogicalJoinType::Left {
                            let null_right = self.null_padded_right_record();
                            return Ok(Some(left_record.merge(&null_right)));
                        }
                        // INNER: skip this row entirely
                        continue;
                    }

                    let key = extract_key(&left_record, &self.left_key_fields);
                    self.current_matches = self.hash_table
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(SmallVec::new);
                    self.current_left = Some(left_record);
                    self.match_index = 0;
                    self.matched = false;
                }
                None => return Ok(None),
            }
        }
    }

    fn close(&self) {
        self.left.close();
    }
}

pub(crate) struct UnionStream {
    left: Box<dyn RecordStream>,
    right: Box<dyn RecordStream>,
    left_exhausted: bool,
}

impl UnionStream {
    pub(crate) fn new(left: Box<dyn RecordStream>, right: Box<dyn RecordStream>) -> Self {
        UnionStream {
            left,
            right,
            left_exhausted: false,
        }
    }
}

impl RecordStream for UnionStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if !self.left_exhausted {
            if let Some(record) = self.left.next()? {
                return Ok(Some(record));
            }
            self.left_exhausted = true;
        }
        self.right.next()
    }

    fn close(&self) {
        self.left.close();
        self.right.close();
    }
}

pub(crate) struct IntersectStream {
    left: Box<dyn RecordStream>,
    right_set: std::collections::HashMap<Vec<(VariableName, Value)>, usize>,
    all: bool,
}

impl IntersectStream {
    pub(crate) fn new(
        left: Box<dyn RecordStream>,
        mut right: Box<dyn RecordStream>,
        all: bool,
    ) -> StreamResult<Self> {
        let mut right_set = std::collections::HashMap::new();
        while let Some(record) = right.next()? {
            let key = record.to_tuples();
            *right_set.entry(key).or_insert(0) += 1;
        }
        Ok(IntersectStream { left, right_set, all })
    }
}

impl RecordStream for IntersectStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        while let Some(record) = self.left.next()? {
            let key = record.to_tuples();
            if let Some(count) = self.right_set.get_mut(&key) {
                if *count > 0 {
                    if self.all {
                        *count -= 1;
                    } else {
                        self.right_set.remove(&key);
                    }
                    return Ok(Some(record));
                }
            }
        }
        Ok(None)
    }

    fn close(&self) {
        self.left.close();
    }
}

pub(crate) struct ExceptStream {
    left: Box<dyn RecordStream>,
    right_set: std::collections::HashMap<Vec<(VariableName, Value)>, usize>,
    all: bool,
}

impl ExceptStream {
    pub(crate) fn new(
        left: Box<dyn RecordStream>,
        mut right: Box<dyn RecordStream>,
        all: bool,
    ) -> StreamResult<Self> {
        let mut right_set = std::collections::HashMap::new();
        while let Some(record) = right.next()? {
            let key = record.to_tuples();
            *right_set.entry(key).or_insert(0) += 1;
        }
        Ok(ExceptStream { left, right_set, all })
    }
}

impl RecordStream for ExceptStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        while let Some(record) = self.left.next()? {
            let key = record.to_tuples();
            if self.all {
                if let Some(count) = self.right_set.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        continue;
                    }
                }
                return Ok(Some(record));
            } else {
                if !self.right_set.contains_key(&key) {
                    return Ok(Some(record));
                }
            }
        }
        Ok(None)
    }

    fn close(&self) {
        self.left.close();
    }
}

pub(crate) struct DistinctStream {
    source: Box<dyn RecordStream>,
    seen: std::collections::HashSet<Vec<(VariableName, Value)>>,
}

impl DistinctStream {
    pub(crate) fn new(source: Box<dyn RecordStream>) -> Self {
        DistinctStream {
            source,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl RecordStream for DistinctStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        while let Some(record) = self.source.next()? {
            let key = record.to_tuples();
            if self.seen.insert(key) {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn close(&self) {
        self.source.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::stream::{Record, RecordStream};
    use crate::execution::types;
    use crate::execution::types::Expression;

    #[test]
    fn test_limit_stream() {
        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut limit_stream = LimitStream::new(1, stream);

        let mut result = Vec::new();
        while let Some(n) = limit_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        )];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_filter_stream() {
        let path_expr_host = ast::PathExpr::new(vec![ast::PathSegment::AttrName("host".to_string())]);
        let path_expr_const = ast::PathExpr::new(vec![ast::PathSegment::AttrName("const".to_string())]);

        let left = Box::new(types::Expression::Variable(path_expr_host));
        let right = Box::new(types::Expression::Variable(path_expr_const));
        let rel = types::Relation::Equal;
        let predicate = types::Formula::Predicate(rel, left, right);

        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let mut filtered_stream = FilterStream::new(predicate, variables, stream, registry);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        )];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_map_stream_with_star() {
        let named_list = vec![Named::Star];

        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let mut filtered_stream = MapStream::new(named_list, variables, stream, registry);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![
            Record::new(
                &vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example01.com".to_string()), Value::Int(8000)],
            ),
            Record::new(
                &vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example.com".to_string()), Value::Int(8001)],
            ),
            Record::new(
                &vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example01.com".to_string()), Value::Int(8002)],
            ),
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_map_stream_with_names() {
        let path_expr_port = ast::PathExpr::new(vec![ast::PathSegment::AttrName("port".to_string())]);
        let named_list = vec![Named::Expression(
            Expression::Variable(path_expr_port),
            Some("port".to_string()),
        )];

        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let mut filtered_stream = MapStream::new(named_list, variables, stream, registry);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![
            Record::new(&vec!["port".to_string()], vec![Value::Int(8000)]),
            Record::new(&vec!["port".to_string()], vec![Value::Int(8001)]),
            Record::new(&vec!["port".to_string()], vec![Value::Int(8002)]),
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_distinct_stream() {
        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("other.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            &vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8000)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut distinct_stream = DistinctStream::new(stream);

        let mut result = Vec::new();
        while let Some(n) = distinct_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![
            Record::new(
                &vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example.com".to_string()), Value::Int(8000)],
            ),
            Record::new(
                &vec!["host".to_string(), "port".to_string()],
                vec![Value::String("other.com".to_string()), Value::Int(8001)],
            ),
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_distinct_stream_all_unique() {
        let mut records = VecDeque::new();
        records.push_back(Record::new(
            &vec!["a".to_string()],
            vec![Value::Int(1)],
        ));
        records.push_back(Record::new(
            &vec!["a".to_string()],
            vec![Value::Int(2)],
        ));
        records.push_back(Record::new(
            &vec!["a".to_string()],
            vec![Value::Int(3)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut distinct_stream = DistinctStream::new(stream);

        let mut result = Vec::new();
        while let Some(n) = distinct_stream.next().unwrap() {
            result.push(n);
        }

        assert_eq!(3, result.len());
    }

    #[test]
    fn test_distinct_stream_empty() {
        let records = VecDeque::new();
        let stream = Box::new(InMemoryStream::new(records));

        let mut distinct_stream = DistinctStream::new(stream);

        let result = distinct_stream.next().unwrap();
        assert_eq!(None, result);
    }

    #[test]
    fn test_record_merge() {
        let left = Record::new(
            &vec!["a".to_string(), "b".to_string()],
            vec![Value::Int(1), Value::Int(2)],
        );
        let right = Record::new(
            &vec!["c".to_string(), "d".to_string()],
            vec![Value::Int(3), Value::Int(4)],
        );

        let merged = left.merge(&right);
        let expected = Record::new(
            &vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)],
        );

        assert_eq!(expected, merged);
    }

    #[test]
    fn test_record_merge_overlapping_keys() {
        // When keys overlap, the right record's values should take precedence
        let left = Record::new(
            &vec!["a".to_string(), "b".to_string()],
            vec![Value::Int(1), Value::Int(2)],
        );
        let right = Record::new(
            &vec!["b".to_string(), "c".to_string()],
            vec![Value::Int(99), Value::Int(3)],
        );

        let merged = left.merge(&right);
        // b should be overwritten with 99
        let path_b = ast::PathExpr::new(vec![ast::PathSegment::AttrName("b".to_string())]);
        assert_eq!(Value::Int(99), merged.get(&path_b));
    }

    #[test]
    fn test_groupby_count_skips_null() {
        // count(a) should not count null values
        let path_expr_a = ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())]);
        let mut records = VecDeque::new();
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(1)]));
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Null]));
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(3)]));
        let stream = Box::new(InMemoryStream::new(records));
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let aggregates = vec![
            types::NamedAggregate::new(
                types::Aggregate::Count(
                    types::CountAggregate::new(),
                    types::Named::Expression(
                        types::Expression::Variable(path_expr_a),
                        Some("a".to_string()),
                    ),
                ),
                Some("cnt".to_string()),
            ),
        ];

        let mut stream = GroupByStream::new(vec![], Variables::default(), aggregates, stream, registry);

        let record = stream.next().unwrap().unwrap();
        assert_eq!(record.to_variables()["cnt"], Value::Int(2));
        assert!(stream.next().unwrap().is_none());
    }

    #[test]
    fn test_groupby_count_star_counts_all() {
        let mut records = VecDeque::new();
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(1)]));
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Null]));
        records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(3)]));
        let stream = Box::new(InMemoryStream::new(records));
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let aggregates = vec![
            types::NamedAggregate::new(
                types::Aggregate::Count(types::CountAggregate::new(), types::Named::Star),
                Some("cnt".to_string()),
            ),
        ];

        let mut stream = GroupByStream::new(vec![], Variables::default(), aggregates, stream, registry);

        let record = stream.next().unwrap().unwrap();
        assert_eq!(record.to_variables()["cnt"], Value::Int(3));
        assert!(stream.next().unwrap().is_none());
    }

    #[test]
    fn test_record_get_field_value() {
        let mut vars = Variables::default();
        vars.insert("status".to_string(), Value::Int(200));
        vars.insert("host".to_string(), Value::String("example.com".to_string()));
        let record = Record::new_with_variables(vars);

        assert_eq!(record.get_field_value("status"), Some(&Value::Int(200)));
        assert_eq!(record.get_field_value("host"), Some(&Value::String("example.com".to_string())));
        assert_eq!(record.get_field_value("missing"), None);
    }

    #[test]
    fn test_groupby_empty_global_aggregate() {
        // SELECT count(*), sum(a) FROM empty_table should return one row: (0, NULL)
        let path_expr_a = ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())]);
        let records = VecDeque::new();
        let stream = Box::new(InMemoryStream::new(records));
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let aggregates = vec![
            types::NamedAggregate::new(
                types::Aggregate::Count(types::CountAggregate::new(), types::Named::Star),
                Some("cnt".to_string()),
            ),
            types::NamedAggregate::new(
                types::Aggregate::Sum(
                    types::SumAggregate::new(),
                    types::Named::Expression(
                        types::Expression::Variable(path_expr_a),
                        Some("a".to_string()),
                    ),
                ),
                Some("total".to_string()),
            ),
        ];

        let mut stream = GroupByStream::new(vec![], Variables::default(), aggregates, stream, registry);

        let record = stream.next().unwrap().unwrap();
        assert_eq!(record.to_variables()["cnt"], Value::Int(0));
        assert_eq!(record.to_variables()["total"], Value::Null);
        assert!(stream.next().unwrap().is_none());
    }

    #[test]
    fn test_join_key_single_int_hash_eq() {
        let k1 = JoinKey::SingleInt(42);
        let k2 = JoinKey::SingleInt(42);
        let k3 = JoinKey::SingleInt(99);
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);

        // Verify equal keys produce equal hashes
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        k1.hash(&mut h1);
        k2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_join_key_single_string_hash_eq() {
        let k1 = JoinKey::SingleString("hello".to_string());
        let k2 = JoinKey::SingleString("hello".to_string());
        let k3 = JoinKey::SingleString("world".to_string());
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_join_key_no_cross_variant_collision() {
        let k_int = JoinKey::SingleInt(42);
        let k_comp = JoinKey::Composite(vec![Value::Int(42)]);
        assert_ne!(k_int, k_comp);
    }

    #[test]
    fn test_extract_key_single_int() {
        let mut vars = Variables::default();
        vars.insert("id".to_string(), Value::Int(42));
        let record = Record::new_with_variables(vars);
        let key = extract_key(&record, &[path("id")]);
        assert_eq!(key, JoinKey::SingleInt(42));
    }

    #[test]
    fn test_extract_key_single_string() {
        let mut vars = Variables::default();
        vars.insert("name".to_string(), Value::String("alice".to_string()));
        let record = Record::new_with_variables(vars);
        let key = extract_key(&record, &[path("name")]);
        assert_eq!(key, JoinKey::SingleString("alice".to_string()));
    }

    #[test]
    fn test_extract_key_null_returns_composite() {
        let mut vars = Variables::default();
        vars.insert("id".to_string(), Value::Null);
        let record = Record::new_with_variables(vars);
        let key = extract_key(&record, &[path("id")]);
        assert!(matches!(key, JoinKey::Composite(_)));
    }

    #[test]
    fn test_extract_key_multi_column() {
        let mut vars = Variables::default();
        vars.insert("a".to_string(), Value::Int(1));
        vars.insert("b".to_string(), Value::String("x".to_string()));
        let record = Record::new_with_variables(vars);
        let key = extract_key(&record, &[path("a"), path("b")]);
        assert_eq!(
            key,
            JoinKey::Composite(vec![Value::Int(1), Value::String("x".to_string())])
        );
    }

    #[test]
    fn test_extract_key_missing_field() {
        let vars = Variables::default();
        let record = Record::new_with_variables(vars);
        let key = extract_key(&record, &[path("nonexistent")]);
        assert_eq!(key, JoinKey::Composite(vec![Value::Missing]));
    }

    fn path(name: &str) -> ast::PathExpr {
        ast::PathExpr::new(vec![ast::PathSegment::AttrName(name.to_string())])
    }

    fn record_from_pairs(pairs: Vec<(&str, Value)>) -> Record {
        let mut vars = Variables::default();
        for (k, v) in pairs {
            vars.insert(k.to_string(), v);
        }
        Record::new_with_variables(vars)
    }

    fn in_memory_stream(records: Vec<Record>) -> Box<InMemoryStream> {
        Box::new(InMemoryStream::new(records.into_iter().collect()))
    }

    #[test]
    fn test_hash_join_inner_basic() {
        let left_records = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
            record_from_pairs(vec![("id", Value::Int(2)), ("x", Value::String("b".into()))]),
        ];
        let right_records = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("c".into()))]),
            record_from_pairs(vec![("id", Value::Int(3)), ("y", Value::String("d".into()))]),
        ];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        let mut join = HashJoinStream::new(
            in_memory_stream(left_records),
            in_memory_stream(right_records),
            vec![path("id")],
            vec![path("id")],
            None,
            types::LogicalJoinType::Inner,
            512 * 1024 * 1024,
            registry,
        );
        let mut results = Vec::new();
        while let Some(r) = join.next().unwrap() {
            results.push(r);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_field_value("x"), Some(&Value::String("a".into())));
        assert_eq!(results[0].get_field_value("y"), Some(&Value::String("c".into())));
    }

    #[test]
    fn test_hash_join_left_with_null_padding() {
        let left_records = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
            record_from_pairs(vec![("id", Value::Int(2)), ("x", Value::String("b".into()))]),
        ];
        let right_records = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("c".into()))]),
        ];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        let mut join = HashJoinStream::new(
            in_memory_stream(left_records),
            in_memory_stream(right_records),
            vec![path("id")], vec![path("id")],
            None, types::LogicalJoinType::Left, 512 * 1024 * 1024,
            registry,
        );
        let mut results = Vec::new();
        while let Some(r) = join.next().unwrap() {
            results.push(r);
        }
        assert_eq!(results.len(), 2);
        // First row matched
        assert_eq!(results[0].get_field_value("y"), Some(&Value::String("c".into())));
        // Second row: NULL-padded right side
        assert_eq!(results[1].get_field_value("y"), Some(&Value::Null));
    }

    #[test]
    fn test_hash_join_null_keys_no_match() {
        let left = vec![record_from_pairs(vec![("id", Value::Null)])];
        let right = vec![record_from_pairs(vec![("id", Value::Null), ("y", Value::Int(1))])];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        let mut join = HashJoinStream::new(
            in_memory_stream(left), in_memory_stream(right),
            vec![path("id")], vec![path("id")],
            None, types::LogicalJoinType::Inner, 512 * 1024 * 1024,
            registry,
        );
        let result = join.next().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_hash_join_many_to_many() {
        let left = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
            record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("b".into()))]),
        ];
        let right = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("c".into()))]),
            record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("d".into()))]),
        ];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        let mut join = HashJoinStream::new(
            in_memory_stream(left), in_memory_stream(right),
            vec![path("id")], vec![path("id")],
            None, types::LogicalJoinType::Inner, 512 * 1024 * 1024,
            registry,
        );
        let mut results = Vec::new();
        while let Some(r) = join.next().unwrap() {
            results.push(r);
        }
        // 2 left x 2 right = 4 matches
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_hash_join_empty_right() {
        let left = vec![
            record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
        ];
        let right: Vec<Record> = vec![];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        // INNER JOIN with empty right → 0 results
        let mut join = HashJoinStream::new(
            in_memory_stream(left.clone()), in_memory_stream(right.clone()),
            vec![path("id")], vec![path("id")],
            None, types::LogicalJoinType::Inner, 512 * 1024 * 1024,
            registry.clone(),
        );
        assert!(join.next().unwrap().is_none());

        // LEFT JOIN with empty right → left row with no right field names (no padding possible)
        let mut join = HashJoinStream::new(
            in_memory_stream(left), in_memory_stream(right),
            vec![path("id")], vec![path("id")],
            None, types::LogicalJoinType::Left, 512 * 1024 * 1024,
            registry,
        );
        let result = join.next().unwrap();
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.get_field_value("x"), Some(&Value::String("a".into())));
    }

    #[test]
    fn test_hash_join_string_keys() {
        let left = vec![
            record_from_pairs(vec![("name", Value::String("alice".into())), ("v", Value::Int(1))]),
            record_from_pairs(vec![("name", Value::String("bob".into())), ("v", Value::Int(2))]),
        ];
        let right = vec![
            record_from_pairs(vec![("name", Value::String("alice".into())), ("w", Value::Int(10))]),
        ];
        let registry = Arc::new(crate::functions::registry::FunctionRegistry::new());
        let mut join = HashJoinStream::new(
            in_memory_stream(left), in_memory_stream(right),
            vec![path("name")], vec![path("name")],
            None, types::LogicalJoinType::Inner, 512 * 1024 * 1024,
            registry,
        );
        let mut results = Vec::new();
        while let Some(r) = join.next().unwrap() {
            results.push(r);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_field_value("v"), Some(&Value::Int(1)));
        assert_eq!(results[0].get_field_value("w"), Some(&Value::Int(10)));
    }
}
