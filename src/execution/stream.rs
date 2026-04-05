use super::datasource::RecordRead;
use super::types::{Aggregate, Expression, Formula, Named, NamedAggregate, Node, StreamResult};
use crate::common;
use crate::common::types::{Tuple, Value, VariableName, Variables};
use crate::functions::FunctionRegistry;
use crate::syntax::ast::{self, PathSegment};
use linked_hash_map::LinkedHashMap;
use prettytable::Cell;
use std::collections::hash_set;
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
        let projection_map = if simple_projection {
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
        MapStream {
            named_list,
            column_names,
            variables,
            source,
            registry,
            simple_projection,
            is_star_only,
            projection_map,
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
            // Move values out of source record instead of cloning.
            if self.simple_projection {
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
        if let Some(record) = self.data.pop_front() {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn close(&self) {}
}

pub struct GroupByStream {
    keys: Vec<ast::PathExpr>,
    variables: Variables,
    aggregates: Vec<NamedAggregate>,
    source: Box<dyn RecordStream>,
    group_iterator: Option<hash_set::IntoIter<Option<Tuple>>>,
    registry: Arc<FunctionRegistry>,
}

impl<'a> GroupByStream {
    pub fn new(
        keys: Vec<ast::PathExpr>,
        variables: Variables,
        aggregates: Vec<NamedAggregate>,
        source: Box<dyn RecordStream>,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        GroupByStream {
            keys,
            variables,
            aggregates,
            source,
            group_iterator: None,
            registry,
        }
    }
}

impl RecordStream for GroupByStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if self.group_iterator.is_none() {
            let mut groups: hash_set::HashSet<Option<Tuple>> = hash_set::HashSet::new();
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

                if !groups.contains(&key) {
                    groups.insert(key.clone());
                }
                for named_agg in self.aggregates.iter_mut() {
                    match &mut named_agg.aggregate {
                        Aggregate::GroupAs(ref mut inner, named) => {
                            let val = match named {
                                Named::Expression(_expr, _) => Value::Object(Box::new(record.to_variables().clone())),
                                Named::Star => {
                                    unreachable!();
                                }
                            };

                            inner.add_record(&key, &val)?;
                        }
                        Aggregate::Avg(ref mut inner, named) => {
                            let val = match named {
                                Named::Expression(expr, _) => expr.expression_value(&variables, &self.registry)?,
                                Named::Star => {
                                    unreachable!();
                                }
                            };

                            inner.add_record(&key, &val)?;
                        }
                        Aggregate::Count(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    inner.add_row(&key)?;
                                }
                            };
                        }
                        Aggregate::First(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Last(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Max(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Min(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Sum(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::ApproxCountDistinct(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables, &self.registry)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::PercentileDisc(ref mut inner, column_name) => {
                            let val = variables.get(column_name).unwrap();
                            inner.add_record(&key, val)?;
                        }
                        Aggregate::ApproxPercentile(ref mut inner, column_name) => {
                            let val = variables.get(column_name).unwrap();
                            inner.add_record(&key, val)?;
                        }
                    }
                }
            }

            self.group_iterator = Some(groups.into_iter());
        }

        let iter = self.group_iterator.as_mut().unwrap();
        if let Some(key) = iter.next() {
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

            for named_agg in self.aggregates.iter_mut() {
                if let Some(ref field_name) = named_agg.name_opt {
                    fields.push(field_name.clone());
                } else {
                    let idx = fields.len() + 1;
                    fields.push(format!("_{}", idx));
                }
                let v = named_agg.aggregate.get_aggregated(&key)?;
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
    right_stream: Option<Box<dyn RecordStream>>,
    registry: Arc<FunctionRegistry>,
}

impl CrossJoinStream {
    pub(crate) fn new(left: Box<dyn RecordStream>, right_node: Node, right_variables: Variables, registry: Arc<FunctionRegistry>) -> Self {
        CrossJoinStream {
            left,
            right_node,
            right_variables,
            current_left: None,
            right_stream: None,
            registry,
        }
    }
}

impl RecordStream for CrossJoinStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        loop {
            // If we have a right stream, try to get next right record
            if let Some(ref mut right) = self.right_stream {
                if let Some(right_record) = right.next()? {
                    // Merge left and right records
                    let left = self.current_left.as_ref().unwrap();
                    let merged = left.merge(&right_record);
                    return Ok(Some(merged));
                }
            }
            // Get next left record and reset right stream
            match self.left.next()? {
                Some(left_record) => {
                    self.current_left = Some(left_record);
                    // Re-create right stream from scratch
                    let right_stream = self.right_node.get(self.right_variables.clone(), self.registry.clone())
                        .map_err(|e| super::types::StreamError::Get(e))?;
                    self.right_stream = Some(right_stream);
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
    right_stream: Option<Box<dyn RecordStream>>,
    matched: bool,
    right_field_names: Option<Vec<String>>,
    registry: Arc<FunctionRegistry>,
}

impl LeftJoinStream {
    pub(crate) fn new(
        left: Box<dyn RecordStream>,
        right_node: Node,
        right_variables: Variables,
        condition: Formula,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        LeftJoinStream {
            left,
            right_node,
            right_variables,
            condition,
            current_left: None,
            right_stream: None,
            matched: false,
            right_field_names: None,
            registry,
        }
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
        loop {
            // If we have a right stream, try to get next right record
            if let Some(ref mut right) = self.right_stream {
                if let Some(right_record) = right.next()? {
                    // Cache right field names from first right record seen
                    if self.right_field_names.is_none() {
                        self.right_field_names = Some(
                            right_record
                                .to_variables()
                                .keys()
                                .cloned()
                                .collect(),
                        );
                    }

                    let left = self.current_left.as_ref().unwrap();
                    let merged = left.merge(&right_record);

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
                // Right stream exhausted for this left row
            }

            // If we just finished scanning right for a left row and found no match,
            // emit left row with NULL padding for right columns
            if self.current_left.is_some() && !self.matched {
                let left = self.current_left.take().unwrap();
                let null_right = self.null_padded_right_record();
                let merged = left.merge(&null_right);
                // Reset state before returning
                self.right_stream = None;
                // Now fall through to get next left row on next call
                // But first return this result
                return Ok(Some(merged));
            }

            // Get next left record and reset right stream
            match self.left.next()? {
                Some(left_record) => {
                    self.current_left = Some(left_record);
                    self.matched = false;
                    // Re-create right stream from scratch
                    let right_stream = self
                        .right_node
                        .get(self.right_variables.clone(), self.registry.clone())
                        .map_err(|e| super::types::StreamError::Get(e))?;
                    self.right_stream = Some(right_stream);

                    // If we don't have right_field_names yet, peek at first right record
                    // to learn them (needed for NULL padding if no match at all)
                    if self.right_field_names.is_none() {
                        // Create a temporary stream to discover field names
                        let mut peek_stream = self
                            .right_node
                            .get(self.right_variables.clone(), self.registry.clone())
                            .map_err(|e| super::types::StreamError::Get(e))?;
                        if let Some(peek_record) = peek_stream.next()? {
                            self.right_field_names = Some(
                                peek_record
                                    .to_variables()
                                    .keys()
                                    .cloned()
                                    .collect(),
                            );
                        }
                    }
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
}
