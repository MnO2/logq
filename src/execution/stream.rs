use super::datasource::RecordRead;
use super::types::{Aggregate, Formula, Named, NamedAggregate, StreamResult};
use crate::common;
use crate::common::types::{Tuple, Value, VariableName, Variables};
use prettytable::Cell;
use std::collections::hash_set;
use std::collections::VecDeque;
use hashbrown::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Record {
    name2idx: HashMap<VariableName, usize>,
    field_names: Vec<VariableName>,
    data: Vec<Value>,
}

impl Record {
    pub(crate) fn new(field_names: Vec<VariableName>, data: Vec<Value>) -> Self {
        let mut name2idx = HashMap::default();
        for i in 0..field_names.len() {
            name2idx.insert(field_names[i].clone(), i);
        }

        Record { name2idx, field_names, data }
    }

    pub(crate) fn get(&self, field_name: &VariableName) -> Option<Value> {
        if let Some(&idx) = self.name2idx.get(field_name) {
            return Some(self.data[idx].clone());
        }

        None
    }

    pub(crate) fn get_many(&self, field_names: &[VariableName]) -> Vec<Value> {
        let mut ret = Vec::with_capacity(field_names.len());
        for name in field_names.iter() {
            if let Some(&idx) = self.name2idx.get(name) {
                ret.push(self.data[idx].clone());
            }
        }

        ret
    }

    pub(crate) fn to_variables(&self) -> Variables {
        let mut variables = Variables::default();

        for i in 0..self.field_names.len() {
            variables.insert(self.field_names[i].clone(), self.data[i].clone());
        }

        variables
    }

    pub(crate) fn to_tuples(&self) -> Vec<(VariableName, Value)> {
        let mut res = Vec::new();
        for i in 0..self.field_names.len() {
            res.push((self.field_names[i].clone(), self.data[i].clone()));
        }

        res
    }

    pub(crate) fn to_row(&self) -> Vec<Cell> {
        self.data
            .iter()
            .map(|val| match val {
                Value::String(s) => Cell::new(&*s),
                Value::Int(i) => Cell::new(&*i.to_string()),
                Value::Float(f) => Cell::new(&*f.to_string()),
                Value::Boolean(b) => Cell::new(&*b.to_string()),
                Value::Null => Cell::new("<null>"),
                Value::DateTime(dt) => Cell::new(&*dt.to_string()),
                Value::HttpRequest(request) => Cell::new(&*request.to_string()),
                Value::Host(host) => Cell::new(&*host.to_string()),
            })
            .collect()
    }

    pub(crate) fn to_csv_record(&self) -> Vec<String> {
        self.data
            .iter()
            .map(|val| match val {
                Value::String(s) => s.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::Null => "<null>".to_string(),
                Value::DateTime(dt) => dt.to_string(),
                Value::HttpRequest(request) => request.to_string(),
                Value::Host(host) => host.to_string(),
            })
            .collect()
    }
}

pub(crate) trait RecordStream {
    fn next(&mut self) -> StreamResult<Option<Record>>;
    fn close(&self);
}

pub(crate) struct MapStream {
    pub(crate) named_list: Vec<Named>,
    pub(crate) variables: Variables,
    pub(crate) source: Box<dyn RecordStream>,
}

impl MapStream {
    pub(crate) fn new(named_list: Vec<Named>, variables: Variables, source: Box<dyn RecordStream>) -> Self {
        MapStream {
            named_list,
            variables,
            source,
        }
    }
}

impl RecordStream for MapStream {
    fn close(&self) {
        self.source.close();
    }

    fn next(&mut self) -> StreamResult<Option<Record>> {
        if let Some(record) = self.source.next()? {
            let variables = common::types::merge(self.variables.clone(), record.to_variables());

            let mut field_names = Vec::new();
            let mut data = Vec::new();
            for (idx, named) in self.named_list.iter().enumerate() {
                match named {
                    Named::Expression(expr, name_opt) => {
                        let name = if let Some(name) = name_opt {
                            name.clone()
                        } else {
                            //Give the column a positional name if not provided.
                            format!("{:02}", idx)
                        };

                        field_names.push(name);
                        let v = expr.expression_value(&variables)?;
                        data.push(v);
                    }
                    Named::Star => {
                        for (k, v) in record.to_tuples().into_iter() {
                            field_names.push(k);
                            data.push(v);
                        }
                    }
                }
            }

            let record = Record::new(field_names, data);
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct LimitStream {
    curr: u32,
    row_count: u32,
    source: Box<dyn RecordStream>,
}

impl LimitStream {
    pub(crate) fn new(row_count: u32, source: Box<dyn RecordStream>) -> Self {
        LimitStream {
            curr: 0,
            row_count,
            source,
        }
    }
}

impl RecordStream for LimitStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
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

pub(crate) struct FilterStream {
    formula: Formula,
    variables: Variables,
    source: Box<dyn RecordStream>,
}

impl FilterStream {
    pub(crate) fn new(formula: Formula, variables: Variables, source: Box<dyn RecordStream>) -> Self {
        FilterStream {
            formula,
            variables,
            source,
        }
    }
}

impl RecordStream for FilterStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        while let Some(record) = self.source.next()? {
            let variables = common::types::merge(self.variables.clone(), record.to_variables());
            let predicate = self.formula.evaluate(&variables)?;

            if predicate {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn close(&self) {
        self.source.close();
    }
}

pub(crate) struct InMemoryStream {
    pub(crate) data: VecDeque<Record>,
}

impl InMemoryStream {
    pub(crate) fn new(data: VecDeque<Record>) -> InMemoryStream {
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

pub(crate) struct GroupByStream {
    keys: Vec<VariableName>,
    variables: Variables,
    aggregates: Vec<NamedAggregate>,
    source: Box<dyn RecordStream>,
    group_iterator: Option<hash_set::IntoIter<Option<Tuple>>>,
}

impl<'a> GroupByStream {
    pub(crate) fn new(
        keys: Vec<VariableName>,
        variables: Variables,
        aggregates: Vec<NamedAggregate>,
        source: Box<dyn RecordStream>,
    ) -> Self {
        GroupByStream {
            keys,
            variables,
            aggregates,
            source,
            group_iterator: None,
        }
    }
}

impl RecordStream for GroupByStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        if self.group_iterator.is_none() {
            let mut groups: hash_set::HashSet<Option<Tuple>> = hash_set::HashSet::new();
            while let Some(record) = self.source.next()? {
                let variables = common::types::merge(self.variables.clone(), record.to_variables());
                let key = if self.keys.is_empty() {
                    None
                } else {
                    Some(record.get_many(&self.keys))
                };

                groups.insert(key.clone());
                for named_agg in self.aggregates.iter_mut() {
                    match &mut named_agg.aggregate {
                        Aggregate::Avg(ref mut inner, named) => {
                            let val = match named {
                                Named::Expression(expr, _) => expr.expression_value(&variables)?,
                                Named::Star => {
                                    unreachable!();
                                }
                            };

                            inner.add_record(key.clone(), val)?;
                        }
                        Aggregate::Count(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    inner.add_row(key.clone())?;
                                }
                            };
                        }
                        Aggregate::First(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Last(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Max(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Min(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::Sum(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::ApproxCountDistinct(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&variables)?;
                                    inner.add_record(key.clone(), val)?;
                                }
                                Named::Star => {
                                    unreachable!();
                                }
                            };
                        }
                        Aggregate::PercentileDisc(ref mut inner, column_name) => {
                            let val = variables.get(column_name).unwrap();
                            inner.add_record(key.clone(), val.clone())?;
                        }
                        Aggregate::ApproxPercentile(ref mut inner, column_name) => {
                            let val = variables.get(column_name).unwrap();
                            inner.add_record(key.clone(), val.clone())?;
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
                for k in self.keys.iter() {
                    fields.push(k.clone());
                }

                for v in values_in_key {
                    values.push(v.clone());
                }
            }

            for named_agg in self.aggregates.iter_mut() {
                if let Some(ref field_name) = named_agg.name_opt {
                    fields.push(field_name.clone());
                } else {
                    //FIXME: Insert empty string for now
                    fields.push("".to_string());
                }
                let v = named_agg.aggregate.get_aggregated(&key)?;
                values.push(v);
            }

            let record = Record::new(fields, values);
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn close(&self) {
        self.source.close();
    }
}

pub(crate) struct LogFileStream {
    pub(crate) reader: Box<dyn RecordRead>,
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
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut limit_stream = LimitStream::new(1, stream);

        let mut result = Vec::new();
        while let Some(n) = limit_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        )];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_filter_stream() {
        let left = Box::new(types::Expression::Variable("host".to_string()));
        let right = Box::new(types::Expression::Variable("const".to_string()));
        let rel = types::Relation::Equal;
        let predicate = types::Formula::Predicate(rel, left, right);

        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut filtered_stream = FilterStream::new(predicate, variables, stream);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![Record::new(
            vec!["host".to_string(), "port".to_string()],
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
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut filtered_stream = MapStream::new(named_list, variables, stream);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![
            Record::new(
                vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example01.com".to_string()), Value::Int(8000)],
            ),
            Record::new(
                vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example.com".to_string()), Value::Int(8001)],
            ),
            Record::new(
                vec!["host".to_string(), "port".to_string()],
                vec![Value::String("example01.com".to_string()), Value::Int(8002)],
            ),
        ];

        assert_eq!(expected, result);
    }

    #[test]
    fn test_map_stream_with_names() {
        let named_list = vec![Named::Expression(
            Expression::Variable("port".to_string()),
            Some("port".to_string()),
        )];

        let mut variables: Variables = Variables::default();
        variables.insert("const".to_string(), Value::String("example.com".to_string()));

        let mut records = VecDeque::new();
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8000)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        ));
        records.push_back(Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example01.com".to_string()), Value::Int(8002)],
        ));
        let stream = Box::new(InMemoryStream::new(records));

        let mut filtered_stream = MapStream::new(named_list, variables, stream);

        let mut result = Vec::new();
        while let Some(n) = filtered_stream.next().unwrap() {
            result.push(n);
        }

        let expected = vec![
            Record::new(vec!["port".to_string()], vec![Value::Int(8000)]),
            Record::new(vec!["port".to_string()], vec![Value::Int(8001)]),
            Record::new(vec!["port".to_string()], vec![Value::Int(8002)]),
        ];

        assert_eq!(expected, result);
    }
}
