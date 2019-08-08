use super::datasource::RecordRead;
use super::types::{Formula, Named, StreamResult};
use crate::common;
use crate::common::types::{Value, VariableName, Variables};
use prettytable::Cell;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Record {
    field_names: Vec<VariableName>,
    data: Vec<Value>,
}

impl Record {
    pub(crate) fn new(field_names: Vec<VariableName>, data: Vec<Value>) -> Self {
        Record { field_names, data }
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
                        let v = expr.expression_value(variables.clone())?;
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
            let predicate = self.formula.evaluate(variables)?;

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
    use std::collections::VecDeque;

    #[derive(Debug)]
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
