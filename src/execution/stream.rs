use super::datasource::{Reader, StringRecord};
use super::types::{Formula, Named, Record, RecordStream, StreamResult};
use crate::common::types::Variables;
use std::fs::File;

pub(crate) struct MapStream {
    pub(crate) named_list: Vec<Named>,
    pub(crate) variables: Variables,
    pub(crate) source: Box<dyn RecordStream>,
}

impl RecordStream for MapStream {
    fn close(&self) {
        self.source.close();
    }

    fn next(&mut self) -> StreamResult<Option<Record>> {
        let record = self.source.next()?;
        let variables = self.variables.clone();

        let mut field_names = Vec::new();
        let mut data = Vec::new();
        for named in self.named_list.iter() {
            match named {
                Named::Expression(expr, name) => {
                    field_names.push(name.clone());
                    let v = expr.expression_value(variables.clone())?;
                    data.push(v);
                }
                Named::Star => {
                    //TODO: Insert all of the variables in records
                    unimplemented!();
                }
            }
        }

        let record = Record::new(field_names, data);
        Ok(Some(record))
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
        loop {
            let record = self.source.next()?;
            let variables = self.variables.clone();
            let predicate = self.formula.evaluate(variables)?;

            if predicate {
                return Ok(record);
            }
        }
    }

    fn close(&self) {
        self.source.close();
    }
}

pub(crate) struct LogFileStream {
    pub(crate) reader: Reader<File>,
}

impl RecordStream for LogFileStream {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        let mut record = StringRecord::new();
        if self.reader.read_record(&mut record)? {
            let field_names = Vec::new();
            let data = Vec::new();

            let record = Record::new(field_names, data);
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
    use crate::execution::types;
    use crate::execution::types::{Record, RecordStream};
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
    #[ignore]
    fn test_filtered_stream() {
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
}
