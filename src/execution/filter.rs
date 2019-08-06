use crate::common::types::Variables;
use crate::execution::types::{Formula, Record, RecordStream, StreamResult};
use std::rc::Rc;

pub(crate) struct FilteredStream {
    formula: Rc<Formula>,
    variables: Variables,
    source: Box<dyn RecordStream>,
}

impl FilteredStream {
    pub(crate) fn new(formula: Rc<Formula>, variables: Variables, source: Box<dyn RecordStream>) -> Self {
        FilteredStream {
            formula,
            variables,
            source,
        }
    }
}

impl RecordStream for FilteredStream {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::datasource::tests::InMemoryStream;
    use crate::execution::types;
    use crate::execution::types::{Record, RecordStream};
    use std::collections::VecDeque;

    #[test]
    #[ignore]
    fn test_filtered_stream() {
        let left = Box::new(types::Expression::Variable("host".to_string()));
        let right = Box::new(types::Expression::Variable("const".to_string()));
        let rel = types::Relation::Equal;
        let predicate = Rc::new(types::Formula::Predicate(rel, left, right));

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

        let mut filtered_stream = FilteredStream::new(predicate, variables, stream);

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
