use crate::execution::logic::Predicate;
use crate::execution::relation::Equal;
use crate::execution::types::{
    Formula, GetResult, Node, Record, RecordStream, StreamError, StreamResult, Value, Variable, Variables,
};
use hashbrown::HashMap;
use std::rc::Rc;

pub(crate) struct FilterIterator {
    pub(crate) source: Box<dyn Node>,
    pub(crate) formula: Rc<dyn Formula>,
}

impl FilterIterator {
    fn new(source: Box<dyn Node>, formula: Box<dyn Formula>) -> Self {
        let formula = Rc::from(formula);
        FilterIterator { source, formula }
    }

    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>> {
        let record_stream = self.source.get(variables.clone())?;
        let stream = FilteredStream::new(self.formula.clone(), variables, record_stream);
        Ok(Box::new(stream))
    }
}

pub(crate) struct FilteredStream {
    formula: Rc<dyn Formula>,
    variables: Variables,
    source: Box<dyn RecordStream>,
}

impl FilteredStream {
    fn new(formula: Rc<dyn Formula>, variables: Variables, source: Box<dyn RecordStream>) -> Self {
        FilteredStream {
            formula,
            variables,
            source,
        }
    }
}

impl RecordStream for FilteredStream {
    fn next(&mut self) -> StreamResult<Record> {
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
    use crate::datasource::in_memory_stream::InMemoryStream;
    use std::collections::VecDeque;

    #[test]
    fn test_filtered_stream() {
        let left = Box::new(Variable::new("host".to_string()));
        let right = Box::new(Variable::new("const".to_string()));
        let rel = Box::new(Equal::new());
        let predicate = Rc::new(Predicate::new(left, right, rel));

        let mut variables: Variables = HashMap::new();
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
        loop {
            let n = filtered_stream.next();
            match n {
                Ok(r) => result.push(r),
                Err(e) => {
                    if e == StreamError::EndOfStream {
                        break;
                    } else {
                        panic!("{:?}", e);
                    }
                }
            }
        }

        let expected = vec![Record::new(
            vec!["host".to_string(), "port".to_string()],
            vec![Value::String("example.com".to_string()), Value::Int(8001)],
        )];

        assert_eq!(expected, result);
    }
}
