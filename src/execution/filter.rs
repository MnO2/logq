use crate::execution::types::{Formula, GetResult, Node, Record, RecordStream, StreamResult, Variables};
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
    fn next(&self) -> StreamResult<Record> {
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
