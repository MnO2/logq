use crate::execution::types::{GetResult, NamedExpression, Node, Record, RecordStream, StreamResult, Value, Variables};
use std::rc::Rc;

pub(crate) struct Map {
    expressions: Vec<Rc<dyn NamedExpression>>,
    source: Box<dyn Node>,
}

impl Map {
    fn new(expressions: Vec<Box<dyn NamedExpression>>, source: Box<dyn Node>) -> Self {
        let mut exps = Vec::new();

        for ne in expressions.into_iter() {
            exps.push(Rc::from(ne));
        }

        Map {
            expressions: exps,
            source,
        }
    }
}

impl Node for Map {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>> {
        let record_stream = self.source.get(variables.clone())?;

        let stream = MappedStream {
            expressions: self.expressions.clone(),
            variables,
            source: record_stream,
        };

        Ok(Box::new(stream))
    }
}

pub(crate) struct MappedStream {
    pub(crate) expressions: Vec<Rc<dyn NamedExpression>>,
    pub(crate) variables: Variables,
    pub(crate) source: Box<dyn RecordStream>,
}

impl RecordStream for MappedStream {
    fn close(&self) {
        self.source.close();
    }

    fn next(&mut self) -> StreamResult<Record> {
        let record = self.source.next()?;
        let variables = self.variables.clone();

        let mut field_names = Vec::new();
        let mut data = Vec::new();
        for i in 0..self.expressions.len() {
            let field_name = self.expressions[i].name();
            field_names.push(field_name.clone());
            let v = self.expressions[i].expression_value(variables.clone())?;
            data.push(v);
        }

        let record = Record { field_names, data };

        Ok(record)
    }
}
