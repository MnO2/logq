use crate::common::types::Variables;
use crate::execution::types::{NamedExpression, Record, RecordStream, StreamResult};

pub(crate) struct MappedStream {
    pub(crate) expressions: Vec<NamedExpression>,
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
            let field_name = self.expressions[i].name.clone();
            field_names.push(field_name.clone());
            let v = self.expressions[i].expr.expression_value(variables.clone())?;
            data.push(v);
        }

        let record = Record::new(field_names, data);
        Ok(record)
    }
}
