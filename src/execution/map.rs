use crate::common::types::Variables;
use crate::execution::types::{Named, Record, RecordStream, StreamResult};

pub(crate) struct MappedStream {
    pub(crate) named_list: Vec<Named>,
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
        Ok(record)
    }
}
