use crate::execution::types::{Record, RecordStream, StreamError, StreamResult};
use std::collections::VecDeque;

#[derive(Debug)]
pub(crate) struct InMemoryStream {
    pub(crate) data: VecDeque<Record>,
}

impl InMemoryStream {
    fn new(data: VecDeque<Record>) -> InMemoryStream {
        InMemoryStream { data }
    }
}

impl RecordStream for InMemoryStream {
    fn next(&mut self) -> StreamResult<Record> {
        if let Some(record) = self.data.pop_front() {
            Ok(record)
        } else {
            Err(StreamError::EndOfStream)
        }
    }

    fn close(&self) {}
}
