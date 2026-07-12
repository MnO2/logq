use crate::common::types::Value;
use crate::execution::stream::Record;
use crate::execution::types::{StreamError, StreamResult};

use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct MemoryState {
    limit: usize,
    used: usize,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryTracker {
    state: Option<Arc<Mutex<MemoryState>>>,
}

impl MemoryTracker {
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            state: limit.map(|limit| Arc::new(Mutex::new(MemoryState { limit, used: 0 }))),
        }
    }

    pub(crate) fn limit(&self) -> Option<usize> {
        self.state.as_ref().map(|state| state.lock().unwrap().limit)
    }

    pub(crate) fn add(&self, bytes: usize) -> StreamResult<()> {
        let Some(state) = &self.state else {
            return Ok(());
        };
        let mut state = state.lock().unwrap();
        state.used = state.used.saturating_add(bytes);
        if state.used > state.limit {
            return Err(StreamError::MemoryBudgetExceeded);
        }
        Ok(())
    }

    pub(crate) fn replace(&self, removed: usize, added: usize) -> StreamResult<()> {
        let Some(state) = &self.state else {
            return Ok(());
        };
        let mut state = state.lock().unwrap();
        state.used = state.used.saturating_sub(removed).saturating_add(added);
        if state.used > state.limit {
            return Err(StreamError::MemoryBudgetExceeded);
        }
        Ok(())
    }
}

pub(crate) fn estimate_record(record: &Record) -> usize {
    64usize.saturating_add(
        record
            .to_variables()
            .iter()
            .map(|(key, value)| 48usize.saturating_add(key.len()).saturating_add(estimate_value(value)))
            .sum(),
    )
}

pub(crate) fn estimate_values(values: &[Value]) -> usize {
    24usize.saturating_add(values.iter().map(estimate_value).sum())
}

pub(crate) fn estimate_value(value: &Value) -> usize {
    match value {
        Value::Int(_) | Value::Float(_) | Value::Boolean(_) => 16,
        Value::String(value) => 24usize.saturating_add(value.len()),
        Value::DateTime(_) => 24,
        Value::HttpRequest(value) => 48usize.saturating_add(value.to_string().len()),
        Value::Host(value) => 32usize.saturating_add(value.to_string().len()),
        Value::Null | Value::Missing => 8,
        Value::Object(values) => 64usize.saturating_add(
            values
                .iter()
                .map(|(key, value)| 48usize.saturating_add(key.len()).saturating_add(estimate_value(value)))
                .sum(),
        ),
        Value::Array(values) => 24usize.saturating_add(values.iter().map(estimate_value).sum()),
    }
}
