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
        let used = state.used.saturating_add(bytes);
        if used > state.limit {
            return Err(StreamError::MemoryBudgetExceeded);
        }
        state.used = used;
        Ok(())
    }

    pub(crate) fn replace(&self, removed: usize, added: usize) -> StreamResult<()> {
        let Some(state) = &self.state else {
            return Ok(());
        };
        let mut state = state.lock().unwrap();
        let used = state.used.saturating_sub(removed).saturating_add(added);
        if used > state.limit {
            return Err(StreamError::MemoryBudgetExceeded);
        }
        state.used = used;
        Ok(())
    }

    pub(crate) fn release(&self, bytes: usize) {
        if let Some(state) = &self.state {
            let mut state = state.lock().unwrap();
            state.used = state.used.saturating_sub(bytes);
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.state.as_ref().map_or(0, |state| state.lock().unwrap().used)
    }
}

/// A charge owned by one operator. Failed reservations do not alter the shared
/// tracker; dropping the owner returns exactly its successfully reserved bytes.
#[derive(Debug, Default)]
pub(crate) struct MemoryReservation {
    tracker: MemoryTracker,
    bytes: usize,
}

impl MemoryReservation {
    pub(crate) fn new(tracker: MemoryTracker) -> Self {
        Self { tracker, bytes: 0 }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.tracker.state.is_some()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn tracker(&self) -> MemoryTracker {
        self.tracker.clone()
    }

    pub(crate) fn add(&mut self, bytes: usize) -> StreamResult<()> {
        self.resize(self.bytes.saturating_add(bytes))
    }

    pub(crate) fn resize(&mut self, bytes: usize) -> StreamResult<()> {
        self.tracker.replace(self.bytes, bytes)?;
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

/// Conservative retained allocation estimate, including vector spare capacity
/// and validity/selection bitmaps. Mixed values include an allowance for their
/// owned payload in addition to the allocated Value slots.
pub(crate) fn estimate_batch(batch: &crate::execution::batch::ColumnBatch) -> usize {
    use crate::execution::batch::TypedColumn;
    use crate::simd::selection::SelectionVector;
    use std::mem::size_of;

    let mut bytes = size_of::<crate::execution::batch::ColumnBatch>()
        + batch.names.capacity() * size_of::<String>()
        + batch.names.iter().map(String::capacity).sum::<usize>()
        + batch.columns.capacity() * size_of::<TypedColumn>();
    for column in &batch.columns {
        let (data_bytes, null, missing) = match column {
            TypedColumn::Int32 { data, null, missing } => (data.capacity() * size_of::<i32>(), null, missing),
            TypedColumn::Float32 { data, null, missing } => (data.capacity() * size_of::<f32>(), null, missing),
            TypedColumn::DateTime { data, null, missing } => (data.capacity() * size_of::<i64>(), null, missing),
            TypedColumn::Boolean { data, null, missing } => (data.words.capacity() * size_of::<u64>(), null, missing),
            TypedColumn::Utf8 {
                data,
                offsets,
                null,
                missing,
            } => (data.capacity() + offsets.capacity() * size_of::<u32>(), null, missing),
            TypedColumn::DictUtf8 {
                dict_data,
                dict_offsets,
                codes,
                null,
                missing,
            } => (
                dict_data.capacity() + dict_offsets.capacity() * size_of::<u32>() + codes.capacity() * size_of::<u16>(),
                null,
                missing,
            ),
            TypedColumn::Mixed { data, null, missing } => (
                data.capacity() * size_of::<Value>() + data.iter().map(estimate_value).sum::<usize>(),
                null,
                missing,
            ),
        };
        bytes = bytes
            .saturating_add(data_bytes)
            .saturating_add((null.words.capacity() + missing.words.capacity()) * size_of::<u64>());
    }
    if let SelectionVector::Bitmap(bitmap) = &batch.selection {
        bytes = bytes.saturating_add(bitmap.words.capacity() * size_of::<u64>());
    }
    bytes
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_charges_leave_the_shared_budget_unchanged() {
        let memory = MemoryTracker::new(Some(100));
        memory.add(20).unwrap();
        assert!(memory.add(90).is_err());
        assert_eq!(memory.state.as_ref().unwrap().lock().unwrap().used, 20);
        assert!(memory.replace(10, 100).is_err());
        assert_eq!(memory.state.as_ref().unwrap().lock().unwrap().used, 20);
        memory.add(80).unwrap();
    }

    #[test]
    fn reservations_release_only_their_own_bytes_and_rollback_failures() {
        let tracker = MemoryTracker::new(Some(100));
        let mut first = MemoryReservation::new(tracker.clone());
        let mut second = MemoryReservation::new(tracker.clone());
        first.add(30).unwrap();
        second.add(40).unwrap();
        assert!(first.resize(80).is_err());
        assert_eq!(first.bytes(), 30);
        assert_eq!(tracker.used(), 70);
        drop(second);
        assert_eq!(tracker.used(), 30);
        first.resize(90).unwrap();
        drop(first);
        assert_eq!(tracker.used(), 0);
    }
}
