use crate::common::types::{Tuple, Value};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use std::result;

pub(crate) type Result<T> = result::Result<T, AggregateError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum AggregateError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Invalid Type")]
    InvalidType,
}

pub(crate) trait AggregateIterator {
    fn add_record(&mut self, key: Tuple, value: Value);
    fn get_aggregated(&self);
}

#[derive(Debug)]
pub(crate) struct AverageIterator {
    pub(crate) averages: HashMap<Tuple, OrderedFloat<f64>>,
    pub(crate) counts: HashMap<Tuple, i64>,
}

impl AverageIterator {
    fn new() -> Self {
        AverageIterator {
            averages: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    fn add_record(&mut self, key: Tuple, value: Value) -> Result<()> {
        let newValue: OrderedFloat<f64> = match value {
            Value::Int(i) => OrderedFloat::from(i as f64),
            Value::Float(f) => f,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        if let (Some(&average), Some(&count)) = (self.averages.get(&key), self.counts.get(&key)) {
            let newCount = count + 1;
            let f64_average: f64 = average.into();
            let f64_newValue: f64 = newValue.into();
            let newAverage: f64 = (f64_average * (count as f64) + f64_newValue) / (newCount as f64);
            self.averages.insert(key.clone(), OrderedFloat::from(newAverage));
            self.counts.insert(key.clone(), newCount);
            Ok(())
        } else {
            self.averages.insert(key.clone(), newValue);
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    fn get_aggregated(&self, key: &Tuple) -> Result<Value> {
        if let Some(&average) = self.averages.get(key) {
            Ok(Value::Float(average))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_iterator_with_one_element() {
        let mut iter = AverageIterator::new();
        let tuple = vec![Value::String("key".to_string())];
        let value = Value::Float(OrderedFloat::from(5.0));

        iter.add_record(tuple.clone(), value.clone());
        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(value), aggregate);
    }
}
