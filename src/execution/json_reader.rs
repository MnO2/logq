//! Decode JSON rows directly into logq values, retaining only required roots.
//!
//! Discarded values still use `deserialize_any`, rather than `IgnoredAny`, so
//! numeric range checks, string decoding and nesting limits match full decoding.

use crate::common::types::{Value, Variables};
use ordered_float::OrderedFloat;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;

pub(crate) fn parse_record(
    line: &str,
    required_fields: Option<&HashSet<String>>,
) -> Result<Variables, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(line);
    let variables = (&mut deserializer).deserialize_map(RecordVisitor { required_fields })?;
    deserializer.end()?;
    Ok(variables)
}

/// Append one row directly to the requested columns without building a row map.
/// Required root names are decoded once; nested selected values remain dynamic.
pub(crate) fn parse_columns(
    line: &str,
    field_indices: &HashMap<String, usize>,
    columns: &mut [Vec<Value>],
) -> Result<(), serde_json::Error> {
    for column in columns.iter_mut() {
        column.push(Value::Missing);
    }
    let mut deserializer = serde_json::Deserializer::from_str(line);
    (&mut deserializer).deserialize_map(ColumnsVisitor { field_indices, columns })?;
    deserializer.end()
}

struct ColumnsVisitor<'a> {
    field_indices: &'a HashMap<String, usize>,
    columns: &'a mut [Vec<Value>],
}

impl<'de> Visitor<'de> for ColumnsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(index) = map.next_key_seed(ColumnKeySeed(self.field_indices))? {
            if let Some(index) = index {
                let value = map.next_value_seed(ValueSeed::<true>)?.unwrap();
                // parse_columns appended this row's placeholder before visiting.
                *self.columns[index].last_mut().unwrap() = value;
            } else {
                map.next_value_seed(ValueSeed::<false>)?;
            }
        }
        Ok(())
    }
}

struct ColumnKeySeed<'a>(&'a HashMap<String, usize>);

impl<'de> DeserializeSeed<'de> for ColumnKeySeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for ColumnKeySeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object key")
    }

    fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
        Ok(self.0.get(key).copied())
    }
}

struct RecordVisitor<'a> {
    required_fields: Option<&'a HashSet<String>>,
}

impl<'de> Visitor<'de> for RecordVisitor<'_> {
    type Value = Variables;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Variables, A::Error> {
        let mut variables = Variables::new();
        while let Some(key) = map.next_key_seed(KeySeed(self.required_fields))? {
            if let Some(key) = key {
                let value = map.next_value_seed(ValueSeed::<true>)?.unwrap();
                insert_preserving_order(&mut variables, key, value);
            } else {
                map.next_value_seed(ValueSeed::<false>)?;
            }
        }
        Ok(variables)
    }
}

// serde_json's preserve_order map replaces duplicate values in place, whereas
// LinkedHashMap::insert moves a replaced key to the end. Update in place here.
fn insert_preserving_order(variables: &mut Variables, key: Cow<'_, str>, value: Value) {
    if let Some(existing) = variables.get_mut(key.as_ref()) {
        *existing = value;
    } else {
        variables.insert(key.into_owned(), value);
    }
}

/// Borrow unescaped keys and avoid allocating even escaped keys when discarded.
struct KeySeed<'a>(Option<&'a HashSet<String>>);

impl KeySeed<'_> {
    fn required(&self, key: &str) -> bool {
        self.0.is_none_or(|fields| fields.contains(key))
    }
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Option<Cow<'de, str>>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeySeed<'_> {
    type Value = Option<Cow<'de, str>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object key")
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, key: &'de str) -> Result<Self::Value, E> {
        Ok(self.required(key).then_some(Cow::Borrowed(key)))
    }

    fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
        Ok(self.required(key).then(|| Cow::Owned(key.to_owned())))
    }

    fn visit_string<E: serde::de::Error>(self, key: String) -> Result<Self::Value, E> {
        Ok(self.required(&key).then_some(Cow::Owned(key)))
    }
}

#[derive(Clone, Copy)]
struct ValueSeed<const MATERIALIZE: bool>;

impl<'de, const MATERIALIZE: bool> DeserializeSeed<'de> for ValueSeed<MATERIALIZE> {
    type Value = Option<Value>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de, const MATERIALIZE: bool> Visitor<'de> for ValueSeed<MATERIALIZE> {
    type Value = Option<Value>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then_some(Value::Boolean(value)))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then(|| match i32::try_from(value) {
            Ok(value) => Value::Int(value),
            Err(_) => Value::Float(OrderedFloat(value as f64 as f32)),
        }))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then(|| match i32::try_from(value) {
            Ok(value) => Value::Int(value),
            Err(_) => Value::Float(OrderedFloat(value as f64 as f32)),
        }))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then_some(Value::Float(OrderedFloat(value as f32))))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then(|| Value::String(value.into())))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then(|| Value::String(value.into())))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(MATERIALIZE.then_some(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        if MATERIALIZE {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element_seed(ValueSeed::<true>)? {
                values.push(value.unwrap());
            }
            Ok(Some(Value::Array(values)))
        } else {
            while seq.next_element_seed(ValueSeed::<false>)?.is_some() {}
            Ok(None)
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        if MATERIALIZE {
            let mut variables = Variables::new();
            while let Some(key) = map.next_key_seed(KeySeed(None))? {
                let value = map.next_value_seed(ValueSeed::<true>)?.unwrap();
                insert_preserving_order(&mut variables, key.unwrap(), value);
            }
            Ok(Some(Value::Object(Box::new(variables))))
        } else {
            while map.next_key_seed(ValueSeed::<false>)?.is_some() {
                map.next_value_seed(ValueSeed::<false>)?;
            }
            Ok(None)
        }
    }
}
