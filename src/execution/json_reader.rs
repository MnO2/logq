//! Decode JSON rows directly into logq values, retaining only required object paths.
//!
//! Discarded values still use `deserialize_any`, rather than `IgnoredAny`, so
//! numeric range checks, string decoding and nesting limits match full decoding.

use crate::common::types::{Value, Variables};
use crate::execution::field_analysis::{JsonFieldProjection, JsonProjection};
use crate::execution::json_column_builder::JsonColumnBuilder;
use ordered_float::OrderedFloat;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

pub(crate) fn parse_record(
    line: &str,
    required_fields: Option<&JsonProjection>,
) -> Result<Variables, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(line);
    let variables = (&mut deserializer).deserialize_map(RecordVisitor {
        required_fields: required_fields.map(JsonProjection::fields),
    })?;
    deserializer.end()?;
    Ok(variables)
}

/// Append one row directly to the requested columns without building a row map.
/// Required root names are decoded once; nested selected values remain dynamic.
#[cfg(test)]
pub(crate) fn parse_columns(
    line: &str,
    field_indices: &HashMap<String, usize>,
    columns: &mut [JsonColumnBuilder],
) -> Result<(), serde_json::Error> {
    parse_projected_columns(line, field_indices, columns, None)
}

pub(crate) fn parse_projected_columns(
    line: &str,
    field_indices: &HashMap<String, usize>,
    columns: &mut [JsonColumnBuilder],
    projection: Option<&[JsonFieldProjection]>,
) -> Result<(), serde_json::Error> {
    for column in columns.iter_mut() {
        column.begin_row();
    }
    let mut deserializer = serde_json::Deserializer::from_str(line);
    (&mut deserializer).deserialize_map(ColumnsVisitor {
        field_indices,
        columns,
        projection,
    })?;
    deserializer.end()
}

struct ColumnsVisitor<'a> {
    field_indices: &'a HashMap<String, usize>,
    columns: &'a mut [JsonColumnBuilder],
    projection: Option<&'a [JsonFieldProjection]>,
}

impl<'de> Visitor<'de> for ColumnsVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(index) = map.next_key_seed(ColumnKeySeed(self.field_indices))? {
            if let Some(index) = index {
                let projection = self.projection.and_then(|fields| fields.get(index));
                map.next_value_seed(ColumnValueSeed(&mut self.columns[index], projection))?;
            } else {
                map.next_value_seed(ValueSeed::<false>(None))?;
            }
        }
        Ok(())
    }
}

/// Scalars are written into their final column storage. Only nested selected
/// values require the dynamic visitor used by the row reader.
struct ColumnValueSeed<'a>(&'a mut JsonColumnBuilder, Option<&'a JsonFieldProjection>);

impl<'de> DeserializeSeed<'de> for ColumnValueSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ColumnValueSeed<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<(), E> {
        self.0.put_bool(value);
        Ok(())
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<(), E> {
        match i32::try_from(value) {
            Ok(value) => self.0.put_int(value),
            Err(_) => self.0.put_float(value as f64 as f32),
        }
        Ok(())
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<(), E> {
        match i32::try_from(value) {
            Ok(value) => self.0.put_int(value),
            Err(_) => self.0.put_float(value as f64 as f32),
        }
        Ok(())
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<(), E> {
        self.0.put_float(value as f32);
        Ok(())
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        self.0.put_str(value);
        Ok(())
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<(), E> {
        self.visit_str(&value)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        self.0.put_null();
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<(), A::Error> {
        self.0.put_value(ValueSeed::<true>(None).visit_seq(seq)?.unwrap());
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        self.0.put_value(ValueSeed::<true>(self.1).visit_map(map)?.unwrap());
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
    required_fields: Option<&'a HashMap<String, JsonFieldProjection>>,
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
                let projection = self.required_fields.and_then(|fields| fields.get(key.as_ref()));
                let value = map.next_value_seed(ValueSeed::<true>(projection))?.unwrap();
                insert_preserving_order(&mut variables, key, value);
            } else {
                map.next_value_seed(ValueSeed::<false>(None))?;
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
struct KeySeed<'a>(Option<&'a HashMap<String, JsonFieldProjection>>);

impl KeySeed<'_> {
    fn required(&self, key: &str) -> bool {
        self.0.is_none_or(|fields| fields.contains_key(key))
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
struct ValueSeed<'a, const MATERIALIZE: bool>(Option<&'a JsonFieldProjection>);

impl<'de, const MATERIALIZE: bool> DeserializeSeed<'de> for ValueSeed<'_, MATERIALIZE> {
    type Value = Option<Value>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de, const MATERIALIZE: bool> Visitor<'de> for ValueSeed<'_, MATERIALIZE> {
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
            while let Some(value) = seq.next_element_seed(ValueSeed::<true>(None))? {
                values.push(value.unwrap());
            }
            Ok(Some(Value::Array(values)))
        } else {
            while seq.next_element_seed(ValueSeed::<false>(None))?.is_some() {}
            Ok(None)
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        if MATERIALIZE {
            let variables = RecordVisitor {
                required_fields: self.0.and_then(JsonFieldProjection::children),
            }
            .visit_map(map)?;
            Ok(Some(Value::Object(Box::new(variables))))
        } else {
            while map.next_key_seed(ValueSeed::<false>(None))?.is_some() {
                map.next_value_seed(ValueSeed::<false>(None))?;
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(paths: &[&[&str]]) -> JsonProjection {
        use crate::common::types::DataSource;
        use crate::execution::field_analysis::extract_required_json_fields;
        use crate::execution::types::{Expression, Named, Node};
        use crate::syntax::ast::{PathExpr, PathSegment};
        let named = paths
            .iter()
            .map(|segments| {
                let path = PathExpr::new(
                    segments
                        .iter()
                        .map(|name| PathSegment::AttrName((*name).into()))
                        .collect(),
                );
                Named::Expression(Expression::Variable(path), None)
            })
            .collect();
        let plan = Node::Map(
            named,
            Box::new(Node::DataSource(DataSource::Stdin("jsonl".into(), "it".into()), vec![])),
        );
        extract_required_json_fields(&plan).unwrap()
    }

    #[test]
    fn nested_projection_discards_unselected_siblings() {
        let required = projection(&[&["nested", "metrics", "v"]]);
        let actual = parse_record(
            r#"{"nested":{"metrics":{"v":7,"payload":"unneeded"},"other":[1,2,3]},"ignored":true}"#,
            Some(&required),
        )
        .unwrap();
        let expected = parse_record(r#"{"nested":{"metrics":{"v":7}}}"#, None).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn nested_projection_merges_paths_and_whole_value_dependencies_in_any_order() {
        let input = r#"{"n":{"a":{"x":1,"y":2},"b":3,"drop":4},"drop":5}"#;
        for paths in [
            vec![&["n", "a"][..], &["n", "a", "x"]],
            vec![&["n", "a", "x"][..], &["n", "a"]],
        ] {
            let actual = parse_record(input, Some(&projection(&paths))).unwrap();
            assert_eq!(actual, parse_record(r#"{"n":{"a":{"x":1,"y":2}}}"#, None).unwrap());
        }
        for paths in [vec![&["n"][..], &["n", "a", "x"]], vec![&["n", "a", "x"][..], &["n"]]] {
            let actual = parse_record(input, Some(&projection(&paths))).unwrap();
            assert_eq!(
                actual,
                parse_record(r#"{"n":{"a":{"x":1,"y":2},"b":3,"drop":4}}"#, None).unwrap()
            );
        }
        let actual = parse_record(input, Some(&projection(&[&["n", "a", "x"], &["n", "b"]]))).unwrap();
        assert_eq!(actual, parse_record(r#"{"n":{"a":{"x":1},"b":3}}"#, None).unwrap());
    }

    #[test]
    fn nested_projection_column_and_row_readers_preserve_duplicates_presence_and_drift() {
        use crate::execution::batch::BatchToRowAdapter;
        let projection = projection(&[&["n", "a", "x"]]);
        let indices = HashMap::from([("n".into(), 0)]);
        let columns_projection = vec![projection.fields()["n"].clone()];
        for (input, expected) in [
            (r#"{"n":{"a":{"x":1,"y":2,"x":3}}}"#, r#"{"n":{"a":{"x":3}}}"#),
            (r#"{"n":{"a":{"x":1}},"n":{"a":{"y":2}}}"#, r#"{"n":{"a":{}}}"#),
            (r#"{"n":{"a":{"x":1},"a":null}}"#, r#"{"n":{"a":null}}"#),
            (
                r#"{"n":{"a":{"x":1},"\u0061":{"x":2,"other":3}}}"#,
                r#"{"n":{"a":{"x":2}}}"#,
            ),
            (r#"{"n":null}"#, r#"{"n":null}"#),
            (r#"{"n":true}"#, r#"{"n":true}"#),
            (r#"{"n":1.0}"#, r#"{"n":1.0}"#),
            (r#"{"n":[{"a":{"x":1,"y":2}}]}"#, r#"{"n":[{"a":{"x":1,"y":2}}]}"#),
            (r#"{"n":{"a":[{"x":1,"y":2}]}}"#, r#"{"n":{"a":[{"x":1,"y":2}]}}"#),
            (r#"{"other":1}"#, r#"{}"#),
        ] {
            let expected = parse_record(expected, None).unwrap();
            assert_eq!(parse_record(input, Some(&projection)).unwrap(), expected, "{input}");
            let mut columns = [JsonColumnBuilder::new()];
            parse_projected_columns(input, &indices, &mut columns, Some(&columns_projection)).unwrap();
            let [column] = columns;
            let actual = BatchToRowAdapter::extract_value(&column.finish(false), 0);
            assert_eq!(actual, expected.get("n").cloned().unwrap_or(Value::Missing), "{input}");
        }
    }

    #[test]
    fn nested_projection_preserves_full_decoding_validation_errors() {
        let projection = projection(&[&["n", "x"]]);
        let indices = HashMap::from([("n".into(), 0)]);
        let columns_projection = vec![projection.fields()["n"].clone()];
        let mut inputs = vec![
            r#"{"n":{"x":1,"drop":1e9999}}"#.to_owned(),
            r#"{"n":{"x":1,"drop":"\uD800"}}"#.to_owned(),
            r#"{"n":{"x":1,"drop":{"\uD800":0}}}"#.to_owned(),
            r#"{"n":{"x":1,"drop":[1,]}}"#.to_owned(),
            r#"{"n":{"x":1,"drop":1e100}}"#.to_owned(),
        ];
        for depth in [124, 125, 126, 127, 128, 160] {
            inputs.push(format!(
                "{{\"n\":{{\"x\":1,\"drop\":{}0{}}}}}",
                "[".repeat(depth),
                "]".repeat(depth)
            ));
        }
        for input in inputs {
            let expected = parse_record(&input, None).err().map(|error| error.to_string());
            let actual = parse_record(&input, Some(&projection))
                .err()
                .map(|error| error.to_string());
            assert_eq!(actual, expected, "row: {input}");
            let mut columns = [JsonColumnBuilder::new()];
            let actual = parse_projected_columns(&input, &indices, &mut columns, Some(&columns_projection))
                .err()
                .map(|error| error.to_string());
            assert_eq!(actual, expected, "column: {input}");
        }
    }
}
