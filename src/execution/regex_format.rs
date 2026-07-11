use crate::common::types::{Value, Variables};
use ordered_float::OrderedFloat;
use regex::Regex;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum RegexFormatError {
    #[error("could not read regex format file: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid regex format TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid regex pattern: {0}")]
    Regex(#[from] regex::Error),
    #[error("regex format must contain at least one named capture group")]
    NoNamedCaptures,
    #[error("type declared for unknown capture group `{0}`")]
    UnknownCapture(String),
    #[error("unsupported type `{kind}` for capture group `{field}`")]
    UnsupportedType { field: String, kind: String },
    #[error("input line does not match the configured regex")]
    LineMismatch,
    #[error("capture group `{field}` is not a valid integer: {source}")]
    ParseInteger {
        field: String,
        source: std::num::ParseIntError,
    },
    #[error("capture group `{field}` is not a valid float: {source}")]
    ParseFloat {
        field: String,
        source: std::num::ParseFloatError,
    },
    #[error("capture group `{field}` is not a valid datetime: {source}")]
    ParseDateTime {
        field: String,
        source: chrono::format::ParseError,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegexFormatFile {
    pattern: String,
    #[serde(default)]
    types: BTreeMap<String, String>,
}

#[derive(Debug)]
enum CaptureType {
    String,
    Integer,
    Float,
    DateTime(String),
}

#[derive(Debug)]
struct CaptureField {
    name: String,
    kind: CaptureType,
}

#[derive(Debug)]
pub(crate) struct RegexFormat {
    regex: Regex,
    fields: Vec<CaptureField>,
}

impl RegexFormat {
    pub(crate) fn from_file(path: &Path) -> Result<Self, RegexFormatError> {
        let contents = std::fs::read_to_string(path)?;
        let definition: RegexFormatFile = toml::from_str(&contents)?;
        Self::from_definition(definition)
    }

    fn from_definition(definition: RegexFormatFile) -> Result<Self, RegexFormatError> {
        let regex = Regex::new(&definition.pattern)?;
        let names: Vec<String> = regex.capture_names().flatten().map(str::to_string).collect();
        if names.is_empty() {
            return Err(RegexFormatError::NoNamedCaptures);
        }
        for name in definition.types.keys() {
            if !names.contains(name) {
                return Err(RegexFormatError::UnknownCapture(name.clone()));
            }
        }

        let fields = names
            .into_iter()
            .map(|name| {
                let kind = match definition.types.get(&name).map(String::as_str).unwrap_or("string") {
                    "string" | "varchar" => CaptureType::String,
                    "int" | "integer" => CaptureType::Integer,
                    "float" => CaptureType::Float,
                    value if value.starts_with("datetime:") => {
                        CaptureType::DateTime(value["datetime:".len()..].to_string())
                    }
                    kind => {
                        return Err(RegexFormatError::UnsupportedType {
                            field: name.clone(),
                            kind: kind.to_string(),
                        });
                    }
                };
                Ok(CaptureField { name, kind })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { regex, fields })
    }

    pub(crate) fn parse_line(&self, line: &str) -> Result<Variables, RegexFormatError> {
        let captures = self
            .regex
            .captures(line.trim_end())
            .ok_or(RegexFormatError::LineMismatch)?;
        let mut variables = Variables::with_capacity(self.fields.len());
        for field in &self.fields {
            let Some(value) = captures.name(&field.name).map(|capture| capture.as_str()) else {
                variables.insert(field.name.clone(), Value::Null);
                continue;
            };
            let value = match &field.kind {
                CaptureType::String => Value::String(value.into()),
                CaptureType::Integer => Value::Int(value.parse().map_err(|source| RegexFormatError::ParseInteger {
                    field: field.name.clone(),
                    source,
                })?),
                CaptureType::Float => Value::Float(OrderedFloat::from(value.parse::<f32>().map_err(|source| {
                    RegexFormatError::ParseFloat {
                        field: field.name.clone(),
                        source,
                    }
                })?)),
                CaptureType::DateTime(format) => {
                    Value::DateTime(chrono::DateTime::parse_from_str(value, format).map_err(|source| {
                        RegexFormatError::ParseDateTime {
                            field: field.name.clone(),
                            source,
                        }
                    })?)
                }
            };
            variables.insert(field.name.clone(), value);
        }
        Ok(variables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_captures_and_declared_types() {
        let definition: RegexFormatFile = toml::from_str(
            r#"
pattern = '^(?P<method>\S+) (?P<status>\d+) (?P<elapsed>[0-9.]+)$'
[types]
status = "int"
elapsed = "float"
"#,
        )
        .unwrap();
        let format = RegexFormat::from_definition(definition).unwrap();
        let values = format.parse_line("GET 200 1.5").unwrap();
        assert_eq!(values["method"], Value::String("GET".into()));
        assert_eq!(values["status"], Value::Int(200));
        assert_eq!(values["elapsed"], Value::Float(OrderedFloat::from(1.5)));
    }

    #[test]
    fn rejects_types_for_unknown_captures() {
        let definition: RegexFormatFile = toml::from_str(
            r#"
pattern = '^(?P<method>\S+)$'
[types]
status = "int"
"#,
        )
        .unwrap();
        assert!(matches!(
            RegexFormat::from_definition(definition),
            Err(RegexFormatError::UnknownCapture(name)) if name == "status"
        ));
    }

    #[test]
    fn parses_datetime_with_a_declared_chrono_format() {
        let definition: RegexFormatFile = toml::from_str(
            r#"
pattern = '^\[(?P<timestamp>[^]]+)\]$'
[types]
timestamp = "datetime:%d/%b/%Y:%H:%M:%S %z"
"#,
        )
        .unwrap();
        let format = RegexFormat::from_definition(definition).unwrap();
        let values = format.parse_line("[10/Oct/2000:13:55:36 -0700]").unwrap();
        assert_eq!(
            values["timestamp"],
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2000-10-10T13:55:36-07:00").unwrap())
        );
    }
}
