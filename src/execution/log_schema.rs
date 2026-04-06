use crate::execution::datasource::{DataType, LogFormat};

/// Schema information for a structured log format.
/// Provides field names, types, and lookup methods.
pub(crate) struct LogSchema {
    field_names: &'static Vec<String>,
    datatypes: &'static Vec<DataType>,
    field_count: usize,
}

impl LogSchema {
    pub fn from_format(format_str: &str) -> Self {
        let format = LogFormat::from_str(format_str);
        let (field_names, datatypes, field_count) = format.field_info();
        Self { field_names, datatypes, field_count }
    }

    pub fn field_count(&self) -> usize {
        self.field_count
    }

    pub fn field_name(&self, idx: usize) -> &str {
        &self.field_names[idx]
    }

    pub fn field_type(&self, idx: usize) -> DataType {
        self.datatypes[idx].clone()
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.field_names.iter().position(|n| n == name)
    }

    /// Returns true if this field type requires scalar per-row parsing
    /// (Host, HttpRequest) and cannot be used in pushed-down predicates.
    pub fn is_mixed_type(&self, idx: usize) -> bool {
        matches!(self.datatypes[idx], DataType::Host | DataType::HttpRequest)
    }

    pub fn field_names_slice(&self) -> &[String] {
        self.field_names
    }

    pub fn datatypes_slice(&self) -> &[DataType] {
        self.datatypes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_schema_elb() {
        let schema = LogSchema::from_format("elb");
        assert!(schema.field_count() > 0);
        assert_eq!(schema.field_name(0), "timestamp");
    }

    #[test]
    fn test_field_index_by_name() {
        let schema = LogSchema::from_format("elb");
        assert!(schema.field_index("timestamp").is_some());
        assert_eq!(schema.field_index("nonexistent"), None);
    }

    #[test]
    fn test_is_mixed_type() {
        let schema = LogSchema::from_format("elb");
        let ts_idx = schema.field_index("timestamp").unwrap();
        assert!(!schema.is_mixed_type(ts_idx)); // DateTime is not mixed

        // Find a Host field
        for i in 0..schema.field_count() {
            if matches!(schema.field_type(i), DataType::Host) {
                assert!(schema.is_mixed_type(i));
            }
        }
    }

    #[test]
    fn test_log_schema_alb() {
        let schema = LogSchema::from_format("alb");
        assert!(schema.field_count() > 0);
    }

    #[test]
    fn test_log_schema_s3() {
        let schema = LogSchema::from_format("s3");
        assert!(schema.field_count() > 0);
    }
}
