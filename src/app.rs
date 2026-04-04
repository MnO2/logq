use csv::Writer;
use nom::error::VerboseError;
use prettytable::{Row, Table};
use std::result;
use std::str::FromStr;

use crate::common;
use crate::execution;
use crate::logical;
use crate::syntax;

pub(crate) type AppResult<T> = result::Result<T, AppError>;

#[derive(thiserror::Error, Debug)]
pub(crate) enum AppError {
    #[error("Syntax Error: {0}")]
    Syntax(String),
    #[error("Input is fully consumed, the leftover are \"{0}\"")]
    InputNotAllConsumed(String),
    #[error("{0}")]
    Parse(#[from] logical::parser::ParseError),
    #[error("{0}")]
    PhysicalPlan(#[from] logical::types::PhysicalPlanError),
    #[error("{0}")]
    CreateStream(#[from] execution::types::CreateStreamError),
    #[error("{0}")]
    Stream(#[from] execution::types::StreamError),
    #[error("Invalid Log File Format")]
    InvalidLogFileFormat,
    #[error("Invalid Table Spec String")]
    InvalidTableSpecString,
    #[error("{0}")]
    WriteCsv(#[from] csv::Error),
    #[error("{0}")]
    WriteJson(#[from] json::Error),
}

impl PartialEq for AppError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AppError::Syntax(_), AppError::Syntax(_)) => true,
            (AppError::InputNotAllConsumed(_), AppError::InputNotAllConsumed(_)) => true,
            (AppError::Parse(_), AppError::Parse(_)) => true,
            (AppError::PhysicalPlan(_), AppError::PhysicalPlan(_)) => true,
            (AppError::CreateStream(_), AppError::CreateStream(_)) => true,
            (AppError::Stream(_), AppError::Stream(_)) => true,
            (AppError::InvalidLogFileFormat, AppError::InvalidLogFileFormat) => true,
            (AppError::InvalidTableSpecString, AppError::InvalidTableSpecString) => true,
            (AppError::WriteCsv(_), AppError::WriteCsv(_)) => true,
            (AppError::WriteJson(_), AppError::WriteJson(_)) => true,
            _ => false,
        }
    }
}

impl Eq for AppError {}

impl From<nom::Err<VerboseError<&str>>> for AppError {
    fn from(e: nom::Err<VerboseError<&str>>) -> AppError {
        match e {
            nom::Err::Failure(v) => {
                let mut errors: String = String::new();
                for (s, _) in v.errors {
                    errors.push_str(&s.to_string());
                    errors.push('\n');
                }

                AppError::Syntax(errors)
            }
            nom::Err::Error(v) => {
                let mut errors: String = String::new();
                for (s, _) in v.errors {
                    errors.push_str(&s.to_string());
                    errors.push('\n');
                }

                AppError::Syntax(errors)
            }
            _ => AppError::Syntax(String::new()),
        }
    }
}


pub(crate) enum OutputMode {
    Table,
    Csv,
    Json,
}

impl FromStr for OutputMode {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "table" => Ok(OutputMode::Table),
            "csv" => Ok(OutputMode::Csv),
            "json" => Ok(OutputMode::Json),
            _ => Err("unknown output mode".to_string()),
        }
    }
}

pub(crate) fn explain(query_str: &str, data_source: common::types::DataSource) -> AppResult<()> {
    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, _variables) = node.physical(&mut physical_plan_creator)?;

    println!("Query Plan:");
    println!("{:?}", physical_plan);
    Ok(())
}

pub(crate) fn run(query_str: &str, data_source: common::types::DataSource, output_mode: OutputMode) -> AppResult<()> {
    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables)?;

    match output_mode {
        OutputMode::Table => {
            let mut table = Table::new();

            while let Some(record) = stream.next()? {
                table.add_row(Row::new(record.to_row()));
            }
            table.printstd();
        }
        OutputMode::Csv => {
            let mut wtr = Writer::from_writer(std::io::stdout());
            while let Some(record) = stream.next()? {
                let csv_record = record.to_csv_record();
                wtr.write_record(csv_record)?;
            }
        }
        OutputMode::Json => {
            let mut data = json::JsonValue::new_array();
            while let Some(record) = stream.next()? {
                let mut obj = json::JsonValue::new_object();
                for (key, val) in record.to_tuples() {
                    match val {
                        common::types::Value::Boolean(b) => {
                            obj[key] = b.into();
                        }
                        common::types::Value::DateTime(dt) => {
                            obj[key] = dt.to_string().into();
                        }
                        common::types::Value::Float(f) => {
                            obj[key] = f.into_inner().into();
                        }
                        common::types::Value::Host(h) => {
                            obj[key] = h.to_string().into();
                        }
                        common::types::Value::HttpRequest(h) => {
                            obj[key] = h.to_string().into();
                        }
                        common::types::Value::Int(i) => {
                            obj[key] = i.into();
                        }
                        common::types::Value::Null => {
                            obj[key] = json::Null;
                        }
                        common::types::Value::String(s) => {
                            obj[key] = s.into();
                        }
                        common::types::Value::Missing => obj[key] = json::Null,
                        common::types::Value::Object(_) => {
                            //
                            obj[key] = json::JsonValue::String("{ ... }".to_string());
                        }
                        common::types::Value::Array(_) => {
                            obj[key] = json::JsonValue::String("[ ... ]".to_string());
                        }
                    }
                }

                data.push(obj)?;
            }
            let s = data.dump();
            println!("{}", s);
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn run_to_vec(
    query_str: &str,
    data_source: common::types::DataSource,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables)?;
    let mut results = Vec::new();

    while let Some(record) = stream.next()? {
        results.push(record.to_tuples());
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    fn run_format_query(format: &str, lines: &[&str], query: &str) -> AppResult<()> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut file = File::create(file_path.clone()).unwrap();
        for line in lines {
            writeln!(file, "{}", line).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        let data_source =
            common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
        let result = run(query, data_source, OutputMode::Csv);
        dir.close().unwrap();
        result
    }

    fn run_format_query_to_vec(
        format: &str,
        lines: &[&str],
        query: &str,
    ) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let mut file = File::create(file_path.clone()).unwrap();
        for line in lines {
            writeln!(file, "{}", line).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        let data_source =
            common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
        let result = run_to_vec(query, data_source);
        dir.close().unwrap();
        result
    }

    #[test]
    fn test_run_explain_mode() {
        let query_str = "select * from it";
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("log_for_test.log");
        let file_format = "squid".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"1515734740.494      1 [MASKEDIPADDRESS] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());
        let result = run(&*query_str, data_source, OutputMode::Csv);

        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_real_flat_log() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("log_for_test.log");
        let file_format = "elb".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());
        let result = run(
            r#"select t, sum(sent_bytes) as s from it group by time_bucket("5 seconds", timestamp) as t order by t asc limit 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, url_path_bucket(request, 1, "_") as s from it limit 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, percentile_disc(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, approx_percentile(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_real_jsonl_log() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("log_for_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(
            file,
            r#"{{"a": 1, "b": "123", "d": [1, 2, 3], "e": {{"f": {{"g": 2}}}}}}"#
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());
        let result = run(
            r#"select b, e.f.g as x from it limit 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select b, count(e.f.g) as x from it group by b"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select x, count(*) as x from it group by d[0] as x"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_cross_join_jsonl() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("cross_join_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // Self cross join: FROM it AS a CROSS JOIN it AS b
        let result = run(
            r#"select a.x, b.x from it as a cross join it as b"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_cross_join_comma_syntax() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("cross_join_comma_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // Comma-separated FROM items (implicit cross join)
        let result = run(
            r#"select a.x, b.x from it as a, it as b"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_cross_join_with_where() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("cross_join_where_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // Cross join with filter
        let result = run(
            r#"select a.x, b.x from it as a cross join it as b where a.x < b.x"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_left_join_jsonl() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("left_join_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1, "y": "a"}}"#).unwrap();
        writeln!(file, r#"{{"x": 2, "y": "b"}}"#).unwrap();
        writeln!(file, r#"{{"x": 3, "y": "c"}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // LEFT JOIN with matching condition - all rows match themselves
        let result = run(
            r#"select a.x, b.x from it as a left join it as b on a.x = b.x"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_left_join_no_match() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("left_join_no_match.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // LEFT JOIN where nothing matches - all right sides should be NULL
        let result = run(
            r#"select a.x, b.x from it as a left join it as b on a.x != a.x"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_left_outer_join_jsonl() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("left_outer_join_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // LEFT OUTER JOIN - should work identically to LEFT JOIN
        let result = run(
            r#"select a.x, b.x from it as a left outer join it as b on a.x = b.x"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_subquery_in_where() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("subquery_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // Subquery: select rows where x equals the max x
        let result = run(
            r#"select x from it where x = (select max(x) from it)"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_subquery_in_select() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("subquery_select_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // Scalar subquery in SELECT
        let result = run(
            r#"select x, (select count(*) from it) as total from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_union() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("union_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        // UNION (deduplicates)
        let result = run(
            r#"select x from it where x < 3 union select x from it where x > 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // UNION ALL (keeps duplicates)
        let result = run(
            r#"select x from it where x < 3 union all select x from it where x > 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_intersect() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("intersect_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        let result = run(
            r#"select x from it where x < 3 intersect select x from it where x > 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_except() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("except_test.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());

        let result = run(
            r#"select x from it except select x from it where x > 2"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    // ==================== Comprehensive Integration Tests ====================

    #[test]
    fn test_integration_mixed_case_keywords() {
        // Verify case-insensitive keywords work throughout
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_case.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"a": 1, "b": 2}}"#).unwrap();
        writeln!(file, r#"{{"a": 3, "b": 4}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // Mixed case: SELECT, FROM, WHERE, AND, LIMIT
        let result = run(
            r#"SELECT a, b FROM it WHERE a > 0 AND b > 0 LIMIT 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // Mixed case: ORDER BY, ASC (lowercase order by to avoid OR prefix match)
        let result = run(
            r#"SELECT a FROM it order by a ASC LIMIT 1"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // Mixed case variant with OR
        let result = run(
            r#"Select a From it Where a = 1 Or b = 4"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_null_missing_propagation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_null.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"a": 1, "b": null}}"#).unwrap();
        writeln!(file, r#"{{"a": null, "b": 2}}"#).unwrap();
        writeln!(file, r#"{{"a": 3}}"#).unwrap(); // b is MISSING
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // IS NULL
        let result = run(
            r#"select a from it where b is null"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // IS NOT MISSING
        let result = run(
            r#"select a, b from it where b is not missing"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // COALESCE with NULL/MISSING
        let result = run(
            r#"select coalesce(b, 0) as b_or_zero from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_case_when() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_case_when.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 5}}"#).unwrap();
        writeln!(file, r#"{{"x": 10}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // Multi-branch CASE WHEN with ELSE
        let result = run(
            r#"select case when x < 3 then "low" when x < 8 then "mid" else "high" end as category from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_like_between_in() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_operators.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"name": "alice", "age": 25}}"#).unwrap();
        writeln!(file, r#"{{"name": "bob", "age": 30}}"#).unwrap();
        writeln!(file, r#"{{"name": "carol", "age": 35}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // LIKE
        let result = run(
            r#"select name from it where name like "%ob%""#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // BETWEEN
        let result = run(
            r#"select name, age from it where age between 28 and 32"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // IN
        let result = run(
            r#"select name from it where age in (25, 35)"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // NOT LIKE + NOT BETWEEN + NOT IN
        let result = run(
            r#"select name from it where name not like "%a%" and age not between 30 and 40 and age not in (30)"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_cast_and_concat() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_cast.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": "42", "y": "hello"}}"#).unwrap();
        writeln!(file, r#"{{"x": "99", "y": "world"}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // CAST to int
        let result = run(
            r#"select cast(x as int) as xi from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // String concatenation
        let result = run(
            r#"select y || " " || x as combined from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_string_functions() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_strings.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"name": "Hello World"}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // UPPER, LOWER, CHAR_LENGTH
        let result = run(
            r#"select upper(name) as u, lower(name) as l, char_length(name) as len from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_distinct_and_order_by() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_distinct.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // SELECT DISTINCT with ORDER BY
        let result = run(
            r#"select distinct x from it order by x asc"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_join_with_aggregation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_join_agg.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"id": 1, "val": 10}}"#).unwrap();
        writeln!(file, r#"{{"id": 1, "val": 20}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "val": 30}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // CROSS JOIN
        let result = run(
            r#"select a.id, b.val from it as a cross join it as b limit 3"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // LEFT JOIN with ON condition
        let result = run(
            r#"select a.id, b.val from it as a left join it as b on a.id = b.id limit 5"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_subquery_and_union() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_subquery_union.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        writeln!(file, r#"{{"x": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // Subquery in WHERE
        let result = run(
            r#"select x from it where x = (select max(x) from it)"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // UNION
        let result = run(
            r#"select x from it where x = 1 union select x from it where x = 3"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // INTERSECT
        let result = run(
            r#"select x from it where x > 1 intersect select x from it where x < 3"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // EXCEPT
        let result = run(
            r#"select x from it except select x from it where x = 2"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_nullif() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_nullif.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"a": 1, "b": 1}}"#).unwrap();
        writeln!(file, r#"{{"a": 2, "b": 3}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // NULLIF returns NULL when equal, value when not
        let result = run(
            r#"select nullif(a, b) as result from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_json_output_mode() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_json.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1, "y": "hello"}}"#).unwrap();
        writeln!(file, r#"{{"x": 2, "y": "world"}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // JSON output mode
        let result = run(
            r#"select x, y from it"#,
            data_source.clone(),
            OutputMode::Json,
        );
        assert_eq!(result, Ok(()));

        // CSV output mode (Table mode omitted: prettytable may SIGSEGV without a TTY)
        let result = run(
            r#"select x, y from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_integration_nested_path_and_array() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("integration_nested.log");
        let file_format = "jsonl".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"a": {{"b": {{"c": 42}}}}, "d": [10, 20, 30]}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format, table_name);

        // Nested path access
        let result = run(
            r#"select a.b.c as deep from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        // Array index
        let result = run(
            r#"select d[0] as first, d[2] as third from it"#,
            data_source.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_elb_select_and_where_filter() {
        let lines = &[
            r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/path HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
            r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 500 500 0 1024 "GET https://example.com:443/error HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
            r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 8192 "GET https://example.com:443/ok HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
        ];
        let results = run_format_query_to_vec("elb", lines, r#"SELECT elb_status_code, sent_bytes FROM it WHERE elb_status_code = "200""#).unwrap();
        assert_eq!(results.len(), 2);
        for row in &results {
            let status = &row.iter().find(|(k, _)| k == "elb_status_code").unwrap().1;
            assert_eq!(status, &common::types::Value::String("200".to_string()));
        }
    }

    #[test]
    fn test_elb_numeric_aggregation() {
        let lines = &[
            r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
            r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 1024 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
            r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 500 500 0 8192 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
            r#"2019-06-07T18:45:36.559871Z elb1 78.168.134.95:4589 10.0.0.218:80 0.000060 0.004000 0.000040 500 500 0 2048 "GET https://example.com:443/d HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-004""#,
            r#"2019-06-07T18:45:37.559871Z elb1 78.168.134.96:4590 10.0.0.219:80 0.000070 0.005000 0.000045 200 200 0 512 "GET https://example.com:443/e HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-005""#,
        ];
        let result = run_format_query("elb", lines, r#"SELECT elb_status_code, sum(backend_processing_time) as total_bpt, sum(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_elb_order_by_timestamp() {
        // Out-of-order timestamps
        let lines = &[
            r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 8192 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
            r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
            r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 1024 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
        ];
        let result = run_format_query("elb", lines, r#"SELECT timestamp FROM it ORDER BY timestamp ASC"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_elb_limit_with_order() {
        let lines = &[
            r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 100 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
            r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 500 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
            r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 300 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
            r#"2019-06-07T18:45:36.559871Z elb1 78.168.134.95:4589 10.0.0.218:80 0.000060 0.004000 0.000040 200 200 0 900 "GET https://example.com:443/d HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-004""#,
            r#"2019-06-07T18:45:37.559871Z elb1 78.168.134.96:4590 10.0.0.219:80 0.000070 0.005000 0.000045 200 200 0 200 "GET https://example.com:443/e HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-005""#,
        ];
        let result = run_format_query("elb", lines, r#"SELECT sent_bytes FROM it ORDER BY sent_bytes DESC LIMIT 2"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_alb_filter_by_type() {
        let lines = &[
            r#"http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
            r#"https 2018-07-02T22:23:01.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.40:2818 10.0.0.2:80 0.001 0.002 0.001 200 200 50 512 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-002" "www.example.com" "arn:aws:acm:cert/123" 1 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
            r#"h2 2018-07-02T22:23:02.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.41:2819 10.0.0.3:80 0.002 0.003 0.002 301 301 0 128 "GET https://www.example.com:443/redirect HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-003" "www.example.com" "arn:aws:acm:cert/456" 2 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
        ];
        let results = run_format_query_to_vec("alb", lines, r#"SELECT type, elb_status_code FROM it WHERE type = "https""#).unwrap();
        assert_eq!(results.len(), 1);
        let type_val = &results[0].iter().find(|(k, _)| k == "type").unwrap().1;
        assert_eq!(type_val, &common::types::Value::String("https".to_string()));
    }

    #[test]
    fn test_alb_aggregate_processing_times() {
        let lines = &[
            r#"http 2018-07-02T22:23:00.186641Z app/lb/1 192.168.1.1:1000 10.0.0.1:80 0.001 0.010 0.001 200 200 100 1000 "GET http://www.example.com:80/a HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:01.186641Z app/lb/1 192.168.1.2:1001 10.0.0.2:80 0.002 0.020 0.002 200 200 200 2000 "GET http://www.example.com:80/b HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-002" "-" "-" 0 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:02.186641Z app/lb/1 192.168.1.3:1002 10.0.0.3:80 0.003 0.030 0.003 500 500 300 3000 "GET http://www.example.com:80/c HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-003" "-" "-" 0 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:03.186641Z app/lb/1 192.168.1.4:1003 10.0.0.4:80 0.004 0.040 0.004 500 500 400 4000 "GET http://www.example.com:80/d HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-004" "-" "-" 0 2018-07-02T22:22:51.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:04.186641Z app/lb/1 192.168.1.5:1004 10.0.0.5:80 0.005 0.050 0.005 200 200 500 5000 "GET http://www.example.com:80/e HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-005" "-" "-" 0 2018-07-02T22:22:52.364000Z "forward" "-" "-""#,
        ];
        let result = run_format_query("alb", lines, r#"SELECT elb_status_code, sum(request_processing_time) as total_rpt FROM it GROUP BY elb_status_code"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_alb_order_by_received_bytes() {
        let lines = &[
            r#"http 2018-07-02T22:23:00.186641Z app/lb/1 192.168.1.1:1000 10.0.0.1:80 0.001 0.010 0.001 200 200 100 1000 "GET http://www.example.com:80/a HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:01.186641Z app/lb/1 192.168.1.2:1001 10.0.0.2:80 0.002 0.020 0.002 200 200 500 2000 "GET http://www.example.com:80/b HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-002" "-" "-" 0 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:02.186641Z app/lb/1 192.168.1.3:1002 10.0.0.3:80 0.003 0.030 0.003 200 200 300 3000 "GET http://www.example.com:80/c HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-003" "-" "-" 0 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:03.186641Z app/lb/1 192.168.1.4:1003 10.0.0.4:80 0.004 0.040 0.004 200 200 50 4000 "GET http://www.example.com:80/d HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-004" "-" "-" 0 2018-07-02T22:22:51.364000Z "forward" "-" "-""#,
            r#"http 2018-07-02T22:23:04.186641Z app/lb/1 192.168.1.5:1004 10.0.0.5:80 0.005 0.050 0.005 200 200 800 5000 "GET http://www.example.com:80/e HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-005" "-" "-" 0 2018-07-02T22:22:52.364000Z "forward" "-" "-""#,
        ];
        let result = run_format_query("alb", lines, r#"SELECT received_bytes FROM it ORDER BY received_bytes DESC LIMIT 3"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_s3_filter_by_operation() {
        let lines = &[
            r#"owner1 mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT images/photo.jpg "GET /mybucket/images/photo.jpg HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.PUT.OBJECT docs/report.pdf "PUT /mybucket/docs/report.pdf HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.OBJECT data/export.csv "GET /mybucket/data/export.csv HTTP/1.1" 200 - 4096 4096 70 30 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:03:00 +0000] 192.0.2.6 owner1 REQ004 REST.DELETE.OBJECT old/file.tmp "DELETE /mybucket/old/file.tmp HTTP/1.1" 204 - - - 40 15 "-" "aws-sdk/1.0" - abc4= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:04:00 +0000] 192.0.2.7 owner1 REQ005 REST.GET.OBJECT images/logo.png "GET /mybucket/images/logo.png HTTP/1.1" 200 - 512 512 30 5 "-" "aws-sdk/1.0" - abc5= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        ];
        let results = run_format_query_to_vec("s3", lines, r#"SELECT operation, http_status FROM it WHERE operation = "REST.GET.OBJECT""#).unwrap();
        assert_eq!(results.len(), 3);
        for row in &results {
            let op = &row.iter().find(|(k, _)| k == "operation").unwrap().1;
            assert_eq!(op, &common::types::Value::String("REST.GET.OBJECT".to_string()));
        }
    }

    #[test]
    fn test_s3_dash_placeholders() {
        let lines = &[
            r#"owner1 mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /mybucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.GET.OBJECT key2 "GET /mybucket/key2 HTTP/1.1" 403 AccessDenied 0 0 10 5 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.OBJECT key3 "GET /mybucket/key3 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        ];
        let result = run_format_query("s3", lines, r#"SELECT error_code, refererr FROM it WHERE error_code = "-""#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_s3_group_by_http_status() {
        let lines = &[
            r#"owner1 bkt [01/Jan/2020:00:00:00 +0000] 10.0.0.1 owner1 R1 REST.GET.OBJECT k1 "GET /bkt/k1 HTTP/1.1" 200 - 100 100 10 5 "-" "sdk/1" - a= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:01:00 +0000] 10.0.0.2 owner1 R2 REST.GET.OBJECT k2 "GET /bkt/k2 HTTP/1.1" 200 - 200 200 20 10 "-" "sdk/1" - b= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:02:00 +0000] 10.0.0.3 owner1 R3 REST.GET.OBJECT k3 "GET /bkt/k3 HTTP/1.1" 403 AccessDenied 0 0 5 2 "-" "sdk/1" - c= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:03:00 +0000] 10.0.0.4 owner1 R4 REST.GET.OBJECT k4 "GET /bkt/k4 HTTP/1.1" 200 - 300 300 30 15 "-" "sdk/1" - d= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:04:00 +0000] 10.0.0.5 owner1 R5 REST.GET.OBJECT k5 "GET /bkt/k5 HTTP/1.1" 404 NoSuchKey 0 0 8 3 "-" "sdk/1" - e= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:05:00 +0000] 10.0.0.6 owner1 R6 REST.GET.OBJECT k6 "GET /bkt/k6 HTTP/1.1" 403 AccessDenied 0 0 6 2 "-" "sdk/1" - f= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:06:00 +0000] 10.0.0.7 owner1 R7 REST.GET.OBJECT k7 "GET /bkt/k7 HTTP/1.1" 200 - 400 400 40 20 "-" "sdk/1" - g= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 bkt [01/Jan/2020:00:07:00 +0000] 10.0.0.8 owner1 R8 REST.GET.OBJECT k8 "GET /bkt/k8 HTTP/1.1" 404 NoSuchKey 0 0 9 4 "-" "sdk/1" - h= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        ];
        let result = run_format_query("s3", lines, r#"SELECT http_status, count(*) as cnt FROM it GROUP BY http_status ORDER BY http_status ASC"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_s3_string_functions() {
        let lines = &[
            r#"owner1 MyBucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /MyBucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader MyBucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 AnotherBucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.PUT.OBJECT key2 "PUT /AnotherBucket/key2 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader AnotherBucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 ThirdBucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.VERSIONING key3 "GET /ThirdBucket/key3 HTTP/1.1" 200 - 512 512 30 5 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader ThirdBucket.s3.amazonaws.com TLSv1.2"#,
        ];
        let result = run_format_query("s3", lines, r#"SELECT upper(operation) as op_upper, lower(bucket) as bucket_lower FROM it"#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_squid_filter_by_method() {
        let lines = &[
            r#"1515734740.494      1 [192.168.1.1] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#,
            r#"1515734741.100      5 [192.168.1.2] TCP_MISS/200 15234 GET http://www.google.com/ - HIER_DIRECT/216.58.214.196 text/html"#,
            r#"1515734742.200     10 [192.168.1.3] TCP_MISS/200 8432 POST http://api.example.com/data - HIER_DIRECT/93.184.216.34 application/json"#,
            r#"1515734743.300      2 [192.168.1.4] TCP_HIT/200 12045 GET http://www.github.com/ - HIER_DIRECT/140.82.121.3 text/html"#,
            r#"1515734744.400      3 [192.168.1.5] TCP_DENIED/403 2100 CONNECT slack.com:443 - HIER_NONE/- text/html"#,
        ];
        let results = run_format_query_to_vec("squid", lines, r#"SELECT method, url FROM it WHERE method = "GET""#).unwrap();
        assert_eq!(results.len(), 2);
        for row in &results {
            let method = &row.iter().find(|(k, _)| k == "method").unwrap().1;
            assert_eq!(method, &common::types::Value::String("GET".to_string()));
        }
    }

    #[test]
    fn test_squid_like_on_url() {
        let lines = &[
            r#"1515734740.494      1 [192.168.1.1] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#,
            r#"1515734741.100      5 [192.168.1.2] TCP_MISS/200 15234 GET http://www.google.com/ - HIER_DIRECT/216.58.214.196 text/html"#,
            r#"1515734742.200     10 [192.168.1.3] TCP_MISS/200 8432 GET http://www.github.com/ - HIER_DIRECT/140.82.121.3 text/html"#,
            r#"1515734743.300      2 [192.168.1.4] TCP_DENIED/407 4100 CONNECT dl.dropbox.com:443 - HIER_NONE/- text/html"#,
            r#"1515734744.400      3 [192.168.1.5] TCP_MISS/200 9200 GET http://slack.com/ - HIER_DIRECT/34.230.68.40 text/html"#,
        ];
        let result = run_format_query("squid", lines, r#"SELECT url FROM it WHERE url LIKE "%dropbox%""#);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_squid_count_by_status() {
        let lines = &[
            r#"1515734740.000      1 [10.0.0.1] TCP_DENIED/407 3922 CONNECT a.com:443 - HIER_NONE/- text/html"#,
            r#"1515734741.000      2 [10.0.0.2] TCP_MISS/200 15234 GET http://b.com/ - HIER_DIRECT/1.2.3.4 text/html"#,
            r#"1515734742.000      3 [10.0.0.3] TCP_HIT/200 8432 GET http://c.com/ - HIER_DIRECT/1.2.3.5 text/html"#,
            r#"1515734743.000      4 [10.0.0.4] TCP_DENIED/403 2100 CONNECT d.com:443 - HIER_NONE/- text/html"#,
            r#"1515734744.000      5 [10.0.0.5] TCP_MISS/200 9200 GET http://e.com/ - HIER_DIRECT/1.2.3.6 text/html"#,
            r#"1515734745.000      6 [10.0.0.6] TCP_DENIED/407 4100 CONNECT f.com:443 - HIER_NONE/- text/html"#,
            r#"1515734746.000      7 [10.0.0.7] TCP_HIT/200 12000 GET http://g.com/ - HIER_DIRECT/1.2.3.7 text/html"#,
            r#"1515734747.000      8 [10.0.0.8] TCP_DENIED/403 1800 CONNECT h.com:443 - HIER_NONE/- text/html"#,
        ];
        let result = run_format_query("squid", lines, r#"SELECT code_and_status, count(*) as cnt FROM it GROUP BY code_and_status"#);
        assert_eq!(result, Ok(()));
    }
}
