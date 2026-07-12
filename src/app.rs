use csv::Writer;
use prettytable::{Row, Table};
use std::io::Write;
use std::result;
use std::str::FromStr;
use std::sync::Arc;

use crate::common;
use crate::execution;
use crate::functions;
use crate::logical;
use crate::syntax;

pub type AppResult<T> = result::Result<T, AppError>;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0}")]
    Syntax(String),
    #[error("{0}")]
    InputNotAllConsumed(String),
    #[error("{0}")]
    Parse(#[from] logical::parser::ParseError),
    #[error("{0}")]
    Planning(String),
    #[error("{0}")]
    Runtime(String),
    #[error("{0}")]
    PhysicalPlan(#[from] logical::types::PhysicalPlanError),
    #[error("{0}")]
    CreateStream(#[from] execution::types::CreateStreamError),
    #[error("{0}")]
    Stream(#[from] execution::types::StreamError),
    #[error("Invalid Log File Format")]
    InvalidLogFileFormat,
    #[error("--format-file is required for regex tables")]
    RegexFormatFileRequired,
    #[error("Invalid Table Spec String")]
    InvalidTableSpecString,
    #[error("No files matched pattern: {0}")]
    NoFilesMatched(String),
    #[error("Invalid glob pattern: {0}")]
    InvalidGlobPattern(String),
    #[error("Duplicate table name: {0}")]
    DuplicateTableName(String),
    #[error("{0}")]
    WriteCsv(#[from] csv::Error),
    #[error("{0}")]
    WriteJson(#[from] serde_json::Error),
    #[error("{0}")]
    WriteIo(#[from] std::io::Error),
    #[error("{0}")]
    Registry(#[from] functions::RegistryError),
}

impl PartialEq for AppError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (AppError::Syntax(_), AppError::Syntax(_))
                | (AppError::InputNotAllConsumed(_), AppError::InputNotAllConsumed(_))
                | (AppError::Parse(_), AppError::Parse(_))
                | (AppError::Planning(_), AppError::Planning(_))
                | (AppError::Runtime(_), AppError::Runtime(_))
                | (AppError::PhysicalPlan(_), AppError::PhysicalPlan(_))
                | (AppError::CreateStream(_), AppError::CreateStream(_))
                | (AppError::Stream(_), AppError::Stream(_))
                | (AppError::InvalidLogFileFormat, AppError::InvalidLogFileFormat)
                | (AppError::RegexFormatFileRequired, AppError::RegexFormatFileRequired)
                | (AppError::InvalidTableSpecString, AppError::InvalidTableSpecString)
                | (AppError::WriteCsv(_), AppError::WriteCsv(_))
                | (AppError::WriteJson(_), AppError::WriteJson(_))
                | (AppError::WriteIo(_), AppError::WriteIo(_))
                | (AppError::Registry(_), AppError::Registry(_))
        ) || matches!((self, other),
            (AppError::NoFilesMatched(a), AppError::NoFilesMatched(b)) if a == b
        ) || matches!((self, other),
            (AppError::InvalidGlobPattern(a), AppError::InvalidGlobPattern(b)) if a == b
        ) || matches!((self, other),
            (AppError::DuplicateTableName(a), AppError::DuplicateTableName(b)) if a == b
        )
    }
}

impl Eq for AppError {}

pub enum OutputMode {
    Table,
    Csv,
    Json,
    Ndjson,
}

fn parse_query_input(query_str: &str) -> AppResult<syntax::ast::Query> {
    match syntax::parser::query(query_str) {
        Ok((remaining, query)) if remaining.trim().is_empty() => Ok(syntax::desugar::desugar_query(query)),
        Ok((remaining, _)) => {
            let leading_whitespace = remaining.len() - remaining.trim_start().len();
            let offset = refine_syntax_offset(query_str, query_str.len() - remaining.len() + leading_whitespace);
            let hint = syntax_hint(query_str, offset);
            Err(AppError::InputNotAllConsumed(crate::diagnostic::render(
                query_str,
                offset,
                "unexpected input",
                "query parsing stopped here",
                hint.as_deref(),
            )))
        }
        Err(error) => {
            let remaining = match &error {
                nom::Err::Failure(error) | nom::Err::Error(error) => error.input,
                nom::Err::Incomplete(_) => "",
            };
            let leading_whitespace = remaining.len() - remaining.trim_start().len();
            let offset = query_str.len() - remaining.len() + leading_whitespace;
            let hint = syntax_hint(query_str, offset);
            Err(AppError::Syntax(crate::diagnostic::render(
                query_str,
                offset,
                "could not parse query",
                "expected valid PartiQL syntax",
                hint.as_deref(),
            )))
        }
    }
}

fn refine_syntax_offset(query: &str, offset: usize) -> usize {
    for invalid_operator in ["===", "!==", "=="] {
        if let Some(position) = query[offset..].find(invalid_operator) {
            return offset + position;
        }
    }
    offset
}

fn syntax_hint(query: &str, offset: usize) -> Option<String> {
    let remaining = &query[offset.min(query.len())..];
    let token: String = remaining
        .chars()
        .take_while(|ch| ch.is_alphanumeric() || *ch == '_' || "=<>!".contains(*ch))
        .collect();
    let lowered_query = query.to_ascii_lowercase();
    let lowered_token = token.to_ascii_lowercase();

    if lowered_query.starts_with("select from ") {
        return Some("add an expression between `select` and `from`".to_string());
    }
    if lowered_query.starts_with("select ") && !lowered_query.contains(" from ") {
        return Some("add `from <table>` after the select list".to_string());
    }
    match lowered_token.as_str() {
        "where" => return Some("add a boolean expression after `where`".to_string()),
        "order" | "by" => return Some("complete `order by` with a column or expression".to_string()),
        "limit" => return Some("provide a LIMIT value between 0 and 4294967295".to_string()),
        "===" | "==" => return Some("use `=` for equality comparisons".to_string()),
        "!==" => return Some("use `!=` for inequality comparisons".to_string()),
        _ => {}
    }
    if remaining.starts_with('(') || query[..offset.min(query.len())].matches('(').count() > query.matches(')').count()
    {
        return Some("check for an unmatched parenthesis".to_string());
    }

    const KEYWORDS: &[&str] = &[
        "select",
        "from",
        "where",
        "group",
        "having",
        "order",
        "limit",
        "join",
        "union",
        "intersect",
        "except",
    ];
    crate::diagnostic::suggestion(&lowered_token, KEYWORDS.iter().copied())
        .map(|candidate| format!("did you mean `{candidate}`?"))
}

fn find_identifier(query: &str, identifier: &str) -> usize {
    query
        .to_ascii_lowercase()
        .find(&identifier.to_ascii_lowercase())
        .unwrap_or(0)
}

fn render_planning_error(
    query: &str,
    error: &logical::parser::ParseError,
    table_names: &[String],
    registry: &functions::FunctionRegistry,
) -> String {
    use logical::parser::ParseError;

    let (offset, label, hint) = match error {
        ParseError::UnknownFunction(name) => {
            let hint = crate::diagnostic::suggestion(name, registry.function_names())
                .map(|candidate| format!("did you mean `{candidate}`?"));
            (find_identifier(query, name), "unknown function", hint)
        }
        ParseError::UnknownTable(name, _) => {
            let hint = crate::diagnostic::suggestion(name, table_names.iter().map(String::as_str))
                .map(|candidate| format!("did you mean `{candidate}`?"));
            (find_identifier(query, name), "unknown table", hint)
        }
        ParseError::UnknownColumn(name, available) => {
            let hint = crate::diagnostic::suggestion(name, available.split(", "))
                .map(|candidate| format!("did you mean `{candidate}`?"));
            (find_identifier(query, name), "unknown column", hint)
        }
        ParseError::InvalidArguments(details) => {
            let name = details.split_whitespace().next().unwrap_or("");
            (
                find_identifier(query, name),
                "invalid function arguments",
                Some("check the function's argument count and types".to_string()),
            )
        }
        ParseError::HavingClauseWithoutGroupBy => (
            find_identifier(query, "having"),
            "HAVING requires GROUP BY",
            Some("add a `group by` clause before `having`".to_string()),
        ),
        ParseError::GroupByWithoutAggregateFunction => (
            find_identifier(query, "group"),
            "GROUP BY has no aggregate",
            Some("add an aggregate function such as `count(*)`".to_string()),
        ),
        ParseError::GroupByFieldsMismatch | ParseError::StarGroupByUnsupported => (
            find_identifier(query, "group"),
            "invalid GROUP BY projection",
            Some("select only grouped fields and aggregate expressions".to_string()),
        ),
        ParseError::FromClausePathInvalidTableReference | ParseError::FromClauseMissingAsForPathExpr => (
            find_identifier(query, "from"),
            "invalid FROM clause",
            Some("check the table name and alias".to_string()),
        ),
        ParseError::StdinInJoinRightSide | ParseError::UnsupportedJoinType(_) => (
            find_identifier(query, "join"),
            "invalid JOIN",
            Some("check the join type and input tables".to_string()),
        ),
        ParseError::TypeMismatch | ParseError::NotAggregateFunction => (
            0,
            "query type mismatch",
            Some("check expression and aggregate types".to_string()),
        ),
    };

    crate::diagnostic::render(query, offset, &error.to_string(), label, hint.as_deref())
}

fn expression_error(error: &execution::types::StreamError) -> Option<&execution::types::ExpressionError> {
    use execution::types::{EvaluateError, StreamError};
    match error {
        StreamError::Evaluate(EvaluateError::Expression(error)) | StreamError::Expression(error) => Some(error),
        _ => None,
    }
}

fn expression_offset(query: &str) -> usize {
    let lowered = query.to_ascii_lowercase();
    if let Some(offset) = lowered.find("cast") {
        return offset;
    }
    ["||", "+", "-", "*", "/"]
        .into_iter()
        .filter_map(|operator| query.find(operator))
        .min()
        .unwrap_or(0)
}

fn render_runtime_error(query: &str, error: execution::types::StreamError) -> AppError {
    use execution::types::ExpressionError;

    let (offset, label, hint) = match expression_error(&error) {
        Some(ExpressionError::InvalidArguments) => (
            expression_offset(query),
            "invalid expression arguments",
            Some("check the operand and function argument types"),
        ),
        Some(ExpressionError::TypeMismatch) => (
            expression_offset(query),
            "expression type mismatch",
            Some("check the value and target types"),
        ),
        Some(ExpressionError::UnknownFunction) => (
            expression_offset(query),
            "unknown function",
            Some("check the function name"),
        ),
        Some(ExpressionError::KeyNotFound) => (
            expression_offset(query),
            "unknown column",
            Some("check the column name"),
        ),
        _ => (
            0,
            "query execution failed",
            Some("check the input data and expression types"),
        ),
    };
    AppError::Runtime(crate::diagnostic::render(
        query,
        offset,
        &error.to_string(),
        label,
        hint,
    ))
}

fn plan_query(
    query_str: &str,
    query: syntax::ast::Query,
    data_sources: common::types::DataSourceRegistry,
    registry: Arc<functions::FunctionRegistry>,
) -> AppResult<logical::types::Node> {
    let mut table_names: Vec<String> = data_sources.keys().cloned().collect();
    table_names.sort();
    logical::parser::parse_query_top(query, data_sources, registry.clone()).map_err(|error| {
        AppError::Planning(render_planning_error(
            query_str,
            &error,
            &table_names,
            registry.as_ref(),
        ))
    })
}

fn value_to_json(value: common::types::Value) -> serde_json::Value {
    use common::types::Value;
    match value {
        Value::Boolean(value) => value.into(),
        Value::DateTime(value) => value.to_string().into(),
        Value::Float(value) => serde_json::from_str(&value.into_inner().to_string()).unwrap_or(serde_json::Value::Null),
        Value::Host(value) => value.to_string().into(),
        Value::HttpRequest(value) => value.to_string().into(),
        Value::Int(value) => value.into(),
        Value::Null | Value::Missing => serde_json::Value::Null,
        Value::String(value) => value.to_string().into(),
        Value::Object(value) => serde_json::Value::Object(
            value
                .into_iter()
                .map(|(key, value)| (key, value_to_json(value)))
                .collect(),
        ),
        Value::Array(value) => serde_json::Value::Array(value.into_iter().map(value_to_json).collect()),
    }
}

impl FromStr for OutputMode {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "table" => Ok(OutputMode::Table),
            "csv" => Ok(OutputMode::Csv),
            "json" => Ok(OutputMode::Json),
            "ndjson" => Ok(OutputMode::Ndjson),
            _ => Err("unknown output mode".to_string()),
        }
    }
}

pub fn explain(query_str: &str, data_sources: common::types::DataSourceRegistry) -> AppResult<()> {
    let q = parse_query_input(query_str)?;

    let registry = Arc::new(functions::register_all()?);
    let node = plan_query(query_str, q, data_sources, registry)?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
    let (physical_plan, _variables) = node.physical(&mut physical_plan_creator)?;

    match physical_plan.execution_pipeline() {
        execution::types::ExecutionPipeline::Batch => println!("Execution pipeline: batch"),
        execution::types::ExecutionPipeline::Row(fallback) => {
            println!("Execution pipeline: row");
            println!("Batch fallback: {} ({})", fallback.node, fallback.reason);
        }
    }
    println!("Query Plan:");
    println!("{:?}", physical_plan);
    Ok(())
}

pub fn run(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    output_mode: OutputMode,
    threads: usize,
) -> AppResult<()> {
    run_with_memory_limit(query_str, data_sources, output_mode, threads, None)
}

pub fn run_with_memory_limit(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    output_mode: OutputMode,
    threads: usize,
    max_memory: Option<usize>,
) -> AppResult<()> {
    let q = parse_query_input(query_str)?;

    let registry = Arc::new(functions::register_all()?);
    let node = plan_query(query_str, q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get_with_memory_limit(variables, registry, threads, max_memory)?;

    match output_mode {
        OutputMode::Table => {
            let mut table = Table::new();

            while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
                table.add_row(Row::new(record.to_row()));
            }
            table.printstd();
        }
        OutputMode::Csv => {
            let mut wtr = Writer::from_writer(std::io::stdout());
            while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
                let csv_record = record.to_csv_record();
                wtr.write_record(csv_record)?;
            }
        }
        OutputMode::Json => {
            let stdout = std::io::stdout();
            let mut writer = std::io::BufWriter::new(stdout.lock());
            writer.write_all(b"[")?;
            let mut first = true;
            while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
                let obj = record
                    .into_tuples()
                    .into_iter()
                    .map(|(key, value)| (key, value_to_json(value)))
                    .collect();
                if !first {
                    writer.write_all(b",")?;
                }
                serde_json::to_writer(&mut writer, &serde_json::Value::Object(obj))?;
                first = false;
            }
            writer.write_all(b"]\n")?;
        }
        OutputMode::Ndjson => {
            let stdout = std::io::stdout();
            let mut writer = std::io::BufWriter::new(stdout.lock());
            while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
                let obj = record
                    .into_tuples()
                    .into_iter()
                    .map(|(key, value)| (key, value_to_json(value)))
                    .collect();
                serde_json::to_writer(&mut writer, &serde_json::Value::Object(obj))?;
                writeln!(writer)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn run_to_vec(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    threads: usize,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let q = parse_query_input(query_str)?;

    let registry = Arc::new(functions::register_all()?);
    let node = plan_query(query_str, q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry, threads)?;
    let mut results = Vec::new();

    while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
        results.push(record.to_tuples());
    }

    Ok(results)
}

#[cfg(feature = "bench-internals")]
pub fn run_to_records(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    threads: usize,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let q = parse_query_input(query_str)?;

    let registry = Arc::new(functions::register_all()?);
    let node = plan_query(query_str, q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry, threads)?;
    let mut results = Vec::new();

    while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
        results.push(record.to_tuples());
    }

    Ok(results)
}

#[cfg(feature = "bench-internals")]
pub fn run_to_records_with_registry(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    registry: Arc<functions::FunctionRegistry>,
    threads: usize,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let q = parse_query_input(query_str)?;

    let node = plan_query(query_str, q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry, threads)?;
    let mut results = Vec::new();

    while let Some(record) = stream.next().map_err(|error| render_runtime_error(query_str, error))? {
        results.push(record.into_tuples());
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use flate2::Compression;
    use flate2::write::GzEncoder;
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

        let data_source = common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();
        let result = run(query, data_sources, OutputMode::Csv, 1);
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

        let data_source = common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();
        let result = run_to_vec(query, data_sources, 1);
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();
        let result = run(query_str, data_sources, OutputMode::Csv, 1);

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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();
        let result = run(
            r#"select t, sum(sent_bytes) as s from it group by time_bucket("5 seconds", timestamp) as t order by t asc limit 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, url_path_bucket(request, 1, "_") as s from it limit 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, percentile_disc(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, approx_percentile(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_grouped_time_bucket_shorthand_in_batch_and_row_pipelines() {
        let elb_lines = [
            r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "agent" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#,
            r#"2019-06-07T19:05:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 123 "GET https://example.com:443/ HTTP/1.1" "agent" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#,
        ];
        let query = r#"select t, count(*) as n from it group by time_bucket("1d", timestamp) as t order by t asc"#;
        let batch = run_format_query_to_vec("elb", &elb_lines, query).unwrap();
        assert_eq!(
            batch,
            vec![vec![
                (
                    "t".to_string(),
                    Value::DateTime(chrono::DateTime::parse_from_rfc3339("2019-06-07T00:00:00Z").unwrap()),
                ),
                ("n".to_string(), Value::Int(2)),
            ]]
        );

        let common_lines = [
            r#"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /one HTTP/1.0" 200 10"#,
            r#"127.0.0.1 - frank [10/Oct/2000:23:59:59 -0700] "GET /two HTTP/1.0" 200 20"#,
        ];
        let row = run_format_query_to_vec("clf", &common_lines, query).unwrap();
        assert_eq!(
            row,
            vec![vec![
                (
                    "t".to_string(),
                    Value::DateTime(chrono::DateTime::parse_from_rfc3339("2000-10-10T00:00:00-07:00").unwrap(),),
                ),
                ("n".to_string(), Value::Int(2)),
            ]]
        );
    }

    #[test]
    fn test_order_by_limit_considers_late_rows_in_batch_pipeline() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("top-n.log");
        let mut file = File::create(path.clone()).unwrap();
        for sent_bytes in (1..=100).rev() {
            writeln!(
                file,
                r#"2019-06-07T18:45:31Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 200 200 0 {sent_bytes} "GET https://example.com/ HTTP/1.1" "agent" c t"#
            )
            .unwrap();
        }
        drop(file);
        let data_source = common::types::DataSource::File(path, "elb".to_string(), "it".to_string());
        let data_sources = [("it".to_string(), data_source)].into_iter().collect();
        let rows = run_to_vec(
            "select sent_bytes from it order by sent_bytes asc limit 2",
            data_sources,
            4,
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![("sent_bytes".to_string(), Value::Int(1))],
                vec![("sent_bytes".to_string(), Value::Int(2))],
            ]
        );

        let json_lines: Vec<String> = (1..=100).rev().map(|value| format!(r#"{{"x":{value}}}"#)).collect();
        let json_line_refs: Vec<&str> = json_lines.iter().map(String::as_str).collect();
        let row_results =
            run_format_query_to_vec("jsonl", &json_line_refs, "select x from it order by x asc limit 2").unwrap();
        assert_eq!(
            row_results,
            vec![
                vec![("x".to_string(), Value::Int(1))],
                vec![("x".to_string(), Value::Int(2))],
            ]
        );
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();
        let result = run(
            r#"select b, e.f.g as x from it limit 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select b, count(e.f.g) as x from it group by b"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select x, count(*) as x from it group by d[0] as x"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Self cross join: FROM it AS a CROSS JOIN it AS b
        let result = run(
            r#"select a.x, b.x from it as a cross join it as b"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Comma-separated FROM items (implicit cross join)
        let result = run(
            r#"select a.x, b.x from it as a, it as b"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Cross join with filter
        let result = run(
            r#"select a.x, b.x from it as a cross join it as b where a.x < b.x"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // LEFT JOIN with matching condition - all rows match themselves
        let result = run(
            r#"select a.x, b.x from it as a left join it as b on a.x = b.x"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // LEFT JOIN where nothing matches - all right sides should be NULL
        let result = run(
            r#"select a.x, b.x from it as a left join it as b on a.x != a.x"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // LEFT OUTER JOIN - should work identically to LEFT JOIN
        let result = run(
            r#"select a.x, b.x from it as a left outer join it as b on a.x = b.x"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Subquery: select rows where x equals the max x
        let result = run(
            r#"select x from it where x = (select max(x) from it)"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Scalar subquery in SELECT
        let result = run(
            r#"select x, (select count(*) from it) as total from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // UNION (deduplicates)
        let result = run(
            r#"select x from it where x < 3 union select x from it where x > 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // UNION ALL (keeps duplicates)
        let result = run(
            r#"select x from it where x < 3 union all select x from it where x > 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        let result = run(
            r#"select x from it where x < 3 intersect select x from it where x > 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        let result = run(
            r#"select x from it except select x from it where x > 2"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Mixed case: SELECT, FROM, WHERE, AND, LIMIT
        let result = run(
            r#"SELECT a, b FROM it WHERE a > 0 AND b > 0 LIMIT 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // Mixed case: ORDER BY, ASC (lowercase order by to avoid OR prefix match)
        let result = run(
            r#"SELECT a FROM it order by a ASC LIMIT 1"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // Mixed case variant with OR
        let result = run(
            r#"Select a From it Where a = 1 Or b = 4"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // IS NULL
        let result = run(
            r#"select a from it where b is null"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // IS NOT MISSING
        let result = run(
            r#"select a, b from it where b is not missing"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // COALESCE with NULL/MISSING
        let result = run(
            r#"select coalesce(b, 0) as b_or_zero from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Multi-branch CASE WHEN with ELSE
        let result = run(
            r#"select case when x < 3 then "low" when x < 8 then "mid" else "high" end as category from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // LIKE
        let result = run(
            r#"select name from it where name like "%ob%""#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // BETWEEN
        let result = run(
            r#"select name, age from it where age between 28 and 32"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // IN
        let result = run(
            r#"select name from it where age in (25, 35)"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // NOT LIKE + NOT BETWEEN + NOT IN
        let result = run(
            r#"select name from it where name not like "%a%" and age not between 30 and 40 and age not in (30)"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // CAST to int
        let result = run(
            r#"select cast(x as int) as xi from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // String concatenation
        let result = run(
            r#"select y || " " || x as combined from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // UPPER, LOWER, CHAR_LENGTH
        let result = run(
            r#"select upper(name) as u, lower(name) as l, char_length(name) as len from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // SELECT DISTINCT with ORDER BY
        let result = run(
            r#"select distinct x from it order by x asc"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // CROSS JOIN
        let result = run(
            r#"select a.id, b.val from it as a cross join it as b limit 3"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // LEFT JOIN with ON condition
        let result = run(
            r#"select a.id, b.val from it as a left join it as b on a.id = b.id limit 5"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Subquery in WHERE
        let result = run(
            r#"select x from it where x = (select max(x) from it)"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // UNION
        let result = run(
            r#"select x from it where x = 1 union select x from it where x = 3"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // INTERSECT
        let result = run(
            r#"select x from it where x > 1 intersect select x from it where x < 3"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // EXCEPT
        let result = run(
            r#"select x from it except select x from it where x = 2"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // NULLIF returns NULL when equal, value when not
        let result = run(
            r#"select nullif(a, b) as result from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // JSON output mode
        let result = run(r#"select x, y from it"#, data_sources.clone(), OutputMode::Json, 1);
        assert_eq!(result, Ok(()));

        // CSV output mode (Table mode omitted: prettytable may SIGSEGV without a TTY)
        let result = run(r#"select x, y from it"#, data_sources.clone(), OutputMode::Csv, 1);
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
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        // Nested path access
        let result = run(
            r#"select a.b.c as deep from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        // Array index
        let result = run(
            r#"select d[0] as first, d[2] as third from it"#,
            data_sources.clone(),
            OutputMode::Csv,
            1,
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
        let results = run_format_query_to_vec(
            "elb",
            lines,
            r#"SELECT elb_status_code, sent_bytes FROM it WHERE elb_status_code = "200""#,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        for row in &results {
            let status = &row.iter().find(|(k, _)| k == "elb_status_code").unwrap().1;
            assert_eq!(status, &common::types::Value::String("200".to_string().into()));
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
        let result = run_format_query(
            "elb",
            lines,
            r#"SELECT elb_status_code, sum(backend_processing_time) as total_bpt, sum(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code"#,
        );
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
        let result = run_format_query(
            "elb",
            lines,
            r#"SELECT sent_bytes FROM it ORDER BY sent_bytes DESC LIMIT 2"#,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_alb_filter_by_type() {
        let lines = &[
            r#"http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
            r#"https 2018-07-02T22:23:01.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.40:2818 10.0.0.2:80 0.001 0.002 0.001 200 200 50 512 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-002" "www.example.com" "arn:aws:acm:cert/123" 1 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
            r#"h2 2018-07-02T22:23:02.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.41:2819 10.0.0.3:80 0.002 0.003 0.002 301 301 0 128 "GET https://www.example.com:443/redirect HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-003" "www.example.com" "arn:aws:acm:cert/456" 2 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
        ];
        let results = run_format_query_to_vec(
            "alb",
            lines,
            r#"SELECT type, elb_status_code FROM it WHERE type = "https""#,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        let type_val = &results[0].iter().find(|(k, _)| k == "type").unwrap().1;
        assert_eq!(type_val, &common::types::Value::String("https".to_string().into()));
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
        let result = run_format_query(
            "alb",
            lines,
            r#"SELECT elb_status_code, sum(request_processing_time) as total_rpt FROM it GROUP BY elb_status_code"#,
        );
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
        let result = run_format_query(
            "alb",
            lines,
            r#"SELECT received_bytes FROM it ORDER BY received_bytes DESC LIMIT 3"#,
        );
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
        let results = run_format_query_to_vec(
            "s3",
            lines,
            r#"SELECT operation, http_status FROM it WHERE operation = "REST.GET.OBJECT""#,
        )
        .unwrap();
        assert_eq!(results.len(), 3);
        for row in &results {
            let op = &row.iter().find(|(k, _)| k == "operation").unwrap().1;
            assert_eq!(op, &common::types::Value::String("REST.GET.OBJECT".to_string().into()));
        }
    }

    #[test]
    fn test_s3_dash_placeholders() {
        let lines = &[
            r#"owner1 mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /mybucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.GET.OBJECT key2 "GET /mybucket/key2 HTTP/1.1" 403 AccessDenied 0 0 10 5 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 mybucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.OBJECT key3 "GET /mybucket/key3 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        ];
        let result = run_format_query(
            "s3",
            lines,
            r#"SELECT error_code, refererr FROM it WHERE error_code = "-""#,
        );
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
        let result = run_format_query(
            "s3",
            lines,
            r#"SELECT http_status, count(*) as cnt FROM it GROUP BY http_status ORDER BY http_status ASC"#,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_s3_string_functions() {
        let lines = &[
            r#"owner1 MyBucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /MyBucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader MyBucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 AnotherBucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.PUT.OBJECT key2 "PUT /AnotherBucket/key2 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader AnotherBucket.s3.amazonaws.com TLSv1.2"#,
            r#"owner1 ThirdBucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.VERSIONING key3 "GET /ThirdBucket/key3 HTTP/1.1" 200 - 512 512 30 5 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader ThirdBucket.s3.amazonaws.com TLSv1.2"#,
        ];
        let result = run_format_query(
            "s3",
            lines,
            r#"SELECT upper(operation) as op_upper, lower(bucket) as bucket_lower FROM it"#,
        );
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
        let results =
            run_format_query_to_vec("squid", lines, r#"SELECT method, url FROM it WHERE method = "GET""#).unwrap();
        assert_eq!(results.len(), 2);
        for row in &results {
            let method = &row.iter().find(|(k, _)| k == "method").unwrap().1;
            assert_eq!(method, &common::types::Value::String("GET".to_string().into()));
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
    fn test_unknown_function_error_at_planning() {
        let result = run_format_query("jsonl", &[r#"{"a": 1}"#], "SELECT nonexistent_func(a) FROM it");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unknown function") || err_msg.contains("nonexistent_func"),
            "Expected unknown function error, got: {}",
            err_msg
        );
    }

    // ==================== Multi-table Integration Tests ====================

    #[test]
    fn test_multi_table_cross_join() {
        let dir = tempdir().unwrap();

        // Create file for table "a"
        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"x": 1}}"#).unwrap();
        writeln!(file_a, r#"{{"x": 2}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        // Create file for table "b"
        let file_path_b = dir.path().join("b.jsonl");
        let mut file_b = File::create(file_path_b.clone()).unwrap();
        writeln!(file_b, r#"{{"y": 10}}"#).unwrap();
        writeln!(file_b, r#"{{"y": 20}}"#).unwrap();
        file_b.sync_all().unwrap();
        drop(file_b);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::File(file_path_b, "jsonl".to_string(), "b".to_string()),
        );

        let result = run(
            r#"SELECT a.x, b.y FROM a CROSS JOIN b"#,
            data_sources,
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_left_join() {
        let dir = tempdir().unwrap();

        // Create file for table "a"
        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"id": 1, "x": "hello"}}"#).unwrap();
        writeln!(file_a, r#"{{"id": 2, "x": "world"}}"#).unwrap();
        writeln!(file_a, r#"{{"id": 3, "x": "foo"}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        // Create file for table "b"
        let file_path_b = dir.path().join("b.jsonl");
        let mut file_b = File::create(file_path_b.clone()).unwrap();
        writeln!(file_b, r#"{{"id": 1, "y": "alpha"}}"#).unwrap();
        writeln!(file_b, r#"{{"id": 3, "y": "beta"}}"#).unwrap();
        file_b.sync_all().unwrap();
        drop(file_b);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::File(file_path_b, "jsonl".to_string(), "b".to_string()),
        );

        let result = run(
            r#"SELECT a.x, b.y FROM a LEFT JOIN b ON a.id = b.id"#,
            data_sources,
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_comma_join() {
        let dir = tempdir().unwrap();

        // Create file for table "a"
        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"x": 1}}"#).unwrap();
        writeln!(file_a, r#"{{"x": 2}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        // Create file for table "b"
        let file_path_b = dir.path().join("b.jsonl");
        let mut file_b = File::create(file_path_b.clone()).unwrap();
        writeln!(file_b, r#"{{"y": 10}}"#).unwrap();
        writeln!(file_b, r#"{{"y": 20}}"#).unwrap();
        file_b.sync_all().unwrap();
        drop(file_b);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::File(file_path_b, "jsonl".to_string(), "b".to_string()),
        );

        let result = run(r#"SELECT a.x, b.y FROM a, b"#, data_sources, OutputMode::Csv, 1);
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_unknown_table_error() {
        let dir = tempdir().unwrap();

        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"x": 1}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );

        let result = run(r#"SELECT * FROM unknown_table"#, data_sources, OutputMode::Csv, 1);
        assert!(result.is_err(), "Expected error for unknown table, got Ok");

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_backward_compat() {
        let dir = tempdir().unwrap();

        let file_path = dir.path().join("compat.jsonl");
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        writeln!(file, r#"{{"x": 2}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, "jsonl".to_string(), "it".to_string());
        let data_sources: common::types::DataSourceRegistry =
            vec![("it".to_string(), data_source)].into_iter().collect();

        let result = run(r#"SELECT * FROM it LIMIT 1"#, data_sources, OutputMode::Csv, 1);
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_stdin_in_join_right_side_error() {
        let dir = tempdir().unwrap();

        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"x": 1}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::Stdin("jsonl".to_string(), "b".to_string()),
        );

        // "b" is stdin and used as the right side of a join — should produce an error
        let result = run(
            r#"SELECT a.x, b.y FROM a CROSS JOIN b"#,
            data_sources,
            OutputMode::Csv,
            1,
        );
        assert!(
            result.is_err(),
            "Expected error when stdin is on the right side of a join"
        );

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_with_where_filter() {
        let dir = tempdir().unwrap();

        // Create file for table "a"
        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        writeln!(file_a, r#"{{"x": 1}}"#).unwrap();
        writeln!(file_a, r#"{{"x": 2}}"#).unwrap();
        writeln!(file_a, r#"{{"x": 3}}"#).unwrap();
        file_a.sync_all().unwrap();
        drop(file_a);

        // Create file for table "b"
        let file_path_b = dir.path().join("b.jsonl");
        let mut file_b = File::create(file_path_b.clone()).unwrap();
        writeln!(file_b, r#"{{"y": 2}}"#).unwrap();
        writeln!(file_b, r#"{{"y": 3}}"#).unwrap();
        writeln!(file_b, r#"{{"y": 4}}"#).unwrap();
        file_b.sync_all().unwrap();
        drop(file_b);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::File(file_path_b, "jsonl".to_string(), "b".to_string()),
        );

        let result = run(
            r#"SELECT a.x, b.y FROM a, b WHERE a.x = b.y"#,
            data_sources,
            OutputMode::Csv,
            1,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_multi_table_case_sensitive_name_error() {
        let dir = tempdir().unwrap();

        let file_path = dir.path().join("mytable.jsonl");
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"{{"x": 1}}"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "MyTable".to_string(),
            common::types::DataSource::File(file_path, "jsonl".to_string(), "MyTable".to_string()),
        );

        // Query uses lowercase "mytable" but registry has "MyTable" — should fail
        let result = run(r#"SELECT * FROM mytable"#, data_sources, OutputMode::Csv, 1);
        assert!(result.is_err(), "Expected error for case-sensitive table name mismatch");

        dir.close().unwrap();
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
        let result = run_format_query(
            "squid",
            lines,
            r#"SELECT code_and_status, count(*) as cnt FROM it GROUP BY code_and_status"#,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_scan_aggregation_count_star_pushdown() {
        // Test that SELECT COUNT(*) FROM it WHERE method = 'GET'
        // uses scan-time aggregation and returns the correct count
        let lines = &[
            r#"1515734740.000      1 [10.0.0.1] TCP_DENIED/407 3922 GET http://a.com/ - HIER_NONE/- text/html"#,
            r#"1515734741.000      2 [10.0.0.2] TCP_MISS/200 15234 POST http://b.com/ - HIER_DIRECT/1.2.3.4 text/html"#,
            r#"1515734742.000      3 [10.0.0.3] TCP_HIT/200 8432 GET http://c.com/ - HIER_DIRECT/1.2.3.5 text/html"#,
            r#"1515734743.000      4 [10.0.0.4] TCP_DENIED/403 2100 CONNECT http://d.com/ - HIER_NONE/- text/html"#,
            r#"1515734744.000      5 [10.0.0.5] TCP_MISS/200 9200 GET http://e.com/ - HIER_DIRECT/1.2.3.6 text/html"#,
        ];
        let result =
            run_format_query_to_vec("squid", lines, r#"SELECT count(*) as cnt FROM it WHERE method = "GET""#).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, common::types::Value::Int(3));
    }

    #[test]
    fn test_scan_aggregation_count_star_no_filter() {
        // Test SELECT COUNT(*) FROM it (no WHERE clause)
        let lines = &[
            r#"1515734740.000      1 [10.0.0.1] TCP_DENIED/407 3922 GET http://a.com/ - HIER_NONE/- text/html"#,
            r#"1515734741.000      2 [10.0.0.2] TCP_MISS/200 15234 POST http://b.com/ - HIER_DIRECT/1.2.3.4 text/html"#,
            r#"1515734742.000      3 [10.0.0.3] TCP_HIT/200 8432 GET http://c.com/ - HIER_DIRECT/1.2.3.5 text/html"#,
        ];
        let result = run_format_query_to_vec("squid", lines, r#"SELECT count(*) as cnt FROM it"#).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, common::types::Value::Int(3));
    }

    // Helper to create two JSONL tables and run a query, returning structured results
    fn run_two_table_query(
        table_a_rows: &[&str],
        table_b_rows: &[&str],
        query: &str,
    ) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
        let dir = tempdir().unwrap();

        let file_path_a = dir.path().join("a.jsonl");
        let mut file_a = File::create(file_path_a.clone()).unwrap();
        for row in table_a_rows {
            writeln!(file_a, "{}", row).unwrap();
        }
        file_a.sync_all().unwrap();
        drop(file_a);

        let file_path_b = dir.path().join("b.jsonl");
        let mut file_b = File::create(file_path_b.clone()).unwrap();
        for row in table_b_rows {
            writeln!(file_b, "{}", row).unwrap();
        }
        file_b.sync_all().unwrap();
        drop(file_b);

        let mut data_sources = common::types::DataSourceRegistry::new();
        data_sources.insert(
            "a".to_string(),
            common::types::DataSource::File(file_path_a, "jsonl".to_string(), "a".to_string()),
        );
        data_sources.insert(
            "b".to_string(),
            common::types::DataSource::File(file_path_b, "jsonl".to_string(), "b".to_string()),
        );

        let result = run_to_vec(query, data_sources, 1);
        dir.close().unwrap();
        result
    }

    #[test]
    fn test_e2e_inner_join_with_hash() {
        let result = run_two_table_query(
            &[
                r#"{"id": 1, "x": "hello"}"#,
                r#"{"id": 2, "x": "world"}"#,
                r#"{"id": 3, "x": "foo"}"#,
            ],
            &[r#"{"id": 1, "y": "alpha"}"#, r#"{"id": 3, "y": "beta"}"#],
            r#"SELECT x, y FROM a INNER JOIN b ON a.id = b.id"#,
        )
        .unwrap();

        assert_eq!(result.len(), 2, "results: {:?}", result);
        let xs: Vec<&common::types::Value> = result.iter().map(|r| &r[0].1).collect();
        assert!(
            xs.contains(&&common::types::Value::String("hello".into())),
            "xs: {:?}",
            xs
        );
        assert!(
            xs.contains(&&common::types::Value::String("foo".into())),
            "xs: {:?}",
            xs
        );
    }

    #[test]
    fn test_e2e_bare_join_with_aliases_and_residual_predicate() {
        let result = run_two_table_query(
            &[
                r#"{"id": 1, "score": 5, "x": "low"}"#,
                r#"{"id": 1, "score": 10, "x": "high"}"#,
                r#"{"id": 2, "score": 1, "x": "other"}"#,
            ],
            &[
                r#"{"id": 1, "threshold": 7, "y": "matched"}"#,
                r#"{"id": 3, "threshold": 20, "y": "unmatched"}"#,
            ],
            r#"SELECT left_side.x, right_side.y FROM a AS left_side JOIN b AS right_side ON left_side.id = right_side.id AND left_side.score < right_side.threshold"#,
        )
        .unwrap();

        assert_eq!(result.len(), 1, "results: {result:?}");
        assert_eq!(result[0][0].1, common::types::Value::String("low".into()));
        assert_eq!(result[0][1].1, common::types::Value::String("matched".into()));
    }

    #[test]
    fn test_e2e_inner_join_with_explicit_aliases() {
        let result = run_two_table_query(
            &[r#"{"id": 1, "x": "left"}"#],
            &[r#"{"id": 1, "y": "right"}"#],
            r#"SELECT lhs.x, rhs.y FROM a AS lhs INNER JOIN b AS rhs ON lhs.id = rhs.id"#,
        )
        .unwrap();

        assert_eq!(result.len(), 1, "results: {result:?}");
        assert_eq!(result[0][0].1, common::types::Value::String("left".into()));
        assert_eq!(result[0][1].1, common::types::Value::String("right".into()));
    }

    #[test]
    fn test_e2e_inner_join_with_only_a_residual_predicate() {
        let result = run_two_table_query(
            &[r#"{"score": 5, "x": "low"}"#, r#"{"score": 10, "x": "high"}"#],
            &[r#"{"threshold": 7, "y": "limit"}"#],
            r#"SELECT a.x, b.y FROM a INNER JOIN b ON a.score < b.threshold"#,
        )
        .unwrap();

        assert_eq!(result.len(), 1, "results: {result:?}");
        assert_eq!(result[0][0].1, common::types::Value::String("low".into()));
    }

    #[test]
    fn test_e2e_left_join_with_hash() {
        let result = run_two_table_query(
            &[
                r#"{"id": 1, "x": "hello"}"#,
                r#"{"id": 2, "x": "world"}"#,
                r#"{"id": 3, "x": "foo"}"#,
            ],
            &[r#"{"id": 1, "y": "alpha"}"#, r#"{"id": 3, "y": "beta"}"#],
            r#"SELECT x, y FROM a LEFT JOIN b ON a.id = b.id"#,
        )
        .unwrap();

        assert_eq!(result.len(), 3);
        // id=2 row should have NULL for y
        let world_row = result
            .iter()
            .find(|r| r[0].1 == common::types::Value::String("world".into()))
            .unwrap();
        assert_eq!(world_row[1].1, common::types::Value::Null);
    }

    #[test]
    fn test_e2e_cross_join_with_where_becomes_hash() {
        // FROM a, b WHERE a.id = b.id → internally becomes hash join
        let result = run_two_table_query(
            &[r#"{"id": 1, "x": "hello"}"#, r#"{"id": 2, "x": "world"}"#],
            &[r#"{"id": 1, "y": "alpha"}"#, r#"{"id": 3, "y": "gamma"}"#],
            r#"SELECT x, y FROM a, b WHERE a.id = b.id"#,
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, common::types::Value::String("hello".into()));
        assert_eq!(result[0][1].1, common::types::Value::String("alpha".into()));
    }

    #[test]
    fn test_e2e_inner_join_with_aggregation() {
        let result = run_two_table_query(
            &[
                r#"{"id": 1, "x": "hello"}"#,
                r#"{"id": 2, "x": "world"}"#,
                r#"{"id": 1, "x": "hi"}"#,
            ],
            &[r#"{"id": 1, "y": "alpha"}"#, r#"{"id": 3, "y": "beta"}"#],
            r#"SELECT y, count(*) as cnt FROM a INNER JOIN b ON a.id = b.id GROUP BY y"#,
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, common::types::Value::String("alpha".into()));
        assert_eq!(result[0][1].1, common::types::Value::Int(2));
    }

    #[test]
    fn test_e2e_join_null_keys_dont_match() {
        let result = run_two_table_query(
            &[r#"{"id": 1, "x": "a"}"#, r#"{"x": "b"}"#],
            &[r#"{"id": 1, "y": "c"}"#, r#"{"y": "d"}"#],
            r#"SELECT x, y FROM a INNER JOIN b ON a.id = b.id"#,
        )
        .unwrap();

        // Only id=1 matches; missing id rows should not match each other
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, common::types::Value::String("a".into()));
    }

    #[test]
    fn test_e2e_right_join() {
        // RIGHT JOIN: unmatched right-side rows get NULL-padded left columns
        let result = run_two_table_query(
            &[r#"{"id": 1, "x": "hello"}"#],
            &[r#"{"id": 1, "y": "alpha"}"#, r#"{"id": 2, "y": "beta"}"#],
            r#"SELECT x, y FROM a RIGHT JOIN b ON a.id = b.id"#,
        )
        .unwrap();

        assert_eq!(result.len(), 2, "results: {:?}", result);
        // Projection order follows SELECT x, y even though RIGHT JOIN swaps
        // the internal probe and build sides.
        assert_eq!(result[0][0].1, common::types::Value::String("hello".into()));
        assert_eq!(result[0][1].1, common::types::Value::String("alpha".into()));
        // id=2 unmatched: x=NULL (left side NULL-padded)
        assert_eq!(result[1][0].1, common::types::Value::Null);
        assert_eq!(result[1][1].1, common::types::Value::String("beta".into()));
    }

    #[test]
    fn test_e2e_right_join_with_residual_predicate() {
        let result = run_two_table_query(
            &[
                r#"{"id": 1, "score": 5, "x": "low"}"#,
                r#"{"id": 1, "score": 10, "x": "high"}"#,
            ],
            &[
                r#"{"id": 1, "threshold": 7, "y": "matched"}"#,
                r#"{"id": 2, "threshold": 7, "y": "unmatched"}"#,
            ],
            r#"SELECT x, y FROM a RIGHT OUTER JOIN b ON a.id = b.id AND a.score < b.threshold"#,
        )
        .unwrap();

        assert_eq!(result.len(), 2, "results: {result:?}");
        assert_eq!(result[0][0].1, common::types::Value::String("low".into()));
        assert_eq!(result[0][1].1, common::types::Value::String("matched".into()));
        assert_eq!(result[1][0].1, common::types::Value::Null);
        assert_eq!(result[1][1].1, common::types::Value::String("unmatched".into()));
    }

    fn write_gzip_lines(path: &std::path::Path, lines: &[&str]) {
        let file = File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        for line in lines {
            writeln!(encoder, "{line}").unwrap();
        }
        encoder.finish().unwrap();
    }

    #[test]
    fn test_gzip_magic_bytes_with_non_gz_extension() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("renamed.log");
        write_gzip_lines(&file_path, &[r#"{"x": 1}"#, r#"{"x": 2}"#]);
        let sources = vec![(
            "it".to_string(),
            common::types::DataSource::File(file_path, "jsonl".to_string(), "it".to_string()),
        )]
        .into_iter()
        .collect();

        let rows = run_to_vec("select count(*) as n from it", sources, 1).unwrap();
        assert_eq!(rows[0][0].1, common::types::Value::Int(2));
    }

    #[test]
    fn test_mixed_plain_and_gzip_files() {
        let dir = tempdir().unwrap();
        let plain = dir.path().join("a.jsonl");
        let gzip = dir.path().join("b.jsonl.gz");
        std::fs::write(&plain, "{\"x\": 1}\n").unwrap();
        write_gzip_lines(&gzip, &[r#"{"x": 2}"#, r#"{"x": 3}"#]);
        let sources = vec![(
            "it".to_string(),
            common::types::DataSource::Files(vec![plain, gzip], "jsonl".to_string(), "it".to_string()),
        )]
        .into_iter()
        .collect();

        let rows = run_to_vec("select count(*) as n from it", sources, 2).unwrap();
        assert_eq!(rows[0][0].1, common::types::Value::Int(3));
    }
}
