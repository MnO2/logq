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

#[derive(Fail, Debug)]
pub(crate) enum AppError {
    #[fail(display = "Syntax Error: {}", _0)]
    Syntax(String),
    #[fail(display = "Input is fully consumed, the leftover are \"{}\"", _0)]
    InputNotAllConsumed(String),
    #[fail(display = "{}", _0)]
    Parse(#[cause] logical::parser::ParseError),
    #[fail(display = "{}", _0)]
    PhysicalPlan(#[cause] logical::types::PhysicalPlanError),
    #[fail(display = "{}", _0)]
    CreateStream(#[cause] execution::types::CreateStreamError),
    #[fail(display = "{}", _0)]
    Stream(#[cause] execution::types::StreamError),
    #[fail(display = "Invalid Log File Format")]
    InvalidLogFileFormat,
    #[fail(display = "Invalid Table Spec String")]
    InvalidTableSpecString,
    #[fail(display = "{}", _0)]
    WriteCsv(#[cause] csv::Error),
    #[fail(display = "{}", _0)]
    WriteJson(#[cause] json::Error),
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

impl From<logical::parser::ParseError> for AppError {
    fn from(e: logical::parser::ParseError) -> AppError {
        AppError::Parse(e)
    }
}

impl From<logical::types::PhysicalPlanError> for AppError {
    fn from(err: logical::types::PhysicalPlanError) -> AppError {
        AppError::PhysicalPlan(err)
    }
}

impl From<execution::types::CreateStreamError> for AppError {
    fn from(err: execution::types::CreateStreamError) -> AppError {
        AppError::CreateStream(err)
    }
}

impl From<execution::types::StreamError> for AppError {
    fn from(err: execution::types::StreamError) -> AppError {
        AppError::Stream(err)
    }
}

impl From<csv::Error> for AppError {
    fn from(err: csv::Error) -> AppError {
        AppError::WriteCsv(err)
    }
}

impl From<json::Error> for AppError {
    fn from(err: json::Error) -> AppError {
        AppError::WriteJson(err)
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

pub(crate) fn explain(
    query_str: &str,
    data_source: common::types::DataSource
) -> AppResult<()> {
    let (rest_of_str, select_stmt) = syntax::parser::select_query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }

    let node = logical::parser::parse_query(select_stmt, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, _variables) = node.physical(&mut physical_plan_creator)?;

    println!("Query Plan:");
    println!("{:?}", physical_plan);
    Ok(())
}

pub(crate) fn run(
    query_str: &str,
    data_source: common::types::DataSource,
    _table_name: String,
    output_mode: OutputMode,
) -> AppResult<()> {
    let (rest_of_str, select_stmt) = syntax::parser::select_query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }

    let node = logical::parser::parse_query(select_stmt, data_source.clone())?;
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
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_run_explain_mode() {
        let query_str = "select * from it";
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("log_for_test.log");
        let file_format= "squid".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"1515734740.494      1 [MASKEDIPADDRESS] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone());
        let result = run(&*query_str, data_source,  table_name, OutputMode::Csv);

        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }

    #[test]
    fn test_run_real_mode() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("log_for_test.log");
        let file_format= "elb".to_string();
        let table_name = "it".to_string();
        let mut file = File::create(file_path.clone()).unwrap();
        writeln!(file, r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let data_source = common::types::DataSource::File(file_path, file_format.clone());
        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from it group by t order by t asc limit 1"#,
            data_source.clone(),
            table_name.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, percentile_disc(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_source.clone(),
            table_name.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        let result = run(
            r#"select time_bucket("5 seconds", timestamp) as t, approx_percentile(0.9) within group (order by backend_processing_time asc) as bps from it group by t"#,
            data_source.clone(),
            table_name.clone(),
            OutputMode::Csv,
        );
        assert_eq!(result, Ok(()));

        dir.close().unwrap();
    }
}
