#[macro_use]
extern crate failure;
extern crate chrono;
extern crate nom;
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

mod common;
mod execution;
mod logical;
mod syntax;

use clap::load_yaml;
use clap::App;
use csv::Writer;
use nom::error::VerboseError;
use prettytable::{Cell, Row, Table};
use std::path::Path;
use std::result;
use std::str::FromStr;

pub(crate) type AppResult<T> = result::Result<T, AppError>;

#[derive(Fail, Debug)]
pub enum AppError {
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
    #[fail(display = "{}", _0)]
    WriteCsv(#[cause] csv::Error),
}

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

enum OutputMode {
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

fn run(
    query_str: &str,
    data_source: common::types::DataSource,
    explain_mode: bool,
    output_mode: OutputMode,
) -> AppResult<()> {
    let (rest_of_str, select_stmt) = syntax::parser::select_query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }

    if select_stmt.table_name != "elb" {
        return Err(AppError::InvalidLogFileFormat);
    }
    let node = logical::parser::parse_query(select_stmt, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    if explain_mode {
        println!("Query Plan:");
        println!("{:?}", physical_plan);
        Ok(())
    } else {
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
            OutputMode::Json => {}
        }

        Ok(())
    }
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("query", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let lower_case_query_str = query_str.to_ascii_lowercase();
                let output_mode = if let Some(output_format) = sub_m.value_of("output") {
                    match OutputMode::from_str(output_format) {
                        Ok(output_mode) => output_mode,
                        Err(e) => {
                            eprintln!("{}", e);
                            std::process::exit(1);
                        }
                    }
                } else {
                    OutputMode::Table
                };

                let result = if let Some(filename) = sub_m.value_of("file_to_select") {
                    let path = Path::new(filename);
                    let data_source = common::types::DataSource::File(path.to_path_buf());
                    run(&*lower_case_query_str, data_source, false, output_mode)
                } else {
                    let data_source = common::types::DataSource::Stdin;
                    run(&*lower_case_query_str, data_source, false, output_mode)
                };

                if let Err(e) = result {
                    println!("{}", e);
                }
            } else {
                sub_m.usage();
            }
        }
        ("explain", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let lower_case_query_str = query_str.to_ascii_lowercase();
                let data_source = common::types::DataSource::Stdin;
                let result = run(&*lower_case_query_str, data_source, true, OutputMode::Table);

                if let Err(e) = result {
                    println!("{}", e);
                }
            } else {
                sub_m.usage();
            }
        }
        ("schema", Some(_)) => {
            let schema = execution::datasource::ClassicLoadBalancerLogField::schema();
            let mut table = Table::new();
            for (field, datatype) in schema.iter() {
                table.add_row(Row::new(vec![
                    Cell::new(&*field.to_string()),
                    Cell::new(&*datatype.to_string()),
                ]));
            }
            table.printstd();
        }
        _ => {
            app_m.usage();
        }
    }
}
