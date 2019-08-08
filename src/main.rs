#[macro_use]
extern crate failure;
extern crate nom;

mod common;
mod execution;
mod logical;
mod syntax;

use clap::load_yaml;
use clap::App;
use nom::error::VerboseError;
use std::path::Path;
use std::result;

pub(crate) type AppResult<T> = result::Result<T, AppError>;

#[derive(Fail, Debug)]
pub enum AppError {
    #[fail(display = "Syntax Error: {}", _0)]
    Syntax(String),
    #[fail(display = "Input is Not All Consumed")]
    InputNotAllConsumed,
    #[fail(display = "{}", _0)]
    Parse(#[cause] logical::parser::ParseError),
    #[fail(display = "Physical Plan Error")]
    PhysicalPlan,
    #[fail(display = "Create Stream Error")]
    CreateStream,
    #[fail(display = "Stream Error")]
    Stream,
    #[fail(display = "No Log Format Specified")]
    NoLogFormatSpecified,
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
    fn from(_: logical::types::PhysicalPlanError) -> AppError {
        AppError::PhysicalPlan
    }
}

impl From<execution::types::CreateStreamError> for AppError {
    fn from(_: execution::types::CreateStreamError) -> AppError {
        AppError::CreateStream
    }
}

impl From<execution::types::StreamError> for AppError {
    fn from(_: execution::types::StreamError) -> AppError {
        AppError::Stream
    }
}

fn run(query_str: &str, data_source: common::types::DataSource) -> AppResult<()> {
    let (rest_of_str, select_stmt) = syntax::parser::select_query(&query_str)?;
    dbg!(&rest_of_str);
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed);
    }
    dbg!(&select_stmt);
    let node = logical::parser::parse_query(select_stmt, data_source.clone())?;
    dbg!(&node);
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;
    dbg!(&physical_plan);
    let mut stream = physical_plan.get(variables)?;

    while let Some(record) = stream.next()? {
        print!("{:?}", record);
    }

    Ok(())
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("query", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let result = if let (Some(format), Some(filename)) =
                    (sub_m.value_of("format"), sub_m.value_of("file_to_select"))
                {
                    if format == "elb" {
                        let path = Path::new(filename);
                        let data_source = common::types::DataSource::ClassicLoadBalancerLogFile(path.to_path_buf());
                        run(query_str, data_source)
                    } else {
                        Err(AppError::NoLogFormatSpecified)
                    }
                } else {
                    let data_source = common::types::DataSource::Stdin;
                    run(query_str, data_source)
                };

                if let Err(e) = result {
                    println!("{}", e);
                }
            } else {
                sub_m.usage();
            }
        }
        ("schema", Some(_)) => {}
        _ => {
            app_m.usage();
        }
    }
}
