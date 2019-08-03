#[macro_use]
extern crate failure;
extern crate nom;

mod classic_load_balancer_log_field;
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

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum AppError {
    #[fail(display = "Parse Error")]
    Parse,
}

impl From<nom::Err<VerboseError<&str>>> for AppError {
    fn from(_: nom::Err<VerboseError<&str>>) -> AppError {
        AppError::Parse
    }
}

impl From<logical::parser::ParseError> for AppError {
    fn from(_: logical::parser::ParseError) -> AppError {
        AppError::Parse
    }
}

fn run(query_str: &str, filename: logical::types::DataSource) -> AppResult<()> {
    let (rest_of_str, select_stmt) = syntax::parser::select_query(&query_str)?;

    if rest_of_str.is_empty() {
        return Err(AppError::Parse);
    }
    dbg!(&select_stmt);

    let node = logical::parser::parse_query(select_stmt, filename)?;
    dbg!(&node);

    Ok(())
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("query", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let result = if let Some(filename) = sub_m.value_of("file_to_select") {
                    let path = Path::new(filename);
                    let data_source = logical::types::DataSource::File(path.to_path_buf());
                    run(query_str, data_source)
                } else {
                    let data_source = logical::types::DataSource::Stdin;
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
