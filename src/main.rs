#[macro_use]
extern crate failure;

mod classic_load_balancer_log_field;
mod common;
mod execution;
mod logical;
mod syntax;

use clap::load_yaml;
use clap::App;
use std::result;

pub(crate) type AppResult<T> = result::Result<T, AppError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum AppError {
    #[fail(display = "Parse Error")]
    Parse,
}

impl From<syntax::parser::ParseError> for AppError {
    fn from(_: syntax::parser::ParseError) -> AppError {
        AppError::Parse
    }
}

fn run(query_str: &str) -> AppResult<()> {
    let stmt = syntax::parser::parse(&query_str)?;
    Ok(())
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("select", Some(sub_m)) => {
            if let (Some(query_str), Some(filename)) = (sub_m.value_of("query"), sub_m.value_of("file_to_select")) {
                run(query_str);
            } else {
                sub_m.usage();
            }
        }
        _ => {
            app_m.usage();
        }
    }
}
