#[macro_use]
extern crate failure;
extern crate chrono;
extern crate nom;
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate pdatastructs;

mod app;
mod common;
mod execution;
mod logical;
mod syntax;

use clap::load_yaml;
use clap::App;
use prettytable::{Cell, Row, Table};

use std::path::Path;
use std::str::FromStr;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("query", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let lower_case_query_str = query_str.to_ascii_lowercase();
                let output_mode = if let Some(output_format) = sub_m.value_of("output") {
                    match app::OutputMode::from_str(output_format) {
                        Ok(output_mode) => output_mode,
                        Err(e) => {
                            eprintln!("{}", e);
                            std::process::exit(1);
                        }
                    }
                } else {
                    app::OutputMode::Table
                };

                let result = if let Some(filename) = sub_m.value_of("file_to_select") {
                    let path = Path::new(filename);
                    let data_source = common::types::DataSource::File(path.to_path_buf());
                    app::run(&*lower_case_query_str, data_source, false, output_mode)
                } else {
                    let data_source = common::types::DataSource::Stdin;
                    app::run(&*lower_case_query_str, data_source, false, output_mode)
                };

                if let Err(e) = result {
                    println!("{}", e);
                }
            } else {
                println!("{}", sub_m.usage());
            }
        }
        ("explain", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
                let lower_case_query_str = query_str.to_ascii_lowercase();
                let data_source = common::types::DataSource::Stdin;
                let result = app::run(&*lower_case_query_str, data_source, true, app::OutputMode::Table);

                if let Err(e) = result {
                    println!("{}", e);
                }
            } else {
                println!("{}", sub_m.usage());
            }
        }
        ("schema", Some(sub_m)) => {
            if let Some(type_str) = sub_m.value_of("type") {
                if type_str == "elb" {
                    let schema = execution::datasource::ClassicLoadBalancerLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&*field.to_string()),
                            Cell::new(&*datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "alb" {
                    let schema = execution::datasource::ApplicationLoadBalancerLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&*field.to_string()),
                            Cell::new(&*datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "s3" {
                    let schema = execution::datasource::S3Field::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&*field.to_string()),
                            Cell::new(&*datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "squid" {
                    let schema = execution::datasource::SquidLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&*field.to_string()),
                            Cell::new(&*datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else {
                    eprintln!("Unknown log format");
                }
            } else {
                println!("The supported log format");
                println!("* elb");
                println!("* alb");
                println!("* squid");
                println!("* s3");
            }
        }
        _ => {
            println!("{}", app_m.usage());
        }
    }
}
