#[macro_use]
extern crate lazy_static;

use logq::app::{self, AppError, OutputMode};
use logq::common;
use logq::execution;

use clap::load_yaml;
use clap::App;
use prettytable::{Cell, Row, Table};
use regex::Regex;
use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;

lazy_static! {
    //FIXME: use different type for string hostname and Ipv4
    static ref TABLE_SPEC_REGEX: Regex = Regex::new(r#"([0-9a-zA-Z]+):([a-zA-Z]+)=([^=\s"':]+)"#).unwrap();
}

fn parse_table_specs<'a, I>(values: I) -> Result<common::types::DataSourceRegistry, AppError>
where
    I: Iterator<Item = &'a str>,
{
    let mut data_sources = common::types::DataSourceRegistry::new();
    let mut seen_names = HashSet::new();

    for table_spec_string in values {
        if let Some(cap) = TABLE_SPEC_REGEX.captures(table_spec_string) {
            let table_name = cap.get(1).map_or("", |m| m.as_str()).to_string();
            let file_format = cap.get(2).map_or("", |m| m.as_str()).to_string();
            let file_path = cap.get(3).map_or("", |m| m.as_str()).to_string();

            if !["elb", "alb", "squid", "s3", "jsonl"].contains(&&*file_format) {
                return Err(AppError::InvalidLogFileFormat);
            }

            if !seen_names.insert(table_name.clone()) {
                eprintln!("Error: duplicate table name '{}'", table_name);
                std::process::exit(1);
            }

            let data_source = if file_path == "stdin" {
                common::types::DataSource::Stdin(file_format, table_name.clone())
            } else {
                let path = Path::new(&file_path);
                common::types::DataSource::File(path.to_path_buf(), file_format, table_name.clone())
            };

            data_sources.insert(table_name, data_source);
        } else {
            return Err(AppError::InvalidTableSpecString);
        }
    }

    Ok(data_sources)
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand() {
        ("query", Some(sub_m)) => {
            if let Some(query_str) = sub_m.value_of("query") {
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

                let threads: usize = sub_m.value_of("threads")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let result = if let Some(table_specs) = sub_m.values_of("table") {
                    match parse_table_specs(table_specs) {
                        Ok(data_sources) => app::run(query_str, data_sources, output_mode, threads),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(AppError::InvalidTableSpecString)
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
                let data_sources = if let Some(table_specs) = sub_m.values_of("table") {
                    match parse_table_specs(table_specs) {
                        Ok(ds) => ds,
                        Err(e) => {
                            println!("{}", e);
                            return;
                        }
                    }
                } else {
                    let mut ds = common::types::DataSourceRegistry::new();
                    ds.insert("it".to_string(), common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string()));
                    ds
                };
                let result = app::explain(query_str, data_sources);

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
