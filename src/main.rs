use logq::app::{self, AppError, OutputMode};
use logq::common;
use logq::execution;

use clap::load_yaml;
use clap::App;
use prettytable::{Cell, Row, Table};
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

fn parse_table_specs<'a, I>(values: I) -> Result<common::types::DataSourceRegistry, AppError>
where
    I: Iterator<Item = &'a str>,
{
    let mut data_sources = common::types::DataSourceRegistry::new();
    let mut seen_names = HashSet::new();

    for table_spec_string in values {
        let (table_and_format, path_spec) = table_spec_string
            .split_once('=')
            .ok_or(AppError::InvalidTableSpecString)?;
        let (table_name, file_format) = table_and_format
            .split_once(':')
            .ok_or(AppError::InvalidTableSpecString)?;
        if table_name.is_empty() || !table_name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(AppError::InvalidTableSpecString);
        }
        if !["elb", "alb", "squid", "s3", "jsonl"].contains(&file_format) {
            return Err(AppError::InvalidLogFileFormat);
        }
        if !seen_names.insert(table_name.to_string()) {
            return Err(AppError::DuplicateTableName(table_name.to_string()));
        }

        let data_source = if path_spec == "stdin" {
            common::types::DataSource::Stdin(file_format.to_string(), table_name.to_string())
        } else {
            let mut paths: Vec<PathBuf> = Vec::new();
            for item in path_spec.split(',') {
                if item.is_empty() || item == "stdin" {
                    return Err(AppError::InvalidTableSpecString);
                }
                if item.bytes().any(|b| matches!(b, b'*' | b'?' | b'[')) {
                    let entries = glob::glob(item).map_err(|_| AppError::InvalidGlobPattern(item.to_string()))?;
                    let mut matched = Vec::new();
                    for entry in entries {
                        matched.push(entry.map_err(|_| AppError::InvalidGlobPattern(item.to_string()))?);
                    }
                    if matched.is_empty() {
                        return Err(AppError::NoFilesMatched(item.to_string()));
                    }
                    paths.extend(matched);
                } else {
                    paths.push(PathBuf::from(item));
                }
            }
            paths.sort();
            paths.dedup();
            if paths.len() == 1 {
                common::types::DataSource::File(paths.pop().unwrap(), file_format.to_string(), table_name.to_string())
            } else {
                common::types::DataSource::Files(paths, file_format.to_string(), table_name.to_string())
            }
        };

        data_sources.insert(table_name.to_string(), data_source);
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

                let threads: usize = sub_m.value_of("threads").and_then(|s| s.parse().ok()).unwrap_or(0);

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
                    ds.insert(
                        "it".to_string(),
                        common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    );
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
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "alb" {
                    let schema = execution::datasource::ApplicationLoadBalancerLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "s3" {
                    let schema = execution::datasource::S3Field::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    table.printstd();
                } else if type_str == "squid" {
                    let schema = execution::datasource::SquidLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parse_table_specs_expands_globs_in_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let second = dir.path().join("b.log");
        let first = dir.path().join("a.log");
        fs::write(&second, "b").unwrap();
        fs::write(&first, "a").unwrap();
        let spec = format!("it:jsonl={}/*.log", dir.path().display());

        let sources = parse_table_specs(std::iter::once(spec.as_str())).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::Files(vec![first, second], "jsonl".to_string(), "it".to_string())
        );
    }

    #[test]
    fn parse_table_specs_accepts_sorted_comma_lists() {
        let spec = "it:jsonl=z.log,a.log";
        let sources = parse_table_specs(std::iter::once(spec)).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::Files(
                vec!["a.log".into(), "z.log".into()],
                "jsonl".to_string(),
                "it".to_string(),
            )
        );
    }

    #[test]
    fn parse_table_specs_names_empty_glob() {
        let dir = tempfile::tempdir().unwrap();
        let pattern = format!("{}/*.missing", dir.path().display());
        let spec = format!("it:jsonl={pattern}");
        let error = parse_table_specs(std::iter::once(spec.as_str())).unwrap_err();
        assert_eq!(error, AppError::NoFilesMatched(pattern));
    }

    #[test]
    fn parse_table_specs_preserves_stdin() {
        let sources = parse_table_specs(std::iter::once("it:jsonl=stdin")).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string())
        );
    }
}
