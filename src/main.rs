use logq::app::{self, AppError, OutputMode};
use logq::common;
use logq::execution;

use clap::{CommandFactory, Parser, Subcommand};
use prettytable::{Cell, Row, Table};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser)]
#[command(author, version, about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Select data using a query string.
    Query {
        /// Output format.
        #[arg(long)]
        output: Option<String>,
        /// Table-to-file mapping. May be provided more than once.
        #[arg(long = "table")]
        tables: Vec<String>,
        /// Number of threads for parallel scanning (0 = auto, 1 = sequential).
        #[arg(long)]
        threads: Option<usize>,
        /// Soft memory ceiling in bytes or with KiB/MiB/GiB suffixes.
        #[arg(long = "max-memory")]
        max_memory: Option<String>,
        /// TOML definition for tables using the regex format.
        #[arg(long = "format-file")]
        format_file: Option<PathBuf>,
        /// Query string.
        query: Option<String>,
    },
    /// Dump the query plan graph.
    Explain {
        /// Table-to-file mapping. May be provided more than once.
        #[arg(long = "table")]
        tables: Vec<String>,
        /// Number of threads for parallel scanning (0 = auto, 1 = sequential).
        #[arg(long)]
        threads: Option<usize>,
        /// Soft memory ceiling in bytes or with KiB/MiB/GiB suffixes.
        #[arg(long = "max-memory")]
        max_memory: Option<String>,
        /// TOML definition for tables using the regex format.
        #[arg(long = "format-file")]
        format_file: Option<PathBuf>,
        /// Query string.
        query: Option<String>,
    },
    /// Show the schema for a log file format.
    Schema {
        /// Log format.
        r#type: Option<String>,
    },
}

fn print_help(command: Option<&str>) {
    let mut cli = Cli::command();
    if let Some(command) = command {
        cli.find_subcommand_mut(command).unwrap().print_help().unwrap();
    } else {
        cli.print_help().unwrap();
    }
    println!();
}

fn print_schema_table(table: &Table) {
    let mut stdout = std::io::stdout().lock();
    if let Err(error) = table.print(&mut stdout).and_then(|_| stdout.flush()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn parse_table_specs<'a, I>(
    values: I,
    format_file: Option<&Path>,
) -> Result<common::types::DataSourceRegistry, AppError>
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
        if table_name.is_empty() || !table_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(AppError::InvalidTableSpecString);
        }
        if !["elb", "alb", "squid", "s3", "jsonl", "regex", "clf", "combined"].contains(&file_format) {
            return Err(AppError::InvalidLogFileFormat);
        }
        let file_format = if file_format == "regex" {
            let path = format_file.ok_or(AppError::RegexFormatFileRequired)?;
            format!("regex:{}", path.display())
        } else {
            file_format.to_string()
        };
        if !seen_names.insert(table_name.to_string()) {
            return Err(AppError::DuplicateTableName(table_name.to_string()));
        }

        let data_source = if path_spec == "stdin" {
            common::types::DataSource::Stdin(file_format.clone(), table_name.to_string())
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
                common::types::DataSource::File(paths.pop().unwrap(), file_format.clone(), table_name.to_string())
            } else {
                common::types::DataSource::Files(paths, file_format.clone(), table_name.to_string())
            }
        };

        data_sources.insert(table_name.to_string(), data_source);
    }

    Ok(data_sources)
}

fn main() {
    match Cli::parse().command {
        Some(Commands::Query {
            output,
            tables,
            threads,
            max_memory,
            format_file,
            query,
        }) => {
            if let Some(query_str) = query {
                let output_mode = if let Some(output_format) = output {
                    match OutputMode::from_str(&output_format) {
                        Ok(output_mode) => output_mode,
                        Err(e) => {
                            eprintln!("{}", e);
                            std::process::exit(1);
                        }
                    }
                } else {
                    OutputMode::Table
                };

                let threads = threads.unwrap_or(0);
                let max_memory = match max_memory.as_deref().map(parse_memory_size).transpose() {
                    Ok(value) => value,
                    Err(error) => {
                        eprintln!("{error}");
                        std::process::exit(1);
                    }
                };

                let result = if tables.is_empty() {
                    Err(AppError::InvalidTableSpecString)
                } else {
                    match parse_table_specs(tables.iter().map(String::as_str), format_file.as_deref()) {
                        Ok(data_sources) => {
                            app::run_with_memory_limit(&query_str, data_sources, output_mode, threads, max_memory)
                        }
                        Err(e) => Err(e),
                    }
                };

                if let Err(e) = result {
                    eprintln!("{}", e);
                    std::process::exit(1);
                }
            } else {
                print_help(Some("query"));
            }
        }
        Some(Commands::Explain {
            tables,
            threads,
            max_memory,
            format_file,
            query,
        }) => {
            if let Some(query_str) = query {
                let max_memory = match max_memory.as_deref().map(parse_memory_size).transpose() {
                    Ok(value) => value,
                    Err(error) => {
                        eprintln!("{error}");
                        std::process::exit(1);
                    }
                };
                let data_sources = if tables.is_empty() {
                    let mut ds = common::types::DataSourceRegistry::new();
                    ds.insert(
                        "it".to_string(),
                        common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    );
                    ds
                } else {
                    match parse_table_specs(tables.iter().map(String::as_str), format_file.as_deref()) {
                        Ok(ds) => ds,
                        Err(e) => {
                            eprintln!("{}", e);
                            std::process::exit(1);
                        }
                    }
                };
                let result = app::explain_with_options(&query_str, data_sources, threads.unwrap_or(0), max_memory);

                if let Err(e) = result {
                    eprintln!("{}", e);
                    std::process::exit(1);
                }
            } else {
                print_help(Some("explain"));
            }
        }
        Some(Commands::Schema { r#type }) => {
            if let Some(type_str) = r#type {
                if type_str == "elb" {
                    let schema = execution::datasource::ClassicLoadBalancerLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    print_schema_table(&table);
                } else if type_str == "alb" {
                    let schema = execution::datasource::ApplicationLoadBalancerLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    print_schema_table(&table);
                } else if type_str == "s3" {
                    let schema = execution::datasource::S3Field::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    print_schema_table(&table);
                } else if type_str == "squid" {
                    let schema = execution::datasource::SquidLogField::schema();
                    let mut table = Table::new();
                    for (field, datatype) in schema.iter() {
                        table.add_row(Row::new(vec![
                            Cell::new(&field.to_string()),
                            Cell::new(&datatype.to_string()),
                        ]));
                    }
                    print_schema_table(&table);
                } else {
                    eprintln!("Unknown log format");
                    std::process::exit(1);
                }
            } else {
                println!("The supported log format");
                println!("* elb");
                println!("* alb");
                println!("* squid");
                println!("* s3");
            }
        }
        None => print_help(None),
    }
}

fn parse_memory_size(input: &str) -> Result<usize, String> {
    let input = input.trim();
    let split = input
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(input.len());
    if split == 0 {
        return Err(format!("invalid memory size `{input}`"));
    }
    let value = input[..split]
        .parse::<usize>()
        .map_err(|_| format!("invalid memory size `{input}`"))?;
    let multiplier = match input[split..].to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        _ => return Err(format!("invalid memory size `{input}`")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("memory size `{input}` is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parses_human_readable_memory_sizes() {
        assert_eq!(parse_memory_size("512").unwrap(), 512);
        assert_eq!(parse_memory_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_memory_size("2MiB").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_memory_size("3GB").unwrap(), 3_000_000_000);
        assert!(parse_memory_size("lots").is_err());
    }

    #[test]
    fn parse_table_specs_expands_globs_in_sorted_order() {
        let dir = tempfile::tempdir().unwrap();
        let second = dir.path().join("b.log");
        let first = dir.path().join("a.log");
        fs::write(&second, "b").unwrap();
        fs::write(&first, "a").unwrap();
        let spec = format!("it:jsonl={}/*.log", dir.path().display());

        let sources = parse_table_specs(std::iter::once(spec.as_str()), None).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::Files(vec![first, second], "jsonl".to_string(), "it".to_string())
        );
    }

    #[test]
    fn parse_table_specs_accepts_sorted_comma_lists() {
        let spec = "it:jsonl=z.log,a.log";
        let sources = parse_table_specs(std::iter::once(spec), None).unwrap();
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
        let error = parse_table_specs(std::iter::once(spec.as_str()), None).unwrap_err();
        assert_eq!(error, AppError::NoFilesMatched(pattern));
    }

    #[test]
    fn parse_table_specs_preserves_stdin() {
        let sources = parse_table_specs(std::iter::once("it:jsonl=stdin"), None).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string())
        );
    }

    #[test]
    fn regex_table_specs_require_and_encode_the_format_file() {
        assert_eq!(
            parse_table_specs(std::iter::once("it:regex=access.log"), None).unwrap_err(),
            AppError::RegexFormatFileRequired
        );

        let format_path = Path::new("formats/access.toml");
        let sources = parse_table_specs(std::iter::once("it:regex=access.log"), Some(format_path)).unwrap();
        assert_eq!(
            sources["it"],
            common::types::DataSource::File(
                PathBuf::from("access.log"),
                "regex:formats/access.toml".to_string(),
                "it".to_string(),
            )
        );
    }
}
