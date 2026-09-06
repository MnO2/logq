//! Same-engine query preparation and execution diagnostics for small results.
use clap::Parser;
use logq::app::lifecycle_probe::profile_lifecycle;
use logq::common::types::DataSource;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, default_value = "select count(*) as n from it")]
    query: String,
    #[arg(long, default_value_t = 10)]
    runs: usize,
    #[arg(long, default_value_t = 1)]
    threads: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let sources = [("it".into(), DataSource::File(args.input, "jsonl".into(), "it".into()))]
        .into_iter()
        .collect();
    let report = profile_lifecycle(&args.query, sources, args.threads, args.runs)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
