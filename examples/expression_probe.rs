//! Diagnostic fixed-built-in expression comparison; never selects a planner path.
use clap::Parser;
use logq::bench_internals::{ExpressionProbeConfig, profile_expressions};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 500_000)]
    rows: usize,
    #[arg(long, default_value_t = 16)]
    chain_length: usize,
    #[arg(long)]
    nullable: bool,
    #[arg(long, default_value_t = 100)]
    active_percent: u8,
    #[arg(long)]
    reverse: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = profile_expressions(ExpressionProbeConfig {
        rows: args.rows,
        chain_length: args.chain_length,
        nullable: args.nullable,
        active_percent: args.active_percent,
        reverse: args.reverse,
    })?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
