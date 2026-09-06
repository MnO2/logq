//! Run isolated GROUP BY phases; no input parsing or concurrent worker scheduling.
use clap::Parser;
use logq::bench_internals::{GroupProbeConfig, profile_group_phases};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 500_000)]
    rows: usize,
    #[arg(long, default_value_t = 9)]
    groups: usize,
    /// Logical contiguous partitions, consumed sequentially, not CPU threads.
    #[arg(long, default_value_t = 4)]
    partitions: usize,
    #[arg(long)]
    skew: bool,
    #[arg(long)]
    nullable: bool,
    /// Optional shared operator-state limit in bytes, not a heap/RSS ceiling.
    #[arg(long)]
    memory_limit: Option<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = profile_group_phases(GroupProbeConfig {
        rows: args.rows,
        groups: args.groups,
        partitions: args.partitions,
        skew: args.skew,
        nullable: args.nullable,
        memory_limit: args.memory_limit,
    })?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
