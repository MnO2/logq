//! Same-scanner COUNT/SUM control for worker count and newline-aligned task policy.
use logq::bench_internals::{JsonParallelProbeConfig, profile_json_parallel};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(5..=6).contains(&args.len()) {
        return Err("usage: json_parallel_probe PATH THREADS|auto mmap|buffered range|TASK_BYTES SUM_FIELD|- [--instrument-workers]".into());
    }
    let threads = if args[1] == "auto" { 0 } else { args[1].parse()? };
    let buffered = match args[2].as_str() {
        "mmap" => false,
        "buffered" => true,
        _ => return Err("backend must be mmap or buffered".into()),
    };
    let task_bytes = if args[3] == "range" {
        None
    } else {
        Some(args[3].parse()?)
    };
    let sum_field = (args[4] != "-").then(|| args[4].clone());
    let instrument_workers = match args.get(5).map(String::as_str) {
        None => false,
        Some("--instrument-workers") => true,
        _ => return Err("unknown probe option".into()),
    };
    let report = profile_json_parallel(
        Path::new(&args[0]),
        JsonParallelProbeConfig {
            threads,
            sum_field: sum_field.clone(),
            task_bytes,
            buffered,
            instrument_workers,
        },
    )?;
    println!(
        "{}",
        serde_json::json!({
            "backend": args[2], "threads_requested": threads, "task_bytes": task_bytes,
            "sum_field": sum_field, "instrument_workers": instrument_workers,
            "result": report,
            "boundary": "file open/map excluded; scanner, worker setup, COUNT/SUM partial states, merge and teardown included; output formatting excluded",
        "worker_timing": "busy is worker lifetime minus channel sends and includes input I/O and scheduling delays, not CPU time; send_wait includes channel operation overhead as well as blocking",
        })
    );
    Ok(())
}
