//! Diagnostic for the production full-scan gzip decoder/parser core.
use logq::bench_internals::{JsonGzipProbeConfig, profile_json_gzip};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(5..=6).contains(&args.len()) {
        return Err("usage: json_gzip_pipeline_probe PATH PARSER_WORKERS|auto CHUNK_BYTES SUM_FIELD|- MAX_MEMORY_BYTES [--instrument-workers]".into());
    }
    let threads = if args[1] == "auto" { 0 } else { args[1].parse()? };
    let chunk_bytes = args[2].parse()?;
    let sum_field = (args[3] != "-").then(|| args[3].clone());
    let max_memory = args[4].parse()?;
    let instrument_workers = match args.get(5).map(String::as_str) {
        None => false,
        Some("--instrument-workers") => true,
        _ => return Err("unknown probe option".into()),
    };
    let report = profile_json_gzip(
        Path::new(&args[0]),
        JsonGzipProbeConfig {
            threads,
            chunk_bytes,
            sum_field: sum_field.clone(),
            max_memory,
            instrument_workers,
        },
    )?;
    println!(
        "{}",
        serde_json::json!({
        "threads_requested": threads, "chunk_bytes": chunk_bytes, "sum_field": sum_field,
        "threads_meaning": "parser workers; one additional decoder thread (production --threads counts both)",
            "max_memory": max_memory, "instrument_workers": instrument_workers, "result": report,
            "boundary": "file open excluded; decoder, framing/copies, worker setup, strict JSON, COUNT/SUM, ordered merge and teardown included; output formatting excluded",
            "worker_timing": "busy excludes input receives and output sends; includes I/O and scheduling delays, not CPU time; wait metrics include channel overhead",
            "buffering": "one input and one partial output queue slot per parser worker; chunk capacities and aggregate state share the explicit memory budget",
        })
    );
    Ok(())
}
