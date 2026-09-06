//! Same-engine planning/reuse diagnostic; inputs are reopened on every execution.

use super::{AppError, AppResult, parse_query_input, plan_query, render_runtime_error, write_json_record};
use crate::common::types::{DataSourceRegistry, Variables};
use crate::execution::types::Node;
use crate::functions::FunctionRegistry;
use std::sync::Arc;
use std::time::Instant;

struct Prepared {
    query: String,
    node: Box<Node>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
}

impl Prepared {
    fn new(query: &str, sources: DataSourceRegistry) -> AppResult<Self> {
        let syntax = parse_query_input(query)?;
        let registry = Arc::new(crate::functions::register_all()?);
        let logical = plan_query(query, syntax, sources, registry.clone())?;
        let (node, variables) = logical.physical(&mut crate::logical::types::PhysicalPlanCreator::new())?;
        Ok(Self {
            query: query.to_owned(),
            node,
            variables,
            registry,
        })
    }

    fn execute(&self, threads: usize, memory: Option<usize>) -> AppResult<Vec<u8>> {
        let mut stream =
            self.node
                .get_with_memory_limit(self.variables.clone(), self.registry.clone(), threads, memory)?;
        // Both modes use the same row serializer. This diagnostic isolates
        // planning reuse, not the CLI's batch sink or process startup.
        let mut output = vec![b'['];
        let mut first = true;
        while let Some(record) = stream
            .next()
            .map_err(|error| render_runtime_error(&self.query, error))?
        {
            if !first {
                output.push(b',');
            }
            write_json_record(&mut output, &record)?;
            first = false;
        }
        output.push(b']');
        Ok(output)
    }
}

#[derive(serde::Serialize)]
pub struct LifecycleSample {
    pub preparation_seconds: f64,
    pub execution_seconds: f64,
    pub total_seconds: f64,
}

#[derive(serde::Serialize)]
pub struct LifecycleReport {
    pub initial_prepare_seconds: f64,
    pub fresh_plan: Vec<LifecycleSample>,
    pub reused_plan: Vec<LifecycleSample>,
    pub answer: serde_json::Value,
    pub output_bytes: usize,
}

/// Alternate fresh and reused physical plans in one process and validate every
/// complete result. Intended for immutable files and bounded result sets: this
/// probe buffers answers and is not a production result cache or session API.
pub fn profile_lifecycle(
    query: &str,
    sources: DataSourceRegistry,
    threads: usize,
    runs: usize,
) -> AppResult<LifecycleReport> {
    if runs == 0 {
        return Err(AppError::WriteIo(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "runs must be positive",
        )));
    }
    if sources
        .values()
        .any(|source| matches!(source, crate::common::types::DataSource::Stdin(..)))
    {
        return Err(AppError::WriteIo(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "lifecycle reuse requires reopenable file inputs",
        )));
    }
    for source in sources.values() {
        let paths: &[std::path::PathBuf] = match source {
            crate::common::types::DataSource::File(path, ..) => std::slice::from_ref(path),
            crate::common::types::DataSource::Files(paths, ..) => paths,
            crate::common::types::DataSource::Stdin(..) => unreachable!("stdin rejected above"),
        };
        for path in paths {
            if !std::fs::metadata(path)?.is_file() {
                return Err(AppError::WriteIo(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "lifecycle reuse requires regular input files",
                )));
            }
        }
    }
    let start = Instant::now();
    let reused = Prepared::new(query, sources.clone())?;
    let initial_prepare_seconds = start.elapsed().as_secs_f64();
    let expected = reused.execute(threads, None)?;
    let mut report = LifecycleReport {
        initial_prepare_seconds,
        fresh_plan: Vec::with_capacity(runs),
        reused_plan: Vec::with_capacity(runs),
        answer: serde_json::from_slice(&expected)?,
        output_bytes: expected.len(),
    };
    for run in 0..runs {
        for fresh in if run % 2 == 0 { [true, false] } else { [false, true] } {
            let start = Instant::now();
            let prepared = if fresh {
                Some(Prepared::new(query, sources.clone())?)
            } else {
                None
            };
            let preparation_seconds = if fresh { start.elapsed().as_secs_f64() } else { 0.0 };
            let execution_start = Instant::now();
            let answer = prepared.as_ref().unwrap_or(&reused).execute(threads, None)?;
            let execution_seconds = execution_start.elapsed().as_secs_f64();
            let total_seconds = start.elapsed().as_secs_f64();
            if answer != expected {
                return Err(AppError::Runtime("lifecycle result changed between executions".into()));
            }
            let sample = LifecycleSample {
                preparation_seconds,
                execution_seconds,
                total_seconds,
            };
            if fresh {
                report.fresh_plan.push(sample);
            } else {
                report.reused_plan.push(sample);
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sources(path: &std::path::Path) -> DataSourceRegistry {
        [(
            "it".into(),
            crate::common::types::DataSource::File(path.into(), "jsonl".into(), "it".into()),
        )]
        .into_iter()
        .collect()
    }

    #[test]
    fn reuse_reopens_input_and_resets_aggregate_state_after_errors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.jsonl");
        std::fs::write(&path, "{\"x\":1}\n{\"x\":2}\n").unwrap();
        let sources = sources(&path);
        let plan = Prepared::new("select count(*) as n, sum(x) as s from it", sources).unwrap();
        for threads in [1, 4] {
            assert_eq!(plan.execute(threads, None).unwrap(), b"[{\"n\":2,\"s\":3}]");
            assert_eq!(plan.execute(threads, None).unwrap(), b"[{\"n\":2,\"s\":3}]");
        }
        std::fs::write(&path, "{bad}\n").unwrap();
        assert!(plan.execute(1, None).is_err());
        std::fs::write(&path, "{\"x\":7}\n").unwrap();
        assert_eq!(plan.execute(1, None).unwrap(), b"[{\"n\":1,\"s\":7}]");
        assert!(plan.execute(1, Some(1)).is_err());
        assert_eq!(plan.execute(1, None).unwrap(), b"[{\"n\":1,\"s\":7}]");
    }

    #[test]
    fn lifecycle_report_checks_all_repetitions_and_rejects_zero_runs() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.jsonl");
        std::fs::write(&path, "{\"x\":1}\n{\"x\":2}\n").unwrap();
        let sources = sources(&path);
        assert!(profile_lifecycle("select count(*) as n from it", sources.clone(), 1, 0).is_err());
        let report = profile_lifecycle("select count(*) as n from it", sources, 1, 3).unwrap();
        assert_eq!(report.answer, serde_json::json!([{ "n": 2 }]));
        assert_eq!(report.fresh_plan.len(), 3);
        assert_eq!(report.reused_plan.len(), 3);
        assert!(report.initial_prepare_seconds >= 0.0);
    }

    #[test]
    fn lifecycle_rejects_nonregular_inputs_before_opening_streams() {
        let directory = tempfile::tempdir().unwrap();
        let result = profile_lifecycle("select count(*) from it", sources(directory.path()), 1, 1);
        assert!(result.err().unwrap().to_string().contains("regular input files"));
        let sources = [(
            "it".into(),
            crate::common::types::DataSource::Stdin("jsonl".into(), "it".into()),
        )]
        .into_iter()
        .collect();
        assert!(profile_lifecycle("select count(*) from it", sources, 1, 1).is_err());
    }
}
