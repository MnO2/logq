//! Execution conformance subset adapted from partiql/partiql-tests.
//!
//! Upstream fixtures are Ion environments and cannot be consumed directly by
//! logq's file-table interface. The JSON manifest therefore hand-ports supported
//! cases onto one JSONL environment and records the upstream area each case came
//! from. Unsupported upstream areas live in an explicit, reasoned skip list.

use serde_json::Value;
use std::path::PathBuf;
use std::process::Command;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("conformance")
        .join(name)
}

#[test]
fn supported_partiql_subset_matches_expected_results() {
    let manifest: Value = serde_json::from_slice(&std::fs::read(fixture_path("cases.json")).unwrap()).unwrap();
    let cases = manifest["cases"].as_array().unwrap();
    assert!(cases.len() >= 50, "the conformance subset unexpectedly shrank");

    let table = format!("it:jsonl={}", fixture_path("input.jsonl").display());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let source = case["source"].as_str().unwrap();
        let query = case["query"].as_str().unwrap();
        assert!(!source.is_empty(), "{name} has no upstream source attribution");

        let output = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args(["query", query, "--table", &table, "--output", "json"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name} failed for `{query}`:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let actual: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{name} returned invalid JSON ({error}): {}",
                String::from_utf8_lossy(&output.stdout)
            )
        });
        assert_eq!(actual, case["expected"], "{name} failed for `{query}`");
    }
}

#[test]
fn unsupported_partiql_areas_have_explicit_reasons() {
    let skips: Value = serde_json::from_slice(&std::fs::read(fixture_path("skips.json")).unwrap()).unwrap();
    let skipped_areas = skips["skipped_areas"].as_array().unwrap();
    assert!(!skipped_areas.is_empty());
    for skipped in skipped_areas {
        assert!(!skipped["upstream_path"].as_str().unwrap().is_empty());
        assert!(!skipped["reason"].as_str().unwrap().is_empty());
    }
}
