use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn write_gzip(path: &std::path::Path, contents: &[u8]) {
    let file = std::fs::File::create(path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(contents).unwrap();
    encoder.finish().unwrap();
}

fn run_query(query: &str, table: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["query", query, "--table", table, "--output", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn queries_a_gzipped_alb_file_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("access.renamed");
    write_gzip(&path, include_bytes!("../data/AWSALB.log"));

    let output = run_query("select count(*) as n from it", &format!("it:alb={}", path.display()));

    assert!(output.contains(r#""_count":7"#), "unexpected output: {}", output);
}

#[test]
fn scans_a_sorted_glob_of_plain_and_gzipped_jsonl_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jsonl"), b"{\"id\":1}\n").unwrap();
    write_gzip(&dir.path().join("b.jsonl.gz"), b"{\"id\":2}\n{\"id\":3}\n");

    let output = run_query("select id from it", &format!("it:jsonl={}/*", dir.path().display()));

    let one = output.find(r#""id":1"#).unwrap();
    let two = output.find(r#""id":2"#).unwrap();
    let three = output.find(r#""id":3"#).unwrap();
    assert!(one < two && two < three, "unexpected output: {}", output);
}

#[test]
fn scans_plain_and_gzipped_alb_shards_through_the_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let contents = include_bytes!("../data/AWSALB.log");
    std::fs::write(dir.path().join("a.log"), contents).unwrap();
    write_gzip(&dir.path().join("b.log.gz"), contents);

    let output = run_query("select count(*) from it", &format!("it:alb={}/*", dir.path().display()));

    assert!(output.contains(":14"), "unexpected output: {}", output);
}

#[test]
fn empty_glob_error_names_the_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let pattern = format!("{}/*.missing", dir.path().display());
    let output = run_query("select count(*) from it", &format!("it:jsonl={pattern}"));

    assert!(output.contains(&format!("No files matched pattern: {pattern}")));
}

#[test]
fn stdin_input_remains_supported() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "select count(*) as n from it",
            "--table",
            "it:jsonl=stdin",
            "--output",
            "json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"id\":1}\n{\"id\":2}\n")
        .unwrap();
    let mut output = String::new();
    child.stdout.take().unwrap().read_to_string(&mut output).unwrap();
    assert!(child.wait().unwrap().success());
    assert!(output.contains(r#""n":2"#), "unexpected output: {}", output);
}

#[test]
fn jsonl_to_json_output_round_trips_nested_values_and_field_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested.jsonl");
    std::fs::write(
        &path,
        b"{\"first\":1,\"nested\":{\"answer\":42},\"items\":[true,null]}\n",
    )
    .unwrap();

    let output = run_query("select * from it", &format!("it:jsonl={}", path.display()));

    assert_eq!(
        output.trim(),
        r#"[{"first":1,"nested":{"answer":42},"items":[true,null]}]"#
    );
}

#[test]
fn json_output_uses_the_shortest_f32_representation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("float.jsonl");
    std::fs::write(&path, b"{\"value\":1.2}\n").unwrap();

    let output = run_query("select * from it", &format!("it:jsonl={}", path.display()));

    assert_eq!(output.trim(), r#"[{"value":1.2}]"#);
}
