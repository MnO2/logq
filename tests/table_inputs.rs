use flate2::Compression;
use flate2::write::GzEncoder;
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
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn queries_a_gzipped_alb_file_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("access.renamed");
    write_gzip(&path, include_bytes!("../data/AWSALB.log"));

    let output = run_query("select count(*) as n from it", &format!("it:alb={}", path.display()));

    assert!(output.contains(r#""n":7"#), "unexpected output: {}", output);
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
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "select count(*) from it",
            "--table",
            &format!("it:jsonl={pattern}"),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains(&format!("No files matched pattern: {pattern}")));
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

#[test]
fn ndjson_output_writes_one_object_per_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.jsonl");
    std::fs::write(
        &path,
        b"{\"id\":1,\"nested\":{\"ok\":true}}\n{\"id\":2,\"nested\":null}\n",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "select id, nested from it",
            "--table",
            &format!("it:jsonl={}", path.display()),
            "--output",
            "ndjson",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"id\":1,\"nested\":{\"ok\":true}}\n{\"id\":2,\"nested\":null}\n"
    );
}

#[test]
fn queries_a_typed_user_defined_regex_format() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("access.log");
    let format_path = dir.path().join("access.toml");
    std::fs::write(&log_path, b"10.0.0.1 GET /ok 200 123\n10.0.0.2 POST /failed 503 42\n").unwrap();
    std::fs::write(
        &format_path,
        r#"
pattern = '^(?P<remote_addr>\S+) (?P<method>\S+) (?P<path>\S+) (?P<status>\d+) (?P<bytes>\d+)$'

[types]
status = "int"
bytes = "int"
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "select path, status, bytes from it where status >= 500",
            "--table",
            &format!("it:regex={}", log_path.display()),
            "--format-file",
            format_path.to_str().unwrap(),
            "--output",
            "json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.trim(), r#"[{"path":"/failed","status":503,"bytes":42}]"#);
}

#[test]
fn queries_builtin_common_and_combined_log_formats() {
    let dir = tempfile::tempdir().unwrap();
    let common_path = dir.path().join("common.log");
    let combined_path = dir.path().join("combined.log");
    std::fs::write(
        &common_path,
        b"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache.gif HTTP/1.0\" 200 2326\n",
    )
    .unwrap();
    std::fs::write(
        &combined_path,
        b"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache.gif HTTP/1.0\" 503 2326 \"https://example.com/\" \"Mozilla/5.0\"\n",
    )
    .unwrap();

    let common = run_query(
        "select path, status, body_bytes_sent from it",
        &format!("it:clf={}", common_path.display()),
    );
    assert_eq!(
        common.trim(),
        r#"[{"path":"/apache.gif","status":200,"body_bytes_sent":2326}]"#
    );

    let combined = run_query(
        "select status, referer, user_agent from it",
        &format!("it:combined={}", combined_path.display()),
    );
    assert_eq!(
        combined.trim(),
        r#"[{"status":503,"referer":"https://example.com/","user_agent":"Mozilla/5.0"}]"#
    );
}
