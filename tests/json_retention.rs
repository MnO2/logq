use std::io::Write;
use std::process::Command;

#[test]
fn overwritten_large_json_string_does_not_consume_the_retained_batch_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"{\"x\":\"").unwrap();
    // Cross the 16 MiB parallel-scan threshold while the final projected value
    // remains NULL. The discarded duplicate must not stay in a queued batch.
    let chunk = vec![b'a'; 1024 * 1024];
    for _ in 0..17 {
        file.write_all(&chunk).unwrap();
    }
    file.write_all(b"\",\"x\":null}\n").unwrap();
    drop(file);
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "--threads",
            "2",
            "--max-memory",
            "1MiB",
            "--output",
            "ndjson",
            "--table",
        ])
        .arg(format!("it:jsonl={}", path.display()))
        .arg("select x from it")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "{\"x\":null}");
}
