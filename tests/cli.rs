use std::process::Command;

fn output(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_logq")).args(args).output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn version_comes_from_the_package_manifest() {
    let stdout = output(&["--version"]);
    assert!(stdout.contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn query_help_preserves_public_options() {
    let stdout = output(&["query", "--help"]);
    assert!(stdout.contains("--output"));
    assert!(stdout.contains("--table"));
    assert!(stdout.contains("--threads"));
    assert!(stdout.contains("--format-file"));
}

#[test]
fn explain_help_preserves_table_option() {
    let stdout = output(&["explain", "--help"]);
    assert!(stdout.contains("--table"));
    assert!(stdout.contains("--format-file"));
}
