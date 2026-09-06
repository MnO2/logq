use serde_json::{Value, json};
use std::process::{Command, Stdio};

const INPUT: &str = "{\"const_000000000\":7,\"const_000000001\":11,\"x\":1}\n{\"const_000000000\":null,\"const_000000001\":9,\"x\":2}\n{\"x\":3}\n";

fn assert_query(sql: &str, expected: Value) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.jsonl");
    std::fs::write(&path, INPUT).unwrap();
    for row in [true, false] {
        for threads in [1, 4] {
            let source = if row {
                "stdin".into()
            } else {
                path.display().to_string()
            };
            let output = Command::new(env!("CARGO_BIN_EXE_logq"))
                .args(["query", sql, "--output", "json", "--threads", &threads.to_string()])
                .args(["--table", &format!("it:jsonl={source}")])
                .args(["--table", &format!("rhs:jsonl={}", path.display())])
                .stdin(if row {
                    Stdio::from(std::fs::File::open(&path).unwrap())
                } else {
                    Stdio::null()
                })
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{sql}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(actual, expected, "{sql}; row={row}; threads={threads}");
        }
    }
}

#[test]
fn constants_and_source_fields_use_separate_namespaces() {
    assert_query(
        "select const_000000000 + 1 as v, const_000000001 as original, 2 as literal from it",
        json!([{"v":8,"original":11,"literal":2},{"v":null,"original":9,"literal":2},{"v":null,"original":null,"literal":2}]),
    );
    assert_query(
        "select 1 as literal from it",
        json!([{"literal":1},{"literal":1},{"literal":1}]),
    );
    assert_query(
        "select * from it where x = 1",
        json!([{"const_000000000":7,"const_000000001":11,"x":1}]),
    );
    assert_query("select x from it where const_000000000 = 7", json!([{"x":1}]));
}

#[test]
fn constants_remain_distinct_in_branches_aggregates_and_joins() {
    assert_query(
        "select case when const_000000000 is null then 1 else (const_000000000 + 2) end as v from it",
        json!([{"v":9},{"v":1},{"v":null}]),
    );
    assert_query("select sum(const_000000000 + 1) as s from it", json!([{"s":8}]));
    assert_query(
        "select l.const_000000000 + 1 as v from it as l join rhs as r on l.x = r.x where r.x = 1",
        json!([{"v":8}]),
    );
    assert_query(
        "select case when x = 1 then (const_000000000 + 1) else (2147483647 + 1) end as v from it where x = 1",
        json!([{"v":8}]),
    );
}

#[test]
fn stdin_table_aliases_match_file_aliases() {
    assert_query(
        "select l.x as x, l.const_000000000 as v from it as l",
        json!([{"x":1,"v":7},{"x":2,"v":null},{"x":3,"v":null}]),
    );
}
