use std::process::{Command, Output, Stdio};

fn gzip(input: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(input).unwrap();
    encoder.finish().unwrap()
}

fn query(input: &[u8], sql: &str, threads: usize, row: bool) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.jsonl");
    std::fs::write(&path, input).unwrap();
    let source = if row {
        "stdin".into()
    } else {
        path.display().to_string()
    };
    Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "--output",
            "ndjson",
            "--threads",
            &threads.to_string(),
            "--table",
        ])
        .arg(format!("it:jsonl={source}"))
        .arg(sql)
        .stdin(if row {
            Stdio::from(std::fs::File::open(path).unwrap())
        } else {
            Stdio::null()
        })
        .output()
        .unwrap()
}

#[test]
fn simple_json_prefix_limits_do_not_parse_or_evaluate_unrequested_rows() {
    for (input, sql) in [
        (b"{\"x\":1}\n{bad}\n".as_slice(), "select x from it limit 1"),
        (b"{\"x\":0}\n{\"x\":1}\n{bad}\n", "select x from it where x > 0 limit 1"),
        (
            b"{\"x\":1}\n{\"x\":1}\n{\"x\":2}\n{bad}\n",
            "select distinct x from it limit 2",
        ),
        (
            b"{\"x\":1}\n{\"x\":\"bad\"}\n",
            "select x from it where (cast(x as int)) > 0 limit 1",
        ),
        (b"{\"x\":1}\n\xff\n", "select x from it limit 1"),
    ] {
        let expected = query(input, sql, 1, true);
        assert!(
            expected.status.success(),
            "{sql}: {}",
            String::from_utf8_lossy(&expected.stderr)
        );
        for threads in [1, 4] {
            let actual = query(input, sql, threads, false);
            assert!(
                actual.status.success(),
                "{sql}, threads={threads}: {}",
                String::from_utf8_lossy(&actual.stderr)
            );
            assert_eq!(actual.stdout, expected.stdout, "{sql}, threads={threads}");
        }
    }
}

#[test]
fn blocking_json_operators_still_validate_the_full_input_below_limit() {
    for sql in [
        "select x from it order by x asc limit 1",
        "select sum(x) as n from it limit 1",
    ] {
        for threads in [1, 4] {
            let valid = query(b"{\"x\":1}\n", sql, threads, false);
            assert!(
                valid.status.success(),
                "{sql}: {}",
                String::from_utf8_lossy(&valid.stderr)
            );
            let result = query(b"{\"x\":1}\n{bad}\n", sql, threads, false);
            assert!(!result.status.success(), "{sql}, threads={threads}");
            assert!(String::from_utf8_lossy(&result.stderr).contains("Reader Error"));
        }
    }
}

#[test]
fn concatenated_gzip_members_are_read_by_row_batch_and_parallel_pipelines() {
    // Gzip members are byte-stream segments, so a JSON row may cross members.
    let input = [
        gzip(b"{\"x\":1}\n{\"x\":"),
        gzip(b"2}\n"),
        gzip(b""),
        gzip(b"{\"x\":3}"),
    ]
    .concat();
    for (sql, expected) in [
        ("select * from it", "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n"),
        ("select x from it", "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n"),
        ("select count(*) as n, sum(x) as s from it", "{\"n\":3,\"s\":6}\n"),
    ] {
        for threads in [1, 4] {
            let actual = query(&input, sql, threads, false);
            assert!(
                actual.status.success(),
                "{sql}, threads={threads}: {}",
                String::from_utf8_lossy(&actual.stderr)
            );
            assert_eq!(actual.stdout, expected.as_bytes(), "{sql}, threads={threads}");
        }
    }
}

#[test]
fn concatenated_gzip_validates_later_members() {
    let valid = gzip(b"{\"x\":2}\n");
    let mut corrupt = valid.clone();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    for tail in [corrupt, valid[..valid.len() - 3].to_vec(), gzip(b"{bad}\n")] {
        let input = [gzip(b"{\"x\":1}\n"), tail].concat();
        for sql in ["select * from it", "select x from it", "select count(*) as n from it"] {
            for threads in [1, 4] {
                let actual = query(&input, sql, threads, false);
                assert!(
                    !actual.status.success(),
                    "{sql}, threads={threads}: ignored bad later gzip member"
                );
            }
        }
    }
}
