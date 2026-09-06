//! Generated independent oracles across input encodings and execution settings.
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::process::{Command, Stdio};

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

fn run(path: &std::path::Path, sql: &str, mode: &str, threads: usize) -> Vec<Value> {
    let source = match mode {
        "stdin" => "stdin".to_owned(),
        "shards" => format!("{}/*", path.display()),
        _ => path.display().to_string(),
    };
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            sql,
            "--output",
            "ndjson",
            "--threads",
            &threads.to_string(),
            "--table",
        ])
        .arg(format!("it:jsonl={source}"))
        .stdin(if mode == "stdin" {
            Stdio::from(std::fs::File::open(path).unwrap())
        } else {
            Stdio::null()
        })
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{sql}, {mode}, {threads}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn assert_semantic_equal(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(left), Value::Number(right)) => assert_eq!(left.as_f64(), right.as_f64()),
        (Value::Array(left), Value::Array(right)) => {
            assert_eq!(left.len(), right.len());
            for (left, right) in left.iter().zip(right) {
                assert_semantic_equal(left, right);
            }
        }
        (Value::Object(left), Value::Object(right)) => {
            assert_eq!(left.len(), right.len());
            for (name, value) in right {
                assert_semantic_equal(left.get(name).expect("missing output key"), value);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn generated_queries_match_independent_oracles(
        rows in prop::collection::vec((-12_i32..13, 0_u8..4, -20_i32..21, 0_u8..5), 1..40)
    ) {
        let directory = tempfile::tempdir().unwrap();
        let plain = directory.path().join("input.jsonl");
        let compressed = directory.path().join("input.gz");
        let shards = directory.path().join("shards");
        std::fs::create_dir(&shards).unwrap();
        let mut lines = Vec::new();
        let mut projection = Vec::new();
        let mut groups = BTreeMap::<i32, usize>::new();
        let mut sum = 0_i32;
        let mut float_sum = 0.0_f32;
        for &(x, shape, f, text) in &rows {
            let payload = ["", "a", "雪", "\"\\", "\n\t"][text as usize];
            let mut row = json!({"x":x,"f":f as f32 * 0.25,"g":0.5,"payload":payload});
            match shape {
                0 => row["nested"] = json!({"v":x,"ignored":[payload, payload]}),
                1 => row["nested"] = Value::Null,
                2 => row["nested"] = json!({"v":null,"ignored":payload}),
                _ => (),
            }
            let n = if shape == 0 { json!(x) } else { Value::Null };
            projection.push(json!({"n":n,"z":x+1,"payload":payload}));
            *groups.entry(x).or_default() += 1;
            sum += x;
            float_sum += f as f32 * 0.25 + 0.5;
            lines.push(serde_json::to_string(&row).unwrap() + "\n");
        }
        let input = lines.concat();
        std::fs::write(&plain, &input).unwrap();
        // Member boundaries deliberately split a JSON row's byte stream.
        let split = input.len() / 2;
        std::fs::write(&compressed, [gzip(&input.as_bytes()[..split]), gzip(&input.as_bytes()[split..])].concat()).unwrap();
        let midpoint = lines.len() / 2;
        std::fs::write(shards.join("a.jsonl"), lines[..midpoint].concat()).unwrap();
        std::fs::write(shards.join("b.gz"), gzip(lines[midpoint..].concat().as_bytes())).unwrap();
        let distinct: Vec<_> = rows.iter().map(|row| row.0).collect::<BTreeSet<_>>().into_iter().map(|x| json!({"x":x})).collect();
        let grouped: Vec<_> = groups.into_iter().map(|(x,n)| json!({"x":x,"n":n})).collect();
        let mut selected: Vec<_> = rows.iter().map(|row| row.0).filter(|x| [-2,0,2].contains(x)).collect();
        selected.sort();
        let queries = [
            ("select nested.v as n, x + 1 as z, payload from it", projection),
            ("select count(*) as n, sum(x) as s, sum(f + g) as f from it", vec![json!({"n":rows.len(),"s":sum,"f":float_sum})]),
            ("select distinct x from it order by x asc", distinct),
            ("select x, count(*) as n from it group by x order by x asc", grouped),
            ("select x from it where x in (-2,0,2) order by x asc", selected.into_iter().map(|x| json!({"x":x})).collect()),
        ];
        for (sql, expected) in queries {
            for (path, mode, threads) in [(&plain, "stdin", 1), (&plain, "plain", 1), (&plain, "plain", 4), (&compressed, "gzip", 4), (&shards, "shards", 4)] {
                let actual = run(path, sql, mode, threads);
                assert_semantic_equal(&json!(actual), &json!(expected));
            }
        }
    }
}

#[test]
fn current_numeric_width_contract_is_explicit_across_row_and_batch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("numbers.jsonl");
    let tokens = [
        "16777217",
        "2147483647",
        "2147483648",
        "2147483649",
        "9007199254740993",
        "-9223372036854775808",
        "18446744073709551615",
    ];
    let input = tokens
        .iter()
        .map(|token| format!("{{\"x\":{token}}}\n"))
        .collect::<String>();
    std::fs::write(&path, input).unwrap();
    for (mode, threads) in [("stdin", 1), ("plain", 1), ("plain", 4)] {
        let rows = run(&path, "select x from it", mode, threads);
        assert_eq!(rows.len(), tokens.len());
        for (row, token) in rows.iter().zip(tokens) {
            let expected = token
                .parse::<i32>()
                .map(f64::from)
                .unwrap_or_else(|_| token.parse::<f64>().unwrap() as f32 as f64);
            // The public serializer uses f32's shortest decimal spelling; parse
            // it back at the declared runtime precision for wide values.
            if token.parse::<i32>().is_ok() {
                assert_eq!(row["x"].as_f64().unwrap(), expected);
            } else {
                assert_eq!(row["x"].as_f64().unwrap() as f32, expected as f32);
            }
        }
        assert_eq!(rows[2], rows[3], "documents current out-of-i32 precision loss");
    }
}
