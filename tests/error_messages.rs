use std::process::Command;

struct ErrorCase {
    name: &'static str,
    query: &'static str,
    expected: &'static str,
}

fn run_query(query: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "--output",
            "csv",
            "--table",
            "it:jsonl=data/structured.log",
            query,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "CLI unexpectedly failed for {query}");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn representative_failures_have_stable_diagnostic_categories() {
    let cases = [
        ErrorCase {
            name: "misspelled_select",
            query: "selec * from it",
            expected: "Syntax Error",
        },
        ErrorCase {
            name: "missing_projection",
            query: "select from it",
            expected: "Syntax Error",
        },
        ErrorCase {
            name: "missing_from",
            query: "select * it",
            expected: "Syntax Error",
        },
        ErrorCase {
            name: "unbalanced_parenthesis",
            query: "select (a from it",
            expected: "Syntax Error",
        },
        ErrorCase {
            name: "dangling_where",
            query: "select * from it where",
            expected: "leftover",
        },
        ErrorCase {
            name: "incomplete_order_by",
            query: "select * from it order by",
            expected: "leftover",
        },
        ErrorCase {
            name: "limit_overflow",
            query: "select * from it limit 999999999999999999999",
            expected: "leftover",
        },
        ErrorCase {
            name: "trailing_input",
            query: "select * from it trailing",
            expected: "leftover",
        },
        ErrorCase {
            name: "unknown_table",
            query: "select * from ti",
            expected: "Unknown table 'ti'",
        },
        ErrorCase {
            name: "unknown_function",
            query: "select nonexistent_func(a) from it",
            expected: "nonexistent_func",
        },
        ErrorCase {
            name: "wrong_function_arity",
            query: "select upper(a, b) from it",
            expected: "expects 1 argument(s), got 2",
        },
        ErrorCase {
            name: "group_without_aggregate",
            query: "select a from it group by a",
            expected: "no aggregate function",
        },
        ErrorCase {
            name: "group_field_mismatch",
            query: "select a, count(*) from it group by b",
            expected: "mismatch",
        },
        ErrorCase {
            name: "having_without_group",
            query: "select count(*) from it having count(*) > 1",
            expected: "Invalid Star",
        },
        ErrorCase {
            name: "star_group_by",
            query: "select * from it group by a",
            expected: "mismatch",
        },
        ErrorCase {
            name: "invalid_arithmetic_arguments",
            query: "select 1 + \"x\" from it",
            expected: "Invalid Arguments",
        },
        ErrorCase {
            name: "invalid_cast",
            query: "select cast(\"x\" as int) from it",
            expected: "Type Mismatch",
        },
        ErrorCase {
            name: "invalid_operator",
            query: "select * from it where a === 1",
            expected: "leftover",
        },
    ];

    for case in cases {
        let output = run_query(case.query);
        assert!(
            output.contains(case.expected),
            "{}: expected {:#?} in output {:#?}",
            case.name,
            case.expected,
            output
        );
    }
}
