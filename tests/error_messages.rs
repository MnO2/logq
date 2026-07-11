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
            expected: "error: could not parse query",
        },
        ErrorCase {
            name: "missing_projection",
            query: "select from it",
            expected: "error: could not parse query",
        },
        ErrorCase {
            name: "missing_from",
            query: "select * it",
            expected: "error: could not parse query",
        },
        ErrorCase {
            name: "unbalanced_parenthesis",
            query: "select (a from it",
            expected: "error: could not parse query",
        },
        ErrorCase {
            name: "dangling_where",
            query: "select * from it where",
            expected: "error: unexpected input",
        },
        ErrorCase {
            name: "incomplete_order_by",
            query: "select * from it order by",
            expected: "error: unexpected input",
        },
        ErrorCase {
            name: "limit_overflow",
            query: "select * from it limit 999999999999999999999",
            expected: "error: unexpected input",
        },
        ErrorCase {
            name: "trailing_input",
            query: "select * from it trailing",
            expected: "error: unexpected input",
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
            expected: "error: unexpected input",
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

#[test]
fn syntax_failures_show_the_query_location_and_a_likely_fix() {
    for (query, hint) in [
        ("selec * from it", "did you mean `select`?"),
        ("select * from it where", "add a boolean expression"),
        ("select * from it where a === 1", "use `=` for equality"),
        ("select (a from it", "unmatched parenthesis"),
    ] {
        let output = run_query(query);
        assert!(output.contains("--> query:1:"), "missing location in {output:#?}");
        assert!(output.contains('^'), "missing caret in {output:#?}");
        assert!(output.contains(hint), "missing hint {hint:#?} in {output:#?}");
    }
}

#[test]
fn planning_failures_point_to_the_invalid_name_and_suggest_a_fix() {
    for (query, label, hint) in [
        ("select uppre(a) from it", "unknown function", "did you mean `upper`?"),
        ("select * from itt", "unknown table", "did you mean `it`?"),
        (
            "select upper(a, b) from it",
            "invalid function arguments",
            "check the function's argument count and types",
        ),
        (
            "select a from it group by a",
            "GROUP BY has no aggregate",
            "add an aggregate function",
        ),
    ] {
        let output = run_query(query);
        assert!(output.contains("--> query:1:"), "missing location in {output:#?}");
        assert!(output.contains('^'), "missing caret in {output:#?}");
        assert!(output.contains(label), "missing label {label:#?} in {output:#?}");
        assert!(output.contains(hint), "missing hint {hint:#?} in {output:#?}");
    }
}
