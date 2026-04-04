# Plan: Complete PartiQL Implementation for logq

**Goal**: Implement full PartiQL spec compliance (with pragmatic exclusions) in logq, a Rust CLI tool for querying server logs.
**Architecture**: 4-layer pipeline: nom parser → logical planner → physical plan → stream execution. Bottom-up phased approach: fix foundations first, then expressions, clauses, set operations.
**Tech Stack**: Rust, nom 7.0, chrono, ordered-float, linked-hash-map, regex, anyhow/thiserror
**Design Doc**: `docs/plans/2026-04-04-partiql-completion-design-final.md`

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 0 | Steps 1-7 (Phase 0 cleanup) | Partially — 1 must be first; 2-7 independent | Foundation fixes |
| 1 | Steps 8-14 (Phase 1 foundation) | No — sequential, each builds on prior | Core type system |
| 2 | Steps 15-22 (Phase 2 expressions) | Partially — 15-17 sequential; 18-22 independent | New operators/functions |
| 3 | Steps 23-28 (Phase 3 clauses) | No — 23-24 independent; 25-28 sequential | Major structural changes |
| 4 | Steps 29-30 (Phase 4 set ops) | No — sequential | Final features |

---

## PHASE 0: Code Cleanup

### Step 1: Fix case-sensitivity (atomic)

**Files**: `src/syntax/parser.rs`, `src/syntax/ast.rs`, `src/main.rs`

#### 1a. Write failing test
```rust
// In src/syntax/parser.rs, add to mod test:
#[test]
fn test_mixed_case_query() {
    // This should parse successfully with preserved string case
    let result = select_query("SELECT a FROM it WHERE a = 1");
    assert!(result.is_ok());
}

#[test]
fn test_string_literal_case_preserved() {
    let result = select_query("select a from it where a = \"Alice\"");
    match result {
        Ok((_, stmt)) => {
            if let Some(where_expr) = stmt.where_expr_opt {
                match where_expr.expr {
                    ast::Expression::BinaryOperator(_, _, right) => match *right {
                        ast::Expression::Value(ast::Value::StringLiteral(s)) => {
                            assert_eq!(s, "Alice"); // NOT "alice"
                        }
                        _ => panic!("Expected string literal"),
                    },
                    _ => panic!("Expected binary operator"),
                }
            }
        }
        Err(e) => panic!("Parse failed: {:?}", e),
    }
}

#[test]
fn test_keyword_as_identifier_rejected() {
    // "SELECT" should be rejected as an identifier regardless of case
    assert!(identifier::<VerboseError<&str>>("SELECT").is_err());
    assert!(identifier::<VerboseError<&str>>("From").is_err());
    assert!(identifier::<VerboseError<&str>>("WHERE").is_err());
}
```

#### 1b. Run test to verify it fails
```bash
cargo test --lib syntax::parser::test::test_mixed_case_query
cargo test --lib syntax::parser::test::test_string_literal_case_preserved
cargo test --lib syntax::parser::test::test_keyword_as_identifier_rejected
```

#### 1c. Write implementation

**parser.rs**: Add `use nom::bytes::complete::tag_no_case;` to imports. Replace every `tag("select")` with `tag_no_case("select")`, `tag("from")` with `tag_no_case("from")`, etc. for all 25+ keyword tags. Also replace `tag("and")` and `tag("or")` in `parse_expression_op`. Update the `boolean` function to use `tag_no_case`.

**parser.rs identifier function**: Change line 117 from `KEYWORDS.contains(&o)` to `KEYWORDS.contains(&o.to_ascii_lowercase().as_str())`.

**parser.rs KEYWORDS list**: Expand to include all keywords:
```rust
lazy_static! {
    static ref KEYWORDS: Vec<&'static str> = {
        vec![
            "select", "value", "from", "where", "group", "by", "limit", "order",
            "true", "false", "case", "when", "then", "else", "end",
            "as", "at", "not", "and", "or", "asc", "desc", "having",
            "within", "distinct",
        ]
    };
}
```

**ast.rs BinaryOperator::from_str**: Make case-insensitive:
```rust
fn from_str(s: &str) -> result::Result<Self, Self::Err> {
    match s.to_ascii_lowercase().as_str() {
        "+" => Ok(BinaryOperator::Plus),
        // ... rest unchanged
        "and" => Ok(BinaryOperator::And),
        "or" => Ok(BinaryOperator::Or),
        _ => Err("unknown binary operator".to_string()),
    }
}
```

**ast.rs Ordering::from_str**: Make case-insensitive:
```rust
fn from_str(s: &str) -> result::Result<Self, Self::Err> {
    match s.to_ascii_lowercase().as_str() {
        "asc" => Ok(Ordering::Asc),
        "desc" => Ok(Ordering::Desc),
        _ => Err("unknown ordering".to_string()),
    }
}
```

**main.rs**: Remove `query_str.to_ascii_lowercase()` at line 36. Keep the original query string.

#### 1d. Run test to verify it passes
```bash
cargo test
```
All existing tests plus new ones must pass.

#### 1e. Commit
```bash
git add src/syntax/parser.rs src/syntax/ast.rs src/main.rs
git commit -m "Fix case-sensitivity: use tag_no_case, preserve string literal case"
```

---

### Step 2: Migrate failure to anyhow/thiserror

**Files**: `Cargo.toml`, `src/main.rs`, `src/common/types.rs`, `src/logical/parser.rs`, `src/logical/types.rs`, `src/execution/types.rs`, `src/app.rs`

#### 2a. No new test needed — mechanical migration, existing tests verify

#### 2b. Write implementation

**Cargo.toml**: Add `thiserror = "1.0"`. Remove `failure = "0.1"`.

**All files**: Replace `#[derive(Fail)]` with `#[derive(thiserror::Error)]`. Replace `#[fail(display = "...")]` with `#[error("...")]`. Replace `#[fail(display = "{}", _0)]` with `#[error(transparent)]` or `#[error("{0}")]`. Replace `#[cause]` with `#[from]` where appropriate. Remove `#[macro_use] extern crate failure;` from `main.rs`.

12 error types across 5 files. Do as single commit.

#### 2c. Run tests
```bash
cargo test
```

#### 2d. Commit
```bash
git add -A && git commit -m "Migrate from deprecated failure crate to anyhow/thiserror"
```

---

### Step 3: Deduplicate get_value_by_path_expr

**Files**: `src/common/types.rs`, `src/execution/types.rs`, `src/execution/stream.rs`

#### 3a. Move function to `src/common/types.rs` as a public function
#### 3b. Remove duplicate from `execution/types.rs:166-213` and `execution/stream.rs:11-58`, import from common
#### 3c. Run tests: `cargo test`
#### 3d. Commit: `git commit -m "Deduplicate get_value_by_path_expr into common module"`

---

### Step 4: Fix PartialEq stub for ApproxCountDistinctAggregate

**File**: `src/execution/types.rs:1366-1371`

#### 4a. Implement proper comparison (compare the counts HashMap keys, ignoring HyperLogLog internals since it's not comparable)
#### 4b. Run tests: `cargo test`
#### 4c. Commit: `git commit -m "Fix PartialEq stub for ApproxCountDistinctAggregate"`

---

### Step 5: Fix version mismatch

**File**: `src/cli.yml`

#### 5a. Change version from "0.1.18" to "0.1.19" in cli.yml
#### 5b. Commit: `git commit -m "Sync cli.yml version to 0.1.19"`

---

### Step 6: Fix is_match_group_by_fields nondeterministic bug

**File**: `src/logical/parser.rs:772-784`

#### 6a. Write test that exercises set equality with >2 elements
#### 6b. Replace positional iteration with `a == b` (HashSet implements proper equality)
#### 6c. Run tests: `cargo test`
#### 6d. Commit: `git commit -m "Fix nondeterministic group-by field matching"`

---

### Step 7: Fix LimitStream early termination bug

**File**: `src/execution/stream.rs:237-251`

#### 7a. Write test that verifies LimitStream stops pulling after limit
#### 7b. Add `if self.curr >= self.row_count { return Ok(None); }` at top of `next()`
#### 7c. Run tests: `cargo test`
#### 7d. Commit: `git commit -m "Fix LimitStream to stop consuming source after limit reached"`

---

## PHASE 1: Foundation (Data Model & Type System)

### Step 8: NULL/MISSING propagation in arithmetic operations

**File**: `src/execution/types.rs`

#### 8a. Write failing tests
```rust
#[test]
fn test_arithmetic_null_propagation() {
    let v = evaluate("Plus", &vec![Value::Null, Value::Int(1)]);
    assert_eq!(v, Ok(Value::Null));
    
    let v = evaluate("Plus", &vec![Value::Int(1), Value::Null]);
    assert_eq!(v, Ok(Value::Null));
    
    let v = evaluate("Plus", &vec![Value::Missing, Value::Int(1)]);
    assert_eq!(v, Ok(Value::Missing));
}

#[test]
fn test_float_arithmetic() {
    let v = evaluate("Plus", &vec![Value::Float(OrderedFloat::from(1.5)), Value::Float(OrderedFloat::from(2.5))]);
    assert_eq!(v, Ok(Value::Float(OrderedFloat::from(4.0))));
    
    // Mixed Int/Float promotion
    let v = evaluate("Plus", &vec![Value::Int(1), Value::Float(OrderedFloat::from(2.5))]);
    assert_eq!(v, Ok(Value::Float(OrderedFloat::from(3.5))));
}
```

#### 8b. Run test to verify it fails
```bash
cargo test --lib execution::types::tests::test_arithmetic_null_propagation
cargo test --lib execution::types::tests::test_float_arithmetic
```

#### 8c. Implement
Update `Plus`, `Minus`, `Times`, `Divide` in `evaluate()` to:
1. Check for Null/Missing first — propagate them
2. Handle Float-Float, Int-Float (promote), Float-Int (promote), Int-Int

#### 8d. Run tests: `cargo test`
#### 8e. Commit: `git commit -m "Add float arithmetic and NULL/MISSING propagation for binary ops"`

---

### Step 9: NULL/MISSING propagation in comparisons + type coercion

**File**: `src/execution/types.rs` — `Relation::apply`

#### 9a. Write failing tests
```rust
#[test]
fn test_comparison_null_propagation() {
    let vars = Variables::default();
    let rel = Relation::Equal;
    let left = Expression::Constant(Value::Null);
    let right = Expression::Constant(Value::Int(1));
    // Comparing with NULL should not error — but we need to change return type
    // For now, test that it doesn't panic/error
}

#[test]
fn test_comparison_int_float_coercion() {
    let vars = Variables::default();
    let rel = Relation::MoreThan;
    let left = Expression::Constant(Value::Float(OrderedFloat::from(2.5)));
    let right = Expression::Constant(Value::Int(1));
    let result = rel.apply(&vars, &left, &right);
    assert_eq!(result, Ok(true));
}
```

#### 9b. Implement
Update `Relation::apply` to handle:
1. Int vs Float (promote Int to Float)
2. NULL on either side → return appropriate result (for Equal: false if other is non-null, for ordered: treat as incomparable)

#### 9c. Run tests: `cargo test`
#### 9d. Commit: `git commit -m "Add type coercion for Int/Float comparisons and NULL handling"`

---

### Step 10: Three-valued logic for Formula::evaluate

**File**: `src/execution/types.rs` — `Formula` and callers

#### 10a. Write failing tests for three-valued AND/OR
```rust
#[test]
fn test_three_valued_and() {
    // TRUE AND NULL = NULL (unknown), FALSE AND NULL = FALSE
    // Need Formula::evaluate to return Option<bool>
}
```

#### 10b. Change `Formula::evaluate` return type from `EvaluateResult<bool>` to `EvaluateResult<Option<bool>>` where `None` = NULL/MISSING
#### 10c. Update all callers: FilterStream treats `None` as false (skip row), Expression::Branch treats `None` as false
#### 10d. Implement three-valued AND/OR/NOT per spec
#### 10e. Run tests: `cargo test`
#### 10f. Commit: `git commit -m "Implement three-valued logic for NULL/MISSING in boolean expressions"`

---

### Step 11: IS [NOT] NULL / IS [NOT] MISSING + Null AST literal

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 11a. Write failing tests
```rust
// Parser test
#[test]
fn test_is_null_parsing() {
    let result = expression("a is null");
    assert!(result.is_ok());
}

#[test]
fn test_null_literal_parsing() {
    let result = value("null");
    assert!(result.is_ok());
}
```

#### 11b. Implement
- Add `Null` to `ast::Value` enum
- Add `null` to parser's `value` function: `map(tag_no_case("null"), |_| ast::Value::Null)`
- Add `IsNull`, `IsNotNull`, `IsMissing`, `IsNotMissing` as new `UnaryOperator` variants (or as postfix special-case in parser)
- Parse `x IS NULL` as postfix after primary expression in `parse_expression_atom`
- Route through logical parser and evaluator

#### 11c. Run tests: `cargo test`
#### 11d. Commit: `git commit -m "Add IS [NOT] NULL/MISSING operators and NULL literal"`

---

### Step 12: Fix mixed-type ordering

**File**: `src/execution/types.rs` — `Node::OrderBy` sort closure

#### 12a. Write failing test
```rust
// In app.rs or execution tests
#[test]
fn test_order_by_with_nulls() {
    // NULL values should sort last in ASC, first in DESC
}
```

#### 12b. Replace `unreachable!()` at line 809 with NULL/MISSING handling
- `(Value::Null, _)` or `(Value::Missing, _)` → sort last for Asc, first for Desc
- `(_, Value::Null)` or `(_, Value::Missing)` → inverse

#### 12c. Run tests: `cargo test`
#### 12d. Commit: `git commit -m "Fix ORDER BY to handle NULL/MISSING values per spec"`

---

### Step 13: Multi-branch CASE WHEN

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 13a. Write failing test
```rust
#[test]
fn test_multi_branch_case_when() {
    let result = case_when_expression("case when true then 1 when false then 2 else 3 end");
    assert!(result.is_ok());
}
```

#### 13b. Implement
- Change `CaseWhenExpression` to `{ branches: Vec<(Expression, Expression)>, else_expr: Option<Box<Expression>> }`
- Update parser `case_when_expression` to parse multiple WHEN clauses
- Update logical planner `parse_case_when_expression` 
- Update `Expression::Branch` to hold `Vec<(Box<Formula>, Box<Expression>)>` + else

#### 13c. Run tests: `cargo test`
#### 13d. Commit: `git commit -m "Support multiple WHEN branches in CASE expressions"`

---

### Step 14: Refactor parse_logic for new expression types

**File**: `src/logical/parser.rs:58-85`

#### 14a. Write failing test
```rust
#[test]
fn test_parse_logic_with_func_call() {
    // WHERE upper(a) = 'FOO' — func_call in WHERE should not panic
    let expr = ast::Expression::BinaryOperator(
        ast::BinaryOperator::Equal,
        Box::new(ast::Expression::FuncCall("upper".to_string(), vec![...], None)),
        Box::new(ast::Expression::Value(ast::Value::StringLiteral("FOO".to_string()))),
    );
    let ctx = ParsingContext { table_name: "a".to_string() };
    let result = parse_logic(&ctx, &expr);
    assert!(result.is_ok()); // Should not unreachable!()
}
```

#### 14b. Replace `_ => unreachable!()` at line 83 with:
```rust
ast::Expression::FuncCall(_, _, _) => {
    parse_condition(ctx, expr)
}
ast::Expression::CaseWhenExpression(_) => {
    let case_expr = parse_case_when_expression(ctx, expr)?;
    // Wrap in a formula that evaluates the expression and checks truthiness
    Ok(Box::new(types::Formula::ExpressionPredicate(case_expr)))
}
_ => Err(ParseError::TypeMismatch),
```
Also update `parse_condition` to handle FuncCall expressions by wrapping in a comparison.

#### 14c. Run tests: `cargo test`
#### 14d. Commit: `git commit -m "Refactor parse_logic to handle FuncCall and CaseWhen in WHERE clauses"`

**--- MILESTONE GATE: "Foundation" — pause for user review ---**

---

## PHASE 2: Expressions

### Step 15: Post-parse AST desugaring pass

**File**: New function in `src/syntax/parser.rs` or new file `src/syntax/desugar.rs`

#### 15a. Create `fn desugar(stmt: SelectStatement) -> SelectStatement` that recursively walks the AST
#### 15b. For now it's identity — will be populated by BETWEEN (step 17) and COALESCE/NULLIF (step 21)
#### 15c. Wire into `app.rs` between parsing and logical planning
#### 15d. Run tests: `cargo test`
#### 15e. Commit: `git commit -m "Add post-parse AST desugaring infrastructure"`

---

### Step 16: LIKE operator

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 16a. Write failing tests for parsing and evaluation
```rust
#[test]
fn test_like_parsing() {
    let result = expression("a like \"%foo%\"");
    assert!(result.is_ok());
}

#[test]
fn test_like_evaluation() {
    let v = evaluate("Like", &vec![Value::String("hello foo bar".to_string()), Value::String("%foo%".to_string())]);
    assert_eq!(v, Ok(Value::Boolean(true)));
}
```

#### 16b. Implement
- Add `Like` and `NotLike` to `BinaryOperator` enum
- Parse `LIKE` as postfix after primary expression (similar to BETWEEN strategy)
- Evaluate by converting `%` → `.*`, `_` → `.` and using regex
- NULL propagation: `NULL LIKE pattern` → NULL

#### 16c. Run tests: `cargo test`
#### 16d. Commit: `git commit -m "Implement LIKE operator with % and _ wildcards"`

---

### Step 17: BETWEEN operator (with desugaring)

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/syntax/desugar.rs`

#### 17a. Write failing tests
```rust
#[test]
fn test_between_parsing() {
    let result = expression("a between 1 and 10");
    assert!(result.is_ok());
}
```

#### 17b. Implement
- Add `Expression::Between(Box<Expression>, Box<Expression>, Box<Expression>)` AST node
- Parse as postfix: after primary expression, check for `BETWEEN` keyword, parse two expressions separated by `AND` (consuming AND as part of BETWEEN, not as logical op)
- In `desugar()`: rewrite `Between(x, lo, hi)` → `And(Gte(x, lo), Lte(x, hi))`

#### 17c. Run tests: `cargo test`
#### 17d. Commit: `git commit -m "Implement BETWEEN operator with post-parse desugaring"`

---

### Step 18: IN operator

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 18a. Write failing tests for `x IN (1, 2, 3)` parsing and evaluation
#### 18b. Add `In` to BinaryOperator or as dedicated AST node
#### 18c. Parse `IN (expr, expr, ...)` as postfix after primary expression
#### 18d. Evaluate by iterating list with equality checks, NULL-aware
#### 18e. Run tests: `cargo test`
#### 18f. Commit: `git commit -m "Implement IN operator for membership testing"`

---

### Step 19: CAST operator

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/execution/types.rs`

#### 19a. Write tests for `CAST(x AS INT)`, `CAST(x AS VARCHAR)`, etc.
#### 19b. Add `Expression::Cast(Box<Expression>, CastType)` with `CastType` enum
#### 19c. Parse `CAST(expr AS type)` in parser
#### 19d. Evaluate type conversions: Int↔Float, String→Int/Float, Int/Float→String
#### 19e. Run tests: `cargo test`
#### 19f. Commit: `git commit -m "Implement CAST operator for type conversions"`

---

### Step 20: String concatenation (||)

**Files**: `src/syntax/ast.rs` (2 places), `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 20a. Write tests for `'foo' || 'bar'` parsing and evaluation
#### 20b. Add `BinaryOperator::Concat`, add `"||"` to `from_str`, add `tag("||")` to `parse_expression_op` with precedence 6, route through logical parser, implement in `evaluate` for String with NULL propagation
#### 20c. Run tests: `cargo test`
#### 20d. Commit: `git commit -m "Implement string concatenation operator (||)"`

---

### Step 21: COALESCE and NULLIF (desugaring)

**Files**: `src/syntax/desugar.rs`

#### 21a. Write tests for `COALESCE(a, b)` and `NULLIF(a, b)` end-to-end
#### 21b. In `desugar()`: detect `FuncCall("coalesce", args)` and `FuncCall("nullif", args)`, rewrite to CASE WHEN expressions
#### 21c. Run tests: `cargo test`
#### 21d. Commit: `git commit -m "Implement COALESCE and NULLIF via CASE WHEN desugaring"`

---

### Step 22: String functions + date_part extension

**File**: `src/execution/types.rs`

#### 22a. Write tests for UPPER, LOWER, CHAR_LENGTH, SUBSTRING, TRIM
#### 22b. Add to `evaluate()` function alongside existing url_/host_ functions
#### 22c. Extend `date_part` to handle Hour, Day, Month, Year (parser already supports them)
#### 22d. Run tests: `cargo test`
#### 22e. Commit: `git commit -m "Add string functions and extend date_part to all units"`

**--- MILESTONE GATE: "Expressions" — pause for user review ---**

---

## PHASE 3: Clauses & Query Structure

### Step 23: SELECT VALUE

**Files**: `src/syntax/ast.rs`, `src/logical/parser.rs`, `src/execution/stream.rs`

#### 23a. Write end-to-end test with SELECT VALUE
#### 23b. Remove `unimplemented!()` at `logical/parser.rs:633` and `ast.rs:54`
#### 23c. Create a ValueMapStream that outputs raw values instead of records
#### 23d. Run tests: `cargo test`
#### 23e. Commit: `git commit -m "Implement SELECT VALUE clause"`

---

### Step 24: DISTINCT

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/execution/stream.rs`

#### 24a. Write test for `SELECT DISTINCT a FROM it`
#### 24b. Add `distinct: bool` to `SelectClause` or `SelectStatement`
#### 24c. Parse `DISTINCT` keyword after `SELECT`
#### 24d. Add `DistinctStream` wrapping output with HashSet dedup
#### 24e. Run tests: `cargo test`
#### 24f. Commit: `git commit -m "Implement SELECT DISTINCT"`

---

### Step 25: Path wildcards ([*] and .*)

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/execution/types.rs`

#### 25a. Write tests for `x[*]` and `x.*` path expressions
#### 25b. Add `Wildcard` and `WildcardAttr` variants to `PathSegment`
#### 25c. Parse `[*]` and `.*` in `path_expr`
#### 25d. In `get_value_by_path_expr`, iterate all array elements / tuple values
#### 25e. Run tests: `cargo test`
#### 25f. Commit: `git commit -m "Implement path wildcards [*] and .*"`

---

### Step 26: CROSS JOIN

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/stream.rs`

#### 26a. Write end-to-end test with CROSS JOIN on two JSONL sources
#### 26b. Add `FromClause` enum to AST with `CrossJoin` variant
#### 26c. Parse `FROM a CROSS JOIN b` and comma-separated FROM items as implicit cross join
#### 26d. Add `CrossJoinStream` with nested-loop execution
#### 26e. Run tests: `cargo test`
#### 26f. Commit: `git commit -m "Implement CROSS JOIN"`

---

### Step 27: LEFT JOIN

**Files**: Same as Step 26 + ON condition handling

#### 27a. Write test for `FROM a LEFT JOIN b ON a.id = b.id`
#### 27b. Add `LeftJoin` variant to `FromClause` with ON condition expression
#### 27c. Parse LEFT JOIN ... ON syntax
#### 27d. Add `LeftJoinStream` — nested loop tracking unmatched left rows, padding with NULLs
#### 27e. Run tests: `cargo test`
#### 27f. Commit: `git commit -m "Implement LEFT JOIN with ON condition"`

---

### Step 28: Non-correlated subqueries

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/logical/parser.rs`, `src/execution/types.rs`

#### 28a. Write tests for `WHERE x IN (SELECT y FROM ...)` and `FROM (SELECT ...) AS t`
#### 28b. Add `Expression::Subquery(Box<SelectStatement>)` to AST
#### 28c. Parse nested SELECT inside parenthesized expressions (distinguish from grouped expr)
#### 28d. Execute by materializing inner query result before outer query
#### 28e. Run tests: `cargo test`
#### 28f. Commit: `git commit -m "Implement non-correlated subqueries in FROM and WHERE"`

**--- MILESTONE GATE: "Clauses" — pause for user review ---**

---

## PHASE 4: Set Operations

### Step 29: Top-level Query AST + UNION

**Files**: `src/syntax/ast.rs`, `src/syntax/parser.rs`, `src/app.rs`, `src/logical/parser.rs`

#### 29a. Write test for `SELECT a FROM t1 UNION SELECT b FROM t2`
#### 29b. Add `Query` enum wrapping `SelectStatement` and `SetOp`
#### 29c. Update parser to recognize UNION/UNION ALL after SFW query
#### 29d. Execute both queries, concatenate, optionally dedup
#### 29e. Run tests: `cargo test`
#### 29f. Commit: `git commit -m "Implement UNION and UNION ALL"`

---

### Step 30: INTERSECT and EXCEPT

**Files**: Same as Step 29

#### 30a. Write tests for INTERSECT and EXCEPT
#### 30b. Add `Intersect` and `Except` to `SetOperator` enum
#### 30c. Implement via materializing right query into set, filtering left
#### 30d. Run tests: `cargo test`
#### 30e. Commit: `git commit -m "Implement INTERSECT and EXCEPT with ALL variants"`

**--- MILESTONE GATE: "Complete" — final review ---**

---

## End-to-End Verification

### Step 31: Comprehensive integration tests

#### 31a. Write integration tests exercising full query pipeline with JSONL data:
- Basic SELECT with mixed-case keywords
- WHERE with LIKE, IN, BETWEEN, IS NULL
- CASE WHEN with multiple branches
- GROUP BY with aggregates on Float data
- ORDER BY with NULL values
- SELECT VALUE
- SELECT DISTINCT
- CROSS JOIN and LEFT JOIN
- Subqueries in WHERE
- UNION of two queries
- String functions: UPPER, LOWER, CHAR_LENGTH
- CAST between types
- String concatenation with ||
- COALESCE and NULLIF

#### 31b. Run full test suite
```bash
cargo test
```

#### 31c. Commit: `git commit -m "Add comprehensive integration tests for PartiQL compliance"`
