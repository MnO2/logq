VERDICT: NEEDS_REVISION

## Summary Assessment

The design is well-structured with sensible phasing and many correctly identified cleanup items, but it contains several critical issues: a fundamental misunderstanding about `Value::Missing` already existing in the codebase, dangerous ordering dependencies within phases that will cause wasted work or regressions, and a `parse_logic` function that will panic (`unreachable!()`) on any of the new expression types (LIKE, IN, BETWEEN, IS NULL) before they are routed through the correct code path. The design also omits the substantial parser rework needed since all keywords are matched case-sensitively via `tag()` in the nom parser.

## Critical Issues (must fix)

### C1. Value::Missing already exists -- Phase 1a is based on a false premise

The design states in Phase 1a: "Currently missing fields just aren't in the record; need explicit MISSING for proper propagation." This is incorrect. `Value::Missing` already exists in `common/types.rs:29` and is already used extensively throughout the codebase:

- `execution/types.rs` `get_value_by_path_expr` returns `Value::Missing` for absent attributes (lines 168, 179, 183, 194, 202, 205, 209)
- `execution/stream.rs` has an identical `get_value_by_path_expr` also returning `Value::Missing`
- `execution/stream.rs` renders `Value::Missing` as `"<null>"` in both table and CSV output

What *is* needed is proper NULL vs. MISSING *semantic* distinction in the evaluator. Currently, comparisons with `Missing` will hit `TypeMismatch` errors (see `Relation::apply` lines 602-623 in `execution/types.rs`, which only match Int-Int and Float-Float for ordered comparisons). The design should be rewritten to focus on the *propagation semantics* rather than adding a type that already exists.

### C2. `parse_logic` will panic on new expression types (LIKE, IN, BETWEEN, IS NULL/MISSING)

The `parse_logic` function in `logical/parser.rs:58-85` is the entry point for WHERE clause processing. It has a `_ => unreachable!()` catch-all on line 83. Currently it only handles:
- `BinaryOperator` (And/Or/comparisons)
- `UnaryOperator` (Not)
- `Value` (boolean constants)

Any new expression type -- LIKE, IN, BETWEEN, IS NULL, IS MISSING, function calls in WHERE -- will hit `unreachable!()` and panic. This is not mentioned in the design. Before implementing Phase 2 features, `parse_logic` must be extended to handle `Expression::FuncCall` (which is how LIKE/IN might be represented) or new AST variants. This is a hard ordering dependency: the logical parser must be refactored to support new expression types before any Phase 2 item can work in a WHERE clause.

### C3. Parser case-sensitivity requires much more work than described

Phase 0a says "Make parser case-insensitive for keywords/identifiers while preserving original case in string literals." The design underestimates this. The nom parser uses `tag()` (case-sensitive exact match) for every keyword: `tag("select")`, `tag("from")`, `tag("where")`, `tag("group")`, `tag("by")`, `tag("order")`, `tag("limit")`, `tag("having")`, `tag("case")`, `tag("when")`, `tag("then")`, `tag("else")`, `tag("end")`, `tag("as")`, `tag("at")`, `tag("value")`, `tag("not")`, `tag("and")`, `tag("or")`, `tag("asc")`, `tag("desc")`, `tag("true")`, `tag("false")`, `tag("within")`, `tag("group")`. That is 25+ `tag()` calls. Also, `BinaryOperator::from_str` and `Ordering::from_str` in `ast.rs` match lowercase only (`"and"`, `"or"`, `"asc"`, `"desc"`).

The fix is nontrivial: either (a) replace every `tag("keyword")` with `tag_no_case("keyword")` from nom, (b) normalize only non-string-literal portions of the query before parsing, or (c) build a lexer phase. Option (b) is what the design implies with "remove lowercasing from main.rs" but the current lowercasing is the *only* reason the parser works at all with mixed-case input. Removing it without simultaneously converting all `tag()` to `tag_no_case()` will *break every query with any uppercase letter*. This dependency must be explicitly acknowledged and the two changes must be atomic.

### C4. The `is_match_group_by_fields` function has a correctness bug that will cause false positives/negatives with new features

In `logical/parser.rs:772-784`, group-by field matching iterates both `HashSet`s with `next()` and compares positionally. `HashSet` has no guaranteed iteration order, so this comparison is nondeterministic -- it can return `false` for sets that are actually equal, or `true` for sets that differ. This works today only by luck (small sets, hash collisions). As more features are added (GROUP BY with expressions, function calls as group keys), this bug will surface. The fix is trivial: use `a == b` which `HashSet` implements correctly via subset checks, or use `a.is_subset(&b) && b.is_subset(&a)`.

### C5. CASE WHEN only supports single branch, but design does not acknowledge this limitation

The AST `CaseWhenExpression` struct (ast.rs:131-135) has a single `condition`/`then_expr`/`else_expr`. Real CASE WHEN supports multiple WHEN branches: `CASE WHEN c1 THEN e1 WHEN c2 THEN e2 ELSE e3 END`. The parser (`case_when_expression`) only parses one WHEN clause. The design lists CASE WHEN as an in-scope feature but does not mention the need to extend the AST to support multiple branches. This will need changes to `CaseWhenExpression`, the parser, the logical planner's `parse_case_when_expression`, and the physical evaluator's `Expression::Branch`.

### C6. JOINs and subqueries (Phase 3d, 3e, 3f) require fundamental AST restructuring that is not scoped

The current `TableReference` is a simple struct with a `PathExpr` and optional AS/AT clauses. There is no representation for:
- JOIN types (CROSS, LEFT, INNER)
- ON conditions
- Subqueries as FROM sources

`SelectStatement.table_references` is `Vec<TableReference>`, which implicitly means comma-separated FROM items (implicit cross join). The design says "New physical stream nodes" for JOINs but does not mention the AST changes needed, the parser work, or the logical plan changes. These are the hardest items in the entire design and deserve more detailed breakdowns.

Similarly, subqueries require parsing nested `SelectStatement` inside expressions, which means `Expression` needs a `Subquery(SelectStatement)` variant, and the execution layer needs to handle materialization. None of these structural changes are mentioned.

## Suggestions (nice to have)

### S1. BETWEEN desugaring should happen in the parser, not the logical planner

The design says "desugar to `x >= y AND x <= z` in logical planner." Desugaring at the AST level (in the parser or a post-parse rewrite) is simpler and avoids duplicating logic. The logical planner would then just see standard binary operators. Same applies to COALESCE and NULLIF -- desugaring to CASE WHEN at the parser/AST level is cleaner than doing it in the logical planner.

### S2. String concatenation (`||`) will conflict with the operator parser

The `parse_expression_op` function matches operators as single `tag()` calls: `tag("|")` doesn't exist, but `||` is two characters. The current operator precedence parser uses `tag(">=")` and `tag("<=")` as two-character ops, so `tag("||")` is feasible. However, the design should note that `||` must be added to both `parse_expression_op`, the precedence table, `BinaryOperator` enum, `BinaryOperator::from_str`, and the `evaluate` function. This is a 5-file change, not a simple addition.

### S3. `thiserror` is mentioned but not in Cargo.toml

Phase 0b says to use `thiserror` for structured error types, but `thiserror` is not currently a dependency. Since every error type currently derives `Fail`, the migration requires changing every `#[derive(Fail)]` to `#[derive(thiserror::Error)]` and every `#[fail(...)]` to `#[error(...)]`. There are 12 error types across 5 files. This is mechanical but should be done as a single commit to avoid a half-migrated state.

### S4. DISTINCT via HashSet (Phase 3b) needs careful Value hashing strategy

The design notes "Requires hashing strategy for Record" but `Value` already derives `Hash` (via `OrderedFloat` and `linked_hash_map`). However, `Record` uses `LinkedHashMap<String, Value>` which does implement `Hash` only through `linked-hash-map` crate features. Check whether the current `Hash` for `LinkedHashMap` is order-dependent or order-independent -- for DISTINCT semantics you need order-independent hashing. This may require serializing Records to a canonical form.

### S5. Set operations (Phase 4) need a top-level query AST node

Currently the only query type is `SelectStatement`. UNION/INTERSECT/EXCEPT combine two queries. The design mentions "Parser extended to recognize trailing set operation keywords after SFW queries" but does not mention that a new top-level AST type (e.g., `Query::Select(SelectStatement) | Query::SetOp(SetOp, Box<Query>, Box<Query>)`) is needed. This is a structural change to `select_query` and `app::run`.

### S6. The `failure` crate uses `#[derive(Fail)]` as an attribute macro -- migration to `thiserror` changes derived traits

With `failure`, error types derive `Fail`. With `thiserror`, they derive `Error` (from std). This means `From` impls change slightly. The manual `impl From<X> for Y` patterns throughout the codebase should be replaced with `#[from]` attribute in `thiserror`. Planning for this mechanical change would save time.

### S7. ORDER BY `unreachable!()` (Phase 1f) should handle Value::Missing

The design correctly identifies the `unreachable!()` in ORDER BY (`execution/types.rs:809`). But it should also handle the case where one value is `Null` and the other is not, or one is `Missing` and the other is not. The spec says NULL/MISSING sort last in ascending order, first in descending order. The current code does handle `(Null, Null)` returning `Equal`, but mixed pairs (e.g., `(Int, Null)`) will panic.

### S8. LimitStream has a bug -- it continues consuming from source after limit is reached

In `execution/stream.rs:237-251`, `LimitStream::next()` uses `while let Some(record) = self.source.next()?` which means it keeps pulling records from the source even after `self.curr >= self.row_count`. Once the limit is hit, it should immediately return `Ok(None)` instead of draining the entire upstream. This is a pre-existing bug but will become more visible with expensive subqueries.

## Verified Claims (things you confirmed are correct)

1. **Phase 0a (Input lowercasing)**: Confirmed. `main.rs:36` does `query_str.to_ascii_lowercase()` before parsing. This destroys case in string literals.

2. **Phase 0b (failure crate)**: Confirmed. `failure = "0.1"` is in Cargo.toml:21. `anyhow = "1.0"` is in Cargo.toml:34 but unused (no imports found). `#[derive(Fail)]` and `#[fail(display = ...)]` are used in 12+ error types across 5 files.

3. **Phase 0c (Duplicated get_value_by_path_expr)**: Confirmed. Identical functions exist in `execution/types.rs:166-213` and `execution/stream.rs:11-58`. Both are private functions with the same signature and logic.

4. **Phase 0d (PartialEq stub)**: Confirmed. `ApproxCountDistinctAggregate::PartialEq` at `execution/types.rs:1367-1369` always returns `true` with the comment "Ignoring the detail since we only use Eq for unit test."

5. **Phase 0e (Version mismatch)**: Confirmed. `cli.yml:2` says `"0.1.18"`, `Cargo.toml:4` says `"0.1.19"`.

6. **Phase 3a (SELECT VALUE unimplemented)**: Confirmed. `logical/parser.rs:633` has `unimplemented!()` for `SelectClause::ValueConstructor`. Also `ast.rs:54` has `unimplemented!()` in `Display for SelectStatement` for the same variant. The parser *can* parse `SELECT VALUE` (tested in `test_select_value_statement`), but the logical planner panics on it.

7. **Phase 1c (Float arithmetic missing)**: Confirmed. `execution/types.rs:427-466` -- Plus, Minus, Times, Divide all only match `(Value::Int, Value::Int)` and return `Err(ExpressionError::InvalidArguments)` for any other type combination including Float.

8. **Phase 1d (Comparison type coercion missing)**: Confirmed. `Relation::apply` in `execution/types.rs:595-623` only handles same-type comparisons (Int-Int, Float-Float) for ordered ops. Mixed Int/Float or comparison with Null returns `Err(ExpressionError::TypeMismatch)`.

9. **Phase 2h (date_part only second/minute)**: Confirmed. `execution/types.rs:477-480` only implements `DatePartUnit::Second` and `DatePartUnit::Minute`, with `_ => Err(ExpressionError::DatePartUnitNotSupported)` for hour/day/month/year. The parsing in `common/types.rs:228-236` *does* parse all units; it is only the evaluator that is incomplete.

10. **Existing test count ~58**: Plausible. The codebase has test modules in `syntax/parser.rs` (~20 tests), `logical/parser.rs` (~7 tests), `logical/types.rs` (~5 tests), `execution/types.rs` (~15 tests), `execution/stream.rs` (~4 tests), `common/types.rs` (~1 test), `app.rs` (~4 tests), which sums to approximately 56 tests.
