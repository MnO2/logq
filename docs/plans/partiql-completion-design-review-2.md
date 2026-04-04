VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design has substantially improved: all six critical issues from round 1 have been addressed with correct analysis and concrete remediation plans. However, two new critical issues emerged -- one related to identifier/keyword case-sensitivity that the design overlooks entirely, and one where the design proposes desugaring BETWEEN/COALESCE/NULLIF at the "parser/AST level" but does not account for the fact that the parser's expression grammar and precedence-climbing algorithm cannot naturally accommodate these constructs without significant restructuring.

## Round 1 Issues Resolution Check

### C1 (Value::Missing already exists) -- RESOLVED
The v2 design correctly acknowledges that `Value::Missing` already exists at `common/types.rs:29` and rewrites Phase 1a to focus on NULL/MISSING propagation semantics. The analysis of where `Relation::apply` returns `TypeMismatch` instead of propagating NULL/MISSING is accurate (confirmed at `execution/types.rs:602-623`). The three-valued logic specification for AND/OR is correct per the PartiQL spec. Well addressed.

### C2 (parse_logic panics on new expression types) -- RESOLVED
Phase 1g now explicitly addresses the `unreachable!()` at `logical/parser.rs:83`. The design correctly identifies that FuncCall, CaseWhenExpression, and future expression types (LIKE, IN, BETWEEN, IS NULL/MISSING) will all hit this panic. Making this a hard prerequisite for Phase 2 is the right call. Well addressed.

### C3 (Parser case-sensitivity fix is non-atomic) -- RESOLVED
Phase 0a now explicitly lists three atomic steps and correctly notes all must land in a single commit. The enumeration of 25+ `tag()` calls that need `tag_no_case()` replacement is accurate. The dependency analysis (removing lowercasing without fixing the parser breaks everything) is correct. Well addressed.

### C4 (is_match_group_by_fields nondeterministic bug) -- RESOLVED
Phase 0f correctly identifies the bug at `logical/parser.rs:772-784` and proposes proper set equality. Confirmed: the current code at lines 775-783 iterates both `HashSet`s positionally with `next()`, which is indeed nondeterministic. The proposed fix (use `a == b`) is correct. Well addressed.

### C5 (CASE WHEN single branch) -- RESOLVED
Phase 1f (previously labeled 1h in the "Changes" section, but correctly labeled 1f in the body) now explicitly identifies the `CaseWhenExpression` limitation. Confirmed: `ast.rs:131-135` has single `condition`/`then_expr`/`else_expr` fields, and the parser at lines 31-49 parses only one WHEN clause. The proposed vector-of-pairs solution is standard and correct. Well addressed.

### C6 (JOINs/subqueries need AST restructuring) -- RESOLVED
Phase 3d now provides explicit `FromClause` enum variants with concrete types for each JOIN type, ON conditions, and subquery references. Phase 3f adds `Expression::Subquery(Box<SelectStatement>)`. The detail level is appropriate for a design document. Well addressed.

### S1 (Desugar at parser level) -- ADOPTED
BETWEEN, COALESCE, and NULLIF are now desugared at the parser/AST level. Good. However, see Critical Issue C1 below for a concern about how this interacts with the parser's expression grammar.

### S2 (|| operator is a 5-file change) -- ADOPTED
Phase 2e now explicitly lists all five files. Good.

### S3 (thiserror not in Cargo.toml) -- ADOPTED
Phase 0b now notes adding `thiserror` to Cargo.toml. Good.

### S5 (Top-level Query AST node) -- ADOPTED
Phase 4 now introduces a `Query` enum. Good.

### S7 (ORDER BY mixed NULL/MISSING pairs) -- ADOPTED
Phase 1e now handles mixed pairs and specifies `(Null, Missing)` ordering. Good.

### S8 (LimitStream bug) -- ADOPTED
Phase 0g now fixes the early termination bug. Confirmed: the current code at `execution/stream.rs:237-246` will keep pulling from the source after the limit is reached because the `while let` loop pulls a record before checking the counter. The proposed fix is correct.

## Critical Issues (must fix)

### C1. BETWEEN desugaring at the "parser/AST level" is architecturally misspecified

The design states that BETWEEN, COALESCE, and NULLIF should be desugared "at the parser/AST level" (Phases 2b, 2f). This is the right *destination* but the design does not account for how the current parser's expression grammar handles this.

The parser uses a precedence-climbing algorithm (`parse_expression_at_precedence` at lines 446-472 of `parser.rs`) where atoms are parsed by `parse_expression_atom` and binary operators by `parse_expression_op`. BETWEEN has the form `expr BETWEEN expr AND expr` -- the `AND` keyword inside BETWEEN conflicts with the logical `AND` operator. The precedence climber will try to parse `x BETWEEN y AND z` and the `AND` will be consumed as a logical operator, not as part of the BETWEEN syntax.

This means BETWEEN cannot simply be "desugared" inside the existing expression parser. It needs one of:
1. Special-case handling in `parse_expression_atom` or `factor` to recognize `BETWEEN` after a primary expression has been parsed (similar to how postfix operators work), OR
2. A separate parsing pass after the precedence climber, OR
3. Integration into the precedence climber with special multi-operand support.

Similarly, `COALESCE(a, b)` and `NULLIF(a, b)` look syntactically like function calls. The design says to desugar them at the parser level, but they would naturally parse as `Expression::FuncCall("coalesce", ...)` and `Expression::FuncCall("nullif", ...)` through the existing `func_call` parser (lines 178-193). Desugaring them would need to happen *after* parsing, in a post-parse AST rewrite, not during parsing. Otherwise the design must explain how `func_call` will detect these names and emit different AST nodes.

The design should clarify the exact mechanism: are these desugared in a post-parse transformation pass over the AST, or does the parser itself produce the desugared form? If the latter, the parser modifications required are non-trivial and should be explicitly described.

### C2. Case-insensitive keyword matching in the identifier function is not addressed

Phase 0a correctly identifies that all `tag()` calls must become `tag_no_case()`. However, it does not address a second, equally critical problem: the `identifier` function at `parser.rs:85-128`.

The `identifier` function rejects strings that match keywords, using `KEYWORDS.contains(&o)` at line 117. The `KEYWORDS` list (lines 22-29) contains only lowercase strings: `"select"`, `"value"`, `"from"`, `"where"`, etc. After removing `to_ascii_lowercase()` from `main.rs`, an input like `SELECT a FROM it WHERE a = 1` will:
1. Correctly match `tag_no_case("select")` (good).
2. Try to parse `a` as an identifier. `identifier("a")` works (good).
3. Try to parse `FROM` as a keyword via `tag_no_case("from")` (good).
4. But also: if a user writes a column named `Select` or `FROM`, the `identifier` function will NOT reject it because `KEYWORDS.contains(&"FROM")` is `false` (the list only has `"from"`).

This means after the case-sensitivity fix, column names can collide with keywords in certain cases. For example, `select FROM from it` would be ambiguous -- is `FROM` a column name or the FROM keyword? Currently this works because everything is lowercased first, making the keyword check reliable.

The fix: the `KEYWORDS.contains(&o)` check at line 117 must be changed to a case-insensitive comparison, such as `KEYWORDS.contains(&o.to_ascii_lowercase().as_str())` or the KEYWORDS list must include all case variants (impractical), or the identifier function must normalize to lowercase before checking.

This is not cosmetic -- if this is missed, the parser will produce incorrect ASTs for queries where identifiers happen to be keyword-like strings in different cases.

## Suggestions (nice to have)

### S1. Phase 1a three-valued logic needs changes to Formula::evaluate signature

The design says `Formula::evaluate` should return three-valued logic results (TRUE/FALSE/NULL for AND/OR). Currently `Formula::evaluate` at `execution/types.rs:642` returns `EvaluateResult<bool>` -- a `Result<bool, EvaluateError>`. To support three-valued logic, the return type needs to change to something like `EvaluateResult<Option<bool>>` where `None` represents NULL/MISSING, or a custom enum `TruthValue { True, False, Unknown }`.

This is a pervasive change -- every caller of `Formula::evaluate` (FilterStream, Expression::Branch, GroupByStream predicate checks) must be updated. The design should note this signature change explicitly, as it is easy to underestimate the scope. FilterStream needs to treat `Unknown` as "not true" (skip the row), while `Expression::Branch` with a CASE WHEN might need to propagate Unknown differently.

### S2. The `parse_expression_op` function will have ambiguity with `and` in BETWEEN

Even with the BETWEEN desugaring approach, the `parse_expression_op` function (lines 429-443) lists `tag("and")` and `tag("or")` as operators. When parsing `x BETWEEN y AND z`, if BETWEEN is handled as a special form, the precedence climber must not see the `AND` inside BETWEEN as a binary logical operator. This is related to C1 but worth calling out: whatever mechanism handles BETWEEN must consume the `AND` token before the precedence climber can see it.

### S3. The `ast::Value` enum in `ast.rs` is missing `Null`

The syntax AST `Value` enum (`ast.rs:204-210`) has `Integral`, `Float`, `StringLiteral`, `Boolean` but no `Null` variant. The common types `Value` (`common/types.rs:20-32`) does have `Null`. For IS NULL tests and NULL literal support (`WHERE x IS NULL`, `CASE WHEN ... THEN NULL`), the parser needs to be able to produce a NULL literal. This means either adding `Null` to `ast::Value` or handling NULL as a keyword that produces `Expression::Value(ast::Value::Null)` somewhere. The design does not mention this gap.

### S4. `LimitStream` fix in Phase 0g is slightly mischaracterized

The design says the bug is that LimitStream "continues consuming from the source after the limit is reached." Looking at the actual code (`stream.rs:237-246`), the behavior is: once `self.curr >= self.row_count`, the `while let` loop will pull one more record from the source (the next `self.source.next()?` call) and then the `if` check will fail, falling through to the next loop iteration. It will keep pulling records one at a time until the source is exhausted. So the characterization is correct in effect, but the fix described ("add early return at the top of `next()`") is the right one. The current code pulls at most one extra record per `next()` call after the limit, but the caller will keep calling `next()` until `Ok(None)` is returned, so it does drain the entire source. Minor point -- the fix is correct.

### S5. Consider splitting Phase 0a and Phase 1g into sub-tasks

Phase 0a (case-sensitivity) touches `parser.rs`, `ast.rs`, and `main.rs` simultaneously. Phase 1g (parse_logic refactor) touches `logical/parser.rs`. These are both foundational and should include tests that verify case-insensitive parsing and that new expression types in WHERE clauses do not panic. The design's test-first approach should be emphasized here -- write tests like `select a from it where UPPER(a) = 'FOO'` before implementing.

### S6. The `distinct` keyword is not in the KEYWORDS list

The KEYWORDS list in `parser.rs:22-29` does not include `"distinct"`, `"having"`, `"as"`, `"at"`, `"not"`, `"and"`, `"or"`, `"asc"`, `"desc"`, `"within"`, `"end"`, `"else"`, or `"limit"`. These are mentioned in Phase 0a's list of keywords to convert to `tag_no_case()`, but the KEYWORDS list is used by the `identifier` function to reject keywords-as-identifiers. If `"having"` is not in KEYWORDS, a user could use `having` as a column name, causing ambiguity. The design should note that the KEYWORDS list must be updated to include all keywords, not just the currently listed ones.

## Verified Claims (things you confirmed are correct)

1. **Phase 0a (tag() calls are all case-sensitive)**: Confirmed. There are 43 `tag()` calls in `parser.rs` and zero `tag_no_case()` calls. All keyword matching is case-sensitive.

2. **Phase 0b (failure crate still in use)**: Confirmed. `failure = "0.1"` at `Cargo.toml:21`. `#[macro_use] extern crate failure;` at `main.rs:2`. `#[derive(Fail)]` used in `logical/parser.rs:10`, `logical/types.rs:11`, `execution/types.rs:18,33,63,108`, `common/types.rs:37,83,177,212`, `app.rs:15`.

3. **Phase 0c (duplicated get_value_by_path_expr)**: Confirmed. Identical implementations at `execution/types.rs:166-213` and `execution/stream.rs:11-58`.

4. **Phase 0e (version mismatch)**: Confirmed. `cli.yml:2` says `"0.1.18"`, `Cargo.toml:5` says `"0.1.19"`.

5. **Phase 0f (is_match_group_by_fields nondeterministic)**: Confirmed. Lines 775-783 of `logical/parser.rs` iterate two `HashSet`s with `next()` and compare positionally.

6. **Phase 0g (LimitStream bug)**: Confirmed. `stream.rs:237-246` shows the `while let` loop pulls from source before checking the limit counter.

7. **Phase 1a (NULL/MISSING propagation missing)**: Confirmed. `Relation::apply` at `execution/types.rs:602-623` returns `TypeMismatch` for any non-matching type pair. Arithmetic operators at lines 427-466 return `InvalidArguments` for non-Int types. `Formula::evaluate` at lines 642-665 returns `bool`, not a three-valued result.

8. **Phase 1b (Float arithmetic missing)**: Confirmed. Plus/Minus/Times/Divide at `execution/types.rs:427-466` only handle `(Value::Int, Value::Int)`.

9. **Phase 1f (CASE WHEN single branch)**: Confirmed. `CaseWhenExpression` at `ast.rs:131-135` has single condition/then_expr/else_expr. Parser `case_when_expression` at `parser.rs:31-49` parses one WHEN clause.

10. **Phase 1g (parse_logic unreachable)**: Confirmed. `logical/parser.rs:83` has `_ => unreachable!()`.

11. **Phase 2h (date_part incomplete)**: Confirmed. `execution/types.rs:477-480` only handles Second and Minute.

12. **Phase 3a (SELECT VALUE unimplemented)**: Confirmed. `logical/parser.rs:633` and `ast.rs:54` both have `unimplemented!()`.

13. **thiserror not in Cargo.toml**: Confirmed. Not present in `Cargo.toml` dependencies.

14. **anyhow in Cargo.toml but unused**: Confirmed. `anyhow = "1.0"` at `Cargo.toml:34` but no imports found in source files.
