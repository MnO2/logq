# PartiQL Full Implementation Design (Final)

**Date:** 2026-04-04
**Goal:** Complete logq's PartiQL implementation to full spec compliance (with pragmatic exclusions), using a bottom-up phased approach with spec-driven test oracle.

## Changes from v2

Addresses two critical issues and incorporates suggestions from design review round 2.

- **C1 (BETWEEN desugaring misspecified):** BETWEEN cannot be desugared inline during parsing because the `AND` keyword inside `x BETWEEN y AND z` conflicts with the logical AND in the precedence-climbing parser. Fixed: BETWEEN is now parsed as a dedicated ternary AST node (postfix special-case after primary expression), then rewritten to `x >= y AND x <= z` in a post-parse AST transformation pass. COALESCE/NULLIF parse naturally as FuncCall nodes and are rewritten to CASE WHEN in the same post-parse pass.
- **C2 (Identifier keyword rejection is case-sensitive):** After removing `to_ascii_lowercase()`, the `KEYWORDS.contains(&o)` check in the `identifier` function will fail for mixed-case keyword-like identifiers (e.g., `FROM` as a column name won't be rejected). Fixed: normalize identifier to lowercase before keyword check, and expand KEYWORDS list to include all keywords (having, not, and, or, distinct, asc, desc, as, at, etc.).
- **S1 (Formula::evaluate return type):** Phase 1a now notes that `Formula::evaluate` must change from `Result<bool>` to `Result<Option<bool>>` or a `TruthValue` enum to support three-valued logic, with all callers updated.
- **S3 (ast::Value missing Null variant):** Phase 1d now notes adding `Null` to the syntax AST's `Value` enum for NULL literal support.
- **S6 (KEYWORDS list incomplete):** Phase 0a now explicitly includes expanding the KEYWORDS list.

**Note:** Design was not fully approved by the reviewer after 2 rounds — please review carefully before proceeding, particularly the BETWEEN parsing strategy and the post-parse transformation approach.

---

## Scope

### In Scope
- SELECT / SELECT VALUE / DISTINCT
- FROM with CROSS JOIN, LEFT JOIN, LATERAL
- WHERE with all operators (LIKE, IN, BETWEEN, IS [NOT] NULL, IS [NOT] MISSING)
- CAST, COALESCE, NULLIF, CASE WHEN (multiple branches)
- String concatenation (`||`), string functions (UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH)
- GROUP BY with GROUP AS, HAVING
- ORDER BY with proper NULL/MISSING ordering
- LIMIT
- UNION / INTERSECT / EXCEPT (and ALL variants)
- Path wildcards (`[*]`, `.*`)
- Non-correlated subqueries (in FROM and WHERE)
- Proper NULL/MISSING propagation semantics
- Type coercion (Int/Float promotion)

### Out of Scope
- PIVOT clause
- Ion literal support (backtick syntax)
- Bag literals (`<<...>>`)
- Correlated subqueries
- Window functions
- Schema-based static type checking

## Test Oracle Strategy

Hybrid approach:
1. **Spec examples** -- Extract concrete examples from the PartiQL Specification PDF as ground truth tests
2. **Hand-crafted edge cases** -- Domain-specific tests for log querying scenarios
3. **Regression** -- All existing ~58 tests must never regress
4. Tests written *before* implementation for each phase (test-first)

## Coordination Model

- **CLAUDE.md** -- Living plan with rules: run `cargo test` before every commit; never commit code that breaks passing tests
- **CHANGELOG.md** -- Progress log tracking completed tasks, failed approaches, known limitations
- **Git commits** after every meaningful unit of work
- **Milestone-gated** -- Autonomous within phases; pause at major milestones for review

---

## Phase 0: Code Cleanup

Fix existing bugs and tech debt before feature work.

### 0a. Fix Input Lowercasing (Atomic Case-Sensitivity Fix)
- **Files:** `src/syntax/parser.rs`, `src/syntax/ast.rs`, `src/main.rs`
- **Problem:** Query string is lowercased at `main.rs:36` before parsing, destroying case in string literals. The lowercasing is the *only* reason the parser works with mixed-case input, because the nom parser uses case-sensitive `tag()` calls for all 25+ keywords.
- **Fix (all steps must be a single atomic commit):**
  1. Replace every `tag("keyword")` call in `parser.rs` with `tag_no_case("keyword")` for all keywords: `select`, `from`, `where`, `group`, `by`, `order`, `limit`, `having`, `case`, `when`, `then`, `else`, `end`, `as`, `at`, `value`, `not`, `and`, `or`, `asc`, `desc`, `true`, `false`, `within`, `distinct`, etc.
  2. Make `BinaryOperator::from_str` and `Ordering::from_str` in `ast.rs` case-insensitive (convert input to lowercase before matching, or use `eq_ignore_ascii_case`).
  3. Fix the `identifier` function: normalize identifier to lowercase before checking `KEYWORDS.contains()` so that `FROM`, `From`, `from` are all rejected as identifiers.
  4. Expand the `KEYWORDS` list to include all keywords: add `having`, `not`, `and`, `or`, `asc`, `desc`, `as`, `at`, `within`, `end`, `else`, `then`, `when`, `case`, `distinct`, `limit`, `join`, `cross`, `left`, `inner`, `on`, `lateral`, `like`, `between`, `in`, `is`, `null`, `missing`, `cast`, `union`, `intersect`, `except`, `all`, `exists`.
  5. Remove `query_str.to_ascii_lowercase()` from `main.rs`.
- **Why atomic:** Doing step 5 without steps 1-4 breaks all queries. Steps 1-4 without step 5 is harmless but incomplete.

### 0b. Migrate `failure` to `anyhow`/`thiserror`
- **Problem:** `failure` crate unmaintained since 2020; `anyhow` already in Cargo.toml but unused
- **Fix:** Add `thiserror` to `Cargo.toml`. Replace `failure::Error` with `anyhow::Error` throughout. Replace every `#[derive(Fail)]` with `#[derive(thiserror::Error)]` and every `#[fail(display = ...)]` with `#[error(...)]`. There are 12 error types across 5 files. Replace manual `From` impls with `#[from]` attribute where applicable. Single commit.

### 0c. Deduplicate `get_value_by_path_expr`
- **Problem:** Identical function in `execution/types.rs:166-213` and `execution/stream.rs:11-58`
- **Fix:** Move to `common/` and import from both call sites

### 0d. Fix `PartialEq` Stub
- **Problem:** `ApproxCountDistinctAggregate::PartialEq` at `execution/types.rs:1367-1369` always returns `true`
- **Fix:** Implement proper comparison or derive

### 0e. Fix Version Mismatch
- **Problem:** `cli.yml` says 0.1.18, `Cargo.toml` says 0.1.19
- **Fix:** Sync to 0.1.19

### 0f. Fix `is_match_group_by_fields` Nondeterministic Bug
- **File:** `logical/parser.rs:772-784`
- **Problem:** Iterates two `HashSet`s with `next()` and compares positionally. `HashSet` has no guaranteed iteration order, so this comparison is nondeterministic.
- **Fix:** Use proper set equality (`a == b`).

### 0g. Fix `LimitStream` Early Termination Bug
- **File:** `execution/stream.rs:237-251`
- **Problem:** `LimitStream::next()` continues pulling records from source after limit is reached.
- **Fix:** Add early return `if self.curr >= self.row_count { return Ok(None); }` at top of `next()`.

---

## Phase 1: Foundation (Data Model & Type System)

### 1a. NULL/MISSING Propagation Semantics
- **Note:** `Value::Missing` already exists in `common/types.rs:29`. No new variant needed.
- **Problem:** Evaluator does not propagate NULL/MISSING per spec. Comparisons with NULL/MISSING hit `TypeMismatch`. Binary ops return `InvalidArguments`.
- **Fix:** Update evaluator so that:
  - Most scalar operations on NULL return NULL, on MISSING return MISSING
  - Comparison with NULL yields NULL (not TypeMismatch)
  - Comparison with MISSING yields MISSING
  - WHERE treats NULL and MISSING as "not true"
  - AND/OR use three-valued logic: `TRUE AND NULL = NULL`, `FALSE AND NULL = FALSE`, `TRUE OR NULL = TRUE`, `FALSE OR NULL = NULL`
- **Signature change:** `Formula::evaluate` must change from `Result<bool>` to `Result<Option<bool>>` or a custom `TruthValue { True, False, Unknown }` enum. All callers (FilterStream, Expression::Branch, GroupByStream) must be updated. FilterStream treats `Unknown` as "not true" (skip row).
- **Files:** `execution/types.rs` (Relation::apply, BinaryOperator evaluate, UnaryOperator evaluate, Formula::evaluate)

### 1b. Float Arithmetic
- Extend Plus/Minus/Times/Divide in `execution/types.rs:427-466` to handle `Value::Float` and mixed Int/Float (promote Int to Float)

### 1c. Type Coercion for Comparisons
- Allow comparing Int with Float (promote Int)
- Allow comparing with NULL (result is NULL)
- Allow comparing with MISSING (result is MISSING)
- **File:** `execution/types.rs` `Relation::apply`

### 1d. IS [NOT] NULL / IS [NOT] MISSING
- New unary operators in parser and evaluator
- `x IS NULL` returns true if x is NULL, false otherwise
- `x IS MISSING` returns true if x is MISSING, false otherwise
- `x IS NOT NULL` and `x IS NOT MISSING` as syntactic sugar
- **AST addition:** Add `Null` variant to the syntax AST's `Value` enum (`ast.rs`) for NULL literal support (needed for `CASE WHEN ... THEN NULL`, `SELECT NULL`, etc.)

### 1e. Fix Mixed-Type Ordering
- **File:** `execution/types.rs:809`
- Replace `unreachable!()` with spec-compliant ordering
- NULL/MISSING sort last ascending, first descending
- Handle mixed pairs: `(Int, Null)`, `(Null, Float)`, `(Missing, Int)`, `(Null, Missing)`, etc.
- `(Null, Missing)`: treat as equal for ordering

### 1f. Multi-Branch CASE WHEN
- **Problem:** `CaseWhenExpression` has single condition/then_expr/else_expr. Real CASE WHEN supports multiple WHEN branches.
- **Fix:**
  - Change `CaseWhenExpression` to `Vec<(Expression, Expression)>` for condition/result pairs + `Option<Box<Expression>>` for ELSE
  - Update parser to parse multiple WHEN clauses
  - Update logical planner and physical evaluator

### 1g. Refactor `parse_logic` for New Expression Types
- **File:** `logical/parser.rs:58-85`
- **Problem:** `_ => unreachable!()` on line 83 — any new expression type panics.
- **Fix:** Route FuncCall, CaseWhenExpression, and future expression types through appropriate handlers.
- **Hard prerequisite for all Phase 2 work.**

**Milestone gate: "Foundation" -- pause for review**

---

## Phase 2: Expressions

### Post-Parse AST Transformation Pass

Before implementing individual expression features, introduce a **post-parse AST rewrite pass** that runs between parsing and logical planning. This pass transforms syntactic sugar nodes into core AST nodes:
- BETWEEN → binary comparisons with AND
- COALESCE → CASE WHEN with IS NOT NULL / IS NOT MISSING checks
- NULLIF → CASE WHEN with equality check

This is a function `fn desugar(expr: Expression) -> Expression` that recursively walks the AST and rewrites sugar nodes. It runs once after parsing, before the logical planner sees the AST.

### 2a. LIKE Operator
- Pattern matching with `%` (any sequence) and `_` (single char) wildcards
- Optional `ESCAPE` clause
- Compile pattern to regex at evaluation time
- `NOT LIKE` as syntactic sugar
- New `BinaryOperator::Like` variant or dedicated AST node

### 2b. BETWEEN Operator
- `x BETWEEN y AND z`
- **Parsing strategy:** Cannot use the precedence-climbing parser directly because `AND` conflicts with logical AND. Instead, handle as a postfix special-case: after parsing a primary expression, check for the `BETWEEN` keyword. If found, parse the next two expressions separated by `AND` (consuming `AND` as part of BETWEEN syntax, not as a logical operator). Produce a `Expression::Between(Box<expr>, Box<low>, Box<high>)` AST node.
- **Post-parse rewrite:** The `desugar` pass rewrites `Between(x, y, z)` → `And(Gte(x, y), Lte(x, z))`
- `NOT BETWEEN` similarly: check for `NOT BETWEEN` after a primary expression.

### 2c. IN Operator
- `x IN (a, b, c)` -- membership test against literal list
- Right side parsed as parenthesized expression list
- NULL-aware per spec

### 2d. CAST Operator
- `CAST(x AS type)` for: INT↔FLOAT, STRING→INT/FLOAT, INT/FLOAT→STRING
- Parse as function-like expression with type argument

### 2e. String Concatenation (`||`)
- New binary operator at same precedence as addition
- NULL propagating
- **5-file change:**
  1. `syntax/ast.rs` -- add `BinaryOperator::Concat` variant
  2. `syntax/ast.rs` -- add `"||"` case to `BinaryOperator::from_str`
  3. `syntax/parser.rs` -- add `tag("||")` to `parse_expression_op` and precedence table
  4. `logical/parser.rs` -- route `Concat` through expression parsing
  5. `execution/types.rs` -- implement concatenation for String values with NULL propagation

### 2f. COALESCE and NULLIF
- `COALESCE(a, b, c)` returns first non-NULL/non-MISSING value
- `NULLIF(a, b)` returns NULL if a=b, else a
- **Parsing:** Parse naturally as `FuncCall("coalesce", args)` and `FuncCall("nullif", args)` through the existing `func_call` parser
- **Post-parse rewrite:** The `desugar` pass detects these by name and rewrites:
  - `COALESCE(a, b)` → `CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a ELSE b END`
  - `COALESCE(a, b, c)` → nested: `CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a ELSE COALESCE(b, c) END`
  - `NULLIF(a, b)` → `CASE WHEN a = b THEN NULL ELSE a END`

### 2g. String Functions
- `UPPER`, `LOWER`, `SUBSTRING(str FROM pos [FOR len])`, `TRIM`, `CHAR_LENGTH`
- Added as built-in scalar functions alongside existing URL/host functions

### 2h. Extend date_part
- Add `hour`, `day`, `month`, `year` support in evaluator (`execution/types.rs:477-480`)
- Parser already handles all units; only evaluator is incomplete

**Milestone gate: "Expressions" -- pause for review**

---

## Phase 3: Clauses & Query Structure

### 3a. SELECT VALUE
- Currently parsed but `unimplemented!()` in logical planner and Display
- Returns raw value rather than wrapping in tuple
- Create special projection node that outputs values directly

### 3b. DISTINCT
- `SELECT DISTINCT` eliminates duplicate rows
- New `DistinctStream` physical node with HashSet of seen records
- Verify `LinkedHashMap`'s `Hash` impl is order-independent; if not, serialize to canonical form (sorted keys) before hashing

### 3c. Path Wildcards
- `x[*]` iterates all elements of an array
- `x.*` iterates all values of a tuple
- New PathExpr variants; evaluator returns array of matched values

### 3d. JOINs (CROSS JOIN and LEFT JOIN)

**AST changes:**
- New `FromClause` enum replacing simple `TableReference`:
  - `Table(PathExpr, Option<String>, Option<String>)` -- table ref with AS/AT
  - `CrossJoin(Box<FromClause>, Box<FromClause>)` -- cartesian product
  - `InnerJoin(Box<FromClause>, Box<FromClause>, Expression)` -- with ON
  - `LeftJoin(Box<FromClause>, Box<FromClause>, Expression)` -- with ON
  - `Subquery(Box<SelectStatement>, Option<String>)` -- derived table

**Parser changes:**
- Parse `FROM a CROSS JOIN b`, `FROM a LEFT JOIN b ON cond`, `FROM a INNER JOIN b ON cond`
- Handle chained joins

**Physical execution:**
- `CrossJoinStream`: nested loop
- `LeftJoinStream`: nested loop with unmatched-row tracking, pad with NULLs

### 3e. LATERAL
- Allows FROM item to reference variables from preceding FROM items
- Implicit in PartiQL
- Pass current binding environment to subsequent FROM item evaluation

### 3f. Non-Correlated Subqueries

**AST changes:**
- `Expression::Subquery(Box<SelectStatement>)` for nested SELECT in expressions

**Parser changes:**
- Parse nested SELECT inside parenthesized expressions
- Distinguish `(SELECT ...)` from `(expr)`

**Execution:**
- Materialize inner query before outer query continues
- For IN: collect subquery result into set, test membership
- For EXISTS: check if subquery returns any rows

**Milestone gate: "Clauses" -- pause for review**

---

## Phase 4: Set Operations

### Top-Level Query AST Node
```
enum Query {
    Select(SelectStatement),
    SetOp {
        op: SetOperator,      // Union, Intersect, Except
        all: bool,            // ALL variant
        left: Box<Query>,
        right: Box<Query>,
    },
}
```

Update parser (`select_query`) and `app::run` to work with `Query` instead of `SelectStatement`.

### 4a. UNION / UNION ALL
- Concatenate results of two queries
- UNION eliminates duplicates (uses DISTINCT from Phase 3b)
- UNION ALL keeps all rows

### 4b. INTERSECT / INTERSECT ALL
- Return rows present in both queries
- ALL preserves duplicate counts (min of two)
- Materialize right query into set, filter left stream

### 4c. EXCEPT / EXCEPT ALL
- Return rows in left query not in right
- ALL subtracts duplicate counts
- Same approach as INTERSECT with inverse filter

Shared implementation: `SetOperationStream` parameterized by operation type.

**Milestone gate: "Complete" -- final review**
