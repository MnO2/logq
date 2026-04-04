# PartiQL Full Implementation Design (v2)

**Date:** 2026-04-04
**Goal:** Complete logq's PartiQL implementation to full spec compliance (with pragmatic exclusions), using a bottom-up phased approach with spec-driven test oracle.

## Changes from Previous Version

This revision addresses all critical issues (C1--C6) and incorporates suggestions (S1--S3, S5, S7, S8) from the first design review.

- **C1 (Value::Missing already exists):** Rewrote Phase 1a. `Value::Missing` already exists in the codebase. Phase 1a now focuses on proper NULL/MISSING propagation semantics in the evaluator -- binary ops, comparisons, and function calls should propagate NULL/MISSING per spec instead of returning TypeMismatch.
- **C2 (parse_logic panics on new expression types):** Added Phase 1g to refactor `parse_logic` in `logical/parser.rs` -- remove the `unreachable!()` catch-all and add routing for FuncCall, LIKE, IN, BETWEEN, IS NULL/MISSING. This is a hard prerequisite for Phase 2.
- **C3 (Parser case-sensitivity fix is non-atomic):** Rewrote Phase 0a with three explicit atomic steps: (1) replace all `tag()` with `tag_no_case()` in parser.rs, (2) make `BinaryOperator::from_str` and `Ordering::from_str` case-insensitive, (3) remove `to_ascii_lowercase()` from main.rs. All three must land in a single commit.
- **C4 (is_match_group_by_fields nondeterministic bug):** Added Phase 0f to fix the `HashSet` positional iteration bug by using proper set equality.
- **C5 (CASE WHEN single branch):** Added Phase 1h to extend `CaseWhenExpression` AST to support multiple WHEN branches, with corresponding parser, logical planner, and evaluator updates.
- **C6 (JOINs/subqueries need AST restructuring):** Expanded Phase 3d, 3e, 3f with explicit AST changes: new `TableReference` variants for JOIN types with ON conditions, `Expression::Subquery` variant for nested SELECT, and new top-level `Query` enum for set operations.
- **S1 (Desugar at parser level):** BETWEEN, COALESCE, and NULLIF are now desugared at the parser/AST level rather than in the logical planner.
- **S2 (|| operator is a 5-file change):** Phase 2e now explicitly lists all five files that need changes.
- **S3 (thiserror not in Cargo.toml):** Phase 0b now explicitly notes adding `thiserror` to Cargo.toml.
- **S5 (Top-level Query AST node):** Phase 4 now introduces a top-level `Query` enum wrapping `SelectStatement` and set operations.
- **S7 (ORDER BY mixed NULL/MISSING pairs):** Phase 1f now explicitly handles mixed NULL/MISSING pairs per spec (NULL/MISSING sort last ascending, first descending).
- **S8 (LimitStream bug):** Added Phase 0g to fix the `LimitStream` bug where it continues consuming from the source after the limit is reached.

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

## Phase 0: Code Cleanup

Fix existing bugs and tech debt before feature work.

### 0a. Fix Input Lowercasing (Atomic Case-Sensitivity Fix)
- **Files:** `src/syntax/parser.rs`, `src/syntax/ast.rs`, `src/main.rs`
- **Problem:** Query string is lowercased at `main.rs:36` before parsing, destroying case in string literals (`WHERE name = 'Alice'` becomes `WHERE name = 'alice'`). The lowercasing is currently the *only* reason the parser works with mixed-case input, because the nom parser uses case-sensitive `tag()` calls for all 25+ keywords.
- **Fix (all three steps must be a single atomic commit):**
  1. Replace every `tag("keyword")` call in `parser.rs` with `tag_no_case("keyword")` for all keywords: `select`, `from`, `where`, `group`, `by`, `order`, `limit`, `having`, `case`, `when`, `then`, `else`, `end`, `as`, `at`, `value`, `not`, `and`, `or`, `asc`, `desc`, `true`, `false`, `within`, `distinct`, etc.
  2. Make `BinaryOperator::from_str` and `Ordering::from_str` in `ast.rs` case-insensitive (e.g., convert input to lowercase before matching, or use `eq_ignore_ascii_case`).
  3. Remove `query_str.to_ascii_lowercase()` from `main.rs`.
- **Why atomic:** Doing step 3 without steps 1-2 will break every query with uppercase letters. Doing steps 1-2 without step 3 is harmless but incomplete.

### 0b. Migrate `failure` to `anyhow`/`thiserror`
- **Problem:** `failure` crate is unmaintained since 2020; `anyhow` is already in Cargo.toml but unused
- **Fix:** Add `thiserror` to `Cargo.toml`. Replace `failure::Error` with `anyhow::Error` throughout. Replace every `#[derive(Fail)]` with `#[derive(thiserror::Error)]` and every `#[fail(display = ...)]` with `#[error(...)]`. There are 12 error types across 5 files. Replace manual `From` impls with `#[from]` attribute where applicable. Do this as a single commit to avoid a half-migrated state.

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
- **Problem:** Group-by field matching iterates both `HashSet`s with `next()` and compares positionally. `HashSet` has no guaranteed iteration order, so this comparison is nondeterministic -- it can return `false` for sets that are actually equal, or `true` for sets that differ. This works today only by luck with small sets.
- **Fix:** Replace positional iteration with proper set equality: use `a == b` (which `HashSet` implements via subset checks) or `a.is_subset(&b) && b.is_subset(&a)`.

### 0g. Fix `LimitStream` Early Termination Bug
- **File:** `execution/stream.rs:237-251`
- **Problem:** `LimitStream::next()` uses `while let Some(record) = self.source.next()?` which continues pulling records from the source even after `self.curr >= self.row_count`. Once the limit is hit, it should immediately return `Ok(None)` instead of draining the entire upstream. This becomes a performance problem with expensive subqueries or large datasets.
- **Fix:** Add an early return `if self.curr >= self.row_count { return Ok(None); }` at the top of `next()`, before pulling from the source.

## Phase 1: Foundation (Data Model & Type System)

### 1a. NULL/MISSING Propagation Semantics
- **Note:** `Value::Missing` already exists in `common/types.rs:29` and is already returned by `get_value_by_path_expr` for absent attributes. No new variant is needed.
- **Problem:** The evaluator does not properly propagate NULL and MISSING. Currently, comparisons with NULL or MISSING hit `TypeMismatch` errors (e.g., `Relation::apply` in `execution/types.rs:595-623` only matches Int-Int and Float-Float for ordered comparisons). Binary operations on NULL/MISSING also return `InvalidArguments` instead of propagating.
- **Fix:** Update the evaluator so that:
  - Most scalar operations (arithmetic, string ops) on NULL return NULL, on MISSING return MISSING
  - Comparison with NULL yields NULL (not true/false, not TypeMismatch)
  - Comparison with MISSING yields MISSING
  - WHERE clause treats NULL and MISSING as "not true" (only `true` rows pass)
  - Logical operators (AND/OR) use three-valued logic per spec (e.g., `TRUE AND NULL` = `NULL`, `FALSE AND NULL` = `FALSE`, `TRUE OR NULL` = `TRUE`)
- **Files:** `execution/types.rs` (Relation::apply, BinaryOperator evaluate, UnaryOperator evaluate)

### 1b. Float Arithmetic
- Extend Plus/Minus/Times/Divide in `execution/types.rs:427-466` to handle `Value::Float` and mixed Int/Float (promote Int to Float)
- Currently only `Value::Int` supported; all other type combinations return `InvalidArguments`

### 1c. Type Coercion for Comparisons
- Allow comparing Int with Float (promote Int to Float)
- Allow comparing with NULL (result is NULL, not TypeMismatch)
- Allow comparing with MISSING (result is MISSING, not TypeMismatch)
- **File:** `execution/types.rs` `Relation::apply`

### 1d. IS [NOT] NULL / IS [NOT] MISSING
- New unary operators in parser and evaluator
- `x IS NULL` returns true if x is NULL, false otherwise
- `x IS MISSING` returns true if x is MISSING, false otherwise
- `x IS NOT NULL` and `x IS NOT MISSING` as syntactic sugar for NOT

### 1e. Fix Mixed-Type Ordering
- **File:** `execution/types.rs:809`
- Replace `unreachable!()` in ORDER BY with spec-compliant ordering
- NULL and MISSING sort last in ascending order, first in descending order
- Explicitly handle mixed pairs: `(Int, Null)`, `(Null, Float)`, `(Missing, Int)`, `(Null, Missing)`, etc.
- For `(Null, Missing)` and `(Missing, Null)`: both are "unknown" -- treat MISSING = NULL for ordering purposes (both sort to the same end)

### 1f. Multi-Branch CASE WHEN
- **Problem:** The AST `CaseWhenExpression` struct (`syntax/ast.rs:131-135`) has a single `condition`/`then_expr`/`else_expr`. Real CASE WHEN supports multiple WHEN branches: `CASE WHEN c1 THEN e1 WHEN c2 THEN e2 ELSE e3 END`.
- **Fix:**
  - Change `CaseWhenExpression` to hold `Vec<(Expression, Expression)>` for condition/result pairs, plus `Option<Box<Expression>>` for the ELSE branch
  - Update the parser (`case_when_expression`) to parse multiple WHEN clauses
  - Update the logical planner's `parse_case_when_expression` to handle the vector of branches
  - Update the physical evaluator's `Expression::Branch` to evaluate branches in order, returning the first match

### 1g. Refactor `parse_logic` for New Expression Types
- **File:** `logical/parser.rs:58-85`
- **Problem:** `parse_logic` has a `_ => unreachable!()` catch-all on line 83. It only handles `BinaryOperator` (And/Or/comparisons), `UnaryOperator` (Not), and `Value` (boolean constants). Any new expression type -- LIKE, IN, BETWEEN, IS NULL, IS MISSING, function calls in WHERE clauses -- will hit `unreachable!()` and panic.
- **Fix:** Replace `_ => unreachable!()` with routing for:
  - `Expression::FuncCall` -- delegate to `parse_condition` or a new handler (needed for WHERE clauses containing function calls like `UPPER(name) = 'ALICE'`)
  - `Expression::CaseWhenExpression` -- delegate to condition parsing
  - New expression variants as they are added (LIKE, IN, BETWEEN, IS NULL/MISSING)
- **This is a hard prerequisite for all Phase 2 work.** Without this fix, no new expression type can appear in a WHERE clause.

**Milestone gate: "Foundation" -- pause for review**

## Phase 2: Expressions

### 2a. LIKE Operator
- Pattern matching with `%` (any sequence) and `_` (single char) wildcards
- Optional `ESCAPE` clause
- Compile pattern to regex at evaluation time
- `NOT LIKE` as syntactic sugar
- May be represented as a new `BinaryOperator::Like` variant or as a dedicated AST node

### 2b. BETWEEN Operator
- `x BETWEEN y AND z`
- **Desugar at the parser/AST level** (not the logical planner): rewrite to `x >= y AND x <= z` immediately after parsing, so downstream code only sees standard binary comparisons
- `NOT BETWEEN` likewise desugars to `x < y OR x > z`

### 2c. IN Operator
- `x IN (a, b, c)` -- membership test against literal list
- Right side parsed as array constructor
- NULL-aware per spec: if x is NULL, result is NULL; if list contains NULL and no exact match, result is NULL

### 2d. CAST Operator
- `CAST(x AS type)` for: INT<->FLOAT, STRING->INT/FLOAT, INT/FLOAT->STRING
- Parse as function-like expression with type argument

### 2e. String Concatenation (`||`)
- New binary operator at same precedence as addition
- NULL propagating
- **This is a 5-file change:**
  1. `syntax/ast.rs` -- add `BinaryOperator::Concat` variant
  2. `syntax/ast.rs` -- add `"||"` case to `BinaryOperator::from_str`
  3. `syntax/parser.rs` -- add `tag("||")` to `parse_expression_op` and the precedence table
  4. `logical/parser.rs` -- route `Concat` through expression parsing
  5. `execution/types.rs` -- implement concatenation in `evaluate` for String values with NULL propagation

### 2f. COALESCE and NULLIF
- `COALESCE(a, b, c)` returns the first non-NULL/non-MISSING value
- `NULLIF(a, b)` returns NULL if a=b, else a
- **Desugar at the parser/AST level** (not the logical planner):
  - `COALESCE(a, b)` rewrites to `CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a ELSE b END`
  - `NULLIF(a, b)` rewrites to `CASE WHEN a = b THEN NULL ELSE a END`
- This avoids duplicating logic in the logical planner and leverages the multi-branch CASE WHEN from Phase 1f

### 2g. String Functions
- `UPPER`, `LOWER`, `SUBSTRING(str FROM pos [FOR len])`, `TRIM`, `CHAR_LENGTH`
- Added as built-in scalar functions alongside existing URL/host functions

### 2h. Extend date_part
- Add `hour`, `day`, `month`, `year` support in the evaluator (`execution/types.rs:477-480`)
- The parser already handles all units (`common/types.rs:228-236`); only the evaluator is incomplete

**Milestone gate: "Expressions" -- pause for review**

## Phase 3: Clauses & Query Structure

### 3a. SELECT VALUE
- Currently parsed but `unimplemented!()` in logical planner (`logical/parser.rs:633`) and Display for SelectStatement (`syntax/ast.rs`)
- Returns raw value of a single expression rather than wrapping in tuple
- Create special projection node that outputs values directly

### 3b. DISTINCT
- `SELECT DISTINCT` eliminates duplicate rows
- New `DistinctStream` physical node with HashSet of seen records
- **Note on hashing:** `Value` already derives `Hash` (via `OrderedFloat` and `linked_hash_map`). However, verify whether `LinkedHashMap`'s `Hash` impl is order-independent -- for DISTINCT semantics we need order-independent hashing. If it is order-dependent, serialize Records to a canonical form (sorted keys) before hashing.

### 3c. Path Wildcards
- `x[*]` iterates all elements of an array
- `x.*` iterates all values of a tuple
- New PathExpr variants; evaluator returns array of matched values

### 3d. JOINs (CROSS JOIN and LEFT JOIN)

**AST changes required:**

The current `TableReference` is a simple struct with a `PathExpr` and optional AS/AT clauses. `SelectStatement.table_references` is `Vec<TableReference>`, which implicitly represents comma-separated FROM items (implicit cross join). This must be restructured:

- Add a `FromClause` enum (or extend `TableReference`) with variants:
  - `Table(PathExpr, Option<String>, Option<String>)` -- simple table ref with optional AS/AT aliases
  - `CrossJoin(Box<FromClause>, Box<FromClause>)` -- cartesian product
  - `InnerJoin(Box<FromClause>, Box<FromClause>, Expression)` -- inner join with ON condition
  - `LeftJoin(Box<FromClause>, Box<FromClause>, Expression)` -- left outer join with ON condition
  - `Subquery(Box<SelectStatement>, Option<String>)` -- derived table with optional alias (see 3f)

**Parser changes:**
- Parse `FROM a CROSS JOIN b`, `FROM a LEFT JOIN b ON condition`, `FROM a INNER JOIN b ON condition`
- Handle chained joins: `FROM a JOIN b ON ... JOIN c ON ...`

**Logical planner changes:**
- New plan nodes for each join type
- ON condition parsed as a filter expression

**Physical execution:**
- `CrossJoinStream`: nested loop -- for each left record, iterate all right records
- `LeftJoinStream`: nested loop with tracking of unmatched left rows; pad unmatched rows with NULL/MISSING for right-side columns

### 3e. LATERAL
- Allows FROM item to reference variables from preceding FROM items
- Implicit in PartiQL (unlike SQL)
- Pass current binding environment to subsequent FROM item evaluation
- This is primarily a change to how the evaluator resolves variable references in nested FROM items, not an AST change

### 3f. Non-Correlated Subqueries

**AST changes required:**

- Add `Expression::Subquery(Box<SelectStatement>)` variant to represent nested SELECT in expressions
- This allows subqueries in WHERE: `WHERE x IN (SELECT y FROM ...)`, `WHERE EXISTS (SELECT ...)`

**Parser changes:**
- Parse nested `SELECT` statements inside expressions (parenthesized)
- Distinguish `(SELECT ...)` from `(expr)` in the expression parser

**Logical planner changes:**
- Materialize inner query result before use in outer query
- For FROM subqueries: evaluate subquery, produce stream of records
- For WHERE subqueries with IN: evaluate subquery, collect into set, test membership
- For WHERE subqueries with EXISTS: evaluate subquery, check if any rows returned

**Execution:**
- Subquery evaluation must complete before outer query continues (non-correlated)
- Materialize subquery results into a `Vec<Record>` or `Vec<Value>`

**Milestone gate: "Clauses" -- pause for review**

## Phase 4: Set Operations

### Top-Level Query AST Node

Before implementing set operations, introduce a top-level `Query` AST type to replace bare `SelectStatement` as the root of parsing:

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

enum SetOperator {
    Union,
    Intersect,
    Except,
}
```

This requires updating `select_query` in the parser and `app::run` to work with `Query` instead of `SelectStatement`. The logical planner and execution layer must also accept `Query` as their entry point.

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
- Same materialization approach as INTERSECT with inverse filter

Shared implementation: single `SetOperationStream` parameterized by operation type.

**Milestone gate: "Complete" -- final review**
