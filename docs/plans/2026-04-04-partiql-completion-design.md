# PartiQL Full Implementation Design

**Date:** 2026-04-04
**Goal:** Complete logq's PartiQL implementation to full spec compliance (with pragmatic exclusions), using a bottom-up phased approach with spec-driven test oracle.

## Scope

### In Scope
- SELECT / SELECT VALUE / DISTINCT
- FROM with CROSS JOIN, LEFT JOIN, LATERAL
- WHERE with all operators (LIKE, IN, BETWEEN, IS [NOT] NULL, IS [NOT] MISSING)
- CAST, COALESCE, NULLIF, CASE WHEN
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
1. **Spec examples** — Extract concrete examples from the PartiQL Specification PDF as ground truth tests
2. **Hand-crafted edge cases** — Domain-specific tests for log querying scenarios
3. **Regression** — All existing ~58 tests must never regress
4. Tests written *before* implementation for each phase (test-first)

## Coordination Model

- **CLAUDE.md** — Living plan with rules: run `cargo test` before every commit; never commit code that breaks passing tests
- **CHANGELOG.md** — Progress log tracking completed tasks, failed approaches, known limitations
- **Git commits** after every meaningful unit of work
- **Milestone-gated** — Autonomous within phases; pause at major milestones for review

## Phase 0: Code Cleanup

Fix existing bugs and tech debt before feature work.

### 0a. Fix Input Lowercasing
- **File:** `src/main.rs:36`
- **Problem:** Query string is lowercased before parsing, destroying case in string literals (`WHERE name = 'Alice'` becomes `WHERE name = 'alice'`)
- **Fix:** Make parser case-insensitive for keywords/identifiers while preserving original case in string literals and identifiers

### 0b. Migrate `failure` to `anyhow`/`thiserror`
- **Problem:** `failure` crate is unmaintained since 2020; `anyhow` is already in Cargo.toml but unused
- **Fix:** Replace `failure::Error` with `anyhow::Error` throughout; use `thiserror` for structured error types

### 0c. Deduplicate `get_value_by_path_expr`
- **Problem:** Identical function in `execution/types.rs` and `execution/stream.rs`
- **Fix:** Move to `common/` and import from both call sites

### 0d. Fix `PartialEq` Stub
- **Problem:** `ApproxCountDistinctAggregate::PartialEq` always returns `true`
- **Fix:** Implement proper comparison or derive

### 0e. Fix Version Mismatch
- **Problem:** `cli.yml` says 0.1.18, `Cargo.toml` says 0.1.19
- **Fix:** Sync to 0.1.19

## Phase 1: Foundation (Data Model & Type System)

### 1a. Add MISSING Value Type
- Add `Value::Missing` variant to `common/types.rs`
- PartiQL distinguishes NULL (exists, value unknown) from MISSING (attribute doesn't exist)
- Currently missing fields just aren't in the record; need explicit MISSING for proper propagation

### 1b. NULL/MISSING Propagation Semantics
- Most scalar operations on NULL return NULL, on MISSING return MISSING
- Comparison with NULL yields NULL (not true/false)
- WHERE treats NULL and MISSING as "not true" (only `true` rows pass)
- Affects expression evaluator in `execution/types.rs`

### 1c. Float Arithmetic
- Extend Plus/Minus/Times/Divide to handle `Value::Float` and mixed Int/Float (promote Int to Float)
- Currently only `Value::Int` supported

### 1d. Type Coercion for Comparisons
- Allow comparing Int with Float (promote Int)
- Allow comparing with NULL (result is NULL, not TypeMismatch)

### 1e. IS [NOT] NULL / IS [NOT] MISSING
- New unary operators in parser and evaluator
- `x IS NULL` → true if x is NULL, false otherwise
- `x IS MISSING` → true if x is MISSING

### 1f. Fix Mixed-Type Ordering
- Replace `unreachable!()` in ORDER BY with spec ordering
- NULL/MISSING sort last, type-based ordering for mixed types

**Milestone gate: "Foundation" — pause for review**

## Phase 2: Expressions

### 2a. LIKE Operator
- Pattern matching with `%` (any sequence) and `_` (single char) wildcards
- Optional `ESCAPE` clause
- Compile pattern to regex at evaluation time
- `NOT LIKE` as syntactic sugar

### 2b. BETWEEN Operator
- `x BETWEEN y AND z` → desugar to `x >= y AND x <= z` in logical planner
- `NOT BETWEEN` likewise

### 2c. IN Operator
- `x IN (a, b, c)` — membership test against literal list
- Right side parsed as array constructor
- NULL-aware per spec

### 2d. CAST Operator
- `CAST(x AS type)` for: INT↔FLOAT, STRING→INT/FLOAT, INT/FLOAT→STRING
- Parse as function-like expression with type argument

### 2e. String Concatenation (`||`)
- New binary operator at same precedence as addition
- NULL propagating

### 2f. COALESCE and NULLIF
- `COALESCE(a, b, c)` → first non-NULL/non-MISSING value
- `NULLIF(a, b)` → NULL if a=b, else a
- Desugar to CASE expressions in logical planner

### 2g. String Functions
- `UPPER`, `LOWER`, `SUBSTRING(str FROM pos [FOR len])`, `TRIM`, `CHAR_LENGTH`
- Added as built-in scalar functions alongside existing URL/host functions

### 2h. Extend date_part
- Add `hour`, `day`, `month`, `year` support (currently only second/minute)

**Milestone gate: "Expressions" — pause for review**

## Phase 3: Clauses & Query Structure

### 3a. SELECT VALUE
- Currently parsed but `unimplemented!()` in logical planner
- Returns raw value of a single expression rather than wrapping in tuple
- Create special projection node that outputs values directly

### 3b. DISTINCT
- `SELECT DISTINCT` eliminates duplicate rows
- New `DistinctStream` physical node with HashSet of seen records
- Requires hashing strategy for Record (serialize to canonical string or derive Hash for Value)

### 3c. Path Wildcards
- `x[*]` iterates all elements of an array
- `x.*` iterates all values of a tuple
- New PathExpr variants; evaluator returns array of matched values

### 3d. JOINs (CROSS JOIN and LEFT JOIN)
- CROSS JOIN: cartesian product via nested loop stream
- LEFT JOIN ... ON: tracks unmatched left rows, pads with NULLs
- New physical stream nodes

### 3e. LATERAL
- Allows FROM item to reference variables from preceding FROM items
- Implicit in PartiQL (unlike SQL)
- Pass current binding environment to subsequent FROM item evaluation

### 3f. Non-Correlated Subqueries
- Subqueries in FROM (derived tables)
- Subqueries in WHERE with IN/EXISTS
- Parse nested SELECT statements
- Execute inner query first, materialize result, use in outer query

**Milestone gate: "Clauses" — pause for review**

## Phase 4: Set Operations

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

Shared implementation: single `SetOperationStream` parameterized by operation type. Parser extended to recognize trailing set operation keywords after SFW queries.

**Milestone gate: "Complete" — final review**
