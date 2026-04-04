# logq — PartiQL Query Engine for Server Logs

## Project Overview
logq is a Rust CLI tool implementing PartiQL to query server log files (ELB, ALB, S3, Squid, JSONL). The goal is full PartiQL spec compliance (with pragmatic exclusions) using a bottom-up phased approach.

## Architecture
- **Parser** (`src/syntax/`): nom-based parser producing AST nodes
- **Logical Planner** (`src/logical/`): AST → logical plan tree
- **Physical Executor** (`src/execution/`): Stream-based pull execution with `RecordStream` trait
- **Common Types** (`src/common/`): `Value` enum, type definitions shared across layers

## Implementation Plan
See `docs/plans/2026-04-04-partiql-completion-design-final.md` for the full design.

Phases:
- **Phase 0**: Code cleanup (case-sensitivity, failure→anyhow, dedup, bug fixes)
- **Phase 1**: Foundation (NULL/MISSING propagation, float arithmetic, type coercion, IS NULL/MISSING, ordering, multi-branch CASE WHEN, parse_logic refactor)
- **Phase 2**: Expressions (LIKE, BETWEEN, IN, CAST, ||, COALESCE/NULLIF, string functions, date_part)
- **Phase 3**: Clauses (SELECT VALUE, DISTINCT, path wildcards, JOINs, LATERAL, subqueries)
- **Phase 4**: Set operations (UNION/INTERSECT/EXCEPT)

## Rules
- Run `cargo test` before every commit
- Never commit code that breaks existing passing tests
- Write tests BEFORE implementation (test-first)
- Commit after every meaningful unit of work
- Update CHANGELOG.md after completing each sub-phase item

## Out of Scope
PIVOT, Ion literals, bag literals (`<<>>`), correlated subqueries, window functions, schema-based type checking.

## Test Oracle
- Spec examples from PartiQL Specification PDF as ground truth
- Hand-crafted edge cases for log-domain behavior
- All existing ~58 tests must never regress

## Progress
Track in CHANGELOG.md
