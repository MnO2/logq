# logq — PartiQL Query Engine for Server Logs

## Project Overview
logq is a Rust CLI tool implementing a PartiQL subset for ELB, ALB, S3, Squid,
JSONL, CLF, combined, and custom regex logs. The CLI also supports gzip input,
sharded tables, and a soft retained-state memory budget. Rust 1.85 is the MSRV.

## Architecture
- **Parser** (`src/syntax/`): nom-based parser producing AST nodes
- **Logical Planner** (`src/logical/`): AST → logical plan tree
- **Physical Executor** (`src/execution/`): Pull execution through `RecordStream` and typed `BatchStream` operators, with selective parallel scans and worker-local aggregation
- **Common Types** (`src/common/`): `Value` enum, type definitions shared across layers
- **Functions** (`src/functions/`): Scalar registry and bound function handles
- **Typed kernels** (`src/simd/`): Bitmaps, selection vectors, padded columns, numeric kernels

See `docs/architecture.md` for current pipeline selection, source ownership,
numeric representation, and memory behavior.

## Implementation Plan
Historical designs and phased plans live in `docs/plans/`. They explain prior
decisions but are not a current feature checklist. Use current code, regression
tests, `tests/conformance/`, and CHANGELOG.md to establish implemented behavior.

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
- Existing unit, integration, conformance, property, and benchmark-harness tests must not regress

## Verification

Run `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test --all-features`, and `cargo check --all-features` for the core CI
surface. Changes to benchmark scripts also require
`python3 -m unittest discover -s scripts/bench_e2e -p 'test_*.py'`.
Feature-gated probes and Criterion benchmarks require `--features bench-internals`.
See `fuzz/README.md` for the separate nightly parser-fuzz workflow.

## Execution invariants

- Preserve NULL/MISSING, field presence, numeric types, and output ordering across row/batch paths and thread settings.
- Runtime numbers remain Int32/Float32. Do not silently introduce different precision in optimized kernels or benchmark oracles.
- Required-field pruning reduces retained values; it must still validate ignored JSON values in consumed lines.
- LIMIT must remain demand-driven where later input or expression errors would otherwise become observable.
- Input files must remain immutable while mapped. Parallel queues and cancellation must remain bounded and must not hide earlier errors.
- `--max-memory` estimates retained execution state, not process RSS or output buffers. New materializing operators must share the query tracker.
- Validate benchmark answers before accepting timings. Preserve historical measurements with their original source/data/query identities; use a new result directory for a new run.

## Progress
Track in CHANGELOG.md
