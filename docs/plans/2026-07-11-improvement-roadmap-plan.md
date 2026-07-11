# logq Improvement Roadmap — Execution Plan

**Date:** 2026-07-11
**Status:** Approved for execution
**Audience:** This document is written for agents executing workstreams independently. Each workstream is self-contained: read the Context section plus your workstream, and you have everything needed. Do not assume knowledge of the conversation that produced this plan.

---

## Context

logq is a Rust CLI (`src/main.rs`, clap-based) that queries server log files (ELB, ALB, S3, Squid, JSONL) with PartiQL. Architecture:

- **Parser** (`src/syntax/parser.rs`, ~2500 lines, nom 7): SQL text → AST
- **Logical planner** (`src/logical/`): AST → logical plan (+ `optimizer.rs`)
- **Physical execution** (`src/execution/`): two pipelines —
  - Row pipeline: pull-based `RecordStream` operators in `src/execution/types.rs`
  - Batch pipeline: columnar operators (`batch_*.rs` files), entered via `try_build_batch_pipeline` in `src/execution/types.rs` (~line 925). Falls back silently to the row pipeline for unsupported plan shapes.
- **SIMD helpers** (`src/simd/`), parallel mmap scan (`src/execution/parallel.rs`)
- **Functions/UDFs** (`src/functions/`), registry-based

State as of this writing:

- All PartiQL implementation phases (0–4) complete; see `CHANGELOG.md`.
- **789 unit tests + 1 integration test, all passing** (`cargo test`). This is the regression baseline: no workstream may break it.
- Heavy performance work already done (batch pipeline, dictionary encoding, CompactString, hash join). Criterion benches in `benches/`, most gated behind `--features bench-internals`.
- INNER and RIGHT JOIN syntax/execution already landed in `0de9aa3`, and `time_bucket` already exists for long-form second/minute/hour intervals. Their WS7 items are validation/documentation and targeted completion work, not greenfield implementations.
- Known feature gaps: no correlated subqueries or window functions (both are **out of scope** — do not implement).

### Ground rules (apply to every workstream)

1. Run `cargo test` before every commit. Never commit with failing tests.
2. Test-first for behavior changes: write the failing test, then the implementation.
3. Commit after each meaningful unit of work; update `CHANGELOG.md` when a workstream item completes.
4. Do not start a workstream marked as depending on an unfinished one.
5. If you discover this document is wrong about the code (line numbers drift, an assumption doesn't hold), trust the code, fix the task's approach, and note the discrepancy in your commit message.

### Suggested execution order

| Order | Workstream | Size | Depends on |
|---|---|---|---|
| 1 | WS1: Clippy cleanup + CI modernization | S | — |
| 2 | WS2: Gzip input + multi-file/glob tables | M | — |
| 3 | WS3: Dependency modernization (clap 4, serde_json, edition) | M–L | WS1 (green CI first) |
| 4 | WS4: Parser fuzzing + conformance testing | M | WS1 |
| 5 | WS5: Competitor benchmark suite | M | WS2 (test on .gz inputs) |
| 6 | WS6: Error message quality | M | WS3 (parser untouched by WS3, but land after churn) |
| 7 | WS7: Feature reach (JOIN validation/docs, custom regex format, time-bucket completion, ndjson) | M–L | WS3 |
| 8 | WS8: Memory ceilings + batch-pipeline coverage | L | WS5 (benchmarks reveal targets) |
| 9 | WS9: Release engineering | S–M | WS1–WS8 |

WS1 and WS2 are independent and can run in parallel. Everything else should land on top of a green, modernized CI.

---

## WS1 — Clippy cleanup + CI modernization

**Why:** `cargo clippy --all-targets` currently emits **5 errors** (`clippy::approx_constant`, hardcoded approximations of PI) and ~150 warnings (57× `useless_vec`, 18× explicit auto-deref, 14× `needless_return`, 11× `redundant_closure`, dead code including an unused `datatype` associated function, etc.). CI (`.github/workflows/CI.yml`) uses the archived `actions-rs/*` actions and `actions/checkout@v2` (EOL). The README carries dead Travis and codecov badges.

**Tasks:**

1. Fix all clippy errors and warnings across `src/`, `benches/`, `tests/`. Mechanical; do not change behavior. For the PI approximations, use `std::f64::consts::PI` (or the appropriate constant). For genuinely-intentional patterns, prefer restructuring over `#[allow]`; use `#[allow]` with a one-line justification comment only as a last resort.
2. Delete truly dead code flagged by clippy (e.g. never-used associated functions) rather than allowing it — check git blame first; if it looks like API surface intended for the library (`src/lib.rs` exports), keep and `#[allow(dead_code)]` with justification.
3. Rewrite `.github/workflows/CI.yml`:
   - `actions/checkout@v4`, `dtolnay/rust-toolchain@stable`, `Swatinem/rust-cache@v2`.
   - Jobs: `fmt` (`cargo fmt --check`), `clippy` (`cargo clippy --all-targets -- -D warnings`), `test` on ubuntu/macos/windows matrix, `check --all-features`.
4. Remove `.travis.yml`, the `[badges]` section in `Cargo.toml`, and the Travis/codecov badges in `README.md`. Replace with a GitHub Actions badge.

**Done when:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` all pass locally and in CI on all three OSes.

---

## WS2 — Gzip input + multi-file/glob tables

**Why:** Real AWS ELB/ALB/S3 access logs arrive gzipped and sharded across many files. Today users must decompress and concatenate manually, and piping via stdin loses the mmap parallel-scan fast path. This is the highest-value practical feature in the plan.

**Where:** Table specs are parsed from `--table it:elb=access.log` style args (see `src/main.rs` / `src/app.rs`); file readers and `LogFormat` dispatch live in `src/execution/datasource.rs` (format strings matched around line 831); the parallel mmap scan strategy is `src/execution/parallel.rs` (`choose_strategy`, used from `src/execution/types.rs` ~line 966).

**Tasks:**

1. **Gzip:** Add `flate2` (rust-backend is fine; consider `zlib-ng` feature for speed). Detect by magic bytes (`0x1f 0x8b`), not extension, so renamed files work — but also accept `.gz` extension as a hint. Wrap the reader transparently for every format. Gzip streams cannot be mmap'd — they take the sequential reader path; make sure `choose_strategy` handles this without panicking.
2. **Multi-file tables:** Accept a glob pattern or comma-separated list in the table spec (`--table it:alb=logs/*.gz`). Use the `glob` crate. Expand at table-registration time; the table scans files in deterministic (sorted) order, concatenated. Empty glob match is an error naming the pattern.
3. **Parallelism across files:** When multiple plain (non-gz) files back one table, each file can still use the existing mmap parallel scan. Cross-file parallelism (rayon over files) is a stretch goal — only if it doesn't complicate the limit/early-termination logic in `parallel_scan_chunks_limited`.
4. **Tests:** Integration tests in `tests/` using `tempfile` + `flate2` to write small gzipped fixtures: gz single file, gz+plain mixed glob, empty-glob error, magic-byte detection with wrong extension, stdin unchanged. Unit tests for spec parsing of globs/lists.
5. Document in `README.md` (Quick Start + a short "Compressed and sharded logs" section).

**Done when:** `logq query 'select count(*) from it' --table it:alb=fixtures/*.gz` works end-to-end; all new tests pass; README updated.

---

## WS3 — Dependency modernization

**Why:** Stale, some risky: clap 2.33 with the `yaml` feature (removed in clap 3+), edition 2018, `json` 0.12 (unmaintained), ahash 0.7 (old, had DoS-hardening fixes since), hashbrown 0.11, criterion 0.3, thiserror 1, ordered-float 2.8. `cargo audit` will likely flag several.

**Tasks (each is its own commit; run full tests between each):**

1. **clap 2 → 4** with derive API. The CLI is defined via `cli.yml` (yaml feature) — port to a `#[derive(Parser)]` struct. Preserve exact flag names, subcommand names (`query`, etc.), and help semantics. Grep for `load_yaml`/`cli.yml` and delete the yaml file after porting. Verify `logq query --help` output covers the same options; keep the version string sourced from `Cargo.toml` (`crate_version`-equivalent) so the 0.1.19 mismatch bug fixed in Phase 0 can't recur.
2. **`json` 0.12 → `serde_json`** (with `preserve_order` feature if JSONL field ordering matters to output — check `src/execution/datasource.rs` JSONL parsing and the JSON output writer before deciding). This touches value conversion code; add round-trip tests (JSONL in → query → JSON out) first.
3. **Edition 2018 → 2021** (then 2024 if MSRV policy allows — pick and document an MSRV in README + CI). `cargo fix --edition` then manual cleanup.
4. **Version bumps:** hashbrown, ahash (verify hash-key behavior — ahash 0.8 changed defaults; the codebase uses it for hasher state, not persisted hashes, so it should be safe — verify no hash values are stored across runs), ordered-float, thiserror 2, criterion 0.5 (bench API changed: `Benchmark`/`ParameterizedBenchmark` removed in favor of `benchmark_group` — the benches likely need mechanical porting), lru, pdatastructs/tdigest if newer exists.
5. **Add `cargo audit`/`cargo deny` to CI** (advisories only; don't gate on licenses without the owner's input).

**Done when:** `cargo audit` clean (or documented exceptions), all tests pass, benches compile and run (`cargo bench --features bench-internals -- --test`), CLI help/behavior verified by running the Quick Start commands from README against files in `data/`.

---

## WS4 — Parser fuzzing + conformance testing

**Why:** A hand-written nom SQL parser is a prime panic target (a precedence-table panic on uppercase input was already found and fixed in Phase 0). Correctness currently rests on hand-transcribed spec examples; the official machine-readable [partiql-tests](https://github.com/partiql/partiql-tests) suite exists.

**Tasks:**

1. **Fuzzing:** `cargo fuzz init`; target `logq::...::parse_query` (find the public parse entry point in `src/syntax/parser.rs` / `src/lib.rs`). Seed the corpus with every SQL string appearing in unit tests (extract with a small script into `fuzz/corpus/`). The target asserts "no panic" (Result::Err is fine). Run locally ≥1 CPU-hour; fix every panic found (each fix gets a regression unit test). Add a CI job that runs the fuzzer for ~5 minutes per PR (smoke, not exhaustive) — keep it non-blocking initially.
2. **Property tests:** Add `proptest` as dev-dependency. Properties worth encoding (all in existing semantics, see `src/common/types.rs` and `Formula::evaluate` in execution):
   - Value comparison is antisymmetric and transitive across Int/Float coercion.
   - Three-valued logic: `NOT(unknown) = unknown`, De Morgan holds for the `Option<bool>` evaluator.
   - NULL/MISSING propagate through every binary arithmetic op.
   - `ORDER BY` NULL placement invariant (NULL last ASC, first DESC — Phase 1 Step 12).
3. **Conformance harness:** Vendor or git-submodule a *subset* of partiql-tests (the evaluation tests for features logq supports; skip Ion/bag-literal/window tests — out of scope per `CLAUDE.md`). Write a test harness in `tests/conformance.rs` that reads the test data files, runs each query against the given environment, and compares results. Start with a passing subset and a skip-list file with reasons; the skip list shrinking over time is the metric. If the data format is too Ion-centric to consume cheaply, fall back to hand-porting the ~50 most relevant cases and note that in the harness header.

**Done when:** fuzzer runs clean for 1 CPU-hour, ≥4 property tests merged, conformance harness runs in `cargo test` with an explicit skip list.

---

## WS5 — Competitor benchmark suite

**Why:** All current numbers are self-relative (see `docs/perf-analysis.md`). Publishing comparisons against established tools both finds real weaknesses and is the strongest README asset a tool like this can have.

**Tasks:**

1. **Dataset:** Script (`scripts/bench_e2e/gen_data.sh` or a Rust helper reusing `benches/helpers/synthetic.rs`) that generates reproducible synthetic ELB/ALB and JSONL files at 100MB and 1GB scales, plus gzipped variants. Seed fixed. Do not commit the data; commit the generator.
2. **Competitors:** `duckdb` (read_csv/read_json over the same files), `clickhouse local`, `angle-grinder`. Document install steps; skip gracefully if a competitor binary is absent.
3. **Queries (≥5):** full-file count, filtered count (selective predicate), GROUP BY status code + count, top-10 by latency (ORDER BY + LIMIT), a string-heavy query (LIKE on user_agent). Express each idiomatically per tool.
4. **Harness:** `hyperfine` driven by a script (`scripts/bench_e2e/run.sh`), JSON export, plus a small formatter that emits a Markdown table. Measure wall time and peak RSS (`/usr/bin/time -l` on macOS, `-v` on Linux).
5. **Publish:** results table + methodology in `docs/benchmarks.md`, headline table in README with a link. Include logq version, competitor versions, hardware, and date.
6. **Analysis:** for each query where logq loses badly, file a short note in `docs/benchmarks.md` under "Known gaps" — this feeds WS8.

**Done when:** `scripts/bench_e2e/run.sh` reproduces the table from a clean checkout (given competitor binaries), and README shows the results.

---

## WS6 — Error message quality

**Why:** nom's default errors surface as unhelpful internals. Users typo queries constantly; the error is the UX.

**Tasks:**

1. Audit current failure modes: collect 15–20 representative broken queries (typo'd keyword, unbalanced parens, bad function name, missing FROM, type errors at runtime) and snapshot current output into a test file first (`tests/error_messages.rs`, using expected-substring assertions, not exact matches).
2. Add span tracking: switch the parser input to `nom_locate::LocatedSpan` or track offsets at the statement level, so errors can point at a byte offset in the query.
3. Render with a caret diagnostic (`ariadne` or hand-rolled — hand-rolled is fine and dependency-free: query line, `^~~~` underline, one-line hint). Route through the existing anyhow error chain in `src/main.rs`/`src/app.rs`.
4. Runtime errors (unknown column, unknown function, type mismatch) should name the offending identifier and, for unknown column/function, suggest the nearest match (simple Levenshtein over known names — column names come from the table schema in `src/execution/log_schema.rs`, functions from `src/functions/registry.rs`).
5. Update the snapshot tests to assert the new, better messages.

**Done when:** the 15–20 case corpus produces messages that name the location and likely fix; no test regressions.

---

## WS7 — Feature reach

Independent sub-items; can be split across agents. Each follows test-first.

### 7a. INNER/RIGHT JOIN validation and documentation (S)
INNER and RIGHT JOIN support already exists (`0de9aa3`): the parser accepts `INNER JOIN`, bare `JOIN`, and `RIGHT [OUTER] JOIN`, and the planner maps them to the hash-join machinery. Audit end-to-end semantics (matched/unmatched rows, NULL keys, aliases, residual predicates), add any missing integration tests, and document both join forms in README and CHANGELOG. Fix only gaps found by those tests.

### 7b. User-defined regex format (M)
New table spec form: `--table it:regex=access.log --format-file fmt.toml` (or inline `regex:PATTERN` — pick the ergonomics that fits the existing spec parser in `src/app.rs`). A format definition = a regex with named capture groups; group names become column names, all typed Varchar unless a `types` map says otherwise (int/float/datetime with a chrono format string). Implement as a new `LogFormat` variant in `src/execution/datasource.rs` that reuses the existing regex-based reader machinery (ELB/ALB already work this way — model on them). This permanently ends per-format hardcoding requests. Ship one worked example in README: nginx combined log format.

### 7c. Built-in nginx/apache combined format (S, after 7b)
Add `clf` and `combined` as built-in formats implemented *as* predefined regex-format definitions on top of 7b, proving the mechanism.

### 7d. time_bucket completion (S–M)
`time_bucket` already exists in `src/functions/datetime.rs` for long-form second/minute/hour intervals and already has a batch streaming-groupby fast path. Extend interval parsing to accept ergonomic `s`/`m`/`h`/`d` shorthand such as `5m`, add day bucketing, and preserve existing long-form inputs. Test scalar boundaries plus grouped queries through both batch and row/fallback paths.

### 7e. ndjson output (S)
`--output ndjson`: one JSON object per row, no wrapping array. Trivial once WS3's serde_json migration lands; add alongside the existing json/csv writers in the output layer (grep `--output` handling in `src/app.rs`).

**Done when (each):** parser/planner/execution tests + one end-to-end integration test + README section.

---

## WS8 — Memory ceilings + batch-pipeline coverage

**Why:** GROUP BY / ORDER BY / DISTINCT / INTERSECT / EXCEPT all materialize in RAM — a multi-GB input can OOM. Separately, the batch fast path's coverage is unknown: queries silently fall back to the slower row pipeline.

**Tasks:**

1. **Observability first:** extend the existing `explain` subcommand to print which pipeline (batch vs row) would be chosen and, on fallback, *which plan node* caused it (instrument `try_build_batch_pipeline`'s failure returns in `src/execution/types.rs`). Do not add a competing `--explain` flag. This is immediately useful for WS5's analysis.
2. **Top-N optimization:** verify whether `ORDER BY x LIMIT k` already uses a bounded heap (check `src/execution/batch_orderby.rs` and the row-pipeline sort in `src/execution/prefix_sort.rs`); if not, implement it — it's the most common log query shape and caps memory at O(k).
3. **Measure ceilings:** using WS5's 1GB dataset, record peak RSS for GROUP BY high-cardinality key, full ORDER BY, and DISTINCT. Document in `docs/benchmarks.md`.
4. **Graceful degradation:** pick ONE of (a) external merge sort spill for ORDER BY, or (b) a soft memory budget that aborts with a clear "query exceeded memory budget (--max-memory)" error instead of OOM. Option (b) is far cheaper and acceptable — decide based on WS5 findings and effort budget; do not attempt spill-to-disk for hash aggregation (out of proportion for a CLI tool).
5. **Batch coverage expansion:** from the `--explain` data over WS5's query set, pick the top 1–2 fallback causes and add batch support only if the row-pipeline cost shows up in benchmarks. Do not expand batch coverage speculatively.

**Done when:** pipeline-aware `explain` is merged; top-N verified/implemented with a memory test; ceilings documented; one degradation mechanism shipped.

---

## WS9 — Release engineering

**Why:** Version 0.1.19 predates the entire PartiQL completion and performance effort. None of it is in users' hands.

**Tasks:**

1. Write a user-facing changelog entry (the current `CHANGELOG.md` is developer-log style — add a `## 0.2.0` section summarizing user-visible changes: full PartiQL subset, new functions, perf numbers, new formats/flags from WS2/WS7).
2. Bump to 0.2.0. Verify `cargo publish --dry-run` (note `exclude` in Cargo.toml already trims benches/data).
3. Set up `cargo-dist` (or a hand-rolled release workflow) building binaries for mac (arm64/x86_64), linux (x86_64, musl preferred for portability), windows, attached to GitHub Releases on tag push.
4. Optional: Homebrew tap formula (cargo-dist can generate one).
5. Only after WS1–WS8 are complete and the full release gate is green: tag and release. Announce section in README ("Install" gains binary-download instructions).

**Done when:** `cargo install logq` gets 0.2.0 and a GitHub Release carries binaries for the three platforms.

---

## Out of scope (do not implement, even if adjacent)

- Correlated subqueries, window functions, PIVOT, Ion literals, bag literals (`<<>>`) — per `CLAUDE.md`.
- Schema-based type checking.
- Spill-to-disk hash aggregation (WS8 explicitly excludes it).
- Rewriting the parser away from nom.

## Reporting

When you finish a workstream: update `CHANGELOG.md`, check the box in the table below (edit this file), and note anything you discovered that changes a later workstream's assumptions.

- [x] WS1 Clippy + CI
- [x] WS2 Gzip + globs
- [x] WS3 Dependency modernization
- [ ] WS4 Fuzzing + conformance
- [ ] WS5 Competitor benchmarks
- [ ] WS6 Error messages
- [ ] WS7a INNER JOIN
- [ ] WS7b Custom regex format
- [ ] WS7c nginx/apache formats
- [ ] WS7d time_bucket
- [ ] WS7e ndjson output
- [ ] WS8 Memory + batch coverage
- [ ] WS9 Release 0.2.0
