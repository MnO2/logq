# Execution, reuse and capacity results — 2026-09-06

The [six-milestone plan](plans/2026-09-06-execution-milestones.md) produced three
production execution improvements, several SQL correctness repairs, and two
bounded architecture experiments. This report supersedes intermediate samples
for this work; the earlier September 5 and audit reports retain their original
binary and workload identities.

## Decisions

| Milestone | Result | Adoption |
| --- | --- | --- |
| M1 nested JSON projection | Retain requested object descendants; validate all consumed values | Enabled with conservative whole-value fallbacks |
| M2 batch output | Borrow batch columns through JSON/NDJSON serialization | Enabled; table/CSV retain their existing output paths |
| M3 typed arithmetic | Trusted Float32 column addition and constant multiplication | Enabled for supported pure expressions; preserve scalar/error/budget fallbacks |
| M4 lifecycle reuse | Actual fresh/reused plans in the same logq process | Diagnostic retained; no production session or transparent cache |
| M5 external sort | Fixed-schema sorted runs and bounded fan-in merge | Example retained; no production spill flag |
| M6 semantics and docs | HAVING, name binding, parser repairs, generated checks, numeric contract | Correctness repairs enabled; Int64/Float64 remains a compatibility proposal |

## Measurement identity and method

The final CLI comparison uses Rust 1.97.0 (`2d8144b78`), release mode, macOS 26.1,
ARM64, warm synthetic files, and seven alternating baseline/candidate pairs per
query/thread setting. Compilation and architecture experiments did not overlap
CLI timing. Each invocation's complete answer is checked against an independent
Python oracle using typed, ordered or bag digests; these checks cover every
returned value but hashes are not a mathematical proof of equality. Regression
tests separately compare concrete values and exact serialized bytes.

Both builds use `cargo build --release --locked --features bench-internals --bin
logq --examples`. The baseline is `b27ab090b1b8d2d0453134a358035fb06f221641`.
The candidate contains all runtime repairs described here, including the final
constant namespace, stdin alias and aggregate argument fixes.
Those runtime sources are committed in `b926318`; documentation and the portable
benchmark runners are recorded in the following commit.

| Artifact | SHA-256 |
| --- | --- |
| Baseline CLI | `f91e55f4d8060f8fbd361451f2ecdbf683c059607a4d7c808f860046fd935d09` |
| Final candidate CLI | `f93a6a8d43123262fdfa3af3691a4e407a6211fd4a05199ae285222fb27ea064` |
| Candidate source archive at build | `65f9cf7593b10b67a44ab08f73e9f0f4e5908206b3b2dd50495831230b37c707` |
| Width-32 input, 15,269,020 bytes | `5095c328496d279cd1be66b771ee09a0f68473b8d505f22ed1ae93fbab33f371` |
| Width-2048 input, 441,669,020 bytes | `706c472e72c1d5a69fefd1b0acf3bf437b6ffdaa2b70103bbe6964f7d2d6fe33` |

Both inputs contain 100,000 rows, an integer `v`, Float32-exact quarter/half-step
`f` and `g`, a root payload, and nested metrics plus an unused nested payload.
Widths refer to payload characters, not complete row bytes. Auto means
`--threads 0`; it permits parallel scanning but does not force small inputs
through worker queues. Timings include CLI execution and output, while answer
validation and separate RSS samples are outside the reported wall samples.

Raw artifacts are local to `/tmp/logq-milestones-20260906-iHBi9I/`: final CLI
results in `results-verified/` and `results-verified-wide/`, build/source identity
in `build-verified.json` and `source-verified.tar.gz`, and final architecture
results in `verified-architecture/architecture-results/`. These temporary files
are not repository dependencies. Archived scripts, exact argv, input manifests
and binary hashes make their measurement identity inspectable. Reproduction
commands are in the [benchmark README](../scripts/bench_e2e/README.md#execution-and-architecture-milestones-2026-09-06).

## M1–M3: full-query results

Times below are mean ± sample standard deviation in milliseconds; lower is
better. “Faster” is the reduction in elapsed time, not a throughput multiplier.

| Query / width | Threads | Baseline ms | Candidate ms | Faster |
| --- | ---: | ---: | ---: | ---: |
| `nested_w32` | auto | 54.10 ± 0.30 | 51.10 ± 0.69 | 5.6% |
| `direct_w32` | auto | 23.42 ± 0.34 | 23.31 ± 0.20 | 0.5% |
| `add_w32` | auto | 26.37 ± 0.78 | 24.67 ± 0.61 | 6.4% |
| `multiply_w32` | auto | 25.22 ± 0.38 | 23.43 ± 0.15 | 7.1% |
| `add16_w32` | auto | 44.32 ± 1.04 | 25.23 ± 0.18 | 43.1% |
| `multiply16_w32` | auto | 43.75 ± 0.46 | 24.65 ± 0.37 | 43.7% |
| `projection_w32` | auto | 45.89 ± 0.50 | 32.45 ± 0.38 | 29.3% |
| `groups_w32` | auto | 49.84 ± 0.55 | 37.78 ± 0.47 | 24.2% |
| `small_groups_w32` | auto | 24.52 ± 0.27 | 24.28 ± 0.18 | 1.0% |
| `nested_w2048` | 1 | 283.14 ± 1.67 | 270.69 ± 0.54 | 4.4% |
| `nested_w2048` | auto | 59.56 ± 1.30 | 51.03 ± 2.01 | 14.3% |
| `direct_w2048` | 1 | 243.26 ± 2.20 | 243.58 ± 1.13 | -0.1% |
| `direct_w2048` | auto | 48.19 ± 2.81 | 48.03 ± 2.23 | 0.3% |
| `add_w2048` | auto | 48.12 ± 2.30 | 47.60 ± 1.64 | 1.1% |
| `multiply_w2048` | auto | 50.66 ± 4.11 | 48.31 ± 2.06 | 4.6% |
| `add16_w2048` | 1 | 262.21 ± 3.04 | 242.55 ± 1.39 | 7.5% |
| `add16_w2048` | auto | 49.18 ± 2.68 | 47.35 ± 2.81 | 3.7% |
| `multiply16_w2048` | 1 | 263.81 ± 2.25 | 244.89 ± 2.32 | 7.2% |
| `multiply16_w2048` | auto | 49.47 ± 3.07 | 48.56 ± 1.60 | 1.8% |

Nested pruning improves the selected nested query while preserving full JSON
validation. The first implementation added a second root-map lookup per value;
the narrow direct-root control exposed that overhead. Indexing column masks
once per batch removed it. Final direct-root and small-output controls are
effectively unchanged. Whole-object references dominate narrower requests;
arrays, wildcard and unsupported scoped paths keep their conservative fallback.

Batch output improves full-result projection and high-cardinality grouping.
For the auto-thread projection, mean user+system CPU falls from 44.66 to
31.25 ms, with separate peak RSS samples of 7.97 and 7.88 MiB. Grouping CPU falls
from 48.21 to 36.19 ms, while retained aggregate state still dominates memory:
the RSS samples are 46.66 and 46.61 MiB. This is an output/CPU improvement,
not a claim that grouping needs less state.

Separately collected NDJSON sizes match between binaries: 5,688,890 bytes for
full projection, 1,788,890 bytes for 100,000 groups, and 2,284 bytes for the
128-group control (`output-sizes-verified.json`). The output improvements are
measured while returning the same amount of data.

The wide nested auto query uses 519.39 versus 452.10 ms total CPU across workers.
Its separate RSS samples are 465.41 versus 437.27 MiB, including mapped input
pages. Those process measurements do not represent allocated heap alone or the
`--max-memory` retained-state estimate.

Arithmetic kernels retain current Float32 rounding at every operation. A
16-step expression benefits much more than one operation on narrow input;
wide-input JSON scanning dilutes that gain. Integer expressions, custom function
overrides, unsupported shapes and budgeted materialization retain safe paths.
Preparsed kernel timings are diagnostic evidence only, not CLI speedups.


Preparsed probe: 500,000 rows, chain length 16, nullable values, five trials;
median milliseconds with setup, validation and disposal excluded. Every output
row is checked against independently rounded scalar operations.
For `add-multiply`, length 16 means 16 addition/multiplication pairs (32
arithmetic operations); the other modes perform 16 operations.

| Operation | Active rows | Bound expression ms | Registered scalar ms | Typed Float32 ms |
| --- | ---: | ---: | ---: | ---: |
| add-columns | 1% | 4.216 | 3.697 | 0.975 |
| add-columns | 100% | 104.791 | 57.372 | 5.145 |
| multiply-constant | 1% | 4.327 | 3.698 | 0.885 |
| multiply-constant | 100% | 103.083 | 55.143 | 4.596 |
| add-multiply | 1% | 5.165 | 4.259 | 0.987 |
| add-multiply | 100% | 202.186 | 108.851 | 7.615 |

## M4: same-engine lifecycle and prepared data

The feature-gated `query_lifecycle_probe` alternates fresh and reused physical
plans inside one process. Every execution reopens its sources, constructs new
streams/aggregate state, and validates the complete output. Both modes use the
same row serializer. Tests check changed-file visibility and recovery after
input errors and memory failures; stdin and nonregular files are rejected.

Actual 1/10/100-query sequences run three times each. Reused sequence totals
include one initial preparation; timings exclude result comparison and the
destruction of the prepared plan/output. The probe buffers answers, requires
immutable sources and deterministic result order, and has no hard output-size
cap. The count/prefix cases have small, independently known answers.

| Input / threads / query | Executions | Fresh sequence ms | Reused sequence ms |
| --- | ---: | ---: | ---: |
| width 32 / 1 / count | 1 | 17.120 | 17.340 |
| width 32 / 1 / count | 10 | 172.413 | 172.215 |
| width 32 / 1 / count | 100 | 1721.538 | 1713.022 |
| width 32 / 1 / prefix | 1 | 0.044 | 0.073 |
| width 32 / 1 / prefix | 10 | 0.328 | 0.241 |
| width 32 / 1 / prefix | 100 | 2.813 | 1.839 |
| width 2048 / auto / count | 1 | 41.933 | 43.245 |
| width 2048 / auto / count | 10 | 427.310 | 430.556 |
| width 2048 / auto / count | 100 | 4165.612 | 4175.915 |
| width 2048 / auto / prefix | 1 | 0.056 | 0.091 |
| width 2048 / auto / prefix | 10 | 0.385 | 0.299 |
| width 2048 / auto / prefix | 100 | 3.279 | 2.278 |

Full scans gain little from plan reuse. Prefix queries save microseconds per
query, with millisecond-scale savings only after many executions. This evidence
does not justify introducing a public session lifecycle, invalidation protocol
or result cache. Retain the diagnostic so a future workload can establish a
material benefit.

The prior [prepared-data experiment](performance-next-milestones-2026-09-05.md#representation-and-adoption-boundaries)
already charges conversion, validation and storage under a restricted schema.
Its ClickHouse/Parquet results do not measure a native logq reader. A future
native pilot should use explicit schema/presence metadata, versioned numeric
contracts, source and query identities, and the standard Arrow reader before
considering a custom decoder. Arrow's `RowFilter` supports projected predicate
decoding and late materialization, but predicate cost and page selectivity still
matter; these are workload experiments, not automatic speedups.
([Arrow reader documentation](https://arrow.apache.org/rust/parquet/arrow/arrow_reader/struct.RowFilter.html))
No Parquet dependency or automatic conversion is added by this milestone.

## M5: external-sort capacity experiment

The standalone `external_sort_probe` accepts exactly an Int32 `key` and a string
`payload`, records original sequence numbers, and stably sorts by key/sequence.
It rejects duplicate/unknown fields and other value shapes. Run size, fan-in,
record size, run count and live temporary-disk bytes have explicit bounds;
intermediate input and output runs count simultaneously during merges.
Temporary files clean up on ordinary error/drop, and published output refuses
to overwrite an existing path. This is a separate example, not a query operator.

The 100,000-row source is 62,627,830 bytes. Every row of all nine external outputs
and the successful logq full sort is compared with an independent SQLite oracle,
including original order for equal keys. The example's normal internal FNV bag
fingerprints alone are probabilistic; complete-output checking supplies the
stronger validation for this experiment.

| Run target MiB | Initial runs | Merge passes | Peak estimated state MiB | Peak live temp disk MiB | Median sort ms | Median process wall ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 61 | 2 | 1.025 | 114.44 | 268.63 | 415.87 |
| 4 | 16 | 2 | 4.018 | 114.44 | 261.09 | 405.58 |
| 16 | 4 | 1 | 16.017 | 114.44 | 208.64 | 366.39 |

The existing in-memory query fails at 16 and 64 MiB `--max-memory` and succeeds
at 256 MiB (147.72 ms in one full-output observation). External sorting demonstrates completion under smaller retained-run
budgets; it is slower than sorting in memory. Estimated run/merge state can
slightly exceed a run target, so that target must not be described as an RSS cap.
The sort timer includes parsing, run sorting and temporary I/O; it excludes final
JSON output. Full-process wall includes output and the normal validation pass;
the external SQLite comparison is outside timing.

Production adoption requires a lossless codec for every `Value` (including
MISSING versus NULL, numeric types and nonfinite values), integration with the
shared query memory tracker, stable sorting across operators, cancellation and
error semantics, and a user-visible temporary-disk policy. Aggregate, DISTINCT
and JOIN spilling need independent algorithms and acceptance tests. This
fixed-schema proof is retained without adding a partially supported `--spill`.

## M6: correctness and executable documentation

- HAVING aggregate calls bind to selected or private aggregate outputs; hidden
  outputs disappear before DISTINCT, ORDER BY and LIMIT. Unsupported global
  forms and nested aggregates fail clearly instead of losing selected fields.
- Aggregate input projections preserve GROUP BY aliases and bind nested, array
  and explicitly named arguments to their actual columns. Collision-free private
  slots preserve expression evaluation positions and public result names.
- SQL constants are direct expression values rather than synthetic source-field
  variables. JSON fields such as `const_000000000` no longer change literals or
  disappear from required-field projection. stdin now applies FROM aliases.
- Duplicate aliases use their last value consistently in projection, full sort
  and Top-N; Top-N no longer reconstructs short columns and panics. Trailing
  whitespace after IN/postfix/CAST expressions no longer blocks later clauses.
- Generated cross-path fixtures compare stdin, file, concatenated gzip and mixed
  shards at one/four threads with independent answers. Boundary fixtures pin
  current Int32/Float32 behavior; [the migration contract](numeric-migration.md)
  specifies a future wider runtime without changing precision in these kernels.
- The conformance index is generated from 61 executable cases. Run
  `python3 scripts/render_conformance.py`; CI uses `--check` to reject drift.
  Supported alias, IN/ORDER and HAVING cases replace their stale skips. README
  also explains type-sensitive numeric equality and current HAVING boundaries.

## Verification and next acceptance gates

Final verification passed: 1,099 all-features Rust tests, five external-sort
example tests, formatting, strict all-target/all-features Clippy, all-features
and Rust 1.85 all-target checks, 58 Python benchmark tests, and generated
conformance-document validation. All commands use the locked dependency graph
where applicable; the exact gate commands are in the milestone plan.

Next work should be driven by a representative workload: measure a native
prepared-data pilot before a public session API; implement a lossless value codec
and shared resource accounting before a production external-sort operator; and
execute the complete numeric compatibility matrix before changing runtime widths.
Per-operator EXPLAIN ANALYZE also needs a tested accounting contract for parallel
overlap, output time and retained state. The current probes expose bounded phase
timings without presenting their sum as a reliable operator wall-time breakdown.
