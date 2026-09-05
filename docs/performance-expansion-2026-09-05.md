# Performance expansion — 2026-09-05

This milestone tests the next set of performance hypotheses against the already
optimized `0d21af7` baseline. It implements direct JSON column construction and
bound batch expressions, fixes computed aggregate inputs and cardinality
narrowing, and retains experiments that can be reproduced. The
[execution plan](plans/2026-09-05-performance-expansion.md) lists acceptance
criteria and the subsequent architecture work.

## Changes and correctness boundaries

Homogeneous JSON scalars now go directly into primitive columns. Nested objects,
arrays and genuinely mixed types retain dynamic values. Parsing still validates
ignored fields, escaped names and numeric ranges, and preserves the last value
of duplicate keys. Large overwritten string arenas are reclaimed before a batch
is queued; SIMD padding must not double an already large allocation.

Supported JSONL projections bind column positions once, reuse function argument
storage and feed computed values into worker-local aggregates. This is a bound
scalar expression evaluator within batches, not a general SIMD or JIT compiler.
The old simple-column move path remains. Arithmetic aggregate inputs previously
read fields missing from their own projected input: for example `SUM(x + 1)`
could return NULL. Separate expression occurrences keep separate projected
values, including volatile functions.

CASE/COALESCE branches remain lazy and inactive rows are not evaluated. New
expression projections below a prefix LIMIT retain demand-driven row execution,
including Filter and DISTINCT, so an unused malformed tail cannot turn an
otherwise successful query into a failure. Blocking GROUP BY / ORDER BY still
allow batch execution. General expression maps on fixed log formats retain their
existing row execution; pre-existing scan/filter and specialized batch paths
remain available. This avoids expanding reader differences in quoted strings and
invalid numbers to new query shapes. Subquery projections retain row execution.
When separate filter/projection stages both fail on different rows, batching can
still change which error is reported first. Custom/stateful UDF call order across
workers is not guaranteed; use
`--threads 1` when sequential calls are needed.

COUNT, COUNT(column) and approximate cardinality now report an explicit error
outside `0..=2147483647`, including the sequential fixed-format COUNT fast path.
This prevents silent narrowing; it does not increase supported result width.

## Numeric migration contract

Public `Value` and primitive columns still use i32/f32. Large JSON integers can
therefore lose precision when converted to f32, and SUM/AVG output
still rounds to f32 even though their internal state uses f64. Wider types remain
a separate compatibility milestone with these gates:

1. Adopt exact i64 parsing/storage/output for in-range integers, and define a
   deliberate error or decimal policy for integers beyond i64. Do not silently
   route large integer keys through floating point.
2. Align literals, JSON/fixed readers, CAST, arithmetic, functions, typed/Mixed
   columns, aggregate states, ordering, serialization and the public Rust API.
3. Define mixed integer/float equality and coercion explicitly. Equal keys must
   hash identically across GROUP BY, DISTINCT, joins and approximate distinct;
   adjacent large integers must remain distinct. Include signed zero, NaN,
   infinities, NULL/MISSING and values around 2^24, 2^31, 2^53 and 2^63.
4. Specify checked arithmetic/CAST overflow and aggregate output types, then
   run row/batch and single/parallel differential tests before performance tests.
   Measure extra column/hashtable bandwidth and memory per group as part of the
   migration, and document the public API compatibility change.

## Measurement conditions

Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.1 arm64; Rust 1.97.0,
`cargo build --release --locked --bin logq`, no custom RUSTFLAGS. Complete CLI
measurements include startup, parsing, execution and output. All are warm-cache
synthetic data; the oracle reads the complete corpus before timing. They do not
establish cold-storage or production-workload performance.

The expansion harness checks complete answers against an independent bounded
SQLite oracle before the matrix is timed, and validates each timed output after
the timer stops. It records a row count and SHA-256 sums/squared sums, plus an
ordered hash for sort queries. These are probabilistic fingerprints, not a
bytewise proof. Floating output is normalized to the existing public f32
precision. Historical wrong answers are recorded as untimed correctness failures,
never as fast measurements. CPU time includes user and system child time. Peak
RSS is a separate sample and includes mapped file pages; it is not retained heap
or the `--max-memory` operator-state estimate.

The default corpus has 100,000 rows, 9 low-cardinality groups and 10,000 high
groups, uniform/skewed keys, repeated/unique short and 384-byte strings, optional
values, and separate wide/nested shapes. The scale control uses 500,000 rows and
500,000 distinct high keys. CLI `--threads 1` and auto select different input
backends as well as worker counts, so they are not isolated scheduler tests.

## Final measurements

Mean ± sample standard deviation, milliseconds; five samples after one warmup.

| Case | Baseline, 1 | Final, 1 | Baseline, auto | Final, auto |
| --- | ---: | ---: | ---: | ---: |
| group_low | 50.54 ± 0.71 | 45.69 ± 0.22 | 11.42 ± 0.20 | 11.58 ± 0.44 |
| group_high | 57.96 ± 0.49 | 53.20 ± 0.25 | 25.93 ± 0.78 | 25.37 ± 0.49 |
| group_skew | 58.01 ± 0.44 | 52.71 ± 0.43 | 19.02 ± 0.59 | 18.86 ± 0.70 |
| string_short_repeated | 47.78 ± 0.29 | 43.63 ± 0.50 | 11.36 ± 0.34 | 11.48 ± 0.52 |
| string_short_unique | 52.00 ± 0.54 | 47.75 ± 0.29 | 12.07 ± 0.53 | 11.83 ± 0.45 |
| string_long_repeated | 53.26 ± 0.15 | 50.55 ± 8.70 | 13.44 ± 0.53 | 12.06 ± 0.68 |
| string_long_unique | 100.51 ± 1.02 | 93.96 ± 1.96 | 20.74 ± 0.47 | 19.57 ± 0.81 |
| expression_direct | 49.26 ± 0.30 | 44.95 ± 0.42 | 11.50 ± 0.48 | 11.48 ± 0.18 |
| expression_arithmetic | wrong answer | 49.43 ± 0.32 | wrong answer | 12.01 ± 0.38 |
| expression_case | wrong answer | 49.56 ± 0.57 | wrong answer | 12.25 ± 0.50 |
| shape_wide | 181.55 ± 0.87 | 173.23 ± 5.06 | 31.83 ± 1.81 | 31.33 ± 2.07 |
| shape_nested | 248.85 ± 1.92 | 247.32 ± 0.67 | 253.16 ± 3.22 | 47.21 ± 2.34 |
| top10 | 54.25 ± 0.50 | 49.71 ± 1.06 | 11.96 ± 0.13 | 11.91 ± 0.26 |
| top1000 | 58.17 ± 0.70 | 54.30 ± 0.53 | 16.31 ± 0.41 | 16.46 ± 0.33 |
| fullsort | 94.63 ± 8.07 | 86.10 ± 0.70 | 49.19 ± 0.31 | 49.13 ± 0.31 |
| scan_shards | 49.41 ± 0.40 | 45.36 ± 0.18 | 49.97 ± 0.61 | 46.53 ± 0.67 |
| scan_gzip | 66.64 ± 1.04 | 65.93 ± 1.10 | 67.01 ± 1.02 | 67.05 ± 3.11 |


All 34 final case/thread combinations passed. Four historical combinations
(arithmetic and CASE, each at 1/auto) failed correctness and have no timed samples
or speedup ratio. Most ordinary single-thread cases improve about 6–10%; default
parallel cases are largely unchanged. Small increases for low-cardinality groups,
short repeated strings, Top1000 and gzip are below their sample variability. The
long-repeated single-thread candidate and baseline full-sort sample have high
variation; their mean improvements alone are not strong evidence.

Nested auto improves **5.36×**, but complete-child CPU rises from 251.80 to
388.16 ms (+54.2%), and RSS rises from 7.91 to 228.66 MiB. EXPLAIN changes from row
to batch; mapped input and concurrent batches explain why lower latency is not
also lower total CPU or RSS. Nested single-thread performance is effectively
unchanged. Long repeated/unique LIKE auto latency improves 10.2%/5.7%, with CPU
reductions of 17.6%/14.1% on these generated strings.


## Scale, output and file scheduling controls

500,000 rows, 500,000 distinct high keys; base input 450,500,084 bytes.
All 36 case/thread/binary combinations below passed complete answer checks.
`expression_direct`, shards and gzip execute the same logical query and rows.

| Case | Baseline, 1 | Final, 1 | Baseline, auto | Final, auto |
| --- | ---: | ---: | ---: | ---: |
| group_low | 249.26 ± 0.49 | 223.45 ± 4.11 | 42.32 ± 2.57 | 42.93 ± 5.25 |
| group_high | 598.31 ± 7.05 | 577.08 ± 4.34 | 375.04 ± 2.95 | 375.38 ± 3.23 |
| group_skew | 282.18 ± 1.48 | 257.99 ± 2.16 | 76.24 ± 1.91 | 72.91 ± 0.97 |
| shape_nested | 1263.39 ± 10.64 | 1270.25 ± 33.73 | 1278.08 ± 14.01 | 179.37 ± 9.57 |
| top10 | 259.57 ± 1.64 | 236.57 ± 1.11 | 42.98 ± 0.32 | 42.82 ± 1.28 |
| fullsort | 463.59 ± 1.12 | 445.60 ± 12.27 | 248.79 ± 1.32 | 249.42 ± 1.95 |
| expression_direct | 226.01 ± 1.31 | 204.26 ± 0.72 | 39.39 ± 3.22 | 37.34 ± 1.55 |
| scan_shards | 230.68 ± 1.36 | 212.07 ± 1.39 | 234.19 ± 1.46 | 216.17 ± 3.95 |
| scan_gzip | 320.72 ± 1.34 | 317.69 ± 1.78 | 318.21 ± 1.93 | 318.01 ± 11.44 |

Nested auto improves 7.13× at this scale. CPU rises from 1,265.69 to 1,687.97 ms,
and RSS from 7.89 to 1,047.52 MiB, largely consistent with making the roughly
1 GiB wide input resident through mmap. These measurements do not isolate heap.

Unique grouping remains 375 ms at auto, versus 577 ms at one thread; final RSS
is 749.45/307.94 MiB respectively. This query also emits 500,000 result rows,
so its wall time cannot identify hash/merge as the bottleneck by itself. The next
experiment must separate local grouping, merge/finalization and formatting.
Full sort also remains effectively unchanged at auto (249 ms).

The equivalent plain/shards/gzip query takes **37.34 / 216.17 / 318.01 ms** at
auto. Shards use 125 files of roughly 3.6 MB each; gzip is a single compressed
stream. The latter two are still close to their single-thread times. This is
stronger evidence for investigating file/decompression scheduling than the
single-large-file results alone. It does not prove that a single gzip stream can
be decoded in arbitrary parallel byte ranges.

## Same-kernel and dictionary experiments

The probe keeps scanner/filter code identical across buffered and mapped reader
backends, with no scan-worker scheduling. Its timer excludes process startup,
file open, buffer construction and mmap creation, but includes scanner/reader
teardown. Complete-child CPU has a wider boundary. The probe validates row and
match counts, not selected values or matched-row identities; the SQL matrix and
regressions provide the broader correctness checks. Allocation counters run in a
separate invocation and are not included in timing means.

Final mapped scan means ± SD, milliseconds:

| Selected strings | Baseline | Direct columns | Direct + dictionary |
| --- | ---: | ---: | ---: |
| short repeated | 37.81 ± 0.26 | 38.84 ± 0.24 | 39.04 ± 0.42 |
| short unique | 38.10 ± 0.27 | 38.10 ± 0.36 | 38.45 ± 0.25 |
| long repeated | 41.54 ± 0.25 | 38.78 ± 0.21 | 41.30 ± 0.41 |
| long unique | 41.32 ± 0.34 | 39.24 ± 0.28 | 38.86 ± 0.21 |

Direct long-string scanning improves about 5–7%; short unique strings are flat,
while short repeated strings are **2.7% slower in this isolated kernel**. The
complete CLI matrix does not show a stable short-string regression. This is a
workload tradeoff, not a universal parser speedup.

For long repeated strings, final candidate scan times with 8 KiB / 64 KiB /
1 MiB buffers are **43.48 / 40.37 / 39.79 ms**, versus 38.78 ms mapped. This
supports the 64 KiB sequential JSON buffer without treating one-thread versus
auto CLI runs as a pure scheduling experiment. It says nothing about cold I/O.

Long-string scan allocation calls fall **100,889 → 1,084 (98.9%)**, but cumulative
requested bytes rise **80,463,795 → 96,319,139 (+19.7%)**, because arena growth
requests are counted in full on every reallocation. These are not live bytes or
peak heap. Short repeated requested bytes fall 4,263,795 → 1,063,971. Fewer calls,
fewer requested bytes and lower runtime are separate outcomes.

Dictionary construction makes the long-repeated scan about 6.5% slower than
candidate-off, while LIKE is approximately unchanged (41.73 ms off / 41.62 ms
on). An additional seven-run complete CLI on/off comparison likewise gives no
stable overall win: long-repeated auto is 13.71 ± 0.51 ms on versus
13.96 ± 0.54 ms off; long-unique is 21.39 ± 1.26 versus 20.97 ± 0.77 ms.
**JSON dictionary encoding stays off by default**, with explicit benchmark/test
hooks retained. A future implementation should test direct dictionary building
or repeated consumers that can amortize construction, with unique-string
controls; these results do not establish that dictionaries are never useful.


## Original-suite and ClickHouse controls

The original five-query suite was rerun on both 100 MiB and 1 GiB inputs, plus
six ELB queries at 100 MiB, with complete answer checking and five samples.
ClickHouse is the same pinned **local 26.4.4.38** executable used in the previous
report. These compare complete raw-JSONL CLI invocations, not persistent
MergeTree tables, indexes, a running server or a general analytical workload.

1 GiB, default logq auto / ClickHouse defaults, milliseconds:

| Query | Baseline logq | Final logq | ClickHouse local |
| --- | ---: | ---: | ---: |
| full_count | 146.83 ± 2.83 | 141.93 ± 4.53 | 752.33 ± 8.08 |
| selective_filter | 201.68 ± 2.77 | 196.76 ± 16.12 | 968.94 ± 7.42 |
| group_by_status | 216.33 ± 7.82 | 200.43 ± 11.59 | 982.64 ± 8.24 |
| top_latency | 259.63 ± 4.68 | 253.91 ± 11.95 | 990.39 ± 6.39 |
| user_agent_like | 209.86 ± 13.32 | 206.54 ± 16.91 | 992.57 ± 7.15 |

The final raw-file CLI remains 3.90–5.30× faster than ClickHouse local in this
narrow default-settings comparison. Several incremental baseline/final
differences are small relative to variability, so they are not evidence for a
broad new default-engine speedup. COUNT illustrates the resource tradeoff: final
logq uses 1,371.44 ms total CPU versus ClickHouse's 1,026.03 ms despite its much
shorter wall time. Mapped logq RSS is about 1,032–1,037 MiB.

1 GiB with logq `--threads 1` and ClickHouse
`--max_threads 1 --max_parsing_threads 1`, milliseconds:

| Query | Baseline logq | Final logq | ClickHouse local |
| --- | ---: | ---: | ---: |
| full_count | 1163.40 ± 6.85 | 1113.77 ± 14.95 | 793.82 ± 11.92 |
| selective_filter | 1419.48 ± 11.49 | 1317.77 ± 1.83 | 1507.70 ± 6.88 |
| group_by_status | 1456.27 ± 7.95 | 1379.29 ± 29.13 | 1535.56 ± 27.97 |
| top_latency | 1661.95 ± 8.07 | 1558.91 ± 11.13 | 1574.87 ± 8.98 |
| user_agent_like | 1469.53 ± 7.94 | 1367.99 ± 14.84 | 1728.76 ± 35.55 |

Final logq improves 4.3–7.2% versus its baseline in this one-thread-settings
control. COUNT still takes **40.3% longer wall time than ClickHouse**; its total
CPU is 1,106.00 versus 1,015.67 ms, a smaller **8.9% gap**. These flags limit
engine workers, not all process/background activity or CPU affinity. Thus the
wall-time ratio cannot be described as a pure one-core parser comparison.
Selective filter, grouping and LIKE have shorter final logq times; TopK is close.
The remaining COUNT gap motivates a zero-selected-column scanner/validation
profile, with buffered/mapped backends held constant, before choosing another
parser optimization.

At 100 MiB, the original JSONL single-thread queries improve 4.6–7.4%. Default
COUNT is 21.96 ± 1.76 → 22.79 ± 1.92 ms, an increase within variability; all
other default means are lower. ELB controls, milliseconds:

| ELB query | Baseline, 1 | Final, 1 | Baseline, auto | Final, auto |
| --- | ---: | ---: | ---: | ---: |
| count | 22.26 ± 0.30 | 22.21 ± 0.22 | 7.71 ± 0.11 | 7.54 ± 0.12 |
| sum | 86.23 ± 0.89 | 86.11 ± 0.92 | 17.86 ± 0.85 | 16.98 ± 0.66 |
| group | 96.96 ± 0.30 | 97.37 ± 0.77 | 18.82 ± 1.06 | 17.83 ± 0.50 |
| group-where-true | 101.94 ± 7.41 | 97.23 ± 0.78 | 17.72 ± 0.66 | 18.82 ± 1.23 |
| bucket | 113.81 ± 0.63 | 112.34 ± 0.53 | 28.80 ± 0.21 | 28.77 ± 0.18 |
| like | 87.46 ± 0.44 | 87.07 ± 0.68 | 18.85 ± 0.43 | 17.48 ± 1.12 |

The largest adverse ELB default mean is redundant-WHERE grouping (+1.10 ms),
smaller than the final sample SD (1.23 ms), with virtually unchanged total CPU.
There is no established broad fixed-format regression in these controls.

## Next implementation sequence

The follow-on experiments are specified in the
[plan](plans/2026-09-05-performance-expansion.md), with advancement gates rather
than assumed speedups. The evidence now supports this order:

1. **File and decompression scheduling**, alongside a COUNT-only profile.
   Compare identical plain/sharded/gzip rows; separate decompression from JSON
   validation, then test bounded producer/worker queues. Keep input order,
   descriptor limits, cancellation and malformed/CRC error behavior. A single
   gzip stream needs ordered decompression; parallel parsing is a separate step.
   For COUNT, use the existing zero-selected-field probe and hold the reader
   backend constant before trying faster validation/tokenization.
2. **High-cardinality state and finalization**. Measure local grouping, merge,
   finalization and output separately at fixed row counts; include integer,
   string and mixed keys, skew and DISTINCT. Specialize keys or partition merges
   only where CPU/bytes per group dominate. Align this work with the numeric
   contract above so equality/hashing are not redesigned twice.
3. **Columnar reuse feasibility**. Compare preparation plus 1/10/100 repeated
   queries over raw files and a standard columnar representation. Include storage
   size, invalidation, skipped bytes and a persisted ClickHouse comparison.
   Wide/nested parsing and the remaining validation cost make this worth testing;
   warm raw-file results do not establish its break-even point.
4. **Worker-local TopK and bounded-memory execution** according to profiles and
   workload need. Current narrow Top10 is already close to low-group scan time;
   test wide payloads before adding worker heaps. For full sort/near-unique state,
   establish precise batch lifetime accounting before external sorted runs or
   partition spilling, with error/cancellation cleanup.
5. **Wider public numerics**, as a separate compatibility change using the
   contract above. The current checked error is the completed correctness fix;
   i64/f64 storage, coercion and exact key semantics are not yet implemented.

These are subsequent implementation milestones, not claims that storage,
spilling, a new scheduler or full numeric migration shipped in this change.

## Verification and reproduction

Implementation commit: `3f24c6173b8d4e228d19ddc10667e83fd1cec838`.
Final default-feature CLI SHA-256:
`265085a5de8e9fd4d141affe9d7fabbf8e2682e8750e968515e7007c234c89e4`.
Baseline `0d21af7` CLI SHA-256:
`e1492f8eb16727a9beb5fc9b88ec3aa1c4efc30822d76e80c7cb2e5d323d122b`.
The final build uses `cargo build --release --locked --bin logq`; the saved
baseline was built with `cargo build --release --locked`. Both use Rust 1.97.0,
default features and no custom RUSTFLAGS. Binaries are saved separately so later
builds cannot overwrite them.
Externally supplied binary build fields in the matrix metadata remain unknown;
these are the actual build commands executed for this report.

- Default Rust suite: **982 passed**; all features: **983 passed**, 28 more than
  the corresponding baseline suites.
- Rustfmt, all-target/all-feature Clippy with warnings denied, and all-target/
  all-feature Rust 1.85 checks passed.
- All five Criterion targets passed smoke execution, including direct JSON
  reader and dictionary-control cases.
- **30 Python tests passed**, including answer rejection, timeout/descendant
  cleanup, failed-run reporting and post-run input/binary provenance checks.
- Failing-before/passing-after regressions cover computed aggregate inputs,
  volatile occurrences, prefix-LIMIT behavior, COUNT narrowing, discarded huge
  duplicate strings and large-string padding. Independent reviews also checked
  strict JSON parsing, aliases/masks, fixed-format boundaries and measurement
  validity. No new production dependency was introduced.

Example reproduction after saving an isolated baseline release build:

```sh
cargo build --release --locked --bin logq
python3 scripts/bench_e2e/explore.py \
  --binary baseline=/path/to/logq-0d21af7 \
  --binary candidate=target/release/logq --allow-invalid baseline \
  --threads 1 0 --runs 5 --warmup 1 \
  --data-dir scripts/bench_e2e/data/explore-v1 \
  --results-dir scripts/bench_e2e/results/reproduction-expansion
```

For scale controls, use `--rows 500000 --groups 500000 --shard-rows 4000`, a new
corpus/results directory, and select the nine scale cases shown above. Probe
build/reproduction commands and measurement boundaries are documented in the
[benchmark README](../scripts/bench_e2e/README.md#json-scanner-and-allocation-probes).

Raw artifacts remain in ignored `scripts/bench_e2e/results/` directories:

- `expansion-final/`: complete 17-case matrix, oracle fingerprints, verification
  status, EXPLAIN, per-sample timings, RSS and source/query/data/binary hashes.
  Full outputs are checked in temporary files and are not retained.
- `expansion-final-scale/` and `expansion-final-scale-plain/`: larger controlled
  cases and the additional equivalent plain-file query.
- `expansion-final-probes/`: final same-kernel timing/allocation runs and protocol
  checks, source snapshots, exact baseline instrumentation patch and source
  hashes. The baseline source was isolated with `git archive 0d21af7`; only
  benchmark exports and the example wrapper were added. The full build command
  is recorded in metadata. Final probe source is the implementation commit.
- `expansion-final-controls/` and `expansion-final-one-thread/`: original JSONL,
  ELB and pinned ClickHouse controls, exact `run_controls.py`, CPU/RSS samples
  and metadata. Every measured invocation's output was independently checked.
- `expansion-dictionary-choice/`: seven-run on/off CLI control. These intermediate
  binaries precede the final buffer/retention changes; both use the original
  8 KiB sequential buffer. The on build also predates the prefix-LIMIT fix,
  which none of these full-scan aggregate queries uses.
- `expansion-dictionary-on/` and `expansion-probes-initial/run/`: retained early
  experiments with their original provenance, not relabeled as final runs.

No cold-cache, larger-than-RAM, persisted-columnar or production-corpus claim is
made. `--max-memory` still limits estimated retained operator state, not process
RSS, mapped input, all transient allocations or OS caches.
