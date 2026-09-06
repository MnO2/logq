# Performance next milestones — 2026-09-05

This implements the M1–M4 plan after `00b7d67`. The earlier raw-input and
ClickHouse measurements are in [the expansion report](performance-expansion-2026-09-05.md).
The scope is query CPU, ownership, complete input pipelines and measured reuse
experiments. No source data is converted automatically.

## Accepted changes

- **M1:** borrow nested values while resolving paths; move eligible passthrough
  columns in mixed projections; read only dependencies in scalar predicate
  fallback. Memory-budgeted projections retain their old allocation policy.
- **M2:** run eligible JSONL shards through shared workers that filter, project
  and aggregate locally; use the measured zlib-rs decoder backend. Regular
  single-gzip full aggregations can use a bounded decoder/parser pipeline.
  `--threads N` counts one decoder and N−1 parsers. Budgeted and one-thread
  gzip queries retain sequential execution. Non-regular inputs retain lazy
  processing, preventing a later FIFO from blocking an earlier error.
- **M3:** resolve actual function definitions once per bound expression and
  serialize borrowed output records without rebuilding a JSON object tree.
  A narrowly gated built-in Plus kernel writes Float32 columns directly while
  preserving every arithmetic step. Custom functions, integer/Mixed inputs,
  casts, branches and budgeted queries retain scalar expression evaluation.
  Diagnostic probes separate aggregation phases, expression evaluation,
  decoding, parsing and queue waits.
- **M4:** an explicit, opt-in experiment measures raw JSONEachRow, standard
  Parquet and persisted ClickHouse MergeTree, including conversion and actual
  1/10/100-query runs. This adds measurement tooling, not a native logq cache.

## Measurement contract

Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS; release builds, engine thread
settings 1 and auto. These are warm synthetic measurements. Each CLI workload
matrix has an independent complete answer oracle, checked before and after
timed execution. Phase probes have the narrower validation boundaries stated
in their sections. Paired CLI comparisons alternate binary order and include process
startup, execution and output. CPU time and separate RSS samples accompany
wall time. RSS includes resident mapped pages and is not an allocation count.
No representative timing overlaps builds or other benchmark processes.

The baseline binary is `/tmp/logq-next-20260905/logq-baseline-00b7d67`, SHA-256
`265085a5de8e9fd4d141affe9d7fabbf8e2682e8750e968515e7007c234c89e4`.
The final default-feature executable is `/tmp/logq-next-20260905/logq-final`,
SHA-256 `348e32a133bc819c7c53825f53c2eba36508ef12d467df114717ca433d5a9ebc`.
Its exact source hashes and compiler/build information are recorded in
`/tmp/logq-next-20260905/final-source-provenance.json`; all source hashes were
checked unchanged after building.
ClickHouse local is pinned to 26.4.4.38, SHA-256
`a6455d266ac5848cbb549569be8835eb82a59f3c25e681263246f9f2c7d30af4`.
Generated corpora, command arrays, full-result fingerprints, raw samples and
binary/data hashes remain under the ignored `scripts/bench_e2e/data/` and
`scripts/bench_e2e/results/` directories. Use a new result directory on rerun.

Public numeric behavior remains Int32/Float32: SUM/AVG accumulate in f64 and
produce f32. Oracles normalize to that representation; decimal spellings that
round to the same f32 are equivalent. Count overflow remains an error.

## Final CLI measurements

The 50K-row ownership/shard matrix uses narrow and wide JSONL with nested
payloads, UTF-8 and escaping. Concatenated uncompressed bytes and rows are
identical across each 1/8/32/125-file split; independent gzip members have
slightly different compressed totals. Five paired samples plus warmup:

| Query / input | One thread before → after ms | Auto before → after ms |
|---|---:|---:|
| Nested leaf, narrow | 39.87 → 28.19 | 40.11 → 29.40 |
| Direct aggregate, narrow control | 11.36 → 11.13 | 11.12 → 11.10 |
| 16 integer additions, narrow | 28.64 → 21.03 | 28.94 → 21.59 |
| Payload + expression Top-10, wide | 217.63 → 180.64 | 96.86 → 66.00 |
| 1% predicate + payload Top-10, wide | 168.17 → 135.93 | 46.25 → 29.83 |
| One gzip, count + sum | 179.40 → 146.49 | 177.50 → 40.15 |
| 8 gzip shards | 180.83 → 146.64 | 178.09 → 31.25 |
| 32 plain shards | 132.03 → 134.94 | 132.03 → 27.59 |
| 32 gzip shards | 176.93 → 146.39 | 180.16 → 30.30 |
| 125 plain shards | 136.35 → 137.77 | 136.25 → 26.39 |
| 125 gzip shards | 181.47 → 149.82 | 181.15 → 28.87 |

The narrow direct control resolved an early single-thread timing outlier: the
final paired run is unchanged. Wide direct aggregation is also essentially
flat (129.55 → 129.08 ms one-thread; 28.49 → 28.91 ms auto). Wide arithmetic
auto results remain within a few percent rather than showing a dependable
gain; parsing dominates those inputs. No global arithmetic speedup is claimed.

Parallelism trades CPU and working state for latency. One gzip uses 176 →
202 ms total CPU and 8.0 → 13.4 MiB RSS in auto mode while wall time falls
177.5 → 40.2 ms. For 125 plain shards, CPU rises 135 → 209 ms while wall time
falls 136.3 → 26.4 ms. Conversely, one-thread mixed projection reduces RSS
16.9 → 11.5 MiB. These are separate samples, not heap allocation measurements.

The Float32-specific control uses 500K rows with NULL/MISSING, a 64-byte
payload and an actual Float32 root. Seven paired samples distinguish function
binding from the subsequent typed kernel:

| Query | Baseline ms | Handles + other changes ms | Final typed kernel ms |
|---|---:|---:|---:|
| One addition, one thread | 68.09 | 57.68 | 46.52 |
| One addition, auto | 16.32 | 14.54 | 13.06 |
| 16 additions, one thread | 212.86 | 154.92 | 48.83 |
| 16 additions, auto | 37.96 | 30.51 | 14.07 |

The intermediate column uses the frozen `logq-gzip-native` executable before
the typed kernel. Its comparison with final isolates the kernel apart from
a planner enum boxing change. It is not a pure isolated handle comparison
with baseline. The 16-addition final one-thread CPU falls 211 → 47 ms, and
sample wall standard deviations are 3.39 ms before and 0.54 ms after.

A separate real gzip query includes a predicate, computed SUM/AVG input,
COUNT(column), COUNT(*) and nine groups. Seven paired runs improve 76.34 →
56.26 ms one-thread and 76.00 → 18.78 ms auto. Tiny gzip files of 1/1024/8192
rows add approximately 0.08–0.21 ms in auto mode, on a 3–4 ms CLI cost; no
compressed-size heuristic is introduced from those small absolute differences.

The 500K-row, 450,500,084-byte grouping control confirms that the output change
helps a full query, including writing all 500K result rows. One-thread latency
falls **598.32 ± 8.54 → 510.90 ± 3.37 ms**, with CPU 589 → 500 ms and roughly
308 MiB RSS in both. Auto falls 403.18 ± 11.58 → 338.92 ± 49.17 ms; its larger
variation makes the one-thread result the stronger evidence. Nine-group
controls remain 229.02 → 219.22 ms one-thread and 42.03 → 39.90 ms auto.

Final runtime gates passed: 1,017 default-feature tests, 1,025 all-feature
tests, all-target/all-feature Clippy with warnings denied, Rust 1.85 checks,
and all five Criterion smoke targets (57 cases). Python benchmark discovery
passes 45 tests. `cargo audit` and `cargo deny check advisories` pass after
updating the existing lru lockfile resolution from 0.18.1 to
0.18.2 for RUSTSEC-2026-0253; no unrelated dependency upgrade was performed.

### Original workload controls

The original 1 GiB JSONL five-query suite remains warm and answer-checked,
with five paired samples and the same query catalog:

| Auto query | Baseline ms | Final ms |
|---|---:|---:|
| COUNT | 139.20 | 136.38 |
| Selective status | 195.42 | 182.47 |
| Status groups | 195.60 | 194.38 |
| Latency Top-10 | 270.39 | 265.99 |
| User-agent LIKE | 204.55 | 195.25 |

These controls are mostly flat within variability; improvements target the
newly identified ownership, expression, shard and output workloads. The six
ELB controls remain within about 3.5% in auto and 1% in one-thread mode. No
fixed-format rewrite is inferred from JSONL gains.

The unchanged original ClickHouse commands use `file(..., JSONEachRow)` with
schema inference in each fresh process and measure 772–996 ms on this corpus.
That comparison cannot establish superiority over a running or prepared
ClickHouse engine. M4 therefore adds an explicit-schema raw control, prepared
storage and repeated-query sessions rather than using inferred raw CLI timing
as a proxy for the remaining architectural gap.

## Experiments that selected the implementation

### Decoder backend

The isolated comparison changes only the flate2 backend, keeping flate2 1.1.9.
On the 50K-row wide corpus, five paired runs after warmup gave:

| Phase | miniz ms | zlib-rs ms |
|---|---:|---:|
| Decode to sink | 57.45 | 42.38 |
| Plain JSON parse | 124.33 | 123.03 |
| Gzip JSON parse | 164.62 | 135.25 |
| One-thread count + sum CLI | 170.92 | 141.45 |

The plain-parse control is unchanged. These results support this backend on
this corpus; they do not establish a win for every compression distribution.
Both builds retain the existing first-member `GzDecoder` contract.

### Aggregation and output

The group probe uses 500K preparsed rows and sequential logical partitions.
It is a phase decomposition, not a parallel scaling measurement. At 9 groups,
local accumulation dominates. At 500K groups, output formatting dominates.
The paired 12-partition comparison was:

| Groups | Serialization before ms | Borrowed serialization ms |
|---|---:|---:|
| 100K | 60.43 | 43.23 |
| 500K | 293.95 | 208.76 |

Local accumulation, merge and finish remain approximately unchanged. The probe
formats through the actual CLI serializer into a counting sink. Its complete
group/value oracle runs outside the phase timers. A format-only reduction is
not a whole-query speedup; the final CLI controls above measure that separately.

### Expression specialization

On 500K preparsed nullable Float32 rows, the expression probe compares actual
bound evaluation, a resolved scalar function with the same Value staging,
and direct typed output construction. Five samples after warmup:

| Expression / active rows | Bound ms | Scalar handle ms | Typed builder ms |
|---|---:|---:|---:|
| One addition / 100% | 9.60 | 6.14 | 0.64 |
| 16 additions / 100% | 99.84 | 51.18 | 2.47 |
| 16 additions / 10% | 12.18 | 7.48 | 0.85 |

Each addition rounds to f32; constants are not folded into one addition.
Every output row is checked before and after timing, including infinity,
signed zero, values near 2^24, NULL/MISSING and inactive rows. The nullable
timing fixture masks its NaN input as MISSING; non-nullable unit tests also
check unmasked NaN. Input, binding, verification and output disposal are
excluded from these kernel timers.

### Scheduling decision

Two equal-byte 231,294,840-byte files contain the same 50K rows. Expensive
escaped strings are clustered in one file and dispersed in the other.
Keeping the mmap reader and aggregate fixed gives:

| Workers / task policy | Clustered ms | Dispersed ms |
|---|---:|---:|
| 1 / existing range | 148.69 | 150.69 |
| 2 / existing range | 130.15 | 89.94 |
| 4 / existing range | 76.50 | 46.62 |
| 12 / existing range | 31.13 | 26.90 |
| 12 / 1 MiB tasks | 29.61 | 24.89 |
| 12 / 256 KiB tasks | 28.66 | 25.76 |

Smaller tasks help this skew but the auto-thread gain is modest, while more
partial states can increase high-cardinality merge costs. Keep the default
plain-file aggregate partition policy. The configurable task-size and worker
wait measurements stay diagnostic. Worker busy time is wall time, not CPU.

## M4: preparation and actual query reuse

The same wide source has 219,877,780 bytes (209.69 MiB). One observed
preparation costs 0.947 s for Parquet and 0.803 s for persisted MergeTree.
Artifacts occupy 6,205,604 bytes (5.92 MiB) and 4,652,616 bytes (4.44 MiB),
including retained original JSON. This compressible synthetic corpus is not
a prediction for production compression ratios. An independent full Python
contract/oracle pass costs another 2.145 s.

Actual auto-thread COUNT + SUM sequences, **seconds including preparation
once** for prepared formats:

| Engine / execution | N=1 | N=10 | N=100 | N=100 plus contract pass |
|---|---:|---:|---:|---:|
| logq raw, fresh CLI each query | 0.029 | 0.292 | 3.162 | 3.162 |
| CH explicit-schema raw, fresh CLI | 0.264 | 2.756 | 26.710 | 26.710 |
| CH Parquet, fresh CLI | 1.092 | 2.479 | 16.015 | 18.160 |
| CH MergeTree, fresh CLI | 0.959 | 2.356 | 16.741 | 18.886 |
| CH explicit-schema raw, one process | 0.291 | 1.303 | 12.548 | 12.548 |
| CH Parquet, one process | 1.102 | 1.112 | 1.220 | 3.365 |
| CH MergeTree, one process | 0.960 | 0.969 | 1.037 | 3.182 |

Every N is an actual sequence with complete answers checked, not N times a
single best sample. There is one observed sequence per mode/N, plus warmup
and pre/post validation; preparation is also one observation. Do not infer
an exact, statistically stable break-even point from this table.

Prepared repeated reads have a clear opportunity: excluding the one-time
preparation, 100 narrow queries in a single process take about 0.273 s on
Parquet and 0.234 s on MergeTree. Fresh local invocations greatly reduce the
prepared-read advantage over CH raw and are slower than logq raw on this
workload. For the one-process prepared sequences, charging the current full
contract pass brings N=100 close to logq's raw CLI total. These
cross-engine/cross-lifecycle observations do not isolate a
native logq reader speedup or claim an equivalent logq session implementation.

The selective wide query returns payload and v for the highest ten rows after
the 1% predicate. Its N=100 fresh-CLI totals, including preparation, are
2.724 s for logq raw, 26.685 s for CH raw, 15.881 s for Parquet and 16.323 s
for MergeTree. This run does not add a wide-query single-process control, so
it does not isolate deferred payload costs from startup. The recorded
`EXPLAIN PLAN actions=1` for the persisted query shows `v >= 49500` in
PREWHERE, with one part and 32 granules. That confirms the selected plan;
physical read bytes, page skipping and a standalone lazy-materialization
speedup were not measured.

**Decision:** retain the reproducible opt-in experiment and pursue any future
prepared reader together with its source identity, numeric/dynamic type
contract and query lifecycle. Do not add a transparent cache, automatic
conversion or a Parquet dependency from a warm read-only kernel result alone.

## Representation and adoption boundaries

The reuse experiment requires present exact i32 `v` and `nested.metrics.v`,
and a present string `payload`. It retains original accepted JSON and separate
presence/raw-token fields for dynamic `mixed` values. Real-format fixtures
cover absent, null, boolean, string, float, object, 2^53+1, i64 minimum and u64
maximum, with full six-column round trips through both prepared formats.

Duplicate keys, nonfinite numbers and integers outside [-2^63, 2^64−1] anywhere
in the JSON are rejected. An actual ClickHouse parser control found that an
out-of-range integer in an otherwise unrelated field could invalidate selected
values. Keeping a raw fallback alone did not make extraction safe. Noncanonical
raw-token spelling can also fail closed. Preparation identities include source,
schema, helper/script and ClickHouse binary hashes; changed or foreign inputs
are rejected rather than silently reusing stale results.

Persisted data uses an Atomic database and MergeTree `ORDER BY tuple()`, so no
sort-key advantage is claimed. ClickHouse query caching is disabled. Fresh
CLI and N queries in one ClickHouse process are reported separately. Their
difference does not isolate startup exactly and is not a logq session result.
The conservative amortization additionally charges the full Python contract
and oracle pass once; that is not an estimate of a future native validator.

A transparent cache remains deferred: this fixed contract does not cover all
logq dynamic semantics. Cold cache, physical read reduction, larger-than-RAM
data and production distributions remain unmeasured. These are explicit limits
of the experiments, not evidence that those problems have been solved.

## Reproduction and decision record

See [the benchmark README](../scripts/bench_e2e/README.md) for runnable commands,
argument contracts and phase boundaries. Final artifacts used here:

| Directory under `scripts/bench_e2e/results/` | Purpose |
|---|---|
| `next-final` | Five-sample ownership/expression/shard matrix |
| `next-special-final` | Seven-sample Float32, tiny gzip and grouped gzip CLI controls; self-contained runner |
| `next-groups-final` | Five-sample 500K-row low/near-unique grouping |
| `next-original-controls` | Original JSONL/ELB and pinned CH commands; self-contained runner |
| `next-phase-initial` | Isolated decoder backends and initial group decomposition |
| `next-probe-controls` | Paired output phases and fixed-reader task-size/skew controls |
| `next-expression-probe` | Same-input bound/scalar/typed expression comparison |
| `next-reuse-w2048` | Prepared storage, count/narrow, actual repetition totals and CH session controls |
| `next-reuse-wide-w2048` | Selective payload queries and persisted EXPLAIN |

All six final CLI matrices have `metadata.status = complete`; their recorded
source and executable hashes were rechecked. Earlier intermediate runs are
used only for the explicitly labeled isolated experiments, not relabeled as
final-binary results. In particular, the initial gzip pipeline prototype is
not used for final performance claims; the native CLI controls exercise the
reviewed core with decoder initialization included.

Independent reviews covered filesystem/decompression boundaries, worker
shutdown/error composition, expression semantics, output formatting, benchmark
oracles and report attribution. The discovered FIFO hang, lost decoded prefix
on terminal gzip errors, helper-identity omission and unsafe opaque-number
projection contract were corrected and covered by regression tests. Competing
errors in different parallel chunks follow ordered chunk processing, not a
promise to reproduce every sequential batch's error identity. Existing lazy
LIMIT and sequential budget paths remain explicit compatibility boundaries.

Reverting runtime commits changes execution without rewriting input. Prepared
artifacts are created only in separately requested experiment directories.
No release, package publication or source-data migration is part of this work.
