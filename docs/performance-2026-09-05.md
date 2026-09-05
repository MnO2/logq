# Performance implementation and measurements — 2026-09-05

The raw JSONL workload now runs in **157–278 ms at 1 GiB**, compared with
**779–975 ms for ClickHouse local** on the same machine. This comparison covers
complete warm-cache CLI invocations over these generated files. It does not
compare persisted MergeTree tables, indexes, joins or a general analytical
workload.

Implementation: `4503e0989a3bc29131e7fdca42ba749271d30fe9`; final memory-lock
correction: `874153c`. Both are local commits. See the
[execution checklist](plans/2026-09-05-performance-execution.md).

## Measurement conditions

Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.1 arm64. Rust 1.97.0;
`cargo build --release --locked`, no custom RUSTFLAGS. ClickHouse local 26.4.4.38,
hyperfine 1.20.0. Main tables show five measured runs after one warmup; controls
use three measured runs after one warmup. All processes include startup, parsing,
execution and CSV formatting; the filesystem cache is warm.

Default logq resolves `--threads 0` to 12. ClickHouse uses its defaults in the main
tables. Thread controls are separate; setting thread limits to one does not
restrict every internal/background activity to one CPU.

The 1 GiB file contains 4,680,190 rows; the 100 MiB file contains 457,048 rows.
The synthetic JSONL rows have nine flat fields, nine status groups and five
user-agent values. Every timed command must exit successfully. An independent
bounded-memory oracle verifies all five answers before timing. A separate review
also checked the counts, groups, LIKE matches and deterministic TopK ties directly
from the generator's periodic construction.

## Final 1 GiB results

Mean ± sample standard deviation, milliseconds. The baseline used the old
implicit single-thread behavior; only its three measured queries are shown.

| Query | Baseline `8baa7fa` | Final logq | ClickHouse local | CH / logq |
| --- | ---: | ---: | ---: | ---: |
| Full-file count | 6,669.0 | 157.1 ± 10.5 | 778.5 ± 6.8 | 4.96× |
| Selective status filter | — | 213.9 ± 14.3 | 962.5 ± 2.2 | 4.50× |
| Group by status | 10,360.5 | 218.0 ± 11.3 | 970.0 ± 10.6 | 4.45× |
| Top-10 latency | — | 277.5 ± 10.0 | 970.0 ± 2.7 | 3.50× |
| User-agent substring | 20,716.6 | 219.2 ± 11.2 | 975.1 ± 11.5 | 4.45× |

Directly comparable baseline-to-final speedups: COUNT **42.5×**, GROUP BY **47.5×**, LIKE **94.5×**.
All five final default queries are 3.50–4.96× faster than ClickHouse local in this
workload. These are CLI wall-time ratios, not isolated operator speeds or an
engine-wide performance guarantee.

## Final 100 MiB results

| Query | logq, ms | ClickHouse local, ms |
| --- | ---: | ---: |
| Full-file count | 22.2 ± 1.1 | 244.7 ± 3.0 |
| Selective status filter | 29.1 ± 1.9 | 293.3 ± 2.0 |
| Group by status | 27.0 ± 1.6 | 292.0 ± 2.4 |
| Top-10 latency | 33.3 ± 1.3 | 293.5 ± 7.3 |
| User-agent substring | 28.1 ± 1.8 | 292.8 ± 3.5 |

Startup matters more at this size. Earlier samples, including a 100 MiB LIKE
outlier, remain in `phase1-4503e09-*`. The final tables use complete new five-run
samples after the memory fix, not selected best runs. Baseline TopK lacked the
new request-id tie-breaker, so its exact before/after speedup is omitted.

## Thread controls

1 GiB logq means, milliseconds, measured on core commit `4503e09` without a memory
ceiling. The subsequent fix skips unchanged memory reservations. This sweep keeps
its original provenance; the default tables above were rerun on `874153c`.

| Query | 1 thread | 2 threads | 4 threads | 8 threads | ClickHouse settings = 1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full-file count | 1,121.3 | 555.5 | 295.9 | 190.9 | 781.8 |
| Selective status filter | 1,344.5 | 717.6 | 434.6 | 234.4 | 1,515.0 |
| Group by status | 1,385.4 | 721.8 | 430.1 | 240.0 | 1,504.2 |
| Top-10 latency | 1,616.7 | 789.1 | 473.3 | 293.7 | 1,569.9 |
| User-agent substring | 1,408.5 | 700.8 | 378.1 | 221.3 | 1,693.3 |

With one-thread settings, COUNT remains about 43% slower than ClickHouse; TopK is
about 3% slower, while the other cases are 8–17% faster. Compared with the original
single-thread logq baseline, one-thread COUNT, GROUP BY and LIKE improved about
6×, 7.5× and 14.7×. Default gains combine less work per row with effective
parallel execution.

## Memory and fixed-format controls

`--max-memory` caps **estimated retained operator state**, including queued
batches. It does not cap process RSS, mapped file pages, all transient parser
allocations or OS caches. At 1 GiB, logq RSS was approximately 1,032–1,038 MiB;
at 100 MiB it was approximately 108–113 MiB. Resident mapped input accounts for
the file-size scaling. The original reader used about 8 MiB; `--threads 1` still
measured about 8 MiB. Bounded queues remove full-input heap accumulation, not mmap
residency.

All final JSONL queries passed with a 16 MiB operator-state ceiling:

| 1 GiB query | Default mean, ms | 16 MiB ceiling mean, ms |
| --- | ---: | ---: |
| Full-file count | 157.1 | 161.6 |
| Selective status filter | 213.9 | 217.2 |
| Group by status | 218.0 | 209.1 |
| Top-10 latency | 277.5 | 272.1 |
| User-agent substring | 219.2 | 218.6 |

Controls exposed one more bottleneck: fixed-size group accumulators locked the
shared memory counter even when their retained size did not change. Skipping
these no-op updates reduced 1 GiB ELB grouping with a ceiling from 485.1 ms to
132.5 ms.

ELB 1 GiB, three-run means, milliseconds:

| Query | Default | 16 MiB ceiling |
| --- | ---: | ---: |
| COUNT | 39.0 | 39.6 |
| SUM(sent_bytes) | 115.2 | 117.3 |
| GROUP BY status | 131.3 | 132.5 |
| GROUP BY status + WHERE true | 132.7 | 129.0 |
| 5-minute time buckets | 248.0 | 254.7 |

Redundant `WHERE true` preserves the fast path. The bucket workload includes
timestamp wraparound, so the optimized projection feeds hash grouping without
assuming sorted input. ELB COUNT/groups/buckets are checked exactly. SUM is checked
against an independent total at the existing f32 output precision. Public
Int32/Float32 representation was not migrated in this change.

## Implemented changes and remaining boundaries

- Strict direct JSON deserialization, root-field projection and typed batches;
  preserve ignored-value validation, nested values, NULL/MISSING and duplicate keys.
- Shared mmap, lazy per-file opening, 256 KiB ordered tasks, bounded queues and
  cancellation; worker-local Count/Sum/Avg states with f64 intermediate sums.
- Correct aggregate aliases/schemas, typed/Mixed keys, scope constants, array
  dependencies and three-valued predicates.
- Cached LIKE search state and masked dictionary evaluation; reusable tokenizer
  offsets and group-key buffers.
- Late TopK payload construction, move-based full sorting and stable numeric/null
  ordering; shared memory reservations that skip unchanged charges.
- Compiled time-bucket projection and memory-accounted grouping. Actual SQL
  literals and EXPLAIN now select the optimized path.
- Reliable error exits, answer-checked benchmarks, full SQL parsing and correct
  LIMIT cleanup timing.

Parallel partial aggregation covers Count/Sum/Avg. Heterogeneous MIN/MAX retain
sequential semantics. Complex projections, nested sorting, joins and unsupported
dynamic shapes conservatively retain row execution. Persisted columnar storage,
indexes, external spilling, wider public numeric types and general vectorized
expressions remain separate future work. These measurements do not establish
performance for high-cardinality groups, wide/nested JSON, gzip, large joins or
full sort.

## Verification and reproduction

Final `cargo test`: **954 tests passed**. The no-op memory-lock regression failed
before the fix and now passes. All-target/all-feature Clippy with `-D warnings`,
formatting, 10 Python harness tests and five Criterion smoke targets passed.
Cross-review found and resolved array dependencies, nested ordering, duplicate
aliases, constant projection, lazy shard opening and unordered time buckets.

```sh
cargo build --release --locked
CLICKHOUSE_BIN=/path/to/clickhouse-26.4.4.38 \
  python3 scripts/bench_e2e/benchmark.py --scale 1gb --tools logq clickhouse \
  --runs 5 --warmup 1 --results-dir scripts/bench_e2e/results/reproduction-1gb
# Repeat with --scale 100mb, or --threads 1/2/4/8 for thread controls.
```

Raw artifacts remain in ignored `scripts/bench_e2e/results/` directories:

- `audit-2026-09-05/`: baseline and original routing/RSS evidence.
- `phase1-4503e09-100mb/`, `phase1-4503e09-1gb/`: complete pre-memory-fix samples.
- `final-2026-09-05-100mb/`, `final-2026-09-05-1gb/`: final five-run comparisons,
  verification, query/command snapshots, RSS and metadata.
- `final-2026-09-05-controls/`: thread sweep and original budget slowdown.
- `final-2026-09-05-budget-controls/`: corrected budget and ELB controls. Both
  controls directories include their exact `run_controls.py` and raw hyperfine JSON.

Metadata records `build_command: null` because LOGQ_BIN supplied the prebuilt
binary. The actual build was `cargo build --release --locked`.

Final binary SHA-256: `e1492f8eb16727a9beb5fc9b88ec3aa1c4efc30822d76e80c7cb2e5d323d122b`.

1 GiB dataset SHA-256: `cc87df3720c3e5b7703874bd2181f34600a928d33bdd73cb223b2531385e4801`.
