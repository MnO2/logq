# End-to-end benchmarks

These benchmarks compare complete command-line invocations, including startup,
JSONL parsing, query execution, and result formatting. They are intended to make
performance work reproducible and to identify gaps, not to claim that the tools
have identical goals or execution models.

See [the 2026-09-05 implementation and measurements](performance-2026-09-05.md)
for current results, thread controls and memory tradeoffs. The results below are
the historical 0.2.0 baseline.

## Historical 0.2.0 results

Dataset: `jsonl-100mb.jsonl` (100.0 MiB, 457,048 rows). Host: Apple M4 Pro with
24 GiB RAM. OS: macOS 26.1 arm64. Date: 2026-07-11. Hyperfine: five measured runs
after one warmup run, using a warm filesystem cache.

| Query | logq | DuckDB | ClickHouse local | angle-grinder |
| --- | ---: | ---: | ---: | ---: |
| Full-file count | 671.0 ± 10.8 ms | 50.2 ± 0.8 ms | 245.7 ± 3.6 ms | 426.3 ± 8.1 ms |
| Selective status filter | 712.8 ± 5.1 ms | 52.2 ± 0.5 ms | 292.2 ± 0.9 ms | 509.6 ± 10.6 ms |
| Group by status | 1,017.4 ± 13.6 ms | 54.6 ± 1.7 ms | 298.0 ± 2.4 ms | 425.4 ± 9.5 ms |
| Top-10 latency | 848.3 ± 4.0 ms | 58.8 ± 8.5 ms | 295.4 ± 2.2 ms | — |
| User-agent substring | 1,981.7 ± 25.5 ms | 54.6 ± 0.4 ms | 299.7 ± 1.0 ms | 592.8 ± 4.4 ms |

`—` means the tool cannot express equivalent semantics. angle-grinder's
`limit` executes before `sort`, so it cannot produce the same bounded top-N
result. A full-sort substitute was deliberately not reported.

## Peak memory

Peak RSS is a separate single warm-cache run measured with `/usr/bin/time`.

| Query | logq | DuckDB | ClickHouse local | angle-grinder |
| --- | ---: | ---: | ---: | ---: |
| Full-file count | 7.7 MiB | 148.5 MiB | 303.9 MiB | 8.1 MiB |
| Selective status filter | 7.8 MiB | 152.8 MiB | 406.7 MiB | 8.0 MiB |
| Group by status | 7.8 MiB | 156.8 MiB | 412.6 MiB | 8.2 MiB |
| Top-10 latency | 7.8 MiB | 153.8 MiB | 409.7 MiB | — |
| User-agent substring | 9.0 MiB | 150.3 MiB | 408.4 MiB | 8.0 MiB |

logq's streaming paths are notably memory-efficient. The original top-10 run
used 233.3 MiB; after WS8's bounded-heap implementation, a repeat peak-RSS run
used 7.8 MiB while still considering the complete input.

## 1 GiB materialization ceilings

These single-process runs use the deterministic `jsonl-1gb.jsonl` corpus
(1,073,741,862 bytes, 4,680,190 rows) on the same host. Output was streamed to
`/dev/null`; peak RSS is `/usr/bin/time -l`'s maximum resident set size. The
queries deliberately stress high-cardinality state rather than the small
grouping keys in the comparison suite.

| Operation | Query shape | Peak RSS | Wall time |
| --- | --- | ---: | ---: |
| High-cardinality GROUP BY | `GROUP BY request_id` | 1,318.6 MiB | 14.75 s |
| Full ORDER BY | `ORDER BY latency DESC` (no limit) | 2,140.6 MiB | 10.28 s |
| DISTINCT | `SELECT DISTINCT request_id` | 1,485.4 MiB | 11.00 s |

The input SHA-256 was
`cc87df3720c3e5b7703874bd2181f34600a928d33bdd73cb223b2531385e4801`.
These measurements motivate the soft memory budget shipped in WS8: full
sort is the highest observed ceiling, while high-cardinality grouping and
deduplication also exceed the input size once hash-table and record overhead is
included.

## Reproduce

The generator uses fixed seed `20260711`. The exact JSONL input above has SHA-256
`39768e2134e7a47c838700ca84d3dba455960d51d0fdd206b3aeb4fc48971a4a`.
Generated data and raw hyperfine JSON are ignored by Git.

```sh
scripts/bench_e2e/gen_data.py
scripts/bench_e2e/run.sh --scale 100mb
```

The default generator creates 100 MB and 1 GB ELB, ALB, and JSONL files plus
deterministic gzip copies. The runner detects each competitor and skips missing
binaries. See [`scripts/bench_e2e/README.md`](../scripts/bench_e2e/README.md) for
installation, environment overrides, gzip runs, and quick smoke commands.

The five operations are defined in
[`scripts/bench_e2e/queries.json`](../scripts/bench_e2e/queries.json). Each tool
uses its native JSONL reader and idiomatic query syntax. Output is redirected so
terminal rendering is not measured, but producing the result is part of the
timed command.

Versions:

- logq: `logq 0.1.19`
- DuckDB: `v1.5.4 (Variegata) 08e34c447b`
- ClickHouse local: `26.4.4.38 (official build)`
- angle-grinder: `ag 0.19.5`
- hyperfine: `1.20.0`

## Known gaps

These results define the performance work for WS8:

1. **JSONL scan throughput:** even `count(*)` is 13.4× slower than DuckDB. Before
   changing operators, `--explain` needs to show whether the query used the
   batch or row pipeline and why any fallback occurred.
2. **String predicates:** the user-agent substring query is the largest gap at
   36.3× versus DuckDB and 3.3× versus angle-grinder. Profile JSON string
   extraction, LIKE compilation/cache use, and batch predicate coverage.
3. **Low-cardinality grouping:** grouping only nine status values is 18.6×
   slower than DuckDB despite using 7.8 MiB RSS. Determine whether parsing,
   hashing, or a batch-to-row boundary dominates before optimizing it.
4. **Bounded top-N:** resolved in WS8 with an O(N log K) heap. Peak RSS for the
   100 MiB top-10 query fell from 233.3 MiB to 7.8 MiB.

Pipeline-aware `explain` reports the same fallback for all five shared queries:
the dynamic `jsonl` datasource. There is no isolated filter, grouping, or sort
node to add to the batch pipeline—the fixed-schema versions of those operators
already run in batch mode. Dynamic JSON batching would first require schema
inference and typed JSON column construction, so WS8 does not add a speculative
operator-specific fast path.

## Limitations

- This is one synthetic dataset on one ARM laptop, not a universal ranking.
- Warm-cache numbers emphasize parsing and execution rather than storage speed.
- CLI startup is included. That particularly affects ClickHouse local at small
  file sizes, though the published file is large enough for scan work to
  dominate logq and DuckDB.
- JSONL is the shared format all four tools can query. The generator also makes
  ELB/ALB and gzip inputs, but those format-specific runs are not in the
  headline comparison.
