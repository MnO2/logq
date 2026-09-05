# End-to-end benchmark suite

This suite compares logq, DuckDB, ClickHouse local, and angle-grinder on the
same deterministic JSONL data. It also generates ELB and ALB data so logq's
native readers and gzip path can be measured separately. Generated data and raw
results are ignored by Git.

## Prerequisites

- [hyperfine](https://github.com/sharkdp/hyperfine) drives repeated wall-time
  measurements.
- [DuckDB CLI](https://duckdb.org/docs/stable/clients/cli/overview) is detected
  as `duckdb`.
- [ClickHouse local](https://clickhouse.com/resources/engineering/run-sql-on-json-file)
  is detected as `clickhouse` or `clickhouse-local`.
- [angle-grinder](https://github.com/rcoh/angle-grinder) is detected as
  `agrind`.

On macOS, Homebrew packages are available for hyperfine, DuckDB, and
angle-grinder:

```sh
brew install hyperfine duckdb angle-grinder
```

ClickHouse's current installation flow uses its official CLI:

```sh
curl https://clickhouse.com/cli | sh
clickhousectl local use latest
```

On other platforms, follow the linked upstream instructions. A missing
competitor is reported and skipped; hyperfine is required. `LOGQ_BIN`,
`HYPERFINE_BIN`, `DUCKDB_BIN`, `CLICKHOUSE_BIN`, and `AGRIND_BIN` can point to
binaries outside `PATH`.

## Generate data

The default generator writes 100 MB and 1 GB ELB, ALB, and JSONL files plus
deterministic gzip copies. The fixed seed, exact byte sizes, row counts, and
SHA-256 digests are recorded in `data/manifest.json`.

```sh
scripts/bench_e2e/gen_data.py
```

For a quick harness check:

```sh
scripts/bench_e2e/gen_data.py --sizes 1mb
scripts/bench_e2e/run.sh --scale 1mb --runs 2 --warmup 0
```

## Run and format

The published suite uses warm filesystem caches, one warmup, and five measured
runs per command:

```sh
scripts/bench_e2e/run.sh --scale 100mb
```

Add `--gzip` to compare compressed JSONL. Each query gets a hyperfine JSON file,
and a separate `/usr/bin/time` run records peak RSS. The final Markdown fragment
is written to `results/table.md`, or to the directory selected with
`--results-dir`. `--dry-run` prints commands only and never formats old results.

Before timing, the runner reads the corpus once with Python and checks every
tool's answers against independent counts, status groups, and top-10 rows.
Incorrect answers, parse errors, and failed gzip readers stop the run. This
untimed validation also warms the input cache. Validation expects the scalar
fields produced by this generator; it is not a cross-engine NULL/MISSING or
malformed-JSON conformance suite.

Use explicit thread limits and separate result directories for comparisons:

```sh
scripts/bench_e2e/run.sh --scale 100mb --tools logq clickhouse --threads 1 --results-dir /tmp/logq-bench-t1
scripts/bench_e2e/run.sh --scale 1gb --tools logq clickhouse --threads 4 --results-dir /tmp/logq-bench-t4
```

Omitting `--threads` measures each tool's defaults. A positive value sets
logq's `--threads`, DuckDB's `threads`, and ClickHouse's `max_threads` plus
`max_parsing_threads`. These are engine thread settings, not OS CPU affinity;
angle-grinder has no matching setting. Metadata records the requested limit,
logical CPU count, git revision and working-tree status, rustc/build command,
binary/data/query SHA-256 values, and tool versions. A supplied `LOGQ_BIN` is
hashed but its build options are unknown. Queries and verified output are saved
with each run so later catalog changes do not alter the result report.

All tools scan the same JSONL file and use idiomatic syntax for full count,
selective filter, status-code grouping, top-10 latency, and a user-agent
substring filter. angle-grinder is marked unsupported for top-N: its `limit`
operator executes before `sort`, so it cannot express the same bounded result.
The harness does not substitute a full-sort benchmark because that would
measure materially different output and resource behavior.

Top-10 orders by latency descending and request ID ascending to resolve ties
deterministically. This adds a secondary key relative to the historical suite;
recorded query hashes distinguish the workloads. Latency output is compared
with a `1e-6` relative / `1e-7` absolute tolerance for logq's Float32 values;
request IDs, counts, and group membership must match exactly.

The harness runs on macOS/Linux with `/bin/sh`; compressed angle-grinder input
also uses `/bin/bash -o pipefail`. Its tests need no competitor installations:

```sh
python3 -m unittest discover -s scripts/bench_e2e -p 'test_*.py'
cargo bench --features bench-internals --bench bench_parser --bench bench_execution --bench bench_datasource --bench bench_udf -- --test
```

Criterion benchmarks now fail on parsing/execution errors. The parser catalog
requires complete consumption, the sort fixture must produce matching rows,
and LIMIT timing excludes destruction of unconsumed input records. Historical
E3 and LIMIT numbers are not valid baselines for these corrected measurements.
