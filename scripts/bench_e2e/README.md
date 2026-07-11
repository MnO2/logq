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
is written to `results/table.md`.

All tools scan the same JSONL file and implement five equivalent operations:
full count, selective filter, status-code grouping, top-10 latency, and a
user-agent substring filter. Syntax is idiomatic to each tool. angle-grinder's
`limit` is a pre-sort streaming operator, so its top-10 command performs the
full sort and applies `head` to the rendered rows; this preserves top-N
semantics while still measuring its full sort cost.
