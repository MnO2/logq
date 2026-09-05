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

## Paired workload exploration

`explore.py` is a separate, standard-library-only harness for comparing saved
logq binaries. It leaves the five-query comparison suite above unchanged.
Its 17 cases pair low/high/skewed grouping, short/long repeated/unique strings,
direct/arithmetic/CASE aggregates, wide/nested input with NULL/MISSING and mixed
values, Top-10/Top-1000/full sort, and identical plain/sharded/gzip input.

The default 100,000 rows produce approximately 100 MiB of base JSONL, plus a
wider derivative, byte-identical small shards and deterministic gzip. The base
contains all four string columns, so the four LIKE cases scan identical bytes
and all match 20% of rows. High grouping has 10,000 uniformly distributed groups;
skew grouping keeps 10,000 groups with 90% of rows in one group. `--groups` can
change cardinality; when using very small smoke inputs, inspect each case's
`expected_output_rows` because there may be too few rows to populate every group.
Wide/nested cases use the same rows and values but more bytes; report both row
throughput and input bytes. Shards default to 4,000 rows, below the 16 MiB
parallel-file threshold. Generation never replaces an existing corpus with a
different configuration and verifies cached file hashes before reuse.

Start with answer checks on a small corpus. Each invocation needs a fresh results
directory; use a different data directory when changing generation parameters.

```sh
python3 scripts/bench_e2e/explore.py --rows 1000 --groups 100 --shard-rows 200 \
  --threads 1 4 --validate-only \
  --binary baseline=/tmp/logq-milestone-20260905/logq-baseline-0d21af7 \
  --binary candidate=target/release/logq --allow-invalid baseline \
  --data-dir /tmp/logq-explore-smoke-data --results-dir /tmp/logq-explore-smoke-results
```

Then run the representative matrix after the candidate build is complete:

```sh
python3 scripts/bench_e2e/explore.py --threads 1 4 --runs 5 --warmup 1 \
  --binary baseline=/tmp/logq-milestone-20260905/logq-baseline-0d21af7 \
  --binary candidate=target/release/logq --allow-invalid baseline \
  --data-dir scripts/bench_e2e/data/explore-v1 \
  --results-dir scripts/bench_e2e/results/explore-v1-paired
```

`--cases group_high group_skew expression_direct expression_arithmetic` selects
cases; `--max-memory 16MiB` adds an operator-state budget. `--generate-only`
prepares data without requiring a binary. `--timeout` bounds each subprocess.
`--skip-rss` omits the separate RSS measurement. No commands pass through a shell;
spaces and shell metacharacters in paths remain literal. Commas and glob syntax
in the data directory are rejected because logq itself interprets these in table
specifications.

Before recording timings, every selected binary/case/thread combination is
validated against an independent SQLite oracle built from the actual decoded
JSON. The oracle uses a 4 MiB SQLite page cache and disk temporary storage;
large GROUP BY and ORDER BY outputs are consumed incrementally. Validation checks
exact output fields/types and row counts, order-independent SHA-256 sums and
squared sums for bags, and an ordered SHA-256 digest for sorting. These are
bounded-memory probabilistic fingerprints, not bytewise equality proofs. Numeric
sums and means are normalized to the current public IEEE Float32 precision;
NULL/MISSING are tested through explicit presence counts, without treating them
as numeric zero. Int32/Float32 representational limits remain engine boundaries.

A correctness failure is recorded with no accepted timing samples. Explicit
`--allow-invalid baseline` keeps historical baseline bugs untimed while allowing
the other cases and candidate to run. It does not permit wrong candidate answers
or create a speedup ratio for an invalid baseline. A failure in a measured repeat
also invalidates that combination. By default, any non-allowed failure makes the
command exit nonzero **after** writing the complete matrix report.

Measurements are complete warm-cache subprocess invocations. Output goes to a
temporary file; formatting and file writes are timed equally for all binaries,
and streaming validation happens after the clock stops. Unlike the original
suite's `/dev/null` sink, large result sets therefore include temporary-file
output cost. Binary order alternates by round. Wall time and child user/system
CPU time have separate mean/sample-SD fields; peak RSS is a separate successful
`/usr/bin/time` sample. RSS includes resident mmap pages and cannot be interpreted
as the `--max-memory` retained-state estimate. Five samples do not establish p95.

Artifacts include commands, raw samples, independent oracle digests, a copied
harness, query snapshot, corpus manifest, binary/data/query/script SHA-256 values,
git/build provenance, and `comparisons.json`. Externally supplied binary build
flags are explicitly unknown; a workspace git hash does not identify that
binary's source. `results.json` status is authoritative: raw samples may precede
a later failure, and only `ok` combinations receive performance comparisons.

The metadata also records two **unmeasured roadmap controls**: cold-cache runs
(require cache eviction after oracle checks and physical-I/O evidence), and
prepared/persistent storage (require preparation time, storage size, cache policy
and query repetitions of 1/10/100). This harness accepts only `--cache-state warm`
and implements no persistent storage. A raw-file/prepared-table ratio must be
reported separately, with preparation amortization.

```sh
python3 -m unittest discover -s scripts/bench_e2e -p 'test_explore.py'
```

## JSON scanner and allocation probes

`examples/json_scan_probe.rs` isolates the JSON batch scanner, with an optional
LIKE filter. `probe_json.py` compares baseline/dictionary-off,
candidate/dictionary-off, and candidate/dictionary-on. Both executables must be
**probe binaries**, not the `logq` CLI. The baseline variant continues to accept
and report `dictionary: false`; the harness does not require a new binary
protocol.

Build the candidate probe after finishing its implementation:

```sh
cargo build --release --locked --features bench-internals --example json_scan_probe
```

For an older baseline, build the same example wrapper and benchmark-only exports
in its isolated source copy. Keep that baseline's scanner implementation intact;
add only the instrumentation hooks required to expose it, and save the exact
instrumentation patch and build command. Do not infer binary provenance from the
current workspace's git commit. The harness accepts declared source/build
information and records executable hashes; externally supplied source identities
remain unverified declarations.

```sh
python3 scripts/bench_e2e/probe_json.py \
  --baseline /tmp/logq-milestone-20260905/json-scan-baseline-0d21af7 \
  --candidate target/release/examples/json_scan_probe \
  --baseline-source '0d21af7 + recorded benchmark instrumentation patch' \
  --candidate-source 'current reviewed worktree' \
  --candidate-build-command 'cargo build --release --locked --features bench-internals --example json_scan_probe' \
  --data scripts/bench_e2e/data/explore-v1/base.jsonl \
  --results-dir scripts/bench_e2e/results/json-probe-reviewed --runs 5
```

The default matrix scans and filters each of `sr/su/lr/lu` through a mapped
reader, then scans `lr` with 8 KiB, 64 KiB and 1 MiB buffered readers. Use
`--fields lr --modes scan --backends mapped buffered64k` for a small selection.
The corpus must be nonempty UTF-8 JSONL with the selected string fields.
The input must remain immutable while mapped: changing/truncating an active mmap
is unsafe. Every invocation has a nonpolling deadline (`--timeout`, default
120 seconds), and failures preserve a `failed` metadata record and partial
results. Binary, corpus and harness/helper/example-source hashes are rechecked at
completion. The result directory includes source snapshots and probe definitions;
changing the data or source invalidates the run. The main `explore.py` matrix
also rechecks its complete corpus after measurements and suppresses comparisons
if those hashes changed.

Interpret the probe measurements within these limits:

- `elapsed_ns` covers scanner/filter construction, scanning, consumed-batch
  destruction and scanner/reader destruction. File opening, buffer construction,
  mmap creation and process startup are outside that interval. Thus mapped
  teardown is included while mapped setup is excluded. Complete-child user and
  system CPU times have a wider boundary and should not be subtracted from the
  internal elapsed time as a phase breakdown.
- Allocation counters come from a **separate** `--allocations` invocation and
  never enter timing means. They count successful allocation/reallocation
  requests; reallocation counts the entire new requested size again. These
  numbers are neither retained heap nor peak RSS. Reader buffers and mappings
  are created before counting starts. Even allocation-disabled probe builds
  retain the allocator's flag-check overhead, so their absolute latency is not
  the uninstrumented production CLI latency.
- The independent Python oracle verifies **counts only**. Each invocation also
  validates input bytes, reported mode/backend, counter consistency and
  instrumentation flags. It does not independently validate selected values or
  the identities of matched rows. The full `explore.py` SQLite/digest matrix and
  engine correctness tests remain necessary. In LIKE mode, `rows` means physical
  rows in returned batches: wholly rejected batches are absent, so this counter
  need not equal the number of rows scanned. `active_rows` is the checked match
  count; metadata `rows` records full input cardinality.
- All runs are warm-cache. One allocation sample provides diagnostic counts,
  while five timing samples describe only this workload on this machine. Compare
  requested bytes, allocation calls and elapsed time separately; fewer calls can
  coexist with more bytes and unchanged runtime. Dictionary-on is an explicit
  experimental control, not a claim that dictionary encoding should be enabled
  everywhere.

The archived `expansion-probes-initial/run` artifacts predate the source-snapshot,
strict protocol and post-run corpus/source checks. They remain initial
count-validated evidence with their original binary/data/script hashes; they must
not be relabeled as runs of the reviewed harness or treated as full value/heap
validation. Reproduce with a new results directory before drawing final claims.

```sh
python3 -m unittest discover -s scripts/bench_e2e -p 'test_probe_json.py'
```
