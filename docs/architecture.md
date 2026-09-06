# logq Architecture

logq parses PartiQL into a logical plan, converts that plan to executable nodes,
and pulls results through row or columnar batch operators. Both paths share the
runtime value model, function registry, and query memory tracker.

```text
SQL → syntax/parser.rs → syntax/desugar.rs → logical/parser.rs
                                                    ↓
                                             logical/types.rs
                                                    ↓
                                             execution/types.rs
                                               ↙          ↘
                                       RecordStream    BatchStream
                                               ↖          ↙
                                           output in app.rs
```

## Source Layout

| Location | Responsibility |
| --- | --- |
| `src/main.rs` | clap commands, table specifications, thread and memory options |
| `src/app.rs` | Parse/plan orchestration, diagnostics, output, library entry points |
| `src/common/types.rs` | Runtime `Value`, paths, variables, data sources |
| `src/syntax/` | AST, nom parser, desugaring |
| `src/logical/` | Logical plan construction and physical conversion |
| `src/execution/types.rs` | Physical nodes, pipeline selection, expression and aggregate behavior |
| `src/execution/stream.rs` | Row records and pull operators |
| `src/execution/batch*.rs` | Typed batches, scans, predicates, projection, grouping, sorting, limits |
| `src/execution/json_reader.rs`, `json_column_builder.rs`, `json_batch_scan.rs` | JSON validation, selective retention, direct column construction |
| `src/execution/datasource.rs`, `regex_format.rs` | Readers, compression detection, built-in and custom formats |
| `src/execution/field_analysis.rs` | Required-field analysis and scan projection |
| `src/execution/parallel.rs`, `json_gzip.rs` | Bounded worker queues, file tasks, parallel aggregation, gzip pipeline |
| `src/execution/memory.rs` | Shared retained-state estimates and reservations |
| `src/execution/prefix_sort.rs` | Sort-key encoding and bounded top-N heap |
| `src/functions/` | Registered scalar functions and shared function handles |
| `src/simd/` | Typed kernels, bitmaps, selection vectors, padded storage |

## Pipeline Stages

### 1. Parsing (`syntax/parser.rs`)

The nom parser turns the query into `ast::Query`: either a boxed
`SelectStatement` or a `SetOp` with left/right queries. SELECT statements contain
projection, FROM, and optional filtering, grouping, HAVING, ordering, and LIMIT
clauses. Keywords are case-insensitive. Expressions include nested paths, array
indices, literals, arithmetic, boolean operators, casts, function calls, CASE,
and subqueries. The application rejects unconsumed query text.

The AST types in `syntax/ast.rs` are the source of truth for supported syntax;
`tests/conformance/` records selected PartiQL examples and explicit exclusions.

### 2. Desugaring (`syntax/desugar.rs`)

This recursive pass rewrites syntactic sugar before planning:

| Input | Core behavior |
| --- | --- |
| `x BETWEEN a AND b` | `x >= a AND x <= b` |
| `x NOT BETWEEN a AND b` | `x < a OR x > b` |
| `COALESCE(a, b, ...)` | CASE branches skip both NULL and MISSING |
| `NULLIF(a, b)` | `CASE WHEN a = b THEN NULL ELSE a END` |

Rewrites also traverse nested expressions and subqueries. Changes here must
preserve three-valued logic and branch evaluation behavior.

### 3. Logical Planning (`logical/parser.rs`)

Planning resolves table references against `DataSourceRegistry`, separates
aggregate inputs from scalar projection, and builds an operator tree. Filters
implement WHERE and HAVING at their respective stages. Projection, grouping,
deduplication, ordering, and limits are placed according to query semantics;
they are not interchangeable passes.

Plans include `DataSource`, `Filter`, `Map`, `GroupBy`, `Distinct`, `OrderBy`,
`Limit`, cross/outer/hash joins, and set operations. Equality join keys can use a
hash join with an optional residual predicate; other joins use nested loops.
Non-correlated subqueries are planned recursively with the same source registry.

The logical layer distinguishes value-producing `Expression` from boolean
`Formula`. Physical conversion in `logical/types.rs` builds execution nodes and
keeps literals as constant values, so JSON fields cannot shadow SQL constants.

`logical/having.rs` binds aggregate calls in HAVING to aggregate outputs before
ordinary planning. It reuses unambiguous SELECT aggregates or adds collision-free
hidden aggregates, then removes hidden outputs after HAVING and before
DISTINCT/ORDER/LIMIT. Nested aggregate calls are rejected. HAVING-only aggregates
require explicit GROUP BY when SELECT contains no aggregate, matching the current
grouped-projection subset rather than silently discarding SELECT expressions.

### 4. Pipeline Selection (`execution/types.rs`)

`Node::get_with_memory_limit` creates one query-wide memory tracker, resolves
`--threads 0`, determines required source fields, and constructs the execution
tree. Eligible subtrees use columnar operators. Unsupported parents can still
consume a batch child through `BatchToRowAdapter`.

Fixed-format files support typed batch scans. JSONL batching requires a known set
of required root fields, which can be empty for `count(*)`. Required object paths
prune unreferenced nested siblings while preserving full consumed-line validation.
Whole-object references override narrower masks; array/index/wildcard and scoped
shapes retain their safe full-value fallback. Nested selected values still use
dynamic `Value` storage. Bare scans, stdin, table bindings, regex
formats, joins, set operations, and some expression/path shapes use row
operators. Prefix LIMIT paths preserve demand-driven parsing and expression
evaluation so they need not read later invalid rows.

`logq explain` reports the top-level batch/row capability and the first fallback
reason, along with requested/resolved thread settings and an optional budget.
It does not execute rows or promise that the input will qualify for parallel
scanning. Pass the same table mappings as the query; otherwise the CLI assumes
`it:jsonl=stdin`.

### 5. Execution

Both interfaces use a pull model:

```rust
trait RecordStream {
    fn next(&mut self) -> StreamResult<Option<Record>>;
    fn close(&self);
}

trait BatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>>;
    fn schema(&self) -> &BatchSchema;
    fn close(&self);
}
```

Row operators process insertion-ordered records. Columnar operators normally
process batches of up to 1,024 rows, using typed columns and selection vectors
to avoid converting rejected rows back into records. Supported expressions bind
function handles for repeated evaluation. Typed kernels operate on contiguous
numeric data and validity bitmaps; mixed values retain scalar semantics.

JSON/NDJSON consume the selected batch stream directly, borrowing string and
mixed-value cells instead of rebuilding a map per output row. The row API, CSV
and table consumers still use `BatchToRowAdapter` as needed. Both result interfaces
share pipeline selection, LIMIT guards and the query memory tracker. Duplicate
output names use the last occurrence consistently in projection, predicates,
sorting and serialization.

Trusted Float32 arithmetic can produce a typed output column directly for a
left-associated chain of addition by constants/root Float32 columns and
multiplication by Float32 constants. Every operation retains f32 rounding;
custom functions, integer overflow-aware arithmetic and unsupported trees use
the original evaluation path.

| Operation | Retained state |
| --- | --- |
| Scan/filter/project/limit | Current input buffers and batches; queued batches for parallel scans |
| COUNT/SUM/AVG/MIN/MAX/FIRST/LAST grouping | Group keys and accumulators, rather than every input row |
| Exact percentile / GROUP AS | Values or records captured for each group |
| Approximate aggregates | Sketch state per group |
| DISTINCT | Seen row keys |
| ORDER BY without LIMIT | Complete sortable result |
| ORDER BY with LIMIT k | At most k candidate rows in a bounded heap |
| Cross/outer/hash joins | Materialized join inputs and optional hash state |
| UNION ALL | Left stream followed by right stream |
| UNION DISTINCT | Concatenated stream plus deduplication state |
| INTERSECT/EXCEPT | Right-side membership or multiplicity state, plus distinctness state when needed |

Grouping still consumes its input before returning final results. Low-cardinality
grouping can use little retained state, while high-cardinality grouping can grow
with the input. Time-bucket grouping does not assume source timestamps are sorted.

### Parallel Scanning and Aggregation

`--threads 0` uses `available_parallelism`; `1` selects sequential execution.
Eligible regular plain files of at least 16 MiB can use mmap and newline-aligned
tasks. Worker queues are bounded, and results are consumed in task order.
Independent JSONL shards can be scheduled at file granularity, including shards
below the mmap threshold and gzip inputs. Query shape and source type determine
which route is available.

Eligible COUNT/SUM/AVG plans accumulate groups locally in workers and merge
partial states. Other aggregate shapes keep their supported sequential operator
path. A single gzip JSONL file can use one decoder thread and the remaining
thread budget for parsers on eligible full-aggregation plans; compressed input
is never memory-mapped. With `--max-memory`, this single-file decoded-chunk route
is disabled, preserving the sequential decoder's budget behavior.

Input files must remain immutable throughout execution. Modifying or truncating
an active memory mapping is unsupported. Worker cancellation and teardown must
also preserve LIMIT behavior, earlier input errors, and ordered aggregate semantics.

### Memory Budget

`--max-memory` applies a shared soft estimate to retained execution state in both
pipelines. Operators reserve estimated bytes for owned keys, values, records,
hash structures, candidate rows, and materialized batches. Exceeding the limit
returns `MemoryBudgetExceeded`; there is no spill-to-disk fallback.

The estimate is not heap usage or an RSS cap. Scanner buffers, resident mmap
pages, output formatting, and allocation overhead may sit outside it. In
particular, the default table renderer buffers all displayed rows separately.
Use NDJSON or CSV for large results. Memory estimates and actual peak RSS must
be measured and reported separately in performance work.

## Data Model

The runtime `Value` in `common/types.rs` includes:

```rust
enum Value {
    Int(i32),
    Float(OrderedFloat<f32>),
    Boolean(bool),
    String(CompactString),
    Null,
    Missing,
    DateTime(chrono::DateTime<chrono::FixedOffset>),
    HttpRequest(Box<HttpRequest>),
    Host(Box<Host>),
    Object(Box<LinkedHashMap<String, Value>>),
    Array(Vec<Value>),
}
```

`Record` wraps an insertion-ordered `Variables` map. Object and array paths resolve
nested JSON values. Integers are signed 32-bit; floats and numeric aggregate
SUM/AVG results use Float32 precision. JSON integers outside the Int32 range become
floats and may lose precision. Large identifiers should be stored as strings.
Integer arithmetic uses checked operations so overflow is an error in both
debug and release builds.

Equality distinguishes Int32 from Float32, while ordered comparisons compare
across numeric types. For example, SUM returns a float and `SUM(x) = 5.0`
differs from `SUM(x) = 5`. The [numeric migration proposal](numeric-migration.md)
specifies the compatibility work required for a future Int64/Float64 runtime;
it does not change the current representation.

NULL means an unknown value; MISSING means an absent field. Both participate in
three-valued arithmetic/comparison logic, and `IS NULL`/`IS MISSING` distinguish
them. Boolean logic includes rules such as `NULL AND FALSE = FALSE` and
`NULL OR TRUE = TRUE`.

Typed columns represent Int32, Float32, Boolean, UTF-8, dictionary UTF-8,
DateTime, or Mixed values. Their `missing` bitmap marks field presence, and their
`null` bitmap marks non-NULL values. A value is readable only when both bits are
set. Dictionary strings and typed kernels must preserve the same results as
their ordinary UTF-8 and row equivalents.

## Log Format Readers

`ReaderBuilder` constructs readers for paths, sorted file lists, or stdin.
File compression is detected from a `.gz` extension or gzip magic bytes; stdin
expects decoded text. Decoding includes all
concatenated members with errors propagated from later members. Fixed-format
schemas describe field names and types; tokenization handles quoted log fields. Custom TOML
regex definitions supply named captures and optional types. CLF and combined
formats use built-in regex definitions through the same reader layer.

JSONL requires one JSON object per line. `json_reader.rs` uses serde visitors
directly: selected scalars can enter column builders without an intermediate
JSON DOM or record map. Required-field analysis avoids retaining unused roots,
but discarded values still undergo JSON validation. Selected nested values
remain dynamic objects/arrays; absent fields become MISSING. Blank lines,
non-object roots, malformed values, and trailing JSON are errors for consumed
lines. A prefix LIMIT can stop before later lines are consumed.

## Output

`app::run_with_memory_limit` consumes records and formats output:

- **Table:** prettytable-rs, buffers rows until completion.
- **CSV:** csv writer, streams rows without a header.
- **JSON:** writes one array incrementally through a buffered writer.
- **NDJSON:** writes one object per line through a buffered writer.

JSON output encodes both MISSING and NULL values as `null`; explicit projection
therefore loses that distinction. Star projection only carries fields present
in its source record. Serialization uses serde_json. Query and output failures
propagate to a nonzero CLI exit;
streaming formats may have already produced partial output, including an
unclosed JSON array. Consumers must check completion status.

## Known Limitations

- Correlated subqueries, window functions, PIVOT, Ion literals, and bag literals
  are excluded; consult the conformance fixture skips for tested boundaries.
- Numeric precision is Int32/Float32, without an exact decimal type.
- Grouping and set results have no promised order without `ORDER BY`.
- Non-equality joins can require nested loops; stateful operators can exceed
  available RAM unless a suitable soft budget stops them first.
- Parallelism is selective, and `explain` is a plan description rather than a
  runtime profiler. See [the benchmark harness](../scripts/bench_e2e/README.md)
  for measured thread, memory, parsing, and aggregation controls.
