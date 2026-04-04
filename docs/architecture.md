# logq Architecture

This document describes how logq processes a query from input to output. logq follows a classic database query engine pipeline: parse, plan, execute.

```
  SQL string
      |
      v
  +-----------+     +----------+     +-----------------+     +---------------+     +-----------+
  |  Parser   | --> | Desugarer| --> | Logical Planner | --> | Physical Plan | --> | Execution |
  | (nom)     |     |          |     |                 |     |               |     | (streams) |
  +-----------+     +----------+     +-----------------+     +---------------+     +-----------+
   syntax/           syntax/          logical/                logical/              execution/
   parser.rs         desugar.rs       parser.rs               types.rs              stream.rs
                                                                                    types.rs
```

## Source Layout

```
src/
  main.rs                   CLI entry point (clap argument dispatch)
  app.rs                    Orchestrates the pipeline: parse → plan → execute → output
  cli.yml                   clap CLI definition (YAML)
  common/
    types.rs                Value enum, DataSource, path resolution, type helpers
  syntax/
    ast.rs                  AST node definitions
    parser.rs               nom-based SQL/PartiQL parser
    desugar.rs              AST rewriting pass (BETWEEN, COALESCE, NULLIF)
  logical/
    parser.rs               AST → logical plan translation
    types.rs                Logical plan nodes, physical plan conversion
  execution/
    types.rs                Physical plan nodes, expression evaluation
    stream.rs               RecordStream trait and all stream implementations
    datasource.rs           Log format readers (ELB, ALB, S3, Squid, JSONL)
```

## Pipeline Stages

### 1. Parsing (`syntax/parser.rs`)

The parser uses [nom](https://github.com/Geal/nom) parser combinators to transform a SQL string into an AST. It handles:

- Keywords (case-insensitive via `tag_no_case`)
- Identifiers (quoted and unquoted)
- Literals (integers, floats, strings, booleans, NULL, MISSING)
- Expressions with operator precedence (arithmetic, comparison, boolean)
- Postfix operators (IS NULL, IS MISSING, LIKE, BETWEEN, IN, CAST)
- Full SELECT statements, set operations (UNION/INTERSECT/EXCEPT), and subqueries

The parser produces `ast::Query`, which is either a `Select(SelectStatement)` or a `SetOp { op, all, left, right }` for compound queries.

**Key AST types** (`syntax/ast.rs`):

```
Query
├── Select(SelectStatement)
│   ├── distinct: bool
│   ├── select_clause: Vec<Expression>
│   ├── from_clause: FromClause
│   │   ├── Tables(Vec<TableReference>)
│   │   └── Join { left, right, join_type, condition }
│   ├── where_expr: Option<Expression>
│   ├── group_by: Vec<Expression>
│   ├── having: Option<Expression>
│   ├── order_by: Vec<OrderByExpr>
│   └── limit: Option<i32>
└── SetOp { op, all, left, right }

Expression
├── Column(PathExpr)         -- field reference, possibly nested (a.b.c, d[0])
├── Value(Value)             -- literal
├── BinaryOperator(op, l, r) -- +, -, *, /, =, !=, <, >, AND, OR, ||
├── UnaryOperator(op, expr)  -- NOT, negation
├── FuncCall(name, args)     -- scalar or aggregate function
├── CaseWhen(branches, else) -- multi-branch CASE
├── IsNull / IsNotNull       -- NULL checks
├── IsMissing / IsNotMissing -- MISSING checks
├── Like / NotLike           -- pattern matching
├── In / NotIn               -- membership test
├── Between / NotBetween     -- range check
├── Cast(expr, type)         -- type conversion
├── SelectValue(Box<Expr>)   -- SELECT VALUE wrapper
└── Subquery(Box<Query>)     -- non-correlated subquery
```

### 2. Desugaring (`syntax/desugar.rs`)

A tree-walking pass that rewrites syntactic sugar into core forms before logical planning:

| Input | Rewritten to |
| --- | --- |
| `x BETWEEN a AND b` | `x >= a AND x <= b` |
| `x NOT BETWEEN a AND b` | `x < a OR x > b` |
| `COALESCE(a, b, c)` | `CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ELSE c END` |
| `NULLIF(a, b)` | `CASE WHEN a = b THEN NULL ELSE a END` |

This keeps the logical planner simpler by reducing the number of expression variants it must handle.

### 3. Logical Planning (`logical/parser.rs`)

Translates the desugared AST into a tree of logical plan nodes. This stage:

- Resolves table references against the provided `DataSource`
- Separates aggregate expressions from scalar expressions in SELECT
- Builds the plan bottom-up: DataSource → Filter → Map → GroupBy → Having → OrderBy → Limit → Distinct
- Handles JOIN planning (CrossJoin, LeftJoin)
- Recursively plans subqueries with shared DataSource context
- Plans set operations (Union, Intersect, Except) over two sub-plans

**Logical plan nodes** (`logical/types.rs`):

```
Node
├── DataSource(source, bindings)
├── Filter(Formula, child)
├── Map(Vec<Named>, child)
├── GroupBy(keys, aggregates, group_as, child)
├── Limit(n, child)
├── OrderBy(specs, child)
├── Distinct(child)
├── CrossJoin(left, right)
├── LeftJoin(left, right, condition)
├── Union(left, right, all)
├── Intersect(left, right, all)
└── Except(left, right, all)
```

**Expression vs Formula**: The logical layer distinguishes between:
- `Expression` — produces a value (column reference, function call, constant)
- `Formula` — produces a boolean (comparison predicates, AND/OR/NOT, IS NULL)

### 4. Physical Plan (`logical/types.rs` → `execution/types.rs`)

The `Node::physical()` method on logical nodes converts them to physical plan nodes. This stage:

- Assigns generated names to constants (e.g., `const_000000000`) and stores their values in a `Variables` map
- Converts logical `Expression`/`Formula` to physical `Expression`/`Formula` that can be evaluated against records
- Preserves the tree structure for the executor

Physical nodes mirror logical nodes. Each implements `get(variables) -> Box<dyn RecordStream>` to construct the stream execution tree.

### 5. Execution (`execution/stream.rs`)

Execution uses a **pull-based iterator model**. The `RecordStream` trait defines the interface:

```rust
trait RecordStream {
    fn next(&mut self) -> StreamResult<Option<Record>>;
    fn close(&self);
}
```

Each operator wraps one or more child streams and transforms records on demand. The top-level `app::run()` pulls records in a loop and formats output.

**Stream implementations**:

| Stream | Operator | Behavior |
| --- | --- | --- |
| `LogFileStream` | Scan | Reads log lines, parses into records |
| `MapStream` | Project | Evaluates expressions, produces output columns |
| `FilterStream` | Select | Evaluates predicate, passes matching records |
| `LimitStream` | Limit | Passes first N records, then stops |
| `GroupByStream` | GroupBy | Materializes all input, groups, computes aggregates |
| `ProjectionStream` | OrderBy | Materializes all input, sorts, emits in order |
| `DistinctStream` | Distinct | Tracks seen rows with HashSet, deduplicates |
| `CrossJoinStream` | Cross Join | Nested-loop: for each left row, scans all right rows |
| `LeftJoinStream` | Left Join | Nested-loop with NULL padding for non-matches |
| `UnionStream` | Union | Drains left stream, then right stream |
| `IntersectStream` | Intersect | Materializes right into multiset, filters left |
| `ExceptStream` | Except | Materializes right into multiset, excludes from left |
| `InMemoryStream` | Subquery | Materializes a subquery result for repeated access |

### Expression Evaluation

The physical `Expression::evaluate()` and `Formula::evaluate()` methods in `execution/types.rs` handle runtime computation:

- **Arithmetic** (`+`, `-`, `*`, `/`): Int/Float coercion, NULL propagation
- **Comparisons** (`=`, `!=`, `<`, `>`, `<=`, `>=`): Three-valued logic with NULL
- **Boolean** (`AND`, `OR`, `NOT`): Three-valued logic (NULL propagation)
- **Functions**: Dispatched by name string to built-in implementations
- **Aggregates**: Stateful accumulators (Sum, Count, Avg, Min, Max, First, Last, PercentileDisc, ApproxPercentile, ApproxCountDistinct)

### NULL and MISSING Semantics

logq implements PartiQL's two-bottom-value system:

- **NULL** — value exists but is unknown (SQL NULL)
- **MISSING** — field does not exist in the record (PartiQL extension)

Both propagate through arithmetic and comparisons. `IS NULL` and `IS MISSING` test for each specifically. In boolean logic, three-valued logic applies (NULL AND FALSE = FALSE, NULL OR TRUE = TRUE, etc.).

## Data Model

### Value

The core runtime type (`common/types.rs`):

```rust
enum Value {
    Int(i32),
    Float(OrderedFloat<f32>),
    Boolean(bool),
    String(String),
    Null,
    Missing,
    DateTime(chrono::DateTime<FixedOffset>),
    HttpRequest(HttpRequest),
    Host(Host),
    Object(LinkedHashMap<String, Value>),
    Array(Vec<Value>),
}
```

`Object` and `Array` support nested semi-structured data from JSONL input. Path expressions (`a.b.c`, `d[0]`, `e[*]`, `f.*`) navigate into nested values.

### Record

A `Record` (`execution/stream.rs`) wraps a `LinkedHashMap<String, Value>`, preserving column insertion order. Records flow through the stream pipeline, with each operator reading, transforming, or filtering them.

## Log Format Readers (`execution/datasource.rs`)

Each log format is defined by:
1. A list of `(field_name, DataType)` pairs describing the schema
2. A regex (`SPLIT_READER_LINE_REGEX`) that tokenizes each log line into fields
3. Type-specific parsing for each field (DateTime, Host, HttpRequest, Float, Int, String)

For **JSONL**, each line is parsed with `json::parse()` and recursively converted to the `Value` data model. JSON objects become `Value::Object`, arrays become `Value::Array`, and numbers are auto-typed as Int or Float.

The `RecordRead` trait abstracts over format-specific readers:

```rust
trait RecordRead {
    fn read_record(&mut self) -> ReaderResult<Option<Record>>;
}
```

`ReaderBuilder` constructs the appropriate reader from a format string and file path (or stdin).

## Output

`app::run()` pulls records from the top-level stream and formats them:

- **Table**: prettytable-rs ASCII table
- **CSV**: csv crate writer to stdout
- **JSON**: json crate, builds array of objects then prints

## Dependencies

| Crate | Purpose |
| --- | --- |
| `nom` | Parser combinators |
| `clap` | CLI argument parsing (YAML-based) |
| `chrono` | DateTime handling |
| `ordered-float` | Hashable/comparable floats |
| `linked-hash-map` | Order-preserving maps for records |
| `regex` | Log line tokenization, LIKE patterns |
| `prettytable-rs` | Table output |
| `csv` | CSV output |
| `json` | JSONL parsing and JSON output |
| `url` | URL parsing for HTTP request fields |
| `tdigest` | Approximate percentile |
| `pdatastructs` | HyperLogLog for approx_count_distinct |
| `thiserror` | Error type derivation |
| `anyhow` | Error handling |

## Known Limitations

- **No correlated subqueries** — only non-correlated scalar subqueries in WHERE and SELECT
- **No INNER JOIN** — use CROSS JOIN + WHERE as a workaround
- **No window functions** — no OVER/PARTITION BY support
- **Nested-loop joins** — no hash join optimization; right side is re-scanned per left row
- **Full materialization** — ORDER BY, GROUP BY, INTERSECT, and EXCEPT load all data into memory
- **Single-threaded** — no parallel execution
