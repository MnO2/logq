# Multi-Table CLI Support Design

## Summary

Add support for multiple `--table` flags so that queries can reference different data source files for different table names. This enables real multi-table joins:

```bash
logq query --table a:jsonl=data1.jsonl --table b:jsonl=data2.jsonl "SELECT * FROM a, b WHERE a.id = b.id"
```

## Approach

Use the **repeated `--table` flag** pattern (like Docker's `-v`, `-e`). Each flag specifies one table-name-to-file mapping. A `DataSourceRegistry` (`HashMap<String, DataSource>`) replaces the single `DataSource` parameter throughout the pipeline.

### Decision Log

| Decision | Choice | Alternatives Considered |
|----------|--------|------------------------|
| Multi-table CLI syntax | Repeated `--table` flag | Comma-separated, semicolon-separated |
| Data source plumbing | `HashMap<String, DataSource>` registry | Single DataSource + optional override map |
| Unknown table in query | Hard error at planning time | Silent empty results |
| Duplicate table names | Error | Last-one-wins |

## Design

### 1. CLI Parsing (`main.rs`)

- `cli.yml`: Add `multiple: true` to the `table` arg.
- `main.rs`: Iterate `values_of("table")` instead of `value_of("table")`. Parse each value with `TABLE_SPEC_REGEX`, collecting into a `HashMap<String, DataSource>`. Error on:
  - Invalid format (not matching regex)
  - Unsupported file format (not in `elb/alb/squid/s3/jsonl`)
  - Duplicate table names across flags
- Pass the `HashMap<String, DataSource>` (the `DataSourceRegistry`) to `app::run` / `app::explain`.

### 2. DataSourceRegistry Type (`common/types.rs`)

```rust
pub type DataSourceRegistry = HashMap<String, DataSource>;
```

### 3. App Layer (`app.rs`)

- `app::run(query_str, data_sources: DataSourceRegistry, output_mode)` replaces the single `DataSource` param.
- `app::explain(query_str, data_sources: DataSourceRegistry)` same.
- `app::run_to_vec` (test helper) same.
- `app::run_to_records` (bench helper) same.

### 4. ParsingContext (`common/types.rs`)

```rust
pub(crate) struct ParsingContext {
    pub(crate) data_sources: DataSourceRegistry,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

Remove the `table_name` and single `data_source` fields.

### 5. Logical Planner (`logical/parser.rs`)

- `parse_query_top` and `parse_query` receive `DataSourceRegistry` instead of `DataSource`.
- `build_from_node` looks up each table reference's base name in the registry to get its `DataSource`. Returns `ParseError::UnknownTable(name)` if not found.
- `check_env` validates that every table name in the FROM clause exists in the `DataSourceRegistry`.
- Subqueries and set operations pass the full registry down.

Add new error variant:
```rust
#[error("Unknown table '{0}'. Available tables: {1}")]
UnknownTable(String, String),
```

### 6. Physical Plan Creator (`logical/types.rs`)

`PhysicalPlanCreator` holds `DataSourceRegistry` instead of a single `DataSource`.

### 7. Backward Compatibility

Single-table usage is unchanged — a single `--table it:jsonl=data.jsonl` produces a one-entry registry `{"it" => DataSource::File(...)}`. All existing tests continue to work by constructing one-entry registries.

## Testing

### CLI Argument Parsing Tests

1. Single table: `--table it:jsonl=data.jsonl` produces one-entry registry
2. Multiple tables: `--table a:jsonl=d1.jsonl --table b:jsonl=d2.jsonl` produces two-entry registry
3. Invalid format: `--table a:badformat=d.jsonl` errors
4. Duplicate table names: `--table a:jsonl=d1.jsonl --table a:jsonl=d2.jsonl` errors
5. Missing table flag: errors with `InvalidTableSpecString`
6. Unknown table in query: `--table a:jsonl=d.jsonl` with `SELECT * FROM b` errors with `UnknownTable`

### Integration Tests

7. Cross join two different files: `SELECT a.x, b.y FROM a, b`
8. Left join two different files: `SELECT a.x, b.y FROM a LEFT JOIN b ON a.id = b.id`
9. All existing ~58 tests pass (one-entry registry)

## Files Changed

| File | Change |
|------|--------|
| `src/cli.yml` | Add `multiple: true` to `table` arg |
| `src/main.rs` | Multi-value table parsing, duplicate detection, build registry |
| `src/common/types.rs` | Add `DataSourceRegistry` type, update `ParsingContext` |
| `src/app.rs` | Change signatures to take `DataSourceRegistry` |
| `src/logical/parser.rs` | Registry-based table lookup in `build_from_node`, new `UnknownTable` error |
| `src/logical/types.rs` | Update `PhysicalPlanCreator` to hold registry |
