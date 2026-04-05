VERDICT: NEEDS_REVISION

## Summary Assessment

The plan is well-structured and nearly ready for execution. Token counts for all fake data lines have been verified correct (ELB=17, ALB=25, S3=24, Squid=10). However, there is one critical query mismatch against the design document, and a few lesser issues worth addressing.

## Critical Issues (must fix)

### 1. Step 3 query omits Float aggregation from design spec

The design document specifies the `test_elb_numeric_aggregation` query as:
```sql
SELECT elb_status_code, SUM(backend_processing_time) as total_bpt, SUM(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code
```

The plan's Step 3 query is:
```sql
SELECT elb_status_code, sum(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code
```

This drops `SUM(backend_processing_time)`, which means the test no longer validates Float aggregation -- the stated purpose in both the design ("Validates Float and Int types flow through aggregation without error") and the plan itself. `sent_bytes` is typed as `Integral`, so only Int aggregation is tested. The `backend_processing_time` field is typed as `Float` and must be included to fulfill the design requirement.

**Fix**: Change the Step 3 query to match the design:
```sql
SELECT elb_status_code, sum(backend_processing_time) as total_bpt, sum(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code
```

## Suggestions (nice to have)

### 1. Consider adding assertions beyond Ok(()) for at least one aggregation test

10 of 14 tests only assert `Ok(())`. While this confirms parsing doesn't crash, it doesn't verify that the right number of groups or correct values are produced. Consider upgrading at least one aggregation test (e.g., `test_s3_group_by_http_status` or `test_squid_count_by_status`) to use `run_format_query_to_vec()` and assert on the number of result rows (e.g., 3 distinct status groups for the S3 test with 200/403/404). This would catch silent data corruption bugs where parsing succeeds but produces wrong groupings.

### 2. run_to_vec function placement could be cleaner

The plan places `run_to_vec()` at module top-level with `#[cfg(test)]`. This works, but it's a bit unusual to have a `#[cfg(test)]` function at the crate level alongside production code. Since it's only used by tests, placing it inside `mod tests` (like the helper functions) would be more idiomatic. However, both approaches compile correctly, so this is a style suggestion only.

### 3. Step 10 test_s3_dash_placeholders query could be more precise

The query `SELECT error_code FROM it WHERE error_code = "-"` checks filtering on dash values, but the design mentions testing `refererr` as well. The query only selects `error_code`. Consider `SELECT error_code, refererr FROM it WHERE error_code = "-"` to match the design's mention of "some with `-` in `error_code` and `refererr` fields".

### 4. JSONL not mentioned in plan scope

The plan and design both correctly note that JSONL already has ~22 integration tests and is out of scope. This is fine.

## Verified Claims (things you confirmed are correct)

### Token counts -- all verified correct

Using the regex `[^\s"'\[\]]+|"([^"]*)"|'([^']*)'|\[([^\[\]]*)\]` from `datasource.rs`:

- **ELB lines** (Steps 2-5): All tokenize to exactly 17 tokens. The quoted fields (`"GET ... HTTP/1.1"`, `"Mozilla/5.0"`, `"Root=1-001"`) each count as one token. The ARN field counts as one token (no spaces or quotes). Verified with multiple lines across all 4 ELB test steps.
- **ALB lines** (Steps 6-8): All tokenize to exactly 25 tokens. The trailing quoted fields (`"-"`, `"forward"`, etc.) each count as one token. Verified with lines from Steps 6, 7, and 8.
- **S3 lines** (Steps 9-12): All tokenize to exactly 24 tokens. The bracket-enclosed timestamp `[06/Feb/2019:00:00:38 +0000]` counts as one token. Verified with lines from Steps 9, 11, and 12.
- **Squid lines** (Steps 13-15): All tokenize to exactly 10 tokens. The bracket-enclosed IP `[192.168.1.1]` counts as one token. Verified with lines from Steps 13, 14, and 15.

### File paths are accurate

- `src/app.rs` exists and contains the `run()` function at line ~120, the `#[cfg(test)] mod tests` block at line ~200, and existing imports for `tempfile::tempdir`, `File`, and `Write` at lines 203-205.
- `src/execution/datasource.rs` exists and contains all schema definitions referenced in the plan.
- The output directory `docs/plans/` exists.

### Test commands are correct and runnable

- `cargo test --no-run` (Step 1c) will compile without running, confirming helpers compile.
- `cargo test test_elb_select_and_where_filter -- --nocapture` format is correct for running a single test with output.
- `cargo test` (Step 16a) runs all tests.

### Schema field names match the code

- ELB: `elb_status_code`, `sent_bytes`, `backend_processing_time`, `timestamp` -- all match `AWS_ELB_FIELD_NAMES`.
- ALB: `type`, `elb_status_code`, `request_processing_time`, `received_bytes` -- all match `AWS_ALB_FIELD_NAMES`.
- S3: `operation`, `http_status`, `error_code`, `bucket` -- all match `AWS_S3_FIELD_NAMES`.
- Squid: `method`, `url`, `code_and_status` -- all match `SQUID_FIELD_NAMES`.

### DataSource construction is correct

The helper functions construct `DataSource::File(file_path, format.to_string(), "it".to_string())` which matches the existing pattern in `src/app.rs` tests. The three-argument format `(PathBuf, String, String)` matches the `DataSource::File` variant in `src/common/types.rs` line 421.

### TDD cycle is not applicable (no new implementation)

These are pure test additions for existing functionality. The design correctly notes that no new implementation is needed -- the tests exercise existing parsing and query execution code. The "failing test then implement" cycle doesn't apply since these tests should pass immediately if the data lines are well-formed.

### Dependency groups are correct

All tests go in the same file (`src/app.rs`), and the plan correctly marks them as sequential within each group. Groups 2-5 could technically be done in any order since they're all appending to the same file in non-conflicting ways, but the sequential approach is safer.

### Task sizing is appropriate

Each step is a single test function (20-30 lines of code) plus a single `cargo test` command. This fits the 2-5 minute target well. Step 1 (helpers) is slightly larger but still manageable.

### Plan covers the design fully

The design specifies 14 tests across 4 formats (ELB=4, ALB=3, S3=4, Squid=3). The plan implements all 14. The only discrepancy is the Step 3 query omission noted as a critical issue above.
