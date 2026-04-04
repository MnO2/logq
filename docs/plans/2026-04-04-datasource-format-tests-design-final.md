# Datasource Format Integration Tests Design (v2)

## Changes from previous version

1. **ELB template field count fixed (critical #1)**: The ELB fake data template now includes all 17 fields, adding `target_group_arn` and `trace_id` as the final two tokens.

2. **Output verification via `run_to_vec()` (critical #2)**: A new `run_to_vec()` helper function will be added to `app.rs` that returns query results as `Vec<Vec<(String, Value)>>` instead of printing to stdout. At least one test per format now asserts on actual output values (row count, specific field values, or aggregation results), not just `Ok(())`. Simple tests that only need to verify error-free parsing still assert `Ok(())`.

3. **Test helper function (suggestion #7)**: A `run_format_query()` helper reduces boilerplate across all 14 tests by encapsulating tempdir creation, file writing, DataSource construction, and query execution.

4. **ALB schema description clarified (suggestion #4)**: The ALB section now accurately describes the field name differences from ELB (`target_processing_time` vs `backend_processing_time`, `target_status_code` vs `backend_status_code`).

5. **Squid bracket note (suggestion #5)**: Added a note that `remote_host` values include brackets from the parser (e.g., `"[192.168.1.1]"`).

6. **S3 LIKE test refocused (suggestion #6)**: `test_s3_like_on_key` replaced with `test_s3_dash_placeholders` to test S3-specific parsing of `-` placeholder values rather than duplicating generic LIKE coverage.

## Goal

Add integration tests that verify each supported log format (ELB, ALB, S3, Squid) can be parsed and queried end-to-end through the query engine. Currently, integration tests in `app.rs` almost exclusively use JSONL. This leaves 4 formats with zero or minimal integration query coverage.

## Gap Analysis

| Format | Current Integration Tests | Current Coverage |
|--------|--------------------------|-----------------|
| JSONL  | ~22 tests | Full query feature coverage |
| ELB    | 1 test | time_bucket, url_path_bucket, percentile only |
| Squid  | 1 test | Basic `SELECT *` only |
| ALB    | 0 tests | None |
| S3     | 0 tests | None |

## Infrastructure Changes

### `run_to_vec()` in `app.rs`

Add a new function alongside the existing `run()` that returns results as structured data instead of printing to stdout:

```rust
pub(crate) fn run_to_vec(
    query_str: &str,
    data_source: common::types::DataSource,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables)?;
    let mut results = Vec::new();

    while let Some(record) = stream.next()? {
        results.push(record.to_tuples());
    }

    Ok(results)
}
```

This reuses the same parse/plan/execute pipeline as `run()` but collects results into a `Vec` instead of formatting them for output. The return type uses `to_tuples()` which is already public on `Record`.

### Test helper function

```rust
#[cfg(test)]
fn run_format_query(format: &str, lines: &[&str], query: &str) -> AppResult<()> {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(file_path.clone()).unwrap();
    for line in lines {
        writeln!(file, "{}", line).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    let data_source = common::types::DataSource::File(
        file_path,
        format.to_string(),
        "it".to_string(),
    );
    run(query, data_source, OutputMode::Csv)
}

#[cfg(test)]
fn run_format_query_to_vec(
    format: &str,
    lines: &[&str],
    query: &str,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(file_path.clone()).unwrap();
    for line in lines {
        writeln!(file, "{}", line).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    let data_source = common::types::DataSource::File(
        file_path,
        format.to_string(),
        "it".to_string(),
    );
    run_to_vec(query, data_source)
}
```

## Approach

- Tests live in `#[cfg(test)] mod tests` in `src/app.rs`, following existing patterns
- Each test uses `run_format_query()` or `run_format_query_to_vec()` helpers
- Record count varies per test: 3 for simple SELECT/WHERE, 5-8 for aggregation
- Tests are format-appropriate: they exercise each format's unique field types and schema, not generic query features (JSONL already covers those)
- At least one test per format uses `run_format_query_to_vec()` to assert on actual output values (row count, field values, or aggregation results)
- Remaining tests use `run_format_query()` and assert `Ok(())` where error-free execution is sufficient proof of correct parsing

## Test Plan

### ELB (4 tests)

Schema: 17 fields. Types: DateTime (`timestamp`), Host (`client_and_port`, `backend_and_port`), Float (`request_processing_time`, `backend_processing_time`, `response_processing_time`), String (`elb_status_code`, `backend_status_code`, `elbname`, `user_agent`, `ssl_cipher`, `ssl_protocol`, `target_group_arn`, `trace_id`), Integral (`received_bytes`, `sent_bytes`), HttpRequest (`request`).

1. **`test_elb_select_and_where_filter`** (3 records) -- **asserts output values**
   - Query: `SELECT elb_status_code, sent_bytes FROM it WHERE elb_status_code = "200"`
   - Uses `run_format_query_to_vec()`
   - Asserts: result row count equals expected number of 200-status records (2 of 3), and each returned row has `elb_status_code` = `Value::String("200")`.

2. **`test_elb_numeric_aggregation`** (5 records)
   - Query: `SELECT elb_status_code, SUM(backend_processing_time) as total_bpt, SUM(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates Float and Int types flow through aggregation without error

3. **`test_elb_order_by_timestamp`** (3 records, out-of-order timestamps)
   - Query: `SELECT timestamp FROM it ORDER BY timestamp ASC`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates DateTime comparison and sorting

4. **`test_elb_limit_with_order`** (5 records)
   - Query: `SELECT sent_bytes FROM it ORDER BY sent_bytes DESC LIMIT 2`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates Int ordering + limit interaction

### ALB (3 tests)

Schema: 25 fields. Similar type structure to ELB but with key differences: leading `type` String field, uses `target_processing_time`/`target_status_code` instead of ELB's `backend_processing_time`/`backend_status_code`, positions 20-24 are all String-typed.

1. **`test_alb_filter_by_type`** (3 records, types: "http", "https", "h2") -- **asserts output values**
   - Query: `SELECT type, elb_status_code FROM it WHERE type = "https"`
   - Uses `run_format_query_to_vec()`
   - Asserts: result row count is 1, and the returned row has `type` = `Value::String("https")`.

2. **`test_alb_aggregate_processing_times`** (5 records)
   - Query: `SELECT elb_status_code, SUM(request_processing_time) as total_rpt FROM it GROUP BY elb_status_code`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates Float aggregation across ALB schema

3. **`test_alb_order_by_received_bytes`** (5 records)
   - Query: `SELECT received_bytes FROM it ORDER BY received_bytes DESC LIMIT 3`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates Int parsing and ordering in ALB context

### S3 (4 tests)

Schema: 24 fields, all String-typed. Many fields use `-` as a placeholder for absent values. Focus on string operations and S3-specific parsing.

1. **`test_s3_filter_by_operation`** (5 records) -- **asserts output values**
   - Query: `SELECT operation, http_status FROM it WHERE operation = "REST.GET.OBJECT"`
   - Uses `run_format_query_to_vec()`
   - Asserts: result row count matches expected number of GET operations (3 of 5), and each returned row has `operation` = `Value::String("REST.GET.OBJECT")`.

2. **`test_s3_dash_placeholders`** (3 records, some with `-` in `error_code` and `refererr` fields)
   - Query: `SELECT error_code, refererr FROM it WHERE error_code = "-"`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates S3-specific parsing of `-` placeholder values (the most common S3-specific data quirk)

3. **`test_s3_group_by_http_status`** (8 records, mixed 200/403/404)
   - Query: `SELECT http_status, COUNT(*) as cnt FROM it GROUP BY http_status ORDER BY http_status`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates GROUP BY + COUNT on all-String schema

4. **`test_s3_string_functions`** (3 records)
   - Query: `SELECT UPPER(operation) as op_upper, LOWER(bucket) as bucket_lower FROM it`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates string functions with S3's all-String schema

### Squid (3 tests)

Schema: 10 fields, all String-typed. Simplest schema. Note: `remote_host` values include brackets from the regex parser (e.g., `"[192.168.1.1]"`).

1. **`test_squid_filter_by_method`** (5 records, methods: CONNECT, GET, POST, GET, CONNECT) -- **asserts output values**
   - Query: `SELECT method, url FROM it WHERE method = "GET"`
   - Uses `run_format_query_to_vec()`
   - Asserts: result row count is 2, and each returned row has `method` = `Value::String("GET")`.

2. **`test_squid_like_on_url`** (5 records, varied URLs)
   - Query: `SELECT url FROM it WHERE url LIKE "%dropbox%"`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates LIKE against Squid URL field

3. **`test_squid_count_by_status`** (8 records, mixed statuses)
   - Query: `SELECT code_and_status, COUNT(*) as cnt FROM it GROUP BY code_and_status`
   - Uses `run_format_query()`, asserts `Ok(())`
   - Validates aggregation on Squid's combined code/status field

## Fake Data Templates

### ELB line (17 fields)
```
2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/path HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-targets/1234567890123456 "Root=1-58337262-36d228ad5d99923122bbe354"
```

Field mapping:
| # | Field | Value |
|---|-------|-------|
| 1 | timestamp | `2019-06-07T18:45:33.559871Z` |
| 2 | elbname | `elb1` |
| 3 | client_and_port | `78.168.134.92:4586` |
| 4 | backend_and_port | `10.0.0.215:80` |
| 5 | request_processing_time | `0.000036` |
| 6 | backend_processing_time | `0.001035` |
| 7 | response_processing_time | `0.000025` |
| 8 | elb_status_code | `200` |
| 9 | backend_status_code | `200` |
| 10 | received_bytes | `0` |
| 11 | sent_bytes | `42355` |
| 12 | request | `"GET https://example.com:443/path HTTP/1.1"` |
| 13 | user_agent | `"Mozilla/5.0"` |
| 14 | ssl_cipher | `ECDHE-RSA-AES128-GCM-SHA256` |
| 15 | ssl_protocol | `TLSv1.2` |
| 16 | target_group_arn | `arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-targets/1234567890123456` |
| 17 | trace_id | `"Root=1-58337262-36d228ad5d99923122bbe354"` |

### ALB line (25 fields)
```
http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-"
```

### S3 line (24 fields)
```
79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.OBJECT images/photo.jpg "GET /mybucket/images/photo.jpg HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk-java/1.11" - abc123= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2
```

### Squid line (10 fields)
```
1515734740.494      1 [192.168.1.1] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html
```

## Output Verification Summary

| Test | Assertion Type | What is checked |
|------|---------------|-----------------|
| `test_elb_select_and_where_filter` | Values | Row count = 2, `elb_status_code` = "200" |
| `test_elb_numeric_aggregation` | Ok(()) | Float/Int aggregation completes |
| `test_elb_order_by_timestamp` | Ok(()) | DateTime sorting completes |
| `test_elb_limit_with_order` | Ok(()) | Int ordering + limit completes |
| `test_alb_filter_by_type` | Values | Row count = 1, `type` = "https" |
| `test_alb_aggregate_processing_times` | Ok(()) | Float aggregation completes |
| `test_alb_order_by_received_bytes` | Ok(()) | Int ordering + limit completes |
| `test_s3_filter_by_operation` | Values | Row count = 3, `operation` = "REST.GET.OBJECT" |
| `test_s3_dash_placeholders` | Ok(()) | `-` placeholder filtering completes |
| `test_s3_group_by_http_status` | Ok(()) | GROUP BY on String schema completes |
| `test_s3_string_functions` | Ok(()) | String functions complete |
| `test_squid_filter_by_method` | Values | Row count = 2, `method` = "GET" |
| `test_squid_like_on_url` | Ok(()) | LIKE on URL field completes |
| `test_squid_count_by_status` | Ok(()) | Aggregation completes |

4 of 14 tests (one per format) verify actual output values. The remaining 10 assert `Ok(())` where error-free execution is sufficient to confirm the format parses without issues.

## Total: 14 new integration tests
