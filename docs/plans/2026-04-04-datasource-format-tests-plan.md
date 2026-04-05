# Plan: Datasource Format Integration Tests

**Goal**: Add 14 integration tests across ELB, ALB, S3, and Squid formats with a `run_to_vec()` helper for output verification, ensuring all log formats parse and query correctly end-to-end.
**Architecture**: Tests in `src/app.rs` `#[cfg(test)]` module; new `run_to_vec()` function alongside existing `run()`; two test helpers (`run_format_query`, `run_format_query_to_vec`) to reduce boilerplate.
**Tech Stack**: Rust, cargo test, tempfile crate (already a dev-dependency)

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Step 1 (run_to_vec + helpers) | No | `src/app.rs` |
| 2 | Steps 2-5 (ELB tests) | No (appending to same file) | `src/app.rs` |
| 3 | Steps 6-8 (ALB tests) | No (appending to same file) | `src/app.rs` |
| 4 | Steps 9-12 (S3 tests) | No (appending to same file) | `src/app.rs` |
| 5 | Steps 13-15 (Squid tests) | No (appending to same file) | `src/app.rs` |
| 6 | Step 16 (full verification) | No (depends on all) | — |

Groups 2-5 are sequential because they all modify the same file (`src/app.rs`), but each step within a group is small (~2-5 min).

---

## Step 1: Add `run_to_vec()` and test helper functions

**File**: `src/app.rs`

### 1a. Write `run_to_vec()` function

Add this function after the existing `run()` function (after line ~198):

```rust
#[cfg(test)]
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

### 1b. Write test helper functions

Add these helper functions inside the `#[cfg(test)] mod tests` block, after the existing imports:

```rust
fn run_format_query(format: &str, lines: &[&str], query: &str) -> AppResult<()> {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(file_path.clone()).unwrap();
    for line in lines {
        writeln!(file, "{}", line).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    let data_source =
        common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
    let result = run(query, data_source, OutputMode::Csv);
    dir.close().unwrap();
    result
}

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

    let data_source =
        common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
    let result = run_to_vec(query, data_source);
    dir.close().unwrap();
    result
}
```

### 1c. Verify helpers compile

```bash
cd /Users/paulmeng/Develop/logq && cargo test --no-run
```

---

## Step 2: Add `test_elb_select_and_where_filter` (asserts output values)

**File**: `src/app.rs`

### 2a. Write test

```rust
#[test]
fn test_elb_select_and_where_filter() {
    let lines = &[
        r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/path HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
        r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 500 500 0 1024 "GET https://example.com:443/error HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
        r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 8192 "GET https://example.com:443/ok HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
    ];
    let results = run_format_query_to_vec("elb", lines, r#"SELECT elb_status_code, sent_bytes FROM it WHERE elb_status_code = "200""#).unwrap();
    assert_eq!(results.len(), 2);
    for row in &results {
        let status = &row.iter().find(|(k, _)| k == "elb_status_code").unwrap().1;
        assert_eq!(status, &common::types::Value::String("200".to_string()));
    }
}
```

### 2b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_elb_select_and_where_filter -- --nocapture
```

---

## Step 3: Add `test_elb_numeric_aggregation`

**File**: `src/app.rs`

### 3a. Write test

```rust
#[test]
fn test_elb_numeric_aggregation() {
    let lines = &[
        r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
        r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 1024 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
        r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 500 500 0 8192 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
        r#"2019-06-07T18:45:36.559871Z elb1 78.168.134.95:4589 10.0.0.218:80 0.000060 0.004000 0.000040 500 500 0 2048 "GET https://example.com:443/d HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-004""#,
        r#"2019-06-07T18:45:37.559871Z elb1 78.168.134.96:4590 10.0.0.219:80 0.000070 0.005000 0.000045 200 200 0 512 "GET https://example.com:443/e HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-005""#,
    ];
    let result = run_format_query("elb", lines, r#"SELECT elb_status_code, sum(backend_processing_time) as total_bpt, sum(sent_bytes) as total_bytes FROM it GROUP BY elb_status_code"#);
    assert_eq!(result, Ok(()));
}
```

### 3b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_elb_numeric_aggregation -- --nocapture
```

---

## Step 4: Add `test_elb_order_by_timestamp`

**File**: `src/app.rs`

### 4a. Write test

```rust
#[test]
fn test_elb_order_by_timestamp() {
    // Out-of-order timestamps
    let lines = &[
        r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 8192 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
        r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
        r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 1024 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
    ];
    let result = run_format_query("elb", lines, r#"SELECT timestamp FROM it ORDER BY timestamp ASC"#);
    assert_eq!(result, Ok(()));
}
```

### 4b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_elb_order_by_timestamp -- --nocapture
```

---

## Step 5: Add `test_elb_limit_with_order`

**File**: `src/app.rs`

### 5a. Write test

```rust
#[test]
fn test_elb_limit_with_order() {
    let lines = &[
        r#"2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 100 "GET https://example.com:443/a HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-001""#,
        r#"2019-06-07T18:45:34.559871Z elb1 78.168.134.93:4587 10.0.0.216:80 0.000040 0.002000 0.000030 200 200 0 500 "GET https://example.com:443/b HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-002""#,
        r#"2019-06-07T18:45:35.559871Z elb1 78.168.134.94:4588 10.0.0.217:80 0.000050 0.003000 0.000035 200 200 0 300 "GET https://example.com:443/c HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-003""#,
        r#"2019-06-07T18:45:36.559871Z elb1 78.168.134.95:4589 10.0.0.218:80 0.000060 0.004000 0.000040 200 200 0 900 "GET https://example.com:443/d HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-004""#,
        r#"2019-06-07T18:45:37.559871Z elb1 78.168.134.96:4590 10.0.0.219:80 0.000070 0.005000 0.000045 200 200 0 200 "GET https://example.com:443/e HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/1234 "Root=1-005""#,
    ];
    let result = run_format_query("elb", lines, r#"SELECT sent_bytes FROM it ORDER BY sent_bytes DESC LIMIT 2"#);
    assert_eq!(result, Ok(()));
}
```

### 5b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_elb_limit_with_order -- --nocapture
```

---

## Step 6: Add `test_alb_filter_by_type` (asserts output values)

**File**: `src/app.rs`

### 6a. Write test

```rust
#[test]
fn test_alb_filter_by_type() {
    let lines = &[
        r#"http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
        r#"https 2018-07-02T22:23:01.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.40:2818 10.0.0.2:80 0.001 0.002 0.001 200 200 50 512 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-002" "www.example.com" "arn:aws:acm:cert/123" 1 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
        r#"h2 2018-07-02T22:23:02.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.41:2819 10.0.0.3:80 0.002 0.003 0.002 301 301 0 128 "GET https://www.example.com:443/redirect HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-003" "www.example.com" "arn:aws:acm:cert/456" 2 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
    ];
    let results = run_format_query_to_vec("alb", lines, r#"SELECT type, elb_status_code FROM it WHERE type = "https""#).unwrap();
    assert_eq!(results.len(), 1);
    let type_val = &results[0].iter().find(|(k, _)| k == "type").unwrap().1;
    assert_eq!(type_val, &common::types::Value::String("https".to_string()));
}
```

### 6b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_alb_filter_by_type -- --nocapture
```

---

## Step 7: Add `test_alb_aggregate_processing_times`

**File**: `src/app.rs`

### 7a. Write test

```rust
#[test]
fn test_alb_aggregate_processing_times() {
    let lines = &[
        r#"http 2018-07-02T22:23:00.186641Z app/lb/1 192.168.1.1:1000 10.0.0.1:80 0.001 0.010 0.001 200 200 100 1000 "GET http://www.example.com:80/a HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:01.186641Z app/lb/1 192.168.1.2:1001 10.0.0.2:80 0.002 0.020 0.002 200 200 200 2000 "GET http://www.example.com:80/b HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-002" "-" "-" 0 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:02.186641Z app/lb/1 192.168.1.3:1002 10.0.0.3:80 0.003 0.030 0.003 500 500 300 3000 "GET http://www.example.com:80/c HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-003" "-" "-" 0 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:03.186641Z app/lb/1 192.168.1.4:1003 10.0.0.4:80 0.004 0.040 0.004 500 500 400 4000 "GET http://www.example.com:80/d HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-004" "-" "-" 0 2018-07-02T22:22:51.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:04.186641Z app/lb/1 192.168.1.5:1004 10.0.0.5:80 0.005 0.050 0.005 200 200 500 5000 "GET http://www.example.com:80/e HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-005" "-" "-" 0 2018-07-02T22:22:52.364000Z "forward" "-" "-""#,
    ];
    let result = run_format_query("alb", lines, r#"SELECT elb_status_code, sum(request_processing_time) as total_rpt FROM it GROUP BY elb_status_code"#);
    assert_eq!(result, Ok(()));
}
```

### 7b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_alb_aggregate_processing_times -- --nocapture
```

---

## Step 8: Add `test_alb_order_by_received_bytes`

**File**: `src/app.rs`

### 8a. Write test

```rust
#[test]
fn test_alb_order_by_received_bytes() {
    let lines = &[
        r#"http 2018-07-02T22:23:00.186641Z app/lb/1 192.168.1.1:1000 10.0.0.1:80 0.001 0.010 0.001 200 200 100 1000 "GET http://www.example.com:80/a HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-001" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:01.186641Z app/lb/1 192.168.1.2:1001 10.0.0.2:80 0.002 0.020 0.002 200 200 500 2000 "GET http://www.example.com:80/b HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-002" "-" "-" 0 2018-07-02T22:22:49.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:02.186641Z app/lb/1 192.168.1.3:1002 10.0.0.3:80 0.003 0.030 0.003 200 200 300 3000 "GET http://www.example.com:80/c HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-003" "-" "-" 0 2018-07-02T22:22:50.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:03.186641Z app/lb/1 192.168.1.4:1003 10.0.0.4:80 0.004 0.040 0.004 200 200 50 4000 "GET http://www.example.com:80/d HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-004" "-" "-" 0 2018-07-02T22:22:51.364000Z "forward" "-" "-""#,
        r#"http 2018-07-02T22:23:04.186641Z app/lb/1 192.168.1.5:1004 10.0.0.5:80 0.005 0.050 0.005 200 200 800 5000 "GET http://www.example.com:80/e HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/tg/1 "Root=1-005" "-" "-" 0 2018-07-02T22:22:52.364000Z "forward" "-" "-""#,
    ];
    let result = run_format_query("alb", lines, r#"SELECT received_bytes FROM it ORDER BY received_bytes DESC LIMIT 3"#);
    assert_eq!(result, Ok(()));
}
```

### 8b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_alb_order_by_received_bytes -- --nocapture
```

---

## Step 9: Add `test_s3_filter_by_operation` (asserts output values)

**File**: `src/app.rs`

### 9a. Write test

```rust
#[test]
fn test_s3_filter_by_operation() {
    let lines = &[
        r#"owner1 mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT images/photo.jpg "GET /mybucket/images/photo.jpg HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.PUT.OBJECT docs/report.pdf "PUT /mybucket/docs/report.pdf HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.OBJECT data/export.csv "GET /mybucket/data/export.csv HTTP/1.1" 200 - 4096 4096 70 30 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:03:00 +0000] 192.0.2.6 owner1 REQ004 REST.DELETE.OBJECT old/file.tmp "DELETE /mybucket/old/file.tmp HTTP/1.1" 204 - - - 40 15 "-" "aws-sdk/1.0" - abc4= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:04:00 +0000] 192.0.2.7 owner1 REQ005 REST.GET.OBJECT images/logo.png "GET /mybucket/images/logo.png HTTP/1.1" 200 - 512 512 30 5 "-" "aws-sdk/1.0" - abc5= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
    ];
    let results = run_format_query_to_vec("s3", lines, r#"SELECT operation, http_status FROM it WHERE operation = "REST.GET.OBJECT""#).unwrap();
    assert_eq!(results.len(), 3);
    for row in &results {
        let op = &row.iter().find(|(k, _)| k == "operation").unwrap().1;
        assert_eq!(op, &common::types::Value::String("REST.GET.OBJECT".to_string()));
    }
}
```

### 9b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_s3_filter_by_operation -- --nocapture
```

---

## Step 10: Add `test_s3_dash_placeholders`

**File**: `src/app.rs`

### 10a. Write test

```rust
#[test]
fn test_s3_dash_placeholders() {
    let lines = &[
        r#"owner1 mybucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /mybucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.GET.OBJECT key2 "GET /mybucket/key2 HTTP/1.1" 403 AccessDenied 0 0 10 5 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 mybucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.OBJECT key3 "GET /mybucket/key3 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader mybucket.s3.amazonaws.com TLSv1.2"#,
    ];
    let result = run_format_query("s3", lines, r#"SELECT error_code, refererr FROM it WHERE error_code = "-""#);
    assert_eq!(result, Ok(()));
}
```

### 10b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_s3_dash_placeholders -- --nocapture
```

---

## Step 11: Add `test_s3_group_by_http_status`

**File**: `src/app.rs`

### 11a. Write test

```rust
#[test]
fn test_s3_group_by_http_status() {
    let lines = &[
        r#"owner1 bkt [01/Jan/2020:00:00:00 +0000] 10.0.0.1 owner1 R1 REST.GET.OBJECT k1 "GET /bkt/k1 HTTP/1.1" 200 - 100 100 10 5 "-" "sdk/1" - a= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:01:00 +0000] 10.0.0.2 owner1 R2 REST.GET.OBJECT k2 "GET /bkt/k2 HTTP/1.1" 200 - 200 200 20 10 "-" "sdk/1" - b= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:02:00 +0000] 10.0.0.3 owner1 R3 REST.GET.OBJECT k3 "GET /bkt/k3 HTTP/1.1" 403 AccessDenied 0 0 5 2 "-" "sdk/1" - c= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:03:00 +0000] 10.0.0.4 owner1 R4 REST.GET.OBJECT k4 "GET /bkt/k4 HTTP/1.1" 200 - 300 300 30 15 "-" "sdk/1" - d= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:04:00 +0000] 10.0.0.5 owner1 R5 REST.GET.OBJECT k5 "GET /bkt/k5 HTTP/1.1" 404 NoSuchKey 0 0 8 3 "-" "sdk/1" - e= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:05:00 +0000] 10.0.0.6 owner1 R6 REST.GET.OBJECT k6 "GET /bkt/k6 HTTP/1.1" 403 AccessDenied 0 0 6 2 "-" "sdk/1" - f= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:06:00 +0000] 10.0.0.7 owner1 R7 REST.GET.OBJECT k7 "GET /bkt/k7 HTTP/1.1" 200 - 400 400 40 20 "-" "sdk/1" - g= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 bkt [01/Jan/2020:00:07:00 +0000] 10.0.0.8 owner1 R8 REST.GET.OBJECT k8 "GET /bkt/k8 HTTP/1.1" 404 NoSuchKey 0 0 9 4 "-" "sdk/1" - h= SigV4 AES AuthHeader bkt.s3.amazonaws.com TLSv1.2"#,
    ];
    let result = run_format_query("s3", lines, r#"SELECT http_status, count(*) as cnt FROM it GROUP BY http_status ORDER BY http_status"#);
    assert_eq!(result, Ok(()));
}
```

### 11b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_s3_group_by_http_status -- --nocapture
```

---

## Step 12: Add `test_s3_string_functions`

**File**: `src/app.rs`

### 12a. Write test

```rust
#[test]
fn test_s3_string_functions() {
    let lines = &[
        r#"owner1 MyBucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 owner1 REQ001 REST.GET.OBJECT key1 "GET /MyBucket/key1 HTTP/1.1" 200 - 1024 1024 50 10 "-" "aws-sdk/1.0" - abc1= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader MyBucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 AnotherBucket [06/Feb/2019:00:01:00 +0000] 192.0.2.4 owner1 REQ002 REST.PUT.OBJECT key2 "PUT /AnotherBucket/key2 HTTP/1.1" 200 - 2048 2048 60 20 "-" "aws-sdk/1.0" - abc2= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader AnotherBucket.s3.amazonaws.com TLSv1.2"#,
        r#"owner1 ThirdBucket [06/Feb/2019:00:02:00 +0000] 192.0.2.5 owner1 REQ003 REST.GET.VERSIONING key3 "GET /ThirdBucket/key3 HTTP/1.1" 200 - 512 512 30 5 "-" "aws-sdk/1.0" - abc3= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader ThirdBucket.s3.amazonaws.com TLSv1.2"#,
    ];
    let result = run_format_query("s3", lines, r#"SELECT upper(operation) as op_upper, lower(bucket) as bucket_lower FROM it"#);
    assert_eq!(result, Ok(()));
}
```

### 12b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_s3_string_functions -- --nocapture
```

---

## Step 13: Add `test_squid_filter_by_method` (asserts output values)

**File**: `src/app.rs`

### 13a. Write test

```rust
#[test]
fn test_squid_filter_by_method() {
    let lines = &[
        r#"1515734740.494      1 [192.168.1.1] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#,
        r#"1515734741.100      5 [192.168.1.2] TCP_MISS/200 15234 GET http://www.google.com/ - HIER_DIRECT/216.58.214.196 text/html"#,
        r#"1515734742.200     10 [192.168.1.3] TCP_MISS/200 8432 POST http://api.example.com/data - HIER_DIRECT/93.184.216.34 application/json"#,
        r#"1515734743.300      2 [192.168.1.4] TCP_HIT/200 12045 GET http://www.github.com/ - HIER_DIRECT/140.82.121.3 text/html"#,
        r#"1515734744.400      3 [192.168.1.5] TCP_DENIED/403 2100 CONNECT slack.com:443 - HIER_NONE/- text/html"#,
    ];
    let results = run_format_query_to_vec("squid", lines, r#"SELECT method, url FROM it WHERE method = "GET""#).unwrap();
    assert_eq!(results.len(), 2);
    for row in &results {
        let method = &row.iter().find(|(k, _)| k == "method").unwrap().1;
        assert_eq!(method, &common::types::Value::String("GET".to_string()));
    }
}
```

### 13b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_squid_filter_by_method -- --nocapture
```

---

## Step 14: Add `test_squid_like_on_url`

**File**: `src/app.rs`

### 14a. Write test

```rust
#[test]
fn test_squid_like_on_url() {
    let lines = &[
        r#"1515734740.494      1 [192.168.1.1] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#,
        r#"1515734741.100      5 [192.168.1.2] TCP_MISS/200 15234 GET http://www.google.com/ - HIER_DIRECT/216.58.214.196 text/html"#,
        r#"1515734742.200     10 [192.168.1.3] TCP_MISS/200 8432 GET http://www.github.com/ - HIER_DIRECT/140.82.121.3 text/html"#,
        r#"1515734743.300      2 [192.168.1.4] TCP_DENIED/407 4100 CONNECT dl.dropbox.com:443 - HIER_NONE/- text/html"#,
        r#"1515734744.400      3 [192.168.1.5] TCP_MISS/200 9200 GET http://slack.com/ - HIER_DIRECT/34.230.68.40 text/html"#,
    ];
    let result = run_format_query("squid", lines, r#"SELECT url FROM it WHERE url LIKE "%dropbox%""#);
    assert_eq!(result, Ok(()));
}
```

### 14b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_squid_like_on_url -- --nocapture
```

---

## Step 15: Add `test_squid_count_by_status`

**File**: `src/app.rs`

### 15a. Write test

```rust
#[test]
fn test_squid_count_by_status() {
    let lines = &[
        r#"1515734740.000      1 [10.0.0.1] TCP_DENIED/407 3922 CONNECT a.com:443 - HIER_NONE/- text/html"#,
        r#"1515734741.000      2 [10.0.0.2] TCP_MISS/200 15234 GET http://b.com/ - HIER_DIRECT/1.2.3.4 text/html"#,
        r#"1515734742.000      3 [10.0.0.3] TCP_HIT/200 8432 GET http://c.com/ - HIER_DIRECT/1.2.3.5 text/html"#,
        r#"1515734743.000      4 [10.0.0.4] TCP_DENIED/403 2100 CONNECT d.com:443 - HIER_NONE/- text/html"#,
        r#"1515734744.000      5 [10.0.0.5] TCP_MISS/200 9200 GET http://e.com/ - HIER_DIRECT/1.2.3.6 text/html"#,
        r#"1515734745.000      6 [10.0.0.6] TCP_DENIED/407 4100 CONNECT f.com:443 - HIER_NONE/- text/html"#,
        r#"1515734746.000      7 [10.0.0.7] TCP_HIT/200 12000 GET http://g.com/ - HIER_DIRECT/1.2.3.7 text/html"#,
        r#"1515734747.000      8 [10.0.0.8] TCP_DENIED/403 1800 CONNECT h.com:443 - HIER_NONE/- text/html"#,
    ];
    let result = run_format_query("squid", lines, r#"SELECT code_and_status, count(*) as cnt FROM it GROUP BY code_and_status"#);
    assert_eq!(result, Ok(()));
}
```

### 15b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_squid_count_by_status -- --nocapture
```

---

## Step 16: End-to-end verification

### 16a. Run all tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 16b. Verify no regressions

All existing ~58 tests must continue to pass. The 14 new tests should all pass. Total test count should be ~72.
