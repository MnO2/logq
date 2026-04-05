# Datasource Format Integration Tests Design

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

## Approach

- Tests live in `#[cfg(test)] mod tests` in `src/app.rs`, following existing patterns
- Each test creates a temp file via `tempdir()`, writes fake log lines, and runs queries via `app::run()`
- Record count varies per test: 3 for simple SELECT/WHERE, 5-8 for aggregation
- Tests are format-appropriate: they exercise each format's unique field types and schema, not generic query features (JSONL already covers those)

## Test Plan

### ELB (4 tests)

Schema highlights: DateTime, Host, Float (processing times), Int (bytes), HttpRequest, String (status codes).

1. **`test_elb_select_and_where_filter`** (3 records)
   - SELECT specific fields, WHERE `elb_status_code` = "200"
   - Validates String-typed status codes filter correctly

2. **`test_elb_numeric_aggregation`** (5 records)
   - GROUP BY `elb_status_code`, SUM on `backend_processing_time` (Float), SUM on `sent_bytes` (Int)
   - Validates Float and Int types flow through aggregation

3. **`test_elb_order_by_timestamp`** (3 records, out-of-order timestamps)
   - ORDER BY `timestamp` ASC
   - Validates DateTime comparison and sorting

4. **`test_elb_limit_with_order`** (5 records)
   - ORDER BY `sent_bytes` DESC LIMIT 2
   - Validates Int ordering + limit interaction

### ALB (3 tests)

Schema highlights: Same types as ELB but 25 fields, leading `type` column, different field names.

1. **`test_alb_filter_by_type`** (3 records, types: "http", "https", "h2")
   - WHERE `type` = "https"
   - Validates ALB-specific leading `type` field parses and filters correctly

2. **`test_alb_aggregate_processing_times`** (5 records)
   - SELECT `elb_status_code`, SUM(`request_processing_time`) GROUP BY `elb_status_code`
   - Validates Float aggregation across ALB schema

3. **`test_alb_order_by_received_bytes`** (5 records)
   - ORDER BY `received_bytes` DESC LIMIT 3
   - Validates Int parsing and ordering in ALB context

### S3 (4 tests)

Schema highlights: All 24 fields are String-typed. Focus on string operations.

1. **`test_s3_filter_by_operation`** (5 records)
   - WHERE `operation` = "REST.GET.OBJECT"
   - Validates basic equality filtering on all-String schema

2. **`test_s3_like_on_key`** (5 records, varied S3 keys)
   - WHERE `key` LIKE "images/%"
   - Validates LIKE pattern matching against S3 key paths

3. **`test_s3_group_by_http_status`** (8 records, mixed 200/403/404)
   - SELECT `http_status`, COUNT(*) GROUP BY `http_status` ORDER BY `http_status`
   - Validates GROUP BY + COUNT on all-String schema

4. **`test_s3_string_functions`** (3 records)
   - SELECT UPPER(`operation`), LOWER(`bucket`)
   - Validates string functions with S3's all-String schema

### Squid (3 tests)

Schema highlights: 10 fields, all String-typed. Simplest schema.

1. **`test_squid_filter_by_method`** (5 records, methods: CONNECT, GET, POST, GET, CONNECT)
   - WHERE `method` = "GET"
   - Validates filtering on Squid's method field

2. **`test_squid_like_on_url`** (5 records, varied URLs)
   - WHERE `url` LIKE "%dropbox%"
   - Validates LIKE against Squid URL field

3. **`test_squid_count_by_status`** (8 records, mixed statuses)
   - SELECT `code_and_status`, COUNT(*) GROUP BY `code_and_status`
   - Validates aggregation on Squid's combined code/status field

## Fake Data Templates

### ELB line (17 fields)
```
2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/path HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
```

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

## Total: 14 new integration tests
