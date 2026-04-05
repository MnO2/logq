VERDICT: NEEDS_REVISION

## Summary Assessment

The design correctly identifies the coverage gap and proposes a reasonable set of 14 tests across 4 under-tested formats. However, the ELB fake data template has a wrong field count that will cause test failures, and the test strategy inherits a codebase-wide weakness of never verifying output correctness -- an issue worth addressing given this is the explicit goal of testing format parsing.

## Critical Issues (must fix)

### 1. ELB template produces only 15 tokens, but the schema expects 17 fields

The provided ELB fake data template:
```
2019-06-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/path HTTP/1.1" "Mozilla/5.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
```
tokenizes to 15 fields (up through `ssl_protocol`). Fields 16 (`target_group_arn`) and 17 (`trace_id`) will be padded as `Value::Null`. This is not a showstopper for the four ELB tests proposed (none reference those fields), but the design claims "17 fields" and the template does not match that claim. When someone generates variations of this template for the 5-record aggregation tests, they may not realize fields are missing. The template should either be documented as intentionally truncated (with the Null-padding behavior noted) or extended to include all 17 fields to avoid confusion.

### 2. Tests only assert `Ok(())` -- format parsing correctness is never verified

Every proposed test follows the existing pattern of `assert_eq!(result, Ok(()))`, which only checks that the query pipeline did not error. This means:
- A record could parse with wrong field values and the test would still pass.
- A WHERE filter could return 0 rows or all rows and the test would still pass.
- An ORDER BY could return rows in wrong order and the test would still pass.
- SUM/COUNT aggregations could produce wrong values and the test would still pass.

Since the stated goal is to "verify each supported log format can be parsed and queried end-to-end," not verifying the actual output undermines the purpose. A parsing regression that silently produces wrong values (e.g., field misalignment causing `sent_bytes` to contain a status code string instead of an integer) would pass all these tests because the pipeline might still succeed without erroring.

**Recommendation**: At minimum, refactor `run()` to accept a `Write` trait object (or add a `run_to_string()` variant), then assert on output content for at least one test per format. Alternatively, if refactoring `run()` is out of scope, add a note in the design acknowledging this limitation and create a follow-up task. At the very least, tests with deterministic expected outputs (like COUNT, SUM with known inputs) should verify results.

## Suggestions (nice to have)

### 3. Consider testing parse-failure behavior for malformed lines

The design focuses entirely on happy-path parsing. One test per format with a truncated or corrupted line would verify that error handling works correctly for each format's parser path (e.g., an ELB line with a non-numeric value in the `sent_bytes` position). The existing `test_reader_on_malformed_input` only covers ELB; ALB, S3, and Squid malformed-input handling is untested.

### 4. ALB schema description is slightly misleading

The design says ALB has "Same types as ELB but 25 fields." This is imprecise -- ALB has a leading `type` String field, uses `target_processing_time`/`target_status_code` instead of ELB's `backend_processing_time`/`backend_status_code`, and positions 20-24 are all String (including `matched_rule_priority` which is numerically valued but String-typed). The queries chosen happen to avoid these naming pitfalls, but the design text could clarify that the field name differences are real and consequential for query authoring.

### 5. The Squid `remote_host` value includes brackets

The Squid regex parser captures bracket-enclosed tokens including the brackets themselves. So for the template `[192.168.1.1]`, the stored value will be `"[192.168.1.1]"` (with brackets). None of the proposed Squid tests query `remote_host`, so this is not a problem, but it's worth noting in case someone extends these tests later.

### 6. Test naming could indicate format-specific vs. generic query testing more clearly

The design correctly states tests should be "format-appropriate" and not duplicate JSONL's generic query coverage. This is a good principle. However, `test_s3_like_on_key` and `test_squid_like_on_url` are effectively testing LIKE on String fields, which JSONL tests already cover. The value of these tests is really in verifying that the S3/Squid *parsers produce correct field values* that work with LIKE -- but since the tests don't verify output, that value is diminished. Consider refocusing these tests to verify parsing edge cases unique to each format (e.g., S3 fields with `-` placeholders, Squid's whitespace-padded elapsed field).

### 7. Consider a helper function for test boilerplate

Each test creates a tempdir, writes lines, constructs a DataSource, runs a query, and asserts Ok. A helper like `run_format_query(format: &str, lines: &[&str], query: &str) -> AppResult<()>` would reduce boilerplate significantly across 14 tests and make the tests more readable.

## Verified Claims (things I confirmed are correct)

- **Gap analysis is accurate**: JSONL has ~22 tests, ELB has 1, Squid has 1, ALB and S3 have 0 integration tests in `app.rs`.
- **ALB template is correct**: The 25-token ALB template matches the `AWS_ALB_FIELD_NAMES` schema exactly and parses correctly with the existing regex tokenizer.
- **S3 template is correct**: The 24-token S3 template matches the `AWS_S3_FIELD_NAMES` schema and the bracket-enclosed time field parses correctly.
- **Squid template is correct**: The 10-token Squid template matches the `SQUID_FIELD_NAMES` schema. Whitespace padding between fields is handled by the regex.
- **Field types referenced in queries are accurate**: `elb_status_code` is String (not Int) in both ELB and ALB; `backend_processing_time` is Float in ELB; `request_processing_time` is Float in ALB; `received_bytes` is Integral in ALB; `sent_bytes` is Integral in ELB.
- **S3 `key` field will not have quotes**: The token `images/photo.jpg` is not quoted in the log line, so `LIKE "images/%"` will match correctly.
- **Existing test pattern is followed**: All existing tests in `app.rs` use `tempdir()`, write data, run queries via `app::run()`, and assert `Ok(())`.
- **UPPER, LOWER, LIKE, BETWEEN, IN, CAST, GROUP BY, ORDER BY, LIMIT, COUNT, SUM** are all implemented and have existing tests.
- **The Null-padding logic** (datasource.rs lines 769-773) correctly handles lines with fewer tokens than the schema expects by filling remaining fields with `Value::Null`.
