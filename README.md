# logq - Query server logs with PartiQL, implemented in Rust

[![Build Status](https://travis-ci.com/MnO2/logq.svg?branch=master)](https://travis-ci.com/MnO2/logq)
[![codecov](https://codecov.io/gh/MnO2/logq/branch/master/graph/badge.svg)](https://codecov.io/gh/MnO2/logq)

logq is a command-line tool for querying and analyzing server log files using [PartiQL](https://partiql.org/), a SQL-compatible query language designed for semi-structured data. It supports structured log formats (ELB, ALB, S3, Squid) and schema-free JSONL logs with nested field access, aggregations, JOINs, subqueries, and set operations.

## Supported Log Formats

| Format | Description |
| --- | --- |
| `elb` | AWS Classic Elastic Load Balancer access logs |
| `alb` | AWS Application Load Balancer access logs |
| `s3` | AWS S3 access logs |
| `squid` | Squid proxy native format |
| `jsonl` | Newline-delimited JSON (schema-free, nested data) |

## Installation

```
cargo install logq
```

## Quick Start

```bash
# Query ELB logs
logq query 'select timestamp, backend_processing_time from it limit 3' \
  --table it:elb=access.log

# Query JSONL logs with nested fields
logq query 'select e.f.g, d[0] from it where a > 1' \
  --table it:jsonl=data.jsonl

# Read from stdin
cat access.log | logq query 'select count(*) from it' --table it:elb=stdin

# Output as JSON or CSV
logq query 'select * from it limit 5' --table it:jsonl=data.jsonl --output json
logq query 'select * from it limit 5' --table it:jsonl=data.jsonl --output csv
```

## SQL Feature Reference

### SELECT and Projection

```sql
-- Column selection and aliases
select timestamp, backend_processing_time as bpt from it

-- Expressions in SELECT
select sent_bytes + received_bytes as total from it

-- Star projection
select * from it limit 10

-- SELECT DISTINCT
select distinct elb_status_code from it

-- SELECT VALUE with constructors
select value {'status': elb_status_code, 'time': backend_processing_time} from it
```

### Filtering

```sql
-- Comparisons
select * from it where backend_processing_time > 1.0

-- Boolean logic with AND/OR/NOT
select * from it where elb_status_code = '500' and sent_bytes > 1000

-- LIKE pattern matching (% = any chars, _ = single char)
select * from it where user_agent like '%Chrome%'

-- BETWEEN
select * from it where backend_processing_time between 0.1 and 0.5

-- IN
select * from it where elb_status_code in ('500', '502', '503')

-- IS NULL / IS MISSING
select * from it where c is missing

-- CASE WHEN
select case when elb_status_code = '200' then 'ok'
            when elb_status_code = '500' then 'error'
            else 'other' end as status
from it
```

### Aggregation and Grouping

```sql
-- Aggregate functions
select count(*), sum(sent_bytes), avg(backend_processing_time),
       min(backend_processing_time), max(backend_processing_time)
from it

-- GROUP BY
select elb_status_code, count(*) as cnt from it group by elb_status_code

-- GROUP BY with time bucketing
select time_bucket('5 seconds', timestamp) as t, sum(sent_bytes) as s
from it group by time_bucket('5 seconds', timestamp) as t

-- HAVING
select elb_status_code, count(*) as cnt from it
group by elb_status_code having count(*) > 10

-- Percentiles
select percentile_disc(0.9) within group (order by backend_processing_time asc) as p90
from it

-- Approximate count distinct (HyperLogLog)
select approx_count_distinct(user_agent) from it
```

### ORDER BY and LIMIT

```sql
select * from it order by backend_processing_time desc limit 10
```

### JOINs

```sql
-- Cross join (explicit)
select * from a cross join b

-- Cross join (comma syntax)
select * from a, b where a.id = b.id

-- Left outer join
select a.name, b.value from a left join b on a.id = b.aid
```

### Subqueries

```sql
-- Scalar subquery in WHERE
select * from it where sent_bytes > (select avg(sent_bytes) from it)

-- Scalar subquery in SELECT
select *, (select max(sent_bytes) from it) as max_bytes from it
```

### Set Operations

```sql
-- Union (deduplicates)
select a from t1 union select a from t2

-- Union all (preserves duplicates)
select a from t1 union all select a from t2

-- Intersect / Except
select a from t1 intersect select a from t2
select a from t1 except select a from t2
```

### JSONL Nested Data

For JSONL input like:
```json
{"a": 1, "b": "hello", "d": [0, 1, 2], "e": {"f": {"g": 1}}}
```

```sql
-- Nested field access
select e.f.g from it

-- Array indexing
select d[0], d[1] from it

-- Path wildcards
select d[*] from it       -- iterate array elements
select e.* from it         -- iterate object fields

-- GROUP BY on nested fields
select x, count(*) from it group by d[0] as x
```

### Type Casting and String Operations

```sql
-- CAST
select cast(elb_status_code as int) from it

-- String concatenation
select 'status: ' || elb_status_code from it

-- String functions
select upper(user_agent), lower(elbname), char_length(user_agent) from it
select substring(user_agent from 1 for 10) from it
select trim(both ' ' from user_agent) from it

-- COALESCE / NULLIF
select coalesce(c, 0) from it
select nullif(a, 0) from it
```

## Functions

### Scalar Functions

| Function | Description | Example |
| --- | --- | --- |
| `url_host(request)` | Extract host from HTTP request | `url_host(request)` |
| `url_port(request)` | Extract port from HTTP request | `url_port(request)` |
| `url_path(request)` | Extract path from HTTP request | `url_path(request)` |
| `url_fragment(request)` | Extract fragment from HTTP request | `url_fragment(request)` |
| `url_query(request)` | Extract query string from HTTP request | `url_query(request)` |
| `url_path_segments(request)` | Extract path segments | `url_path_segments(request)` |
| `url_path_bucket(request, depth, placeholder)` | Canonicalize URL path for grouping | `url_path_bucket(request, 1, "_")` |
| `time_bucket(interval, datetime)` | Bucket timestamp into intervals | `time_bucket('5 seconds', timestamp)` |
| `date_part(unit, datetime)` | Extract part of datetime | `date_part('hour', timestamp)` |
| `host_name(host)` | Extract hostname from host field | `host_name(backend_and_port)` |
| `host_port(host)` | Extract port from host field | `host_port(backend_and_port)` |
| `upper(string)` | Convert to uppercase | `upper(user_agent)` |
| `lower(string)` | Convert to lowercase | `lower(elbname)` |
| `char_length(string)` | Length of string | `char_length(user_agent)` |
| `substring(string from start for length)` | Extract substring | `substring(user_agent from 1 for 10)` |
| `trim(both char from string)` | Trim characters | `trim(both ' ' from user_agent)` |

### Aggregate Functions

| Function | Description |
| --- | --- |
| `count(*)` / `count(expr)` | Count rows |
| `sum(expr)` | Sum of numeric values |
| `avg(expr)` | Average of numeric values |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |
| `first(expr)` | First value in group |
| `last(expr)` | Last value in group |
| `percentile_disc(p) within group (order by expr)` | Exact percentile |
| `approx_percentile(p) within group (order by expr)` | Approximate percentile (t-digest) |
| `approx_count_distinct(expr)` | Approximate distinct count (HyperLogLog) |

## Output Formats

logq supports three output modes via `--output`:

- **`table`** (default) -- formatted ASCII table
- **`csv`** -- comma-separated values, pipe-friendly
- **`json`** -- JSON array of objects

### Piping to Visualization Tools

```bash
# Bar chart with termgraph
logq query --output csv 'select backend_and_port, sum(sent_bytes) from it group by backend_and_port' \
  --table it:elb=data/AWSELB.log | termgraph

# Sparkline with spark
logq query --output csv 'select backend_processing_time from it' \
  --table it:elb=data/AWSELB.log | cut -d, -f1 | spark
```

## Other Commands

### Explain

Print the query plan without executing:

```
logq explain 'select t, sum(sent_bytes) as s from it group by time_bucket("5 seconds", timestamp) as t'
```

### Schema

Show field names and types for a log format:

```
logq schema elb
logq schema alb
```

## Log Format Schemas

### ELB (Classic Elastic Load Balancer)

| Field | Type |
| --- | --- |
| `timestamp` | DateTime |
| `elbname` | String |
| `client_and_port` | Host |
| `backend_and_port` | Host |
| `request_processing_time` | Float |
| `backend_processing_time` | Float |
| `response_processing_time` | Float |
| `elb_status_code` | String |
| `backend_status_code` | String |
| `received_bytes` | Int |
| `sent_bytes` | Int |
| `request` | HttpRequest |
| `user_agent` | String |
| `ssl_cipher` | String |
| `ssl_protocol` | String |
| `target_group_arn` | String |
| `trace_id` | String |

### ALB (Application Load Balancer)

| Field | Type |
| --- | --- |
| `type` | String |
| `timestamp` | DateTime |
| `elb` | String |
| `client_and_port` | Host |
| `target_and_port` | Host |
| `request_processing_time` | Float |
| `target_processing_time` | Float |
| `response_processing_time` | Float |
| `elb_status_code` | String |
| `target_status_code` | String |
| `received_bytes` | Int |
| `sent_bytes` | Int |
| `request` | HttpRequest |
| `user_agent` | String |
| `ssl_cipher` | String |
| `ssl_protocol` | String |
| `target_group_arn` | String |
| `trace_id` | String |
| `domain_name` | String |
| `chosen_cert_arn` | String |
| `matched_rule_priority` | String |
| `request_creation_time` | DateTime |
| `actions_executed` | String |
| `redirect_url` | String |
| `error_reason` | String |

### JSONL

No fixed schema. Fields are auto-detected from each JSON line. Nested objects and arrays are supported with path access (`a.b.c`, `d[0]`).

## Motivation

When troubleshooting production issues, you often need metrics not provided by CloudWatch or ELK. Downloading access logs and writing one-off scripts has several drawbacks:

1. **Time wasted on parsing** -- common log formats should be handled automatically
2. **No reuse** -- each script is thrown away
3. **Performance** -- scripting languages are too slow for multi-GB log files

logq addresses these by providing a fast, Rust-based query engine with built-in parsers for common formats. A modern laptop can comfortably analyze gigabytes of logs without setting up Athena or ELK.

## License

Apache-2.0 OR BSD-3-Clause
