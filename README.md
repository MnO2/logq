# logq - A web-server log file command line toolkit with SQL interface written in Rust

[![Build Status](https://travis-ci.com/MnO2/logq.svg?branch=master)](https://travis-ci.com/MnO2/logq)
[![codecov](https://codecov.io/gh/MnO2/logq/branch/master/graph/badge.svg)](https://codecov.io/gh/MnO2/logq)


This project is in alpha stage, PRs are welcomed.

logq is a command line program for easily analyzing, querying, aggregating and
joining log files. Right now only AWS Elastic Load Balancer's log format is
supported, since it is where this project was inspired.


## Installation

```
cargo install logq
```

## Examples

Project the columns of `timestamp` and `backend_and_port` field and print the first three records out.

```
> logq query 'select timestamp, backend_processing_time from elb order by timestamp asc limit 3' data/AWSLogs.log

+-----------------------------------+----------+
| 2015-11-07 18:45:33.007671 +00:00 | 0.618779 |
+-----------------------------------+----------+
| 2015-11-07 18:45:33.054086 +00:00 | 0.654135 |
+-----------------------------------+----------+
| 2015-11-07 18:45:33.094266 +00:00 | 0.506634 |
+-----------------------------------+----------+
```


Summing up the total sent bytes in 5 seconds time frame.
```
> logq query 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from elb group by t' data/AWSLogs.log
+----------------------------+----------+
| 2015-11-07 18:45:30 +00:00 | 12256229 |
+----------------------------+----------+
| 2015-11-07 18:45:35 +00:00 | 33148328 |
+----------------------------+----------+
```


Select the 90th percentile backend_processsing_time.
```
> logq query 'select time_bucket("5 seconds", timestamp) as t, percentile_disc(0.9) within group (order by backend_processing_time asc) as bps from elb group by t' data/AWSLogs.log
+----------------------------+----------+
| 2015-11-07 18:45:30 +00:00 | 0.112312 |
+----------------------------+----------+
| 2015-11-07 18:45:35 +00:00 | 0.088791 |
+----------------------------+----------+
```

To collapse the part of the url path so that they are mapping to the same Restful handler, you could use `url_path_bucket`
```
> logq query 'select time_bucket("5 seconds", timestamp) as t, url_path_bucket(request, 1, "_") as s from elb limit 10' data/AWSLogs.log
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /                                            |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /img/_/000000000000000000000000              |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /favicons/_                                  |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /images/_/devices.png                        |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /stylesheets/_/font-awesome.css              |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /favicons/_                                  |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /mobile/_/register-push                      |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /img/_/205/2r1/562e37d9208bee5b70f56836.anim |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /img/_/300/2r0/54558148eab71c6c2517f1d9.jpg  |
+----------------------------+----------------------------------------------+
| 2015-11-07 18:45:30 +00:00 | /                                            |
+----------------------------+----------------------------------------------+
```

To output in different format, you can specify the format by `--output`, it supports `json` and `csv` at this moment.
```
> logq query --output csv 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from elb group by t' data/AWSLogs.log
2015-11-07 18:45:35 +00:00,33148328
2015-11-07 18:45:30 +00:00,12256229
```

```
> logq query --output json 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from elb group by t' data/AWSLogs.log
[{"t":"2015-11-07 18:45:30 +00:00","s":12256229},{"t":"2015-11-07 18:45:35 +00:00","s":33148328}]
```

You can use graphing command line tools to draw
```
> logq query --output csv 'select backend_and_port, sum(sent_bytes) from elb group by backend_and_port' data/AWSLogs.log | termgraph

10.0.2.143:80: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 20014156.00
10.0.0.215:80: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 25390392.00
```


If you are unclear how the execution was running, you can explain the query.
```
> logq explain 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from elb group by t'
Query Plan:
GroupBy(["t"], [NamedAggregate { aggregate: Sum(SumAggregate { sums: {} }, Expression(Variable("sent_bytes"), Some("sent_bytes"))), name_opt: Some("s") }], Map([Expression(Function("time_bucket", [Expression(Variable("const_000000000"), None), Expression(Variable("timestamp"), Some("timestamp"))]), Some("t")), Expression(Variable("sent_bytes"), Some("sent_bytes"))], DataSource(Stdin)))
```

To know what are the fields, here is the table schema.
```
> logq schema elb
+--------------------------+-------------+
| timestamp                | DateTime    |
+--------------------------+-------------+
| elbname                  | String      |
+--------------------------+-------------+
| client_and_port          | Host        |
+--------------------------+-------------+
| backend_and_port         | Host        |
+--------------------------+-------------+
| request_processing_time  | Float       |
+--------------------------+-------------+
| backend_processing_time  | Float       |
+--------------------------+-------------+
| response_processing_time | Float       |
+--------------------------+-------------+
| elb_status_code          | String      |
+--------------------------+-------------+
| backend_status_code      | String      |
+--------------------------+-------------+
| received_bytes           | Integral    |
+--------------------------+-------------+
| sent_bytes               | Integral    |
+--------------------------+-------------+
| request                  | HttpRequest |
+--------------------------+-------------+
| user_agent               | String      |
+--------------------------+-------------+
| ssl_cipher               | String      |
+--------------------------+-------------+
| ssl_protocol             | String      |
+--------------------------+-------------+
| target_group_arn         | String      |
+--------------------------+-------------+
| trace_id                 | String      |
+--------------------------+-------------+
```

To know the supported log format at this moment
```
> logq schema 
The supported log format
* elb
```

## Motivation

The very same criticisms to xsv could also be asked to this project.

That is, you shouldn't be working with log file directly in a large scale settings, you should ship it to stack like ELK for searching and analyzing. For more advanced need you could leverage on Spark etc. However, in the daily work when you
would like to quickly troubleshooting things, oftentime you are still handed with several gigabytes of log file, and it's time consuming to set things up.  Usually the modern laptop/pc is very powerful and enough for analyzing gigabytes of volumes, when the implementation is well performance considered.
This software is inspired by the daily need in the author's daily work, where I
believe many people have the same kind of needs.

### Why not TextQL, or insert the space delimited fields into sqlite?

TextQL is implemented in python ant it's ok for the smaller cases. In the case
of AWS ELB log where it often goes up to gigabytes of logs it is too slow
regarding to speed. Furthermore, either TextQL and sqlite are limited in their
provided SQL functions, it makes the domain processing like URL, and HTTP
headers very hard. You would probably need to answer the questions like "What is
the 99th percentile to this endpoint, by ignoring the user_id in the restful
endpoing within a certain time range". It would be easier to have a software
providing those handy function to extract or canonicalize the information from
the log.


## Roadmap

* Conforms to the much more complicated SQL syntax as [sqlite](https://www.sqlite.org/lang_expr.html)
* Performance optimization, avoid unnecessary parsing
* More supported functions
* time_bucket with arbitrary interval (begin from epoch)
* Window Function
* Implementing approximate_percentile_disc with t-digest algorithm when the input is large.
* Streaming mode to work with `tail -f`
* Customizable Reader, to follow GoAccess's style
* More supported log format
* Plugin quickjs for user-defined functions