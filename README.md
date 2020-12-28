# logq - Analyzing log files in SQL with command-line toolkit, implemented in Rust

[![Build Status](https://travis-ci.com/MnO2/logq.svg?branch=master)](https://travis-ci.com/MnO2/logq)
[![codecov](https://codecov.io/gh/MnO2/logq/branch/master/graph/badge.svg)](https://codecov.io/gh/MnO2/logq)


This project is in alpha stage, PRs are welcomed.

logq is a command line tool for easily analyzing, querying, aggregating web-server log files though SQL inteface. 
Right now the supported formats are

1. AWS classic elastic load balancer
2. AWS application load balancer
3. AWS S3 Access Log (preliminary support)
4. Squid native format (preliminary support)

More log formats would be supported in the future, and ideally it could be customized through configuration like what GoAccess does.

## Installation

```
cargo install logq
```

## Examples

Project the columns of `timestamp` and `backend_and_port` fields from the log file and print the first three records out.

```
> logq query 'select timestamp, backend_processing_time from it order by timestamp asc limit 3' --table it:elb=data/AWSLogs.log

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
> logq query 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from it group by t' --table it:elb=data/AWSLogs.log
+----------------------------+----------+
| 2015-11-07 18:45:30 +00:00 | 12256229 |
+----------------------------+----------+
| 2015-11-07 18:45:35 +00:00 | 33148328 |
+----------------------------+----------+
```

Select the 90th percentile backend_processsing_time with 5 second as the time frame.
```
> logq query 'select time_bucket("5 seconds", timestamp) as t, percentile_disc(0.9) within group (order by backend_processing_time asc) as bps from it group by t' --table it:elb=data/AWSLogs.log
+----------------------------+----------+
| 2015-11-07 18:45:30 +00:00 | 0.112312 |
+----------------------------+----------+
| 2015-11-07 18:45:35 +00:00 | 0.088791 |
+----------------------------+----------+
```

To collapse the part of the url path so that they are mapping to the same Restful handler, you could use `url_path_bucket`
```
> logq query 'select time_bucket("5 seconds", timestamp) as t, url_path_bucket(request, 1, "_") as s from it limit 10' --table it:elb=data/AWSLogs.log
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

Output in different format, you can specify the format by `--output`, it supports `json` and `csv` at this moment.
```
> logq query --output csv 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from it group by t' --table it:elb=data/AWSLogs.log
2015-11-07 18:45:35 +00:00,33148328
2015-11-07 18:45:30 +00:00,12256229
```

```
> logq query --output json 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from it group by t' --table it:elb=data/AWSLogs.log
[{"t":"2015-11-07 18:45:30 +00:00","s":12256229},{"t":"2015-11-07 18:45:35 +00:00","s":33148328}]
```

You can use graphing command-line tools to graph the data set in terminal. For example, [termgraph](https://github.com/mkaz/termgraph) would be a good choice for bar charts
```
> logq query --output csv 'select backend_and_port, sum(sent_bytes) from it group by backend_and_port' --table it:elb=data/AWSLogs.log | termgraph

10.0.2.143:80: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 20014156.00
10.0.0.215:80: ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 25390392.00
```

Or you could use [spark](https://github.com/holman/spark) to draw the processing time over time
```
> logq query --output csv 'select host_name(backend_and_port) as h, backend_processing_time from it where h = "10.0.2.143"' --table it:elb=data/AWSLogs.log | cut -d, -f2 | spark
▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁█▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁██▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁█▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁
```

If you are unclear how the execution was running, the query plan could be explained.
```
> logq explain 'select time_bucket("5 seconds", timestamp) as t, sum(sent_bytes) as s from it group by t'
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

To know the supported log format at this moment.
```
> logq schema 
The supported log format
* elb
```

## Available Functions

| Function Name | Description | Input Type | Output Type | 
| --- | --- | --- | --- |
| url_host | To retrieve the host from the request | Request | String |
| url_port | To retrieve the port from teh request | Request | String |
| url_path | To retrieve the path from the request | Request | String |
| url_fragment | To retrieve the fragment from the request | Request | String |
| url_query | To retrive the query from the request | Request | String |
| url_path_segments | To retrieve the path segments from the request | Request | String |
| url_path_bucket | To map the path segments into given string | Request, Integral, String | String |
| time_bucket | To bucket the timestamp into given interval | String, DateTime | DateTime |
| date_part | To get the part of the datetime with the given unit | String, DateTime | Float |
| host_name | To retreive the hostname from host | Host | String |
| host_port | To retreive the port from host | Host | String |

## Aggregation Functions

| Function Name | Description | Input Type |
| --- | --- | --- |
| avg | average the numbers | Integral or Float |
| count | counting the number of records | Any |
| first | get the first of the records | Any |
| last | get the last of the records | Any |
| min | get the min of the records | Any |
| max | get the max of the records | Any |
| sum | get the sum of the numbers | Integral or Float |
| percentile_disc | calculate record at the percentile | Float |
| approx_percentile | calculate approximate record at the percentile | Float |


## Motivation

Often time in the daily work when you are troubleshooting the production issues, there are certain metrics that's not provided by AWS CloudWatch or in-house ELK. Then you would download the original access logs from your company's archive and write an one-off script to analyze it. However, this approach has a few drawbacks.

1. You spend a lot of time to parse of the log format, but not focus on calculating the metrics helping to troubleshoot your production issues.
2. Most of the log formats are commonly seen and we should ideally abstract it and have every one benefit from the shared abstraction
3. For web-server log cases, the log volume usually is huge, it could be several hundred MB or even a few GB. Doing it in scripting langauges would make yourself impatiently waiting it is running at your local.

For sure you could finely tuned the analytical tooling like AWS Athena or ELK to analyze the large volume of data, but often times you just want to adhocly analyze logs and don't bother to set things up and cost extra money. Also, the modern laptop/PC is actually powerful enough to analyze gigabytes of log volumes, just that the implementation is not efficient enough for doing that. Implementing logq in Rust is in hope to resolve those inconvenience and concerns.


### Why not TextQL, or insert the space delimited fields into sqlite?

TextQL is implemented in python ant it's ok for the smaller cases. In the case of high traffic AWS ELB log files, it often goes up to gigabytes in volume and it is slow
regarding to the speed. Furthermore, either TextQL and sqlite are limited in their provided SQL functions and data types, it makes the domain processing like URL, and HTTP
reuqests line and User-Agents tedious. 

Also, in the use case of web-traffic analytics, the questions you would to be answered are like "What is the 99th percentile in a given time frame to this Restful endpoint, by ignoring the user_id in the URL path segments". It would be easier to have a software providing handy functions to extract or canonicalize the information from the log.


## Roadmap

[] Using cmdline flag to specify the table names and their the backing files.
[] Spin-off the syntax parser as a separate cargo crate.
[] Support the [PartiQL](https://partiql.org/assets/PartiQL-Specification.pdf) specification.
[] Performance optimization, avoid unnecessary parsing
[] More supported functions
[] time_bucket with arbitrary interval (begin from epoch)
[] Window Function
[] Streaming mode to work with `tail -f`
[] Customizable Reader, to follow GoAccess's style
[] More supported log format
[] Plugin quickjs for user-defined functions
[] Implement APPROX_COUNT_DISTINCT with Hyperloglog
[] Building index for repetitive queries
