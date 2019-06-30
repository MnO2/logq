# logq - A fast server log file command line toolkit written in Rust

This project is still under active development and not yet finished.

logq is a command line program for easily analyzing, querying, aggregating and
joining log files. Right now only AWS Elastic Load Balancer's log format is
supported, since it is where this project was inspired.

This project took a lot of inspiration from [xsv](https://github.com/BurntSushi/xsv) and jq. I agree that the commands should be simple, fast, and composable:

1. Simple tasks should be easy.
2. Performance should be transparent and very little overhead due to the volume
   size.


## Examples

Project the columns of `timestamp` and `backend_and_port` field and print it out.
```
logq select --query "timestamp, backend_and_port" data/AWSLogs.log
```


Aggregate the average processing time and partition by backend_and_port.
```
logq select --query "avg(backend_processing_time) over (partition by backend_and_port)" data/AWSLogs.log
```

## Motivation

The very same criticisms to xsv could also be asked to this project.

That is, you shouldn't be working with log file directly, you should ship it to
Spark or other data analyzing framework. However, in the daily work when you
would like to quickly troubleshooting things, oftentime you are still handed
with several gigabytes of log file, and it's time consuming to set things up.
Usually the modern laptop/pc is very powerful and enough for analyzing gigabytes
of volumes, when the implementation is well performance considered.
