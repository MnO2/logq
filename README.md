# logq - A fast server log file command line toolkit written in Rust

[![Build Status](https://travis-ci.com/MnO2/logq.svg?branch=master)](https://travis-ci.com/MnO2/logq)

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

Select the most latency hitting to a backend server 
```
logq select --query "PERCENTILE_DISC(1) within group (order by backend_processing_time desc) over (partition by backend_and_port)" data/AWSLogs.log
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
