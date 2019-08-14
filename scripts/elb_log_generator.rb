#!/usr/bin/env ruby
require 'date'

for i in 1..100000 do
  dt = DateTime.now
  current_dt_str = dt.iso8601(6)

  log = %?#{current_dt_str} elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2?
  puts log
end
