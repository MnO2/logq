/// Parser benchmark queries -- graduated ladder
pub const PARSE_L1: &str = "SELECT a FROM t";
pub const PARSE_L2: &str = "SELECT a, b, c FROM t WHERE a > 10";
pub const PARSE_L3: &str = "SELECT a, count(b) FROM t WHERE a > 10 GROUP BY a ORDER BY a DESC LIMIT 100";
pub const PARSE_L4: &str = r#"SELECT a, sum(b) FROM t WHERE a > 10 AND c LIKE "%foo%" GROUP BY a HAVING sum(b) > 100 ORDER BY a"#;
pub const PARSE_L5: &str = r#"SELECT a, CASE WHEN b > 10 THEN "high" WHEN b > 5 THEN "mid" ELSE "low" END AS tier, count(*) FROM t AS t1 LEFT JOIN t AS t2 ON t1.id = t2.id WHERE a BETWEEN 1 AND 100 GROUP BY a, tier"#;
pub const PARSE_L6: &str = "SELECT a, b FROM t WHERE a IN (10, 20, 30) AND b IS NOT NULL UNION SELECT c, d FROM t2 ORDER BY a LIMIT 50";

/// Execution Tier A queries (run against data/AWSELB.log)
pub const EXEC_E1: &str = "SELECT * FROM elb LIMIT 10";
pub const EXEC_E2: &str = "SELECT elbname, count(*) FROM elb GROUP BY elbname";
pub const EXEC_E3: &str = r#"SELECT elbname, elb_status_code FROM elb WHERE elb_status_code = "200" ORDER BY elbname"#;
pub const EXEC_E4: &str = "SELECT elbname, count(*), sum(received_bytes), avg(received_bytes) FROM elb GROUP BY elbname";

/// Execution Tier C queries -- component-level profiling (run against data/AWSELB.log)
pub const PROF_SCAN_ONLY: &str = "SELECT * FROM elb";
pub const PROF_SCAN_FILTER: &str = r#"SELECT * FROM elb WHERE elb_status_code = "200""#;
pub const PROF_SCAN_FILTER_GROUPBY: &str = r#"SELECT elb_status_code, count(*) FROM elb WHERE elb_status_code = "200" GROUP BY elb_status_code"#;
