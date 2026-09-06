# PartiQL conformance subset

These cases are adapted from the Apache-2.0 licensed
[`partiql/partiql-tests`](https://github.com/partiql/partiql-tests) evaluation
areas. The upstream suite uses Ion-encoded global environments, bags, and
evaluation modes. logq instead queries named file-backed tables, so directly
vendoring the Ion fixtures would test a compatibility adapter rather than the
query engine.

`cases.json` hand-ports supported semantics onto `input.jsonl` and attributes
each case to its upstream area. `skips.json` records upstream areas that cannot
yet be represented, with a reason for every omission. The harness requires at
least 50 passing cases so accidental loss of coverage fails CI.

<!-- generated conformance cases -->

## Executable examples

Generated from `cases.json`; `cargo test --test conformance` executes every query
against `input.jsonl` and checks its complete expected answer. Regenerate with
`python3 scripts/render_conformance.py`; CI rejects a stale table.

| Case | SQL |
| --- | --- |
| having_aggregate_call | `select category, count(*) as n from it group by category having count(*) > 1 order by category asc` |
| having_hidden_aggregate | `select category from it group by category having sum(score) > 25 order by category asc` |
| having_global_aggregate | `select count(*) as n from it having count(*) = 4` |
| duplicate_aggregate_alias_topn | `select category, count(*) as n, sum(uid) as n from it group by category order by n desc limit 1` |
| projection_alias | `select uid as id from it order by id desc limit 2` |
| in_followed_by_order | `select uid from it where uid in (1,3) order by uid desc` |
| not_in_followed_by_limit | `select uid from it where uid not in (1,3) limit 1` |
| projection_one | `select uid from it order by uid asc` |
| projection_two | `select uid, person from it order by uid asc` |
| distinct | `select distinct category from it order by category asc` |
| order_desc | `select uid from it order by uid desc` |
| limit | `select uid from it order by uid asc limit 2` |
| equal | `select uid from it where uid = 2` |
| not_equal | `select uid from it where uid != 2 order by uid asc` |
| greater | `select uid from it where uid > 2 order by uid asc` |
| greater_equal | `select uid from it where uid >= 2 order by uid asc` |
| less | `select uid from it where uid < 3 order by uid asc` |
| less_equal | `select uid from it where uid <= 3 order by uid asc` |
| between | `select uid from it where uid between 2 and 3 order by uid asc` |
| not_between | `select uid from it where uid not between 2 and 3 order by uid asc` |
| in | `select uid from it where uid in (1, 3)` |
| not_in | `select uid from it where uid not in (1, 3)` |
| like | `select uid from it where person like "A%" order by uid asc` |
| not_like | `select uid from it where person not like "%o%" order by uid asc` |
| is_null | `select uid from it where optional is null` |
| is_not_null | `select uid from it where optional is not null` |
| is_missing | `select uid from it where optional is missing` |
| is_not_missing | `select uid from it where optional is not missing` |
| logical_and | `select uid from it where active and uid < 3 order by uid asc` |
| logical_or | `select uid from it where active or uid = 2 order by uid asc` |
| logical_not | `select uid from it where not active order by uid asc` |
| plus | `select uid + 1 as result from it order by uid asc` |
| minus | `select uid - 1 as result from it order by uid asc` |
| times | `select uid * 2 as result from it order by uid asc` |
| divide | `select uid / 2 as result from it order by uid asc` |
| concat | `select person \|\| "!" as result from it order by uid asc` |
| upper | `select upper(person) as result from it order by uid asc` |
| lower | `select lower(person) as result from it order by uid asc` |
| char_length | `select char_length(person) as result from it order by uid asc` |
| substring | `select substring(person, 2, 2) as result from it order by uid asc` |
| case | `select case when active then "yes" else "no" end as result from it order by uid asc` |
| coalesce | `select coalesce(optional, "fallback") as result from it order by uid asc` |
| nullif | `select nullif(category, "alpha") as result from it order by uid asc` |
| cast | `select cast(uid as float) as result from it order by uid asc` |
| count_star | `select count(*) as result from it` |
| count_value | `select count(optional) as result from it` |
| sum | `select sum(uid) as result from it` |
| avg | `select avg(score) as result from it` |
| min | `select min(uid) as result from it` |
| max | `select max(uid) as result from it` |
| group_count | `select category, count(*) as result from it group by category order by category asc` |
| group_sum | `select category, sum(uid) as result from it group by category order by category asc` |
| group_avg | `select category, avg(score) as result from it group by category order by category asc` |
| union | `select uid from it where uid <= 2 union select uid from it where uid >= 2 order by uid asc` |
| union_all | `select uid from it where uid = 1 union all select uid from it where uid = 1` |
| intersect | `select uid from it where uid <= 3 intersect select uid from it where uid >= 2 order by uid asc` |
| except | `select uid from it except select uid from it where uid = 2 order by uid asc` |
| distinct_boolean | `select distinct active from it order by active asc` |
| boolean_equal | `select uid from it where active = true order by uid asc` |
| string_equal | `select uid from it where category = "alpha" order by uid asc` |
| literal_projection | `select 1 as result from it limit 1` |
