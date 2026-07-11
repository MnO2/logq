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
