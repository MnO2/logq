VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 plan addresses all 9 critical issues from the first review with impressive specificity -- call site enumerations, line numbers, and behavioral change documentation are thorough. However, two compilation-breaking issues remain: the design doc still contradicts the plan on the `ParsingContext` registry type, and `FunctionRegistry` cannot implement `Debug` due to its closure fields, which will break `ParsingContext`'s `Debug` derive.

## Critical Issues (must fix)

### 1. Design doc section 3.2 still contradicts the plan on `ParsingContext` registry type

The design doc (section 3.2) still specifies `&FunctionRegistry` (a plain reference) for `ParsingContext`:

```rust
pub(crate) registry: &FunctionRegistry,  // borrowed, not Arc -- planner is synchronous
```

The plan (Step 5b) correctly uses `Arc<FunctionRegistry>` and explicitly explains why. But the design doc was not updated to match. The design doc even includes a justification paragraph ("The planner does not need `Arc` because it runs synchronously...") that directly contradicts the plan's choice. An implementer reading the design doc first would produce incompatible code. The design doc must be updated to use `Arc<FunctionRegistry>`.

### 2. `FunctionRegistry` cannot derive `Debug`, breaking `ParsingContext`'s `Debug` derive

`ParsingContext` at `common/types.rs` line 413 currently derives `Debug, Clone, PartialEq, Eq`. The plan (Step 5b) correctly removes `PartialEq, Eq` but retains `Debug`:

```rust
#[derive(Debug, Clone)]  // PartialEq, Eq REMOVED
pub(crate) struct ParsingContext {
    ...
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

For `Arc<FunctionRegistry>` to implement `Debug`, `FunctionRegistry` must implement `Debug`. The plan's Step 1c defines `FunctionRegistry` as containing `HashMap<String, FunctionDef>`, and `FunctionDef` contains `Box<dyn Fn(&[Value]) -> ExpressionResult<Value> + Send + Sync>`. The `dyn Fn(...)` trait object does **not** implement `Debug`. Therefore:
- `FunctionDef` cannot `#[derive(Debug)]`
- `FunctionRegistry` cannot `#[derive(Debug)]`
- `Arc<FunctionRegistry>` does not implement `Debug`
- `ParsingContext`'s `#[derive(Debug)]` will **fail to compile**

**Fix**: Add a manual `Debug` implementation for `FunctionRegistry` and `FunctionDef`. For example:

```rust
impl std::fmt::Debug for FunctionDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionDef")
            .field("name", &self.name)
            .field("arity", &self.arity)
            .field("null_handling", &self.null_handling)
            .field("func", &"<closure>")
            .finish()
    }
}

impl std::fmt::Debug for FunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionRegistry")
            .field("functions", &self.functions.keys().collect::<Vec<_>>())
            .finish()
    }
}
```

Alternatively, remove `Debug` from `ParsingContext` -- verified that no code in the codebase formats `ParsingContext` with `{:?}`. But the manual `Debug` approach is preferable for debuggability.

## Suggestions (nice to have)

### A. Add end-to-end integration test for `Concat` null/missing behavioral change

The plan has a unit test (`test_concat_null_propagation_behavioral_change` in Step 3a) that verifies `Concat(Null, Missing)` now returns `Missing`. But there is no end-to-end test through the full pipeline. Add one in Step 7 to catch wiring errors:

```rust
#[test]
fn test_concat_null_propagation_e2e() {
    let result = run_format_query_to_vec(
        "jsonl",
        &[r#"{"a": null}"#],
        r#"SELECT a || "x" as result FROM it"#,
    );
    assert!(result.is_ok());
}
```

Verified that no existing integration tests exercise `Concat` with `Null`/`Missing` arguments, so there is no regression risk.

### B. Step 5 should recommend `cargo check` after each sub-step

Step 5 is the largest step (6 sub-steps across 5 files). Running `cargo check` after each sub-step (5a through 5f) would catch type errors incrementally rather than discovering dozens of errors at step 5g.

### C. Line number references should be marked as approximate

The plan references many specific line numbers. These are accurate as of the current file state but will shift after any preceding edit. Consider noting "as of current HEAD" or using function/struct names as anchors instead.

### D. The `from_str` function name shadows `std::str::FromStr`

As noted in the original review -- consider renaming to `parse_aggregate_name` to avoid confusion, especially since the new `is_aggregate_name()` is co-located.

### E. The `group_as` entry in `is_aggregate_name()` is unnecessary but harmless

`group_as` is never parsed as a `FuncCall` expression. It enters the system through the `group_as_clause` field on `GroupByExpression`, not through `parse_value_expression`'s `FuncCall` arm. Including it in `is_aggregate_name()` provides defense-in-depth but will never actually match. This is fine to keep.

## Verified Claims (things I confirmed are correct)

1. **Critical #1 fix (PartialEq/Eq removal)**: Verified -- no code in the codebase compares `ParsingContext` with `==`. All 10+ `ParsingContext` construction sites in `logical/parser.rs` only construct values and pass them to functions. Zero equality checks found.

2. **Critical #2 fix (Subquery path)**: Verified at `logical/parser.rs` line 317-319 -- `parse_query(*stmt.clone(), ctx.data_source.clone())` correctly identified as needing `ctx.registry.clone()`. The plan's updated code is correct.

3. **Critical #3 fix (aggregate detection)**: The `is_aggregate_name()` function correctly lists all aggregate names: `from_str()` names (avg, count, first, last, max, min, sum, approx_count_distinct) plus within_group names (percentile_disc, approx_percentile) plus group_as. Co-location with `from_str()` at line 386 is the right placement to prevent drift.

4. **Critical #4 fix (HttpRequest fields)**: Verified at `common/types.rs` line 92-97 -- the struct uses `http_method` and `http_version`. The plan's test code uses the correct field names.

5. **Critical #5 fix (GroupByStream calls)**: Verified all 8 `expression_value()` calls in `GroupByStream::next()` at `stream.rs` lines 326, 337, 348, 359, 370, 381, 392, 403. Also confirmed that `PercentileDisc` (line 411-413) and `ApproxPercentile` (line 415-417) use `variables.get(column_name)` directly, and `GroupAs` (line 314-323) uses `Value::Object(record.to_variables().clone())` -- none call `expression_value()`.

6. **Critical #6 fix (Formula::evaluate internals)**: Verified all `expression_value()` calls within `Formula::evaluate()` at `execution/types.rs`: IsNull (line 837), IsNotNull (841), IsMissing (845), IsNotMissing (849), ExpressionPredicate (853), Like (861, 862), NotLike (875, 876), In (889, 895), NotIn (914 via recursive evaluate). Also verified recursive `evaluate()` calls at lines 808, 809, 818, 819, 828, and `relation.apply()` at line 832.

7. **Critical #7 fix (lazy_static)**: Verified both files need `#[macro_use] extern crate lazy_static;`: `main.rs` for `TABLE_SPEC_REGEX` (line 22-24), and `common/types.rs` for `HOST_REGEX` etc. (lines 12-17). In edition 2018 dual-target layout, each target has its own crate scope.

8. **Critical #8 fix (explain function)**: Verified at `app.rs` line 104-118 -- `explain()` calls `parse_query_top(q, data_source.clone())` which needs the registry. The plan correctly shows creating and passing it.

9. **Critical #9 fix (test helpers)**: Verified `run_to_vec()` (line 201-223) calls both `parse_query_top()` and `physical_plan.get(variables)`. The plan correctly notes that `run_format_query()` and `run_format_query_to_vec()` (lines 232-268) delegate to `run()` and `run_to_vec()` respectively, so they need no direct changes.

10. **Stream structs not needing registry**: Verified that `LimitStream`, `InMemoryStream`, `LogFileStream`, `ProjectionStream`, `DistinctStream`, `UnionStream`, `IntersectStream`, `ExceptStream` never call `expression_value()` or `formula.evaluate()`. The plan's enumeration at Step 5d lines 1032-1041 is correct.

11. **Relation::apply signature**: Verified at `execution/types.rs` line 710-712 -- `apply()` calls `left.expression_value(variables)` and `right.expression_value(variables)`, both needing registry.

12. **Concat null precedence bug**: Confirmed at `execution/types.rs` lines 606-608 -- `Null` is checked before `Missing`, giving incorrect precedence. The registry's `NullHandling::Propagate` correctly gives `Missing` precedence.

13. **Binary operator translation**: Confirmed at `logical/parser.rs` lines 207-214 -- `parse_binary_operator` converts `+/-/*/||` into `Expression::Function("Plus"/"Minus"/"Times"/"Divide"/"Concat", args)`.

14. **Node::Filter field order**: Confirmed the two `Node` enums differ: `logical::types::Node::Filter(Box<Formula>, Box<Node>)` vs `execution::types::Node::Filter(Box<Node>, Box<Formula>)`. The `physical()` method at `logical/types.rs` line 51 correctly maps between them.

15. **Node::get arm enumeration**: Verified all arms (Filter, Map, DataSource, GroupBy, Limit, OrderBy, Distinct, CrossJoin, LeftJoin, Union, Intersect, Except) at `execution/types.rs` lines 938-1125. The plan's Step 5d table covers all arms that need `registry.clone()` passed to recursive `get()` calls.

16. **LeftJoinStream call sites**: Verified three sites needing registry: `self.condition.evaluate()` at line 743, `self.right_node.get()` at line 774, and `self.right_node.get()` at line 784. All are enumerated in the plan.

17. **CrossJoinStream call site**: Verified `self.right_node.get()` at `stream.rs` line 666. Enumerated in the plan.

18. **ExpressionError::UnknownFunction variant**: Confirmed it exists in `execution/types.rs` (used at lines 359, 384). The plan's `registry.call()` correctly uses this variant.
