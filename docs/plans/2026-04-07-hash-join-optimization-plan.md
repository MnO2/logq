# Plan: Hash Join Optimization (Phase 1)

**Goal**: Replace logq's O(|L| x |R| x disk I/O) nested-loop joins with hash joins, starting with Phase 1 from the design at `docs/plans/2026-04-07-hash-join-optimization-design-final.md`.
**Architecture**: Parser-level equi-predicate extraction emits `HashJoin` logical nodes; `HashJoinStream` materializes the build side into a `hashbrown::HashMap<JoinKey, SmallVec<[Record; 1]>>` and probes from the left. Existing nested-loop streams get a materialize-once fix as immediate fallback.
**Tech Stack**: Rust, hashbrown, ahash 0.7, smallvec 1, cargo test

---

## Step 1: Materialize-once fallback for nested-loop joins

**File**: `src/execution/stream.rs`

This is the highest-ROI change with zero risk. Currently `CrossJoinStream` and `LeftJoinStream` re-open and re-parse the right-side file for every left row. Fix: materialize the right side into `Vec<Record>` on first iteration.

### 1a. Write failing test

```rust
// In tests at bottom of src/execution/stream.rs or in a new test file
// This is a correctness test — behavior should not change, just performance.
// The existing integration tests in tests/ cover join correctness.
// We verify the existing CROSS JOIN and LEFT JOIN tests still pass after the refactor.
```

No new failing test needed — this is a refactor. Existing tests serve as the safety net.

### 1b. Run tests to establish baseline

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -5
```

### 1c. Implement: CrossJoinStream materialize-once

In `CrossJoinStream` (line 658), replace `right_node: Node` with `right_rows: Vec<Record>` and `right_index: usize`:

```rust
pub(crate) struct CrossJoinStream {
    left: Box<dyn RecordStream>,
    right_node: Node,           // keep for initial materialization
    right_variables: Variables,
    current_left: Option<Record>,
    right_rows: Option<Vec<Record>>,  // NEW: materialized right side
    right_index: usize,               // NEW: current position in right_rows
    registry: Arc<FunctionRegistry>,
    threads: usize,
}
```

In `next()`, on first call, materialize right side once:
```rust
// At start of next():
if self.right_rows.is_none() {
    let mut right_stream = self.right_node.get(self.right_variables.clone(), self.registry.clone(), self.threads)
        .map_err(|e| super::types::StreamError::Get(e))?;
    let mut rows = Vec::new();
    while let Some(record) = right_stream.next()? {
        rows.push(record);
    }
    self.right_rows = Some(rows);
}
// Then iterate via index instead of re-creating stream
```

### 1d. Implement: LeftJoinStream materialize-once

Same pattern for `LeftJoinStream` (line 713). Replace the `right_node.get(...)` call on line 813 with iteration over `right_rows` via index. Cache `right_field_names` from the first record in `right_rows`.

### 1e. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -5
```

### 1f. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/stream.rs && git commit -m "$(cat <<'EOF'
perf: materialize right side once for nested-loop joins

Previously CrossJoinStream and LeftJoinStream re-opened and re-parsed
the right-side file from disk for every left row. Now the right side
is materialized into Vec<Record> on first iteration and reused.
EOF
)"
```

---

## Step 2: Add dependencies (ahash, smallvec)

**File**: `Cargo.toml`

### 2a. Add dependencies

Add after the `rayon` line (line 40):

```toml
ahash = "0.7"
smallvec = "1"
```

### 2b. Verify build

```bash
cd /Users/paulmeng/Develop/logq && cargo check 2>&1 | tail -5
```

### 2c. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add Cargo.toml Cargo.lock && git commit -m "$(cat <<'EOF'
build: add ahash 0.7 and smallvec 1 dependencies for hash join
EOF
)"
```

---

## Step 3: Add `Inner` and `Right` to JoinType enum

**File**: `src/syntax/ast.rs`

### 3a. Write failing test

```rust
// In src/syntax/parser.rs tests (or a new test module):
#[test]
fn test_parse_inner_join() {
    let input = "SELECT a.x, b.y FROM a INNER JOIN b ON a.id = b.id";
    let result = query(input);
    assert!(result.is_ok());
}

#[test]
fn test_parse_right_join() {
    let input = "SELECT a.x, b.y FROM a RIGHT JOIN b ON a.id = b.id";
    let result = query(input);
    assert!(result.is_ok());
}
```

### 3b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_parse_inner_join 2>&1 | tail -10
```

### 3c. Implement: Extend JoinType enum

In `src/syntax/ast.rs` line 26-29, add `Inner` and `Right`:

```rust
pub(crate) enum JoinType {
    Cross,
    Left,
    Inner,
    Right,
}
```

### 3d. Implement: Parse INNER JOIN and RIGHT JOIN

In `src/syntax/parser.rs`, add parser functions for `INNER JOIN ... ON` and `RIGHT JOIN ... ON`, following the existing `left_join_clause` pattern. Add `inner` and `right` to the join keyword parsers. Parse:
- `[INNER] JOIN table ON condition` → `JoinType::Inner`
- `RIGHT [OUTER] JOIN table ON condition` → `JoinType::Right`

Handle bare `JOIN` (without INNER keyword) as `JoinType::Inner`.

### 3e. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_parse_inner 2>&1 | tail -5
cd /Users/paulmeng/Develop/logq && cargo test test_parse_right 2>&1 | tail -5
```

### 3f. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/syntax/ast.rs src/syntax/parser.rs && git commit -m "$(cat <<'EOF'
feat: add INNER JOIN and RIGHT JOIN syntax support

Parse [INNER] JOIN ... ON and RIGHT [OUTER] JOIN ... ON.
Bare JOIN is treated as INNER JOIN.
EOF
)"
```

---

## Step 4: Add `Record::get_field_value` method

**File**: `src/execution/stream.rs`

### 4a. Write failing test

```rust
#[test]
fn test_record_get_field_value() {
    let mut vars = Variables::default();
    vars.insert("status".to_string(), Value::Int(200));
    vars.insert("host".to_string(), Value::String("example.com".to_string()));
    let record = Record::new_with_variables(vars);

    assert_eq!(record.get_field_value("status"), Some(&Value::Int(200)));
    assert_eq!(record.get_field_value("host"), Some(&Value::String("example.com".to_string())));
    assert_eq!(record.get_field_value("missing"), None);
}
```

### 4b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_record_get_field_value 2>&1 | tail -10
```

### 4c. Implement

Add after `get_ref` (line 50) in `src/execution/stream.rs`:

```rust
/// Direct field access by bare name. Bypasses PathExpr construction.
/// For the join key extraction hot path.
#[inline]
pub(crate) fn get_field_value(&self, field_name: &str) -> Option<&Value> {
    self.variables.get(field_name)
}
```

### 4d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_record_get_field_value 2>&1 | tail -5
```

### 4e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/stream.rs && git commit -m "$(cat <<'EOF'
feat: add Record::get_field_value for direct field access by name

Zero-overhead field lookup bypassing PathExpr construction,
for the hash join key extraction hot path.
EOF
)"
```

---

## Step 5: Add `HashJoin` logical node variant and `LogicalJoinType`

**File**: `src/logical/types.rs`

### 5a. Write failing test

```rust
#[test]
fn test_hash_join_physical() {
    let path_a = PathExpr::new(vec![PathSegment::AttrName("a_id".to_string())]);
    let path_b = PathExpr::new(vec![PathSegment::AttrName("b_id".to_string())]);

    let left = Node::DataSource(
        common::DataSource::Stdin("jsonl".to_string(), "a".to_string()),
        vec![],
    );
    let right = Node::DataSource(
        common::DataSource::Stdin("jsonl".to_string(), "b".to_string()),
        vec![],
    );

    let hash_join = Node::HashJoin {
        left: Box::new(left),
        right: Box::new(right),
        equi_keys: vec![(path_a.clone(), path_b.clone())],
        residual: None,
        join_type: LogicalJoinType::Inner,
    };

    let mut creator = PhysicalPlanCreator::new();
    let result = hash_join.physical(&mut creator);
    assert!(result.is_ok());
}
```

### 5b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_hash_join_physical 2>&1 | tail -10
```

### 5c. Implement

In `src/logical/types.rs`:

1. Add the `LogicalJoinType` enum (before `Node`):
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicalJoinType {
    Inner,
    Left,
    Right,
}
```

2. Add `HashJoin` variant to `Node` enum (after `LeftJoin`):
```rust
HashJoin {
    left: Box<Node>,
    right: Box<Node>,
    equi_keys: Vec<(ast::PathExpr, ast::PathExpr)>,
    residual: Option<Box<Formula>>,
    join_type: LogicalJoinType,
},
```

3. Add the `physical()` match arm for `HashJoin`:
```rust
Node::HashJoin { left, right, equi_keys, residual, join_type } => {
    let (left_child, left_variables) = left.physical(physical_plan_creator)?;
    let (right_child, right_variables) = right.physical(physical_plan_creator)?;
    let physical_residual = match residual {
        Some(r) => {
            let (pr, rv) = r.physical(physical_plan_creator)?;
            Some((pr, rv))
        }
        None => None,
    };
    let mut return_variables = common::merge(&left_variables, &right_variables);
    if let Some((_, ref rv)) = physical_residual {
        return_variables = common::merge(&return_variables, rv);
    }
    let physical_residual_formula = physical_residual.map(|(f, _)| f);
    let node = execution::Node::HashJoin {
        left: left_child,
        right: right_child,
        equi_keys: equi_keys.clone(),
        residual: physical_residual_formula,
        join_type: join_type.clone(),
    };
    Ok((Box::new(node), return_variables))
}
```

4. Add `LogicalJoinType` to `execution::Node` as well (see Step 8 for execution-side `HashJoin`). For now, add a placeholder `HashJoin` variant in `src/execution/types.rs` `Node` enum (line 550) so it compiles:
```rust
HashJoin {
    left: Box<Node>,
    right: Box<Node>,
    equi_keys: Vec<(PathExpr, PathExpr)>,
    residual: Option<Box<Formula>>,
    join_type: LogicalJoinType,
},
```

Also add `LogicalJoinType` to `execution/types.rs`:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalJoinType {
    Inner,
    Left,
    Right,
}
```

### 5d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_hash_join_physical 2>&1 | tail -5
```

### 5e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/logical/types.rs src/execution/types.rs && git commit -m "$(cat <<'EOF'
feat: add HashJoin logical and physical node variants

New Node::HashJoin with equi_keys, residual filter, and LogicalJoinType.
Physical plan translation is a 1:1 structural mapping.
EOF
)"
```

---

## Step 6: Equi-predicate extraction from AST at parser level

**File**: `src/logical/parser.rs`

### 6a. Write failing test

```rust
#[test]
fn test_equi_predicate_extraction_simple() {
    // a.id = b.id → extracted as equi-key (a.id, b.id), no residual
    let expr = ast::Expression::BinaryOperator(
        ast::BinaryOperator::Equal,
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::AttrName("id".to_string()),
        ]))),
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("b".to_string()),
            ast::PathSegment::AttrName("id".to_string()),
        ]))),
    );
    let left_aliases: HashSet<String> = ["a".to_string()].into();
    let right_alias = "b";
    let (equi_keys, residual) = extract_equi_predicates_from_ast(&expr, &left_aliases, right_alias);
    assert_eq!(equi_keys.len(), 1);
    assert!(residual.is_none());
}

#[test]
fn test_equi_predicate_extraction_with_residual() {
    // a.id = b.id AND a.x > 10 → equi-key (a.id, b.id), residual a.x > 10
    let equi = ast::Expression::BinaryOperator(
        ast::BinaryOperator::Equal,
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::AttrName("id".to_string()),
        ]))),
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("b".to_string()),
            ast::PathSegment::AttrName("id".to_string()),
        ]))),
    );
    let non_equi = ast::Expression::BinaryOperator(
        ast::BinaryOperator::MoreThan,
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::AttrName("x".to_string()),
        ]))),
        Box::new(ast::Expression::Value(ast::Value::Integral(10))),
    );
    let expr = ast::Expression::BinaryOperator(
        ast::BinaryOperator::And,
        Box::new(equi),
        Box::new(non_equi),
    );
    let left_aliases: HashSet<String> = ["a".to_string()].into();
    let right_alias = "b";
    let (equi_keys, residual) = extract_equi_predicates_from_ast(&expr, &left_aliases, right_alias);
    assert_eq!(equi_keys.len(), 1);
    assert!(residual.is_some());
}

#[test]
fn test_equi_predicate_no_alias_fallback() {
    // x = y without table aliases → empty equi-keys (nested-loop fallback)
    let expr = ast::Expression::BinaryOperator(
        ast::BinaryOperator::Equal,
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("x".to_string()),
        ]))),
        Box::new(ast::Expression::Column(ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("y".to_string()),
        ]))),
    );
    let left_aliases: HashSet<String> = ["a".to_string()].into();
    let right_alias = "b";
    let (equi_keys, _) = extract_equi_predicates_from_ast(&expr, &left_aliases, right_alias);
    assert_eq!(equi_keys.len(), 0);
}
```

### 6b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_equi_predicate 2>&1 | tail -10
```

### 6c. Implement

Add to `src/logical/parser.rs`:

1. **`extract_equi_predicates_from_ast` function:**
   - Input: `&ast::Expression`, `&HashSet<String>` (left aliases), `&str` (right alias)
   - Flatten top-level AND conjuncts
   - For each conjunct, check if `BinaryOperator::Equal` with two `Column(PathExpr)` operands
   - Check first segment of each PathExpr against left/right aliases
   - Return `(Vec<(PathExpr, PathExpr)>, Option<ast::Expression>)`

2. **`collect_from_clause_aliases` function:** Walk `FromClause` tree to collect aliases from `TableReference.as_clause` (or base table name if no alias).

3. **Modify `build_from_node`** (line 666-684):
   - In the `JoinType::Left` arm, call `extract_equi_predicates_from_ast` with left/right aliases
   - If equi-keys found, emit `types::Node::HashJoin { ... }` instead of `types::Node::LeftJoin`
   - Add `JoinType::Inner` arm: same as Left but with `LogicalJoinType::Inner`
   - Add `JoinType::Right` arm: swap left/right, use `LogicalJoinType::Left`, mark as right-join swap

4. **Modify `parse_query`** (line 789-792):
   - Before applying WHERE filter, check if root is a `CrossJoin` and WHERE has equi-predicates
   - If so, replace `CrossJoin` + `Filter` with `HashJoin { join_type: Inner }`

### 6d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_equi_predicate 2>&1 | tail -5
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -5
```

### 6e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/logical/parser.rs && git commit -m "$(cat <<'EOF'
feat: parser-level equi-predicate extraction for hash join

Extract equi-join predicates at parse time where table alias info is
available. Emit HashJoin logical nodes for LEFT/INNER/RIGHT joins with
equi-keys. Fall back to nested-loop when aliases are absent.
EOF
)"
```

---

## Step 7: JoinKey enum with specialized hashing

**File**: `src/execution/stream.rs` (or new `src/execution/join.rs`)

### 7a. Write failing test

```rust
#[test]
fn test_join_key_single_int_hash_eq() {
    let k1 = JoinKey::SingleInt(42);
    let k2 = JoinKey::SingleInt(42);
    let k3 = JoinKey::SingleInt(99);
    assert_eq!(k1, k2);
    assert_ne!(k1, k3);

    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    let mut h1 = DefaultHasher::new();
    let mut h2 = DefaultHasher::new();
    k1.hash(&mut h1);
    k2.hash(&mut h2);
    assert_eq!(h1.finish(), h2.finish());
}

#[test]
fn test_join_key_single_string_hash_eq() {
    let k1 = JoinKey::SingleString("hello".to_string());
    let k2 = JoinKey::SingleString("hello".to_string());
    let k3 = JoinKey::SingleString("world".to_string());
    assert_eq!(k1, k2);
    assert_ne!(k1, k3);
}

#[test]
fn test_join_key_no_cross_variant_collision() {
    // SingleInt(42) should NOT equal Composite(vec![Value::Int(42)])
    let k_int = JoinKey::SingleInt(42);
    let k_comp = JoinKey::Composite(vec![Value::Int(42)]);
    assert_ne!(k_int, k_comp);
}

#[test]
fn test_extract_key_single_int() {
    let mut vars = Variables::default();
    vars.insert("id".to_string(), Value::Int(42));
    let record = Record::new_with_variables(vars);
    let key = extract_key(&record, &["id".to_string()]);
    assert_eq!(key, JoinKey::SingleInt(42));
}

#[test]
fn test_extract_key_null_returns_composite() {
    let mut vars = Variables::default();
    vars.insert("id".to_string(), Value::Null);
    let record = Record::new_with_variables(vars);
    let key = extract_key(&record, &["id".to_string()]);
    // NULL keys go through Composite path
    matches!(key, JoinKey::Composite(_));
}
```

### 7b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_join_key 2>&1 | tail -10
cd /Users/paulmeng/Develop/logq && cargo test test_extract_key 2>&1 | tail -10
```

### 7c. Implement

Add to `src/execution/stream.rs` (or create `src/execution/join.rs` and add `pub(crate) mod join;` to `src/execution/mod.rs` if it exists, or to `src/lib.rs`):

```rust
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub(crate) enum JoinKey {
    SingleString(String),
    SingleInt(i32),
    Composite(Vec<Value>),
}

impl PartialEq for JoinKey { /* variant-aware equality */ }
impl Eq for JoinKey {}

impl std::hash::Hash for JoinKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Discriminant tag first to avoid cross-variant collisions
        std::mem::discriminant(self).hash(state);
        match self {
            JoinKey::SingleString(s) => s.hash(state),
            JoinKey::SingleInt(i) => i.hash(state),
            JoinKey::Composite(vals) => vals.hash(state),
        }
    }
}

pub(crate) fn extract_key(record: &Record, key_fields: &[String]) -> JoinKey {
    if key_fields.len() == 1 {
        match record.get_field_value(&key_fields[0]) {
            Some(Value::String(s)) => JoinKey::SingleString(s.clone()),
            Some(Value::Int(i)) => JoinKey::SingleInt(*i),
            Some(other) => JoinKey::Composite(vec![other.clone()]),
            None => JoinKey::Composite(vec![Value::Missing]),
        }
    } else {
        JoinKey::Composite(
            key_fields.iter()
                .map(|f| record.get_field_value(f).cloned().unwrap_or(Value::Missing))
                .collect()
        )
    }
}
```

### 7d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_join_key 2>&1 | tail -5
cd /Users/paulmeng/Develop/logq && cargo test test_extract_key 2>&1 | tail -5
```

### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add -A && git commit -m "$(cat <<'EOF'
feat: add JoinKey enum with specialized hashing for hash join

SingleString hashes raw bytes, SingleInt hashes i32 directly.
Falls back to Composite(Vec<Value>) for multi-column or exotic keys.
Discriminant tag prevents cross-variant hash collisions.
EOF
)"
```

---

## Step 8: HashJoinStream implementation

**File**: `src/execution/stream.rs`, `src/execution/types.rs`

### 8a. Write failing test

```rust
#[test]
fn test_hash_join_inner_basic() {
    // Left: [{id: 1, x: "a"}, {id: 2, x: "b"}]
    // Right: [{id: 1, y: "c"}, {id: 3, y: "d"}]
    // INNER JOIN on id → [{id: 1, x: "a", y: "c"}]
    let left_records = vec![
        record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
        record_from_pairs(vec![("id", Value::Int(2)), ("x", Value::String("b".into()))]),
    ];
    let right_records = vec![
        record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("c".into()))]),
        record_from_pairs(vec![("id", Value::Int(3)), ("y", Value::String("d".into()))]),
    ];
    let left_stream = Box::new(InMemoryStream::new(left_records));
    let right_stream = Box::new(InMemoryStream::new(right_records));
    let mut join = HashJoinStream::new(
        left_stream,
        right_stream,
        vec!["id".to_string()],
        vec!["id".to_string()],
        None,
        LogicalJoinType::Inner,
        512 * 1024 * 1024,
    );
    let mut results = Vec::new();
    while let Some(r) = join.next().unwrap() {
        results.push(r);
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_field_value("x"), Some(&Value::String("a".into())));
    assert_eq!(results[0].get_field_value("y"), Some(&Value::String("c".into())));
}

#[test]
fn test_hash_join_left_with_null_padding() {
    // LEFT JOIN: unmatched left rows get NULL-padded right columns
    let left_records = vec![
        record_from_pairs(vec![("id", Value::Int(1)), ("x", Value::String("a".into()))]),
        record_from_pairs(vec![("id", Value::Int(2)), ("x", Value::String("b".into()))]),
    ];
    let right_records = vec![
        record_from_pairs(vec![("id", Value::Int(1)), ("y", Value::String("c".into()))]),
    ];
    let left_stream = Box::new(InMemoryStream::new(left_records));
    let right_stream = Box::new(InMemoryStream::new(right_records));
    let mut join = HashJoinStream::new(
        left_stream, right_stream,
        vec!["id".to_string()], vec!["id".to_string()],
        None, LogicalJoinType::Left, 512 * 1024 * 1024,
    );
    let mut results = Vec::new();
    while let Some(r) = join.next().unwrap() {
        results.push(r);
    }
    assert_eq!(results.len(), 2);
    // Second row should have NULL for y
    assert_eq!(results[1].get_field_value("y"), Some(&Value::Null));
}

#[test]
fn test_hash_join_null_keys_no_match() {
    // NULL keys should not match each other
    let left = vec![record_from_pairs(vec![("id", Value::Null)])];
    let right = vec![record_from_pairs(vec![("id", Value::Null), ("y", Value::Int(1))])];
    let mut join = HashJoinStream::new(
        Box::new(InMemoryStream::new(left)),
        Box::new(InMemoryStream::new(right)),
        vec!["id".to_string()], vec!["id".to_string()],
        None, LogicalJoinType::Inner, 512 * 1024 * 1024,
    );
    let result = join.next().unwrap();
    assert!(result.is_none()); // No match
}

// Helper
fn record_from_pairs(pairs: Vec<(&str, Value)>) -> Record {
    let mut vars = Variables::default();
    for (k, v) in pairs {
        vars.insert(k.to_string(), v);
    }
    Record::new_with_variables(vars)
}
```

### 8b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_hash_join 2>&1 | tail -10
```

### 8c. Implement

Add `HashJoinStream` to `src/execution/stream.rs`:

```rust
pub(crate) struct HashJoinStream {
    left: Box<dyn RecordStream>,
    hash_table: hashbrown::HashMap<JoinKey, SmallVec<[Record; 1]>>,
    join_type: LogicalJoinType,
    left_key_fields: Vec<String>,
    right_key_fields: Vec<String>,
    residual: Option<Formula>,
    // Iteration state
    current_left: Option<Record>,
    current_matches: SmallVec<[Record; 1]>,
    match_index: usize,
    matched: bool,
    right_field_names: Vec<String>,
    built: bool,
    memory_limit: usize,
    // Build-side input (consumed during build)
    build_input: Option<Box<dyn RecordStream>>,
}
```

Key implementation points:
- `new()` takes left stream, right (build) stream, key field names, residual, join type, memory limit
- On first `next()`, call `build()` which drains `build_input` into `hash_table`, skipping NULL/Missing keys
- Track approximate memory during build; return error if exceeds `memory_limit`
- Probe: extract key from left row via `extract_key`, lookup in hash table
- For LEFT JOIN: emit NULL-padded right side when no match
- Residual filter applied to each merged candidate

Wire up in `execution/types.rs` `Node::get()`:
```rust
Node::HashJoin { left, right, equi_keys, residual, join_type } => {
    let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
    let right_stream = right.get(variables.clone(), registry.clone(), threads)?;
    // Resolve PathExpr equi_keys to bare field names
    let left_key_fields: Vec<String> = equi_keys.iter()
        .map(|(l, _)| l.path_segments.last().unwrap()./* extract name */)
        .collect();
    let right_key_fields: Vec<String> = equi_keys.iter()
        .map(|(_, r)| r.path_segments.last().unwrap()./* extract name */)
        .collect();
    Ok(Box::new(HashJoinStream::new(
        left_stream, right_stream,
        left_key_fields, right_key_fields,
        residual.map(|r| *r),
        join_type.clone(),
        memory_limit,
    )))
}
```

### 8d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_hash_join 2>&1 | tail -5
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -5
```

### 8e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/stream.rs src/execution/types.rs && git commit -m "$(cat <<'EOF'
feat: implement HashJoinStream with build/probe execution

Build side materialized into hashbrown::HashMap<JoinKey, SmallVec<[Record; 1]>>.
Supports INNER and LEFT join types. NULL keys excluded from build.
Memory budget enforced during build phase.
EOF
)"
```

---

## Step 9: End-to-end integration tests

**File**: `tests/` (or inline in `src/app.rs` tests)

### 9a. Write integration tests

```rust
#[test]
fn test_e2e_inner_join_with_hash() {
    // Create two temp JSONL files, run a JOIN query via app::run
    // Verify correct results
}

#[test]
fn test_e2e_left_join_with_hash() {
    // LEFT JOIN with qualified field names → hash join path
    // Verify NULL padding for unmatched rows
}

#[test]
fn test_e2e_left_join_without_alias_falls_back() {
    // LEFT JOIN without table aliases → nested-loop fallback
    // Verify same results as hash join
}

#[test]
fn test_e2e_cross_join_with_where_becomes_hash() {
    // FROM a, b WHERE a.id = b.id → internally becomes hash join
    // Verify correct inner-join semantics
}

#[test]
fn test_e2e_join_memory_limit_exceeded() {
    // Set tiny memory limit, verify clear error message
}
```

### 9b. Run all tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -10
```

### 9c. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add tests/ && git commit -m "$(cat <<'EOF'
test: add end-to-end integration tests for hash join

Cover INNER JOIN, LEFT JOIN with hash path, nested-loop fallback
for unqualified fields, CrossJoin+WHERE optimization, and memory
limit enforcement.
EOF
)"
```

---

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Steps 1, 2, 3, 4 | Yes (all independent) | stream.rs, Cargo.toml, ast.rs, parser.rs |
| 2 | Step 5 | No (needs Step 3 for JoinType::Inner) | logical/types.rs, execution/types.rs |
| 3 | Steps 6, 7 | Yes (6 needs Step 5; 7 needs Step 2; independent of each other) | logical/parser.rs, execution/stream.rs |
| 4 | Step 8 | No (needs Steps 4, 5, 6, 7) | execution/stream.rs, execution/types.rs |
| 5 | Step 9 | No (needs Step 8) | tests/ |
