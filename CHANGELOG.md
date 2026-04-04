# CHANGELOG — PartiQL Implementation Progress

## Current Status
Starting Phase 1: Foundation (Data Model & Type System)

## Completed Tasks

### Phase 0: Code Cleanup (2026-04-04)
- **Step 1:** Fixed case-sensitivity — replaced all `tag()` with `tag_no_case()`, made `from_str` case-insensitive, expanded KEYWORDS list to 44 entries, removed `to_ascii_lowercase()` from main.rs. Fixed critical bug: precedence table lookup panicked on uppercase AND/OR.
- **Step 2:** Migrated `failure` crate to `thiserror`/`anyhow` — 13 error types across 6 files, 23 manual `From` impls replaced with `#[from]`. Net -210 lines.
- **Step 3:** Deduplicated `get_value_by_path_expr` — moved from execution/types.rs and execution/stream.rs to common/types.rs.
- **Step 4:** Fixed `ApproxCountDistinctAggregate::PartialEq` stub — replaced always-true with proper HyperLogLog count comparison.
- **Step 5:** Fixed version mismatch — synced cli.yml to 0.1.19.
- **Step 6:** Fixed `is_match_group_by_fields` nondeterministic bug — replaced positional HashSet iteration with proper set equality.
- **Step 7:** Fixed `LimitStream` early termination bug — added early return when limit reached.

## Failed Approaches
(none yet)

## Known Limitations
- Float arithmetic not supported (only Int)
- NULL/MISSING not properly propagated (TypeMismatch errors)
- CASE WHEN only supports single branch
- parse_logic panics on unrecognized expression types
- No LIKE, BETWEEN, IN, CAST, string concat, COALESCE/NULLIF
- No JOINs, subqueries, DISTINCT, SELECT VALUE (parsed but unimplemented)
- No UNION/INTERSECT/EXCEPT
- Formula::evaluate returns bool, not three-valued logic
- ORDER BY panics on mixed types
- date_part only supports Second and Minute
