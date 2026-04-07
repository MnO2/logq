# Planning Output

## Metadata
- **Planning Skill:** brainstorming
- **Date:** 2026-04-06
- **Project Name Hint:** logq
- **Plan File:** docs/plans/prefix-sort-design-final.md

## Architecture

### PrefixSort for ORDER BY

Introduce a PrefixSort optimization to logq's ORDER BY that encodes sort keys into a flat byte buffer once (O(N*K)), then sorts an index array using byte-slice comparisons. This eliminates per-comparison hash-map lookups, type dispatch, and string allocations.

**Encoding scheme:** Each sort key is encoded as `[null_byte][type_tag][value]` in a contiguous buffer. Type-specific encodings (sign-flip for integers/floats, big-endian byte order, string prefix truncation) make byte comparison equivalent to value comparison. DESC ordering applies bitwise NOT to all bytes.

**Three-phase algorithm:**
1. **Encode** — extract and encode sort keys from each Record into a flat byte buffer (one hash-map lookup per key per row, done once)
2. **Sort** — sort a `Vec<usize>` index array using `sort_unstable_by` with a comparator that does memcmp on the byte buffer, falling back to full `compare_values()` only when string prefixes tie
3. **Reorder** — build sorted output by moving Records via `Option::take()`

**Integration:** Single modification to `Node::OrderBy` match arm in `src/execution/types.rs`. New module `src/execution/prefix_sort.rs`. Falls back to current direct sort for < 64 rows.

**Expected impact:** 2-4x speedup on ORDER BY benchmarks for medium-to-large result sets.

## Key Decisions

### PrefixSort chosen over extract-and-index approach

**Chosen:** Full PrefixSort with byte encoding and memcmp comparison.

**Alternative considered:** Pre-extract sort key values into `Vec<Vec<Value>>`, sort an index array using existing `compare_values()`. Simpler (~20 lines) but retains type dispatch overhead per comparison.

**Why PrefixSort:** Cache-friendly contiguous buffer for comparison reads; eliminates type dispatch (not just hash lookups); aligns with project's SIMD/batch trajectory. The extract-and-index approach is documented as an acceptable fallback if PrefixSort proves overly complex.

### Null ordering via byte layout

**Chosen:** `[null_byte][type_tag][value]` layout where null_byte is the first byte compared.

**Why:** An earlier design had `[type_tag][null_byte][value]` which caused nulls to sort FIRST (type_tag 0x00 < any non-null type_tag). Caught during design review. The corrected layout ensures memcmp resolves null-vs-non-null on the first byte.

### Mixed-type columns get deterministic type-group ordering

**Chosen:** Type tag byte produces deterministic cross-type ordering (Bool < Int < Float < String < ...).

**Alternative:** Current behavior returns Equal for cross-type comparisons (depends on stable sort input order).

**Why:** Deterministic ordering is strictly better than input-order-dependent behavior. The previous behavior was undocumented and likely accidental.

## Stakeholders

- Paul Meng (author/maintainer)
