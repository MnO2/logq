VERDICT: NEEDS_REVISION

## Summary Assessment

The design proposes a well-structured hybrid columnar batch execution model inspired by Velox, but has several critical issues: a type system mismatch between `TypedColumn` and the existing `Value` enum that silently drops data variants, a `std::arch` SIMD strategy that is impractical for the scope described while better Rust alternatives exist, and unaddressed correctness hazards around zero-copy string lifetimes and `SelectionVector` interactions with materializing operators. The overall architecture is sound in principle, but the implementation plan underestimates the complexity of bridging the existing row-oriented codebase with the proposed columnar model.

## Critical Issues (must fix)

### C1: TypedColumn does not cover all Value variants -- silent data loss

The existing `Value` enum (`src/common/types.rs`, line 15) has 11 variants:

```
Int(i32), Float(OrderedFloat<f32>), Boolean(bool), String(String),
Null, Missing, DateTime(...), HttpRequest(Box<HttpRequest>),
Host(Box<Host>), Object(Box<LinkedHashMap<...>>), Array(Vec<Value>)
```

The proposed `TypedColumn` has 6 variants: `Int32`, `Float32`, `Boolean`, `Utf8`, `DateTime`, `Mixed`. This means:

- **`Value::Null` and `Value::Missing` are semantically distinct in PartiQL** (the codebase relies on this distinction heavily in `Formula::evaluate`, `FunctionRegistry::call`, and IS NULL/IS MISSING predicates), but the design collapses them into a single null bitmap. The `BatchFilterOperator` will be unable to distinguish IS NULL from IS MISSING without a second bitmap or a separate mechanism.

- **`Value::Host` and `Value::HttpRequest`** are domain-specific structured types used pervasively in ELB/ALB log formats. The design does not mention them at all. They would presumably fall into `Mixed`, but ELB logs are the primary use case -- the most common queries will hit the `Mixed` fallback path, negating the SIMD benefit for the tool's core workload.

- **`Value::Object` and `Value::Array`** (used by JSONL) would also fall into `Mixed`. Since JSONL is one of the five supported formats, a significant fraction of real-world usage gets no SIMD benefit.

**Fix:** Add a separate MISSING bitmap (or a two-bit-per-value encoding for NULL/MISSING/present). Consider adding `Host` and `HttpRequest` columns, or at minimum document that the SIMD path does not apply to these fields and quantify the expected performance impact.

### C2: std::arch intrinsics are impractical for this scope; consider portable SIMD or auto-vectorization

The design proposes writing raw `std::arch` intrinsics across 4 backends (SSE2, AVX2, NEON, scalar), implementing ~10 kernel functions each. That is roughly 40 separate unsafe function implementations, all of which need independent testing and maintenance. For a ~19K-line single-developer project, this is a disproportionate maintenance burden.

Concrete problems with the `std::arch` approach:

1. **Compile-time dispatch is wrong for binary distribution.** The design uses `#[cfg(target_feature = "avx2")]` which is a compile-time check. A binary compiled on an AVX2 machine will crash on a non-AVX2 machine, and a binary compiled without `target_feature` will never use AVX2. Velox solves this with runtime dispatch (`cpuid`); the design does not address this. Rust's `is_x86_feature_detected!()` macro exists for runtime dispatch but is not mentioned.

2. **`_mm_crc32_u32` requires SSE4.2, not SSE2.** Section 6 lists CRC32 hashing under the SSE2 backend but `_mm_crc32_u32` is an SSE4.2 instruction. The SSE2 backend would need a different hash function.

3. **The `str_contains` SIMD kernel (Velox-style simdStrstr) is one of the hardest SIMD algorithms to implement correctly.** It handles variable-length strings with offsets, requires careful boundary checking across SIMD lanes, and has subtle edge cases with strings shorter than the SIMD width. This alone could take weeks to implement and debug.

**Alternatives to consider:**
- **`std::simd` (portable SIMD)** -- while still nightly, it is actively being stabilized and provides a single-source implementation across all architectures. It would eliminate 3 of the 4 backends.
- **The `wide` crate** -- stable Rust, provides portable SIMD types.
- **Let LLVM auto-vectorize.** Many of the proposed kernels (integer comparison, addition, sum) will auto-vectorize if written as simple loops with `--target-cpu=native`. The scalar fallback may already be fast enough with LLVM's vectorizer, especially for the simple filter and arithmetic cases. Benchmark the scalar fallback before writing intrinsics.

### C3: Zero-copy string lifetime management is underspecified and likely unsound

Section 5 states: "the `Utf8` column stores byte offsets into the shared line buffer. The buffer lives as long as the batch." But this creates a self-referential structure -- the `ColumnBatch` would own the line buffer while the `Utf8` column references into it. In Rust, this is either:

- Impossible without unsafe (self-referential structs are not supported by the borrow checker)
- Requires a crate like `ouroboros` or `self_cell`
- Requires arena allocation with explicit lifetime parameters on `ColumnBatch<'a>`, which would infect the entire `BatchStream` trait with lifetime parameters, making it non-object-safe (`Box<dyn BatchStream>` would not work without `'static` bounds)

The current `LogTokenizer` in `datasource.rs` (line 93) already borrows `&'a str` from the input, and each field is converted to an owned `Value::String(s.to_string())` on line 899. The zero-copy design would need to fundamentally change this ownership model.

**Fix:** Either (a) use `Arc<[u8]>` or `Bytes` as a shared buffer with `(start, len)` pairs instead of raw offsets, eliminating the self-referential issue, or (b) accept the allocation cost and use owned strings in the `Utf8` column (offset-encoded but into an owned `Vec<u8>` within the column itself, not into an external buffer). Option (b) is what Arrow actually does and it is still much more cache-friendly than per-row `String` allocations.

### C4: SelectionVector interaction with materializing operators is unspecified

The design says "Compaction only happens when the active ratio drops below ~25%." But several operators **must** materialize -- they cannot work with selection vectors:

- **GROUP BY** (`GroupByStream`): Currently materializes all input into a `HashSet<Option<Tuple>>` and aggregator maps. The batch GROUP BY must read all active rows and insert them into a hash table. What happens to the selection vector after materialization? Is the output batch always `SelectionVector::All`?

- **ORDER BY** (`Node::OrderBy`): Currently loads all records into a `Vec<Record>`, sorts, and emits. The batch ORDER BY must produce fully materialized, compacted output batches. The design says "Vectorized comparison keys, SIMD radix sort" but does not address how partial batches (with selection vectors) are compacted before sorting.

- **DISTINCT** (`DistinctStream`): Uses `HashSet<Vec<(VariableName, Value)>>` for dedup. What does the batch version hash? It needs to compact or iterate only active rows.

- **Set operations** (`IntersectStream`, `ExceptStream`): Materialize the right side entirely.

**Fix:** Explicitly specify that materializing operators (GROUP BY, ORDER BY, DISTINCT, set operations) compact their input batches (applying the selection vector to produce dense output) before processing. Document which operators produce `SelectionVector::All` output and which propagate selection vectors.

### C5: The `elb_status_code` example is misleading -- status codes are strings, not integers

The design's filtering example `col >= 500` (Section 4) implies integer SIMD comparison on status codes. But examining the actual schema (`src/execution/datasource.rs`, lines 193-213), `elb_status_code` and `backend_status_code` are `DataType::String`, not `DataType::Integral`. The only integer fields in ELB logs are `received_bytes` and `sent_bytes`.

This matters because the "SIMD opportunity" for the project's core use case (ELB/ALB log filtering) is primarily string comparison, not integer comparison. The design heavily emphasizes integer SIMD kernels but does not adequately address the fact that most ELB fields are strings, floats, or domain-specific types (`Host`, `HttpRequest`).

**Fix:** Either (a) change the schema to parse status codes as integers (breaking change for existing queries), or (b) acknowledge that the primary SIMD benefit for structured logs comes from string operations and tokenization, not integer filtering, and adjust the prioritization accordingly.

## Suggestions (nice to have)

### S1: Consider Arrow as the columnar format instead of a custom implementation

The proposed `TypedColumn` is essentially a simplified Arrow columnar format. The `arrow` crate is mature, stable, well-tested, and already handles all the edge cases (null bitmaps, string offsets, nested types, SIMD-friendly alignment). Using it would:
- Eliminate the need to implement and test `TypedColumn`, `SelectionVector`, and null bitmap management
- Provide SIMD-optimized compute kernels out of the box via `arrow::compute`
- Handle the zero-copy string problem (Arrow uses an owned `Buffer` + offsets model)
- Enable future interop with DataFusion if more advanced query processing is needed

The downside is a heavier dependency, but given the project is already at 19K lines and adding this feature would likely double it, the tradeoff may be worthwhile.

### S2: Float precision mismatch between Value::Float(f32) and TypedColumn::Float32

The design uses `Float32(Vec<f32>)` which matches the existing `Value::Float(OrderedFloat<f32>)`. However, the aggregation section (Section 6) uses `Vec<f64>` accumulators for `SUM(float)` and `AVG`. The existing `AvgAggregate` (line 939 of `execution/types.rs`) already has precision issues -- it accumulates into `OrderedFloat<f32>`. If the design intends to fix this (f64 accumulators), it should also add a `Float64` column variant, or document that accumulator precision differs from column precision.

### S3: The `simd-json` dependency for JSONL is reasonable but should be noted

The design mentions using the `simd-json` crate. This is a good choice, but note that `simd-json` requires mutable access to the input buffer (it modifies the input in-place for zero-copy parsing). This is different from the current `json::parse()` usage which works on borrowed `&str`. The migration will need to change the JSONL parsing path to pass owned/mutable buffers.

### S4: BitVec crate selection matters for performance

The design references `BitVec` without specifying which crate. The `bitvec` crate is the most popular but has known performance issues due to its generality (it supports arbitrary bit ordering and storage). For SIMD-oriented workloads, a simpler `Vec<u64>` with manual bit manipulation (as Arrow does) would be faster and more natural for SIMD operations (you process 64 rows at a time per word).

### S5: Batch size should be configurable, not hardcoded at 1024

While 1024 is a reasonable default, different workloads benefit from different batch sizes. Wide schemas (25 columns for ALB) will exceed L1 cache at 1024 rows. The design should allow batch size tuning, at least as a compile-time constant.

### S6: The adapter approach adds overhead that should be benchmarked

The `BatchToRowAdapter` and `RowToBatchAdapter` in Phase 2 will materialize/dematerialize batches at every boundary. If the plan tree is `BatchScan -> BatchFilter -> BatchToRowAdapter -> OrderByStream`, every filtered batch gets converted to individual Records. This could be slower than the current all-row path due to the conversion overhead. Benchmark the adapter path against the baseline before declaring Phase 2 a success.

### S7: CRC32 is a poor choice for hash table probing

CRC32 is fast but has poor distribution properties for hash table use. Velox actually uses xxHash or MurmurHash for hash table probing, not CRC32. CRC32 is used for checksumming, not general-purpose hashing. Using CRC32 for group-by hash tables could lead to high collision rates, degrading performance.

### S8: The 10x performance target needs qualification

The "10x+ on large log files" target is aspirational but ungrounded. The current bottleneck for large files is likely I/O and parsing (regex/tokenizer), not expression evaluation. If parsing takes 80% of execution time, even infinitely fast expression evaluation yields only 5x. The design should include a profiling step to identify where time is actually spent before committing to specific targets.

## Verified Claims (things I confirmed are correct)

1. **The existing execution model is Volcano-style pull (confirmed).** `RecordStream::next()` returns one record at a time, exactly as described. All operators follow this pattern.

2. **The operator listing is accurate.** All listed current streams exist in `stream.rs`: `LogFileStream`, `FilterStream`, `MapStream`, `GroupByStream`, `LimitStream`, `DistinctStream`, `CrossJoinStream`, `LeftJoinStream`, `UnionStream`, `IntersectStream`, `ExceptStream`, plus `ProjectionStream` (used for LATERAL-like array expansion, not mentioned in the design but not relevant to the migration).

3. **The LogTokenizer does scan bytes one at a time (confirmed).** `src/execution/datasource.rs` lines 93-165 show a byte-at-a-time tokenizer with match on individual bytes. This is a legitimate SIMD opportunity.

4. **The `parse_utc_timestamp` function already uses byte-level parsing (confirmed).** `datasource.rs` lines 21-64 show a hand-rolled byte parser with `parse_2digit`/`parse_4digit` helpers. This is already optimized and may not benefit much from SIMD.

5. **Batch size of 1024 for i32 = 4KB (confirmed).** 1024 * 4 bytes = 4096 bytes = 4KB, which fits in L1 cache on all modern x86/ARM CPUs.

6. **The codebase currently has no SIMD code (confirmed).** No imports of `std::arch`, no `bitvec` dependency, no SIMD-related crates in `Cargo.toml`. This is a greenfield addition.

7. **The existing benchmarking infrastructure exists (confirmed).** `benches/bench_execution.rs` has tier-A (end-to-end) and tier-B (isolated operator) benchmarks that can serve as baseline measurements.

8. **496 tests currently pass.** The test suite is healthy and would serve as a regression safety net during migration.

9. **The Expression/Formula distinction matches the design's predicate model.** The existing `Formula` enum (with `And`, `Or`, `Not`, `Predicate`, `Like`, `In`, etc.) maps cleanly to the proposed predicate-to-kernel table.

10. **The `FunctionRegistry` architecture supports the fallback path.** User-defined and built-in functions go through `registry.call()` which operates on `&[Value]`. The design's fallback path (iterate batch, call scalar `expression_value()`) would work with the existing function infrastructure without modification.
