# Int64 / Float64 migration proposal

Status: proposed contract, **not the current runtime**. The 2026-09-06 milestones
retain `Value::Int(i32)`, `Value::Float(OrderedFloat<f32>)`, Int32 / Float32
columns, and their current output behavior. The boundary regression in
[`tests/execution_matrix.rs`](../tests/execution_matrix.rs) deliberately records
today's precision loss outside the i32 range. It is not an endorsement of that
behavior for a future wider runtime.

This proposal makes the gates in the
[earlier numeric contract](performance-expansion-2026-09-05.md#numeric-migration-contract)
concrete. It requires an explicit compatibility release and a corresponding
change to the invariant in [`CLAUDE.md`](../CLAUDE.md). Increasing the widths in a
reader or a fast kernel alone is not an acceptable migration.

## Input and representation

The proposed runtime types are signed Int64 and IEEE 754 binary64 Float64.
Boolean, string, object, array, date/time, host, request, NULL and MISSING retain
their existing representations and nonnumeric semantics.

| Input | Proposed result |
| --- | --- |
| Integer JSON token or integer SQL literal in `-9223372036854775808..=9223372036854775807` | Exact Int64 |
| Integer token outside that range, including a JSON u64 above `i64::MAX` | Explicit numeric-range error |
| Token containing a decimal point or exponent | Float64, rounded once from the decimal token |
| Negative zero token, including `-0` | Float64 negative zero |
| Decimal/exponent token outside finite Float64 range, such as `1e309` | Explicit numeric-range error |
| Finite token too small for a nonzero Float64 | Rounded subnormal or signed zero |

There is no automatic decimal, unsigned-integer or string fallback. A separate
Decimal type would need its own proposal. In particular, an out-of-range integer
identifier must never become a rounded floating-point grouping or join key.
The lexer must allow the full negative endpoint before applying unary negation;
parsing its positive magnitude as an already-valid signed literal would reject
`i64::MIN` incorrectly.

JSONL, fixed-format integral and floating fields, custom regex conversions, SQL
literals and explicit casts must use the same range decisions. Pruned JSON
values still receive the same validation as retained values. Duplicate keys
remain last-value-wins, and every duplicate value is validated before replacement.
Changing numeric width must not make LIMIT consume additional input.

## Arithmetic and casts

- Int64 addition, subtraction, multiplication, negation, absolute value and
  division use checked operations. Overflow is a query error, including
  `i64::MIN / -1`, `-i64::MIN` and `abs(i64::MIN)`. Integral division continues
  to truncate toward zero; division by zero retains the existing NULL behavior.
- Integer shift counts must lie in `0..64`. Left shift also errors if the
  mathematical result is outside Int64; right shift is arithmetic. This makes
  the widened shift contract explicit instead of inheriting a wrapping cast.
- An arithmetic operation with a Float64 operand returns Float64. Conversion
  of an Int64 operand to Float64 uses nearest representable binary64, so mixed
  arithmetic can round integers above `2^53`. Equality, ordering and hashing
  must use the exact comparison rules below instead of this arithmetic coercion.
- Each floating operation rounds to binary64 in expression order. Reassociation
  and fused multiply-add are disallowed unless a separately documented operation
  requests them. Integer operations must never enter an unchecked float kernel.
- Floating arithmetic can still produce infinity or NaN through IEEE operations
  and existing mathematical functions. Finite-only validation applies to parsed
  external numeric tokens; it does not silently replace internal nonfinite values.
- `CAST(... AS INT)` produces Int64. A finite Float64 is truncated toward zero
  and accepted only when the resulting mathematical integer fits in Int64.
  Nonfinite floats and out-of-range values error; saturating Rust casts are
  insufficient. String-to-Int64 accepts the checked integer grammar.
- `CAST(... AS FLOAT)` produces Float64. Int64 conversion can round as described
  above; converting an existing Float64 preserves it. String conversion accepts
  finite numeric tokens and rejects special strings such as `NaN` and `inf`.
- NULL/MISSING propagation, lazy branches, argument evaluation order and custom
  function calls retain the existing contracts. In particular, MISSING dominates
  NULL in the current propagating arithmetic functions.

## Aggregate results and execution order

| Aggregate | Proposed result and state |
| --- | --- |
| `COUNT(*)`, `COUNT(value)` | Checked nonnegative Int64 count; empty input yields zero |
| Integer-only `SUM` | Checked i128 internal total, checked final conversion to Int64 |
| `SUM` with any Float64 input | Float64 |
| `AVG` | Float64 total divided by a checked Int64 count |
| `MIN`, `MAX`, `FIRST`, `LAST`, exact discrete percentile | Preserve the selected input value's Int64 / Float64 type |
| Approximate distinct count | Checked Int64 conversion of its existing estimator result |
| Approximate percentile | Float64 |

SUM and AVG continue to ignore NULL/MISSING and return NULL when there are no
numeric inputs. Other invalid input types retain explicit aggregate errors.

The i128 SUM state is a deliberate proposal: `[i64::MAX, 1, -1]` returns
`i64::MAX`, even if a worker's subtotal exceeds i64. Final overflow still errors.
Checked partial-state merges and a checked count prevent silent intermediate
overflow; they must not narrow each worker's result to i64 before merging.

For Float64 SUM/AVG, the reference algorithm accumulates numeric inputs in source
order with binary64 rounding after each addition. An implementation can maintain
an exact integer subtotal and a parallel reference floating subtotal while
discovering whether a group contains a Float64. The integer-only SUM selects
the exact subtotal; mixed SUM and AVG select the floating subtotal. This avoids
letting the first floating row change how earlier integers were accumulated.

Batching must preserve that reference result. A parallel implementation that
cannot reproduce it must retain ordered accumulation or use the serial aggregate
path. Arbitrary merging of rounded Float64 partial sums is not implicitly
authorized by the width change. A different reproducible or approximate summation
contract would need its own acceptance tests and documented decision.

## Equality, hashing and ordering

All numeric consumers must share one canonical numeric comparison and key
representation: scalar equality, GROUP BY, DISTINCT, hash joins, approximate
distinct hashing, sorting, array sorting, extrema and percentile ordering.

1. Int64 / Int64 compares exactly. Mixed Int64 / Float64 compares their
   mathematical values without first converting the integer to Float64. Compare
   a finite float's exponent and significand, or use an equivalently exact
   decomposition; a saturating float-to-integer cast is not a comparator.
2. An integral finite Float64 inside the Int64 range is equal to the exact Int64
   with that value and receives the same numeric key encoding and hash. A
   Float64 at `2^63` is outside that range and is greater than `i64::MAX`.
3. Int64 zero, positive Float64 zero and negative Float64 zero are equal numeric
   keys with one canonical encoding. Projection preserves the original float
   sign bit; normalization applies to comparison and keys.
4. All NaN payloads form one equality/key class, matching the intended
   OrderedFloat-style convention. NaN sorts after positive infinity, which
   sorts after finite numbers; negative infinity sorts before finite numbers.
   Numeric equality therefore treats NaN as equal to NaN under this proposal.
5. NULL and MISSING remain distinct key classes, with different hashes and
   presence semantics. Ordinary SQL predicates involving either remain unknown.
   Both retain their current final ascending sort position and stable tie;
   their sort tie does not merge their distinct grouping keys.
6. Equal keys must hash identically in every execution path. Objects and arrays
   containing numbers must apply the same recursive numeric rules while retaining
   their existing structural semantics. Approximate algorithms use the same
   canonical key bytes; hashing must not narrow values through Float32 or Float64.

The current `Value` derives equality and hashing from its variants. The migration
must explicitly replace or reconcile that implementation with cross-type numeric
equality, alongside encoded keys and prefix comparisons. Replacing only the
`OrderedFloat` parameter would leave consumers inconsistent.

Stable sort ties and the chosen representative of equal numeric keys preserve
source order. If `1.0` is the first occurrence and `1` is later, a DISTINCT output
can retain the original Float64 representative; it must still produce one key.

## Output, public API and persisted data

JSON/NDJSON writes Int64 values as exact decimal integers and finite Float64
values using a shortest round-tripping binary64 representation. It must not
round through f32, parse an i64 through f64, or change a numeric key to a string.
Signed floating zero is preserved. Following the current output policy, internal
infinity and NaN serialize to JSON null; NULL and MISSING also serialize to null.
These conversions make ordinary JSON output unsuitable as a lossless execution
checkpoint. Table and CSV numeric formatting must also be documented and tested.

Public Rust consumers will need to update pattern matches, typed-column element
types, `OrderedFloat<f32>` uses, function argument/result handling, size estimates
and any serialized `Value` representations. This is a source and behavior
compatibility change even if enum variant names stay the same. A release must
identify it clearly and supply before/after examples; no version bump or API
migration is part of the current milestone.

Any future prepared Parquet data, spill representation, cache identity or schema
fingerprint must declare the numeric-contract version. An older Float32 dataset
cannot regain lost bits by relabeling its columns Float64. Rebuild it from the
original source. A persisted representation must also preserve NULL versus
MISSING, mixed numeric types, nonfinite internal values and duplicate-key policy,
or explicitly reject unsupported values before writing any reusable result.

## Adoption gates and executable checks

First keep the current-width test passing on the current runtime:

```sh
cargo test --test execution_matrix current_numeric_width_contract_is_explicit_across_row_and_batch
```

The following are proposed migration assertions, not claims that current logq
passes them. Convert them into focused Rust value/key tests and CLI fixtures
before changing production widths.

| Case | Required assertion |
| --- | --- |
| JSON `2147483648` and `2147483649` | Two exact Int64 values and distinct keys |
| JSON `9007199254740992` and `9007199254740993` | Two exact Int64 values and distinct keys |
| Int64 `9007199254740993` versus Float64 `9007199254740992.0` | Integer is greater; keys are unequal |
| Int64 `9223372036854775807` versus Float64 `9223372036854775808.0` | Integer is smaller; keys are unequal |
| JSON `9223372036854775808`, `18446744073709551615`, `-9223372036854775809` | Range errors, including inside pruned fields |
| JSON `1e309` and an ignored nested `1e309` | The same range error |
| Int64 endpoints, `2^24 ± 1`, `2^31 ± 1`, `2^53 ± 1`, `2^63` boundaries | Exact reader/literal/cast classification and expected ordering |
| `i64::MAX + 1`, `i64::MIN / -1`, float-to-int cast of `2^63` | Explicit errors without panic or saturation |
| Integer SUM of `[i64::MAX, 1, -1]` | Exact `i64::MAX` for every partition layout |
| Integer SUM of `[i64::MAX, 1]` | Final overflow error for every partition layout |
| `Int64(0)`, `Float64(0.0)`, `Float64(-0.0)` | Equal keys/hashes; original projected float sign preserved |
| Distinct NaN payloads, infinities, NULL and MISSING | Canonical numeric classes, explicit nullish distinction and stable ordering |

Use independent integer oracles for the large-integer cases; converting both
expected and actual values to f64 would conceal failures above `2^53`. Check
Float64 bit patterns and sign bits directly where exact behavior is required.
Hash tests assert equality compatibility, not fixed randomized hash outputs.

Run the matrix through row and batch readers, plain and concatenated gzip input,
mixed shards, one and multiple threads, computed projections, grouping, DISTINCT,
joins, full sorting, Top-N, all output modes and custom function calls. Include
empty input, masked rows, schema/type changes between batches, duplicate aliases,
malformed tails and prefix LIMIT. COUNT overflow can be tested using constructed
partial states rather than generating billions of rows.

Only after all consumers and fixtures agree should benchmarks assess the extra
column bandwidth, bytes per group/key, retained-memory estimates, output size
and end-to-end latency. An isolated faster kernel cannot justify incomplete
numeric migration. The current adoption decision is therefore **defer runtime
widening**, keep the executable current contract, and require the complete
compatibility migration above before enabling Int64 / Float64 or publishing a
prepared-data format that depends on it.
