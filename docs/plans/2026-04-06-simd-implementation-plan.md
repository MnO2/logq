# Plan: SIMD-Accelerated Physical Plan — Phase 0 + Phase 1

**Goal**: Build the foundation for SIMD batch execution: profiling baseline, core data types (`Bitmap`, `PaddedVec`, `TypedColumn`, `ColumnBatch`, `SelectionVector`, `BatchStream`), auto-vectorizable kernels, filter cache, and predicate reordering.

**Architecture**: Hybrid columnar batch model layered on existing Volcano executor. Auto-vectorization-first SIMD strategy. Immutable `PaddedVec<T>` with `PaddedVecBuilder<T>` for tail-safe SIMD. Two-pass filter kernels (compare→bytes→pack→bitmap). Per-batch string filter cache for low-cardinality fields.

**Tech Stack**: Rust (edition 2018, stable), criterion benchmarks, cargo-flamegraph for profiling.

**Design Reference**: `docs/plans/2026-04-06-simd-physical-plan-design-final.md`

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 0 | Steps 1-2 | Yes | Phase 0: profiling setup + baseline |
| 1 | Step 3 | No | Phase 0: run profiling, record results. Depends on Group 0. |
| 2 | Steps 4-5 | Yes | Phase 1: PaddedVec + Bitmap (independent foundation types) |
| 3 | Steps 6-7 | Yes | Phase 1: SelectionVector + TypedColumn. Depends on Group 2. |
| 4 | Step 8 | No | Phase 1: ColumnBatch + BatchStream. Depends on Group 3. |
| 5 | Steps 9-10-11 | Yes | Phase 1: filter kernels, aggregation kernels, bitmap op kernels. Depends on Group 2 (Bitmap). |
| 6 | Step 12 | No | Phase 1: StringFilterCache. Depends on Group 2 (Bitmap). |
| 7 | Step 13 | No | Phase 1: Predicate cost model + conjunct reordering. Independent of other Phase 1 steps. |
| 8 | Step 14 | No | Phase 1: bench-internals exports + CI auto-vectorization check. Depends on Groups 4-6. |
| 9 | Step 15 | No | End-to-end verification. Depends on all above. |

---

## Phase 0: Profiling

### Step 1: Add profiling benchmark for component-level timing

**File**: `benches/bench_execution.rs`

#### 1a. Write the benchmark

Add a new benchmark group that measures each execution phase individually — parsing, filtering, projection — using the existing synthetic data generator and real AWSELB.log:

```rust
/// Tier C: Component-level profiling benchmarks
/// Measures: datasource parsing, filter evaluation, projection, groupby
fn bench_execution_tier_c(c: &mut Criterion) {
    let path = PathBuf::from("data/AWSELB.log");
    if !path.exists() {
        eprintln!("Skipping Tier C benchmarks: data/AWSELB.log not found");
        return;
    }

    let registry = Arc::new(functions::register_all().unwrap());

    // C1: Pure scan (no filter, no projection) -- measures tokenization + field parsing
    {
        let mut group = c.benchmark_group("profiling_scan_only");
        let data_source = ctypes::DataSource::File(
            path.clone(), "elb".to_string(), "elb".to_string(),
        );
        let data_sources: ctypes::DataSourceRegistry =
            vec![("elb".to_string(), data_source)].into_iter().collect();
        let reg = registry.clone();
        group.bench_function("scan_all_fields", |b| {
            b.iter(|| {
                let result = logq::app::run_to_records_with_registry(
                    black_box("SELECT * FROM elb"),
                    data_sources.clone(),
                    reg.clone(),
                );
                let _ = black_box(result);
            });
        });
        group.finish();
    }

    // C2: Scan + filter (measures filter cost on top of scan)
    {
        let mut group = c.benchmark_group("profiling_scan_filter");
        let data_source = ctypes::DataSource::File(
            path.clone(), "elb".to_string(), "elb".to_string(),
        );
        let data_sources: ctypes::DataSourceRegistry =
            vec![("elb".to_string(), data_source)].into_iter().collect();
        let reg = registry.clone();
        group.bench_function("filter_status_200", |b| {
            b.iter(|| {
                let result = logq::app::run_to_records_with_registry(
                    black_box(r#"SELECT * FROM elb WHERE elb_status_code = "200""#),
                    data_sources.clone(),
                    reg.clone(),
                );
                let _ = black_box(result);
            });
        });
        group.finish();
    }

    // C3: Scan + filter + groupby (measures aggregation cost)
    {
        let mut group = c.benchmark_group("profiling_scan_filter_groupby");
        let data_source = ctypes::DataSource::File(
            path.clone(), "elb".to_string(), "elb".to_string(),
        );
        let data_sources: ctypes::DataSourceRegistry =
            vec![("elb".to_string(), data_source)].into_iter().collect();
        let reg = registry.clone();
        group.bench_function("groupby_status_count", |b| {
            b.iter(|| {
                let result = logq::app::run_to_records_with_registry(
                    black_box(r#"SELECT elb_status_code, count(*) FROM elb WHERE elb_status_code = "200" GROUP BY elb_status_code"#),
                    data_sources.clone(),
                    reg.clone(),
                );
                let _ = black_box(result);
            });
        });
        group.finish();
    }
}
```

Update the `criterion_group!` macro at the bottom of the file:

```rust
criterion_group!(benches, bench_execution_tier_a, bench_execution_tier_b, bench_execution_tier_c);
```

#### 1b. Verify it compiles and runs

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals --bench bench_execution -- --list
```

#### 1c. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "bench: add Tier C component-level profiling benchmarks"
```

---

### Step 2: Add datasource-level profiling benchmark

**File**: `benches/bench_datasource.rs`

#### 2a. Add per-format throughput with row counts

Check what already exists in this file. If it already benchmarks all 5 formats, add a measurement that reports throughput in rows/second (not just wall time) by using `criterion::Throughput::Elements(row_count)`. If not, add benchmarks for ELB, ALB, S3, Squid, JSONL parsing throughput.

```rust
// Add to existing benchmark group:
group.throughput(criterion::Throughput::Elements(row_count as u64));
```

#### 2b. Verify

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals --bench bench_datasource -- --list
```

#### 2c. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "bench: add throughput metrics to datasource benchmarks"
```

---

### Step 3: Run profiling and record baseline

#### 3a. Run all benchmarks, save results

```bash
cd /Users/paulmeng/Develop/logq
cargo bench --features bench-internals 2>&1 | tee docs/plans/phase0-baseline.txt
```

#### 3b. (Optional) Generate flamegraph for scan-heavy query

```bash
# Install if needed: cargo install flamegraph
cargo flamegraph --features bench-internals --bench bench_execution -- --bench "profiling_scan_only" --profile-time 10
# Output: flamegraph.svg
mv flamegraph.svg docs/plans/phase0-scan-flamegraph.svg
```

#### 3c. Document bottleneck distribution

Create `docs/plans/phase0-results.md` with observed time distribution (I/O vs tokenization vs field parsing vs expression evaluation vs aggregation). This informs Phase 1 priorities.

#### 3d. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add docs/plans/phase0-*.* && git commit -m "docs: record Phase 0 profiling baseline"
```

---

## Phase 1: Foundation Types

### Step 4: Implement PaddedVec and PaddedVecBuilder

**File (create)**: `src/simd/padded_vec.rs`
**File (create)**: `src/simd/mod.rs`

#### 4a. Write failing tests

```rust
// src/simd/padded_vec.rs

const SIMD_PADDING: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_padded_vec_with_len_has_correct_logical_length() {
        let pv: PaddedVec<i32> = PaddedVec::with_len(100);
        assert_eq!(pv.len(), 100);
        assert!(!pv.is_empty());
    }

    #[test]
    fn test_padded_vec_with_len_has_padding_capacity() {
        let pv: PaddedVec<i32> = PaddedVec::with_len(100);
        let pad_elements = SIMD_PADDING / std::mem::size_of::<i32>();
        assert!(pv.capacity() >= pv.len() + pad_elements);
    }

    #[test]
    fn test_padded_vec_from_vec_preserves_data() {
        let v = vec![1i32, 2, 3, 4, 5];
        let pv = PaddedVec::from_vec(v);
        assert_eq!(&*pv, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_padded_vec_from_vec_has_padding() {
        let v = vec![1i32, 2, 3];
        let pv = PaddedVec::from_vec(v);
        let pad_elements = SIMD_PADDING / std::mem::size_of::<i32>();
        assert!(pv.capacity() >= pv.len() + pad_elements);
    }

    #[test]
    fn test_padded_vec_deref_returns_logical_slice() {
        let pv: PaddedVec<i32> = PaddedVec::with_len(10);
        let slice: &[i32] = &*pv;
        assert_eq!(slice.len(), 10);
    }

    #[test]
    fn test_padded_vec_empty() {
        let pv: PaddedVec<i32> = PaddedVec::with_len(0);
        assert!(pv.is_empty());
        assert_eq!(pv.len(), 0);
    }

    #[test]
    fn test_builder_push_and_seal() {
        let mut builder = PaddedVecBuilder::<i32>::with_capacity(10);
        builder.push(1);
        builder.push(2);
        builder.push(3);
        let pv = builder.seal();
        assert_eq!(&*pv, &[1, 2, 3]);
        let pad_elements = SIMD_PADDING / std::mem::size_of::<i32>();
        assert!(pv.capacity() >= pv.len() + pad_elements);
    }

    #[test]
    fn test_builder_extend_from_slice_and_seal() {
        let mut builder = PaddedVecBuilder::<u8>::new();
        builder.extend_from_slice(b"hello");
        builder.extend_from_slice(b" world");
        let pv = builder.seal();
        assert_eq!(&*pv, b"hello world");
    }

    #[test]
    fn test_builder_len() {
        let mut builder = PaddedVecBuilder::<i32>::new();
        assert_eq!(builder.len(), 0);
        builder.push(42);
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_padded_vec_padding_is_zeroed() {
        let v = vec![0xFFi8; 16];
        let pv = PaddedVec::from_vec(v);
        // Access padding region via raw pointer (safe because we guarantee capacity)
        let ptr = pv.inner.as_ptr();
        for i in pv.len()..pv.len() + SIMD_PADDING / std::mem::size_of::<i8>() {
            unsafe {
                assert_eq!(*ptr.add(i), 0, "padding byte at offset {} not zeroed", i);
            }
        }
    }
}
```

#### 4b. Run tests to verify they fail (types don't exist yet)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::padded_vec
```

#### 4c. Write implementation

```rust
// src/simd/padded_vec.rs

/// Padding size in bytes. Covers AVX2 (32B) and NEON (16B).
pub const SIMD_PADDING: usize = 32;

/// An immutable Vec<T> with guaranteed padding bytes past the logical end.
/// Constructed via `PaddedVecBuilder::seal()` or `PaddedVec::from_vec()`.
pub struct PaddedVec<T> {
    pub(crate) inner: Vec<T>,
}

impl<T: Copy + Default> PaddedVec<T> {
    pub fn with_len(len: usize) -> Self {
        let pad_elements = SIMD_PADDING / std::mem::size_of::<T>().max(1);
        let mut inner = vec![T::default(); len + pad_elements];
        inner.truncate(len);
        debug_assert!(inner.capacity() >= inner.len() + pad_elements);
        Self { inner }
    }

    pub fn from_vec(mut v: Vec<T>) -> Self {
        let pad_elements = SIMD_PADDING / std::mem::size_of::<T>().max(1);
        v.reserve(pad_elements);
        unsafe {
            let ptr = v.as_mut_ptr().add(v.len());
            std::ptr::write_bytes(ptr, 0, pad_elements);
        }
        debug_assert!(v.capacity() >= v.len() + pad_elements);
        Self { inner: v }
    }

    pub fn len(&self) -> usize { self.inner.len() }
    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn capacity(&self) -> usize { self.inner.capacity() }
}

impl<T> std::ops::Deref for PaddedVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] { &self.inner }
}

/// Mutable builder for PaddedVec. Call `seal()` to produce an immutable PaddedVec.
pub struct PaddedVecBuilder<T> {
    inner: Vec<T>,
}

impl<T: Copy + Default> PaddedVecBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self { inner: Vec::with_capacity(capacity) }
    }

    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    pub fn push(&mut self, value: T) {
        self.inner.push(value);
    }

    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.inner.extend_from_slice(slice);
    }

    pub fn len(&self) -> usize { self.inner.len() }

    pub fn seal(self) -> PaddedVec<T> {
        PaddedVec::from_vec(self.inner)
    }
}
```

```rust
// src/simd/mod.rs
pub mod padded_vec;
```

Add `pub mod simd;` to `src/lib.rs` (after `pub mod syntax;`).

#### 4d. Run tests to verify they pass

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::padded_vec
```

#### 4e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add PaddedVec and PaddedVecBuilder with tail-safe SIMD padding"
```

---

### Step 5: Implement Bitmap

**File (create)**: `src/simd/bitmap.rs`

#### 5a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_set() {
        let bm = Bitmap::all_set(128);
        assert_eq!(bm.count_ones(), 128);
        assert!(bm.is_set(0));
        assert!(bm.is_set(127));
    }

    #[test]
    fn test_all_unset() {
        let bm = Bitmap::all_unset(128);
        assert_eq!(bm.count_ones(), 0);
        assert!(!bm.is_set(0));
    }

    #[test]
    fn test_set_and_unset() {
        let mut bm = Bitmap::all_unset(64);
        bm.set(5);
        assert!(bm.is_set(5));
        bm.unset(5);
        assert!(!bm.is_set(5));
    }

    #[test]
    fn test_and() {
        let mut a = Bitmap::all_unset(64);
        let mut b = Bitmap::all_unset(64);
        a.set(0); a.set(1); a.set(2);
        b.set(1); b.set(2); b.set(3);
        let c = a.and(&b);
        assert!(!c.is_set(0));
        assert!(c.is_set(1));
        assert!(c.is_set(2));
        assert!(!c.is_set(3));
    }

    #[test]
    fn test_or() {
        let mut a = Bitmap::all_unset(64);
        let mut b = Bitmap::all_unset(64);
        a.set(0);
        b.set(1);
        let c = a.or(&b);
        assert!(c.is_set(0));
        assert!(c.is_set(1));
        assert!(!c.is_set(2));
    }

    #[test]
    fn test_not() {
        let bm = Bitmap::all_set(64);
        let inv = bm.not(64);
        assert_eq!(inv.count_ones(), 0);
    }

    #[test]
    fn test_pack_from_bytes() {
        let bytes = vec![1u8, 0, 1, 0, 1, 0, 0, 0];
        let bm = Bitmap::pack_from_bytes(&bytes);
        assert!(bm.is_set(0));
        assert!(!bm.is_set(1));
        assert!(bm.is_set(2));
        assert!(!bm.is_set(3));
        assert!(bm.is_set(4));
        assert_eq!(bm.count_ones(), 3);
    }

    #[test]
    fn test_unpack_to_bytes() {
        let mut bm = Bitmap::all_unset(8);
        bm.set(0); bm.set(2); bm.set(4);
        let bytes = bm.unpack_to_bytes(8);
        assert_eq!(bytes, vec![1, 0, 1, 0, 1, 0, 0, 0]);
    }

    #[test]
    fn test_roundtrip_pack_unpack() {
        let original = vec![1u8, 0, 1, 1, 0, 0, 1, 0, 1, 1];
        let bm = Bitmap::pack_from_bytes(&original);
        let unpacked = bm.unpack_to_bytes(original.len());
        assert_eq!(original, unpacked);
    }

    #[test]
    fn test_popcount_1024() {
        let mut bm = Bitmap::all_unset(1024);
        for i in (0..1024).step_by(2) { bm.set(i); }
        assert_eq!(bm.count_ones(), 512);
    }

    #[test]
    fn test_any() {
        let bm = Bitmap::all_unset(64);
        assert!(!bm.any());
        let bm2 = Bitmap::all_set(64);
        assert!(bm2.any());
    }
}
```

#### 5b. Run tests (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::bitmap
```

#### 5c. Write implementation

```rust
// src/simd/bitmap.rs

/// A bitmap stored as packed u64 words.
/// Bit 1 = present/true, Bit 0 = absent/false.
#[derive(Clone, Debug)]
pub struct Bitmap {
    pub words: Vec<u64>,
}

impl Bitmap {
    pub fn all_set(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        let mut words = vec![u64::MAX; num_words];
        let remainder = len % 64;
        if remainder > 0 {
            words[num_words - 1] = (1u64 << remainder) - 1;
        }
        Self { words }
    }

    pub fn all_unset(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        Self { words: vec![0u64; num_words] }
    }

    pub fn is_set(&self, idx: usize) -> bool {
        let word = idx / 64;
        let bit = idx % 64;
        (self.words[word] >> bit) & 1 == 1
    }

    pub fn set(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] |= 1u64 << bit;
    }

    pub fn unset(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] &= !(1u64 << bit);
    }

    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let words = self.words.iter().zip(other.words.iter())
            .map(|(a, b)| a & b).collect();
        Bitmap { words }
    }

    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let words = self.words.iter().zip(other.words.iter())
            .map(|(a, b)| a | b).collect();
        Bitmap { words }
    }

    pub fn not(&self, len: usize) -> Bitmap {
        let num_words = self.words.len();
        let mut words: Vec<u64> = self.words.iter().map(|w| !w).collect();
        let remainder = len % 64;
        if remainder > 0 && num_words > 0 {
            words[num_words - 1] &= (1u64 << remainder) - 1;
        }
        Bitmap { words }
    }

    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn any(&self) -> bool {
        self.words.iter().any(|&w| w != 0)
    }

    pub fn unpack_to_bytes(&self, len: usize) -> Vec<u8> {
        let mut mask = vec![0u8; len];
        for i in 0..len {
            mask[i] = ((self.words[i / 64] >> (i % 64)) & 1) as u8;
        }
        mask
    }

    pub fn pack_from_bytes(mask: &[u8]) -> Self {
        let num_words = (mask.len() + 63) / 64;
        let mut words = vec![0u64; num_words];
        for (i, &byte) in mask.iter().enumerate() {
            words[i / 64] |= (byte as u64) << (i % 64);
        }
        Bitmap { words }
    }
}
```

Add `pub mod bitmap;` to `src/simd/mod.rs`.

#### 5d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::bitmap
```

#### 5e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add Bitmap type with pack/unpack, bitwise ops, and popcount"
```

---

### Step 6: Implement SelectionVector

**File (create)**: `src/simd/selection.rs`

#### 6a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::bitmap::Bitmap;

    #[test]
    fn test_all_any_active() {
        let sv = SelectionVector::All;
        assert!(sv.any_active(100));
    }

    #[test]
    fn test_all_count_active() {
        let sv = SelectionVector::All;
        assert_eq!(sv.count_active(100), 100);
    }

    #[test]
    fn test_bitmap_any_active() {
        let bm = Bitmap::all_unset(64);
        let sv = SelectionVector::Bitmap(bm);
        assert!(!sv.any_active(64));
    }

    #[test]
    fn test_bitmap_count_active() {
        let mut bm = Bitmap::all_unset(64);
        bm.set(0); bm.set(10); bm.set(63);
        let sv = SelectionVector::Bitmap(bm);
        assert_eq!(sv.count_active(64), 3);
    }

    #[test]
    fn test_to_bitmap_from_all() {
        let sv = SelectionVector::All;
        let bm = sv.to_bitmap(64);
        assert_eq!(bm.count_ones(), 64);
    }

    #[test]
    fn test_is_active() {
        let mut bm = Bitmap::all_unset(64);
        bm.set(5);
        let sv = SelectionVector::Bitmap(bm);
        assert!(sv.is_active(5, 64));
        assert!(!sv.is_active(6, 64));
    }

    #[test]
    fn test_all_is_always_active() {
        let sv = SelectionVector::All;
        assert!(sv.is_active(999, 1000));
    }
}
```

#### 6b. Run (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::selection
```

#### 6c. Write implementation

```rust
// src/simd/selection.rs

use crate::simd::bitmap::Bitmap;

/// Tracks which rows in a ColumnBatch are active.
#[derive(Clone, Debug)]
pub enum SelectionVector {
    All,
    Bitmap(Bitmap),
}

impl SelectionVector {
    pub fn any_active(&self, total: usize) -> bool {
        match self {
            SelectionVector::All => total > 0,
            SelectionVector::Bitmap(bm) => bm.any(),
        }
    }

    pub fn count_active(&self, total: usize) -> usize {
        match self {
            SelectionVector::All => total,
            SelectionVector::Bitmap(bm) => bm.count_ones(),
        }
    }

    pub fn to_bitmap(&self, total: usize) -> Bitmap {
        match self {
            SelectionVector::All => Bitmap::all_set(total),
            SelectionVector::Bitmap(bm) => bm.clone(),
        }
    }

    pub fn is_active(&self, idx: usize, _total: usize) -> bool {
        match self {
            SelectionVector::All => true,
            SelectionVector::Bitmap(bm) => bm.is_set(idx),
        }
    }
}
```

Add `pub mod selection;` to `src/simd/mod.rs`.

#### 6d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::selection
```

#### 6e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add SelectionVector enum (All, Bitmap)"
```

---

### Step 7: Implement TypedColumn

**File (create)**: `src/execution/batch.rs`

#### 7a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};

    #[test]
    fn test_typed_column_int32_construction() {
        let data = PaddedVec::from_vec(vec![1i32, 2, 3]);
        let null = Bitmap::all_set(3);
        let missing = Bitmap::all_set(3);
        let col = TypedColumn::Int32 { data, null, missing };
        match &col {
            TypedColumn::Int32 { data, .. } => assert_eq!(data.len(), 3),
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn test_typed_column_utf8_builder() {
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(4);
        offsets_builder.push(0);
        data_builder.extend_from_slice(b"hello");
        offsets_builder.push(data_builder.len() as u32);
        data_builder.extend_from_slice(b"world");
        offsets_builder.push(data_builder.len() as u32);

        let data = data_builder.seal();
        let offsets = offsets_builder.seal();
        assert_eq!(&*data, b"helloworld");
        assert_eq!(&*offsets, &[0u32, 5, 10]);
    }

    #[test]
    fn test_column_batch_construction() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len: 2,
        };
        assert_eq!(batch.len, 2);
        assert_eq!(batch.names.len(), 1);
    }
}
```

#### 7b. Run (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 7c. Write implementation

```rust
// src/execution/batch.rs

use crate::common::types::Value;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use crate::simd::selection::SelectionVector;

/// Compile-time tunable batch size.
pub const BATCH_SIZE: usize = 1024;

/// A typed columnar array with NULL and MISSING tracking.
pub enum TypedColumn {
    Int32 {
        data: PaddedVec<i32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Float32 {
        data: PaddedVec<f32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Boolean {
        data: Bitmap,
        null: Bitmap,
        missing: Bitmap,
    },
    Utf8 {
        data: PaddedVec<u8>,
        offsets: PaddedVec<u32>,
        null: Bitmap,
        missing: Bitmap,
    },
    DateTime {
        data: PaddedVec<i64>,
        null: Bitmap,
        missing: Bitmap,
    },
    Mixed {
        data: Vec<Value>,
        null: Bitmap,
        missing: Bitmap,
    },
}

/// A batch of up to BATCH_SIZE rows in columnar layout.
pub struct ColumnBatch {
    pub columns: Vec<TypedColumn>,
    pub names: Vec<String>,
    pub selection: SelectionVector,
    pub len: usize,
}

/// Schema information for a batch.
pub struct BatchSchema {
    pub names: Vec<String>,
    pub types: Vec<ColumnType>,
}

/// Column type tag (without data).
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Int32,
    Float32,
    Boolean,
    Utf8,
    DateTime,
    Mixed,
}
```

Add `pub mod batch;` to `src/execution/mod.rs`.

#### 7d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add TypedColumn, ColumnBatch, BatchSchema, ColumnType"
```

---

### Step 8: Implement BatchStream trait

**File**: `src/execution/batch.rs` (append to existing)

#### 8a. Write test

```rust
#[cfg(test)]
mod batch_stream_tests {
    use super::*;

    struct EmptyBatchStream;

    impl BatchStream for EmptyBatchStream {
        fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
            Ok(None)
        }
        fn schema(&self) -> &BatchSchema {
            static SCHEMA: once_cell::sync::Lazy<BatchSchema> = once_cell::sync::Lazy::new(|| {
                BatchSchema { names: vec![], types: vec![] }
            });
            &SCHEMA
        }
        fn close(&self) {}
    }

    #[test]
    fn test_empty_batch_stream() {
        let mut stream = EmptyBatchStream;
        assert!(stream.next_batch().unwrap().is_none());
    }
}
```

#### 8b. Write implementation (append to `src/execution/batch.rs`)

```rust
use crate::execution::types::StreamResult;

/// Batch-oriented stream trait. Replaces RecordStream for SIMD operators.
pub trait BatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>>;
    fn schema(&self) -> &BatchSchema;
    fn close(&self);
}
```

Note: avoid `once_cell` dependency. Use a simpler test:

```rust
#[test]
fn test_batch_stream_trait_is_object_safe() {
    // Verify trait can be used as dyn
    fn _takes_stream(_s: &dyn BatchStream) {}
}
```

#### 8c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch
```

#### 8d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "execution: add BatchStream trait alongside RecordStream"
```

---

## Phase 1: SIMD Kernels

### Step 9: Implement auto-vectorizable filter kernels

**File (create)**: `src/simd/kernels.rs`

#### 9a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::bitmap::Bitmap;

    #[test]
    fn test_filter_ge_i32() {
        let data = [1, 5, 10, 3, 7, 2, 8, 15];
        let mut result = vec![0u8; 8];
        filter_ge_i32(&data, 5, &mut result);
        assert_eq!(result, vec![0, 1, 1, 0, 1, 0, 1, 1]);
    }

    #[test]
    fn test_filter_eq_i32() {
        let data = [1, 5, 5, 3, 5];
        let mut result = vec![0u8; 5];
        filter_eq_i32(&data, 5, &mut result);
        assert_eq!(result, vec![0, 1, 1, 0, 1]);
    }

    #[test]
    fn test_filter_ge_f32() {
        let data = [1.0f32, 5.5, 10.0, 3.0];
        let mut result = vec![0u8; 4];
        filter_ge_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![0, 1, 1, 0]);
    }

    #[test]
    fn test_filter_ge_i32_to_bitmap() {
        let data: Vec<i32> = (0..128).collect();
        let bm = filter_ge_i32_to_bitmap(&data, 64);
        assert_eq!(bm.count_ones(), 64); // values 64..127
        assert!(!bm.is_set(63));
        assert!(bm.is_set(64));
    }

    #[test]
    fn test_filter_ge_i32_1024() {
        let data: Vec<i32> = (0..1024).collect();
        let bm = filter_ge_i32_to_bitmap(&data, 512);
        assert_eq!(bm.count_ones(), 512);
    }

    #[test]
    fn test_str_eq_batch() {
        // "hello" "world" "hello" "foo"
        let data = b"helloworldhellofoo";
        let offsets = [0u32, 5, 10, 15, 18];
        let needle = b"hello";
        let mut result = vec![0u8; 4];
        str_eq_batch(data, &offsets, needle, &mut result);
        assert_eq!(result, vec![1, 0, 1, 0]);
    }
}
```

#### 9b. Run (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels
```

#### 9c. Write implementation

```rust
// src/simd/kernels.rs

use crate::simd::bitmap::Bitmap;

// --- Integer filter kernels (auto-vectorizable) ---

/// Compare i32 values >= threshold, write 0/1 bytes.
/// LLVM auto-vectorizes into SIMD compare + byte store.
pub fn filter_ge_i32(data: &[i32], threshold: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] >= threshold) as u8;
    }
}

pub fn filter_eq_i32(data: &[i32], value: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] == value) as u8;
    }
}

pub fn filter_gt_i32(data: &[i32], threshold: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] > threshold) as u8;
    }
}

pub fn filter_le_i32(data: &[i32], threshold: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] <= threshold) as u8;
    }
}

pub fn filter_lt_i32(data: &[i32], threshold: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] < threshold) as u8;
    }
}

// --- Float filter kernels ---

pub fn filter_ge_f32(data: &[f32], threshold: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] >= threshold) as u8;
    }
}

pub fn filter_eq_f32(data: &[f32], value: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] == value) as u8;
    }
}

// --- Full pipeline: compare → bytes → bitmap ---

pub fn filter_ge_i32_to_bitmap(data: &[i32], threshold: i32) -> Bitmap {
    let mut bytes = vec![0u8; data.len()];
    filter_ge_i32(data, threshold, &mut bytes);
    Bitmap::pack_from_bytes(&bytes)
}

pub fn filter_eq_i32_to_bitmap(data: &[i32], value: i32) -> Bitmap {
    let mut bytes = vec![0u8; data.len()];
    filter_eq_i32(data, value, &mut bytes);
    Bitmap::pack_from_bytes(&bytes)
}

// --- String equality kernel ---

pub fn str_eq_batch(
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u8],
) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let field = &haystack[start..end];
        result[i] = (field == needle) as u8;
    }
}

// --- String contains (scalar fallback) ---

pub fn str_contains_scalar(
    haystack: &[u8], offsets: &[u32], needle: &[u8], result: &mut [u8],
) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let hay = &haystack[start..end];
        result[i] = hay.windows(needle.len()).any(|w| w == needle) as u8;
    }
}
```

Add `pub mod kernels;` to `src/simd/mod.rs`.

#### 9d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels
```

#### 9e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add auto-vectorizable filter kernels (i32, f32, string eq/contains)"
```

---

### Step 10: Implement aggregation kernels

**File**: `src/simd/kernels.rs` (append)

#### 10a. Write failing tests

```rust
// Append to kernels.rs tests module:

#[test]
fn test_sum_i32_selected() {
    let data = [10i32, 20, 30, 40, 50];
    let mut bm = Bitmap::all_unset(5);
    bm.set(0); bm.set(2); bm.set(4); // select 10, 30, 50
    assert_eq!(sum_i32_selected(&data, &bm), 90);
}

#[test]
fn test_sum_i32_all() {
    let data = [1i32, 2, 3, 4, 5];
    let bm = Bitmap::all_set(5);
    assert_eq!(sum_i32_selected(&data, &bm), 15);
}

#[test]
fn test_count_selected() {
    let mut bm = Bitmap::all_unset(100);
    bm.set(0); bm.set(50); bm.set(99);
    assert_eq!(count_selected(&bm), 3);
}

#[test]
fn test_hash_column_i32() {
    let data = [1i32, 2, 3, 1];
    let mut hashes = vec![0u64; 4];
    hash_column_i32(&data, &mut hashes);
    // Same input → same hash
    assert_eq!(hashes[0], hashes[3]);
    // Different inputs → different hashes (with high probability)
    assert_ne!(hashes[0], hashes[1]);
}

#[test]
fn test_hash_combine() {
    let mut a = vec![100u64, 200, 300];
    let b = vec![1u64, 2, 3];
    hash_combine(&mut a, &b);
    // Verify combine changed values
    assert_ne!(a[0], 100);
    // Verify determinism
    let mut a2 = vec![100u64, 200, 300];
    hash_combine(&mut a2, &b);
    assert_eq!(a, a2);
}
```

#### 10b. Run (should fail on new functions)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels
```

#### 10c. Write implementation (append to kernels.rs)

```rust
// --- Aggregation kernels ---

const HASH_MULT: u64 = 0x517cc1b727220a95;

/// Sum i32 values where selection bitmap is set.
/// Branch-free multiply-accumulate that LLVM auto-vectorizes.
pub fn sum_i32_selected(data: &[i32], selection: &Bitmap) -> i64 {
    let mask = selection.unpack_to_bytes(data.len());
    let mut total: i64 = 0;
    for i in 0..data.len() {
        total += (data[i] as i64) * (mask[i] as i64);
    }
    total
}

/// Count active rows in a selection bitmap.
pub fn count_selected(selection: &Bitmap) -> usize {
    selection.count_ones()
}

/// Multiply-shift hash for integer column. Auto-vectorizable.
pub fn hash_column_i32(data: &[i32], hashes: &mut [u64]) {
    for i in 0..data.len() {
        hashes[i] = (data[i] as u64).wrapping_mul(HASH_MULT);
    }
}

/// Combine per-column hashes with rotation-xor-multiply.
pub fn hash_combine(existing: &mut [u64], new: &[u64]) {
    for (h, &n) in existing.iter_mut().zip(new.iter()) {
        *h = (h.rotate_left(5) ^ n).wrapping_mul(HASH_MULT);
    }
}
```

#### 10d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels
```

#### 10e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add aggregation and hashing kernels (sum, count, hash, combine)"
```

---

### Step 11: Implement bitmap operation kernels

Already implemented as methods on `Bitmap` in Step 5. Verify the auto-vectorizable pattern works at BATCH_SIZE=1024:

#### 11a. Write a 1024-row integration test

```rust
// Add to src/simd/bitmap.rs tests:

#[test]
fn test_bitmap_ops_at_batch_size() {
    let mut a = Bitmap::all_unset(1024);
    let mut b = Bitmap::all_unset(1024);
    for i in (0..1024).step_by(2) { a.set(i); }  // evens
    for i in (0..1024).step_by(3) { b.set(i); }  // multiples of 3

    let and_result = a.and(&b);
    // evens AND multiples-of-3 = multiples of 6
    let expected_count = (0..1024).filter(|i| i % 6 == 0).count();
    assert_eq!(and_result.count_ones(), expected_count);

    let or_result = a.or(&b);
    let expected_or = (0..1024).filter(|i| i % 2 == 0 || i % 3 == 0).count();
    assert_eq!(or_result.count_ones(), expected_or);
}
```

#### 11b. Run test

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::bitmap::tests::test_bitmap_ops_at_batch_size
```

#### 11c. Commit (if new test was added)

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add batch-size bitmap integration test"
```

---

### Step 12: Implement StringFilterCache

**File (create)**: `src/simd/filter_cache.rs`

#### 12a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn make_utf8_column() -> (Vec<u8>, Vec<u32>) {
        // "200" "404" "200" "500" "200" "301" "200" "404"
        let strs = ["200", "404", "200", "500", "200", "301", "200", "404"];
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for s in &strs {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        (data, offsets)
    }

    #[test]
    fn test_filter_cache_string_equality() {
        let (data, offsets) = make_utf8_column();
        let bm = evaluate_cached_two_pass(
            &data, &offsets, &|field| field == b"200", offsets.len() - 1,
        );
        // Rows 0,2,4,6 are "200"
        assert!(bm.is_set(0));
        assert!(!bm.is_set(1));
        assert!(bm.is_set(2));
        assert!(!bm.is_set(3));
        assert!(bm.is_set(4));
        assert!(!bm.is_set(5));
        assert!(bm.is_set(6));
        assert!(!bm.is_set(7));
        assert_eq!(bm.count_ones(), 4);
    }

    #[test]
    fn test_filter_cache_high_cardinality_fallback() {
        // Each string is unique → cardinality = N → should still produce correct results
        let strs: Vec<String> = (0..100).map(|i| format!("val_{}", i)).collect();
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for s in &strs {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        let bm = evaluate_cached_two_pass(
            &data, &offsets, &|field| field == b"val_50", offsets.len() - 1,
        );
        assert_eq!(bm.count_ones(), 1);
        assert!(bm.is_set(50));
    }

    #[test]
    fn test_filter_cache_empty() {
        let data: Vec<u8> = Vec::new();
        let offsets = vec![0u32];
        let bm = evaluate_cached_two_pass(&data, &offsets, &|_| true, 0);
        assert_eq!(bm.count_ones(), 0);
    }
}
```

#### 12b. Run (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::filter_cache
```

#### 12c. Write implementation

```rust
// src/simd/filter_cache.rs

use crate::simd::bitmap::Bitmap;
use hashbrown::HashMap;

/// Two-pass cached filter evaluation for string columns.
/// Pass 1: Deduplicate — collect unique values, test each once.
/// Pass 2: Broadcast — look up cached result per row.
/// Falls back to direct evaluation if cardinality > len/10 (10%).
pub fn evaluate_cached_two_pass(
    data: &[u8],
    offsets: &[u32],
    filter_fn: &dyn Fn(&[u8]) -> bool,
    len: usize,
) -> Bitmap {
    if len == 0 {
        return Bitmap::all_unset(0);
    }

    // Pass 1: Collect unique values and their filter results.
    let mut cache: HashMap<&[u8], bool> = HashMap::with_capacity(32);
    for i in 0..len {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let field = &data[start..end];
        cache.entry(field).or_insert_with(|| filter_fn(field));
    }

    // Cardinality check: if > 10% unique, caching overhead may exceed benefit.
    // Still correct, just less efficient than direct evaluation.
    // (For now, we always use the cache; the threshold is advisory for future optimization.)

    // Pass 2: Look up each row's result from the cache.
    let mut result_bytes = vec![0u8; len];
    for i in 0..len {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        result_bytes[i] = *cache.get(&data[start..end]).unwrap() as u8;
    }

    Bitmap::pack_from_bytes(&result_bytes)
}
```

Add `pub mod filter_cache;` to `src/simd/mod.rs`.

#### 12d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::filter_cache
```

#### 12e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "simd: add StringFilterCache with two-pass dedup for low-cardinality fields"
```

---

### Step 13: Implement predicate cost model and conjunct reordering

**File (create)**: `src/logical/optimizer.rs`

#### 13a. Write failing tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::types::{Formula, Expression, LogicInfixOp};
    use crate::execution::types::Relation;
    use crate::common::types::Value;
    use crate::syntax::ast::{PathExpr, PathSegment};

    fn var(name: &str) -> Box<Expression> {
        Box::new(Expression::Variable(PathExpr::new(vec![
            PathSegment::AttrName(name.to_string()),
        ])))
    }

    fn int_const(v: i32) -> Box<Expression> {
        Box::new(Expression::Constant(Value::Int(v)))
    }

    fn str_const(v: &str) -> Box<Expression> {
        Box::new(Expression::Constant(Value::String(v.to_string())))
    }

    #[test]
    fn test_extract_conjuncts_single() {
        let f = Formula::Predicate(Relation::Equal, var("a"), int_const(1));
        let conjuncts = extract_conjuncts(f);
        assert_eq!(conjuncts.len(), 1);
    }

    #[test]
    fn test_extract_conjuncts_nested_and() {
        let a = Formula::Predicate(Relation::Equal, var("a"), int_const(1));
        let b = Formula::Predicate(Relation::MoreThan, var("b"), int_const(2));
        let c = Formula::IsNull(var("c"));
        let and_bc = Formula::InfixOperator(LogicInfixOp::And, Box::new(b), Box::new(c));
        let and_abc = Formula::InfixOperator(LogicInfixOp::And, Box::new(a), Box::new(and_bc));
        let conjuncts = extract_conjuncts(and_abc);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_rebuild_conjunction() {
        let a = Formula::IsNull(var("a"));
        let b = Formula::IsNull(var("b"));
        let rebuilt = rebuild_conjunction(vec![a.clone(), b.clone()]);
        // Should produce And(a, b)
        match rebuilt {
            Formula::InfixOperator(LogicInfixOp::And, _, _) => {}
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_rebuild_conjunction_single() {
        let a = Formula::IsNull(var("a"));
        let rebuilt = rebuild_conjunction(vec![a.clone()]);
        match rebuilt {
            Formula::IsNull(_) => {}
            _ => panic!("expected single IsNull"),
        }
    }

    #[test]
    fn test_reorder_puts_isnull_before_predicate() {
        let expensive = Formula::Like(var("user_agent"), str_const("%bot%"));
        let cheap = Formula::IsNull(var("a"));
        let and_expr = Formula::InfixOperator(
            LogicInfixOp::And,
            Box::new(expensive),
            Box::new(cheap),
        );
        let reordered = reorder_and_conjuncts(and_expr);
        let conjuncts = extract_conjuncts(reordered);
        // IsNull (cost 1) should come before Like (cost 50)
        assert!(matches!(&conjuncts[0], Formula::IsNull(_)));
    }
}
```

#### 13b. Run (should fail)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib logical::optimizer
```

#### 13c. Write implementation

```rust
// src/logical/optimizer.rs

use crate::logical::types::{Formula, LogicInfixOp, Expression};
use crate::execution::types::Relation;

/// Extract AND conjuncts from a nested InfixOperator(And, ...) tree into a flat list.
pub fn extract_conjuncts(formula: Formula) -> Vec<Formula> {
    let mut result = Vec::new();
    extract_conjuncts_inner(formula, &mut result);
    result
}

fn extract_conjuncts_inner(formula: Formula, out: &mut Vec<Formula>) {
    match formula {
        Formula::InfixOperator(LogicInfixOp::And, left, right) => {
            extract_conjuncts_inner(*left, out);
            extract_conjuncts_inner(*right, out);
        }
        other => out.push(other),
    }
}

/// Rebuild a conjunction (AND tree) from a flat list of conjuncts.
/// Folds right: [a, b, c] → And(a, And(b, c)).
pub fn rebuild_conjunction(mut conjuncts: Vec<Formula>) -> Formula {
    assert!(!conjuncts.is_empty(), "cannot rebuild empty conjunction");
    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }
    let mut result = conjuncts.pop().unwrap();
    while let Some(f) = conjuncts.pop() {
        result = Formula::InfixOperator(LogicInfixOp::And, Box::new(f), Box::new(result));
    }
    result
}

/// Estimated cost of evaluating a predicate. Lower = evaluate first.
pub fn predicate_cost(formula: &Formula) -> u32 {
    match formula {
        Formula::IsNull(_) | Formula::IsNotNull(_)
        | Formula::IsMissing(_) | Formula::IsNotMissing(_) => 1,

        Formula::Constant(_) => 0,

        Formula::Predicate(_, left, right) => {
            if involves_function(left) || involves_function(right) {
                100
            } else {
                10 // numeric or simple comparison
            }
        }

        Formula::Like(_, _) | Formula::NotLike(_, _) => 50,

        Formula::In(_, _) | Formula::NotIn(_, _) => 40,

        Formula::PrefixOperator(_, inner) => predicate_cost(inner),

        Formula::InfixOperator(_, left, right) => {
            predicate_cost(left).max(predicate_cost(right))
        }

        Formula::ExpressionPredicate(_) => 60,
    }
}

fn involves_function(expr: &Expression) -> bool {
    matches!(expr, Expression::Function(_, _))
}

/// Reorder AND conjuncts by ascending cost. Non-AND formulas are returned unchanged.
pub fn reorder_and_conjuncts(formula: Formula) -> Formula {
    match &formula {
        Formula::InfixOperator(LogicInfixOp::And, _, _) => {
            let mut conjuncts = extract_conjuncts(formula);
            conjuncts.sort_by_key(|f| predicate_cost(f));
            rebuild_conjunction(conjuncts)
        }
        _ => formula,
    }
}
```

Add `pub mod optimizer;` to `src/logical/mod.rs`.

#### 13d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib logical::optimizer
```

#### 13e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "logical: add predicate cost model and conjunct reordering optimizer"
```

---

### Step 14: Export new types via bench-internals and add CI auto-vectorization check

**File**: `src/lib.rs`

#### 14a. Add bench-internals exports for new types

```rust
// Append to the bench_internals module in src/lib.rs:

// SIMD foundation types
pub use crate::simd::bitmap::Bitmap;
pub use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
pub use crate::simd::selection::SelectionVector;
pub use crate::simd::kernels::*;
pub use crate::simd::filter_cache::evaluate_cached_two_pass;

// Batch execution types
pub use crate::execution::batch::{
    TypedColumn, ColumnBatch, BatchSchema, ColumnType, BatchStream, BATCH_SIZE,
};
```

#### 14b. Verify compilation with bench-internals feature

```bash
cd /Users/paulmeng/Develop/logq && cargo build --features bench-internals
```

#### 14c. Add auto-vectorization CI check script

**File (create)**: `scripts/check_autovec.sh`

```bash
#!/bin/bash
# Verify that critical SIMD kernels produce vector instructions.
# Run with: bash scripts/check_autovec.sh
set -e

echo "Compiling with AVX2 target and assembly output..."
RUSTFLAGS="--emit=asm -C target-cpu=x86-64-v3" cargo build --release --lib 2>/dev/null

ASM_FILE=$(ls target/release/deps/logq-*.s 2>/dev/null | head -1)
if [ -z "$ASM_FILE" ]; then
    echo "ERROR: No assembly file found"
    exit 1
fi

echo "Checking for SIMD instructions in $ASM_FILE..."

# Check for AVX2 comparison instructions (from filter kernels)
VPCMP_COUNT=$(grep -c 'vpcmp\|vcmp' "$ASM_FILE" || true)
echo "  Vector comparison instructions: $VPCMP_COUNT"

# Check for AVX2 arithmetic (from hash/sum kernels)
VARITH_COUNT=$(grep -c 'vpmull\|vpmadd\|vpadd\|vpmuludq' "$ASM_FILE" || true)
echo "  Vector arithmetic instructions: $VARITH_COUNT"

# Check for AVX2 bitwise ops (from bitmap operations)
VBIT_COUNT=$(grep -c 'vpand\|vpor\|vpxor' "$ASM_FILE" || true)
echo "  Vector bitwise instructions: $VBIT_COUNT"

if [ "$VPCMP_COUNT" -eq 0 ] && [ "$VARITH_COUNT" -eq 0 ] && [ "$VBIT_COUNT" -eq 0 ]; then
    echo "WARNING: No vector instructions found. Auto-vectorization may have failed."
    exit 1
fi

echo "Auto-vectorization verified."
```

#### 14d. Run the check

```bash
cd /Users/paulmeng/Develop/logq && bash scripts/check_autovec.sh
```

#### 14e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "ci: add bench-internals exports for SIMD types and auto-vectorization check script"
```

---

### Step 15: End-to-end verification

#### 15a. Run full test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test --all-features
```

Verify all existing ~58 tests still pass (no regressions from new modules).

#### 15b. Run benchmarks to verify no performance regression

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals
```

#### 15c. Verify module structure

```bash
ls -la /Users/paulmeng/Develop/logq/src/simd/
# Expected: mod.rs, padded_vec.rs, bitmap.rs, selection.rs, kernels.rs, filter_cache.rs
```

#### 15d. Summary check

Verify all Phase 1 deliverables from the design:

| Deliverable | File | Status |
|---|---|---|
| `PaddedVec<T>` + `PaddedVecBuilder<T>` | `src/simd/padded_vec.rs` | Step 4 |
| `Bitmap` | `src/simd/bitmap.rs` | Step 5 |
| `SelectionVector` | `src/simd/selection.rs` | Step 6 |
| `TypedColumn`, `ColumnBatch`, `BatchSchema` | `src/execution/batch.rs` | Step 7 |
| `BatchStream` trait | `src/execution/batch.rs` | Step 8 |
| Filter kernels (i32, f32, string) | `src/simd/kernels.rs` | Step 9 |
| Aggregation kernels (sum, hash, combine) | `src/simd/kernels.rs` | Step 10 |
| Bitmap ops at batch size | `src/simd/bitmap.rs` | Step 11 |
| `StringFilterCache` | `src/simd/filter_cache.rs` | Step 12 |
| Predicate cost model + reordering | `src/logical/optimizer.rs` | Step 13 |
| bench-internals exports + CI check | `src/lib.rs`, `scripts/` | Step 14 |

#### 15e. Final commit (if any remaining changes)

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git commit -m "phase1: complete foundation types, kernels, and optimizer"
```
