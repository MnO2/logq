# Plan: SIMD-Accelerated Physical Plan for logq (v2)

**Goal**: Introduce a hybrid columnar batch execution model to logq's physical plan, enabling SIMD acceleration across data source parsing, filtering, projection, and aggregation while maintaining all existing functionality.
**Architecture**: Hybrid columnar batches inside existing Volcano-style operator tree. Auto-vectorization-first SIMD strategy with targeted intrinsics for string search only. Two-pass filter kernels (byte-per-row comparison + bitmap packing). `hashbrown` for hash tables. `ahash` for string hashing, multiply-shift for integer hashing.
**Tech Stack**: Rust (stable), `ahash` crate, `simd-json` crate, `hashbrown` (existing dep), `criterion` (existing dev dep)
**Design Document**: `docs/plans/2026-04-05-simd-physical-plan-design-final.md`

---

## Changes from previous version

| Issue | Category | Summary | Resolution |
|-------|----------|---------|------------|
| **C1** | Critical | All `git commit` commands should be `sl commit` (project uses Sapling) | Changed every `git commit` to `sl commit` throughout the plan |
| **C2** | Critical | `simd-json` version outdated at "0.14" | Updated to `simd-json = "0.17"` |
| **C3** | Critical | Module wiring (Steps 6/10) happened after tests that depend on the modules being wired — tests cannot compile | Restructured so that creating a new file always includes wiring it into the module tree in the same step. Step 2 now adds `pub mod batch;` to `src/execution/mod.rs` alongside creating `src/execution/batch.rs`. Steps 7/9 are preceded by new Step 7 that creates `src/simd/mod.rs` and wires it into `src/lib.rs` before any simd code is written. |
| **C4** | Critical | `SelectionVector::Indices` test used `vec![0, 5, 10]` (defaults to i32) but type is `Vec<u32>` | Fixed to `vec![0u32, 5, 10]` |
| **C6** | Critical | `ColumnBuilder` type mismatch silently pushed to `mixed_data` creating vector length mismatches and index-out-of-bounds panics | On type mismatch, push a default value to the typed data vector and mark the row as NULL instead of pushing to `mixed_data` |
| **C7** | Critical | `ColumnBuilder::push` Utf8 control flow used fragile early returns and had dead code paths | Rewrote to use a clear two-phase approach: first determine null/missing status, then push data. No early returns, no dead code. |
| **C9** | Critical | Design Phase 1 includes JSONL `simd-json` migration but plan had no task for it | Added Step 14: JSONL simd-json migration task |
| **C10** | Critical | Redundant `if self.col_type == ColumnType::Utf8` check after match arm already handled Utf8 | Removed the redundant check as part of the C7 rewrite |
| **S3** | Suggestion | `Bitmap::not()` inverts padding bits, causing `count_ones()` to return wrong counts | Added `len` field to `Bitmap`, mask last word in `not()` and `count_ones()` |
| **S4** | Suggestion | `BatchToRowAdapter` test only uses `SelectionVector::All`, doesn't verify filtering | Added `test_batch_to_row_adapter_with_selection` test using `SelectionVector::Bitmap` |
| **S8** | Suggestion | Missing `hash_column_utf8` implementation alongside other hash kernels | Added `hash_column_utf8` using AHash in Step 9 (hash kernels) |

---

## Step 1: Add `ahash` and `simd-json` dependencies to Cargo.toml

**File**: `Cargo.toml`

### 1a. Write failing test

No test needed — this is a dependency addition.

### 1b. Implementation

Add to `[dependencies]`:
```toml
ahash = "0.8"
simd-json = "0.17"
```

### 1c. Verify

```bash
cd /Users/paulmeng/Develop/logq && cargo check
```

### 1d. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add ahash and simd-json dependencies for SIMD physical plan"
```

---

## Step 2: Create `batch` module and implement `Bitmap` struct

**Files**: `src/execution/batch.rs` (new file), `src/execution/mod.rs`

### 2a. Wire module into crate

Add `pub mod batch;` to `src/execution/mod.rs` so that the new file is part of the module tree and tests can compile.

**`src/execution/mod.rs`** becomes:
```rust
pub mod batch;
pub mod datasource;
pub mod stream;
pub mod types;
```

### 2b. Write failing test

Create `src/execution/batch.rs` with tests:

```rust
// src/execution/batch.rs

/// Compile-time tunable batch size. 1024 is optimal for narrow schemas
/// (4KB per i32 column fits L1). Reduce for wide schemas (25+ columns).
pub const BATCH_SIZE: usize = 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_all_set() {
        let bm = Bitmap::all_set(128);
        assert_eq!(bm.len, 128);
        assert!(bm.is_set(0));
        assert!(bm.is_set(127));
        assert_eq!(bm.count_ones(), 128);
    }

    #[test]
    fn test_bitmap_all_unset() {
        let bm = Bitmap::all_unset(128);
        assert_eq!(bm.count_ones(), 0);
        assert!(!bm.is_set(0));
    }

    #[test]
    fn test_bitmap_set_unset() {
        let mut bm = Bitmap::all_unset(64);
        bm.set(5);
        bm.set(63);
        assert!(bm.is_set(5));
        assert!(bm.is_set(63));
        assert!(!bm.is_set(0));
        bm.unset(5);
        assert!(!bm.is_set(5));
    }

    #[test]
    fn test_bitmap_and_or_not() {
        let mut a = Bitmap::all_unset(64);
        a.set(0); a.set(1); a.set(2);
        let mut b = Bitmap::all_unset(64);
        b.set(1); b.set(2); b.set(3);
        let and = a.and(&b);
        assert!(and.is_set(1));
        assert!(and.is_set(2));
        assert!(!and.is_set(0));
        assert!(!and.is_set(3));
        let or = a.or(&b);
        assert_eq!(or.count_ones(), 4);
        let not_a = a.not();
        assert!(!not_a.is_set(0));
        assert!(not_a.is_set(3));
    }

    #[test]
    fn test_bitmap_not_masks_high_bits() {
        // 65-element bitmap: 2 words, second word has only 1 valid bit
        let bm = Bitmap::all_unset(65);
        let not_bm = bm.not();
        // Should be 65 (not 128) because high bits in last word are masked
        assert_eq!(not_bm.count_ones(), 65);
    }

    #[test]
    fn test_bitmap_count_ones_ignores_padding() {
        // 65-element bitmap with all bits set
        let bm = Bitmap::all_set(65);
        assert_eq!(bm.count_ones(), 65);
    }

    #[test]
    fn test_bitmap_pack_unpack_roundtrip() {
        let mut bm = Bitmap::all_unset(1024);
        for i in (0..1024).step_by(3) { bm.set(i); }
        let bytes = bm.unpack_to_bytes(1024);
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_eq!(bytes[3], 1);
        let repacked = Bitmap::pack_from_bytes(&bytes);
        for i in 0..1024 {
            assert_eq!(bm.is_set(i), repacked.is_set(i), "mismatch at {}", i);
        }
    }

    #[test]
    fn test_bitmap_any() {
        let bm = Bitmap::all_unset(64);
        assert!(!bm.any());
        let mut bm2 = Bitmap::all_unset(128);
        bm2.set(127);
        assert!(bm2.any());
    }
}
```

### 2c. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 2d. Write implementation

Add to `src/execution/batch.rs` (above the tests module):

```rust
/// A bitmap stored as packed u64 words.
/// For BATCH_SIZE=1024, this is 16 words (128 bytes).
/// Bit 1 = present/true, Bit 0 = absent/false.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bitmap {
    pub(crate) words: Vec<u64>,
    pub(crate) len: usize,
}

impl Bitmap {
    pub fn all_set(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        let mut words = vec![u64::MAX; num_words];
        let remainder = len % 64;
        if remainder > 0 {
            words[num_words - 1] = (1u64 << remainder) - 1;
        }
        Bitmap { words, len }
    }

    pub fn all_unset(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        Bitmap { words: vec![0u64; num_words], len }
    }

    #[inline]
    pub fn is_set(&self, idx: usize) -> bool {
        let word = idx / 64;
        let bit = idx % 64;
        (self.words[word] >> bit) & 1 == 1
    }

    #[inline]
    pub fn set(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] |= 1u64 << bit;
    }

    #[inline]
    pub fn unset(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] &= !(1u64 << bit);
    }

    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let len = self.len.min(other.len);
        let words = self.words.iter().zip(other.words.iter())
            .map(|(&a, &b)| a & b).collect();
        Bitmap { words, len }
    }

    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let len = self.len.max(other.len);
        let words = self.words.iter().zip(other.words.iter())
            .map(|(&a, &b)| a | b).collect();
        Bitmap { words, len }
    }

    pub fn not(&self) -> Bitmap {
        let mut words: Vec<u64> = self.words.iter().map(|&w| !w).collect();
        // Mask out padding bits in the last word
        let remainder = self.len % 64;
        if remainder > 0 && !words.is_empty() {
            let last = words.len() - 1;
            words[last] &= (1u64 << remainder) - 1;
        }
        Bitmap { words, len: self.len }
    }

    pub fn count_ones(&self) -> usize {
        if self.words.is_empty() {
            return 0;
        }
        // Count all words except the last one directly
        let full_words = if self.len % 64 == 0 { self.words.len() } else { self.words.len() - 1 };
        let mut count: usize = self.words[..full_words].iter()
            .map(|w| w.count_ones() as usize).sum();
        // For the last partial word, mask out padding bits before counting
        if self.len % 64 != 0 {
            let remainder = self.len % 64;
            let last_word = *self.words.last().unwrap();
            let masked = last_word & ((1u64 << remainder) - 1);
            count += masked.count_ones() as usize;
        }
        count
    }

    pub fn any(&self) -> bool {
        self.words.iter().any(|&w| w != 0)
    }

    pub fn unpack_to_bytes(&self, len: usize) -> Vec<u8> {
        let mut mask = vec![0u8; len];
        for i in 0..len {
            let word = i / 64;
            let bit = i % 64;
            mask[i] = ((self.words[word] >> bit) & 1) as u8;
        }
        mask
    }

    pub fn pack_from_bytes(mask: &[u8]) -> Self {
        let len = mask.len();
        let num_words = (len + 63) / 64;
        let mut words = vec![0u64; num_words];
        for (i, &byte) in mask.iter().enumerate() {
            let word = i / 64;
            let bit = i % 64;
            words[word] |= (byte as u64) << bit;
        }
        Bitmap { words, len }
    }
}
```

### 2e. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 2f. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add Bitmap struct with bitwise ops, length tracking, and pack/unpack"
```

---

## Step 3: Implement `TypedColumn` enum

**File**: `src/execution/batch.rs`

### 3a. Write failing test

```rust
#[test]
fn test_typed_column_int32_creation() {
    let col = TypedColumn::Int32 {
        data: vec![1, 2, 3, 4],
        null: Bitmap::all_set(4),
        missing: Bitmap::all_set(4),
    };
    match &col {
        TypedColumn::Int32 { data, null, missing } => {
            assert_eq!(data, &[1, 2, 3, 4]);
            assert_eq!(null.count_ones(), 4);
            assert_eq!(missing.count_ones(), 4);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn test_typed_column_utf8_creation() {
    // Two strings: "hello" and "world"
    let col = TypedColumn::Utf8 {
        data: b"helloworld".to_vec(),
        offsets: vec![0, 5, 10],
        null: Bitmap::all_set(2),
        missing: Bitmap::all_set(2),
    };
    match &col {
        TypedColumn::Utf8 { data, offsets, .. } => {
            let s0 = &data[offsets[0] as usize..offsets[1] as usize];
            let s1 = &data[offsets[1] as usize..offsets[2] as usize];
            assert_eq!(s0, b"hello");
            assert_eq!(s1, b"world");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn test_typed_column_null_missing_semantics() {
    // Row 0: present (missing=1, null=1)
    // Row 1: NULL (missing=1, null=0)
    // Row 2: MISSING (missing=0, null=don't care)
    let mut null = Bitmap::all_set(3);
    null.unset(1); // row 1 is NULL
    let mut missing = Bitmap::all_set(3);
    missing.unset(2); // row 2 is MISSING

    let col = TypedColumn::Int32 {
        data: vec![42, 0, 0],
        null,
        missing,
    };
    match &col {
        TypedColumn::Int32 { null, missing, .. } => {
            // Row 0: present
            assert!(missing.is_set(0) && null.is_set(0));
            // Row 1: NULL
            assert!(missing.is_set(1) && !null.is_set(1));
            // Row 2: MISSING
            assert!(!missing.is_set(2));
        }
        _ => panic!(),
    }
}
```

### 3b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 3c. Write implementation

Add to `src/execution/batch.rs`:

```rust
use crate::common::types::Value;

/// A typed columnar array with separate NULL and MISSING tracking.
#[derive(Clone, Debug)]
pub enum TypedColumn {
    Int32 {
        data: Vec<i32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Float32 {
        data: Vec<f32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Boolean {
        data: Bitmap,
        null: Bitmap,
        missing: Bitmap,
    },
    Utf8 {
        data: Vec<u8>,
        offsets: Vec<u32>,
        null: Bitmap,
        missing: Bitmap,
    },
    DateTime {
        data: Vec<i64>,
        null: Bitmap,
        missing: Bitmap,
    },
    Mixed {
        data: Vec<Value>,
        null: Bitmap,
        missing: Bitmap,
    },
}
```

### 3d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 3e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add TypedColumn enum with NULL/MISSING bitmaps"
```

---

## Step 4: Implement `SelectionVector` and `ColumnBatch`

**File**: `src/execution/batch.rs`

### 4a. Write failing test

```rust
#[test]
fn test_selection_vector_all() {
    let sv = SelectionVector::All;
    assert!(sv.any_active());
    assert_eq!(sv.count_active(1024), 1024);
}

#[test]
fn test_selection_vector_bitmap() {
    let mut bm = Bitmap::all_unset(64);
    bm.set(0); bm.set(10); bm.set(63);
    let sv = SelectionVector::Bitmap(bm);
    assert!(sv.any_active());
    assert_eq!(sv.count_active(64), 3);
}

#[test]
fn test_selection_vector_indices() {
    let sv = SelectionVector::Indices(vec![0u32, 5, 10]);
    assert!(sv.any_active());
    assert_eq!(sv.count_active(1024), 3);
}

#[test]
fn test_selection_vector_empty() {
    let sv = SelectionVector::Bitmap(Bitmap::all_unset(64));
    assert!(!sv.any_active());
    assert_eq!(sv.count_active(64), 0);
}

#[test]
fn test_column_batch_creation() {
    let col = TypedColumn::Int32 {
        data: vec![1, 2, 3],
        null: Bitmap::all_set(3),
        missing: Bitmap::all_set(3),
    };
    let batch = ColumnBatch {
        columns: vec![col],
        names: vec!["x".to_string()],
        selection: SelectionVector::All,
        len: 3,
    };
    assert_eq!(batch.len, 3);
    assert_eq!(batch.names[0], "x");
}
```

### 4b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 4c. Write implementation

Add to `src/execution/batch.rs`:

```rust
/// Tracks active rows through the pipeline.
#[derive(Clone, Debug)]
pub enum SelectionVector {
    All,
    Bitmap(Bitmap),
    Indices(Vec<u32>),
}

impl SelectionVector {
    pub fn any_active(&self) -> bool {
        match self {
            SelectionVector::All => true,
            SelectionVector::Bitmap(bm) => bm.any(),
            SelectionVector::Indices(indices) => !indices.is_empty(),
        }
    }

    pub fn count_active(&self, total: usize) -> usize {
        match self {
            SelectionVector::All => total,
            SelectionVector::Bitmap(bm) => bm.count_ones(),
            SelectionVector::Indices(indices) => indices.len(),
        }
    }

    pub fn to_bitmap(&self, total: usize) -> Bitmap {
        match self {
            SelectionVector::All => Bitmap::all_set(total),
            SelectionVector::Bitmap(bm) => bm.clone(),
            SelectionVector::Indices(indices) => {
                let mut bm = Bitmap::all_unset(total);
                for &idx in indices {
                    bm.set(idx as usize);
                }
                bm
            }
        }
    }
}

/// A batch of rows in columnar layout.
pub struct ColumnBatch {
    pub columns: Vec<TypedColumn>,
    pub names: Vec<String>,
    pub selection: SelectionVector,
    pub len: usize,
}
```

### 4d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 4e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add SelectionVector and ColumnBatch types"
```

---

## Step 5: Implement `BatchStream` trait and `BatchSchema`

**File**: `src/execution/batch.rs`

### 5a. Write failing test

```rust
#[test]
fn test_batch_schema() {
    let schema = BatchSchema {
        names: vec!["a".to_string(), "b".to_string()],
        types: vec![ColumnType::Int32, ColumnType::Utf8],
    };
    assert_eq!(schema.names.len(), 2);
    assert_eq!(schema.types[0], ColumnType::Int32);
}
```

### 5b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 5c. Write implementation

Add to `src/execution/batch.rs`:

```rust
use super::types::StreamResult;

/// Column type tag for schema description.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Int32,
    Float32,
    Boolean,
    Utf8,
    DateTime,
    Mixed,
}

/// Schema of a ColumnBatch — column names and types.
#[derive(Clone, Debug)]
pub struct BatchSchema {
    pub names: Vec<String>,
    pub types: Vec<ColumnType>,
}

/// Batch-oriented execution trait (replaces RecordStream for converted operators).
pub trait BatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>>;
    fn schema(&self) -> &BatchSchema;
    fn close(&self);
}
```

### 5d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests
```

### 5e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add BatchStream trait, BatchSchema, and ColumnType"
```

---

## Step 6: Create `simd` module tree and wire into crate

**Files**: `src/simd/mod.rs` (new file), `src/simd/kernels.rs` (new file, initially empty), `src/simd/string_search.rs` (new file, initially empty), `src/lib.rs`

This step creates the module tree so that subsequent steps (7-9) can write code and tests that compile.

### 6a. Implementation

Create `src/simd/mod.rs`:
```rust
pub mod kernels;
pub mod string_search;
```

Create `src/simd/kernels.rs`:
```rust
// SIMD-friendly auto-vectorizable kernels (filter, sum, hash, bitmap ops)
```

Create `src/simd/string_search.rs`:
```rust
// Runtime-dispatched string search kernels
```

Add `pub mod simd;` to `src/lib.rs`.

### 6b. Verify

```bash
cd /Users/paulmeng/Develop/logq && cargo check
```

### 6c. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Wire simd module tree into crate"
```

---

## Step 7: Implement auto-vectorizable filter kernels

**File**: `src/simd/kernels.rs`

### 7a. Write failing test

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_ge_i32() {
        let data = vec![1, 5, 3, 10, 2, 8];
        let mut result = vec![0u8; 6];
        filter_ge_i32(&data, 5, &mut result);
        assert_eq!(result, vec![0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn test_filter_eq_i32() {
        let data = vec![1, 5, 3, 5, 2, 5];
        let mut result = vec![0u8; 6];
        filter_eq_i32(&data, 5, &mut result);
        assert_eq!(result, vec![0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn test_filter_ge_f32() {
        let data = vec![1.0f32, 5.5, 3.0, 10.0];
        let mut result = vec![0u8; 4];
        filter_ge_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![0, 1, 0, 1]);
    }

    #[test]
    fn test_sum_i32_selected() {
        let data = vec![10, 20, 30, 40];
        let mut sel = crate::execution::batch::Bitmap::all_unset(4);
        sel.set(0); sel.set(2); // select rows 0 and 2
        assert_eq!(sum_i32_selected(&data, &sel), 40);
    }

    #[test]
    fn test_sum_f32_selected() {
        let data = vec![1.0f32, 2.0, 3.0, 4.0];
        let sel = crate::execution::batch::Bitmap::all_set(4);
        let result = sum_f32_selected(&data, &sel);
        assert!((result - 10.0).abs() < 1e-6);
    }

    #[test]
    fn test_add_i32() {
        let a = vec![1, 2, 3, 4];
        let b = vec![10, 20, 30, 40];
        let mut out = vec![0i32; 4];
        add_i32(&a, &b, &mut out);
        assert_eq!(out, vec![11, 22, 33, 44]);
    }

    #[test]
    fn test_mul_i32() {
        let a = vec![2, 3, 4, 5];
        let b = vec![10, 10, 10, 10];
        let mut out = vec![0i32; 4];
        mul_i32(&a, &b, &mut out);
        assert_eq!(out, vec![20, 30, 40, 50]);
    }

    #[test]
    fn test_filter_ge_i32_batch_size() {
        // Test with full BATCH_SIZE
        let data: Vec<i32> = (0..1024).collect();
        let mut result = vec![0u8; 1024];
        filter_ge_i32(&data, 512, &mut result);
        assert_eq!(result.iter().filter(|&&b| b == 1).count(), 512);
    }
}
```

### 7b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels::tests
```

### 7c. Write implementation

```rust
// src/simd/kernels.rs
use crate::execution::batch::Bitmap;

/// Two-pass filter: compare to byte array (auto-vectorizable), then pack to bitmap.

#[inline]
pub fn filter_ge_i32(data: &[i32], threshold: i32, result_bytes: &mut [u8]) {
    for i in 0..data.len() {
        result_bytes[i] = (data[i] >= threshold) as u8;
    }
}

#[inline]
pub fn filter_eq_i32(data: &[i32], value: i32, result_bytes: &mut [u8]) {
    for i in 0..data.len() {
        result_bytes[i] = (data[i] == value) as u8;
    }
}

#[inline]
pub fn filter_ge_f32(data: &[f32], threshold: f32, result_bytes: &mut [u8]) {
    for i in 0..data.len() {
        result_bytes[i] = (data[i] >= threshold) as u8;
    }
}

#[inline]
pub fn filter_eq_f32(data: &[f32], value: f32, result_bytes: &mut [u8]) {
    for i in 0..data.len() {
        result_bytes[i] = (data[i] == value) as u8;
    }
}

/// Selection-aware sum. Unpacks bitmap to bytes for branch-free accumulation.
pub fn sum_i32_selected(data: &[i32], selection: &Bitmap) -> i64 {
    let mask = selection.unpack_to_bytes(data.len());
    let mut total: i64 = 0;
    for i in 0..data.len() {
        total += (data[i] as i64) * (mask[i] as i64);
    }
    total
}

pub fn sum_f32_selected(data: &[f32], selection: &Bitmap) -> f64 {
    let mask = selection.unpack_to_bytes(data.len());
    let mut total: f64 = 0.0;
    for i in 0..data.len() {
        total += (data[i] as f64) * (mask[i] as f64);
    }
    total
}

/// Element-wise arithmetic (auto-vectorizable).
#[inline]
pub fn add_i32(a: &[i32], b: &[i32], out: &mut [i32]) {
    for i in 0..a.len() {
        out[i] = a[i] + b[i];
    }
}

#[inline]
pub fn mul_i32(a: &[i32], b: &[i32], out: &mut [i32]) {
    for i in 0..a.len() {
        out[i] = a[i] * b[i];
    }
}

#[inline]
pub fn add_f32(a: &[f32], b: &[f32], out: &mut [f32]) {
    for i in 0..a.len() {
        out[i] = a[i] + b[i];
    }
}

#[inline]
pub fn mul_f32(a: &[f32], b: &[f32], out: &mut [f32]) {
    for i in 0..a.len() {
        out[i] = a[i] * b[i];
    }
}
```

### 7d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels::tests
```

### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add auto-vectorizable filter and arithmetic SIMD kernels"
```

---

## Step 8: Implement string search kernels (scalar + SIMD dispatch)

**File**: `src/simd/string_search.rs`

### 8a. Write failing test

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn make_utf8(strings: &[&str]) -> (Vec<u8>, Vec<u32>) {
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for s in strings {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        (data, offsets)
    }

    #[test]
    fn test_str_contains_scalar_basic() {
        let (data, offsets) = make_utf8(&["hello world", "foo bar", "hello again"]);
        let mut result = vec![0u8; 3];
        str_contains_scalar(&data, &offsets, b"hello", &mut result);
        assert_eq!(result, vec![1, 0, 1]);
    }

    #[test]
    fn test_str_contains_scalar_no_match() {
        let (data, offsets) = make_utf8(&["abc", "def", "ghi"]);
        let mut result = vec![0u8; 3];
        str_contains_scalar(&data, &offsets, b"xyz", &mut result);
        assert_eq!(result, vec![0, 0, 0]);
    }

    #[test]
    fn test_str_contains_scalar_empty_needle() {
        let (data, offsets) = make_utf8(&["abc", "def"]);
        let mut result = vec![0u8; 2];
        str_contains_scalar(&data, &offsets, b"", &mut result);
        assert_eq!(result, vec![1, 1]);
    }

    #[test]
    fn test_str_eq_batch() {
        let (data, offsets) = make_utf8(&["GET", "POST", "GET", "DELETE"]);
        let mut result = vec![0u8; 4];
        str_eq_batch(&data, &offsets, b"GET", &mut result);
        assert_eq!(result, vec![1, 0, 1, 0]);
    }

    #[test]
    fn test_str_contains_dispatch() {
        let kernels = SimdStringKernels::detect();
        let (data, offsets) = make_utf8(&["hello world", "foo", "world hello"]);
        let mut result = vec![0u8; 3];
        (kernels.str_contains_fn)(&data, &offsets, b"world", &mut result);
        assert_eq!(result, vec![1, 0, 1]);
    }
}
```

### 8b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::string_search::tests
```

### 8c. Write implementation

```rust
// src/simd/string_search.rs

/// Runtime-dispatched string search kernels.
pub struct SimdStringKernels {
    pub str_contains_fn: fn(&[u8], &[u32], &[u8], &mut [u8]),
}

impl SimdStringKernels {
    pub fn detect() -> Self {
        // For now, use scalar implementation.
        // AVX2/SSE2/NEON intrinsic implementations can be added later
        // behind #[target_feature] gates.
        SimdStringKernels {
            str_contains_fn: str_contains_scalar,
        }
    }
}

/// Scalar string contains: check if needle is a substring of each string.
/// Results written as one byte per row (0 or 1).
pub fn str_contains_scalar(
    haystack: &[u8],
    offsets: &[u32],
    needle: &[u8],
    result: &mut [u8],
) {
    let num_strings = offsets.len() - 1;
    for i in 0..num_strings {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let hay = &haystack[start..end];
        result[i] = if needle.is_empty() {
            1
        } else if hay.len() < needle.len() {
            0
        } else {
            hay.windows(needle.len()).any(|w| w == needle) as u8
        };
    }
}

/// String equality: check if each string equals the needle exactly.
pub fn str_eq_batch(
    data: &[u8],
    offsets: &[u32],
    needle: &[u8],
    result: &mut [u8],
) {
    let num_strings = offsets.len() - 1;
    let needle_len = needle.len() as u32;
    for i in 0..num_strings {
        let start = offsets[i];
        let end = offsets[i + 1];
        let len = end - start;
        result[i] = if len == needle_len {
            (&data[start as usize..end as usize] == needle) as u8
        } else {
            0
        };
    }
}
```

### 8d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::string_search::tests
```

### 8e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add string search kernels with runtime dispatch"
```

---

## Step 9: Implement hash kernels (integer, string, and combine)

**File**: `src/simd/kernels.rs`

### 9a. Write failing test

Add to the existing `tests` module in `src/simd/kernels.rs`:

```rust
#[test]
fn test_hash_column_i32_deterministic() {
    let data = vec![1, 2, 3, 4];
    let mut h1 = vec![0u64; 4];
    let mut h2 = vec![0u64; 4];
    hash_column_i32(&data, &mut h1);
    hash_column_i32(&data, &mut h2);
    assert_eq!(h1, h2);
}

#[test]
fn test_hash_column_i32_distinct() {
    let data = vec![0, 1, 2, 3];
    let mut hashes = vec![0u64; 4];
    hash_column_i32(&data, &mut hashes);
    // All different inputs should produce different hashes
    let mut unique: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for &h in &hashes { unique.insert(h); }
    assert_eq!(unique.len(), 4);
}

#[test]
fn test_hash_column_utf8_deterministic() {
    let data = b"helloworld";
    let offsets = vec![0u32, 5, 10];
    let mut h1 = vec![0u64; 2];
    let mut h2 = vec![0u64; 2];
    hash_column_utf8(data, &offsets, &mut h1);
    hash_column_utf8(data, &offsets, &mut h2);
    assert_eq!(h1, h2);
}

#[test]
fn test_hash_column_utf8_distinct() {
    let data = b"helloworld";
    let offsets = vec![0u32, 5, 10];
    let mut hashes = vec![0u64; 2];
    hash_column_utf8(data, &offsets, &mut hashes);
    assert_ne!(hashes[0], hashes[1]); // "hello" != "world"
}

#[test]
fn test_hash_combine() {
    let mut a = vec![100u64, 200, 300];
    let b = vec![1u64, 2, 3];
    hash_combine(&mut a, &b);
    // Result should differ from original
    assert_ne!(a[0], 100);
    // Should be deterministic
    let mut a2 = vec![100u64, 200, 300];
    hash_combine(&mut a2, &b);
    assert_eq!(a, a2);
}
```

### 9b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels::tests
```

### 9c. Write implementation

Add to `src/simd/kernels.rs`:

```rust
use ahash::AHasher;
use std::hash::Hasher;

const HASH_MULT: u64 = 0x517cc1b727220a95;

/// Multiply-shift hash for integer keys (auto-vectorizable).
#[inline]
pub fn hash_column_i32(data: &[i32], hashes: &mut [u64]) {
    for i in 0..data.len() {
        hashes[i] = (data[i] as u64).wrapping_mul(HASH_MULT);
    }
}

/// AHash-based hash for string keys. Not auto-vectorizable (variable-length data),
/// but AHash uses hardware AES-NI where available.
pub fn hash_column_utf8(data: &[u8], offsets: &[u32], hashes: &mut [u64]) {
    for i in 0..offsets.len() - 1 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let mut hasher = AHasher::default();
        hasher.write(&data[start..end]);
        hashes[i] = hasher.finish();
    }
}

/// Combine hashes from multiple columns using rotation-xor-multiply.
#[inline]
pub fn hash_combine(existing: &mut [u64], new: &[u64]) {
    for (h, &n) in existing.iter_mut().zip(new.iter()) {
        *h = (h.rotate_left(5) ^ n).wrapping_mul(HASH_MULT);
    }
}
```

### 9d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib simd::kernels::tests
```

### 9e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add multiply-shift hash, AHash string hash, and hash_combine kernels"
```

---

## Step 10: Implement `BatchToRowAdapter`

**File**: `src/execution/batch.rs`

### 10a. Write failing test

```rust
#[test]
fn test_batch_to_row_adapter() {
    use crate::common::types::Value;
    use super::stream::RecordStream;

    // Create a batch with 3 rows, 2 columns: "x" (Int32), "name" (Utf8)
    let col_x = TypedColumn::Int32 {
        data: vec![10, 20, 30],
        null: Bitmap::all_set(3),
        missing: Bitmap::all_set(3),
    };
    let col_name = TypedColumn::Utf8 {
        data: b"alicebobcharlie".to_vec(),
        offsets: vec![0, 5, 8, 15],
        null: Bitmap::all_set(3),
        missing: Bitmap::all_set(3),
    };
    let batch = ColumnBatch {
        columns: vec![col_x, col_name],
        names: vec!["x".to_string(), "name".to_string()],
        selection: SelectionVector::All,
        len: 3,
    };

    let schema = BatchSchema {
        names: vec!["x".to_string(), "name".to_string()],
        types: vec![ColumnType::Int32, ColumnType::Utf8],
    };

    // Create a mock BatchStream that yields one batch then None
    struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> { Ok(self.batch.take()) }
        fn schema(&self) -> &BatchSchema { &self.schema }
        fn close(&self) {}
    }

    let inner = Box::new(OneBatch { batch: Some(batch), schema });
    let mut adapter = BatchToRowAdapter::new(inner);

    let r0 = adapter.next().unwrap().unwrap();
    assert_eq!(r0.get_ref(&crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("x".to_string())
    ])), Some(&Value::Int(10)));

    let r1 = adapter.next().unwrap().unwrap();
    assert_eq!(r1.get_ref(&crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("name".to_string())
    ])), Some(&Value::String("bob".to_string())));

    let r2 = adapter.next().unwrap().unwrap();
    assert!(r2.get_ref(&crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("x".to_string())
    ])) == Some(&Value::Int(30)));

    let r3 = adapter.next().unwrap();
    assert!(r3.is_none());
}

#[test]
fn test_batch_to_row_adapter_with_selection() {
    use crate::common::types::Value;
    use super::stream::RecordStream;

    // Create a batch with 4 rows, only rows 1 and 3 active via Bitmap
    let col_x = TypedColumn::Int32 {
        data: vec![10, 20, 30, 40],
        null: Bitmap::all_set(4),
        missing: Bitmap::all_set(4),
    };

    let mut sel_bm = Bitmap::all_unset(4);
    sel_bm.set(1); // row 1 active
    sel_bm.set(3); // row 3 active

    let batch = ColumnBatch {
        columns: vec![col_x],
        names: vec!["x".to_string()],
        selection: SelectionVector::Bitmap(sel_bm),
        len: 4,
    };

    let schema = BatchSchema {
        names: vec!["x".to_string()],
        types: vec![ColumnType::Int32],
    };

    struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> { Ok(self.batch.take()) }
        fn schema(&self) -> &BatchSchema { &self.schema }
        fn close(&self) {}
    }

    let inner = Box::new(OneBatch { batch: Some(batch), schema });
    let mut adapter = BatchToRowAdapter::new(inner);

    // Only rows 1 and 3 should be emitted
    let r0 = adapter.next().unwrap().unwrap();
    assert_eq!(r0.get_ref(&crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("x".to_string())
    ])), Some(&Value::Int(20)));

    let r1 = adapter.next().unwrap().unwrap();
    assert_eq!(r1.get_ref(&crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("x".to_string())
    ])), Some(&Value::Int(40)));

    // No more rows
    let r2 = adapter.next().unwrap();
    assert!(r2.is_none());
}
```

### 10b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_batch_to_row_adapter
```

### 10c. Write implementation

Add to `src/execution/batch.rs`:

```rust
use super::stream::{Record, RecordStream};
use linked_hash_map::LinkedHashMap;
use ordered_float::OrderedFloat;
use std::collections::VecDeque;

/// Converts a BatchStream into a RecordStream by materializing batches into individual Records.
pub struct BatchToRowAdapter {
    inner: Box<dyn BatchStream>,
    buffer: VecDeque<Record>,
}

impl BatchToRowAdapter {
    pub fn new(inner: Box<dyn BatchStream>) -> Self {
        BatchToRowAdapter { inner, buffer: VecDeque::new() }
    }

    fn materialize_batch(&mut self, batch: ColumnBatch) {
        for row_idx in 0..batch.len {
            // Check selection vector
            let active = match &batch.selection {
                SelectionVector::All => true,
                SelectionVector::Bitmap(bm) => bm.is_set(row_idx),
                SelectionVector::Indices(indices) => indices.contains(&(row_idx as u32)),
            };
            if !active { continue; }

            let mut variables = LinkedHashMap::new();
            for (col_idx, col) in batch.columns.iter().enumerate() {
                let name = &batch.names[col_idx];
                let value = typed_column_get_value(col, row_idx);
                variables.insert(name.clone(), value);
            }
            self.buffer.push_back(Record::new_with_variables(variables));
        }
    }
}

impl RecordStream for BatchToRowAdapter {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        loop {
            if let Some(record) = self.buffer.pop_front() {
                return Ok(Some(record));
            }
            match self.inner.next_batch()? {
                Some(batch) => self.materialize_batch(batch),
                None => return Ok(None),
            }
        }
    }

    fn close(&self) {
        self.inner.close();
    }
}

/// Extract a single Value from a TypedColumn at a given row index.
fn typed_column_get_value(col: &TypedColumn, idx: usize) -> Value {
    match col {
        TypedColumn::Int32 { data, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            Value::Int(data[idx])
        }
        TypedColumn::Float32 { data, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            Value::Float(OrderedFloat(data[idx]))
        }
        TypedColumn::Boolean { data, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            Value::Boolean(data.is_set(idx))
        }
        TypedColumn::Utf8 { data, offsets, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            Value::String(String::from_utf8_lossy(&data[start..end]).into_owned())
        }
        TypedColumn::DateTime { data, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            use chrono::{TimeZone, FixedOffset};
            let utc = FixedOffset::east_opt(0).unwrap();
            let secs = data[idx] / 1_000_000;
            let nanos = ((data[idx] % 1_000_000) * 1000) as u32;
            match utc.timestamp_opt(secs, nanos) {
                chrono::LocalResult::Single(dt) => Value::DateTime(dt),
                _ => Value::Null,
            }
        }
        TypedColumn::Mixed { data, null, missing } => {
            if !missing.is_set(idx) { return Value::Missing; }
            if !null.is_set(idx) { return Value::Null; }
            data[idx].clone()
        }
    }
}
```

### 10d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_batch_to_row_adapter
```

### 10e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add BatchToRowAdapter with selection vector support"
```

---

## Step 11: Implement `ColumnBuilder` helper

**File**: `src/execution/batch.rs`

This step implements `ColumnBuilder` separately from `RowToBatchAdapter` for clarity and testability.

### 11a. Write failing test

```rust
#[test]
fn test_column_builder_int32_basic() {
    use crate::common::types::Value;

    let mut builder = ColumnBuilder::new(ColumnType::Int32);
    builder.push(Value::Int(10));
    builder.push(Value::Int(20));
    builder.push(Value::Int(30));
    let col = builder.finish();

    match &col {
        TypedColumn::Int32 { data, null, missing } => {
            assert_eq!(data, &[10, 20, 30]);
            assert_eq!(null.count_ones(), 3);
            assert_eq!(missing.count_ones(), 3);
        }
        _ => panic!("expected Int32"),
    }
}

#[test]
fn test_column_builder_null_and_missing() {
    use crate::common::types::Value;

    let mut builder = ColumnBuilder::new(ColumnType::Int32);
    builder.push(Value::Int(42));
    builder.push(Value::Null);
    builder.push(Value::Missing);
    let col = builder.finish();

    match &col {
        TypedColumn::Int32 { data, null, missing } => {
            assert_eq!(data.len(), 3); // default values for NULL/MISSING slots
            assert_eq!(data[0], 42);
            // Row 0: present
            assert!(missing.is_set(0) && null.is_set(0));
            // Row 1: NULL
            assert!(missing.is_set(1) && !null.is_set(1));
            // Row 2: MISSING
            assert!(!missing.is_set(2));
        }
        _ => panic!("expected Int32"),
    }
}

#[test]
fn test_column_builder_utf8() {
    use crate::common::types::Value;

    let mut builder = ColumnBuilder::new(ColumnType::Utf8);
    builder.push(Value::String("hello".to_string()));
    builder.push(Value::Null);
    builder.push(Value::String("world".to_string()));
    let col = builder.finish();

    match &col {
        TypedColumn::Utf8 { data, offsets, null, missing } => {
            assert_eq!(offsets.len(), 4); // 3 rows + 1 initial offset
            let s0 = &data[offsets[0] as usize..offsets[1] as usize];
            assert_eq!(s0, b"hello");
            // Row 1 is NULL: offset[1] == offset[2] (zero-length)
            assert_eq!(offsets[1], offsets[2]);
            let s2 = &data[offsets[2] as usize..offsets[3] as usize];
            assert_eq!(s2, b"world");
            // null bitmap
            assert!(null.is_set(0));
            assert!(!null.is_set(1)); // NULL
            assert!(null.is_set(2));
        }
        _ => panic!("expected Utf8"),
    }
}

#[test]
fn test_column_builder_type_mismatch_becomes_null() {
    use crate::common::types::Value;

    let mut builder = ColumnBuilder::new(ColumnType::Int32);
    builder.push(Value::Int(10));
    builder.push(Value::String("oops".to_string())); // type mismatch
    builder.push(Value::Int(30));
    let col = builder.finish();

    match &col {
        TypedColumn::Int32 { data, null, missing } => {
            assert_eq!(data.len(), 3);
            assert_eq!(data[0], 10);
            assert_eq!(data[2], 30);
            // Row 1: type mismatch -> treated as NULL
            assert!(missing.is_set(1)); // field exists (it had a value)
            assert!(!null.is_set(1));   // but it's NULL (couldn't convert)
        }
        _ => panic!("expected Int32"),
    }
}
```

### 11b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_column_builder
```

### 11c. Write implementation

Add to `src/execution/batch.rs`:

```rust
/// Helper for building TypedColumns row-by-row.
pub(crate) struct ColumnBuilder {
    col_type: ColumnType,
    int32_data: Vec<i32>,
    float32_data: Vec<f32>,
    bool_data: Vec<bool>,
    utf8_data: Vec<u8>,
    utf8_offsets: Vec<u32>,
    datetime_data: Vec<i64>,
    mixed_data: Vec<Value>,
    null: Vec<bool>,
    missing: Vec<bool>,
}

impl ColumnBuilder {
    pub(crate) fn new(col_type: ColumnType) -> Self {
        ColumnBuilder {
            col_type,
            int32_data: Vec::new(),
            float32_data: Vec::new(),
            bool_data: Vec::new(),
            utf8_data: Vec::new(),
            utf8_offsets: vec![0],
            datetime_data: Vec::new(),
            mixed_data: Vec::new(),
            null: Vec::new(),
            missing: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, value: Value) {
        // Phase 1: Determine null/missing status and extract the typed value.
        let typed_value = match value {
            Value::Missing => {
                self.missing.push(false);
                self.null.push(false);
                None
            }
            Value::Null => {
                self.missing.push(true);
                self.null.push(false);
                None
            }
            other => {
                // Try to push as the expected type. If there is a type mismatch,
                // push a default and mark as NULL.
                let matched = match (&self.col_type, &other) {
                    (ColumnType::Int32, Value::Int(_)) => true,
                    (ColumnType::Float32, Value::Float(_)) => true,
                    (ColumnType::Boolean, Value::Boolean(_)) => true,
                    (ColumnType::Utf8, Value::String(_)) => true,
                    (ColumnType::DateTime, Value::DateTime(_)) => true,
                    (ColumnType::Mixed, _) => true,
                    _ => false,
                };
                if matched {
                    self.missing.push(true);
                    self.null.push(true);
                    Some(other)
                } else {
                    // Type mismatch: mark as NULL, push default below
                    self.missing.push(true);
                    self.null.push(false);
                    None
                }
            }
        };

        // Phase 2: Push data (either the real value or a default placeholder).
        match typed_value {
            Some(v) => {
                match (&self.col_type, v) {
                    (ColumnType::Int32, Value::Int(n)) => self.int32_data.push(n),
                    (ColumnType::Float32, Value::Float(f)) => self.float32_data.push(f.into_inner()),
                    (ColumnType::Boolean, Value::Boolean(b)) => self.bool_data.push(b),
                    (ColumnType::Utf8, Value::String(s)) => {
                        self.utf8_data.extend_from_slice(s.as_bytes());
                        self.utf8_offsets.push(self.utf8_data.len() as u32);
                    }
                    (ColumnType::DateTime, Value::DateTime(dt)) => {
                        self.datetime_data.push(dt.timestamp() * 1_000_000 + dt.timestamp_subsec_micros() as i64);
                    }
                    (ColumnType::Mixed, v) => self.mixed_data.push(v),
                    _ => unreachable!("matched guard above ensures this arm is unreachable"),
                }
            }
            None => {
                // Push default for NULL, MISSING, or type-mismatch rows
                self.push_default();
            }
        }
    }

    fn push_default(&mut self) {
        match &self.col_type {
            ColumnType::Int32 => self.int32_data.push(0),
            ColumnType::Float32 => self.float32_data.push(0.0),
            ColumnType::Boolean => self.bool_data.push(false),
            ColumnType::Utf8 => {
                // Push a duplicate offset (zero-length string) for NULL/MISSING
                let last = *self.utf8_offsets.last().unwrap();
                self.utf8_offsets.push(last);
            }
            ColumnType::DateTime => self.datetime_data.push(0),
            ColumnType::Mixed => self.mixed_data.push(Value::Null),
        }
    }

    pub(crate) fn finish(self) -> TypedColumn {
        let null_bm = Bitmap::pack_from_bytes(
            &self.null.iter().map(|&b| b as u8).collect::<Vec<_>>()
        );
        let missing_bm = Bitmap::pack_from_bytes(
            &self.missing.iter().map(|&b| b as u8).collect::<Vec<_>>()
        );
        match self.col_type {
            ColumnType::Int32 => TypedColumn::Int32 { data: self.int32_data, null: null_bm, missing: missing_bm },
            ColumnType::Float32 => TypedColumn::Float32 { data: self.float32_data, null: null_bm, missing: missing_bm },
            ColumnType::Boolean => {
                let data_bm = Bitmap::pack_from_bytes(
                    &self.bool_data.iter().map(|&b| b as u8).collect::<Vec<_>>()
                );
                TypedColumn::Boolean { data: data_bm, null: null_bm, missing: missing_bm }
            }
            ColumnType::Utf8 => TypedColumn::Utf8 { data: self.utf8_data, offsets: self.utf8_offsets, null: null_bm, missing: missing_bm },
            ColumnType::DateTime => TypedColumn::DateTime { data: self.datetime_data, null: null_bm, missing: missing_bm },
            ColumnType::Mixed => TypedColumn::Mixed { data: self.mixed_data, null: null_bm, missing: missing_bm },
        }
    }
}
```

### 11d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_column_builder
```

### 11e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add ColumnBuilder with correct type-mismatch and Utf8 handling"
```

---

## Step 12: Implement `RowToBatchAdapter`

**File**: `src/execution/batch.rs`

### 12a. Write failing test

```rust
#[test]
fn test_row_to_batch_adapter() {
    use crate::common::types::Value;
    use super::stream::{InMemoryStream, RecordStream, Record};
    use linked_hash_map::LinkedHashMap;

    let mut vars = LinkedHashMap::new();
    vars.insert("x".to_string(), Value::Int(42));
    vars.insert("name".to_string(), Value::String("alice".to_string()));
    let r = Record::new_with_variables(vars);

    let records: VecDeque<Record> = vec![r].into_iter().collect();
    let source = InMemoryStream::new(records);

    let schema = BatchSchema {
        names: vec!["x".to_string(), "name".to_string()],
        types: vec![ColumnType::Int32, ColumnType::Utf8],
    };

    let mut adapter = RowToBatchAdapter::new(Box::new(source), schema);
    let batch = adapter.next_batch().unwrap().unwrap();
    assert_eq!(batch.len, 1);
    assert_eq!(batch.names, vec!["x", "name"]);

    match &batch.columns[0] {
        TypedColumn::Int32 { data, .. } => assert_eq!(data[0], 42),
        _ => panic!("expected Int32"),
    }
    match &batch.columns[1] {
        TypedColumn::Utf8 { data, offsets, .. } => {
            assert_eq!(&data[offsets[0] as usize..offsets[1] as usize], b"alice");
        }
        _ => panic!("expected Utf8"),
    }

    let next = adapter.next_batch().unwrap();
    assert!(next.is_none());
}
```

### 12b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_row_to_batch_adapter
```

### 12c. Write implementation

Add to `src/execution/batch.rs`:

```rust
/// Converts a RecordStream into a BatchStream by buffering rows into columnar batches.
pub struct RowToBatchAdapter {
    inner: Box<dyn RecordStream>,
    schema: BatchSchema,
    exhausted: bool,
}

impl RowToBatchAdapter {
    pub fn new(inner: Box<dyn RecordStream>, schema: BatchSchema) -> Self {
        RowToBatchAdapter { inner, schema, exhausted: false }
    }
}

impl BatchStream for RowToBatchAdapter {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.exhausted { return Ok(None); }

        // Initialize column builders
        let mut builders: Vec<ColumnBuilder> = self.schema.types.iter()
            .map(|t| ColumnBuilder::new(t.clone()))
            .collect();
        let mut row_count = 0;

        // Read up to BATCH_SIZE rows
        while row_count < BATCH_SIZE {
            match self.inner.next()? {
                Some(record) => {
                    let vars = record.into_variables();
                    for (col_idx, name) in self.schema.names.iter().enumerate() {
                        let value = vars.get(name).cloned().unwrap_or(Value::Missing);
                        builders[col_idx].push(value);
                    }
                    row_count += 1;
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if row_count == 0 { return Ok(None); }

        let columns: Vec<TypedColumn> = builders.into_iter()
            .map(|b| b.finish())
            .collect();

        Ok(Some(ColumnBatch {
            columns,
            names: self.schema.names.clone(),
            selection: SelectionVector::All,
            len: row_count,
        }))
    }

    fn schema(&self) -> &BatchSchema { &self.schema }
    fn close(&self) { self.inner.close(); }
}
```

### 12d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch::tests::test_row_to_batch_adapter
```

### 12e. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Add RowToBatchAdapter for incremental operator migration"
```

---

## Step 13: Run full test suite to verify no regressions

### 13a. Run all tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 13b. Commit (if any fixes needed)

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Fix any regressions from batch infrastructure"
```

---

## Step 14: JSONL simd-json migration (mutable buffer)

**File**: `src/execution/datasource.rs`

This step changes the JSONL parsing path to use `simd-json` with mutable `Vec<u8>` buffers, as specified in Phase 1 of the design document.

### 14a. Write failing test

```rust
#[cfg(test)]
mod simd_json_tests {
    use super::*;

    #[test]
    fn test_jsonl_parse_with_simd_json() {
        // Verify that a JSONL line can be parsed via simd-json's mutable buffer API
        let mut buf = br#"{"name":"alice","age":30,"active":true}"#.to_vec();
        let val: simd_json::BorrowedValue = simd_json::to_borrowed_value(&mut buf).unwrap();
        assert_eq!(val["name"], "alice");
        assert_eq!(val["age"], 30);
        assert_eq!(val["active"], true);
    }
}
```

### 14b. Implementation

Locate the JSONL record reading path in `src/execution/datasource.rs` (the section that calls `json::parse`). Change it to:

1. Read each line into a `String` as before.
2. Convert to `Vec<u8>` (via `line.into_bytes()` or by reading into a byte buffer directly).
3. Call `simd_json::to_borrowed_value(&mut buf)` or `simd_json::to_owned_value(&mut buf)` instead of `json::parse(&line)`.
4. Convert the `simd_json` value to the existing `Value` enum representation.

The key change is that `simd-json` modifies the input buffer in-place for zero-copy parsing, so immutable `&str` borrows are replaced with mutable `Vec<u8>` ownership.

### 14c. Verify

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 14d. Commit

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Migrate JSONL parsing from json crate to simd-json with mutable buffers"
```

---

## Step 15: Run full test suite — final verification

### 15a. Run all tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 15b. Commit (if any fixes needed)

```bash
cd /Users/paulmeng/Develop/logq && sl commit -m "Final test suite verification for SIMD physical plan foundation"
```

---

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Step 1 | No | `Cargo.toml` |
| 2 | Step 2 | No (creates module + wires it) | `src/execution/batch.rs`, `src/execution/mod.rs` |
| 3 | Steps 3, 4, 5 | Sequential (same file, depend on Step 2) | `src/execution/batch.rs` |
| 4 | Step 6 | No (creates module tree) | `src/simd/mod.rs`, `src/simd/kernels.rs`, `src/simd/string_search.rs`, `src/lib.rs` |
| 5 | Steps 7, 8, 9 | Steps 7 and 8 parallel (independent files); Step 9 depends on Step 7 (same file) | `src/simd/kernels.rs`, `src/simd/string_search.rs` |
| 6 | Steps 10, 11, 12 | Sequential (same file, each depends on prior) | `src/execution/batch.rs` |
| 7 | Step 13 | No (integration check) | All |
| 8 | Step 14 | No (depends on Step 1 for simd-json dep) | `src/execution/datasource.rs` |
| 9 | Step 15 | No (final check) | All |

**Key ordering constraints:**
- Step 2 must come before Steps 3-5 (batch module must exist and be wired)
- Step 6 must come before Steps 7-9 (simd module must exist and be wired)
- Step 1 must come before Step 14 (simd-json dependency must be added first)
- Steps 7-9 depend on Step 2 (kernels reference `Bitmap` from `execution::batch`)
- Step 11 depends on Step 5 (ColumnBuilder uses ColumnType)
- Step 12 depends on Step 11 (RowToBatchAdapter uses ColumnBuilder)

---

## Notes

This plan covers **Phase 1** (foundation) and **Phase 2** (adapters) from the design document. It does NOT cover:
- Phase 0 (profiling) -- should be done before this plan, or in parallel
- Phase 3 (converting individual operators to batch versions: `BatchScanOperator`, `BatchFilterOperator`, etc.)
- Phase 4 (removing old row-based code)

Each subsequent phase should have its own implementation plan, informed by profiling data from Phase 0 and benchmark results from this phase.
