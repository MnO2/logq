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

pub fn filter_ne_i32(data: &[i32], value: i32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] != value) as u8;
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

pub fn filter_gt_f32(data: &[f32], threshold: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] > threshold) as u8;
    }
}

pub fn filter_lt_f32(data: &[f32], threshold: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] < threshold) as u8;
    }
}

pub fn filter_le_f32(data: &[f32], threshold: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] <= threshold) as u8;
    }
}

pub fn filter_ne_f32(data: &[f32], value: f32, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] != value) as u8;
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

// --- Dictionary-encoded string kernels ---

/// Compare u16 dictionary codes for equality. Auto-vectorizable.
pub fn filter_eq_u16(data: &[u16], value: u16, result: &mut [u8]) {
    for i in 0..data.len() {
        result[i] = (data[i] == value) as u8;
    }
}

/// Broadcast dictionary code matches into a result bitmap.
/// `match_table` has one entry per dictionary code (true if that code matches).
/// For each row, look up its code in the table.
pub fn dict_broadcast(codes: &[u16], match_table: &[u8], result: &mut [u8]) {
    for i in 0..codes.len() {
        result[i] = match_table[codes[i] as usize];
    }
}

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

    #[test]
    fn test_filter_gt_f32() {
        let data = [1.0f32, 5.5, 10.0, 3.0];
        let mut result = vec![0u8; 4];
        filter_gt_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![0, 1, 1, 0]);
    }

    #[test]
    fn test_filter_lt_f32() {
        let data = [1.0f32, 5.5, 10.0, 3.0];
        let mut result = vec![0u8; 4];
        filter_lt_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![1, 0, 0, 1]);
    }

    #[test]
    fn test_filter_le_f32() {
        let data = [1.0f32, 5.0, 10.0, 3.0];
        let mut result = vec![0u8; 4];
        filter_le_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![1, 1, 0, 1]);
    }

    #[test]
    fn test_filter_ne_f32() {
        let data = [1.0f32, 5.0, 5.0, 3.0];
        let mut result = vec![0u8; 4];
        filter_ne_f32(&data, 5.0, &mut result);
        assert_eq!(result, vec![1, 0, 0, 1]);
    }

    #[test]
    fn test_filter_ne_i32() {
        let data = [1, 5, 5, 3, 5];
        let mut result = vec![0u8; 5];
        filter_ne_i32(&data, 5, &mut result);
        assert_eq!(result, vec![1, 0, 0, 1, 0]);
    }

    #[test]
    fn test_filter_eq_u16() {
        let data = [0u16, 1, 0, 2, 1, 0];
        let mut result = vec![0u8; 6];
        filter_eq_u16(&data, 1, &mut result);
        assert_eq!(result, vec![0, 1, 0, 0, 1, 0]);
    }

    #[test]
    fn test_dict_broadcast() {
        // 3 dict entries: code 0 matches, code 1 doesn't, code 2 matches
        let codes = [0u16, 1, 2, 0, 1, 2, 0];
        let match_table = [1u8, 0, 1];
        let mut result = vec![0u8; 7];
        dict_broadcast(&codes, &match_table, &mut result);
        assert_eq!(result, vec![1, 0, 1, 1, 0, 1, 1]);
    }
}
