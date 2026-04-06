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
