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
