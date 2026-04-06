/// A bitmap stored as packed u64 words.
/// Bit 1 = present/true, Bit 0 = absent/false.
#[derive(Clone, Debug)]
pub struct Bitmap {
    pub(crate) words: Vec<u64>,
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

    #[inline]
    pub fn is_set(&self, idx: usize) -> bool {
        debug_assert!(idx < self.words.len() * 64);
        let word = idx / 64;
        let bit = idx % 64;
        (self.words[word] >> bit) & 1 == 1
    }

    #[inline]
    pub fn set(&mut self, idx: usize) {
        debug_assert!(idx < self.words.len() * 64);
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] |= 1u64 << bit;
    }

    #[inline]
    pub fn unset(&mut self, idx: usize) {
        debug_assert!(idx < self.words.len() * 64);
        let word = idx / 64;
        let bit = idx % 64;
        self.words[word] &= !(1u64 << bit);
    }

    pub fn and(&self, other: &Bitmap) -> Bitmap {
        debug_assert_eq!(self.words.len(), other.words.len());
        let words = self.words.iter().zip(other.words.iter())
            .map(|(a, b)| a & b).collect();
        Bitmap { words }
    }

    pub fn or(&self, other: &Bitmap) -> Bitmap {
        debug_assert_eq!(self.words.len(), other.words.len());
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

    #[inline]
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    #[inline]
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
            debug_assert!(byte <= 1, "pack_from_bytes expects 0 or 1, got {}", byte);
            words[i / 64] |= (byte as u64 & 1) << (i % 64);
        }
        Bitmap { words }
    }
}

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
}
