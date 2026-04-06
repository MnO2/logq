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

    pub fn is_active(&self, idx: usize, total: usize) -> bool {
        debug_assert!(idx < total, "is_active index {} out of bounds (total {})", idx, total);
        match self {
            SelectionVector::All => true,
            SelectionVector::Bitmap(bm) => bm.is_set(idx),
        }
    }
}

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
