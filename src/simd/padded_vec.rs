use std::ops::Deref;
use std::ptr;

/// Number of bytes of zeroed padding appended past the logical end.
const SIMD_PADDING: usize = 32;

/// An immutable vector that guarantees at least [`SIMD_PADDING`] bytes of
/// zeroed memory past the logical end.  This makes it safe for SIMD routines
/// to perform unaligned reads that overshoot the last element without
/// triggering undefined behaviour from uninitialised memory.
///
/// Construct via [`PaddedVec::with_len`], [`PaddedVec::from_vec`], or
/// [`PaddedVecBuilder::seal`].
pub struct PaddedVec<T: Copy + Default> {
    pub(crate) inner: Vec<T>,
    logical_len: usize,
}

impl<T: Copy + Default> PaddedVec<T> {
    /// Minimum number of **elements** required as padding for type `T`.
    fn pad_elements() -> usize {
        let elem = std::mem::size_of::<T>();
        if elem == 0 {
            return 0;
        }
        // ceiling division: (SIMD_PADDING + elem - 1) / elem
        (SIMD_PADDING + elem - 1) / elem
    }

    /// Creates a `PaddedVec` of `len` default-initialised elements with
    /// at least [`SIMD_PADDING`] bytes of zeroed padding past the logical
    /// end.
    pub fn with_len(len: usize) -> Self {
        let pad = Self::pad_elements();
        let total = len + pad;
        let mut v = vec![T::default(); total];
        // Truncate the logical length but keep the capacity (and the
        // default-initialised padding elements).
        v.truncate(len);
        // Safety: the extra capacity was filled with T::default() which,
        // for the numeric types we care about, is zero.  We re-zero with
        // ptr::write_bytes for belt-and-suspenders safety.
        v.reserve(pad);
        unsafe {
            let base = v.as_mut_ptr().add(len);
            ptr::write_bytes(base, 0, pad);
        }
        debug_assert!(v.capacity() >= len + pad);
        Self {
            inner: v,
            logical_len: len,
        }
    }

    /// Wraps an existing `Vec<T>`, appending zeroed padding.
    pub fn from_vec(mut v: Vec<T>) -> Self {
        let len = v.len();
        let pad = Self::pad_elements();
        v.reserve(pad);
        unsafe {
            let base = v.as_mut_ptr().add(len);
            ptr::write_bytes(base, 0, pad);
        }
        debug_assert!(v.capacity() >= len + pad);
        Self {
            inner: v,
            logical_len: len,
        }
    }

    /// Returns the number of logical (user-visible) elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.logical_len
    }

    /// Returns `true` when the logical length is zero.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.logical_len == 0
    }

    /// Returns the allocated capacity of the underlying buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

impl<T: Copy + Default> Deref for PaddedVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        // Only expose the logical portion — padding is an implementation
        // detail.
        &self.inner[..self.logical_len]
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// A mutable construction helper for [`PaddedVec`].
///
/// Use [`PaddedVecBuilder::seal`] to freeze the contents and obtain an
/// immutable `PaddedVec` with the required SIMD padding.
pub struct PaddedVecBuilder<T: Copy + Default> {
    inner: Vec<T>,
}

impl<T: Copy + Default> PaddedVecBuilder<T> {
    /// Creates a builder with the given initial capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Vec::with_capacity(cap),
        }
    }

    /// Creates a builder with no pre-allocated capacity.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Appends a single element.
    pub fn push(&mut self, val: T) {
        self.inner.push(val);
    }

    /// Appends all elements from the slice.
    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.inner.extend_from_slice(slice);
    }

    /// Returns the number of elements pushed so far.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Consumes the builder and returns an immutable [`PaddedVec`] with
    /// zeroed tail padding.
    pub fn seal(self) -> PaddedVec<T> {
        PaddedVec::from_vec(self.inner)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn with_len_has_correct_logical_length() {
        let pv = PaddedVec::<u8>::with_len(100);
        assert_eq!(pv.len(), 100);
    }

    #[test]
    fn with_len_has_padding_capacity() {
        let pv = PaddedVec::<u8>::with_len(100);
        let pad = PaddedVec::<u8>::pad_elements();
        assert!(pv.capacity() >= 100 + pad);
    }

    #[test]
    fn from_vec_preserves_data() {
        let src = vec![1u32, 2, 3, 4, 5];
        let pv = PaddedVec::from_vec(src.clone());
        assert_eq!(&*pv, &src[..]);
    }

    #[test]
    fn from_vec_has_padding() {
        let src = vec![10u8; 50];
        let pv = PaddedVec::from_vec(src);
        let pad = PaddedVec::<u8>::pad_elements();
        assert!(pv.capacity() >= 50 + pad);
    }

    #[test]
    fn deref_returns_logical_slice() {
        let pv = PaddedVec::<u16>::with_len(10);
        let slice: &[u16] = &*pv;
        assert_eq!(slice.len(), 10);
    }

    #[test]
    fn empty_vec_works() {
        let pv = PaddedVec::<u64>::with_len(0);
        assert!(pv.is_empty());
        assert_eq!(pv.len(), 0);
        assert_eq!(pv.deref().len(), 0);
    }

    #[test]
    fn builder_push_and_seal() {
        let mut b = PaddedVecBuilder::<u32>::new();
        b.push(1);
        b.push(2);
        b.push(3);
        let pv = b.seal();
        assert_eq!(pv.len(), 3);
        assert_eq!(&*pv, &[1u32, 2, 3]);
    }

    #[test]
    fn builder_extend_from_slice_and_seal() {
        let mut b = PaddedVecBuilder::<u8>::with_capacity(8);
        b.extend_from_slice(&[10, 20, 30]);
        let pv = b.seal();
        assert_eq!(pv.len(), 3);
        assert_eq!(&*pv, &[10u8, 20, 30]);
    }

    #[test]
    fn builder_len() {
        let mut b = PaddedVecBuilder::<u8>::new();
        assert_eq!(b.len(), 0);
        b.push(1);
        assert_eq!(b.len(), 1);
        b.extend_from_slice(&[2, 3, 4]);
        assert_eq!(b.len(), 4);
    }

    #[test]
    fn padding_bytes_are_zeroed() {
        let src = vec![0xFFu8; 64];
        let pv = PaddedVec::from_vec(src);
        let pad = PaddedVec::<u8>::pad_elements();
        // Read beyond logical length into the padding zone.
        // Safety: we know capacity >= len + pad and we zeroed those bytes.
        unsafe {
            let base = pv.inner.as_ptr().add(pv.len());
            for i in 0..pad {
                let byte = *base.add(i);
                assert_eq!(
                    byte, 0,
                    "padding byte at offset {} was {:#x}, expected 0x00",
                    i, byte
                );
            }
        }
    }

    #[test]
    fn padding_for_larger_types() {
        // For u32 (4 bytes), pad_elements should be ceil(32/4) = 8
        assert_eq!(PaddedVec::<u32>::pad_elements(), 8);
        // For u64 (8 bytes), pad_elements should be ceil(32/8) = 4
        assert_eq!(PaddedVec::<u64>::pad_elements(), 4);
        // For u8 (1 byte), pad_elements should be 32
        assert_eq!(PaddedVec::<u8>::pad_elements(), 32);
        // For u16 (2 bytes), pad_elements should be 16
        assert_eq!(PaddedVec::<u16>::pad_elements(), 16);
    }

    #[test]
    fn padding_bytes_zeroed_for_u32() {
        let src = vec![0xDEADBEEFu32; 10];
        let pv = PaddedVec::from_vec(src);
        let pad = PaddedVec::<u32>::pad_elements();
        unsafe {
            let base = pv.inner.as_ptr().add(pv.len()) as *const u8;
            let byte_count = pad * mem::size_of::<u32>();
            for i in 0..byte_count {
                let byte = *base.add(i);
                assert_eq!(
                    byte, 0,
                    "padding byte at offset {} was {:#x}, expected 0x00",
                    i, byte
                );
            }
        }
    }
}
