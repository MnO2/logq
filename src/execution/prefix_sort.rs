use chrono;

/// Encode i32 into 4 bytes, big-endian, with sign bit flipped for unsigned sort order.
/// i32::MIN -> 0x00000000, 0 -> 0x80000000, i32::MAX -> 0xFFFFFFFF.
#[inline]
fn encode_i32(value: i32, dest: &mut [u8]) {
    let unsigned = (value as u32) ^ 0x80000000;
    dest[..4].copy_from_slice(&unsigned.to_be_bytes());
}

/// Encode bool into 1 byte: false -> 0x00, true -> 0x01.
#[inline]
fn encode_bool(value: bool, dest: &mut [u8]) {
    dest[0] = value as u8;
}

/// Encode f32 into 4 bytes for sort-preserving unsigned comparison.
/// NaN -> 0xFFFFFFFF, +Inf -> 0xFF800000 (via sign-flip of 0x7F800000),
/// -Inf -> 0x007FFFFF (via bit-flip of 0xFF800000).
/// Positive values: flip sign bit. Negative values: flip all bits.
#[inline]
fn encode_f32_to(value: f32, dest: &mut [u8]) {
    let encoded = if value.is_nan() {
        u32::MAX
    } else {
        let bits = value.to_bits();
        if bits & 0x80000000 != 0 {
            !bits // negative: flip all bits
        } else {
            bits ^ 0x80000000 // non-negative: flip sign bit
        }
    };
    dest[..4].copy_from_slice(&encoded.to_be_bytes());
}

/// Encode DateTime as i64 epoch seconds, sign-flipped big-endian.
#[inline]
fn encode_datetime(dt: &chrono::DateTime<chrono::FixedOffset>, dest: &mut [u8]) {
    let secs = dt.timestamp();
    let unsigned = (secs as u64) ^ 0x8000000000000000;
    dest[..8].copy_from_slice(&unsigned.to_be_bytes());
}

/// Encode a string into a fixed-width prefix. Copies the first min(len, prefix_len)
/// bytes of the UTF-8 representation and zero-pads the rest.
#[inline]
fn encode_string_prefix(s: &str, dest: &mut [u8], prefix_len: usize) {
    let bytes = s.as_bytes();
    let copy_len = bytes.len().min(prefix_len);
    dest[..copy_len].copy_from_slice(&bytes[..copy_len]);
    if copy_len < prefix_len {
        dest[copy_len..prefix_len].fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- encode_i32 tests ----

    #[test]
    fn test_encode_i32_ordering() {
        let mut buf_neg = [0u8; 4];
        let mut buf_zero = [0u8; 4];
        let mut buf_pos = [0u8; 4];
        let mut buf_max = [0u8; 4];
        let mut buf_min = [0u8; 4];

        encode_i32(i32::MIN, &mut buf_min);
        encode_i32(-1, &mut buf_neg);
        encode_i32(0, &mut buf_zero);
        encode_i32(1, &mut buf_pos);
        encode_i32(i32::MAX, &mut buf_max);

        assert!(buf_min < buf_neg);
        assert!(buf_neg < buf_zero);
        assert!(buf_zero < buf_pos);
        assert!(buf_pos < buf_max);
    }

    #[test]
    fn test_encode_i32_specific_values() {
        let mut buf = [0u8; 4];
        encode_i32(i32::MIN, &mut buf);
        assert_eq!(buf, [0x00, 0x00, 0x00, 0x00]);

        encode_i32(0, &mut buf);
        assert_eq!(buf, [0x80, 0x00, 0x00, 0x00]);

        encode_i32(i32::MAX, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    // ---- encode_bool tests ----

    #[test]
    fn test_encode_bool() {
        let mut buf_f = [0u8; 1];
        let mut buf_t = [0u8; 1];
        encode_bool(false, &mut buf_f);
        encode_bool(true, &mut buf_t);
        assert!(buf_f < buf_t);
        assert_eq!(buf_f[0], 0x00);
        assert_eq!(buf_t[0], 0x01);
    }

    // ---- encode_f32_to tests ----

    #[test]
    fn test_encode_f32_ordering() {
        let cases: Vec<f32> = vec![
            f32::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1000.0,
            f32::INFINITY,
            f32::NAN,
        ];
        let mut prev = [0u8; 4];
        encode_f32_to(cases[0], &mut prev);
        for &val in &cases[1..] {
            let mut curr = [0u8; 4];
            encode_f32_to(val, &mut curr);
            assert!(
                prev <= curr,
                "failed: prev {:?} should <= curr {:?} for value {}",
                prev,
                curr,
                val
            );
            prev = curr;
        }
    }

    #[test]
    fn test_encode_f32_special_values() {
        let mut buf = [0u8; 4];

        encode_f32_to(f32::NEG_INFINITY, &mut buf);
        assert_eq!(buf, [0x00, 0x7F, 0xFF, 0xFF]);

        encode_f32_to(0.0, &mut buf);
        assert_eq!(buf, [0x80, 0x00, 0x00, 0x00]);

        encode_f32_to(f32::NAN, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_f32_neg_nan() {
        let neg_nan = f32::from_bits(0xFFC00000);
        let mut buf = [0u8; 4];
        encode_f32_to(neg_nan, &mut buf);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    // ---- encode_datetime tests ----

    #[test]
    fn test_encode_datetime_ordering() {
        use chrono::{FixedOffset, TimeZone};

        let utc = FixedOffset::east(0);

        let t0 = utc.ymd(1960, 1, 1).and_hms(0, 0, 0);
        let t1 = utc.ymd(2020, 1, 1).and_hms(0, 0, 0);
        let t2 = utc.ymd(2025, 6, 15).and_hms(12, 0, 0);
        let t3 = utc.ymd(2026, 1, 1).and_hms(0, 0, 0);

        let mut buf0 = [0u8; 8];
        let mut buf1 = [0u8; 8];
        let mut buf2 = [0u8; 8];
        let mut buf3 = [0u8; 8];

        encode_datetime(&t0, &mut buf0);
        encode_datetime(&t1, &mut buf1);
        encode_datetime(&t2, &mut buf2);
        encode_datetime(&t3, &mut buf3);

        assert!(
            buf0 < buf1,
            "pre-epoch date should sort before post-epoch date"
        );
        assert!(buf1 < buf2);
        assert!(buf2 < buf3);
    }

    // ---- encode_string_prefix tests ----

    #[test]
    fn test_encode_string_prefix_basic() {
        let mut buf = [0xFFu8; 16];
        encode_string_prefix("hello", &mut buf, 16);
        assert_eq!(&buf[..5], b"hello");
        assert_eq!(&buf[5..], &[0u8; 11]);
    }

    #[test]
    fn test_encode_string_prefix_truncation() {
        let long = "abcdefghijklmnopqrstuvwxyz";
        let mut buf = [0u8; 16];
        encode_string_prefix(long, &mut buf, 16);
        assert_eq!(&buf, b"abcdefghijklmnop");
    }

    #[test]
    fn test_encode_string_prefix_ordering() {
        let mut buf_a = [0u8; 16];
        let mut buf_b = [0u8; 16];
        encode_string_prefix("apple", &mut buf_a, 16);
        encode_string_prefix("banana", &mut buf_b, 16);
        assert!(buf_a < buf_b);
    }

    #[test]
    fn test_encode_string_prefix_empty() {
        let mut buf = [0xFFu8; 16];
        encode_string_prefix("", &mut buf, 16);
        assert_eq!(&buf, &[0u8; 16]);
    }
}
