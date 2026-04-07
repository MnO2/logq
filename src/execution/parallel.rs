use memmap2::MmapOptions;
use std::cmp;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

pub(crate) const PARALLEL_THRESHOLD: u64 = 16 * 1024 * 1024; // 16MB

pub(crate) enum ScanStrategy {
    Mmap(memmap2::Mmap),
    BufReader(Box<dyn BufRead>),
}

pub(crate) fn choose_strategy(path: &Path) -> ScanStrategy {
    let file_size = path.metadata().map(|m| m.len()).unwrap_or(0);

    if file_size < PARALLEL_THRESHOLD || file_size == 0 {
        return match File::open(path) {
            Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(64 * 1024, f))),
            Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
        };
    }

    #[cfg(target_pointer_width = "64")]
    {
        match File::open(path).and_then(|f| unsafe { MmapOptions::new().map(&f) }) {
            Ok(mmap) => return ScanStrategy::Mmap(mmap),
            Err(_) => {}
        }
    }

    match File::open(path) {
        Ok(f) => ScanStrategy::BufReader(Box::new(BufReader::with_capacity(64 * 1024, f))),
        Err(_) => ScanStrategy::BufReader(Box::new(io::Cursor::new(Vec::new()))),
    }
}

/// Split a byte slice into chunks along newline boundaries.
pub(crate) fn split_chunks(data: &[u8], num_chunks: usize) -> Vec<&[u8]> {
    if data.is_empty() || num_chunks == 0 {
        return vec![];
    }

    let chunk_size = data.len() / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;

    for i in 0..num_chunks {
        if start >= data.len() {
            break;
        }

        if i == num_chunks - 1 {
            chunks.push(&data[start..]);
            break;
        }

        let raw_end = cmp::min(start + chunk_size, data.len());

        let end = match data[raw_end..].iter().position(|&b| b == b'\n') {
            Some(pos) => raw_end + pos + 1,
            None => data.len(),
        };

        chunks.push(&data[start..end]);
        start = end;
    }

    while chunks.last().map_or(false, |c| c.is_empty()) {
        chunks.pop();
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_chunks_basic() {
        let data = b"line1\nline2\nline3\nline4\n";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
        for chunk in &chunks {
            assert!(chunk.ends_with(b"\n"), "each chunk should end with newline");
        }
    }

    #[test]
    fn test_split_chunks_empty() {
        let data = b"";
        let chunks = split_chunks(data, 4);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_zero_chunks() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_more_chunks_than_lines() {
        let data = b"line1\nline2\n";
        let chunks = split_chunks(data, 10);
        assert!(chunks.len() <= 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_chunk() {
        let data = b"line1\nline2\nline3\n";
        let chunks = split_chunks(data, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data.as_slice());
    }

    #[test]
    fn test_split_chunks_no_trailing_newline() {
        let data = b"line1\nline2";
        let chunks = split_chunks(data, 2);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, data.len());
    }

    #[test]
    fn test_split_chunks_single_line() {
        let data = b"single_line\n";
        let chunks = split_chunks(data, 4);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_scan_strategy_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("small.log");
        std::fs::write(&file_path, "hello world\n").unwrap();
        let strategy = choose_strategy(&file_path);
        assert!(matches!(strategy, ScanStrategy::BufReader(_)));
    }

    #[test]
    fn test_scan_strategy_nonexistent() {
        let strategy = choose_strategy(std::path::Path::new("/tmp/nonexistent_logq_test_file.log"));
        assert!(matches!(strategy, ScanStrategy::BufReader(_)));
    }
}
