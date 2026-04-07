// src/execution/batch_tokenizer.rs

/// Tokenize a single line into field byte ranges (start, end).
/// Mirrors the existing LogTokenizer but works on &[u8] and returns
/// byte offsets instead of string slices.
pub(crate) fn tokenize_line(line: &[u8]) -> Vec<(usize, usize)> {
    let mut fields = Vec::with_capacity(20);
    let mut pos = 0;
    let len = line.len();

    while pos < len {
        // Skip whitespace
        while pos < len && line[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        match line[pos] {
            b'"' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'"' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            b'\'' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'\'' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            b'[' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b']' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            _ => {
                let start = pos;
                while pos < len {
                    match line[pos] {
                        b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b'[' | b']' => break,
                        _ => pos += 1,
                    }
                }
                fields.push((start, pos));
            }
        }
    }

    fields
}

/// Tokenize a single line into field byte ranges, reusing an existing Vec
/// to avoid per-line allocation. Clears `fields` before pushing.
pub(crate) fn tokenize_line_into(line: &[u8], fields: &mut Vec<(usize, usize)>) {
    fields.clear();
    let mut pos = 0;
    let len = line.len();

    while pos < len {
        // Skip whitespace
        while pos < len && line[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        match line[pos] {
            b'"' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'"' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            b'\'' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b'\'' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            b'[' => {
                let start = pos;
                pos += 1;
                while pos < len && line[pos] != b']' {
                    pos += 1;
                }
                if pos < len {
                    pos += 1;
                }
                fields.push((start, pos));
            }
            _ => {
                let start = pos;
                while pos < len {
                    match line[pos] {
                        b' ' | b'\t' | b'\n' | b'\r' | b'"' | b'\'' | b'[' | b']' => break,
                        _ => pos += 1,
                    }
                }
                fields.push((start, pos));
            }
        }
    }
}

/// Tokenize a batch of lines, returning field byte ranges for each line.
pub(crate) fn tokenize_batch_lines(lines: &[Vec<u8>]) -> Vec<Vec<(usize, usize)>> {
    lines.iter().map(|line| tokenize_line(line)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_line_simple() {
        let line = b"2024-01-01 hello world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        assert_eq!(&line[fields[0].0..fields[0].1], b"2024-01-01");
        assert_eq!(&line[fields[1].0..fields[1].1], b"hello");
        assert_eq!(&line[fields[2].0..fields[2].1], b"world");
    }

    #[test]
    fn test_tokenize_line_quoted() {
        let line = b"hello \"quoted string\" world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        assert_eq!(&line[fields[1].0..fields[1].1], b"\"quoted string\"");
    }

    #[test]
    fn test_tokenize_line_bracket() {
        let line = b"hello [bracketed] world";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 3);
        assert_eq!(&line[fields[1].0..fields[1].1], b"[bracketed]");
    }

    #[test]
    fn test_tokenize_batch() {
        let lines: Vec<Vec<u8>> = vec![b"a b c".to_vec(), b"d e f".to_vec()];
        let result = tokenize_batch_lines(&lines);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[1].len(), 3);
    }

    #[test]
    fn test_tokenize_empty_line() {
        let line = b"";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_tokenize_single_quoted() {
        let line = b"'single quoted'";
        let fields = tokenize_line(line);
        assert_eq!(fields.len(), 1);
        assert_eq!(&line[fields[0].0..fields[0].1], b"'single quoted'");
    }

    #[test]
    fn test_tokenize_line_into_reusable_buffer() {
        let mut scratch = Vec::new();

        // First use
        let line1 = b"2024-01-01 hello world";
        tokenize_line_into(line1, &mut scratch);
        assert_eq!(scratch.len(), 3);
        assert_eq!(&line1[scratch[0].0..scratch[0].1], b"2024-01-01");

        // Reuse the same scratch for a different line
        let line2 = b"foo bar";
        tokenize_line_into(line2, &mut scratch);
        assert_eq!(scratch.len(), 2);
    }
}
