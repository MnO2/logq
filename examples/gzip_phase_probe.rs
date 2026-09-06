//! Decode-only and strict JSON scan controls. Compile with exactly one flate2 backend.
use flate2::read::MultiGzDecoder;
use logq::bench_internals::json_batch_scanner;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Instant;

fn decode(reader: impl Read) -> std::io::Result<u64> {
    std::io::copy(&mut MultiGzDecoder::new(reader), &mut std::io::sink())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: gzip_phase_probe PATH decode|gzip|plain FIELD,...|-".into());
    }
    let file = File::open(&args[0])?;
    let start = Instant::now();
    let (bytes, rows) = if args[1] == "decode" {
        (Some(decode(file)?), None)
    } else {
        let reader: Box<dyn Read> = match args[1].as_str() {
            "plain" => Box::new(file),
            "gzip" => Box::new(MultiGzDecoder::new(file)),
            _ => return Err("unknown mode".into()),
        };
        let fields = if args[2] == "-" {
            vec![]
        } else {
            args[2].split(',').map(str::to_owned).collect()
        };
        let mut scan = json_batch_scanner(Box::new(BufReader::with_capacity(64 * 1024, reader)), fields, false);
        let mut rows = 0;
        while let Some(batch) = scan.next_batch()? {
            rows += batch.len;
            std::hint::black_box(batch);
        }
        (None, Some(rows))
    };
    println!(
        "{}",
        serde_json::json!({"mode": args[1], "elapsed_seconds": start.elapsed().as_secs_f64(),
        "decoded_bytes": bytes, "rows": rows, "fields": args[2],
        "boundary": "open excluded; decode/strict parse and batch destruction included; no aggregation/output formatting"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    #[test]
    fn gzip_probe_counts_bytes_and_rejects_truncated_or_corrupt_data() {
        let input = b"{\"v\":1}\n";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(input).unwrap();
        let mut bytes = encoder.finish().unwrap();
        assert_eq!(decode(bytes.as_slice()).unwrap(), input.len() as u64);
        assert!(decode(&bytes[..bytes.len() - 3]).is_err());
        let footer = bytes.len() - 8;
        bytes[footer] ^= 1;
        assert!(decode(bytes.as_slice()).is_err());
    }
}
