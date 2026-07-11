#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(query) = std::str::from_utf8(data) {
        // Syntax errors are expected. A panic, abort, or memory error is not.
        let _ = logq::bench_internals::parse_query(query);
    }
});
