# Parser fuzzing

The `parse_query` target feeds arbitrary UTF-8 to the production nom parser and
allows normal parse failures. Its only assertion is that parsing never panics or
hits a sanitizer error.

Refresh the checked-in seed corpus after adding parser tests:

```sh
python3 scripts/extract_sql_corpus.py
```

Run the target with nightly Rust. A one-hour local soak is:

```sh
cargo +nightly fuzz run parse_query -- -max_total_time=3600
```

CI runs a non-blocking five-minute smoke test. Long fuzz runs remain a local or
scheduled maintenance task so a transient infrastructure failure cannot block a
pull request.
