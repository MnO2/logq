VERDICT: NEEDS_REVISION

## Summary Assessment

The design proposes sound low-risk allocation improvements (Parts 1.1-1.3) that are well-matched to the existing codebase, but the parallel execution layer (Part 2) has a critical correctness bug in the chunk splitting algorithm, underestimates the scope of changes needed to make the current execution types `Send`/`Sync`-compatible for rayon, and introduces tension between the mmap approach and the existing arena proposal.

## Critical Issues (must fix)

### C1: Chunk splitting algorithm has an out-of-bounds access bug

The `split_chunks` function on line 129 of the design searches for a newline starting at `data[raw_end..]`. When `raw_end` equals `data.len()` (which happens for the last chunk, or when `chunk_size` aligns exactly with `data.len()`), this creates an empty slice and `position()` returns `None`, falling through to `data.len()`. However, for intermediate chunks where `raw_end` is close to but less than `data.len()`, and the scan overshoots past the start of the next chunk's intended region, subsequent chunks can become empty or overlap. More critically, if `num_chunks > data.len()` (e.g., a 4-byte file with 8 chunks), `chunk_size` is 0, `raw_end` stays at 0, and the loop produces degenerate results -- the first chunk captures everything and remaining chunks are empty slices.

**Fix**: Add a guard: if `start >= data.len()`, stop producing chunks. Also handle the edge case where `num_chunks` exceeds the number of newlines in the file.

### C2: `BatchStream`, `RecordStream`, and key types are not `Send` -- rayon requires `Send`

The design proposes `chunks.par_iter().map(|chunk| { ... scanner.next_batch() ... })` but `BatchScanOperator` holds a `Box<dyn BufRead>` (not `Send`-bounded), and `BatchStream` is not `Send`. The `Formula`, `Expression`, and `FunctionRegistry` types used in pushed predicates are also not required to be `Send`. Rayon's `par_iter().map()` closure must be `Send`. This is not a minor refactor -- it requires adding `Send` bounds to trait objects throughout the batch execution layer, or restructuring to avoid sharing non-`Send` types across threads. The design estimates ~200 lines for parallel scanning but this threading constraint will touch many more files.

**Fix**: Audit all types that flow into the per-chunk closure for `Send`-safety. At minimum: `Box<dyn BufRead + Send>`, `BatchStream: Send`, `Formula: Send + Sync`, `FunctionRegistry: Send + Sync`. Verify `Arc<FunctionRegistry>` is already `Send + Sync` (it should be if `FunctionRegistry` is `Send + Sync`, but the `thread_local!` usage in `regexp.rs` and `types.rs` needs review). Document the required trait bound changes explicitly.

### C3: Mmap and arena allocation (1.3) are mutually exclusive -- design proposes both without resolving the conflict

Section 1.3 introduces a `BatchLineArena` to hold contiguous line copies. Section 2.1 says mmap eliminates "the entire read_line -> Vec<u8> copy chain" and that "combined with the arena from Section 1.3, per-line allocation is replaced by direct slices into the mmap." But if lines are direct slices into the mmap, the arena is not needed at all -- its entire purpose is to hold copies. The design needs to clarify: are these sequential phases (arena first, then replaced by mmap)? Or are they alternatives? Currently they read as both being part of the final architecture, which is contradictory.

**Fix**: Explicitly state the phasing. If the intent is "implement arena first for immediate gains, then replace it with mmap in the parallel phase," say so and mark the arena as a stepping-stone that gets removed. If mmap is the target, the arena work (~80 lines) may be wasted effort.

### C4: No error handling strategy for mmap failures on large files

The design mentions "edge cases: empty files, files with no trailing newline" but does not address: files being truncated or modified during the mmap scan (SIGBUS on Linux/macOS), mmap failures due to address space exhaustion on 32-bit targets, or permission errors that differ from `File::open` errors. Since `mmap` is `unsafe`, the design should specify how SIGBUS signals are handled (they cannot be caught as Rust errors -- they are process-fatal). The design calls logq's use case "read-only files not concurrently modified" but this is an assumption about user behavior, not a guarantee.

**Fix**: At minimum, document that mmap assumes files are not modified during query execution. Consider adding a file-size check to avoid mmap on files larger than available address space (relevant for 32-bit). For robustness, consider keeping the BufReader fallback path when mmap fails, rather than treating it as the only code path.

## Suggestions (nice to have)

### S1: Consider the BufReader buffer size more carefully

The design claims "64KB matches typical OS readahead page sizes." Modern Linux readahead is typically 128KB-256KB (32-64 pages), and macOS uses a different adaptive readahead scheme. The 10-20% improvement estimate for I/O reduction is plausible but the mechanism cited (syscall count) may not be the dominant factor -- kernel readahead means the buffer is often already populated. A quick benchmark with 64KB vs 128KB vs 256KB would validate the sweet spot claim before committing to 64KB. The change is trivial in any case.

### S2: The parallel aggregation merge for GROUP BY needs to handle non-associative aggregates

The design lists `sum += sum, count += count, min = min(min, min)` as merge operations. This works for SUM, COUNT, MIN, MAX. But the existing codebase has AVG, PERCENTILE_DISC, APPROX_PERCENTILE, FIRST, LAST, and GROUP_AS aggregates. AVG requires carrying (sum, count) pairs. PERCENTILE_DISC and APPROX_PERCENTILE (backed by t-digest) require merging digest structures. FIRST and LAST are order-dependent and cannot be trivially merged from parallel chunks without tracking which chunk came first. GROUP_AS collects all records into a list. The design's ~100-line estimate for parallel aggregation merge does not account for these.

### S3: LIMIT query coordination via AtomicUsize has a subtlety

The design proposes a global `AtomicUsize` budget for LIMIT queries. When a thread reads the budget, produces a batch, and decrements, another thread may also read the budget concurrently and overshoot. The total output can exceed the LIMIT by up to `(num_threads - 1) * batch_size` rows. For correctness, the output must be truncated by the consumer. This is not necessarily a bug (the existing `LimitStream` can sit on top), but the design should mention this explicitly, since "each thread tracks a local count" implies precision.

### S4: The 16MB threshold for parallel activation is arbitrary

The design states files under 16MB should stay sequential. This threshold depends heavily on query selectivity, number of cores, and I/O speed (SSD vs HDD vs NFS). A 16MB file with a highly selective filter might complete in microseconds sequentially, while a 16MB file with `SELECT *` and no filter could benefit from parallelism. Consider making this configurable or at least documenting the rationale beyond "large enough to amortize thread overhead."

### S5: ORDER BY with parallel sort-merge needs the existing prefix_sort infrastructure to be thread-safe

The design references "the existing `prefix_sort.rs` infrastructure." The current `PrefixSortEncoder` operates on `Vec<Record>` and is a straightforward sequential sort. For parallel use, each thread would need its own encoder and sorted buffer. The k-way merge step is new code. The estimate of ~150 lines seems reasonable for the merge, but the integration with the existing sort path (which currently happens in `execution/types.rs` Node::OrderBy) needs more detail on where the parallel decision is made.

### S6: The design does not address stdin input

The codebase supports `DataSource::Stdin` as an input source. Mmap cannot be applied to stdin. The design's activation criteria should explicitly exclude stdin (currently it only mentions "File size" which implicitly excludes stdin, but this should be stated).

### S7: Effort estimates seem optimistic

The total is listed as ~750 lines. Based on codebase analysis: the `BatchScanOperator` alone is 180 lines, `batch_tokenizer.rs` is 130 lines, `field_parser.rs` is ~200 lines. Adding mmap integration, chunk splitting, rayon coordination, aggregate merging, sort-merge, the decision layer, CLI flag parsing, and tests for all of these is more likely 1200-1500 lines of new/modified code. This is not a blocker but sets incorrect expectations.

## Verified Claims (things you confirmed are correct)

1. **DEFAULT_CAPACITY is 8KB**: Confirmed at `datasource.rs:765` -- `capacity: 8 * (1 << 10)`. The design correctly identifies this as the BufReader buffer size.

2. **tokenize_line returns a new Vec per call**: Confirmed in `batch_tokenizer.rs:6` -- `pub(crate) fn tokenize_line(line: &[u8]) -> Vec<(usize, usize)>`. The scratch buffer optimization (1.2) is sound.

3. **Batch size is 1024 lines**: Confirmed in `batch.rs:13` -- `pub const BATCH_SIZE: usize = 1024`. The allocation counts cited (1024 allocs/batch) are accurate.

4. **read_lines copies each line to a Vec<u8>**: Confirmed in `batch_scan.rs:64` -- `self.buf.trim_end().as_bytes().to_vec()`. The arena optimization (1.3) targets a real allocation pattern.

5. **Tokenizer and field parsers operate on `&[u8]` slices**: Confirmed. `tokenize_line` takes `&[u8]`, `parse_field_column` takes `&[Vec<u8>]` and field offset pairs. The claim that these can work on mmap'd memory is directionally correct, though `parse_field_column` takes `&[Vec<u8>]` (not `&[&[u8]]`), so the signature would need adjustment for zero-copy mmap slices.

6. **JSONL is excluded from the batch path**: Confirmed in `execution/types.rs:599` -- `if file_format == "jsonl" { return None; }`. The design's statement "JSONL stays sequential" matches existing behavior.

7. **The codebase already has a two-phase scan with predicate pushdown**: Confirmed in `batch_scan.rs:96-153`. The existing optimization infrastructure supports the design's approach.

8. **No existing parallelism in the codebase**: Confirmed. The only `thread_local!` usages are for regex caching and LRU caches, not for data parallelism. There is no `rayon` or other threading dependency.

9. **Field analysis infrastructure exists**: Confirmed in `field_analysis.rs`. The `extract_required_fields` and `extract_fields_from_formula` functions correctly walk the plan tree. This supports the claim that "the unchanged" components include field analysis.

10. **prefix_sort.rs exists and handles ORDER BY**: Confirmed. The `PrefixSortEncoder` provides binary-comparable key encoding for sort operations. The parallel sort-merge proposal could build on this.
