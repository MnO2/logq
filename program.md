# autoresearch
This is an experiment to have the LLM do its own research on logq component performance, using an **AlphaCode-inspired search strategy**: instead of trying one optimization idea at a time sequentially, we generate a massive diverse set of candidate optimizations upfront, filter cheaply, cluster by category, and then validate the most promising candidate from each cluster — maximizing the probability of finding real wins.
## Setup
To set up a new experiment, work with the user to:
1. **Agree on a run tag**: propose a branch name based on today's date (e.g. `autoresearch/parser-apr5`). The branch `autoresearch/<tag>` must not already exist — this is a fresh run.
2. **Create the branch**: `git checkout -b autoresearch/<tag>` from current master.
3. **Understand the logq architecture**: Read the source in `src/` to understand logq's components:
   - **Parser** (`src/syntax/`): nom-based parser producing AST nodes
   - **Logical Planner** (`src/logical/`): AST → logical plan tree
   - **Physical Executor** (`src/execution/`): Stream-based pull execution with `RecordStream` trait
   - **Common Types** (`src/common/`): `Value` enum, type definitions shared across layers
   - **UDFs** (`src/functions/`): User-defined functions (string, math, date)
   * Run the unit tests: `cargo test`. If a test is flaky, run it 3 times — if it fails all 3, it's a real failure.
   * Run all the Criterion benchmarks: `cargo bench`
4. **Confirm and go**: Confirm setup looks good.
Once you get confirmation, kick off the experimentation.
## Experimentation
**What you CAN do:**
- Modify code in `src/` — parser, logical planner, executor, common types, and UDFs.
- Optimize for the following Criterion benchmark groups:
   - `cargo bench --bench bench_parser` (nom parsing throughput)
   - `cargo bench --bench bench_execution` (end-to-end query execution and isolated operator performance)
   - `cargo bench --bench bench_datasource` (log format parsing: ELB, ALB, S3, Squid, JSONL)
   - `cargo bench --bench bench_udf` (UDF call overhead)
**What you CANNOT do:**
- Install new packages or add dependencies.
- Delete or break the unit tests.
**The goal is simple: get the shortest time/iter, or equally highest iters/s while respecting the correctness**
**Simplicity criterion**: All else being equal, simpler is better. A small improvement that adds ugly complexity is not worth it. Conversely, removing something and getting equal or better results is a great outcome — that's a simplification win.
**Important**: You should behave like a top performance engineer. Think from first principles on how to optimize each component of logq — the parser, planner, executor, datasource layer, and UDFs.
---
## The AlphaCode-Inspired Experiment Strategy
The core insight from DeepMind's AlphaCode: **searching broadly over a massive, diverse candidate space and then selecting winners vastly outperforms searching sequentially with a single "best guess" at each step.** AlphaCode generates ~1 million candidate programs per problem, filters them with test cases, clusters the survivors by output behavior to maximize diversity, and picks one submission per cluster. We adapt this pipeline for performance optimization.
### Overview of the 5-Phase Pipeline
```
Phase 1: DEEP ANALYSIS          — Understand the codebase deeply (≈ AlphaCode pre-training)
    ↓
Phase 2: MASSIVE IDEATION       — Generate 50-100+ candidate optimization hypotheses (≈ large-scale sampling)
    ↓
Phase 3: CHEAP FILTERING        — Analytically reject unsound/redundant ideas (≈ test-case filtering)
    ↓
Phase 4: CLUSTERING & SELECTION — Group by optimization category, pick best per cluster (≈ output clustering)
    ↓
Phase 5: VALIDATE & COMBINE     — Benchmark winners, combine orthogonal successes (≈ final submission)
```
---
### Phase 1: Deep Analysis (≈ Pre-training)
**Goal**: Build a thorough mental model of the code before generating any ideas. AlphaCode trains on massive code corpora before generating solutions — analogously, you must deeply understand the code *before* proposing optimizations.
1. **Read every file** in `src/`. Don't skim — read for structure, data flow, hot paths, and implicit assumptions.
2. **Understand the benchmark workloads**: Read each benchmark file in `benches/` to understand what operations are being measured, what data sizes and distributions are tested, and what the critical path is.
3. **Identify the hot functions**: Reason about which functions dominate benchmark runtime based on the workload structure. Think about: parser combinator overhead, memory allocation patterns, iterator chains, string processing, and data structure choices.
4. **Map the abstraction layers**: Understand how the parser feeds into the logical planner, which feeds into the physical executor. Often the optimization surface is in the interaction between layers, not in any single function.
5. **Note existing optimizations**: Identify what the code already does well. Don't re-invent what's already there.
**Output**: A written analysis document (in your context) listing:
- The top 5-10 hottest code paths per benchmark group
- The current bottleneck type for each (compute-bound, memory-bound, allocation-bound, parsing-bound)
- Existing optimizations already in place
- The "optimization surface" — what levers exist to improve each path
**Establish baseline**: Run ALL benchmarks to record baseline numbers before any modifications.
---
### Phase 2: Massive Ideation (≈ Large-Scale Sampling)
**Goal**: Generate a large, diverse set of candidate optimization hypotheses. AlphaCode's key insight is that the probability of finding a correct solution scales with the *number and diversity* of candidates, not with the quality of any single candidate. **Quantity and diversity beat quality of individual attempts.**
**Generate at least 50 hypotheses** (target 100 if the codebase is rich enough). For each hypothesis, write one line:
```
ID | Category    | Target_Benchmark | Hypothesis                                    | Expected_Impact          | Confidence
H1 | PARSING     | parser           | Use &str slices instead of String allocs in AST| -30% parse time          | HIGH
H2 | MEMORY      | execution        | Pre-allocate record buffers in RecordStream    | -15% allocation overhead | MEDIUM
H3 | DATASOURCE  | datasource       | Compile regex patterns once at startup         | -40% ELB parse time      | HIGH
H4 | ALGORITHM   | execution        | Use hash-based grouping instead of sort-based  | -25% GROUP BY time       | MEDIUM
H5 | UDF         | udf              | Avoid cloning Value args in UDF dispatch       | -20% UDF call overhead   | HIGH
...
```
**Ideation strategies** (use ALL of these — diversity is the point):
1. **Allocation reduction**: Avoid unnecessary String/Vec allocations, use borrowing, arena allocators, Cow<str>, smallvec
2. **Parser optimization**: nom combinator rewriting, avoiding backtracking, zero-copy parsing, pre-compiled regex
3. **Iterator/stream efficiency**: Fuse iterator chains, avoid unnecessary collect(), lazy evaluation, batch processing
4. **Data structure choices**: HashMap vs BTreeMap, Vec vs SmallVec, interning strings, flat vs nested structures
5. **Type-level optimization**: Monomorphization, avoid dynamic dispatch (dyn Trait → enum dispatch), inline hot paths
6. **Memory layout**: Struct-of-arrays vs array-of-structs for record batches, cache-friendly iteration
7. **String processing**: Avoid regex where simple parsing suffices, use memchr for scanning, SIMD string operations
8. **Algorithmic**: Better sorting for ORDER BY, hash-based GROUP BY, short-circuit evaluation for WHERE
9. **Serialization/deserialization**: Zero-copy log line parsing, lazy field extraction, columnar vs row-based processing
10. **Simplification/removal**: Remove unnecessary checks, dead code, over-general abstractions
11. **Combining known tricks**: Pair ideas from different categories (e.g., zero-copy parsing + pre-compiled regex)
12. **Reading-inspired**: Look at similar systems (DuckDB, DataFusion, ClickHouse) for ideas
**Critical rule**: Do NOT self-censor during ideation. Write down ideas even if you think they "probably won't work." AlphaCode generates millions of candidates knowing most are wrong — the value is in the long tail. A 5% hit rate on 100 hypotheses yields 5 wins; a 100% hit rate on 1 hypothesis yields at most 1.
---
### Phase 3: Cheap Filtering (≈ Test-Case Filtering)
**Goal**: Eliminate clearly bad candidates without spending expensive benchmark time. AlphaCode filters ~1M candidates down to ~tens of thousands using cheap example test cases. We analogously filter using analytical reasoning and fast compilation checks.
**For each hypothesis, apply these filters in order (cheapest first):**
1. **Theoretical soundness** (cost: thinking time only)
   - Does this violate correctness? (e.g., changing parser semantics, losing precision) → REJECT
   - Is this provably a no-op under the compiler? (e.g., LLVM already optimizes this with -O2) → REJECT
   - Has this *exact* idea already been tried and measured as neutral in prior runs? → REJECT
2. **Compiler/architecture analysis** (cost: thinking time only)
   - Will `--release` mode (-O2 + LTO) already do this transformation? Many "optimizations" (manual inlining, strength reduction, loop unrolling of small loops) are already handled by LLVM.
   - Is the target code path actually hot? If it runs <1% of benchmark time, even a 50% improvement is noise. → REJECT or DEPRIORITIZE
3. **Quick compilation test** (cost: ~1-2 minutes)
   - For ideas that require non-trivial code changes, write the code and check it compiles: `cargo build --release`
   - If it doesn't compile after a quick fix attempt → REJECT
4. **Redundancy with other hypotheses**
   - If two hypotheses attack the exact same code path with the same technique, keep only the stronger variant → MERGE or REJECT duplicate
**Output**: A filtered list. Record the filter-out reason for rejected hypotheses (useful for learning). Target: keep 30-60% of hypotheses (if you're keeping >80%, you weren't creative enough in Phase 2; if <20%, you were too aggressive in filtering).
---
### Phase 4: Clustering & Selection (≈ Output Clustering)
**Goal**: Group surviving hypotheses by category to ensure diversity, then pick the single most promising candidate from each cluster for benchmarking. AlphaCode clusters programs by their *output behavior* on test inputs — programs that produce the same output are redundant. We cluster by *optimization category and target* — ideas that modify the same code path are partially redundant.
**Clustering dimensions**:
- **Target benchmark**: Which benchmark group(s) does this primarily affect?
- **Optimization category**: PARSING, MEMORY, ALGORITHM, DATASOURCE, UDF, SIMPLIFICATION, etc.
- **Code location**: Which file(s) and function(s) are modified?
**Two hypotheses are in the same cluster if they share the same (target, category, code location) triple.** Within each cluster, rank by `expected_impact × confidence` and select the top candidate.
**Selection rule**: Pick exactly ONE hypothesis per cluster for the first pass. This is the AlphaCode insight — **maximizing diversity of attempts beats deepening any single direction**. You can always come back to the backup candidates in later rounds.
**Ordering**: Rank clusters by the selected hypothesis's `expected_impact × confidence`. Execute highest-ranked clusters first.
---
### Phase 5: Validate & Combine (≈ Final Submission)
**Goal**: Benchmark the selected candidates, keep the winners, and then try combining orthogonal wins.
The experiment runs on a dedicated branch (e.g. `autoresearch/parser-apr5`).
#### 5a. Single-Hypothesis Validation Loop
For each selected hypothesis (in priority order from Phase 4):
1. Check git state: confirm you're on the right branch/commit.
2. Implement the optimization by modifying code in `src/`.
3. Commit the change. Issue the command separately so the permission tool can catch it.
4. Run the relevant benchmarks: `cargo bench --bench <target> 2>&1 | tee run.log` or for all benchmarks: `cargo bench 2>&1 | tee run.log`
5. Read the results from the benchmark output.
6. If the run crashed, check the output for diagnostics. Quick-fix if trivial, else skip.
7. **KEEP** if time/iter decreased while correctness holds (unit tests pass via `cargo test`). The commit message must contain: hypothesis ID, cluster ID, benchmark improvement numbers, and the category tag.
8. **REVERT** if neutral or regression: `git revert` or `git reset` back to where you started.
9. **Record the result** regardless of outcome in the running log in CHANGELOG.md.
#### 5b. Combination Phase (Orthogonal Wins)
After completing one pass through all clusters, you will have a set of **kept** optimizations (winners from different clusters). Because they target different code paths and categories, they are likely **orthogonal** — their benefits should stack.
1. Starting from the branch tip (which has all kept optimizations stacked), run the FULL benchmark suite to measure the combined effect.
2. If any negative interaction between optimizations is detected (combined result worse than sum of parts), bisect to find the conflicting pair and remove the weaker one.
#### 5c. Second-Pass: Backup Candidates
After the combination phase, go back to clusters where the first-choice hypothesis was REVERTED. Try the **backup** candidate from that cluster. This is another AlphaCode insight — when your top candidate from a cluster fails, the next-most-promising one from the same cluster may still succeed because it takes a slightly different approach to the same problem.
#### 5d. Subsequent Rounds
After exhausting all clusters from the initial ideation, start a **new round**:
1. Re-read the (now modified) code — your successful optimizations have changed the performance landscape.
2. Generate a new batch of 30-50 hypotheses informed by what you've learned (what categories yielded wins, what the new bottlenecks are).
3. Repeat Phases 3-5.
This mirrors how AlphaCode could be run iteratively: each round generates a fresh diverse sample conditioned on what's been learned.
---
### Strategy Principles (from AlphaCode)
1. **Sample massively, filter cheaply**: The cost of generating an idea is near-zero (thinking time). The cost of benchmarking is high (~5-10 min per experiment). Therefore, spend MORE time in Phases 2-4 (ideation, filtering, clustering) and LESS time per-experiment in Phase 5. A well-filtered, well-clustered set of 20 experiments beats 20 randomly-ordered sequential attempts.
2. **Diversity over depth**: It is better to try 1 idea from each of 20 different categories than 20 variations of the same idea. The marginal value of the Nth variant within a cluster drops rapidly, while the first attempt in a new cluster has high information value.
3. **Test-case generation**: AlphaCode generates additional test cases to improve filtering. Analogously, when evaluating an optimization hypothesis analytically, *generate your own micro-reasoning*: "If the parser processes 1000 tokens and each String allocation costs 50ns, switching to &str slices saves at most 50us = X% of the Y ms benchmark time." This analytical filtering is your "generated test case."
4. **Loss is a poor proxy**: AlphaCode found that validation loss poorly predicts solve rate. Analogously, don't over-index on *theoretical* appeal of an idea. The only ground truth is the benchmark. Some ugly hacks win; some elegant ideas are neutral. Let the data decide.
5. **Scaling law**: AlphaCode's solve rate scales log-linearly with the number of samples. Similarly, your odds of finding optimizations scale with the number of *diverse* hypotheses you evaluate. If you're stuck, the answer is almost always "generate more diverse candidates" — not "think harder about the same idea."
---
### Timeout & Crash Handling
**Timeout**: Each individual experiment (Phase 5a, one hypothesis) should take ~15 minutes for implementation + benchmarking. If a run exceeds 30 minutes, kill it and treat as a failure. The overall session has no timeout — keep going indefinitely.
**Crashes**: If a run crashes (OOM, bug, etc.), use your judgment: If it's a typo or trivial fix, fix and re-run. If the idea is fundamentally broken, skip it, log "CRASH" in the results table, and move to the next hypothesis.
**NEVER STOP**: Once the experiment loop has begun (after the initial setup), do NOT pause to ask the human if you should continue. Do NOT ask "should I keep going?" or "is this a good stopping point?". The human might be asleep, or gone from a computer and expects you to continue working *indefinitely* until you are manually stopped. You are autonomous. If you exhaust all clusters, start a new round of ideation. The loop runs until the human interrupts you, period.
As an example use case, a user might leave you running while they sleep. With the AlphaCode strategy, the first ~30 minutes are spent on deep analysis and ideation (Phases 1-4), and then you execute ~3-4 experiments per hour in Phase 5. Over an 8-hour sleep, that's 25-30 diverse, well-targeted experiments plus combination attempts — each one chosen to maximize the probability of a hit rather than tried at random.
---
## Long-Running Session Guidelines
These guidelines improve reliability and recoverability for multi-hour or overnight autonomous runs. They are adapted from patterns proven in multi-day agentic scientific computing workflows.
### Progress File (CHANGELOG.md)
Maintain a `CHANGELOG.md` file in the project root as **portable long-term memory** — your lab notebook across sessions. This is critical because context may be compressed or lost in long sessions, and future sessions need to know what happened.
**After every experiment (kept or reverted), update CHANGELOG.md with:**
- Current status and what you're working on next
- Completed tasks with benchmark results
- **Failed approaches and WHY they didn't work** — without these, successive sessions will re-attempt the same dead ends (e.g., "Tried pre-compiling all regex in ELB parser — already compiled once via lazy_static; REVERTED")
- Performance tables at key checkpoints
- Known limitations or open questions
**Format each entry with enough context to be useful in isolation:**
```
## Exp42: Zero-copy parsing in ELB datasource (REVERTED)
- Hypothesis: Use &str slices referencing the original log line instead of allocating new Strings per field
- Result: +3% regression. Lifetime constraints forced extra copies at the RecordStream boundary, negating the savings.
- Lesson: Zero-copy only helps if the borrow can propagate through the full pipeline.
```
**At session start**, always read `CHANGELOG.md` to resume from where the last session left off.
### Test Oracle & Regression Prevention
Every long-running experiment loop needs a way to know whether it's making progress and not breaking things. **The unit tests are your oracle.**
**Rules:**
- Run `cargo test` before every commit. Never commit code that breaks existing passing tests.
- If you discover a new invariant during experimentation, consider adding a lightweight test or assertion for it — this prevents future experiments from silently violating it.
- When an experiment produces unexpected results (better OR worse than predicted), investigate before moving on. Surprising results often indicate a measurement error or an incorrect assumption that could invalidate future experiments.
### Git as Coordination & Recovery
Use git commits as your checkpoint/recovery mechanism. The human may check `git log --oneline` from another terminal to monitor your progress without interrupting you. Frequent commits make progress visible and recoverable.
**Rules:**
- **Commit after every meaningful unit of work** — each experiment (kept or reverted-then-restored) should be a commit. This gives a recoverable history if something goes wrong.
- **Commit messages must be descriptive**: Include the experiment ID, hypothesis, benchmark delta, and KEPT/REVERTED status. Example:
  ```
  [autoresearch] Exp17 H23 (Cluster D): Zero-copy ELB field parsing — KEPT (-22% datasource ELB parse)
  ```
- **Before reverting**, always commit the current state first so the attempt is preserved in history.
- **Never amend a commit that contains a KEPT experiment** — create new commits instead, so the history of each experiment is independently recoverable.
### Living Instructions
This `program.md` file is a living document. As you work through experiments and learn things about the codebase, you may update this file to:
- Add newly discovered constraints or gotchas (e.g., "RecordStream trait objects prevent monomorphization — changes to the trait boundary affect all executor benchmarks")
- Refine the ideation categories based on what's actually working
- Record architectural insights that would help future rounds of ideation
**Do NOT delete existing strategy or rules** — append new learnings in a dedicated section below.
### Anti-Laziness: Completion Verification
Long-running agents can drift toward premature completion — claiming a task is done when there's still work to be done. To counter this:
1. **After each full pass through all clusters**, re-read `CHANGELOG.md` and this file. Ask yourself: "Have I truly exhausted the current round? Are there backup candidates untried? Are there clusters I skipped?"
2. **After combination phase**, re-run the full benchmark suite and compare against the original baseline (not just the previous experiment). Report cumulative improvement.
3. **Between rounds**, generate a fresh analysis of the new performance landscape before ideating. The bottlenecks have shifted — yesterday's hot path may no longer be hot.
4. **If you find yourself writing "no more ideas"** — that's a signal to try a fundamentally different analysis approach (e.g., read the benchmark code more carefully, look at similar systems like DataFusion or DuckDB, think about the problem from the memory allocator's perspective instead of the compute perspective).
### Session Execution Tips
- Run inside `tmux` so the session survives terminal disconnects. The human can `tmux attach` to check in at any time.
- Redirect benchmark output to files (`cargo bench 2>&1 | tee run.log`) and read results after completion.
- If the session is approaching context limits, write a comprehensive status update to `CHANGELOG.md` before context compression occurs, so critical information survives.
- Periodically (every ~5 experiments) re-read `CHANGELOG.md` to ensure your mental model is consistent with recorded results. Context compression can cause you to forget earlier findings.
