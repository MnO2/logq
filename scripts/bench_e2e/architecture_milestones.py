#!/usr/bin/env python3
"""Reproduce lifecycle, typed-kernel and bounded-sort experiments.

--work-dir contains data/width-{32,2048}.jsonl (100000 rows from
execution_milestones.py), spill-data/input.jsonl (generate with --prepare-sort),
and frozen candidate-final, query_lifecycle_probe-final,
expression_probe-final and external_sort_probe-final executables.
Results are exclusively created in architecture-results/.
"""

import argparse
import hashlib
import json
import sqlite3
import statistics
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path


FIXTURE_ROWS = 100_000
MIB = 1024 * 1024
PROCESS_TIMEOUT_SECONDS = 60


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(MIB), b""):
            digest.update(block)
    return digest.hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def sort_fixture_row(index):
    payload = (hashlib.sha256(str(index).encode()).hexdigest() + ' 雪 \\ " ') * 8
    return {"key": index * 17 % 10007 - 5000, "payload": payload}


def prepare_sort_fixture(work_dir):
    """Create the fixed sort corpus, refusing even an empty existing directory."""
    fixture = Path(work_dir) / "spill-data"
    fixture.mkdir(parents=True, exist_ok=False)
    with (fixture / "input.jsonl").open("x", encoding="utf-8") as output:
        for index in range(FIXTURE_ROWS):
            output.write(json.dumps(sort_fixture_row(index), ensure_ascii=False, separators=(",", ":")) + "\n")


def lifecycle_command(binary, source, query, runs, threads):
    return [str(binary), "--input", str(source), "--query", query,
            "--runs", str(runs), "--threads", str(threads)]


def kernel_command(binary, operation, active_percent, trial):
    argv = [str(binary), "--operation", operation, "--rows", "500000",
            "--chain-length", "16", "--active-percent", str(active_percent), "--nullable"]
    if trial % 2:
        argv.append("--reverse")
    return argv


def logq_sort_command(binary, source, budget_mib):
    return [str(binary), "query", "select key,payload from it order by key asc",
            "--table", f"it:jsonl={source}", "--output", "ndjson", "--threads", "1",
            "--max-memory", f"{budget_mib}MiB"]


def external_sort_command(binary, source, budget_mib, scratch, output):
    return [str(binary), str(source), "--run-bytes", str(budget_mib * MIB),
            "--fan-in", "8", "--disk-bytes", str(256 * MIB),
            "--scratch-dir", str(scratch), "--output", str(output)]


def run_probe(argv):
    """Return captured process output; callers keep JSON parsing outside timers."""
    return subprocess.run(
        argv, capture_output=True, text=True, check=True, timeout=PROCESS_TIMEOUT_SECONDS
    )


def validate_lifecycle_answer(report, expected):
    if report["answer"] != expected:
        raise ValueError(f"lifecycle answer mismatch: expected {expected!r}, got {report['answer']!r}")


def run_lifecycle(binary, sources, output_dir):
    records = []
    for source, width, threads in [(sources[0], 32, 1), (sources[1], 2048, 0)]:
        for kind, query in [
            ("count", "select count(*) as n from it"),
            ("prefix", "select v from it limit 1"),
        ]:
            expected = [{"n": FIXTURE_ROWS}] if kind == "count" else [{"v": 0}]
            for runs in (1, 10, 100):
                for trial in range(3):
                    argv = lifecycle_command(binary, source, query, runs, threads)
                    report = json.loads(run_probe(argv).stdout)
                    validate_lifecycle_answer(report, expected)
                    records.append({
                        "argv": argv, "case": kind, "width": width, "threads": threads,
                        "runs": runs, "trial": trial, "report": report,
                    })
                recent = records[-3:]
                fresh = statistics.median(
                    sum(sample["total_seconds"] for sample in record["report"]["fresh_plan"])
                    for record in recent
                )
                reused = statistics.median(
                    record["report"]["initial_prepare_seconds"]
                    + sum(sample["total_seconds"] for sample in record["report"]["reused_plan"])
                    for record in recent
                )
                print("lifecycle", source.name, kind, runs, round(fresh * 1000, 3),
                      round(reused * 1000, 3), flush=True)
                save_json(output_dir / "lifecycle.json", records)


def run_kernels(binary, output_dir):
    records = []
    for operation in ("add-columns", "multiply-constant", "add-multiply"):
        for active_percent in (1, 100):
            for trial in range(5):
                argv = kernel_command(binary, operation, active_percent, trial)
                report = json.loads(run_probe(argv).stdout)
                records.append({"argv": argv, "trial": trial, "report": report})
            print("kernel", operation, active_percent, "complete", flush=True)
            save_json(output_dir / "kernels.json", records)


@contextmanager
def sort_oracle(source_path, database_path):
    """Keep complete source values and source positions in an independent oracle."""
    database = sqlite3.connect(database_path)
    try:
        database.execute("create table records (key integer, payload text, sequence integer)")
        with Path(source_path).open(encoding="utf-8") as source:
            rows = (
                (row["key"], row["payload"], sequence)
                for sequence, line in enumerate(source)
                for row in [json.loads(line)]
            )
            database.executemany("insert into records values(?,?,?)", rows)
        database.commit()
        database.execute("create index ordering on records(key,sequence)")
        yield database
    finally:
        database.close()


def validate_sorted_output(output_path, database, *, include_sequence):
    """Check every field, stable tie position and the complete output length."""
    expected_rows = database.execute(
        "select key,payload,sequence from records order by key,sequence"
    )
    with Path(output_path).open(encoding="utf-8") as actual:
        for position, (key, payload, sequence) in enumerate(expected_rows):
            line = next(actual, None)
            if line is None:
                raise ValueError(f"sort output ended before record {position}: {output_path}")
            expected = {"key": key, "payload": payload}
            if include_sequence:
                expected["sequence"] = sequence
            if json.loads(line) != expected:
                raise ValueError(f"sort output differs from SQLite at record {position}: {output_path}")
        if next(actual, None) is not None:
            raise ValueError(f"sort output has extra records: {output_path}")


def run_sort_experiments(logq, external_sort, source, output_dir):
    records = []
    with sort_oracle(source, output_dir / "sort-oracle.sqlite") as database:
        for budget_mib in (16, 64, 256):
            argv = logq_sort_command(logq, source, budget_mib)
            output = output_dir / f"logq-sort-{budget_mib}.ndjson"
            # Include opening/closing the output and the entire CLI process;
            # complete answer validation starts after the timer stops.
            start = time.perf_counter()
            with output.open("w", encoding="utf-8") as destination:
                completed = subprocess.run(
                    argv, stdout=destination, stderr=subprocess.PIPE,
                    text=True, timeout=PROCESS_TIMEOUT_SECONDS,
                )
            elapsed = time.perf_counter() - start
            if completed.returncode == 0:
                validate_sorted_output(output, database, include_sequence=False)
            elif "Memory" not in completed.stderr and "memory" not in completed.stderr:
                raise ValueError(f"unexpected logq sort failure: {completed.stderr}")
            records.append({
                "kind": "logq", "argv": argv, "status": completed.returncode,
                "stderr": completed.stderr, "wall_seconds": elapsed,
                "max_memory_mib": budget_mib,
            })
            print("logq sort budget", budget_mib, completed.returncode,
                  round(elapsed * 1000, 2), flush=True)
            save_json(output_dir / "sort.json", records)

        scratch = output_dir / "scratch"
        scratch.mkdir()
        for budget_mib in (1, 4, 16):
            for trial in range(3):
                output = output_dir / f"spill-{budget_mib}-{trial}.ndjson"
                argv = external_sort_command(external_sort, source, budget_mib, scratch, output)
                start = time.perf_counter()
                completed = run_probe(argv)
                elapsed = time.perf_counter() - start
                report = json.loads(completed.stdout)
                if any(scratch.iterdir()):
                    raise ValueError(f"external sort left scratch files: {scratch}")
                validate_sorted_output(output, database, include_sequence=True)
                records.append({
                    "kind": "external", "argv": argv, "wall_seconds": elapsed,
                    "run_mib": budget_mib, "trial": trial,
                    "exact_sqlite_oracle": "passed; all100000 records", "report": report,
                })
            recent = records[-3:]
            sort_ms = statistics.median(
                record["report"]["report"]["sort_ns"] / 1_000_000 for record in recent
            )
            print("external sort", budget_mib, "MiB", sort_ms, "ms", flush=True)
            save_json(output_dir / "sort.json", records)


def validate_frozen_inputs(binaries, sources, metadata):
    for binary in binaries:
        if sha256(binary) != metadata["binaries"][binary.name]:
            raise ValueError(f"binary changed during experiments: {binary}")
    for source in sources:
        if sha256(source) != metadata["sources"][str(source)]["sha256"]:
            raise ValueError(f"source changed during experiments: {source}")


def run_experiments(work_dir):
    root = Path(work_dir)
    output_dir = root / "architecture-results"
    output_dir.mkdir(exist_ok=False)
    logq = root / "candidate-final"
    lifecycle = root / "query_lifecycle_probe-final"
    external_sort = root / "external_sort_probe-final"
    expression = root / "expression_probe-final"
    binaries = (logq, lifecycle, external_sort, expression)
    sources = [root / "data/width-32.jsonl", root / "data/width-2048.jsonl",
               root / "spill-data/input.jsonl"]
    script = Path(__file__)
    metadata = {
        "cache": "warm synthetic",
        "binaries": {path.name: sha256(path) for path in binaries},
        "sources": {str(path): {"sha256": sha256(path), "bytes": path.stat().st_size} for path in sources},
        "script_sha256": sha256(script),
        "status": "running",
    }
    save_json(output_dir / "metadata.json", metadata)
    (output_dir / "architecture_milestones.py").write_bytes(script.read_bytes())
    try:
        run_lifecycle(lifecycle, sources, output_dir)
        run_kernels(expression, output_dir)
        run_sort_experiments(logq, external_sort, sources[2], output_dir)
        validate_frozen_inputs(binaries, sources, metadata)
        metadata["status"] = "complete"
    except BaseException as error:
        metadata.update(status="failed", error=str(error))
        raise
    finally:
        save_json(output_dir / "metadata.json", metadata)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument(
        "--prepare-sort", action="store_true",
        help="create only the fixed 100000-row sort fixture in a new directory",
    )
    args = parser.parse_args(argv)
    root = args.work_dir.resolve()
    if args.prepare_sort:
        prepare_sort_fixture(root)
    else:
        run_experiments(root)


if __name__ == "__main__":
    main()
