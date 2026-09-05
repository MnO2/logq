#!/usr/bin/env python3
"""Controlled JSON scanner probes; allocation instrumentation is a separate run."""
from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

from explore import ROOT, information, sha256, write_json

HERE = Path(__file__).resolve().parent
FIELDS = ["sr", "su", "lr", "lu"]
BACKENDS = ["mapped", "buffered8k", "buffered64k", "buffered1m"]
MAX_OUTPUT = 64 * 1024


def validate_result(result, argv, expected, rows, input_bytes):
    if not isinstance(result, dict):
        raise ValueError("probe must emit one JSON object")
    for name in ["input_bytes", "rows", "batches", "active_rows", "dictionary_columns", "elapsed_ns"]:
        if type(result.get(name)) is not int or result[name] < 0:
            raise ValueError(f"invalid probe counter: {name}")
    if result["elapsed_ns"] == 0:
        raise ValueError("elapsed_ns must be positive")
    pattern = argv[argv.index("--like") + 1] if "--like" in argv else None
    counted = "--allocations" in argv
    if (result.get("backend") != argv[2] or type(result.get("dictionary")) is not bool
            or result["dictionary"] != (argv[3] == "on") or result.get("like_pattern") != pattern):
        raise ValueError("probe did not report the requested backend/dictionary/predicate")
    if type(result.get("allocation_instrumentation")) is not bool or result["allocation_instrumentation"] != counted:
        raise ValueError("timing and allocation instrumentation modes do not match")
    for key in ["allocation_calls", "allocated_bytes_including_realloc"]:
        if key not in result or (counted and (type(result[key]) is not int or result[key] < 0)) or (not counted and result[key] is not None):
            raise ValueError(f"invalid allocation result: {key}")
    if result["input_bytes"] != input_bytes or result["active_rows"] != expected:
        raise ValueError("incorrect input size or matching-row count")
    # BatchFilterOperator omits wholly rejected batches. Its returned physical
    # rows can be fewer than input rows even though the scanner read all input.
    if not (0 <= result["active_rows"] <= result["rows"] <= rows):
        raise ValueError("inconsistent returned batch row count")
    if pattern is None and result["rows"] != rows:
        raise ValueError("scan did not return every input row")
    fields = [] if argv[4] == "-" else list(dict.fromkeys(argv[4].split(",")))
    if result["batches"] > result["rows"] or result["dictionary_columns"] > result["batches"] * len(fields):
        raise ValueError("inconsistent batch/dictionary counters")
    if argv[3] == "off" and result["dictionary_columns"]:
        raise ValueError("dictionary-off probe emitted dictionary columns")
    return result


def invoke(argv, expected, *, rows, input_bytes, timeout=120):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as output, tempfile.TemporaryFile(mode="w+", encoding="utf-8") as error:
        process = subprocess.Popen(argv, stdout=output, stderr=error, start_new_session=True)
        expired = threading.Event()
        def terminate():
            if process.poll() is None:
                expired.set()
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        watchdog = threading.Timer(timeout, terminate)
        watchdog.daemon = True
        watchdog.start()
        try:
            returncode = process.wait()  # No timeout polling/backoff.
        except BaseException:
            terminate()
            process.wait()
            raise
        finally:
            watchdog.cancel()
            watchdog.join()
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        error.seek(0)
        stderr = error.read(MAX_OUTPUT)
        if expired.is_set():
            raise ValueError(f"probe exceeded {timeout} seconds")
        if returncode:
            raise ValueError(f"probe exited {returncode}: {stderr}")
        output.seek(0)
        text = output.read(MAX_OUTPUT + 1)
        if len(text) > MAX_OUTPUT:
            raise ValueError("probe output exceeds its JSON-summary bound")
    result = validate_result(json.loads(text), argv, expected, rows, input_bytes)
    result["argv"] = argv
    result["user_cpu_seconds"] = after.ru_utime - before.ru_utime
    result["system_cpu_seconds"] = after.ru_stime - before.ru_stime
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-source", default="unknown", help="declared source revision + instrumentation patch identity")
    parser.add_argument("--candidate-source", default="unknown", help="declared source revision/worktree identity")
    parser.add_argument("--baseline-build-command")
    parser.add_argument("--candidate-build-command")
    parser.add_argument("--data", type=Path, required=True, help="nonempty base.jsonl generated by explore.py")
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--fields", choices=FIELDS, nargs="+", default=FIELDS)
    parser.add_argument("--modes", choices=["scan", "like"], nargs="+", default=["scan", "like"])
    parser.add_argument("--backends", choices=BACKENDS, nargs="+", default=BACKENDS)
    args = parser.parse_args(argv)
    if args.runs < 2 or args.warmup < 0 or args.timeout <= 0:
        raise ValueError("at least two runs, nonnegative warmup and positive timeout required")
    data = args.data.resolve()
    binaries = {}
    for label in ["baseline", "candidate"]:
        path = getattr(args, label).resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"probe is not executable: {path}")
        binaries[label] = {"path": str(path), "sha256": sha256(path),
                           "declared_source": getattr(args, label + "_source"),
                           "declared_build_command": getattr(args, label + "_build_command")}
    input_hash, input_bytes = sha256(data), data.stat().st_size
    definitions = [(field, mode, "mapped") for field in dict.fromkeys(args.fields)
                   for mode in dict.fromkeys(args.modes) if "mapped" in args.backends]
    if "lr" in args.fields and "scan" in args.modes:
        definitions += [("lr", "scan", backend) for backend in dict.fromkeys(args.backends) if backend != "mapped"]
    if not definitions:
        raise ValueError("no selected probes: buffered controls require --fields lr --modes scan")
    args.results_dir.mkdir(parents=True, exist_ok=False)
    snapshots = {}
    for path in [Path(__file__).resolve(), HERE / "explore.py", ROOT / "examples" / "json_scan_probe.rs"]:
        shutil.copyfile(path, args.results_dir / path.name)
        snapshots[path.name] = {"workspace_source": str(path), "sha256": sha256(args.results_dir / path.name)}
    metadata = {
        "status": "running", "started_utc": datetime.now(timezone.utc).isoformat(),
        "binaries": binaries, "data": str(data), "data_sha256": input_hash, "input_bytes": input_bytes,
        "script_sha256": snapshots["probe_json.py"]["sha256"], "source_snapshots": snapshots,
        "source_note": "workspace source snapshots are not proof of externally supplied binaries' build inputs; declarations are user-supplied",
        "runs": args.runs, "warmup": args.warmup, "timeout_seconds": args.timeout,
        "cache": "warm after complete oracle read",
        "duration": "Rust Instant: scanner/filter construction, scanning, batch drop, and scanner/reader teardown; excludes file open, buffer creation/mmap and process startup. CPU includes complete child.",
        "allocation_note": "separate instrumented run; successful alloc/alloc_zeroed/realloc requests, full realloc size counted again; not peak/live heap. Reader allocation/mmap setup is outside counting. Disabled runs retain allocator flag-check overhead.",
        "validation_scope": "count-only: exact input bytes, physical scan rows or LIKE matching count, output counters and requested modes; selected values and match identities are not independently validated",
        "rows_note": "probe rows counts physical rows in returned batches; LIKE may omit wholly rejected batches. Oracle rows is full input cardinality.",
        "git_commit": information(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "git_status": information(["git", "-C", str(ROOT), "status", "--short"]),
        "platform": platform.platform(), "logical_cpus": os.cpu_count(), "python": sys.version,
        "rustc": information(["rustc", "--version"]),
    }
    write_json(args.results_dir / "metadata.json", metadata)
    write_json(args.results_dir / "definitions.json", definitions)
    metadata["definitions_sha256"] = sha256(args.results_dir / "definitions.json")
    results = []
    try:
        rows = 0
        matches = dict.fromkeys(args.fields, 0)
        with data.open(encoding="utf-8") as source:
            for line in source:
                row = json.loads(line)
                rows += 1
                for field in matches:
                    if not isinstance(row, dict) or type(row.get(field)) is not str:
                        raise ValueError(f"probe corpus requires string field {field}")
                    matches[field] += "needle" in row[field]
        if rows == 0:
            raise ValueError("probe corpus must be nonempty")
        metadata.update(rows=rows, matches=matches)
        write_json(args.results_dir / "metadata.json", metadata)
        variants = [("baseline", "off"), ("candidate", "off"), ("candidate", "on")]
        for field, mode, backend in definitions:
            paired = []
            for label, dictionary in variants:
                command = [binaries[label]["path"], str(data), backend, dictionary, field]
                if mode == "like":
                    command += ["--like", "%needle%"]
                paired.append({"field": field, "mode": mode, "backend": backend,
                               "binary": label, "dictionary": dictionary, "argv": command, "samples": []})
            expected = matches[field] if mode == "like" else rows
            for iteration in range(args.runs + args.warmup):
                for row in paired if iteration % 2 == 0 else reversed(paired):
                    sample = invoke(row["argv"], expected, rows=rows, input_bytes=input_bytes, timeout=args.timeout)
                    if iteration >= args.warmup:
                        row["samples"].append(sample)
            for row in paired:
                row["allocations"] = invoke(row["argv"] + ["--allocations"], expected,
                                             rows=rows, input_bytes=input_bytes, timeout=args.timeout)
                values = [sample["elapsed_ns"] / 1e6 for sample in row["samples"]]
                row["mean_ms"], row["sd_ms"] = statistics.mean(values), statistics.stdev(values)
                row["user_cpu_mean_seconds"] = statistics.mean(sample["user_cpu_seconds"] for sample in row["samples"])
                row["system_cpu_mean_seconds"] = statistics.mean(sample["system_cpu_seconds"] for sample in row["samples"])
                results.append(row)
                print(f"{field}/{mode}/{backend}/{row['binary']}/dict-{row['dictionary']}: {row['mean_ms']:.2f} ms", flush=True)
            write_json(args.results_dir / "results.json", results)
        watched = [("input data", data, input_hash)]
        watched += [(label + " binary", Path(binary["path"]), binary["sha256"]) for label, binary in binaries.items()]
        watched += [(name + " source", Path(snapshot["workspace_source"]), snapshot["sha256"]) for name, snapshot in snapshots.items()]
        changed = [name for name, path, original_hash in watched if sha256(path) != original_hash]
        if changed:
            raise ValueError("changed during probe run: " + ", ".join(changed))
        metadata["status"] = "complete"
    except BaseException as error:
        metadata.update(status="failed", error=str(error) or type(error).__name__)
        raise
    finally:
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        write_json(args.results_dir / "results.json", results)
        write_json(args.results_dir / "metadata.json", metadata)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
