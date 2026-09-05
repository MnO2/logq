#!/usr/bin/env python3
"""Paired, answer-checked warm-cache logq exploration; no third-party packages."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import platform
import re
import resource
import shutil
import signal
import sqlite3
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
VERSION = 1
MAX_LINE = 1024 * 1024
ROADMAP = {
    "cold_cache": {"status": "not_measured", "required_metrics": ["verified cache eviction after oracle", "physical read bytes", "page faults", "larger-than-RAM corpus"]},
    "persistent_amortization": {"status": "not_measured", "required_metrics": ["preparation", "storage bytes", "query repetitions 1/10/100", "result cache policy", "raw and prepared query latency"]},
}


def case(name, query, columns, oracle, dataset="base", ordered=False):
    return {"id": name, "query": query, "columns": columns, "oracle": oracle,
            "dataset": dataset, "ordered": ordered}


CASES = []
for key in ("low", "high", "skew"):
    CASES.append(case("group_" + key,
        f"select {key}, count(*) as n, sum(v) as total, avg(v) as mean from it group by {key}",
        [(key, "int"), ("n", "int"), ("total", "float"), ("mean", "float")],
        f"select {key},count(*),sum(v),avg(v) from rows group by {key}"))
for name, column in [("short_repeated", "sr"), ("short_unique", "su"), ("long_repeated", "lr"), ("long_unique", "lu")]:
    CASES.append(case("string_" + name, f'select count(*) as n from it where {column} like "%needle%"',
                      [("n", "int")], f"select sum({column}) from rows"))
AGG_COLUMNS = [("n", "int"), ("present", "int"), ("total", "float")]
for name, expression, sql in [("direct", "v", "v"), ("arithmetic", "v + 1", "v + 1"),
                              ("case", "case when low = 0 then v else 0 end", "case when low = 0 then v else 0 end")]:
    CASES.append(case("expression_" + name,
                      f"select count(*) as n, count(mixed) as present, sum({expression}) as total from it",
                      AGG_COLUMNS, f"select count(*),sum(present),sum({sql}) from rows"))
CASES.append(case("shape_wide", CASES[7]["query"], AGG_COLUMNS,
                  "select count(*),sum(present),sum(v) from rows", "wide"))
CASES.append(case("shape_nested", "select count(*) as n, count(mixed) as present, sum(nested.metrics.v) as total from it",
                  AGG_COLUMNS, "select count(*),sum(present),sum(nv) from rows", "wide"))
for name, limit in [("top10", " limit 10"), ("top1000", " limit 1000"), ("fullsort", "")]:
    CASES.append(case(name, "select id, v from it order by v desc, id asc" + limit,
                      [("id", "str"), ("v", "int")], "select id,v from rows order by v desc,id asc" + limit,
                      ordered=True))
for name, dataset in [("scan_shards", "shards"), ("scan_gzip", "gzip")]:
    CASES.append(case(name, CASES[7]["query"], AGG_COLUMNS,
                      "select count(*),sum(present),sum(v) from rows", dataset))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def generated_row(index, groups):
    value = (index * 17) % 10_000
    head = "needle" if index % 5 == 0 else f"agent{index % 5}"
    ident = f"r-{index:012d}"
    row = {"id": ident, "v": value, "low": index % 9, "high": index % groups,
           "skew": 0 if index % 10 or groups == 1 else 1 + (index // 10) % (groups - 1),
           "sr": head, "su": head + ident,
           "lr": head.ljust(384, "x"), "lu": (head + ident).ljust(384, "x")}
    if index % 5:
        row["mixed"] = [None, None, value, str(value), False][index % 5]
    return row


def generate(directory, rows, groups, shard_rows):
    """Create/reuse a manifest-owned corpus; never overwrite foreign data."""
    directory = Path(directory)
    if min(rows, groups, shard_rows) <= 0:
        raise ValueError("rows, groups and shard_rows must be positive")
    config = {"version": VERSION, "rows": rows, "groups": groups, "shard_rows": shard_rows}
    manifest_path = directory / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest["config"] != config:
            raise ValueError("corpus configuration changed; use a new --data-dir")
        for entry in manifest["files"]:
            if sha256(directory / entry["path"]) != entry["sha256"]:
                raise ValueError("corpus hash mismatch: " + entry["path"])
        return manifest
    if directory.exists() and any(directory.iterdir()):
        raise ValueError("refusing to overwrite a nonempty corpus without a manifest")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "shards").mkdir()
    shard = None
    try:
        with (directory / "base.jsonl").open("wb") as base, (directory / "wide.jsonl").open("wb") as wide:
            for index in range(rows):
                row = generated_row(index, min(groups, rows))
                line = (json.dumps(row, separators=(",", ":")) + "\n").encode()
                base.write(line)
                if index % shard_rows == 0:
                    if shard:
                        shard.close()
                    shard = (directory / "shards" / f"part-{index // shard_rows:06d}.jsonl").open("wb")
                shard.write(line)
                row["nested"] = {"metrics": {"v": row["v"]}, "tags": ["alpha", None, {"active": True}]}
                for field in range(24):
                    row[f"extra_{field:02d}"] = ('escaped " text ☃ ' + str(index % 13)).ljust(32, "z")
                wide.write((json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n").encode())
    finally:
        if shard:
            shard.close()
    with (directory / "base.jsonl").open("rb") as source, (directory / "base.jsonl.gz").open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=6) as output:
            shutil.copyfileobj(source, output, 1024 * 1024)
    paths = sorted(path for path in directory.rglob("*") if path.is_file())
    manifest = {"config": config, "files": [{"path": str(path.relative_to(directory)), "bytes": path.stat().st_size,
                                              "sha256": sha256(path)} for path in paths]}
    write_json(manifest_path, manifest)
    return manifest


class Digest:
    """Constant-space answer fingerprint, with ordering only where SQL promises it."""
    def __init__(self, definition):
        self.definition = definition
        self.rows = self.total = self.squares = 0
        self.ordered = hashlib.sha256()

    def add(self, row):
        if len(row) != len(self.definition["columns"]):
            raise ValueError("unexpected output column count")
        canonical = []
        for value, (_, kind) in zip(row, self.definition["columns"]):
            if kind == "int":
                if type(value) is not int:
                    raise ValueError("expected integer")
                canonical.append(value)
            elif kind == "str":
                if not isinstance(value, str):
                    raise ValueError("expected string")
                canonical.append(value)
            elif kind == "float":
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError("expected finite number")
                # Oracle sums/means use f64; the public logq result is f32.
                canonical.append(struct.pack("!f", float(value)).hex())
            else:
                raise ValueError("unknown oracle type")
        encoded = json.dumps(canonical, separators=(",", ":"), ensure_ascii=True).encode()
        hashed = hashlib.sha256(encoded).digest()
        number = int.from_bytes(hashed, "big")
        self.rows += 1
        self.total = (self.total + number) % (1 << 256)
        self.squares = (self.squares + number * number) % (1 << 256)
        self.ordered.update(hashed)

    def snapshot(self):
        result = {"rows": self.rows, "sum_sha256_mod_2_256": f"{self.total:064x}", "sum_squared_sha256_mod_2_256": f"{self.squares:064x}"}
        if self.definition["ordered"]:
            result["ordered_sha256"] = self.ordered.hexdigest()
        return result


def digest_rows(definition, rows):
    digest = Digest(definition)
    for row in rows:
        digest.add(row)
    return digest.snapshot()


def validate(source, definition, expected):
    digest = Digest(definition)
    names = [name for name, _ in definition["columns"]]
    while True:
        line = source.readline(MAX_LINE + 1)
        if not line:
            break
        if len(line) > MAX_LINE:
            raise ValueError("output line exceeds validation bound")
        row = json.loads(line)
        if not isinstance(row, dict) or set(row) != set(names):
            raise ValueError("unexpected output fields")
        digest.add([row[name] for name in names])
    actual = digest.snapshot()
    if actual != expected:
        raise ValueError(f"answer digest mismatch: expected {expected}, got {actual}")
    return actual


def oracles(directory, manifest, cases):
    """Read actual JSON independently; SQLite bounds GROUP BY/ORDER BY memory."""
    answers = {}
    with tempfile.TemporaryDirectory(prefix="logq-explore-oracle-") as temporary:
        db = sqlite3.connect(str(Path(temporary) / "oracle.sqlite"))
        try:
            db.execute("pragma temp_store=FILE")
            db.execute("pragma cache_size=-4096")
            for dataset in ("base", "wide"):
                selected = [item for item in cases if ("wide" if item["dataset"] == "wide" else "base") == dataset]
                if not selected:
                    continue
                db.execute("drop table if exists rows")
                db.execute("create table rows (id text,v integer,low integer,high integer,skew integer,present integer,sr integer,su integer,lr integer,lu integer,nv integer)")
                pending = []
                count = 0
                with (Path(directory) / f"{dataset}.jsonl").open(encoding="utf-8") as source:
                    for line in source:
                        row = json.loads(line)
                        pending.append((row["id"], row["v"], row["low"], row["high"], row["skew"],
                                        int("mixed" in row and row["mixed"] is not None),
                                        *(int("needle" in row[key]) for key in ("sr", "su", "lr", "lu")),
                                        row.get("nested", {}).get("metrics", {}).get("v")))
                        count += 1
                        if len(pending) == 2048:
                            db.executemany("insert into rows values (?,?,?,?,?,?,?,?,?,?,?)", pending)
                            pending.clear()
                db.executemany("insert into rows values (?,?,?,?,?,?,?,?,?,?,?)", pending)
                db.commit()
                if count != manifest["config"]["rows"]:
                    raise ValueError("corpus row count changed")
                for item in selected:
                    answers[item["id"]] = digest_rows(item, db.execute(item["oracle"]))
        finally:
            db.close()
    return answers


def command(binary, definition, directory, threads, memory=None):
    directory = Path(directory).resolve()
    if any(char in str(directory) for char in ",*?["):
        raise ValueError("data path contains table-list/glob syntax")
    paths = {"base": "base.jsonl", "wide": "wide.jsonl", "gzip": "base.jsonl.gz", "shards": "shards/*.jsonl"}
    args = [str(binary), "query", "--output", "ndjson", "--table", f"it:jsonl={directory / paths[definition['dataset']]}", "--threads", str(threads)]
    if memory:
        args.extend(["--max-memory", memory])
    return args + [definition["query"]]


def explain_snapshot(argv, timeout):
    explain = [argv[0], "explain", *argv[4:]]  # Remove query --output ndjson.
    try:
        result = subprocess.run(explain, capture_output=True, text=True, timeout=timeout, check=False)
        return {"argv": explain, "exit_code": result.returncode,
                "stdout": result.stdout[:64 * 1024], "stderr": result.stderr[:64 * 1024]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"argv": explain, "error": str(error)}


def run_once(argv, definition, expected, rss=False, timeout=300):
    """Time only the child; validate bounded output after stopping the clock."""
    if rss:
        argv = ["/usr/bin/time", "-l" if sys.platform == "darwin" else "-v", *argv]
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as output, tempfile.TemporaryFile(mode="w+", encoding="utf-8") as error:
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.perf_counter()
        process = subprocess.Popen(argv, stdout=output, stderr=error, start_new_session=True)
        expired = threading.Event()
        def terminate():
            # Each run owns a new session. Check the tracked child before
            # signalling its group; joined watchdogs cannot act on later runs.
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
            returncode = process.wait()  # Blocking wait avoids timeout polling's latency quantization.
            elapsed = time.perf_counter() - start
        except BaseException:
            terminate()
            process.wait()
            raise
        finally:
            watchdog.cancel()
            watchdog.join()
        if expired.is_set():
            raise ValueError(f"child exceeded {timeout} seconds")
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        error.seek(0)
        stderr = error.read(64 * 1024)
        if returncode:
            raise ValueError(f"child exited {returncode}: {stderr}")
        output.seek(0)
        validate(output, definition, expected)
    result = {"wall_seconds": elapsed, "user_cpu_seconds": after.ru_utime - before.ru_utime,
              "system_cpu_seconds": after.ru_stime - before.ru_stime}
    if rss:
        pattern = r"(\d+)\s+maximum resident set size" if sys.platform == "darwin" else r"Maximum resident set size \(kbytes\):\s+(\d+)"
        match = re.search(pattern, stderr)
        if not match:
            raise ValueError("/usr/bin/time did not report peak RSS")
        result["peak_rss_bytes"] = int(match.group(1)) * (1 if sys.platform == "darwin" else 1024)
    return result


def summarize(samples):
    return {key: {"mean": statistics.mean(row[key] for row in samples),
                  "sample_sd": statistics.stdev(row[key] for row in samples) if len(samples) > 1 else None}
            for key in ("wall_seconds", "user_cpu_seconds", "system_cpu_seconds")}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--binary", action="append", default=[], metavar="LABEL=PATH")
    result.add_argument("--allow-invalid", action="append", default=[], metavar="LABEL", help="keep explicitly named historical bugs untimed without failing the overall run")
    result.add_argument("--rows", type=int, default=100_000)
    result.add_argument("--groups", type=int, default=10_000)
    result.add_argument("--shard-rows", type=int, default=4000, help="about 4 MiB shards at the default row width")
    result.add_argument("--cases", nargs="+", choices=[item["id"] for item in CASES])
    result.add_argument("--threads", nargs="+", type=int, default=[1, 4])
    result.add_argument("--runs", type=int, default=5)
    result.add_argument("--warmup", type=int, default=1)
    result.add_argument("--max-memory")
    result.add_argument("--timeout", type=float, default=300, help="maximum seconds per subprocess")
    result.add_argument("--data-dir", type=Path, default=HERE / "data" / "explore-v1")
    result.add_argument("--results-dir", type=Path, default=HERE / "results" / "explore-v1")
    result.add_argument("--cache-state", choices=["warm"], default="warm")
    result.add_argument("--generate-only", action="store_true")
    result.add_argument("--skip-rss", action="store_true")
    result.add_argument("--validate-only", action="store_true", help="run the complete oracle and answer checks, with no timings")
    return result


def information(argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def main(argv=None):
    args = parser().parse_args(argv)
    if min(args.rows, args.groups, args.shard_rows, args.runs) <= 0 or args.warmup < 0 or any(thread < 0 for thread in args.threads) or args.timeout <= 0:
        raise ValueError("positive sizes/runs, nonnegative warmup/threads required")
    binaries = {}
    for entry in args.binary:
        label, separator, path = entry.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z0-9_-]+", label) or label in binaries:
            raise ValueError("--binary requires a unique simple LABEL=PATH")
        binary = Path(path).expanduser().resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError(f"binary is not executable: {binary}")
        binaries[label] = {"path": str(binary), "sha256": sha256(binary), "version": information([str(binary), "--version"]),
                           "build_command": None, "build_flags": "unknown; externally supplied binary"}
    if set(args.allow_invalid) - set(binaries):
        raise ValueError("--allow-invalid must name a supplied binary")
    if not binaries and not args.generate_only:
        raise ValueError("at least one --binary is required")
    manifest = generate(args.data_dir, args.rows, args.groups, args.shard_rows)
    if args.generate_only:
        print(json.dumps(manifest, indent=2))
        return 0
    if args.results_dir.exists():
        raise ValueError("results directory already exists; use a fresh directory")
    args.results_dir.mkdir(parents=True)
    cases = [item for item in CASES if not args.cases or item["id"] in args.cases]
    write_json(args.results_dir / "queries.json", cases)
    write_json(args.results_dir / "manifest.json", manifest)
    shutil.copyfile(__file__, args.results_dir / "explore.py")
    meta = {"status": "running", "started_utc": datetime.now(timezone.utc).isoformat(), "cache_state": "warm",
            "cache_note": "oracle/validation read data before timing; no cold-cache or persisted-storage claims",
            "timing_output": "temporary file; formatting/writes included, answer validation excluded",
            "rss_note": "separate successful sample; includes file-backed mmap pages, unlike --max-memory",
            "fingerprint_note": "row count plus order-independent SHA-256 sums/squared sums; order hash for ORDER BY; probabilistic digest, not a bytewise proof",
            "float_policy": "finite numeric output normalized to IEEE f32, matching current public result precision",
            "binaries": binaries, "rows": args.rows, "groups": args.groups, "threads": args.threads,
            "thread_policy": "engine limits only; 0=auto; no CPU affinity", "runs": args.runs, "warmup": args.warmup,
            "timeout": args.timeout, "max_memory": args.max_memory, "allow_invalid": args.allow_invalid, "validate_only": args.validate_only,
            "script_sha256": sha256(Path(__file__)), "query_sha256": sha256(args.results_dir / "queries.json"),
            "data_dir": str(args.data_dir.resolve()), "git_commit": information(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
            "git_status": information(["git", "-C", str(ROOT), "status", "--short"]),
            "platform": platform.platform(), "logical_cpus": os.cpu_count(), "python": sys.version,
            "rustc": information(["rustc", "--version"]), "roadmap": ROADMAP}
    write_json(args.results_dir / "metadata.json", meta)
    expected = oracles(args.data_dir, manifest, cases)
    write_json(args.results_dir / "oracle.json", expected)
    results = []
    file_sizes = {entry["path"]: entry["bytes"] for entry in manifest["files"]}
    # Validate the entire matrix BEFORE recording any timed samples.
    for item in cases:
        for threads in dict.fromkeys(args.threads):
            for label, binary in binaries.items():
                row = {"case": item["id"], "binary": label, "threads": threads, "status": "verified", "samples": [],
                       "argv": command(Path(binary["path"]), item, args.data_dir, threads, args.max_memory)}
                row["input_rows"] = args.rows
                row["logical_input_bytes"] = file_sizes["wide.jsonl" if item["dataset"] == "wide" else "base.jsonl"]
                row["physical_input_bytes"] = file_sizes["base.jsonl.gz"] if item["dataset"] == "gzip" else row["logical_input_bytes"]
                row["expected_output_rows"] = expected[item["id"]]["rows"]
                try:
                    run_once(row["argv"], item, expected[item["id"]], timeout=args.timeout)
                except (ValueError, OSError) as error:
                    row.update(status="correctness_failure", error=str(error))
                row["explain"] = explain_snapshot(row["argv"], args.timeout)
                results.append(row)
                print(f"verify {label}/{item['id']}/t{threads}: {row['status']}", flush=True)
    write_json(args.results_dir / "verification.json", results)
    with (args.results_dir / "samples.jsonl").open("w", encoding="utf-8") as raw:
        for item in cases:
            for threads in dict.fromkeys(args.threads):
                paired = [row for row in results if row["case"] == item["id"] and row["threads"] == threads and row["status"] == "verified"]
                for iteration in range(0 if args.validate_only else args.warmup + args.runs):
                    # Alternate binary order deterministically to reduce run-order bias.
                    for row in paired if iteration % 2 == 0 else reversed(paired):
                        if row["status"] not in ("verified", "ok"):
                            continue
                        try:
                            sample = run_once(row["argv"], item, expected[item["id"]], timeout=args.timeout)
                            if iteration >= args.warmup:
                                row["samples"].append(sample)
                                raw.write(json.dumps({"case": item["id"], "binary": row["binary"], "threads": threads, **sample}) + "\n")
                                raw.flush()
                            row["status"] = "ok"
                        except (ValueError, OSError) as error:
                            row.update(status="correctness_failure", error=str(error))
                            row["samples"] = []
                for row in paired:
                    if row["status"] == "ok":
                        row["summary"] = summarize(row["samples"])
                        if not args.skip_rss:
                            try:
                                row["rss_sample"] = run_once(row["argv"], item, expected[item["id"]], rss=True, timeout=args.timeout)
                            except (ValueError, OSError) as error:
                                row.update(status="measurement_failure", error=str(error))
                write_json(args.results_dir / "results.json", results)
    changed = [label for label, binary in binaries.items() if sha256(binary["path"]) != binary["sha256"]]
    if changed:
        meta["binary_changed_during_run"] = changed
    data_changed = []
    for entry in manifest["files"]:
        try:
            matches = sha256(args.data_dir / entry["path"]) == entry["sha256"]
        except OSError:
            matches = False
        if not matches:
            data_changed.append(entry["path"])
    if data_changed:
        meta["data_changed_during_run"] = data_changed
    for row in results:
        if data_changed or row["binary"] in changed:
            row.update(status="provenance_failure", error="input corpus or executable changed during measurement")
    failures = [row for row in results if row["status"] == "measurement_failure"
                or (row["status"] not in ("ok", "verified") and row["binary"] not in args.allow_invalid)]
    meta["status"] = "failed" if failures or changed or data_changed else "complete"
    meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
    write_json(args.results_dir / "metadata.json", meta)
    write_json(args.results_dir / "results.json", results)
    comparisons = []
    reference = next(iter(binaries))
    for row in results:
        if row["binary"] == reference:
            continue
        baseline = next(other for other in results if other["binary"] == reference and other["case"] == row["case"] and other["threads"] == row["threads"])
        comparison = {"case": row["case"], "threads": row["threads"], "reference": reference, "candidate": row["binary"], "status": "not_comparable"}
        if row["status"] == baseline["status"] == "ok":
            comparison.update(status="ok", wall_speedup=baseline["summary"]["wall_seconds"]["mean"] / row["summary"]["wall_seconds"]["mean"])
        else:
            comparison["reason"] = f"reference={baseline['status']}; candidate={row['status']}"
        comparisons.append(comparison)
    write_json(args.results_dir / "comparisons.json", comparisons)
    lines = ["| Case | Binary | Threads | Status | Wall mean ± SD, ms | CPU user+sys mean, ms | Peak RSS, MiB |", "| --- | --- | ---: | --- | ---: | ---: | ---: |"]
    for row in results:
        wall = cpu = rss = "—"
        if row["status"] == "ok":
            summary = row["summary"]
            stat = summary["wall_seconds"]
            sd = f"{stat['sample_sd'] * 1000:.2f}" if stat["sample_sd"] is not None else "n/a"
            wall = f"{stat['mean'] * 1000:.2f} ± {sd}"
            cpu = f"{(summary['user_cpu_seconds']['mean'] + summary['system_cpu_seconds']['mean']) * 1000:.2f}"
            if "rss_sample" in row:
                rss = f"{row['rss_sample']['peak_rss_bytes'] / 1024**2:.2f}"
        lines.append(f"| {row['case']} | {row['binary']} | {row['threads']} | {row['status']} | {wall} | {cpu} | {rss} |")
    (args.results_dir / "table.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 1 if failures or changed or data_changed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
