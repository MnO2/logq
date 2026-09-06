#!/usr/bin/env python3
"""Answer-checked controls for ownership, predicates and complete file pipelines."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import platform
import shutil
import sys
from pathlib import Path

import explore


def generate(root, rows, widths, shards):
    root = Path(root)
    if rows <= 0 or not widths or not shards or min(widths + shards) <= 0:
        raise ValueError("positive rows, widths and shard counts required")
    config = {"version": 1, "rows": rows, "widths": widths, "shards": shards}
    manifest = root / "manifest.json"
    if manifest.exists():
        saved = json.loads(manifest.read_text())
        if saved["config"] != config:
            raise ValueError("changed corpus configuration")
        for item in saved["files"]:
            if explore.sha256(root / item["path"]) != item["sha256"]:
                raise ValueError("changed corpus data")
        return saved
    if root.exists() and any(root.iterdir()):
        raise ValueError("refusing foreign nonempty directory")
    root.mkdir(parents=True, exist_ok=True)
    for width in widths:
        with (root / f"width-{width}.jsonl").open("w", encoding="utf-8") as output:
            for i in range(rows):
                # Different blocks, UTF-8 and escaping; deterministic, not padded x's.
                seed = hashlib.sha256(str(i).encode()).hexdigest() + '☃ " \\ '
                payload = (seed * math.ceil(width / len(seed)))[:width]
                row = {"v": i, "payload": payload, "nested": {"metrics": {"v": i}, "unused": payload}}
                output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    source = root / f"width-{max(widths)}.jsonl"
    for count in shards:
        plain_dir, gzip_dir = root / f"shards-{count}", root / f"gzip-{count}"
        plain_dir.mkdir()
        gzip_dir.mkdir()
        per_file = math.ceil(rows / count)
        with source.open("rb") as input_file:
            for index in range(count):
                name = f"part-{index:06d}.jsonl"
                with (plain_dir / name).open("wb") as output:
                    for _ in range(per_file):
                        line = input_file.readline()
                        if not line:
                            break
                        output.write(line)
                with (plain_dir / name).open("rb") as plain, (gzip_dir / (name + ".gz")).open("wb") as raw:
                    with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as output:
                        shutil.copyfileobj(plain, output)
    saved = {"config": config, "files": [
        {"path": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": explore.sha256(p)}
        for p in sorted(root.rglob("*")) if p.is_file()]}
    explore.write_json(manifest, saved)
    return saved


def definitions(root, manifest):
    cases = []
    rows = manifest["config"]["rows"]
    for width in manifest["config"]["widths"]:
        path = f"width-{width}.jsonl"
        for kind, query, columns in [
            ("nested", "select count(*) as n, sum(nested.metrics.v) as total from it", [("n", "int"), ("total", "float")]),
            ("direct", "select count(*) as n, sum(v) as total from it", [("n", "int"), ("total", "float")]),
            ("hybrid", "select payload, v + 1 as x from it order by x desc limit 10", [("payload", "str"), ("x", "int")]),
        ]:
            cases.append({"id": f"{kind}_w{width}", "kind": kind, "path": path, "oracle_path": path,
                          "query": query, "columns": columns, "ordered": kind == "hybrid"})
        for selectivity in [1, 50, 100]:
            threshold = rows - max(1, rows * selectivity // 100)
            cases.append({"id": f"predicate_{selectivity}_w{width}", "kind": "predicate", "threshold": threshold,
                          "path": path, "oracle_path": path, "ordered": True,
                          "query": f"select payload, v from it where (cast(v as int)) >= {threshold} order by v desc limit 10",
                          "columns": [("payload", "str"), ("v", "int")]})
    for count in manifest["config"]["shards"]:
        for kind, suffix in [("shards", "jsonl"), ("gzip", "gz")]:
            cases.append({"id": f"{kind}_{count}", "kind": "direct", "path": f"{kind}-{count}/*.{suffix}",
                          "oracle_path": f"width-{max(manifest['config']['widths'])}.jsonl", "ordered": False,
                          "query": "select count(*) as n, sum(v) as total from it",
                          "columns": [("n", "int"), ("total", "float")]})
    return cases


def expected(root, case):
    count = total = 0
    top = []
    with (Path(root) / case["oracle_path"]).open(encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            if case["kind"] in ("nested", "direct"):
                count += 1
                total += row["nested"]["metrics"]["v"] if case["kind"] == "nested" else row["v"]
            elif case["kind"] == "hybrid" or row["v"] >= case["threshold"]:
                top.append((row["payload"], row["v"] + int(case["kind"] == "hybrid")))
                top.sort(key=lambda pair: pair[1], reverse=True)
                del top[10:]
    return explore.digest_rows(case, [(count, total)] if case["kind"] in ("nested", "direct") else top)


def command(binary, root, case, threads):
    root = Path(root).resolve()
    if any(c in str(root) for c in ",*?["):
        raise ValueError("ambiguous data directory")
    return [str(binary), "query", "--output", "ndjson", "--threads", str(threads),
            "--table", f"it:jsonl={root / case['path']}", case["query"]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", default=[])
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--widths", nargs="+", type=int, default=[32, 2048])
    parser.add_argument("--shards", nargs="+", type=int, default=[1, 8, 32, 125])
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 0])
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--generate-only", action="store_true")
    args = parser.parse_args()
    if args.runs <= 0 or any(t < 0 for t in args.threads):
        raise ValueError("invalid run/thread count")
    manifest = generate(args.data_dir, args.rows, args.widths, args.shards)
    if args.generate_only:
        print(json.dumps(manifest))
        return
    binaries = {}
    for entry in args.binary:
        label, separator, value = entry.partition("=")
        if not separator or not label or label in binaries:
            raise ValueError("unique LABEL=PATH binaries required")
        path = Path(value).resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError("binary not executable")
        binaries[label] = {"path": str(path), "sha256": explore.sha256(path)}
    if not binaries or args.results_dir is None:
        raise ValueError("binaries and new results directory required")
    args.results_dir.mkdir(parents=True, exist_ok=False)
    cases = definitions(args.data_dir, manifest)
    if args.cases:
        if set(args.cases) - {c["id"] for c in cases}:
            raise ValueError("unknown cases")
        cases = [c for c in cases if c["id"] in args.cases]
    answers = {c["id"]: expected(args.data_dir, c) for c in cases}
    explore.write_json(args.results_dir / "oracles.json", answers)
    metadata = {"binaries": binaries, "argv": sys.argv, "platform": platform.platform(),
                "commit": explore.information(["git", "rev-parse", "HEAD"]),
                "status": "running", "cache": "warm; full oracle before timing", "manifest": manifest,
                "script_sha256": explore.sha256(__file__), "helper_sha256": explore.sha256(explore.__file__)}
    explore.write_json(args.results_dir / "metadata.json", metadata)
    results = []
    try:
        for case in cases:
            for threads in args.threads:
                entries = []
                for label, binary in binaries.items():
                    argv = command(binary["path"], args.data_dir, case, threads)
                    explore.run_once(argv, case, answers[case["id"]])
                    entries.append({"case": case["id"], "binary": label, "threads": threads, "argv": argv,
                                    "explain": explore.explain_snapshot(argv, 300), "samples": []})
                for run in range(args.runs):
                    for entry in entries if run % 2 == 0 else reversed(entries):
                        entry["samples"].append(explore.run_once(entry["argv"], case, answers[case["id"]]))
                for entry in entries:
                    entry["summary"] = explore.summarize(entry["samples"])
                    entry["rss_sample"] = explore.run_once(entry["argv"], case, answers[case["id"]], rss=True)
                    results.append(entry)
                    print(case["id"], threads, entry["binary"], round(entry["summary"]["wall_seconds"]["mean"] * 1000, 2), flush=True)
                explore.write_json(args.results_dir / "results.json", results)
        for binary in binaries.values():
            if explore.sha256(binary["path"]) != binary["sha256"]:
                raise ValueError("binary changed during measurement")
        generate(args.data_dir, args.rows, args.widths, args.shards)
        metadata["status"] = "complete"
    except BaseException as error:
        metadata.update(status="failed", error=str(error))
        raise
    finally:
        explore.write_json(args.results_dir / "metadata.json", metadata)
        shutil.copyfile(__file__, args.results_dir / "next_milestones.py")
        shutil.copyfile(explore.__file__, args.results_dir / "explore.py")


if __name__ == "__main__":
    main()
