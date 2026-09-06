#!/usr/bin/env python3
"""Answer-checked nested projection, batch output and Float32 CLI comparisons."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import struct
import sys
from pathlib import Path

import explore
import next_milestones
from next_milestones import command


def generate(root, rows, widths):
    root = Path(root)
    if rows <= 0 or not widths or min(widths) <= 0 or len(set(widths)) != len(widths):
        raise ValueError("positive rows and unique positive widths required")
    config = {"version": 1, "rows": rows, "widths": widths}
    manifest = root / "manifest.json"
    if manifest.exists():
        saved = json.loads(manifest.read_text())
        if saved["config"] != config:
            raise ValueError("changed corpus configuration")
        actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file() and path != manifest}
        if actual != {item["path"] for item in saved["files"]}:
            raise ValueError("changed corpus inventory")
        for item in saved["files"]:
            if explore.sha256(root / item["path"]) != item["sha256"]:
                raise ValueError("changed corpus data")
        return saved
    if root.exists() and any(root.iterdir()):
        raise ValueError("refusing foreign nonempty directory")
    root.mkdir(parents=True, exist_ok=True)
    for width in widths:
        with (root / f"width-{width}.jsonl").open("w", encoding="utf-8") as output:
            for index in range(rows):
                seed = hashlib.sha256(str(index).encode()).hexdigest() + '雪 " \\ '
                payload = (seed * ((width + len(seed) - 1) // len(seed)))[:width]
                row = {"v": index, "f": (index % 1024) * 0.25, "g": (index % 128) * 0.5,
                       "payload": payload, "nested": {"metrics": {"v": index}, "unused": payload}}
                output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    saved = {"config": config, "files": [
        {"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "sha256": explore.sha256(path)}
        for path in sorted(root.glob("*.jsonl"))]}
    explore.write_json(manifest, saved)
    return saved


def definitions(manifest):
    cases = []
    for width in manifest["config"]["widths"]:
        for kind, query, columns, ordered in [
            ("nested", "select count(*) as n, sum(nested.metrics.v) as s from it", [("n", "int"), ("s", "float")], False),
            ("direct", "select count(*) as n, sum(v) as s from it", [("n", "int"), ("s", "float")], False),
            ("add", "select sum(f + g) as s from it", [("s", "float")], False),
            ("multiply", "select sum(f * 2.0) as s from it", [("s", "float")], False),
            ("add16", "select sum(f" + " + g" * 16 + ") as s from it", [("s", "float")], False),
            ("multiply16", "select sum(f" + " * 1.01" * 16 + ") as s from it", [("s", "float")], False),
            ("projection", "select v, payload from it", [("v", "int"), ("payload", "str")], True),
            ("groups", "select v, count(*) as n from it group by v", [("v", "int"), ("n", "int")], False),
            ("small_groups", "select count(*) as n from it group by g", [("g", "float"), ("n", "int")], False),
        ]:
            cases.append({"id": f"{kind}_w{width}", "kind": kind, "path": f"width-{width}.jsonl",
                          "query": query, "columns": columns, "ordered": ordered})
    return cases


def f32(value):
    return struct.unpack("!f", struct.pack("!f", value))[0]


def expected(root, case):
    digest = explore.Digest(case)
    total = count = 0
    groups = {}
    with (Path(root) / case["path"]).open(encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            count += 1
            kind = case["kind"]
            if kind == "projection":
                digest.add([row["v"], row["payload"]])
            elif kind == "groups":
                digest.add([row["v"], 1])
            elif kind == "small_groups":
                groups[row["g"]] = groups.get(row["g"], 0) + 1
            elif kind == "nested":
                total += row["nested"]["metrics"]["v"]
            elif kind == "direct":
                total += row["v"]
            elif kind == "add":
                total += f32(f32(row["f"]) + f32(row["g"]))
            elif kind == "multiply":
                total += f32(f32(row["f"]) * 2.0)
            elif kind in ("add16", "multiply16"):
                value = f32(row["f"])
                for _ in range(16):
                    value = f32(value + f32(row["g"])) if kind == "add16" else f32(value * f32(1.01))
                total += value
            else:
                raise ValueError("unknown case")
    if case["kind"] in ("direct", "nested"):
        digest.add([count, f32(total)])
    elif case["kind"] in ("add", "multiply", "add16", "multiply16"):
        digest.add([f32(total)])
    elif case["kind"] == "small_groups":
        for key, count in groups.items():
            digest.add([key, count])
    return digest.snapshot()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--widths", nargs="+", type=int, default=[32, 2048])
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 0])
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--generate-only", action="store_true")
    args = parser.parse_args()
    if args.runs <= 0 or any(value < 0 for value in args.threads):
        raise ValueError("invalid run/thread count")
    if not args.generate_only and (args.results_dir is None or args.results_dir.exists()):
        raise ValueError("a new results directory is required")
    manifest = generate(args.data_dir, args.rows, args.widths)
    if args.generate_only:
        print(json.dumps(manifest))
        return
    binaries = {}
    for entry in args.binary:
        label, separator, value = entry.partition("=")
        path = Path(value).resolve()
        if not separator or not label or label in binaries or not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError("unique LABEL=PATH executable binaries required")
        binaries[label] = {"path": str(path), "sha256": explore.sha256(path)}
    if not binaries:
        raise ValueError("at least one binary required")
    cases = definitions(manifest)
    if args.cases:
        if set(args.cases) - {case["id"] for case in cases}:
            raise ValueError("unknown cases")
        cases = [case for case in cases if case["id"] in args.cases]
    args.results_dir.mkdir(parents=True, exist_ok=False)
    answers = {case["id"]: expected(args.data_dir, case) for case in cases}
    explore.write_json(args.results_dir / "oracles.json", answers)
    metadata = {"binaries": binaries, "argv": sys.argv, "platform": platform.platform(),
                "commit": explore.information(["git", "rev-parse", "HEAD"]), "manifest": manifest,
                "cache": "warm synthetic; complete oracle before timing", "status": "running"}
    for path in [Path(__file__), Path(explore.__file__), Path(next_milestones.__file__)]:
        metadata[path.name + "_sha256"] = explore.sha256(path)
        shutil.copyfile(path, args.results_dir / path.name)
    explore.write_json(args.results_dir / "metadata.json", metadata)
    results = []
    try:
        for case in cases:
            for threads in args.threads:
                entries = []
                for label, binary in binaries.items():
                    argv = command(binary["path"], args.data_dir, case, threads)
                    explore.run_once(argv, case, answers[case["id"]])
                    entries.append({"case": case["id"], "binary": label, "threads": threads,
                                    "argv": argv, "samples": []})
                for run in range(args.runs):
                    for entry in entries if run % 2 == 0 else reversed(entries):
                        entry["samples"].append(explore.run_once(entry["argv"], case, answers[case["id"]]))
                for entry in entries:
                    entry["summary"] = explore.summarize(entry["samples"])
                    entry["rss_sample"] = explore.run_once(entry["argv"], case, answers[case["id"]], rss=True)
                    results.append(entry)
                    print(case["id"], threads, entry["binary"], round(entry["summary"]["wall_seconds"]["mean"] * 1000, 2), flush=True)
                explore.write_json(args.results_dir / "results.json", results)
        generate(args.data_dir, args.rows, args.widths)
        for binary in binaries.values():
            if explore.sha256(binary["path"]) != binary["sha256"]:
                raise ValueError("binary changed during timing")
        metadata["status"] = "complete"
    except BaseException as error:
        metadata.update(status="failed", error=str(error))
        raise
    finally:
        explore.write_json(args.results_dir / "metadata.json", metadata)


if __name__ == "__main__":
    main()
