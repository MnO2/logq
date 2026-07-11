#!/usr/bin/env python3
"""Run the JSONL end-to-end suite through hyperfine and record peak RSS."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
TOOL_LABELS = {
    "logq": "logq",
    "duckdb": "DuckDB",
    "clickhouse": "ClickHouse local",
    "angle_grinder": "angle-grinder",
}


def executable(env_name: str, fallback: str | Path) -> str | None:
    configured = os.environ.get(env_name)
    if configured:
        return configured
    if isinstance(fallback, Path):
        return str(fallback) if fallback.is_file() else None
    return shutil.which(fallback)


def discover_tools() -> dict[str, str]:
    tools: dict[str, str] = {}
    candidates = {
        "logq": executable("LOGQ_BIN", ROOT / "target" / "release" / "logq"),
        "duckdb": executable("DUCKDB_BIN", "duckdb"),
        "clickhouse": executable("CLICKHOUSE_BIN", "clickhouse")
        or executable("CLICKHOUSE_BIN", "clickhouse-local"),
        "angle_grinder": executable("AGRIND_BIN", "agrind"),
    }
    for name, path in candidates.items():
        if path:
            tools[name] = path
    return tools


def shell_join(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def tool_command(tool: str, binary: str, query: dict, data: Path) -> str:
    data_string = str(data)
    if "'" in data_string:
        raise ValueError("benchmark data path cannot contain a single quote")

    statement = query[tool].format(data=data_string)
    if tool == "logq":
        command = [
            binary,
            "query",
            "--output",
            "csv",
            "--table",
            f"it:jsonl={data}",
            statement,
        ]
        return f"{shell_join(command)} > /dev/null"
    if tool == "duckdb":
        command = [binary, "-init", "/dev/null", "-csv", ":memory:", statement]
        return f"{shell_join(command)} > /dev/null"
    if tool == "clickhouse":
        mode = [] if Path(binary).name == "clickhouse-local" else ["local"]
        command = [binary, *mode, "--query", f"{statement} FORMAT CSV"]
        return f"{shell_join(command)} > /dev/null"
    if tool == "angle_grinder":
        reader = ["gzip", "-dc", str(data)] if data.suffix == ".gz" else ["cat", str(data)]
        output = "logfmt" if query["id"] == "top_latency" else "json"
        command = [binary, "--output", output, statement]
        postprocess = " | head -n 10" if query["id"] == "top_latency" else ""
        return f"{shell_join(reader)} | {shell_join(command)}{postprocess} > /dev/null"
    raise ValueError(f"unknown tool: {tool}")


def version(binary: str, tool: str) -> str:
    arguments = [binary, "--version"]
    if tool == "clickhouse" and Path(binary).name != "clickhouse-local":
        arguments = [binary, "local", "--version"]
    completed = subprocess.run(arguments, capture_output=True, text=True, check=False)
    output = completed.stdout.strip() or completed.stderr.strip()
    return output.splitlines()[0] if output else "unknown"


def hardware() -> str:
    if sys.platform == "darwin":
        completed = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        )
        cpu = completed.stdout.strip()
        if cpu:
            return cpu
    return platform.processor() or platform.machine()


def peak_rss(command: str) -> float | None:
    time_binary = Path("/usr/bin/time")
    if not time_binary.exists():
        return None
    flag = "-l" if sys.platform == "darwin" else "-v"
    completed = subprocess.run(
        [str(time_binary), flag, "/bin/sh", "-c", command],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"RSS run failed for {command}\n{completed.stderr}")

    if sys.platform == "darwin":
        match = re.search(r"(\d+)\s+maximum resident set size", completed.stderr)
        return int(match.group(1)) / 1024**2 if match else None
    match = re.search(r"Maximum resident set size \(kbytes\):\s+(\d+)", completed.stderr)
    return int(match.group(1)) / 1024 if match else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", default="100mb", help="generated data scale (default: %(default)s)")
    parser.add_argument("--gzip", action="store_true", help="benchmark the gzip dataset")
    parser.add_argument("--runs", type=int, default=5, help="hyperfine measured runs (default: %(default)s)")
    parser.add_argument("--warmup", type=int, default=1, help="hyperfine warmup runs (default: %(default)s)")
    parser.add_argument("--data-dir", type=Path, default=HERE / "data")
    parser.add_argument("--results-dir", type=Path, default=HERE / "results")
    parser.add_argument("--dry-run", action="store_true", help="print resolved commands without running them")
    args = parser.parse_args()

    suffix = ".jsonl.gz" if args.gzip else ".jsonl"
    data = (args.data_dir / f"jsonl-{args.scale}{suffix}").resolve()
    if not data.is_file():
        raise SystemExit(f"missing {data}; run scripts/bench_e2e/gen_data.py first")
    if args.runs <= 0 or args.warmup < 0:
        raise SystemExit("--runs must be positive and --warmup cannot be negative")

    if not os.environ.get("LOGQ_BIN") and not args.dry_run:
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT, check=True)

    tools = discover_tools()
    if "logq" not in tools:
        tools["logq"] = str(ROOT / "target" / "release" / "logq")
    missing = [label for name, label in TOOL_LABELS.items() if name not in tools]
    if missing:
        print(f"skipping unavailable tools: {', '.join(missing)}", file=sys.stderr)

    queries = json.loads((HERE / "queries.json").read_text())["queries"]
    commands = {
        query["id"]: {
            tool: tool_command(tool, binary, query, data)
            for tool, binary in tools.items()
        }
        for query in queries
    }
    if args.dry_run:
        print(json.dumps(commands, indent=2))
        return

    hyperfine = executable("HYPERFINE_BIN", "hyperfine")
    if not hyperfine:
        raise SystemExit(
            "hyperfine is required; install it with `brew install hyperfine` or "
            "`cargo install hyperfine --locked`"
        )

    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir / "commands.json").write_text(
        json.dumps(commands, indent=2) + "\n", encoding="utf-8"
    )
    rss: dict[str, dict[str, float | None]] = {}
    for query in queries:
        query_id = query["id"]
        hyperfine_args = [
            hyperfine,
            "--warmup",
            str(args.warmup),
            "--runs",
            str(args.runs),
            "--export-json",
            str(args.results_dir / f"{query_id}.json"),
        ]
        for tool, command in commands[query_id].items():
            hyperfine_args.extend(["--command-name", tool, command])
        subprocess.run(hyperfine_args, check=True)
        rss[query_id] = {
            tool: peak_rss(command) for tool, command in commands[query_id].items()
        }

    (args.results_dir / "rss.json").write_text(
        json.dumps(rss, indent=2) + "\n", encoding="utf-8"
    )
    metadata = {
        "date": date.today().isoformat(),
        "dataset": data.name,
        "dataset_bytes": data.stat().st_size,
        "hardware": hardware(),
        "operating_system": platform.platform(),
        "runs": args.runs,
        "warmup": args.warmup,
        "versions": {tool: version(binary, tool) for tool, binary in tools.items()},
        "hyperfine": version(hyperfine, "hyperfine"),
    }
    (args.results_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote raw results to {args.results_dir}")


if __name__ == "__main__":
    main()
