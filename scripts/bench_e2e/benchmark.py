#!/usr/bin/env python3
"""Run the JSONL end-to-end suite through hyperfine and record peak RSS."""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import hashlib
import io
import json
import math
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
from collections import Counter
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
        resolved = shutil.which(configured)
        if not resolved:
            raise SystemExit(f"{env_name} is not an executable: {configured}")
        return str(Path(resolved).resolve())
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


def tool_command(
    tool: str, binary: str, query: dict, data: Path,
    threads: int | None = None, redirect: bool = True,
) -> str:
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
        ]
        if threads is not None:
            command.extend(["--threads", str(threads)])
        command.append(statement)
    elif tool == "duckdb":
        if threads is not None:
            statement = f"SET threads={threads}; {statement}"
        command = [binary, "-init", "/dev/null", "-csv", ":memory:", statement]
    elif tool == "clickhouse":
        mode = [] if Path(binary).name == "clickhouse-local" else ["local"]
        command = [binary, *mode]
        if threads is not None:
            command.extend(["--max_threads", str(threads), "--max_parsing_threads", str(threads)])
        command.extend(["--query", f"{statement} FORMAT CSV"])
    elif tool == "angle_grinder":
        command = [binary, "--output", "json", statement]
        if data.suffix == ".gz":
            pipeline = f"{shell_join(['gzip', '-dc', str(data)])} | {shell_join(command)}"
            # /bin/sh does not portably support pipefail. Keep decompression
            # failures visible even when the consumer successfully counts EOF.
            command = ["/bin/bash", "-o", "pipefail", "-c", pipeline]
        else:
            text = f"{shell_join(command)} < {shlex.quote(str(data))}"
            return f"{text} > /dev/null" if redirect else text
    else:
        raise ValueError(f"unknown tool: {tool}")
    text = shell_join(command)
    return f"{text} > /dev/null" if redirect else text


def expected_results(data: Path) -> dict[str, list[tuple]]:
    """Independent, bounded-memory oracle for this suite's JSONL workload."""
    statuses: Counter = Counter()
    chrome_count = 0
    top: list[tuple[float, str]] = []
    opener = gzip.open if data.suffix == ".gz" else open
    with opener(data, "rt", encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            status, request_id, latency, agent = (
                row["status_code"], row["request_id"], row["latency"], row["user_agent"]
            )
            if (type(status) is not int or not isinstance(request_id, str)
                    or type(latency) not in (int, float) or not math.isfinite(latency)
                    or not isinstance(agent, str)):
                raise ValueError("benchmark input must use the generator's scalar field types")
            statuses[status] += 1
            chrome_count += "Chrome" in agent
            key = (-latency, request_id)
            if len(top) < 10 or key < top[-1]:
                bisect.insort(top, key)
                del top[10:]
    return {
        "full_count": [(sum(statuses.values()),)],
        "selective_filter": [(statuses[503],)],
        "group_by_status": sorted(statuses.items()),
        "top_latency": [(request_id, -latency) for latency, request_id in top],
        "user_agent_like": [(chrome_count,)],
    }


def validate_output(tool: str, query_id: str, output: str, expected: dict) -> None:
    """Reject both wrong answers and legacy CLIs that print errors with exit 0."""
    try:
        if tool == "angle_grinder":
            objects = [json.loads(line) for line in output.splitlines() if line.strip()]
            if query_id == "group_by_status":
                rows = [(int(str(row["status_code"])), int(str(row["_count"]))) for row in objects]
            else:
                rows = [(int(str(row["_count"])),) for row in objects]
        else:
            raw_rows = list(csv.reader(io.StringIO(output)))
            if query_id == "top_latency":
                rows = [(row[0], float(row[1])) for row in raw_rows if len(row) == 2]
            else:
                rows = [tuple(int(value) for value in row) for row in raw_rows]
            if len(rows) != len(raw_rows):
                raise ValueError("unexpected column count")
        if query_id == "group_by_status":
            rows.sort()
        answer = expected[query_id]
        if query_id == "top_latency":
            equal = len(rows) == len(answer) and all(
                actual[0] == wanted[0] and math.isclose(actual[1], wanted[1], rel_tol=1e-6, abs_tol=1e-7)
                for actual, wanted in zip(rows, answer)
            )
        else:
            equal = rows == answer
        if not equal:
            raise ValueError(f"expected {answer!r}, got {rows!r}")
    except (ValueError, TypeError, KeyError, IndexError) as error:
        raise ValueError(f"{tool}/{query_id} answer verification failed: {error}") from error


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(arguments: list[str]) -> str:
    completed = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True, check=True)
    return completed.stdout.strip()


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
    parser.add_argument("--threads", type=int, help="pin logq, DuckDB and ClickHouse to this positive thread limit")
    parser.add_argument("--tools", nargs="+", choices=list(TOOL_LABELS), help="run only these tools; default: all available")
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
    if args.threads is not None and args.threads <= 0:
        raise SystemExit("--threads must be positive; omit it to measure tool defaults")
    if not args.dry_run and args.results_dir.exists():
        if not args.results_dir.is_dir() or any(args.results_dir.iterdir()):
            raise SystemExit(
                f"results directory must be empty: {args.results_dir}; "
                "choose a new --results-dir to preserve previous measurements"
            )

    build_command = None
    if (not args.tools or "logq" in args.tools) and not os.environ.get("LOGQ_BIN") and not args.dry_run:
        subprocess.run(["cargo", "build", "--release", "--locked"], cwd=ROOT, check=True)
        build_command = "cargo build --release --locked"

    tools = discover_tools()
    if "logq" not in tools:
        tools["logq"] = str(ROOT / "target" / "release" / "logq")
    if args.tools:
        unavailable = set(args.tools) - tools.keys()
        if unavailable:
            raise SystemExit(f"requested tools unavailable: {', '.join(sorted(unavailable))}")
        tools = {tool: tools[tool] for tool in args.tools}
    missing = [label for name, label in TOOL_LABELS.items() if name not in tools]
    if missing:
        print(f"skipping unavailable tools: {', '.join(missing)}", file=sys.stderr)

    queries = json.loads((HERE / "queries.json").read_text())["queries"]
    queries = [query for query in queries if any(tool in query for tool in tools)]
    commands = {
        query["id"]: {
            tool: tool_command(tool, binary, query, data, threads=args.threads)
            for tool, binary in tools.items()
            if tool in query
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

    expected = expected_results(data)
    verification: dict[str, dict[str, str]] = {}
    for query in queries:
        verification[query["id"]] = {}
        for tool, binary in tools.items():
            if tool not in query:
                continue
            command = tool_command(tool, binary, query, data, threads=args.threads, redirect=False)
            completed = subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True, check=True)
            validate_output(tool, query["id"], completed.stdout, expected)
            verification[query["id"]][tool] = completed.stdout

    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir / "verification.json").write_text(json.dumps(verification, indent=2) + "\n", encoding="utf-8")
    (args.results_dir / "queries.json").write_text(json.dumps({"queries": queries}, indent=2) + "\n", encoding="utf-8")
    (args.results_dir / "commands.json").write_text(
        json.dumps(commands, indent=2) + "\n", encoding="utf-8"
    )
    rss: dict[str, dict[str, float | None]] = {}
    for query in queries:
        query_id = query["id"]
        hyperfine_args = [
            hyperfine,
            "--shell", "/bin/sh",
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
        "dataset_sha256": sha256(data),
        "dataset_rows": expected["full_count"][0][0],
        "query_sha256": sha256(args.results_dir / "queries.json"),
        "git_commit": command_output(["git", "rev-parse", "HEAD"]),
        "git_status": command_output(["git", "status", "--short"]),
        "build_command": build_command,
        "rustc": command_output(["rustc", "--version"]),
        "rustflags_environment": os.environ.get("RUSTFLAGS", ""),
        "logical_cpus": os.cpu_count(),
        "thread_limit": args.threads,
        "thread_policy": "tool defaults" if args.threads is None else "pinned; angle-grinder has no thread setting",
        "binary_sha256": {tool: sha256(Path(binary).resolve()) for tool, binary in tools.items()},
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
    from format_results import render
    table = args.results_dir / "table.md"
    table.write_text(render(args.results_dir), encoding="utf-8")
    print(f"wrote {table}")


if __name__ == "__main__":
    main()
