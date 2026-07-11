#!/usr/bin/env python3
"""Format benchmark.py output as a Markdown report fragment."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


HERE = Path(__file__).resolve().parent
TOOL_LABELS = {
    "logq": "logq",
    "duckdb": "DuckDB",
    "clickhouse": "ClickHouse local",
    "angle_grinder": "angle-grinder",
}


def milliseconds(value: float, deviation: float) -> str:
    return f"{value * 1000:.1f} ± {deviation * 1000:.1f} ms"


def render(results_dir: Path) -> str:
    metadata = json.loads((results_dir / "metadata.json").read_text())
    rss = json.loads((results_dir / "rss.json").read_text())
    queries = json.loads((HERE / "queries.json").read_text())["queries"]
    tools = list(metadata["versions"])

    lines = [
        f"Dataset: `{metadata['dataset']}` ({metadata['dataset_bytes'] / 1024**2:.1f} MiB). "
        f"Host: {metadata['hardware']}. Date: {metadata['date']}. "
        f"Hyperfine: {metadata['runs']} measured runs after "
        f"{metadata['warmup']} warmup run(s).",
        "",
        "| Query | " + " | ".join(TOOL_LABELS[tool] for tool in tools) + " |",
        "| --- | " + " | ".join("---:" for _ in tools) + " |",
    ]
    detailed = [
        "",
        "Peak RSS is a separate single warm-cache run measured with `/usr/bin/time`.",
        "",
        "| Query | Tool | Wall time | Peak RSS |",
        "| --- | --- | ---: | ---: |",
    ]
    for query in queries:
        benchmark = json.loads((results_dir / f"{query['id']}.json").read_text())
        measured = {item["command"]: item for item in benchmark["results"]}
        values = []
        for tool in tools:
            item = measured[tool]
            wall_time = milliseconds(item["mean"], item["stddev"])
            values.append(wall_time)
            rss_value = rss[query["id"]][tool]
            rss_text = f"{rss_value:.1f} MiB" if rss_value is not None else "n/a"
            detailed.append(
                f"| {query['label']} | {TOOL_LABELS[tool]} | {wall_time} | {rss_text} |"
            )
        lines.append(f"| {query['label']} | " + " | ".join(values) + " |")

    lines.extend(detailed)
    lines.extend(["", "Versions:"])
    for tool in tools:
        lines.append(f"- {TOOL_LABELS[tool]}: `{metadata['versions'][tool]}`")
    lines.append(f"- hyperfine: `{metadata['hyperfine']}`")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=HERE / "results")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    markdown = render(args.results_dir)
    if args.output:
        args.output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")


if __name__ == "__main__":
    main()
