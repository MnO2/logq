#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


benchmark = load("benchmark")
formatter = load("format_results")


class BenchmarkTest(unittest.TestCase):
    def test_commands_use_each_tools_native_query_language(self) -> None:
        query = json.loads((HERE / "queries.json").read_text())["queries"][0]
        data = Path("/tmp/events.jsonl")
        self.assertIn("logq query", benchmark.tool_command("logq", "logq", query, data))
        self.assertIn("read_ndjson_auto", benchmark.tool_command("duckdb", "duckdb", query, data))
        self.assertIn("JSONEachRow", benchmark.tool_command("clickhouse", "clickhouse", query, data))
        self.assertIn("| agrind", benchmark.tool_command("angle_grinder", "agrind", query, data))

    def test_formatter_combines_hyperfine_and_rss_results(self) -> None:
        queries = json.loads((HERE / "queries.json").read_text())["queries"]
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            (results / "metadata.json").write_text(
                json.dumps(
                    {
                        "date": "2026-07-11",
                        "dataset": "jsonl-100mb.jsonl",
                        "dataset_bytes": 100 * 1024**2,
                        "hardware": "Test CPU",
                        "runs": 5,
                        "warmup": 1,
                        "versions": {
                            "logq": "logq 0.1.0",
                            "angle_grinder": "ag 0.19.5",
                        },
                        "hyperfine": "hyperfine 1.20.0",
                    }
                )
            )
            (results / "rss.json").write_text(
                json.dumps(
                    {
                        query["id"]: {
                            "logq": 12.5,
                            **({} if query["id"] == "top_latency" else {"angle_grinder": 8.0}),
                        }
                        for query in queries
                    }
                )
            )
            for query in queries:
                result = {"results": [{"command": "logq", "mean": 0.1, "stddev": 0.01}]}
                if query["id"] != "top_latency":
                    result["results"].append(
                        {"command": "angle_grinder", "mean": 0.2, "stddev": 0.02}
                    )
                (results / f"{query['id']}.json").write_text(
                    json.dumps(result)
                )

            rendered = formatter.render(results)
            self.assertIn("| Full-file count | 100.0 ± 10.0 ms |", rendered)
            self.assertIn("| Full-file count | logq | 100.0 ± 10.0 ms | 12.5 MiB |", rendered)
            self.assertIn("| Top-10 latency | 100.0 ± 10.0 ms | — |", rendered)


if __name__ == "__main__":
    unittest.main()
