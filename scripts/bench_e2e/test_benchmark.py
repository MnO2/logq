#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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
        self.assertIn("agrind", benchmark.tool_command("angle_grinder", "agrind", query, data))

    def test_pinned_threads_are_applied_to_each_configurable_engine(self) -> None:
        query = json.loads((HERE / "queries.json").read_text())["queries"][0]
        data = Path("/tmp/events.jsonl")
        self.assertIn("--threads 2", benchmark.tool_command("logq", "logq", query, data, threads=2))
        self.assertIn("SET threads=2", benchmark.tool_command("duckdb", "duckdb", query, data, threads=2))
        command = benchmark.tool_command("clickhouse", "clickhouse", query, data, threads=2)
        self.assertIn("--max_threads 2", command)
        self.assertIn("--max_parsing_threads 2", command)

    def test_answers_are_checked_against_the_input_including_top_k_ties(self) -> None:
        rows = [
            {"status_code": 503 if i % 2 else 200, "request_id": f"req-{i:02}",
             "latency": 1.5, "user_agent": "Chrome" if i % 3 else "curl"}
            for i in reversed(range(12))
        ]
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "events.jsonl.gz"
            with gzip.open(data, "wt") as output:
                for row in rows:
                    output.write(json.dumps(row) + "\n")
            expected = benchmark.expected_results(data)
        self.assertEqual(expected["full_count"], [(12,)])
        self.assertEqual(expected["selective_filter"], [(6,)])
        self.assertEqual(expected["user_agent_like"], [(8,)])
        self.assertEqual(expected["group_by_status"], [(200, 6), (503, 6)])
        self.assertEqual(expected["top_latency"], [(f"req-{i:02}", 1.5) for i in range(10)])
        benchmark.validate_output("logq", "group_by_status", "503,6\n200,6\n", expected)
        benchmark.validate_output("angle_grinder", "full_count", '{"_count":12}\n', expected)
        with self.assertRaises(ValueError):
            benchmark.validate_output("logq", "full_count", "11\n", expected)
        with self.assertRaises(ValueError):
            benchmark.validate_output("logq", "full_count", "error: invalid query\n", expected)
        with self.assertRaises(ValueError):
            benchmark.validate_output("logq", "top_latency", "req-11,1.5\n", expected)
        with self.assertRaises(ValueError):
            benchmark.validate_output("angle_grinder", "full_count", '{"_count":12.5}\n', expected)

    def test_gzip_reader_failure_is_not_hidden_by_a_successful_consumer(self) -> None:
        query = {"angle_grinder": "* | json | count"}
        with tempfile.TemporaryDirectory() as directory:
            consumer = Path(directory) / "consumer"
            consumer.write_text("#!/bin/sh\ncat >/dev/null\nexit 0\n")
            consumer.chmod(0o755)
            command = benchmark.tool_command("angle_grinder", str(consumer), query,
                                             Path(directory) / "missing.jsonl.gz")
            result = subprocess.run(["/bin/sh", "-c", command], capture_output=True)
            self.assertNotEqual(result.returncode, 0)

    def test_runner_validates_before_starting_hyperfine(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "jsonl-test.jsonl"
            data.write_text('{"status_code":200,"request_id":"r","latency":1,"user_agent":"curl"}\n')
            fake = Path(directory) / "fake-logq"
            fake.write_text("#!/bin/sh\nprintf 'error: old CLI exits successfully\\n'\n")
            fake.chmod(0o755)
            with patch.object(sys, "argv", ["benchmark.py", "--scale", "test", "--data-dir", directory,
                                            "--results-dir", str(Path(directory) / "results")]), \
                 patch.dict(benchmark.os.environ, {"LOGQ_BIN": str(fake), "HYPERFINE_BIN": str(fake)}), \
                 patch.object(benchmark, "discover_tools", return_value={"logq": str(fake)}):
                with self.assertRaises(ValueError):
                    benchmark.main()
            self.assertFalse((Path(directory) / "results" / "metadata.json").exists())

    def test_dry_run_does_not_format_old_results_or_create_a_results_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "jsonl-test.jsonl"
            data.write_text('{}\n')
            results = Path(directory) / "separate-results"
            result = subprocess.run([str(HERE / "run.sh"), "--dry-run", "--scale", "test", "--data-dir", directory,
                                     "--results-dir", str(results)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIsInstance(json.loads(result.stdout), dict)
            self.assertFalse(results.exists())

    def test_selected_tools_omit_queries_with_no_supported_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "jsonl-test.jsonl").write_text('{}\n')
            with patch.dict(benchmark.os.environ, {"AGRIND_BIN": sys.executable}):
                result = subprocess.run([str(HERE / "run.sh"), "--dry-run", "--scale", "test",
                                         "--data-dir", directory, "--tools", "angle_grinder"],
                                        capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            commands = json.loads(result.stdout)
            self.assertNotIn("top_latency", commands)
            self.assertTrue(all(commands.values()))

    def test_successful_run_formats_the_requested_directory_and_records_provenance(self) -> None:
        with tempfile.TemporaryDirectory(prefix="logq benchmark ") as directory:
            root = Path(directory)
            (root / "jsonl-test.jsonl").write_text(
                '{"status_code":200,"request_id":"r","latency":1,"user_agent":"curl"}\n')
            fake_logq = root / "logq"
            fake_logq.write_text(f"#!{sys.executable}\n" + '''import sys
if sys.argv[-1] == "--version":
    print("fake logq")
elif "group by" in sys.argv[-1].lower():
    print("200,1")
elif "order by" in sys.argv[-1].lower():
    print("r,1")
else:
    print("0" if "where" in sys.argv[-1].lower() else "1")
''')
            fake_logq.chmod(0o755)
            fake_hyperfine = root / "hyperfine"
            fake_hyperfine.write_text(f"#!{sys.executable}\n" + '''import json, pathlib, subprocess, sys
args = sys.argv[1:]
if args == ["--version"]:
    print("fake hyperfine")
else:
    results = []
    for index, value in enumerate(args):
        if value == "--command-name":
            subprocess.run(["/bin/sh", "-c", args[index + 2]], check=True)
            results.append({"command": args[index + 1], "mean": 0.1, "stddev": 0.01})
    pathlib.Path(args[args.index("--export-json") + 1]).write_text(json.dumps({"results": results}))
''')
            fake_hyperfine.chmod(0o755)
            results = root / "custom results"
            with patch.dict(benchmark.os.environ, {"LOGQ_BIN": str(fake_logq), "HYPERFINE_BIN": str(fake_hyperfine)}):
                result = subprocess.run([str(HERE / "run.sh"), "--scale", "test", "--data-dir", directory,
                                         "--results-dir", str(results), "--tools", "logq", "--threads", "2"],
                                        capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Full-file count", (results / "table.md").read_text())
            metadata = json.loads((results / "metadata.json").read_text())
            self.assertEqual(metadata["thread_limit"], 2)
            self.assertEqual(metadata["dataset_rows"], 1)
            self.assertEqual(metadata["dataset_sha256"], benchmark.sha256(root / "jsonl-test.jsonl"))
            self.assertEqual(metadata["binary_sha256"]["logq"], benchmark.sha256(fake_logq))
            self.assertEqual(len(metadata["git_commit"]), 40)
            self.assertIsNone(metadata["build_command"])
            self.assertTrue((results / "verification.json").is_file())

            # A run containing only an external engine never builds logq.
            external_results = root / "external only"
            with patch.dict(benchmark.os.environ, {"LOGQ_BIN": "", "DUCKDB_BIN": str(fake_logq),
                                                   "HYPERFINE_BIN": str(fake_hyperfine)}):
                result = subprocess.run([str(HERE / "run.sh"), "--scale", "test", "--data-dir", directory,
                                         "--results-dir", str(external_results), "--tools", "duckdb"],
                                        capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            external_metadata = json.loads((external_results / "metadata.json").read_text())
            self.assertIsNone(external_metadata["build_command"])

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
