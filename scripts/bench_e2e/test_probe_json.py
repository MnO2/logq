from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import probe_json as probe


class ProbeTest(unittest.TestCase):
    def sample(self, **changes):
        result = {"input_bytes": 10, "rows": 100, "batches": 1, "active_rows": 20,
                  "like_pattern": "%needle%", "backend": "mapped", "dictionary": False,
                  "dictionary_columns": 0, "elapsed_ns": 1000, "allocation_instrumentation": False,
                  "allocation_calls": None, "allocated_bytes_including_realloc": None}
        result.update(changes)
        return result

    def invoke_result(self, result, *, extra=()):
        with tempfile.TemporaryDirectory(prefix="probe path ") as directory:
            binary = Path(directory) / "fake probe"
            binary.write_text(f"#!{sys.executable}\nprint({json.dumps(json.dumps(result))})\n")
            binary.chmod(0o755)
            return probe.invoke([str(binary), "/tmp/input", "mapped", "off", "sr", "--like", "%needle%", *extra],
                                20, rows=100, input_bytes=10, timeout=2)

    def test_validate_counter_types_configuration_and_instrumentation(self):
        self.assertEqual(self.invoke_result(self.sample())["active_rows"], 20)
        for changed in [dict(active_rows=21), dict(active_rows=True), dict(rows=101), dict(input_bytes=11),
                        dict(elapsed_ns=0), dict(elapsed_ns=float("nan")), dict(dictionary=True),
                        dict(backend="buffered8k"), dict(dictionary_columns=1), dict(batches=-1),
                        dict(allocation_instrumentation=True), dict(allocation_calls=5)]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                self.invoke_result(self.sample(**changed))
        instrumented = self.sample(allocation_instrumentation=True, allocation_calls=7,
                                   allocated_bytes_including_realloc=100)
        self.assertEqual(self.invoke_result(instrumented, extra=("--allocations",))["allocation_calls"], 7)

    def test_nonzero_exit_and_timeout_are_not_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "bad"
            binary.write_text(f"#!{sys.executable}\nimport sys\nsys.exit(4)\n")
            binary.chmod(0o755)
            args = [str(binary), "input", "mapped", "off", "sr"]
            with self.assertRaises(ValueError):
                probe.invoke(args, 1, rows=1, input_bytes=1, timeout=1)
            binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(1)\n")
            with self.assertRaises(ValueError):
                probe.invoke(args, 1, rows=1, input_bytes=1, timeout=0.02)

    def test_like_may_omit_fully_rejected_batches_but_scan_may_not(self):
        # Returned batch rows are not scanned rows when whole batches disappear.
        result = self.sample(rows=20)
        self.assertEqual(self.invoke_result(result)["rows"], 20)
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fake"
            result.update(like_pattern=None, active_rows=100)
            binary.write_text(f"#!{sys.executable}\nprint({json.dumps(json.dumps(result))})\n")
            binary.chmod(0o755)
            with self.assertRaises(ValueError):
                probe.invoke([str(binary), "input", "mapped", "off", "sr"], 100, rows=100, input_bytes=10)

    def setup_run(self, root):
        data = root / "data.jsonl"
        data.write_text('{"sr":"needle","su":"needle1","lr":"needle","lu":"needle2"}\n')
        baseline = root / "baseline"
        candidate = root / "candidate"
        for binary in [baseline, candidate]:
            binary.write_text(f"#!{sys.executable}\npass\n")
            binary.chmod(0o755)
        return ["--baseline", str(baseline), "--candidate", str(candidate), "--data", str(data),
                "--results-dir", str(root / "results"), "--runs", "2", "--fields", "sr",
                "--modes", "scan", "--backends", "mapped"]

    @staticmethod
    def fake_invoke(argv, expected, **kwargs):
        return {"elapsed_ns": 1000, "active_rows": expected, "argv": argv,
                "user_cpu_seconds": 0.01, "system_cpu_seconds": 0.001}

    def test_run_snapshots_sources_and_separates_unknown_binary_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.setup_run(root)
            with patch.object(probe, "invoke", side_effect=self.fake_invoke):
                probe.main(args)
            result = root / "results"
            metadata = json.loads((result / "metadata.json").read_text())
            self.assertEqual(metadata["status"], "complete")
            self.assertEqual(metadata["binaries"]["baseline"]["declared_source"], "unknown")
            self.assertIsNone(metadata["binaries"]["baseline"]["declared_build_command"])
            for name in ["probe_json.py", "explore.py", "json_scan_probe.rs"]:
                self.assertTrue((result / name).is_file())
            self.assertIn("git_status", metadata)
            self.assertIn("count-only", metadata["validation_scope"])
            self.assertTrue((result / "definitions.json").exists())

    def test_modified_input_or_binary_marks_run_failed(self):
        for kind in ["data", "candidate"]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = self.setup_run(root)
                modified = False
                def mutation(argv, expected, **kwargs):
                    nonlocal modified
                    if not modified:
                        path = root / ("data.jsonl" if kind == "data" else "candidate")
                        path.write_text(path.read_text() + "\n")
                        modified = True
                    return self.fake_invoke(argv, expected, **kwargs)
                with patch.object(probe, "invoke", side_effect=mutation), self.assertRaises(ValueError):
                    probe.main(args)
                metadata = json.loads((root / "results" / "metadata.json").read_text())
                self.assertEqual(metadata["status"], "failed")
                self.assertIn("changed", metadata["error"])

    def test_failure_preserves_report_and_has_no_complete_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.setup_run(root)
            with patch.object(probe, "invoke", side_effect=ValueError("wrong count")), self.assertRaises(ValueError):
                probe.main(args)
            metadata = json.loads((root / "results" / "metadata.json").read_text())
            self.assertEqual(metadata["status"], "failed")
            self.assertEqual(metadata["error"], "wrong count")
            self.assertTrue((root / "results" / "results.json").exists())


if __name__ == "__main__":
    unittest.main()
