from __future__ import annotations

import gzip
import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("explore", HERE / "explore.py")
explore = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(explore)


class ExploreTest(unittest.TestCase):
    def case(self, name):
        return next(case for case in explore.CASES if case["id"] == name)

    def test_generation_is_deterministic_and_shards_gzip_preserve_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = explore.generate(root / "first", 37, 13, 7)
            second = explore.generate(root / "second", 37, 13, 7)
            self.assertEqual(first, second)
            plain = (root / "first" / "base.jsonl").read_bytes()
            shards = b"".join(path.read_bytes() for path in sorted((root / "first" / "shards").glob("*.jsonl")))
            self.assertEqual(plain, shards)
            with gzip.open(root / "first" / "base.jsonl.gz", "rb") as source:
                self.assertEqual(plain, source.read())
            self.assertEqual(explore.generate(root / "first", 37, 13, 7), first)
            (root / "first" / "base.jsonl").write_bytes(b"changed")
            with self.assertRaises(ValueError):
                explore.generate(root / "first", 37, 13, 7)

    def test_argv_preserves_shell_metacharacters_as_literal_arguments(self):
        root = Path("/tmp/a space ' $HOME `touch BAD`;")
        args = explore.command(Path("/tmp/binary with spaces"), self.case("string_short_repeated"), root, 2)
        self.assertEqual(args[0], "/tmp/binary with spaces")
        self.assertIn("it:jsonl=" + str(root.resolve() / "base.jsonl"), args)
        self.assertEqual(args[-1], self.case("string_short_repeated")["query"])
        self.assertEqual(args[args.index("--threads") + 1], "2")
        with self.assertRaises(ValueError):
            explore.command(Path("logq"), self.case("string_short_repeated"), Path("/tmp/ambiguous,dir"), 1)

    def test_digest_rejects_duplicate_missing_extra_and_wrong_typed_rows(self):
        case = {"columns": [("g", "int"), ("n", "int")], "ordered": False}
        expected = explore.digest_rows(case, [(1, 2), (2, 3)])
        explore.validate(io.StringIO('{"n":3,"g":2}\n{"g":1,"n":2}\n'), case, expected)
        for text in ['{"g":1,"n":2}\n' * 2, '{"g":1}\n', '{"g":1,"n":2,"extra":3}\n', '{"g":true,"n":2}\n']:
            with self.assertRaises(ValueError):
                explore.validate(io.StringIO(text), case, expected)

    def test_ordered_digest_detects_wrong_topk_tie_order(self):
        case = {"columns": [("id", "str"), ("v", "int")], "ordered": True}
        expected = explore.digest_rows(case, [("a", 2), ("b", 2)])
        with self.assertRaises(ValueError):
            explore.validate(io.StringIO('{"id":"b","v":2}\n{"id":"a","v":2}\n'), case, expected)

    def test_numeric_comparison_uses_public_f32_precision_and_rejects_nan(self):
        case = {"columns": [("total", "float")], "ordered": False}
        expected = explore.digest_rows(case, [(16_777_217.0,)])
        explore.validate(io.StringIO('{"total":16777216}\n'), case, expected)
        for text in ['{"total":16777220}\n', '{"total":null}\n', '{"total":NaN}\n']:
            with self.assertRaises(ValueError):
                explore.validate(io.StringIO(text), case, expected)

    def test_output_validation_streams_without_read_all(self):
        class LinesOnly(io.StringIO):
            def read(self, *args):
                raise AssertionError("must not materialize all output")
        case = {"columns": [("n", "int")], "ordered": False}
        expected = explore.digest_rows(case, ((i,) for i in range(10_000)))
        explore.validate(LinesOnly("".join(json.dumps({"n": i}) + "\n" for i in range(10_000))), case, expected)

    def test_oracle_agrees_for_same_corpus_and_direct_nested_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = explore.generate(root / "data", 37, 13, 7)
            answers = explore.oracles(root / "data", manifest, explore.CASES)
            self.assertEqual(answers["expression_direct"], answers["scan_shards"])
            self.assertEqual(answers["expression_direct"], answers["scan_gzip"])
            self.assertEqual(answers["expression_direct"], answers["shape_wide"])
            self.assertEqual(answers["expression_direct"], answers["shape_nested"])
            self.assertEqual(answers["top10"]["rows"], 10)
            self.assertEqual(answers["fullsort"]["rows"], 37)
            self.assertEqual(answers["group_high"]["rows"], 13)

    def test_nonzero_and_exit_zero_wrong_answers_are_never_successful_samples(self):
        case = self.case("string_short_repeated")
        expected = explore.digest_rows(case, [(4,)])
        for script in ['import sys;print(\'{"n":4}\');sys.exit(7)', 'print(\'{"n":3}\')', 'print("error: failed")']:
            with self.assertRaises(ValueError):
                explore.run_once([sys.executable, "-c", script], case, expected)
        sample = explore.run_once([sys.executable, "-c", 'print(\'{"n":4}\')'], case, expected)
        self.assertGreater(sample["wall_seconds"], 0)
        self.assertGreaterEqual(sample["user_cpu_seconds"], 0)

    def test_cli_keeps_invalid_baseline_untimed_and_records_all_provenance(self):
        with tempfile.TemporaryDirectory(prefix="logq explore ") as directory:
            root = Path(directory)
            for label, value in [("baseline", 0), ("candidate", 4)]:
                binary = root / label
                binary.write_text(f"#!{sys.executable}\nimport sys\nprint('fake 1' if '--version' in sys.argv else '{{\"n\":{value}}}')\n")
                binary.chmod(0o755)
            result_dir = root / "results"
            args = [sys.executable, str(HERE / "explore.py"), "--rows", "20", "--groups", "7", "--shard-rows", "7",
                    "--data-dir", str(root / "data"), "--results-dir", str(result_dir), "--cases", "string_short_repeated",
                    "--threads", "1", "--runs", "1", "--warmup", "0", "--skip-rss",
                    "--binary", f"baseline={root / 'baseline'}", "--binary", f"candidate={root / 'candidate'}",
                    "--allow-invalid", "baseline"]
            result = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads((result_dir / "results.json").read_text())
            invalid = next(row for row in report if row["binary"] == "baseline")
            valid = next(row for row in report if row["binary"] == "candidate")
            self.assertEqual(invalid["status"], "correctness_failure")
            self.assertEqual(invalid["samples"], [])
            self.assertEqual(valid["status"], "ok")
            comparison = json.loads((result_dir / "comparisons.json").read_text())[0]
            self.assertEqual(comparison["status"], "not_comparable")
            self.assertNotIn("wall_speedup", comparison)
            meta = json.loads((result_dir / "metadata.json").read_text())
            self.assertEqual(meta["cache_state"], "warm")
            self.assertEqual(meta["binaries"]["candidate"]["sha256"], explore.sha256(root / "candidate"))
            self.assertEqual(len(meta["script_sha256"]), 64)
            self.assertEqual(len(meta["query_sha256"]), 64)
            self.assertTrue((result_dir / "manifest.json").exists())
            self.assertIn("not_measured", meta["roadmap"]["persistent_amortization"]["status"])
            # Invalid candidates fail the overall run, while still preserving the report.
            args[args.index(str(result_dir))] = str(root / "failed-results")
            del args[-2:]
            failed = subprocess.run(args, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertTrue((root / "failed-results" / "results.json").exists())

    def test_timeout_cannot_be_a_successful_sample(self):
        case = self.case("string_short_repeated")
        expected = explore.digest_rows(case, [(4,)])
        with self.assertRaises(ValueError):
            explore.run_once([sys.executable, "-c", "import time; time.sleep(1)"], case, expected, timeout=0.02)

    def test_timeout_kills_owned_descendants_not_just_wrapper(self):
        case = self.case("string_short_repeated")
        expected = explore.digest_rows(case, [(4,)])
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "leaked-child"
            child = f"import pathlib,time;time.sleep(0.35);pathlib.Path({str(marker)!r}).write_text('leaked')"
            parent = f"import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',{child!r}]);time.sleep(2)"
            with self.assertRaises(ValueError):
                explore.run_once([sys.executable, "-c", parent], case, expected, timeout=0.15)
            time.sleep(0.35)
            self.assertFalse(marker.exists(), "timeout must stop descendants before returning")

    def test_skew_changes_frequency_without_changing_default_group_count(self):
        uniform, skew = {}, {}
        for index in range(1000):
            row = explore.generated_row(index, 100)
            uniform[row["high"]] = uniform.get(row["high"], 0) + 1
            skew[row["skew"]] = skew.get(row["skew"], 0) + 1
        self.assertEqual(len(uniform), 100)
        self.assertEqual(len(skew), 100)
        self.assertEqual(skew[0], 900)
        self.assertEqual(set(uniform.values()), {10})

    def test_postrun_corpus_mutation_fails_provenance_even_if_answers_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fake"
            binary.write_text(f"#!{sys.executable}\nprint('fake')\n")
            binary.chmod(0o755)
            changed = False
            def mutation(*args, **kwargs):
                nonlocal changed
                if not changed:
                    with (root / "data" / "base.jsonl").open("a") as output:
                        output.write("\n")
                    changed = True
                return {"wall_seconds": 0.1, "user_cpu_seconds": 0.01, "system_cpu_seconds": 0.001}
            args = ["--rows", "20", "--groups", "7", "--shard-rows", "7", "--threads", "1", "--runs", "1",
                    "--warmup", "0", "--skip-rss", "--binary", f"candidate={binary}",
                    "--cases", "string_short_repeated", "--data-dir", str(root / "data"),
                    "--results-dir", str(root / "results")]
            with patch.object(explore, "run_once", side_effect=mutation):
                self.assertEqual(explore.main(args), 1)
            metadata = json.loads((root / "results" / "metadata.json").read_text())
            self.assertEqual(metadata["status"], "failed")
            self.assertEqual(metadata["data_changed_during_run"], ["base.jsonl"])

    def test_cold_or_prepared_results_cannot_be_claimed_by_this_harness(self):
        parser = explore.parser()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            parser.parse_args(["--cache-state", "cold"])
        self.assertEqual(explore.ROADMAP["cold_cache"]["status"], "not_measured")
        self.assertIn("preparation", explore.ROADMAP["persistent_amortization"]["required_metrics"])


if __name__ == "__main__":
    unittest.main()
