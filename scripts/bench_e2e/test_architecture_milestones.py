import importlib
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class ArchitectureMilestoneTests(unittest.TestCase):
    def test_import_does_not_parse_cli_or_create_results(self):
        script_directory = str(Path(__file__).resolve().parent)
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(
                [sys.executable, "-c", f"import sys; sys.path.insert(0, {script_directory!r}); import architecture_milestones"],
                cwd=temporary,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_existing_results_and_sort_fixture_are_preserved(self):
        milestone = importlib.import_module("architecture_milestones")
        for directory, action in [
            ("architecture-results", milestone.run_experiments),
            ("spill-data", milestone.prepare_sort_fixture),
        ]:
            for populated in [False, True]:
                with self.subTest(directory=directory, populated=populated):
                    with tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary)
                        existing = root / directory
                        existing.mkdir()
                        if populated:
                            (existing / "keep").write_bytes(b"prior experiment")
                        with mock.patch.object(milestone.subprocess, "run") as run:
                            with self.assertRaises(FileExistsError):
                                action(root)
                            run.assert_not_called()
                        self.assertEqual(
                            [path.name for path in existing.iterdir()], ["keep"] if populated else []
                        )
                        if populated:
                            self.assertEqual((existing / "keep").read_bytes(), b"prior experiment")

    def test_prepare_sort_dispatch_does_not_run_other_experiments(self):
        milestone = importlib.import_module("architecture_milestones")
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(milestone, "prepare_sort_fixture") as prepare:
                with mock.patch.object(milestone, "run_experiments") as run:
                    milestone.main(["--work-dir", temporary, "--prepare-sort"])
            prepare.assert_called_once_with(Path(temporary).resolve())
            run.assert_not_called()

    def test_sort_fixture_encoding_matches_frozen_driver_samples(self):
        milestone = importlib.import_module("architecture_milestones")
        # Captured from the executed driver's fixture formula before refactoring.
        samples = [
            (0, 627, "3239389d7f72bd765014f5daefe7833572168015d1763990d0a8991542b81591"),
            (1, 627, "e5fed33a3d16638d34e990380042ce46f71d787009fece6359476fd39dded74e"),
            (99999, 626, "b3368673716c026df1154bbcd998224d0d7d5b062ae077783cd9bcea033c3ef9"),
        ]
        for index, size, fingerprint in samples:
            line = (json.dumps(milestone.sort_fixture_row(index), ensure_ascii=False,
                               separators=(",", ":")) + "\n").encode("utf-8")
            self.assertEqual(len(line), size)
            self.assertEqual(hashlib.sha256(line).hexdigest(), fingerprint)

    def test_argv_preserves_paths_query_and_probe_configuration(self):
        milestone = importlib.import_module("architecture_milestones")
        binary = Path("/tmp/frozen binaries/probe")
        source = Path("/tmp/input files/data.jsonl")
        query = "select count(*) as n from it"
        self.assertEqual(
            milestone.lifecycle_command(binary, source, query, 10, 0),
            [str(binary), "--input", str(source), "--query", query, "--runs", "10", "--threads", "0"],
        )
        expected_kernel = [str(binary), "--operation", "add-columns", "--rows", "500000",
                           "--chain-length", "16", "--active-percent", "1", "--nullable"]
        self.assertEqual(milestone.kernel_command(binary, "add-columns", 1, 0), expected_kernel)
        self.assertEqual(milestone.kernel_command(binary, "add-columns", 1, 1), expected_kernel + ["--reverse"])
        self.assertEqual(
            milestone.logq_sort_command(binary, source, 64),
            [str(binary), "query", "select key,payload from it order by key asc", "--table",
             f"it:jsonl={source}", "--output", "ndjson", "--threads", "1", "--max-memory", "64MiB"],
        )
        scratch, output = Path("/tmp/scratch dir"), Path("/tmp/out file.jsonl")
        self.assertEqual(
            milestone.external_sort_command(binary, source, 4, scratch, output),
            [str(binary), str(source), "--run-bytes", str(4 * 1024 * 1024), "--fan-in", "8",
             "--disk-bytes", str(256 * 1024 * 1024), "--scratch-dir", str(scratch), "--output", str(output)],
        )

    def test_sqlite_oracle_checks_every_value_stable_ties_and_record_count(self):
        milestone = importlib.import_module("architecture_milestones")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root / "input.jsonl", root / "output.jsonl"
            source.write_text('\n'.join(json.dumps(row, ensure_ascii=False) for row in [
                {"key": 2, "payload": "雪\n\\\""}, {"key": 1, "payload": "first"},
                {"key": 1, "payload": "second"},
            ]) + '\n', encoding="utf-8")
            with milestone.sort_oracle(source, root / "oracle.sqlite") as database:
                rows = [{"key": 1, "payload": "first", "sequence": 1},
                        {"key": 1, "payload": "second", "sequence": 2},
                        {"key": 2, "payload": "雪\n\\\"", "sequence": 0}]
                for include_sequence in [False, True]:
                    expected = rows if include_sequence else [
                        {key: value for key, value in row.items() if key != "sequence"} for row in rows
                    ]
                    output.write_text(''.join(json.dumps(row) + '\n' for row in expected))
                    milestone.validate_sorted_output(output, database, include_sequence=include_sequence)
                    invalid_outputs = [
                        expected[:-1], expected + [expected[-1]],
                        [expected[1], expected[0], expected[2]],
                        [expected[0], {**expected[1], "payload": "changed"}, expected[2]],
                        [expected[0], expected[1], {**expected[2], "extra": "unrequested"}],
                    ]
                    for invalid in invalid_outputs:
                        output.write_text(''.join(json.dumps(row) + '\n' for row in invalid))
                        with self.assertRaises(ValueError):
                            milestone.validate_sorted_output(output, database, include_sequence=include_sequence)

    def test_lifecycle_complete_answer_and_frozen_identity_checks(self):
        milestone = importlib.import_module("architecture_milestones")
        milestone.validate_lifecycle_answer({"answer": [{"n": 3}]}, [{"n": 3}])
        for answer in [[{"n": 2}], [{"n": 3}, {"n": 3}], [{"n": 3, "extra": 0}]]:
            with self.assertRaises(ValueError):
                milestone.validate_lifecycle_answer({"answer": answer}, [{"n": 3}])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary, source = root / "binary", root / "source"
            binary.write_bytes(b"frozen binary")
            source.write_bytes(b"source data")
            metadata = {
                "binaries": {binary.name: milestone.sha256(binary)},
                "sources": {str(source): {"sha256": milestone.sha256(source), "bytes": source.stat().st_size}},
            }
            milestone.validate_frozen_inputs([binary], [source], metadata)
            for target, original in [(binary, b"frozen binary"), (source, b"source data")]:
                target.write_bytes(b"changed")
                with self.assertRaises(ValueError):
                    milestone.validate_frozen_inputs([binary], [source], metadata)
                target.write_bytes(original)

    def test_validation_remains_enabled_under_python_optimization(self):
        script_directory = str(Path(__file__).resolve().parent)
        code = (
            f"import sys; sys.path.insert(0, {script_directory!r})\n"
            "from architecture_milestones import validate_lifecycle_answer\n"
            "try:\n"
            "    validate_lifecycle_answer({'answer': [{'n': 2}]}, [{'n': 3}])\n"
            "except ValueError:\n"
            "    pass\n"
            "else:\n"
            "    raise SystemExit('incorrect answer accepted under python -O')\n"
        )
        result = subprocess.run([sys.executable, "-O", "-c", code], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_run_metadata_retains_fingerprints_and_records_failure(self):
        milestone = importlib.import_module("architecture_milestones")
        for failure in [None, "probe", "binary", "source"]:
            with self.subTest(failure=failure):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    for name in ["candidate-final", "query_lifecycle_probe-final",
                                 "external_sort_probe-final", "expression_probe-final"]:
                        (root / name).write_bytes(name.encode())
                    for name in ["data/width-32.jsonl", "data/width-2048.jsonl", "spill-data/input.jsonl"]:
                        path = root / name
                        path.parent.mkdir(exist_ok=True)
                        path.write_bytes(b'{}\n')
                    initial_source_hash = milestone.sha256(root / "data/width-32.jsonl")

                    def finish_sort(*args):
                        if failure == "probe":
                            raise RuntimeError("probe failed")
                        if failure in ("binary", "source"):
                            target = root / ("candidate-final" if failure == "binary" else "data/width-32.jsonl")
                            target.write_bytes(b"changed during measurement")

                    with mock.patch.object(milestone, "run_lifecycle") as lifecycle:
                        with mock.patch.object(milestone, "run_kernels") as kernels:
                            with mock.patch.object(milestone, "run_sort_experiments", side_effect=finish_sort):
                                if failure is None:
                                    milestone.run_experiments(root)
                                else:
                                    with self.assertRaises((RuntimeError, ValueError)):
                                        milestone.run_experiments(root)
                        lifecycle.assert_called_once()
                        kernels.assert_called_once()
                    output_dir = root / "architecture-results"
                    metadata = json.loads((output_dir / "metadata.json").read_text())
                    self.assertEqual(metadata["status"], "complete" if failure is None else "failed")
                    self.assertEqual(len(metadata["binaries"]), 4)
                    self.assertEqual(len(metadata["sources"]), 3)
                    self.assertEqual(metadata["sources"][str(root / "data/width-32.jsonl")],
                                     {"sha256": initial_source_hash, "bytes": 3})
                    self.assertEqual(milestone.sha256(output_dir / "architecture_milestones.py"),
                                     metadata["script_sha256"])
                    if failure is not None:
                        self.assertIn("error", metadata)


if __name__ == "__main__":
    unittest.main()
