import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import columnar_reuse as reuse


class ColumnarReuseTest(unittest.TestCase):
    def write_rows(self, root, rows):
        path = root / "source.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows))
        return path

    @staticmethod
    def row(value, **extra):
        return {"v": value, "payload": "雪 \\\"", "nested": {"metrics": {"v": value}}, **extra}

    def test_source_contract_keeps_absent_null_and_dynamic_raw_values(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [self.row(0)] + [self.row(index + 1, mixed=value) for index, value in
                    enumerate([None, False, "1", 1, 1.25, {"a": [1, None]}, 9007199254740993])]
            info = reuse.inspect_source(self.write_rows(Path(directory), rows))
            self.assertEqual(info["rows"], 8)
            self.assertEqual(info["mixed_present_rows"], 7)
            self.assertEqual(info["mixed_null_rows"], 1)
            self.assertEqual(info["roundtrip"]["rows"], 8)
            self.assertEqual(info["queries"]["narrow"]["expected"]["rows"], 1)

    def test_lossy_acceleration_contract_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for invalid in [None, True, "1", 2**31, -(2**31)-1, 1.25]:
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    reuse.inspect_source(self.write_rows(root, [self.row(invalid)]))
            path = root / "source.jsonl"
            path.write_text('{"v":0,"v":1,"payload":"x","nested":{"metrics":{"v":1}}}\n')
            with self.assertRaisesRegex(ValueError, "duplicate"):
                reuse.inspect_source(path)

    def test_identity_rejects_append_replacement_schema_and_binary_drift(self):
        identity = {"source_sha256": "a", "schema_version": 1, "clickhouse_sha256": "b"}
        reuse.check_identity(identity, dict(identity))
        for key in identity:
            changed = dict(identity, **{key: "changed"})
            with self.subTest(key=key), self.assertRaises(ValueError):
                reuse.check_identity(identity, changed)

    def test_unrepresentable_opaque_number_cannot_silently_default_typed_projection(self):
        # Real ClickHouse JSONHas/JSONExtract returned zero/default for the whole
        # object containing -9223372036854775809, even though v itself was valid.
        with tempfile.TemporaryDirectory() as directory:
            for invalid in [-(2**63)-1, 2**64, float("inf")]:
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    reuse.inspect_source(self.write_rows(Path(directory), [self.row(1, mixed={"deep": [invalid]})]))

    def test_structured_command_keeps_quotes_spaces_and_worker_flags(self):
        value = "/tmp/a'b\\c path"
        self.assertEqual(reuse.sql_literal(value), "'/tmp/a\\'b\\\\c path'")
        args = reuse.ch_command("/tmp/ch space", "select 1", 1, Path("/tmp/store space"))
        self.assertEqual(args[0], "/tmp/ch space")
        self.assertIn("/tmp/store space", args)
        self.assertEqual(args[args.index("--max_threads") + 1], "1")
        self.assertEqual(args[args.index("--max_parsing_threads") + 1], "1")
        self.assertNotIn("--max_threads", reuse.ch_command("ch", "select 1", 0))

    def test_amortization_uses_actual_samples_and_charges_preparation_once(self):
        samples = [{"wall_seconds": 0.2, "user_cpu_seconds": 0.1, "system_cpu_seconds": 0.02},
                   {"wall_seconds": 0.3, "user_cpu_seconds": 0.2, "system_cpu_seconds": 0.03}]
        prep = {"wall_seconds": 1.0, "user_cpu_seconds": 0.8, "system_cpu_seconds": 0.1}
        result = reuse.amortized(prep, samples)
        self.assertEqual(result["query_count"], 2)
        self.assertAlmostEqual(result["total_wall_seconds"], 1.5)
        self.assertAlmostEqual(result["total_cpu_seconds"], 1.25)

    def test_prepared_artifact_tampering_and_foreign_directories_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "data.parquet").write_bytes(b"original")
            saved = reuse.artifact_snapshot(root)
            (root / "data.parquet").write_bytes(b"modified")
            with self.assertRaises(ValueError):
                reuse.verify_artifacts(root, saved)
            with self.assertRaisesRegex(ValueError, "foreign"):
                reuse.prepare(root, {}, "not invoked", root / "source", {}, 1, 1)

    def setup_main(self, root):
        data = root / "data"
        data.mkdir()
        source = self.write_rows(data, [self.row(value) for value in range(3)])
        reuse.explore.write_json(data / "manifest.json", {"files": [{"path": source.name,
            "bytes": source.stat().st_size, "sha256": reuse.explore.sha256(source)}]})
        binaries = []
        for name in ["ch", "logq"]:
            path = root / name
            path.write_text("#!/bin/sh\nprintf 'test version\\n'\n")
            path.chmod(0o755)
            binaries.append(path)
        return source, ["--data-dir", str(data), "--file", source.name,
            "--results-dir", str(root / "results"), "--prepared-dir", str(root / "prepared"),
            "--clickhouse", str(binaries[0]), "--logq", str(binaries[1]), "--runs", "1",
            "--warmup", "0", "--repetitions", "1", "--cases", "count", "--skip-rss"]

    def test_failed_or_modified_input_run_preserves_invalid_results(self):
        for failure in ["answer", "mutation"]:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, args = self.setup_main(root)
                sample = {"wall_seconds": .01, "user_cpu_seconds": .005, "system_cpu_seconds": .001}
                def invoke(*_args, **_kwargs):
                    if failure == "answer":
                        raise ValueError("wrong answer")
                    if source.read_text().endswith("}\n"):
                        with source.open("a") as output:
                            output.write("\n")
                    return sample
                prepared = {"preparation": {name: {"sample": sample} for name in ["parquet", "persisted"]}, "artifacts": []}
                with patch.object(reuse, "semantic_fixture", return_value={"status": "passed"}), \
                     patch.object(reuse, "prepare", return_value=prepared), \
                     patch.object(reuse, "verify_artifacts"), patch.object(reuse, "execute", side_effect=invoke):
                    with self.assertRaises(ValueError):
                        reuse.main(args)
                metadata = json.loads((root / "results/metadata.json").read_text())
                self.assertEqual(metadata["status"], "failed")
                results = json.loads((root / "results/results.json").read_text())
                self.assertTrue(all(row["status"] != "ok" for row in results))

    def test_preparation_identity_includes_timing_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, args = self.setup_main(root)
            with patch.object(reuse, "semantic_fixture", return_value={"status": "passed"}), \
                 patch.object(reuse, "prepare", side_effect=ValueError("stop before preparation")) as prepare:
                with self.assertRaisesRegex(ValueError, "stop before preparation"):
                    reuse.main(args)
            identity = prepare.call_args.args[1]
            self.assertEqual(identity["measurement_helper_sha256"],
                             reuse.explore.sha256(Path(reuse.explore.__file__).resolve()))
            old_method = dict(identity, measurement_helper_sha256="previous helper")
            with self.assertRaisesRegex(ValueError, "identity changed"):
                reuse.check_identity(old_method, identity)

    def test_session_answer_checks_every_result_and_keeps_structured_query(self):
        definition = {"columns": [("n", "int"), ("total", "float")], "ordered": False}
        expected = reuse.explore.digest_rows(definition, [(3, 1.25)])
        repeated = reuse.repeated_answer(definition, expected, 3)
        self.assertEqual(repeated, reuse.explore.digest_rows(definition, [(3, 1.25)] * 3))
        answer = '{"n":3,"total":1.25}\n'
        reuse.explore.validate(io.StringIO(answer * 3), definition, repeated)
        for invalid in [answer * 2, answer * 2 + '{"n":3,"total":2}\n']:
            with self.assertRaisesRegex(ValueError, "answer digest mismatch"):
                reuse.explore.validate(io.StringIO(invalid), definition, repeated)
        argv = reuse.ch_command("/tmp/ch space", "SELECT 1 AS n FORMAT JSONEachRow", 1)
        session = reuse.session_command(argv, 3)
        self.assertEqual(session[session.index("--query") + 1],
                         ";\n".join(["SELECT 1 AS n FORMAT JSONEachRow"] * 3))
        self.assertEqual(argv[argv.index("--query") + 1], "SELECT 1 AS n FORMAT JSONEachRow")
        self.assertEqual(session[0], "/tmp/ch space")
        with self.assertRaises(ValueError):
            reuse.repeated_answer({**definition, "ordered": True}, expected, 3)

    def test_session_reuse_is_separate_and_only_runs_clickhouse_engines(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, args = self.setup_main(root)
            args += ["--session-reuse", "--repetitions", "2"]
            sample = {"wall_seconds": .01, "user_cpu_seconds": .005, "system_cpu_seconds": .001}
            prepared = {"preparation": {name: {"sample": sample} for name in ["parquet", "persisted"]}, "artifacts": []}
            with patch.object(reuse, "semantic_fixture", return_value={"status": "passed"}), \
                 patch.object(reuse, "prepare", return_value=prepared), \
                 patch.object(reuse, "verify_artifacts"), patch.object(reuse, "execute", return_value=sample):
                self.assertEqual(reuse.main(args), 0)
            sessions = json.loads((root / "results/session-results.json").read_text())
            self.assertEqual({row["engine"] for row in sessions}, {"clickhouse_raw", "parquet", "persisted"})
            for row in sessions:
                self.assertEqual(row["query_count"], 2)
                self.assertEqual(row["process_count"], 1)
                self.assertEqual(len(row["samples"]), 1)
                self.assertEqual(row["query_wall_seconds"], .01)
            fresh = json.loads((root / "results/results.json").read_text())
            self.assertEqual(len(fresh), 4)
            self.assertTrue(all(row["query_count"] == 2 and len(row["samples"]) == 2 for row in fresh))


if __name__ == "__main__":
    unittest.main()
