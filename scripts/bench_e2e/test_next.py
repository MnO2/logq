from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path

import next_milestones as next_bench


class NextMilestonesTest(unittest.TestCase):
    def test_corpus_variants_preserve_rows_and_independent_answers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            manifest = next_bench.generate(root, 37, [32, 256], [1, 8])
            base = (root / "width-256.jsonl").read_bytes()
            for count in [1, 8]:
                pieces = sorted((root / f"shards-{count}").glob("*.jsonl"))
                self.assertEqual(b"".join(p.read_bytes() for p in pieces), base)
                self.assertEqual(b"".join(gzip.decompress(p.read_bytes()) for p in sorted((root / f"gzip-{count}").glob("*.gz"))), base)
            cases = next_bench.definitions(root, manifest)
            self.assertEqual(next_bench.expected(root, cases[0])["rows"], 1)
            dense = next(c for c in cases if c["id"] == "arithmetic16_w32")
            self.assertEqual(next_bench.expected(root, dense), next_bench.explore.digest_rows(dense, [(37, sum(range(37)) + 16 * 37)]))
            floats = next(c for c in cases if c["id"] == "float16_w32")
            self.assertEqual(next_bench.expected(root, floats), next_bench.explore.digest_rows(floats, [(37, sum(range(37)) + 8 * 37)]))
            for name in ["hybrid_w256", "predicate_1_w256", "predicate_50_w256"]:
                case = next(c for c in cases if c["id"] == name)
                self.assertLessEqual(next_bench.expected(root, case)["rows"], 10)
            self.assertEqual(next_bench.generate(root, 37, [32, 256], [1, 8]), manifest)

    def test_foreign_and_modified_data_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "foreign").write_text("keep")
            with self.assertRaises(ValueError):
                next_bench.generate(root, 4, [32], [1])
            corpus = root / "corpus"
            next_bench.generate(corpus, 4, [32], [1])
            (corpus / "width-32.jsonl").write_text("changed")
            with self.assertRaises(ValueError):
                next_bench.generate(corpus, 4, [32], [1])

    def test_query_paths_are_argv_not_shell_and_reject_table_syntax(self):
        case = {"path": "data.jsonl", "query": "select count(*) as n from it"}
        argv = next_bench.command(Path("/tmp/bin with spaces"), Path("/tmp/$x `literal`"), case, 1)
        self.assertIn("it:jsonl=" + str(Path("/tmp/$x `literal`").resolve() / "data.jsonl"), argv)
        with self.assertRaises(ValueError):
            next_bench.command(Path("logq"), Path("/tmp/a,b"), case, 1)

    def test_extra_glob_member_invalidates_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            next_bench.generate(root, 4, [32], [1])
            (root / "shards-1" / "extra.jsonl").write_text("")
            with self.assertRaisesRegex(ValueError, "inventory"):
                next_bench.generate(root, 4, [32], [1])


if __name__ == "__main__":
    unittest.main()
