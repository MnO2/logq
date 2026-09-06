import json
import tempfile
import unittest
from pathlib import Path

import execution_milestones as milestone
import explore


class ExecutionMilestoneTests(unittest.TestCase):
    def test_manifest_reuse_validates_contents_and_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            saved = milestone.generate(root, 5, [8])
            self.assertEqual(saved, milestone.generate(root, 5, [8]))
            with self.assertRaises(ValueError):
                milestone.generate(root, 6, [8])
            source = root / "width-8.jsonl"
            source.write_text("{}\n")
            with self.assertRaises(ValueError):
                milestone.generate(root, 5, [8])

    def test_foreign_directory_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = root / "keep"
            original.write_text("user data")
            with self.assertRaises(ValueError):
                milestone.generate(root, 5, [8])
            self.assertEqual(original.read_text(), "user data")

    def test_complete_oracles_for_projection_grouping_and_arithmetic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            manifest = milestone.generate(root, 3, [8])
            rows = [json.loads(line) for line in (root / "width-8.jsonl").read_text().splitlines()]
            cases = {case["kind"]: case for case in milestone.definitions(manifest)}
            for kind, expected in {
                "nested": [[3, 3]],
                "direct": [[3, 3]],
                "add": [[2.25]],
                "multiply": [[1.5]],
                "add16": [[24.75]],
                "projection": [[row["v"], row["payload"]] for row in rows],
                "groups": [[row["v"], 1] for row in rows],
            }.items():
                case = cases[kind]
                self.assertEqual(milestone.expected(root, case), explore.digest_rows(case, expected), kind)


if __name__ == "__main__":
    unittest.main()
