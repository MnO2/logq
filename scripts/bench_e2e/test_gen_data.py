#!/usr/bin/env python3

from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("gen_data.py")


class GeneratorTest(unittest.TestCase):
    def generate(self, output: Path) -> dict:
        subprocess.run(
            [sys.executable, str(SCRIPT), "--output", str(output), "--sizes", "32kb"],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads((output / "manifest.json").read_text())

    def test_generates_reproducible_plain_and_gzip_files(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_manifest = self.generate(Path(first))
            second_manifest = self.generate(Path(second))

            self.assertEqual(first_manifest, second_manifest)
            self.assertEqual(len(first_manifest["files"]), 6)
            for entry in first_manifest["files"]:
                self.assertGreaterEqual(entry["bytes"], 1)
                self.assertGreater(entry["rows"], 0)

            for format_name, extension in (("elb", "log"), ("alb", "log"), ("jsonl", "jsonl")):
                plain = Path(first) / f"{format_name}-32kb.{extension}"
                compressed = Path(f"{plain}.gz")
                self.assertGreaterEqual(plain.stat().st_size, 32 * 1024)
                with plain.open("rb") as source, gzip.open(compressed, "rb") as zipped:
                    self.assertEqual(source.read(), zipped.read())


if __name__ == "__main__":
    unittest.main()
