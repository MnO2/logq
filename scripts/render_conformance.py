#!/usr/bin/env python3
"""Render the tested SQL examples; --check fails when their documentation drifts."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "tests/conformance/README.md"
MARKER = "<!-- generated conformance cases -->"


def render():
    text = TARGET.read_text(encoding="utf-8")
    prefix = text.split(MARKER, 1)[0].rstrip()
    cases = json.loads((TARGET.parent / "cases.json").read_text(encoding="utf-8"))["cases"]
    rows = [prefix, "", MARKER, "", "## Executable examples", "",
            "Generated from `cases.json`; `cargo test --test conformance` executes every query",
            "against `input.jsonl` and checks its complete expected answer. Regenerate with",
            "`python3 scripts/render_conformance.py`; CI rejects a stale table.", "",
            "| Case | SQL |", "| --- | --- |"]
    for case in cases:
        name = case["name"].replace("|", "\\|")
        # The manifest queries have no literal backticks; fail explicitly if a
        # future grammar addition requires a different Markdown code delimiter.
        query = case["query"]
        if "`" in query or "\n" in query:
            raise ValueError("table queries must be single-line without backticks")
        rows.append("| " + name + " | `" + query.replace("|", "\\|") + "` |")
    return "\n".join(rows) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render()
    if args.check:
        if TARGET.read_text(encoding="utf-8") != generated:
            parser.exit(1, "Conformance docs are stale; run python3 scripts/render_conformance.py\n")
    else:
        TARGET.write_text(generated, encoding="utf-8")


if __name__ == "__main__":
    main()
