#!/usr/bin/env python3
"""Extract SQL-bearing Rust string literals into the parser fuzz corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUTS = (ROOT / "src", ROOT / "tests", ROOT / "benches")
DEFAULT_OUTPUT = ROOT / "fuzz" / "corpus" / "parse_query"
RAW_STRING = re.compile(r'r(?P<hashes>#{0,8})"(?P<body>.*?)"(?P=hashes)', re.DOTALL)
NORMAL_STRING = re.compile(r'"(?:\\.|[^"\\])*"', re.DOTALL)
SQL = re.compile(r"\b(?:select|with)\b", re.IGNORECASE)


def rust_strings(source: str) -> list[str]:
    strings = [match.group("body") for match in RAW_STRING.finditer(source)]
    without_raw = RAW_STRING.sub("", source)
    for match in NORMAL_STRING.finditer(without_raw):
        try:
            strings.append(json.loads(match.group()))
        except json.JSONDecodeError:
            # Rust has escape forms JSON does not. Those literals are uncommon in
            # query tests and are safe to leave out of a seed corpus.
            continue
    return strings


def collect(inputs: tuple[Path, ...]) -> list[str]:
    queries: set[str] = set()
    for directory in inputs:
        for path in directory.rglob("*.rs"):
            for value in rust_strings(path.read_text(encoding="utf-8")):
                value = value.strip()
                if SQL.search(value) and len(value.encode()) <= 4096:
                    queries.add(value)
    return sorted(queries)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    for old_seed in args.output.iterdir():
        if old_seed.is_file():
            old_seed.unlink()

    queries = collect(DEFAULT_INPUTS)
    for query in queries:
        digest = hashlib.sha256(query.encode()).hexdigest()[:16]
        (args.output / f"{digest}.sql").write_text(query, encoding="utf-8")
    print(f"wrote {len(queries)} parser seeds to {args.output.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
