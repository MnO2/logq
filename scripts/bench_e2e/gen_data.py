#!/usr/bin/env python3
"""Generate reproducible end-to-end benchmark datasets.

The default invocation writes 100 MB and 1 GB ELB, ALB, and JSONL files plus
deterministic gzip copies. Files are grown to at least the requested byte size;
the manifest records their exact size, row count, and SHA-256 digest.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


SEED = 20_260_711
DEFAULT_SIZES = "100mb,1gb"
STATUSES = (200, 200, 200, 201, 204, 301, 404, 429, 500, 502, 503)
AGENTS = (
    "Mozilla/5.0 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 Firefox/126.0",
    "curl/8.7.1",
    "Googlebot/2.1",
    "logq-benchmark-client/1.0",
)
BASE_TIME = datetime(2026, 7, 11, tzinfo=timezone.utc)


def parse_size(value: str) -> int:
    value = value.strip().lower()
    suffixes = {"kb": 1024, "mb": 1024**2, "gb": 1024**3}
    for suffix, multiplier in suffixes.items():
        if value.endswith(suffix):
            number = value[: -len(suffix)]
            if not number.isdigit() or int(number) <= 0:
                break
            return int(number) * multiplier
    raise argparse.ArgumentTypeError(
        f"invalid size {value!r}; expected a positive integer with kb, mb, or gb"
    )


def timestamp(index: int) -> str:
    value = BASE_TIME + timedelta(milliseconds=(index * 137) % 86_400_000)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def values(index: int) -> dict[str, object]:
    shifted = index + SEED
    status = STATUSES[shifted % len(STATUSES)]
    latency = round(((shifted * 17) % 50_000) / 10_000, 4)
    return {
        "timestamp": timestamp(index),
        "request_id": f"req-{index:012d}",
        "method": ("GET", "POST", "PUT", "DELETE")[shifted % 4],
        "path": f"/api/v1/resource/{shifted % 10_000}",
        "status_code": status,
        "latency": latency,
        "bytes": 128 + ((shifted * 7919) % 2_000_000),
        "user_agent": AGENTS[shifted % len(AGENTS)],
        "region": ("us-west-2", "us-east-1", "eu-west-1")[shifted % 3],
    }


def jsonl_row(index: int) -> bytes:
    return (json.dumps(values(index), separators=(",", ":")) + "\n").encode()


def elb_row(index: int) -> bytes:
    row = values(index)
    status = row["status_code"]
    line = (
        f'{row["timestamp"]} elb-benchmark '
        f'192.0.2.{index % 250 + 1}:{10_000 + index % 50_000} '
        f'10.0.{index % 16}.{index % 250 + 1}:80 '
        f'0.0001 {row["latency"]} 0.0002 {status} {status} '
        f'0 {row["bytes"]} "{row["method"]} https://example.test{row["path"]} HTTP/1.1" '
        f'"{row["user_agent"]}" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n'
    )
    return line.encode()


def alb_row(index: int) -> bytes:
    row = values(index)
    status = row["status_code"]
    line = (
        f'https {row["timestamp"]} app/logq-benchmark/0123456789abcdef '
        f'192.0.2.{index % 250 + 1}:{10_000 + index % 50_000} '
        f'10.0.{index % 16}.{index % 250 + 1}:80 '
        f'0.0001 {row["latency"]} 0.0002 {status} {status} 0 {row["bytes"]} '
        f'"{row["method"]} https://example.test{row["path"]} HTTP/1.1" '
        f'"{row["user_agent"]}" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 '
        'arn:aws:elasticloadbalancing:us-west-2:000000000000:targetgroup/benchmark/0123456789abcdef '
        f'"Root=1-{index:08x}-0123456789abcdef" "example.test" '
        '"arn:aws:acm:us-west-2:000000000000:certificate/benchmark" 1 '
        f'{row["timestamp"]} "forward" "-" "-"\n'
    )
    return line.encode()


GENERATORS = {"elb": elb_row, "alb": alb_row, "jsonl": jsonl_row}
EXTENSIONS = {"elb": "log", "alb": "log", "jsonl": "jsonl"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_dataset(path: Path, target_size: int, generator) -> int:
    rows = 0
    with path.open("wb") as output:
        while output.tell() < target_size:
            output.write(generator(rows))
            rows += 1
    return rows


def write_gzip(source: Path, destination: Path) -> None:
    with source.open("rb") as input_file, destination.open("wb") as raw_output:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw_output, compresslevel=6, mtime=0
        ) as output:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                output.write(chunk)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "data")
    parser.add_argument(
        "--sizes",
        default=DEFAULT_SIZES,
        help="comma-separated sizes such as 100mb,1gb (default: %(default)s)",
    )
    args = parser.parse_args()

    requested_sizes = []
    for label in args.sizes.split(","):
        label = label.strip().lower()
        requested_sizes.append((label, parse_size(label)))

    args.output.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {"seed": SEED, "files": []}
    files: list[dict[str, object]] = []
    for label, byte_count in requested_sizes:
        for format_name, generator in GENERATORS.items():
            extension = EXTENSIONS[format_name]
            path = args.output / f"{format_name}-{label}.{extension}"
            print(f"generating {path} (at least {byte_count:,} bytes)", flush=True)
            rows = write_dataset(path, byte_count, generator)
            gz_path = Path(f"{path}.gz")
            write_gzip(path, gz_path)
            for generated_path, compressed in ((path, False), (gz_path, True)):
                files.append(
                    {
                        "path": generated_path.name,
                        "format": format_name,
                        "scale": label,
                        "compressed": compressed,
                        "rows": rows,
                        "bytes": generated_path.stat().st_size,
                        "sha256": sha256(generated_path),
                    }
                )

    manifest["files"] = files
    manifest_path = args.output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {manifest_path}")


if __name__ == "__main__":
    main()
