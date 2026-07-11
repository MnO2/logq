#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

python3 "$SCRIPT_DIR/benchmark.py" "$@"
python3 "$SCRIPT_DIR/format_results.py" --output "$SCRIPT_DIR/results/table.md"
echo "Wrote $SCRIPT_DIR/results/table.md"
