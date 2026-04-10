#!/usr/bin/env bash
# Explain why ARR changed between January and February.
# Uses recursive root-cause analysis to find the smallest (component, segment)
# pairs that explain the change. Executes queries against DuckDB using the
# CSV files in data/.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Recursive explain (text) ==="
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Recursive explain (JSON) ==="
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --json
