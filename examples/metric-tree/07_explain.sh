#!/usr/bin/env bash
# Explain why ARR changed between January and February.
# Walks the metric tree backward from revenue.arr, generating comparison
# queries for each component and driver. Executes them against DuckDB
# using the CSV files in data/.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Explain plan (SQL only) ==="
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --granularity month \
  --dimension revenue.plan \
  --dimension revenue.region \
  2>&1 | head -40

echo ""
echo "=== Executed against DuckDB ==="
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --granularity month \
  --dimension revenue.plan \
  --dimension revenue.region \
  -x
