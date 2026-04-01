#!/usr/bin/env bash
# Execute a sequence against DuckDB (requires data/sales.duckdb).
#
# This runs every step in the sequence and returns a JSON envelope per step.
# Each step's envelope contains status, sql, columns, data, and row_count.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Execute revenue_investigation ==="
airlayer sequence run revenue_investigation --path . --config config.yml -x

echo ""
echo "=== Execute platform_comparison ==="
airlayer sequence run platform_comparison --path . --config config.yml -x
