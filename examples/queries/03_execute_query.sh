#!/usr/bin/env bash
# Execute a saved query against DuckDB (requires data/sales.duckdb).
#
# This runs every step in the query and returns a JSON envelope per step.
# Each step's envelope contains status, sql, columns, data, and row_count.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Execute revenue_investigation ==="
airlayer query queries/revenue_investigation.query.yml --config config.yml -x

echo ""
echo "=== Execute platform_comparison ==="
airlayer query queries/platform_comparison.query.yml --config config.yml -x
