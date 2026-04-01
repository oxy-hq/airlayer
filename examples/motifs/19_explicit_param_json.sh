#!/usr/bin/env bash
# Explicit motif params via JSON query — same concept as --motif-param,
# but using the -q JSON format with "motif_params".
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Contribution analysis on total_orders (JSON) ==="
echo "Query has two measures; motif_params picks which one to analyze"
echo ""

airlayer query --execute --config config.yml -q '{
  "dimensions": ["daily_sales.region"],
  "measures": ["daily_sales.total_revenue", "daily_sales.total_orders"],
  "motif": "contribution",
  "motif_params": {"measure": "daily_sales.total_orders"}
}'

echo ""
echo "=== Anomaly detection on total_revenue with custom threshold (JSON) ==="
echo "Explicit measure param + anomaly threshold override"
echo ""

airlayer query --execute --config config.yml -q '{
  "dimensions": ["daily_sales.category"],
  "measures": ["daily_sales.total_revenue", "daily_sales.total_customers"],
  "motif": "anomaly",
  "motif_params": {"measure": "daily_sales.total_revenue", "threshold": 1.5}
}'
