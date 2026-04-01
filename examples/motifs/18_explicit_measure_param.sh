#!/usr/bin/env bash
# Explicit motif params: when a query has multiple measures, you must specify
# which measure the motif operates on via --motif-param.
#
# With a single measure, the motif auto-binds — no params needed.
# With multiple measures, omitting --motif-param produces a clear error.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Single measure (auto-binds) ==="
echo "Ranks regions by total_revenue — no --motif-param needed"
echo ""

airlayer query --execute --config config.yml \
  --dimension daily_sales.region \
  --measure daily_sales.total_revenue \
  --motif rank

echo ""
echo "=== Multiple measures + explicit param (--motif-param) ==="
echo "Ranks regions by total_orders (not total_revenue)"
echo ""

airlayer query --execute --config config.yml \
  --dimension daily_sales.region \
  --measure daily_sales.total_revenue --measure daily_sales.total_orders \
  --motif rank \
  --motif-param measure=daily_sales.total_orders
