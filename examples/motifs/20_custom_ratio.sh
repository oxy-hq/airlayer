#!/usr/bin/env bash
# Custom motif with two measure params: ratio
# Defined in motifs/ratio.motif.yml
#
# This demonstrates a motif where two measures play different roles
# (numerator vs denominator), which is only possible with explicit params.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Custom motif: ratio (revenue per order by region) ==="
echo "numerator = total_revenue, denominator = total_orders"
echo ""

airlayer query --execute --config config.yml \
  --dimension daily_sales.region \
  --measure daily_sales.total_revenue --measure daily_sales.total_orders \
  --motif ratio \
  --motif-param numerator=daily_sales.total_revenue \
  --motif-param denominator=daily_sales.total_orders

echo ""
echo "=== Same motif, different params: orders per customer ==="
echo "numerator = total_orders, denominator = total_customers"
echo ""

airlayer query --execute --config config.yml \
  --dimension daily_sales.region \
  --measure daily_sales.total_orders --measure daily_sales.total_customers \
  --motif ratio \
  --motif-param numerator=daily_sales.total_orders \
  --motif-param denominator=daily_sales.total_customers
