#!/usr/bin/env bash
# Custom motif with two measure params: efficiency
# Defined in motifs/efficiency.motif.yml
#
# Compares actual vs baseline measures and outputs efficiency %, gap, and gap %.
# Here we compare total_orders (actual throughput) against total_customers (potential).
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Custom motif: efficiency (conversion rate by category) ==="
echo "actual = total_orders, baseline = total_customers"
echo "efficiency_pct shows orders as % of customers (conversion rate)"
echo ""

airlayer query --execute --config config.yml \
  --dimension daily_sales.category \
  --measure daily_sales.total_orders --measure daily_sales.total_customers \
  --motif efficiency \
  --motif-param actual=daily_sales.total_orders \
  --motif-param baseline=daily_sales.total_customers
