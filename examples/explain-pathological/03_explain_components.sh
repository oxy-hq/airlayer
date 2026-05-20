#!/usr/bin/env bash
# Explain on total_revenue.total — composite metric backed by product + service lines.
#
# The data is designed so the TRUE root cause is region=EU dropping across
# BOTH product and service lines. The greedy algorithm faces a choice:
#   - Split by component first → finds product_revenue dropped and
#     service_revenue dropped, but may not surface that BOTH dropped
#     because of EU
#   - Split by dimension first → immediately finds region=EU
#
# Additionally, we explain each leaf independently to compare.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Explain: total_revenue.total (Jan → Feb) ==="
echo ""
echo "total_revenue sits atop product_revenue + service_revenue."
echo "The composite type:number measure (total_revenue.amount) forms metric tree edges"
echo "but the explainable measure is total_revenue.total (a direct SUM)."
echo ""
airlayer explain total_revenue.total \
  --time total_revenue.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Explain: product_revenue.amount (leaf) ==="
echo ""
airlayer explain product_revenue.amount \
  --time product_revenue.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Explain: service_revenue.amount (leaf) ==="
echo ""
airlayer explain service_revenue.amount \
  --time service_revenue.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Deep explain: total_revenue.total ==="
echo ""
airlayer explain total_revenue.total \
  --time total_revenue.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --deep
