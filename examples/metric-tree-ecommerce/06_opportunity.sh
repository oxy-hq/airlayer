#!/bin/bash
# Opportunity sizing: find underperforming segments across the marketplace
cd "$(dirname "$0")"

echo "=== Where is the biggest opportunity in take rate? ==="
echo ""
airlayer opportunity orders.take_rate \
  --time orders.order_date \
  --period 2024-02-01:2024-02-28

echo ""
echo "=== Where is the biggest opportunity in AOV? ==="
echo ""
airlayer opportunity orders.aov \
  --time orders.order_date \
  --period 2024-02-01:2024-02-28
