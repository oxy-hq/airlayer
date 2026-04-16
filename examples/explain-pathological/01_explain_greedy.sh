#!/usr/bin/env bash
# Greedy explain on sales.revenue — demonstrates suboptimal behavior.
#
# The data is designed so the TRUE root cause is plan=enterprise dropping,
# but the greedy algorithm gets distracted by:
#   - channel proportion swaps (high JSD, low actual impact)
#   - promo_trial vanishing (infinite WOE, tiny volume)
#   - product_sku micro-segments inflating dimension scores
#
# Watch for: the greedy picker may choose channel or product_sku first,
# missing the plan=enterprise signal entirely.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Greedy explain: sales.revenue (Jan → Feb) ==="
echo ""
airlayer explain sales.revenue \
  --time sales.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Same analysis, JSON output ==="
echo ""
airlayer explain sales.revenue \
  --time sales.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --json | python3 -m json.tool
