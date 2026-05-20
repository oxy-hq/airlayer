#!/usr/bin/env bash
# Deep explain on sales.revenue — multi-strategy beam search.
#
# Unlike greedy, the deep search runs 4 scoring strategies in parallel:
#   1. max_concentration — single highest-impact segment
#   2. topk_concentration — top-K segments by concentration
#   3. jsd_smoothed — Laplace-smoothed Jensen-Shannon divergence
#   4. iv_woe — Information Value / Weight of Evidence
#
# This should surface plan=enterprise as a top-ranked alternative even
# when greedy picks a different dimension, because the beam search
# explores multiple paths simultaneously.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Deep explain: sales.revenue (Jan → Feb) ==="
echo ""
airlayer explain sales.revenue \
  --time sales.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --deep

echo ""
echo "=== Deep explain, JSON (beam-width=15, max-alternatives=8) ==="
echo ""
airlayer explain sales.revenue \
  --time sales.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --deep --beam-width 15 --max-alternatives 8 \
  --json | python3 -m json.tool
