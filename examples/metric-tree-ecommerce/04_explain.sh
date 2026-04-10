#!/bin/bash
# Explain: recursive root-cause analysis for why revenue changed
# The data has a deliberate pattern: EU orders collapsed in Feb
# (home + electronics tanked, clothing was already small)
cd "$(dirname "$0")"

echo "=== Why did revenue change Jan → Feb? ==="
echo ""
../../target/debug/airlayer explain orders.revenue \
  --time orders.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Same analysis, JSON output ==="
echo ""
../../target/debug/airlayer explain orders.revenue \
  --time orders.order_date \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --json | python3 -m json.tool | head -40
