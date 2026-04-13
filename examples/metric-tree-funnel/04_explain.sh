#!/bin/bash
# Explain: root-cause analysis for the Feb address conversion regression
# The data has a deliberate pattern: Android+India address conversion drops to 0% in Feb
cd "$(dirname "$0")"

echo "=== Why did amenities_to_address_rate drop Jan → Feb? ==="
echo "    (Decomposes into components → address_completes → android → India)"
echo ""
airlayer explain funnel.amenities_to_address_rate \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Decompose address_completes Jan → Feb ==="
echo ""
airlayer explain funnel.address_completes \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Full active listings decomposition ==="
echo ""
airlayer explain funnel.active_listings \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Same analysis, JSON output ==="
echo ""
airlayer explain funnel.active_listings \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31 \
  --json | python3 -m json.tool | head -40
