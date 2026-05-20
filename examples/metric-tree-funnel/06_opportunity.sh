#!/bin/bash
# Opportunity sizing: find underperforming segments and estimate the upside
# The data has a deliberate pattern: Android+India has 0% address conversion in Feb
cd "$(dirname "$0")"

echo "=== Where is the biggest opportunity in address conversion? ==="
echo "    (Should surface platform=android as the top opportunity)"
echo ""
airlayer opportunity funnel.amenities_to_address_rate \
  --time funnel.event_date \
  --period 2024-02-01:2024-02-29

echo ""
echo "=== Opportunity in overall active listings ==="
echo ""
airlayer opportunity funnel.active_listings \
  --time funnel.event_date \
  --period 2024-02-01:2024-02-29

echo ""
echo "=== Same analysis, JSON output ==="
echo ""
airlayer opportunity funnel.amenities_to_address_rate \
  --time funnel.event_date \
  --period 2024-02-01:2024-02-29 \
  --json | python3 -m json.tool | head -30
