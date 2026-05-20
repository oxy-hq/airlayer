#!/bin/bash
# Explain: root-cause analysis for the Feb address conversion regression
# The data has a deliberate pattern: Android+India address conversion drops to 0% in Feb
cd "$(dirname "$0")"

echo "=== Why did new_active_listings drop Jan → Feb? ==="
echo "    (Component decomposition + driver attribution)"
echo "    Component path finds: active_listings → address_completes → android+India"
echo "    Driver attribution shows: amenities_to_address_rate is the top driver impact"
echo ""
airlayer explain funnel.new_active_listings \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31

echo ""
echo "=== Decompose amenities_to_address_rate directly ==="
echo ""
airlayer explain funnel.amenities_to_address_rate \
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
echo "=== JSON output (includes driver_attribution field) ==="
echo ""
airlayer explain funnel.new_active_listings \
  --time funnel.event_date \
  --current 2024-02-01:2024-02-29 \
  --previous 2024-01-01:2024-01-31 \
  --json | python3 -m json.tool | head -60
