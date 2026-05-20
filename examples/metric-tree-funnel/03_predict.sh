#!/bin/bash
# Predict: opportunity sizing — what if we fix the Address step?
cd "$(dirname "$0")"

echo "=== What if amenities_to_address_rate improves by 0.10 (10pp)? ==="
echo "    (Fixing Android+India would move overall rate from ~75% toward ~80%)"
echo ""
airlayer predict --if funnel.amenities_to_address_rate=0.10

echo ""
echo "=== What if we add 50 more HLP visitors? (linear) ==="
echo ""
airlayer predict --if funnel.hlp_visitors=50

echo ""
echo "=== What if activation rate improves by 0.05 (5pp)? (log-linear) ==="
echo ""
airlayer predict --if funnel.activation_rate=0.05

echo ""
echo "=== Combined: fix address conversion + grow traffic ==="
echo ""
airlayer predict \
  --if funnel.amenities_to_address_rate=0.10 \
  --if funnel.hlp_visitors=100
