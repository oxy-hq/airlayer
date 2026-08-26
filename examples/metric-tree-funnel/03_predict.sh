#!/bin/bash
# Predict: opportunity sizing — what if we fix the Address step?
#
# --time/--period supplies the current levels. Every form except `linear` is a
# statement about a PROPORTIONAL move (an elasticity, a log-point), so without a
# baseline to take the proportion against there is no size to report — those
# impacts come back as "unquantifiable" rather than as a linear guess.
cd "$(dirname "$0")"

WINDOW=(--time funnel.event_date --period 2024-01-01:2024-02-29)

echo "=== What if amenities_to_address_rate improves by 0.10 (10pp)? ==="
echo "    (Fixing Android+India would move overall rate from ~75% toward ~80%)"
echo ""
airlayer predict --if funnel.amenities_to_address_rate=0.10 "${WINDOW[@]}"

echo ""
echo "=== What if we add 50 more HLP visitors? (linear) ==="
echo ""
airlayer predict --if funnel.hlp_visitors=50 "${WINDOW[@]}"

echo ""
echo "=== What if activation rate improves by 0.05 (5pp)? (log-linear) ==="
echo ""
airlayer predict --if funnel.activation_rate=0.05 "${WINDOW[@]}"

echo ""
echo "=== What if calendar completions grow by 100? (linear-log) ==="
echo ""
airlayer predict --if funnel.calendar_completes=100 "${WINDOW[@]}"

echo ""
echo "=== Combined: fix address conversion + grow traffic ==="
echo ""
airlayer predict \
  --if funnel.amenities_to_address_rate=0.10 \
  --if funnel.hlp_visitors=100 \
  "${WINDOW[@]}"
