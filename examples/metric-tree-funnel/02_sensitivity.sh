#!/bin/bash
# Sensitivity analysis: which funnel steps have the most leverage on active listings?
cd "$(dirname "$0")"

echo "=== Sensitivity: funnel.new_active_listings ==="
echo ""
airlayer sensitivity funnel.new_active_listings

echo ""
echo "=== Sensitivity: funnel.amenities_to_address_rate ==="
echo ""
airlayer sensitivity funnel.amenities_to_address_rate

echo ""
echo "=== Sensitivity: funnel.overall_conversion ==="
echo ""
airlayer sensitivity funnel.overall_conversion
