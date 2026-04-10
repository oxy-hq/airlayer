#!/bin/bash
# Sensitivity analysis: which drivers influence revenue most?
# Shows all four driver forms: linear, log-log, log-linear, linear-log
cd "$(dirname "$0")"
echo "=== Sensitivity: orders.revenue ==="
echo ""
../../target/debug/airlayer sensitivity orders.revenue
echo ""
echo "=== Sensitivity: traffic.conversion_rate ==="
echo ""
../../target/debug/airlayer sensitivity traffic.conversion_rate
echo ""
echo "=== Sensitivity: sellers.listings_per_seller ==="
echo ""
../../target/debug/airlayer sensitivity sellers.listings_per_seller
