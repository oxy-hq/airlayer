#!/bin/bash
# Predict: propagate hypothetical changes through the metric tree
# Uses different driver forms to estimate downstream impact
cd "$(dirname "$0")"

echo "=== What if total orders increase by 100? (linear) ==="
echo ""
../../target/debug/airlayer predict --if orders.total_orders=100
echo ""

echo "=== What if sessions double (+28 sessions)? (linear-log → orders, orders → revenue) ==="
echo ""
../../target/debug/airlayer predict --if traffic.sessions=28
echo ""

echo "=== What if take rate increases by 0.02 (2pp)? (log-linear → revenue) ==="
echo ""
../../target/debug/airlayer predict --if orders.take_rate=0.02
echo ""

echo "=== Combined scenario: more traffic + better conversion ==="
echo ""
../../target/debug/airlayer predict --if traffic.sessions=10 --if traffic.conversions=5
