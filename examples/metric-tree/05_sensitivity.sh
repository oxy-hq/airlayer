#!/usr/bin/env bash
# Rank all drivers of ARR by influence magnitude.
# Quantitative drivers (with coefficients) are ranked by |effective_coefficient|;
# qualitative drivers (direction/strength only) appear at the bottom.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Sensitivity analysis for revenue.arr ==="
airlayer sensitivity revenue.arr

echo ""
echo "=== JSON output ==="
airlayer sensitivity revenue.arr --json
