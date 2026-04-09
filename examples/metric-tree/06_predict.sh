#!/usr/bin/env bash
# Predict the impact of hypothetical changes on upstream metrics.
# Deltas propagate upward through the metric tree using declared coefficients.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== What happens if churn_rate increases by 1%? ==="
airlayer predict --if revenue.churn_rate=0.01

echo ""
echo "=== What happens if new_mrr increases by \$5K AND churn_rate drops 0.5%? ==="
airlayer predict --if revenue.new_mrr=5000 --if revenue.churn_rate=-0.005

echo ""
echo "=== JSON output ==="
airlayer predict --if revenue.churn_rate=0.01 --json
