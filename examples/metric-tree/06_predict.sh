#!/usr/bin/env bash
# Predict the impact of hypothetical changes on upstream metrics.
# Deltas propagate upward through the metric tree using declared coefficients.
#
# --time/--period supplies the current levels. Every form except `linear` is a
# statement about a PROPORTIONAL move (an elasticity, a log-point), so without a
# baseline to take the proportion against there is no size to report — those
# impacts come back as "unquantifiable" rather than as a linear guess.
set -euo pipefail
cd "$(dirname "$0")"

WINDOW=(--time revenue.created_at --period 2024-01-01:2024-02-29)

echo "=== What happens if churn_rate increases by 1%? ==="
airlayer predict --if revenue.churn_rate=0.01 "${WINDOW[@]}"

echo ""
echo "=== What happens if new_mrr increases by \$5K AND churn_rate drops 0.5%? ==="
airlayer predict --if revenue.new_mrr=5000 --if revenue.churn_rate=-0.005 "${WINDOW[@]}"

echo ""
echo "=== JSON output ==="
airlayer predict --if revenue.churn_rate=0.01 "${WINDOW[@]}" --json
