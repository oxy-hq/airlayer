#!/usr/bin/env bash
# Anomaly detection: find outlier regions in revenue using z-score.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml \
  --dimensions daily_sales.region \
  --measures daily_sales.total_revenue \
  --motif anomaly
