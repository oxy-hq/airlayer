#!/usr/bin/env bash
# Trend: linear regression on daily revenue (slope, intercept, trend_value).
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "trend"
}'
