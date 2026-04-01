#!/usr/bin/env bash
# Moving average: 7-day rolling average of daily revenue.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "moving_average"
}'
