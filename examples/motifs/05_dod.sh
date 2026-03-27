#!/usr/bin/env bash
# Day-over-day: compare each day's revenue to the previous day.
# Shows previous_value and growth_rate columns.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "dod"
}'
