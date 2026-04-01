#!/usr/bin/env bash
# Week-over-week: compare each week's revenue to the previous week.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "week"}],
  "motif": "wow"
}'
