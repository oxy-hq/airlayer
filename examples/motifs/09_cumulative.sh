#!/usr/bin/env bash
# Cumulative: running total of revenue over time.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "cumulative"
}'
