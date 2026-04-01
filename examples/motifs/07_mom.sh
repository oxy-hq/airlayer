#!/usr/bin/env bash
# Month-over-month: compare each month's revenue to the previous month.
# With only 10 days of data this produces a single month, so growth_rate is NULL.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "month"}],
  "motif": "mom"
}'
