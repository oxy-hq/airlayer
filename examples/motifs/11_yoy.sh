#!/usr/bin/env bash
# Year-over-year: compare each year's revenue to the prior year.
# With only 10 days of data this produces 1 year, so growth_rate is NULL.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "year"}],
  "motif": "yoy"
}'
