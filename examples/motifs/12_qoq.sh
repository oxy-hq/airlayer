#!/usr/bin/env bash
# Quarter-over-quarter: compare each quarter's revenue to the prior quarter.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "quarter"}],
  "motif": "qoq"
}'
