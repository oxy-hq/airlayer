#!/usr/bin/env bash
# Custom motif: index to base period (base = 100)
# Compare how daily revenue grows relative to the first day
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "index"
}'
