#!/usr/bin/env bash
# Custom motif: 3-day rolling sum of revenue
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml -q '{
  "measures": ["daily_sales.total_revenue"],
  "time_dimensions": [{"dimension": "daily_sales.date", "granularity": "day"}],
  "motif": "rolling_sum",
  "motif_params": {"window": 2}
}'
