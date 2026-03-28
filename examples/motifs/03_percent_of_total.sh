#!/usr/bin/env bash
# Percent of total: each region's revenue as a percentage of grand total.
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml \
  --dimensions daily_sales.region \
  --measures daily_sales.total_revenue \
  --motif percent_of_total
