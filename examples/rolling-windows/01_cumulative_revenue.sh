#!/usr/bin/env bash
# Cumulative (running total) revenue by sale date
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d postgres \
  --dimensions daily_sales.sale_date \
  --measures daily_sales.cumulative_revenue \
  --order daily_sales.sale_date:asc
