#!/usr/bin/env bash
# Shipping aggregated by year and month
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions shipping_daily.ship_year \
  --dimensions shipping_daily.ship_month \
  --measures shipping_daily.total_revenue \
  --measures shipping_daily.total_net_revenue \
  --measures shipping_daily.total_refunds \
  --measures shipping_daily.days_count \
  --order shipping_daily.ship_year:asc \
  --order shipping_daily.ship_month:asc
