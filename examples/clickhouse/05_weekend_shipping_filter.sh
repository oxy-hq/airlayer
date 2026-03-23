#!/usr/bin/env bash
# Query-level filter: only weekend days, with auto-join to warehouses
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions warehouses.warehouse_name \
  --measures shipping_daily.total_net_revenue \
  --measures shipping_daily.total_surcharges \
  --measures shipping_daily.total_weight \
  --filter shipping_daily.is_weekend:equals:true \
  --order shipping_daily.total_net_revenue:desc \
  --limit 10
