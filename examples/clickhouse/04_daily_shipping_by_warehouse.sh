#!/usr/bin/env bash
# Auto-join: daily shipping metrics joined to warehouse locations
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d clickhouse \
  --dimensions warehouses.warehouse_name \
  --dimensions shipping_daily.day_of_week \
  --measures shipping_daily.total_net_revenue \
  --measures shipping_daily.total_weight \
  --measures shipping_daily.avg_parcel_value \
  --order shipping_daily.total_net_revenue:desc \
  --limit 20
