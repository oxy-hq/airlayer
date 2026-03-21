#!/usr/bin/env bash
# Auto-join: shipment metrics by warehouse name and city
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions warehouses.warehouse_name \
  --dimensions warehouses.city \
  --measures shipments.total_shipments \
  --measures shipments.total_weight \
  --measures shipments.avg_weight \
  --order shipments.total_shipments:desc \
  --limit 10
