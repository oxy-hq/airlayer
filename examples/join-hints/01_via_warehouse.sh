#!/usr/bin/env bash
# Orders to shipments routed through warehouse fulfillment
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --measures orders.total_orders \
  --measures shipments.shipment_count \
  --through warehouse_order
