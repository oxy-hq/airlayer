#!/usr/bin/env bash
# Using JSON query input instead of CLI flags
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  -q '{
    "dimensions": ["warehouses.warehouse_name", "warehouses.region_code"],
    "measures": ["shipments.total_shipments", "shipments.unique_shipments", "shipments.avg_weight"],
    "filters": [
      {"member": "warehouses.country", "operator": "equals", "values": ["US"]}
    ],
    "order": [{"id": "shipments.total_shipments", "desc": true}],
    "limit": 10
  }'
