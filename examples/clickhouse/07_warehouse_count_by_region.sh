#!/usr/bin/env bash
# Single-view query: warehouse counts by region
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions warehouses.region_code \
  --dimensions warehouses.country \
  --measures warehouses.warehouse_count \
  --order warehouses.warehouse_count:desc
