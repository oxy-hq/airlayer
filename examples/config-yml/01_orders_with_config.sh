#!/usr/bin/env bash
# Query orders using config.yml for dialect resolution (Oxy-compatible)
# The orders view has datasource: warehouse, which maps to postgres in config.yml
set -euo pipefail
cd "$(dirname "$0")"

airlayer query -c config.yml \
  --dimensions orders.status \
  --measures orders.total_revenue \
  --measures orders.total_orders \
  --order orders.total_revenue:desc \
  --limit 10
