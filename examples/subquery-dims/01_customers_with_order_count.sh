#!/usr/bin/env bash
# Customer names with their total order count (subquery dimension)
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d postgres \
  --dimensions customers.name \
  --dimensions customers.total_orders \
  --order customers.total_orders:desc
