#!/usr/bin/env bash
# Cross-view join: orders + customers via config.yml
# Both views use datasource: warehouse → postgres
set -euo pipefail
cd "$(dirname "$0")"

airlayer query -c config.yml \
  --dimensions customers.name \
  --measures orders.total_revenue \
  --order orders.total_revenue:desc \
  --limit 20
