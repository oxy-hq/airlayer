#!/usr/bin/env bash
# Contribution analysis: what share of total revenue does each region contribute?
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml \
  --dimensions daily_sales.region \
  --measures daily_sales.total_revenue \
  --motif contribution
