#!/usr/bin/env bash
# Rank categories by total revenue (highest first).
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --path . --config config.yml \
  --dimensions daily_sales.category \
  --measures daily_sales.total_revenue \
  --motif rank
