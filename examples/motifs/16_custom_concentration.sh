#!/usr/bin/env bash
# Custom motif: HHI concentration analysis by region
# Shows how concentrated revenue is across regions
set -euo pipefail
cd "$(dirname "$0")"

airlayer query --execute --config config.yml \
  --dimensions daily_sales.region \
  --measures daily_sales.total_revenue \
  --motif concentration
