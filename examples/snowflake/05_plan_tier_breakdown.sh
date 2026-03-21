#!/usr/bin/env bash
# Revenue by plan tier, filtered to a specific market
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d snowflake \
  --dimensions subscriptions.plan_tier \
  --dimensions subscriptions.product_category \
  --measures subscriptions.total_revenue \
  --measures subscriptions.new_revenue \
  --measures subscriptions.active_subscription_count \
  --filter "subscriptions.market:equals:Domestic" \
  --order subscriptions.total_revenue:desc
