#!/usr/bin/env bash
# Filter: new accounts vs existing account revenue
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d snowflake \
  --dimensions subscriptions.account_type \
  --dimensions subscriptions.market \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --order subscriptions.total_revenue:desc
