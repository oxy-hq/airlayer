#!/usr/bin/env bash
# Revenue trend by month
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d snowflake \
  --dimensions subscriptions.billing_month \
  --dimensions subscriptions.billing_year \
  --measures subscriptions.total_revenue \
  --measures subscriptions.new_revenue \
  --measures subscriptions.active_subscription_count \
  --order subscriptions.billing_month:asc
