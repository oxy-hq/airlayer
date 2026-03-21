#!/usr/bin/env bash
# Revenue broken down by market and vertical
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d snowflake \
  --dimensions subscriptions.market \
  --dimensions subscriptions.vertical \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --order subscriptions.total_revenue:desc \
  --limit 20
