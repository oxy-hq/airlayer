#!/usr/bin/env bash
# Auto-join: manager names from account_managers joined to subscription data
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d snowflake \
  --dimensions account_managers.manager_name \
  --dimensions account_managers.market \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --order subscriptions.total_revenue:desc \
  --limit 10
