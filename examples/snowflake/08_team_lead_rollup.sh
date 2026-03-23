#!/usr/bin/env bash
# Auto-join: revenue rolled up by team lead from the account_managers view
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions account_managers.team_lead \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --measures subscriptions.avg_revenue_per_sub \
  --order subscriptions.total_revenue:desc \
  --limit 15
