#!/usr/bin/env bash
# Custom measures: premium subscription count and average revenue per sub
# value_bucket uses {{monthly_value}} self-referencing dimension
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions subscriptions.value_bucket \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --measures subscriptions.premium_subscription_count \
  --measures subscriptions.avg_revenue_per_sub \
  --order subscriptions.total_revenue:desc
