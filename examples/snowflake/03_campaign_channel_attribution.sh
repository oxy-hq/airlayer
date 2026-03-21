#!/usr/bin/env bash
# Auto-join: campaign channel (from campaigns view) with subscription metrics
# The campaigns.campaign_channel dimension uses a CASE expression for normalization
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions campaigns.campaign_channel \
  --measures subscriptions.total_revenue \
  --measures subscriptions.active_subscription_count \
  --measures subscriptions.avg_revenue_per_sub \
  --order subscriptions.total_revenue:desc
