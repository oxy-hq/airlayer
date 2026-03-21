#!/usr/bin/env bash
# Same events query compiled to bigquery
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d bigquery \
  --dimensions events.event_type \
  --dimensions events.platform \
  --measures events.total_events \
  --measures events.total_revenue \
  --measures events.purchase_count \
  --filter events.platform:equals:web \
  --order events.total_revenue:desc \
  --limit 5
