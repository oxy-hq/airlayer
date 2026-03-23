#!/usr/bin/env bash
# Same events query compiled to domo
set -euo pipefail
cd "$(dirname "$0")"

airlayer query -d domo \
  --dimensions events.event_type \
  --dimensions events.platform \
  --measures events.total_events \
  --measures events.total_revenue \
  --measures events.purchase_count \
  --filter events.platform:equals:web \
  --order events.total_revenue:desc \
  --limit 5
