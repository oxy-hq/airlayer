#!/usr/bin/env bash
# Brand performance with filter on channel
o3 query --path "$(dirname "$0")" \
  --dimensions content_performance.brand \
  --measures content_performance.total_views \
  --measures content_performance.total_watch_minutes \
  --filter content_performance.channel:equals:Youtube \
  --order content_performance.total_views:desc \
  --limit 20
