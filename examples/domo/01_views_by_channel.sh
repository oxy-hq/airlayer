#!/usr/bin/env bash
# Views by channel — basic dimension + measure query
o3 query --path "$(dirname "$0")" -d domo \
  --dimensions content_performance.channel \
  --measures content_performance.total_views \
  --order content_performance.total_views:desc \
  --limit 10
