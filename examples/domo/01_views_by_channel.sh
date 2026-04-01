#!/usr/bin/env bash
# Views by channel — basic dimension + measure query
airlayer query \
  --dimensions content_performance.channel \
  --measures content_performance.total_views \
  --order content_performance.total_views:desc \
  --limit 10
