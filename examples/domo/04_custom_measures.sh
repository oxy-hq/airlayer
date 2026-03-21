#!/usr/bin/env bash
# Custom measures — avg views per post and engagement rate by channel (JSON query)
o3 query --path "$(dirname "$0")" -d domo -q '{
  "dimensions": ["content_performance.channel"],
  "measures": [
    "content_performance.avg_views_per_post",
    "content_performance.engagement_rate",
    "content_performance.post_count"
  ],
  "order": [{"id": "content_performance.post_count", "desc": true}],
  "limit": 10
}'
