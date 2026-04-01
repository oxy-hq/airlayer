#!/usr/bin/env bash
# Engagement breakdown by content type
airlayer query \
  --dimensions content_performance.content_type \
  --measures content_performance.total_engagements \
  --measures content_performance.engagement_rate \
  --order content_performance.total_engagements:desc
