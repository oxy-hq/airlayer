#!/usr/bin/env bash
# Course-level allocation with a filter on enrolled courses only
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d duckdb \
  --dimensions enrollments.course_name \
  --dimensions enrollments.course_status \
  --measures enrollments.unique_students \
  --measures enrollments.credit_hours \
  --measures enrollments.avg_credits \
  --filter enrollments.course_status:equals:ENROLLED \
  --order enrollments.credit_hours:desc
