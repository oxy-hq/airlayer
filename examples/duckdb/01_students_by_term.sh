#!/usr/bin/env bash
# Student count and credit allocation by term and course status
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions enrollments.term \
  --dimensions enrollments.course_status \
  --measures enrollments.unique_students \
  --measures enrollments.avg_credits \
  --order enrollments.unique_students:desc
