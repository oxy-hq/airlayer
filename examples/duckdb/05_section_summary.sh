#!/usr/bin/env bash
# Sections within courses
set -euo pipefail
cd "$(dirname "$0")"

o3 query \
  --dimensions enrollments.course_name \
  --dimensions enrollments.section_name \
  --dimensions enrollments.section_status \
  --measures enrollments.unique_students \
  --measures enrollments.avg_credits \
  --order enrollments.course_name:asc \
  --order enrollments.unique_students:desc
