#!/usr/bin/env bash
# Credit breakdown by department hierarchy
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d duckdb \
  --dimensions enrollments.department \
  --dimensions enrollments.sub_department \
  --measures enrollments.unique_students \
  --measures enrollments.credit_hours \
  --measures enrollments.unique_courses \
  --order enrollments.unique_students:desc
