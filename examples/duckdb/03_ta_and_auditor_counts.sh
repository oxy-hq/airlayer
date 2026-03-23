#!/usr/bin/env bash
# Measure-level filters: ta_count and auditor_count use CASE WHEN
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions enrollments.term \
  --measures enrollments.total_enrollments \
  --measures enrollments.ta_count \
  --measures enrollments.auditor_count \
  --measures enrollments.active_enrollment_count \
  --order enrollments.term:asc
