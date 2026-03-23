#!/usr/bin/env bash
# Active users broken down by plan type (using segment filter)
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions users.plan \
  --measures users.user_count \
  --segments users.active \
  --order users.user_count:desc
