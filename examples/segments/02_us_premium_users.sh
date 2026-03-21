#!/usr/bin/env bash
# Premium and total user counts for active US-based users (two segments)
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d postgres \
  --dimensions users.plan \
  --measures users.premium_users \
  --measures users.user_count \
  --segments users.active \
  --segments users.us_based \
  --order users.user_count:desc
