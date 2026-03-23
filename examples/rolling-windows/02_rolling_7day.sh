#!/usr/bin/env bash
# Rolling 7-day revenue alongside daily revenue by sale date
set -euo pipefail
cd "$(dirname "$0")"

airlayer query \
  --dimensions daily_sales.sale_date \
  --measures daily_sales.daily_revenue \
  --measures daily_sales.rolling_7day_revenue \
  --order daily_sales.sale_date:asc
