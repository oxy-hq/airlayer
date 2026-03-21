#!/usr/bin/env bash
# Average revenue per transaction alongside total transaction count
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d postgres \
  --dimensions financials.category \
  --measures financials.avg_revenue_per_transaction \
  --measures financials.total_transactions \
  --order financials.category:asc
