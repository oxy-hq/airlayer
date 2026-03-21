#!/usr/bin/env bash
# Shipments broken down by channel (Retail, Wholesale, Direct, etc.)
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d clickhouse \
  --dimensions shipments.channel \
  --measures shipments.total_shipments \
  --measures shipments.avg_weight \
  --measures shipments.avg_transit_days \
  --order shipments.total_shipments:desc
