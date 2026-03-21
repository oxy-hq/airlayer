#!/usr/bin/env bash
# Measure-level filters: completed_shipments and returned_shipments use CASE WHEN
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d clickhouse \
  --dimensions shipments.channel \
  --measures shipments.completed_shipments \
  --measures shipments.returned_shipments \
  --measures shipments.total_shipments \
  --order shipments.total_shipments:desc
