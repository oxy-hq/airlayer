#!/usr/bin/env bash
# Profit and profit margin by category using measure-to-measure references
set -euo pipefail
cd "$(dirname "$0")"

o3 query -d postgres \
  --dimensions financials.category \
  --measures financials.profit \
  --measures financials.profit_margin \
  --order financials.profit:desc
