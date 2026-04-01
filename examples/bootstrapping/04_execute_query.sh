#!/usr/bin/env bash
# Step 4: Execute queries and inspect the structured envelope.
#
# The --execute flag compiles + runs the query, returning a JSON envelope
# with status, SQL, column metadata, data, and views_used.
# This is the primary interface for agent iteration.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Revenue by category (executed) ==="
echo ""
airlayer query --execute --config config.yml \
  --dimensions orders.category \
  --measures orders.total_revenue \
  --measures orders.order_count \
  --order orders.total_revenue:desc

echo ""
echo "=== Top customers by region (executed) ==="
echo ""
airlayer query --execute --config config.yml \
  --dimensions orders.region \
  --dimensions orders.customer_name \
  --measures orders.total_revenue \
  --measures orders.order_count \
  --filter orders.status:equals:completed \
  --order orders.total_revenue:desc \
  --limit 5
