#!/usr/bin/env bash
# Step 3: Compile a semantic query to SQL (no execution).
#
# Verify that the semantic layer generates sensible SQL before hitting the database.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Revenue by category ==="
airlayer query --config config.yml \
  --dimensions orders.category \
  --measures orders.total_revenue \
  --order orders.total_revenue:desc
