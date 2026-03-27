#!/usr/bin/env bash
# Step 1: Discover what's in the database.
#
# This is the starting point for bootstrapping a semantic layer.
# The agent (or human) runs this to see all tables, columns, and types
# available in the warehouse — before writing any .view.yml files.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Schema introspection ==="
echo ""
airlayer inspect --schema --config config.yml
