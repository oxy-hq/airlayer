#!/usr/bin/env bash
# Step 2: Profile dimensions to understand the data.
#
# After creating an initial .view.yml (step 1 output → agent generates it),
# profile dimensions to discover valid values, ranges, and cardinality.
# This tells the agent what filters make sense and what the data looks like.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Profile all dimensions in the orders view ==="
echo ""
airlayer inspect --profile orders --config config.yml --path .
