#!/usr/bin/env bash
# Show just the subtree rooted at ARR
set -euo pipefail
cd "$(dirname "$0")"

airlayer inspect --metric-tree revenue.arr
