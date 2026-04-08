#!/usr/bin/env bash
# Generate an interactive HTML visualization and open it in your browser
set -euo pipefail
cd "$(dirname "$0")"

airlayer visualize --output metric-tree.html

# To visualize only the subtree rooted at a specific metric:
#   airlayer visualize --root revenue.arr --output arr-tree.html
