#!/bin/bash
# Generate interactive HTML visualization of the funnel metric tree
cd "$(dirname "$0")"
airlayer visualize --output metric-tree.html
echo "Open metric-tree.html in a browser to explore the funnel graph"
