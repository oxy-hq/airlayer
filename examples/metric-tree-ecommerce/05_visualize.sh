#!/bin/bash
# Generate interactive HTML visualization of the metric tree
cd "$(dirname "$0")"
../../target/debug/airlayer visualize --output metric-tree.html
echo "Open metric-tree.html in a browser to explore the graph"
