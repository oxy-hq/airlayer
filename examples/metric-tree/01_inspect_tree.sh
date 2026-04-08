#!/usr/bin/env bash
# Show the full metric tree as text (all roots, all relationships)
set -euo pipefail
cd "$(dirname "$0")"

airlayer inspect --metric-tree
