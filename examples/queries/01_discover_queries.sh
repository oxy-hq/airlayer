#!/usr/bin/env bash
# Discover available saved queries using inspect flags.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Available saved queries ==="
airlayer inspect --queries --path .

echo ""
echo "=== Available saved queries (JSON) ==="
airlayer inspect --queries --json --path .
