#!/usr/bin/env bash
# Compile a saved query to SQL (dry run — no database needed).
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Compile revenue_investigation ==="
airlayer query revenue_investigation --path .

echo ""
echo "=== Compile platform_comparison ==="
airlayer query platform_comparison --path .
