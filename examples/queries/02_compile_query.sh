#!/usr/bin/env bash
# Compile a saved query to SQL (dry run — no database needed).
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Compile revenue_investigation ==="
airlayer query queries/revenue_investigation.query.yml
echo ""
echo "=== Compile platform_comparison ==="
airlayer query queries/platform_comparison.query.yml