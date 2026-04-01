#!/usr/bin/env bash
# Compile a sequence to SQL (dry run — no database needed).
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Compile revenue_investigation ==="
airlayer sequence run revenue_investigation --path .

echo ""
echo "=== Compile platform_comparison ==="
airlayer sequence run platform_comparison --path .
