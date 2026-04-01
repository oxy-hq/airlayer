#!/usr/bin/env bash
# Discover available motifs (builtins + custom) using inspect flags.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Available motifs ==="
airlayer inspect --motifs --path .

echo ""
echo "=== Available motifs (JSON) ==="
airlayer inspect --motifs --json --path .
