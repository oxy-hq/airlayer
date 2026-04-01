#!/usr/bin/env bash
# Discover available sequences and motifs using inspect flags.
set -euo pipefail
cd "$(dirname "$0")"

echo "=== Available sequences ==="
airlayer inspect --sequences --path .

echo ""
echo "=== Available sequences (JSON) ==="
airlayer inspect --sequences --json --path .

echo ""
echo "=== Available motifs ==="
airlayer inspect --motifs --path .
