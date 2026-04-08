#!/usr/bin/env bash
# Machine-readable metric tree (for agent consumption)
set -euo pipefail
cd "$(dirname "$0")"

airlayer inspect --metric-tree --json
