#!/usr/bin/env bash
# Custom motif: min-max normalization
# Defined in motifs/normalized.motif.yml

cd "$(dirname "$0")"

echo "=== Custom motif: normalized ==="
echo "Scales each platform's revenue to [0, 1] range"
echo ""

cargo run --features exec-duckdb -- query \
  --path . \
  --config config.yml \
  --dimensions events.platform \
  --measures events.total_revenue \
  --motif normalized \
  --execute
