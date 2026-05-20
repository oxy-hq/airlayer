#!/usr/bin/env bash
set -euo pipefail

# Build WASM from the Rust crate (run from repo root)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SDK_DIR="$SCRIPT_DIR/.."

FEATURES="${1:-wasm}"

echo "Building airlayer WASM with features: $FEATURES"
cd "$REPO_ROOT"
wasm-pack build --target web --no-default-features --features "$FEATURES" --out-dir "$SDK_DIR/wasm"

# Remove wasm-pack generated files we don't need
rm -f "$SDK_DIR/wasm/.gitignore" "$SDK_DIR/wasm/package.json" "$SDK_DIR/wasm/README.md"

echo "WASM artifacts copied to sdk/wasm/"
ls -la "$SDK_DIR/wasm/"
