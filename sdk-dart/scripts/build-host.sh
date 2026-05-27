#!/usr/bin/env bash
# Build the host (macOS / Linux) .dylib / .so for local testing.
#
# Output lands at airlayer/target/release/libairlayer.{dylib,so} and can be
# pointed at via AIRLAYER_LIB=... when running `dart test`.
set -euo pipefail
cd "$(dirname "$0")/../.."   # airlayer repo root
cargo build --release --no-default-features --features ffi
case "$(uname -s)" in
  Darwin)  artifact=target/release/libairlayer.dylib ;;
  Linux)   artifact=target/release/libairlayer.so ;;
  *)       echo "unsupported host: $(uname -s)" >&2 ; exit 1 ;;
esac
echo "built: $artifact"
echo "test with:"
echo "  cd sdk-dart && AIRLAYER_LIB=$(pwd)/$artifact dart test"
