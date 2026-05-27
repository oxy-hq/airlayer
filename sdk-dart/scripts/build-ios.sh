#!/usr/bin/env bash
# Build a static archive (.a) for iOS device + simulator and bundle as an
# xcframework so Flutter iOS apps can link it.
#
# Prereqs:
#   - Xcode + command-line tools
#   - Rust targets:
#       rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
#
# We build a static lib (not a dylib) because Apple's app review process
# rejects loose .dylibs. The lib gets statically linked into the host app.
# In Dart, use `Airlayer.fromPath(...)` is unnecessary on iOS — pass
# `ffi.DynamicLibrary.process()` (which `Airlayer.load()` does for you).
set -euo pipefail
cd "$(dirname "$0")/../.."   # airlayer repo root

build_for() {
  local target=$1
  echo "==> $target"
  cargo rustc --release --no-default-features --features ffi \
    --target "$target" --crate-type staticlib
}

build_for aarch64-apple-ios
build_for aarch64-apple-ios-sim
build_for x86_64-apple-ios

out=sdk-dart/build/Airlayer.xcframework
rm -rf "$out"

# Combine sim slices into one lib via lipo, then bundle as xcframework.
mkdir -p sdk-dart/build/sim
lipo -create \
  target/aarch64-apple-ios-sim/release/libairlayer.a \
  target/x86_64-apple-ios/release/libairlayer.a \
  -output sdk-dart/build/sim/libairlayer.a

xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libairlayer.a \
  -library sdk-dart/build/sim/libairlayer.a \
  -output "$out"

echo
echo "xcframework ready: $out"
echo "in your Flutter iOS app's Runner.xcworkspace, add this as a"
echo "framework dependency under General > Frameworks, Libraries, and Embedded Content."
