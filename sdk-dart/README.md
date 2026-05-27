# airlayer (Dart)

Dart FFI bindings for the airlayer semantic-layer compiler. Compiles
`.view.yml` + a JSON query request into SQL for any of the supported
dialects (DuckDB, Postgres, BigQuery, Snowflake, …).

Works in Flutter (Android, iOS, macOS, Linux, Windows) and any Dart VM
environment.

```dart
import 'package:airlayer/airlayer.dart';

final airlayer = Airlayer.load();

final result = airlayer.compile(
  views: [
    File('views/orders.view.yml').readAsStringSync(),
  ],
  query: {
    'measures':   ['orders.total_revenue'],
    'dimensions': ['orders.region'],
    'order':      [{'id': 'orders.total_revenue', 'desc': true}],
    'limit':      10,
  },
  dialect: 'duckdb',
);

print(result.sql);
```

## How it works

This package wraps `libairlayer` — a `cdylib` produced from the airlayer Rust
crate — via `dart:ffi`. The C ABI is intentionally tiny: every entry point
takes a JSON args string and returns a JSON result string of the form
`{"ok": ...}` or `{"error": "..."}`.

That keeps the FFI surface stable, the Dart wrapper trivial, and the
serialization overhead negligible relative to the SQL compilation itself.

## Native library setup

You need to ship `libairlayer` alongside your Dart/Flutter app.

### Local dev (macOS / Linux)

```sh
# from the airlayer repo root
./sdk-dart/scripts/build-host.sh
# point dart test at the built lib
cd sdk-dart
AIRLAYER_LIB=$(pwd)/../target/release/libairlayer.dylib dart test
```

### Android (Flutter)

```sh
# install once
cargo install cargo-ndk
rustup target add aarch64-linux-android armv7-linux-androideabi \
                  x86_64-linux-android i686-linux-android

# build
./sdk-dart/scripts/build-android.sh

# Copy the libs into your Flutter app:
cp -R sdk-dart/build/jniLibs/* <your_app>/android/app/src/main/jniLibs/
```

`Airlayer.load()` then finds the right `libairlayer.so` per ABI at runtime
via Android's normal library loader. No code changes needed in your Flutter
app beyond adding the `airlayer` dependency.

### iOS (Flutter)

```sh
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

./sdk-dart/scripts/build-ios.sh
# produces sdk-dart/build/Airlayer.xcframework
```

In `<your_app>/ios/Runner.xcworkspace`, add the xcframework as an embedded
framework. On iOS, `Airlayer.load()` uses `DynamicLibrary.process()` —
symbols are pulled from the host binary which statically links the archive.

### macOS desktop / Linux server / Windows

Build with the host script (or the per-platform Rust target), and ensure
the resulting `libairlayer.{dylib,so,dll}` is on the loader path
(`@executable_path/../Frameworks` on macOS apps, next to the binary on
Windows, `LD_LIBRARY_PATH` on Linux).

For ad-hoc paths, use `Airlayer.fromPath('/abs/path/to/libairlayer.dylib')`.

## API

| Method                       | What it does                                                |
|------------------------------|-------------------------------------------------------------|
| `Airlayer.load()`            | Load the native lib from the platform default location.     |
| `Airlayer.fromPath(path)`    | Load from an explicit path (tests, custom installs).        |
| `airlayer.version`           | Returns the linked airlayer semver.                         |
| `airlayer.compile(...)`      | views + query + dialect → `CompileResult { sql, params, columns }`. |
| `airlayer.validate(...)`     | views (+ topics) → `bool` (throws on failure).              |
| `airlayer.catalog(...)`      | List every dimension, measure, motif across schemas.        |

Errors from airlayer (schema parse failure, unknown dialect, invalid query,
etc.) surface as `AirlayerException` with the message from the Rust side.

## Layout

```
sdk-dart/
├── pubspec.yaml
├── lib/
│   ├── airlayer.dart                public exports
│   └── src/
│       ├── airlayer_base.dart       high-level Dart API
│       └── bindings.dart            raw FFI lookups
├── scripts/
│   ├── build-host.sh                macOS / Linux dev build
│   ├── build-android.sh             android NDK build, all ABIs
│   └── build-ios.sh                 iOS device + simulator xcframework
├── example/
│   └── compile_example.dart         minimal CLI demo
└── test/
    └── airlayer_test.dart           e2e tests (need AIRLAYER_LIB set)
```

## Status

Alpha. The C ABI is small (5 symbols) and tested at the Rust layer (see
`src/ffi.rs` tests). The Dart wrapper has e2e tests against a built dylib.
Coverage of the WASM surface:

| WASM function              | Dart equivalent           |
|----------------------------|---------------------------|
| `compile`                  | `Airlayer.compile`         |
| `validate`                 | `Airlayer.validate`        |
| `catalog_list`             | `Airlayer.catalog`         |
| `cache_resolve`            | not yet — open an issue   |
| `cache_build_manifest`     | not yet                   |
| `cache_resolve_warehouse`  | not yet                   |
| `compile_foreign`          | not yet                   |

The remaining surface is mechanically easy to add — three more entry points
in `src/ffi.rs` plus Dart wrappers. Holding for the first round of consumer
feedback before scoping them.
