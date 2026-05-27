# airlayer (Dart)

Dart FFI bindings for the airlayer semantic-layer compiler. Compiles
`.view.yml` + a JSON query request into SQL for any of the supported
dialects (DuckDB, Postgres, BigQuery, Snowflake, …), and provides the
pre-aggregation `build` / `pull` cache primitives so a Flutter app can
serve queries from local rollups without round-tripping to a backend.

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

### Isolates

`dart:ffi` calls block the calling isolate. Most calls (`compile`,
`validate`, `cacheResolve`, `cacheKey`) are sub-millisecond and fine on
the UI isolate. For large schemas or heavy `catalog()` calls, instantiate
[`Airlayer`] on a background isolate via `Isolate.run` / `compute()`.

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
framework. **You must also force-load the static archive** so Xcode's
dead-code stripper doesn't drop the `airlayer_*` symbols — `Airlayer.load()`
on iOS uses `DynamicLibrary.process()`, which only finds symbols that the
linker actually kept.

In `ios/Podfile` (or your target's build settings), add to
`OTHER_LDFLAGS`:

```
-force_load $(BUILT_PRODUCTS_DIR)/Airlayer.xcframework/ios-arm64/libairlayer.a
```

(Use the matching slice for simulator builds, or wrap with
`$(EFFECTIVE_PLATFORM_SUFFIX)`.) Without this, symbol lookups will fail at
runtime with `Failed to lookup symbol 'airlayer_compile'`.

### macOS desktop / Linux server / Windows

Build with the host script (or the per-platform Rust target), and ensure
the resulting `libairlayer.{dylib,so,dll}` is on the loader path
(`@executable_path/../Frameworks` on macOS apps, next to the binary on
Windows, `LD_LIBRARY_PATH` on Linux).

For ad-hoc paths, use `Airlayer.fromPath('/abs/path/to/libairlayer.dylib')`.

## API

| Method                                | What it does                                                        |
|---------------------------------------|---------------------------------------------------------------------|
| `Airlayer.load()`                     | Load the native lib from the platform default location.             |
| `Airlayer.fromPath(path)`             | Load from an explicit path (tests, custom installs).                |
| `airlayer.version`                    | Returns the linked airlayer semver.                                 |
| `airlayer.compile(...)`               | views + query + dialect → `CompileResult { sql, params, columns }`. |
| `airlayer.validate(...)`              | views (+ topics/motifs/queries) → throws on failure.                |
| `airlayer.catalog(...)`               | List every dimension, measure, motif as `List<CatalogEntry>`.        |
| `airlayer.cacheResolve(...)`          | Local manifest + query → `CachedResolution?` (null when no cover).  |
| `airlayer.cacheBuildManifest(...)`    | Warehouse `__manifest` rows → `LocalManifest` map.                  |
| `airlayer.cacheKey(...)`              | `(view, hash)` → `"view__hash"`.                                    |
| `airlayer.cacheResolveWarehouse(...)` | Warehouse rows + query → `WarehouseResolution?`.                    |

Errors from airlayer (schema parse failure, unknown dialect, invalid query,
etc.) surface as `AirlayerException` with the message from the Rust side.

## Build / pull cache flow

Mirrors the npm SDK's `PreAggregateStore`. The full mobile flow:

1. **Build** — somewhere with warehouse access, run `airlayer build` to
   materialize rollups in the warehouse + write the `__manifest` table.
2. **Pull** — fetch the `__manifest` rows + per-rollup data (parquet or
   exported JSON) and stash them locally (sqflite / shared docs / etc.).
   Pass the manifest rows through `cacheBuildManifest` to canonicalize.
3. **Resolve** — on each query, call `cacheResolve(manifest, query)`. If
   non-null, load the data for `cacheKey` into a DuckDB table named
   `__cache` and execute `reaggSql`. If null, fall through to a network
   query (proxy or direct warehouse call).

The Dart SDK provides the **compile + resolve** halves of that flow. The
actual DuckDB execution and storage is left to the consumer — pair this
package with `package:duckdb` (or similar) for local execution.

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

Alpha. The C ABI surface is small and tested on both sides:

- Rust unit tests in `src/ffi.rs` (happy path, error paths, null-pointer
  safety, panic catching, cache_* roundtrips).
- Dart e2e tests in `sdk-dart/test/airlayer_test.dart` running against a
  built dylib (compile, validate, catalog, cache_*).

WASM surface coverage:

| WASM function              | Dart equivalent                  |
|----------------------------|----------------------------------|
| `compile`                  | `Airlayer.compile`               |
| `validate`                 | `Airlayer.validate`              |
| `catalog_list`             | `Airlayer.catalog`               |
| `cache_resolve`            | `Airlayer.cacheResolve`          |
| `cache_build_manifest`     | `Airlayer.cacheBuildManifest`    |
| `cache_key`                | `Airlayer.cacheKey`              |
| `cache_resolve_warehouse`  | `Airlayer.cacheResolveWarehouse` |
| `compile_foreign`          | not yet — open an issue          |
