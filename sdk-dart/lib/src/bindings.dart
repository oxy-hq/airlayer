// Hand-written FFI bindings for libairlayer's C ABI (src/ffi.rs).
//
// The C surface is intentionally tiny — every symbol takes/returns
// null-terminated UTF-8 strings. We don't use `ffigen` because there's no
// header to generate from (the symbols are exported by `#[no_mangle] pub
// extern "C"` in Rust, not declared in a `.h`). Keeping the bindings
// hand-written + small is simpler than maintaining a header for ffigen.

import 'dart:ffi' as ffi;

typedef _CallNative = ffi.Pointer<ffi.Char> Function(ffi.Pointer<ffi.Char>);
typedef _CallDart = ffi.Pointer<ffi.Char> Function(ffi.Pointer<ffi.Char>);

typedef _VersionNative = ffi.Pointer<ffi.Char> Function();
typedef _VersionDart = ffi.Pointer<ffi.Char> Function();

typedef _FreeNative = ffi.Void Function(ffi.Pointer<ffi.Char>);
typedef _FreeDart = void Function(ffi.Pointer<ffi.Char>);

/// Looks up each `airlayer_*` symbol in the loaded library and exposes them
/// as Dart functions. Constructing this throws if any symbol is missing —
/// usually a sign that the loaded dylib is older than this binding, or that
/// the iOS host didn't link the symbols (see README on `-force_load`).
///
/// All `*Dart` callbacks pass JSON-string pointers across the FFI boundary
/// and return a pointer that MUST be freed by [`free`]. The Dart wrapper
/// (`Airlayer`) handles that — direct callers must call [`free`] themselves.
///
/// `free` is `void` for Dart's sake but is `unsafe` on the Rust side:
/// passing a pointer that wasn't produced by `airlayer_*` is undefined
/// behavior. The wrapper only ever passes pointers it just received from
/// a call, so this is sound in practice.
class AirlayerBindings {
  AirlayerBindings(ffi.DynamicLibrary lib)
      : compile = lib
            .lookup<ffi.NativeFunction<_CallNative>>('airlayer_compile')
            .asFunction(),
        validate = lib
            .lookup<ffi.NativeFunction<_CallNative>>('airlayer_validate')
            .asFunction(),
        catalog = lib
            .lookup<ffi.NativeFunction<_CallNative>>('airlayer_catalog')
            .asFunction(),
        cacheResolve = lib
            .lookup<ffi.NativeFunction<_CallNative>>('airlayer_cache_resolve')
            .asFunction(),
        cacheBuildManifest = lib
            .lookup<ffi.NativeFunction<_CallNative>>(
                'airlayer_cache_build_manifest')
            .asFunction(),
        cacheKey = lib
            .lookup<ffi.NativeFunction<_CallNative>>('airlayer_cache_key')
            .asFunction(),
        cacheResolveWarehouse = lib
            .lookup<ffi.NativeFunction<_CallNative>>(
                'airlayer_cache_resolve_warehouse')
            .asFunction(),
        version = lib
            .lookup<ffi.NativeFunction<_VersionNative>>('airlayer_version')
            .asFunction(),
        free = lib
            .lookup<ffi.NativeFunction<_FreeNative>>('airlayer_free')
            .asFunction();

  final _CallDart compile;
  final _CallDart validate;
  final _CallDart catalog;
  final _CallDart cacheResolve;
  final _CallDart cacheBuildManifest;
  final _CallDart cacheKey;
  final _CallDart cacheResolveWarehouse;
  final _VersionDart version;
  final _FreeDart free;
}
