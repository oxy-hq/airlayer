// Hand-written FFI bindings for libairlayer's C ABI (src/ffi.rs).
//
// The C surface is intentionally tiny — five symbols, all taking/returning
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

class AirlayerBindings {
  AirlayerBindings(ffi.DynamicLibrary lib)
      : compile = lib.lookup<ffi.NativeFunction<_CallNative>>('airlayer_compile').asFunction(),
        validate = lib.lookup<ffi.NativeFunction<_CallNative>>('airlayer_validate').asFunction(),
        catalog = lib.lookup<ffi.NativeFunction<_CallNative>>('airlayer_catalog').asFunction(),
        version = lib.lookup<ffi.NativeFunction<_VersionNative>>('airlayer_version').asFunction(),
        free = lib.lookup<ffi.NativeFunction<_FreeNative>>('airlayer_free').asFunction();

  final _CallDart compile;
  final _CallDart validate;
  final _CallDart catalog;
  final _VersionDart version;
  final _FreeDart free;
}
