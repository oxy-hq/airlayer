import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';

import 'package:ffi/ffi.dart';

import 'bindings.dart';

/// High-level Dart wrapper around the libairlayer C ABI. Loads the native
/// library, marshals JSON across the FFI boundary, and surfaces results as
/// typed Dart objects.
///
/// One instance can be shared across the app — the underlying calls are
/// stateless and thread-safe. Construct with `Airlayer.load()` (auto-finds
/// the lib for the current platform) or `Airlayer.fromPath(path)` for tests.
class Airlayer {
  final AirlayerBindings _b;

  Airlayer._(this._b);

  /// Loads `libairlayer` from the standard location for the current platform:
  ///
  /// - **Android**: `libairlayer.so` (resolved by the system loader from
  ///   `jniLibs/<abi>/`)
  /// - **iOS**: statically linked into the app binary
  /// - **macOS**: `libairlayer.dylib` (next to the executable, or in
  ///   `DYLD_LIBRARY_PATH`)
  /// - **Linux**: `libairlayer.so`
  /// - **Windows**: `airlayer.dll`
  ///
  /// Throws if the library can't be found. For custom paths use
  /// `Airlayer.fromPath(...)`.
  factory Airlayer.load() {
    return Airlayer._(AirlayerBindings(_openDefault()));
  }

  /// Loads from an explicit filesystem path. Useful for tests and for
  /// non-standard deployments.
  factory Airlayer.fromPath(String path) {
    return Airlayer._(AirlayerBindings(ffi.DynamicLibrary.open(path)));
  }

  /// Returns the linked airlayer version (semver string).
  String get version {
    final ptr = _b.version();
    final result = _decode(ptr);
    return result['ok'] as String;
  }

  /// Compiles a semantic query to SQL against [views] (+ optional [topics],
  /// [motifs], [savedQueries]).
  ///
  /// [views] is a list of `.view.yml` file contents. [query] is a
  /// query-request map (same shape as the JS SDK; see [QueryRequest]
  /// docs in the airlayer repo for fields). [dialect] is a SQL dialect
  /// name: `"duckdb"`, `"postgres"`, `"bigquery"`, etc.
  ///
  /// Throws [AirlayerException] on schema or compilation errors.
  CompileResult compile({
    required List<String> views,
    required Map<String, dynamic> query,
    required String dialect,
    List<String>? topics,
    List<String>? motifs,
    List<String>? savedQueries,
  }) {
    final args = jsonEncode({
      'views': views,
      'query': query,
      'dialect': dialect,
      if (topics != null) 'topics': topics,
      if (motifs != null) 'motifs': motifs,
      if (savedQueries != null) 'queries': savedQueries,
    });
    final result = _call(_b.compile, args);
    return CompileResult.fromJson(result);
  }

  /// Validates [views] (+ optional [topics]) without compiling a query.
  /// Returns true on success; throws [AirlayerException] on failure.
  bool validate({required List<String> views, List<String>? topics}) {
    final args = jsonEncode({
      'views': views,
      if (topics != null) 'topics': topics,
    });
    _call(_b.validate, args);
    return true;
  }

  /// Lists every semantic object (views, dimensions, measures, motifs)
  /// across the supplied schemas. Returns a JSON-like list of catalog
  /// entries — see airlayer's catalog module for the field shape.
  List<dynamic> catalog({
    required List<String> views,
    List<String>? topics,
    List<String>? motifs,
    List<String>? savedQueries,
  }) {
    final args = jsonEncode({
      'views': views,
      if (topics != null) 'topics': topics,
      if (motifs != null) 'motifs': motifs,
      if (savedQueries != null) 'queries': savedQueries,
    });
    final result = _call(_b.catalog, args);
    return result as List<dynamic>;
  }

  // ---- internals ----

  dynamic _call(ffi.Pointer<ffi.Char> Function(ffi.Pointer<ffi.Char>) fn, String argsJson) {
    final argsPtr = argsJson.toNativeUtf8().cast<ffi.Char>();
    final resultPtr = fn(argsPtr);
    calloc.free(argsPtr);
    final body = _decode(resultPtr);
    if (body.containsKey('error')) {
      throw AirlayerException(body['error'] as String);
    }
    return body['ok'];
  }

  Map<String, dynamic> _decode(ffi.Pointer<ffi.Char> ptr) {
    if (ptr == ffi.nullptr) {
      throw AirlayerException('airlayer returned null pointer');
    }
    final raw = ptr.cast<Utf8>().toDartString();
    _b.free(ptr);
    return jsonDecode(raw) as Map<String, dynamic>;
  }
}

ffi.DynamicLibrary _openDefault() {
  if (Platform.isMacOS) return ffi.DynamicLibrary.open('libairlayer.dylib');
  if (Platform.isLinux) return ffi.DynamicLibrary.open('libairlayer.so');
  if (Platform.isAndroid) return ffi.DynamicLibrary.open('libairlayer.so');
  if (Platform.isWindows) return ffi.DynamicLibrary.open('airlayer.dll');
  if (Platform.isIOS) return ffi.DynamicLibrary.process();
  throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
}

/// Thrown when airlayer reports a schema/query error or the FFI boundary
/// produces an unexpected response.
class AirlayerException implements Exception {
  final String message;
  AirlayerException(this.message);
  @override
  String toString() => 'AirlayerException: $message';
}

/// Result of [Airlayer.compile]: the generated SQL plus column metadata.
class CompileResult {
  final String sql;
  final List<String> params;
  final List<ColumnMeta> columns;

  CompileResult({required this.sql, required this.params, required this.columns});

  factory CompileResult.fromJson(dynamic json) {
    final m = json as Map<String, dynamic>;
    return CompileResult(
      sql: m['sql'] as String,
      params: (m['params'] as List).cast<String>(),
      columns: (m['columns'] as List)
          .map((c) => ColumnMeta.fromJson(c as Map<String, dynamic>))
          .toList(),
    );
  }
}

class ColumnMeta {
  final String member;
  final String alias;
  final String kind;

  ColumnMeta({required this.member, required this.alias, required this.kind});

  factory ColumnMeta.fromJson(Map<String, dynamic> json) => ColumnMeta(
        member: json['member'] as String,
        alias: json['alias'] as String,
        kind: json['kind'] as String,
      );
}
