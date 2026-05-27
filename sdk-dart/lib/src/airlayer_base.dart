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
/// stateless (the Rust crate keeps no global mutable state). Construct with
/// `Airlayer.load()` (auto-finds the lib for the current platform) or
/// `Airlayer.fromPath(path)` for tests.
///
/// ## Isolates
///
/// `dart:ffi` calls block the calling isolate. The native calls here are
/// fast (typically <1ms for `compile`, more for large catalogs) so calling
/// from the UI isolate is usually fine. For heavy queries against many views,
/// run the [Airlayer] instance on a background isolate via `Isolate.run` or
/// `compute()` to keep the UI responsive.
class Airlayer {
  final AirlayerBindings _b;

  Airlayer._(this._b);

  /// Loads `libairlayer` from the standard location for the current platform:
  ///
  /// - **Android**: `libairlayer.so` (resolved by the system loader from
  ///   `jniLibs/<abi>/`)
  /// - **iOS**: statically linked into the app binary (see README on
  ///   `-force_load`)
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
  /// query-request map (same shape as the JS SDK). [dialect] is a SQL
  /// dialect name: `"duckdb"`, `"postgres"`, `"bigquery"`, etc.
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

  /// Validates [views] (+ optional [topics], [motifs], [savedQueries])
  /// without compiling a query. Throws [AirlayerException] on failure.
  void validate({
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
    _call(_b.validate, args);
  }

  /// Lists every semantic object (views, dimensions, measures, motifs)
  /// across the supplied schemas.
  List<CatalogEntry> catalog({
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
    return (result as List<dynamic>)
        .map((e) => CatalogEntry.fromJson(e as Map<String, dynamic>))
        .toList();
  }

  /// Check whether a cached rollup in [manifest] covers [query] and return
  /// the re-aggregation SQL plus cache key. Mirrors the npm SDK's
  /// `PreAggregateStore.execute` flow:
  ///
  /// 1. Call [cacheResolve] with the local manifest + the query.
  /// 2. If non-null, load the parquet/data for [`CachedResolution.cacheKey`]
  ///    into a table named `__cache` in DuckDB.
  /// 3. Execute the returned [`CachedResolution.reaggSql`] against DuckDB.
  ///
  /// Returns `null` when no rollup covers the query — the caller should
  /// fall through to a proxy / direct warehouse execution.
  CachedResolution? cacheResolve({
    required Map<String, dynamic> manifest,
    required Map<String, dynamic> query,
  }) {
    final args = jsonEncode({'manifest': manifest, 'query': query});
    final result = _call(_b.cacheResolve, args);
    if (result == null) return null;
    return CachedResolution.fromJson(result as Map<String, dynamic>);
  }

  /// Parse warehouse `__manifest` rows into a `LocalManifest` map ready to be
  /// passed to [cacheResolve]. Mirrors WASM `cache_build_manifest`. The
  /// typical flow is:
  ///
  /// 1. Query the warehouse's `__manifest` table.
  /// 2. Hand the rows here.
  /// 3. Persist the returned manifest in sqflite / IndexedDB / disk.
  ///
  /// [rows] are JSON-shaped manifest rows (see `airlayer build`
  /// documentation). [sourceDatabase] is just metadata attached to the
  /// manifest.
  Map<String, dynamic> cacheBuildManifest({
    required List<Map<String, dynamic>> rows,
    String sourceDatabase = '',
  }) {
    final args =
        jsonEncode({'rows': rows, 'source_database': sourceDatabase});
    final result = _call(_b.cacheBuildManifest, args);
    return result as Map<String, dynamic>;
  }

  /// Returns the cache key (`"<view>__<hash>"`) for a rollup. Trivial, but
  /// exposed for parity with WASM so callers don't risk drifting from the
  /// canonical format.
  String cacheKey({required String viewName, required String rollupHash}) {
    final args = jsonEncode({
      'view_name': viewName,
      'rollup_hash': rollupHash,
    });
    final result = _call(_b.cacheKey, args);
    return result as String;
  }

  /// Resolve [query] against warehouse [rows] (Layer 2 cache). Mirrors WASM
  /// `cache_resolve_warehouse`. Returns `null` if no rollup covers the query;
  /// otherwise returns the re-aggregation SQL and the warehouse table to
  /// execute it against.
  WarehouseResolution? cacheResolveWarehouse({
    required List<Map<String, dynamic>> rows,
    required Map<String, dynamic> query,
    required String schema,
    required String dialect,
  }) {
    final args = jsonEncode({
      'rows': rows,
      'query': query,
      'schema': schema,
      'dialect': dialect,
    });
    final result = _call(_b.cacheResolveWarehouse, args);
    if (result == null) return null;
    return WarehouseResolution.fromJson(result as Map<String, dynamic>);
  }

  // ---- internals ----

  dynamic _call(
    ffi.Pointer<ffi.Char> Function(ffi.Pointer<ffi.Char>) fn,
    String argsJson,
  ) {
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

  CompileResult({
    required this.sql,
    required this.params,
    required this.columns,
  });

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

/// One entry in the result of [Airlayer.catalog]. The catalog enumerates
/// every dimension, measure, motif, etc. across the supplied schemas. The
/// fields available depend on the entry [kind] — fields not relevant to a
/// given entry are null.
class CatalogEntry {
  final String kind;
  final String name;
  final String? view;
  final String? type;
  final String? description;
  /// Raw entry map for fields not promoted to typed accessors.
  final Map<String, dynamic> raw;

  CatalogEntry({
    required this.kind,
    required this.name,
    required this.raw,
    this.view,
    this.type,
    this.description,
  });

  factory CatalogEntry.fromJson(Map<String, dynamic> json) => CatalogEntry(
        kind: (json['kind'] ?? json['type'] ?? '') as String,
        name: (json['name'] ?? '') as String,
        view: json['view'] as String?,
        type: json['type'] as String?,
        description: json['description'] as String?,
        raw: json,
      );
}

/// Result of [Airlayer.cacheResolve]. The caller is responsible for loading
/// the cached data identified by [cacheKey] into a table named `__cache`
/// (DuckDB) before executing [reaggSql].
class CachedResolution {
  /// Re-aggregation SQL with `FROM "__cache"` as a placeholder table name.
  /// Either create a table named `__cache` with the cached data, or rewrite
  /// the placeholder to your actual data source.
  final String reaggSql;

  /// Cache key for looking up the stored data (e.g. `"events__a1b2c3d4"`).
  /// Use as the filename / IndexedDB key / sqflite row id.
  final String cacheKey;

  /// The matched rollup entry, for metadata inspection.
  final Map<String, dynamic> entry;

  CachedResolution({
    required this.reaggSql,
    required this.cacheKey,
    required this.entry,
  });

  factory CachedResolution.fromJson(Map<String, dynamic> json) =>
      CachedResolution(
        reaggSql: json['reagg_sql'] as String,
        cacheKey: json['cache_key'] as String,
        entry: (json['entry'] as Map<String, dynamic>),
      );
}

/// Result of [Airlayer.cacheResolveWarehouse]: SQL to execute against the
/// warehouse + the fully-qualified rollup table it reads from.
class WarehouseResolution {
  final String reaggSql;
  final String tableName;

  WarehouseResolution({required this.reaggSql, required this.tableName});

  factory WarehouseResolution.fromJson(Map<String, dynamic> json) =>
      WarehouseResolution(
        reaggSql: json['reagg_sql'] as String,
        tableName: json['table_name'] as String,
      );
}
