// Integration tests that exercise the loaded native lib end-to-end.
//
// Before running: build the native library with the `ffi` feature so the
// .dylib / .so / .dll exists, then point `AIRLAYER_LIB` at it.
//
//   cargo build --release --no-default-features --features ffi
//   AIRLAYER_LIB=$(pwd)/../target/release/libairlayer.dylib dart test
//
// On CI we put the binary on the loader path so `Airlayer.load()` works
// directly. For local dev the explicit env var is the simplest knob.

import 'dart:io';

import 'package:airlayer/airlayer.dart';
import 'package:test/test.dart';

const _viewYaml = '''
name: orders
table: orders
datasource: local
dialect: duckdb
dimensions:
  - name: status
    type: string
    expr: status
measures:
  - name: count
    type: count
''';

Airlayer _airlayer() {
  final envPath = Platform.environment['AIRLAYER_LIB'];
  return envPath != null && envPath.isNotEmpty
      ? Airlayer.fromPath(envPath)
      : Airlayer.load();
}

void main() {
  late Airlayer airlayer;

  setUpAll(() {
    airlayer = _airlayer();
  });

  group('version', () {
    test('exposes a semver', () {
      expect(airlayer.version, matches(RegExp(r'^\d+\.\d+\.\d+')));
    });
  });

  group('validate', () {
    test('accepts a good view', () {
      expect(() => airlayer.validate(views: [_viewYaml]), returnsNormally);
    });

    test('rejects broken yaml', () {
      expect(
        () => airlayer.validate(views: ['name: [unclosed']),
        throwsA(isA<AirlayerException>()),
      );
    });
  });

  group('compile', () {
    test('produces SQL for a basic query', () {
      final result = airlayer.compile(
        views: [_viewYaml],
        query: {
          'measures': ['orders.count'],
          'dimensions': ['orders.status'],
        },
        dialect: 'duckdb',
      );
      expect(result.sql.toLowerCase(), contains('orders'));
      expect(result.columns, hasLength(2));
    });

    test('throws on unknown measure', () {
      expect(
        () => airlayer.compile(
          views: [_viewYaml],
          query: {
            'measures': ['orders.does_not_exist'],
            'dimensions': ['orders.status'],
          },
          dialect: 'duckdb',
        ),
        throwsA(isA<AirlayerException>()),
      );
    });

    test('throws on unknown dialect', () {
      expect(
        () => airlayer.compile(
          views: [_viewYaml],
          query: {
            'measures': ['orders.count'],
          },
          dialect: 'not_a_real_dialect',
        ),
        throwsA(isA<AirlayerException>()),
      );
    });
  });

  group('catalog', () {
    test('lists the view', () {
      final entries = airlayer.catalog(views: [_viewYaml]);
      expect(entries, isNotEmpty);
      expect(entries.first, isA<CatalogEntry>());
    });
  });

  group('cache', () {
    test('cacheKey produces "view__hash" format', () {
      expect(
        airlayer.cacheKey(viewName: 'orders', rollupHash: 'abc123'),
        equals('orders__abc123'),
      );
    });

    test('cacheBuildManifest produces a LocalManifest', () {
      final manifest = airlayer.cacheBuildManifest(
        sourceDatabase: 'warehouse',
        rows: [
          {
            'view_name': 'orders',
            'rollup_name': 'by_day',
            'rollup_hash': 'abc123',
            'table_name': 'airlayer.orders__abc123',
            'dimensions': '["status"]',
            'measures':
                '[{"name":"count","measure_type":"count","expr":null,"columns":["count__count"]}]',
            'time_dimension': 'created_at',
            'granularity': 'day',
            'build_date': '2026-01-01',
          },
        ],
      );
      expect(manifest['source_database'], equals('warehouse'));
      final rollups = manifest['rollups'] as List<dynamic>;
      expect(rollups, hasLength(1));
      expect(
        (rollups.first as Map<String, dynamic>)['file'],
        equals('orders__abc123'),
      );
    });

    test('cacheResolve returns null when no rollup covers query', () {
      final resolution = airlayer.cacheResolve(
        manifest: {
          'pulled_at': '',
          'source_database': 'warehouse',
          'rollups': <dynamic>[],
        },
        query: {
          'measures': ['orders.count'],
          'dimensions': ['orders.status'],
        },
      );
      expect(resolution, isNull);
    });

    test('cacheResolveWarehouse returns null when no rollup covers query', () {
      final resolution = airlayer.cacheResolveWarehouse(
        rows: <Map<String, dynamic>>[],
        query: {
          'measures': ['orders.count'],
          'dimensions': ['orders.status'],
        },
        schema: 'airlayer',
        dialect: 'postgres',
      );
      expect(resolution, isNull);
    });
  });
}
