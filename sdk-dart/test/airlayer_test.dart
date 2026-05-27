// Integration tests that exercise the loaded native lib end-to-end.
//
// Before running: build the native library with the `ffi` feature so the
// .dylib / .so / .dll exists, then point `AIRLAYER_LIB` at it.
//
//   cargo build --release --no-default-features --features ffi
//   AIRLAYER_LIB=$(pwd)/../target/release/libairlayer.dylib dart test
//
// On CI we'd put the binary on the loader path so `Airlayer.load()` works
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

  test('version exposes a semver', () {
    expect(airlayer.version, matches(RegExp(r'^\d+\.\d+\.\d+')));
  });

  test('validate accepts a good view', () {
    expect(airlayer.validate(views: [_viewYaml]), isTrue);
  });

  test('validate rejects broken yaml', () {
    expect(
      () => airlayer.validate(views: ['name: [unclosed']),
      throwsA(isA<AirlayerException>()),
    );
  });

  test('compile produces SQL for a basic query', () {
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

  test('catalog lists the view', () {
    final entries = airlayer.catalog(views: [_viewYaml]);
    expect(entries, isNotEmpty);
  });
}
