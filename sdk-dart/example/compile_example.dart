// Minimal CLI demo. Run with:
//   cd sdk-dart
//   dart pub get
//   ./scripts/build-host.sh   # from airlayer repo root
//   AIRLAYER_LIB=$(pwd)/../target/release/libairlayer.dylib dart run example/compile_example.dart

import 'dart:io';

import 'package:airlayer/airlayer.dart';

const _viewYaml = '''
name: orders
table: orders
datasource: local
dialect: duckdb
dimensions:
  - name: status
    type: string
    expr: status
  - name: region
    type: string
    expr: region
measures:
  - name: count
    type: count
  - name: total_revenue
    type: sum
    expr: revenue
''';

void main() {
  final libPath = Platform.environment['AIRLAYER_LIB'];
  final airlayer = libPath != null
      ? Airlayer.fromPath(libPath)
      : Airlayer.load();

  print('airlayer ${airlayer.version}\n');

  final result = airlayer.compile(
    views: [_viewYaml],
    query: {
      'measures': ['orders.total_revenue'],
      'dimensions': ['orders.region'],
      'order': [
        {'id': 'orders.total_revenue', 'desc': true}
      ],
      'limit': 5,
    },
    dialect: 'duckdb',
  );

  print('SQL:\n${result.sql}\n');
  print('columns:');
  for (final c in result.columns) {
    print('  ${c.alias}  (${c.kind})  <- ${c.member}');
  }
}
