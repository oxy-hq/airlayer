/// Dart bindings for the airlayer semantic-layer compiler.
///
/// Usage:
/// ```dart
/// import 'package:airlayer/airlayer.dart';
///
/// final airlayer = Airlayer.load();          // finds the native lib
/// final result = airlayer.compile(
///   views: [viewYaml],
///   query: {'measures': ['orders.count'], 'dimensions': ['orders.status']},
///   dialect: 'duckdb',
/// );
/// print(result.sql);
/// ```
library airlayer;

export 'src/airlayer_base.dart' show Airlayer, AirlayerException, CompileResult, ColumnMeta;
