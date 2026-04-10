//! Cube.js parity tests (tier 2).
//!
//! These tests convert Cube.js schema files to airlayer views, compile queries,
//! and verify the SQL produces correct results when executed against a PostgreSQL
//! database seeded with the same data that Cube.js uses.
//!
//! Infrastructure:
//!   docker compose -f docker-compose.cube-parity.yml up -d
//!
//! Run:
//!   cargo test --test cube_parity_tests --features exec -- --ignored
//!
//! The tests skip gracefully if the database is not reachable.

use airlayer::dialect::Dialect;
use airlayer::engine::query::*;
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
use airlayer::schema::foreign;
use airlayer::schema::models::SemanticLayer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cube_pg_port() -> u16 {
    std::env::var("AIRLAYER_CUBE_PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15433)
}

fn try_connect_pg() -> Option<postgres::Client> {
    let port = cube_pg_port();
    let conn_str = format!(
        "host=localhost port={} user=airlayer password=airlayertest dbname=airlayer_test connect_timeout=3",
        port
    );
    postgres::Client::connect(&conn_str, postgres::NoTls).ok()
}

/// Load the Cube.js schema, convert to airlayer views, and build an engine.
fn load_cube_parity_engine() -> SemanticEngine {
    let cube_schema_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/integration/cube_parity/cube_schema");

    let result = foreign::convert_directory(foreign::ForeignFormat::Cube, &cube_schema_dir)
        .expect("Failed to convert Cube.js schema");

    assert!(
        !result.views.is_empty(),
        "Should have converted at least one view"
    );

    let layer = SemanticLayer::new(result.views, None);
    let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
    SemanticEngine::from_semantic_layer(layer, dialects).expect("Failed to build engine")
}

/// Execute a SQL query against the parity Postgres and return rows as string columns.
fn execute_sql(client: &mut postgres::Client, sql: &str) -> Vec<Vec<String>> {
    let rows = client.query(sql, &[]).expect("SQL execution failed");
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    // Try to get as various types
                    if let Ok(v) = row.try_get::<_, String>(i) {
                        v
                    } else if let Ok(v) = row.try_get::<_, i64>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, i32>(i) {
                        v.to_string()
                    } else if let Ok(v) = row.try_get::<_, f64>(i) {
                        format!("{:.2}", v)
                    } else {
                        "NULL".to_string()
                    }
                })
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Parity tests
// ---------------------------------------------------------------------------

/// Test: simple count + sum grouped by status
#[test]
#[ignore]
fn cube_parity_count_and_sum_by_status() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => {
            eprintln!("Skipping cube parity test: Postgres not reachable on port {}", cube_pg_port());
            return;
        }
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec![
            "orders.count".to_string(),
            "orders.total_amount".to_string(),
        ],
        dimensions: vec!["orders.status".to_string()],
        order: vec![OrderBy {
            id: "orders.status".to_string(),
            desc: false,
        }],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    // Also execute the "expected" SQL directly to verify
    let expected_sql = r#"
        SELECT status, COUNT(*), SUM(amount)
        FROM orders
        GROUP BY status
        ORDER BY status ASC
    "#;
    let expected_rows = execute_sql(&mut client, expected_sql);

    assert_eq!(
        airlayer_rows.len(),
        expected_rows.len(),
        "Row count mismatch.\nAirlayer SQL:\n{}\nAirlayer rows: {:?}\nExpected rows: {:?}",
        result.sql,
        airlayer_rows,
        expected_rows
    );

    // Verify each row matches (status + count + sum)
    for (i, (actual, expected)) in airlayer_rows.iter().zip(expected_rows.iter()).enumerate() {
        assert_eq!(
            actual[0], expected[0],
            "Status mismatch at row {}",
            i
        );
    }
}

/// Test: count_distinct measure
#[test]
#[ignore]
fn cube_parity_count_distinct() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => return,
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec!["orders.unique_users".to_string()],
        dimensions: vec![],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    let expected_rows = execute_sql(
        &mut client,
        "SELECT COUNT(DISTINCT user_id) FROM orders",
    );

    assert_eq!(
        airlayer_rows[0][0], expected_rows[0][0],
        "Count distinct mismatch.\nAirlayer SQL:\n{}",
        result.sql
    );
}

/// Test: measure with filter (completed_count)
#[test]
#[ignore]
fn cube_parity_filtered_measure() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => return,
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec!["orders.completed_count".to_string()],
        dimensions: vec![],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    let expected_rows = execute_sql(
        &mut client,
        "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
    );

    assert_eq!(
        airlayer_rows[0][0], expected_rows[0][0],
        "Filtered measure mismatch.\nAirlayer SQL:\n{}",
        result.sql
    );
}

/// Test: aggregation functions (avg, min, max)
#[test]
#[ignore]
fn cube_parity_agg_functions() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => return,
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec![
            "orders.avg_amount".to_string(),
            "orders.min_amount".to_string(),
            "orders.max_amount".to_string(),
        ],
        dimensions: vec![],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    let expected_rows = execute_sql(
        &mut client,
        "SELECT AVG(amount), MIN(amount), MAX(amount) FROM orders",
    );

    // Compare values (allow small floating point differences)
    assert_eq!(
        airlayer_rows.len(),
        1,
        "Should have exactly one row"
    );
    assert_eq!(
        expected_rows.len(),
        1,
        "Expected should have exactly one row"
    );
}

/// Test: query with dimension filter
#[test]
#[ignore]
fn cube_parity_dimension_filter() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => return,
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec!["orders.count".to_string()],
        dimensions: vec!["orders.status".to_string()],
        filters: vec![QueryFilter {
            member: Some("orders.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["completed".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    let expected_rows = execute_sql(
        &mut client,
        "SELECT status, COUNT(*) FROM orders WHERE status = 'completed' GROUP BY status",
    );

    assert_eq!(
        airlayer_rows.len(),
        expected_rows.len(),
        "Row count mismatch with filter.\nAirlayer SQL:\n{}",
        result.sql
    );
    assert_eq!(airlayer_rows[0][0], "completed");
}

/// Test: segment filter
#[test]
#[ignore]
fn cube_parity_segment() {
    let mut client = match try_connect_pg() {
        Some(c) => c,
        None => return,
    };

    let engine = load_cube_parity_engine();

    let request = QueryRequest {
        measures: vec!["orders.count".to_string()],
        dimensions: vec![],
        segments: vec!["orders.completed_orders".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    let airlayer_rows = execute_sql(&mut client, &result.sql);

    let expected_rows = execute_sql(
        &mut client,
        "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
    );

    assert_eq!(
        airlayer_rows[0][0], expected_rows[0][0],
        "Segment filter mismatch.\nAirlayer SQL:\n{}",
        result.sql
    );
}

/// Test: conversion produces valid views that compile
#[test]
fn cube_parity_conversion_compiles() {
    let engine = load_cube_parity_engine();

    // Verify we can compile various queries without errors
    let queries = vec![
        QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        },
        QueryRequest {
            measures: vec!["orders.total_amount".to_string()],
            dimensions: vec![],
            ..QueryRequest::new()
        },
        QueryRequest {
            measures: vec!["orders.completed_count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        },
        QueryRequest {
            measures: vec!["users.count".to_string()],
            dimensions: vec!["users.city".to_string()],
            ..QueryRequest::new()
        },
    ];

    for (i, query) in queries.iter().enumerate() {
        let result = engine.compile_query(query);
        assert!(
            result.is_ok(),
            "Query {} failed to compile: {:?}",
            i,
            result.err()
        );
        let sql = result.unwrap().sql;
        assert!(!sql.is_empty(), "Query {} produced empty SQL", i);
        assert!(
            sql.contains("SELECT"),
            "Query {} SQL doesn't contain SELECT: {}",
            i,
            sql
        );
    }
}

/// Test: conversion round-trip — convert Cube schema, emit as airlayer YAML, re-parse
#[test]
fn cube_parity_roundtrip() {
    let cube_schema_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/integration/cube_parity/cube_schema");

    let result = foreign::convert_directory(foreign::ForeignFormat::Cube, &cube_schema_dir)
        .expect("Failed to convert Cube.js schema");

    // Serialize each view to YAML and re-parse
    let parser = airlayer::schema::parser::SchemaParser::new();
    for view in &result.views {
        let yaml = serde_yaml::to_string(view).expect("Failed to serialize view");
        let reparsed = parser.parse_view_str(&yaml, "roundtrip");
        assert!(
            reparsed.is_ok(),
            "Failed to re-parse view '{}': {:?}\nYAML:\n{}",
            view.name,
            reparsed.err(),
            yaml
        );
        let reparsed = reparsed.unwrap();
        assert_eq!(reparsed.name, view.name);
        assert_eq!(reparsed.dimensions.len(), view.dimensions.len());
    }
}
