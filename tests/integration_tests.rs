//! Integration tests: compile queries and execute them against real databases.
//!
//! Tier 1 (execution tests, no external services):
//!   - DuckDB: in-process, reads CSV seed data
//!   - SQLite: in-process, reads SQL seed data
//!
//! Tier 2 (execution tests, requires `docker compose -f docker-compose.test.yml up`):
//!   - PostgreSQL: on port 15432
//!   - MySQL: on port 13306
//!   - ClickHouse: on port 18123 (HTTP)
//!
//! Run tier-1 tests:  cargo test --test integration_tests -- --ignored tier1
//! Run all tiers:     cargo test --test integration_tests -- --ignored
//!
//! All tier-2 tests check if the service is reachable and skip (pass) if not.

use o3::dialect::Dialect;
use o3::engine::query::*;
use o3::engine::{DatasourceDialectMap, SemanticEngine};
use std::path::Path;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_engine(dialect: Dialect) -> SemanticEngine {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views");
    let dialects = DatasourceDialectMap::with_default(dialect);
    SemanticEngine::load(&views_dir, None, dialects).expect("failed to load test views")
}

/// Standard query: count + total_revenue grouped by platform, filtered to web.
fn standard_query() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "events.total_events".to_string(),
            "events.total_revenue".to_string(),
        ],
        dimensions: vec!["events.platform".to_string()],
        filters: vec![QueryFilter {
            member: Some("events.platform".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["web".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    }
}

/// Query with no filter (returns all platforms).
fn unfiltered_query() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "events.total_events".to_string(),
            "events.unique_users".to_string(),
            "events.purchase_count".to_string(),
        ],
        dimensions: vec!["events.platform".to_string()],
        ..QueryRequest::new()
    }
}

/// Query using a segment.
fn segment_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_events".to_string()],
        dimensions: vec![],
        segments: vec!["events.web_only".to_string()],
        ..QueryRequest::new()
    }
}

// ---------------------------------------------------------------------------
// Tier 1: DuckDB (in-process)
// ---------------------------------------------------------------------------
mod duckdb_tests {
    use super::*;

    fn create_db() -> duckdb::Connection {
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE events (
                event_id VARCHAR PRIMARY KEY,
                event_type VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                country VARCHAR,
                platform VARCHAR NOT NULL,
                revenue_cents INTEGER DEFAULT 0
            );
            INSERT INTO events VALUES
            ('e001', 'page_view', 'u1', '2025-01-15 10:00:00', 'US', 'web', 0),
            ('e002', 'click',     'u1', '2025-01-15 10:05:00', 'US', 'web', 0),
            ('e003', 'purchase',  'u1', '2025-01-15 10:10:00', 'US', 'web', 4999),
            ('e004', 'page_view', 'u2', '2025-01-15 11:00:00', 'UK', 'ios', 0),
            ('e005', 'purchase',  'u2', '2025-01-15 11:05:00', 'UK', 'ios', 2500),
            ('e006', 'signup',    'u3', '2025-01-16 09:00:00', 'DE', 'android', 0),
            ('e007', 'page_view', 'u3', '2025-01-16 09:05:00', 'DE', 'android', 0),
            ('e008', 'click',     'u4', '2025-01-16 14:00:00', 'US', 'web', 0),
            ('e009', 'purchase',  'u4', '2025-01-16 14:30:00', 'US', 'web', 9999),
            ('e010', 'page_view', 'u5', '2025-01-17 08:00:00', 'JP', 'web', 0),
            ('e011', 'purchase',  'u5', '2025-01-17 08:15:00', 'JP', 'web', 1500),
            ('e012', 'click',     'u1', '2025-01-17 16:00:00', 'US', 'ios', 0);",
        )
        .expect("seed events");
        db
    }

    fn execute_query(sql: &str, params: &[String]) -> Vec<Vec<String>> {
        let db = create_db();

        // DuckDB Rust driver uses ? not $1
        let rewritten = rewrite_params(sql);

        let mut stmt = db.prepare(&rewritten).expect(&format!("prepare failed for:\n{}", rewritten));
        let param_refs: Vec<&dyn duckdb::ToSql> = params
            .iter()
            .map(|p| p as &dyn duckdb::ToSql)
            .collect();

        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            loop {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(v) => {
                        vals.push(format!("{:?}", v));
                        i += 1;
                    }
                    Err(_) => break,
                }
            }
            rows_out.push(vals);
        }
        rows_out
    }

    fn rewrite_params(sql: &str) -> String {
        let re = regex::Regex::new(r"\$(\d+)").unwrap();
        re.replace_all(sql, "?").to_string()
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_standard_query() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected results for web platform");
        // web platform should return rows
        println!("Rows: {:?}", rows);
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_unfiltered_query() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        // Should have 3 platforms: web, ios, android
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_segment_query() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine.compile_query(&segment_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1, "Segment query should return 1 row");
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_measure_values_correct() {
        let engine = load_engine(Dialect::DuckDB);
        // Query all events, no filter, no grouping — just total counts
        let req = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.purchase_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1);
        // 12 total events, 4 purchases (e003, e005, e009, e011)
        let row = &rows[0];
        println!("Row: {:?}", row);
        // DuckDB Value debug format: Int(12), Int(4)
        assert!(row[0].contains("12"), "Expected 12 total events, got: {}", row[0]);
        assert!(row[1].contains("4"), "Expected 4 purchases, got: {}", row[1]);
    }
}

// ---------------------------------------------------------------------------
// Tier 1: SQLite (in-process)
// ---------------------------------------------------------------------------
mod sqlite_tests {
    use super::*;

    fn execute_query(sql: &str, params: &[String]) -> Vec<Vec<String>> {
        let db = rusqlite::Connection::open_in_memory().expect("sqlite open");

        // Seed data
        let seed = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/seed/sqlite.sql"),
        )
        .expect("read sqlite seed");
        db.execute_batch(&seed).expect("seed sqlite");

        // SQLite driver uses ? params natively
        let mut stmt = db.prepare(sql).expect(&format!("prepare failed for:\n{}", sql));
        let param_refs: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|p| Box::new(p.clone()) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::types::ToSql> = param_refs.iter().map(|b| b.as_ref()).collect();

        let col_count = stmt.column_count();
        let rows: Vec<Vec<String>> = stmt
            .query_map(refs.as_slice(), |row| {
                let mut vals = Vec::new();
                for i in 0..col_count {
                    let val: String = row
                        .get::<_, rusqlite::types::Value>(i)
                        .map(|v| format!("{:?}", v))
                        .unwrap_or_default();
                    vals.push(val);
                }
                Ok(vals)
            })
            .expect("query_map")
            .filter_map(|r| r.ok())
            .collect();

        rows
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_standard_query() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected results");
        println!("Rows: {:?}", rows);
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_unfiltered_query() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_segment_query() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine.compile_query(&segment_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1, "Segment query should return 1 row");
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_measure_values_correct() {
        let engine = load_engine(Dialect::SQLite);
        let req = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.purchase_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        println!("Row: {:?}", row);
        assert!(row[0].contains("12"), "Expected 12 total events, got: {}", row[0]);
        assert!(row[1].contains("4"), "Expected 4 purchases, got: {}", row[1]);
    }
}

// ---------------------------------------------------------------------------
// Tier 2: PostgreSQL (docker, port 15432)
// ---------------------------------------------------------------------------
mod postgres_tests {
    use super::*;

    fn try_connect() -> Option<postgres::Client> {
        postgres::Client::connect(
            "host=localhost port=15432 user=o3 password=o3test dbname=o3_test",
            postgres::NoTls,
        )
        .ok()
    }

    fn execute_query_simple(client: &mut postgres::Client, sql: &str, params: &[String]) -> Result<usize, String> {
        let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p as &(dyn postgres::types::ToSql + Sync))
            .collect();

        client
            .query(sql, &param_refs)
            .map(|rows| rows.len())
            .map_err(|e| format!("Query failed: {}\nSQL:\n{}", e, sql))
    }

    #[test]
    #[ignore = "tier2"]
    fn postgres_standard_query() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                eprintln!("PostgreSQL not available, skipping");
                return;
            }
        };

        // Use the postgres-specific view with analytics. schema prefix
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count = execute_query_simple(&mut client, &result.sql, &result.params)
            .expect("execute");
        assert!(row_count > 0, "Expected results");
        println!("Got {} rows", row_count);
    }

    #[test]
    #[ignore = "tier2"]
    fn postgres_unfiltered_query() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => { return; }
        };

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count = execute_query_simple(&mut client, &result.sql, &result.params)
            .expect("execute");
        assert_eq!(row_count, 3, "Expected 3 platforms");
    }
}

// ---------------------------------------------------------------------------
// Tier 2: MySQL (docker, port 13306)
// ---------------------------------------------------------------------------
mod mysql_tests {
    use super::*;
    use mysql::prelude::Queryable;

    fn try_connect() -> Option<mysql::Pool> {
        let opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(13306)
            .user(Some("o3"))
            .pass(Some("o3test"))
            .db_name(Some("o3_test"));
        mysql::Pool::new(opts).ok()
    }

    #[test]
    #[ignore = "tier2"]
    fn mysql_standard_query() {
        let pool = match try_connect() {
            Some(p) => p,
            None => {
                eprintln!("MySQL not available, skipping");
                return;
            }
        };

        // MySQL uses o3_test.events (no analytics schema)
        let engine = load_engine(Dialect::MySQL);
        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let mut conn = pool.get_conn().expect("get conn");
        // MySQL driver uses ? params natively — our generated SQL already uses ?
        let stmt = conn.prep(&result.sql).expect(&format!("prepare:\n{}", result.sql));
        let params_mysql: Vec<mysql::Value> = result
            .params
            .iter()
            .map(|p| mysql::Value::from(p.as_str()))
            .collect();
        let rows: Vec<mysql::Row> = conn.exec(stmt, params_mysql).expect("exec");
        assert!(!rows.is_empty(), "Expected results");
        println!("Got {} rows", rows.len());
    }
}

// ---------------------------------------------------------------------------
// Tier 2: ClickHouse (docker, HTTP port 18123)
// ---------------------------------------------------------------------------
mod clickhouse_tests {
    use super::*;

    fn is_available() -> bool {
        ureq::get("http://localhost:18123/ping")
            .call()
            .is_ok()
    }

    fn execute_query(sql: &str, params: &[String]) -> Result<String, String> {
        if !is_available() {
            return Err("ClickHouse not available".to_string());
        }

        // ClickHouse HTTP interface: substitute $1, $2 params inline for simplicity
        // (ClickHouse HTTP supports {name:Type} params but $N is simpler to rewrite)
        let mut rewritten = sql.to_string();
        for (i, param) in params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            rewritten = rewritten.replace(&placeholder, &format!("'{}'", param.replace('\'', "\\'")));
        }

        let resp = ureq::post("http://localhost:18123/")
            .query("database", "analytics")
            .send_string(&rewritten)
            .map_err(|e| format!("ClickHouse query failed: {}\nSQL:\n{}", e, rewritten))?;

        resp.into_string().map_err(|e| format!("Read response: {}", e))
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_standard_query() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }

        // ClickHouse uses analytics.events
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let output = execute_query(&result.sql, &result.params).expect("execute");
        println!("Output:\n{}", output);
        assert!(!output.trim().is_empty(), "Expected results");
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_unfiltered_query() {
        if !is_available() { return; }

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        let output = execute_query(&result.sql, &result.params).expect("execute");
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3, "Expected 3 platforms, got:\n{}", output);
    }
}

// ---------------------------------------------------------------------------
// Tier 2: Parse-only validation (Snowflake, BigQuery, Databricks, Redshift)
// These dialects have no local runtime. We validate the SQL parses without
// syntax errors by running it through DuckDB's parser (best-effort).
// ---------------------------------------------------------------------------
mod parse_validation_tests {
    use super::*;

    /// Try to EXPLAIN the SQL in DuckDB. This catches most syntax errors
    /// even for non-DuckDB dialects (quoting differences aside).
    fn validate_sql_parses(sql: &str, dialect: &str) {
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");

        // Create the events table so column refs resolve
        db.execute_batch(
            "CREATE TABLE events (
                event_id VARCHAR, event_type VARCHAR, user_id VARCHAR,
                created_at TIMESTAMP, country VARCHAR, platform VARCHAR,
                revenue_cents INTEGER
            )",
        )
        .expect("create table");

        // Rewrite dialect-specific quoting to DuckDB-compatible
        let normalized = sql
            .replace('`', "\"") // BigQuery/MySQL backticks -> double quotes
            .replace("@p", "$");  // BigQuery @p0 -> $0

        // Try to prepare (not execute) — catches syntax errors
        match db.prepare(&normalized) {
            Ok(_) => println!("[{}] SQL parses OK", dialect),
            Err(e) => {
                // Some dialect-specific functions won't exist in DuckDB, that's OK
                let err_str = e.to_string();
                if err_str.contains("Catalog Error") || err_str.contains("not found") {
                    println!("[{}] SQL has unknown functions (expected for cross-dialect): {}", dialect, err_str);
                } else {
                    panic!("[{}] SQL parse error: {}\nSQL:\n{}", dialect, e, normalized);
                }
            }
        }
    }

    #[test]
    #[ignore = "tier1"]
    fn parse_snowflake_queries() {
        let engine = load_engine(Dialect::Snowflake);
        for query in &[standard_query(), unfiltered_query(), segment_query()] {
            let result = engine.compile_query(query).expect("compile");
            println!("Snowflake SQL:\n{}", result.sql);
            validate_sql_parses(&result.sql, "snowflake");
        }
    }

    #[test]
    #[ignore = "tier1"]
    fn parse_bigquery_queries() {
        let engine = load_engine(Dialect::BigQuery);
        for query in &[standard_query(), unfiltered_query(), segment_query()] {
            let result = engine.compile_query(query).expect("compile");
            println!("BigQuery SQL:\n{}", result.sql);
            validate_sql_parses(&result.sql, "bigquery");
        }
    }

    #[test]
    #[ignore = "tier1"]
    fn parse_databricks_queries() {
        let engine = load_engine(Dialect::Databricks);
        for query in &[standard_query(), unfiltered_query(), segment_query()] {
            let result = engine.compile_query(query).expect("compile");
            println!("Databricks SQL:\n{}", result.sql);
            validate_sql_parses(&result.sql, "databricks");
        }
    }

    #[test]
    #[ignore = "tier1"]
    fn parse_redshift_queries() {
        let engine = load_engine(Dialect::Redshift);
        for query in &[standard_query(), unfiltered_query(), segment_query()] {
            let result = engine.compile_query(query).expect("compile");
            println!("Redshift SQL:\n{}", result.sql);
            validate_sql_parses(&result.sql, "redshift");
        }
    }
}
