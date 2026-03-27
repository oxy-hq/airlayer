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

use airlayer::dialect::Dialect;
use airlayer::engine::query::*;
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
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
            "host=localhost port=15432 user=airlayer password=airlayertest dbname=airlayer_test",
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
            .user(Some("airlayer"))
            .pass(Some("airlayertest"))
            .db_name(Some("airlayer_test"));
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

        // MySQL uses airlayer_test.events (no analytics schema)
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
            rewritten = rewritten.replace(&placeholder, &format!("'{}'", param.replace('\'', "''")));
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

// ---------------------------------------------------------------------------
// Tier 3: Snowflake (live warehouse, requires credentials)
//
// Env vars:
//   SNOWFLAKE_ACCOUNT    — account identifier (e.g. "jla01554")
//   SNOWFLAKE_USER       — login name
//   SNOWFLAKE_PASSWORD   — password
//   SNOWFLAKE_WAREHOUSE  — warehouse (default: COMPUTE_WH)
//
// The tests seed an AIRLAYER_TEST.ANALYTICS schema on first run.
// ---------------------------------------------------------------------------
mod snowflake_tests {
    use super::*;

    const DATABASE: &str = "AIRLAYER_TEST";
    const SCHEMA: &str = "ANALYTICS";

    struct SnowflakeSession {
        account: String,
        token: String,
        warehouse: String,
    }

    /// Read credentials from env and log in via the Snowflake session API.
    fn try_connect() -> Option<SnowflakeSession> {
        dotenvy::dotenv().ok();
        let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
        let user = std::env::var("SNOWFLAKE_USER").ok()?;
        let password = std::env::var("SNOWFLAKE_PASSWORD").ok()?;
        let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE")
            .unwrap_or_else(|_| "COMPUTE_WH".to_string());

        let url = format!(
            "https://{}.snowflakecomputing.com/session/v1/login-request",
            account,
        );

        let body = serde_json::json!({
            "data": {
                "LOGIN_NAME": user,
                "PASSWORD": password,
                "ACCOUNT_NAME": account,
            }
        });

        let resp = ureq::post(&url)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(&body.to_string())
            .ok()?;

        let json: serde_json::Value = resp.into_json().ok()?;
        let token = json["data"]["token"].as_str()?.to_string();

        Some(SnowflakeSession { account, token, warehouse })
    }

    /// Execute a SQL statement via the Snowflake session-based query API.
    /// Uses session token from login-request. Each call is a single statement.
    /// When `use_test_db` is true, sets DATABASE/SCHEMA context via parameters.
    fn execute_sql_inner(
        session: &SnowflakeSession,
        sql: &str,
        bindings: &[String],
        use_test_db: bool,
    ) -> Result<serde_json::Value, String> {
        // Inline ? param placeholders (the session query API doesn't support bindings)
        let mut rewritten = sql.to_string();
        for param in bindings.iter().rev() {
            if let Some(pos) = rewritten.rfind('?') {
                let escaped = param.replace('\'', "''");
                rewritten.replace_range(pos..pos + 1, &format!("'{}'", escaped));
            }
        }

        // Set context via USE statements before the actual query
        let mut stmts = vec![format!("USE WAREHOUSE {}", session.warehouse)];
        if use_test_db {
            stmts.push(format!("USE DATABASE {}", DATABASE));
            stmts.push(format!("USE SCHEMA {}", SCHEMA));
        }
        stmts.push(rewritten);

        let mut last = serde_json::json!(null);
        for stmt in &stmts {
            last = execute_single(session, stmt)?;
        }
        Ok(last)
    }

    fn execute_single(session: &SnowflakeSession, sql: &str) -> Result<serde_json::Value, String> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Generate a pseudo-unique request ID (UUID v4-ish)
        let request_id = format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            (seq * 2654435761) as u32,
            (seq * 40503) as u16,
            (seq * 12345) as u16 & 0xFFF,
            0x8000 | ((seq * 54321) as u16 & 0x3FFF),
            seq * 1099511628211u64,
        );

        let url = format!(
            "https://{}.snowflakecomputing.com/queries/v1/query-request?requestId={}",
            session.account, request_id,
        );

        let body = serde_json::json!({
            "sqlText": sql,
            "asyncExec": false,
            "sequenceId": seq,
        });

        let result = ureq::post(&url)
            .set("Authorization", &format!("Snowflake Token=\"{}\"", session.token))
            .set("Content-Type", "application/json")
            .set("Accept", "application/snowflake")
            .send_string(&body.to_string());

        match result {
            Ok(resp) => resp.into_json::<serde_json::Value>()
                .map_err(|e| format!("Failed to parse response: {}", e)),
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("Snowflake API error (HTTP {}): {}\nSQL:\n{}", code, body, sql))
            }
            Err(e) => Err(format!("Snowflake API error: {}\nSQL:\n{}", e, sql)),
        }
    }

    /// Execute SQL with the test database/schema context.
    fn execute_sql(
        session: &SnowflakeSession,
        sql: &str,
        bindings: &[String],
    ) -> Result<serde_json::Value, String> {
        let resp = execute_sql_inner(session, sql, bindings, true)?;
        if !resp["success"].as_bool().unwrap_or(true) {
            return Err(format!(
                "Snowflake query error: {}\nSQL:\n{}",
                resp["message"].as_str().unwrap_or("unknown"),
                sql
            ));
        }
        Ok(resp)
    }

    /// Ensure seed runs only once across all tests in this module.
    static SEED_ONCE: std::sync::Once = std::sync::Once::new();

    /// Run the seed SQL to create and populate the test table (idempotent, runs once).
    fn seed(session: &SnowflakeSession) {
        SEED_ONCE.call_once(|| seed_inner(session));
    }

    fn seed_inner(session: &SnowflakeSession) {
        let seed_sql = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/seed/snowflake.sql"),
        )
        .expect("read snowflake seed");

        for stmt in seed_sql.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() || stmt.starts_with("--") {
                continue;
            }
            // CREATE DATABASE needs no database context; everything else uses AIRLAYER_TEST
            let is_create_db = stmt.to_uppercase().starts_with("CREATE DATABASE");
            match execute_sql_inner(session, stmt, &[], !is_create_db) {
                Ok(resp) => {
                    if !resp["success"].as_bool().unwrap_or(true) {
                        panic!("Seed statement failed: {:?}\nSQL:\n{}", resp["message"], stmt);
                    }
                }
                Err(e) => panic!("Seed failed: {}", e),
            }
        }
    }

    /// Extract the number of result rows from a Snowflake query response.
    fn row_count(resp: &serde_json::Value) -> usize {
        // Session API: data.rowset is an array of row arrays
        resp["data"]["rowset"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_seed() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                eprintln!("Snowflake not configured, skipping");
                return;
            }
        };
        seed(&session);

        // Verify seed data
        let resp = execute_sql(&session, "SELECT COUNT(*) FROM analytics.events", &[])
            .expect("count query");
        println!("Seed verification: {:?}", resp["data"]);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_standard_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                eprintln!("Snowflake not configured, skipping");
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert!(count > 0, "Expected results for web platform, got 0 rows. Response: {:?}", resp);
        println!("Got {} rows", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_unfiltered_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert_eq!(count, 3, "Expected 3 platforms, got {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_segment_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        // Use integration views (which define segments), not multi-dialect views.
        // The segment query uses `events.web_only` which only exists in integration views.
        // But integration views use unqualified table name `events`, so we run it
        // against the analytics schema where `events` resolves via USE SCHEMA.
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&segment_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert_eq!(count, 1, "Segment query should return 1 row, got {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_measure_values_correct() {
        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let req = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.purchase_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        println!("SQL:\n{}", result.sql);

        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute");
        println!("Response: {:?}", resp["data"]);

        // Session API returns results in data.rowset as array of row arrays
        let rowset = resp["data"]["rowset"]
            .as_array()
            .expect("data.rowset should be array");
        assert_eq!(rowset.len(), 1, "Expected 1 row");
        let row = rowset[0].as_array().expect("row should be array");
        // 12 total events, 4 purchases
        assert_eq!(row[0].as_str().unwrap_or(""), "12", "Expected 12 total events, got: {:?}", row[0]);
        assert_eq!(row[1].as_str().unwrap_or(""), "4", "Expected 4 purchases, got: {:?}", row[1]);
    }
}

// ---------------------------------------------------------------------------
// Tier 3: BigQuery (live GCP project)
// ---------------------------------------------------------------------------
//
// Env vars:
//   BIGQUERY_PROJECT       — GCP project ID
//   BIGQUERY_ACCESS_TOKEN  — OAuth2 token (e.g., from `gcloud auth print-access-token`)
//
// The tests seed an `analytics` dataset with the standard events table.
// ---------------------------------------------------------------------------
mod bigquery_tests {
    use super::*;

    struct BigQuerySession {
        project: String,
        token: String,
    }

    fn try_connect() -> Option<BigQuerySession> {
        dotenvy::dotenv().ok();
        let project = std::env::var("BIGQUERY_PROJECT").ok()?;
        let token = std::env::var("BIGQUERY_ACCESS_TOKEN").ok()?;
        Some(BigQuerySession { project, token })
    }

    fn execute_sql(
        session: &BigQuerySession,
        sql: &str,
    ) -> Result<serde_json::Value, String> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
            session.project,
        );

        let body = serde_json::json!({
            "query": sql,
            "useLegacySql": false,
            "maxResults": 10000,
            "defaultDataset": {
                "projectId": session.project,
                "datasetId": "analytics",
            },
        });

        let result = ureq::post(&url)
            .set("Authorization", &format!("Bearer {}", session.token))
            .set("Content-Type", "application/json")
            .send_string(&body.to_string());

        let resp = match result {
            Ok(resp) => resp,
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                return Err(format!(
                    "BigQuery API error (HTTP {}): {}\nURL: {}\nSQL: {}",
                    code, body, url, sql
                ));
            }
            Err(e) => return Err(format!("BigQuery request failed: {}", e)),
        };

        let json: serde_json::Value = resp
            .into_json()
            .map_err(|e| format!("Failed to parse BigQuery response: {}", e))?;

        if let Some(err) = json.get("error") {
            return Err(format!(
                "BigQuery error: {}",
                err["message"].as_str().unwrap_or("unknown")
            ));
        }

        Ok(json)
    }

    /// Inline ? or $N params into SQL for BigQuery (which uses @p0 natively,
    /// but our compiled SQL uses ? for bigquery dialect).
    fn execute_compiled(
        session: &BigQuerySession,
        sql: &str,
        params: &[String],
    ) -> Result<serde_json::Value, String> {
        // Inline parameters — BigQuery REST API supports parameterized queries
        // but it's simpler to inline for tests, matching the executor pattern.
        let mut final_sql = sql.to_string();

        // Handle @p0, @p1, ... style (BigQuery dialect)
        for (i, param) in params.iter().enumerate().rev() {
            let placeholder = format!("@p{}", i);
            let escaped = param.replace('\'', "''");
            final_sql = final_sql.replace(&placeholder, &format!("'{}'", escaped));
        }

        execute_sql(session, &final_sql)
    }

    fn row_count(resp: &serde_json::Value) -> usize {
        resp["totalRows"]
            .as_str()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
    }

    fn get_cell(resp: &serde_json::Value, row: usize, col: usize) -> String {
        resp["rows"][row]["f"][col]["v"]
            .as_str()
            .unwrap_or("")
            .to_string()
    }

    static SEED_ONCE: std::sync::Once = std::sync::Once::new();

    fn seed(session: &BigQuerySession) {
        SEED_ONCE.call_once(|| seed_inner(session));
    }

    fn seed_inner(session: &BigQuerySession) {
        let seed_sql = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/seed/bigquery.sql"),
        )
        .expect("read bigquery seed");

        for stmt in seed_sql.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() || stmt.starts_with("--") {
                continue;
            }
            match execute_sql(session, stmt) {
                Ok(resp) => {
                    if let Some(err) = resp.get("error") {
                        panic!("Seed statement failed: {:?}\nSQL:\n{}", err, stmt);
                    }
                }
                Err(e) => panic!("Seed failed: {}", e),
            }
        }
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_seed() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                eprintln!("BigQuery not configured, skipping");
                return;
            }
        };
        seed(&session);

        let resp = execute_sql(&session, "SELECT COUNT(*) as cnt FROM analytics.events")
            .expect("count query");
        println!("Seed verification: {:?}", resp);
        let count = get_cell(&resp, 0, 0);
        assert_eq!(count, "12", "Expected 12 rows, got {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_standard_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                eprintln!("BigQuery not configured, skipping");
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::BigQuery);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_compiled(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert!(count > 0, "Expected results for web platform, got 0 rows. Response: {:?}", resp);
        println!("Got {} rows", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_unfiltered_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::BigQuery);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_compiled(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert_eq!(count, 3, "Expected 3 platforms, got {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_measure_values_correct() {
        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::BigQuery);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let req = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.purchase_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        println!("SQL:\n{}", result.sql);

        let resp = execute_compiled(&session, &result.sql, &result.params).expect("execute");
        println!("Response: {:?}", resp);

        assert_eq!(row_count(&resp), 1, "Expected 1 row");
        // BigQuery returns all values as strings in the REST API
        assert_eq!(get_cell(&resp, 0, 0), "12", "Expected 12 total events");
        assert_eq!(get_cell(&resp, 0, 1), "4", "Expected 4 purchases");
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_profile_string_dimension() {
        use airlayer::engine::profiler;
        use airlayer::schema::parser::SchemaParser;

        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let parser = SchemaParser::new();
        let views = parser.parse_views(&views_dir).expect("parse");
        let view = views.iter().find(|v| v.name == "events").expect("find events view");

        let plan = profiler::plan_profile(view, "platform", &Dialect::BigQuery).unwrap();

        // Execute stats query
        let stats_resp = execute_sql(&session, &plan.stats_sql).expect("stats query");
        let cardinality: u64 = get_cell(&stats_resp, 0, 1).parse().expect("cardinality");
        assert_eq!(cardinality, 3, "Expected 3 distinct platforms");

        // Execute values query
        let values_fn = plan.values_sql_fn.as_ref().unwrap();
        let values_sql = values_fn(cardinality);
        let values_resp = execute_sql(&session, &values_sql).expect("values query");
        let count = row_count(&values_resp);
        assert_eq!(count, 3, "Expected 3 value rows");

        // Check top value is "web"
        let top_value = get_cell(&values_resp, 0, 0);
        assert_eq!(top_value, "web", "Expected top platform to be 'web'");
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_profile_number_dimension() {
        use airlayer::engine::profiler;
        use airlayer::schema::parser::SchemaParser;

        let session = match try_connect() {
            Some(s) => s,
            None => { return; }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let parser = SchemaParser::new();
        let views = parser.parse_views(&views_dir).expect("parse");
        let view = views.iter().find(|v| v.name == "events").expect("find events view");

        let plan = profiler::plan_profile(view, "revenue", &Dialect::BigQuery).unwrap();

        let stats_resp = execute_sql(&session, &plan.stats_sql).expect("stats query");
        println!("Number profile: {:?}", stats_resp);

        // min should be 0, max should be 99.99
        let min_val: f64 = get_cell(&stats_resp, 0, 3).parse().expect("min");
        let max_val: f64 = get_cell(&stats_resp, 0, 4).parse().expect("max");
        assert_eq!(min_val, 0.0, "Expected min 0");
        assert!((max_val - 99.99).abs() < 0.01, "Expected max ~99.99, got {}", max_val);

        assert!(plan.values_sql_fn.is_none(), "Number profiles should not have values query");
    }
}

// ---------------------------------------------------------------------------
// Tier 3: MotherDuck (cloud-hosted DuckDB)
// ---------------------------------------------------------------------------
mod motherduck_tests {
    use super::*;

    const DATABASE: &str = "airlayer_test";

    /// Connect to MotherDuck without specifying a database (needed for seed to CREATE DATABASE).
    fn try_connect_root() -> Option<duckdb::Connection> {
        dotenvy::dotenv().ok();
        let token = std::env::var("MOTHERDUCK_TOKEN").ok()?;
        if token.is_empty() {
            return None;
        }
        duckdb::Connection::open(&format!("md:?motherduck_token={}", token)).ok()
    }

    /// Connect to the airlayer_test database (used for queries after seeding).
    fn try_connect() -> Option<duckdb::Connection> {
        dotenvy::dotenv().ok();
        let token = std::env::var("MOTHERDUCK_TOKEN").ok()?;
        if token.is_empty() {
            return None;
        }
        duckdb::Connection::open(&format!("md:{}?motherduck_token={}", DATABASE, token)).ok()
    }

    fn execute_sql(conn: &duckdb::Connection, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = conn.prepare(sql).expect(&format!("prepare: {}", sql));
        let mut rows_out = Vec::new();
        let mut rows = stmt.query([]).expect("query");
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

    fn execute_compiled(
        conn: &duckdb::Connection,
        sql: &str,
        params: &[String],
    ) -> Vec<Vec<String>> {
        let rewritten = rewrite_params(sql);
        let mut stmt = conn
            .prepare(&rewritten)
            .expect(&format!("prepare failed for:\n{}", rewritten));
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

    static SEED_ONCE: std::sync::Once = std::sync::Once::new();

    fn seed() {
        SEED_ONCE.call_once(|| {
            // Use root connection (no database) for CREATE DATABASE
            let conn = try_connect_root().expect("connect to MotherDuck for seeding");
            let seed_sql = std::fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/seed/motherduck.sql"),
            )
            .expect("read motherduck seed");

            for stmt in seed_sql.split(';') {
                let stmt = stmt.trim();
                if stmt.is_empty() || stmt.starts_with("--") {
                    continue;
                }
                conn.execute_batch(stmt)
                    .unwrap_or_else(|e| panic!("Seed failed: {}\nSQL:\n{}", e, stmt));
            }
        });
    }

    fn load_motherduck_engine() -> SemanticEngine {
        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-motherduck");
        let dialects = DatasourceDialectMap::with_default(Dialect::DuckDB);
        SemanticEngine::load(&views_dir, None, dialects).expect("failed to load motherduck views")
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_seed() {
        let conn = match try_connect() {
            Some(c) => c,
            None => {
                eprintln!("MotherDuck not configured, skipping");
                return;
            }
        };
        seed();

        let rows = execute_sql(&conn, "SELECT COUNT(*) FROM analytics.events");
        assert_eq!(rows.len(), 1);
        let count = &rows[0][0];
        assert!(
            count.contains("12"),
            "Expected 12 rows, got {}",
            count
        );
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_standard_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => { return; }
        };
        seed();

        let engine = load_motherduck_engine();
        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected results for web platform");
        println!("Rows: {:?}", rows);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_unfiltered_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => { return; }
        };
        seed();

        let engine = load_motherduck_engine();
        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_segment_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => { return; }
        };
        seed();

        let engine = load_motherduck_engine();
        let result = engine.compile_query(&segment_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "Segment query should return 1 row");
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_measure_values_correct() {
        let conn = match try_connect() {
            Some(c) => c,
            None => { return; }
        };
        seed();

        let engine = load_motherduck_engine();
        let req = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.purchase_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        println!("SQL:\n{}", result.sql);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "Expected 1 row");
        println!("Values: {:?}", rows[0]);
        // total_events = 12, purchase_count = 4
        assert!(rows[0][0].contains("12"), "Expected 12 total events, got {}", rows[0][0]);
        assert!(rows[0][1].contains("4"), "Expected 4 purchases, got {}", rows[0][1]);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_schema_introspection() {
        let conn = match try_connect() {
            Some(c) => c,
            None => { return; }
        };
        seed();

        // Run the same information_schema query that introspect uses
        let rows = execute_sql(
            &conn,
            "SELECT table_schema, table_name, column_name, data_type, ordinal_position \
             FROM information_schema.columns \
             WHERE table_schema = 'analytics' AND table_name = 'events' \
             ORDER BY ordinal_position",
        );

        assert!(
            rows.len() >= 7,
            "Expected at least 7 columns in events table, got {}",
            rows.len()
        );
        println!("Schema columns: {:?}", rows);
    }
}
