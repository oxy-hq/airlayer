//! Integration tests: compile queries and execute them against real databases.
//!
//! Tier 1 (execution tests, no external services):
//!   - DuckDB: in-process, reads CSV seed data
//!   - SQLite: in-process, reads SQL seed data
//!
//! Tier 2 (execution tests, requires `docker compose -f docker-compose.test.yml up`):
//!   - PostgreSQL: on port $AIRLAYER_PG_PORT (default 15432)
//!   - MySQL: on port $AIRLAYER_MYSQL_PORT (default 13306)
//!   - ClickHouse: on port $AIRLAYER_CH_HTTP_PORT (default 18123)
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

/// Load port overrides from .test-ports.env if it exists (written by scripts/test-db-up.sh).
/// Only sets env vars that aren't already set, so explicit env vars still take precedence.
fn load_test_ports() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-ports.env");
        if let Ok(contents) = std::fs::read_to_string(&path) {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    if std::env::var(key).is_err() {
                        std::env::set_var(key, value);
                    }
                }
            }
        }
    });
}

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

/// Query with contribution motif: revenue by platform + share/total.
fn contribution_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("contribution".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with rank motif: rank platforms by revenue.
fn rank_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("rank".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with anomaly motif: detect anomalies in revenue by platform.
fn anomaly_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("anomaly".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with percent_of_total motif.
fn percent_of_total_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("percent_of_total".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with cumulative motif (time-series).
fn cumulative_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "events.created_at".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
        }],
        motif: Some("cumulative".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with moving_average motif (time-series).
fn moving_average_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "events.created_at".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
        }],
        motif: Some("moving_average".to_string()),
        ..QueryRequest::new()
    }
}

/// Query with period-over-period motif (time-series).
fn pop_motif_query() -> QueryRequest {
    QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "events.created_at".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
        }],
        motif: Some("dod".to_string()),
        ..QueryRequest::new()
    }
}

/// Load the checked-in `examples/same-store-sales` model for a given dialect.
/// Shared by the tier-2 shift execution tests.
#[allow(dead_code)]
fn load_engine_for_shift(dialect: Dialect) -> SemanticEngine {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/same-store-sales");
    SemanticEngine::load(&dir, None, DatasourceDialectMap::with_default(dialect))
        .expect("load same-store-sales views")
}

/// Same-store-sales FY2026-vs-FY2025 comp query (shift + lifespan cohort).
/// Shared by the DuckDB and tier-2 execution tests against the
/// `examples/same-store-sales` model.
fn shift_fy_query() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "sales.same_store_sales".to_string(),
            "sales.net_sales".to_string(),
            "sales.net_sales_prior".to_string(),
        ],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "sales.sale_date".to_string(),
            granularity: Some("year".to_string()),
            date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
        }],
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

        let mut stmt = db
            .prepare(&rewritten)
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", rewritten, e));
        let param_refs: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();

        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
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
    fn duckdb_boolean_dimension_filter() {
        // Regression: filtering a boolean dimension whose `expr` is itself a
        // comparison (`event_type = 'purchase'`) must compile to valid SQL —
        // `(event_type = 'purchase') = ?` — not the chained predicate
        // `event_type = 'purchase' = ?` that fails with "syntax error at or
        // near =". The 12 seeded rows include 4 purchases, so `is_purchase`
        // false returns the 8 non-purchase events.
        let engine = load_engine(Dialect::DuckDB);
        let request = QueryRequest {
            measures: vec!["events.total_events".to_string()],
            filters: vec![QueryFilter {
                member: Some("events.is_purchase".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["false".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        assert!(
            result.sql.contains("'purchase')"),
            "boolean dimension expr must be parenthesized, got:\n{}",
            result.sql
        );
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1, "aggregate without GROUP BY returns one row");
        assert!(
            rows[0][0].contains('8'),
            "expected 8 non-purchase events, got: {:?}",
            rows
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_is_not_null_predicate_dimension() {
        // Regression (#73): a boolean dimension whose `expr` is a word-only
        // predicate (`country IS NOT NULL`, the pokehouse `is_modifier` shape)
        // must render as a real predicate against the qualified column — never be
        // quoted whole as the nonexistent identifier `"country IS NOT NULL"`,
        // which fails at the warehouse. Executing against DuckDB proves the SQL
        // is valid, not just well-shaped.
        let engine = load_engine(Dialect::DuckDB);
        let request = QueryRequest {
            dimensions: vec!["events.has_country".to_string()],
            measures: vec!["events.total_events".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        // The predicate survives; it is not collapsed into one quoted identifier.
        assert!(
            result.sql.contains("IS NOT NULL"),
            "predicate must be emitted as-is, got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains("\"country IS NOT NULL\""),
            "predicate must not be quoted whole as an identifier, got:\n{}",
            result.sql
        );
        // All 12 seeded rows have a country, so there is a single `true` group.
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected one group, got: {:?}", rows);
        assert!(
            rows[0].iter().any(|c| c.contains("true")),
            "has_country should be true, got: {:?}",
            rows
        );
        assert!(
            rows[0].iter().any(|c| c.contains("12")),
            "expected count 12, got: {:?}",
            rows
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_measure_filter_bare_member_ref() {
        // Regression (#73): a measure filter that references a sibling member by
        // BARE name `{{is_purchase}}` (no view prefix — the pokehouse
        // `valid_orders` shape) must resolve to that dimension's expr, not be
        // left as an unresolvable `{{ "events"."is_purchase" }}`. The bare-ref
        // measure must yield the same count as the literal-filter measure.
        let engine = load_engine(Dialect::DuckDB);
        let request = QueryRequest {
            measures: vec![
                "events.purchase_count".to_string(),
                "events.purchase_count_via_ref".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
        // No unresolved template braces survive into the compiled SQL.
        assert!(
            !result.sql.contains("{{"),
            "unresolved bare member ref left in SQL:\n{}",
            result.sql
        );
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 1, "aggregate without GROUP BY returns one row");
        // Both measures count the 4 purchase events — and must agree.
        assert_eq!(
            rows[0][0], rows[0][1],
            "bare-ref measure must match literal-filter measure, got: {:?}",
            rows
        );
        assert!(
            rows[0][0].contains('4'),
            "expected 4 purchase events, got: {:?}",
            rows
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_contribution() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
        // Should have base columns (platform, total_revenue) + motif columns (total, share)
        assert!(
            rows[0].len() >= 4,
            "Expected >= 4 columns per row, got {}",
            rows[0].len()
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_rank() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine.compile_query(&rank_motif_query()).expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms");
        // Should have rank column
        assert!(
            result.sql.contains("RANK()"),
            "SQL should have RANK:\n{}",
            result.sql
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_anomaly() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine
            .compile_query(&anomaly_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms");
        // Should have z_score, is_anomaly columns
        assert!(
            result.sql.contains("z_score"),
            "SQL should have z_score:\n{}",
            result.sql
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_percent_of_total() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine
            .compile_query(&percent_of_total_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms");
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_cumulative() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine
            .compile_query(&cumulative_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected time-series rows");
        assert!(
            result.sql.contains("UNBOUNDED PRECEDING"),
            "SQL should have cumulative window:\n{}",
            result.sql
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_moving_average() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine
            .compile_query(&moving_average_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected time-series rows");
        assert!(
            result.sql.contains("moving_avg"),
            "SQL should have moving_avg:\n{}",
            result.sql
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_motif_dod() {
        let engine = load_engine(Dialect::DuckDB);
        let result = engine.compile_query(&pop_motif_query()).expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert!(!rows.is_empty(), "Expected time-series rows");
        assert!(
            result.sql.contains("previous_value"),
            "SQL should have previous_value:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("growth_rate"),
            "SQL should have growth_rate:\n{}",
            result.sql
        );
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
        assert!(
            row[0].contains("12"),
            "Expected 12 total events, got: {}",
            row[0]
        );
        assert!(
            row[1].contains("4"),
            "Expected 4 purchases, got: {}",
            row[1]
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_custom_motif_normalized() {
        let engine = load_engine_with_motifs(Dialect::DuckDB);
        let req = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            motif: Some("normalized".to_string()),
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile custom motif");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
        println!("Custom motif rows: {:?}", rows);
        // web has max revenue → normalized should be 1.0
        // android has 0 revenue → normalized should be 0.0
    }

    // ── Induced (promoted) measures ──────────────────────────
    //
    // Hand-built schema: `tx` (fact, store-grain) and `stores` (dim,
    // store-grain primary) with `stores.store_id parent: company_id`.
    // Verifies the v1 routing: an additive measure declared on `tx` is
    // induced on `stores` and on the parent grain via the entity hierarchy.

    fn induced_seed_sql() -> &'static str {
        "DROP TABLE IF EXISTS tx;
         DROP TABLE IF EXISTS stores;
         DROP TABLE IF EXISTS companies;
         CREATE TABLE companies (company_id VARCHAR PRIMARY KEY, name VARCHAR);
         CREATE TABLE stores (store_id VARCHAR PRIMARY KEY, company_id VARCHAR, region VARCHAR);
         CREATE TABLE tx (tx_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
         INSERT INTO companies VALUES ('c1', 'Acme'), ('c2', 'Beta');
         INSERT INTO stores VALUES
            ('s1', 'c1', 'West'),
            ('s2', 'c1', 'East'),
            ('s3', 'c2', 'West');
         INSERT INTO tx VALUES
            ('t1', 's1', 100),
            ('t2', 's1', 50),
            ('t3', 's2', 200),
            ('t4', 's3', 75),
            ('t5', 's3', 25);"
    }

    fn induced_views_yaml() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "tx",
                r#"
name: tx
table: tx
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: tx_id, type: string, expr: tx_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: net_sales, type: sum, expr: amount }
  - { name: avg_ticket, type: average, expr: amount }
"#,
            ),
            (
                "stores",
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id, parent: company_id }
  - { name: company_id, type: foreign, key: company_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: company_id, type: string, expr: company_id }
  - { name: region, type: string, expr: region }
"#,
            ),
            (
                "companies",
                r#"
name: companies
table: companies
entities:
  - { name: company_id, type: primary, key: company_id }
dimensions:
  - { name: company_id, type: string, expr: company_id }
  - { name: name, type: string, expr: name }
"#,
            ),
        ]
    }

    fn induced_engine() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let views = induced_views_yaml()
            .into_iter()
            .map(|(name, yaml)| parser.parse_view_str(yaml, name).expect("parse view"))
            .collect();
        let layer = SemanticLayer::new(views, None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    fn execute_with_seed(seed: &str, sql: &str, params: &[String]) -> Vec<Vec<String>> {
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(seed).expect("seed");
        let rewritten = rewrite_params(sql);
        let mut stmt = db
            .prepare(&rewritten)
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", rewritten, e));
        let param_refs: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
            }
            rows_out.push(vals);
        }
        rows_out
    }

    /// Additive single-hop: `stores.net_sales` is induced from `tx.net_sales`
    /// via the `store_id` Foreign edge. SUM(tx.amount) per store_id, joined
    /// to stores, grouped by region.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_additive_single_hop() {
        let engine = induced_engine();
        let req = QueryRequest {
            measures: vec!["stores.net_sales".to_string()],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile induced additive measure");
        println!("SQL:\n{}", result.sql);
        // The user-facing column name must restore to stores.net_sales even
        // though we computed it via tx.net_sales.
        let restored = result
            .columns
            .iter()
            .find(|c| c.member == "stores.net_sales");
        assert!(
            restored.is_some(),
            "expected stores.net_sales in result columns, got: {:?}",
            result.columns
        );
        let rows = execute_with_seed(induced_seed_sql(), &result.sql, &result.params);
        // West region: store s1 (150) + store s3 (100) = 250.
        // East region: store s2 (200).
        assert_eq!(rows.len(), 2, "expected 2 region rows, got: {:?}", rows);
        let mut sums: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for r in &rows {
            sums.insert(r[0].clone(), r[1].clone());
        }
        let west = sums
            .iter()
            .find(|(k, _)| k.contains("West"))
            .map(|(_, v)| v.clone())
            .expect("West row");
        let east = sums
            .iter()
            .find(|(k, _)| k.contains("East"))
            .map(|(_, v)| v.clone())
            .expect("East row");
        assert!(west.contains("250"), "West should sum to 250, got {}", west);
        assert!(east.contains("200"), "East should sum to 200, got {}", east);
    }

    /// Additive transitive: `companies.net_sales` is induced from
    /// `tx.net_sales` via store_id → company_id. SUM aggregates correctly
    /// across two hops because SUM is re-foldable.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_additive_two_hop() {
        let engine = induced_engine();
        let req = QueryRequest {
            measures: vec!["companies.net_sales".to_string()],
            dimensions: vec!["companies.name".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile two-hop induced measure");
        println!("SQL:\n{}", result.sql);
        let rows = execute_with_seed(induced_seed_sql(), &result.sql, &result.params);
        // Acme (c1): tx t1+t2 (s1) + t3 (s2) = 100+50+200 = 350
        // Beta (c2): tx t4+t5 (s3) = 100
        assert_eq!(rows.len(), 2, "expected 2 company rows, got: {:?}", rows);
        let mut sums: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for r in &rows {
            sums.insert(r[0].clone(), r[1].clone());
        }
        let acme = sums
            .iter()
            .find(|(k, _)| k.contains("Acme"))
            .map(|(_, v)| v.clone())
            .expect("Acme row");
        let beta = sums
            .iter()
            .find(|(k, _)| k.contains("Beta"))
            .map(|(_, v)| v.clone())
            .expect("Beta row");
        assert!(acme.contains("350"), "Acme should sum to 350, got {}", acme);
        assert!(beta.contains("100"), "Beta should sum to 100, got {}", beta);
    }

    /// Non-additive routing: `stores.avg_ticket` induced from `tx.avg_ticket`
    /// must aggregate source rows *directly* at target grain, not re-fold a
    /// per-store intermediate (which would average per-store averages).
    ///
    /// True values per region:
    ///   West = AVG(100, 50, 75, 25)  = 62.5
    ///   East = AVG(200)              = 200
    /// Average-of-averages would give:
    ///   West = AVG(AVG(100,50), AVG(75,25)) = AVG(75, 50) = 62.5     [same here by coincidence]
    /// Pick another shape: median or count_distinct.
    /// For COUNT_DISTINCT(store_id) per region:
    ///   West has stores {s1, s3} → 2
    ///   East has stores {s2}     → 1
    /// Re-folding per-store COUNT_DISTINCT would give 1+1=2 for West and 1
    /// for East → looks coincidentally right too. The test we actually want
    /// is AVG with values that *expose* the difference. Adjust the seed.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_non_additive_avg_is_direct() {
        let engine = induced_engine();
        let req = QueryRequest {
            measures: vec!["stores.avg_ticket".to_string()],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile non-additive induced");
        println!("SQL:\n{}", result.sql);
        // Sanity-check the SQL shape: must NOT introduce a per-store
        // intermediate aggregation of tx then average the result.
        assert!(
            !result.sql.contains("AVG(AVG"),
            "must not average averages; got: {}",
            result.sql
        );

        let rows = execute_with_seed(induced_seed_sql(), &result.sql, &result.params);
        // Expected (computed by hand against the seed):
        //   West region = stores {s1, s3}
        //     tx for s1: 100, 50 ; tx for s3: 75, 25 → rows = [100, 50, 75, 25]
        //     AVG = 250 / 4 = 62.5
        //   East region = stores {s2}
        //     tx for s2: 200 → AVG = 200
        let mut got: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for r in &rows {
            got.insert(r[0].clone(), r[1].clone());
        }
        let west = got
            .iter()
            .find(|(k, _)| k.contains("West"))
            .map(|(_, v)| v.clone())
            .expect("West row");
        let east = got
            .iter()
            .find(|(k, _)| k.contains("East"))
            .map(|(_, v)| v.clone())
            .expect("East row");
        // Parse the numeric portion out of duckdb's Debug format (e.g. "Float(62.5)").
        let west_num: f64 = west
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse()
            .unwrap_or(-1.0);
        let east_num: f64 = east
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse()
            .unwrap_or(-1.0);
        assert!(
            (west_num - 62.5).abs() < 1e-6,
            "West avg should be 62.5 (direct over 4 tx rows), got {}",
            west
        );
        assert!(
            (east_num - 200.0).abs() < 1e-6,
            "East avg should be 200 (s2 single tx of 200), got {}",
            east
        );
    }

    /// Passthrough discriminator dataset: per-store row counts and amounts
    /// chosen so that aggregate-quotient and average-of-store-ratios give
    /// observably different answers, ruling out coincidence.
    ///
    /// West region:
    ///   s1: tx [100, 50, 30] → SUM=180, COUNT=3, per-store ratio = 60
    ///   s3: tx [1000]        → SUM=1000, COUNT=1, per-store ratio = 1000
    ///   ── correct aggregate-quotient: 1180 / 4 = 295
    ///   ── wrong avg-of-store-ratios:  (60+1000)/2 = 530
    /// East region:
    ///   s2: tx [200, 200]    → SUM=400, COUNT=2, per-store ratio = 200
    ///   ── both interpretations give 200 (single-store region) — used as a
    ///     sanity row, not as a discriminator.
    fn passthrough_discriminator_seed() -> &'static str {
        "DROP TABLE IF EXISTS tx;
         DROP TABLE IF EXISTS stores;
         CREATE TABLE stores (store_id VARCHAR PRIMARY KEY, region VARCHAR);
         CREATE TABLE tx (tx_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
         INSERT INTO stores VALUES
            ('s1', 'West'),
            ('s2', 'East'),
            ('s3', 'West');
         INSERT INTO tx VALUES
            ('t1','s1',100), ('t2','s1',50), ('t3','s1',30),
            ('t4','s2',200), ('t5','s2',200),
            ('t6','s3',1000);"
    }

    /// Passthrough induced (`type: number`) — the canonical ratio case.
    /// `tx.amount_per_tx = SUM(tx.amount) / COUNT(tx.tx_id)` (a Number
    /// expression). Induced on stores must evaluate as
    /// `SUM(tx.amount per store-fiber) / COUNT(tx.tx_id per store-fiber)`,
    /// **not** `AVG(per-tx ratios)` or `AVG(per-store ratios)`. The leaves
    /// (sum and count) are projected to the target grain by the existing
    /// `{{view.measure}}` resolution; with the source view as base, the
    /// resolved expression aggregates the leaves at the requested target
    /// grain and the ratio is computed over those aggregates.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_passthrough_ratio_is_aggregate_quotient() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let tx_yaml = r#"
name: tx
table: tx
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: tx_id, type: string, expr: tx_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: total_amount, type: sum, expr: amount }
  - { name: tx_count, type: count }
  - name: amount_per_tx
    type: number
    expr: "CAST({{tx.total_amount}} AS DOUBLE) / NULLIF({{tx.tx_count}}, 0)"
"#;
        let stores_yaml = r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#;
        let layer = SemanticLayer::new(
            vec![
                parser.parse_view_str(tx_yaml, "tx").unwrap(),
                parser.parse_view_str(stores_yaml, "stores").unwrap(),
            ],
            None,
        );
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .unwrap();
        let req = QueryRequest {
            measures: vec!["stores.amount_per_tx".to_string()],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile passthrough induced");
        println!("SQL:\n{}", result.sql);

        // Restored member name (passthrough must still rename like additive).
        assert!(
            result
                .columns
                .iter()
                .any(|c| c.member == "stores.amount_per_tx"),
            "expected stores.amount_per_tx in result columns, got: {:?}",
            result.columns
        );

        let rows = execute_with_seed(
            passthrough_discriminator_seed(),
            &result.sql,
            &result.params,
        );
        // West:   SUM/COUNT = 1180/4 = 295  (avg-of-store-ratios would be 530)
        // East:   SUM/COUNT = 400/2  = 200  (sanity)
        let west = rows
            .iter()
            .find(|r| r[0].contains("West"))
            .map(|r| r[1].clone())
            .expect("West row");
        let east = rows
            .iter()
            .find(|r| r[0].contains("East"))
            .map(|r| r[1].clone())
            .expect("East row");
        let west_num: f64 = west
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse()
            .unwrap_or(-1.0);
        let east_num: f64 = east
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse()
            .unwrap_or(-1.0);
        assert!(
            (west_num - 295.0).abs() < 1e-6,
            "West = SUM/COUNT over joined rows (1180/4 = 295), got {} \
             (avg-of-store-ratios bug would give 530)",
            west
        );
        assert!(
            (east_num - 200.0).abs() < 1e-6,
            "East = SUM/COUNT (400/2 = 200), got {}",
            east
        );
    }

    // ── No-fanout (chasm trap) ───────────────────────────────
    //
    // Two independent fact views (`sales` and `returns`) both promote to
    // `stores`. Querying both induced measures together at the store grain
    // must NOT pair sales rows with returns rows — that's the dimensional
    // modeling "chasm trap" and silently inflates both totals by the other
    // side's row count per fiber.
    //
    // Seed uses coprime row counts per store so any cartesian inflation is
    // unmissable:
    //   Store s1 (West):  sales [10, 20, 30] (sum=60, n=3),
    //                     returns [5, 5]      (sum=10, n=2)
    //   Store s2 (East):  sales [50, 50]      (sum=100, n=2),
    //                     returns [15]        (sum=15, n=1)
    //
    // Correct unmultiplied totals:
    //   West: total_amount=60,  refund_amount=10
    //   East: total_amount=100, refund_amount=15
    //
    // Chasm-trap inflation would give:
    //   West: total_amount=60*2=120,  refund_amount=10*3=30
    //   East: total_amount=100*1=100, refund_amount=15*2=30
    //
    // Note East.total_amount happens to coincide; the discriminator is
    // East.refund_amount (15 correct, 30 trap) and West (both inflated).

    fn chasm_seed_sql() -> &'static str {
        "DROP TABLE IF EXISTS sales;
         DROP TABLE IF EXISTS returns;
         DROP TABLE IF EXISTS stores;
         CREATE TABLE stores (store_id VARCHAR PRIMARY KEY, region VARCHAR);
         CREATE TABLE sales (sale_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
         CREATE TABLE returns (return_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
         INSERT INTO stores VALUES ('s1', 'West'), ('s2', 'East');
         INSERT INTO sales VALUES
            ('sa1', 's1', 10),
            ('sa2', 's1', 20),
            ('sa3', 's1', 30),
            ('sa4', 's2', 50),
            ('sa5', 's2', 50);
         INSERT INTO returns VALUES
            ('r1', 's1', 5),
            ('r2', 's1', 5),
            ('r3', 's2', 15);"
    }

    fn chasm_engine() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: total_amount, type: sum, expr: amount }
"#,
                "sales",
            )
            .unwrap();
        let returns = parser
            .parse_view_str(
                r#"
name: returns
table: returns
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: return_id, type: string, expr: return_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: refund_amount, type: sum, expr: amount }
"#,
                "returns",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, returns, stores], None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    fn parse_num(s: &str) -> f64 {
        s.chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect::<String>()
            .parse()
            .unwrap_or(f64::NAN)
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_no_fanout_across_two_source_views() {
        let engine = chasm_engine();
        let req = QueryRequest {
            measures: vec![
                "stores.total_amount".to_string(),
                "stores.refund_amount".to_string(),
            ],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile two-induced no-fanout query");
        println!("SQL:\n{}", result.sql);

        // Both restored member names should appear.
        let mems: Vec<&str> = result.columns.iter().map(|c| c.member.as_str()).collect();
        assert!(
            mems.contains(&"stores.total_amount"),
            "missing stores.total_amount in {:?}",
            mems
        );
        assert!(
            mems.contains(&"stores.refund_amount"),
            "missing stores.refund_amount in {:?}",
            mems
        );

        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        // Sort columns by member position so we can read them deterministically.
        // The result row order matches columns metadata order.
        let region_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.region")
            .expect("stores.region column");
        let total_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.total_amount")
            .expect("stores.total_amount column");
        let refund_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.refund_amount")
            .expect("stores.refund_amount column");

        let mut got: std::collections::HashMap<String, (f64, f64)> =
            std::collections::HashMap::new();
        for r in &rows {
            let region = r[region_idx].clone();
            let total = parse_num(&r[total_idx]);
            let refund = parse_num(&r[refund_idx]);
            got.insert(region, (total, refund));
        }
        let west = got
            .iter()
            .find(|(k, _)| k.contains("West"))
            .map(|(_, v)| *v)
            .expect("West row");
        let east = got
            .iter()
            .find(|(k, _)| k.contains("East"))
            .map(|(_, v)| *v)
            .expect("East row");

        // West correct (60, 10); chasm-trap would give (120, 30)
        assert!(
            (west.0 - 60.0).abs() < 1e-6,
            "West.total_amount must be 60 (no fan-out), got {} \
             (chasm-trap inflation would give 120)",
            west.0
        );
        assert!(
            (west.1 - 10.0).abs() < 1e-6,
            "West.refund_amount must be 10 (no fan-out), got {} \
             (chasm-trap inflation would give 30)",
            west.1
        );
        // East correct (100, 15); chasm-trap on refund would give 30
        assert!(
            (east.0 - 100.0).abs() < 1e-6,
            "East.total_amount must be 100, got {}",
            east.0
        );
        assert!(
            (east.1 - 15.0).abs() < 1e-6,
            "East.refund_amount must be 15 (no fan-out), got {} \
             (chasm-trap inflation would give 30)",
            east.1
        );
    }

    // ── Zero-dimension additive measure, filtered on a chasm-trap sibling ──
    //
    // `sales` and `returns` are both ManyToOne siblings of the `stores` hub
    // (the same topology as the no-fanout test above), which makes
    // `detect_multiplied_views`'s chasm-trap case mark `sales` as
    // "multiplied" and route this query through the fan-out CTE path even
    // though a single-store filter can't actually fan out here. With zero
    // requested dimensions, the outer query has no GROUP BY, so the additive
    // path's `__dim_spine`/`__measures_sales` reconciliation is the only
    // thing anchoring the aggregate to the filtered store. It must still
    // resolve to the correct value (sales.total_amount for store s1 = 60),
    // not NULL.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_additive_measure_filtered_on_chasm_sibling() {
        let engine = chasm_engine();
        let req = QueryRequest {
            measures: vec!["sales.total_amount".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("returns.return_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["r1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected exactly one row, got {:?}", rows);
        let total = parse_num(&rows[0][0]);
        assert!(
            (total - 60.0).abs() < 1e-6,
            "sales.total_amount for store s1 (filtered via returns.return_id='r1') \
             must be 60, got {} (NULL/wrong reconciliation would show as NaN/0)",
            total
        );
    }

    // ── Additive fan-out path (generate_with_fanout_protection main body):
    //    a filter matching rows across MULTIPLE stores ──
    //
    // Round-1's test only covered a filter matching a single unique row.
    // This exercises the same zero-dim reconciliation with a filter that
    // matches returns rows across BOTH stores, so `__dim_spine` carries more
    // than one reconciliation row. Correct answer is the grand total across
    // every store with a matching return: 60 (s1) + 100 (s2) = 160.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_additive_measure_multi_row_filter_match() {
        let engine = chasm_engine();
        let req = QueryRequest {
            measures: vec!["sales.total_amount".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("returns.amount".to_string()),
                operator: Some(FilterOperator::Gt),
                values: vec!["0".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected exactly one row, got {:?}", rows);
        let total = parse_num(&rows[0][0]);
        assert!(
            (total - 160.0).abs() < 1e-6,
            "sales.total_amount across both stores (filter matches returns in \
             both s1 and s2) must be 160, got {}",
            total
        );
    }

    // ── Additive fan-out path: a COUNT measure (not SUM), filtered on a
    //    chasm-trap sibling ──
    //
    // Matches the "reachable-entity count" shape a caller most likely wants
    // (a real COUNT(*) rather than SUM). store s1 has 3 sales rows.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_count_measure_filtered_on_chasm_sibling() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: sale_count, type: count }
"#,
                "sales",
            )
            .unwrap();
        let returns = parser
            .parse_view_str(
                r#"
name: returns
table: returns
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: return_id, type: string, expr: return_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: refund_amount, type: sum, expr: amount }
"#,
                "returns",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, returns, stores], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine");

        let req = QueryRequest {
            measures: vec!["sales.sale_count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("returns.return_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["r1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected exactly one row, got {:?}", rows);
        let count = parse_num(&rows[0][0]);
        assert!(
            (count - 3.0).abs() < 1e-6,
            "sale_count for store s1 (filtered via returns.return_id='r1') must be 3, got {}",
            count
        );
    }

    // ── Additive fan-out path: BOTH chasm-trap siblings' measures together,
    //    zero dims, filtered on the shared hub itself ──
    //
    // Exercises the case where `measures_by_view` has two entries (each
    // gets its own measure CTE) and the filter lands on the hub (not either
    // sibling). Correct: refund_amount=10, total_amount=60 (store s1 only).
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_two_sibling_additive_measures_filtered_on_hub() {
        let engine = chasm_engine();
        let req = QueryRequest {
            measures: vec![
                "sales.total_amount".to_string(),
                "returns.refund_amount".to_string(),
            ],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("stores.store_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["s1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");
        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected exactly one row, got {:?}", rows);
        let refund_idx = result
            .columns
            .iter()
            .position(|c| c.member == "returns.refund_amount")
            .expect("refund_amount column");
        let total_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sales.total_amount")
            .expect("total_amount column");
        let refund = parse_num(&rows[0][refund_idx]);
        let total = parse_num(&rows[0][total_idx]);
        assert!(
            (refund - 10.0).abs() < 1e-6,
            "returns.refund_amount for store s1 must be 10, got {}",
            refund
        );
        assert!(
            (total - 60.0).abs() < 1e-6,
            "sales.total_amount for store s1 must be 60, got {}",
            total
        );
    }

    // ── Additive fan-out path: filter on a view TWO hops from the shared
    //    hub (not directly attached) ──
    //
    // sales -ManyToOne-> stores <-ManyToOne- returns <-ManyToOne- return_items.
    // The filtered view (return_items) isn't directly attached to the hub —
    // it hangs off `returns`, which is itself a chasm-trap sibling of
    // `sales`. Correct: sales.total_amount for store s1 (via return_items
    // i1 -> return r1 -> store s1) = 60.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_additive_measure_filtered_two_hops_from_hub() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: total_amount, type: sum, expr: amount }
"#,
                "sales",
            )
            .unwrap();
        let returns = parser
            .parse_view_str(
                r#"
name: returns
table: returns
entities:
  - { name: store_id, type: foreign, key: store_id }
  - { name: return_id, type: primary, key: return_id }
dimensions:
  - { name: return_id, type: string, expr: return_id }
  - { name: store_id, type: string, expr: store_id }
"#,
                "returns",
            )
            .unwrap();
        let return_items = parser
            .parse_view_str(
                r#"
name: return_items
table: return_items
entities:
  - { name: return_id, type: foreign, key: return_id }
dimensions:
  - { name: item_id, type: string, expr: item_id }
  - { name: return_id, type: string, expr: return_id }
"#,
                "return_items",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, returns, return_items, stores], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine");

        let req = QueryRequest {
            measures: vec!["sales.total_amount".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("return_items.item_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["i1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile");

        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (store_id VARCHAR PRIMARY KEY, region VARCHAR);
             CREATE TABLE sales (sale_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
             CREATE TABLE returns (return_id VARCHAR PRIMARY KEY, store_id VARCHAR);
             CREATE TABLE return_items (item_id VARCHAR PRIMARY KEY, return_id VARCHAR);
             INSERT INTO stores VALUES ('s1', 'West'), ('s2', 'East');
             INSERT INTO sales VALUES
                ('sa1', 's1', 10), ('sa2', 's1', 20), ('sa3', 's1', 30),
                ('sa4', 's2', 50), ('sa5', 's2', 50);
             INSERT INTO returns VALUES ('r1', 's1'), ('r2', 's2');
             INSERT INTO return_items VALUES ('i1', 'r1'), ('i2', 'r2');",
        )
        .expect("seed");
        let rewritten = rewrite_params(&result.sql);
        let mut stmt = db.prepare(&rewritten).expect("prepare");
        let param_refs: Vec<&dyn duckdb::ToSql> = result
            .params
            .iter()
            .map(|p| p as &dyn duckdb::ToSql)
            .collect();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        let row = rows
            .next()
            .expect("next")
            .expect("expected exactly one row");
        let total: f64 = row.get(0).expect("col0");
        assert!(
            (total - 60.0).abs() < 1e-6,
            "sales.total_amount for store s1 (filtered 2 hops via return_items.item_id='i1') \
             must be 60, got {}",
            total
        );
    }

    fn chasm_engine_count_distinct() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: distinct_sales, type: count_distinct, expr: sale_id }
"#,
                "sales",
            )
            .unwrap();
        let returns = parser
            .parse_view_str(
                r#"
name: returns
table: returns
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: return_id, type: string, expr: return_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: refund_amount, type: sum, expr: amount }
"#,
                "returns",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, returns, stores], None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    // ── Zero-dimension non-additive (COUNT_DISTINCT) measure, filtered on a
    //    chasm-trap sibling ──
    //
    // Same chasm topology, but `sales.distinct_sales` is COUNT_DISTINCT — a
    // non-additive measure type, so this routes through
    // `generate_with_user_grain_ctes` instead of the additive fan-out CTE
    // path. That function only pulled a filter's view into the per-source
    // CTE's join scope when `expand_views_for_expr_refs` discovered it via a
    // `{{view.field}}` template reference inside some other member's expr —
    // but a filter names its view directly (`"returns.return_id"`), not
    // through a template, so `returns` was never joined into the
    // `__measures_sales` CTE and compiling the WHERE clause failed with
    // "View 'returns' not in query" (a stand-in, in production, for a
    // caller that maps any compile failure to a null/zero count instead of
    // the true non-zero one). Regression test for that gap.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_zero_dim_count_distinct_filtered_on_chasm_sibling() {
        let engine = chasm_engine_count_distinct();
        let req = QueryRequest {
            measures: vec!["sales.distinct_sales".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("returns.return_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["r1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile zero-dim count_distinct query filtered on a sibling view");
        let rows = execute_with_seed(chasm_seed_sql(), &result.sql, &result.params);
        assert_eq!(rows.len(), 1, "expected exactly one row, got {:?}", rows);
        let distinct_sales = parse_num(&rows[0][0]);
        assert!(
            (distinct_sales - 3.0).abs() < 1e-6,
            "distinct_sales for store s1 (filtered via returns.return_id='r1') \
             must be 3 (sa1, sa2, sa3), got {}",
            distinct_sales
        );
    }

    // ── Mixed additive + non-additive measures on one view, fanning join ──
    //
    // `sales` now owns both an additive measure (`total_amount`, sum) and a
    // non-additive one (`distinct_sales`, count_distinct). Requesting both
    // together, filtered on the `returns` sibling, routes through
    // `generate_with_user_grain_ctes` (triggered by the non-additive
    // measure) with a single shared CTE for `sales` whose join tree includes
    // the OneToMany hop `stores -> returns`. That join can duplicate `sales`
    // rows once per matching `returns` row; `distinct_sales` is immune
    // (COUNT DISTINCT dedupes), but `total_amount` (SUM) would silently
    // double-count. Rather than guess, the engine must refuse to compile
    // this combination.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_mixed_additivity_measures_with_fanning_join_is_rejected() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: total_amount, type: sum, expr: amount }
  - { name: distinct_sales, type: count_distinct, expr: sale_id }
"#,
                "sales",
            )
            .unwrap();
        let returns = parser
            .parse_view_str(
                r#"
name: returns
table: returns
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: return_id, type: string, expr: return_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: refund_amount, type: sum, expr: amount }
"#,
                "returns",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, returns, stores], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine");

        let req = QueryRequest {
            measures: vec![
                "sales.total_amount".to_string(),
                "sales.distinct_sales".to_string(),
            ],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("returns.return_id".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["r1".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let err = engine
            .compile_query(&req)
            .expect_err("mixing additive + non-additive measures across a fanning join must be rejected, not silently wrong");
        let msg = err.to_string();
        assert!(
            msg.contains("additive") && msg.contains("non-additive"),
            "error should explain the additive/non-additive conflict, got: {}",
            msg
        );
    }

    // ── Mixed: explicit-on-target + induced-from-source ──────
    //
    // The target view declares its own measure AND inherits an induced
    // measure from a child view. Query both at once with a target-grain dim.
    //
    // Setup:
    //   stores has an explicit measure (store_count = COUNT(*))
    //   stores also has induced net_sales from sales (via store_id Foreign)
    //
    // For region West (s1, s3): store_count = 2, net_sales = SUM of sales
    // for s1+s3.
    //
    // The chasm trap doesn't apply here (only one source view multiplies
    // stores), but the planner has to decide which view to base on. The
    // `pick_base_view` tiebreaker prefers measure-owning views; with both
    // stores and sales owning measures, the cheaper join wins. Either base
    // should produce correct totals because SUM is additive — but
    // store_count on the parent could be miscounted if stores gets
    // multiplied by the join to sales without proper fan-out protection.

    fn mixed_seed_sql() -> &'static str {
        "DROP TABLE IF EXISTS sales;
         DROP TABLE IF EXISTS stores;
         CREATE TABLE stores (store_id VARCHAR PRIMARY KEY, region VARCHAR);
         CREATE TABLE sales (sale_id VARCHAR PRIMARY KEY, store_id VARCHAR, amount INTEGER);
         -- 3 stores: West has s1,s3; East has s2
         INSERT INTO stores VALUES ('s1', 'West'), ('s2', 'East'), ('s3', 'West');
         -- Multiple sales per store: s1 has 4, s2 has 1, s3 has 2
         INSERT INTO sales VALUES
            ('sa1','s1',10), ('sa2','s1',10), ('sa3','s1',10), ('sa4','s1',10),
            ('sa5','s2',100),
            ('sa6','s3',50), ('sa7','s3',50);"
    }

    fn mixed_engine() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: sale_id, type: string, expr: sale_id }
  - { name: store_id, type: string, expr: store_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: net_sales, type: sum, expr: amount }
"#,
                "sales",
            )
            .unwrap();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
measures:
  - { name: store_count, type: count }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![sales, stores], None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_mixed_explicit_and_induced() {
        let engine = mixed_engine();
        let req = QueryRequest {
            measures: vec![
                "stores.store_count".to_string(),
                "stores.net_sales".to_string(),
            ],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile mixed explicit+induced");
        println!("SQL:\n{}", result.sql);

        let rows = execute_with_seed(mixed_seed_sql(), &result.sql, &result.params);
        let region_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.region")
            .unwrap();
        let count_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.store_count")
            .unwrap();
        let sales_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.net_sales")
            .unwrap();

        let mut got: std::collections::HashMap<String, (f64, f64)> =
            std::collections::HashMap::new();
        for r in &rows {
            got.insert(
                r[region_idx].clone(),
                (parse_num(&r[count_idx]), parse_num(&r[sales_idx])),
            );
        }
        let west = got
            .iter()
            .find(|(k, _)| k.contains("West"))
            .map(|(_, v)| *v)
            .expect("West row");
        let east = got
            .iter()
            .find(|(k, _)| k.contains("East"))
            .map(|(_, v)| *v)
            .expect("East row");

        // West has 2 stores (s1, s3) → store_count = 2; sales = 4*10 + 2*50 = 140
        // East has 1 store (s2)     → store_count = 1; sales = 100
        // If stores got multiplied by sales without fan-out protection:
        //   West.store_count would be 4+2=6 (rows of sales for West stores)
        //   East.store_count would be 1 (s2 has 1 sale)
        // The discriminator: West.store_count == 2 vs the bug's 6.
        assert!(
            (west.0 - 2.0).abs() < 1e-6,
            "West.store_count must be 2 (two distinct stores), got {} \
             (fan-out by sales rows would give 6)",
            west.0
        );
        assert!(
            (west.1 - 140.0).abs() < 1e-6,
            "West.net_sales must be 140 (SUM over sales for s1+s3), got {}",
            west.1
        );
        assert!(
            (east.0 - 1.0).abs() < 1e-6,
            "East.store_count must be 1 (one store s2), got {}",
            east.0
        );
        assert!(
            (east.1 - 100.0).abs() < 1e-6,
            "East.net_sales must be 100 (s2 has one sale of 100), got {}",
            east.1
        );
    }

    // ── Ambiguous induced names + `through:` resolution ──────
    //
    // Marketplace shape: two fact views (`gmv`, `takerate`) both declare a
    // measure literally named `total` and both promote to `sellers` via
    // `seller_id`. Asking for `sellers.total` is ambiguous; today's planner
    // refuses unless `request.through` picks a side.
    //
    // The hint matches if it names a candidate's source view directly, or
    // if it names any entity in that candidate's hierarchy path.

    fn ambiguous_engine() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let gmv = parser
            .parse_view_str(
                r#"
name: gmv
table: gmv
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: total, type: sum, expr: amount }
"#,
                "gmv",
            )
            .unwrap();
        let takerate = parser
            .parse_view_str(
                r#"
name: takerate
table: takerate
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: fee, type: number, expr: fee }
measures:
  - { name: total, type: sum, expr: fee }
"#,
                "takerate",
            )
            .unwrap();
        let sellers = parser
            .parse_view_str(
                r#"
name: sellers
table: sellers
entities:
  - { name: seller_id, type: primary, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: tier, type: string, expr: tier }
"#,
                "sellers",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![gmv, takerate, sellers], None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    fn ambiguous_seed_sql() -> &'static str {
        "DROP TABLE IF EXISTS gmv;
         DROP TABLE IF EXISTS takerate;
         DROP TABLE IF EXISTS sellers;
         CREATE TABLE sellers (seller_id VARCHAR PRIMARY KEY, tier VARCHAR);
         CREATE TABLE gmv (gmv_id VARCHAR PRIMARY KEY, seller_id VARCHAR, amount INTEGER);
         CREATE TABLE takerate (tr_id VARCHAR PRIMARY KEY, seller_id VARCHAR, fee INTEGER);
         INSERT INTO sellers VALUES ('a', 'gold'), ('b', 'gold'), ('c', 'silver');
         -- GMV: large numbers per seller
         INSERT INTO gmv VALUES
            ('g1','a',1000), ('g2','a',2000),
            ('g3','b',5000),
            ('g4','c',300);
         -- Takerate: small fees per seller
         INSERT INTO takerate VALUES
            ('t1','a',100), ('t2','a',200),
            ('t3','b',500),
            ('t4','c',30);"
    }

    /// Without `through:`, asking for `sellers.total` errors with a clear
    /// message listing the candidate source views.
    #[test]
    fn induced_ambiguous_errors_with_candidates() {
        let engine = ambiguous_engine();
        let req = QueryRequest {
            measures: vec!["sellers.total".to_string()],
            dimensions: vec!["sellers.tier".to_string()],
            ..QueryRequest::new()
        };
        let err = engine
            .compile_query(&req)
            .expect_err("ambiguous induced must error");
        let msg = format!("{:?}", err);
        // Must name BOTH sources so the user can pick.
        assert!(
            msg.contains("gmv") && msg.contains("takerate"),
            "expected both source views in the error, got: {msg}"
        );
        // Must mention how to resolve.
        assert!(
            msg.contains("through:") || msg.contains("source view"),
            "expected a hint about disambiguation, got: {msg}"
        );
    }

    /// With `through: ["gmv"]`, the planner picks the gmv-derived induced
    /// measure. Sums match the gmv seed (large numbers).
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_through_picks_source_view() {
        let engine = ambiguous_engine();
        let req = QueryRequest {
            measures: vec!["sellers.total".to_string()],
            dimensions: vec!["sellers.tier".to_string()],
            through: vec!["gmv".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile with through hint");
        println!("SQL:\n{}", result.sql);
        let rows = execute_with_seed(ambiguous_seed_sql(), &result.sql, &result.params);
        let tier_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.tier")
            .unwrap();
        let total_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.total")
            .unwrap();
        let mut got: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for r in &rows {
            got.insert(r[tier_idx].clone(), parse_num(&r[total_idx]));
        }
        let gold = got
            .iter()
            .find(|(k, _)| k.contains("gold"))
            .map(|(_, v)| *v)
            .expect("gold tier");
        let silver = got
            .iter()
            .find(|(k, _)| k.contains("silver"))
            .map(|(_, v)| *v)
            .expect("silver tier");
        // GMV: a (1000+2000), b (5000), c (300)
        //   gold = a + b = 8000
        //   silver = c = 300
        assert!(
            (gold - 8000.0).abs() < 1e-6,
            "gold gmv.total should be 8000, got {} (takerate would give 800)",
            gold
        );
        assert!(
            (silver - 300.0).abs() < 1e-6,
            "silver gmv.total should be 300, got {} (takerate would give 30)",
            silver
        );
    }

    /// Swap the hint to `takerate` — same query, different numbers (now
    /// the small-fee side). The discriminator: takerate values are exactly
    /// 1/10 of gmv values in the seed.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_through_picks_other_source() {
        let engine = ambiguous_engine();
        let req = QueryRequest {
            measures: vec!["sellers.total".to_string()],
            dimensions: vec!["sellers.tier".to_string()],
            through: vec!["takerate".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile with takerate hint");
        let rows = execute_with_seed(ambiguous_seed_sql(), &result.sql, &result.params);
        let tier_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.tier")
            .unwrap();
        let total_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.total")
            .unwrap();
        let mut got: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for r in &rows {
            got.insert(r[tier_idx].clone(), parse_num(&r[total_idx]));
        }
        let gold = got
            .iter()
            .find(|(k, _)| k.contains("gold"))
            .map(|(_, v)| *v)
            .expect("gold tier");
        let silver = got
            .iter()
            .find(|(k, _)| k.contains("silver"))
            .map(|(_, v)| *v)
            .expect("silver tier");
        assert!(
            (gold - 800.0).abs() < 1e-6,
            "gold takerate.total should be 800 (10x smaller than gmv's 8000), got {}",
            gold
        );
        assert!(
            (silver - 30.0).abs() < 1e-6,
            "silver takerate.total should be 30 (10x smaller than gmv's 300), got {}",
            silver
        );
    }

    // ── Non-additive routing across two source views ─────────
    //
    // Both `gmv` and `takerate` promote to `sellers` with NON-additive
    // measures (AVG). At the seller-tier grain, the correct value is
    // AVG over all source rows in the tier — not AVG of per-seller AVGs.
    //
    // Seed values chosen so the discriminator is visible:
    //   Tier "gold" has sellers a, b
    //     gmv.amount: a=[10,30] → 20, b=[100] → 100
    //     true AVG over {10,30,100} = 140/3 ≈ 46.667
    //     avg-of-per-seller-AVGs = (20+100)/2 = 60  ← wrong
    //
    //     takerate.fee: a=[1,9] → 5, b=[50,50] → 50
    //     true AVG over {1,9,50,50} = 110/4 = 27.5
    //     avg-of-per-seller-AVGs = (5+50)/2 = 27.5  ← coincidentally same
    //   Tier "silver" has seller c: single row of each; both interpretations
    //     agree.
    //
    // The gold-gmv assertion (46.667 vs 60) is the load-bearing one.

    fn non_additive_chasm_seed() -> &'static str {
        "DROP TABLE IF EXISTS gmv;
         DROP TABLE IF EXISTS takerate;
         DROP TABLE IF EXISTS sellers;
         CREATE TABLE sellers (seller_id VARCHAR PRIMARY KEY, tier VARCHAR);
         CREATE TABLE gmv (gmv_id VARCHAR PRIMARY KEY, seller_id VARCHAR, amount INTEGER);
         CREATE TABLE takerate (tr_id VARCHAR PRIMARY KEY, seller_id VARCHAR, fee INTEGER);
         INSERT INTO sellers VALUES ('a','gold'), ('b','gold'), ('c','silver');
         INSERT INTO gmv VALUES
            ('g1','a',10), ('g2','a',30),
            ('g3','b',100),
            ('g4','c',75);
         INSERT INTO takerate VALUES
            ('t1','a',1), ('t2','a',9),
            ('t3','b',50), ('t4','b',50),
            ('t5','c',20);"
    }

    fn non_additive_chasm_engine() -> SemanticEngine {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let gmv = parser
            .parse_view_str(
                r#"
name: gmv
table: gmv
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: amount, type: number, expr: amount }
measures:
  - { name: avg_amount, type: average, expr: amount }
"#,
                "gmv",
            )
            .unwrap();
        let takerate = parser
            .parse_view_str(
                r#"
name: takerate
table: takerate
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: fee, type: number, expr: fee }
measures:
  - { name: avg_fee, type: average, expr: fee }
"#,
                "takerate",
            )
            .unwrap();
        let sellers = parser
            .parse_view_str(
                r#"
name: sellers
table: sellers
entities:
  - { name: seller_id, type: primary, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: tier, type: string, expr: tier }
"#,
                "sellers",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![gmv, takerate, sellers], None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine")
    }

    /// Two non-additive measures from two source views, both promoted to
    /// `sellers`. The correct values aggregate source rows directly at the
    /// requested tier grain — not the AVG of per-seller AVGs.
    #[test]
    #[ignore = "tier1"]
    fn duckdb_induced_non_additive_two_sources_at_shared_grain() {
        let engine = non_additive_chasm_engine();
        let req = QueryRequest {
            measures: vec![
                "sellers.avg_amount".to_string(), // induced from gmv
                "sellers.avg_fee".to_string(),    // induced from takerate
            ],
            dimensions: vec!["sellers.tier".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile two non-additive induced measures");
        println!("SQL:\n{}", result.sql);
        // Must NOT pre-aggregate per seller then re-average — that would
        // produce AVG-of-AVGs. The shape we want is one CTE per side, each
        // joining straight to sellers and grouping by tier inside the CTE.
        assert!(
            !result.sql.contains("AVG(AVG"),
            "must not average averages; got: {}",
            result.sql
        );

        let rows = execute_with_seed(non_additive_chasm_seed(), &result.sql, &result.params);
        let tier_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.tier")
            .unwrap();
        let amount_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.avg_amount")
            .unwrap();
        let fee_idx = result
            .columns
            .iter()
            .position(|c| c.member == "sellers.avg_fee")
            .unwrap();
        let mut got: std::collections::HashMap<String, (f64, f64)> =
            std::collections::HashMap::new();
        for r in &rows {
            got.insert(
                r[tier_idx].clone(),
                (parse_num(&r[amount_idx]), parse_num(&r[fee_idx])),
            );
        }
        let gold = got
            .iter()
            .find(|(k, _)| k.contains("gold"))
            .map(|(_, v)| *v)
            .expect("gold tier");
        // True AVG(gmv.amount) over {10,30,100} = 140/3 ≈ 46.667
        // Bug (avg-of-per-seller-AVGs) = (20+100)/2 = 60
        assert!(
            (gold.0 - (140.0 / 3.0)).abs() < 1e-3,
            "gold avg_amount should be 46.667 (direct over 3 gmv rows), got {} \
             (avg-of-per-seller-avgs bug would give 60)",
            gold.0
        );
        // True AVG(takerate.fee) over {1,9,50,50} = 27.5 (coincides with
        // avg-of-per-seller-AVGs here — the gold-gmv assertion is the real
        // discriminator).
        assert!(
            (gold.1 - 27.5).abs() < 1e-3,
            "gold avg_fee should be 27.5, got {}",
            gold.1
        );
        let silver = got
            .iter()
            .find(|(k, _)| k.contains("silver"))
            .map(|(_, v)| *v)
            .expect("silver tier");
        assert!(
            (silver.0 - 75.0).abs() < 1e-3,
            "silver avg_amount should be 75 (single row), got {}",
            silver.0
        );
        assert!(
            (silver.1 - 20.0).abs() < 1e-3,
            "silver avg_fee should be 20 (single row), got {}",
            silver.1
        );
    }

    // -------------------------------------------------------------------------
    // Bug-surfacing tests (expected to fail until fixed)
    // -------------------------------------------------------------------------

    /// Bug 4: When the same query contains both the induced form of a measure
    /// ("stores.net_sales") and the explicit source form ("tx.net_sales"),
    /// `rewrite_induced_measures` rewrites the induced form to "tx.net_sales"
    /// and records `restorations["tx.net_sales"] = "stores.net_sales"`. The
    /// restoration loop then patches EVERY column whose `member` equals
    /// "tx.net_sales" — including the one that was explicitly requested —
    /// stomping both to "stores.net_sales". The explicit column loses its
    /// identity.
    ///
    /// Fix: key the restoration map by a stable per-slot identifier (e.g.
    /// position or a fresh UUID) rather than the rewritten measure name.
    #[test]
    fn compile_induced_and_explicit_source_preserves_distinct_member_names() {
        let engine = induced_engine();
        let req = QueryRequest {
            measures: vec![
                "stores.net_sales".to_string(), // induced form → rewrites to tx.net_sales
                "tx.net_sales".to_string(),     // explicit source form → stays tx.net_sales
            ],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&req)
            .expect("compile with induced + explicit source should succeed");
        let members: Vec<&str> = result.columns.iter().map(|c| c.member.as_str()).collect();
        // The explicit column must keep its own member name.
        assert!(
            members.contains(&"tx.net_sales"),
            "explicit tx.net_sales column should retain member=\"tx.net_sales\"; got: {:?}",
            members
        );
        // The induced column must surface under its user-facing name.
        assert!(
            members.contains(&"stores.net_sales"),
            "induced stores.net_sales column should surface as member=\"stores.net_sales\"; got: {:?}",
            members
        );
    }

    /// Bug 7: `generate_fanout_ctes` (chasm path) builds `measure_cte_names`
    /// by iterating a `HashMap<String, Vec<&str>>`, then zips it a second time
    /// via `measures_by_view.values()` when constructing the final SELECT.
    /// HashMap iteration order is not guaranteed to be identical across two
    /// separate calls; if it diverges, CTE names are paired with the wrong
    /// measure column aliases, producing SQL that references columns that don't
    /// exist on the referenced CTE.
    ///
    /// This test verifies the column-metadata contract for the fan-out path
    /// with two source views having DIFFERENT measure names. If the zip
    /// ordering ever diverges, `.position(|c| c.member == "stores.total_amount")`
    /// will panic (None.unwrap) rather than silently returning wrong data.
    #[test]
    fn fanout_chasm_column_metadata_aligned_with_correct_source_view() {
        // Use the existing chasm_engine (sales + returns both induced onto stores).
        let engine = chasm_engine();
        let req = QueryRequest {
            measures: vec![
                "stores.total_amount".to_string(),  // induced from sales
                "stores.refund_amount".to_string(), // induced from returns
            ],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req).expect("compile chasm query");
        // Both restored member names must be present and distinct.
        let total_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.total_amount")
            .expect("stores.total_amount must be in column metadata");
        let refund_idx = result
            .columns
            .iter()
            .position(|c| c.member == "stores.refund_amount")
            .expect("stores.refund_amount must be in column metadata");
        assert_ne!(
            total_idx, refund_idx,
            "total_amount and refund_amount must occupy distinct column positions"
        );
        // The SQL must reference the correct CTE column for each measure alias.
        // If Bug 7 fires, the SQL would reference e.g. "__cte_sales"."stores__refund_amount"
        // (which doesn't exist) rather than "__cte_returns"."stores__refund_amount".
        assert!(
            result.sql.contains("refund_amount"),
            "SQL must reference refund_amount column: {}",
            result.sql
        );
        assert!(
            result.sql.contains("total_amount"),
            "SQL must reference total_amount column: {}",
            result.sql
        );
    }

    /// Bug 12: `rewrite_induced_measures` resolves `through` hints with the
    /// check `h == &c.source_view || c.path.contains(h)`. When an entity name
    /// coincidentally equals a view name used as the source in another
    /// candidate, the path-contains branch over-matches: BOTH candidates pass
    /// the filter even though the user intended to pick only the one whose
    /// source_view matches the hint.
    ///
    /// Concrete scenario:
    ///   - Entity "gmv" is defined as Primary on a "markets" view (entity name
    ///     and view name "gmv" are the same string).
    ///   - A fact view "gmv" AND a fact view "takerate" both have a Foreign
    ///     `seller_id`, whose Primary is "sellers" (parent: "gmv" entity).
    ///   - At the "markets" grain both "gmv" and "takerate" induce `total`.
    ///   - With `through: ["gmv"]`:
    ///       candidate "gmv"       path=["seller_id","gmv"] → h==source_view → matches ✓
    ///       candidate "takerate"  path=["seller_id","gmv"] → path.contains("gmv") → OVER-MATCHES ✗
    ///   - Result: matched.len()==2, so the engine returns "still ambiguous
    ///     after hint" even though the hint was unambiguous (source_view "gmv").
    ///
    /// Expected (correct) behaviour: `through: ["gmv"]` should select exactly
    /// the candidate whose `source_view == "gmv"`, producing matched.len()==1.
    ///
    /// This test is expected to FAIL until the bug is fixed.
    #[test]
    fn induced_through_hint_entity_name_equals_view_name_does_not_over_match() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        // Fact view "gmv" — the view name is "gmv", the same string as the
        // entity name used in the hierarchy root.
        let gmv = parser
            .parse_view_str(
                r#"
name: gmv
table: gmv
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
measures:
  - { name: total, type: sum, expr: amount }
"#,
                "gmv",
            )
            .unwrap();
        let takerate = parser
            .parse_view_str(
                r#"
name: takerate
table: takerate
entities:
  - { name: seller_id, type: foreign, key: seller_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
measures:
  - { name: total, type: sum, expr: fee }
"#,
                "takerate",
            )
            .unwrap();
        // sellers: seller_id Primary, parent "gmv" (entity named "gmv").
        // Also declares gmv as Foreign so the join graph has an edge to markets.
        let sellers = parser
            .parse_view_str(
                r#"
name: sellers
table: sellers
entities:
  - { name: seller_id, type: primary, key: seller_id, parent: gmv }
  - { name: gmv, type: foreign, key: gmv_id }
dimensions:
  - { name: seller_id, type: string, expr: seller_id }
  - { name: gmv_id, type: string, expr: gmv_id }
  - { name: tier, type: string, expr: tier }
"#,
                "sellers",
            )
            .unwrap();
        // markets: entity "gmv" Primary (entity name == view name "gmv").
        let markets = parser
            .parse_view_str(
                r#"
name: markets
table: markets
entities:
  - { name: gmv, type: primary, key: gmv_id }
dimensions:
  - { name: gmv_id, type: string, expr: gmv_id }
  - { name: region, type: string, expr: region }
"#,
                "markets",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![gmv, takerate, sellers, markets], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("engine");

        // markets.total is induced from both "gmv" and "takerate" (ambiguous).
        // `through: ["gmv"]` should resolve to exactly the "gmv" source view.
        // Bug 12: c.path.contains("gmv") also matches the "takerate" candidate
        // (whose path includes the "gmv" entity), causing "still ambiguous" error.
        let req = QueryRequest {
            measures: vec!["markets.total".to_string()],
            dimensions: vec!["markets.region".to_string()],
            through: vec!["gmv".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req);
        assert!(
            result.is_ok(),
            "through: [\"gmv\"] should unambiguously select source_view=\"gmv\"; \
             got error: {:?}",
            result.err()
        );
    }

    /// Bug 3: `compile_query` calls `resolve_dialect_for_query(request)` BEFORE
    /// `rewrite_induced_measures(request)`. The dialect is resolved from the
    /// ORIGINAL request's views (e.g. "stores"), not from the post-rewrite
    /// views (e.g. "tx"). If the target view and source view are on datasources
    /// with different dialects, the SQL is compiled in the target view's
    /// dialect instead of the source view's dialect.
    ///
    /// Concrete scenario: "stores" has dialect snowflake (uppercase identifiers);
    /// "tx" has dialect duckdb (lowercase identifiers). Querying
    /// `stores.net_sales` (an induced measure from tx) should generate SQL in
    /// DuckDB dialect (since tx is the source). Currently it generates SQL in
    /// Snowflake dialect (uppercase identifiers) because the dialect is resolved
    /// from "stores" before the rewrite.
    ///
    /// This test is expected to FAIL until the bug is fixed.
    #[test]
    fn induced_measure_dialect_resolved_from_source_view_not_target_view() {
        use airlayer::schema::models::SemanticLayer;
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        // tx: DuckDB datasource (duckdb dialect — lowercase identifiers).
        let tx = parser
            .parse_view_str(
                r#"
name: tx
table: tx
datasource: duckdb_db
entities:
  - { name: store_id, type: foreign, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
measures:
  - { name: net_sales, type: sum, expr: amount }
"#,
                "tx",
            )
            .unwrap();
        // stores: Snowflake datasource (snowflake dialect — UPPERCASE identifiers).
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
datasource: snowflake_db
entities:
  - { name: store_id, type: primary, key: store_id }
dimensions:
  - { name: store_id, type: string, expr: store_id }
  - { name: region, type: string, expr: region }
"#,
                "stores",
            )
            .unwrap();
        let layer = SemanticLayer::new(vec![tx, stores], None);
        let mut dialects = DatasourceDialectMap::new();
        dialects.insert("duckdb_db", Dialect::DuckDB);
        dialects.insert("snowflake_db", Dialect::Snowflake);
        // Note: the engine may error on cross-datasource queries. If it succeeds,
        // the dialect must be DuckDB (the source view's dialect), not Snowflake.
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).expect("engine");
        let req = QueryRequest {
            measures: vec!["stores.net_sales".to_string()],
            dimensions: vec!["stores.region".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&req);
        // After rewrite, the request uses tx.net_sales. The dialect check on the
        // original request only sees "stores" (snowflake_db → Snowflake). After
        // rewrite, tx (duckdb_db → DuckDB) is also used. The correct behaviour
        // is to error (cross-dialect) OR to use DuckDB dialect (the source).
        // The bug: the engine silently uses Snowflake dialect (from "stores")
        // without checking tx's dialect, generating UPPERCASE identifiers for tx
        // columns while the DuckDB engine expects lowercase.
        match result {
            Err(_) => {
                // Acceptable: cross-datasource induced measures are not supported.
                // The test passes if the engine errors on the inconsistency.
            }
            Ok(ref r) => {
                // If the engine succeeds, the SQL must NOT use Snowflake-style
                // UPPERCASE quoted identifiers (which come from the target view's
                // dialect being incorrectly applied to the source view's columns).
                // Snowflake dialect uppercases identifiers: "AMOUNT" instead of "amount".
                assert!(
                    !r.sql.contains("\"AMOUNT\""),
                    "SQL must not use Snowflake UPPERCASE quoting for tx columns; \
                     the source view (tx) uses duckdb_db dialect. Got SQL:\n{}",
                    r.sql
                );
            }
        }
    }

    /// `through:` referencing a non-candidate name (e.g. a misspelling)
    /// errors instead of silently falling back.
    #[test]
    fn induced_through_no_match_errors() {
        let engine = ambiguous_engine();
        let req = QueryRequest {
            measures: vec!["sellers.total".to_string()],
            dimensions: vec!["sellers.tier".to_string()],
            through: vec!["does_not_exist".to_string()],
            ..QueryRequest::new()
        };
        let err = engine
            .compile_query(&req)
            .expect_err("unmatched through must error");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("ambiguous") || msg.contains("Disambiguate"),
            "expected an ambiguity error, got: {msg}"
        );
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
        let mut stmt = db
            .prepare(sql)
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", sql, e));
        let param_refs: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|p| Box::new(p.clone()) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::types::ToSql> =
            param_refs.iter().map(|b| b.as_ref()).collect();

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
    fn sqlite_motif_contribution() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
        assert!(
            rows[0].len() >= 4,
            "Expected >= 4 columns per row, got {}",
            rows[0].len()
        );
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_motif_rank() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine.compile_query(&rank_motif_query()).expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms");
    }

    #[test]
    #[ignore = "tier1"]
    fn sqlite_motif_percent_of_total() {
        let engine = load_engine(Dialect::SQLite);
        let result = engine
            .compile_query(&percent_of_total_motif_query())
            .expect("compile");
        println!("SQL:\n{}", result.sql);
        let rows = execute_query(&result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms");
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
        assert!(
            row[0].contains("12"),
            "Expected 12 total events, got: {}",
            row[0]
        );
        assert!(
            row[1].contains("4"),
            "Expected 4 purchases, got: {}",
            row[1]
        );
    }
}

// ---------------------------------------------------------------------------
// Tier 2: PostgreSQL (docker, port 15432)
// ---------------------------------------------------------------------------
mod postgres_tests {
    use super::*;
    use std::sync::Once;

    static PG_SEED: Once = Once::new();

    fn try_connect() -> Option<postgres::Client> {
        load_test_ports();
        let port = std::env::var("AIRLAYER_PG_PORT").unwrap_or_else(|_| "15432".to_string());
        postgres::Client::connect(
            &format!(
                "host=localhost port={} user=airlayer password=airlayertest dbname=airlayer_test",
                port
            ),
            postgres::NoTls,
        )
        .ok()
    }

    fn seed() {
        PG_SEED.call_once(|| {
            // Idempotent: drop schema cascade then recreate from seed SQL.
            // Once ensures this only runs once even with parallel tests.
            let mut client = try_connect().expect("connect for seed");
            client
                .batch_execute("DROP SCHEMA IF EXISTS analytics CASCADE")
                .expect("drop schema");
            let seed_sql = include_str!("integration/seed/postgres.sql");
            client.batch_execute(seed_sql).expect("seed postgres");
        });
    }

    fn execute_query_simple(
        client: &mut postgres::Client,
        sql: &str,
        params: &[String],
    ) -> Result<usize, String> {
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
    fn postgres_seed() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                eprintln!("PostgreSQL not available, skipping");
                return;
            }
        };
        seed();
        let rows = client
            .query("SELECT COUNT(*) FROM analytics.events", &[])
            .expect("count");
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 12, "Expected 12 rows, got {}", count);
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
        seed();

        // Use the postgres-specific view with analytics. schema prefix
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count =
            execute_query_simple(&mut client, &result.sql, &result.params).expect("execute");
        assert!(row_count > 0, "Expected results");
        println!("Got {} rows", row_count);
    }

    #[test]
    #[ignore = "tier2"]
    fn postgres_motif_contribution() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
        };
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count =
            execute_query_simple(&mut client, &result.sql, &result.params).expect("execute");
        assert_eq!(row_count, 3, "Expected 3 platforms");
    }

    #[test]
    #[ignore = "tier2"]
    fn postgres_motif_rank() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
        };
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&rank_motif_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count =
            execute_query_simple(&mut client, &result.sql, &result.params).expect("execute");
        assert_eq!(row_count, 3, "Expected 3 platforms");
    }

    #[test]
    #[ignore = "tier2"]
    fn postgres_unfiltered_query() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
        };
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&unfiltered_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let row_count =
            execute_query_simple(&mut client, &result.sql, &result.params).expect("execute");
        assert_eq!(row_count, 3, "Expected 3 platforms");
    }

    /// Same-store-sales (shift + lifespan cohort) executed on real Postgres —
    /// proves the dialect's INTERVAL-cast date arithmetic and DATE literals run,
    /// and that the cohort restricts to {A, B} (current 2130 / prior 2200).
    #[test]
    #[ignore = "tier2"]
    fn postgres_shift_same_store_sales() {
        let mut client = match try_connect() {
            Some(c) => c,
            None => {
                eprintln!("PostgreSQL not available, skipping");
                return;
            }
        };
        // The example seed.sql is plain ANSI DDL and runs as-is on Postgres.
        let seed_sql = include_str!("../examples/same-store-sales/seed.sql");
        client.batch_execute(seed_sql).expect("seed shift tables");

        let engine = load_engine_for_shift(Dialect::Postgres);
        let result = engine.compile_query(&shift_fy_query()).expect("compile");
        println!("SQL:\n{}", result.sql);

        let rows = client
            .query(result.sql.as_str(), &[])
            .expect("execute shift");
        assert_eq!(rows.len(), 1, "expected one (year) row");
        // SUM(INTEGER) → bigint → i64. Cohort = {A, B}.
        let net: i64 = rows[0].get("sales__net_sales");
        let prior: i64 = rows[0].get("sales__net_sales_prior");
        assert_eq!(net, 2130, "current cohort net_sales (A+B)");
        assert_eq!(prior, 2200, "prior cohort net_sales (A+B)");
    }
}

// ---------------------------------------------------------------------------
// Tier 2: MySQL (docker, port 13306)
// ---------------------------------------------------------------------------
mod mysql_tests {
    use super::*;
    use mysql::prelude::Queryable;
    use std::sync::Once;

    static MYSQL_SEED: Once = Once::new();

    fn try_connect() -> Option<mysql::Pool> {
        load_test_ports();
        let port: u16 = std::env::var("AIRLAYER_MYSQL_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(13306);
        let opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("airlayer"))
            .pass(Some("airlayertest"))
            .db_name(Some("airlayer_test"));
        mysql::Pool::new(opts).ok()
    }

    fn seed(pool: &mysql::Pool) {
        MYSQL_SEED.call_once(|| {
            let mut conn = pool.get_conn().expect("get conn for seed");
            conn.query_drop("DROP TABLE IF EXISTS events")
                .expect("drop events");
            let seed_sql = include_str!("integration/seed/mysql.sql");
            // MySQL driver doesn't support multi-statement by default; split on semicolons
            for stmt in seed_sql.split(';') {
                // Strip comment lines, then check if anything remains
                let stripped: String = stmt
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let trimmed = stripped.trim();
                if !trimmed.is_empty() {
                    conn.query_drop(trimmed)
                        .unwrap_or_else(|e| panic!("seed statement: {}\n{}", trimmed, e));
                }
            }
        });
    }

    #[test]
    #[ignore = "tier2"]
    fn mysql_seed() {
        let pool = match try_connect() {
            Some(p) => p,
            None => {
                eprintln!("MySQL not available, skipping");
                return;
            }
        };
        seed(&pool);
        let mut conn = pool.get_conn().expect("get conn");
        let count: Vec<(i64,)> = conn.query("SELECT COUNT(*) FROM events").expect("count");
        assert_eq!(count[0].0, 12, "Expected 12 rows, got {}", count[0].0);
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
        seed(&pool);

        // MySQL uses airlayer_test.events (no analytics schema)
        let engine = load_engine(Dialect::MySQL);
        let result = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let mut conn = pool.get_conn().expect("get conn");
        // MySQL driver uses ? params natively — our generated SQL already uses ?
        let stmt = conn
            .prep(&result.sql)
            .unwrap_or_else(|e| panic!("prepare:\n{}\n{}", result.sql, e));
        let params_mysql: Vec<mysql::Value> = result
            .params
            .iter()
            .map(|p| mysql::Value::from(p.as_str()))
            .collect();
        let rows: Vec<mysql::Row> = conn.exec(stmt, params_mysql).expect("exec");
        assert!(!rows.is_empty(), "Expected results");
        println!("Got {} rows", rows.len());
    }

    /// Same-store-sales on real MySQL — proves DATE_ADD arithmetic and the
    /// CAST(DATE_FORMAT(...) AS DATE) bucket normalization run, and the cohort
    /// restricts to {A, B}. MySQL also does integer division, so this exercises
    /// the `* 1.0` float-promotion in the ratio measure.
    #[test]
    #[ignore = "tier2"]
    fn mysql_shift_same_store_sales() {
        let pool = match try_connect() {
            Some(p) => p,
            None => {
                eprintln!("MySQL not available, skipping");
                return;
            }
        };
        let mut conn = pool.get_conn().expect("get conn");
        // MySQL requires VARCHAR lengths, so use an engine-specific seed.
        let ddl = [
            "DROP TABLE IF EXISTS sales_daily",
            "DROP TABLE IF EXISTS stores",
            "CREATE TABLE stores (store_id VARCHAR(8), region VARCHAR(8), opened_at DATE, closed_at DATE)",
            "INSERT INTO stores VALUES \
               ('A','East','2021-01-01',NULL),('B','East','2023-01-01',NULL),\
               ('C','West','2025-07-01',NULL),('D','West','2026-02-01',NULL),\
               ('E','South','2019-01-01','2026-09-15')",
            "CREATE TABLE sales_daily (store_id VARCHAR(8), sale_date DATE, net_sales INT, transaction_count INT)",
            "INSERT INTO sales_daily VALUES \
               ('A','2025-01-15',500,50),('A','2025-07-15',500,50),('A','2026-01-15',490,49),('A','2026-07-15',490,49),\
               ('B','2025-01-15',600,60),('B','2025-07-15',600,60),('B','2026-01-15',575,57),('B','2026-07-15',575,58),\
               ('C','2025-08-15',200,20),('C','2025-10-15',200,20),('C','2026-01-15',425,42),('C','2026-07-15',425,43),\
               ('D','2026-03-15',250,25),('D','2026-07-15',250,25),\
               ('E','2025-01-15',450,45),('E','2025-07-15',450,45),('E','2026-01-15',350,35),('E','2026-08-15',350,35)",
        ];
        for stmt in ddl {
            conn.query_drop(stmt)
                .unwrap_or_else(|e| panic!("seed: {}\n{}", stmt, e));
        }

        let engine = load_engine_for_shift(Dialect::MySQL);
        let result = engine.compile_query(&shift_fy_query()).expect("compile");
        println!("SQL:\n{}", result.sql);

        let stmt = conn
            .prep(&result.sql)
            .unwrap_or_else(|e| panic!("prepare:\n{}\n{}", result.sql, e));
        let rows: Vec<mysql::Row> = conn.exec(stmt, ()).expect("exec shift");
        assert_eq!(rows.len(), 1, "expected one (year) row");
        // SUM(INT) comes back as DECIMAL; read as f64 to avoid type-mapping pitfalls.
        let net: f64 = rows[0].get("sales__net_sales").expect("net_sales");
        let prior: f64 = rows[0].get("sales__net_sales_prior").expect("prior");
        let comp: f64 = rows[0].get("sales__same_store_sales").expect("comp");
        assert_eq!(net, 2130.0, "current cohort net_sales (A+B)");
        assert_eq!(prior, 2200.0, "prior cohort net_sales (A+B)");
        assert!(
            (comp - (-0.031818)).abs() < 1e-4,
            "same_store_sales ≈ -3.18% (proves * 1.0 float division), got {}",
            comp
        );
    }
}

// ---------------------------------------------------------------------------
// Tier 2: ClickHouse (docker, HTTP port 18123)
// ---------------------------------------------------------------------------
mod clickhouse_tests {
    use super::*;
    use std::sync::Once;

    static CH_SEED: Once = Once::new();

    fn ch_base_url() -> String {
        load_test_ports();
        let port = std::env::var("AIRLAYER_CH_HTTP_PORT").unwrap_or_else(|_| "18123".to_string());
        format!("http://localhost:{}", port)
    }

    fn is_available() -> bool {
        ureq::get(&format!("{}/ping", ch_base_url())).call().is_ok()
    }

    fn seed() {
        CH_SEED.call_once(|| {
            // Idempotent: drop tables then recreate from seed SQL.
            for table in &["sales_daily_metrics", "restaurants", "orders", "events"] {
                let drop = format!("DROP TABLE IF EXISTS analytics.{}", table);
                ureq::post(&format!("{}/", ch_base_url()))
                    .send_string(&drop)
                    .unwrap_or_else(|e| panic!("drop {}: {}", table, e));
            }
            let seed_sql = include_str!("integration/seed/clickhouse.sql");
            for stmt in seed_sql.split(';') {
                // Strip comment lines, then check if anything remains
                let stripped: String = stmt
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let trimmed = stripped.trim();
                if !trimmed.is_empty() {
                    ureq::post(&format!("{}/", ch_base_url()))
                        .send_string(trimmed)
                        .unwrap_or_else(|e| {
                            panic!(
                                "seed statement: {}: {}",
                                &trimmed[..trimmed.len().min(80)],
                                e
                            )
                        });
                }
            }
        });
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
            rewritten =
                rewritten.replace(&placeholder, &format!("'{}'", param.replace('\'', "''")));
        }

        let resp = ureq::post(&format!("{}/", ch_base_url()))
            .query("database", "analytics")
            .send_string(&rewritten)
            .map_err(|e| format!("ClickHouse query failed: {}\nSQL:\n{}", e, rewritten))?;

        resp.into_string()
            .map_err(|e| format!("Read response: {}", e))
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_seed() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        seed();
        let output = execute_query("SELECT COUNT(*) FROM analytics.events", &[]).expect("count");
        assert!(
            output.trim().contains("12"),
            "Expected 12 rows, got: {}",
            output
        );
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_standard_query() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        seed();

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
    fn clickhouse_motif_contribution() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let output = execute_query(&result.sql, &result.params).expect("execute");
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3, "Expected 3 platforms, got:\n{}", output);
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_motif_rank() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine.compile_query(&rank_motif_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let output = execute_query(&result.sql, &result.params).expect("execute");
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3, "Expected 3 platforms, got:\n{}", output);
    }

    #[test]
    #[ignore = "tier2"]
    fn clickhouse_unfiltered_query() {
        if !is_available() {
            return;
        }
        seed();

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
// Tier 2: Presto/Trino (Docker, memory connector)
// ---------------------------------------------------------------------------
#[cfg(feature = "exec-presto")]
mod presto_tests {
    use super::*;
    use airlayer::executor::{self, PrestoConnection};
    use std::sync::Once;

    static PRESTO_SEED: Once = Once::new();

    fn presto_base_url() -> String {
        load_test_ports();
        let port = std::env::var("AIRLAYER_PRESTO_PORT").unwrap_or_else(|_| "18080".to_string());
        format!("http://localhost:{}", port)
    }

    fn presto_port() -> String {
        load_test_ports();
        std::env::var("AIRLAYER_PRESTO_PORT").unwrap_or_else(|_| "18080".to_string())
    }

    fn is_available() -> bool {
        ureq::get(&format!("{}/v1/info", presto_base_url()))
            .call()
            .is_ok()
    }

    /// Build a PrestoConnection pointing at the Docker Trino instance.
    fn test_connection() -> PrestoConnection {
        PrestoConnection {
            name: "test".to_string(),
            host: Some("http://localhost".to_string()),
            host_var: None,
            port: Some(presto_port()),
            port_var: None,
            user: Some("test".to_string()),
            user_var: None,
            password: None,
            password_var: None,
            catalog: Some("memory".to_string()),
            schema: Some("analytics".to_string()),
        }
    }

    fn execute_trino_sql(sql: &str) -> Result<(), String> {
        let url = format!("{}/v1/statement", presto_base_url());
        let resp: serde_json::Value = ureq::post(&url)
            .set("X-Trino-User", "test")
            .set("X-Trino-Catalog", "memory")
            .set("X-Trino-Schema", "analytics")
            .send_string(sql)
            .map_err(|e| format!("Trino submit failed: {}", e))?
            .into_json()
            .map_err(|e| format!("Parse response: {}", e))?;

        let mut current = resp;
        loop {
            if current.get("error").is_some() {
                let msg = current["error"]["message"]
                    .as_str()
                    .unwrap_or("unknown error");
                return Err(format!("Trino error: {}", msg));
            }
            match current.get("nextUri").and_then(|u| u.as_str()) {
                Some(next) => {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    current = ureq::get(next)
                        .call()
                        .map_err(|e| format!("Poll failed: {}", e))?
                        .into_json()
                        .map_err(|e| format!("Parse poll: {}", e))?;
                }
                None => return Ok(()),
            }
        }
    }

    fn seed() {
        PRESTO_SEED.call_once(|| {
            let seed_sql = include_str!("integration/seed/presto.sql");
            for stmt in seed_sql.split(';') {
                let stripped: String = stmt
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let trimmed = stripped.trim();
                if !trimmed.is_empty() {
                    if trimmed.starts_with("CREATE TABLE") {
                        if let Some(table_name) = trimmed.split_whitespace().nth(5) {
                            let _ =
                                execute_trino_sql(&format!("DROP TABLE IF EXISTS {}", table_name));
                        }
                    }
                    execute_trino_sql(trimmed).expect(&format!(
                        "seed statement: {}",
                        &trimmed[..trimmed.len().min(80)]
                    ));
                }
            }
        });
    }

    /// Execute via the real presto::execute() production code path.
    fn exec(sql: &str, params: &[String]) -> executor::ExecutionResult {
        let conn = test_connection();
        let db_conn = executor::DatabaseConnection::Presto(conn);
        executor::execute(&db_conn, sql, params).expect("executor::execute failed")
    }

    // --- Tests ---

    #[test]
    #[ignore = "tier2"]
    fn presto_seed() {
        if !is_available() {
            eprintln!("Presto/Trino not available, skipping");
            return;
        }
        seed();
        let result = exec("SELECT COUNT(*) AS cnt FROM memory.analytics.events", &[]);
        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0]["cnt"].as_i64().unwrap_or(0);
        assert_eq!(count, 12, "Expected 12 rows, got: {}", count);
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_standard_query() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", compiled.sql, compiled.params);

        // Execute through the real executor
        let result = exec(&compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 1, "Expected 1 row (web platform only)");
        // standard_query filters to platform='web', should have 7 events
        let row = &result.rows[0];
        let total_events = row
            .get("events__total_events")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(
            total_events, 7,
            "Expected 7 web events, got {}",
            total_events
        );
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_unfiltered_query() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine.compile_query(&unfiltered_query()).expect("compile");
        let result = exec(&compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        // Check column names are present
        assert!(result.columns.contains(&"events__platform".to_string()));
        assert!(result.columns.contains(&"events__total_events".to_string()));
        assert!(result.columns.contains(&"events__unique_users".to_string()));
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_motif_contribution() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        let result = exec(&compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        // Contribution motif adds total + share columns
        assert!(result.columns.contains(&"total".to_string()));
        assert!(result.columns.contains(&"share".to_string()));
        // Shares should sum to ~1.0
        let share_sum: f64 = result
            .rows
            .iter()
            .filter_map(|r| r.get("share").and_then(|v| v.as_f64()))
            .sum();
        assert!(
            (share_sum - 1.0).abs() < 0.01,
            "Shares should sum to 1.0, got {}",
            share_sum
        );
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_motif_rank() {
        if !is_available() {
            return;
        }
        seed();

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine.compile_query(&rank_motif_query()).expect("compile");
        let result = exec(&compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        assert!(result.columns.contains(&"rank".to_string()));
        // Ranks should be 1, 2, 3
        let mut ranks: Vec<i64> = result
            .rows
            .iter()
            .filter_map(|r| r.get("rank").and_then(|v| v.as_i64()))
            .collect();
        ranks.sort();
        assert_eq!(
            ranks,
            vec![1, 2, 3],
            "Expected ranks 1,2,3, got {:?}",
            ranks
        );
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_time_dimension() {
        if !is_available() {
            return;
        }
        seed();

        // Tests DATE_TRUNC('day', ...) which is Presto-specific syntax
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine
            .compile_query(&cumulative_motif_query())
            .expect("compile");
        println!("SQL:\n{}", compiled.sql);
        let result = exec(&compiled.sql, &compiled.params);
        // Seed data spans 3 days: 2025-01-15, 2025-01-16, 2025-01-17
        assert_eq!(
            result.rows.len(),
            3,
            "Expected 3 days, got {}",
            result.rows.len()
        );
        assert!(result.columns.contains(&"cumulative_value".to_string()));
        // Cumulative values should be monotonically non-decreasing
        let cumulative: Vec<f64> = result
            .rows
            .iter()
            .filter_map(|r| r.get("cumulative_value").and_then(|v| v.as_f64()))
            .collect();
        for i in 1..cumulative.len() {
            assert!(
                cumulative[i] >= cumulative[i - 1],
                "Cumulative values should be non-decreasing: {:?}",
                cumulative
            );
        }
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_anomaly_motif() {
        if !is_available() {
            return;
        }
        seed();

        // Tests STDDEV_POP and two-stage CTE which are Presto-specific
        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Presto);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine
            .compile_query(&anomaly_motif_query())
            .expect("compile");
        println!("SQL:\n{}", compiled.sql);
        let result = exec(&compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        // Anomaly motif adds: mean_value, stddev_value, z_score, is_anomaly
        assert!(result.columns.contains(&"mean_value".to_string()));
        assert!(result.columns.contains(&"stddev_value".to_string()));
        assert!(result.columns.contains(&"z_score".to_string()));
        assert!(result.columns.contains(&"is_anomaly".to_string()));
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_executor_error_handling() {
        if !is_available() {
            return;
        }

        let conn = test_connection();
        let db_conn = executor::DatabaseConnection::Presto(conn);
        // Invalid SQL should return an error, not panic
        let result = executor::execute(&db_conn, "SELECT FROM NONEXISTENT_TABLE_XYZ", &[]);
        assert!(result.is_err(), "Expected error for invalid SQL");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Presto error"),
            "Error should mention Presto: {}",
            err
        );
    }

    #[test]
    #[ignore = "tier2"]
    fn presto_connection_config_deserializes() {
        // Verify PrestoConnection round-trips through serde (config.yml format)
        let json = serde_json::json!({
            "name": "warehouse",
            "type": "presto",
            "host": "http://presto.example.com",
            "port": "8080",
            "user": "analyst",
            "catalog": "hive",
            "schema": "default"
        });

        let config: executor::ExecutionConfig = serde_json::from_value(serde_json::json!({
            "databases": [json]
        }))
        .expect("parse config");

        let conn = config
            .find_connection("warehouse")
            .expect("find connection");
        assert_eq!(conn.dialect_str(), "presto");
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
            .replace("@p", "$"); // BigQuery @p0 -> $0

        // Try to prepare (not execute) — catches syntax errors
        match db.prepare(&normalized) {
            Ok(_) => println!("[{}] SQL parses OK", dialect),
            Err(e) => {
                // Some dialect-specific functions won't exist in DuckDB, that's OK
                let err_str = e.to_string();
                if err_str.contains("Catalog Error") || err_str.contains("not found") {
                    println!(
                        "[{}] SQL has unknown functions (expected for cross-dialect): {}",
                        dialect, err_str
                    );
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
        let warehouse =
            std::env::var("SNOWFLAKE_WAREHOUSE").unwrap_or_else(|_| "COMPUTE_WH".to_string());

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

        Some(SnowflakeSession {
            account,
            token,
            warehouse,
        })
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
            .set(
                "Authorization",
                &format!("Snowflake Token=\"{}\"", session.token),
            )
            .set("Content-Type", "application/json")
            .set("Accept", "application/snowflake")
            .send_string(&body.to_string());

        match result {
            Ok(resp) => resp
                .into_json::<serde_json::Value>()
                .map_err(|e| format!("Failed to parse response: {}", e)),
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!(
                    "Snowflake API error (HTTP {}): {}\nSQL:\n{}",
                    code, body, sql
                ))
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
                        panic!(
                            "Seed statement failed: {:?}\nSQL:\n{}",
                            resp["message"], stmt
                        );
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
        assert!(
            count > 0,
            "Expected results for web platform, got 0 rows. Response: {:?}",
            resp
        );
        println!("Got {} rows", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_unfiltered_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                return;
            }
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
            None => {
                return;
            }
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
    fn snowflake_motif_contribution() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute");
        let count = row_count(&resp);
        assert_eq!(count, 3, "Expected 3 platforms, got {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn snowflake_measure_values_correct() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                return;
            }
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
        assert_eq!(
            row[0].as_str().unwrap_or(""),
            "12",
            "Expected 12 total events, got: {:?}",
            row[0]
        );
        assert_eq!(
            row[1].as_str().unwrap_or(""),
            "4",
            "Expected 4 purchases, got: {:?}",
            row[1]
        );
    }

    /// Issue #55 on its original dialect: cross-entity references in
    /// view-definition exprs must trigger JOINs. Executes all three broken
    /// contexts from the report against live Snowflake.
    #[test]
    #[ignore = "tier3"]
    fn snowflake_issue_55_cross_view_expr_refs() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                eprintln!("Snowflake not configured, skipping");
                return;
            }
        };
        seed(&session); // ensures AIRLAYER_TEST database + ANALYTICS schema exist

        for stmt in [
            "CREATE OR REPLACE TABLE ANALYTICS.ORDERS_I55 (ORDER_ID VARCHAR)",
            "INSERT INTO ANALYTICS.ORDERS_I55 VALUES ('o1'), ('o2'), ('o3')",
            "CREATE OR REPLACE TABLE ANALYTICS.ORDER_FLAGS_I55 (ORDER_ID VARCHAR, IS_FLAGGED BOOLEAN)",
            "INSERT INTO ANALYTICS.ORDER_FLAGS_I55 VALUES ('o1', true), ('o2', false)",
        ] {
            execute_sql(&session, stmt, &[]).expect("seed issue-55 tables");
        }

        let orders = r#"
name: orders
table: ANALYTICS.ORDERS_I55
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: flag_from_other_view
    type: boolean
    expr: "{{order_flags.is_flagged}}"
measures:
  - name: total_orders
    type: count_distinct
    expr: ORDER_ID
  - name: flagged_order_sum
    type: number
    expr: "SUM(CASE WHEN {{order_flags.is_flagged}} THEN 1 ELSE 0 END)"
  - name: total_flagged_orders
    type: count_distinct
    expr: ORDER_ID
    filters:
      - expr: "{{order_flags.is_flagged}}"
"#;
        let order_flags = r#"
name: order_flags
table: ANALYTICS.ORDER_FLAGS_I55
entities:
  - name: order
    type: foreign
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: is_flagged
    type: boolean
    expr: IS_FLAGGED
"#;
        let parser = airlayer::schema::parser::SchemaParser::new();
        let layer = airlayer::SemanticLayer::new(
            vec![
                parser.parse_view_str(orders, "<orders>").unwrap(),
                parser.parse_view_str(order_flags, "<order_flags>").unwrap(),
            ],
            None,
        );
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::Snowflake),
        )
        .expect("build engine");

        // Test 1: cross-view ref in dimension expr — three flag groups
        let result = engine
            .compile_query(&QueryRequest {
                measures: vec!["orders.total_orders".to_string()],
                dimensions: vec!["orders.flag_from_other_view".to_string()],
                ..QueryRequest::new()
            })
            .expect("compile dim-expr query");
        println!("Test 1 SQL:\n{}", result.sql);
        let resp = execute_sql(&session, &result.sql, &result.params).expect("execute dim-expr");
        assert_eq!(
            row_count(&resp),
            3,
            "one row per flag value: {:?}",
            resp["data"]
        );

        // Test 2: cross-view ref in measure expr — only o1 flagged
        let result = engine
            .compile_query(&QueryRequest {
                measures: vec!["orders.flagged_order_sum".to_string()],
                ..QueryRequest::new()
            })
            .expect("compile measure-expr query");
        println!("Test 2 SQL:\n{}", result.sql);
        let resp =
            execute_sql(&session, &result.sql, &result.params).expect("execute measure-expr");
        let row = resp["data"]["rowset"][0].as_array().expect("row");
        assert_eq!(row[0].as_str().unwrap_or(""), "1", "flagged sum: {:?}", row);

        // Test 3: cross-view ref in measure filter — one flagged order
        let result = engine
            .compile_query(&QueryRequest {
                measures: vec!["orders.total_flagged_orders".to_string()],
                ..QueryRequest::new()
            })
            .expect("compile measure-filter query");
        println!("Test 3 SQL:\n{}", result.sql);
        let resp =
            execute_sql(&session, &result.sql, &result.params).expect("execute measure-filter");
        let row = resp["data"]["rowset"][0].as_array().expect("row");
        assert_eq!(
            row[0].as_str().unwrap_or(""),
            "1",
            "flagged count: {:?}",
            row
        );
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

    fn execute_sql(session: &BigQuerySession, sql: &str) -> Result<serde_json::Value, String> {
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
        assert!(
            count > 0,
            "Expected results for web platform, got 0 rows. Response: {:?}",
            resp
        );
        println!("Got {} rows", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn bigquery_unfiltered_query() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                return;
            }
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
    fn bigquery_motif_contribution() {
        let session = match try_connect() {
            Some(s) => s,
            None => {
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let dialects = DatasourceDialectMap::with_default(Dialect::BigQuery);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
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
            None => {
                return;
            }
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
            None => {
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let parser = SchemaParser::new();
        let views = parser.parse_views(&views_dir).expect("parse");
        let view = views
            .iter()
            .find(|v| v.name == "events")
            .expect("find events view");

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
            None => {
                return;
            }
        };
        seed(&session);

        let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/multi-dialect/views");
        let parser = SchemaParser::new();
        let views = parser.parse_views(&views_dir).expect("parse");
        let view = views
            .iter()
            .find(|v| v.name == "events")
            .expect("find events view");

        let plan = profiler::plan_profile(view, "revenue", &Dialect::BigQuery).unwrap();

        let stats_resp = execute_sql(&session, &plan.stats_sql).expect("stats query");
        println!("Number profile: {:?}", stats_resp);

        // min should be 0, max should be 99.99
        let min_val: f64 = get_cell(&stats_resp, 0, 3).parse().expect("min");
        let max_val: f64 = get_cell(&stats_resp, 0, 4).parse().expect("max");
        assert_eq!(min_val, 0.0, "Expected min 0");
        assert!(
            (max_val - 99.99).abs() < 0.01,
            "Expected max ~99.99, got {}",
            max_val
        );

        assert!(
            plan.values_sql_fn.is_none(),
            "Number profiles should not have values query"
        );
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
        duckdb::Connection::open(format!("md:?motherduck_token={}", token)).ok()
    }

    /// Connect to the airlayer_test database (used for queries after seeding).
    fn try_connect() -> Option<duckdb::Connection> {
        dotenvy::dotenv().ok();
        let token = std::env::var("MOTHERDUCK_TOKEN").ok()?;
        if token.is_empty() {
            return None;
        }
        duckdb::Connection::open(format!("md:{}?motherduck_token={}", DATABASE, token)).ok()
    }

    fn execute_sql(conn: &duckdb::Connection, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = conn
            .prepare(sql)
            .unwrap_or_else(|e| panic!("prepare: {}\n{}", sql, e));
        let mut rows_out = Vec::new();
        let mut rows = stmt.query([]).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
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
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", rewritten, e));
        let param_refs: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
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
        assert!(count.contains("12"), "Expected 12 rows, got {}", count);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_standard_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
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
            None => {
                return;
            }
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
            None => {
                return;
            }
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
            None => {
                return;
            }
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
        assert!(
            rows[0][0].contains("12"),
            "Expected 12 total events, got {}",
            rows[0][0]
        );
        assert!(
            rows[0][1].contains("4"),
            "Expected 4 purchases, got {}",
            rows[0][1]
        );
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_motif_contribution() {
        let conn = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
        };
        seed();

        let engine = load_motherduck_engine();
        let result = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_motif_rank() {
        let conn = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
        };
        seed();

        let engine = load_motherduck_engine();
        let result = engine.compile_query(&rank_motif_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);

        let rows = execute_compiled(&conn, &result.sql, &result.params);
        assert_eq!(rows.len(), 3, "Expected 3 platforms, got: {:?}", rows);
    }

    #[test]
    #[ignore = "tier3_motherduck"]
    fn motherduck_schema_introspection() {
        let conn = match try_connect() {
            Some(c) => c,
            None => {
                return;
            }
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

// ---------------------------------------------------------------------------
// Tier 3: Databricks (live workspace)
// ---------------------------------------------------------------------------
//
// Env vars:
//   DATABRICKS_HOST          — workspace host (e.g., dbc-abc123.cloud.databricks.com)
//   DATABRICKS_TOKEN         — personal access token
//   DATABRICKS_WAREHOUSE_ID  — SQL warehouse ID
//
// The tests seed a workspace.airlayer_test schema with the standard events table.
// ---------------------------------------------------------------------------
#[cfg(feature = "exec-databricks")]
mod databricks_tests {
    use super::*;
    use airlayer::executor::{self, DatabricksConnection};
    use std::sync::Once;

    static DATABRICKS_SEED: Once = Once::new();

    fn try_connect() -> Option<DatabricksConnection> {
        dotenvy::dotenv().ok();
        let host = std::env::var("DATABRICKS_HOST").ok()?;
        let token = std::env::var("DATABRICKS_TOKEN").ok()?;
        let warehouse_id = std::env::var("DATABRICKS_WAREHOUSE_ID").ok()?;

        Some(DatabricksConnection {
            name: "test".to_string(),
            host: Some(host),
            host_var: None,
            token: Some(token),
            token_var: None,
            warehouse_id: Some(warehouse_id),
            warehouse_id_var: None,
            catalog: Some("workspace".to_string()),
            schema: Some("airlayer_test".to_string()),
        })
    }

    fn exec(
        conn: &DatabricksConnection,
        sql: &str,
        params: &[String],
    ) -> executor::ExecutionResult {
        let db_conn = executor::DatabaseConnection::Databricks(conn.clone());
        executor::execute(&db_conn, sql, params).expect("executor::execute failed")
    }

    fn seed() {
        DATABRICKS_SEED.call_once(|| {
            let conn = try_connect().expect("Databricks connection required for seeding");
            let seed_sql = include_str!("integration/seed/databricks.sql");
            for stmt in seed_sql.split(';') {
                let stripped: String = stmt
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let trimmed = stripped.trim();
                if !trimmed.is_empty() {
                    exec(&conn, trimmed, &[]);
                }
            }
        });
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_seed() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();
        let result = exec(
            &conn,
            "SELECT COUNT(*) AS cnt FROM workspace.airlayer_test.events",
            &[],
        );
        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0]["cnt"].as_i64().unwrap_or(0);
        assert_eq!(count, 12, "Expected 12 rows, got: {}", count);
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_standard_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();

        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-databricks");
        let dialects = DatasourceDialectMap::with_default(Dialect::Databricks);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine.compile_query(&standard_query()).expect("compile");
        println!("SQL:\n{}\nParams: {:?}", compiled.sql, compiled.params);

        let result = exec(&conn, &compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 1, "Expected 1 row (web platform only)");
        let total_events = result.rows[0]
            .get("events__total_events")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(
            total_events, 7,
            "Expected 7 web events, got {}",
            total_events
        );
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_unfiltered_query() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();

        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-databricks");
        let dialects = DatasourceDialectMap::with_default(Dialect::Databricks);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine.compile_query(&unfiltered_query()).expect("compile");
        let result = exec(&conn, &compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        assert!(result.columns.contains(&"events__platform".to_string()));
        assert!(result.columns.contains(&"events__total_events".to_string()));
        assert!(result.columns.contains(&"events__unique_users".to_string()));
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_motif_contribution() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();

        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-databricks");
        let dialects = DatasourceDialectMap::with_default(Dialect::Databricks);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine
            .compile_query(&contribution_motif_query())
            .expect("compile");
        let result = exec(&conn, &compiled.sql, &compiled.params);
        assert_eq!(result.rows.len(), 3, "Expected 3 platforms");
        assert!(result.columns.contains(&"total".to_string()));
        assert!(result.columns.contains(&"share".to_string()));
        let share_sum: f64 = result
            .rows
            .iter()
            .filter_map(|r| r.get("share").and_then(|v| v.as_f64()))
            .sum();
        assert!(
            (share_sum - 1.0).abs() < 0.01,
            "Shares should sum to 1.0, got {}",
            share_sum
        );
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_measure_values() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();

        let result = exec(
            &conn,
            "SELECT \
               SUM(CASE WHEN platform = 'web' THEN revenue_cents ELSE 0 END) AS web_rev, \
               SUM(CASE WHEN platform = 'ios' THEN revenue_cents ELSE 0 END) AS ios_rev, \
               COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases \
             FROM workspace.airlayer_test.events",
            &[],
        );
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row["web_rev"].as_i64().unwrap(), 16498);
        assert_eq!(row["ios_rev"].as_i64().unwrap(), 2500);
        assert_eq!(row["purchases"].as_i64().unwrap(), 4);
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_time_dimension() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        seed();

        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-databricks");
        let dialects = DatasourceDialectMap::with_default(Dialect::Databricks);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let compiled = engine
            .compile_query(&cumulative_motif_query())
            .expect("compile");
        println!("SQL:\n{}", compiled.sql);
        let result = exec(&conn, &compiled.sql, &compiled.params);
        assert_eq!(
            result.rows.len(),
            3,
            "Expected 3 days, got {}",
            result.rows.len()
        );
        assert!(result.columns.contains(&"cumulative_value".to_string()));
        // Cumulative values should be monotonically non-decreasing
        let cumulative: Vec<f64> = result
            .rows
            .iter()
            .filter_map(|r| r.get("cumulative_value").and_then(|v| v.as_f64()))
            .collect();
        for w in cumulative.windows(2) {
            assert!(
                w[1] >= w[0],
                "Cumulative should be non-decreasing: {:?}",
                cumulative
            );
        }
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_error_handling() {
        let conn = match try_connect() {
            Some(c) => c,
            None => return,
        };
        let db_conn = executor::DatabaseConnection::Databricks(conn);
        let result = executor::execute(&db_conn, "SELECT FROM NONEXISTENT_TABLE_XYZ", &[]);
        assert!(result.is_err(), "Invalid SQL should return an error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Databricks"),
            "Error should mention Databricks: {}",
            err
        );
    }

    #[test]
    #[ignore = "tier3"]
    fn databricks_connection_config_deserializes() {
        let yaml = r#"
name: my_databricks
host: dbc-abc123.cloud.databricks.com
token_var: DATABRICKS_TOKEN
warehouse_id: abc123
catalog: main
schema: default
"#;
        let conn: DatabricksConnection = serde_yaml::from_str(yaml).expect("deserialize");
        assert_eq!(conn.name, "my_databricks");
        assert_eq!(
            conn.host.as_deref(),
            Some("dbc-abc123.cloud.databricks.com")
        );
        assert_eq!(conn.token_var.as_deref(), Some("DATABRICKS_TOKEN"));
        assert_eq!(conn.warehouse_id.as_deref(), Some("abc123"));
        assert_eq!(conn.catalog.as_deref(), Some("main"));
        assert_eq!(conn.schema.as_deref(), Some("default"));
    }
}

// ---------------------------------------------------------------------------
// Motif compilation tests (no external services needed)
// ---------------------------------------------------------------------------

#[test]
fn test_motif_contribution_compiles() {
    let engine = load_engine(Dialect::Postgres);
    let req = QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("contribution".to_string()),
        ..QueryRequest::new()
    };
    let result = engine
        .compile_query(&req)
        .expect("compile with contribution motif");
    assert!(
        result.sql.contains("WITH __base AS"),
        "SQL should have CTE:\n{}",
        result.sql
    );
    assert!(
        result.sql.contains("SUM("),
        "SQL should have SUM OVER:\n{}",
        result.sql
    );
    assert!(
        result.sql.contains("share"),
        "SQL should have share column:\n{}",
        result.sql
    );
    // Should have base columns + motif columns
    assert!(
        result.columns.len() >= 4,
        "Expected >= 4 columns, got {}",
        result.columns.len()
    );
}

#[test]
fn test_motif_rank_compiles() {
    let engine = load_engine(Dialect::Postgres);
    let req = QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("rank".to_string()),
        ..QueryRequest::new()
    };
    let result = engine.compile_query(&req).expect("compile with rank motif");
    assert!(
        result.sql.contains("RANK()"),
        "SQL should have RANK:\n{}",
        result.sql
    );
}

#[test]
fn test_motif_percent_of_total_compiles() {
    let engine = load_engine(Dialect::BigQuery);
    let req = QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("percent_of_total".to_string()),
        ..QueryRequest::new()
    };
    let result = engine
        .compile_query(&req)
        .expect("compile with percent_of_total motif");
    assert!(
        result.sql.contains("percent_of_total"),
        "SQL:\n{}",
        result.sql
    );
    // BigQuery uses backtick quoting
    assert!(
        result.sql.contains('`'),
        "SQL should use BigQuery quoting:\n{}",
        result.sql
    );
}

#[test]
fn test_motif_unknown_errors() {
    let engine = load_engine(Dialect::Postgres);
    let req = QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("nonexistent_motif".to_string()),
        ..QueryRequest::new()
    };
    let err = engine.compile_query(&req).unwrap_err();
    assert!(err.to_string().contains("Unknown motif"), "Error: {}", err);
}

// ---------------------------------------------------------------------------
// Custom motif tests
// ---------------------------------------------------------------------------

/// Load engine from the integration directory with motifs/ and queries/.
fn load_engine_with_motifs(dialect: Dialect) -> SemanticEngine {
    use airlayer::schema::models::SemanticLayer;
    use airlayer::schema::parser::SchemaParser;

    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
    let parser = SchemaParser::new();

    let layer = parser
        .parse_directory(&base.join("views"), None)
        .expect("parse views");
    let motifs = parser
        .parse_motifs(&base.join("motifs"))
        .expect("parse motifs");
    let queries = parser
        .parse_saved_queries(&base.join("queries"))
        .expect("parse queries");

    let full_layer = SemanticLayer::with_motifs_and_queries(
        layer.views,
        layer.topics.clone(),
        if motifs.is_empty() {
            None
        } else {
            Some(motifs)
        },
        if queries.is_empty() {
            None
        } else {
            Some(queries)
        },
    );

    let dialects = DatasourceDialectMap::with_default(dialect);
    SemanticEngine::from_semantic_layer(full_layer, dialects).expect("build engine")
}

#[test]
fn test_custom_motif_normalized_compiles() {
    let engine = load_engine_with_motifs(Dialect::Postgres);
    let req = QueryRequest {
        measures: vec!["events.total_revenue".to_string()],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("normalized".to_string()),
        ..QueryRequest::new()
    };
    let result = engine
        .compile_query(&req)
        .expect("compile with custom motif");
    assert!(
        result.sql.contains("WITH __base AS"),
        "Should wrap as CTE:\n{}",
        result.sql
    );
    assert!(
        result.sql.contains("MIN("),
        "Should have MIN:\n{}",
        result.sql
    );
    assert!(
        result.sql.contains("MAX("),
        "Should have MAX:\n{}",
        result.sql
    );
    assert!(
        result.sql.contains("normalized"),
        "Should have normalized column:\n{}",
        result.sql
    );
    println!("Custom motif SQL:\n{}", result.sql);
}

#[test]
fn test_custom_motif_normalized_multi_measure_requires_explicit_param() {
    let engine = load_engine_with_motifs(Dialect::Postgres);
    // Multi-measure without explicit motif_params → should error
    let req = QueryRequest {
        measures: vec![
            "events.total_revenue".to_string(),
            "events.total_events".to_string(),
        ],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("normalized".to_string()),
        ..QueryRequest::new()
    };
    let err = engine.compile_query(&req).unwrap_err();
    assert!(
        err.to_string().contains("motif_params"),
        "Error should mention motif_params: {}",
        err
    );
}

#[test]
fn test_custom_motif_normalized_multi_measure_with_explicit_param() {
    let engine = load_engine_with_motifs(Dialect::Postgres);
    let mut motif_params = std::collections::HashMap::new();
    motif_params.insert(
        "measure".to_string(),
        serde_json::json!("events.total_revenue"),
    );
    let req = QueryRequest {
        measures: vec![
            "events.total_revenue".to_string(),
            "events.total_events".to_string(),
        ],
        dimensions: vec!["events.platform".to_string()],
        motif: Some("normalized".to_string()),
        motif_params,
        ..QueryRequest::new()
    };
    let result = engine
        .compile_query(&req)
        .expect("compile with explicit measure param");
    assert!(
        result.sql.contains("normalized"),
        "Should have normalized column:\n{}",
        result.sql
    );
    println!(
        "Multi-measure custom motif with explicit param SQL:\n{}",
        result.sql
    );
}

// ---------------------------------------------------------------------------
// Saved query parsing/validation tests
// ---------------------------------------------------------------------------

#[test]
fn test_saved_queries_parse_and_validate() {
    use airlayer::schema::parser::SchemaParser;

    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
    let parser = SchemaParser::new();
    let queries = parser
        .parse_saved_queries(&base.join("queries"))
        .expect("parse queries");

    assert_eq!(
        queries.len(),
        2,
        "Expected 2 saved queries, got {}",
        queries.len()
    );

    let revenue = queries
        .iter()
        .find(|s| s.name == "revenue_investigation")
        .expect("find revenue_investigation");
    let steps = revenue.effective_steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].name, "overall_trend");
    assert_eq!(steps[1].name, "anomaly_detection");
    assert_eq!(steps[2].name, "platform_breakdown");
    assert!(revenue.params.contains_key("metric"));

    let platform = queries
        .iter()
        .find(|s| s.name == "platform_comparison")
        .expect("find platform_comparison");
    let platform_steps = platform.effective_steps();
    assert_eq!(platform_steps.len(), 3);
    assert!(platform.params.is_empty());
}

#[test]
fn test_saved_query_steps_compile() {
    let engine = load_engine_with_motifs(Dialect::Postgres);

    // Every step in a saved query is a structured QueryRequest — verify each compiles to valid SQL.
    use airlayer::schema::parser::SchemaParser;

    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
    let parser = SchemaParser::new();
    let queries = parser
        .parse_saved_queries(&base.join("queries"))
        .expect("parse");
    let revenue = queries
        .iter()
        .find(|s| s.name == "revenue_investigation")
        .expect("find");
    let steps = revenue.effective_steps();

    for step in &steps {
        let result = engine
            .compile_query(&step.query)
            .unwrap_or_else(|e| panic!("compile step '{}': {}", step.name, e));
        println!("Step '{}' SQL:\n{}", step.name, result.sql);
        assert!(
            !result.sql.is_empty(),
            "Step '{}' produced empty SQL",
            step.name
        );
    }
}

// ---------------------------------------------------------------------------
// Pre-aggregation: ClickHouse build + coverage integration tests
// ---------------------------------------------------------------------------
#[cfg(feature = "exec")]
mod preagg_tests {
    use super::*;
    use std::sync::Once;

    static PREAGG_SEED: Once = Once::new();
    static PREAGG_BUILD: Once = Once::new();

    const PREAGG_SCHEMA: &str = "airlayer_test_preagg";
    const DATE_STR: &str = "20260415";

    fn ch_base_url() -> String {
        load_test_ports();
        let port = std::env::var("AIRLAYER_CH_HTTP_PORT").unwrap_or_else(|_| "18123".to_string());
        format!("http://localhost:{}", port)
    }

    fn is_available() -> bool {
        ureq::get(&format!("{}/ping", ch_base_url())).call().is_ok()
    }

    fn ch_exec(sql: &str) -> Result<String, String> {
        let resp = ureq::post(&format!("{}/", ch_base_url()))
            .send_string(sql)
            .map_err(|e| format!("ClickHouse error: {}\nSQL: {}", e, sql))?;
        resp.into_string().map_err(|e| format!("Read error: {}", e))
    }

    fn seed() {
        PREAGG_SEED.call_once(|| {
            for table in &["events"] {
                let drop = format!("DROP TABLE IF EXISTS analytics.{}", table);
                ch_exec(&drop).ok();
            }
            let seed_sql = include_str!("integration/seed/clickhouse.sql");
            for stmt in seed_sql.split(';') {
                let stripped: String = stmt
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let trimmed = stripped.trim();
                if !trimmed.is_empty()
                    && (trimmed.contains("analytics.events")
                        || trimmed.starts_with("CREATE DATABASE"))
                {
                    ch_exec(trimmed).ok();
                }
            }
        });
    }

    /// Shared build step: seeds data, creates the preagg schema, builds rollup + manifest.
    /// Returns the rollup table name.
    fn build() -> String {
        seed();

        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-preagg");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");
        let view = engine.view("events").expect("events view");
        let rollups = airlayer::engine::preagg::resolve_rollups(view);
        let rollup = &rollups[0];
        let table_name = format!("{}.events__{}__{}", PREAGG_SCHEMA, rollup.hash, DATE_STR);

        PREAGG_BUILD.call_once(|| {
            // Create schema
            ch_exec(&format!("CREATE DATABASE IF NOT EXISTS {}", PREAGG_SCHEMA))
                .expect("create preagg db");

            // Drop pre-existing manifest table
            ch_exec(&format!(
                "DROP TABLE IF EXISTS {}.__manifest",
                PREAGG_SCHEMA
            ))
            .expect("drop manifest");

            // Build rollup table (DROP + CTAS)
            let sqls = airlayer::engine::preagg::generate_build_sql(
                &engine,
                view,
                rollup,
                PREAGG_SCHEMA,
                DATE_STR,
            )
            .expect("generate_build_sql failed");
            for sql in &sqls {
                ch_exec(sql).expect("build SQL failed");
            }

            // Create manifest table
            let manifest_ddl = airlayer::engine::preagg::generate_manifest_create_sql(
                PREAGG_SCHEMA,
                &Dialect::ClickHouse,
            );
            ch_exec(&manifest_ddl).expect("manifest DDL failed");

            // Insert manifest entry
            let entry = airlayer::engine::preagg::build_manifest_entry(
                view,
                rollup,
                PREAGG_SCHEMA,
                DATE_STR,
            )
            .expect("build_manifest_entry failed");
            let upsert_stmts = airlayer::engine::preagg::generate_manifest_upsert_sql(
                PREAGG_SCHEMA,
                &entry,
                &Dialect::ClickHouse,
            );
            for stmt in &upsert_stmts {
                ch_exec(stmt).expect("manifest upsert failed");
            }
        });

        table_name
    }

    // -----------------------------------------------------------------------
    // Tests that don't need ClickHouse
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "tier2"]
    fn preagg_resolve_rollups() {
        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-preagg");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");

        let view = engine.view("events").expect("events view");
        let rollups = airlayer::engine::preagg::resolve_rollups(view);
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].name, "by_platform_daily");
        assert_eq!(rollups[0].dimensions, vec!["platform"]);
        assert_eq!(rollups[0].measures.len(), 3);
    }

    #[test]
    #[ignore = "tier2"]
    fn preagg_coverage_check() {
        let entry = airlayer::engine::preagg::LocalRollupEntry {
            view_name: "events".into(),
            rollup_name: "by_platform_daily".into(),
            rollup_hash: "test1234".into(),
            file: "events__test1234.parquet".into(),
            dimensions: vec!["platform".into()],
            measures: vec![
                serde_json::json!({"name": "total_events", "type": "count", "columns": ["total_events__count"]}),
                serde_json::json!({"name": "total_revenue", "type": "sum", "columns": ["total_revenue__sum"]}),
            ],
            time_dimension: Some("created_at".into()),
            granularity: Some("day".into()),
            build_date: "2026-04-15".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };

        // Covered query
        let covered = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            ..QueryRequest::new()
        };
        assert!(airlayer::engine::preagg::check_coverage(&covered, &[entry.clone()]).is_some());

        // Not covered — dimension not in rollup
        let not_covered = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.country".to_string()],
            ..QueryRequest::new()
        };
        assert!(airlayer::engine::preagg::check_coverage(&not_covered, &[entry.clone()]).is_none());

        // Covered — filter on a dimension that IS in the rollup
        let filtered = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            filters: vec![airlayer::engine::query::QueryFilter {
                member: Some("events.platform".to_string()),
                operator: Some(airlayer::engine::query::FilterOperator::Equals),
                values: vec!["web".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(airlayer::engine::preagg::check_coverage(&filtered, &[entry.clone()]).is_some());

        // Not covered — filter on a dimension NOT in the rollup
        let filtered_missing = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            filters: vec![airlayer::engine::query::QueryFilter {
                member: Some("events.country".to_string()),
                operator: Some(airlayer::engine::query::FilterOperator::Equals),
                values: vec!["US".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(airlayer::engine::preagg::check_coverage(&filtered_missing, &[entry]).is_none());
    }

    // -----------------------------------------------------------------------
    // ClickHouse tier 2: seed → build → verify
    // -----------------------------------------------------------------------

    /// Verify the CTAS creates the rollup table with the expected row count.
    /// Seed data has 12 events; GROUP BY (platform, day, user_id) yields 6 rows.
    #[test]
    #[ignore = "tier2"]
    fn preagg_build_creates_rollup_table() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        let table_name = build();

        let count = ch_exec(&format!("SELECT COUNT(*) FROM {}", table_name)).expect("count");
        let n: i64 = count.trim().parse().unwrap_or(0);
        // 6 unique (platform, day, user_id) groups in seed data
        assert_eq!(n, 6, "Expected 6 rows in rollup, got {}", n);
    }

    /// Verify actual aggregated values in the rollup table.
    /// Checks SUM and COUNT columns per (platform, day, user_id) group.
    #[test]
    #[ignore = "tier2"]
    fn preagg_rollup_data_correctness() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        let table_name = build();

        // Total event count across all rows should equal 12 (original row count)
        let total_count = ch_exec(&format!(
            "SELECT SUM(`total_events__count`) FROM {}",
            table_name
        ))
        .expect("total count");
        assert_eq!(
            total_count.trim(),
            "12",
            "SUM of total_events__count should be 12, got: {}",
            total_count.trim()
        );

        // Total revenue: SUM of all revenue_cents / 100.0
        // 4999 + 2500 + 0 + 0 + 0 + 9999 + 0 + 1500 + 0 + 0 + 0 + 0 = 18998 → 189.98
        let total_rev = ch_exec(&format!(
            "SELECT SUM(`total_revenue__sum`) FROM {}",
            table_name
        ))
        .expect("total rev");
        let rev: f64 = total_rev.trim().parse().unwrap_or(0.0);
        assert!(
            (rev - 189.98).abs() < 0.01,
            "Total revenue should be ~189.98, got: {}",
            rev
        );

        // Verify per-platform re-aggregation: web should have 3 rollup rows
        let web_rows = ch_exec(&format!(
            "SELECT COUNT(*) FROM {} WHERE `platform` = 'web'",
            table_name
        ))
        .expect("web rows");
        assert_eq!(
            web_rows.trim(),
            "3",
            "Web platform should have 3 rollup rows (3 user-day combos), got: {}",
            web_rows.trim()
        );

        // Web total events: u1(3) + u4(2) + u5(2) = 7
        let web_events = ch_exec(&format!(
            "SELECT SUM(`total_events__count`) FROM {} WHERE `platform` = 'web'",
            table_name
        ))
        .expect("web events");
        assert_eq!(
            web_events.trim(),
            "7",
            "Web total events should be 7, got: {}",
            web_events.trim()
        );
    }

    /// Verify the manifest table roundtrip: create, insert, read back with FINAL.
    #[test]
    #[ignore = "tier2"]
    fn preagg_manifest_roundtrip() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        build();

        // Read manifest with FINAL (ReplacingMergeTree dedup)
        let manifest_sql = format!(
            "SELECT view_name, rollup_name, rollup_hash, table_name, \
             time_dimension, granularity FROM {}.__manifest FINAL",
            PREAGG_SCHEMA
        );
        let result = ch_exec(&manifest_sql).expect("manifest query");
        let line = result.trim();
        assert!(!line.is_empty(), "Manifest should have at least one row");

        // Tab-separated: view_name, rollup_name, rollup_hash, table_name, time_dim, gran
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols[0], "events", "view_name");
        assert_eq!(cols[1], "by_platform_daily", "rollup_name");
        assert!(
            cols[3].contains("events__"),
            "table_name should contain view prefix"
        );
        assert!(cols[3].contains(DATE_STR), "table_name should contain date");
        assert_eq!(cols[4], "created_at", "time_dimension");
        assert_eq!(cols[5], "day", "granularity");

        // Verify dimensions JSON stored correctly
        let dims_sql = format!(
            "SELECT dimensions FROM {}.__manifest FINAL WHERE view_name = 'events'",
            PREAGG_SCHEMA
        );
        let dims_result = ch_exec(&dims_sql).expect("dims query");
        assert!(
            dims_result.contains("platform"),
            "Dimensions should contain 'platform', got: {}",
            dims_result.trim()
        );
    }

    /// Re-aggregate from the rollup table: SUM(total_events__count) and
    /// SUM(total_revenue__sum) grouped by platform, compared to raw data.
    #[test]
    #[ignore = "tier2"]
    fn preagg_reagg_sum_count_by_platform() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        let table_name = build();

        // Re-aggregate from rollup table: GROUP BY platform
        let reagg_sql = format!(
            "SELECT `platform`, \
             SUM(`total_events__count`) AS events, \
             SUM(`total_revenue__sum`) AS revenue \
             FROM {} GROUP BY `platform` ORDER BY `platform`",
            table_name
        );
        let result = ch_exec(&reagg_sql).expect("reagg query");

        // Also query the raw table for comparison
        let raw_sql = "SELECT platform, \
             COUNT(*) AS events, \
             SUM(revenue_cents / 100.0) AS revenue \
             FROM analytics.events GROUP BY platform ORDER BY platform";
        let raw_result = ch_exec(raw_sql).expect("raw query");

        // Parse both results (tab-separated lines)
        let reagg_lines: Vec<&str> = result.trim().lines().collect();
        let raw_lines: Vec<&str> = raw_result.trim().lines().collect();

        assert_eq!(
            reagg_lines.len(),
            raw_lines.len(),
            "Row count mismatch: reagg={}, raw={}",
            reagg_lines.len(),
            raw_lines.len()
        );

        // Compare each row
        for (reagg_line, raw_line) in reagg_lines.iter().zip(raw_lines.iter()) {
            let reagg_cols: Vec<&str> = reagg_line.split('\t').collect();
            let raw_cols: Vec<&str> = raw_line.split('\t').collect();

            assert_eq!(
                reagg_cols[0], raw_cols[0],
                "Platform mismatch: reagg={}, raw={}",
                reagg_cols[0], raw_cols[0]
            );
            assert_eq!(
                reagg_cols[1], raw_cols[1],
                "Event count mismatch for {}: reagg={}, raw={}",
                reagg_cols[0], reagg_cols[1], raw_cols[1]
            );

            let reagg_rev: f64 = reagg_cols[2].parse().unwrap_or(-1.0);
            let raw_rev: f64 = raw_cols[2].parse().unwrap_or(-2.0);
            assert!(
                (reagg_rev - raw_rev).abs() < 0.01,
                "Revenue mismatch for {}: reagg={}, raw={}",
                reagg_cols[0],
                reagg_rev,
                raw_rev
            );
        }
    }

    /// Re-aggregate COUNT(DISTINCT user_id) from the rollup table.
    /// The rollup stores raw user_id in GROUP BY, so COUNT(DISTINCT) should be exact.
    #[test]
    #[ignore = "tier2"]
    fn preagg_reagg_count_distinct() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        let table_name = build();

        // Re-aggregate count_distinct from rollup
        let reagg_sql = format!(
            "SELECT `platform`, COUNT(DISTINCT `user_id`) AS unique_users \
             FROM {} GROUP BY `platform` ORDER BY `platform`",
            table_name
        );
        let reagg_result = ch_exec(&reagg_sql).expect("reagg cd query");

        // Raw comparison
        let raw_sql = "SELECT platform, COUNT(DISTINCT user_id) AS unique_users \
             FROM analytics.events GROUP BY platform ORDER BY platform";
        let raw_result = ch_exec(raw_sql).expect("raw cd query");

        let reagg_lines: Vec<&str> = reagg_result.trim().lines().collect();
        let raw_lines: Vec<&str> = raw_result.trim().lines().collect();

        assert_eq!(reagg_lines.len(), raw_lines.len());

        for (reagg_line, raw_line) in reagg_lines.iter().zip(raw_lines.iter()) {
            let reagg_cols: Vec<&str> = reagg_line.split('\t').collect();
            let raw_cols: Vec<&str> = raw_line.split('\t').collect();
            assert_eq!(reagg_cols[0], raw_cols[0], "Platform mismatch");
            assert_eq!(
                reagg_cols[1], raw_cols[1],
                "Count distinct mismatch for {}: reagg={}, raw={}",
                reagg_cols[0], reagg_cols[1], raw_cols[1]
            );
        }
    }

    /// Re-aggregate with time dimension: GROUP BY (platform, day).
    /// Verifies the time truncation column is usable for re-aggregation.
    #[test]
    #[ignore = "tier2"]
    fn preagg_reagg_with_time_dimension() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        let table_name = build();

        // Re-aggregate from rollup: GROUP BY (platform, day)
        let reagg_sql = format!(
            "SELECT `platform`, `created_at__day`, \
             SUM(`total_events__count`) AS events \
             FROM {} GROUP BY `platform`, `created_at__day` \
             ORDER BY `platform`, `created_at__day`",
            table_name
        );
        let reagg_result = ch_exec(&reagg_sql).expect("reagg time query");

        // Raw comparison: GROUP BY (platform, toStartOfDay(created_at))
        let raw_sql = "SELECT platform, toStartOfDay(created_at) AS d, \
             COUNT(*) AS events \
             FROM analytics.events GROUP BY platform, d \
             ORDER BY platform, d";
        let raw_result = ch_exec(raw_sql).expect("raw time query");

        let reagg_lines: Vec<&str> = reagg_result.trim().lines().collect();
        let raw_lines: Vec<&str> = raw_result.trim().lines().collect();

        assert_eq!(
            reagg_lines.len(),
            raw_lines.len(),
            "Row count mismatch: reagg has {} rows, raw has {} rows\nReagg:\n{}\nRaw:\n{}",
            reagg_lines.len(),
            raw_lines.len(),
            reagg_result.trim(),
            raw_result.trim()
        );

        // Verify event counts match row by row
        for (reagg_line, raw_line) in reagg_lines.iter().zip(raw_lines.iter()) {
            let reagg_cols: Vec<&str> = reagg_line.split('\t').collect();
            let raw_cols: Vec<&str> = raw_line.split('\t').collect();
            assert_eq!(reagg_cols[0], raw_cols[0], "Platform mismatch");
            assert_eq!(
                reagg_cols[2], raw_cols[2],
                "Event count mismatch for {} on {}: reagg={}, raw={}",
                reagg_cols[0], reagg_cols[1], reagg_cols[2], raw_cols[2]
            );
        }
    }

    /// Verify that a second build (different date) produces correct results.
    /// Uses a separate date string to avoid racing with other tests.
    #[test]
    #[ignore = "tier2"]
    fn preagg_rebuild_idempotent() {
        if !is_available() {
            eprintln!("ClickHouse not available, skipping");
            return;
        }
        // Ensure shared build has run (creates the database)
        build();

        let rebuild_date = "20260416";
        let views_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-preagg");
        let dialects = DatasourceDialectMap::with_default(Dialect::ClickHouse);
        let engine = SemanticEngine::load(&views_dir, None, dialects).expect("load");
        let view = engine.view("events").expect("events view");
        let rollups = airlayer::engine::preagg::resolve_rollups(view);

        let rebuild_table = format!(
            "{}.events__{}__{}",
            PREAGG_SCHEMA, rollups[0].hash, rebuild_date
        );

        let sqls = airlayer::engine::preagg::generate_build_sql(
            &engine,
            view,
            &rollups[0],
            PREAGG_SCHEMA,
            rebuild_date,
        )
        .expect("generate_build_sql failed");

        // Build twice to prove idempotency (generate_build_sql includes DROP IF EXISTS)
        for sql in &sqls {
            ch_exec(sql).expect("first build");
        }
        for sql in &sqls {
            ch_exec(sql).expect("rebuild");
        }

        let count = ch_exec(&format!("SELECT COUNT(*) FROM {}", rebuild_table))
            .expect("count after rebuild");
        let n: i64 = count.trim().parse().unwrap_or(0);
        assert_eq!(n, 6, "Rebuilt table should have 6 rows, got {}", n);

        // Cleanup
        ch_exec(&format!("DROP TABLE IF EXISTS {}", rebuild_table)).ok();
    }
}

// ---------------------------------------------------------------------------
// Tier 1: Shift measures + lifespan cohorts (DuckDB, in-process)
//
// Proves the same-store-sales primitives end to end against the checked-in
// `examples/same-store-sales` model.
// ---------------------------------------------------------------------------
#[cfg(feature = "exec-duckdb")]
mod shift_tests {
    use super::*;

    fn load_shift_engine() -> SemanticEngine {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/same-store-sales");
        let dialects = DatasourceDialectMap::with_default(Dialect::DuckDB);
        SemanticEngine::load(&dir, None, dialects).expect("load same-store-sales views")
    }

    /// Canonical 5-store seed from the acceptance criteria. Daily rows (two per
    /// store-year) summing to the annual totals.
    fn seed_canonical() -> duckdb::Connection {
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (
                store_id VARCHAR PRIMARY KEY,
                region VARCHAR,
                opened_at DATE,
                closed_at DATE
            );
            INSERT INTO stores VALUES
              ('A','East','2021-01-01',NULL),
              ('B','East','2023-01-01',NULL),
              ('C','West','2025-07-01',NULL),
              ('D','West','2026-02-01',NULL),
              ('E','South','2019-01-01','2026-09-15');

            CREATE TABLE sales_daily (
                store_id VARCHAR,
                sale_date DATE,
                net_sales INTEGER,
                transaction_count INTEGER
            );
            INSERT INTO sales_daily VALUES
              -- A: 2025=1000 (txn 100), 2026=980 (txn 98)
              ('A','2025-01-15',500,50),('A','2025-07-15',500,50),
              ('A','2026-01-15',490,49),('A','2026-07-15',490,49),
              -- B: 2025=1200 (120), 2026=1150 (115)
              ('B','2025-01-15',600,60),('B','2025-07-15',600,60),
              ('B','2026-01-15',575,57),('B','2026-07-15',575,58),
              -- C: opened mid-2025. 2025=400 (40), 2026=850 (85)
              ('C','2025-08-15',200,20),('C','2025-10-15',200,20),
              ('C','2026-01-15',425,42),('C','2026-07-15',425,43),
              -- D: opened 2026. 2026=500 (50)
              ('D','2026-03-15',250,25),('D','2026-07-15',250,25),
              -- E: closed 2026-09-15. 2025=900 (90), 2026=700 (70)
              ('E','2025-01-15',450,45),('E','2025-07-15',450,45),
              ('E','2026-01-15',350,35),('E','2026-08-15',350,35);",
        )
        .expect("seed canonical");
        db
    }

    fn rewrite_params(sql: &str) -> String {
        regex::Regex::new(r"\$(\d+)")
            .unwrap()
            .replace_all(sql, "?")
            .to_string()
    }

    /// Run a compiled query against a given connection, returning rows as the
    /// duckdb debug-string form (matching the other tier-1 helpers).
    fn run(db: &duckdb::Connection, sql: &str, params: &[String]) -> Vec<Vec<String>> {
        let rewritten = rewrite_params(sql);
        let mut stmt = db
            .prepare(&rewritten)
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", rewritten, e));
        let param_refs: Vec<&dyn duckdb::ToSql> =
            params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        let mut out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
            }
            out.push(vals);
        }
        out
    }

    /// Extract the numeric value from a duckdb debug string like `Int(2130)`,
    /// `Double(-0.0318)`, `Decimal(...)`. Returns None for `Null`.
    fn num(cell: &str) -> Option<f64> {
        if cell == "Null" {
            return None;
        }
        let inner = cell
            .split_once('(')
            .map(|(_, rest)| rest.trim_end_matches(')'))
            .unwrap_or(cell);
        inner.trim().parse::<f64>().ok()
    }

    fn fy_query() -> QueryRequest {
        QueryRequest {
            measures: vec![
                "sales.same_store_sales".to_string(),
                "sales.net_sales".to_string(),
                "sales.net_sales_prior".to_string(),
            ],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        }
    }

    #[test]
    fn shift_same_store_sales_acceptance() {
        let engine = load_shift_engine();
        let compiled = engine.compile_query(&fy_query()).expect("compile");
        let db = seed_canonical();
        let rows = run(&db, &compiled.sql, &compiled.params);

        assert_eq!(rows.len(), 1, "expected one (year) row, got {:?}", rows);
        let row = &rows[0];
        // columns: year, same_store_sales, net_sales, net_sales_prior
        let ratio = num(&row[1]).expect("ratio");
        let current = num(&row[2]).expect("current net_sales");
        let prior = num(&row[3]).expect("prior net_sales");

        // Property 1 — new-store leak prevented: C (opened mid-prior-year) and D
        // (opened in current year) are excluded from the numerator. If leaked,
        // current would be 2130 + 850 + 500.
        assert_eq!(
            current, 2130.0,
            "current cohort net_sales must be A+B only (2130)"
        );

        // Property 2 — mid-period closure handled (two-sided): E (opened early but
        // closed 2026-09-15, before current end) is excluded by the lifespan.end
        // half of the predicate. If leaked, prior would include 900 (→ 3100).
        assert_eq!(
            prior, 2200.0,
            "prior cohort net_sales must be A+B only (2200)"
        );

        // same_store_sales ≈ -0.0318
        assert!(
            (ratio - (-0.031818)).abs() < 1e-4,
            "same_store_sales ≈ -3.18%, got {}",
            ratio
        );
    }

    #[test]
    fn shift_decomposition_generalizes_to_traffic() {
        // Swapping the base measure (transactions) yields the same cohort.
        let engine = load_shift_engine();
        let request = QueryRequest {
            measures: vec![
                "sales.transactions".to_string(),
                "sales.comp_traffic".to_string(),
            ],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        let compiled = engine.compile_query(&request).expect("compile");
        let db = seed_canonical();
        let rows = run(&db, &compiled.sql, &compiled.params);

        assert_eq!(rows.len(), 1, "expected one row, got {:?}", rows);
        let row = &rows[0];
        // columns: year, transactions, comp_traffic
        let current = num(&row[1]).expect("current txn");
        let prior = num(&row[2]).expect("prior txn");
        assert_eq!(
            current, 213.0,
            "current cohort transactions = A+B 2026 (98+115)"
        );
        assert_eq!(
            prior, 220.0,
            "prior cohort transactions = A+B 2025 (100+120)"
        );
    }

    #[test]
    fn shift_maturity_offset_excludes_immature_store() {
        // Dedicated seed: A (always present) + F (opened 2024-12-01, just inside
        // the prior window). maturity 0 includes F; maturity 14 months excludes it.
        let engine = load_shift_engine();
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (store_id VARCHAR, region VARCHAR, opened_at DATE, closed_at DATE);
             INSERT INTO stores VALUES
               ('A','East','2021-01-01',NULL),
               ('F','East','2024-12-01',NULL);
             CREATE TABLE sales_daily (store_id VARCHAR, sale_date DATE, net_sales INTEGER, transaction_count INTEGER);
             INSERT INTO sales_daily VALUES
               ('A','2025-06-15',1000,100),('A','2026-06-15',980,98),
               ('F','2025-06-15',300,30),('F','2026-06-15',320,32);",
        )
        .expect("seed maturity");

        let mut q = QueryRequest {
            measures: vec!["sales.net_sales_prior".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };

        // maturity 0 (net_sales_prior): cohort {A, F} → prior = 1000 + 300 = 1300.
        let c0 = engine.compile_query(&q).expect("compile maturity 0");
        let r0 = run(&db, &c0.sql, &c0.params);
        let prior0 = num(&r0[0][1]).expect("prior maturity 0");
        assert_eq!(prior0, 1300.0, "maturity 0 includes F (1000+300)");

        // maturity 14 months (net_sales_prior_mature): cohort {A} → prior = 1000.
        q.measures = vec!["sales.net_sales_prior_mature".to_string()];
        let c14 = engine.compile_query(&q).expect("compile maturity 14");
        let r14 = run(&db, &c14.sql, &c14.params);
        let prior14 = num(&r14[0][1]).expect("prior maturity 14");
        assert_eq!(prior14, 1000.0, "maturity 14 months excludes immature F");
    }

    #[test]
    fn shift_next_direction_aligns_forward_and_two_sided_cohort() {
        // `direction: next` compares the current window to the *next* window, and
        // the cohort must be live across both (two-sided lifespan check).
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      start: opened_at
      end: closed_at
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: opened_at
    type: date
    expr: opened_at
  - name: closed_at
    type: date
    expr: closed_at
"#,
                "stores",
            )
            .unwrap();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: sale_date
    type: date
    expr: sale_date
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_next
    shift:
      measure: net_sales
      by: 1 year
      direction: next
      comparable_by: store_id
"#,
                "sales",
            )
            .unwrap();
        let layer = airlayer::schema::models::SemanticLayer::new(vec![stores, sales], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("build engine");

        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (store_id VARCHAR, opened_at DATE, closed_at DATE);
             INSERT INTO stores VALUES
               ('A','2021-01-01',NULL),            -- live across 2024 + 2025
               ('X','2021-01-01','2025-06-30');    -- closes before end of next window
             CREATE TABLE sales_daily (store_id VARCHAR, sale_date DATE, net_sales INTEGER, transaction_count INTEGER);
             INSERT INTO sales_daily VALUES
               ('A','2024-06-15',1000,0),('A','2025-06-15',900,0),
               ('X','2024-06-15',500,0),('X','2025-06-15',300,0);",
        )
        .expect("seed next");

        let request = QueryRequest {
            measures: vec![
                "sales.net_sales".to_string(),
                "sales.net_sales_next".to_string(),
            ],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        let compiled = engine.compile_query(&request).expect("compile next");
        let rows = run(&db, &compiled.sql, &compiled.params);

        assert_eq!(rows.len(), 1, "expected one (2024) row, got {:?}", rows);
        // columns: year, net_sales, net_sales_next. Cohort = {A} (X closed mid-next).
        assert_eq!(
            num(&rows[0][1]),
            Some(1000.0),
            "current cohort = A only (1000)"
        );
        assert_eq!(
            num(&rows[0][2]),
            Some(900.0),
            "next-window cohort = A only (900)"
        );
    }

    #[test]
    fn shift_comparable_by_selects_the_cohort_entity() {
        // A fact that reaches TWO lifespan-bearing entities (stores AND regions).
        // `comparable_by` chooses which entity's lifespan defines the cohort, so
        // the same query produces different comps depending on the grain. (This
        // case previously had no way to disambiguate.)
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      start: opened_at
      end: closed_at
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: opened_at
    type: date
    expr: opened_at
  - name: closed_at
    type: date
    expr: closed_at
"#,
                "stores",
            )
            .unwrap();
        let regions = parser
            .parse_view_str(
                r#"
name: regions
table: regions
entities:
  - name: region_id
    type: primary
    key: region_id
    lifespan:
      start: launched_at
      end: sunset_at
dimensions:
  - name: region_id
    type: string
    expr: region_id
  - name: launched_at
    type: date
    expr: launched_at
  - name: sunset_at
    type: date
    expr: sunset_at
"#,
                "regions",
            )
            .unwrap();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
  - name: region_id
    type: foreign
    key: region_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: region_id
    type: string
    expr: region_id
  - name: sale_date
    type: date
    expr: sale_date
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: prior_by_store
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: store_id
  - name: prior_by_region
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: region_id
"#,
                "sales",
            )
            .unwrap();
        let layer =
            airlayer::schema::models::SemanticLayer::new(vec![stores, regions, sales], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("build engine");

        // S1 (old store) sells in R2 (new region); S2 (new store) sells in R1 (old region).
        //   store cohort  = {S1}  → S1's numbers (current 900, prior 1000)
        //   region cohort = {R1}  → S2's numbers (current 480, prior 500)
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (store_id VARCHAR, opened_at DATE, closed_at DATE);
             INSERT INTO stores VALUES
               ('S1','2020-01-01',NULL),       -- old store
               ('S2','2025-06-01',NULL);       -- new store
             CREATE TABLE regions (region_id VARCHAR, launched_at DATE, sunset_at DATE);
             INSERT INTO regions VALUES
               ('R1','2020-01-01',NULL),       -- old region
               ('R2','2025-06-01',NULL);       -- new region
             CREATE TABLE sales_daily (store_id VARCHAR, region_id VARCHAR, sale_date DATE, net_sales INTEGER);
             INSERT INTO sales_daily VALUES
               ('S1','R2','2025-06-15',1000),('S1','R2','2026-06-15',900),
               ('S2','R1','2025-06-15',500), ('S2','R1','2026-06-15',480);",
        )
        .expect("seed disambig");

        let q = |measure: &str| QueryRequest {
            measures: vec!["sales.net_sales".to_string(), format!("sales.{measure}")],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };

        // comparable_by: store_id → cohort {S1}.
        let cs = engine
            .compile_query(&q("prior_by_store"))
            .expect("compile store");
        let rs = run(&db, &cs.sql, &cs.params);
        assert_eq!(
            num(&rs[0][1]),
            Some(900.0),
            "store cohort current = S1 (900)"
        );
        assert_eq!(
            num(&rs[0][2]),
            Some(1000.0),
            "store cohort prior = S1 (1000)"
        );

        // comparable_by: region_id → cohort {R1}, a different set of rows.
        let cr = engine
            .compile_query(&q("prior_by_region"))
            .expect("compile region");
        let rr = run(&db, &cr.sql, &cr.params);
        assert_eq!(
            num(&rr[0][1]),
            Some(480.0),
            "region cohort current = S2 in R1 (480)"
        );
        assert_eq!(
            num(&rr[0][2]),
            Some(500.0),
            "region cohort prior = S2 in R1 (500)"
        );
    }

    #[test]
    fn shift_self_join_tolerates_period_gaps() {
        // A gappy monthly series: Jan, Feb, (no Mar), Apr. A LAG over ordered rows
        // would pair Apr with Feb; the self-join pairs Apr with the absent Mar (so
        // prior is NULL), and Feb with Jan (present).
        let engine = load_shift_engine();
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE sales_daily (store_id VARCHAR, sale_date DATE, net_sales INTEGER, transaction_count INTEGER);
             INSERT INTO sales_daily VALUES
               ('G','2026-01-10',100,10),
               ('G','2026-02-10',200,20),
               ('G','2026-04-10',400,40);",
        )
        .expect("seed gap");

        let request = QueryRequest {
            measures: vec![
                "sales.net_sales".to_string(),
                "sales.net_sales_prev_month".to_string(),
            ],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("month".to_string()),
                date_range: Some(vec!["2026-02-01".to_string(), "2026-04-30".to_string()]),
            }],
            order: vec![OrderBy {
                id: "sales.sale_date.month".to_string(),
                desc: false,
            }],
            ..QueryRequest::new()
        };
        let compiled = engine.compile_query(&request).expect("compile");
        let rows = run(&db, &compiled.sql, &compiled.params);

        // Expect Feb and Apr (Jan is outside the current window).
        assert_eq!(rows.len(), 2, "expected Feb + Apr rows, got {:?}", rows);
        // columns: month, net_sales, net_sales_prev_month
        // Feb: prev month = Jan = 100 (present).
        assert_eq!(
            num(&rows[0][2]),
            Some(100.0),
            "Feb's previous month is Jan (100)"
        );
        // Apr: previous month is March, which is absent → NULL (gap tolerance).
        assert_eq!(
            num(&rows[1][2]),
            None,
            "Apr's previous month (absent March) is NULL"
        );
    }

    /// Derived lifespan: the `stores` table carries no open/close columns —
    /// lifespan is inferred from MIN/MAX of `sale_date` in the fact view. The
    /// engine synthesizes a `__lifespan_store_id` CTE and joins it for the
    /// cohort predicate.
    ///
    /// Cohort math: current window 2026, prior shift 1 year → prior window
    /// 2025. Cutoff: derived_start <= 2025-01-01 AND derived_end >= 2026-12-31.
    ///   A,B   — first sale 2024-12 (≤ cutoff), last sale 2026-12-31 (≥ floor) → IN
    ///   C     — first sale 2026-01 (>= cutoff) → OUT
    ///   D     — first sale 2026-03                                              → OUT
    ///   E     — first sale 2024-12, last sale 2026-03 (< floor — went dark)     → OUT
    #[test]
    fn shift_derived_lifespan_via_aggregation() {
        use airlayer::schema::parser::SchemaParser;
        let parser = SchemaParser::new();
        // Stores table has no opened_at/closed_at; the lifespan is derived from
        // sales activity. This is the "no ETL change" case for legacy POS data.
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      from: sales
      start: MIN(sale_date)
      end: MAX(sale_date)
dimensions:
  - name: store_id
    type: string
    expr: store_id
"#,
                "stores",
            )
            .unwrap();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: sale_date
    type: date
    expr: sale_date
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_prior
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: store_id
  - name: same_store_sales
    type: number
    expr: "{{sales.net_sales}} / NULLIF({{sales.net_sales_prior}}, 0) - 1"
"#,
                "sales",
            )
            .unwrap();
        let layer = airlayer::schema::models::SemanticLayer::new(vec![stores, sales], None);
        let engine = SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("build engine");

        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE stores (store_id VARCHAR PRIMARY KEY);
             INSERT INTO stores VALUES ('A'),('B'),('C'),('D'),('E');
             CREATE TABLE sales_daily (store_id VARCHAR, sale_date DATE, net_sales INTEGER);
             INSERT INTO sales_daily VALUES
               -- A: in cohort. 2025=1000, 2026=1100.
               ('A','2024-12-15',200),
               ('A','2025-06-15',1000),
               ('A','2026-12-31',1100),
               -- B: in cohort. 2025=2000, 2026=1500.
               ('B','2024-12-20',300),
               ('B','2025-06-15',2000),
               ('B','2026-12-31',1500),
               -- C: opened 2026; first sale after cutoff → excluded.
               ('C','2026-06-15',500),
               -- D: opened 2026; excluded.
               ('D','2026-03-15',300),
               -- E: went dark in early 2026; last sale before end floor → excluded.
               ('E','2024-12-01',400),
               ('E','2025-06-15',700),
               ('E','2026-03-15',350);",
        )
        .expect("seed derived");

        let request = QueryRequest {
            measures: vec![
                "sales.same_store_sales".to_string(),
                "sales.net_sales".to_string(),
                "sales.net_sales_prior".to_string(),
            ],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        let compiled = engine.compile_query(&request).expect("compile derived");

        // The derived-lifespan CTE should appear in the compiled SQL.
        assert!(
            compiled.sql.contains("__lifespan_store_id"),
            "expected __lifespan_store_id CTE in compiled SQL:\n{}",
            compiled.sql
        );

        let rows = run(&db, &compiled.sql, &compiled.params);
        assert_eq!(rows.len(), 1, "expected one (year) row, got {:?}", rows);
        let row = &rows[0];
        // columns: year, same_store_sales, net_sales, net_sales_prior
        let current = num(&row[2]).expect("current net_sales");
        let prior = num(&row[3]).expect("prior net_sales");
        let ratio = num(&row[1]).expect("ratio");

        // Cohort = {A, B} only — C/D have no 2025 baseline, E went dark.
        assert_eq!(current, 2600.0, "current cohort = A+B 2026 (1100+1500)");
        assert_eq!(prior, 3000.0, "prior cohort = A+B 2025 (1000+2000)");
        // ratio = 2600/3000 - 1 ≈ -0.13333.
        assert!(
            (ratio - (-0.13333)).abs() < 1e-3,
            "ratio ≈ -13.3%, got {}",
            ratio
        );
    }
}

// ---------------------------------------------------------------------------
// Shift cross-dialect COMPILE tests (no database required).
//
// The shift self-join key (`prior.bucket + interval`) and date literals are
// dialect-sensitive. These tests lock the documented-correct SQL each dialect
// emits, so a regression toward non-portable syntax fails fast — even for the
// warehouses we cannot execute against in this environment (validated by the
// tier-2/tier-3 CI jobs).
// ---------------------------------------------------------------------------
mod shift_dialect_compile_tests {
    use super::*;
    use airlayer::schema::parser::SchemaParser;

    /// Build a same-store-sales engine for `dialect` from inline YAML. Includes a
    /// year-shift (month base unit) and a week-shift (day base unit) measure to
    /// exercise both date-arithmetic paths.
    fn engine_for(dialect: Dialect) -> SemanticEngine {
        let parser = SchemaParser::new();
        let stores = parser
            .parse_view_str(
                r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    lifespan:
      start: opened_at
      end: closed_at
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: opened_at
    type: date
    expr: opened_at
  - name: closed_at
    type: date
    expr: closed_at
"#,
                "stores",
            )
            .unwrap();
        let sales = parser
            .parse_view_str(
                r#"
name: sales
table: sales_daily
entities:
  - name: store_id
    type: foreign
    key: store_id
dimensions:
  - name: store_id
    type: string
    expr: store_id
  - name: sale_date
    type: date
    expr: sale_date
measures:
  - name: net_sales
    type: sum
    expr: net_sales
  - name: net_sales_prior
    shift:
      measure: net_sales
      by: 1 year
      direction: prior
      comparable_by: store_id
  - name: net_sales_prior_week
    shift:
      measure: net_sales
      by: 1 week
      direction: prior
      comparable_by: store_id
"#,
                "sales",
            )
            .unwrap();
        let layer = airlayer::schema::models::SemanticLayer::new(vec![stores, sales], None);
        SemanticEngine::from_semantic_layer(layer, DatasourceDialectMap::with_default(dialect))
            .expect("engine")
    }

    fn compile(dialect: Dialect, measure: &str, granularity: &str) -> String {
        let request = QueryRequest {
            measures: vec![format!("sales.{measure}"), "sales.net_sales".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some(granularity.to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        engine_for(dialect)
            .compile_query(&request)
            .expect("compile shift")
            .sql
    }

    #[test]
    fn year_shift_self_join_key_is_dialect_correct() {
        // (dialect, expected self-join date-add fragment for a 1-year (12-month) shift)
        let cases = [
            (Dialect::Postgres, "+ INTERVAL '12 month') AS DATE)"),
            (Dialect::Redshift, "+ INTERVAL '12 month') AS DATE)"),
            (Dialect::DuckDB, "+ INTERVAL '12 month') AS DATE)"),
            (
                Dialect::MySQL,
                "DATE_ADD(prior.`sales__sale_date__year`, INTERVAL 12 MONTH)",
            ),
            (
                Dialect::BigQuery,
                "DATE_ADD(prior.`sales__sale_date__year`, INTERVAL 12 MONTH)",
            ),
            (
                Dialect::Snowflake,
                "DATEADD(month, 12, prior.\"SALES__SALE_DATE__YEAR\")",
            ),
            (
                Dialect::ClickHouse,
                "addMonths(prior.\"sales__sale_date__year\", 12)",
            ),
            (
                Dialect::Databricks,
                "add_months(prior.`sales__sale_date__year`, 12)",
            ),
            (
                Dialect::Presto,
                "date_add('month', 12, prior.\"sales__sale_date__year\")",
            ),
        ];
        for (dialect, fragment) in cases {
            let sql = compile(dialect.clone(), "net_sales_prior", "year");
            assert!(
                sql.contains(fragment),
                "{:?}: expected self-join fragment `{}` in:\n{}",
                dialect,
                fragment,
                sql
            );
        }
    }

    #[test]
    fn week_shift_uses_day_base_unit() {
        // A 1-week shift normalizes to 7 days, exercising the day arithmetic path.
        let cases = [
            (Dialect::Postgres, "+ INTERVAL '7 day') AS DATE)"),
            (Dialect::MySQL, "INTERVAL 7 DAY)"),
            (Dialect::BigQuery, "INTERVAL 7 DAY)"),
            (Dialect::Snowflake, "DATEADD(day, 7,"),
            (
                Dialect::ClickHouse,
                "addDays(prior.\"sales__sale_date__week\", 7)",
            ),
            (
                Dialect::Databricks,
                "date_add(prior.`sales__sale_date__week`, 7)",
            ),
            (Dialect::Presto, "date_add('day', 7,"),
        ];
        for (dialect, fragment) in cases {
            let sql = compile(dialect.clone(), "net_sales_prior_week", "week");
            assert!(
                sql.contains(fragment),
                "{:?}: expected day fragment `{}` in:\n{}",
                dialect,
                fragment,
                sql
            );
        }
    }

    #[test]
    fn no_dialect_emits_non_portable_interval_addition() {
        // The bare `+ interval '...'` / `+ INTERVAL n unit` form is only valid on
        // Postgres-family engines (where we wrap it in CAST(... AS DATE)). Assert
        // the others never emit a raw `bucket + interval`.
        for dialect in [
            Dialect::MySQL,
            Dialect::BigQuery,
            Dialect::Snowflake,
            Dialect::ClickHouse,
            Dialect::Databricks,
            Dialect::Presto,
        ] {
            let sql = compile(dialect.clone(), "net_sales_prior", "year");
            assert!(
                !sql.contains("__year` + ") && !sql.contains("__year\" + "),
                "{:?} emitted raw `bucket + ...` interval addition:\n{}",
                dialect,
                sql
            );
        }
    }

    #[test]
    fn date_literals_are_cast_not_bare_strings() {
        // BigQuery/Presto reject `date_col >= '2025-01-01'` (no implicit string
        // coercion). Every window/cohort bound must be a cast DATE literal.
        for dialect in [Dialect::BigQuery, Dialect::Presto, Dialect::Postgres] {
            let sql = compile(dialect.clone(), "net_sales_prior", "year");
            assert!(
                sql.contains("CAST('2025-01-01' AS DATE)"),
                "{:?}: expected cast date literal in:\n{}",
                dialect,
                sql
            );
        }
        // ClickHouse uses toDate('...').
        let ch = compile(Dialect::ClickHouse, "net_sales_prior", "year");
        assert!(
            ch.contains("toDate('2025-01-01')"),
            "clickhouse literal:\n{}",
            ch
        );
    }

    #[test]
    fn sqlite_shift_is_rejected_with_clear_error() {
        let request = QueryRequest {
            measures: vec!["sales.net_sales_prior".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "sales.sale_date".to_string(),
                granularity: Some("year".to_string()),
                date_range: Some(vec!["2026-01-01".to_string(), "2026-12-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        let err = engine_for(Dialect::SQLite)
            .compile_query(&request)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("SQLite") && err.contains("date_trunc"),
            "expected a clear SQLite-unsupported error, got: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #55: cross-entity references in view-definition exprs must trigger
// JOINs. These tests EXECUTE the issue's exact repro against DuckDB — SQL
// that references an alias missing from the FROM clause fails at prepare
// time, so plain execution is the regression guard (string assertions on
// the SQL can't see inside each CTE).
// ---------------------------------------------------------------------------
#[cfg(feature = "exec-duckdb")]
mod issue_55_expr_ref_join_tests {
    use super::*;
    use airlayer::schema::parser::SchemaParser;
    use airlayer::SemanticLayer;

    const ORDERS_VIEW: &str = r#"
name: orders
table: orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: flag_from_other_view
    type: boolean
    expr: "{{order_flags.is_flagged}}"
measures:
  - name: total_orders
    type: count_distinct
    expr: ORDER_ID
  - name: flagged_order_sum
    type: number
    expr: "SUM(CASE WHEN {{order_flags.is_flagged}} THEN 1 ELSE 0 END)"
  - name: total_flagged_orders
    type: count_distinct
    expr: ORDER_ID
    filters:
      - expr: "{{order_flags.is_flagged}}"
"#;

    const ORDER_FLAGS_VIEW: &str = r#"
name: order_flags
table: order_flags
entities:
  - name: order
    type: foreign
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: is_flagged
    type: boolean
    expr: IS_FLAGGED
"#;

    fn engine() -> SemanticEngine {
        let parser = SchemaParser::new();
        let views = vec![
            parser.parse_view_str(ORDERS_VIEW, "<orders>").unwrap(),
            parser
                .parse_view_str(ORDER_FLAGS_VIEW, "<order_flags>")
                .unwrap(),
        ];
        let layer = SemanticLayer::new(views, None);
        SemanticEngine::from_semantic_layer(
            layer,
            DatasourceDialectMap::with_default(Dialect::DuckDB),
        )
        .expect("build engine")
    }

    fn seed() -> duckdb::Connection {
        let db = duckdb::Connection::open_in_memory().expect("duckdb open");
        db.execute_batch(
            "CREATE TABLE orders (ORDER_ID VARCHAR);
             INSERT INTO orders VALUES ('o1'), ('o2'), ('o3');
             CREATE TABLE order_flags (ORDER_ID VARCHAR, IS_FLAGGED BOOLEAN);
             INSERT INTO order_flags VALUES ('o1', true), ('o2', false);",
        )
        .expect("seed");
        db
    }

    fn run(request: QueryRequest) -> Vec<Vec<String>> {
        let result = engine().compile_query(&request).expect("compile");
        let db = seed();
        let rewritten = regex::Regex::new(r"\$(\d+)")
            .unwrap()
            .replace_all(&result.sql, "?")
            .to_string();
        let mut stmt = db
            .prepare(&rewritten)
            .unwrap_or_else(|e| panic!("prepare failed for:\n{}\n{}", rewritten, e));
        let param_refs: Vec<&dyn duckdb::ToSql> = result
            .params
            .iter()
            .map(|p| p as &dyn duckdb::ToSql)
            .collect();
        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice()).expect("query");
        while let Some(row) = rows.next().expect("next") {
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get::<_, duckdb::types::Value>(i) {
                vals.push(format!("{:?}", v));
                i += 1;
            }
            rows_out.push(vals);
        }
        rows_out
    }

    #[test]
    fn test_cross_view_ref_in_dimension_expr_executes() {
        let rows = run(QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["orders.flag_from_other_view".to_string()],
            ..QueryRequest::new()
        });
        // Flags: o1=true, o2=false, o3=NULL (left join miss) — three groups
        assert_eq!(rows.len(), 3, "expected one row per flag value: {:?}", rows);
    }

    #[test]
    fn test_cross_view_ref_in_measure_expr_executes() {
        let rows = run(QueryRequest {
            measures: vec!["orders.flagged_order_sum".to_string()],
            ..QueryRequest::new()
        });
        // Only o1 is flagged
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0][0].contains('1'),
            "flagged_order_sum should be 1: {:?}",
            rows
        );
    }

    #[test]
    fn test_cross_view_ref_in_measure_filter_executes() {
        let rows = run(QueryRequest {
            measures: vec!["orders.total_flagged_orders".to_string()],
            ..QueryRequest::new()
        });
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0][0].contains('1'),
            "total_flagged_orders should be 1: {:?}",
            rows
        );
    }

    #[test]
    fn test_query_level_equivalent_still_executes() {
        // The issue's "what works" case — must keep working identically.
        let rows = run(QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["order_flags.is_flagged".to_string()],
            ..QueryRequest::new()
        });
        assert_eq!(rows.len(), 3, "expected one row per flag value: {:?}", rows);
    }
}
