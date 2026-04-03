//! E-commerce parity tests: validate feature parity with Cube.js, Looker, and Omni.
//!
//! These tests use a multi-table e-commerce data model inspired by:
//! - Looker's thelook (order_items, users, products, inventory_items)
//! - Cube.js e-commerce examples (orders with segments, filtered measures)
//! - Omni documentation (filtered measures, compound measures, joins)
//!
//! All tests compile AND execute against DuckDB in-process.
//!
//! Run: cargo test --test ecommerce_parity_tests -- --ignored

use airlayer::dialect::Dialect;
use airlayer::engine::query::*;
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
use std::path::Path;

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

fn load_ecommerce_engine() -> SemanticEngine {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-ecommerce");
    let dialects = DatasourceDialectMap::with_default(Dialect::DuckDB);
    SemanticEngine::load(&views_dir, None, dialects).expect("failed to load ecommerce views")
}

fn create_ecommerce_db() -> duckdb::Connection {
    let db = duckdb::Connection::open_in_memory().expect("duckdb open");
    db.execute_batch(
        "
        -- Users (inspired by Looker thelook users table)
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            age INTEGER,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            gender VARCHAR,
            created_at TIMESTAMP
        );
        INSERT INTO users VALUES
        (1, 'Alice',   'Smith',    'alice@example.com',   34, 'San Francisco', 'CA', 'US', 'F', '2024-01-10 08:00:00'),
        (2, 'Bob',     'Jones',    'bob@example.com',     28, 'New York',      'NY', 'US', 'M', '2024-02-15 12:00:00'),
        (3, 'Carol',   'Williams', 'carol@example.com',   72, 'Los Angeles',   'CA', 'US', 'F', '2024-03-01 09:00:00'),
        (4, 'David',   'Brown',    'david@example.com',   45, 'Chicago',       'IL', 'US', 'M', '2024-03-20 14:00:00'),
        (5, 'Eve',     'Davis',    'eve@example.com',     67, 'Miami',         'FL', 'US', 'F', '2024-04-05 11:00:00');

        -- Products (inspired by Looker products table)
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            retail_price DOUBLE,
            department VARCHAR
        );
        INSERT INTO products VALUES
        (1, 'Slim Jeans',       'Jeans',       'Levi',    89.99,  'Women'),
        (2, 'Classic Tee',      'Tops',        'Nike',    29.99,  'Men'),
        (3, 'Leather Belt',     'Accessories', 'Gucci',   199.99, 'Men'),
        (4, 'Winter Jacket',    'Outerwear',   'NorthFace', 249.99, 'Women'),
        (5, 'Running Shoes',    'Shoes',       'Nike',    129.99, 'Men');

        -- Orders (inspired by Cube.js orders with status)
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            status VARCHAR,
            total_amount DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO orders VALUES
        (1, 1, 'complete',    119.98, '2024-06-01 10:00:00'),
        (2, 1, 'complete',    199.99, '2024-06-15 14:00:00'),
        (3, 2, 'complete',    29.99,  '2024-07-01 09:00:00'),
        (4, 2, 'returned',    89.99,  '2024-07-10 11:00:00'),
        (5, 3, 'complete',    379.98, '2024-08-01 16:00:00'),
        (6, 3, 'processing',  129.99, '2024-08-15 10:00:00'),
        (7, 4, 'cancelled',   249.99, '2024-09-01 12:00:00'),
        (8, 4, 'complete',    159.98, '2024-09-10 15:00:00'),
        (9, 5, 'complete',    89.99,  '2024-10-01 08:00:00'),
        (10, 5, 'returned',   29.99,  '2024-10-15 13:00:00');

        -- Order Items (inspired by Looker order_items with sale_price and cost)
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            sale_price DOUBLE,
            cost DOUBLE,
            status VARCHAR,
            created_at TIMESTAMP,
            returned_at TIMESTAMP
        );
        INSERT INTO order_items VALUES
        (1,  1, 2, 29.99,  12.00, 'complete',   '2024-06-01 10:00:00', NULL),
        (2,  1, 1, 89.99,  35.00, 'complete',   '2024-06-01 10:00:00', NULL),
        (3,  2, 3, 199.99, 80.00, 'complete',   '2024-06-15 14:00:00', NULL),
        (4,  3, 2, 29.99,  12.00, 'complete',   '2024-07-01 09:00:00', NULL),
        (5,  4, 1, 89.99,  35.00, 'returned',   '2024-07-10 11:00:00', '2024-07-20 09:00:00'),
        (6,  5, 4, 249.99, 100.00,'complete',   '2024-08-01 16:00:00', NULL),
        (7,  5, 5, 129.99, 52.00, 'complete',   '2024-08-01 16:00:00', NULL),
        (8,  6, 5, 129.99, 52.00, 'processing', '2024-08-15 10:00:00', NULL),
        (9,  7, 4, 249.99, 100.00,'cancelled',  '2024-09-01 12:00:00', NULL),
        (10, 8, 2, 29.99,  12.00, 'complete',   '2024-09-10 15:00:00', NULL),
        (11, 8, 5, 129.99, 52.00, 'complete',   '2024-09-10 15:00:00', NULL),
        (12, 9, 1, 89.99,  35.00, 'complete',   '2024-10-01 08:00:00', NULL),
        (13, 10, 2, 29.99, 12.00, 'returned',   '2024-10-15 13:00:00', '2024-10-25 11:00:00');
        ",
    )
    .expect("seed ecommerce");
    db
}

fn rewrite_params(sql: &str) -> String {
    static RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\$(\d+)").unwrap());
    RE.replace_all(sql, "?").to_string()
}

fn execute(sql: &str, params: &[String]) -> Vec<Vec<String>> {
    let db = create_ecommerce_db();
    let rewritten = rewrite_params(sql);
    let mut stmt = db
        .prepare(&rewritten)
        .unwrap_or_else(|e| panic!("prepare failed: {e}\nSQL:\n{rewritten}"));
    let param_refs: Vec<&dyn duckdb::ToSql> =
        params.iter().map(|p| p as &dyn duckdb::ToSql).collect();

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

fn compile_and_run(request: &QueryRequest) -> (QueryResult, Vec<Vec<String>>) {
    let engine = load_ecommerce_engine();
    let result = engine.compile_query(request).expect("compile");
    println!("SQL:\n{}\nParams: {:?}", result.sql, result.params);
    let rows = execute(&result.sql, &result.params);
    println!("Rows ({}):", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }
    (result, rows)
}

// ---------------------------------------------------------------------------
// Multi-table joins (Looker explore pattern, Cube.js joins)
// ---------------------------------------------------------------------------

/// Looker pattern: order_items joined to orders via entity relationship.
/// Validates entity-based auto-join across two tables.
#[test]
#[ignore = "tier1"]
fn parity_join_order_items_to_orders() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["orders.status".to_string()],
        ..QueryRequest::new()
    });
    assert!(
        result.sql.to_lowercase().contains("join"),
        "Should generate a JOIN:\n{}",
        result.sql
    );
    assert!(
        !rows.is_empty(),
        "Should return rows grouped by order status"
    );
}

/// Three-table transitive join: order_items → orders → users.
/// Cube.js/Looker pattern: query spans from line items to user demographics.
#[test]
#[ignore = "tier1"]
fn parity_transitive_join_items_to_users() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["users.state".to_string()],
        ..QueryRequest::new()
    });
    // Should have two JOINs: order_items → orders, orders → users
    let join_count = result.sql.to_lowercase().matches("join").count();
    assert!(
        join_count >= 2,
        "Expected at least 2 JOINs for transitive path, got {join_count}:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
}

/// Two-table join: order_items → products.
/// Validates direct entity join without going through orders.
#[test]
#[ignore = "tier1"]
fn parity_join_items_to_products() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["products.category".to_string()],
        ..QueryRequest::new()
    });
    assert!(
        result.sql.to_lowercase().contains("join"),
        "Should generate a JOIN:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
}

/// Four-table join: order_items → orders → users + order_items → products.
/// Exercises the join graph with a fan-out from order_items.
#[test]
#[ignore = "tier1"]
fn parity_four_table_join() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.count".to_string()],
        dimensions: vec![
            "users.country".to_string(),
            "products.department".to_string(),
        ],
        ..QueryRequest::new()
    });
    let join_count = result.sql.to_lowercase().matches("join").count();
    assert!(
        join_count >= 3,
        "Expected at least 3 JOINs for 4-table query, got {join_count}:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
}

// ---------------------------------------------------------------------------
// Measure types (Cube.js, Looker, Omni all support these)
// ---------------------------------------------------------------------------

/// Basic aggregate measures: count, sum, average, count_distinct.
#[test]
#[ignore = "tier1"]
fn parity_basic_measure_types() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec![
            "order_items.count".to_string(),
            "order_items.total_sale_price".to_string(),
            "order_items.average_sale_price".to_string(),
            "order_items.unique_orders".to_string(),
        ],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1, "Single aggregate row expected");
    let row = &rows[0];
    // 13 order items
    assert!(row[0].contains("13"), "Expected 13 items, got: {}", row[0]);
    // 10 unique orders
    assert!(
        row[3].contains("10"),
        "Expected 10 unique orders, got: {}",
        row[3]
    );
}

/// Median measure (Looker supports median, most tools don't).
#[test]
#[ignore = "tier1"]
fn parity_median_measure() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.median_sale_price".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    // Verify the SQL uses a median function (PERCENTILE_CONT or MEDIAN)
    let sql_lower = result.sql.to_lowercase();
    assert!(
        sql_lower.contains("percentile_cont") || sql_lower.contains("median"),
        "Expected median SQL function, got:\n{}",
        result.sql
    );
}

/// Min/Max measures (common across all semantic layers).
#[test]
#[ignore = "tier1"]
fn parity_min_max_measures() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec![
            "products.min_price".to_string(),
            "products.max_price".to_string(),
        ],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // Min: 29.99, Max: 249.99
    assert!(
        row[0].contains("29.99"),
        "Expected min 29.99, got: {}",
        row[0]
    );
    assert!(
        row[1].contains("249.99"),
        "Expected max 249.99, got: {}",
        row[1]
    );
}

// ---------------------------------------------------------------------------
// Filtered measures (Cube.js, Looker, Omni all support these)
// ---------------------------------------------------------------------------

/// Cube.js pattern: completed_count with status filter on the measure.
#[test]
#[ignore = "tier1"]
fn parity_filtered_measure_cube_style() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec![
            "orders.count".to_string(),
            "orders.completed_count".to_string(),
        ],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // 10 total orders, 6 completed
    assert!(
        row[0].contains("10"),
        "Expected 10 total orders, got: {}",
        row[0]
    );
    assert!(
        row[1].contains("6"),
        "Expected 6 completed orders, got: {}",
        row[1]
    );
}

/// Looker pattern: returned_count and returned_total_sale_price.
#[test]
#[ignore = "tier1"]
fn parity_filtered_measure_looker_style() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec![
            "order_items.count".to_string(),
            "order_items.returned_count".to_string(),
            "order_items.returned_total_sale_price".to_string(),
        ],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // 13 total items, 2 returned (items 5 and 13)
    assert!(
        row[0].contains("13"),
        "Expected 13 total items, got: {}",
        row[0]
    );
    assert!(
        row[1].contains("2"),
        "Expected 2 returned items, got: {}",
        row[1]
    );
    // Returned revenue: 89.99 + 29.99 ≈ 119.98 (floating point)
    assert!(
        row[2].contains("119.9"),
        "Expected returned revenue ~119.98, got: {}",
        row[2]
    );
}

// ---------------------------------------------------------------------------
// Calculated dimensions (Looker, Omni patterns)
// ---------------------------------------------------------------------------

/// Calculated numeric dimension: gross_margin = sale_price - cost.
/// Looker pattern from order_items view.
#[test]
#[ignore = "tier1"]
fn parity_calculated_dimension_gross_margin() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_gross_margin".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    // Total margin = sum of (sale_price - cost) for all 13 items ≈ 890.86
    assert!(
        rows[0][0].contains("890.8"),
        "Expected gross margin ~890.86, got: {}",
        rows[0][0]
    );
}

/// Boolean dimension used in grouping (Looker yesno pattern).
/// Groups users by is_senior to verify the calculated boolean dimension works.
#[test]
#[ignore = "tier1"]
fn parity_boolean_dimension_grouping() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["users.count".to_string()],
        dimensions: vec!["users.is_senior".to_string()],
        ..QueryRequest::new()
    });
    // Should have 2 groups: true (seniors) and false (non-seniors)
    assert_eq!(rows.len(), 2, "Expected 2 boolean groups, got: {:?}", rows);
}

/// Filtered measure as alternative to boolean dimension filter.
/// Uses senior_count (filtered measure) to count seniors — the idiomatic airlayer approach.
#[test]
#[ignore = "tier1"]
fn parity_filtered_measure_senior_count() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["users.count".to_string(), "users.senior_count".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // 5 total users, 2 seniors (Carol 72, Eve 67)
    assert!(
        row[0].contains("5"),
        "Expected 5 total users, got: {}",
        row[0]
    );
    assert!(
        row[1].contains("2"),
        "Expected 2 senior users, got: {}",
        row[1]
    );
}

// ---------------------------------------------------------------------------
// Segments (Cube.js pattern)
// ---------------------------------------------------------------------------

/// Cube.js segment: only_completed filters the entire query.
#[test]
#[ignore = "tier1"]
fn parity_segment_cube_style() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["orders.count".to_string()],
        segments: vec!["orders.only_completed".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // 6 completed orders
    assert!(
        row[0].contains("6"),
        "Expected 6 completed orders, got: {}",
        row[0]
    );
}

/// Segment combined with grouping (Cube.js pattern).
#[test]
#[ignore = "tier1"]
fn parity_segment_with_grouping() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["users.count".to_string()],
        dimensions: vec!["users.state".to_string()],
        segments: vec!["users.california_users".to_string()],
        ..QueryRequest::new()
    });
    // Only CA users
    assert_eq!(rows.len(), 1, "Expected 1 state (CA), got: {:?}", rows);
}

// ---------------------------------------------------------------------------
// SQL-derived table (Looker derived_table pattern)
// ---------------------------------------------------------------------------

/// Looker pattern: user_order_facts is a SQL-based derived table.
/// Tests that views with `sql:` instead of `table:` compile and execute.
#[test]
#[ignore = "tier1"]
fn parity_sql_derived_table() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["user_order_facts.average_lifetime_orders".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    // Subquery pattern: should wrap the SQL as a subquery
    let sql_lower = result.sql.to_lowercase();
    assert!(
        sql_lower.contains("select")
            && (sql_lower.contains("group by") || sql_lower.contains("lifetime_orders")),
        "Should reference the derived SQL:\n{}",
        result.sql
    );
}

/// SQL-derived table joined to another table: user_order_facts → users
/// via the shared user entity.
#[test]
#[ignore = "tier1"]
fn parity_sql_derived_table_joined() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["user_order_facts.total_lifetime_revenue".to_string()],
        dimensions: vec!["users.state".to_string()],
        ..QueryRequest::new()
    });
    assert!(
        result.sql.to_lowercase().contains("join"),
        "Should JOIN user_order_facts to users:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
}

/// Boolean dimension from derived table used in grouping (Looker repeat_customer yesno).
#[test]
#[ignore = "tier1"]
fn parity_derived_table_boolean_dim() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["user_order_facts.count".to_string()],
        dimensions: vec!["user_order_facts.is_repeat_customer".to_string()],
        ..QueryRequest::new()
    });
    // All 5 users have 2 orders each, so all should be repeat customers (1 group: true)
    assert_eq!(
        rows.len(),
        1,
        "Expected 1 group (all repeat), got: {:?}",
        rows
    );
    assert!(
        rows[0][1].contains("5"),
        "Expected 5 repeat customers, got: {}",
        rows[0][1]
    );
}

// ---------------------------------------------------------------------------
// Time dimensions (all three tools support time granularity)
// ---------------------------------------------------------------------------

/// Time dimension with month granularity grouped — Cube.js/Looker/Omni all support this.
#[test]
#[ignore = "tier1"]
fn parity_time_dimension_month_granularity() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "order_items.created_at".to_string(),
            granularity: Some("month".to_string()),
            date_range: None,
        }],
        ..QueryRequest::new()
    });
    // Items span June-October 2024 (5 months)
    assert_eq!(rows.len(), 5, "Expected 5 months of data, got: {:?}", rows);
}

/// Time dimension date range filter (common in all tools).
#[test]
#[ignore = "tier1"]
fn parity_time_dimension_date_range() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["orders.count".to_string()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "orders.created_at".to_string(),
            granularity: Some("month".to_string()),
            date_range: Some(vec!["2024-06-01".to_string(), "2024-08-31".to_string()]),
        }],
        ..QueryRequest::new()
    });
    // June, July, August = 3 months
    assert_eq!(rows.len(), 3, "Expected 3 months in range, got: {:?}", rows);
}

// ---------------------------------------------------------------------------
// Motifs on multi-table queries (validates motifs work with joins)
// ---------------------------------------------------------------------------

/// Contribution motif on a joined query: revenue share by product category.
#[test]
#[ignore = "tier1"]
fn parity_motif_contribution_with_join() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["products.category".to_string()],
        motif: Some("contribution".to_string()),
        ..QueryRequest::new()
    });
    assert!(
        result.sql.to_lowercase().contains("join"),
        "Should JOIN to products:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
    // Should have motif output columns (total, share)
    assert!(
        rows[0].len() >= 4,
        "Expected >=4 columns (category + revenue + total + share), got {}",
        rows[0].len()
    );
}

/// Rank motif on user demographics: rank states by revenue.
#[test]
#[ignore = "tier1"]
fn parity_motif_rank_with_transitive_join() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["users.state".to_string()],
        motif: Some("rank".to_string()),
        ..QueryRequest::new()
    });
    // Should traverse: order_items → orders → users
    let join_count = result.sql.to_lowercase().matches("join").count();
    assert!(
        join_count >= 2,
        "Expected transitive JOINs, got {join_count}:\n{}",
        result.sql
    );
    assert!(result.sql.contains("RANK()"));
    assert!(!rows.is_empty());
}

// ---------------------------------------------------------------------------
// Query filters with various operators (all tools support these)
// ---------------------------------------------------------------------------

/// Equals filter on string dimension.
#[test]
#[ignore = "tier1"]
fn parity_filter_equals() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["orders.count".to_string()],
        dimensions: vec!["orders.status".to_string()],
        filters: vec![QueryFilter {
            member: Some("orders.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["complete".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1, "Expected 1 status row (complete)");
}

/// Not-equals filter.
#[test]
#[ignore = "tier1"]
fn parity_filter_not_equals() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["orders.count".to_string()],
        dimensions: vec!["orders.status".to_string()],
        filters: vec![QueryFilter {
            member: Some("orders.status".to_string()),
            operator: Some(FilterOperator::NotEquals),
            values: vec!["cancelled".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    });
    // Should exclude cancelled, leaving: complete, returned, processing
    assert_eq!(
        rows.len(),
        3,
        "Expected 3 status rows (complete, returned, processing), got: {:?}",
        rows
    );
}

/// Greater-than filter on numeric dimension (Omni pattern).
#[test]
#[ignore = "tier1"]
fn parity_filter_gt_numeric() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["products.count".to_string()],
        filters: vec![QueryFilter {
            member: Some("products.retail_price".to_string()),
            operator: Some(FilterOperator::Gt),
            values: vec!["100".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    // Products > $100: Leather Belt (199.99), Winter Jacket (249.99), Running Shoes (129.99)
    assert!(
        row[0].contains("3"),
        "Expected 3 products > $100, got: {}",
        row[0]
    );
}

/// Contains filter on string dimension.
#[test]
#[ignore = "tier1"]
fn parity_filter_contains() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["products.count".to_string()],
        filters: vec![QueryFilter {
            member: Some("products.name".to_string()),
            operator: Some(FilterOperator::Contains),
            values: vec!["Jeans".to_string()],
            and: None,
            or: None,
        }],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].contains("1"),
        "Expected 1 jeans product, got: {}",
        rows[0][0]
    );
}

// ---------------------------------------------------------------------------
// Multi-dialect compilation (validates same views compile for different dialects)
// ---------------------------------------------------------------------------

/// Verify the e-commerce model compiles to valid SQL for Postgres.
#[test]
#[ignore = "tier1"]
fn parity_compile_postgres() {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-ecommerce");
    let engine = SemanticEngine::load(
        &views_dir,
        None,
        DatasourceDialectMap::with_default(Dialect::Postgres),
    )
    .expect("load for postgres");
    let result = engine
        .compile_query(&QueryRequest {
            measures: vec!["order_items.total_sale_price".to_string()],
            dimensions: vec!["products.category".to_string()],
            ..QueryRequest::new()
        })
        .expect("compile postgres");
    // Postgres uses double-quote identifiers
    assert!(
        result.sql.contains('"'),
        "Postgres should use double-quote identifiers:\n{}",
        result.sql
    );
}

/// Verify the e-commerce model compiles to valid SQL for BigQuery.
#[test]
#[ignore = "tier1"]
fn parity_compile_bigquery() {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-ecommerce");
    let engine = SemanticEngine::load(
        &views_dir,
        None,
        DatasourceDialectMap::with_default(Dialect::BigQuery),
    )
    .expect("load for bigquery");
    let result = engine
        .compile_query(&QueryRequest {
            measures: vec!["order_items.total_sale_price".to_string()],
            dimensions: vec!["products.category".to_string()],
            ..QueryRequest::new()
        })
        .expect("compile bigquery");
    // BigQuery uses backtick identifiers
    assert!(
        result.sql.contains('`'),
        "BigQuery should use backtick identifiers:\n{}",
        result.sql
    );
}

/// Verify the e-commerce model compiles to valid SQL for Snowflake.
#[test]
#[ignore = "tier1"]
fn parity_compile_snowflake() {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration/views-ecommerce");
    let engine = SemanticEngine::load(
        &views_dir,
        None,
        DatasourceDialectMap::with_default(Dialect::Snowflake),
    )
    .expect("load for snowflake");
    let result = engine
        .compile_query(&QueryRequest {
            measures: vec![
                "order_items.total_sale_price".to_string(),
                "order_items.returned_count".to_string(),
            ],
            dimensions: vec!["users.state".to_string()],
            ..QueryRequest::new()
        })
        .expect("compile snowflake");
    println!("Snowflake SQL:\n{}", result.sql);
    // Snowflake uses double-quote identifiers (same as Postgres)
    assert!(
        result.sql.contains('"'),
        "Snowflake should use double-quote identifiers:\n{}",
        result.sql
    );
    // Should have JOINs for the transitive path
    assert!(
        result.sql.to_lowercase().contains("join"),
        "Expected JOINs in multi-table Snowflake query:\n{}",
        result.sql
    );
}
