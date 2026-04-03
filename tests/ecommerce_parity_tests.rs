//! E-commerce multi-table tests: exercises join graph, SQL-derived views, and
//! motifs across joined tables — patterns not covered by the single-table events suite.
//!
//! Data model (inspired by Looker thelook / Cube.js ecommerce):
//!   order_items → orders → users
//!   order_items → products
//!   user_order_facts (SQL-derived) → users
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
        CREATE TABLE users (
            id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR,
            email VARCHAR, age INTEGER, city VARCHAR, state VARCHAR,
            country VARCHAR, gender VARCHAR, created_at TIMESTAMP
        );
        INSERT INTO users VALUES
        (1, 'Alice',   'Smith',    'alice@example.com',   34, 'San Francisco', 'CA', 'US', 'F', '2024-01-10 08:00:00'),
        (2, 'Bob',     'Jones',    'bob@example.com',     28, 'New York',      'NY', 'US', 'M', '2024-02-15 12:00:00'),
        (3, 'Carol',   'Williams', 'carol@example.com',   72, 'Los Angeles',   'CA', 'US', 'F', '2024-03-01 09:00:00'),
        (4, 'David',   'Brown',    'david@example.com',   45, 'Chicago',       'IL', 'US', 'M', '2024-03-20 14:00:00'),
        (5, 'Eve',     'Davis',    'eve@example.com',     67, 'Miami',         'FL', 'US', 'F', '2024-04-05 11:00:00');

        CREATE TABLE products (
            id INTEGER PRIMARY KEY, name VARCHAR, category VARCHAR,
            brand VARCHAR, retail_price DOUBLE, department VARCHAR
        );
        INSERT INTO products VALUES
        (1, 'Slim Jeans',    'Jeans',       'Levi',      89.99,  'Women'),
        (2, 'Classic Tee',   'Tops',        'Nike',      29.99,  'Men'),
        (3, 'Leather Belt',  'Accessories', 'Gucci',     199.99, 'Men'),
        (4, 'Winter Jacket', 'Outerwear',   'NorthFace', 249.99, 'Women'),
        (5, 'Running Shoes', 'Shoes',       'Nike',      129.99, 'Men');

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, user_id INTEGER, status VARCHAR,
            total_amount DOUBLE, created_at TIMESTAMP
        );
        INSERT INTO orders VALUES
        (1, 1, 'complete',   119.98, '2024-06-01 10:00:00'),
        (2, 1, 'complete',   199.99, '2024-06-15 14:00:00'),
        (3, 2, 'complete',   29.99,  '2024-07-01 09:00:00'),
        (4, 2, 'returned',   89.99,  '2024-07-10 11:00:00'),
        (5, 3, 'complete',   379.98, '2024-08-01 16:00:00'),
        (6, 3, 'processing', 129.99, '2024-08-15 10:00:00'),
        (7, 4, 'cancelled',  249.99, '2024-09-01 12:00:00'),
        (8, 4, 'complete',   159.98, '2024-09-10 15:00:00'),
        (9, 5, 'complete',   89.99,  '2024-10-01 08:00:00'),
        (10, 5, 'returned',  29.99,  '2024-10-15 13:00:00');

        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER,
            sale_price DOUBLE, cost DOUBLE, status VARCHAR,
            created_at TIMESTAMP, returned_at TIMESTAMP
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
// Multi-table joins (not covered by single-table events suite)
// ---------------------------------------------------------------------------

/// Two-table join: order_items → orders via shared `order` entity.
#[test]
#[ignore = "tier1"]
fn join_two_tables() {
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
    assert!(!rows.is_empty());
}

/// Three-table transitive join: order_items → orders → users.
#[test]
#[ignore = "tier1"]
fn join_three_tables_transitive() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["users.state".to_string()],
        ..QueryRequest::new()
    });
    let join_count = result.sql.to_lowercase().matches("join").count();
    assert!(
        join_count >= 2,
        "Expected at least 2 JOINs for transitive path, got {join_count}:\n{}",
        result.sql
    );
    assert!(!rows.is_empty());
}

/// Four-table fan-out: order_items → orders → users + order_items → products.
#[test]
#[ignore = "tier1"]
fn join_four_tables_fan_out() {
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
// SQL-derived table (Looker derived_table pattern — not tested elsewhere)
// ---------------------------------------------------------------------------

/// View with `sql:` instead of `table:` compiles and executes.
#[test]
#[ignore = "tier1"]
fn sql_derived_table_executes() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["user_order_facts.average_lifetime_orders".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    let sql_lower = result.sql.to_lowercase();
    assert!(
        sql_lower.contains("group by") || sql_lower.contains("lifetime_orders"),
        "Should reference the derived SQL:\n{}",
        result.sql
    );
}

/// SQL-derived table joined to another table via shared entity.
#[test]
#[ignore = "tier1"]
fn sql_derived_table_joined_to_users() {
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

/// Boolean dimension on a SQL-derived table (Looker repeat_customer yesno).
#[test]
#[ignore = "tier1"]
fn sql_derived_table_boolean_dim() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["user_order_facts.count".to_string()],
        dimensions: vec!["user_order_facts.is_repeat_customer".to_string()],
        ..QueryRequest::new()
    });
    // All 5 users have 2 orders each → 1 group (all true)
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
// Motifs combined with joins (not tested in single-table suite)
// ---------------------------------------------------------------------------

/// Contribution motif on a joined query: revenue share by product category.
#[test]
#[ignore = "tier1"]
fn motif_contribution_with_join() {
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
    // Base columns (category + revenue) + motif columns (total + share)
    assert!(
        rows[0].len() >= 4,
        "Expected >=4 columns, got {}",
        rows[0].len()
    );
}

/// Rank motif on a transitive join: rank states by revenue.
#[test]
#[ignore = "tier1"]
fn motif_rank_with_transitive_join() {
    let (result, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_sale_price".to_string()],
        dimensions: vec!["users.state".to_string()],
        motif: Some("rank".to_string()),
        ..QueryRequest::new()
    });
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
// Calculated expression measure with value correctness
// ---------------------------------------------------------------------------

/// Gross margin (sale_price - cost) summed across all items.
#[test]
#[ignore = "tier1"]
fn calculated_expression_measure() {
    let (_, rows) = compile_and_run(&QueryRequest {
        measures: vec!["order_items.total_gross_margin".to_string()],
        ..QueryRequest::new()
    });
    assert_eq!(rows.len(), 1);
    // sum of (sale_price - cost) for all 13 items ≈ 890.86
    assert!(
        rows[0][0].contains("890.8"),
        "Expected gross margin ~890.86, got: {}",
        rows[0][0]
    );
}
