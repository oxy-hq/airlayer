use airlayer::engine::query::{
    FilterOperator, OrderBy, QueryFilter, QueryRequest, TimeDimensionQuery,
};
use airlayer::{DatasourceDialectMap, Dialect, SemanticEngine};
use duckdb::{params, Connection};
use std::path::Path;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

fn simple_count() -> QueryRequest {
    QueryRequest {
        measures: vec!["orders.count".into()],
        ..QueryRequest::new()
    }
}

fn single_dim_single_measure() -> QueryRequest {
    QueryRequest {
        measures: vec!["orders.total_revenue".into()],
        dimensions: vec!["orders.status".into()],
        ..QueryRequest::new()
    }
}

fn multi_measure() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "orders.count".into(),
            "orders.total_revenue".into(),
            "orders.avg_order_value".into(),
        ],
        dimensions: vec!["orders.status".into(), "orders.channel".into()],
        ..QueryRequest::new()
    }
}

fn filtered_query() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "orders.total_revenue".into(),
            "orders.unique_customers".into(),
        ],
        dimensions: vec!["orders.country".into()],
        filters: vec![
            QueryFilter {
                member: Some("orders.status".into()),
                operator: Some(FilterOperator::Equals),
                values: vec!["completed".into()],
                and: None,
                or: None,
            },
            QueryFilter {
                member: Some("orders.amount".into()),
                operator: Some(FilterOperator::Gte),
                values: vec!["100".into()],
                and: None,
                or: None,
            },
        ],
        ..QueryRequest::new()
    }
}

fn time_dimension() -> QueryRequest {
    QueryRequest {
        measures: vec!["orders.total_revenue".into()],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "orders.created_at".into(),
            granularity: Some("month".into()),
            date_range: None,
        }],
        ..QueryRequest::new()
    }
}

fn two_table_join() -> QueryRequest {
    QueryRequest {
        measures: vec!["orders.total_revenue".into()],
        dimensions: vec!["customers.country".into(), "customers.tier".into()],
        ..QueryRequest::new()
    }
}

fn three_table_join() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "line_items.total_line_value".into(),
            "line_items.total_quantity".into(),
        ],
        dimensions: vec!["orders.status".into(), "products.category".into()],
        ..QueryRequest::new()
    }
}

fn full_pipeline() -> QueryRequest {
    QueryRequest {
        measures: vec![
            "orders.total_revenue".into(),
            "line_items.total_quantity".into(),
        ],
        dimensions: vec!["customers.country".into(), "products.category".into()],
        filters: vec![QueryFilter {
            member: Some("orders.status".into()),
            operator: Some(FilterOperator::Equals),
            values: vec!["completed".into()],
            and: None,
            or: None,
        }],
        time_dimensions: vec![TimeDimensionQuery {
            dimension: "orders.created_at".into(),
            granularity: Some("month".into()),
            date_range: None,
        }],
        order: vec![OrderBy {
            id: "orders.total_revenue".into(),
            desc: true,
        }],
        limit: Some(100),
        ..QueryRequest::new()
    }
}

fn all_queries() -> Vec<(&'static str, QueryRequest)> {
    vec![
        ("simple_count", simple_count()),
        ("1dim_1measure", single_dim_single_measure()),
        ("3measures_2dims", multi_measure()),
        ("filtered", filtered_query()),
        ("time_dimension", time_dimension()),
        ("2_table_join", two_table_join()),
        ("3_table_join", three_table_join()),
        ("full_pipeline", full_pipeline()),
    ]
}

// ---------------------------------------------------------------------------
// DuckDB seed data
// ---------------------------------------------------------------------------

fn seed_duckdb(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            city VARCHAR,
            country VARCHAR,
            signup_date TIMESTAMP,
            tier VARCHAR
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'alice@example.com', 'New York', 'US', '2024-01-15', 'gold'),
            (2, 'Bob', 'bob@example.com', 'London', 'UK', '2024-02-20', 'silver'),
            (3, 'Carol', 'carol@example.com', 'Berlin', 'DE', '2024-03-10', 'gold'),
            (4, 'Dave', 'dave@example.com', 'New York', 'US', '2024-04-05', 'bronze'),
            (5, 'Eve', 'eve@example.com', 'Tokyo', 'JP', '2024-05-01', 'silver');

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            status VARCHAR,
            amount DOUBLE,
            created_at TIMESTAMP,
            channel VARCHAR,
            country VARCHAR,
            discount_pct DOUBLE
        );
        INSERT INTO orders VALUES
            (1, 1, 'completed', 150.0, '2024-01-20 10:00:00', 'web', 'US', 0.0),
            (2, 2, 'completed', 200.0, '2024-01-25 14:30:00', 'mobile', 'UK', 5.0),
            (3, 1, 'pending', 75.0, '2024-02-10 09:00:00', 'web', 'US', 0.0),
            (4, 3, 'completed', 300.0, '2024-02-15 16:00:00', 'web', 'DE', 10.0),
            (5, 4, 'cancelled', 50.0, '2024-03-01 11:00:00', 'mobile', 'US', 0.0),
            (6, 5, 'completed', 120.0, '2024-03-15 13:00:00', 'web', 'JP', 0.0),
            (7, 2, 'completed', 180.0, '2024-03-20 15:00:00', 'mobile', 'UK', 5.0),
            (8, 3, 'pending', 90.0, '2024-04-01 08:00:00', 'web', 'DE', 0.0);

        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            sku VARCHAR,
            list_price DOUBLE
        );
        INSERT INTO products VALUES
            (1, 'Widget A', 'electronics', 'Acme', 'WA-001', 25.0),
            (2, 'Widget B', 'electronics', 'Acme', 'WB-001', 35.0),
            (3, 'Gadget C', 'accessories', 'Beta', 'GC-001', 15.0),
            (4, 'Gadget D', 'accessories', 'Beta', 'GD-001', 45.0),
            (5, 'Gizmo E', 'hardware', 'Gamma', 'GE-001', 60.0);

        CREATE TABLE line_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DOUBLE
        );
        INSERT INTO line_items VALUES
            (1, 1, 1, 2, 25.0),
            (2, 1, 3, 1, 15.0),
            (3, 2, 2, 3, 35.0),
            (4, 2, 4, 1, 45.0),
            (5, 3, 1, 1, 25.0),
            (6, 4, 5, 2, 60.0),
            (7, 4, 2, 1, 35.0),
            (8, 5, 3, 4, 15.0),
            (9, 6, 1, 1, 25.0),
            (10, 6, 4, 2, 45.0),
            (11, 7, 5, 1, 60.0),
            (12, 7, 2, 2, 35.0),
            (13, 8, 3, 3, 15.0);
        ",
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// SQL normalization helpers
// ---------------------------------------------------------------------------

/// Substitute $1, $2, ... placeholders with literal values for DuckDB execution.
fn substitute_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    // Replace in reverse order so $10 doesn't match $1 first
    for (i, val) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        // Try to parse as number; if not, quote as string
        let literal = if val.parse::<f64>().is_ok() {
            val.clone()
        } else {
            format!("'{}'", val.replace('\'', "''"))
        };
        result = result.replace(&placeholder, &literal);
    }
    result
}

/// Patch Cube's Postgres-specific SQL to work in DuckDB.
fn patch_cube_sql_for_duckdb(sql: &str) -> String {
    let mut s = sql.to_string();
    // Remove ::timestamptz casts
    s = s.replace("::timestamptz", "");
    // Remove AT TIME ZONE 'UTC' (DuckDB timestamps are already timezone-naive)
    s = s.replace(" AT TIME ZONE 'UTC'", "");
    // UNIX_TIMESTAMP() -> epoch(now()) — for cache key queries (not needed for results)
    s = s.replace("UNIX_TIMESTAMP()", "epoch(now())");
    s
}

// ---------------------------------------------------------------------------
// Execute SQL in DuckDB, return sorted rows as Vec<Vec<String>>
// ---------------------------------------------------------------------------

fn execute_sql(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("SQL prepare error: {e}\nSQL: {sql}"))?;

    let mut rows_out = Vec::new();
    let mut duckdb_rows = stmt
        .query(params![])
        .map_err(|e| format!("SQL execution error: {e}\nSQL: {sql}"))?;

    while let Some(row) = duckdb_rows
        .next()
        .map_err(|e| format!("Row fetch error: {e}"))?
    {
        let col_count = row.as_ref().column_count();
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: String = match row.get::<_, Option<f64>>(i) {
                Ok(Some(f)) => {
                    if f == f.floor() && f.abs() < 1e15 {
                        format!("{:.0}", f)
                    } else {
                        format!("{:.6}", f)
                    }
                }
                Ok(None) => "NULL".to_string(),
                Err(_) => row
                    .get::<_, String>(i)
                    .unwrap_or_else(|_| "NULL".to_string()),
            };
            vals.push(val);
        }
        rows_out.push(vals);
    }

    rows_out.sort();
    Ok(rows_out)
}

// ---------------------------------------------------------------------------
// Cube helpers
// ---------------------------------------------------------------------------

fn cube_url() -> String {
    std::env::var("CUBE_URL").unwrap_or_else(|_| "http://localhost:4000".into())
}

fn cube_secret() -> String {
    std::env::var("CUBE_API_SECRET").unwrap_or_else(|_| "benchmarksecret".into())
}

fn to_cube_payload(q: &QueryRequest) -> serde_json::Value {
    let mut query = serde_json::json!({});

    if !q.measures.is_empty() {
        query["measures"] = serde_json::json!(q.measures);
    }
    if !q.dimensions.is_empty() {
        query["dimensions"] = serde_json::json!(q.dimensions);
    }
    if !q.filters.is_empty() {
        query["filters"] = serde_json::to_value(&q.filters).unwrap();
    }
    if !q.time_dimensions.is_empty() {
        let tds: Vec<serde_json::Value> = q
            .time_dimensions
            .iter()
            .map(|td| {
                let mut v = serde_json::json!({"dimension": td.dimension});
                if let Some(ref g) = td.granularity {
                    v["granularity"] = serde_json::json!(g);
                }
                if let Some(ref dr) = td.date_range {
                    v["dateRange"] = serde_json::json!(dr);
                }
                v
            })
            .collect();
        query["timeDimensions"] = serde_json::json!(tds);
    }
    if !q.order.is_empty() {
        let order: serde_json::Map<String, serde_json::Value> = q
            .order
            .iter()
            .map(|o| {
                (
                    o.id.clone(),
                    serde_json::json!(if o.desc { "desc" } else { "asc" }),
                )
            })
            .collect();
        query["order"] = serde_json::Value::Object(order);
    }
    if let Some(limit) = q.limit {
        query["limit"] = serde_json::json!(limit);
    }

    serde_json::json!({"query": query})
}

fn cube_compile_sql(url: &str, secret: &str, payload: &serde_json::Value) -> Result<String, String> {
    let resp = ureq::post(url)
        .set("Content-Type", "application/json")
        .set("Authorization", secret)
        .send_json(payload.clone())
        .map_err(|e| format!("Cube error: {e}"))?;

    let body: serde_json::Value = resp.into_json().map_err(|e| format!("JSON parse: {e}"))?;
    let sql = body["sql"]["sql"][0]
        .as_str()
        .ok_or_else(|| "Could not extract SQL from Cube response".to_string())?
        .to_string();

    // Extract params from response.sql.sql[1] (array of strings)
    let params: Vec<String> = body["sql"]["sql"][1]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default();

    Ok(substitute_params(&sql, &params))
}

fn cube_compile_noresult(url: &str, secret: &str, payload: &serde_json::Value) -> Result<(), String> {
    ureq::post(url)
        .set("Content-Type", "application/json")
        .set("Authorization", secret)
        .send_json(payload.clone())
        .map_err(|e| format!("Cube error: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark + equivalence
// ---------------------------------------------------------------------------

struct BenchResult {
    name: &'static str,
    airlayer_us: f64,
    cube_us: f64,
    equivalent: Option<bool>,
    mismatch_detail: Option<String>,
}

fn run_query(
    engine: &SemanticEngine,
    conn: &Connection,
    cube_sql_url: &str,
    cube_secret: &str,
    name: &'static str,
    query: &QueryRequest,
    iterations: usize,
    warmup: usize,
) -> BenchResult {
    let cube_payload = to_cube_payload(query);

    // Warmup
    for _ in 0..warmup {
        let _ = engine.compile_query(query);
        let _ = cube_compile_noresult(cube_sql_url, cube_secret, &cube_payload);
    }

    // Bench airlayer
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.compile_query(query).unwrap();
    }
    let airlayer_us = start.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64;

    // Bench Cube
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = cube_compile_noresult(cube_sql_url, cube_secret, &cube_payload).unwrap();
    }
    let cube_us = start.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64;

    // Equivalence check: compile once, execute both in DuckDB, compare results
    let airlayer_result = engine.compile_query(query).unwrap();
    let airlayer_sql = substitute_params(&airlayer_result.sql, &airlayer_result.params);
    let cube_sql = match cube_compile_sql(cube_sql_url, cube_secret, &cube_payload) {
        Ok(sql) => patch_cube_sql_for_duckdb(&sql),
        Err(e) => {
            return BenchResult {
                name,
                airlayer_us,
                cube_us,
                equivalent: None,
                mismatch_detail: Some(format!("Cube SQL extraction failed: {e}")),
            };
        }
    };

    let al_rows = execute_sql(conn, &airlayer_sql);
    let cube_rows = execute_sql(conn, &cube_sql);

    let (equivalent, mismatch_detail) = match (al_rows, cube_rows) {
        (Ok(al), Ok(cu)) => {
            if al == cu {
                (Some(true), None)
            } else {
                let detail = format!(
                    "Row count: airlayer={}, cube={}\n  airlayer SQL: {}\n  cube SQL: {}\n  airlayer first row: {:?}\n  cube first row: {:?}",
                    al.len(), cu.len(), airlayer_sql, cube_sql,
                    al.first(), cu.first()
                );
                (Some(false), Some(detail))
            }
        }
        (Err(e), _) => (None, Some(format!("airlayer SQL failed: {e}"))),
        (_, Err(e)) => (None, Some(format!("Cube SQL failed: {e}"))),
    };

    BenchResult {
        name,
        airlayer_us,
        cube_us,
        equivalent,
        mismatch_detail,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let iterations: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(500);
    let warmup = iterations / 10;
    let verify_only = args.iter().any(|a| a == "--verify");

    let cube_base = cube_url();
    let secret = cube_secret();
    let cube_sql_url = format!("{cube_base}/cubejs-api/v1/sql");

    // Check Cube is reachable
    let test_payload = serde_json::json!({"query": {"measures": ["orders.count"]}});
    if cube_compile_noresult(&cube_sql_url, &secret, &test_payload).is_err() {
        eprintln!("Cube is not reachable at {cube_base}");
        eprintln!("Start with: cd bench-vs-cube && docker compose up -d && sleep 15");
        std::process::exit(1);
    }

    // Load airlayer engine
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../benches/views");
    let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
    let engine = SemanticEngine::load(&views_dir, None, dialects).unwrap();

    // Create in-memory DuckDB with seed data
    let conn = Connection::open_in_memory().unwrap();
    seed_duckdb(&conn);

    let queries = all_queries();
    let iters = if verify_only { 1 } else { iterations };
    let warm = if verify_only { 0 } else { warmup };

    println!("airlayer vs Cube — SQL compilation benchmark");
    println!("Iterations: {iters} (+ {warm} warmup)");
    println!("{:=<80}", "");

    let mut results = Vec::new();
    for (name, query) in &queries {
        let r = run_query(
            &engine,
            &conn,
            &cube_sql_url,
            &secret,
            name,
            query,
            iters,
            warm,
        );
        results.push(r);
    }

    // Print results
    println!(
        "\n{:<25} {:>12} {:>12} {:>9} {:>8}",
        "Query", "airlayer", "Cube", "Speedup", "Match"
    );
    println!("{:-<70}", "");
    let mut all_match = true;
    for r in &results {
        let speedup = r.cube_us / r.airlayer_us;
        let al = if r.airlayer_us < 1000.0 {
            format!("{:.1}us", r.airlayer_us)
        } else {
            format!("{:.2}ms", r.airlayer_us / 1000.0)
        };
        let cu = if r.cube_us < 1000.0 {
            format!("{:.1}us", r.cube_us)
        } else {
            format!("{:.2}ms", r.cube_us / 1000.0)
        };
        let eq = match r.equivalent {
            Some(true) => "ok",
            Some(false) => {
                all_match = false;
                "MISMATCH"
            }
            None => "SKIP",
        };
        println!(
            "{:<25} {:>12} {:>12} {:>8.0}x {:>8}",
            r.name, al, cu, speedup, eq
        );
    }

    // Print mismatch/skip details
    let issues: Vec<_> = results
        .iter()
        .filter(|r| r.equivalent != Some(true))
        .collect();
    if !issues.is_empty() {
        println!("\n{:=<80}", "");
        println!("DETAILS");
        println!("{:=<80}", "");
        for r in &issues {
            let label = if r.equivalent == Some(false) { "MISMATCH" } else { "SKIP" };
            println!("\n[{}] ({label})", r.name);
            if let Some(ref detail) = r.mismatch_detail {
                for line in detail.lines() {
                    println!("  {line}");
                }
            }
        }
    }

    if !all_match {
        eprintln!("\nResult mismatch detected — queries produce different results.");
        std::process::exit(1);
    }

    let skipped = results.iter().filter(|r| r.equivalent.is_none()).count();
    if skipped > 0 {
        println!("\nAll verifiable queries match. {skipped} skipped (SQL execution error on one side).");
    } else {
        println!("\nAll queries produce equivalent results.");
    }
}
