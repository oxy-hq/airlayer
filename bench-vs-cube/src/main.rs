use airlayer::{SemanticEngine, DatasourceDialectMap, Dialect};
use airlayer::engine::query::{QueryRequest, QueryFilter, FilterOperator, TimeDimensionQuery, OrderBy};
use std::path::Path;
use std::time::Instant;

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
        let tds: Vec<serde_json::Value> = q.time_dimensions.iter().map(|td| {
            let mut v = serde_json::json!({"dimension": td.dimension});
            if let Some(ref g) = td.granularity {
                v["granularity"] = serde_json::json!(g);
            }
            if let Some(ref dr) = td.date_range {
                v["dateRange"] = serde_json::json!(dr);
            }
            v
        }).collect();
        query["timeDimensions"] = serde_json::json!(tds);
    }
    if !q.order.is_empty() {
        let order: serde_json::Map<String, serde_json::Value> = q.order.iter().map(|o| {
            (o.id.clone(), serde_json::json!(if o.desc { "desc" } else { "asc" }))
        }).collect();
        query["order"] = serde_json::Value::Object(order);
    }
    if let Some(limit) = q.limit {
        query["limit"] = serde_json::json!(limit);
    }

    serde_json::json!({"query": query})
}

fn cube_compile(url: &str, secret: &str, payload: &serde_json::Value) -> Result<(), String> {
    let resp = ureq::post(url)
        .set("Content-Type", "application/json")
        .set("Authorization", secret)
        .send_json(payload.clone());

    match resp {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Cube error: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

struct BenchResult {
    name: &'static str,
    airlayer_us: f64,
    cube_us: f64,
}

fn bench_iterations(
    engine: &SemanticEngine,
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
        let _ = cube_compile(cube_sql_url, cube_secret, &cube_payload);
    }

    // Bench airlayer
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.compile_query(query).unwrap();
    }
    let airlayer_total = start.elapsed();
    let airlayer_us = airlayer_total.as_secs_f64() * 1_000_000.0 / iterations as f64;

    // Bench Cube
    let start = Instant::now();
    for _ in 0..iterations {
        cube_compile(cube_sql_url, cube_secret, &cube_payload).unwrap();
    }
    let cube_total = start.elapsed();
    let cube_us = cube_total.as_secs_f64() * 1_000_000.0 / iterations as f64;

    BenchResult { name, airlayer_us, cube_us }
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let warmup = iterations / 10;

    let cube_base = cube_url();
    let secret = cube_secret();
    let cube_sql_url = format!("{cube_base}/cubejs-api/v1/sql");

    // Check Cube is reachable (use a test query — /readyz fails without a real DB)
    let test_payload = serde_json::json!({"query": {"measures": ["orders.count"]}});
    if cube_compile(&cube_sql_url, &secret, &test_payload).is_err() {
        eprintln!("Cube is not reachable at {cube_base}");
        eprintln!("Start with: cd bench-vs-cube && docker compose up -d && sleep 15");
        std::process::exit(1);
    }

    // Load airlayer engine
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../benches/views");
    let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
    let engine = SemanticEngine::load(&views_dir, None, dialects).unwrap();

    let queries = all_queries();

    println!("airlayer vs Cube — SQL compilation benchmark");
    println!("Iterations: {iterations} (+ {warmup} warmup)");
    println!("{:=<70}", "");

    let mut results = Vec::new();
    for (name, query) in &queries {
        let r = bench_iterations(&engine, &cube_sql_url, &secret, name, query, iterations, warmup);
        results.push(r);
    }

    // Print results
    println!("\n{:<25} {:>15} {:>15} {:>10}", "Query", "airlayer", "Cube", "Speedup");
    println!("{:-<70}", "");
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
        println!("{:<25} {:>15} {:>15} {:>9.0}x", r.name, al, cu, speedup);
    }
}
