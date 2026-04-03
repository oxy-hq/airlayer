use airlayer::engine::query::{
    FilterOperator, OrderBy, QueryFilter, QueryRequest, TimeDimensionQuery,
};
use airlayer::{DatasourceDialectMap, Dialect, SemanticEngine};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::path::Path;

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

fn load_engine() -> SemanticEngine {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/views");
    let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
    SemanticEngine::load(&views_dir, None, dialects).unwrap()
}

fn bench_compilation(c: &mut Criterion) {
    let engine = load_engine();
    let queries = all_queries();

    let mut group = c.benchmark_group("sql_compilation");
    for (name, query) in &queries {
        group.bench_with_input(BenchmarkId::new("airlayer", name), query, |b, q| {
            b.iter(|| engine.compile_query(q).unwrap());
        });
    }
    group.finish();
}

fn bench_schema_load(c: &mut Criterion) {
    let views_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/views");

    c.bench_function("schema_load", |b| {
        b.iter(|| {
            let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
            SemanticEngine::load(&views_dir, None, dialects).unwrap()
        });
    });
}

criterion_group!(benches, bench_compilation, bench_schema_load);
criterion_main!(benches);
