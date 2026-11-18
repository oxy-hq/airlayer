//! A view compiled against a caller-supplied relation (`SemanticEngine::with_relations`)
//! and the raw columns a query reads (`SemanticEngine::columns_read`).
//!
//! The decision engine uses both to evaluate a measure such as `ads.spend` over its
//! simulated rows, held in an in-memory DuckDB table, instead of hand-writing the same
//! quantity a second time.
#![cfg(feature = "exec-duckdb")]

use std::collections::{BTreeMap, BTreeSet};

use airlayer::dialect::Dialect;
use airlayer::engine::query::{QueryFilter, QueryRequest};
use airlayer::engine::relations::{RelationOverride, RelationOverrides};
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
use airlayer::executor::duckdb::execute_on_connection;
use airlayer::schema::models::SemanticLayer;
use airlayer::schema::parser::SchemaParser;

const ADS: &str = r#"
name: ads
table: analytics.ads
datasource: warehouse
entities:
  - { name: product_id, type: foreign, key: product_id }
dimensions:
  - { name: channel, type: string, expr: channel }
  - { name: product_id, type: string, expr: product_id }
measures:
  - { name: spend, type: sum, expr: ad_spend }
  - { name: clicks, type: sum, expr: clicks }
  - name: brand_spend
    type: sum
    expr: ad_spend
    filters:
      - expr: "channel = 'brand'"
  - name: cost_per_click
    type: number
    expr: "CAST({{ads.spend}} AS DOUBLE) / NULLIF({{ads.clicks}}, 0)"
  - { name: rows, type: count }
  - { name: category_count, type: count_distinct, expr: "{{products.category}}" }
"#;

const PRODUCTS: &str = r#"
name: products
table: analytics.products
datasource: warehouse
entities:
  - { name: product_id, type: primary, key: product_id }
dimensions:
  - { name: product_id, type: string, expr: product_id }
  - { name: category, type: string, expr: category }
"#;

/// The warehouse engine: Snowflake, so a compile that ignored the override's dialect
/// would show in the SQL and fail on DuckDB.
fn engine() -> SemanticEngine {
    let parser = SchemaParser::new();
    let layer = SemanticLayer::new(
        vec![
            parser.parse_view_str(ADS, "ads").unwrap(),
            parser.parse_view_str(PRODUCTS, "products").unwrap(),
        ],
        None,
    );
    let mut dialects = DatasourceDialectMap::new();
    dialects.insert("warehouse", Dialect::Snowflake);
    SemanticEngine::from_semantic_layer(layer, dialects).unwrap()
}

fn sim_ads() -> RelationOverrides {
    let mut views = BTreeMap::new();
    views.insert(
        "ads".to_string(),
        RelationOverride {
            table: "sim_ads".to_string(),
            dimensions: vec!["candidate".to_string(), "scenario".to_string()],
        },
    );
    RelationOverrides {
        dialect: Dialect::DuckDB,
        views,
    }
}

/// Simulated rows: 2 candidates × 2 scenarios × 2 rows each.
fn sim_db() -> duckdb::Connection {
    let db = duckdb::Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE sim_ads (candidate INTEGER, scenario INTEGER, product_id VARCHAR,
                               channel VARCHAR, ad_spend DOUBLE, clicks DOUBLE);
         INSERT INTO sim_ads VALUES
           (0, 0, 'a', 'brand',  10, 5), (0, 0, 'b', 'search',  1, 1),
           (0, 1, 'a', 'brand',  20, 4), (0, 1, 'b', 'search',  2, 1),
           (1, 0, 'a', 'brand',  30, 3), (1, 0, 'b', 'search',  3, 2),
           (1, 1, 'a', 'search', 40, 8), (1, 1, 'b', 'search',  4, 2);",
    )
    .unwrap();
    db
}

fn by_cell(measure: &str) -> QueryRequest {
    QueryRequest {
        measures: vec![measure.to_string()],
        dimensions: vec!["ads.candidate".to_string(), "ads.scenario".to_string()],
        limit: Some(airlayer::engine::UNBOUNDED_QUERY_LIMIT),
        ..QueryRequest::new()
    }
}

/// `(candidate, scenario) → value` from running `request` on the simulated rows.
fn run(sim: &SemanticEngine, request: &QueryRequest) -> BTreeMap<(i64, i64), f64> {
    let compiled = sim.compile_query(request).unwrap();
    let out = execute_on_connection(&sim_db(), &compiled.sql, &compiled.params).unwrap();
    let alias = |member: &str| {
        (compiled.columns.iter())
            .find(|c| c.member == member)
            .unwrap_or_else(|| panic!("no column for {member}: {:?}", compiled.columns))
            .alias
            .clone()
    };
    let (c, s, m) = (
        alias("ads.candidate"),
        alias("ads.scenario"),
        alias(&request.measures[0]),
    );
    (out.rows.iter())
        .map(|r| {
            let key = (r[&c].as_i64().unwrap(), r[&s].as_i64().unwrap());
            (key, r[&m].as_f64().unwrap())
        })
        .collect()
}

#[test]
fn a_sum_measure_totals_the_simulated_rows_per_candidate_and_scenario() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let totals = run(&sim, &by_cell("ads.spend"));
    let want: BTreeMap<_, _> = [
        ((0, 0), 11.0),
        ((0, 1), 22.0),
        ((1, 0), 33.0),
        ((1, 1), 44.0),
    ]
    .into();
    assert_eq!(totals, want);
}

#[test]
fn the_compiled_sql_reads_the_override_table_in_the_override_dialect() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let sql = sim.compile_query(&by_cell("ads.spend")).unwrap().sql;
    assert!(sql.contains("sim_ads"), "{sql}");
    assert!(!sql.contains("analytics.ads"), "{sql}");
}

#[test]
fn a_measure_filter_carries_over_to_the_simulated_rows() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let totals = run(&sim, &by_cell("ads.brand_spend"));
    // Cell (1, 1) has no brand row, so it has no brand spend.
    assert_eq!(totals.get(&(0, 0)), Some(&10.0));
    assert_eq!(totals.get(&(1, 0)), Some(&30.0));
    assert!(totals.get(&(1, 1)).is_none_or(|v| *v == 0.0), "{totals:?}");
}

#[test]
fn a_measure_built_from_other_measures_is_their_ratio_per_cell() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let totals = run(&sim, &by_cell("ads.cost_per_click"));
    assert_eq!(totals[&(0, 0)], 11.0 / 6.0);
    assert_eq!(totals[&(1, 1)], 44.0 / 10.0);
}

#[test]
fn a_query_filter_on_a_synthetic_dimension_selects_those_rows() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let request = QueryRequest {
        filters: vec![QueryFilter {
            member: Some("ads.candidate".to_string()),
            operator: Some(airlayer::engine::query::FilterOperator::Equals),
            values: vec!["1".to_string()],
            and: None,
            or: None,
        }],
        ..by_cell("ads.spend")
    };
    let totals = run(&sim, &request);
    assert_eq!(totals.keys().map(|k| k.0).collect::<BTreeSet<_>>(), [1].into());
}

#[test]
fn a_query_that_joins_a_view_with_no_override_is_refused_naming_it() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    let request = QueryRequest {
        dimensions: vec!["products.category".to_string()],
        ..by_cell("ads.spend")
    };
    let err = sim.compile_query(&request).unwrap_err().to_string();
    assert!(err.contains("products"), "{err}");
}

#[test]
fn an_override_of_an_unknown_view_is_refused() {
    let mut o = sim_ads();
    let ads = o.views.remove("ads").unwrap();
    o.views.insert("adz".to_string(), ads);
    let err = engine().with_relations(&o).err().unwrap().to_string();
    assert!(err.contains("adz"), "{err}");
}

#[test]
fn a_synthetic_dimension_that_shadows_a_declared_one_is_refused() {
    let mut o = sim_ads();
    o.views.get_mut("ads").unwrap().dimensions.push("channel".to_string());
    let err = engine().with_relations(&o).err().unwrap().to_string();
    assert!(err.contains("channel"), "{err}");
}

#[test]
fn the_warehouse_engine_is_unchanged_by_deriving_a_simulated_one() {
    let warehouse = engine();
    let request = QueryRequest {
        measures: vec!["ads.spend".to_string()],
        dimensions: vec!["ads.channel".to_string()],
        ..QueryRequest::new()
    };
    let before = warehouse.compile_query(&request).unwrap().sql;
    let _sim = warehouse.with_relations(&sim_ads()).unwrap();
    assert_eq!(warehouse.compile_query(&request).unwrap().sql, before);
    assert!(before.contains("analytics.ads"), "{before}");
}

fn read(request: QueryRequest) -> BTreeMap<String, BTreeSet<String>> {
    engine().columns_read(&request).unwrap()
}

fn cols(view: &str, names: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
    [(view.to_string(), names.iter().map(|n| n.to_string()).collect())].into()
}

#[test]
fn columns_read_names_the_column_a_sum_aggregates() {
    let request = QueryRequest {
        measures: vec!["ads.spend".to_string()],
        ..QueryRequest::new()
    };
    assert_eq!(read(request), cols("ads", &["ad_spend"]));
}

#[test]
fn columns_read_follows_a_measure_into_the_measures_it_is_built_from() {
    let request = QueryRequest {
        measures: vec!["ads.cost_per_click".to_string()],
        ..QueryRequest::new()
    };
    assert_eq!(read(request), cols("ads", &["ad_spend", "clicks"]));
}

#[test]
fn columns_read_includes_a_measure_filter_but_not_its_string_literal() {
    let request = QueryRequest {
        measures: vec!["ads.brand_spend".to_string()],
        ..QueryRequest::new()
    };
    assert_eq!(read(request), cols("ads", &["ad_spend", "channel"]));
}

#[test]
fn columns_read_includes_dimensions_and_reads_nothing_for_a_plain_count() {
    let request = QueryRequest {
        measures: vec!["ads.rows".to_string()],
        dimensions: vec!["ads.channel".to_string(), "products.category".to_string()],
        ..QueryRequest::new()
    };
    let mut want = cols("ads", &["channel"]);
    want.extend(cols("products", &["category"]));
    assert_eq!(read(request), want);
}

#[test]
fn a_member_that_reaches_a_view_with_no_override_is_left_out_and_the_rest_compiles() {
    let sim = engine().with_relations(&sim_ads()).unwrap();
    assert_eq!(run(&sim, &by_cell("ads.spend"))[&(0, 0)], 11.0);
    let err = sim
        .compile_query(&by_cell("ads.category_count"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("category_count"), "{err}");
}
