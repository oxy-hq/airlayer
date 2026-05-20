//! LookML parity tests.
//!
//! These tests convert real-world LookML files (inspired by Looker's open-source
//! blocks: block-jira-new, block-google-ads, healthcare_demo, block-sales) to
//! airlayer views, compile queries, and verify the SQL is structurally correct.
//!
//! Patterns validated:
//!   - `@{CONSTANT}` in sql_table_name (preserved as-is)
//!   - `link:` blocks inside dimensions (skipped, don't break parsing)
//!   - `filters:` on measures (block syntax with field/value)
//!   - `#` comments on same line as code
//!   - `dimension_group` with many timeframes (hour_of_day, day_of_week, etc.)
//!   - `hidden: yes`, `value_format_name:`, `group_label:` (skipped gracefully)
//!   - `set:` blocks (skipped gracefully)
//!   - `type: running_total` → Sum measure
//!   - `type: yesno` → Boolean dimension
//!   - `type: zipcode` → String dimension
//!   - `type: count_distinct` → CountDistinct measure
//!   - `type: date_raw` → Datetime dimension
//!   - Cross-view `${field}` refs without TABLE prefix
//!   - `CASE WHEN` in sql expressions
//!   - Explore with many joins + relationship types
//!   - `dimension_group` type: duration with intervals
//!   - CONCAT in dimension sql
//!   - Measure filters (filtered measures)

use airlayer::dialect::Dialect;
use airlayer::engine::query::*;
use airlayer::engine::{DatasourceDialectMap, SemanticEngine};
use airlayer::schema::foreign;
use airlayer::schema::models::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_lookml_engine(fixture_dir: &str) -> SemanticEngine {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contrib")
        .join(fixture_dir);

    let result = foreign::convert_directory(foreign::ForeignFormat::LookML, &dir)
        .unwrap_or_else(|e| panic!("Failed to convert LookML from {}: {}", fixture_dir, e));

    assert!(
        !result.views.is_empty(),
        "Should have converted at least one view from {}",
        fixture_dir
    );

    let layer = SemanticLayer::new(result.views, None);
    let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
    SemanticEngine::from_semantic_layer(layer, dialects)
        .unwrap_or_else(|e| panic!("Failed to build engine from {}: {}", fixture_dir, e))
}

// ---------------------------------------------------------------------------
// Jira block tests
// ---------------------------------------------------------------------------

#[test]
fn lookml_jira_conversion_and_view_count() {
    let engine = load_lookml_engine("jira-lookml");
    assert!(
        engine.views().len() >= 4,
        "Jira should have at least 4 views, got {}",
        engine.views().len()
    );

    // Verify specific views exist
    assert!(engine.view("issue").is_some(), "Should have issue view");
    assert!(
        engine.view("status_category").is_some(),
        "Should have status_category view"
    );
    assert!(
        engine.view("priority").is_some(),
        "Should have priority view"
    );
    assert!(engine.view("project").is_some(), "Should have project view");
}

#[test]
fn lookml_jira_dimension_group_timeframes() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();

    // dimension_group: _fivetran_synced with 7 timeframes should expand
    let synced_dims: Vec<&Dimension> = issue
        .dimensions
        .iter()
        .filter(|d| d.name.starts_with("_fivetran_synced_"))
        .collect();
    assert_eq!(
        synced_dims.len(),
        7,
        "Should have 7 timeframe dims for _fivetran_synced, got: {:?}",
        synced_dims.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
}

#[test]
fn lookml_jira_yesno_dimension() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();

    let needs_triage = issue
        .dimensions
        .iter()
        .find(|d| d.name == "needs_triage")
        .expect("Should have needs_triage dimension");
    assert_eq!(needs_triage.dimension_type, DimensionType::Boolean);
    assert!(
        needs_triage.expr.contains("CASE WHEN"),
        "Should contain CASE WHEN, got: {}",
        needs_triage.expr
    );
}

#[test]
fn lookml_jira_measure_filters() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();
    let measures = issue.measures_list();

    let open_issues = measures
        .iter()
        .find(|m| m.name == "number_of_open_issues")
        .expect("Should have number_of_open_issues measure");
    assert_eq!(open_issues.measure_type, MeasureType::Count);
    assert!(
        open_issues.filters.is_some(),
        "Should have filters on number_of_open_issues"
    );
}

#[test]
fn lookml_jira_explore_joins() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();

    // Explore should add foreign entities to the issue view
    let entity_names: Vec<&str> = issue.entities.iter().map(|e| e.name.as_str()).collect();
    assert!(
        entity_names.contains(&"project"),
        "Should have project entity, got: {:?}",
        entity_names
    );
    assert!(
        entity_names.contains(&"priority"),
        "Should have priority entity, got: {:?}",
        entity_names
    );
    assert!(
        entity_names.contains(&"status"),
        "Should have status entity, got: {:?}",
        entity_names
    );
    assert!(
        entity_names.contains(&"status_category"),
        "Should have status_category entity, got: {:?}",
        entity_names
    );
}

#[test]
fn lookml_jira_compile_basic_query() {
    let engine = load_lookml_engine("jira-lookml");

    let request = QueryRequest {
        measures: vec!["issue.count".to_string()],
        dimensions: vec!["issue.status".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(result.sql.contains("SELECT"), "SQL should contain SELECT");
    assert!(
        result.sql.contains("COUNT"),
        "SQL should contain COUNT for count measure"
    );
}

#[test]
fn lookml_jira_compile_cross_view_query() {
    let engine = load_lookml_engine("jira-lookml");

    // Query that spans issue and priority views (via entity join)
    let request = QueryRequest {
        measures: vec!["issue.count".to_string()],
        dimensions: vec!["priority.name".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(result.sql.contains("SELECT"), "SQL should contain SELECT");
    assert!(
        result.sql.contains("JOIN"),
        "SQL should contain JOIN for cross-view query"
    );
}

#[test]
fn lookml_jira_at_constant_in_table_name() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();

    // @{SCHEMA_NAME}.issue should be preserved as the table name
    assert_eq!(
        issue.table.as_deref(),
        Some("@{SCHEMA_NAME}.issue"),
        "Table name should preserve @{{CONSTANT}} syntax"
    );
}

#[test]
fn lookml_jira_comment_on_code_line() {
    let engine = load_lookml_engine("jira-lookml");
    let issue = engine.view("issue").unwrap();

    // `#hidden: yes` is a comment — the status dimension should still exist
    let status = issue
        .dimensions
        .iter()
        .find(|d| d.name == "status")
        .expect("Should have status dimension despite # comment on preceding line");
    assert_eq!(status.dimension_type, DimensionType::Number);
}

// ---------------------------------------------------------------------------
// Google Ads block tests
// ---------------------------------------------------------------------------

#[test]
fn lookml_google_ads_conversion_and_view_count() {
    let engine = load_lookml_engine("google-ads-lookml");
    assert!(
        engine.views().len() >= 3,
        "Google Ads should have at least 3 views, got {}",
        engine.views().len()
    );
}

#[test]
fn lookml_google_ads_running_total_measure() {
    let engine = load_lookml_engine("google-ads-lookml");
    let base = engine
        .view("ad_metrics_base")
        .expect("Should have ad_metrics_base view");

    let measures = base.measures_list();
    let cumulative = measures
        .iter()
        .find(|m| m.name == "cumulative_spend")
        .expect("Should have cumulative_spend measure");
    // running_total maps to Sum in airlayer
    assert_eq!(cumulative.measure_type, MeasureType::Sum);
}

#[test]
fn lookml_google_ads_count_distinct_measure() {
    let engine = load_lookml_engine("google-ads-lookml");
    let campaign = engine.view("campaign").expect("Should have campaign view");

    let measures = campaign.measures_list();
    let count = measures
        .iter()
        .find(|m| m.name == "count")
        .expect("Should have count measure");
    assert_eq!(count.measure_type, MeasureType::CountDistinct);
}

#[test]
fn lookml_google_ads_date_raw_dimension() {
    let engine = load_lookml_engine("google-ads-lookml");
    let impressions = engine
        .view("ad_impressions")
        .expect("Should have ad_impressions view");

    let date_dim = impressions
        .dimensions
        .iter()
        .find(|d| d.name == "_date")
        .expect("Should have _date dimension");
    // date_raw maps to Datetime
    assert_eq!(date_dim.dimension_type, DimensionType::Datetime);
}

#[test]
fn lookml_google_ads_dimension_group_many_timeframes() {
    let engine = load_lookml_engine("google-ads-lookml");
    let impressions = engine
        .view("ad_impressions")
        .expect("Should have ad_impressions view");

    // dimension_group: date with 10 timeframes
    let date_dims: Vec<&Dimension> = impressions
        .dimensions
        .iter()
        .filter(|d| d.name.starts_with("date_"))
        .collect();
    assert_eq!(
        date_dims.len(),
        10,
        "Should have 10 date timeframe dims, got: {:?}",
        date_dims.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
}

#[test]
fn lookml_google_ads_cross_ref_dimension() {
    let engine = load_lookml_engine("google-ads-lookml");
    let base = engine
        .view("ad_metrics_base")
        .expect("Should have ad_metrics_base view");

    // click_rate uses ${clicks} and ${impressions} — cross-field refs within same view
    let click_rate = base
        .dimensions
        .iter()
        .find(|d| d.name == "click_rate")
        .expect("Should have click_rate dimension");
    assert!(
        click_rate.expr.contains("clicks"),
        "click_rate expr should reference clicks, got: {}",
        click_rate.expr
    );
}

#[test]
fn lookml_google_ads_compile_basic_query() {
    let engine = load_lookml_engine("google-ads-lookml");

    let request = QueryRequest {
        measures: vec!["ad_impressions.total_impressions".to_string()],
        dimensions: vec!["ad_impressions.ad_network_type1".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(result.sql.contains("SELECT"), "SQL should contain SELECT");
    assert!(
        result.sql.contains("SUM"),
        "SQL should contain SUM for total_impressions"
    );
}

#[test]
fn lookml_google_ads_compile_cross_view_query() {
    let engine = load_lookml_engine("google-ads-lookml");

    let request = QueryRequest {
        measures: vec!["ad_impressions.total_clicks".to_string()],
        dimensions: vec!["campaign.name".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(
        result.sql.contains("JOIN"),
        "SQL should contain JOIN for campaign lookup.\nSQL: {}",
        result.sql
    );
}

// ---------------------------------------------------------------------------
// Healthcare demo tests
// ---------------------------------------------------------------------------

#[test]
fn lookml_healthcare_conversion_and_view_count() {
    let engine = load_lookml_engine("healthcare-lookml");
    assert!(
        engine.views().len() >= 3,
        "Healthcare should have at least 3 views, got {}",
        engine.views().len()
    );
}

#[test]
fn lookml_healthcare_yesno_dimension() {
    let engine = load_lookml_engine("healthcare-lookml");
    let patient = engine.view("patient").expect("Should have patient view");

    let is_deceased = patient
        .dimensions
        .iter()
        .find(|d| d.name == "is_deceased")
        .expect("Should have is_deceased dimension");
    assert_eq!(is_deceased.dimension_type, DimensionType::Boolean);
}

#[test]
fn lookml_healthcare_zipcode_dimension() {
    let engine = load_lookml_engine("healthcare-lookml");
    let patient = engine.view("patient").expect("Should have patient view");

    let zip = patient
        .dimensions
        .iter()
        .find(|d| d.name == "zip")
        .expect("Should have zip dimension");
    // zipcode maps to String
    assert_eq!(zip.dimension_type, DimensionType::String);
}

#[test]
fn lookml_healthcare_duration_dimension_group() {
    let engine = load_lookml_engine("healthcare-lookml");
    let encounter = engine
        .view("encounter")
        .expect("Should have encounter view");

    // dimension_group: length_of_stay type: duration with intervals [day, hour, minute]
    let los_dims: Vec<&Dimension> = encounter
        .dimensions
        .iter()
        .filter(|d| d.name.starts_with("length_of_stay_"))
        .collect();
    assert_eq!(
        los_dims.len(),
        3,
        "Should have 3 duration interval dims, got: {:?}",
        los_dims.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
    // Duration intervals produce Number type
    for d in &los_dims {
        assert_eq!(
            d.dimension_type,
            DimensionType::Number,
            "Duration dimension {} should be Number type",
            d.name
        );
    }
}

#[test]
fn lookml_healthcare_measure_types() {
    let engine = load_lookml_engine("healthcare-lookml");
    let encounter = engine
        .view("encounter")
        .expect("Should have encounter view");
    let measures = encounter.measures_list();

    let total_cost = measures
        .iter()
        .find(|m| m.name == "total_cost")
        .expect("Should have total_cost");
    assert_eq!(total_cost.measure_type, MeasureType::Sum);

    let avg_cost = measures
        .iter()
        .find(|m| m.name == "average_cost")
        .expect("Should have average_cost");
    assert_eq!(avg_cost.measure_type, MeasureType::Average);

    let distinct_patients = measures
        .iter()
        .find(|m| m.name == "count_distinct_patients")
        .expect("Should have count_distinct_patients");
    assert_eq!(distinct_patients.measure_type, MeasureType::CountDistinct);
}

#[test]
fn lookml_healthcare_filtered_measures() {
    let engine = load_lookml_engine("healthcare-lookml");
    let patient = engine.view("patient").expect("Should have patient view");
    let measures = patient.measures_list();

    let count_female = measures
        .iter()
        .find(|m| m.name == "count_female")
        .expect("Should have count_female measure");
    assert!(
        count_female.filters.is_some(),
        "count_female should have filters"
    );
}

#[test]
fn lookml_healthcare_explore_joins() {
    let engine = load_lookml_engine("healthcare-lookml");
    let encounter = engine
        .view("encounter")
        .expect("Should have encounter view");

    let entity_names: Vec<&str> = encounter.entities.iter().map(|e| e.name.as_str()).collect();
    assert!(
        entity_names.contains(&"patient"),
        "Should have patient entity, got: {:?}",
        entity_names
    );
    assert!(
        entity_names.contains(&"observation_vitals"),
        "Should have observation_vitals entity, got: {:?}",
        entity_names
    );
}

#[test]
fn lookml_healthcare_compile_encounter_by_class() {
    let engine = load_lookml_engine("healthcare-lookml");

    let request = QueryRequest {
        measures: vec!["encounter.count".to_string()],
        dimensions: vec!["encounter.encounter_class".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(result.sql.contains("SELECT"));
    assert!(result.sql.contains("COUNT"));
}

#[test]
fn lookml_healthcare_compile_cross_view_patient_encounter() {
    let engine = load_lookml_engine("healthcare-lookml");

    let request = QueryRequest {
        measures: vec!["encounter.count".to_string()],
        dimensions: vec!["patient.gender".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(
        result.sql.contains("JOIN"),
        "Should JOIN patient table.\nSQL: {}",
        result.sql
    );
}

// ---------------------------------------------------------------------------
// Sales block tests
// ---------------------------------------------------------------------------

#[test]
fn lookml_sales_conversion_and_view_count() {
    let engine = load_lookml_engine("sales-lookml");
    assert!(
        engine.views().len() >= 2,
        "Sales should have at least 2 views, got {}",
        engine.views().len()
    );
}

#[test]
fn lookml_sales_dimension_group_time() {
    let engine = load_lookml_engine("sales-lookml");
    let opportunity = engine
        .view("opportunity")
        .expect("Should have opportunity view");

    // dimension_group: close with 6 timeframes
    let close_dims: Vec<&Dimension> = opportunity
        .dimensions
        .iter()
        .filter(|d| d.name.starts_with("close_"))
        .collect();
    assert_eq!(
        close_dims.len(),
        6,
        "Should have 6 close timeframe dims, got: {:?}",
        close_dims.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
}

#[test]
fn lookml_sales_yesno_dimensions() {
    let engine = load_lookml_engine("sales-lookml");
    let opportunity = engine
        .view("opportunity")
        .expect("Should have opportunity view");

    let is_won = opportunity
        .dimensions
        .iter()
        .find(|d| d.name == "is_won")
        .expect("Should have is_won dimension");
    assert_eq!(is_won.dimension_type, DimensionType::Boolean);

    let is_closed = opportunity
        .dimensions
        .iter()
        .find(|d| d.name == "is_closed")
        .expect("Should have is_closed dimension");
    assert_eq!(is_closed.dimension_type, DimensionType::Boolean);
}

#[test]
fn lookml_sales_filtered_measure() {
    let engine = load_lookml_engine("sales-lookml");
    let opportunity = engine
        .view("opportunity")
        .expect("Should have opportunity view");
    let measures = opportunity.measures_list();

    let win_count = measures
        .iter()
        .find(|m| m.name == "win_count")
        .expect("Should have win_count measure");
    assert!(win_count.filters.is_some(), "win_count should have filters");

    let total_pipeline = measures
        .iter()
        .find(|m| m.name == "total_pipeline")
        .expect("Should have total_pipeline measure");
    assert!(
        total_pipeline.filters.is_some(),
        "total_pipeline should have filters"
    );
}

#[test]
fn lookml_sales_explore_joins() {
    let engine = load_lookml_engine("sales-lookml");
    let opportunity = engine
        .view("opportunity")
        .expect("Should have opportunity view");

    let entity_names: Vec<&str> = opportunity
        .entities
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        entity_names.contains(&"account"),
        "Should have account entity, got: {:?}",
        entity_names
    );
}

#[test]
fn lookml_sales_compile_pipeline_query() {
    let engine = load_lookml_engine("sales-lookml");

    let request = QueryRequest {
        measures: vec![
            "opportunity.total_amount".to_string(),
            "opportunity.count".to_string(),
        ],
        dimensions: vec!["opportunity.stage_name".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(result.sql.contains("SELECT"));
    assert!(result.sql.contains("SUM"));
    assert!(result.sql.contains("COUNT"));
}

#[test]
fn lookml_sales_compile_cross_view_query() {
    let engine = load_lookml_engine("sales-lookml");

    let request = QueryRequest {
        measures: vec!["opportunity.total_amount".to_string()],
        dimensions: vec!["account.industry".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();
    assert!(
        result.sql.contains("JOIN"),
        "Should JOIN account table.\nSQL: {}",
        result.sql
    );
}

// ---------------------------------------------------------------------------
// SQL correctness checks
// ---------------------------------------------------------------------------

#[test]
fn lookml_jira_sql_structure_correctness() {
    let engine = load_lookml_engine("jira-lookml");

    // Simple count by status
    let request = QueryRequest {
        measures: vec!["issue.count".to_string()],
        dimensions: vec!["issue.key".to_string()],
        ..QueryRequest::new()
    };
    let result = engine.compile_query(&request).unwrap();

    // SQL should reference the table name (with @{CONSTANT})
    assert!(
        result.sql.contains("issue"),
        "SQL should reference issue table.\nSQL: {}",
        result.sql
    );
    // Should have GROUP BY
    assert!(
        result.sql.contains("GROUP BY"),
        "SQL should have GROUP BY.\nSQL: {}",
        result.sql
    );
}

#[test]
fn lookml_healthcare_sql_join_correctness() {
    let engine = load_lookml_engine("healthcare-lookml");

    let request = QueryRequest {
        measures: vec![
            "encounter.count".to_string(),
            "encounter.total_cost".to_string(),
        ],
        dimensions: vec!["patient.gender".to_string(), "patient.city".to_string()],
        ..QueryRequest::new()
    };
    let result = engine.compile_query(&request).unwrap();

    // Should have JOIN and GROUP BY with multiple dimensions
    assert!(
        result.sql.contains("JOIN"),
        "SQL should JOIN patient.\nSQL: {}",
        result.sql
    );
    assert!(
        result.sql.contains("GROUP BY"),
        "SQL should have GROUP BY.\nSQL: {}",
        result.sql
    );
    assert!(
        result.sql.contains("SUM"),
        "SQL should have SUM for total_cost.\nSQL: {}",
        result.sql
    );
}

#[test]
fn lookml_google_ads_sql_number_measure() {
    let engine = load_lookml_engine("google-ads-lookml");

    // average_click_rate is type: number with sql referencing other measures
    let request = QueryRequest {
        measures: vec!["ad_impressions.average_click_rate".to_string()],
        dimensions: vec![],
        ..QueryRequest::new()
    };
    let result = engine.compile_query(&request).unwrap();

    assert!(
        result.sql.contains("SELECT"),
        "SQL should contain SELECT.\nSQL: {}",
        result.sql
    );
}

// ---------------------------------------------------------------------------
// Bulk compilation tests — verify all views compile without errors
// ---------------------------------------------------------------------------

#[test]
fn lookml_bulk_compile_all_views() {
    let fixtures = vec![
        "jira-lookml",
        "google-ads-lookml",
        "healthcare-lookml",
        "sales-lookml",
    ];
    let mut total_views = 0;
    let mut total_queries = 0;

    for fixture in &fixtures {
        let engine = load_lookml_engine(fixture);

        for view in engine.views() {
            total_views += 1;

            // Try to compile a dimension-only query for each view
            if !view.dimensions.is_empty() {
                let first_dim = &view.dimensions[0];
                let request = QueryRequest {
                    dimensions: vec![format!("{}.{}", view.name, first_dim.name)],
                    ..QueryRequest::new()
                };
                let result = engine.compile_query(&request);
                assert!(
                    result.is_ok(),
                    "Failed to compile dimension query for {}.{}: {:?}",
                    view.name,
                    first_dim.name,
                    result.err()
                );
                total_queries += 1;
            }

            // Try to compile a measure query for each view
            let measures = view.measures_list();
            if !measures.is_empty() {
                let first_measure = &measures[0];
                let request = QueryRequest {
                    measures: vec![format!("{}.{}", view.name, first_measure.name)],
                    ..QueryRequest::new()
                };
                let result = engine.compile_query(&request);
                assert!(
                    result.is_ok(),
                    "Failed to compile measure query for {}.{}: {:?}",
                    view.name,
                    first_measure.name,
                    result.err()
                );
                total_queries += 1;
            }
        }
    }

    assert!(
        total_views >= 10,
        "Should have tested at least 10 views, got {}",
        total_views
    );
    assert!(
        total_queries >= 15,
        "Should have compiled at least 15 queries, got {}",
        total_queries
    );
}

/// Verify SQL correctness across all fixtures by inspecting compiled SQL.
#[test]
fn lookml_sql_quality_check() {
    let test_cases: Vec<(&str, QueryRequest, Vec<&str>)> = vec![
        // (fixture, query, expected_sql_fragments)
        (
            "healthcare-lookml",
            QueryRequest {
                measures: vec![
                    "encounter.count".to_string(),
                    "encounter.total_cost".to_string(),
                ],
                dimensions: vec!["encounter.encounter_class".to_string()],
                ..QueryRequest::new()
            },
            vec![
                "SELECT",
                "COUNT",
                "SUM",
                "GROUP BY",
                "encounter_class",
                "total_claim_cost",
            ],
        ),
        (
            "jira-lookml",
            QueryRequest {
                measures: vec!["issue.count".to_string()],
                dimensions: vec!["priority.name".to_string()],
                ..QueryRequest::new()
            },
            vec!["SELECT", "COUNT", "JOIN", "GROUP BY"],
        ),
        (
            "sales-lookml",
            QueryRequest {
                measures: vec!["opportunity.total_amount".to_string()],
                dimensions: vec!["account.industry".to_string()],
                ..QueryRequest::new()
            },
            vec!["SELECT", "SUM", "JOIN", "GROUP BY", "industry"],
        ),
        (
            "google-ads-lookml",
            QueryRequest {
                measures: vec![
                    "ad_impressions.total_impressions".to_string(),
                    "ad_impressions.total_clicks".to_string(),
                ],
                dimensions: vec!["ad_impressions.ad_network_type1".to_string()],
                ..QueryRequest::new()
            },
            vec!["SELECT", "SUM", "GROUP BY", "ad_network_type1"],
        ),
    ];

    for (fixture, request, expected_fragments) in &test_cases {
        let engine = load_lookml_engine(fixture);
        let result = engine.compile_query(request).unwrap();

        for fragment in expected_fragments {
            assert!(
                result.sql.contains(fragment),
                "[{}] SQL should contain '{}'\nSQL: {}",
                fixture,
                fragment,
                result.sql
            );
        }
    }
}

/// Test that the SQL produced for a 2-view cross-entity join has proper structure.
#[test]
fn lookml_sql_join_structure() {
    let engine = load_lookml_engine("healthcare-lookml");

    // 2-view join: encounter → patient
    let request = QueryRequest {
        measures: vec!["encounter.total_cost".to_string()],
        dimensions: vec!["patient.gender".to_string()],
        ..QueryRequest::new()
    };

    let result = engine.compile_query(&request).unwrap();

    let join_count = result.sql.matches("JOIN").count();
    assert!(
        join_count >= 1,
        "Should have at least 1 JOIN for 2-view query, got {}.\nSQL: {}",
        join_count,
        result.sql
    );
    assert!(
        result.sql.contains("patient"),
        "SQL should reference patient table.\nSQL: {}",
        result.sql
    );
}
