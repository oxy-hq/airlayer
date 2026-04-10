//! Data profiler — generates type-aware profiling SQL for dimensions.
//!
//! Given a view + dimension, produces SQL that returns summary statistics
//! appropriate to the dimension's type:
//!
//! - **string**: cardinality, distinct values (if low-cardinality), top values by frequency
//! - **number**: min, max, mean, distinct count, null count
//! - **date/datetime**: min, max, null count, span
//! - **boolean**: true/false/null counts

use crate::dialect::Dialect;
use crate::schema::models::{DimensionType, View};
use serde::Serialize;
use serde_json::Value as JsonValue;

/// Maximum cardinality before we stop enumerating individual values.
const CARDINALITY_THRESHOLD: u64 = 100;

/// Maximum number of top values to return for high-cardinality string dimensions.
const TOP_N: u64 = 20;

/// Profile result for a single dimension.
#[derive(Debug, Clone, Serialize)]
pub struct DimensionProfile {
    /// Fully-qualified member path (e.g., "events.platform").
    pub member: String,
    /// Dimension type.
    #[serde(rename = "type")]
    pub dimension_type: String,
    /// Type-specific profile data.
    pub profile: JsonValue,
}

/// Profiling SQL to execute. The profiler generates one or two SQL statements
/// that must be executed in order; the second is conditional on the first's results.
pub struct ProfilePlan {
    /// Stats query — always executed.
    pub stats_sql: String,
    /// If the dimension is a string, this generates the values query.
    /// Called with the cardinality from the stats result to decide whether
    /// to enumerate all values or just top-N.
    pub values_sql_fn: Option<Box<dyn Fn(u64) -> String>>,
}

/// Generate the stats SQL for a dimension.
pub fn plan_profile(
    view: &View,
    dimension_name: &str,
    dialect: &Dialect,
) -> Result<ProfilePlan, String> {
    let dim = view
        .dimensions
        .iter()
        .find(|d| d.name == dimension_name)
        .ok_or_else(|| {
            format!(
                "Dimension '{}' not found in view '{}'",
                dimension_name, view.name
            )
        })?;

    let source = view.source_sql();
    let expr = &dim.expr;

    match dim.dimension_type {
        DimensionType::String => plan_string_profile(&source, expr, dialect),
        DimensionType::Number => plan_number_profile(&source, expr, dialect),
        DimensionType::Date | DimensionType::Datetime => plan_date_profile(&source, expr, dialect),
        DimensionType::Boolean => plan_boolean_profile(&source, expr, dialect),
        DimensionType::Geo => plan_string_profile(&source, expr, dialect), // treat geo like string
    }
}

/// Parse the stats query result into a DimensionProfile.
pub fn build_profile(
    member: &str,
    dimension_type: &DimensionType,
    stats_row: &serde_json::Map<String, JsonValue>,
    values_rows: Option<&[serde_json::Map<String, JsonValue>]>,
) -> DimensionProfile {
    match dimension_type {
        DimensionType::String | DimensionType::Geo => {
            build_string_profile(member, dimension_type, stats_row, values_rows)
        }
        DimensionType::Number => build_number_profile(member, stats_row),
        DimensionType::Date | DimensionType::Datetime => {
            build_date_profile(member, dimension_type, stats_row)
        }
        DimensionType::Boolean => build_boolean_profile(member, stats_row),
    }
}

/// Get the cardinality from a stats row (used to decide whether to enumerate values).
pub fn extract_cardinality(stats_row: &serde_json::Map<String, JsonValue>) -> u64 {
    stats_row
        .get("__cardinality")
        .and_then(json_to_u64)
        .unwrap_or(0)
}

/// Whether we should enumerate values (cardinality is below threshold).
pub fn should_enumerate_values(cardinality: u64) -> bool {
    cardinality <= CARDINALITY_THRESHOLD
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

fn plan_string_profile(
    source: &str,
    expr: &str,
    _dialect: &Dialect,
) -> Result<ProfilePlan, String> {
    let stats_sql = format!(
        "SELECT COUNT(*) AS __total_rows, COUNT(DISTINCT ({expr})) AS __cardinality, \
         SUM(CASE WHEN ({expr}) IS NULL THEN 1 ELSE 0 END) AS __null_count \
         FROM {source}",
        expr = expr,
        source = source,
    );

    let source_owned = source.to_string();
    let expr_owned = expr.to_string();

    let values_sql_fn = Box::new(move |cardinality: u64| {
        if cardinality <= CARDINALITY_THRESHOLD {
            // Enumerate all values with frequency (LIMIT as safety cap against TOCTOU race)
            format!(
                "SELECT ({expr}) AS __value, COUNT(*) AS __frequency \
                 FROM {source} \
                 WHERE ({expr}) IS NOT NULL \
                 GROUP BY ({expr}) \
                 ORDER BY COUNT(*) DESC \
                 LIMIT 200",
                expr = expr_owned,
                source = source_owned,
            )
        } else {
            // Top-N only
            format!(
                "SELECT ({expr}) AS __value, COUNT(*) AS __frequency \
                 FROM {source} \
                 WHERE ({expr}) IS NOT NULL \
                 GROUP BY ({expr}) \
                 ORDER BY COUNT(*) DESC \
                 LIMIT {limit}",
                expr = expr_owned,
                source = source_owned,
                limit = TOP_N,
            )
        }
    });

    Ok(ProfilePlan {
        stats_sql,
        values_sql_fn: Some(values_sql_fn),
    })
}

fn build_string_profile(
    member: &str,
    dimension_type: &DimensionType,
    stats_row: &serde_json::Map<String, JsonValue>,
    values_rows: Option<&[serde_json::Map<String, JsonValue>]>,
) -> DimensionProfile {
    let cardinality = extract_cardinality(stats_row);
    let total_rows = stats_row
        .get("__total_rows")
        .and_then(json_to_u64)
        .unwrap_or(0);
    let null_count = stats_row
        .get("__null_count")
        .and_then(json_to_u64)
        .unwrap_or(0);

    let mut profile = serde_json::json!({
        "cardinality": cardinality,
        "total_rows": total_rows,
        "null_count": null_count,
    });

    if let Some(rows) = values_rows {
        if cardinality <= CARDINALITY_THRESHOLD {
            // All values
            let values: Vec<JsonValue> = rows
                .iter()
                .filter_map(|r| r.get("__value").cloned())
                .collect();
            profile["values"] = JsonValue::Array(values);
        }

        // Top values with frequency (always included)
        let top_values: Vec<JsonValue> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "value": r.get("__value").cloned().unwrap_or(JsonValue::Null),
                    "count": r.get("__frequency").and_then(json_to_u64).unwrap_or(0),
                })
            })
            .collect();
        profile["top_values"] = JsonValue::Array(top_values);
    }

    DimensionProfile {
        member: member.to_string(),
        dimension_type: format!("{}", dimension_type),
        profile,
    }
}

// ---------------------------------------------------------------------------
// Number
// ---------------------------------------------------------------------------

fn plan_number_profile(source: &str, expr: &str, dialect: &Dialect) -> Result<ProfilePlan, String> {
    // AVG needs a float cast; the syntax varies by dialect
    let avg_expr = match dialect {
        Dialect::BigQuery => format!("AVG(CAST(({}) AS FLOAT64))", expr),
        Dialect::MySQL | Dialect::Domo => format!("AVG(({}))", expr), // MySQL auto-casts
        Dialect::ClickHouse => format!("AVG(toFloat64({}))", expr),
        _ => format!("AVG(CAST(({}) AS DOUBLE PRECISION))", expr),
    };

    let stats_sql = format!(
        "SELECT \
         COUNT(*) AS __total_rows, \
         COUNT(DISTINCT ({expr})) AS __cardinality, \
         SUM(CASE WHEN ({expr}) IS NULL THEN 1 ELSE 0 END) AS __null_count, \
         MIN(({expr})) AS __min, \
         MAX(({expr})) AS __max, \
         {avg_expr} AS __mean \
         FROM {source}",
        expr = expr,
        source = source,
        avg_expr = avg_expr,
    );

    Ok(ProfilePlan {
        stats_sql,
        values_sql_fn: None,
    })
}

fn build_number_profile(
    member: &str,
    stats_row: &serde_json::Map<String, JsonValue>,
) -> DimensionProfile {
    let profile = serde_json::json!({
        "total_rows": stats_row.get("__total_rows").and_then(json_to_u64).unwrap_or(0),
        "distinct_count": stats_row.get("__cardinality").and_then(json_to_u64).unwrap_or(0),
        "null_count": stats_row.get("__null_count").and_then(json_to_u64).unwrap_or(0),
        "min": stats_row.get("__min").cloned().unwrap_or(JsonValue::Null),
        "max": stats_row.get("__max").cloned().unwrap_or(JsonValue::Null),
        "mean": stats_row.get("__mean").cloned().unwrap_or(JsonValue::Null),
    });

    DimensionProfile {
        member: member.to_string(),
        dimension_type: "number".to_string(),
        profile,
    }
}

// ---------------------------------------------------------------------------
// Date / Datetime
// ---------------------------------------------------------------------------

fn plan_date_profile(source: &str, expr: &str, _dialect: &Dialect) -> Result<ProfilePlan, String> {
    let stats_sql = format!(
        "SELECT \
         COUNT(*) AS __total_rows, \
         SUM(CASE WHEN ({expr}) IS NULL THEN 1 ELSE 0 END) AS __null_count, \
         MIN(({expr})) AS __min, \
         MAX(({expr})) AS __max \
         FROM {source}",
        expr = expr,
        source = source,
    );

    Ok(ProfilePlan {
        stats_sql,
        values_sql_fn: None,
    })
}

fn build_date_profile(
    member: &str,
    dimension_type: &DimensionType,
    stats_row: &serde_json::Map<String, JsonValue>,
) -> DimensionProfile {
    let profile = serde_json::json!({
        "total_rows": stats_row.get("__total_rows").and_then(json_to_u64).unwrap_or(0),
        "null_count": stats_row.get("__null_count").and_then(json_to_u64).unwrap_or(0),
        "min": stats_row.get("__min").cloned().unwrap_or(JsonValue::Null),
        "max": stats_row.get("__max").cloned().unwrap_or(JsonValue::Null),
    });

    DimensionProfile {
        member: member.to_string(),
        dimension_type: format!("{}", dimension_type),
        profile,
    }
}

// ---------------------------------------------------------------------------
// Boolean
// ---------------------------------------------------------------------------

fn plan_boolean_profile(
    source: &str,
    expr: &str,
    _dialect: &Dialect,
) -> Result<ProfilePlan, String> {
    let stats_sql = format!(
        "SELECT \
         COUNT(*) AS __total_rows, \
         SUM(CASE WHEN ({expr}) IS NULL THEN 1 ELSE 0 END) AS __null_count, \
         SUM(CASE WHEN ({expr}) = true THEN 1 ELSE 0 END) AS __true_count, \
         SUM(CASE WHEN ({expr}) = false THEN 1 ELSE 0 END) AS __false_count \
         FROM {source}",
        expr = expr,
        source = source,
    );

    Ok(ProfilePlan {
        stats_sql,
        values_sql_fn: None,
    })
}

fn build_boolean_profile(
    member: &str,
    stats_row: &serde_json::Map<String, JsonValue>,
) -> DimensionProfile {
    let profile = serde_json::json!({
        "total_rows": stats_row.get("__total_rows").and_then(json_to_u64).unwrap_or(0),
        "null_count": stats_row.get("__null_count").and_then(json_to_u64).unwrap_or(0),
        "true_count": stats_row.get("__true_count").and_then(json_to_u64).unwrap_or(0),
        "false_count": stats_row.get("__false_count").and_then(json_to_u64).unwrap_or(0),
    });

    DimensionProfile {
        member: member.to_string(),
        dimension_type: "boolean".to_string(),
        profile,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Coerce a JSON value to u64 (handles strings, ints, floats).
fn json_to_u64(val: &JsonValue) -> Option<u64> {
    match val {
        JsonValue::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().filter(|&i| i >= 0).map(|i| i as u64))
            .or_else(|| n.as_f64().filter(|&f| f >= 0.0).map(|f| f as u64)),
        JsonValue::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::{Dimension, DimensionType, Entity, EntityType, View};

    fn test_view() -> View {
        View {
            name: "events".to_string(),
            description: Some("Test events".to_string()),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("analytics.events".to_string()),
            sql: None,
            entities: vec![Entity {
                name: "event".to_string(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("event_id".to_string()),
                keys: None,
                inherits_from: None,
                meta: None,
            }],
            dimensions: vec![
                Dimension {
                    name: "platform".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "platform".to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                    meta: None,
                },
                Dimension {
                    name: "revenue".to_string(),
                    dimension_type: DimensionType::Number,
                    description: None,
                    expr: "revenue_cents / 100.0".to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                    meta: None,
                },
                Dimension {
                    name: "created_at".to_string(),
                    dimension_type: DimensionType::Datetime,
                    description: None,
                    expr: "created_at".to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                    meta: None,
                },
            ],
            measures: None,
            segments: vec![],
            meta: None,
        }
    }

    #[test]
    fn string_profile_sql_contains_cardinality() {
        let view = test_view();
        let plan = plan_profile(&view, "platform", &Dialect::Postgres).unwrap();
        assert!(plan.stats_sql.contains("COUNT(DISTINCT"));
        assert!(plan.stats_sql.contains("__cardinality"));
        assert!(plan.stats_sql.contains("__null_count"));
        assert!(plan.values_sql_fn.is_some());
    }

    #[test]
    fn number_profile_sql_contains_min_max() {
        let view = test_view();
        let plan = plan_profile(&view, "revenue", &Dialect::Postgres).unwrap();
        assert!(plan.stats_sql.contains("MIN("));
        assert!(plan.stats_sql.contains("MAX("));
        assert!(plan.stats_sql.contains("AVG("));
        assert!(plan.stats_sql.contains("__mean"));
        assert!(plan.values_sql_fn.is_none());
    }

    #[test]
    fn number_profile_bigquery_uses_float64() {
        let view = test_view();
        let plan = plan_profile(&view, "revenue", &Dialect::BigQuery).unwrap();
        assert!(
            plan.stats_sql.contains("FLOAT64"),
            "BigQuery should use FLOAT64 cast, got: {}",
            plan.stats_sql
        );
    }

    #[test]
    fn date_profile_sql_contains_min_max() {
        let view = test_view();
        let plan = plan_profile(&view, "created_at", &Dialect::Postgres).unwrap();
        assert!(plan.stats_sql.contains("MIN("));
        assert!(plan.stats_sql.contains("MAX("));
        assert!(!plan.stats_sql.contains("AVG("));
        assert!(plan.values_sql_fn.is_none());
    }

    #[test]
    fn string_values_sql_low_cardinality_has_safety_limit() {
        let view = test_view();
        let plan = plan_profile(&view, "platform", &Dialect::Postgres).unwrap();
        let values_fn = plan.values_sql_fn.unwrap();
        let sql = values_fn(5); // low cardinality
        assert!(
            sql.contains("LIMIT 200"),
            "Low-cardinality should have safety LIMIT 200"
        );
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn string_values_sql_high_cardinality_has_limit() {
        let view = test_view();
        let plan = plan_profile(&view, "platform", &Dialect::Postgres).unwrap();
        let values_fn = plan.values_sql_fn.unwrap();
        let sql = values_fn(500); // high cardinality
        assert!(
            sql.contains("LIMIT 20"),
            "High-cardinality should have LIMIT 20"
        );
    }

    #[test]
    fn dimension_not_found_returns_error() {
        let view = test_view();
        let result = plan_profile(&view, "nonexistent", &Dialect::Postgres);
        match result {
            Err(msg) => assert!(
                msg.contains("not found"),
                "Error should mention 'not found': {}",
                msg
            ),
            Ok(_) => panic!("Expected error for nonexistent dimension"),
        }
    }

    #[test]
    fn build_string_profile_with_values() {
        let stats = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
            r#"{"__total_rows": 12, "__cardinality": 3, "__null_count": 0}"#,
        )
        .unwrap();
        let values = vec![
            serde_json::from_str::<serde_json::Map<String, JsonValue>>(
                r#"{"__value": "web", "__frequency": 7}"#,
            )
            .unwrap(),
            serde_json::from_str::<serde_json::Map<String, JsonValue>>(
                r#"{"__value": "ios", "__frequency": 3}"#,
            )
            .unwrap(),
        ];

        let profile = build_profile(
            "events.platform",
            &DimensionType::String,
            &stats,
            Some(&values),
        );
        assert_eq!(profile.member, "events.platform");
        assert_eq!(profile.dimension_type, "string");
        assert_eq!(profile.profile["cardinality"], 3);
        assert_eq!(profile.profile["values"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn build_number_profile_from_stats() {
        let stats = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
            r#"{"__total_rows": 12, "__cardinality": 5, "__null_count": 0, "__min": 0, "__max": 99.99, "__mean": 15.83}"#,
        ).unwrap();

        let profile = build_profile("events.revenue", &DimensionType::Number, &stats, None);
        assert_eq!(profile.dimension_type, "number");
        assert_eq!(profile.profile["distinct_count"], 5);
        assert_eq!(profile.profile["min"], 0);
    }
}
