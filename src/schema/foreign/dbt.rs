//! dbt MetricFlow semantic model parser.
//!
//! Converts dbt semantic_models and metrics into airlayer views. Supports:
//! - Semantic models: entities (primary, unique, foreign, natural), dimensions, measures
//! - Dimension types: categorical, time
//! - Measure aggregations: sum, count, count_distinct, average, min, max, median,
//!   sum_boolean, percentile
//! - Metrics: simple, derived, cumulative, ratio
//! - Entity-based joins (primary/foreign key relationships)
//! - Time dimensions with granularity
//! - Measure filters (where clauses)
//! - model: ref('model_name') references

use super::{ConversionResult, parse_foreign_dimension_type, parse_foreign_measure_type,
            rewrite_dbt_jinja, RE_DBT_REF, RE_DBT_SOURCE};
use crate::schema::models::*;
use serde::Deserialize;

// ── dbt MetricFlow native types ──────────────────────────────────────

/// Top-level dbt semantic layer file.
#[derive(Debug, Deserialize)]
struct DbtFile {
    #[serde(default)]
    semantic_models: Vec<DbtSemanticModel>,
    #[serde(default)]
    metrics: Vec<DbtMetric>,
}

/// A dbt semantic model — the core modeling unit.
#[derive(Debug, Deserialize)]
struct DbtSemanticModel {
    name: String,
    #[serde(default)]
    description: Option<String>,
    /// Model reference: `ref('model_name')` or a table name.
    #[serde(default)]
    model: Option<String>,
    /// Explicit node relation (alternative to model).
    #[serde(default)]
    node_relation: Option<DbtNodeRelation>,
    #[serde(default)]
    defaults: Option<DbtDefaults>,
    #[serde(default)]
    entities: Vec<DbtEntity>,
    #[serde(default)]
    dimensions: Vec<DbtDimension>,
    #[serde(default)]
    measures: Vec<DbtMeasure>,
    #[serde(default)]
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DbtNodeRelation {
    #[serde(default)]
    schema_name: Option<String>,
    #[serde(default, alias = "alias")]
    relation_name: Option<String>,
    #[serde(default)]
    database: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DbtDefaults {
    #[serde(default)]
    agg_time_dimension: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DbtEntity {
    name: String,
    #[serde(rename = "type")]
    entity_type: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    expr: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DbtDimension {
    name: String,
    #[serde(rename = "type")]
    dim_type: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    expr: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    type_params: Option<DbtDimensionTypeParams>,
    #[serde(default)]
    is_partition: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DbtDimensionTypeParams {
    #[serde(default)]
    time_granularity: Option<String>,
    #[serde(default)]
    validity_params: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct DbtMeasure {
    name: String,
    /// Aggregation type: sum, count, count_distinct, average, min, max, median, etc.
    #[serde(default)]
    agg: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    expr: Option<String>,
    #[serde(default)]
    label: Option<String>,
    /// agg_time_dimension override for this measure.
    #[serde(default)]
    agg_time_dimension: Option<String>,
    #[serde(default)]
    create_metric: Option<bool>,
    /// Filters (where clauses) applied to this measure.
    #[serde(default, alias = "filter")]
    filters: Vec<DbtMeasureFilter>,
    /// Non-additive dimension config (e.g., for semi-additive measures).
    #[serde(default)]
    non_additive_dimension: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct DbtMeasureFilter {
    #[serde(default, alias = "where")]
    filter_expr: Option<String>,
    // MetricFlow supports `{{ Dimension(...) }}` Jinja syntax
    #[serde(default, alias = "sql")]
    sql: Option<String>,
}

/// A dbt metric definition.
#[derive(Debug, Deserialize)]
struct DbtMetric {
    name: String,
    #[serde(rename = "type")]
    metric_type: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    type_params: Option<DbtMetricTypeParams>,
    #[serde(default, alias = "filter")]
    filters: Vec<DbtMeasureFilter>,
}

#[derive(Debug, Deserialize)]
struct DbtMetricTypeParams {
    /// For simple metrics — reference to a measure.
    #[serde(default)]
    measure: Option<DbtMetricMeasureRef>,
    /// For ratio metrics — numerator.
    #[serde(default)]
    numerator: Option<DbtMetricMeasureRef>,
    /// For ratio metrics — denominator.
    #[serde(default)]
    denominator: Option<DbtMetricMeasureRef>,
    /// For derived metrics — expression.
    #[serde(default)]
    expr: Option<String>,
    /// For derived metrics — list of input metrics.
    #[serde(default)]
    metrics: Vec<DbtDerivedMetricInput>,
    /// For cumulative metrics — window and grain.
    #[serde(default)]
    window: Option<String>,
    #[serde(default)]
    grain_to_date: Option<String>,
}

/// A measure reference in a metric (can be a string or an object).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DbtMetricMeasureRef {
    Name(String),
    Object { name: String, #[serde(default)] filter: Option<String> },
}

impl DbtMetricMeasureRef {
    fn name(&self) -> &str {
        match self {
            Self::Name(s) => s,
            Self::Object { name, .. } => name,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DbtDerivedMetricInput {
    name: String,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    offset_window: Option<String>,
    #[serde(default, alias = "filter")]
    filters: Vec<DbtMeasureFilter>,
}

// ── Conversion logic ─────────────────────────────────────────────────

/// Convert dbt MetricFlow YAML content into airlayer views.
pub fn convert(content: &str, source: &str) -> Result<ConversionResult, String> {
    let dbt_file: DbtFile = serde_yaml::from_str(content)
        .map_err(|e| format!("Failed to parse dbt schema from {}: {}", source, e))?;

    let mut views = Vec::new();
    let mut warnings = Vec::new();

    for model in &dbt_file.semantic_models {
        let view = convert_semantic_model(model, &mut warnings);
        views.push(view);
    }

    // Convert metrics into additional measures on existing views
    for metric in &dbt_file.metrics {
        apply_metric(&mut views, metric, &mut warnings);
    }

    if views.is_empty() {
        return Err(format!("No semantic_models found in {}", source));
    }

    Ok(ConversionResult { views, warnings })
}

fn convert_semantic_model(model: &DbtSemanticModel, warnings: &mut Vec<String>) -> View {
    let model_name = &model.name;

    // Resolve the table name from model ref or node_relation
    let table = resolve_table(model);

    // Convert entities
    let entities: Vec<Entity> = model
        .entities
        .iter()
        .map(|e| convert_dbt_entity(e, warnings))
        .collect();

    // Convert dimensions
    let dimensions: Vec<Dimension> = model
        .dimensions
        .iter()
        .map(|d| convert_dbt_dimension(d, model_name, warnings))
        .collect();

    // Convert measures
    let measures: Vec<Measure> = model
        .measures
        .iter()
        .map(|m| convert_dbt_measure(m, model_name, warnings))
        .collect();

    let description = model
        .description
        .clone()
        .unwrap_or_else(|| format!("Converted from dbt semantic model '{}'", model_name));

    View {
        name: model_name.clone(),
        description,
        label: model.label.clone(),
        datasource: None,
        dialect: None,
        table,
        sql: None,
        entities,
        dimensions,
        measures: if measures.is_empty() {
            None
        } else {
            Some(measures)
        },
        segments: vec![],
        meta: None,
    }
}

fn resolve_table(model: &DbtSemanticModel) -> Option<String> {
    if let Some(ref m) = model.model {
        if let Some(caps) = RE_DBT_REF.captures(m) {
            return Some(caps[1].to_string());
        }
        if let Some(caps) = RE_DBT_SOURCE.captures(m) {
            return Some(format!("{}.{}", &caps[1], &caps[2]));
        }
        return Some(m.clone());
    }
    if let Some(ref nr) = model.node_relation {
        let mut parts = Vec::new();
        if let Some(ref db) = nr.database {
            parts.push(db.as_str());
        }
        if let Some(ref schema) = nr.schema_name {
            parts.push(schema.as_str());
        }
        if let Some(ref rel) = nr.relation_name {
            parts.push(rel.as_str());
        }
        if !parts.is_empty() {
            return Some(parts.join("."));
        }
    }
    Some(model.name.clone())
}

fn convert_dbt_entity(e: &DbtEntity, _warnings: &mut Vec<String>) -> Entity {
    let entity_type = match e.entity_type.to_lowercase().as_str() {
        "primary" | "unique" | "natural" => EntityType::Primary,
        "foreign" => EntityType::Foreign,
        _ => EntityType::Primary,
    };

    let expr = e.expr.as_deref().unwrap_or(&e.name);

    Entity {
        name: e.name.clone(),
        entity_type,
        description: e.description.clone(),
        key: Some(expr.to_string()),
        keys: None,
        inherits_from: None,
        meta: None,
    }
}

fn convert_dbt_dimension(
    d: &DbtDimension,
    _model_name: &str,
    _warnings: &mut Vec<String>,
) -> Dimension {
    let mut dimension_type = parse_foreign_dimension_type(&d.dim_type);
    // dbt-specific: refine time type based on granularity
    if dimension_type == DimensionType::Datetime {
        if let Some(ref tp) = d.type_params {
            match tp.time_granularity.as_deref() {
                Some("day") | Some("week") | Some("month") | Some("quarter")
                | Some("year") => dimension_type = DimensionType::Date,
                _ => {}
            }
        }
    }

    let expr = d.expr.clone().unwrap_or_else(|| d.name.clone());

    Dimension {
        name: d.name.clone(),
        dimension_type,
        description: d.description.clone().or_else(|| d.label.clone()),
        expr,
        original_expr: None,
        samples: None,
        synonyms: None,
        primary_key: None,
        sub_query: None,
        inherits_from: None,
        meta: None,
    }
}

fn convert_dbt_measure(
    m: &DbtMeasure,
    _model_name: &str,
    _warnings: &mut Vec<String>,
) -> Measure {
    let measure_type = parse_foreign_measure_type(m.agg.as_deref().unwrap_or("count"));

    let expr = m.expr.clone();

    // Convert filters
    let filters = if m.filters.is_empty() {
        None
    } else {
        Some(
            m.filters
                .iter()
                .filter_map(|f| {
                    let filter_expr = f.filter_expr.as_ref().or(f.sql.as_ref())?;
                    Some(MeasureFilter {
                        expr: rewrite_dbt_jinja(filter_expr),
                        original_expr: Some(filter_expr.clone()),
                        description: None,
                    })
                })
                .collect::<Vec<_>>(),
        )
    };

    Measure {
        name: m.name.clone(),
        measure_type,
        description: m.description.clone().or_else(|| m.label.clone()),
        expr,
        original_expr: None,
        filters,
        samples: None,
        synonyms: None,
        rolling_window: None,
        inherits_from: None,
        meta: None,
    }
}

/// Convert metrics into measures and add them to the appropriate views.
fn apply_metric(views: &mut [View], metric: &DbtMetric, warnings: &mut Vec<String>) {
    match metric.metric_type.as_str() {
        "simple" => {
            if let Some(ref tp) = metric.type_params {
                if let Some(ref measure_ref) = tp.measure {
                    let measure_name = measure_ref.name();
                    // Find the view containing this measure and add a derived measure
                    for view in views.iter_mut() {
                        if let Some(ref measures) = view.measures {
                            if measures.iter().any(|m| m.name == measure_name) {
                                // Simple metric — just an alias for the measure, skip
                                return;
                            }
                        }
                    }
                }
            }
        }
        "ratio" => {
            if let Some(ref tp) = metric.type_params {
                if let (Some(num), Some(den)) = (&tp.numerator, &tp.denominator) {
                    // Create a derived Number measure
                    let expr = format!(
                        "CAST({} AS DOUBLE) / NULLIF({}, 0)",
                        num.name(),
                        den.name()
                    );
                    let measure = Measure {
                        name: metric.name.clone(),
                        measure_type: MeasureType::Number,
                        description: metric.description.clone().or_else(|| metric.label.clone()),
                        expr: Some(expr),
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        meta: None,
                    };
                    // Add to the first view that has either the numerator or denominator
                    for view in views.iter_mut() {
                        if let Some(ref measures) = view.measures {
                            if measures.iter().any(|m| m.name == num.name() || m.name == den.name()) {
                                if let Some(ref mut ms) = view.measures {
                                    ms.push(measure);
                                }
                                return;
                            }
                        }
                    }
                    warnings.push(format!(
                        "Could not find view for ratio metric '{}' (numerator: '{}', denominator: '{}')",
                        metric.name, num.name(), den.name()
                    ));
                }
            }
        }
        "cumulative" => {
            if let Some(ref tp) = metric.type_params {
                if let Some(ref measure_ref) = tp.measure {
                    let measure_name = measure_ref.name();
                    for view in views.iter_mut() {
                        if let Some(ref mut measures) = view.measures {
                            if measures.iter().any(|m| m.name == measure_name) {
                                let measure = Measure {
                                    name: metric.name.clone(),
                                    measure_type: MeasureType::Sum,
                                    description: metric
                                        .description
                                        .clone()
                                        .or_else(|| metric.label.clone()),
                                    expr: measures
                                        .iter()
                                        .find(|m| m.name == measure_name)
                                        .and_then(|m| m.expr.clone()),
                                    original_expr: None,
                                    filters: None,
                                    samples: None,
                                    synonyms: None,
                                    rolling_window: Some(RollingWindow {
                                        trailing: tp.window.clone().or(Some("unbounded".to_string())),
                                        leading: None,
                                        offset: None,
                                    }),
                                    inherits_from: None,
                                    meta: None,
                                };
                                measures.push(measure);
                                return;
                            }
                        }
                    }
                }
            }
        }
        "derived" => {
            if let Some(ref tp) = metric.type_params {
                if let Some(ref expr) = tp.expr {
                    let measure = Measure {
                        name: metric.name.clone(),
                        measure_type: MeasureType::Number,
                        description: metric.description.clone().or_else(|| metric.label.clone()),
                        expr: Some(rewrite_dbt_metric_expr(expr, &tp.metrics)),
                        original_expr: Some(expr.clone()),
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        meta: None,
                    };
                    // Add to first view
                    if let Some(view) = views.first_mut() {
                        if let Some(ref mut ms) = view.measures {
                            ms.push(measure);
                        } else {
                            view.measures = Some(vec![measure]);
                        }
                    }
                }
            }
        }
        other => {
            warnings.push(format!(
                "Unknown dbt metric type '{}' for metric '{}'",
                other, metric.name
            ));
        }
    }
}

/// Rewrite derived metric expressions — replace metric aliases with references.
fn rewrite_dbt_metric_expr(expr: &str, inputs: &[DbtDerivedMetricInput]) -> String {
    let mut result = expr.to_string();
    for input in inputs {
        let alias = input.alias.as_deref().unwrap_or(&input.name);
        // In MetricFlow derived expressions, metrics are referenced by name/alias directly
        result = result.replace(alias, &format!("{{{{{}}}}}", input.name));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_simple_semantic_model() {
        let yaml = r#"
semantic_models:
  - name: orders
    model: ref('stg_orders')
    description: "Order facts"
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
    dimensions:
      - name: status
        type: categorical
        expr: status
      - name: ordered_at
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: order_count
        agg: count
        expr: "1"
      - name: total_amount
        agg: sum
        expr: amount
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 1);

        let view = &result.views[0];
        assert_eq!(view.name, "orders");
        assert_eq!(view.table, Some("stg_orders".to_string()));

        // Entities
        assert_eq!(view.entities.len(), 2);
        assert_eq!(view.entities[0].name, "order_id");
        assert_eq!(view.entities[0].entity_type, EntityType::Primary);
        assert_eq!(view.entities[1].name, "customer_id");
        assert_eq!(view.entities[1].entity_type, EntityType::Foreign);

        // Dimensions
        assert_eq!(view.dimensions.len(), 2);
        assert_eq!(view.dimensions[0].name, "status");
        assert_eq!(view.dimensions[0].dimension_type, DimensionType::String);
        assert_eq!(view.dimensions[1].name, "ordered_at");
        assert_eq!(view.dimensions[1].dimension_type, DimensionType::Date);

        // Measures
        let measures = view.measures_list();
        assert_eq!(measures.len(), 2);
        assert_eq!(measures[0].measure_type, MeasureType::Count);
        assert_eq!(measures[1].measure_type, MeasureType::Sum);
    }

    #[test]
    fn test_convert_with_metrics() {
        let yaml = r#"
semantic_models:
  - name: orders
    model: ref('stg_orders')
    entities:
      - name: order_id
        type: primary
    dimensions:
      - name: ordered_at
        type: time
    measures:
      - name: total_revenue
        agg: sum
        expr: amount
      - name: total_cost
        agg: sum
        expr: cost

metrics:
  - name: profit_margin
    type: derived
    type_params:
      expr: "total_revenue - total_cost"
      metrics:
        - name: total_revenue
        - name: total_cost
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 1);

        let measures = result.views[0].measures_list();
        // 2 base measures + 1 derived metric
        assert_eq!(measures.len(), 3);
        let derived = measures.iter().find(|m| m.name == "profit_margin").unwrap();
        assert_eq!(derived.measure_type, MeasureType::Number);
    }

    #[test]
    fn test_convert_ratio_metric() {
        let yaml = r#"
semantic_models:
  - name: orders
    model: ref('stg_orders')
    entities:
      - name: order_id
        type: primary
    dimensions:
      - name: status
        type: categorical
    measures:
      - name: completed_orders
        agg: count
        expr: "1"
      - name: total_orders
        agg: count
        expr: "1"

metrics:
  - name: completion_rate
    type: ratio
    type_params:
      numerator: completed_orders
      denominator: total_orders
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let measures = result.views[0].measures_list();
        let ratio = measures
            .iter()
            .find(|m| m.name == "completion_rate")
            .unwrap();
        assert_eq!(ratio.measure_type, MeasureType::Number);
        assert!(ratio.expr.as_ref().unwrap().contains("NULLIF"));
    }

    #[test]
    fn test_convert_cumulative_metric() {
        let yaml = r#"
semantic_models:
  - name: orders
    model: ref('stg_orders')
    entities:
      - name: order_id
        type: primary
    dimensions:
      - name: ordered_at
        type: time
    measures:
      - name: revenue
        agg: sum
        expr: amount

metrics:
  - name: cumulative_revenue
    type: cumulative
    type_params:
      measure: revenue
      window: 7 days
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let measures = result.views[0].measures_list();
        let cum = measures
            .iter()
            .find(|m| m.name == "cumulative_revenue")
            .unwrap();
        assert!(cum.rolling_window.is_some());
        assert_eq!(
            cum.rolling_window.as_ref().unwrap().trailing,
            Some("7 days".to_string())
        );
    }

    #[test]
    fn test_convert_measure_with_filters() {
        let yaml = r#"
semantic_models:
  - name: orders
    model: ref('stg_orders')
    entities:
      - name: order_id
        type: primary
    dimensions:
      - name: status
        type: categorical
    measures:
      - name: completed_count
        agg: count
        expr: "1"
        filters:
          - filter_expr: "status = 'completed'"
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let measure = &result.views[0].measures_list()[0];
        assert!(measure.filters.is_some());
        assert_eq!(
            measure.filters.as_ref().unwrap()[0].expr,
            "status = 'completed'"
        );
    }

    #[test]
    fn test_rewrite_dbt_jinja() {
        assert_eq!(
            rewrite_dbt_jinja("{{ Dimension('order_id__status') }} = 'completed'"),
            "status = 'completed'"
        );
        assert_eq!(
            rewrite_dbt_jinja("{{ TimeDimension('order_id__ordered_at', 'month') }} > '2024-01-01'"),
            "ordered_at > '2024-01-01'"
        );
    }

    #[test]
    fn test_resolve_model_ref() {
        let model = DbtSemanticModel {
            name: "orders".to_string(),
            description: None,
            model: Some("ref('stg_orders')".to_string()),
            node_relation: None,
            defaults: None,
            entities: vec![],
            dimensions: vec![],
            measures: vec![],
            label: None,
        };
        assert_eq!(resolve_table(&model), Some("stg_orders".to_string()));
    }

    #[test]
    fn test_resolve_source_ref() {
        let model = DbtSemanticModel {
            name: "orders".to_string(),
            description: None,
            model: Some("source('raw', 'orders')".to_string()),
            node_relation: None,
            defaults: None,
            entities: vec![],
            dimensions: vec![],
            measures: vec![],
            label: None,
        };
        assert_eq!(resolve_table(&model), Some("raw.orders".to_string()));
    }
}
