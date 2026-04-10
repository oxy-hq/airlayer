//! Omni Analytics semantic layer parser.
//!
//! Omni uses a YAML-based modeling format inspired by LookML but with key differences:
//! - Schema files define views with dimensions and measures (like LookML, but in YAML)
//! - Topics define joins and relationships (similar to LookML explores)
//! - Supports ${TABLE}.column and ${view.field} references
//! - Dimension groups with timeframes
//! - Aggregate awareness and query optimization hints
//!
//! Omni's schema format closely mirrors LookML concepts but uses a cleaner YAML syntax.

use super::{ConversionResult, parse_foreign_dimension_type, parse_foreign_measure_type,
            relationship_to_entity_type, rewrite_dollar_refs, extract_dollar_join_key,
            expand_dimension_group};
use crate::schema::models::*;
use serde::Deserialize;
use std::collections::HashMap;

// ── Omni native types ────────────────────────────────────────────────

/// Top-level Omni schema file.
#[derive(Debug, Deserialize)]
struct OmniFile {
    #[serde(default)]
    views: HashMap<String, OmniView>,
    #[serde(default)]
    topics: HashMap<String, OmniTopic>,
}

/// An Omni view definition.
#[derive(Debug, Deserialize)]
struct OmniView {
    #[serde(default, alias = "sql_table_name")]
    sql_table_name: Option<String>,
    /// Derived table SQL.
    #[serde(default)]
    derived_table: Option<OmniDerivedTable>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    dimensions: HashMap<String, OmniDimension>,
    #[serde(default)]
    dimension_groups: HashMap<String, OmniDimensionGroup>,
    #[serde(default)]
    measures: HashMap<String, OmniMeasure>,
    #[serde(default)]
    filters: HashMap<String, OmniFilter>,
    #[serde(default)]
    sets: HashMap<String, OmniSet>,
}

#[derive(Debug, Deserialize)]
struct OmniDerivedTable {
    #[serde(default)]
    sql: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OmniDimension {
    #[serde(rename = "type", default)]
    dim_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    primary_key: Option<bool>,
    #[serde(default)]
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OmniDimensionGroup {
    #[serde(rename = "type", default)]
    group_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    timeframes: Option<Vec<String>>,
    #[serde(default)]
    intervals: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct OmniMeasure {
    #[serde(rename = "type")]
    measure_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    filters: Option<HashMap<String, String>>,
    #[serde(default)]
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OmniFilter {
    #[serde(rename = "type", default)]
    filter_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OmniSet {
    #[serde(default)]
    fields: Vec<String>,
}

/// An Omni topic (like a LookML explore).
#[derive(Debug, Deserialize)]
struct OmniTopic {
    #[serde(default)]
    base_view: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    joins: HashMap<String, OmniJoin>,
}

#[derive(Debug, Deserialize)]
struct OmniJoin {
    #[serde(default)]
    sql_on: Option<String>,
    #[serde(default)]
    relationship: Option<String>,
    #[serde(rename = "type", default)]
    join_type: Option<String>,
}

// ── Conversion logic ─────────────────────────────────────────────────

/// Convert Omni YAML content into airlayer views.
pub fn convert(content: &str, source: &str) -> Result<ConversionResult, String> {
    let omni_file: OmniFile = serde_yaml::from_str(content)
        .map_err(|e| format!("Failed to parse Omni schema from {}: {}", source, e))?;

    let mut views = Vec::new();
    let mut warnings = Vec::new();

    for (name, omni_view) in &omni_file.views {
        let view = convert_omni_view(name, omni_view, &mut warnings);
        views.push(view);
    }

    // Apply topic joins to views
    for (topic_name, topic) in &omni_file.topics {
        apply_topic_joins(&mut views, topic_name, topic, &mut warnings);
    }

    if views.is_empty() {
        return Err(format!("No views found in Omni file {}", source));
    }

    Ok(ConversionResult { views, warnings })
}

fn convert_omni_view(name: &str, omni: &OmniView, warnings: &mut Vec<String>) -> View {
    let mut dimensions = Vec::new();
    let mut measures = Vec::new();
    let mut segments = Vec::new();

    // Convert dimensions
    for (dim_name, dim) in &omni.dimensions {
        let d = convert_omni_dimension(dim_name, dim, name, warnings);
        dimensions.push(d);
    }

    // Convert dimension groups
    for (group_name, group) in &omni.dimension_groups {
        let mut dims = convert_omni_dimension_group(group_name, group, name, warnings);
        dimensions.append(&mut dims);
    }

    // Convert measures
    for (measure_name, measure) in &omni.measures {
        if let Some(m) = convert_omni_measure(measure_name, measure, name, warnings) {
            measures.push(m);
        }
    }

    // Convert filters to segments
    for (filter_name, filter) in &omni.filters {
        if let Some(seg) = convert_omni_filter(filter_name, filter, name) {
            segments.push(seg);
        }
    }

    // Table / SQL
    let (table, sql) = if let Some(ref t) = omni.sql_table_name {
        (Some(t.clone()), None)
    } else if let Some(ref dt) = omni.derived_table {
        (None, dt.sql.as_ref().map(|s| rewrite_dollar_refs(s, name)))
    } else {
        (Some(name.to_string()), None)
    };

    // Build entities from primary key dimensions
    let mut entities = Vec::new();
    if let Some(pk_dim) = dimensions.iter().find(|d| d.primary_key == Some(true)) {
        entities.push(Entity {
            name: name.to_string(),
            entity_type: EntityType::Primary,
            description: None,
            key: Some(pk_dim.name.clone()),
            keys: None,
            inherits_from: None,
            meta: None,
        });
    }

    View {
        name: name.to_string(),
        description: omni
            .description
            .clone()
            .unwrap_or_else(|| format!("Converted from Omni view '{}'", name)),
        label: omni.label.clone(),
        datasource: None,
        dialect: None,
        table,
        sql,
        entities,
        dimensions,
        measures: if measures.is_empty() {
            None
        } else {
            Some(measures)
        },
        segments,
        meta: None,
    }
}

fn convert_omni_dimension(
    name: &str,
    dim: &OmniDimension,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Dimension {
    let dim_type_str = dim.dim_type.as_deref().unwrap_or("string");
    let dimension_type = parse_foreign_dimension_type(dim_type_str);

    let expr = dim
        .sql
        .as_ref()
        .map(|s| rewrite_dollar_refs(s, view_name))
        .unwrap_or_else(|| name.to_string());

    Dimension {
        name: name.to_string(),
        dimension_type,
        description: dim.description.clone().or_else(|| dim.label.clone()),
        expr,
        original_expr: dim.sql.clone(),
        samples: None,
        synonyms: None,
        primary_key: dim.primary_key,
        sub_query: None,
        inherits_from: None,
        meta: None,
    }
}

fn convert_omni_dimension_group(
    name: &str,
    group: &OmniDimensionGroup,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Vec<Dimension> {
    let group_type = group.group_type.as_deref().unwrap_or("time");
    let sql_expr = group.sql.as_deref().unwrap_or(name);
    let rewritten = rewrite_dollar_refs(sql_expr, view_name);

    if group_type == "time" {
        let tf_strs: Vec<&str> = group.timeframes.as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        expand_dimension_group(
            name, "time", &rewritten, group.sql.as_deref(),
            group.description.as_deref(), &tf_strs, &[],
        )
    } else if group_type == "duration" {
        let iv_strs: Vec<&str> = group.intervals.as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        expand_dimension_group(
            name, "duration", &rewritten, group.sql.as_deref(),
            group.description.as_deref(), &[], &iv_strs,
        )
    } else {
        vec![]
    }
}

fn convert_omni_measure(
    name: &str,
    measure: &OmniMeasure,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Option<Measure> {
    let type_str = measure.measure_type.as_deref().unwrap_or("count");
    let measure_type = parse_foreign_measure_type(type_str);

    let expr = measure
        .sql
        .as_ref()
        .map(|s| rewrite_dollar_refs(s, view_name));

    let filters = measure.filters.as_ref().and_then(|f| {
        let filter_exprs: Vec<MeasureFilter> = f
            .iter()
            .map(|(field, value)| {
                let rewritten_field = rewrite_dollar_refs(field, view_name);
                MeasureFilter {
                    expr: format!("{} = '{}'", rewritten_field, value),
                    original_expr: None,
                    description: None,
                }
            })
            .collect();
        if filter_exprs.is_empty() {
            None
        } else {
            Some(filter_exprs)
        }
    });

    let rolling_window = if type_str == "running_total" {
        Some(RollingWindow {
            trailing: Some("unbounded".to_string()),
            leading: None,
            offset: None,
        })
    } else {
        None
    };

    Some(Measure {
        name: name.to_string(),
        measure_type,
        description: measure.description.clone().or_else(|| measure.label.clone()),
        expr,
        original_expr: measure.sql.clone(),
        filters,
        samples: None,
        synonyms: None,
        rolling_window,
        inherits_from: None,
        meta: None,
    })
}

fn convert_omni_filter(name: &str, filter: &OmniFilter, view_name: &str) -> Option<Segment> {
    let sql = filter.sql.as_ref()?;
    Some(Segment {
        name: name.to_string(),
        expr: rewrite_dollar_refs(sql, view_name),
        description: filter.description.clone(),
        inherits_from: None,
        meta: None,
    })
}

/// Apply topic-level joins to views.
fn apply_topic_joins(
    views: &mut [View],
    _topic_name: &str,
    topic: &OmniTopic,
    _warnings: &mut Vec<String>,
) {
    let base_view_name = topic.base_view.as_deref().unwrap_or("");

    for (join_name, join) in &topic.joins {
        let entity_type = relationship_to_entity_type(
            join.relationship.as_deref().unwrap_or("many_to_one"),
        );

        let fk = join
            .sql_on
            .as_ref()
            .and_then(|s| extract_dollar_join_key(s, base_view_name));

        if let Some(base_view) = views.iter_mut().find(|v| v.name == base_view_name) {
            base_view.entities.push(Entity {
                name: join_name.to_string(),
                entity_type,
                description: None,
                key: fk,
                keys: None,
                inherits_from: None,
                meta: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_simple_omni_view() {
        let yaml = r#"
views:
  orders:
    sql_table_name: public.orders
    description: "Order data"
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
        primary_key: true
      status:
        type: string
        sql: "${TABLE}.status"
    measures:
      count:
        type: count
      total_amount:
        type: sum
        sql: "${TABLE}.amount"
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 1);

        let view = &result.views[0];
        assert_eq!(view.name, "orders");
        assert_eq!(view.table, Some("public.orders".to_string()));
        assert_eq!(view.dimensions.len(), 2);

        let id_dim = view.dimensions.iter().find(|d| d.name == "id").unwrap();
        assert_eq!(id_dim.dimension_type, DimensionType::Number);
        assert_eq!(id_dim.primary_key, Some(true));
        assert_eq!(id_dim.expr, "id");

        let measures = view.measures_list();
        assert_eq!(measures.len(), 2);
    }

    #[test]
    fn test_convert_omni_with_dimension_groups() {
        let yaml = r#"
views:
  orders:
    sql_table_name: orders
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
    dimension_groups:
      created:
        type: time
        sql: "${TABLE}.created_at"
        timeframes: [date, month, year]
    measures:
      count:
        type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let view = &result.views[0];
        // id + 3 time dimensions
        assert_eq!(view.dimensions.len(), 4);
        assert!(view.dimensions.iter().any(|d| d.name == "created_date"));
        assert!(view.dimensions.iter().any(|d| d.name == "created_month"));
        assert!(view.dimensions.iter().any(|d| d.name == "created_year"));
    }

    #[test]
    fn test_convert_omni_with_topics() {
        let yaml = r#"
views:
  orders:
    sql_table_name: orders
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
        primary_key: true
      user_id:
        type: number
        sql: "${TABLE}.user_id"
    measures:
      count:
        type: count
  users:
    sql_table_name: users
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
        primary_key: true
      name:
        type: string
        sql: "${TABLE}.name"
    measures:
      count:
        type: count

topics:
  order_analytics:
    base_view: orders
    joins:
      users:
        sql_on: "${orders.user_id} = ${users.id}"
        relationship: many_to_one
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 2);

        let orders = result.views.iter().find(|v| v.name == "orders").unwrap();
        let foreign = orders
            .entities
            .iter()
            .find(|e| e.name == "users")
            .expect("Should have foreign entity for users");
        assert_eq!(foreign.entity_type, EntityType::Foreign);
        assert_eq!(foreign.key, Some("user_id".to_string()));
    }

    #[test]
    fn test_convert_omni_derived_table() {
        let yaml = r#"
views:
  active_users:
    derived_table:
      sql: "SELECT * FROM users WHERE active = true"
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
        primary_key: true
    measures:
      count:
        type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let view = &result.views[0];
        assert!(view.table.is_none());
        assert!(view.sql.is_some());
    }

    #[test]
    fn test_rewrite_dollar_refs() {
        assert_eq!(rewrite_dollar_refs("${TABLE}.id", "orders"), "id");
        assert_eq!(rewrite_dollar_refs("${orders.id}", "orders"), "id");
        assert_eq!(
            rewrite_dollar_refs("${users.id}", "orders"),
            "{{users.id}}"
        );
    }

    #[test]
    fn test_convert_omni_measure_with_filters() {
        let yaml = r#"
views:
  orders:
    sql_table_name: orders
    dimensions:
      id:
        type: number
        sql: "${TABLE}.id"
      status:
        type: string
        sql: "${TABLE}.status"
    measures:
      completed_count:
        type: count
        filters:
          status: "completed"
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let measure = result.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "completed_count")
            .unwrap();
        assert!(measure.filters.is_some());
    }
}
