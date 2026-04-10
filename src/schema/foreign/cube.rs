//! Cube.js YAML schema parser.
//!
//! Converts Cube.js cube definitions into airlayer views. Supports:
//! - Dimensions: string, number, time, boolean, geo
//! - Measures: count, sum, avg, min, max, count_distinct, count_distinct_approx, number
//! - Joins: belongs_to (many_to_one), has_many (one_to_many), has_one (one_to_one)
//! - Segments: boolean SQL conditions
//! - sql_table / sql (derived tables)
//! - primary_key on dimensions
//! - Measure filters
//! - Rolling window measures
//! - Sub-query dimensions
//! - {CUBE} / {TABLE} reference rewriting

use super::{ConversionResult, parse_foreign_measure_type, relationship_to_entity_type,
            rewrite_cube_refs, extract_cube_join_key};
use crate::schema::models::*;
use serde::Deserialize;
use std::collections::HashMap;

// ── Cube.js native types ─────────────────────────────────────────────

/// Top-level Cube.js schema file — may contain one or more cubes.
#[derive(Debug, Deserialize)]
struct CubeFile {
    #[serde(default)]
    cubes: Vec<CubeDef>,
}

/// A single cube definition.
#[derive(Debug, Deserialize)]
struct CubeDef {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    /// Table reference: `sql_table` in Cube.js YAML.
    #[serde(default, alias = "sqlTable", alias = "sql_table_name")]
    sql_table: Option<String>,
    /// Custom SQL (derived table).
    #[serde(default)]
    sql: Option<String>,
    /// Data source name (maps to a configured database).
    #[serde(default, alias = "dataSource", alias = "data_source")]
    data_source: Option<String>,
    #[serde(default)]
    dimensions: Vec<CubeDimension>,
    #[serde(default)]
    measures: Vec<CubeMeasure>,
    #[serde(default)]
    joins: Vec<CubeJoin>,
    #[serde(default)]
    segments: Vec<CubeSegment>,
    #[serde(default, alias = "preAggregations", alias = "pre_aggregations")]
    pre_aggregations: Vec<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct CubeDimension {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    /// SQL expression. In Cube.js: `sql: {CUBE}.column_name` or `sql: column_name`.
    #[serde(default)]
    sql: Option<String>,
    /// Dimension type: string, number, time, boolean, geo.
    #[serde(rename = "type")]
    dim_type: String,
    #[serde(default, alias = "primaryKey", alias = "primary_key")]
    primary_key: Option<bool>,
    /// Sub-query dimension — references a measure from a joined cube.
    #[serde(default, alias = "subQuery", alias = "sub_query")]
    sub_query: Option<bool>,
    #[serde(default)]
    shown: Option<bool>,
    #[serde(default)]
    meta: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct CubeMeasure {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    /// SQL expression for the measure.
    #[serde(default)]
    sql: Option<String>,
    /// Measure type: count, sum, avg, min, max, count_distinct, count_distinct_approx, number, etc.
    #[serde(rename = "type")]
    measure_type: String,
    /// Filters applied to this measure.
    #[serde(default)]
    filters: Vec<CubeMeasureFilter>,
    /// Rolling window configuration.
    #[serde(default, alias = "rollingWindow", alias = "rolling_window")]
    rolling_window: Option<CubeRollingWindow>,
    /// Drill members for exploration.
    #[serde(default, alias = "drillMembers", alias = "drill_members")]
    drill_members: Vec<String>,
    #[serde(default)]
    shown: Option<bool>,
    #[serde(default)]
    meta: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct CubeMeasureFilter {
    /// SQL boolean expression for the filter.
    sql: String,
}

#[derive(Debug, Deserialize)]
struct CubeRollingWindow {
    #[serde(default)]
    trailing: Option<String>,
    #[serde(default)]
    leading: Option<String>,
    #[serde(default)]
    offset: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CubeJoin {
    name: String,
    /// Join SQL condition: `{CUBE}.user_id = {users}.id`.
    sql: String,
    /// Relationship type: belongs_to, has_many, has_one.
    relationship: String,
}

#[derive(Debug, Deserialize)]
struct CubeSegment {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    /// SQL boolean expression.
    sql: String,
}

// ── Conversion logic ─────────────────────────────────────────────────

/// Convert Cube.js YAML content into airlayer views.
pub fn convert(content: &str, source: &str) -> Result<ConversionResult, String> {
    // Try parsing as a multi-cube file first
    let cube_file: CubeFile = match serde_yaml::from_str::<CubeFile>(content) {
        Ok(f) if !f.cubes.is_empty() => f,
        _ => {
            // Try parsing as a single cube
            match serde_yaml::from_str::<CubeDef>(content) {
                Ok(cube) => CubeFile {
                    cubes: vec![cube],
                },
                Err(e) => {
                    return Err(format!(
                        "Failed to parse Cube.js schema from {}: {}",
                        source, e
                    ))
                }
            }
        }
    };

    let mut views = Vec::new();
    let mut warnings = Vec::new();

    // Build a name→CubeDef map for join target lookup
    let cube_map: HashMap<&str, &CubeDef> = cube_file
        .cubes
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    for cube in &cube_file.cubes {
        let view = convert_cube(cube, &cube_map, &mut warnings);
        views.push(view);
    }

    if views.is_empty() {
        return Err(format!("No cubes found in {}", source));
    }

    Ok(ConversionResult { views, warnings })
}

fn convert_cube(
    cube: &CubeDef,
    cube_map: &HashMap<&str, &CubeDef>,
    warnings: &mut Vec<String>,
) -> View {
    let cube_name = &cube.name;

    // Convert dimensions
    let mut dimensions = Vec::new();
    let mut primary_key_dim: Option<String> = None;
    for d in &cube.dimensions {
        let dim = convert_dimension(d, cube_name, warnings);
        if d.primary_key == Some(true) {
            primary_key_dim = Some(dim.name.clone());
        }
        dimensions.push(dim);
    }

    // Convert measures
    let measures: Vec<Measure> = cube
        .measures
        .iter()
        .map(|m| convert_measure(m, cube_name, warnings))
        .collect();

    // Build entities from joins
    let mut entities = Vec::new();

    // Primary entity from the cube's primary key dimension
    if let Some(ref pk) = primary_key_dim {
        entities.push(Entity {
            name: cube_name.to_string(),
            entity_type: EntityType::Primary,
            description: cube.description.clone(),
            key: Some(pk.clone()),
            keys: None,
            inherits_from: None,
            meta: None,
        });
    }

    // Foreign entities from joins
    for join in &cube.joins {
        let entity_name = &join.name;

        let entity_type = relationship_to_entity_type(&join.relationship);

        // Extract the foreign key from the join SQL
        let fk = extract_cube_join_key(&join.sql, cube_name);

        // Look up the target cube's primary key
        let target_pk = cube_map.get(entity_name.as_str()).and_then(|target| {
            target
                .dimensions
                .iter()
                .find(|d| d.primary_key == Some(true))
                .map(|d| d.name.clone())
        });

        let key = fk.or(target_pk);
        if key.is_none() {
            warnings.push(format!(
                "Could not determine join key for '{}' → '{}' in cube '{}'. \
                 Add a primary_key dimension to the target cube.",
                cube_name, entity_name, cube_name
            ));
        }

        entities.push(Entity {
            name: entity_name.to_string(),
            entity_type,
            description: None,
            key,
            keys: None,
            inherits_from: None,
            meta: None,
        });
    }

    // Convert segments
    let segments: Vec<Segment> = cube
        .segments
        .iter()
        .map(|s| convert_segment(s, cube_name))
        .collect();

    // Table / SQL
    let (table, sql) = if let Some(ref t) = cube.sql_table {
        (Some(t.clone()), None)
    } else if let Some(ref s) = cube.sql {
        (None, Some(rewrite_cube_refs(s, cube_name)))
    } else {
        // Default to using the cube name as the table name
        (Some(cube_name.clone()), None)
    };

    let description = cube
        .description
        .clone()
        .or_else(|| cube.title.clone())
        .unwrap_or_else(|| format!("Converted from Cube.js cube '{}'", cube_name));

    View {
        name: cube_name.clone(),
        description,
        label: cube.title.clone(),
        datasource: cube.data_source.clone(),
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

fn convert_dimension(
    d: &CubeDimension,
    cube_name: &str,
    _warnings: &mut Vec<String>,
) -> Dimension {
    let dimension_type = super::parse_foreign_dimension_type(&d.dim_type);

    let expr = d
        .sql
        .as_ref()
        .map(|s| rewrite_cube_refs(s, cube_name))
        .unwrap_or_else(|| d.name.clone());

    Dimension {
        name: d.name.clone(),
        dimension_type,
        description: d.description.clone().or_else(|| d.title.clone()),
        expr,
        original_expr: d.sql.clone(),
        samples: None,
        synonyms: None,
        primary_key: d.primary_key,
        sub_query: d.sub_query,
        inherits_from: None,
        meta: None,
    }
}

fn convert_measure(m: &CubeMeasure, cube_name: &str, _warnings: &mut Vec<String>) -> Measure {
    let measure_type = parse_foreign_measure_type(&m.measure_type);
    let type_lower = m.measure_type.to_lowercase();

    let expr = m
        .sql
        .as_ref()
        .map(|s| rewrite_cube_refs(s, cube_name));

    let filters = if m.filters.is_empty() {
        None
    } else {
        Some(
            m.filters
                .iter()
                .map(|f| MeasureFilter {
                    expr: rewrite_cube_refs(&f.sql, cube_name),
                    original_expr: Some(f.sql.clone()),
                    description: None,
                })
                .collect(),
        )
    };

    let rolling_window = m.rolling_window.as_ref().map(|rw| RollingWindow {
        trailing: rw.trailing.clone(),
        leading: rw.leading.clone(),
        offset: rw.offset.clone(),
    });

    // Handle running_total as sum + unbounded rolling window
    let rolling_window = if matches!(
        type_lower.as_str(),
        "run_total" | "runtotal" | "running_total"
    ) && rolling_window.is_none()
    {
        Some(RollingWindow {
            trailing: Some("unbounded".to_string()),
            leading: None,
            offset: None,
        })
    } else {
        rolling_window
    };

    Measure {
        name: m.name.clone(),
        measure_type,
        description: m.description.clone().or_else(|| m.title.clone()),
        expr,
        original_expr: m.sql.clone(),
        filters,
        samples: None,
        synonyms: None,
        rolling_window,
        inherits_from: None,
        meta: None,
    }
}

fn convert_segment(s: &CubeSegment, cube_name: &str) -> Segment {
    Segment {
        name: s.name.clone(),
        expr: rewrite_cube_refs(&s.sql, cube_name),
        description: s.description.clone().or_else(|| s.title.clone()),
        inherits_from: None,
        meta: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_cube_refs_self() {
        assert_eq!(super::rewrite_cube_refs("{CUBE}.user_id", "orders"), "user_id");
        assert_eq!(super::rewrite_cube_refs("{TABLE}.user_id", "orders"), "user_id");
        assert_eq!(super::rewrite_cube_refs("{orders}.user_id", "orders"), "user_id");
    }

    #[test]
    fn test_rewrite_cube_refs_cross() {
        assert_eq!(
            super::rewrite_cube_refs("{users}.id", "orders"),
            "{{users.id}}"
        );
    }

    #[test]
    fn test_rewrite_complex_expr() {
        let input = "{CUBE}.amount / 100.0 + {taxes}.rate";
        let result = super::rewrite_cube_refs(input, "orders");
        assert_eq!(result, "amount / 100.0 + {{taxes.rate}}");
    }

    #[test]
    fn test_convert_simple_cube() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: status
        sql: "{CUBE}.status"
        type: string
      - name: created_at
        sql: "{CUBE}.created_at"
        type: time
    measures:
      - name: count
        type: count
      - name: total_amount
        type: sum
        sql: "{CUBE}.amount"
      - name: avg_amount
        type: avg
        sql: "{CUBE}.amount"
    segments:
      - name: completed
        sql: "{CUBE}.status = 'completed'"
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 1);

        let view = &result.views[0];
        assert_eq!(view.name, "orders");
        assert_eq!(view.table, Some("public.orders".to_string()));
        assert_eq!(view.dimensions.len(), 3);
        assert_eq!(view.dimensions[0].name, "id");
        assert_eq!(view.dimensions[0].expr, "id");
        assert_eq!(view.dimensions[0].primary_key, Some(true));
        assert_eq!(view.dimensions[0].dimension_type, DimensionType::Number);
        assert_eq!(view.dimensions[1].dimension_type, DimensionType::String);
        assert_eq!(view.dimensions[2].dimension_type, DimensionType::Datetime);

        let measures = view.measures_list();
        assert_eq!(measures.len(), 3);
        assert_eq!(measures[0].measure_type, MeasureType::Count);
        assert_eq!(measures[1].measure_type, MeasureType::Sum);
        assert_eq!(measures[1].expr, Some("amount".to_string()));
        assert_eq!(measures[2].measure_type, MeasureType::Average);

        assert_eq!(view.segments.len(), 1);
        assert_eq!(view.segments[0].expr, "status = 'completed'");

        // Primary entity from primary key
        assert!(view.entities.iter().any(|e| e.name == "orders"
            && e.entity_type == EntityType::Primary
            && e.key == Some("id".to_string())));
    }

    #[test]
    fn test_convert_cube_with_joins() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: user_id
        sql: "{CUBE}.user_id"
        type: number
    measures:
      - name: count
        type: count
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: belongs_to

  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: name
        sql: "{CUBE}.name"
        type: string
    measures:
      - name: count
        type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 2);

        // Orders view should have a foreign entity for users
        let orders = &result.views[0];
        let foreign_entity = orders
            .entities
            .iter()
            .find(|e| e.name == "users")
            .expect("Should have foreign entity for users");
        assert_eq!(foreign_entity.entity_type, EntityType::Foreign);
        assert_eq!(foreign_entity.key, Some("user_id".to_string()));
    }

    #[test]
    fn test_convert_cube_with_measure_filters() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: status
        sql: "{CUBE}.status"
        type: string
    measures:
      - name: completed_count
        type: count
        filters:
          - sql: "{CUBE}.status = 'completed'"
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let view = &result.views[0];
        let measure = &view.measures_list()[0];
        assert!(measure.filters.is_some());
        assert_eq!(measure.filters.as_ref().unwrap().len(), 1);
        assert_eq!(
            measure.filters.as_ref().unwrap()[0].expr,
            "status = 'completed'"
        );
    }

    #[test]
    fn test_convert_cube_with_rolling_window() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: rolling_total
        type: sum
        sql: "{CUBE}.amount"
        rolling_window:
          trailing: 7 day
          offset: start
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let view = &result.views[0];
        let measure = &view.measures_list()[0];
        assert!(measure.rolling_window.is_some());
        let rw = measure.rolling_window.as_ref().unwrap();
        assert_eq!(rw.trailing, Some("7 day".to_string()));
        assert_eq!(rw.offset, Some("start".to_string()));
    }

    #[test]
    fn test_convert_cube_with_derived_sql() {
        let yaml = r#"
cubes:
  - name: active_users
    sql: "SELECT * FROM users WHERE active = true"
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let view = &result.views[0];
        assert!(view.table.is_none());
        assert_eq!(
            view.sql,
            Some("SELECT * FROM users WHERE active = true".to_string())
        );
    }

    #[test]
    fn test_convert_single_cube() {
        let yaml = r#"
name: orders
sql_table: orders
dimensions:
  - name: id
    sql: "{CUBE}.id"
    type: number
    primary_key: true
measures:
  - name: count
    type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        assert_eq!(result.views.len(), 1);
        assert_eq!(result.views[0].name, "orders");
    }

    #[test]
    fn test_convert_cube_subquery_dimension() {
        let yaml = r#"
cubes:
  - name: users
    sql_table: users
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: total_orders
        sql: "{orders.count}"
        type: number
        sub_query: true
    measures:
      - name: count
        type: count
"#;

        let result = convert(yaml, "test.yml").unwrap();
        let dim = result.views[0]
            .dimensions
            .iter()
            .find(|d| d.name == "total_orders")
            .unwrap();
        assert_eq!(dim.sub_query, Some(true));
    }

    #[test]
    fn test_extract_join_key() {
        assert_eq!(
            super::extract_cube_join_key("{CUBE}.user_id = {users}.id", "orders"),
            Some("user_id".to_string())
        );
        assert_eq!(
            super::extract_cube_join_key("{orders}.user_id = {users}.id", "orders"),
            Some("user_id".to_string())
        );
    }
}
