//! Omni Analytics semantic layer parser.
//!
//! Supports two Omni formats:
//!
//! **Directory format (real-world):**
//! - `model.yaml` — global config (included_schemas, access_grants)
//! - `relationships.yaml` — flat list of join definitions
//! - `*.topic.yaml` — topic files (field curation, join scoping)
//! - `schema_dir/*.view.yaml` — one view per file with `schema:` + `table_name:`
//! - `schema_dir/*.query.view.yaml` — derived table views with root-level `sql:`
//!
//! **Single-file format (legacy/test):**
//! - Single YAML with `views:` map + optional `topics:` map
//! - Uses `sql_table_name:`, `type:` on dimensions, etc.
//!
//! Both formats produce airlayer `View` types that compile to SQL immediately.

use super::{
    expand_dimension_group, extract_dollar_join_key, parse_foreign_dimension_type,
    parse_foreign_measure_type, relationship_to_entity_type, rewrite_dollar_refs, ConversionResult,
};
use crate::schema::models::*;
use serde::Deserialize;
use std::collections::BTreeMap;

// ── Omni native types (directory format) ────────────────────────────

/// A single Omni view file (schema_dir/name.view.yaml).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniViewFile {
    /// Database schema name.
    #[serde(default)]
    schema: Option<String>,
    /// Table name within the schema.
    #[serde(default)]
    table_name: Option<String>,
    /// Display label.
    #[serde(default)]
    label: Option<String>,
    /// Schema display label.
    #[serde(default)]
    schema_label: Option<String>,
    /// Description.
    #[serde(default)]
    description: Option<String>,
    /// Root-level SQL for derived table / query views.
    #[serde(default)]
    sql: Option<String>,
    /// Dimensions — can be empty `{}` for auto-inferred fields.
    #[serde(default)]
    dimensions: BTreeMap<String, Option<OmniDirDimension>>,
    /// Measures.
    #[serde(default)]
    measures: BTreeMap<String, Option<OmniDirMeasure>>,
    /// Named filters.
    #[serde(default)]
    filters: BTreeMap<String, Option<OmniDirFilter>>,
    /// dbt metadata (read-only, ignored).
    #[serde(default)]
    dbt: Option<serde_yaml::Value>,
}

/// Dimension in the directory format.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniDirDimension {
    /// SQL expression.
    #[serde(default)]
    sql: Option<String>,
    /// Display label.
    #[serde(default)]
    label: Option<String>,
    /// Description.
    #[serde(default)]
    description: Option<String>,
    /// Format hint: ID, number, number_0..3, date, usdaccounting, etc.
    #[serde(default)]
    format: Option<String>,
    /// Is this the primary key?
    #[serde(default)]
    primary_key: Option<bool>,
    /// Hidden from UI.
    #[serde(default)]
    hidden: Option<bool>,
    /// Timeframes for date dimensions (replaces dimension_groups).
    #[serde(default)]
    timeframes: Option<Vec<String>>,
    /// Group label for UI organization.
    #[serde(default)]
    group_label: Option<String>,
    /// View label override.
    #[serde(default)]
    view_label: Option<String>,
}

/// Measure in the directory format.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniDirMeasure {
    /// Aggregation type: count, count_distinct, sum, average, min, max, etc.
    #[serde(default)]
    aggregate_type: Option<String>,
    /// SQL expression.
    #[serde(default)]
    sql: Option<String>,
    /// Display label.
    #[serde(default)]
    label: Option<String>,
    /// Description.
    #[serde(default)]
    description: Option<String>,
    /// Format hint.
    #[serde(default)]
    format: Option<String>,
    /// Hidden from UI.
    #[serde(default)]
    hidden: Option<bool>,
    /// Group label for UI organization.
    #[serde(default)]
    group_label: Option<String>,
    /// Measure filters with operator syntax.
    #[serde(default)]
    filters: Option<BTreeMap<String, OmniMeasureFilterOp>>,
    /// Custom primary key SQL for distinct aggregations.
    #[serde(default)]
    custom_primary_key_sql: Option<String>,
}

/// Filter operators on a measure filter. Supports `is`, `not`, `greater_than`, `less_than`.
/// Can also be a simple string value (deserialized as `is`).
/// Values can be strings or booleans (YAML `true`/`false`) — we normalize to strings.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OmniMeasureFilterOp {
    Simple(String),
    SimpleBool(bool),
    Operators {
        #[serde(default, deserialize_with = "deserialize_yaml_value_as_string")]
        is: Option<String>,
        #[serde(default, deserialize_with = "deserialize_yaml_value_as_string")]
        not: Option<String>,
        #[serde(default, deserialize_with = "deserialize_yaml_value_as_string")]
        greater_than: Option<String>,
        #[serde(default, deserialize_with = "deserialize_yaml_value_as_string")]
        less_than: Option<String>,
    },
}

/// Deserialize a YAML value that can be a string, boolean, or number into Option<String>.
fn deserialize_yaml_value_as_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let val: Option<serde_yaml::Value> = Option::deserialize(deserializer)?;
    Ok(val.and_then(|v| match v {
        serde_yaml::Value::String(s) => Some(s),
        serde_yaml::Value::Bool(b) => Some(b.to_string()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        serde_yaml::Value::Null => None,
        _ => None,
    }))
}

/// Named filter in the directory format.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniDirFilter {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// A relationship entry from relationships.yaml.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniRelationship {
    join_from_view: String,
    join_to_view: String,
    #[serde(default)]
    join_type: Option<String>,
    #[serde(default)]
    on_sql: Option<String>,
    #[serde(default)]
    relationship_type: Option<String>,
    #[serde(default)]
    reversible: Option<bool>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    join_from_view_as_label: Option<String>,
    #[serde(default)]
    join_to_view_as: Option<String>,
}

/// Topic file (*.topic.yaml).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniTopicFile {
    #[serde(default)]
    base_view: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    joins: BTreeMap<String, serde_yaml::Value>,
}

/// model.yaml global config.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniModelFile {
    #[serde(default)]
    included_schemas: Vec<String>,
    #[serde(default)]
    access_grants: BTreeMap<String, serde_yaml::Value>,
}

// ── Legacy single-file format types ─────────────────────────────────

/// Top-level Omni schema file (legacy single-file format).
#[derive(Debug, Deserialize)]
struct OmniLegacyFile {
    #[serde(default)]
    views: BTreeMap<String, OmniLegacyView>,
    #[serde(default)]
    topics: BTreeMap<String, OmniLegacyTopic>,
}

/// A legacy Omni view definition.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniLegacyView {
    #[serde(default, alias = "sql_table_name")]
    sql_table_name: Option<String>,
    #[serde(default)]
    derived_table: Option<OmniLegacyDerivedTable>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    dimensions: BTreeMap<String, OmniLegacyDimension>,
    #[serde(default)]
    dimension_groups: BTreeMap<String, OmniLegacyDimensionGroup>,
    #[serde(default)]
    measures: BTreeMap<String, OmniLegacyMeasure>,
    #[serde(default)]
    filters: BTreeMap<String, OmniLegacyFilter>,
}

#[derive(Debug, Deserialize)]
struct OmniLegacyDerivedTable {
    #[serde(default)]
    sql: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniLegacyDimension {
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
struct OmniLegacyDimensionGroup {
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
#[allow(dead_code)]
struct OmniLegacyMeasure {
    #[serde(rename = "type")]
    measure_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    filters: Option<BTreeMap<String, String>>,
    #[serde(default)]
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniLegacyFilter {
    #[serde(rename = "type", default)]
    filter_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// A legacy Omni topic.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniLegacyTopic {
    #[serde(default)]
    base_view: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    joins: BTreeMap<String, OmniLegacyJoin>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OmniLegacyJoin {
    #[serde(default)]
    sql_on: Option<String>,
    #[serde(default)]
    relationship: Option<String>,
    #[serde(rename = "type", default)]
    join_type: Option<String>,
}

// ── Conversion: single-file entry point ─────────────────────────────

/// Convert a single Omni YAML file into airlayer views.
///
/// Handles both formats:
/// - Directory format: a single `.view.yaml` file with `schema:` + `table_name:`
/// - Legacy format: a file with `views:` top-level map
pub fn convert(content: &str, source: &str) -> Result<ConversionResult, String> {
    // Try directory-format single view first (has `schema:` or `table_name:` at top level)
    if let Ok(view_file) = serde_yaml::from_str::<OmniViewFile>(content) {
        if view_file.schema.is_some() || view_file.table_name.is_some() {
            // Extract view name from filename: "name.view.yaml" or "name.query.view.yaml"
            let view_name = view_name_from_source(source);
            let schema = view_file.schema.as_deref();
            let full_name = if let Some(s) = schema {
                format!("{}__{}", s, view_name)
            } else {
                view_name.to_string()
            };
            let mut warnings = Vec::new();
            let view = convert_dir_view_file(&full_name, &view_file, &mut warnings);
            return Ok(ConversionResult {
                views: vec![view],
                warnings,
            });
        }
    }

    // Try relationships.yaml
    if let Ok(rels) = serde_yaml::from_str::<Vec<OmniRelationship>>(content) {
        if !rels.is_empty() {
            // Relationships are applied at the directory level, not per-file
            return Ok(ConversionResult {
                views: vec![],
                warnings: vec![format!(
                    "Relationships file {} will be applied during directory loading",
                    source
                )],
            });
        }
    }

    // Fall back to legacy single-file format
    let omni_file: OmniLegacyFile = serde_yaml::from_str(content)
        .map_err(|e| format!("Failed to parse Omni schema from {}: {}", source, e))?;

    let mut views = Vec::new();
    let mut warnings = Vec::new();

    for (name, omni_view) in &omni_file.views {
        let view = convert_legacy_view(name, omni_view, &mut warnings);
        views.push(view);
    }

    for (topic_name, topic) in &omni_file.topics {
        apply_legacy_topic_joins(&mut views, topic_name, topic, &mut warnings);
    }

    if views.is_empty() {
        return Err(format!("No views found in Omni file {}", source));
    }

    Ok(ConversionResult { views, warnings })
}

/// Convert an Omni directory into airlayer views.
///
/// Reads model.yaml, all *.view.yaml files from schema subdirectories,
/// relationships.yaml, and *.topic.yaml files.
#[cfg(feature = "cli")]
pub fn convert_directory(dir: &std::path::Path) -> Result<ConversionResult, String> {
    let mut all_views = Vec::new();
    let mut all_warnings = Vec::new();

    // 1. Read model.yaml for included_schemas (optional)
    let model_path = dir.join("model.yaml");
    let _model: Option<OmniModelFile> = if model_path.exists() {
        match std::fs::read_to_string(&model_path) {
            Ok(content) => serde_yaml::from_str(&content).ok(),
            Err(_) => None,
        }
    } else {
        None
    };

    // 2. Find and parse all .view.yaml files in subdirectories
    for pattern_str in ["**/*.view.yaml", "**/*.view.yml"] {
        let pattern = dir.join(pattern_str);
        if let Ok(paths) = glob::glob(pattern.to_str().unwrap_or("")) {
            for entry in paths.flatten() {
                let content = match std::fs::read_to_string(&entry) {
                    Ok(c) => c,
                    Err(e) => {
                        all_warnings.push(format!("Failed to read {}: {}", entry.display(), e));
                        continue;
                    }
                };

                let view_file: OmniViewFile = match serde_yaml::from_str(&content) {
                    Ok(v) => v,
                    Err(e) => {
                        all_warnings.push(format!("Skipping {}: {}", entry.display(), e));
                        continue;
                    }
                };

                // Derive view name from path: schema_dir/name.view.yaml → schema__name
                let view_name = view_name_from_source(entry.to_str().unwrap_or(""));
                let schema = view_file.schema.as_deref();

                // Infer schema from parent directory if not specified in the file
                let inferred_schema = schema.map(String::from).or_else(|| {
                    entry
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|f| f.to_str())
                        .filter(|s| *s != "." && dir.join(s) != dir)
                        .map(String::from)
                });

                let full_name = if let Some(ref s) = inferred_schema {
                    format!("{}__{}", s, view_name)
                } else {
                    view_name.to_string()
                };

                let mut warnings = Vec::new();
                let view = convert_dir_view_file(&full_name, &view_file, &mut warnings);
                all_views.push(view);
                all_warnings.extend(warnings);
            }
        }
    }

    // 3. Parse relationships.yaml and apply joins
    let rel_path = dir.join("relationships.yaml");
    if rel_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&rel_path) {
            match serde_yaml::from_str::<Vec<OmniRelationship>>(&content) {
                Ok(relationships) => {
                    apply_relationships(&mut all_views, &relationships, &mut all_warnings);
                }
                Err(e) => {
                    all_warnings.push(format!("Failed to parse relationships.yaml: {}", e));
                }
            }
        }
    }

    // 4. Parse *.topic.yaml files (informational — topics define field curation)
    let topic_pattern = dir.join("*.topic.yaml");
    if let Ok(paths) = glob::glob(topic_pattern.to_str().unwrap_or("")) {
        for entry in paths.flatten() {
            if let Ok(content) = std::fs::read_to_string(&entry) {
                if let Ok(topic) = serde_yaml::from_str::<OmniTopicFile>(&content) {
                    // Topics in directory format are primarily for field curation,
                    // not join definitions (those come from relationships.yaml)
                    if !topic.joins.is_empty() {
                        if let Some(base) = &topic.base_view {
                            all_warnings.push(format!(
                                "Topic {} has inline joins for base_view '{}' — using relationships.yaml instead",
                                entry.display(),
                                base
                            ));
                        }
                    }
                }
            }
        }
    }

    // Also try legacy single-file format (*.yml, *.yaml in root that have views: key)
    if all_views.is_empty() {
        for ext in ["yml", "yaml"] {
            let pattern = dir.join(format!("*.{}", ext));
            if let Ok(paths) = glob::glob(pattern.to_str().unwrap_or("")) {
                for entry in paths.flatten() {
                    // Skip known non-view files
                    let fname = entry.file_name().and_then(|f| f.to_str()).unwrap_or("");
                    if fname == "model.yaml"
                        || fname == "relationships.yaml"
                        || fname.ends_with(".topic.yaml")
                    {
                        continue;
                    }
                    if let Ok(content) = std::fs::read_to_string(&entry) {
                        if let Ok(result) = convert(&content, entry.to_str().unwrap_or("<unknown>"))
                        {
                            all_views.extend(result.views);
                            all_warnings.extend(result.warnings);
                        }
                    }
                }
            }
        }
    }

    if all_views.is_empty() {
        return Err(format!("No Omni views found in {}", dir.display()));
    }

    Ok(ConversionResult {
        views: all_views,
        warnings: all_warnings,
    })
}

/// Detect whether a directory contains an Omni project (directory format).
#[cfg(feature = "cli")]
pub fn is_omni_directory(dir: &std::path::Path) -> bool {
    // Check for model.yaml or relationships.yaml (strong signal)
    let has_model = dir.join("model.yaml").exists();
    let has_relationships = dir.join("relationships.yaml").exists();

    if has_model || has_relationships {
        // Verify there are .view.yaml files in subdirectories
        let pattern = dir.join("**/*.view.yaml");
        if let Ok(paths) = glob::glob(pattern.to_str().unwrap_or("")) {
            if paths.into_iter().any(|p| p.is_ok()) {
                return true;
            }
        }
    }

    false
}

// ── Directory format conversion ─────────────────────────────────────

/// Extract view name from a file path: "orders.view.yaml" → "orders", "orders.query.view.yaml" → "orders"
fn view_name_from_source(source: &str) -> String {
    let fname = std::path::Path::new(source)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or(source);
    // Strip .view.yaml / .view.yml / .query.view.yaml / .query.view.yml
    fname
        .strip_suffix(".query.view.yaml")
        .or_else(|| fname.strip_suffix(".query.view.yml"))
        .or_else(|| fname.strip_suffix(".view.yaml"))
        .or_else(|| fname.strip_suffix(".view.yml"))
        .unwrap_or(fname)
        .to_string()
}

/// Convert a directory-format view file into an airlayer View.
fn convert_dir_view_file(name: &str, file: &OmniViewFile, warnings: &mut Vec<String>) -> View {
    let mut dimensions = Vec::new();
    let mut measures = Vec::new();

    // Convert dimensions
    for (dim_name, dim_opt) in &file.dimensions {
        let dim = dim_opt.as_ref();
        let mut dims = convert_dir_dimension(dim_name, dim, name, warnings);
        dimensions.append(&mut dims);
    }

    // Convert measures
    for (measure_name, measure_opt) in &file.measures {
        if let Some(m) = convert_dir_measure(measure_name, measure_opt.as_ref(), name, warnings) {
            measures.push(m);
        }
    }

    // Table: schema.table_name, or derived SQL
    let (table, sql) = if let Some(ref sql_str) = file.sql {
        // Derived table / query view
        (None, Some(rewrite_dollar_refs(sql_str, name)))
    } else {
        let table_str = match (&file.schema, &file.table_name) {
            (Some(schema), Some(table)) => format!("{}.{}", schema, table),
            (None, Some(table)) => table.clone(),
            (Some(schema), None) => {
                // Infer table name from the view name (strip schema__ prefix)
                // Try double-underscore first to avoid partial match on single underscore
                let bare = name
                    .strip_prefix(&format!("{}__", schema))
                    .or_else(|| name.strip_prefix(&format!("{}_", schema)))
                    .unwrap_or(name);
                format!("{}.{}", schema, bare)
            }
            (None, None) => name.to_string(),
        };
        (Some(table_str), None)
    };

    // Build entities from primary key dimensions
    let mut entities = Vec::new();
    if let Some(pk_dim) = dimensions.iter().find(|d| d.primary_key == Some(true)) {
        entities.push(Entity {
            name: name.to_string(),
            entity_type: EntityType::Primary,
            lifespan: None,
            description: None,
            key: Some(pk_dim.name.clone()),
            keys: None,
            inherits_from: None,
            meta: None,
            parent: None,
        });
    }

    View {
        name: name.to_string(),
        description: file.description.clone(),
        label: file.label.clone(),
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
        segments: vec![],
        pre_aggregations: None,
        refresh_key: None,
        meta: None,
    }
}

/// Convert a directory-format dimension. Handles bare `{}` dimensions,
/// `format:` type hints, `timeframes:` for date expansion.
fn convert_dir_dimension(
    name: &str,
    dim: Option<&OmniDirDimension>,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Vec<Dimension> {
    let (sql, label, description, format, primary_key, timeframes) = match dim {
        Some(d) => (
            d.sql.as_deref(),
            d.label.as_deref(),
            d.description.as_deref(),
            d.format.as_deref(),
            d.primary_key,
            d.timeframes.as_ref(),
        ),
        None => (None, None, None, None, None, None),
    };

    // If timeframes are specified, expand into multiple date dimensions
    if let Some(tfs) = timeframes {
        if !tfs.is_empty() {
            let sql_expr = sql.unwrap_or(name);
            let rewritten = rewrite_dollar_refs(sql_expr, view_name);
            let tf_strs: Vec<&str> = tfs.iter().map(|s| s.as_str()).collect();
            return expand_dimension_group(
                name,
                "time",
                &rewritten,
                sql,
                description.or(label),
                &tf_strs,
                &[],
            );
        }
    }

    // Infer dimension type from format hint
    let dimension_type = match format {
        Some("ID") | Some("id") => DimensionType::String,
        Some(f) if f.starts_with("number") || f == "usdaccounting" => DimensionType::Number,
        Some("date") => DimensionType::Date,
        Some("boolean") => DimensionType::Boolean,
        _ => {
            // Heuristic: names ending in _at, _date, _time suggest date/time
            if name.ends_with("_at") || name.ends_with("_date") || name.ends_with("_timestamp") {
                DimensionType::Datetime
            } else if name.starts_with("is_") || name.starts_with("has_") {
                DimensionType::Boolean
            } else {
                DimensionType::String
            }
        }
    };

    let expr = sql
        .map(|s| rewrite_dollar_refs(s, view_name))
        .unwrap_or_else(|| name.to_string());

    vec![Dimension {
        name: name.to_string(),
        dimension_type,
        description: description.or(label).map(|s| s.to_string()),
        expr,
        original_expr: sql.map(|s| s.to_string()),
        samples: None,
        synonyms: None,
        primary_key,
        sub_query: None,
        segmentable: None,
        inherits_from: None,
        meta: None,
    }]
}

/// Convert a directory-format measure.
fn convert_dir_measure(
    name: &str,
    measure: Option<&OmniDirMeasure>,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Option<Measure> {
    let (agg_type, sql, label, description, filters_map) = match measure {
        Some(m) => (
            m.aggregate_type.as_deref(),
            m.sql.as_deref(),
            m.label.as_deref(),
            m.description.as_deref(),
            m.filters.as_ref(),
        ),
        None => (None, None, None, None, None),
    };

    let type_str = agg_type.unwrap_or("count");
    let measure_type = parse_foreign_measure_type(type_str);

    let expr = sql.map(|s| rewrite_dollar_refs(s, view_name));

    // Convert filter operators to filter expressions
    let filters = filters_map.and_then(|f| {
        let filter_exprs: Vec<MeasureFilter> = f
            .iter()
            .filter_map(|(field, op)| {
                let filter_expr = match op {
                    OmniMeasureFilterOp::Simple(val) => {
                        if val == "null" {
                            format!("{} IS NULL", field)
                        } else {
                            format!("{} = '{}'", field, val)
                        }
                    }
                    OmniMeasureFilterOp::SimpleBool(val) => {
                        format!("{} = {}", field, val)
                    }
                    OmniMeasureFilterOp::Operators {
                        is,
                        not,
                        greater_than,
                        less_than,
                    } => {
                        let mut parts = Vec::new();
                        if let Some(val) = is {
                            if val == "null" {
                                parts.push(format!("{} IS NULL", field));
                            } else {
                                parts.push(format!("{} = '{}'", field, val));
                            }
                        }
                        if let Some(val) = not {
                            if val == "null" {
                                parts.push(format!("{} IS NOT NULL", field));
                            } else {
                                parts.push(format!("{} != '{}'", field, val));
                            }
                        }
                        if let Some(val) = greater_than {
                            parts.push(format!("{} > {}", field, val));
                        }
                        if let Some(val) = less_than {
                            parts.push(format!("{} < {}", field, val));
                        }
                        if parts.is_empty() {
                            return None;
                        }
                        parts.join(" AND ")
                    }
                };
                Some(MeasureFilter {
                    expr: filter_expr,
                    original_expr: None,
                    description: None,
                })
            })
            .collect();
        if filter_exprs.is_empty() {
            None
        } else {
            Some(filter_exprs)
        }
    });

    Some(Measure {
        name: name.to_string(),
        measure_type,
        description: description.or(label).map(|s| s.to_string()),
        expr,
        original_expr: sql.map(|s| s.to_string()),
        filters,
        samples: None,
        synonyms: None,
        rolling_window: None,
        inherits_from: None,
        meta: None,
        drivers: None,
        shift: None,
    })
}

/// Apply relationships from relationships.yaml to views.
fn apply_relationships(
    views: &mut [View],
    relationships: &[OmniRelationship],
    _warnings: &mut Vec<String>,
) {
    for rel in relationships {
        let entity_type =
            relationship_to_entity_type(rel.relationship_type.as_deref().unwrap_or("many_to_one"));

        let fk = rel
            .on_sql
            .as_ref()
            .and_then(|s| extract_dollar_join_key(s, &rel.join_from_view));

        if let Some(from_view) = views.iter_mut().find(|v| v.name == rel.join_from_view) {
            // Avoid duplicate entities
            if !from_view
                .entities
                .iter()
                .any(|e| e.name == rel.join_to_view)
            {
                // Auto-create implicit dimension for the join key if it doesn't exist
                if let Some(ref key_name) = fk {
                    if !from_view.dimensions.iter().any(|d| d.name == *key_name) {
                        from_view.dimensions.push(Dimension {
                            name: key_name.clone(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: key_name.clone(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: None,
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        });
                    }
                }
                from_view.entities.push(Entity {
                    name: rel.join_to_view.clone(),
                    entity_type,
                    lifespan: None,
                    description: None,
                    key: fk,
                    keys: None,
                    inherits_from: None,
                    meta: None,
                    parent: None,
                });
            }
        }
    }
}

// ── Legacy format conversion ────────────────────────────────────────

fn convert_legacy_view(name: &str, omni: &OmniLegacyView, warnings: &mut Vec<String>) -> View {
    let mut dimensions = Vec::new();
    let mut measures = Vec::new();
    let mut segments = Vec::new();

    for (dim_name, dim) in &omni.dimensions {
        let d = convert_legacy_dimension(dim_name, dim, name, warnings);
        dimensions.push(d);
    }

    for (group_name, group) in &omni.dimension_groups {
        let mut dims = convert_legacy_dimension_group(group_name, group, name, warnings);
        dimensions.append(&mut dims);
    }

    for (measure_name, measure) in &omni.measures {
        if let Some(m) = convert_legacy_measure(measure_name, measure, name, warnings) {
            measures.push(m);
        }
    }

    for (filter_name, filter) in &omni.filters {
        if let Some(seg) = convert_legacy_filter(filter_name, filter, name) {
            segments.push(seg);
        }
    }

    let (table, sql) = if let Some(ref t) = omni.sql_table_name {
        (Some(t.clone()), None)
    } else if let Some(ref dt) = omni.derived_table {
        (None, dt.sql.as_ref().map(|s| rewrite_dollar_refs(s, name)))
    } else {
        (Some(name.to_string()), None)
    };

    let mut entities = Vec::new();
    if let Some(pk_dim) = dimensions.iter().find(|d| d.primary_key == Some(true)) {
        entities.push(Entity {
            name: name.to_string(),
            entity_type: EntityType::Primary,
            lifespan: None,
            description: None,
            key: Some(pk_dim.name.clone()),
            keys: None,
            inherits_from: None,
            meta: None,
            parent: None,
        });
    }

    View {
        name: name.to_string(),
        description: omni.description.clone(),
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
        pre_aggregations: None,
        refresh_key: None,
        meta: None,
    }
}

fn convert_legacy_dimension(
    name: &str,
    dim: &OmniLegacyDimension,
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
        segmentable: None,
        inherits_from: None,
        meta: None,
    }
}

fn convert_legacy_dimension_group(
    name: &str,
    group: &OmniLegacyDimensionGroup,
    view_name: &str,
    _warnings: &mut Vec<String>,
) -> Vec<Dimension> {
    let group_type = group.group_type.as_deref().unwrap_or("time");
    let sql_expr = group.sql.as_deref().unwrap_or(name);
    let rewritten = rewrite_dollar_refs(sql_expr, view_name);

    if group_type == "time" || group_type == "duration" {
        let tf_strs: Vec<&str> = group
            .timeframes
            .as_ref()
            .or(group.intervals.as_ref())
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        expand_dimension_group(
            name,
            group_type,
            &rewritten,
            group.sql.as_deref(),
            group.description.as_deref(),
            &tf_strs,
            &tf_strs,
        )
    } else {
        vec![]
    }
}

fn convert_legacy_measure(
    name: &str,
    measure: &OmniLegacyMeasure,
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
        description: measure
            .description
            .clone()
            .or_else(|| measure.label.clone()),
        expr,
        original_expr: measure.sql.clone(),
        filters,
        samples: None,
        synonyms: None,
        rolling_window,
        inherits_from: None,
        meta: None,
        drivers: None,
        shift: None,
    })
}

fn convert_legacy_filter(
    name: &str,
    filter: &OmniLegacyFilter,
    view_name: &str,
) -> Option<Segment> {
    let sql = filter.sql.as_ref()?;
    Some(Segment {
        name: name.to_string(),
        expr: rewrite_dollar_refs(sql, view_name),
        description: filter.description.clone(),
        inherits_from: None,
        meta: None,
    })
}

fn apply_legacy_topic_joins(
    views: &mut [View],
    _topic_name: &str,
    topic: &OmniLegacyTopic,
    _warnings: &mut Vec<String>,
) {
    let base_view_name = topic.base_view.as_deref().unwrap_or("");

    for (join_name, join) in &topic.joins {
        let entity_type =
            relationship_to_entity_type(join.relationship.as_deref().unwrap_or("many_to_one"));

        let fk = join
            .sql_on
            .as_ref()
            .and_then(|s| extract_dollar_join_key(s, base_view_name));

        if let Some(base_view) = views.iter_mut().find(|v| v.name == base_view_name) {
            base_view.entities.push(Entity {
                name: join_name.to_string(),
                entity_type,
                lifespan: None,
                description: None,
                key: fk,
                keys: None,
                inherits_from: None,
                meta: None,
                parent: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Directory format tests ──────────────────────────────────────

    #[test]
    fn test_convert_dir_view_basic() {
        let yaml = r#"
schema: dbt_prod_fpx
table_name: ax_dim_applicants

dimensions:
  dataset: {}
  applicant_id:
    format: ID
    primary_key: true
  is_hired: {}
  hired_at: {}

measures:
  count:
    aggregate_type: count
"#;

        let result = convert(yaml, "ax_dim_applicants.view.yaml").unwrap();
        assert_eq!(result.views.len(), 1);

        let view = &result.views[0];
        assert_eq!(view.name, "dbt_prod_fpx__ax_dim_applicants");
        assert_eq!(
            view.table,
            Some("dbt_prod_fpx.ax_dim_applicants".to_string())
        );
        assert_eq!(view.dimensions.len(), 4);

        // Check primary key
        let pk = view
            .dimensions
            .iter()
            .find(|d| d.name == "applicant_id")
            .unwrap();
        assert_eq!(pk.primary_key, Some(true));

        // Check type inference for bare dimensions
        let is_hired = view
            .dimensions
            .iter()
            .find(|d| d.name == "is_hired")
            .unwrap();
        assert_eq!(is_hired.dimension_type, DimensionType::Boolean);

        let hired_at = view
            .dimensions
            .iter()
            .find(|d| d.name == "hired_at")
            .unwrap();
        assert_eq!(hired_at.dimension_type, DimensionType::Datetime);

        // Measures
        let measures = view.measures_list();
        assert_eq!(measures.len(), 1);
        assert_eq!(measures[0].measure_type, MeasureType::Count);

        // Primary entity
        assert_eq!(view.entities.len(), 1);
        assert_eq!(view.entities[0].entity_type, EntityType::Primary);
    }

    #[test]
    fn test_convert_dir_view_with_sql() {
        let yaml = r#"
schema: dbt_prod_fpx
table_name: ax_fct_applicants

dimensions:
  applicant_id:
    format: ID
    primary_key: true
  total_days:
    sql: "cast(${time_to_fill_seconds} as float64) / 86400"
    format: number_1

measures:
  count:
    aggregate_type: count
  n_transitions_into:
    aggregate_type: sum
    sql: n_transitions_into
"#;

        let result = convert(yaml, "ax_fct_applicants.view.yaml").unwrap();
        let view = &result.views[0];

        let total_days = view
            .dimensions
            .iter()
            .find(|d| d.name == "total_days")
            .unwrap();
        assert_eq!(total_days.dimension_type, DimensionType::Number);
        assert!(total_days.expr.contains("float64"));

        let sum_measure = view
            .measures_list()
            .iter()
            .find(|m| m.name == "n_transitions_into")
            .unwrap();
        assert_eq!(sum_measure.measure_type, MeasureType::Sum);
    }

    #[test]
    fn test_convert_dir_view_with_timeframes() {
        let yaml = r#"
schema: dbt_prod_fpx
table_name: events

dimensions:
  id:
    primary_key: true
  event_at:
    timeframes: [date, week, month, year]

measures:
  count:
    aggregate_type: count
"#;

        let result = convert(yaml, "events.view.yaml").unwrap();
        let view = &result.views[0];

        // id + 4 expanded date dims
        assert_eq!(view.dimensions.len(), 5);
        assert!(view.dimensions.iter().any(|d| d.name == "event_at_date"));
        assert!(view.dimensions.iter().any(|d| d.name == "event_at_month"));
        assert!(view.dimensions.iter().any(|d| d.name == "event_at_year"));
    }

    #[test]
    fn test_convert_dir_measure_with_filters() {
        let yaml = r#"
schema: dbt_prod_fpx
table_name: applicants

dimensions:
  id:
    primary_key: true
  is_hired: {}

measures:
  hired_count:
    aggregate_type: count
    filters:
      is_hired:
        is: "true"
  active_count:
    aggregate_type: count
    filters:
      status:
        not: "null"
"#;

        let result = convert(yaml, "applicants.view.yaml").unwrap();
        let measures = result.views[0].measures_list();

        let hired = measures.iter().find(|m| m.name == "hired_count").unwrap();
        assert!(hired.filters.is_some());
        assert!(hired.filters.as_ref().unwrap()[0]
            .expr
            .contains("is_hired = 'true'"));

        let active = measures.iter().find(|m| m.name == "active_count").unwrap();
        assert!(active.filters.is_some());
        assert!(active.filters.as_ref().unwrap()[0]
            .expr
            .contains("IS NOT NULL"));
    }

    #[test]
    fn test_convert_dir_derived_table() {
        let yaml = r#"
schema: omni_dbt
sql: |-
  SELECT id, name FROM users WHERE active = true

dimensions:
  id:
    primary_key: true
  name: {}

measures:
  count:
    aggregate_type: count
"#;

        let result = convert(yaml, "active_users.query.view.yaml").unwrap();
        let view = &result.views[0];
        assert!(view.table.is_none());
        assert!(view.sql.is_some());
        assert!(view.sql.as_ref().unwrap().contains("SELECT id"));
    }

    #[test]
    fn test_view_name_from_source() {
        assert_eq!(
            view_name_from_source("ax_dim_applicants.view.yaml"),
            "ax_dim_applicants"
        );
        assert_eq!(
            view_name_from_source("active_users.query.view.yaml"),
            "active_users"
        );
        assert_eq!(
            view_name_from_source("/path/to/schema/name.view.yaml"),
            "name"
        );
        assert_eq!(view_name_from_source("name.view.yml"), "name");
    }

    // ── Legacy format tests (preserved from original) ───────────────

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
        assert_eq!(rewrite_dollar_refs("${users.id}", "orders"), "{{users.id}}");
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

    /// Test: directory-format view with boolean measure filter (e.g., `is: true`).
    /// Real-world Omni repos use boolean YAML values in filter operators.
    #[test]
    fn test_dir_view_boolean_measure_filter() {
        let yaml = r#"
schema: analytics
table_name: applicants
dimensions:
  id:
    primary_key: true
  is_hired:
    format: boolean
measures:
  hired_count:
    aggregate_type: count
    sql: ${id}
    filters:
      is_hired:
        is: true
  not_hired_count:
    aggregate_type: count
    sql: ${id}
    filters:
      is_hired:
        is: false
"#;
        let result = convert(yaml, "applicants.view.yaml").unwrap();
        assert_eq!(result.views.len(), 1);
        let view = &result.views[0];

        let hired = view
            .measures_list()
            .iter()
            .find(|m| m.name == "hired_count")
            .unwrap();
        let filters = hired.filters.as_ref().expect("Should have filters");
        assert_eq!(filters.len(), 1);
        assert!(
            filters[0].expr.contains("is_hired"),
            "Filter should reference is_hired: {}",
            filters[0].expr
        );

        let not_hired = view
            .measures_list()
            .iter()
            .find(|m| m.name == "not_hired_count")
            .unwrap();
        let filters = not_hired.filters.as_ref().expect("Should have filters");
        assert!(
            filters[0].expr.contains("is_hired"),
            "Filter should reference is_hired: {}",
            filters[0].expr
        );
    }

    /// Test: directory-format view with operator-based filter using mixed types.
    #[test]
    fn test_dir_view_operator_filter_mixed_types() {
        let yaml = r#"
schema: prod
table_name: events
dimensions:
  id:
    primary_key: true
  amount: {}
  status: {}
measures:
  big_completed_count:
    aggregate_type: count
    filters:
      status:
        is: completed
      amount:
        greater_than: 100
"#;
        let result = convert(yaml, "events.view.yaml").unwrap();
        let view = &result.views[0];
        let m = view
            .measures_list()
            .iter()
            .find(|m| m.name == "big_completed_count")
            .unwrap();
        let filters = m.filters.as_ref().expect("Should have filters");
        assert_eq!(filters.len(), 2);
    }

    /// Test: bare `{}` dimensions infer type from name heuristics.
    #[test]
    fn test_dir_view_bare_dimension_type_inference() {
        let yaml = r#"
schema: prod
table_name: users
dimensions:
  id:
    primary_key: true
  name: {}
  created_at: {}
  is_active: {}
  has_email: {}
  age: {}
"#;
        let result = convert(yaml, "users.view.yaml").unwrap();
        let view = &result.views[0];

        let created = view
            .dimensions
            .iter()
            .find(|d| d.name == "created_at")
            .unwrap();
        assert_eq!(created.dimension_type, DimensionType::Datetime);

        let is_active = view
            .dimensions
            .iter()
            .find(|d| d.name == "is_active")
            .unwrap();
        assert_eq!(is_active.dimension_type, DimensionType::Boolean);

        let has_email = view
            .dimensions
            .iter()
            .find(|d| d.name == "has_email")
            .unwrap();
        assert_eq!(has_email.dimension_type, DimensionType::Boolean);

        let name = view.dimensions.iter().find(|d| d.name == "name").unwrap();
        assert_eq!(name.dimension_type, DimensionType::String);
    }

    /// Test: schema-qualified table names use schema.table_name format.
    #[test]
    fn test_dir_view_schema_qualified_table() {
        let yaml = r#"
schema: dbt_prod
table_name: dim_orders
dimensions:
  id:
    primary_key: true
"#;
        let result = convert(yaml, "dim_orders.view.yaml").unwrap();
        let view = &result.views[0];
        assert_eq!(view.table.as_deref(), Some("dbt_prod.dim_orders"));
        assert_eq!(view.name, "dbt_prod__dim_orders");
    }

    /// Test: table name inference from view name when only schema is given.
    #[test]
    fn test_dir_view_table_inferred_from_name() {
        let yaml = r#"
schema: dbt_prod
dimensions:
  id: {}
"#;
        let result = convert(yaml, "orders.view.yaml").unwrap();
        let view = &result.views[0];
        // name = dbt_prod__orders, table should be dbt_prod.orders
        assert_eq!(view.name, "dbt_prod__orders");
        assert_eq!(view.table.as_deref(), Some("dbt_prod.orders"));
    }

    /// Test: timeframes on individual dimensions (Omni directory format).
    #[test]
    fn test_dir_view_dimension_timeframes() {
        let yaml = r#"
schema: prod
table_name: events
dimensions:
  id:
    primary_key: true
  created_at:
    sql: ${TABLE}.created_at
    timeframes:
      - date
      - month
      - year
"#;
        let result = convert(yaml, "events.view.yaml").unwrap();
        let view = &result.views[0];

        // Should expand into 3 dimensions: created_at_date, created_at_month, created_at_year
        assert!(view.dimensions.iter().any(|d| d.name == "created_at_date"));
        assert!(view.dimensions.iter().any(|d| d.name == "created_at_month"));
        assert!(view.dimensions.iter().any(|d| d.name == "created_at_year"));
        // Original name should NOT be present as a standalone dimension
        assert!(!view.dimensions.iter().any(|d| d.name == "created_at"));
    }

    /// Test: relationships auto-create implicit dimensions for join keys.
    #[test]
    fn test_relationships_auto_create_join_key_dimension() {
        let mut views = vec![View {
            name: "orders".to_string(),
            description: None,
            label: None,
            datasource: None,
            dialect: None,
            table: Some("orders".to_string()),
            sql: None,
            entities: vec![],
            dimensions: vec![Dimension {
                name: "id".to_string(),
                dimension_type: DimensionType::Number,
                description: None,
                expr: "id".to_string(),
                original_expr: None,
                samples: None,
                synonyms: None,
                primary_key: Some(true),
                sub_query: None,
                segmentable: None,
                inherits_from: None,
                meta: None,
            }],
            // No user_id dimension declared
            measures: None,
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }];

        let relationships = vec![OmniRelationship {
            join_from_view: "orders".to_string(),
            join_to_view: "users".to_string(),
            join_type: None,
            on_sql: Some("${orders.user_id} = ${users.id}".to_string()),
            relationship_type: Some("many_to_one".to_string()),
            reversible: None,
            id: None,
            join_from_view_as_label: None,
            join_to_view_as: None,
        }];

        let mut warnings = Vec::new();
        apply_relationships(&mut views, &relationships, &mut warnings);

        // Entity should be created
        assert_eq!(views[0].entities.len(), 1);
        assert_eq!(views[0].entities[0].key, Some("user_id".to_string()));

        // Implicit dimension should be auto-created for user_id
        assert!(
            views[0].dimensions.iter().any(|d| d.name == "user_id"),
            "Should auto-create user_id dimension for join key"
        );
        // Original dimension should still exist
        assert!(views[0].dimensions.iter().any(|d| d.name == "id"));
    }

    #[test]
    fn test_convert_relationships() {
        let mut views = vec![
            View {
                name: "orders".to_string(),
                description: None,
                label: None,
                datasource: None,
                dialect: None,
                table: Some("orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![],
                measures: None,
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            },
            View {
                name: "users".to_string(),
                description: None,
                label: None,
                datasource: None,
                dialect: None,
                table: Some("users".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![],
                measures: None,
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            },
        ];

        let relationships = vec![OmniRelationship {
            join_from_view: "orders".to_string(),
            join_to_view: "users".to_string(),
            join_type: Some("always_left".to_string()),
            on_sql: Some("${orders.user_id} = ${users.id}".to_string()),
            relationship_type: Some("many_to_one".to_string()),
            reversible: None,
            id: None,
            join_from_view_as_label: None,
            join_to_view_as: None,
        }];

        let mut warnings = Vec::new();
        apply_relationships(&mut views, &relationships, &mut warnings);

        let orders = &views[0];
        assert_eq!(orders.entities.len(), 1);
        assert_eq!(orders.entities[0].name, "users");
        assert_eq!(orders.entities[0].entity_type, EntityType::Foreign);
        assert_eq!(orders.entities[0].key, Some("user_id".to_string()));
    }

    #[test]
    #[ignore] // requires external repo at ~/customer-repos/fountain/foxy-semantic/omni
    #[cfg(feature = "cli")]
    fn test_real_world_fountain_omni() {
        let dir = std::path::PathBuf::from(env!("HOME"))
            .join("customer-repos/fountain/foxy-semantic/omni");
        if !dir.exists() {
            eprintln!("Skipping: Fountain Omni repo not found");
            return;
        }

        assert!(is_omni_directory(&dir));

        let result = convert_directory(&dir).expect("Should convert");
        eprintln!(
            "Views: {}, Warnings: {}",
            result.views.len(),
            result.warnings.len()
        );
        assert!(result.views.len() > 10);

        // Check schema.table qualification
        let applicants = result
            .views
            .iter()
            .find(|v| v.name.contains("ax_dim_applicants") && !v.name.contains("label"))
            .expect("Should find applicants");
        eprintln!("Applicants table: {:?}", applicants.table);
        assert!(applicants.table.as_ref().unwrap().contains('.'));

        // Check relationships
        let with_entities: usize = result
            .views
            .iter()
            .filter(|v| !v.entities.is_empty())
            .count();
        eprintln!("Views with entities: {}", with_entities);
        assert!(with_entities > 5);

        let total_dims: usize = result.views.iter().map(|v| v.dimensions.len()).sum();
        let total_measures: usize = result.views.iter().map(|v| v.measures_list().len()).sum();
        eprintln!("Total dims: {}, measures: {}", total_dims, total_measures);
    }

    #[test]
    #[ignore] // requires external repo at ~/customer-repos/fountain/foxy-semantic/omni
    #[cfg(feature = "cli")]
    fn test_fountain_omni_query_compilation() {
        use crate::dialect::Dialect;
        use crate::engine::query::QueryRequest;
        use crate::engine::{DatasourceDialectMap, SemanticEngine};
        use crate::schema::models::SemanticLayer;

        let dir = std::path::PathBuf::from(env!("HOME"))
            .join("customer-repos/fountain/foxy-semantic/omni");
        if !dir.exists() {
            eprintln!("Skipping: Fountain Omni repo not found");
            return;
        }

        let result = convert_directory(&dir).expect("Should convert Omni directory");
        eprintln!("Loaded {} views", result.views.len());

        // Build engine with Snowflake dialect (Fountain uses Snowflake)
        let layer = SemanticLayer::new(result.views, None);
        let dialects = DatasourceDialectMap::with_default(Dialect::Snowflake);
        let engine =
            SemanticEngine::from_semantic_layer(layer, dialects).expect("Should build engine");

        // 1. Dimension-only query
        let request = QueryRequest {
            dimensions: vec![
                "dbt_prod_fpx__ax_dim_applicants.current_stage_name".to_string(),
                "dbt_prod_fpx__ax_dim_applicants.is_hired".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&request)
            .expect("Dimension-only query should compile");
        eprintln!("Dimension-only SQL:\n{}", result.sql);
        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("dbt_prod_fpx.ax_dim_applicants"));

        // 2. Measure query (count grouped by dimension)
        let request = QueryRequest {
            measures: vec!["dbt_prod_fpx__ax_dim_applicants.count".to_string()],
            dimensions: vec!["dbt_prod_fpx__ax_dim_applicants.current_stage_name".to_string()],
            ..QueryRequest::new()
        };
        let result = engine
            .compile_query(&request)
            .expect("Measure query should compile");
        eprintln!("Measure SQL:\n{}", result.sql);
        assert!(result.sql.contains("COUNT("));
        assert!(result.sql.contains("GROUP BY"));

        // 3. Bulk compilation: try every view that has measures
        let views = engine.views();
        let views_with_measures: Vec<_> = views
            .iter()
            .filter(|v| !v.measures_list().is_empty())
            .collect();
        eprintln!(
            "Compiling {} views with measures...",
            views_with_measures.len()
        );

        let mut compiled = 0;
        let mut failed = 0;
        for view in &views_with_measures {
            let first_measure = &view.measures_list()[0];
            let request = QueryRequest {
                measures: vec![format!("{}.{}", view.name, first_measure.name)],
                ..QueryRequest::new()
            };
            match engine.compile_query(&request) {
                Ok(r) => {
                    assert!(r.sql.contains("SELECT"));
                    compiled += 1;
                }
                Err(e) => {
                    eprintln!("  FAIL {}: {}", view.name, e);
                    failed += 1;
                }
            }
        }
        eprintln!(
            "Compiled: {}/{}, Failed: {}",
            compiled,
            views_with_measures.len(),
            failed
        );
        // All views with measures should compile
        assert_eq!(failed, 0, "Some measure queries failed to compile");
    }
}
