//! Foreign semantic model parsers.
//!
//! Converts external semantic layer formats (Cube.js, LookML, dbt MetricFlow, Omni)
//! into airlayer's native `View` representation, enabling SQL compilation from any
//! supported modeling language.

pub mod cube;
pub mod dbt;
pub mod lookml;
pub mod omni;

use crate::schema::models::*;
use regex::Regex;
use std::sync::LazyLock;

/// Supported foreign semantic model formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignFormat {
    /// Cube.js YAML schema (cubes with dimensions, measures, joins).
    Cube,
    /// Looker LookML (.lkml files with views, explores, dimension_groups).
    LookML,
    /// dbt MetricFlow (semantic_models with entities, measures, metrics).
    Dbt,
    /// Omni Analytics YAML (views with dimensions, measures, topics).
    Omni,
}

impl ForeignFormat {
    /// Parse a format name string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cube" | "cubejs" | "cube.js" | "cube_js" => Some(Self::Cube),
            "lookml" | "looker" | "lkml" => Some(Self::LookML),
            "dbt" | "metricflow" | "dbt_metricflow" => Some(Self::Dbt),
            "omni" | "omni_analytics" => Some(Self::Omni),
            _ => None,
        }
    }

    /// File extensions typically used by this format.
    pub fn extensions(&self) -> &[&str] {
        match self {
            Self::Cube => &["yml", "yaml", "js"],
            Self::LookML => &["lkml"],
            Self::Dbt => &["yml", "yaml"],
            Self::Omni => &["yml", "yaml"],
        }
    }

    /// Human-readable name.
    pub fn display_name(&self) -> &str {
        match self {
            Self::Cube => "Cube.js",
            Self::LookML => "LookML",
            Self::Dbt => "dbt MetricFlow",
            Self::Omni => "Omni",
        }
    }
}

impl std::fmt::Display for ForeignFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Result of converting a foreign model — may produce multiple views plus diagnostics.
#[derive(Debug)]
pub struct ConversionResult {
    /// Converted airlayer views.
    pub views: Vec<View>,
    /// Non-fatal warnings encountered during conversion.
    pub warnings: Vec<String>,
}

/// Convert a foreign model string into airlayer views.
///
/// # Arguments
/// * `format` - The source format
/// * `content` - The raw file content (YAML, LookML, etc.)
/// * `source` - Source file path (for error messages)
pub fn convert(format: ForeignFormat, content: &str, source: &str) -> Result<ConversionResult, String> {
    match format {
        ForeignFormat::Cube => cube::convert(content, source),
        ForeignFormat::LookML => lookml::convert(content, source),
        ForeignFormat::Dbt => dbt::convert(content, source),
        ForeignFormat::Omni => omni::convert(content, source),
    }
}

/// Convert all files in a directory for the given format.
#[cfg(feature = "cli")]
pub fn convert_directory(
    format: ForeignFormat,
    dir: &std::path::Path,
) -> Result<ConversionResult, String> {
    let mut all_views = Vec::new();
    let mut all_warnings = Vec::new();

    let extensions = format.extensions();
    for ext in extensions {
        let pattern = dir.join(format!("**/*.{}", ext));
        let pattern_str = pattern.to_str().ok_or("Invalid path encoding")?;
        for entry in glob::glob(pattern_str).map_err(|e| format!("Glob error: {}", e))? {
            let path = entry.map_err(|e| format!("Path error: {}", e))?;
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
            match convert(format, &content, path.to_str().unwrap_or("<unknown>")) {
                Ok(result) => {
                    all_views.extend(result.views);
                    all_warnings.extend(result.warnings);
                }
                Err(e) => {
                    all_warnings.push(format!("Skipping {}: {}", path.display(), e));
                }
            }
        }
    }

    if all_views.is_empty() && !all_warnings.is_empty() {
        return Err(format!(
            "No views converted from {} files in {}. Warnings:\n{}",
            format,
            dir.display(),
            all_warnings.join("\n")
        ));
    }

    Ok(ConversionResult {
        views: all_views,
        warnings: all_warnings,
    })
}

// ── Shared helpers for foreign parsers ───────────────────────────────

/// Map a foreign dimension type string to airlayer's DimensionType.
/// Handles all known foreign format type names.
pub(crate) fn parse_foreign_dimension_type(s: &str) -> DimensionType {
    match s.to_lowercase().as_str() {
        "string" | "" | "categorical" | "category" | "zipcode" | "location" => {
            DimensionType::String
        }
        "number" | "tier" => DimensionType::Number,
        "time" | "datetime" | "date_time" => DimensionType::Datetime,
        "date" => DimensionType::Date,
        "boolean" | "yesno" | "yes_no" => DimensionType::Boolean,
        "geo" => DimensionType::Geo,
        _ => DimensionType::String,
    }
}

/// Map a foreign measure type string to airlayer's MeasureType.
pub(crate) fn parse_foreign_measure_type(s: &str) -> MeasureType {
    match s.to_lowercase().as_str() {
        "count" => MeasureType::Count,
        "sum" => MeasureType::Sum,
        "avg" | "average" => MeasureType::Average,
        "min" => MeasureType::Min,
        "max" => MeasureType::Max,
        "count_distinct" | "countdistinct" | "countDistinct" => MeasureType::CountDistinct,
        "count_distinct_approx" | "countdistinctapprox" | "countDistinctApprox" => {
            MeasureType::CountDistinctApprox
        }
        "median" | "percentile" => MeasureType::Median,
        "number" => MeasureType::Number,
        "run_total" | "runtotal" | "running_total" | "sum_boolean" => MeasureType::Sum,
        "sum_distinct" | "average_distinct" => MeasureType::Average,
        "list" | "string" => MeasureType::Custom,
        _ => MeasureType::Custom,
    }
}

/// Map a relationship string (from joins) to airlayer's EntityType.
pub(crate) fn relationship_to_entity_type(rel: &str) -> EntityType {
    match rel {
        "belongs_to" | "belongsTo" | "many_to_one" => EntityType::Foreign,
        "has_many" | "hasMany" | "one_to_many" => EntityType::Primary,
        "has_one" | "hasOne" | "one_to_one" => EntityType::Foreign,
        _ => EntityType::Foreign,
    }
}

// ── Shared regex patterns (compiled once) ────────────────────────────

/// `${TABLE}.col` / `${view.field}` / `${field}` — used by LookML and Omni.
static RE_DOLLAR_TABLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{TABLE\}\.").unwrap());
static RE_DOLLAR_VIEW_FIELD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{(\w+)\.(\w+)\}").unwrap());
static RE_DOLLAR_FIELD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{(\w+)\}").unwrap());

/// Rewrite `${TABLE}.col` / `${view.field}` references (LookML & Omni syntax).
pub(crate) fn rewrite_dollar_refs(sql: &str, self_view: &str) -> String {
    let result = RE_DOLLAR_TABLE.replace_all(sql, "");

    let result = RE_DOLLAR_VIEW_FIELD
        .replace_all(&result, |caps: &regex::Captures| {
            let view = &caps[1];
            let field = &caps[2];
            if view == self_view {
                field.to_string()
            } else {
                format!("{{{{{}.{}}}}}", view, field)
            }
        })
        .to_string();

    RE_DOLLAR_FIELD
        .replace_all(&result, |caps: &regex::Captures| caps[1].to_string())
        .to_string()
}

/// Extract join key from `${view.field}` join expressions (LookML & Omni).
pub(crate) fn extract_dollar_join_key(sql_on: &str, base_view: &str) -> Option<String> {
    for caps in RE_DOLLAR_VIEW_FIELD.captures_iter(sql_on) {
        let view = &caps[1];
        let field = &caps[2];
        if view == base_view {
            return Some(field.to_string());
        }
    }
    None
}

/// `{CUBE}.col` / `{view.col}` — used by Cube.js.
static RE_CUBE_REF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{(\w+)\}\.(\w+)").unwrap());

/// Rewrite `{CUBE}.col` / `{view.col}` references (Cube.js syntax).
pub(crate) fn rewrite_cube_refs(sql: &str, self_cube: &str) -> String {
    RE_CUBE_REF
        .replace_all(sql, |caps: &regex::Captures| {
            let ref_name = &caps[1];
            let column = &caps[2];
            match ref_name {
                "CUBE" | "TABLE" => column.to_string(),
                name if name == self_cube => column.to_string(),
                other => format!("{{{{{}.{}}}}}", other, column),
            }
        })
        .to_string()
}

/// Extract join key from `{CUBE}.col` join expressions (Cube.js).
pub(crate) fn extract_cube_join_key(join_sql: &str, self_cube: &str) -> Option<String> {
    for caps in RE_CUBE_REF.captures_iter(join_sql) {
        let ref_name = &caps[1];
        let column = &caps[2];
        if ref_name == "CUBE" || ref_name == "TABLE" || ref_name == self_cube {
            return Some(column.to_string());
        }
    }
    None
}

/// dbt Jinja patterns.
static RE_DBT_JINJA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{\s*(?:Dimension|TimeDimension)\s*\(\s*['"](\w+__)?(\w+)['"](?:,\s*['"]?\w+['"]?)?\s*\)\s*\}\}"#).unwrap()
});
static RE_DBT_REF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"ref\(['"](\w+)['"]\)"#).unwrap());
static RE_DBT_SOURCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"source\(['"](\w+)['"],\s*['"](\w+)['"]\)"#).unwrap());

/// Rewrite dbt Jinja filter expressions.
pub(crate) fn rewrite_dbt_jinja(expr: &str) -> String {
    RE_DBT_JINJA
        .replace_all(expr, |caps: &regex::Captures| caps[2].to_string())
        .to_string()
}

/// Default time dimension timeframes.
pub(crate) const DEFAULT_TIMEFRAMES: &[&str] =
    &["raw", "time", "date", "week", "month", "quarter", "year"];

/// Default duration intervals.
pub(crate) const DEFAULT_DURATION_INTERVALS: &[&str] = &["day", "hour", "minute"];

/// Expand a dimension group into individual dimensions (one per timeframe or interval).
pub(crate) fn expand_dimension_group(
    name: &str,
    group_type: &str,
    sql_expr: &str,
    original_sql: Option<&str>,
    description: Option<&str>,
    timeframes: &[&str],
    intervals: &[&str],
) -> Vec<Dimension> {
    if group_type == "time" {
        let tfs = if timeframes.is_empty() {
            DEFAULT_TIMEFRAMES
        } else {
            timeframes
        };
        tfs.iter()
            .map(|tf| {
                let dimension_type = match *tf {
                    "raw" | "time" => DimensionType::Datetime,
                    "date" | "week" | "month" | "quarter" | "year" => DimensionType::Date,
                    _ => DimensionType::String,
                };
                Dimension {
                    name: format!("{}_{}", name, tf),
                    dimension_type,
                    description: description.map(|s| s.to_string()),
                    expr: sql_expr.to_string(),
                    original_expr: original_sql.map(|s| s.to_string()),
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                    meta: None,
                }
            })
            .collect()
    } else if group_type == "duration" {
        let ivs = if intervals.is_empty() {
            DEFAULT_DURATION_INTERVALS
        } else {
            intervals
        };
        ivs.iter()
            .map(|interval| Dimension {
                name: format!("{}_{}", name, interval),
                dimension_type: DimensionType::Number,
                description: description.map(|s| s.to_string()),
                expr: sql_expr.to_string(),
                original_expr: original_sql.map(|s| s.to_string()),
                samples: None,
                synonyms: None,
                primary_key: None,
                sub_query: None,
                inherits_from: None,
                meta: None,
            })
            .collect()
    } else {
        vec![]
    }
}
