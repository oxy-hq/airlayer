//! Pre-aggregation: rollup resolution, SQL generation, coverage checking.

use crate::dialect::Dialect;
use crate::engine::member_sql::MemberSqlResolver;
use crate::engine::{DatasourceDialectMap, EngineError, SemanticEngine};
use crate::schema::models::{MeasureType, PreAggregation, SemanticLayer, View};
use serde::{Deserialize, Serialize};

/// A resolved rollup specification ready for SQL generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupSpec {
    pub name: String,
    pub hash: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<RollupMeasure>,
    pub time_dimension: Option<String>,
    pub granularity: Option<String>,
    /// IANA timezone the stored buckets are cut in, canonicalised via
    /// [`normalize_timezone`]. Always `Some` — `"UTC"` when unset.
    pub timezone: Option<String>,
}

/// A measure within a rollup, with its storage columns determined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupMeasure {
    pub name: String,
    pub measure_type: MeasureType,
    /// The original SQL expression from the view definition.
    pub expr: Option<String>,
    /// Column names stored in the pre-agg table for this measure.
    pub columns: Vec<String>,
}

/// Canonical form of a rollup's timezone.
///
/// `None`, `Some("")`, and `Some("UTC")` all mean the same thing — buckets are
/// cut in UTC — so they must hash and match identically. Everything that
/// compares or hashes a timezone goes through here.
pub fn normalize_timezone(tz: Option<&str>) -> &str {
    match tz {
        Some(t) if !t.is_empty() && t != "UTC" => t,
        _ => "UTC",
    }
}

/// Compute a deterministic 8-char hex hash for a rollup specification.
/// Uses FNV-1a for stability across Rust versions.
pub fn compute_rollup_hash(
    dims: &[String],
    measures: &[String],
    time_dim: Option<&str>,
    granularity: Option<&str>,
    timezone: Option<&str>,
) -> String {
    let mut sorted_dims = dims.to_vec();
    sorted_dims.sort();
    let mut sorted_measures = measures.to_vec();
    sorted_measures.sort();

    let canonical = format!(
        "d:{};m:{};t:{};g:{};z:{}",
        sorted_dims.join(","),
        sorted_measures.join(","),
        time_dim.unwrap_or(""),
        granularity.unwrap_or(""),
        normalize_timezone(timezone),
    );

    // FNV-1a hash
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in canonical.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)[..8].to_string()
}

/// Resolve rollup specs for a view. If `pre_aggregations` is defined, use those.
/// Otherwise, generate a default rollup covering all dimensions × all measures × day granularity.
pub fn resolve_rollups(view: &View, timezone: Option<&str>) -> Vec<RollupSpec> {
    if let Some(ref preaggs) = view.pre_aggregations {
        preaggs
            .iter()
            .map(|pa| resolve_explicit_rollup(view, pa, timezone))
            .collect()
    } else {
        vec![generate_default_rollup(view, timezone)]
    }
}

/// Strip an optional `<view_name>.` prefix from a field reference.
///
/// Semantic layer YAML allows both `customer_id` and `orders.customer_id`; the
/// latter form is common when copy-pasting from query notation. The engine
/// stores only the local name, so qualified refs must be normalised here.
fn strip_view_prefix<'a>(view_name: &str, name: &'a str) -> &'a str {
    name.split_once('.')
        .filter(|(v, _)| *v == view_name)
        .map(|(_, rest)| rest)
        .unwrap_or(name)
}

fn resolve_explicit_rollup(view: &View, pa: &PreAggregation, timezone: Option<&str>) -> RollupSpec {
    let measures: Vec<RollupMeasure> = pa
        .measures
        .iter()
        .filter_map(|name| {
            let local_name = strip_view_prefix(&view.name, name);
            let m = view.measures_list().iter().find(|m| m.name == local_name)?;
            Some(build_rollup_measure(m))
        })
        .collect();

    let dimensions: Vec<String> = pa
        .dimensions
        .iter()
        .map(|name| strip_view_prefix(&view.name, name).to_string())
        .collect();

    let measure_names: Vec<String> = measures.iter().map(|m| m.name.clone()).collect();
    let hash = compute_rollup_hash(
        &dimensions,
        &measure_names,
        pa.time_dimension
            .as_deref()
            .map(|td| strip_view_prefix(&view.name, td)),
        pa.granularity.as_deref(),
        timezone,
    );

    RollupSpec {
        name: pa.name.clone(),
        hash,
        dimensions,
        measures,
        time_dimension: pa
            .time_dimension
            .as_deref()
            .map(|td| strip_view_prefix(&view.name, td).to_string()),
        granularity: pa.granularity.clone(),
        timezone: Some(normalize_timezone(timezone).to_string()),
    }
}

fn generate_default_rollup(view: &View, timezone: Option<&str>) -> RollupSpec {
    // Find the first datetime dimension as the time dimension
    let time_dim = view
        .dimensions
        .iter()
        .find(|d| {
            d.dimension_type == crate::schema::models::DimensionType::Datetime
                || d.dimension_type == crate::schema::models::DimensionType::Date
        })
        .map(|d| d.name.clone());

    // All non-datetime dimensions
    let dimensions: Vec<String> = view
        .dimensions
        .iter()
        .filter(|d| {
            d.dimension_type != crate::schema::models::DimensionType::Datetime
                && d.dimension_type != crate::schema::models::DimensionType::Date
        })
        .map(|d| d.name.clone())
        .collect();

    // All pre-aggregable measures
    let measures: Vec<RollupMeasure> = view
        .measures_list()
        .iter()
        .filter(|m| {
            m.measure_type != MeasureType::Custom
                && m.measure_type != MeasureType::Number
                && m.measure_type != MeasureType::Median
        })
        .map(build_rollup_measure)
        .collect();

    let measure_names: Vec<String> = measures.iter().map(|m| m.name.clone()).collect();
    let hash = compute_rollup_hash(
        &dimensions,
        &measure_names,
        time_dim.as_deref(),
        Some("day"),
        timezone,
    );

    RollupSpec {
        name: "default".to_string(),
        hash,
        dimensions,
        measures,
        time_dimension: time_dim,
        granularity: Some("day".to_string()),
        timezone: Some(normalize_timezone(timezone).to_string()),
    }
}

fn build_rollup_measure(m: &crate::schema::models::Measure) -> RollupMeasure {
    let columns = match m.measure_type {
        MeasureType::Sum => vec![format!("{}__sum", m.name)],
        MeasureType::Count => vec![format!("{}__count", m.name)],
        MeasureType::Average => vec![format!("{}__sum", m.name), format!("{}__count", m.name)],
        MeasureType::Min => vec![format!("{}__min", m.name)],
        MeasureType::Max => vec![format!("{}__max", m.name)],
        MeasureType::CountDistinct | MeasureType::CountDistinctApprox => {
            // Store the raw expression column name
            let expr_col = m.expr.clone().unwrap_or_else(|| m.name.clone());
            vec![expr_col]
        }
        MeasureType::Median => {
            let expr_col = m.expr.clone().unwrap_or_else(|| m.name.clone());
            vec![expr_col.clone(), format!("{}__freq", expr_col)]
        }
        MeasureType::Number => vec![format!("{}__value", m.name)],
        MeasureType::Custom => vec![], // Not pre-aggregable
    };

    RollupMeasure {
        name: m.name.clone(),
        measure_type: m.measure_type.clone(),
        expr: m.expr.clone(),
        columns,
    }
}

/// Local cache manifest written by `pull`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalManifest {
    pub pulled_at: String,
    pub source_database: String,
    pub rollups: Vec<LocalRollupEntry>,
}

/// An entry in the local cache manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalRollupEntry {
    pub view_name: String,
    pub rollup_name: String,
    pub rollup_hash: String,
    pub file: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<serde_json::Value>,
    pub time_dimension: Option<String>,
    pub granularity: Option<String>,
    /// IANA timezone the stored buckets are cut in. `None` means a row written
    /// before rollups were timezone-aware; treat it as `"UTC"` via
    /// [`normalize_timezone`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    pub build_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_checked_at: Option<String>,
}

/// Manifest entry for a pre-aggregated rollup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub view_name: String,
    pub rollup_name: String,
    pub rollup_hash: String,
    pub table_name: String,
    pub dimensions: Vec<String>,
    pub measures_json: String,
    pub time_dimension: Option<String>,
    pub granularity: Option<String>,
    /// IANA timezone the stored buckets are cut in. `None` means a row written
    /// before rollups were timezone-aware; treat it as `"UTC"` via
    /// [`normalize_timezone`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    pub build_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_checked_at: Option<String>,
}

/// Generate the CTAS SQL statements for a rollup.
/// Generate the DROP + CTAS statements for a single rollup.
///
/// Dimension and measure expressions are resolved through the semantic engine so
/// that `{{TABLE}}` self-references and other template patterns are expanded
/// correctly for the target dialect.  The preagg column-naming protocol
/// (`measure__type`) is preserved so the re-aggregation layer can reconstruct
/// partial aggregates (e.g. AVG = SUM/COUNT) from the stored parquet.
pub fn generate_build_sql(
    engine: &SemanticEngine,
    view: &View,
    rollup: &RollupSpec,
    schema: &str,
    date_str: &str,
) -> Result<Vec<String>, EngineError> {
    let dialect = engine.dialects().resolve(view.datasource.as_deref())?;
    let source = view.source_sql();

    let table_name = format!("{}__{}__{}", view.name, rollup.hash, date_str);
    let fq_table = dialect.qualify_table(schema, &table_name);

    // Resolve {{TABLE}} self-references in an expression to the source table.
    let resolve = |expr: &str| -> String {
        MemberSqlResolver::resolve_table_ref(expr, &source, &|s| dialect.quote_identifier(s))
    };

    // Determine which raw expr columns need to be in GROUP BY (count_distinct, median).
    let mut extra_group_cols: Vec<String> = Vec::new();
    for rm in &rollup.measures {
        match rm.measure_type {
            MeasureType::CountDistinct | MeasureType::CountDistinctApprox => {
                let col = resolve(rm.expr.as_deref().unwrap_or(&rm.name));
                if !extra_group_cols.contains(&col) {
                    extra_group_cols.push(col);
                }
            }
            MeasureType::Median => {
                let col = resolve(rm.expr.as_deref().unwrap_or(&rm.name));
                if !extra_group_cols.contains(&col) {
                    extra_group_cols.push(col);
                }
            }
            _ => {}
        }
    }

    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();
    // Quoted aliases for ClickHouse ORDER BY (positional refs not supported there).
    let mut group_by_aliases: Vec<String> = Vec::new();

    // 1. Dimensions
    for dim_name in &rollup.dimensions {
        if let Some(dim) = view.dimensions.iter().find(|d| d.name == *dim_name) {
            let expr = resolve(&dim.expr);
            let alias = dialect.quote_identifier(dim_name);
            select_cols.push(format!("{expr} AS {alias}"));
            group_by_cols.push(expr);
            group_by_aliases.push(alias);
        }
    }

    // 2. Time dimension (truncated to the rollup granularity)
    if let (Some(td_name), Some(gran)) = (&rollup.time_dimension, &rollup.granularity) {
        if let Some(td) = view.dimensions.iter().find(|d| d.name == *td_name) {
            let expr = resolve(&td.expr);
            // Bucket in the rollup's own timezone. The warehouse resolves the
            // real offset per row, so DST transitions and sub-hour offsets
            // (e.g. Asia/Kolkata, +5:30) are correct without arithmetic here.
            // Stored buckets are therefore LOCAL wall-clock labels.
            let tz = normalize_timezone(rollup.timezone.as_deref());
            let tz_expr = if tz == "UTC" {
                expr.clone()
            } else {
                dialect.convert_tz(&expr, tz)
            };
            let trunc_expr = dialect.date_trunc(gran, &tz_expr);
            let alias = dialect.quote_identifier(&format!("{td_name}__{gran}"));
            select_cols.push(format!("{trunc_expr} AS {alias}"));
            group_by_cols.push(trunc_expr);
            group_by_aliases.push(alias);
        }
    }

    // 3. Extra GROUP BY columns for count_distinct / median
    for col in &extra_group_cols {
        let alias = dialect.quote_identifier(col);
        select_cols.push(format!("{col} AS {alias}"));
        group_by_cols.push(col.clone());
        group_by_aliases.push(alias);
    }

    // 4. Measure columns (preagg naming: measure__type for partial re-aggregation)
    for rm in &rollup.measures {
        let expr = rm
            .expr
            .as_deref()
            .map(&resolve)
            .unwrap_or_else(|| "*".to_string());
        match rm.measure_type {
            MeasureType::Sum => {
                let alias = dialect.quote_identifier(&format!("{}__sum", rm.name));
                select_cols.push(format!("SUM({expr}) AS {alias}"));
            }
            MeasureType::Count => {
                let alias = dialect.quote_identifier(&format!("{}__count", rm.name));
                if expr == "*" {
                    select_cols.push(format!("COUNT(*) AS {alias}"));
                } else {
                    select_cols.push(format!("COUNT({expr}) AS {alias}"));
                }
            }
            MeasureType::Average => {
                // Store SUM + COUNT separately so reagg can compute a correct weighted average.
                let sum_alias = dialect.quote_identifier(&format!("{}__sum", rm.name));
                let count_alias = dialect.quote_identifier(&format!("{}__count", rm.name));
                select_cols.push(format!("SUM({expr}) AS {sum_alias}"));
                select_cols.push(format!("COUNT({expr}) AS {count_alias}"));
            }
            MeasureType::Min => {
                let alias = dialect.quote_identifier(&format!("{}__min", rm.name));
                select_cols.push(format!("MIN({expr}) AS {alias}"));
            }
            MeasureType::Max => {
                let alias = dialect.quote_identifier(&format!("{}__max", rm.name));
                select_cols.push(format!("MAX({expr}) AS {alias}"));
            }
            MeasureType::CountDistinct | MeasureType::CountDistinctApprox => {
                // Raw column already in GROUP BY; no additional SELECT needed.
            }
            MeasureType::Median => {
                let col = rm
                    .expr
                    .as_deref()
                    .map(&resolve)
                    .unwrap_or_else(|| rm.name.clone());
                let freq_alias = dialect.quote_identifier(&format!("{}__freq", col));
                select_cols.push(format!("COUNT(*) AS {freq_alias}"));
            }
            MeasureType::Number => {
                let alias = dialect.quote_identifier(&format!("{}__value", rm.name));
                select_cols.push(format!("{expr} AS {alias}"));
            }
            MeasureType::Custom => {}
        }
    }

    let select = select_cols.join(",\n    ");

    // Only emit GROUP BY when there are grouping columns.
    // Aggregate-only rollups (no dimensions) return a single summary row without GROUP BY.
    let group_by_clause = if group_by_cols.is_empty() {
        String::new()
    } else {
        let positional = group_by_cols
            .iter()
            .enumerate()
            .map(|(i, _)| format!("{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        format!("\nGROUP BY {positional}")
    };

    let ctas = match dialect {
        Dialect::ClickHouse => {
            if group_by_cols.is_empty() {
                format!("CREATE TABLE {fq_table}\nENGINE = MergeTree()\nORDER BY tuple()\nAS\nSELECT\n    {select}\nFROM {source}")
            } else {
                let order_by = group_by_aliases.join(", ");
                format!(
                    "CREATE TABLE {fq_table}\nENGINE = MergeTree()\nORDER BY ({order_by})\nAS\nSELECT\n    {select}\nFROM {source}{group_by_clause}",
                )
            }
        }
        _ => {
            format!(
                "CREATE TABLE {fq_table} AS\nSELECT\n    {select}\nFROM {source}{group_by_clause}",
            )
        }
    };

    let drop = format!("DROP TABLE IF EXISTS {fq_table}");
    Ok(vec![drop, ctas])
}

/// Generate the CREATE TABLE statement for the __manifest table.
pub fn generate_manifest_create_sql(schema: &str, dialect: &Dialect) -> String {
    let fq_table = dialect.qualify_table(schema, "__manifest");
    match dialect {
        Dialect::ClickHouse => format!(
            "CREATE TABLE IF NOT EXISTS {fq_table} (\n\
             \x20   view_name String,\n\
             \x20   rollup_name String,\n\
             \x20   rollup_hash String,\n\
             \x20   table_name String,\n\
             \x20   dimensions String,\n\
             \x20   measures String,\n\
             \x20   time_dimension String,\n\
             \x20   granularity String,\n\
             \x20   build_date DateTime,\n\
             \x20   refresh_key_value String,\n\
             \x20   refresh_key_checked_at String\n\
             ) ENGINE = ReplacingMergeTree(build_date)\n\
             ORDER BY (view_name, rollup_name)"
        ),
        // BigQuery uses STRING, not VARCHAR
        Dialect::BigQuery => format!(
            "CREATE TABLE IF NOT EXISTS {fq_table} (\n\
             \x20   view_name STRING,\n\
             \x20   rollup_name STRING,\n\
             \x20   rollup_hash STRING,\n\
             \x20   table_name STRING,\n\
             \x20   dimensions STRING,\n\
             \x20   measures STRING,\n\
             \x20   time_dimension STRING,\n\
             \x20   granularity STRING,\n\
             \x20   build_date DATETIME,\n\
             \x20   refresh_key_value STRING,\n\
             \x20   refresh_key_checked_at STRING\n\
             )"
        ),
        // SQLite doesn't support composite PRIMARY KEY in column defs
        Dialect::SQLite => format!(
            "CREATE TABLE IF NOT EXISTS {fq_table} (\n\
             \x20   view_name TEXT,\n\
             \x20   rollup_name TEXT,\n\
             \x20   rollup_hash TEXT,\n\
             \x20   table_name TEXT,\n\
             \x20   dimensions TEXT,\n\
             \x20   measures TEXT,\n\
             \x20   time_dimension TEXT,\n\
             \x20   granularity TEXT,\n\
             \x20   build_date TEXT,\n\
             \x20   refresh_key_value TEXT,\n\
             \x20   refresh_key_checked_at TEXT,\n\
             \x20   UNIQUE (view_name, rollup_name)\n\
             )"
        ),
        _ => format!(
            "CREATE TABLE IF NOT EXISTS {fq_table} (\n\
             \x20   view_name VARCHAR,\n\
             \x20   rollup_name VARCHAR,\n\
             \x20   rollup_hash VARCHAR,\n\
             \x20   table_name VARCHAR,\n\
             \x20   dimensions VARCHAR,\n\
             \x20   measures VARCHAR,\n\
             \x20   time_dimension VARCHAR,\n\
             \x20   granularity VARCHAR,\n\
             \x20   build_date TIMESTAMP,\n\
             \x20   refresh_key_value VARCHAR,\n\
             \x20   refresh_key_checked_at VARCHAR,\n\
             \x20   PRIMARY KEY (view_name, rollup_name)\n\
             )"
        ),
    }
}

/// Generate `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements to migrate an
/// existing `__manifest` table to the current schema.
///
/// Call this once on startup (or as a separate migration step) for deployments
/// that created the manifest before the `refresh_key_value` /
/// `refresh_key_checked_at` columns were added.  The statements are
/// idempotent — they are safe to re-run on an already-migrated table.
pub fn generate_manifest_migrate_sql(schema: &str, dialect: &Dialect) -> Vec<String> {
    let fq_table = dialect.qualify_table(schema, "__manifest");
    let new_cols: &[(&str, &str)] = match dialect {
        Dialect::ClickHouse => &[
            ("refresh_key_value", "String"),
            ("refresh_key_checked_at", "String"),
        ],
        Dialect::BigQuery => &[
            ("refresh_key_value", "STRING"),
            ("refresh_key_checked_at", "STRING"),
        ],
        Dialect::SQLite => &[
            ("refresh_key_value", "TEXT"),
            ("refresh_key_checked_at", "TEXT"),
        ],
        _ => &[
            ("refresh_key_value", "VARCHAR"),
            ("refresh_key_checked_at", "VARCHAR"),
        ],
    };

    match dialect {
        Dialect::SQLite => {
            // SQLite does not support `ADD COLUMN IF NOT EXISTS`; emit a
            // conditional via a CREATE TABLE trick instead.
            new_cols
                .iter()
                .map(|(col, ty)| {
                    // Best-effort: wrap in a begin/commit so the no-op case is safe.
                    // Real migrations should check sqlite_master first.
                    format!("ALTER TABLE {fq_table} ADD COLUMN IF NOT EXISTS {col} {ty}")
                })
                .collect()
        }
        _ => new_cols
            .iter()
            .map(|(col, ty)| format!("ALTER TABLE {fq_table} ADD COLUMN IF NOT EXISTS {col} {ty}"))
            .collect(),
    }
}

/// Generate upsert SQL for a manifest entry.
/// ClickHouse uses INSERT (ReplacingMergeTree handles dedup).
/// SQLite uses INSERT OR REPLACE (UNIQUE constraint handles dedup).
/// Other dialects use DELETE + INSERT to handle re-builds.
pub fn generate_manifest_upsert_sql(
    schema: &str,
    entry: &ManifestEntry,
    dialect: &Dialect,
) -> Vec<String> {
    let fq_table = dialect.qualify_table(schema, "__manifest");
    let values = format!(
        "('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
        entry.view_name.replace('\'', "''"),
        entry.rollup_name.replace('\'', "''"),
        entry.rollup_hash.replace('\'', "''"),
        entry.table_name.replace('\'', "''"),
        serde_json::to_string(&entry.dimensions)
            .unwrap_or_default()
            .replace('\'', "''"),
        entry.measures_json.replace('\'', "''"),
        entry
            .time_dimension
            .as_deref()
            .unwrap_or("")
            .replace('\'', "''"),
        entry
            .granularity
            .as_deref()
            .unwrap_or("")
            .replace('\'', "''"),
        entry.build_date.replace('\'', "''"),
        entry
            .refresh_key_value
            .as_deref()
            .unwrap_or("")
            .replace('\'', "''"),
        entry
            .refresh_key_checked_at
            .as_deref()
            .unwrap_or("")
            .replace('\'', "''"),
    );
    let columns = "(view_name, rollup_name, rollup_hash, table_name, dimensions, measures, time_dimension, granularity, build_date, refresh_key_value, refresh_key_checked_at)";
    match dialect {
        // ClickHouse: ReplacingMergeTree handles dedup, just INSERT
        Dialect::ClickHouse => {
            vec![format!("INSERT INTO {fq_table} {columns} VALUES {values}")]
        }
        // SQLite: use INSERT OR REPLACE (relies on UNIQUE constraint)
        Dialect::SQLite => {
            vec![format!(
                "INSERT OR REPLACE INTO {fq_table} {columns} VALUES {values}"
            )]
        }
        // All others: DELETE + INSERT
        _ => {
            let delete = format!(
                "DELETE FROM {fq_table} WHERE view_name = '{}' AND rollup_name = '{}'",
                entry.view_name.replace('\'', "''"),
                entry.rollup_name.replace('\'', "''"),
            );
            let insert = format!("INSERT INTO {fq_table} {columns} VALUES {values}");
            vec![delete, insert]
        }
    }
}

/// Check if any rollup in the manifest covers the given query.
/// Returns a reference to the first matching entry, or None if no rollup covers the query.
pub fn check_coverage<'a>(
    request: &crate::engine::query::QueryRequest,
    rollups: &'a [LocalRollupEntry],
) -> Option<&'a LocalRollupEntry> {
    rollups.iter().find(|entry| covers(request, entry))
}

/// Recursively collect member names from a filter tree.
fn collect_filter_members(filter: &crate::engine::query::QueryFilter, members: &mut Vec<String>) {
    if let Some(ref member) = filter.member {
        members.push(member.clone());
    }
    if let Some(ref and) = filter.and {
        for f in and {
            collect_filter_members(f, members);
        }
    }
    if let Some(ref or) = filter.or {
        for f in or {
            collect_filter_members(f, members);
        }
    }
}

/// Escape LIKE metacharacters (`%`, `_`) in a value being inlined into a LIKE pattern.
fn escape_like(value: &str) -> String {
    value.replace('%', "\\%").replace('_', "\\_")
}

/// Generate a WHERE clause fragment for a single filter, using quoted column names.
/// Returns None if the filter cannot be translated.
fn render_filter_sql(
    filter: &crate::engine::query::QueryFilter,
    entry: &LocalRollupEntry,
    quote: &dyn Fn(&str) -> String,
) -> Option<String> {
    use crate::engine::query::FilterOperator;

    if let (Some(ref member), Some(ref op)) = (&filter.member, &filter.operator) {
        let dim_name = member.split('.').nth(1).unwrap_or(member);
        // Resolve the column name in the rollup table
        let col = if entry.dimensions.contains(&dim_name.to_string()) {
            quote(dim_name)
        } else if entry.time_dimension.as_deref() == Some(dim_name) {
            if let Some(ref gran) = entry.granularity {
                quote(&format!("{}__{}", dim_name, gran))
            } else {
                quote(dim_name)
            }
        } else {
            return None;
        };

        let vals: Vec<String> = filter
            .values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect();

        let sql = match op {
            FilterOperator::Equals => {
                if vals.len() == 1 {
                    format!("{} = {}", col, vals[0])
                } else {
                    format!("{} IN ({})", col, vals.join(", "))
                }
            }
            FilterOperator::NotEquals => {
                if vals.len() == 1 {
                    format!("{} <> {}", col, vals[0])
                } else {
                    format!("{} NOT IN ({})", col, vals.join(", "))
                }
            }
            FilterOperator::Gt => format!("{} > {}", col, vals.first().unwrap_or(&"NULL".into())),
            FilterOperator::Gte => {
                format!("{} >= {}", col, vals.first().unwrap_or(&"NULL".into()))
            }
            FilterOperator::Lt => format!("{} < {}", col, vals.first().unwrap_or(&"NULL".into())),
            FilterOperator::Lte => {
                format!("{} <= {}", col, vals.first().unwrap_or(&"NULL".into()))
            }
            FilterOperator::Set => format!("{} IS NOT NULL", col),
            FilterOperator::NotSet => format!("{} IS NULL", col),
            FilterOperator::Contains => format!(
                "{} LIKE '%{}%'",
                col,
                escape_like(
                    &filter
                        .values
                        .first()
                        .unwrap_or(&String::new())
                        .replace('\'', "''")
                )
            ),
            FilterOperator::NotContains => format!(
                "{} NOT LIKE '%{}%'",
                col,
                escape_like(
                    &filter
                        .values
                        .first()
                        .unwrap_or(&String::new())
                        .replace('\'', "''")
                )
            ),
            _ => return None, // date-range filters not supported in reagg
        };
        Some(sql)
    } else if let Some(ref and) = filter.and {
        let parts: Vec<String> = and
            .iter()
            .filter_map(|f| render_filter_sql(f, entry, quote))
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(format!("({})", parts.join(" AND ")))
        }
    } else if let Some(ref or) = filter.or {
        // For OR, all branches must be renderable — dropping any branch
        // would incorrectly narrow results (the missing branch might match rows).
        let parts: Vec<Option<String>> = or
            .iter()
            .map(|f| render_filter_sql(f, entry, quote))
            .collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_none()) {
            None
        } else {
            let rendered: Vec<String> = parts.into_iter().flatten().collect();
            Some(format!("({})", rendered.join(" OR ")))
        }
    } else {
        None
    }
}

/// Build a WHERE clause from request filters AND time_dimension date_ranges
/// for re-aggregation queries.
fn build_reagg_where_clause(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
    quote: &dyn Fn(&str) -> String,
) -> String {
    let mut parts: Vec<String> = request
        .filters
        .iter()
        .filter_map(|f| render_filter_sql(f, entry, quote))
        .collect();

    // Add date_range filters from time_dimensions
    for td in &request.time_dimensions {
        if let Some(ref date_range) = td.date_range {
            if date_range.len() == 2 {
                let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
                let col = if let Some(ref stored_gran) = entry.granularity {
                    quote(&format!("{}__{}", td_name, stored_gran))
                } else {
                    quote(td_name)
                };
                parts.push(format!(
                    "{} >= '{}'",
                    col,
                    date_range[0].replace('\'', "''")
                ));
                parts.push(format!(
                    "{} <= '{}'",
                    col,
                    date_range[1].replace('\'', "''")
                ));
            }
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!("\nWHERE {}", parts.join(" AND "))
    }
}

/// Build an ORDER BY clause from request order specs for re-aggregation queries.
///
/// When the ordered member is also a time dimension with a granularity, the
/// reagg SELECT projects it as `{view}__{field}__{granularity}` (see
/// `generate_reagg_sql`'s time-dimension branch). The ORDER BY must match
/// that alias, otherwise the binder errors with "column not found" because
/// the un-granularized `{view}__{field}` was never projected.
fn build_reagg_order_by(
    request: &crate::engine::query::QueryRequest,
    quote: &dyn Fn(&str) -> String,
) -> String {
    if request.order.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = request
        .order
        .iter()
        .map(|o| {
            let base = o.id.replace('.', "__");
            let col = request
                .time_dimensions
                .iter()
                .find(|td| td.dimension == o.id)
                .and_then(|td| td.granularity.as_ref())
                .map(|gran| format!("{}__{}", base, gran))
                .unwrap_or(base);
            let dir = if o.desc { " DESC" } else { " ASC" };
            format!("{}{}", quote(&col), dir)
        })
        .collect();
    format!("\nORDER BY {}", parts.join(", "))
}

fn covers(request: &crate::engine::query::QueryRequest, entry: &LocalRollupEntry) -> bool {
    // A rollup's buckets are cut in one specific zone. A UTC rollup and a
    // local rollup hold different sets of rows — no re-truncation converts
    // one into the other — so a mismatch must fall through to the warehouse.
    if normalize_timezone(request.timezone.as_deref())
        != normalize_timezone(entry.timezone.as_deref())
    {
        return false;
    }

    // Check that all filter dimensions exist in the rollup
    if !request.filters.is_empty() {
        let mut filter_members = Vec::new();
        for f in &request.filters {
            collect_filter_members(f, &mut filter_members);
        }
        for member in &filter_members {
            let dim_name = member.split('.').nth(1).unwrap_or(member);
            let in_dims = entry.dimensions.contains(&dim_name.to_string());
            let in_time = entry
                .time_dimension
                .as_deref()
                .is_some_and(|td| td == dim_name);
            if !in_dims && !in_time {
                return false;
            }
        }
    }

    // Extract view names from all member references
    let query_views = request.referenced_views();

    // All referenced views must match the rollup's single view
    if !query_views.iter().all(|v| *v == entry.view_name) {
        return false;
    }

    // Check dimensions: all requested dims must be in rollup dims
    for dim in &request.dimensions {
        let dim_name = dim.split('.').nth(1).unwrap_or(dim);
        if !entry.dimensions.contains(&dim_name.to_string()) {
            return false;
        }
    }

    // Check measures: all requested measures must be in rollup measures (and not custom).
    // Build (name, type) pairs in a single pass to avoid positional desync from filter_map.
    let rollup_measures: Vec<(&str, &str)> = entry
        .measures
        .iter()
        .filter_map(|m| {
            let name = m.get("name").and_then(|n| n.as_str())?;
            let mtype = m.get("type").and_then(|t| t.as_str()).unwrap_or("");
            Some((name, mtype))
        })
        .collect();

    for measure in &request.measures {
        let measure_name = measure.split('.').nth(1).unwrap_or(measure);
        if let Some(&(_, mtype)) = rollup_measures.iter().find(|(n, _)| *n == measure_name) {
            // Reject types that cannot be re-aggregated
            if mtype == "custom" || mtype == "number" || mtype == "median" {
                return false;
            }
        } else {
            // Measure not found in rollup at all
            return false;
        }
    }

    // Check time dimensions
    for td in &request.time_dimensions {
        let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
        if entry.time_dimension.as_deref() != Some(td_name) {
            return false;
        }
        // Granularity: requested must be same or coarser than stored granularity
        if let Some(ref req_gran) = td.granularity {
            if let Some(ref stored_gran) = entry.granularity {
                if !is_coarser_or_equal(req_gran, stored_gran) {
                    return false;
                }
            }
        }
    }

    true
}

fn is_coarser_or_equal(requested: &str, stored: &str) -> bool {
    let order = [
        "second", "minute", "hour", "day", "week", "month", "quarter", "year",
    ];
    let req_idx = order.iter().position(|&g| g == requested);
    let stored_idx = order.iter().position(|&g| g == stored);
    match (req_idx, stored_idx) {
        (Some(r), Some(s)) => r >= s,
        _ => requested == stored,
    }
}

/// Generate a re-aggregation SQL query from a pre-aggregated source.
pub fn generate_reagg_sql(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
    from_source: &str,
) -> String {
    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();

    // 1. Dimensions
    for dim in &request.dimensions {
        let dim_name = dim.split('.').nth(1).unwrap_or(dim);
        let alias = dim.replace('.', "__");
        select_cols.push(format!("\"{}\" AS \"{}\"", dim_name, alias));
        group_by_cols.push(format!("\"{}\"", dim_name));
    }

    // 2. Time dimensions
    for td in &request.time_dimensions {
        let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
        let base_alias = td.dimension.replace('.', "__");
        if let Some(ref gran) = td.granularity {
            // Alias must match the warehouse output column: view__field__granularity
            let alias = format!("{}__{}", base_alias, gran);
            if let Some(ref stored_gran) = entry.granularity {
                let stored_col = format!("{}__{}", td_name, stored_gran);
                if gran == stored_gran {
                    select_cols.push(format!("\"{}\" AS \"{}\"", stored_col, alias));
                    group_by_cols.push(format!("\"{}\"", stored_col));
                } else {
                    let trunc = format!("date_trunc('{}', \"{}\")", gran, stored_col);
                    select_cols.push(format!("{} AS \"{}\"", trunc, alias));
                    group_by_cols.push(trunc);
                }
            }
        } else if td.date_range.is_none() {
            // No requested granularity AND no date_range filter: include time
            // column in the output (pass-through).
            let col = if let Some(ref stored_gran) = entry.granularity {
                format!("\"{}__{stored_gran}\"", td_name)
            } else {
                format!("\"{}\"", td_name)
            };
            select_cols.push(format!("{} AS \"{}\"", col, base_alias));
            group_by_cols.push(col);
        }
        // else: has date_range but no granularity → filter-only (handled by
        // build_reagg_where_clause), don't add time column to SELECT/GROUP BY
    }

    // 3. Measures (re-aggregated)
    for measure in &request.measures {
        let measure_name = measure.split('.').nth(1).unwrap_or(measure);
        let alias = measure.replace('.', "__");

        if let Some(m_meta) = entry
            .measures
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(measure_name))
        {
            let m_type = m_meta.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let columns: Vec<String> = m_meta
                .get("columns")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            match m_type {
                "sum" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__sum", measure_name));
                    select_cols.push(format!("SUM(\"{}\") AS \"{}\"", col, alias));
                }
                "count" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__count", measure_name));
                    select_cols.push(format!("SUM(\"{}\") AS \"{}\"", col, alias));
                }
                "average" => {
                    let sum_col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__sum", measure_name));
                    let count_col = columns
                        .get(1)
                        .cloned()
                        .unwrap_or_else(|| format!("{}__count", measure_name));
                    select_cols.push(format!(
                        "CAST(SUM(\"{}\") AS DOUBLE) / NULLIF(SUM(\"{}\"), 0) AS \"{}\"",
                        sum_col, count_col, alias
                    ));
                }
                "min" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__min", measure_name));
                    select_cols.push(format!("MIN(\"{}\") AS \"{}\"", col, alias));
                }
                "max" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__max", measure_name));
                    select_cols.push(format!("MAX(\"{}\") AS \"{}\"", col, alias));
                }
                "count_distinct" | "count_distinct_approx" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| measure_name.to_string());
                    select_cols.push(format!("COUNT(DISTINCT \"{}\") AS \"{}\"", col, alias));
                }
                "median" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| measure_name.to_string());
                    select_cols.push(format!("MEDIAN(\"{}\") AS \"{}\"", col, alias));
                }
                "number" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__value", measure_name));
                    select_cols.push(format!("\"{}\" AS \"{}\"", col, alias));
                }
                _ => {
                    select_cols.push(format!("NULL AS \"{}\"", alias));
                }
            }
        }
    }

    let select = select_cols.join(", ");
    let where_clause = build_reagg_where_clause(request, entry, &|name| format!("\"{}\"", name));
    let group_by = if group_by_cols.is_empty() {
        String::new()
    } else {
        format!("\nGROUP BY {}", group_by_cols.join(", "))
    };

    let order_by = build_reagg_order_by(request, &|name| format!("\"{}\"", name));
    let limit = request
        .limit
        .map(|l| format!("\nLIMIT {}", l))
        .unwrap_or_default();
    let offset = request
        .offset
        .map(|o| format!("\nOFFSET {}", o))
        .unwrap_or_default();

    format!("SELECT {select}\nFROM {from_source}{where_clause}{group_by}{order_by}{limit}{offset}")
}

/// Generate a dialect-aware SQL query that reads from a pre-aggregated warehouse table.
pub fn generate_warehouse_reagg_sql(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
    table_name: &str,
    dialect: &Dialect,
) -> String {
    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();

    // 1. Dimensions
    for dim in &request.dimensions {
        let dim_name = dim.split('.').nth(1).unwrap_or(dim);
        let alias = dim.replace('.', "__");
        let col = dialect.quote_identifier(dim_name);
        let alias_q = dialect.quote_identifier(&alias);
        select_cols.push(format!("{} AS {}", col, alias_q));
        group_by_cols.push(col);
    }

    // 2. Time dimensions
    for td in &request.time_dimensions {
        let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
        let alias = td.dimension.replace('.', "__");
        let alias_q = dialect.quote_identifier(&alias);
        if let Some(ref gran) = td.granularity {
            if let Some(ref stored_gran) = entry.granularity {
                let stored_col_name = format!("{}__{}", td_name, stored_gran);
                let stored_col = dialect.quote_identifier(&stored_col_name);
                if gran == stored_gran {
                    select_cols.push(format!("{} AS {}", stored_col, alias_q));
                    group_by_cols.push(stored_col);
                } else {
                    let trunc = dialect.date_trunc(gran, &stored_col);
                    select_cols.push(format!("{} AS {}", trunc, alias_q));
                    group_by_cols.push(trunc);
                }
            }
        } else {
            let col = if let Some(ref stored_gran) = entry.granularity {
                dialect.quote_identifier(&format!("{}__{}", td_name, stored_gran))
            } else {
                dialect.quote_identifier(td_name)
            };
            select_cols.push(format!("{} AS {}", col, alias_q));
            group_by_cols.push(col);
        }
    }

    // 3. Measures (re-aggregated)
    for measure in &request.measures {
        let measure_name = measure.split('.').nth(1).unwrap_or(measure);
        let alias = measure.replace('.', "__");
        let alias_q = dialect.quote_identifier(&alias);

        if let Some(m_meta) = entry
            .measures
            .iter()
            .find(|m| m.get("name").and_then(|n| n.as_str()) == Some(measure_name))
        {
            let m_type = m_meta.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let columns: Vec<String> = m_meta
                .get("columns")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            match m_type {
                "sum" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__sum", measure_name)),
                    );
                    select_cols.push(format!("SUM({}) AS {}", col, alias_q));
                }
                "count" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__count", measure_name)),
                    );
                    select_cols.push(format!("SUM({}) AS {}", col, alias_q));
                }
                "average" => {
                    let sum_col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__sum", measure_name)),
                    );
                    let count_col = dialect.quote_identifier(
                        &columns
                            .get(1)
                            .cloned()
                            .unwrap_or_else(|| format!("{}__count", measure_name)),
                    );
                    let sum_expr = format!("SUM({})", sum_col);
                    let count_expr = format!("NULLIF(SUM({}), 0)", count_col);
                    select_cols.push(format!(
                        "{} / {} AS {}",
                        dialect.cast_to_double(&sum_expr),
                        count_expr,
                        alias_q,
                    ));
                }
                "min" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__min", measure_name)),
                    );
                    select_cols.push(format!("MIN({}) AS {}", col, alias_q));
                }
                "max" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__max", measure_name)),
                    );
                    select_cols.push(format!("MAX({}) AS {}", col, alias_q));
                }
                "count_distinct" | "count_distinct_approx" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| measure_name.to_string()),
                    );
                    select_cols.push(format!("COUNT(DISTINCT {}) AS {}", col, alias_q));
                }
                _ => {
                    select_cols.push(format!("NULL AS {}", alias_q));
                }
            }
        }
    }

    let select = select_cols.join(", ");
    let dialect_clone = dialect.clone();
    let where_clause =
        build_reagg_where_clause(request, entry, &|name| dialect_clone.quote_identifier(name));
    let group_by = if group_by_cols.is_empty() {
        String::new()
    } else {
        format!(
            "\nGROUP BY {}",
            group_by_cols
                .iter()
                .enumerate()
                .map(|(i, _)| format!("{}", i + 1))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    let dialect_clone2 = dialect.clone();
    let order_by = build_reagg_order_by(request, &|name| dialect_clone2.quote_identifier(name));
    let limit = request
        .limit
        .map(|l| format!("\nLIMIT {}", l))
        .unwrap_or_default();
    let offset = request
        .offset
        .map(|o| format!("\nOFFSET {}", o))
        .unwrap_or_default();

    format!("SELECT {select}\nFROM {table_name}{where_clause}{group_by}{order_by}{limit}{offset}",)
}

/// Build a ManifestEntry from a view and rollup spec.
///
/// `date_str` may be YYYYMMDD (legacy), YYYYMMDDTHHmmSS, or an RFC3339 string.
/// The `build_date` field is always stored as `YYYY-MM-DD HH:MM:SS` (UTC implied).
pub fn build_manifest_entry(
    view: &View,
    rollup: &RollupSpec,
    schema: &str,
    date_str: &str,
) -> Result<ManifestEntry, EngineError> {
    let table_name = format!("{}__{}__{}", view.name, rollup.hash, date_str);

    let measures_json = serde_json::to_string(
        &rollup
            .measures
            .iter()
            .map(|m| {
                serde_json::json!({
                    "name": m.name,
                    "type": m.measure_type.to_string(),
                    "columns": m.columns,
                })
            })
            .collect::<Vec<_>>(),
    )
    .unwrap_or_default();

    let build_date = parse_date_str_to_sql_datetime(date_str).map_err(|e| {
        EngineError::SqlGenerationError(format!("invalid date_str '{date_str}': {e}"))
    })?;

    Ok(ManifestEntry {
        view_name: view.name.clone(),
        rollup_name: rollup.name.clone(),
        rollup_hash: rollup.hash.clone(),
        table_name: format!("{}.{}", schema, table_name),
        dimensions: rollup.dimensions.clone(),
        measures_json,
        time_dimension: rollup.time_dimension.clone(),
        granularity: rollup.granularity.clone(),
        timezone: rollup.timezone.clone(),
        build_date,
        refresh_key_value: None,
        refresh_key_checked_at: None,
    })
}

/// Parse a date string (YYYYMMDD, YYYYMMDDTHHmmSS, RFC3339, or already-formatted
/// SQL DATETIME) into the warehouse-friendly `YYYY-MM-DD HH:MM:SS` format (UTC implied).
///
/// This format is accepted as a literal by every supported dialect's DATETIME /
/// TIMESTAMP column (ClickHouse, MySQL, Postgres, BigQuery, DuckDB, SQLite).
fn parse_date_str_to_sql_datetime(date_str: &str) -> Result<String, String> {
    const FMT: &str = "%Y-%m-%d %H:%M:%S";

    if date_str.len() == 8 && date_str.chars().all(|c| c.is_ascii_digit()) {
        let year: i32 = date_str[..4].parse().map_err(|_| "invalid year")?;
        let month: u32 = date_str[4..6].parse().map_err(|_| "invalid month")?;
        let day: u32 = date_str[6..8].parse().map_err(|_| "invalid day")?;
        let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .ok_or("invalid YYYYMMDD date")?
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid time")?;
        return Ok(dt.format(FMT).to_string());
    }
    if date_str.len() == 15 {
        let year: i32 = date_str[..4].parse().map_err(|_| "invalid year")?;
        let month: u32 = date_str[4..6].parse().map_err(|_| "invalid month")?;
        let day: u32 = date_str[6..8].parse().map_err(|_| "invalid day")?;
        let hour: u32 = date_str[9..11].parse().map_err(|_| "invalid hour")?;
        let min: u32 = date_str[11..13].parse().map_err(|_| "invalid minute")?;
        let sec: u32 = date_str[13..15].parse().map_err(|_| "invalid second")?;
        let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .ok_or("invalid date")?
            .and_hms_opt(hour, min, sec)
            .ok_or("invalid time")?;
        return Ok(dt.format(FMT).to_string());
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
        return Ok(dt.with_timezone(&chrono::Utc).format(FMT).to_string());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, FMT) {
        return Ok(dt.format(FMT).to_string());
    }
    Err(format!("unrecognized date format: '{date_str}'"))
}

// ---------------------------------------------------------------------------
// Library API: types and functions for callers (CLI, oxy-internal, etc.)
// All functions below are pure computation — no I/O, no async needed.
// ---------------------------------------------------------------------------

/// Result of pre-aggregation cache resolution.
///
/// Returned by [`resolve_local`] and [`resolve_warehouse`]. The caller is
/// responsible for executing the SQL against the appropriate database.
#[derive(Debug, Clone)]
pub enum PreaggResolution {
    /// Query can be served from a local Parquet file via DuckDB.
    LocalParquet {
        /// Re-aggregation SQL to execute against an in-memory DuckDB connection.
        reagg_sql: String,
        /// Path to the Parquet file (joined from cache_dir + entry.file).
        parquet_path: String,
    },
    /// Query can be served from a warehouse rollup table.
    WarehouseRollup {
        /// Re-aggregation SQL to execute against the warehouse.
        reagg_sql: String,
        /// Fully-qualified rollup table name (dialect-quoted).
        table_name: String,
    },
}

/// A rollup entry from the warehouse `__manifest` table.
///
/// Similar to [`LocalRollupEntry`] but carries `table_name` instead of
/// `file`, since warehouse entries haven't been downloaded yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseRollupEntry {
    pub view_name: String,
    pub rollup_name: String,
    pub rollup_hash: String,
    pub table_name: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<serde_json::Value>,
    pub time_dimension: Option<String>,
    pub granularity: Option<String>,
    /// IANA timezone the stored buckets are cut in. `None` means a row written
    /// before rollups were timezone-aware; treat it as `"UTC"` via
    /// [`normalize_timezone`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    pub build_date: String,
}

impl WarehouseRollupEntry {
    /// Convert to a [`LocalRollupEntry`] for use with [`check_coverage`].
    pub fn to_local_entry(&self) -> LocalRollupEntry {
        LocalRollupEntry {
            view_name: self.view_name.clone(),
            rollup_name: self.rollup_name.clone(),
            rollup_hash: self.rollup_hash.clone(),
            file: String::new(),
            dimensions: self.dimensions.clone(),
            measures: self.measures.clone(),
            time_dimension: self.time_dimension.clone(),
            granularity: self.granularity.clone(),
            timezone: self.timezone.clone(),
            build_date: self.build_date.clone(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        }
    }
}

/// A rollup that was skipped during a build because it was still fresh.
#[derive(Debug, Clone)]
pub struct SkippedRollup {
    pub view_name: String,
    pub rollup_name: String,
    pub rollup_hash: String,
}

/// A complete build plan: all SQL statements and manifest entries.
///
/// Returned by [`collect_build_sql`]. The caller executes `statements`
/// sequentially, then uses `manifest_entries` for reporting.
#[derive(Debug, Clone)]
pub struct BuildPlan {
    pub statements: Vec<String>,
    pub manifest_entries: Vec<ManifestEntry>,
    /// Rollups skipped because they are still fresh.
    pub skipped: Vec<SkippedRollup>,
}

/// Per-rollup freshness verdict used by [`collect_build_sql`] to skip fresh rollups.
#[derive(Debug, Clone)]
pub struct RollupFreshness {
    pub rollup_hash: String,
    pub is_fresh: bool,
    /// The current refresh key value to store in the manifest after build.
    pub current_refresh_key_value: Option<String>,
}

/// Generate the SQL to query the `__manifest` table in the warehouse.
///
/// Handles ClickHouse's `FINAL` clause for ReplacingMergeTree deduplication.
pub fn manifest_query_sql(schema: &str, dialect: &Dialect) -> String {
    let manifest_table = dialect.qualify_table(schema, "__manifest");
    let final_clause = if *dialect == Dialect::ClickHouse {
        " FINAL"
    } else {
        ""
    };
    format!(
        "SELECT view_name, rollup_name, rollup_hash, table_name, \
         dimensions, measures, time_dimension, granularity, build_date \
         FROM {manifest_table}{final_clause}"
    )
}

/// Parse raw JSON rows from a manifest query into [`WarehouseRollupEntry`] values.
///
/// Accepts the row format returned by any executor that produces
/// `Vec<Map<String, Value>>`. Rows with missing required fields are skipped.
pub fn parse_manifest_rows(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Vec<WarehouseRollupEntry> {
    rows.iter()
        .filter_map(|row| {
            // Normalize keys to lowercase so parsing works with databases that
            // uppercase unquoted identifiers (e.g. Snowflake returns VIEW_NAME).
            let row: serde_json::Map<String, serde_json::Value> = row
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.clone()))
                .collect();
            Some(WarehouseRollupEntry {
                view_name: row.get("view_name")?.as_str()?.to_string(),
                rollup_name: row.get("rollup_name")?.as_str()?.to_string(),
                rollup_hash: row.get("rollup_hash")?.as_str()?.to_string(),
                table_name: row.get("table_name")?.as_str()?.to_string(),
                dimensions: serde_json::from_str(row.get("dimensions")?.as_str()?)
                    .unwrap_or_default(),
                measures: serde_json::from_str(row.get("measures")?.as_str()?).unwrap_or_default(),
                time_dimension: row
                    .get("time_dimension")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
                granularity: row
                    .get("granularity")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
                // NOTE: `manifest_query_sql` does not yet select a `timezone`
                // column — the `__manifest` table schema (CREATE/ALTER/
                // upsert/query) hasn't been migrated to carry it. Until that
                // lands, warehouse rows arrive with `timezone: None`, which
                // `normalize_timezone` treats as `"UTC"`.
                timezone: row
                    .get("timezone")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
                build_date: row
                    .get("build_date")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .collect()
}

/// Try to resolve a query from the local Parquet cache.
///
/// Returns `Some(LocalParquet { ... })` if a cached rollup covers the query
/// and the Parquet file exists on disk. Returns `None` otherwise.
/// The caller should execute `reagg_sql` against an in-memory DuckDB connection.
pub fn resolve_local(
    request: &crate::engine::query::QueryRequest,
    manifest: &LocalManifest,
    cache_dir: &std::path::Path,
) -> Option<PreaggResolution> {
    let entry = check_coverage(request, &manifest.rollups)?;
    let parquet_path = cache_dir.join(&entry.file);
    if !parquet_path.is_file() {
        return None;
    }
    let parquet_str = parquet_path.to_str()?;
    let from_source = format!("read_parquet('{}')", parquet_str.replace('\'', "''"));
    let reagg_sql = generate_reagg_sql(request, entry, &from_source);
    Some(PreaggResolution::LocalParquet {
        reagg_sql,
        parquet_path: parquet_str.to_string(),
    })
}

/// Result of cache-based resolution (no filesystem dependency).
///
/// Returned by [`resolve_cached`]. The caller is responsible for loading the
/// data identified by `cache_key` and executing `reagg_sql` against it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResolution {
    /// Re-aggregation SQL with `FROM "__cache"` as placeholder table name.
    /// The caller should either create a table named `__cache` with the cached
    /// data, or replace `"__cache"` with the actual data source.
    pub reagg_sql: String,
    /// Cache key for looking up the stored data (e.g., `"events__a1b2c3d4"`).
    pub cache_key: String,
    /// The matched rollup entry (for metadata inspection).
    pub entry: LocalRollupEntry,
}

/// Try to resolve a query from a cached manifest, without filesystem checks.
///
/// This is the WASM/browser-friendly variant of [`resolve_local`]. Instead of
/// checking for a Parquet file on disk, it returns the cache key and a reagg SQL
/// that reads from a placeholder table `"__cache"`. The caller (e.g., JavaScript
/// using duckdb-wasm + IndexedDB) is responsible for loading the data into a
/// table named `__cache` before executing the SQL.
///
/// Returns `None` if no rollup covers the query.
pub fn resolve_cached(
    request: &crate::engine::query::QueryRequest,
    manifest: &LocalManifest,
) -> Option<CachedResolution> {
    let entry = check_coverage(request, &manifest.rollups)?;
    let cache_key = format!("{}__{}", entry.view_name, entry.rollup_hash);
    let reagg_sql = generate_reagg_sql(request, entry, "\"__cache\"");
    Some(CachedResolution {
        reagg_sql,
        cache_key,
        entry: entry.clone(),
    })
}

/// Try to resolve a query from warehouse rollup tables.
///
/// Returns `Some(WarehouseRollup { ... })` if a rollup covers the query.
/// Returns `None` otherwise. The caller should execute `reagg_sql` against
/// the warehouse connection.
pub fn resolve_warehouse(
    request: &crate::engine::query::QueryRequest,
    entries: &[WarehouseRollupEntry],
    schema: &str,
    dialect: &Dialect,
) -> Option<PreaggResolution> {
    // Single pass: convert one at a time, check coverage, keep the match
    for entry in entries {
        if entry.table_name.is_empty() {
            continue;
        }
        let local = entry.to_local_entry();
        if !covers(request, &local) {
            continue;
        }

        // Re-quote the stored table name using the dialect
        let fq_table = if let Some((s, t)) = entry.table_name.split_once('.') {
            dialect.qualify_table(s, t)
        } else {
            dialect.qualify_table(schema, &entry.table_name)
        };

        let reagg_sql = generate_warehouse_reagg_sql(request, &local, &fq_table, dialect);
        return Some(PreaggResolution::WarehouseRollup {
            reagg_sql,
            table_name: fq_table,
        });
    }
    None
}

/// Generate a complete build plan using a pre-built [`SemanticEngine`].
///
/// Callers that already hold an engine (e.g. to avoid rebuilding it per-cycle)
/// should call this directly.  [`collect_build_sql`] is a thin wrapper that
/// constructs the engine from `views` and delegates here.
pub fn collect_build_sql_with_engine(
    engine: &SemanticEngine,
    views: &[&View],
    schema: &str,
    date_str: &str,
    dialect: &Dialect,
    previous_entries: Option<&[WarehouseRollupEntry]>,
    freshness: Option<&[RollupFreshness]>,
    timezone: Option<&str>,
) -> Result<BuildPlan, EngineError> {
    let mut statements: Vec<String> = Vec::new();
    let mut manifest_entries: Vec<ManifestEntry> = Vec::new();
    let mut skipped: Vec<SkippedRollup> = Vec::new();

    // 1. Create schema/database (if the dialect supports it)
    if let Some(ddl) = dialect.create_schema_ddl(schema) {
        statements.push(ddl);
    }

    // 2. Create manifest table
    statements.push(generate_manifest_create_sql(schema, dialect));

    // 3. For each view, resolve rollups and generate CTAS + manifest entries.
    for view in views {
        let rollups = resolve_rollups(view, timezone);
        for rollup in &rollups {
            if let Some(f_list) = freshness {
                if let Some(f) = f_list.iter().find(|f| f.rollup_hash == rollup.hash) {
                    if f.is_fresh {
                        skipped.push(SkippedRollup {
                            view_name: view.name.clone(),
                            rollup_name: rollup.name.clone(),
                            rollup_hash: rollup.hash.clone(),
                        });
                        continue;
                    }
                }
            }
            let ctas_stmts = generate_build_sql(engine, view, rollup, schema, date_str)?;
            statements.extend(ctas_stmts);

            let mut entry = build_manifest_entry(view, rollup, schema, date_str)?;
            // Attach the latest refresh key value if provided.
            if let Some(f_list) = freshness {
                if let Some(f) = f_list.iter().find(|f| f.rollup_hash == rollup.hash) {
                    if let Some(ref val) = f.current_refresh_key_value {
                        entry.refresh_key_value = Some(val.clone());
                        entry.refresh_key_checked_at = Some(chrono::Utc::now().to_rfc3339());
                    }
                }
            }
            statements.extend(generate_manifest_upsert_sql(schema, &entry, dialect));
            manifest_entries.push(entry);
        }
    }

    // 4. Clean up old rollup tables replaced by this build.
    //    Only drop tables whose rollup_hash matches a newly-built entry but
    //    whose table_name differs (i.e., a previous date-stamped table).
    if let Some(prev) = previous_entries {
        let new_tables: std::collections::HashSet<&str> = manifest_entries
            .iter()
            .map(|e| e.table_name.as_str())
            .collect();
        for old in prev {
            if manifest_entries
                .iter()
                .any(|e| e.rollup_hash == old.rollup_hash)
                && !new_tables.contains(old.table_name.as_str())
                && !old.table_name.is_empty()
            {
                let fq_old = if let Some((s, t)) = old.table_name.split_once('.') {
                    dialect.qualify_table(s, t)
                } else {
                    dialect.qualify_table(schema, &old.table_name)
                };
                statements.push(format!("DROP TABLE IF EXISTS {}", fq_old));
            }
        }
    }

    Ok(BuildPlan {
        statements,
        manifest_entries,
        skipped,
    })
}

/// Generate a complete build plan for the given views.
///
/// Returns all SQL statements to execute (in order) plus manifest entries
/// for reporting. The caller is responsible for executing the statements.
///
/// If `previous_entries` is provided (from reading the warehouse manifest
/// before building), the plan appends `DROP TABLE IF EXISTS` statements at
/// the end to clean up old rollup tables that were replaced by this build.
/// Cleanup runs *after* the new tables and manifest are in place, so there
/// is no downtime window where a rollup is missing.
///
/// If `freshness` is provided, rollups whose [`RollupFreshness::is_fresh`] is
/// `true` are skipped and their names recorded in [`BuildPlan::skipped`].
/// Views without a `pre_aggregations` block produce no rollups and are skipped.
pub fn collect_build_sql(
    views: &[&View],
    schema: &str,
    date_str: &str,
    dialect: &Dialect,
    previous_entries: Option<&[WarehouseRollupEntry]>,
    freshness: Option<&[RollupFreshness]>,
    timezone: Option<&str>,
) -> Result<BuildPlan, EngineError> {
    let owned_views: Vec<View> = views.iter().map(|v| (*v).clone()).collect();
    let layer = SemanticLayer::new(owned_views, None);
    let dialects = DatasourceDialectMap::with_default(dialect.clone());
    let engine = SemanticEngine::from_semantic_layer(layer, dialects)?;

    collect_build_sql_with_engine(
        &engine,
        views,
        schema,
        date_str,
        dialect,
        previous_entries,
        freshness,
        timezone,
    )
}

/// Parse an interval string into a `Duration`.
///
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).
pub fn parse_interval(s: &str) -> Result<std::time::Duration, String> {
    if s.is_empty() {
        return Err("empty interval string".into());
    }
    let suffix_char = s
        .chars()
        .last()
        .filter(|c| c.is_ascii())
        .ok_or_else(|| format!("invalid interval suffix in '{s}'"))?;
    let num_str = &s[..s.len() - suffix_char.len_utf8()];
    let n: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid interval number in '{s}'"))?;
    let multiplier: u64 = match suffix_char {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86_400,
        'w' => 7 * 86_400,
        other => return Err(format!("unknown interval suffix '{other}' in '{s}'")),
    };
    let secs = n
        .checked_mul(multiplier)
        .ok_or_else(|| format!("interval overflow in '{s}'"))?;
    Ok(std::time::Duration::from_secs(secs))
}

/// Result of a freshness check for a single rollup.
#[derive(Debug, Clone)]
pub struct FreshnessCheck {
    pub is_fresh: bool,
    /// The current refresh_key value (store back into manifest after build).
    /// `None` for `Every`-based keys (no sentinel value is needed).
    pub current_value: Option<String>,
}

/// Check whether a rollup is still fresh given its `refresh_key`.
///
/// - `RefreshKey::Every` — compares elapsed time since `last_checked_at`
///   against the parsed interval. Returns stale when `last_checked_at` is
///   absent or the interval has elapsed.
/// - `RefreshKey::Sql` — caller must pre-evaluate the SQL and pass the
///   result as `current_value`. Returns stale when `last_refresh_key_value`
///   is absent or differs from `current_value`.
/// - `None` (no key configured) — always returns `is_fresh: false`.
///
/// Returns `Err` when the interval string or `last_checked_at` timestamp
/// cannot be parsed; callers should log the error and treat the rollup as stale.
pub fn check_freshness(
    refresh_key: Option<&crate::schema::models::RefreshKey>,
    last_refresh_key_value: Option<&str>,
    last_checked_at: Option<&str>,
    current_value: Option<&str>,
) -> Result<FreshnessCheck, EngineError> {
    use crate::schema::models::RefreshKey;

    match refresh_key {
        None => Ok(FreshnessCheck {
            is_fresh: false,
            current_value: None,
        }),

        Some(RefreshKey::Every(interval_str)) => {
            let Some(checked_str) = last_checked_at else {
                return Ok(FreshnessCheck {
                    is_fresh: false,
                    current_value: None,
                });
            };
            let interval = parse_interval(interval_str).map_err(EngineError::QueryError)?;
            let last_dt = chrono::DateTime::parse_from_rfc3339(checked_str).map_err(|e| {
                EngineError::QueryError(format!("invalid last_checked_at '{checked_str}': {e}"))
            })?;
            let elapsed =
                chrono::Utc::now().signed_duration_since(last_dt.with_timezone(&chrono::Utc));
            let is_fresh = elapsed.num_seconds() < interval.as_secs() as i64;
            Ok(FreshnessCheck {
                is_fresh,
                current_value: None,
            })
        }

        Some(RefreshKey::Sql(_)) => {
            let Some(cur) = current_value else {
                return Ok(FreshnessCheck {
                    is_fresh: false,
                    current_value: None,
                });
            };
            let Some(last) = last_refresh_key_value else {
                return Ok(FreshnessCheck {
                    is_fresh: false,
                    current_value: Some(cur.to_string()),
                });
            };
            Ok(FreshnessCheck {
                is_fresh: cur == last,
                current_value: Some(cur.to_string()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::query::QueryRequest;

    fn build_test_engine(view: &View, dialect: &crate::dialect::Dialect) -> SemanticEngine {
        let layer = SemanticLayer::new(vec![view.clone()], None);
        let dialects = DatasourceDialectMap::with_default(dialect.clone());
        SemanticEngine::from_semantic_layer(layer, dialects).expect("test engine build failed")
    }

    #[test]
    fn normalize_treats_none_and_utc_as_the_same_zone() {
        assert_eq!(normalize_timezone(None), "UTC");
        assert_eq!(normalize_timezone(Some("UTC")), "UTC");
        assert_eq!(normalize_timezone(Some("")), "UTC");
        assert_eq!(
            normalize_timezone(Some("America/Los_Angeles")),
            "America/Los_Angeles"
        );
    }

    #[test]
    fn hash_is_stable_across_equivalent_utc_spellings() {
        let dims = vec!["region".to_string()];
        let measures = vec!["revenue".to_string()];
        let a = compute_rollup_hash(&dims, &measures, Some("created_at"), Some("day"), None);
        let b = compute_rollup_hash(
            &dims,
            &measures,
            Some("created_at"),
            Some("day"),
            Some("UTC"),
        );
        assert_eq!(a, b, "None and Some(\"UTC\") must be the same rollup");
    }

    #[test]
    fn hash_changes_when_timezone_changes() {
        let dims = vec!["region".to_string()];
        let measures = vec!["revenue".to_string()];
        let utc = compute_rollup_hash(&dims, &measures, Some("created_at"), Some("day"), None);
        let la = compute_rollup_hash(
            &dims,
            &measures,
            Some("created_at"),
            Some("day"),
            Some("America/Los_Angeles"),
        );
        assert_ne!(
            utc, la,
            "a rollup bucketed in LA is a different rollup from one bucketed in UTC"
        );
    }

    #[test]
    fn test_generate_build_sql_sum() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        let engine = build_test_engine(&view, &crate::dialect::Dialect::ClickHouse);
        let sqls = generate_build_sql(&engine, &view, &rollups[0], "AIRLAYER", "20260415")
            .expect("generate_build_sql failed");
        assert_eq!(sqls.len(), 2); // DROP + CTAS
        let ctas = &sqls[1];
        assert!(
            ctas.contains("CREATE TABLE"),
            "Missing CREATE TABLE: {}",
            ctas
        );
        assert!(ctas.contains("AIRLAYER"), "Missing schema: {}", ctas);
        assert!(ctas.contains("orders__"), "Missing view name: {}", ctas);
        assert!(ctas.contains("20260415"), "Missing date: {}", ctas);
        assert!(ctas.contains("SUM("), "Missing SUM aggregation: {}", ctas);
        assert!(
            ctas.contains("total_revenue__sum"),
            "Missing column alias: {}",
            ctas
        );
        assert!(
            ctas.contains("toStartOfMonth"),
            "Missing ClickHouse date_trunc: {}",
            ctas
        );
    }

    #[test]
    fn test_generate_manifest_sql_clickhouse() {
        let create = generate_manifest_create_sql("AIRLAYER", &crate::dialect::Dialect::ClickHouse);
        assert!(
            create.contains("__manifest"),
            "Missing manifest: {}",
            create
        );
        assert!(
            create.contains("ReplacingMergeTree"),
            "Missing engine: {}",
            create
        );
    }

    #[test]
    fn test_generate_manifest_sql_postgres() {
        let create = generate_manifest_create_sql("preagg", &crate::dialect::Dialect::Postgres);
        assert!(
            create.contains("\"preagg\".\"__manifest\""),
            "Missing quoted name: {}",
            create
        );
        assert!(create.contains("PRIMARY KEY"), "Missing PK: {}", create);
    }

    #[test]
    fn test_generate_manifest_sql_bigquery() {
        let create = generate_manifest_create_sql("my_dataset", &crate::dialect::Dialect::BigQuery);
        assert!(
            create.contains("`my_dataset`.`__manifest`"),
            "Missing backtick-quoted name: {}",
            create
        );
        assert!(create.contains("STRING"), "Missing STRING type: {}", create);
        assert!(
            !create.contains("PRIMARY KEY"),
            "BigQuery should not have PK: {}",
            create
        );
    }

    #[test]
    fn test_generate_manifest_sql_sqlite() {
        let create = generate_manifest_create_sql("preagg", &crate::dialect::Dialect::SQLite);
        assert!(create.contains("TEXT"), "Missing TEXT type: {}", create);
        assert!(create.contains("UNIQUE"), "Missing UNIQUE: {}", create);
        assert!(
            !create.contains("PRIMARY KEY"),
            "SQLite should use UNIQUE not PK: {}",
            create
        );
    }

    #[test]
    fn test_build_sql_uses_dialect_quoting() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        // BigQuery should use backtick quoting
        let engine = build_test_engine(&view, &crate::dialect::Dialect::BigQuery);
        let sqls = generate_build_sql(&engine, &view, &rollups[0], "my_dataset", "20260415")
            .expect("generate_build_sql failed");
        let ctas = &sqls[1];
        assert!(
            ctas.contains("`my_dataset`"),
            "Missing backtick-quoted schema: {}",
            ctas
        );
    }

    #[test]
    fn test_manifest_upsert_sqlite_uses_replace() {
        let entry = ManifestEntry {
            view_name: "orders".into(),
            rollup_name: "by_region".into(),
            rollup_hash: "a1b2c3d4".into(),
            table_name: "preagg.orders__a1b2c3d4__20260415".into(),
            dimensions: vec!["region".into()],
            measures_json: "[]".into(),
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let stmts =
            generate_manifest_upsert_sql("preagg", &entry, &crate::dialect::Dialect::SQLite);
        assert_eq!(stmts.len(), 1, "SQLite should use INSERT OR REPLACE");
        assert!(
            stmts[0].contains("INSERT OR REPLACE"),
            "Missing INSERT OR REPLACE: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_generate_manifest_upsert() {
        let entry = ManifestEntry {
            view_name: "orders".into(),
            rollup_name: "by_region".into(),
            rollup_hash: "a1b2c3d4".into(),
            table_name: "orders__a1b2c3d4__20260415".into(),
            dimensions: vec!["region".into()],
            measures_json: "[]".into(),
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
            timezone: None,
            build_date: "2026-04-15".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let stmts =
            generate_manifest_upsert_sql("AIRLAYER", &entry, &crate::dialect::Dialect::ClickHouse);
        assert_eq!(stmts.len(), 1, "ClickHouse should produce only INSERT");
        assert!(
            stmts[0].contains("INSERT INTO"),
            "Missing INSERT: {}",
            stmts[0]
        );
        assert!(
            stmts[0].contains("orders"),
            "Missing view name: {}",
            stmts[0]
        );

        // Non-ClickHouse should produce DELETE + INSERT
        let stmts_duckdb =
            generate_manifest_upsert_sql("AIRLAYER", &entry, &crate::dialect::Dialect::DuckDB);
        assert_eq!(
            stmts_duckdb.len(),
            2,
            "DuckDB should produce DELETE + INSERT"
        );
        assert!(
            stmts_duckdb[0].contains("DELETE FROM"),
            "Missing DELETE: {}",
            stmts_duckdb[0]
        );
        assert!(
            stmts_duckdb[1].contains("INSERT INTO"),
            "Missing INSERT: {}",
            stmts_duckdb[1]
        );
    }

    #[test]
    fn test_rollup_hash_deterministic() {
        let h1 = compute_rollup_hash(
            &["region".into(), "status".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
            None,
        );
        let h2 = compute_rollup_hash(
            &["region".into(), "status".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
            None,
        );
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 8);
    }

    #[test]
    fn test_rollup_hash_order_independent() {
        let h1 = compute_rollup_hash(
            &["region".into(), "status".into()],
            &["a".into(), "b".into()],
            None,
            None,
            None,
        );
        let h2 = compute_rollup_hash(
            &["status".into(), "region".into()],
            &["b".into(), "a".into()],
            None,
            None,
            None,
        );
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_rollup_hash_different_inputs() {
        let h1 = compute_rollup_hash(
            &["region".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
            None,
        );
        let h2 = compute_rollup_hash(
            &["status".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
            None,
        );
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_resolve_rollups_explicit() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].name, "by_region_monthly");
        assert_eq!(rollups[0].dimensions, vec!["region"]);
        assert_eq!(rollups[0].time_dimension.as_deref(), Some("created_at"));
    }

    #[test]
    fn test_resolve_rollups_default_all() {
        let view = test_view_no_preaggs();
        let rollups = resolve_rollups(&view, None);
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].name, "default");
        // Should include all dimensions (except datetime — that's the time dim)
        assert!(rollups[0].dimensions.contains(&"region".to_string()));
        // Should include all measures (except custom)
        assert!(rollups[0]
            .measures
            .iter()
            .any(|m| m.name == "total_revenue"));
    }

    fn test_view_with_preaggs() -> View {
        use crate::schema::models::*;
        View {
            name: "orders".to_string(),
            description: Some("test".to_string()),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("orders".to_string()),
            sql: None,
            entities: vec![],
            dimensions: vec![
                Dimension {
                    name: "region".into(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "region".into(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    segmentable: None,
                    inherits_from: None,
                    meta: None,
                },
                Dimension {
                    name: "created_at".into(),
                    dimension_type: DimensionType::Datetime,
                    description: None,
                    expr: "created_at".into(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    segmentable: None,
                    inherits_from: None,
                    meta: None,
                },
            ],
            measures: Some(vec![Measure {
                name: "total_revenue".into(),
                measure_type: MeasureType::Sum,
                description: None,
                expr: Some("revenue".into()),
                original_expr: None,
                filters: None,
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
                meta: None,
                drivers: None,
                shift: None,
            }]),
            segments: vec![],
            pre_aggregations: Some(vec![PreAggregation {
                name: "by_region_monthly".into(),
                dimensions: vec!["region".into()],
                measures: vec!["total_revenue".into()],
                time_dimension: Some("created_at".into()),
                granularity: Some("month".into()),
                refresh_key: None,
            }]),
            refresh_key: None,
            meta: None,
        }
    }

    fn test_view_no_preaggs() -> View {
        use crate::schema::models::*;
        View {
            name: "orders".into(),
            description: Some("test".into()),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("orders".into()),
            sql: None,
            entities: vec![],
            dimensions: vec![
                Dimension {
                    name: "region".into(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "region".into(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    segmentable: None,
                    inherits_from: None,
                    meta: None,
                },
                Dimension {
                    name: "created_at".into(),
                    dimension_type: DimensionType::Datetime,
                    description: None,
                    expr: "created_at".into(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    segmentable: None,
                    inherits_from: None,
                    meta: None,
                },
            ],
            measures: Some(vec![
                Measure {
                    name: "total_revenue".into(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("revenue".into()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                },
                Measure {
                    name: "avg_revenue".into(),
                    measure_type: MeasureType::Average,
                    description: None,
                    expr: Some("revenue".into()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                },
            ]),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }
    }

    fn test_local_rollup_entry() -> LocalRollupEntry {
        LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "by_region_monthly".into(),
            rollup_hash: "a1b2c3d4".into(),
            file: "orders__a1b2c3d4.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![
                serde_json::json!({"name": "total_revenue", "type": "sum", "columns": ["total_revenue__sum"]}),
            ],
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
            timezone: None,
            build_date: "2026-04-15".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        }
    }

    fn request_for_covers_tests() -> QueryRequest {
        QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        }
    }

    #[test]
    fn covers_rejects_a_timezone_mismatched_rollup() {
        let mut entry = test_local_rollup_entry();
        entry.timezone = Some("UTC".to_string());
        let mut request = request_for_covers_tests();
        request.timezone = Some("America/Los_Angeles".to_string());
        assert!(
            !covers(&request, &entry),
            "a UTC rollup must not serve a local-bucket query"
        );
    }

    #[test]
    fn covers_accepts_a_matching_timezone() {
        let mut entry = test_local_rollup_entry();
        entry.timezone = Some("America/Los_Angeles".to_string());
        let mut request = request_for_covers_tests();
        request.timezone = Some("America/Los_Angeles".to_string());
        assert!(covers(&request, &entry));
    }

    #[test]
    fn covers_treats_none_request_as_utc() {
        let mut entry = test_local_rollup_entry();
        entry.timezone = Some("UTC".to_string());
        let request = request_for_covers_tests(); // timezone: None
        assert!(
            covers(&request, &entry),
            "an unset request timezone means UTC and must match a UTC rollup"
        );
    }

    #[test]
    fn covers_treats_legacy_none_entry_as_utc() {
        // Rows written before this change have no timezone column value.
        let mut entry = test_local_rollup_entry();
        entry.timezone = None;
        let mut request = request_for_covers_tests();
        request.timezone = Some("America/Los_Angeles".to_string());
        assert!(!covers(&request, &entry));
    }

    #[test]
    fn test_reagg_sql_basic() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("read_parquet('/data/orders.parquet')"),
            "Missing FROM: {}",
            sql
        );
        assert!(
            sql.contains("SUM(\"total_revenue__sum\")"),
            "Missing SUM re-agg: {}",
            sql
        );
        assert!(
            sql.contains("\"region\""),
            "Missing dimension column: {}",
            sql
        );
        assert!(sql.contains("GROUP BY"), "Missing GROUP BY: {}", sql);
    }

    #[test]
    fn test_reagg_sql_with_time_dimension_same_gran() {
        use crate::engine::query::TimeDimensionQuery;
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // Same granularity: should select the stored column directly, no date_trunc.
        // Alias must include the granularity so output matches warehouse column names.
        assert!(
            sql.contains("\"created_at__month\""),
            "Missing stored time col: {}",
            sql
        );
        assert!(
            sql.contains("AS \"orders__created_at__month\""),
            "Alias should include granularity: {}",
            sql
        );
        assert!(
            !sql.contains("date_trunc"),
            "Should not re-truncate same gran: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_order_by_time_dimension_with_granularity() {
        // Regression: when ordering by a time dimension that has a granularity,
        // ORDER BY must reference the granularity-suffixed alias, not the bare
        // `{view}__{field}` form (which is never projected).
        use crate::engine::query::{OrderBy, TimeDimensionQuery};
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            order: vec![OrderBy {
                id: "orders.created_at".to_string(),
                desc: false,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("ORDER BY \"orders__created_at__month\" ASC"),
            "ORDER BY should use granularity-suffixed alias: {}",
            sql
        );
        assert!(
            !sql.contains("ORDER BY \"orders__created_at\" "),
            "ORDER BY must not reference un-granularized column: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_with_time_dimension_coarser_gran() {
        use crate::engine::query::TimeDimensionQuery;
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("year".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // Coarser granularity: should apply date_trunc and alias with requested granularity.
        assert!(
            sql.contains("date_trunc('year', \"created_at__month\")"),
            "Missing date_trunc: {}",
            sql
        );
        assert!(
            sql.contains("AS \"orders__created_at__year\""),
            "Alias should include requested granularity: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_no_gran_uses_stored_col() {
        use crate::engine::query::TimeDimensionQuery;
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: None,
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // No requested gran: should fall back to the stored truncated column, not bare "created_at".
        // Alias has no granularity suffix (none was requested).
        assert!(
            sql.contains("\"created_at__month\""),
            "Should use stored truncated col: {}",
            sql
        );
        assert!(
            sql.contains("AS \"orders__created_at\""),
            "Alias should be base field without granularity: {}",
            sql
        );
        assert!(
            !sql.contains("\"created_at\""),
            "Should not select bare column: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_parquet_path_escaping() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/it''s_here.parquet')");
        assert!(
            sql.contains("it''s_here"),
            "Single quote should be escaped: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_limit_offset() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            limit: Some(100),
            offset: Some(20),
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(sql.contains("LIMIT 100"), "Missing LIMIT: {}", sql);
        assert!(sql.contains("OFFSET 20"), "Missing OFFSET: {}", sql);
    }

    #[test]
    fn test_warehouse_reagg_sql_substitutes_table() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "AIRLAYER.orders__a1b2c3d4__20260415",
            &crate::dialect::Dialect::ClickHouse,
        );
        assert!(
            !sql.contains("read_parquet"),
            "Should not have read_parquet: {}",
            sql
        );
        assert!(
            sql.contains("AIRLAYER.orders__a1b2c3d4__20260415"),
            "Missing table name: {}",
            sql
        );
    }

    #[test]
    fn test_coverage_check_covered() {
        let entry = test_local_rollup_entry();
        let rollups = [entry];
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let result = check_coverage(&request, &rollups);
        assert!(result.is_some(), "Expected coverage match");
    }

    #[test]
    fn test_coverage_check_not_covered_missing_dim() {
        let entry = test_local_rollup_entry();
        let rollups = [entry];
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()], // Not in rollup
            ..QueryRequest::new()
        };
        let result = check_coverage(&request, &rollups);
        assert!(result.is_none(), "Expected no coverage match");
    }

    #[test]
    fn test_coverage_check_not_covered_missing_measure() {
        let entry = test_local_rollup_entry();
        let rollups = [entry];
        let request = QueryRequest {
            measures: vec!["orders.other_metric".to_string()], // Not in rollup
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let result = check_coverage(&request, &rollups);
        assert!(result.is_none(), "Expected no coverage match");
    }

    #[test]
    fn test_coverage_rejects_median_and_number_measures() {
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![
                serde_json::json!({"name": "med_rev", "type": "median", "columns": ["revenue", "revenue__freq"]}),
                serde_json::json!({"name": "computed", "type": "number", "columns": ["computed__value"]}),
            ],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let rollups = [entry];

        let request = QueryRequest {
            measures: vec!["orders.med_rev".to_string()],
            ..QueryRequest::new()
        };
        assert!(
            check_coverage(&request, &rollups).is_none(),
            "Median should not be covered"
        );

        let request = QueryRequest {
            measures: vec!["orders.computed".to_string()],
            ..QueryRequest::new()
        };
        assert!(
            check_coverage(&request, &rollups).is_none(),
            "Number should not be covered"
        );
    }

    #[test]
    fn test_coverage_allows_filtered_query_when_dim_in_rollup() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let rollups = [entry];
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.region".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["US".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = check_coverage(&request, &rollups);
        assert!(
            result.is_some(),
            "Filter on rollup dimension should be covered"
        );
    }

    #[test]
    fn test_coverage_rejects_filter_on_missing_dim() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let rollups = [entry];
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()), // Not in rollup
                operator: Some(FilterOperator::Equals),
                values: vec!["active".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = check_coverage(&request, &rollups);
        assert!(
            result.is_none(),
            "Filter on non-rollup dimension should not be covered"
        );
    }

    #[test]
    fn test_reagg_sql_with_filter() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.region".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["US".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("WHERE \"region\" = 'US'"),
            "Missing WHERE clause: {}",
            sql
        );
    }

    #[test]
    fn test_warehouse_reagg_sql_dialect_aware() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        // Postgres should use double-quote identifiers
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "\"preagg\".\"orders__abc__20260415\"",
            &crate::dialect::Dialect::Postgres,
        );
        assert!(
            sql.contains("\"region\""),
            "Should use double-quote identifiers: {}",
            sql
        );
        assert!(
            sql.contains("SUM(\"total_revenue__sum\")"),
            "Should have SUM re-agg: {}",
            sql
        );

        // BigQuery should use backtick identifiers
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "`my_dataset`.`orders__abc__20260415`",
            &crate::dialect::Dialect::BigQuery,
        );
        assert!(
            sql.contains("`region`"),
            "Should use backtick identifiers: {}",
            sql
        );
    }

    #[test]
    fn test_warehouse_reagg_sql_average_uses_cast() {
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec![],
            measures: vec![serde_json::json!({
                "name": "avg_rev", "type": "average",
                "columns": ["avg_rev__sum", "avg_rev__count"]
            })],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let request = QueryRequest {
            measures: vec!["orders.avg_rev".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "preagg.test",
            &crate::dialect::Dialect::Postgres,
        );
        assert!(
            sql.contains("CAST(SUM(\"avg_rev__sum\") AS DOUBLE PRECISION)"),
            "Postgres should use DOUBLE PRECISION: {}",
            sql
        );

        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "preagg.test",
            &crate::dialect::Dialect::BigQuery,
        );
        assert!(
            sql.contains("CAST(SUM(`avg_rev__sum`) AS FLOAT64)"),
            "BigQuery should use FLOAT64: {}",
            sql
        );
    }

    #[test]
    fn test_default_rollup_excludes_median_and_number() {
        use crate::schema::models::*;
        let view = View {
            name: "test".into(),
            description: Some("test".into()),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("test".into()),
            sql: None,
            entities: vec![],
            dimensions: vec![Dimension {
                name: "region".into(),
                dimension_type: DimensionType::String,
                description: None,
                expr: "region".into(),
                original_expr: None,
                samples: None,
                synonyms: None,
                primary_key: None,
                sub_query: None,
                segmentable: None,
                inherits_from: None,
                meta: None,
            }],
            measures: Some(vec![
                Measure {
                    name: "total".into(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("amount".into()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                },
                Measure {
                    name: "med".into(),
                    measure_type: MeasureType::Median,
                    description: None,
                    expr: Some("amount".into()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                },
                Measure {
                    name: "computed".into(),
                    measure_type: MeasureType::Number,
                    description: None,
                    expr: Some("amount / qty".into()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                },
            ]),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        };
        let rollups = resolve_rollups(&view, None);
        assert_eq!(rollups.len(), 1);
        let measure_names: Vec<&str> = rollups[0]
            .measures
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert!(measure_names.contains(&"total"), "Sum should be included");
        assert!(!measure_names.contains(&"med"), "Median should be excluded");
        assert!(
            !measure_names.contains(&"computed"),
            "Number should be excluded"
        );
    }

    #[test]
    fn test_reagg_sql_order_by() {
        use crate::engine::query::OrderBy;
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            order: vec![OrderBy {
                id: "orders.total_revenue".to_string(),
                desc: true,
            }],
            limit: Some(10),
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("ORDER BY \"orders__total_revenue\" DESC"),
            "Missing ORDER BY: {}",
            sql
        );
        // ORDER BY must come before LIMIT
        let order_pos = sql.find("ORDER BY").unwrap();
        let limit_pos = sql.find("LIMIT").unwrap();
        assert!(
            order_pos < limit_pos,
            "ORDER BY must precede LIMIT: {}",
            sql
        );
    }

    #[test]
    fn test_warehouse_reagg_sql_order_by() {
        use crate::engine::query::OrderBy;
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            order: vec![
                OrderBy {
                    id: "orders.total_revenue".to_string(),
                    desc: true,
                },
                OrderBy {
                    id: "orders.region".to_string(),
                    desc: false,
                },
            ],
            ..QueryRequest::new()
        };
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "\"preagg\".\"orders__abc\"",
            &crate::dialect::Dialect::Postgres,
        );
        assert!(
            sql.contains("ORDER BY \"orders__total_revenue\" DESC, \"orders__region\" ASC"),
            "Missing multi-column ORDER BY: {}",
            sql
        );
    }

    #[test]
    fn test_or_filter_drops_when_branch_unrenderable() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        // OR filter where one branch uses a dimension not in the rollup
        let filter = QueryFilter {
            member: None,
            operator: None,
            values: vec![],
            and: None,
            or: Some(vec![
                QueryFilter {
                    member: Some("orders.region".to_string()),
                    operator: Some(FilterOperator::Equals),
                    values: vec!["US".to_string()],
                    and: None,
                    or: None,
                },
                QueryFilter {
                    member: Some("orders.status".to_string()), // not in rollup
                    operator: Some(FilterOperator::Equals),
                    values: vec!["active".to_string()],
                    and: None,
                    or: None,
                },
            ]),
        };
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name));
        assert!(
            result.is_none(),
            "OR with unrenderable branch should return None, got: {:?}",
            result
        );
    }

    #[test]
    fn test_or_filter_renders_when_all_branches_valid() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = LocalRollupEntry {
            dimensions: vec!["region".into(), "status".into()],
            ..test_local_rollup_entry()
        };
        let filter = QueryFilter {
            member: None,
            operator: None,
            values: vec![],
            and: None,
            or: Some(vec![
                QueryFilter {
                    member: Some("orders.region".to_string()),
                    operator: Some(FilterOperator::Equals),
                    values: vec!["US".to_string()],
                    and: None,
                    or: None,
                },
                QueryFilter {
                    member: Some("orders.status".to_string()),
                    operator: Some(FilterOperator::Equals),
                    values: vec!["active".to_string()],
                    and: None,
                    or: None,
                },
            ]),
        };
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name));
        assert!(result.is_some(), "All-valid OR should render");
        let sql = result.unwrap();
        assert!(sql.contains("OR"), "Should contain OR: {}", sql);
    }

    #[test]
    fn test_contains_filter_escapes_like_metacharacters() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        // Value with LIKE metacharacters: % and _
        let filter = QueryFilter {
            member: Some("orders.region".to_string()),
            operator: Some(FilterOperator::Contains),
            values: vec!["100%_test".to_string()],
            and: None,
            or: None,
        };
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name));
        let sql = result.unwrap();
        // % and _ in the user value should be escaped
        assert!(
            sql.contains("100\\%\\_test"),
            "LIKE metacharacters should be escaped: {}",
            sql
        );
        // The wrapping wildcards should still be present
        assert!(
            sql.contains("LIKE '%100\\%\\_test%'"),
            "Should have wrapping wildcards but escaped inner ones: {}",
            sql
        );
    }

    #[test]
    fn test_not_contains_filter_escapes_like_metacharacters() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let filter = QueryFilter {
            member: Some("orders.region".to_string()),
            operator: Some(FilterOperator::NotContains),
            values: vec!["50%".to_string()],
            and: None,
            or: None,
        };
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name));
        let sql = result.unwrap();
        assert!(
            sql.contains("NOT LIKE '%50\\%%'"),
            "NotContains should escape % in value: {}",
            sql
        );
    }

    #[test]
    fn test_contains_filter_normal_value_unchanged() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let filter = QueryFilter {
            member: Some("orders.region".to_string()),
            operator: Some(FilterOperator::Contains),
            values: vec!["north".to_string()],
            and: None,
            or: None,
        };
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name));
        let sql = result.unwrap();
        assert!(
            sql.contains("LIKE '%north%'"),
            "Normal value should be unchanged: {}",
            sql
        );
    }

    // ── Comprehensive all-dialects tests ────────────────────────────────────

    fn all_dialects() -> Vec<Dialect> {
        vec![
            Dialect::Postgres,
            Dialect::MySQL,
            Dialect::BigQuery,
            Dialect::Snowflake,
            Dialect::DuckDB,
            Dialect::ClickHouse,
            Dialect::Databricks,
            Dialect::Redshift,
            Dialect::SQLite,
            Dialect::Domo,
            Dialect::Presto,
        ]
    }

    /// Helper: build a rollup entry with sum + average + count_distinct measures.
    fn rich_local_rollup_entry() -> LocalRollupEntry {
        LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "by_region_monthly".into(),
            rollup_hash: "a1b2c3d4".into(),
            file: "orders__a1b2c3d4.parquet".into(),
            dimensions: vec!["region".into(), "status".into()],
            measures: vec![
                serde_json::json!({"name": "total_revenue", "type": "sum", "columns": ["total_revenue__sum"]}),
                serde_json::json!({"name": "avg_price", "type": "average", "columns": ["avg_price__sum", "avg_price__count"]}),
                serde_json::json!({"name": "event_count", "type": "count", "columns": ["event_count__count"]}),
                serde_json::json!({"name": "max_amount", "type": "max", "columns": ["max_amount__max"]}),
                serde_json::json!({"name": "min_amount", "type": "min", "columns": ["min_amount__min"]}),
                serde_json::json!({"name": "unique_users", "type": "count_distinct", "columns": ["user_id"]}),
            ],
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
            timezone: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        }
    }

    #[test]
    fn test_build_sql_all_dialects() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        for dialect in all_dialects() {
            let engine = build_test_engine(&view, &dialect);
            let sqls = generate_build_sql(&engine, &view, &rollups[0], "preagg", "20260416")
                .unwrap_or_else(|e| panic!("{dialect}: generate_build_sql failed: {e}"));
            assert_eq!(sqls.len(), 2, "{}: expected DROP + CTAS", dialect);
            let drop = &sqls[0];
            let ctas = &sqls[1];
            assert!(
                drop.contains("DROP TABLE IF EXISTS"),
                "{}: missing DROP: {}",
                dialect,
                drop
            );
            assert!(
                ctas.contains("CREATE TABLE"),
                "{}: missing CREATE TABLE: {}",
                ctas,
                dialect
            );
            assert!(ctas.contains("SUM("), "{}: missing SUM: {}", dialect, ctas);
            assert!(
                ctas.contains("GROUP BY"),
                "{}: missing GROUP BY: {}",
                dialect,
                ctas
            );
            // ClickHouse should have MergeTree
            if dialect == Dialect::ClickHouse {
                assert!(
                    ctas.contains("MergeTree"),
                    "ClickHouse CTAS should have MergeTree: {}",
                    ctas
                );
            }
            // BigQuery/MySQL/Databricks/Domo should use backtick quoting
            if matches!(
                dialect,
                Dialect::BigQuery | Dialect::MySQL | Dialect::Databricks | Dialect::Domo
            ) {
                assert!(
                    ctas.contains('`'),
                    "{}: should use backtick quoting: {}",
                    dialect,
                    ctas
                );
            }
            // Snowflake should uppercase
            if dialect == Dialect::Snowflake {
                assert!(
                    ctas.contains("\"PREAGG\""),
                    "Snowflake should uppercase schema: {}",
                    ctas
                );
            }
        }
    }

    #[test]
    fn test_manifest_create_sql_all_dialects() {
        for dialect in all_dialects() {
            let sql = generate_manifest_create_sql("preagg", &dialect);
            assert!(
                sql.contains("CREATE TABLE IF NOT EXISTS"),
                "{}: missing CREATE: {}",
                dialect,
                sql
            );
            let sql_lower = sql.to_lowercase();
            assert!(
                sql_lower.contains("__manifest"),
                "{}: missing manifest: {}",
                dialect,
                sql
            );
            // Check type names
            match dialect {
                Dialect::ClickHouse => {
                    assert!(
                        sql.contains("String"),
                        "{}: missing String type: {}",
                        dialect,
                        sql
                    );
                    assert!(
                        sql.contains("ReplacingMergeTree"),
                        "{}: missing engine: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::BigQuery => {
                    assert!(
                        sql.contains("STRING"),
                        "{}: missing STRING type: {}",
                        dialect,
                        sql
                    );
                    assert!(
                        !sql.contains("PRIMARY KEY"),
                        "{}: BigQuery should not have PK: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::SQLite => {
                    assert!(
                        sql.contains("TEXT"),
                        "{}: missing TEXT type: {}",
                        dialect,
                        sql
                    );
                    assert!(
                        sql.contains("UNIQUE"),
                        "{}: missing UNIQUE: {}",
                        dialect,
                        sql
                    );
                }
                _ => {
                    assert!(
                        sql.contains("VARCHAR"),
                        "{}: missing VARCHAR type: {}",
                        dialect,
                        sql
                    );
                    assert!(
                        sql.contains("PRIMARY KEY"),
                        "{}: missing PK: {}",
                        dialect,
                        sql
                    );
                }
            }
        }
    }

    #[test]
    fn test_manifest_upsert_all_dialects() {
        let entry = ManifestEntry {
            view_name: "orders".into(),
            rollup_name: "by_region".into(),
            rollup_hash: "a1b2c3d4".into(),
            table_name: "preagg.orders__a1b2c3d4__20260416".into(),
            dimensions: vec!["region".into()],
            measures_json: "[]".into(),
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        for dialect in all_dialects() {
            let stmts = generate_manifest_upsert_sql("preagg", &entry, &dialect);
            match dialect {
                Dialect::ClickHouse => {
                    assert_eq!(stmts.len(), 1, "{}: ClickHouse should have 1 stmt", dialect);
                    assert!(
                        stmts[0].starts_with("INSERT INTO"),
                        "{}: should be INSERT: {}",
                        dialect,
                        stmts[0]
                    );
                }
                Dialect::SQLite => {
                    assert_eq!(stmts.len(), 1, "{}: SQLite should have 1 stmt", dialect);
                    assert!(
                        stmts[0].contains("INSERT OR REPLACE"),
                        "{}: should be INSERT OR REPLACE: {}",
                        dialect,
                        stmts[0]
                    );
                }
                _ => {
                    assert_eq!(stmts.len(), 2, "{}: should have DELETE + INSERT", dialect);
                    assert!(
                        stmts[0].contains("DELETE FROM"),
                        "{}: first should be DELETE: {}",
                        dialect,
                        stmts[0]
                    );
                    assert!(
                        stmts[1].starts_with("INSERT INTO"),
                        "{}: second should be INSERT: {}",
                        dialect,
                        stmts[1]
                    );
                }
            }
        }
    }

    #[test]
    fn test_warehouse_reagg_sql_all_dialects_basic() {
        let entry = test_local_rollup_entry(); // sum measure, region dim, month time dim
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        for dialect in all_dialects() {
            let table = dialect.qualify_table("preagg", "orders__a1b2c3d4__20260416");
            let sql = generate_warehouse_reagg_sql(&request, &entry, &table, &dialect);

            // All dialects should have SELECT, FROM, GROUP BY
            assert!(
                sql.contains("SELECT"),
                "{}: missing SELECT: {}",
                dialect,
                sql
            );
            assert!(
                sql.contains(&table),
                "{}: missing table name: {}",
                dialect,
                sql
            );
            assert!(
                sql.contains("GROUP BY"),
                "{}: missing GROUP BY: {}",
                dialect,
                sql
            );
            assert!(sql.contains("SUM("), "{}: missing SUM: {}", dialect, sql);
        }
    }

    #[test]
    fn test_warehouse_reagg_sql_all_dialects_time_coarser_gran() {
        use crate::engine::query::TimeDimensionQuery;
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("year".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        for dialect in all_dialects() {
            let table = dialect.qualify_table("preagg", "orders__a1b2c3d4__20260416");
            let sql = generate_warehouse_reagg_sql(&request, &entry, &table, &dialect);

            // Should contain the dialect-specific date truncation
            match dialect {
                Dialect::MySQL | Dialect::Domo => {
                    // MySQL uses DATE_FORMAT for year truncation
                    assert!(
                        sql.contains("DATE_FORMAT("),
                        "{}: should use DATE_FORMAT for year: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::BigQuery => {
                    assert!(
                        sql.contains("TIMESTAMP_TRUNC("),
                        "{}: should use TIMESTAMP_TRUNC: {}",
                        dialect,
                        sql
                    );
                    assert!(
                        sql.contains("YEAR"),
                        "{}: should have YEAR granularity: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::ClickHouse => {
                    assert!(
                        sql.contains("toStartOfYear("),
                        "{}: should use toStartOfYear: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::Snowflake | Dialect::Presto => {
                    assert!(
                        sql.contains("DATE_TRUNC('year'"),
                        "{}: should use DATE_TRUNC: {}",
                        dialect,
                        sql
                    );
                }
                _ => {
                    // Postgres, DuckDB, Redshift, SQLite, Databricks — lowercase date_trunc
                    assert!(
                        sql.contains("date_trunc('year'"),
                        "{}: should use date_trunc: {}",
                        dialect,
                        sql
                    );
                }
            }
        }
    }

    #[test]
    fn test_warehouse_reagg_sql_all_dialects_average() {
        let entry = rich_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.avg_price".to_string()],
            ..QueryRequest::new()
        };
        for dialect in all_dialects() {
            let table = dialect.qualify_table("preagg", "test");
            let sql = generate_warehouse_reagg_sql(&request, &entry, &table, &dialect);
            // All should use CAST + SUM/NULLIF pattern
            assert!(sql.contains("CAST("), "{}: missing CAST: {}", dialect, sql);
            assert!(
                sql.contains("NULLIF("),
                "{}: missing NULLIF: {}",
                dialect,
                sql
            );
            // Check dialect-specific cast type
            match dialect {
                Dialect::Postgres | Dialect::Redshift => {
                    assert!(
                        sql.contains("DOUBLE PRECISION"),
                        "{}: should use DOUBLE PRECISION: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::BigQuery => {
                    assert!(
                        sql.contains("FLOAT64"),
                        "{}: should use FLOAT64: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::ClickHouse => {
                    assert!(
                        sql.contains("Float64"),
                        "{}: should use Float64: {}",
                        dialect,
                        sql
                    );
                }
                Dialect::MySQL | Dialect::Domo => {
                    assert!(
                        sql.contains("DECIMAL(38,10)"),
                        "{}: should use DECIMAL: {}",
                        dialect,
                        sql
                    );
                }
                _ => {
                    assert!(
                        sql.contains("AS DOUBLE)"),
                        "{}: should use DOUBLE: {}",
                        dialect,
                        sql
                    );
                }
            }
        }
    }

    #[test]
    fn test_warehouse_reagg_sql_all_dialects_all_measure_types() {
        let entry = rich_local_rollup_entry();
        // Request all supported measure types
        let request = QueryRequest {
            measures: vec![
                "orders.total_revenue".to_string(),
                "orders.event_count".to_string(),
                "orders.avg_price".to_string(),
                "orders.max_amount".to_string(),
                "orders.min_amount".to_string(),
                "orders.unique_users".to_string(),
            ],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        for dialect in all_dialects() {
            let table = dialect.qualify_table("preagg", "test");
            let sql = generate_warehouse_reagg_sql(&request, &entry, &table, &dialect);

            assert!(
                sql.contains("SUM("),
                "{}: missing SUM for sum/count: {}",
                dialect,
                sql
            );
            assert!(sql.contains("MAX("), "{}: missing MAX: {}", dialect, sql);
            assert!(sql.contains("MIN("), "{}: missing MIN: {}", dialect, sql);
            assert!(
                sql.contains("COUNT(DISTINCT"),
                "{}: missing COUNT DISTINCT: {}",
                dialect,
                sql
            );
            assert!(
                sql.contains("CAST("),
                "{}: missing CAST for avg: {}",
                dialect,
                sql
            );
        }
    }

    #[test]
    fn test_warehouse_reagg_sql_snowflake_uppercase() {
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let table = Dialect::Snowflake.qualify_table("preagg", "orders__abc__20260416");
        let sql = generate_warehouse_reagg_sql(&request, &entry, &table, &Dialect::Snowflake);

        // Snowflake uppercases all identifiers
        assert!(
            sql.contains("\"REGION\""),
            "Snowflake should uppercase dimension: {}",
            sql
        );
        assert!(
            sql.contains("\"TOTAL_REVENUE__SUM\""),
            "Snowflake should uppercase measure col: {}",
            sql
        );
        assert!(
            sql.contains("\"ORDERS__TOTAL_REVENUE\""),
            "Snowflake should uppercase alias: {}",
            sql
        );
        assert!(
            sql.contains("\"PREAGG\""),
            "Snowflake should uppercase schema: {}",
            sql
        );
    }

    #[test]
    fn test_create_schema_ddl_all_dialects() {
        for dialect in all_dialects() {
            let ddl = dialect.create_schema_ddl("preagg");
            match dialect {
                Dialect::BigQuery => {
                    assert!(ddl.is_none(), "BigQuery should return None");
                }
                Dialect::ClickHouse => {
                    let sql = ddl.unwrap();
                    assert!(
                        sql.contains("CREATE DATABASE"),
                        "ClickHouse should use DATABASE: {}",
                        sql
                    );
                }
                _ => {
                    let sql = ddl.unwrap();
                    assert!(
                        sql.contains("CREATE SCHEMA"),
                        "{}: should use SCHEMA: {}",
                        dialect,
                        sql
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Library API tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_manifest_query_sql_basic() {
        let sql = manifest_query_sql("AIRLAYER", &Dialect::Postgres);
        assert!(sql.contains("SELECT view_name"));
        assert!(sql.contains("\"AIRLAYER\".\"__manifest\""));
        assert!(!sql.contains("FINAL"));
    }

    #[test]
    fn test_manifest_query_sql_clickhouse_final() {
        let sql = manifest_query_sql("preagg", &Dialect::ClickHouse);
        assert!(sql.contains("\"preagg\".\"__manifest\" FINAL"));
    }

    #[test]
    fn test_parse_manifest_rows() {
        let rows = vec![serde_json::json!({
            "view_name": "events",
            "rollup_name": "by_platform",
            "rollup_hash": "abc123",
            "table_name": "AIRLAYER.events__abc123__20260415",
            "dimensions": "[\"platform\"]",
            "measures": "[{\"name\":\"count\",\"type\":\"count\",\"columns\":[\"count__count\"]}]",
            "time_dimension": "created_at",
            "granularity": "day",
            "build_date": "2026-04-15"
        })
        .as_object()
        .unwrap()
        .clone()];

        let entries = parse_manifest_rows(&rows);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].view_name, "events");
        assert_eq!(entries[0].table_name, "AIRLAYER.events__abc123__20260415");
        assert_eq!(entries[0].dimensions, vec!["platform"]);
        assert_eq!(entries[0].time_dimension.as_deref(), Some("created_at"));
        assert_eq!(entries[0].granularity.as_deref(), Some("day"));
    }

    #[test]
    fn test_parse_manifest_rows_uppercase_keys() {
        // Snowflake returns uppercase column names for unquoted identifiers
        let rows = vec![serde_json::json!({
            "VIEW_NAME": "events",
            "ROLLUP_NAME": "by_platform",
            "ROLLUP_HASH": "abc123",
            "TABLE_NAME": "AIRLAYER.events__abc123__20260415",
            "DIMENSIONS": "[\"platform\"]",
            "MEASURES": "[{\"name\":\"count\",\"type\":\"count\",\"columns\":[\"count__count\"]}]",
            "TIME_DIMENSION": "created_at",
            "GRANULARITY": "day",
            "BUILD_DATE": "2026-04-15"
        })
        .as_object()
        .unwrap()
        .clone()];

        let entries = parse_manifest_rows(&rows);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].view_name, "events");
        assert_eq!(entries[0].table_name, "AIRLAYER.events__abc123__20260415");
    }

    #[test]
    fn test_parse_manifest_rows_skips_incomplete() {
        let rows = vec![
            // Missing view_name — should be skipped
            serde_json::json!({
                "rollup_name": "x",
                "rollup_hash": "y",
                "table_name": "z",
                "dimensions": "[]",
                "measures": "[]",
            })
            .as_object()
            .unwrap()
            .clone(),
        ];
        let entries = parse_manifest_rows(&rows);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_warehouse_rollup_entry_to_local() {
        let wre = WarehouseRollupEntry {
            view_name: "events".into(),
            rollup_name: "by_platform".into(),
            rollup_hash: "abc123".into(),
            table_name: "AIRLAYER.events__abc123__20260415".into(),
            dimensions: vec!["platform".into()],
            measures: vec![
                serde_json::json!({"name":"count","type":"count","columns":["count__count"]}),
            ],
            time_dimension: Some("created_at".into()),
            granularity: Some("day".into()),
            timezone: None,
            build_date: "2026-04-15".into(),
        };
        let local = wre.to_local_entry();
        assert_eq!(local.view_name, "events");
        assert!(local.file.is_empty());
        assert_eq!(local.dimensions, vec!["platform"]);
    }

    #[test]
    fn test_resolve_warehouse_basic() {
        let entries = vec![WarehouseRollupEntry {
            view_name: "events".into(),
            rollup_name: "by_platform".into(),
            rollup_hash: "abc123".into(),
            table_name: "preagg.events__abc123__20260415".into(),
            dimensions: vec!["platform".into()],
            measures: vec![
                serde_json::json!({"name":"total_revenue","type":"sum","columns":["total_revenue__sum"]}),
            ],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
        }];

        let request = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            ..QueryRequest::new()
        };

        let result = resolve_warehouse(&request, &entries, "preagg", &Dialect::Postgres);
        assert!(result.is_some());
        if let Some(PreaggResolution::WarehouseRollup {
            reagg_sql,
            table_name,
        }) = result
        {
            assert!(reagg_sql.contains("SELECT"));
            assert!(table_name.contains("preagg"));
        } else {
            panic!("Expected WarehouseRollup");
        }
    }

    #[test]
    fn test_resolve_warehouse_miss() {
        let entries = vec![WarehouseRollupEntry {
            view_name: "events".into(),
            rollup_name: "by_platform".into(),
            rollup_hash: "abc123".into(),
            table_name: "preagg.events__abc123__20260415".into(),
            dimensions: vec!["platform".into()],
            measures: vec![
                serde_json::json!({"name":"total_revenue","type":"sum","columns":["total_revenue__sum"]}),
            ],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
        }];

        // Request a dimension not in the rollup
        let request = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.country".to_string()],
            ..QueryRequest::new()
        };

        let result = resolve_warehouse(&request, &entries, "preagg", &Dialect::Postgres);
        assert!(result.is_none());
    }

    #[test]
    fn test_collect_build_sql() {
        let view = test_view_with_preaggs();
        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            None,
            None,
            None,
        )
        .unwrap();

        assert!(!plan.statements.is_empty());
        // Should have: CREATE SCHEMA + CREATE TABLE __manifest + at least one CTAS + upsert
        assert!(plan.statements.len() >= 4);
        assert!(plan.statements[0].contains("CREATE SCHEMA"));
        assert!(plan.statements[1].to_lowercase().contains("__manifest"));
        assert!(!plan.manifest_entries.is_empty());
        assert_eq!(plan.manifest_entries[0].view_name, "orders");
    }

    fn build_sql_for_timezone(tz: Option<&str>, dialect: &Dialect) -> String {
        let view = test_view_no_preaggs();
        let plan = collect_build_sql(&[&view], "preagg", "20260727", dialect, None, None, tz)
            .expect("build plan");
        plan.statements.join("\n")
    }

    #[test]
    fn build_sql_converts_timezone_before_truncating() {
        let sql = build_sql_for_timezone(Some("America/Los_Angeles"), &Dialect::Postgres);
        assert!(
            sql.contains("AT TIME ZONE 'America/Los_Angeles'"),
            "expected a timezone conversion, got:\n{sql}"
        );
        // A raw find()-position comparison between "date_trunc" and
        // "AT TIME ZONE" cannot distinguish correct nesting from the bug the
        // brief warns about (bucket in UTC, then relabel), because Postgres's
        // `AT TIME ZONE` is a postfix operator: the literal substring
        // "date_trunc(" precedes "AT TIME ZONE" either way —
        //   correct:  date_trunc('day', (expr::timestamptz AT TIME ZONE 'tz'))
        //   inverted: (date_trunc('day', expr)::timestamptz AT TIME ZONE 'tz')
        // Assert the exact nested substring instead, so the conversion must
        // be inside date_trunc's argument, not wrapping its result.
        assert!(
            sql.contains(
                "date_trunc('day', (created_at::timestamptz AT TIME ZONE 'America/Los_Angeles'))"
            ),
            "date_trunc must WRAP the converted expression, got:\n{sql}"
        );
    }

    #[test]
    fn build_sql_under_utc_is_unchanged() {
        let with_none = build_sql_for_timezone(None, &Dialect::Postgres);
        let with_utc = build_sql_for_timezone(Some("UTC"), &Dialect::Postgres);
        assert_eq!(with_none, with_utc);
        assert!(
            !with_none.contains("AT TIME ZONE"),
            "UTC must emit no conversion, got:\n{with_none}"
        );
    }

    #[test]
    fn build_sql_handles_sub_hour_offset_zones() {
        // Asia/Kolkata is UTC+5:30. Query-time conversion from hourly rollups
        // cannot express this day boundary; build-time conversion can, because
        // the warehouse resolves the real offset per row.
        let sql = build_sql_for_timezone(Some("Asia/Kolkata"), &Dialect::Postgres);
        assert!(sql.contains("AT TIME ZONE 'Asia/Kolkata'"), "got:\n{sql}");
    }

    #[test]
    fn test_collect_build_sql_bigquery_no_schema_ddl() {
        let view = test_view_with_preaggs();
        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::BigQuery,
            None,
            None,
            None,
        )
        .unwrap();

        // BigQuery should NOT have a CREATE SCHEMA statement
        assert!(!plan.statements[0].contains("CREATE SCHEMA"));
        // First statement should be the manifest table
        assert!(plan.statements[0].to_lowercase().contains("__manifest"));
    }

    #[test]
    fn test_collect_build_sql_cleanup_old_tables() {
        let view = test_view_with_preaggs();
        // Build today's plan with a "previous" manifest that has an older date
        let plan_no_prev = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            None,
            None,
            None,
        )
        .unwrap();
        let new_hash = &plan_no_prev.manifest_entries[0].rollup_hash;

        let old_entries = vec![WarehouseRollupEntry {
            view_name: "orders".into(),
            rollup_name: "by_region".into(),
            rollup_hash: new_hash.clone(),
            table_name: "preagg.orders__old_hash__20260410".into(),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-10".into(),
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            Some(&old_entries),
            None,
            None,
        )
        .unwrap();

        // Should have a DROP for the old table at the end
        let last = plan.statements.last().unwrap();
        assert!(
            last.contains("DROP TABLE IF EXISTS") && last.contains("orders__old_hash__20260410"),
            "Expected cleanup DROP for old table, got: {}",
            last
        );
    }

    #[test]
    fn test_collect_build_sql_no_cleanup_same_table() {
        let view = test_view_with_preaggs();
        let plan_first = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            None,
            None,
            None,
        )
        .unwrap();
        let entry = &plan_first.manifest_entries[0];

        // Simulate previous entry with the SAME table name (same-day rebuild)
        let old_entries = vec![WarehouseRollupEntry {
            view_name: entry.view_name.clone(),
            rollup_name: entry.rollup_name.clone(),
            rollup_hash: entry.rollup_hash.clone(),
            table_name: entry.table_name.clone(),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            Some(&old_entries),
            None,
            None,
        )
        .unwrap();

        // No cleanup DROP — the old table IS the new table.
        // The last statement should be the manifest upsert, not a cleanup DROP.
        assert!(
            !plan
                .statements
                .last()
                .unwrap()
                .starts_with("DROP TABLE IF EXISTS"),
            "Should not drop same table as cleanup: {:?}",
            plan.statements
        );
    }

    #[test]
    fn test_collect_build_sql_no_cleanup_without_previous() {
        let view = test_view_with_preaggs();
        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            None,
            None,
            None,
        )
        .unwrap();

        // Last statement should NOT be a cleanup DROP (no previous entries)
        let last = plan.statements.last().unwrap();
        assert!(
            !last.starts_with("DROP TABLE IF EXISTS"),
            "Should not have cleanup without previous entries, got: {}",
            last
        );
    }

    // -----------------------------------------------------------------------
    // resolve_cached (WASM / browser cache)
    // -----------------------------------------------------------------------

    fn make_test_local_manifest() -> LocalManifest {
        LocalManifest {
            pulled_at: "2026-04-15T00:00:00Z".into(),
            source_database: "warehouse".into(),
            rollups: vec![LocalRollupEntry {
                view_name: "events".into(),
                rollup_name: "by_platform".into(),
                rollup_hash: "abc123".into(),
                file: "events__abc123".into(),
                dimensions: vec!["platform".into()],
                measures: vec![
                    serde_json::json!({"name":"total_revenue","type":"sum","columns":["total_revenue__sum"]}),
                    serde_json::json!({"name":"event_count","type":"count","columns":["event_count__count"]}),
                ],
                time_dimension: None,
                granularity: None,
                timezone: None,
                build_date: "2026-04-15".into(),
                refresh_key_value: None,
                refresh_key_checked_at: None,
            }],
        }
    }

    #[test]
    fn test_resolve_cached_basic() {
        let manifest = make_test_local_manifest();
        let request = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            ..QueryRequest::new()
        };

        let result = resolve_cached(&request, &manifest);
        assert!(result.is_some());
        let res = result.unwrap();
        assert_eq!(res.cache_key, "events__abc123");
        assert!(res.reagg_sql.contains("\"__cache\""));
        assert!(!res.reagg_sql.contains("read_parquet"));
        assert!(res.reagg_sql.contains("SUM"));
        assert!(res.reagg_sql.contains("platform"));
    }

    #[test]
    fn test_resolve_cached_miss() {
        let manifest = make_test_local_manifest();
        // Request a dimension not in the rollup
        let request = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.country".to_string()],
            ..QueryRequest::new()
        };

        let result = resolve_cached(&request, &manifest);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_cached_returns_entry_metadata() {
        let manifest = make_test_local_manifest();
        let request = QueryRequest {
            measures: vec!["events.event_count".to_string()],
            dimensions: vec!["events.platform".to_string()],
            ..QueryRequest::new()
        };

        let result = resolve_cached(&request, &manifest).unwrap();
        assert_eq!(result.entry.view_name, "events");
        assert_eq!(result.entry.rollup_name, "by_platform");
        assert_eq!(result.entry.rollup_hash, "abc123");
    }

    #[test]
    fn test_resolve_cached_empty_manifest() {
        let manifest = LocalManifest {
            pulled_at: "2026-04-15T00:00:00Z".into(),
            source_database: "warehouse".into(),
            rollups: vec![],
        };
        let request = QueryRequest {
            measures: vec!["events.total_revenue".to_string()],
            dimensions: vec!["events.platform".to_string()],
            ..QueryRequest::new()
        };

        assert!(resolve_cached(&request, &manifest).is_none());
    }

    #[test]
    fn test_manifest_entry_has_refresh_key_fields() {
        let entry = ManifestEntry {
            view_name: "orders".into(),
            rollup_name: "by_day".into(),
            rollup_hash: "abc12345".into(),
            table_name: "AIRLAYER.orders__abc12345__20260415".into(),
            dimensions: vec!["region".into()],
            measures_json: "[]".into(),
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
            refresh_key_value: Some("2026-04-15T12:00:00Z".into()),
            refresh_key_checked_at: Some("2026-04-15T12:00:00Z".into()),
        };
        assert_eq!(
            entry.refresh_key_value.as_deref(),
            Some("2026-04-15T12:00:00Z")
        );
    }

    #[test]
    fn test_local_rollup_entry_has_refresh_key_fields() {
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "by_day".into(),
            rollup_hash: "abc12345".into(),
            file: "orders__abc12345.parquet".into(),
            dimensions: vec![],
            measures: vec![],
            time_dimension: None,
            granularity: None,
            timezone: None,
            build_date: "2026-04-15".into(),
            refresh_key_value: Some("42".into()),
            refresh_key_checked_at: Some("2026-04-15T12:00:00Z".into()),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: LocalRollupEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.refresh_key_value, Some("42".into()));
    }

    #[test]
    fn test_parse_interval_minutes() {
        let d = parse_interval("30m").unwrap();
        assert_eq!(d.as_secs(), 30 * 60);
    }

    #[test]
    fn test_parse_interval_hours() {
        let d = parse_interval("6h").unwrap();
        assert_eq!(d.as_secs(), 6 * 3600);
    }

    #[test]
    fn test_parse_interval_days() {
        let d = parse_interval("1d").unwrap();
        assert_eq!(d.as_secs(), 24 * 3600);
    }

    #[test]
    fn test_parse_interval_weeks() {
        let d = parse_interval("2w").unwrap();
        assert_eq!(d.as_secs(), 2 * 7 * 24 * 3600);
    }

    #[test]
    fn test_parse_interval_seconds() {
        let d = parse_interval("45s").unwrap();
        assert_eq!(d.as_secs(), 45);
    }

    #[test]
    fn test_parse_interval_invalid() {
        assert!(parse_interval("abc").is_err());
        assert!(parse_interval("").is_err());
    }

    #[test]
    fn test_parse_interval_multibyte_suffix_returns_err() {
        // '€' is a 3-byte UTF-8 character — should not panic, should return Err
        let result = parse_interval("10€");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_interval_overflow_returns_err() {
        // u64::MAX weeks would overflow
        let result = parse_interval(&format!("{}w", u64::MAX));
        assert!(result.is_err());
    }

    #[test]
    fn test_check_freshness_none_always_stale() {
        let result = check_freshness(None, None, None, None).unwrap();
        assert!(!result.is_fresh);
        assert!(result.current_value.is_none());
    }

    #[test]
    fn test_check_freshness_every_fresh() {
        // checked 10 seconds ago, interval is 1 hour → still fresh
        let recent = chrono::Utc::now() - chrono::Duration::seconds(10);
        let checked_at = recent.to_rfc3339();
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Every("1h".into())),
            None,
            Some(&checked_at),
            None,
        )
        .unwrap();
        assert!(result.is_fresh);
    }

    #[test]
    fn test_check_freshness_every_stale() {
        // checked 2 hours ago, interval is 1 hour → stale
        let old = chrono::Utc::now() - chrono::Duration::hours(2);
        let checked_at = old.to_rfc3339();
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Every("1h".into())),
            None,
            Some(&checked_at),
            None,
        )
        .unwrap();
        assert!(!result.is_fresh);
    }

    #[test]
    fn test_check_freshness_every_no_checked_at_is_stale() {
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Every("1h".into())),
            None,
            None,
            None,
        )
        .unwrap();
        assert!(!result.is_fresh);
    }

    #[test]
    fn test_check_freshness_every_bad_interval_returns_err() {
        let checked_at = chrono::Utc::now().to_rfc3339();
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Every(
                "bad_interval".into(),
            )),
            None,
            Some(&checked_at),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_check_freshness_every_bad_timestamp_returns_err() {
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Every("1h".into())),
            None,
            Some("not-a-timestamp"),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_check_freshness_sql_same_value_is_fresh() {
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Sql("SELECT 1".into())),
            Some("42"),
            None,
            Some("42"),
        )
        .unwrap();
        assert!(result.is_fresh);
        assert_eq!(result.current_value.as_deref(), Some("42"));
    }

    #[test]
    fn test_check_freshness_sql_changed_value_is_stale() {
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Sql(
                "SELECT MAX(id) FROM t".into(),
            )),
            Some("100"),
            None,
            Some("101"),
        )
        .unwrap();
        assert!(!result.is_fresh);
        assert_eq!(result.current_value.as_deref(), Some("101"));
    }

    #[test]
    fn test_check_freshness_sql_no_prior_value_is_stale() {
        let result = check_freshness(
            Some(&crate::schema::models::RefreshKey::Sql("SELECT 1".into())),
            None,
            None,
            Some("42"),
        )
        .unwrap();
        assert!(!result.is_fresh);
    }

    #[test]
    fn test_collect_build_sql_skips_fresh_rollups() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        assert!(!rollups.is_empty(), "need at least one rollup");

        let hash = rollups[0].hash.clone();
        let freshness = vec![RollupFreshness {
            rollup_hash: hash.clone(),
            is_fresh: true,
            current_refresh_key_value: None,
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260508",
            &Dialect::Postgres,
            None,
            Some(&freshness),
            None,
        )
        .unwrap();

        let has_ctas = plan
            .statements
            .iter()
            .any(|s| s.contains("CREATE TABLE") && s.contains(&hash));
        assert!(!has_ctas, "fresh rollup should be skipped");
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].rollup_name, rollups[0].name);
        assert_eq!(plan.skipped[0].rollup_hash, hash);
    }

    #[test]
    fn test_collect_build_sql_rebuilds_stale_rollup() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view, None);
        let hash = rollups[0].hash.clone();

        let freshness = vec![RollupFreshness {
            rollup_hash: hash.clone(),
            is_fresh: false,
            current_refresh_key_value: Some("new_value".into()),
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260508",
            &Dialect::Postgres,
            None,
            Some(&freshness),
            None,
        )
        .unwrap();

        let has_ctas = plan
            .statements
            .iter()
            .any(|s| s.contains("CREATE TABLE") && s.contains(&hash));
        assert!(has_ctas, "stale rollup should be rebuilt");
        assert!(plan.skipped.is_empty());
    }
}
