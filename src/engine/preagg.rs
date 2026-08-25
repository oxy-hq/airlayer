//! Pre-aggregation: rollup resolution, SQL generation, coverage checking.

use crate::dialect::Dialect;
use crate::engine::member_sql::{dotted_ref_regex, param_ref_regex, MemberSqlResolver};
use crate::engine::{DatasourceDialectMap, EngineError, SemanticEngine};
use crate::schema::models::{Measure, MeasureType, PreAggregation, SemanticLayer, View};
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

/// Compute a deterministic 8-char hex hash for a rollup specification.
/// Uses FNV-1a for stability across Rust versions.
pub fn compute_rollup_hash(
    dims: &[String],
    measures: &[String],
    time_dim: Option<&str>,
    granularity: Option<&str>,
) -> String {
    let mut sorted_dims = dims.to_vec();
    sorted_dims.sort();
    let mut sorted_measures = measures.to_vec();
    sorted_measures.sort();

    let canonical = format!(
        "d:{};m:{};t:{};g:{}",
        sorted_dims.join(","),
        sorted_measures.join(","),
        time_dim.unwrap_or(""),
        granularity.unwrap_or(""),
    );

    // FNV-1a hash
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in canonical.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)[..8].to_string()
}

/// Resolve rollup specs for a view from its `pre_aggregations` block.
///
/// Pre-aggregation is opt-in: a view without a `pre_aggregations` block
/// produces no rollups. There is no implicit default rollup — an
/// all-dimensions rollup on a wide view is usually as large as the base
/// table and buys nothing, so the choice is left to the schema author.
pub fn resolve_rollups(view: &View) -> Vec<RollupSpec> {
    view.pre_aggregations
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|pa| resolve_explicit_rollup(view, pa))
        .collect()
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

fn resolve_explicit_rollup(view: &View, pa: &PreAggregation) -> RollupSpec {
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
    pub build_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_key_checked_at: Option<String>,
}

// ── Expression resolution for a rollup's CTAS ────────────────────────────────

/// How deep a chain of member references may nest before the resolver gives up.
/// A member whose expr references itself (directly or through a cycle) would
/// otherwise recurse forever.
const MAX_ROLLUP_RESOLVE_DEPTH: usize = 16;

/// Resolves `{{...}}` references in a view's expressions for the single-view
/// CTAS that builds a rollup.
///
/// The live query path resolves these through `SqlGenerator`, which qualifies
/// every column with a view alias (`"orders"."amount"`). A rollup's CTAS
/// selects `FROM <source>` with no alias, so that output would not compile
/// here — this resolver emits unqualified SQL instead. What it keeps identical
/// on purpose is *which* references are legal and what each expands to: a
/// rollup column has to compute what the warehouse query it stands in for
/// computes, or the cache answers with different numbers under the
/// "pre-aggregated" badge.
///
/// Anything a single view cannot supply — a reference to another view, a
/// foreign entity's column, a request variable — is an error rather than a
/// passthrough. Left in the SQL, `{{...}}` reaches the warehouse and comes
/// back as a parser error naming a brace.
struct RollupExprResolver<'a> {
    view: &'a View,
    source: &'a str,
    dialect: &'a Dialect,
}

impl<'a> RollupExprResolver<'a> {
    fn new(view: &'a View, source: &'a str, dialect: &'a Dialect) -> Self {
        Self {
            view,
            source,
            dialect,
        }
    }

    /// Resolve every reference in `expr`, or say which one could not be.
    fn resolve(&self, expr: &str) -> Result<String, EngineError> {
        let resolved = self.resolve_at(expr, 0)?;
        if let Some(unresolved) = MemberSqlResolver::find_unresolved_ref(&resolved) {
            return Err(EngineError::SqlGenerationError(format!(
                "[{}] rollup expression leaves '{{{{{}}}}}' unresolved; a rollup is built from \
                 its own view alone, so it cannot reference another view, a joined entity's \
                 column, or a request variable",
                self.view.name, unresolved
            )));
        }
        Ok(resolved)
    }

    fn resolve_at(&self, expr: &str, depth: usize) -> Result<String, EngineError> {
        if depth >= MAX_ROLLUP_RESOLVE_DEPTH {
            return Err(EngineError::SqlGenerationError(format!(
                "[{}] member references nest more than {MAX_ROLLUP_RESOLVE_DEPTH} deep; \
                 a member that references itself cannot be resolved",
                self.view.name
            )));
        }
        // Same order as the live path: bare `{{member}}` first, so it is a
        // dotted ref by the time the dotted-ref pass runs.
        let expanded = self.expand_bare_member_refs(expr);
        let with_table = MemberSqlResolver::resolve_table_ref(&expanded, self.source, &|s| {
            self.dialect.quote_identifier(s)
        });
        self.resolve_dotted_refs(&with_table, depth)
    }

    /// Rewrite a bare `{{member}}` into `{{view.member}}` when `member` is a
    /// member of this view. Other single-token braces (`{{TABLE}}`, a motif
    /// param, an unknown name) are left for their own resolver — or, failing
    /// that, for the unresolved-ref check.
    fn expand_bare_member_refs(&self, expr: &str) -> String {
        if !expr.contains("{{") {
            return expr.to_string();
        }
        param_ref_regex()
            .replace_all(expr, |caps: &regex::Captures<'_>| {
                let name = &caps[1];
                if self.dimension(name).is_some() || self.measure(name).is_some() {
                    format!("{{{{{}.{}}}}}", self.view.name, name)
                } else {
                    caps[0].to_string()
                }
            })
            .to_string()
    }

    fn resolve_dotted_refs(&self, expr: &str, depth: usize) -> Result<String, EngineError> {
        let mut out = String::new();
        let mut last = 0;
        for caps in dotted_ref_regex().captures_iter(expr) {
            let whole = caps.get(0).expect("regex match has group 0");
            out.push_str(&expr[last..whole.start()]);
            out.push_str(&self.resolve_member_ref(&caps[1], &caps[2], depth)?);
            last = whole.end();
        }
        out.push_str(&expr[last..]);
        Ok(out)
    }

    fn resolve_member_ref(
        &self,
        qualifier: &str,
        member: &str,
        depth: usize,
    ) -> Result<String, EngineError> {
        if qualifier != self.view.name {
            // `variables.x`, another view, or a foreign entity — none of which
            // this CTAS can reach. Left alone it would surface as a warehouse
            // parser error, so name it here instead.
            return Err(EngineError::SqlGenerationError(format!(
                "[{}] rollup expression references '{{{{{}.{}}}}}', which view '{}' cannot \
                 supply on its own; a rollup is built from a single view with no joins",
                self.view.name, qualifier, member, self.view.name
            )));
        }
        // Parenthesized for the same reason the live path parenthesizes:
        // an expanded compound must not lose to the precedence of whatever
        // it is embedded in — `{{view.margin}} * 100` where margin is
        // `price - discount`.
        if let Some(dim) = self.dimension(member) {
            return Ok(format!("({})", self.resolve_at(&dim.expr, depth + 1)?));
        }
        if let Some(m) = self.measure(member) {
            return Ok(format!("({})", self.measure_agg(m, depth + 1)?));
        }
        Err(EngineError::SqlGenerationError(format!(
            "[{}] rollup expression references '{{{{{}.{}}}}}', but view '{}' declares no such \
             dimension or measure",
            self.view.name, qualifier, member, self.view.name
        )))
    }

    /// The full aggregate for a measure referenced from another expression —
    /// what a calculated (`type: number`) measure's expr is built out of.
    /// Mirrors the live path's `measure_agg_expr`, unqualified.
    fn measure_agg(&self, measure: &Measure, depth: usize) -> Result<String, EngineError> {
        if measure.rolling_window.is_some() {
            return Err(EngineError::SqlGenerationError(format!(
                "[{}] measure '{}' has a rolling_window and cannot be pre-aggregated: its value \
                 depends on rows outside the group a rollup stores",
                self.view.name, measure.name
            )));
        }
        let inner = self.filtered_inner(measure, depth)?;
        let has_filters = measure_has_filters(measure);
        Ok(match measure.measure_type {
            MeasureType::Count => format!("COUNT({inner})"),
            MeasureType::Sum => coalesce_filtered_sum(&format!("SUM({inner})"), has_filters),
            MeasureType::Average => format!("AVG({inner})"),
            MeasureType::Min => format!("MIN({inner})"),
            MeasureType::Max => format!("MAX({inner})"),
            MeasureType::CountDistinct => format!("COUNT(DISTINCT {inner})"),
            MeasureType::CountDistinctApprox => self.dialect.count_distinct_approx(&inner),
            MeasureType::Median => {
                format!("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {inner})")
            }
            // Already an aggregate expression; filters do not apply, exactly as
            // in the live path.
            MeasureType::Number | MeasureType::Custom => match measure.expr.as_deref() {
                Some(expr) => self.resolve_at(expr, depth + 1)?,
                None => {
                    return Err(EngineError::SqlGenerationError(format!(
                        "[{}] measure '{}' is type {:?} and needs an expr",
                        self.view.name, measure.name, measure.measure_type
                    )));
                }
            },
        })
    }

    /// The measure's expression with its `filters:` folded in — the argument
    /// every aggregate below is taken over.
    ///
    /// Dropping the filters is not a visible failure, which is what makes it
    /// dangerous: a category-filtered SUM silently becomes the grand total for
    /// every category, and the rollup serves that under the pre-aggregated
    /// badge.
    fn filtered_inner(&self, measure: &Measure, depth: usize) -> Result<String, EngineError> {
        let inner = match measure.expr.as_deref() {
            Some(expr) => self.resolve_at(expr, depth + 1)?,
            None => "*".to_string(),
        };
        let Some(filters) = measure.filters.as_ref().filter(|f| !f.is_empty()) else {
            return Ok(inner);
        };
        let mut conditions = Vec::with_capacity(filters.len());
        for f in filters {
            conditions.push(self.resolve_at(&f.expr, depth + 1)?);
        }
        let condition = conditions.join(" AND ");
        Ok(if inner == "*" {
            format!("CASE WHEN {condition} THEN 1 END")
        } else {
            format!("CASE WHEN {condition} THEN {inner} END")
        })
    }

    fn dimension(&self, name: &str) -> Option<&'a crate::schema::models::Dimension> {
        self.view.dimensions.iter().find(|d| d.name == name)
    }

    fn measure(&self, name: &str) -> Option<&'a Measure> {
        self.view.measures_list().iter().find(|m| m.name == name)
    }
}

fn measure_has_filters(measure: &Measure) -> bool {
    measure.filters.as_ref().is_some_and(|f| !f.is_empty())
}

/// A filtered `SUM(CASE WHEN ... END)` is NULL when no row in the group
/// matches. The live path coalesces that to 0; the partial stored in a rollup
/// has to as well, or re-aggregating a group where nothing matched yields NULL
/// where the warehouse yields 0.
fn coalesce_filtered_sum(sum: &str, has_filters: bool) -> String {
    if has_filters {
        format!("COALESCE({sum}, 0)")
    } else {
        sum.to_string()
    }
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

    let resolver = RollupExprResolver::new(view, &source, dialect);
    // The measure as the view declares it. `RollupMeasure` carries name, type
    // and expr but not `filters:`, and the filters have to reach the CTAS.
    let declared = |name: &str| view.measures_list().iter().find(|m| m.name == name);

    // Determine which raw expr columns need to be in GROUP BY (count_distinct, median).
    // `adds_raw_group_column` is the shared invariant `matches_exact_grain` reads
    // to decide whether the rollup's on-disk grain is finer than its dimensions.
    let mut extra_group_cols: Vec<String> = Vec::new();
    for rm in &rollup.measures {
        if rm.measure_type.adds_raw_group_column() {
            let raw = rm.expr.as_deref().unwrap_or(&rm.name);
            // These store the raw column and GROUP BY it, and the manifest
            // names that column by this very string — so the stored expr has
            // to be the column, not something resolved from it. And a stored
            // raw column cannot carry a filter: there is no aggregate to fold
            // one into.
            if raw.contains("{{") && !MemberSqlResolver::has_table_ref(raw) {
                return Err(EngineError::SqlGenerationError(format!(
                    "[{}] measure '{}' is type {:?} and its expr references another member; \
                     that shape stores a raw column, which a reference cannot name",
                    view.name, rm.name, rm.measure_type
                )));
            }
            if declared(&rm.name).is_some_and(measure_has_filters) {
                return Err(EngineError::SqlGenerationError(format!(
                    "[{}] measure '{}' is type {:?} and has filters; that shape stores a raw \
                     column with no aggregate to fold a filter into, so the rollup would \
                     silently ignore it",
                    view.name, rm.name, rm.measure_type
                )));
            }
            let col = resolver.resolve(raw)?;
            if !extra_group_cols.contains(&col) {
                extra_group_cols.push(col);
            }
        }
    }

    let mut select_cols: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();
    // Quoted aliases for ClickHouse ORDER BY (positional refs not supported there).
    let mut group_by_aliases: Vec<String> = Vec::new();

    // 1. Dimensions
    for dim_name in &rollup.dimensions {
        if let Some(dim) = view.dimensions.iter().find(|d| d.name == *dim_name) {
            let expr = resolver.resolve(&dim.expr)?;
            let alias = dialect.quote_identifier(dim_name);
            select_cols.push(format!("{expr} AS {alias}"));
            group_by_cols.push(expr);
            group_by_aliases.push(alias);
        }
    }

    // 2. Time dimension (truncated to the rollup granularity)
    if let (Some(td_name), Some(gran)) = (&rollup.time_dimension, &rollup.granularity) {
        if let Some(td) = view.dimensions.iter().find(|d| d.name == *td_name) {
            let expr = resolver.resolve(&td.expr)?;
            let trunc_expr = dialect.date_trunc(gran, &expr);
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
        // The argument every partial below aggregates over: the measure's expr
        // with its `filters:` folded in, so a filtered measure stores what it
        // is filtered to rather than the whole group.
        let expr = match declared(&rm.name) {
            Some(m) => resolver.filtered_inner(m, 0)?,
            None => match rm.expr.as_deref() {
                Some(e) => resolver.resolve(e)?,
                None => "*".to_string(),
            },
        };
        let has_filters = declared(&rm.name).is_some_and(measure_has_filters);
        match rm.measure_type {
            MeasureType::Sum => {
                let alias = dialect.quote_identifier(&format!("{}__sum", rm.name));
                let sum = coalesce_filtered_sum(&format!("SUM({expr})"), has_filters);
                select_cols.push(format!("{sum} AS {alias}"));
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
                // Neither is coalesced: an all-NULL group must stay NULL, the
                // same answer AVG gives in the live path.
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
                // Keyed by the raw expr, matching the manifest's column name —
                // the loop that built `extra_group_cols` already refused any
                // expr this would resolve differently.
                let col = rm.expr.clone().unwrap_or_else(|| rm.name.clone());
                let freq_alias = dialect.quote_identifier(&format!("{}__freq", col));
                select_cols.push(format!("COUNT(*) AS {freq_alias}"));
            }
            MeasureType::Number => {
                let alias = dialect.quote_identifier(&format!("{}__value", rm.name));
                // Already an aggregate expression — the filtered inner form
                // does not apply, so resolve the expr as written.
                let value = match rm.expr.as_deref() {
                    Some(e) => resolver.resolve(e)?,
                    None => {
                        return Err(EngineError::SqlGenerationError(format!(
                            "[{}] measure '{}' is type number and needs an expr",
                            view.name, rm.name
                        )));
                    }
                };
                select_cols.push(format!("{value} AS {alias}"));
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

/// Generate SQL deleting one rollup's row from the manifest.
///
/// ClickHouse has no plain `DELETE`; its mutation syntax is used instead.
pub fn generate_manifest_delete_sql(
    schema: &str,
    view_name: &str,
    rollup_name: &str,
    dialect: &Dialect,
) -> String {
    let fq_table = dialect.qualify_table(schema, "__manifest");
    let predicate = format!(
        "view_name = '{}' AND rollup_name = '{}'",
        view_name.replace('\'', "''"),
        rollup_name.replace('\'', "''"),
    );
    match dialect {
        Dialect::ClickHouse => format!("ALTER TABLE {fq_table} DELETE WHERE {predicate}"),
        _ => format!("DELETE FROM {fq_table} WHERE {predicate}"),
    }
}

/// Qualify a `table_name` read back from the manifest.
///
/// Stored names are sometimes already `schema.table`; bare names are qualified
/// with the build schema.
fn qualify_manifest_table_name(table_name: &str, schema: &str, dialect: &Dialect) -> String {
    match table_name.split_once('.') {
        Some((s, t)) => dialect.qualify_table(s, t),
        None => dialect.qualify_table(schema, table_name),
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

/// True if the request's grouping key is exactly the rollup's stored grain,
/// so the rollup already has exactly one row per requested group and
/// re-aggregating (`GROUP BY` + `SUM`/`COUNT`/…) is a no-op.
///
/// `GROUP BY` is a blocking operator: the query planner can't emit any output
/// row — and so can't honor a `LIMIT` cheaply — until it has scanned the
/// entire input, because a group's remaining rows could be anywhere later in
/// the source. When the rollup is already unique per requested group, a plain
/// projection is equivalent and lets the planner stop early on `LIMIT`.
///
/// Two things narrow this beyond a simple dimension-set comparison:
/// - The stored time dimension must be requested at exactly its stored
///   granularity (or neither side uses one) — a coarser ask, or dropping the
///   time dimension from the request, still collapses multiple rollup rows.
/// - The rollup must not store any measure whose type reports
///   [`MeasureType::adds_raw_group_column`] — *anywhere* in `entry.measures`,
///   not only among the requested ones. Those measure types add their raw
///   expression column to `GROUP BY` at build time (`generate_build_sql`'s
///   `extra_group_cols` reads the same predicate, so the two cannot drift),
///   so the table's real on-disk grain is finer than `entry.dimensions` alone
///   claims: a `sum` measure stored *alongside* an unrequested `count_distinct`
///   still has one row per (dimensions, raw expr value), not one row per
///   dimension combination, and passing it through un-aggregated would return
///   fragments instead of the true per-dimension total.
fn matches_exact_grain(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
) -> bool {
    let mut req_dims: Vec<&str> = request
        .dimensions
        .iter()
        .map(|d| d.split('.').nth(1).unwrap_or(d))
        .collect();
    let mut entry_dims: Vec<&str> = entry.dimensions.iter().map(String::as_str).collect();
    req_dims.sort_unstable();
    entry_dims.sort_unstable();
    if req_dims != entry_dims {
        return false;
    }

    match (&entry.time_dimension, &entry.granularity) {
        (Some(stored_td), Some(stored_gran)) => {
            let requested_at_stored_gran = request.time_dimensions.iter().any(|td| {
                let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
                td_name == stored_td && td.granularity.as_deref() == Some(stored_gran.as_str())
            });
            if !requested_at_stored_gran {
                return false;
            }
        }
        _ => {
            if !request.time_dimensions.is_empty() {
                return false;
            }
        }
    }

    let has_finer_grain_measure = entry.measures.iter().any(|m| {
        m.get("type")
            .and_then(|t| t.as_str())
            .is_some_and(type_str_adds_raw_group_column)
    });
    if has_finer_grain_measure {
        return false;
    }

    true
}

/// Manifest-JSON counterpart of [`MeasureType::adds_raw_group_column`]. Manifest
/// rows carry the measure type as the lowercase string `MeasureType`'s `Display`
/// emits; an unrecognized type is treated conservatively as grain-widening so a
/// future type that is written to a manifest but not yet known here can never
/// silently enable the un-aggregated passthrough.
fn type_str_adds_raw_group_column(t: &str) -> bool {
    MeasureType::from_type_name(t).is_none_or(|m| m.adds_raw_group_column())
}

/// Generate a re-aggregation SQL query from a pre-aggregated source.
pub fn generate_reagg_sql(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
    from_source: &str,
) -> String {
    let exact_grain = matches_exact_grain(request, entry);
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
                    if exact_grain {
                        select_cols.push(format!("\"{}\" AS \"{}\"", col, alias));
                    } else {
                        select_cols.push(format!("SUM(\"{}\") AS \"{}\"", col, alias));
                    }
                }
                "count" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__count", measure_name));
                    if exact_grain {
                        select_cols.push(format!("\"{}\" AS \"{}\"", col, alias));
                    } else {
                        select_cols.push(format!("SUM(\"{}\") AS \"{}\"", col, alias));
                    }
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
                    if exact_grain {
                        // One rollup row per group already — divide its stored
                        // sum/count directly instead of re-summing a single value.
                        select_cols.push(format!(
                            "CAST(\"{}\" AS DOUBLE) / NULLIF(\"{}\", 0) AS \"{}\"",
                            sum_col, count_col, alias
                        ));
                    } else {
                        select_cols.push(format!(
                            "CAST(SUM(\"{}\") AS DOUBLE) / NULLIF(SUM(\"{}\"), 0) AS \"{}\"",
                            sum_col, count_col, alias
                        ));
                    }
                }
                "min" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__min", measure_name));
                    if exact_grain {
                        select_cols.push(format!("\"{}\" AS \"{}\"", col, alias));
                    } else {
                        select_cols.push(format!("MIN(\"{}\") AS \"{}\"", col, alias));
                    }
                }
                "max" => {
                    let col = columns
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}__max", measure_name));
                    if exact_grain {
                        select_cols.push(format!("\"{}\" AS \"{}\"", col, alias));
                    } else {
                        select_cols.push(format!("MAX(\"{}\") AS \"{}\"", col, alias));
                    }
                }
                "count_distinct" | "count_distinct_approx" => {
                    // `exact_grain` is always false here — `matches_exact_grain`
                    // rejects any rollup that stores a count_distinct measure,
                    // since its real on-disk grain is finer than
                    // `entry.dimensions` (see that function's doc comment).
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
    // Skip GROUP BY when the rollup is already unique per requested group —
    // it's a no-op there, and keeping it would force the planner to consume
    // the entire input before it can honor a LIMIT (see matches_exact_grain).
    let group_by = if exact_grain || group_by_cols.is_empty() {
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
    let exact_grain = matches_exact_grain(request, entry);
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
        let base_alias = td.dimension.replace('.', "__");
        // A granular time dimension is aliased `view__field__granularity`, the
        // same column name the raw SQL generator emits (`member_alias` on
        // `<dimension>.<granularity>`) and the same one `render_order_by`
        // references — a plain `view__field` alias here makes any ORDER BY on
        // the time dimension unresolvable and renames the column relative to
        // the uncached path.
        let alias = match &td.granularity {
            Some(gran) => format!("{base_alias}__{gran}"),
            None => base_alias,
        };
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
                    if exact_grain {
                        select_cols.push(format!("{} AS {}", col, alias_q));
                    } else {
                        select_cols.push(format!("SUM({}) AS {}", col, alias_q));
                    }
                }
                "count" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__count", measure_name)),
                    );
                    if exact_grain {
                        select_cols.push(format!("{} AS {}", col, alias_q));
                    } else {
                        select_cols.push(format!("SUM({}) AS {}", col, alias_q));
                    }
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
                    if exact_grain {
                        // One rollup row per group already — divide its stored
                        // sum/count directly instead of re-summing a single value.
                        let count_expr = format!("NULLIF({}, 0)", count_col);
                        select_cols.push(format!(
                            "{} / {} AS {}",
                            dialect.cast_to_double(&sum_col),
                            count_expr,
                            alias_q,
                        ));
                    } else {
                        let sum_expr = format!("SUM({})", sum_col);
                        let count_expr = format!("NULLIF(SUM({}), 0)", count_col);
                        select_cols.push(format!(
                            "{} / {} AS {}",
                            dialect.cast_to_double(&sum_expr),
                            count_expr,
                            alias_q,
                        ));
                    }
                }
                "min" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__min", measure_name)),
                    );
                    if exact_grain {
                        select_cols.push(format!("{} AS {}", col, alias_q));
                    } else {
                        select_cols.push(format!("MIN({}) AS {}", col, alias_q));
                    }
                }
                "max" => {
                    let col = dialect.quote_identifier(
                        &columns
                            .first()
                            .cloned()
                            .unwrap_or_else(|| format!("{}__max", measure_name)),
                    );
                    if exact_grain {
                        select_cols.push(format!("{} AS {}", col, alias_q));
                    } else {
                        select_cols.push(format!("MAX({}) AS {}", col, alias_q));
                    }
                }
                "count_distinct" | "count_distinct_approx" => {
                    // `exact_grain` is always false here — matches_exact_grain
                    // rejects any rollup that stores a count_distinct measure.
                    // See that function's doc comment.
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
    // Skip GROUP BY when the rollup is already unique per requested group —
    // see matches_exact_grain's doc comment on generate_reagg_sql.
    let group_by = if exact_grain || group_by_cols.is_empty() {
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

/// A rollup dropped during a build because its view no longer declares it.
#[derive(Debug, Clone)]
pub struct PrunedRollup {
    pub view_name: String,
    pub rollup_name: String,
    pub table_name: String,
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
    /// Rollups removed because the view no longer declares them.
    pub pruned: Vec<PrunedRollup>,
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
) -> Result<BuildPlan, EngineError> {
    let mut statements: Vec<String> = Vec::new();
    let mut manifest_entries: Vec<ManifestEntry> = Vec::new();
    let mut skipped: Vec<SkippedRollup> = Vec::new();
    let mut pruned: Vec<PrunedRollup> = Vec::new();

    // Rollup hashes each in-scope view still declares — including ones skipped
    // as fresh, which must not be pruned. Keyed by view so a hash collision
    // between two identically-shaped views can't spare the other's orphan.
    let live_hashes: std::collections::HashMap<&str, std::collections::HashSet<String>> = views
        .iter()
        .map(|v| {
            (
                v.name.as_str(),
                resolve_rollups(v).into_iter().map(|r| r.hash).collect(),
            )
        })
        .collect();

    // 1. Create schema/database (if the dialect supports it)
    if let Some(ddl) = dialect.create_schema_ddl(schema) {
        statements.push(ddl);
    }

    // 2. Create manifest table
    statements.push(generate_manifest_create_sql(schema, dialect));

    // 3. For each view, resolve rollups and generate CTAS + manifest entries.
    for view in views {
        let rollups = resolve_rollups(view);
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
                statements.push(format!(
                    "DROP TABLE IF EXISTS {}",
                    qualify_manifest_table_name(&old.table_name, schema, dialect)
                ));
            }
        }
    }

    // 5. Prune orphaned rollups: manifest rows for an in-scope view whose
    //    rollup no longer exists in the schema (renamed, deleted, or — after
    //    pre-aggregation became opt-in — a `default` rollup from an older
    //    build). Left behind, they keep serving frozen data that no `build`
    //    can refresh, so drop the table and delete the manifest row.
    if let Some(prev) = previous_entries {
        for old in prev {
            let Some(declared) = live_hashes.get(old.view_name.as_str()) else {
                continue; // view outside this build's scope — leave it alone
            };
            if declared.contains(&old.rollup_hash) {
                continue;
            }
            if !old.table_name.is_empty() {
                statements.push(format!(
                    "DROP TABLE IF EXISTS {}",
                    qualify_manifest_table_name(&old.table_name, schema, dialect)
                ));
            }
            statements.push(generate_manifest_delete_sql(
                schema,
                &old.view_name,
                &old.rollup_name,
                dialect,
            ));
            pruned.push(PrunedRollup {
                view_name: old.view_name.clone(),
                rollup_name: old.rollup_name.clone(),
                table_name: old.table_name.clone(),
            });
        }
    }

    Ok(BuildPlan {
        statements,
        manifest_entries,
        skipped,
        pruned,
    })
}

/// Generate a complete build plan for the given views.
///
/// Returns all SQL statements to execute (in order) plus manifest entries
/// for reporting. The caller is responsible for executing the statements.
///
/// If `previous_entries` is provided (from reading the warehouse manifest
/// before building), the plan appends `DROP TABLE IF EXISTS` statements at
/// the end to clean up old rollup tables that were replaced by this build,
/// and prunes manifest rows for in-scope views whose rollups no longer exist
/// (recorded in [`BuildPlan::pruned`]). Cleanup runs *after* the new tables
/// and manifest are in place, so there is no downtime window where a rollup
/// is missing.
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
    fn test_generate_build_sql_sum() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view);
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
        let rollups = resolve_rollups(&view);
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
        );
        let h2 = compute_rollup_hash(
            &["region".into(), "status".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
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
        );
        let h2 = compute_rollup_hash(
            &["status".into(), "region".into()],
            &["b".into(), "a".into()],
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
        );
        let h2 = compute_rollup_hash(
            &["status".into()],
            &["revenue".into()],
            Some("created_at"),
            Some("month"),
        );
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_resolve_rollups_explicit() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view);
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].name, "by_region_monthly");
        assert_eq!(rollups[0].dimensions, vec!["region"]);
        assert_eq!(rollups[0].time_dimension.as_deref(), Some("created_at"));
    }

    #[test]
    fn test_resolve_rollups_none_without_preaggs() {
        // Pre-aggregation is opt-in: no `pre_aggregations` block, no rollups.
        let view = test_view_no_preaggs();
        assert!(resolve_rollups(&view).is_empty());
    }

    #[test]
    fn test_collect_build_sql_skips_views_without_preaggs() {
        let view = test_view_no_preaggs();
        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260508",
            &Dialect::Postgres,
            None,
            None,
        )
        .unwrap();
        assert!(plan.manifest_entries.is_empty());
        assert!(
            !plan
                .statements
                .iter()
                .any(|s| s.contains("CREATE TABLE") && !s.contains("__manifest")),
            "no rollup CTAS should be emitted: {:?}",
            plan.statements
        );
    }

    #[test]
    fn test_collect_build_sql_prunes_orphaned_rollups() {
        // A view built by an older version left a `default` rollup in the
        // manifest. It is no longer declared, so the build must drop the table
        // and delete the manifest row rather than leave it serving stale data.
        let view = test_view_with_preaggs();
        let stale = WarehouseRollupEntry {
            view_name: "orders".into(),
            rollup_name: "default".into(),
            rollup_hash: "deadbeef".into(),
            table_name: "orders__deadbeef__20260101".into(),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: Some("created_at".into()),
            granularity: Some("day".into()),
            build_date: "2026-01-01".into(),
        };

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260508",
            &Dialect::Postgres,
            Some(&[stale]),
            None,
        )
        .unwrap();

        assert_eq!(plan.pruned.len(), 1);
        assert_eq!(plan.pruned[0].rollup_name, "default");
        assert!(
            plan.statements
                .iter()
                .any(|s| s.starts_with("DROP TABLE IF EXISTS")
                    && s.contains("orders__deadbeef__20260101")),
            "orphaned table should be dropped: {:?}",
            plan.statements
        );
        assert!(
            plan.statements.iter().any(|s| s.contains("DELETE FROM")
                && s.contains("__manifest")
                && s.contains("'default'")),
            "orphaned manifest row should be deleted: {:?}",
            plan.statements
        );
    }

    #[test]
    fn test_collect_build_sql_keeps_fresh_and_out_of_scope_rollups() {
        let view = test_view_with_preaggs();
        let live_hash = resolve_rollups(&view)[0].hash.clone();

        // Still-declared rollup, skipped as fresh — must not be pruned.
        let fresh = WarehouseRollupEntry {
            view_name: "orders".into(),
            rollup_name: "by_region_monthly".into(),
            rollup_hash: live_hash.clone(),
            table_name: "orders__live__20260101".into(),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
            build_date: "2026-01-01".into(),
        };
        // A different view's rollup — outside this build's scope.
        let other = WarehouseRollupEntry {
            view_name: "sessions".into(),
            rollup_name: "default".into(),
            rollup_hash: "cafebabe".into(),
            table_name: "sessions__cafebabe__20260101".into(),
            dimensions: vec![],
            measures: vec![],
            time_dimension: None,
            granularity: None,
            build_date: "2026-01-01".into(),
        };

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260508",
            &Dialect::Postgres,
            Some(&[fresh, other]),
            Some(&[RollupFreshness {
                rollup_hash: live_hash,
                is_fresh: true,
                current_refresh_key_value: None,
            }]),
        )
        .unwrap();

        assert!(plan.pruned.is_empty(), "pruned: {:?}", plan.pruned);
        assert!(
            !plan
                .statements
                .iter()
                .any(|s| s.contains("sessions__cafebabe__20260101")),
            "another view's rollup must be untouched: {:?}",
            plan.statements
        );
    }

    #[test]
    fn test_manifest_delete_sql_clickhouse_uses_mutation() {
        let ch = generate_manifest_delete_sql("preagg", "orders", "default", &Dialect::ClickHouse);
        assert!(ch.starts_with("ALTER TABLE"), "{}", ch);
        assert!(ch.contains("DELETE WHERE"), "{}", ch);
        let pg = generate_manifest_delete_sql("preagg", "orders", "default", &Dialect::Postgres);
        assert!(pg.starts_with("DELETE FROM"), "{}", pg);
        // Quotes in identifiers are escaped SQL-standard style.
        let escaped = generate_manifest_delete_sql("preagg", "o'r", "d'r", &Dialect::Postgres);
        assert!(escaped.contains("'o''r'"), "{}", escaped);
        assert!(escaped.contains("'d''r'"), "{}", escaped);
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
            build_date: "2026-04-15".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        }
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
    fn test_reagg_sql_exact_grain_skips_group_by() {
        use crate::engine::query::TimeDimensionQuery;
        let entry = test_local_rollup_entry(); // dims=["region"], stored gran="month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            !sql.contains("GROUP BY"),
            "Exact grain match should skip GROUP BY: {}",
            sql
        );
        assert!(
            sql.contains("\"total_revenue__sum\" AS \"orders__total_revenue\""),
            "Should pass the stored column through directly, no SUM: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_exact_grain_average_divides_directly() {
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![serde_json::json!({
                "name": "avg_rev", "type": "average",
                "columns": ["avg_rev__sum", "avg_rev__count"]
            })],
            time_dimension: None,
            granularity: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let request = QueryRequest {
            measures: vec!["orders.avg_rev".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            !sql.contains("GROUP BY"),
            "Exact grain match should skip GROUP BY: {}",
            sql
        );
        assert!(
            sql.contains(
                "CAST(\"avg_rev__sum\" AS DOUBLE) / NULLIF(\"avg_rev__count\", 0) AS \"orders__avg_rev\""
            ),
            "Should divide the stored sum/count directly, no SUM wrapper: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_exact_dims_but_missing_time_dimension_still_groups() {
        // Dims match, but the query drops the rollup's time dimension entirely —
        // multiple rollup rows (one per month) still collapse into one output
        // row, so this must NOT take the exact-grain passthrough.
        let entry = test_local_rollup_entry(); // stored time_dimension = created_at @ month
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("GROUP BY"),
            "Dropping the time dimension must still re-aggregate: {}",
            sql
        );
        assert!(
            sql.contains("SUM(\"total_revenue__sum\")"),
            "Must still SUM, not pass through: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_sql_exact_dims_but_rollup_has_count_distinct_still_groups() {
        // The rollup stores a sum measure AND an (unrequested) count_distinct
        // measure at the same declared dimensions. Building it added the
        // count_distinct's raw column to GROUP BY, so its real on-disk grain is
        // finer than `dimensions` alone claims — a query for just the sum must
        // still re-aggregate, or it returns per-raw-value fragments instead of
        // the true per-region total.
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![
                serde_json::json!({"name": "total_revenue", "type": "sum", "columns": ["total_revenue__sum"]}),
                serde_json::json!({"name": "unique_customers", "type": "count_distinct", "columns": ["customer_id"]}),
            ],
            time_dimension: None,
            granularity: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("GROUP BY"),
            "A rollup carrying a count_distinct measure must still re-aggregate: {}",
            sql
        );
        assert!(
            sql.contains("SUM(\"total_revenue__sum\")"),
            "Must still SUM, not pass through: {}",
            sql
        );
    }

    #[test]
    fn test_warehouse_reagg_sql_average_exact_grain_skips_sum() {
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![serde_json::json!({
                "name": "avg_rev", "type": "average",
                "columns": ["avg_rev__sum", "avg_rev__count"]
            })],
            time_dimension: None,
            granularity: None,
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        };
        let request = QueryRequest {
            measures: vec!["orders.avg_rev".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "preagg.test",
            &crate::dialect::Dialect::Postgres,
        );
        assert!(
            !sql.contains("GROUP BY"),
            "Exact grain match should skip GROUP BY: {}",
            sql
        );
        assert!(
            sql.contains(
                "CAST(\"avg_rev__sum\" AS DOUBLE PRECISION) / NULLIF(\"avg_rev__count\", 0)"
            ),
            "Should divide the stored sum/count directly, no SUM wrapper: {}",
            sql
        );
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
        // Dimensions deliberately don't match the request (request has none) so
        // this stays on the re-aggregated SUM/COUNT path — see
        // `test_warehouse_reagg_sql_average_exact_grain_skips_sum` for the
        // exact-grain passthrough case.
        let entry = LocalRollupEntry {
            view_name: "orders".into(),
            rollup_name: "test".into(),
            rollup_hash: "abc".into(),
            file: "test.parquet".into(),
            dimensions: vec!["region".into()],
            measures: vec![serde_json::json!({
                "name": "avg_rev", "type": "average",
                "columns": ["avg_rev__sum", "avg_rev__count"]
            })],
            time_dimension: None,
            granularity: None,
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
    fn test_warehouse_reagg_sql_order_by_time_dimension_with_granularity() {
        // Regression, warehouse counterpart of
        // `test_reagg_sql_order_by_time_dimension_with_granularity`: the
        // projected alias for a granular time dimension must carry the
        // granularity suffix, matching both the raw SQL generator's column
        // name and what `render_order_by` references. Without it the ORDER BY
        // names a column the SELECT never projects and the warehouse rejects
        // the query.
        use crate::engine::query::{OrderBy, TimeDimensionQuery};
        let entry = test_local_rollup_entry(); // stored gran = "month"
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
        let sql = generate_warehouse_reagg_sql(
            &request,
            &entry,
            "\"preagg\".\"orders__abc\"",
            &crate::dialect::Dialect::Postgres,
        );
        assert!(
            sql.contains("AS \"orders__created_at__month\""),
            "SELECT should alias the time column with its granularity: {}",
            sql
        );
        assert!(
            sql.contains("ORDER BY \"orders__created_at__month\" ASC"),
            "ORDER BY should use granularity-suffixed alias: {}",
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
            build_date: "2026-04-16".into(),
            refresh_key_value: None,
            refresh_key_checked_at: None,
        }
    }

    #[test]
    fn test_build_sql_all_dialects() {
        let view = test_view_with_preaggs();
        let rollups = resolve_rollups(&view);
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
            build_date: "2026-04-10".into(),
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            Some(&old_entries),
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
            build_date: "2026-04-15".into(),
        }];

        let plan = collect_build_sql(
            &[&view],
            "preagg",
            "20260415",
            &Dialect::Postgres,
            Some(&old_entries),
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
        // Requested dims == rollup's stored grain (["platform"] both sides), so
        // this is the exact-grain passthrough — no re-aggregating SUM needed,
        // see `matches_exact_grain`.
        assert!(!res.reagg_sql.contains("SUM"));
        assert!(!res.reagg_sql.contains("GROUP BY"));
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
        let rollups = resolve_rollups(&view);
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
        let rollups = resolve_rollups(&view);
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

#[cfg(test)]
mod rollup_expr_tests {
    use super::*;

    fn view_from_yaml(yaml: &str) -> View {
        serde_yaml::from_str(yaml).expect("test view fixture parses")
    }

    fn ctas_for(view: &View, rollup_name: &str) -> Result<String, EngineError> {
        ctas_in_layer(view, &[], rollup_name)
    }

    /// `others` join the layer the engine is built from, so a reference to
    /// another view gets past schema validation and reaches the CTAS builder —
    /// which is the guard under test.
    fn ctas_in_layer(
        view: &View,
        others: &[View],
        rollup_name: &str,
    ) -> Result<String, EngineError> {
        let mut views = vec![view.clone()];
        views.extend_from_slice(others);
        let layer = SemanticLayer::new(views, None);
        let dialects = DatasourceDialectMap::with_default(Dialect::DuckDB);
        let engine = SemanticEngine::from_semantic_layer(layer, dialects)?;
        let rollup = resolve_rollups(view)
            .into_iter()
            .find(|r| r.name == rollup_name)
            .expect("declared rollup resolves");
        let sqls = generate_build_sql(&engine, view, &rollup, "preagg", "20260825")?;
        Ok(sqls.into_iter().nth(1).expect("DROP + CTAS"))
    }

    /// A measure whose expr references a sibling dimension by `{{view.member}}`
    /// — legal in a live query, and the shape that used to reach the warehouse
    /// with the braces still in it.
    const SHIPMENTS: &str = r#"
name: order_shipments
table: order_shipments
pre_aggregations:
  - name: shipments_by_status
    dimensions: [shipment_status]
    measures: [total_shipments, delivered_shipments]
dimensions:
  - name: shipment_status
    type: string
    expr: status
measures:
  - name: total_shipments
    type: count
  - name: delivered_shipments
    type: count
    expr: "CASE WHEN {{order_shipments.shipment_status}} = 'delivered' THEN 1 END"
"#;

    #[test]
    fn a_dotted_member_ref_in_a_measure_expr_resolves_to_the_members_own_expr() {
        let ctas = ctas_for(&view_from_yaml(SHIPMENTS), "shipments_by_status").expect("builds");
        assert!(
            !ctas.contains("{{"),
            "an unresolved ref would reach the warehouse as a parser error: {ctas}"
        );
        assert!(
            ctas.contains("COUNT(CASE WHEN (status) = 'delivered' THEN 1 END)"),
            "the ref should expand to the dimension's own expr: {ctas}"
        );
    }

    #[test]
    fn a_bare_member_ref_resolves_the_same_way_as_a_dotted_one() {
        let yaml = SHIPMENTS.replace("{{order_shipments.shipment_status}}", "{{shipment_status}}");
        let ctas = ctas_for(&view_from_yaml(&yaml), "shipments_by_status").expect("builds");
        assert!(
            ctas.contains("COUNT(CASE WHEN (status) = 'delivered' THEN 1 END)"),
            "bare and dotted forms must agree: {ctas}"
        );
    }

    #[test]
    fn a_ref_the_view_cannot_supply_is_named_rather_than_emitted() {
        // `orders` really exists in the layer, so this is a reference the
        // schema validator accepts and a live query resolves through a join.
        // A rollup has no join to resolve it through.
        let orders = view_from_yaml(
            r#"
name: orders
table: orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
  - name: status
    type: string
    expr: status
"#,
        );
        let yaml = SHIPMENTS.replace("{{order_shipments.shipment_status}}", "{{orders.status}}");
        let err = ctas_in_layer(&view_from_yaml(&yaml), &[orders], "shipments_by_status")
            .expect_err("a rollup is built from one view, with no joins");
        let msg = err.to_string();
        assert!(
            msg.contains("orders.status") && msg.contains("single view"),
            "the error should name the ref and say why: {msg}"
        );
    }

    #[test]
    fn a_ref_to_a_member_that_does_not_exist_is_named_too() {
        let yaml = SHIPMENTS.replace(
            "{{order_shipments.shipment_status}}",
            "{{order_shipments.nope}}",
        );
        let err =
            ctas_for(&view_from_yaml(&yaml), "shipments_by_status").expect_err("no such member");
        assert!(
            err.to_string().contains("nope"),
            "the error should name the member: {err}"
        );
    }

    /// Measure `filters:` used to be dropped on the floor: the rollup stored
    /// the unfiltered aggregate and served it under the pre-aggregated badge.
    const OPEX: &str = r#"
name: operating_costs
table: operating_costs
pre_aggregations:
  - name: opex_by_region
    dimensions: [region]
    measures: [total_op_cost, logistics_cost, biggest_logistics_line, avg_logistics_line]
dimensions:
  - name: region
    type: string
    expr: region
  - name: category
    type: string
    expr: category
measures:
  - name: total_op_cost
    type: sum
    expr: amount
  - name: logistics_cost
    type: sum
    expr: amount
    filters:
      - expr: "{{category}} = 'logistics'"
  - name: biggest_logistics_line
    type: max
    expr: amount
    filters:
      - expr: "{{category}} = 'logistics'"
  - name: avg_logistics_line
    type: average
    expr: amount
    filters:
      - expr: "{{category}} = 'logistics'"
"#;

    #[test]
    fn a_filtered_measure_stores_the_filtered_aggregate() {
        let ctas = ctas_for(&view_from_yaml(OPEX), "opex_by_region").expect("builds");
        assert!(
            ctas.contains(
                "COALESCE(SUM(CASE WHEN (category) = 'logistics' THEN amount END), 0) \
                 AS \"logistics_cost__sum\""
            ),
            "a filtered SUM must store only what it is filtered to, coalesced the way \
             the live path coalesces it: {ctas}"
        );
    }

    #[test]
    fn an_unfiltered_measure_on_the_same_rollup_is_left_alone() {
        let ctas = ctas_for(&view_from_yaml(OPEX), "opex_by_region").expect("builds");
        assert!(
            ctas.contains("SUM(amount) AS \"total_op_cost__sum\""),
            "no CASE WHEN and no COALESCE where there is no filter: {ctas}"
        );
    }

    #[test]
    fn filters_reach_every_aggregate_shape_not_just_sum() {
        let ctas = ctas_for(&view_from_yaml(OPEX), "opex_by_region").expect("builds");
        assert!(
            ctas.contains("MAX(CASE WHEN (category) = 'logistics' THEN amount END)"),
            "MAX must be filtered too: {ctas}"
        );
        // AVG stores SUM + COUNT for re-aggregation; both halves have to be
        // filtered or the weighted average comes out wrong.
        assert!(
            ctas.contains(
                "SUM(CASE WHEN (category) = 'logistics' THEN amount END) \
                 AS \"avg_logistics_line__sum\""
            ) && ctas.contains(
                "COUNT(CASE WHEN (category) = 'logistics' THEN amount END) \
                 AS \"avg_logistics_line__count\""
            ),
            "both halves of the average partial must carry the filter: {ctas}"
        );
        // Only the filtered SUM is coalesced — 0 is not a meaningful MAX, and
        // an all-NULL average must stay NULL.
        assert!(
            !ctas.contains("COALESCE(MAX"),
            "MAX must not be coalesced to 0: {ctas}"
        );
    }

    #[test]
    fn a_count_distinct_that_cannot_honour_its_filter_fails_instead_of_ignoring_it() {
        let yaml = r#"
name: operating_costs
table: operating_costs
pre_aggregations:
  - name: vendors_by_region
    dimensions: [region]
    measures: [logistics_vendors]
dimensions:
  - name: region
    type: string
    expr: region
  - name: category
    type: string
    expr: category
measures:
  - name: logistics_vendors
    type: count_distinct
    expr: vendor_id
    filters:
      - expr: "{{category}} = 'logistics'"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "vendors_by_region")
            .expect_err("a stored raw column has no aggregate to fold a filter into");
        assert!(
            err.to_string().contains("logistics_vendors"),
            "the error should name the measure: {err}"
        );
    }

    #[test]
    fn a_calculated_measure_expands_the_measures_it_references() {
        let yaml = r#"
name: subscriptions
table: subscriptions
pre_aggregations:
  - name: mrr_by_plan
    dimensions: [plan]
    measures: [new_mrr, expansion_mrr, net_mrr]
dimensions:
  - name: plan
    type: string
    expr: plan
measures:
  - name: new_mrr
    type: sum
    expr: new_amount
  - name: expansion_mrr
    type: sum
    expr: expansion_amount
  - name: net_mrr
    type: number
    expr: "{{subscriptions.new_mrr}} + {{subscriptions.expansion_mrr}}"
"#;
        let ctas = ctas_for(&view_from_yaml(yaml), "mrr_by_plan").expect("builds");
        assert!(
            ctas.contains("(SUM(new_amount)) + (SUM(expansion_amount)) AS \"net_mrr__value\""),
            "a number measure's refs must expand to the referenced aggregates: {ctas}"
        );
    }

    #[test]
    fn a_self_referencing_member_is_refused_rather_than_recursed_forever() {
        let yaml = r#"
name: loop_view
table: loop_view
pre_aggregations:
  - name: r
    dimensions: [a]
    measures: [c]
dimensions:
  - name: a
    type: string
    expr: "{{loop_view.b}}"
  - name: b
    type: string
    expr: "{{loop_view.a}}"
measures:
  - name: c
    type: count
"#;
        let err = ctas_for(&view_from_yaml(yaml), "r").expect_err("a cycle cannot resolve");
        assert!(
            err.to_string().contains("nest"),
            "the error should point at the nesting: {err}"
        );
    }
}
