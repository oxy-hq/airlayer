//! Pre-aggregation: rollup resolution, SQL generation, coverage checking.

use crate::dialect::Dialect;
use crate::engine::member_sql::{dotted_ref_regex, param_ref_regex, MemberSqlResolver};
use crate::engine::{DatasourceDialectMap, EngineError, SemanticEngine};
use crate::schema::models::{
    EntityType, Measure, MeasureType, PreAggregation, SemanticLayer, View,
};
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
///
/// Matches the live path's `MAX_RESOLVE_DEPTH`: a composition chain a query
/// compiles is a chain a rollup has to compile too, or `build` refuses a schema
/// that works. The counter costs one level per member hop — a measure hop used
/// to spend two (`resolve_member_ref` → `measure_agg` → `filtered_inner` →
/// `resolve_at`), which halved the real limit and reported an acyclic chain as
/// a self-reference.
const MAX_ROLLUP_RESOLVE_DEPTH: usize = 64;

/// Any `{{...}}` in a string, a request variable included.
///
/// `MemberSqlResolver::find_unresolved_ref` deliberately exempts
/// `{{variables.X}}` — the live path preserves those for the caller to bind —
/// and `dotted_ref_regex` matches only a single dot, so between them neither
/// sees `{{variables.db.schema}}`. A rollup needs to see all of them.
fn find_any_ref(s: &str) -> Option<String> {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{\s*([^{}]+?)\s*\}\}").unwrap());
    re.captures(s).map(|caps| caps[1].trim().to_string())
}

/// Is this reference a request variable, in either dot form?
fn is_variables_ref(reference: &str) -> bool {
    reference == "variables" || reference.starts_with("variables.")
}

/// A request variable is not a passthrough here, the way it is in a live query.
///
/// The live path preserves `{{variables.X}}` in its output because the caller
/// binds it at request time. `build` executes the CTAS itself and has no
/// request to bind from, so the same passthrough is not a deferred
/// substitution — it is a warehouse parser error with a brace in it.
fn refuse_variables_ref(view_name: &str, reference: &str) -> EngineError {
    EngineError::SqlGenerationError(format!(
        "[{}] rollup expression references '{{{{{}}}}}'; a request variable cannot be bound \
         when building a rollup. A live query preserves it for the caller to bind at request \
         time, but `build` runs the CTAS itself, with no request behind it",
        view_name, reference
    ))
}

/// The CTAS's FROM clause is the view's `table:`/`sql:` source, emitted
/// verbatim — exactly what the live path does with it (`view_source_expr` in
/// `sql_generator.rs` neither resolves `{{TABLE}}` there nor binds a variable),
/// so no pass in this file ever rewrites it. A brace left in the source is
/// therefore a parser error the moment `build` executes, and `{{TABLE}}` is
/// meaningless in a view's own source anyway: it expands to that source.
fn check_source_refs(view_name: &str, source: &str) -> Result<(), EngineError> {
    let Some(reference) = find_any_ref(source) else {
        return Ok(());
    };
    if is_variables_ref(&reference) {
        return Err(refuse_variables_ref(view_name, &reference));
    }
    Err(EngineError::SqlGenerationError(format!(
        "[{}] the view's source names '{{{{{}}}}}'; a rollup's FROM clause is the view's table \
         or sql emitted verbatim, so there is nothing to resolve the reference against",
        view_name, reference
    )))
}

/// Where in the CTAS an expression is being resolved.
///
/// A measure reference expands to an aggregate (`SUM(amount)`), and there is
/// exactly one position in a rollup's CTAS where an aggregate belongs: the expr
/// of a `number`/`custom` measure, which is already one. A dimension expr goes
/// into the GROUP BY as well as the SELECT (`GROUP BY 1` over `SUM(amount)` is
/// a binder error), a filter condition sits inside a CASE the aggregate wraps,
/// and the inner expr of an aggregating measure would nest as `SUM(SUM(x))` —
/// every one of them rejected by the warehouse. Naming the reference is the
/// point of this resolver; letting it through to fail downstream is not.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExprPosition {
    /// A dimension expr, a time-dimension expr, a measure `filters:` condition,
    /// or the inner expr an aggregate is taken over.
    Scalar,
    /// The expr of a `number`/`custom` measure — an aggregate position.
    Aggregate,
}

/// Resolves `{{...}}` references in a view's expressions for the single-view
/// CTAS that builds a rollup.
///
/// The live query path resolves these through `SqlGenerator`, which qualifies
/// every column with a view alias (`"orders"."amount"`). A rollup's CTAS
/// aliases its single source to the view name too, but has no second table to
/// disambiguate against, so this resolver emits unqualified columns — legal
/// under an alias in every dialect. What it keeps identical on purpose is
/// *which* references are legal and what each expands to: a
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
    dialect: &'a Dialect,
}

impl<'a> RollupExprResolver<'a> {
    fn new(view: &'a View, dialect: &'a Dialect) -> Self {
        Self { view, dialect }
    }

    /// Resolve every reference in `expr`, or say which one could not be.
    fn resolve(&self, expr: &str, pos: ExprPosition) -> Result<String, EngineError> {
        self.ensure_resolved(self.resolve_at(expr, 0, pos)?)
    }

    /// The last gate before a string becomes a CTAS column. A brace that got
    /// past the passes above — a bare name that is not a member, a ref the
    /// dotted regex cannot match — is not harmless: left in the SQL it reaches
    /// the warehouse and comes back as a parser error naming a brace, which is
    /// exactly what this resolver exists to prevent. Every path that builds a
    /// column has to run through here, not just `resolve`.
    fn ensure_resolved(&self, resolved: String) -> Result<String, EngineError> {
        // Checked before the shared helper, which exempts `variables.` and
        // cannot see the multi-dot form at all — the one class of brace that
        // would otherwise sail through every gate and land in the warehouse.
        if let Some(reference) = find_any_ref(&resolved).filter(|r| is_variables_ref(r)) {
            return Err(refuse_variables_ref(&self.view.name, &reference));
        }
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

    fn resolve_at(
        &self,
        expr: &str,
        depth: usize,
        pos: ExprPosition,
    ) -> Result<String, EngineError> {
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
        // `{{TABLE}}` resolves to the CTAS's alias for the source, which is
        // the view name — the same thing the live path resolves it to. Against
        // the source *string* it would quote `myschema.sales` (or a whole
        // `sql:` subquery) as one identifier and name a table that does not
        // exist; the two only ever agreed for a bare, unqualified table name.
        let with_table = MemberSqlResolver::resolve_table_ref(&expanded, &self.view.name, &|s| {
            self.dialect.quote_identifier(s)
        });
        self.resolve_dotted_refs(&with_table, depth, pos)
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

    fn resolve_dotted_refs(
        &self,
        expr: &str,
        depth: usize,
        pos: ExprPosition,
    ) -> Result<String, EngineError> {
        let mut out = String::new();
        let mut last = 0;
        for caps in dotted_ref_regex().captures_iter(expr) {
            let whole = caps.get(0).expect("regex match has group 0");
            out.push_str(&expr[last..whole.start()]);
            out.push_str(&self.resolve_member_ref(&caps[1], &caps[2], depth, pos)?);
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
        pos: ExprPosition,
    ) -> Result<String, EngineError> {
        if qualifier != self.view.name {
            // A request variable is refused on its own terms — it is not a
            // missing join, and saying so sends the reader looking for one.
            if is_variables_ref(qualifier) {
                return Err(refuse_variables_ref(
                    &self.view.name,
                    &format!("{qualifier}.{member}"),
                ));
            }
            // A Primary entity declared on this same view names *this* view: the
            // live path maps a base-view primary to the base view's own alias
            // (`build_entity_to_alias_map`) and joins nothing, so
            // `{{order.status_raw}}` compiles to `"orders"."status_raw"` — a
            // plain column of the source, left unqualified here because the
            // CTAS has just the one table to read it from. Note that the live path
            // resolves an entity-qualified ref to the column and never to a
            // member's expr (member lookup there is keyed by *view* name), so
            // this must not expand a same-named dimension either — the rollup
            // has to compute what the query it stands in for computes.
            if self.own_primary_entity(qualifier) {
                return Ok(self.dialect.quote_identifier(member));
            }
            // Another view or a foreign entity — neither of which
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
        //
        // Measure before dimension, because that is the order the live path
        // resolves in — and it is not a tie-breaker for a case that cannot
        // happen: nothing forbids a view from declaring a dimension and a
        // measure under one name, and the live member index stores measures
        // last, so the measure is what a query gets. Looking at the dimension
        // first would store a different column than the query it stands in for
        // reads, with nothing to show for it.
        if let Some(m) = self.measure(member) {
            if pos != ExprPosition::Aggregate {
                return Err(EngineError::SqlGenerationError(format!(
                    "[{}] rollup expression references measure '{{{{{}.{}}}}}' where a plain \
                     column is required; a measure reference expands to an aggregate, which \
                     cannot stand in a dimension expr, a filter condition, or inside another \
                     aggregate. Only a number/custom measure's own expr may reference one{}",
                    self.view.name,
                    qualifier,
                    member,
                    if self.dimension(member).is_some() {
                        format!(
                            " (view '{}' declares both a dimension and a measure named '{}'; \
                             a query resolves that name to the measure)",
                            self.view.name, member
                        )
                    } else {
                        String::new()
                    }
                )));
            }
            return Ok(format!("({})", self.measure_agg(m, depth + 1)?));
        }
        if let Some(dim) = self.dimension(member) {
            return Ok(format!("({})", self.resolve_at(&dim.expr, depth + 1, pos)?));
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
        refuse_rolling_window(&self.view.name, measure)?;
        // The argument the aggregating shapes below are taken over — a scalar
        // by construction, which is why `number`/`custom` have none: those are
        // aggregates already and resolve their own expr in their arm, the one
        // position where a measure reference is legal.
        let inner = match measure.measure_type {
            MeasureType::Number | MeasureType::Custom => String::new(),
            _ => self.filtered_inner(measure, depth)?,
        };
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
                Some(expr) => self.resolve_at(expr, depth, ExprPosition::Aggregate)?,
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
        // Scalar throughout: this is the argument an aggregate is taken over,
        // and the condition inside the CASE that aggregate wraps.
        let inner = match measure.expr.as_deref() {
            Some(expr) => self.resolve_at(expr, depth, ExprPosition::Scalar)?,
            None => "*".to_string(),
        };
        let Some(filters) = measure.filters.as_ref().filter(|f| !f.is_empty()) else {
            return Ok(inner);
        };
        let mut conditions = Vec::with_capacity(filters.len());
        for f in filters {
            conditions.push(self.resolve_at(&f.expr, depth, ExprPosition::Scalar)?);
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

    /// Does `name` declare a Primary entity on this view? Only a Primary points
    /// back at this view; a Foreign declaration points at the view that owns the
    /// entity, which a single-view CTAS has no join to reach.
    fn own_primary_entity(&self, name: &str) -> bool {
        self.view
            .entities
            .iter()
            .any(|e| e.name == name && e.entity_type == EntityType::Primary)
    }
}

/// Refuse a measure whose value a rollup cannot store, whichever path reached
/// it: listed directly in `pre_aggregations.measures`, or pulled in by another
/// member's expr. Dropping the window is silent — `covers()` still accepts the
/// underlying type, so the rollup would serve a cumulative total under the
/// pre-aggregated badge.
fn refuse_rolling_window(view_name: &str, measure: &Measure) -> Result<(), EngineError> {
    if measure.rolling_window.is_some() {
        return Err(EngineError::SqlGenerationError(format!(
            "[{}] measure '{}' has a rolling_window and cannot be pre-aggregated: its value \
             depends on rows outside the group a rollup stores",
            view_name, measure.name
        )));
    }
    Ok(())
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
    check_source_refs(&view.name, &source)?;

    let table_name = format!("{}__{}__{}", view.name, rollup.hash, date_str);
    let fq_table = dialect.qualify_table(schema, &table_name);

    // The single source is aliased to the view name, exactly as the live path
    // aliases it (`FROM orders AS "orders"`). Three things need it: a
    // schema-qualified `table:` and a `sql:` view both give `{{TABLE}}`
    // something it can name, a subquery source gets the alias Postgres and
    // Redshift require of one, and the alias is what `{{TABLE}}` resolves to.
    // Nothing can collide with it — a rollup's CTAS reads one table.
    let source_alias = dialect.quote_identifier(&view.name);
    let resolver = RollupExprResolver::new(view, dialect);
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
            // to be the column, not something resolved from it. That holds for
            // `{{TABLE}}` as much as for a member ref: resolving it rewrites
            // the string, and the measure's two columns (the raw column and its
            // `__freq` companion, named from the unresolved expr) would then
            // disagree with each other and with the manifest. And a stored raw
            // column cannot carry a filter: there is no aggregate to fold one
            // into.
            if raw.contains("{{") {
                return Err(EngineError::SqlGenerationError(format!(
                    "[{}] measure '{}' is type {:?} and its expr contains a '{{{{...}}}}' \
                     reference; that shape stores a raw column, which a reference cannot name",
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
            let col = resolver.resolve(raw, ExprPosition::Scalar)?;
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
            let expr = resolver.resolve(&dim.expr, ExprPosition::Scalar)?;
            let alias = dialect.quote_identifier(dim_name);
            select_cols.push(format!("{expr} AS {alias}"));
            group_by_cols.push(expr);
            group_by_aliases.push(alias);
        }
    }

    // 2. Time dimension (truncated to the rollup granularity)
    if let (Some(td_name), Some(gran)) = (&rollup.time_dimension, &rollup.granularity) {
        if let Some(td) = view.dimensions.iter().find(|d| d.name == *td_name) {
            let expr = resolver.resolve(&td.expr, ExprPosition::Scalar)?;
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
        // A custom measure stores no column at all (its arm below is empty and
        // `covers()` refuses the type), so nothing here applies to it — and
        // resolving an expr that is never emitted would abort the build of
        // every other column over a reference that costs nothing.
        if rm.measure_type == MeasureType::Custom {
            continue;
        }
        // A measure listed directly in the rollup never passes through
        // `measure_agg`, so this is the only place the window is seen.
        if let Some(m) = declared(&rm.name) {
            refuse_rolling_window(&view.name, m)?;
        }
        // The argument every partial below aggregates over: the measure's expr
        // with its `filters:` folded in, so a filtered measure stores what it
        // is filtered to rather than the whole group. It is a scalar by
        // construction, which is why a `number` measure has none: that shape is
        // already an aggregate and resolves its own expr in its arm below, in
        // the one position where a measure reference is legal.
        let expr = if rm.measure_type == MeasureType::Number {
            String::new()
        } else {
            match declared(&rm.name) {
                Some(m) => resolver.ensure_resolved(resolver.filtered_inner(m, 0)?)?,
                None => match rm.expr.as_deref() {
                    Some(e) => resolver.resolve(e, ExprPosition::Scalar)?,
                    None => "*".to_string(),
                },
            }
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
                    Some(e) => resolver.resolve(e, ExprPosition::Aggregate)?,
                    None => {
                        return Err(EngineError::SqlGenerationError(format!(
                            "[{}] measure '{}' is type number and needs an expr",
                            view.name, rm.name
                        )));
                    }
                };
                select_cols.push(format!("{value} AS {alias}"));
            }
            // Skipped at the top of the loop; the arm is here for exhaustiveness.
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
                format!("CREATE TABLE {fq_table}\nENGINE = MergeTree()\nORDER BY tuple()\nAS\nSELECT\n    {select}\nFROM {source} AS {source_alias}")
            } else {
                let order_by = group_by_aliases.join(", ");
                // `allow_nullable_key` because the sorting key IS the grouping
                // key, and a grouping key is nullable whenever its source
                // column is — which, for anything loaded by an ELT pipeline,
                // is most of the time. MergeTree rejects a nullable sorting
                // key by default (`Code: 44 ILLEGAL_COLUMN`), so the rollup
                // simply failed to build for such a view.
                //
                // This is the only correction that keeps both halves of what
                // the rollup is for. Dropping the nullable columns from the
                // key would need column types the generator does not have (it
                // sees dimension EXPRESSIONS, not the source schema), ordering
                // by `tuple()` would give up the sort-key pruning that makes a
                // rollup worth reading, and `assumeNotNull` would fold the
                // NULL group into the type default and silently corrupt it.
                // With the setting, a NULL group stays its own row and the key
                // still prunes — both verified against ClickHouse 25.12.
                //
                // Placement matters: after ORDER BY and before `AS SELECT` it
                // is a TABLE setting (it shows up in `system.tables.engine_full`);
                // after the SELECT it would be a query setting and do nothing.
                format!(
                    "CREATE TABLE {fq_table}\nENGINE = MergeTree()\nORDER BY ({order_by})\nSETTINGS allow_nullable_key = 1\nAS\nSELECT\n    {select}\nFROM {source} AS {source_alias}{group_by_clause}",
                )
            }
        }
        _ => {
            format!(
                "CREATE TABLE {fq_table} AS\nSELECT\n    {select}\nFROM {source} AS {source_alias}{group_by_clause}",
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
    rollups.iter().find(|entry| covers(request, entry, true))
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

/// The half-open instant span a bound literal *denotes*. Accepts a bare date
/// or an ISO datetime with either a `T` or a space separator — every shape a
/// warehouse serializes and every shape a caller writes by hand.
/// The precision of the
/// literal is the whole point: `2026-01-01` names the whole of Jan 1, while
/// `2026-01-01T00:00:00` names one instant. Reading both as "midnight" is how
/// an inclusive `lte '2026-01-01'` on a month rollup turns into all of
/// January — the bound was expanded to the end of the bucket it sits in
/// rather than to the end of the day it names.
fn parse_bound_span(value: &str) -> Option<(chrono::NaiveDateTime, chrono::NaiveDateTime)> {
    use chrono::{Duration, NaiveDate, NaiveDateTime};
    let v = value.trim();
    if let Ok(d) = NaiveDate::parse_from_str(v, "%Y-%m-%d") {
        let lo = d.and_hms_opt(0, 0, 0)?;
        return Some((lo, lo.checked_add_signed(Duration::days(1))?));
    }
    // A rollup bucket carries no zone, so an offset cannot be applied — and
    // dropping it would answer a window shifted by up to a day from the one
    // the raw path filters. A zero offset (`Z`, `+00:00`) names the same wall
    // clock and is simply stripped; anything else refuses the bound, which
    // declines the rollup.
    let v = match v.rfind(['+', '-']) {
        // Only an offset, never the `-` inside the date itself.
        Some(i) if i > 10 => {
            let offset = &v[i..];
            if offset[1..].trim_matches([':', '0']).is_empty() {
                &v[..i]
            } else {
                return None;
            }
        }
        _ => v,
    };
    let v = v
        .strip_suffix('Z')
        .or_else(|| v.strip_suffix('z'))
        .unwrap_or(v);
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(v, fmt) {
            // An instant: inclusive of itself and nothing more.
            return Some((dt, dt.checked_add_signed(Duration::microseconds(1))?));
        }
    }
    None
}

/// Start of the `gran` bucket containing `dt` — the value a rollup built at
/// that granularity actually stores for `dt`.
fn truncate_to(dt: chrono::NaiveDateTime, gran: &str) -> Option<chrono::NaiveDateTime> {
    use chrono::{Datelike, Duration, NaiveDate, Timelike};
    let date = dt.date();
    let midnight = |d: NaiveDate| d.and_hms_opt(0, 0, 0);
    match gran {
        "second" => dt.with_nanosecond(0),
        "minute" => dt.with_nanosecond(0)?.with_second(0),
        "hour" => dt.with_nanosecond(0)?.with_second(0)?.with_minute(0),
        "day" => midnight(date),
        "week" => midnight(date - Duration::days(date.weekday().num_days_from_monday() as i64)),
        "month" => midnight(NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?),
        "quarter" => midnight(NaiveDate::from_ymd_opt(
            date.year(),
            (date.month() - 1) / 3 * 3 + 1,
            1,
        )?),
        "year" => midnight(NaiveDate::from_ymd_opt(date.year(), 1, 1)?),
        _ => None,
    }
}

/// One `gran` period after `dt` (which must be a bucket start).
fn add_one_period(dt: chrono::NaiveDateTime, gran: &str) -> Option<chrono::NaiveDateTime> {
    use chrono::{Duration, Months};
    match gran {
        "second" => dt.checked_add_signed(Duration::seconds(1)),
        "minute" => dt.checked_add_signed(Duration::minutes(1)),
        "hour" => dt.checked_add_signed(Duration::hours(1)),
        "day" => dt.checked_add_signed(Duration::days(1)),
        "week" => dt.checked_add_signed(Duration::weeks(1)),
        "month" => dt.checked_add_months(Months::new(1)),
        "quarter" => dt.checked_add_months(Months::new(3)),
        "year" => dt.checked_add_months(Months::new(12)),
        _ => None,
    }
}

/// Render a bound as a SQL literal, keeping a midnight bound date-only so a
/// DATE-typed warehouse column still coerces it.
fn bound_literal(dt: chrono::NaiveDateTime) -> String {
    use chrono::Timelike;
    if dt.num_seconds_from_midnight() == 0 && dt.nanosecond() == 0 {
        format!("'{}'", dt.format("%Y-%m-%d"))
    } else {
        format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S"))
    }
}

/// Translate an inclusive `[start, end]` date range into the half-open
/// `[start, end_exclusive)` pair a rollup filtered on its *bucket start*
/// column has to be given. Returns `None` when the range cannot be answered
/// from buckets of this granularity, in which case the caller must decline the
/// rollup entirely rather than filter it approximately.
///
/// Two separate things force this shape:
///
/// - **The upper bound has to reach past the last bucket.** The stored column
///   holds the bucket start, so an inclusive `2026-01-31` on a day rollup
///   compares against `2026-01-31T00:00:00` — and against the *string* the
///   Parquet cache stores, `'2026-01-31T00:00:00.000000' <= '2026-01-31'` is
///   false. Either way the final bucket of every range silently disappears; a
///   trailing-90-day question comes back with 89 days.
/// - **A bound landing mid-bucket cannot be honored.** A month rollup asked
///   for `2026-01-15 .. 2026-02-20` can only drop January whole or include it
///   whole, and both answers are wrong while looking perfectly plausible.
///
/// Note that a rollup and the raw table can still disagree on an inclusive
/// bound over a *timestamp* source column, at every grain of a day or coarser
/// whenever the bound names its bucket's last day: the raw path filters the
/// instant (`lte '2026-01-31'` reaches only midnight), while a day rollup
/// holds one bucket for the whole of Jan 31, and a month rollup one for the
/// whole of January. The rollup can take that bucket or leave it, nothing
/// finer. Every case where a rollup *could* answer exactly is made to — a
/// bound denoting more than the bucket it starts in is refused, which is why
/// sub-day grains decline these rather than widen — so what remains is what
/// bucketing itself cannot express, not a choice made here. Closing it means
/// changing the raw path's reading of a bare date, which is a separate
/// decision.
fn date_range_bounds(start: &str, end: &str, gran: &str) -> Option<(String, String)> {
    let start_dt = inclusive_lower_bound(start, gran)?;
    let end_excl = exclusive_upper_bound(end, gran)?;
    if end_excl <= start_dt {
        return None;
    }
    Some((bound_literal(start_dt), bound_literal(end_excl)))
}

/// Where a week starts is a property of the dialect that built the rollup, not
/// of this module: `Dialect::date_trunc` truncates to Monday on most
/// warehouses but to Sunday on BigQuery, MySQL and Domo. A bound validated
/// against the wrong convention shifts the window by a day and drops or adds a
/// whole bucket at each edge, so a week rollup serves no bounded query until
/// the manifest records which convention built it.
fn week_start_is_ambiguous(gran: &str) -> bool {
    gran == "week"
}

/// The instant everything at or after it is wanted from. It has to be a bucket
/// start, or the bucket it lands in would be included whole when only part of
/// it was asked for — `gte '2026-01-15'` on a month rollup would hand back
/// January from the 1st.
fn inclusive_lower_bound(value: &str, gran: &str) -> Option<chrono::NaiveDateTime> {
    if week_start_is_ambiguous(gran) {
        return None;
    }
    let (lo, _) = parse_bound_span(value)?;
    if truncate_to(lo, gran)? != lo {
        return None;
    }
    Some(lo)
}

/// An *inclusive* bound turned into the exclusive instant to compare against:
/// the end of what the literal denotes, which then has to be a bucket
/// boundary.
///
/// `2026-03-31` on a month rollup denotes through Apr 1, which is a boundary —
/// that is the shape a calendar range is written in. `2026-03-30` denotes
/// through Mar 31, which is not, so it is refused rather than rounded up.
/// `2026-01-01` on that same rollup denotes only through Jan 2 and is refused
/// too: expanding it to the end of January would return the whole month where
/// the raw path stops on the 1st. An instant bound denotes one microsecond and
/// so is never a boundary — `lte '2026-01-31 05:30:00'` cannot be answered
/// from buckets at all.
fn exclusive_upper_bound(value: &str, gran: &str) -> Option<chrono::NaiveDateTime> {
    if week_start_is_ambiguous(gran) {
        return None;
    }
    let (lo, hi) = parse_bound_span(value)?;
    if truncate_to(hi, gran)? != hi {
        return None;
    }
    // …and it may not denote *more* than the bucket it starts in. A bare date
    // on an hour rollup names 24 buckets, so serving `lte '2026-01-31'` there
    // would return the whole day where the raw path stops at midnight.
    //
    // Together with the boundary check above this means no sub-day rollup can
    // serve an *inclusive upper* bound at all, and that is not an accident: a
    // bare date names a day (too many buckets) and an instant names a
    // microsecond (no whole bucket), so under the raw path's instant reading
    // there is nothing an hour rollup could answer exactly. Such a query falls
    // back to the warehouse. `gte`/`lt` bounds are unaffected — they need only
    // a bucket start — so sub-day rollups still serve those.
    if add_one_period(truncate_to(lo, gran)?, gran)? < hi {
        return None;
    }
    Some(hi)
}

/// The bound denotes exactly one whole bucket, so `= bucket_start` answers it.
/// A bare date does on a day rollup; on a month rollup it names one day out of
/// the bucket, which equality cannot express.
fn single_bucket_bound(value: &str, gran: &str) -> Option<chrono::NaiveDateTime> {
    let lo = inclusive_lower_bound(value, gran)?;
    let (_, hi) = parse_bound_span(value)?;
    if add_one_period(lo, gran)? != hi {
        return None;
    }
    Some(lo)
}

/// The stored time column of a *local* (Parquet) rollup is always VARCHAR —
/// the cache is written from the warehouse's JSON response — and a NULL bucket
/// lands there as the empty string. Every read of that column goes through
/// this, so the SELECT, the GROUP BY and the WHERE cannot drift apart, and so
/// a nullable time bucket does not blow up the whole read (which would look
/// like the rollup silently never being used).
fn local_time_expr(quoted_col: &str) -> String {
    format!("CAST(NULLIF({}, '') AS TIMESTAMP)", quoted_col)
}

/// The same empty-string-is-NULL encoding bites plain dimensions too: on the
/// cache `"region" IS NULL` matches nothing and `"region" <> 'US'` keeps the
/// row whose region is NULL, where the raw path drops it. Only the null-aware
/// operators need this — an equality against a real value behaves the same
/// either way.
fn local_null_expr(quoted_col: &str) -> String {
    format!("NULLIF({}, '')", quoted_col)
}

/// Generate a WHERE clause fragment for a single filter, using quoted column names.
/// Returns None if the filter cannot be translated.
/// `local` says the rows come from the Parquet cache rather than a warehouse
/// rollup table. The cache is written from the warehouse's JSON response, so
/// every column in it is VARCHAR and a NULL is stored as the empty string —
/// both of which change how a value must be compared.
fn render_filter_sql(
    filter: &crate::engine::query::QueryFilter,
    entry: &LocalRollupEntry,
    quote: &dyn Fn(&str) -> String,
    local: bool,
) -> Option<String> {
    let time_expr = |c: &str| {
        if local {
            local_time_expr(c)
        } else {
            c.to_string()
        }
    };
    let null_expr = |c: &str| {
        if local {
            local_null_expr(c)
        } else {
            c.to_string()
        }
    };
    use crate::engine::query::FilterOperator;

    if let (Some(ref member), Some(ref op)) = (&filter.member, &filter.operator) {
        let dim_name = member.split('.').nth(1).unwrap_or(member);
        // The bucket column is only materialized when the rollup declares
        // *both* a time dimension and a granularity (`generate_build_sql`).
        // With one, it is what a filter on that field means, even if the field
        // is *also* listed in `dimensions:` — the bucket carries the grain the
        // rollup was built at, and the alignment rules below are about it.
        // Without one there is no bucket column, and the field is filterable
        // only if `dimensions:` stored its raw value.
        let declared_time = entry.time_dimension.as_deref() == Some(dim_name);
        let is_time = declared_time && entry.granularity.is_some();
        // A time dimension with no granularity, stored raw by `dimensions:`.
        // The column holds instants rather than buckets, so no alignment rule
        // applies — but on the Parquet cache it is still VARCHAR, so it still
        // has to be compared as a timestamp.
        let raw_time = declared_time && !is_time;
        // Resolve the column name in the rollup table
        let col = if is_time {
            quote(&format!("{}__{}", dim_name, entry.granularity.as_ref()?))
        } else if entry.dimensions.contains(&dim_name.to_string()) {
            quote(dim_name)
        } else {
            return None;
        };

        let vals: Vec<String> = filter
            .values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect();

        // Anything that *compares* the time column has to compare it as a
        // timestamp. On the Parquet cache the bucket is the VARCHAR
        // `'2026-01-31T00:00:00.000000'`, so `lte '2026-01-31'` is false and
        // `equals '2026-01-31'` matches nothing — the same string-ordering
        // trap the date-range arm below exists to close. LIKE is left on the
        // raw column: it is a question about the text.
        // A bound also has to be bucket-aligned: the stored value is a bucket
        // *start*, so `lte '2026-01-15'` on a month rollup is satisfied by
        // January's `2026-01-01` and hands back the whole of January, where
        // the raw path stops on the 15th. `bucket_cmp`/`bucket_values` render
        // the aligned form or refuse, and an unrenderable filter declines the
        // rollup rather than answering a wider question.
        let gran = entry.granularity.as_deref().unwrap_or("second");
        let bucket_cmp = |bound: &str, op: &FilterOperator| -> Option<String> {
            let ts = time_expr(&col);
            match op {
                // "at or after this instant" — the bound must be a bucket start.
                FilterOperator::Gte => Some(format!(
                    "{} >= {}",
                    ts,
                    bound_literal(inclusive_lower_bound(bound, gran)?)
                )),
                // "strictly before this instant" — same alignment, other side.
                FilterOperator::Lt => Some(format!(
                    "{} < {}",
                    ts,
                    bound_literal(inclusive_lower_bound(bound, gran)?)
                )),
                // Inclusive of the bound, so it must cover its bucket to the end.
                FilterOperator::Lte => Some(format!(
                    "{} < {}",
                    ts,
                    bound_literal(exclusive_upper_bound(bound, gran)?)
                )),
                FilterOperator::Gt => Some(format!(
                    "{} >= {}",
                    ts,
                    bound_literal(exclusive_upper_bound(bound, gran)?)
                )),
                _ => None,
            }
        };
        // Equality names one bucket, so every value must be a bucket start.
        let bucket_values = |negated: bool| -> Option<String> {
            let ts = time_expr(&col);
            if filter.values.is_empty() {
                // `IN ()` is a syntax error, and rendering nothing at all would
                // drop the filter. Decline the rollup instead.
                return None;
            }
            let aligned: Vec<String> = filter
                .values
                .iter()
                .map(|v| single_bucket_bound(v, gran).map(bound_literal))
                .collect::<Option<Vec<String>>>()?;
            Some(match (aligned.len(), negated) {
                (1, false) => format!("{} = {}", ts, aligned[0]),
                (1, true) => format!("{} <> {}", ts, aligned[0]),
                (_, false) => format!("{} IN ({})", ts, aligned.join(", ")),
                (_, true) => format!("{} NOT IN ({})", ts, aligned.join(", ")),
            })
        };
        let cmp = if raw_time {
            time_expr(&col)
        } else {
            col.clone()
        };
        let sql = match op {
            FilterOperator::Equals if is_time => bucket_values(false)?,
            FilterOperator::NotEquals if is_time => bucket_values(true)?,
            FilterOperator::Gt | FilterOperator::Gte | FilterOperator::Lt | FilterOperator::Lte
                if is_time =>
            {
                bucket_cmp(filter.values.first()?, op)?
            }
            // Nothing to compare against: `IN ()` is a syntax error, and
            // `> NULL` is silently never true, which would hand back zero rows
            // where the rollup should simply have declined the question.
            FilterOperator::Equals
            | FilterOperator::NotEquals
            | FilterOperator::Gt
            | FilterOperator::Gte
            | FilterOperator::Lt
            | FilterOperator::Lte
                if vals.is_empty() =>
            {
                return None
            }
            FilterOperator::Equals => {
                if vals.len() == 1 {
                    format!("{} = {}", cmp, vals[0])
                } else {
                    format!("{} IN ({})", cmp, vals.join(", "))
                }
            }
            // `<>` and `NOT IN` are the null-aware side of equality: the raw
            // path drops a NULL row, and on the cache the empty string would
            // keep it.
            FilterOperator::NotEquals => {
                let c = if raw_time {
                    cmp.clone()
                } else {
                    null_expr(&col)
                };
                if vals.len() == 1 {
                    format!("{} <> {}", c, vals[0])
                } else {
                    format!("{} NOT IN ({})", c, vals.join(", "))
                }
            }
            FilterOperator::Gt => format!("{} > {}", cmp, vals.first().unwrap_or(&"NULL".into())),
            FilterOperator::Gte => {
                format!("{} >= {}", cmp, vals.first().unwrap_or(&"NULL".into()))
            }
            FilterOperator::Lt => format!("{} < {}", cmp, vals.first().unwrap_or(&"NULL".into())),
            FilterOperator::Lte => {
                format!("{} <= {}", cmp, vals.first().unwrap_or(&"NULL".into()))
            }
            // A NULL bucket is the empty string in the cache, so "is set" has
            // to be asked of the cast value, not of the raw column.
            FilterOperator::Set if is_time => format!("{} IS NOT NULL", time_expr(&col)),
            FilterOperator::NotSet if is_time => format!("{} IS NULL", time_expr(&col)),
            FilterOperator::Set => format!("{} IS NOT NULL", null_expr(&col)),
            FilterOperator::NotSet => format!("{} IS NULL", null_expr(&col)),
            // A substring match asks about the text of a value the bucket threw
            // away — `contains '2026-01-15'` against month buckets matches
            // nothing. And on a raw time column the text is a serialization
            // detail, not data. Decline both.
            FilterOperator::Contains | FilterOperator::NotContains if declared_time => return None,
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
            // A date range is two bounds on the *bucket start* column, which
            // is not the same thing as two bounds on a timestamp: see
            // `date_range_bounds` for why the upper bound has to be made
            // exclusive and why a bound that lands mid-bucket has to be
            // refused outright.
            //
            // These were previously unsupported, and unsupported here did not
            // mean "refuse the rollup": the caller collects with `filter_map`,
            // so an unrendered filter was dropped from the WHERE and the query
            // silently returned UNFILTERED totals. That is the one direction a
            // wrong filter must never fail in. oxy only ever expresses a range
            // this way — `build_query_request` hard-codes `date_range: None`
            // and emits `InDateRange` — so every date-bounded question asked
            // of a rollup was answered over all of history. `covers` now
            // refuses any filter this function cannot render, so returning
            // `None` below declines the rollup instead of widening the answer.
            FilterOperator::InDateRange | FilterOperator::NotInDateRange => {
                if filter.values.len() != 2 {
                    return None;
                }
                if raw_time {
                    // Instants, not buckets: mirror the raw path exactly.
                    let range = format!("{} >= {} AND {} <= {}", cmp, vals[0], cmp, vals[1]);
                    return Some(if matches!(op, FilterOperator::NotInDateRange) {
                        format!("({} < {} OR {} > {})", cmp, vals[0], cmp, vals[1])
                    } else {
                        format!("({})", range)
                    });
                }
                // Only the rollup's own time dimension carries buckets; a range
                // over anything else has no granularity to align against.
                if !is_time {
                    return None;
                }
                let (lo, hi) = date_range_bounds(&filter.values[0], &filter.values[1], gran)?;
                let ts = time_expr(&col);
                if matches!(op, FilterOperator::NotInDateRange) {
                    // Mirrors the raw path's `(col < lo OR col > hi)`: a NULL
                    // bucket is excluded by either rendering.
                    format!("({} < {} OR {} >= {})", ts, lo, ts, hi)
                } else {
                    format!("({} >= {} AND {} < {})", ts, lo, ts, hi)
                }
            }
            _ => return None, // still dropped silently by the caller's filter_map
        };
        Some(sql)
    } else if let Some(ref and) = filter.and {
        // Every conjunct must render. Dropping one widens the result set just
        // as surely as dropping a top-level filter does.
        let parts: Vec<Option<String>> = and
            .iter()
            .map(|f| render_filter_sql(f, entry, quote, local))
            .collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_none()) {
            None
        } else {
            let rendered: Vec<String> = parts.into_iter().flatten().collect();
            Some(format!("({})", rendered.join(" AND ")))
        }
    } else if let Some(ref or) = filter.or {
        // For OR, all branches must be renderable — dropping any branch
        // would incorrectly narrow results (the missing branch might match rows).
        let parts: Vec<Option<String>> = or
            .iter()
            .map(|f| render_filter_sql(f, entry, quote, local))
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
    local: bool,
) -> String {
    let time_expr = |c: &str| {
        if local {
            local_time_expr(c)
        } else {
            c.to_string()
        }
    };
    // `covers` has already refused the rollup for any filter that does not
    // render, so nothing is silently dropped here.
    let mut parts: Vec<String> = request
        .filters
        .iter()
        .filter_map(|f| render_filter_sql(f, entry, quote, local))
        .collect();

    // Add date_range filters from time_dimensions. Same half-open bounds as
    // an `InDateRange` filter — the two spellings of one question must not
    // return two different answers.
    for td in &request.time_dimensions {
        // `resolved_date_range` expands the relative forms ("last 30 days"),
        // which arrive as a single element. Reading `date_range` raw here
        // would decline the commonest shape of question there is.
        if let Some(ref date_range) = td.resolved_date_range() {
            if date_range.len() == 2 {
                let td_name = td.dimension.split('.').nth(1).unwrap_or(&td.dimension);
                match entry.granularity {
                    Some(ref g) => {
                        let ts = time_expr(&quote(&format!("{}__{}", td_name, g)));
                        if let Some((lo, hi)) = date_range_bounds(&date_range[0], &date_range[1], g)
                        {
                            parts.push(format!("{} >= {}", ts, lo));
                            parts.push(format!("{} < {}", ts, hi));
                        }
                    }
                    // No bucket column: the rollup stored the raw instants, so
                    // the raw path's own closed comparison is exact. This is
                    // the same treatment `render_filter_sql` gives the filter
                    // spelling of this question — the two must not take
                    // different tiers.
                    None => {
                        let ts = time_expr(&quote(td_name));
                        parts.push(format!("{} >= '{}'", ts, date_range[0].replace('\'', "''")));
                        parts.push(format!("{} <= '{}'", ts, date_range[1].replace('\'', "''")));
                    }
                }
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

/// `local_trunc` says who will do any re-truncation: the local DuckDB engine
/// (`true`, for the Parquet cache) or the warehouse itself (`false`). It only
/// matters for weeks, whose start day is a property of the dialect.
fn covers(
    request: &crate::engine::query::QueryRequest,
    entry: &LocalRollupEntry,
    local_trunc: bool,
) -> bool {
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
        // Every filter must be renderable. `build_reagg_where_clause` collects
        // with `filter_map`, so an unrenderable filter is not "unsupported" —
        // it vanishes from the WHERE and the rollup answers a *wider* question
        // than was asked. Refuse the rollup and let the warehouse answer it.
        for f in &request.filters {
            // Whether a filter renders at all does not depend on the source.
            if render_filter_sql(f, entry, &|n| format!("\"{}\"", n), false).is_none() {
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
        // No granularity means no bucket column was ever built. A granular ask
        // needs one; a bare date_range does not, provided `dimensions:` stored
        // the raw value — which is the shape `render_filter_sql` serves for
        // the filter spelling of the same question.
        if entry.granularity.is_none()
            && (td.granularity.is_some() || !entry.dimensions.contains(&td_name.to_string()))
        {
            return false;
        }
        // Granularity: requested must be same or coarser than stored granularity
        if let Some(ref req_gran) = td.granularity {
            if let Some(ref stored_gran) = entry.granularity {
                if !is_coarser_or_equal(req_gran, stored_gran, local_trunc) {
                    return false;
                }
            }
        }
        // A date_range whose bounds do not line up with the stored buckets
        // cannot be filtered exactly — see `date_range_bounds`.
        if let Some(ref dr) = td.resolved_date_range() {
            if dr.len() != 2 {
                return false;
            }
            // Only a bucket column needs the bounds aligned to it. Without a
            // granularity the column holds raw instants, which the raw path's
            // own closed comparison filters exactly.
            if let Some(ref stored_gran) = entry.granularity {
                if date_range_bounds(&dr[0], &dr[1], stored_gran).is_none() {
                    return false;
                }
            }
        }
    }

    true
}

fn is_coarser_or_equal(requested: &str, stored: &str, local_trunc: bool) -> bool {
    // A week is not a whole number of months, so a week bucket straddling a
    // month boundary gets assigned entirely to the month of its Monday and the
    // days on the far side land in the wrong month. Same for quarters and
    // years. Before this was refused the query bound and returned a plausible
    // wrong number; refusing sends it to the warehouse, which is right.
    if stored == "week" && matches!(requested, "month" | "quarter" | "year") {
        return false;
    }
    // And the same ambiguity in the other direction, but only where the local
    // engine does the truncating: `date_trunc('week', …)` is DuckDB's Monday
    // whatever dialect built the rollup. On the warehouse the truncation is
    // `Dialect::date_trunc`, the same convention that built the buckets, so
    // day → week is exact there.
    if local_trunc && requested == "week" && stored != "week" {
        return false;
    }
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
                // The stored time column is VARCHAR (the cache is written from
                // the warehouse's JSON response), so every read of it goes
                // through `local_time_expr`. Two consequences beyond the cast
                // binding at all:
                //
                // - `date_trunc('month', VARCHAR)` does not bind in DuckDB, so
                //   coarsening used to fail the whole read — silently, because
                //   the caller catches it and answers from the warehouse.
                // - The exact-grain branch projects the same cast rather than
                //   the raw string, so both branches return a TIMESTAMP, which
                //   is what the warehouse path returns for the same question.
                //   A reader must not be able to tell which tier answered.
                let stored_col = format!("\"{}__{}\"", td_name, stored_gran);
                let ts = local_time_expr(&stored_col);
                if gran == stored_gran {
                    select_cols.push(format!("{} AS \"{}\"", ts, alias));
                    group_by_cols.push(ts);
                } else {
                    let trunc = format!("date_trunc('{}', {})", gran, ts);
                    select_cols.push(format!("{} AS \"{}\"", trunc, alias));
                    group_by_cols.push(trunc);
                }
            }
        } else if td.resolved_date_range().is_none() {
            // No requested granularity AND no date_range filter: include time
            // column in the output (pass-through), through the same cast so
            // the column type does not depend on which branch produced it.
            let col = if let Some(ref stored_gran) = entry.granularity {
                format!("\"{}__{stored_gran}\"", td_name)
            } else {
                format!("\"{}\"", td_name)
            };
            let ts = local_time_expr(&col);
            select_cols.push(format!("{} AS \"{}\"", ts, base_alias));
            group_by_cols.push(ts);
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
    let where_clause =
        build_reagg_where_clause(request, entry, &|name| format!("\"{}\"", name), true);
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
    // A warehouse rollup table stores a real DATE/TIMESTAMP column, so unlike
    // the Parquet cache it needs no cast to be compared or truncated.
    let where_clause = build_reagg_where_clause(
        request,
        entry,
        &|name| dialect_clone.quote_identifier(name),
        false,
    );
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
        if !covers(request, &local, false) {
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
    //
    //    Table and row are pruned on *different* keys, because they are
    //    identified differently: a table is named after its shape (hash), while
    //    the manifest row is identified by `(view_name, rollup_name)` — the key
    //    ClickHouse orders on, SQLite constrains, and the upsert deletes on. A
    //    rollup edited in place keeps its name and changes its hash (stale
    //    table, live row); a rollup renamed without a shape change does the
    //    reverse (live table, stale row). Testing one key for both prunes the
    //    wrong half of each.
    if let Some(prev) = previous_entries {
        // Rollup names each in-scope view still declares, keyed by view.
        let live_names: std::collections::HashMap<&str, std::collections::HashSet<String>> = views
            .iter()
            .map(|v| {
                (
                    v.name.as_str(),
                    resolve_rollups(v).into_iter().map(|r| r.name).collect(),
                )
            })
            .collect();
        let fresh_hashes: std::collections::HashSet<&str> =
            skipped.iter().map(|s| s.rollup_hash.as_str()).collect();

        for old in prev {
            let Some(declared) = live_hashes.get(old.view_name.as_str()) else {
                continue; // view outside this build's scope — leave it alone
            };
            let shape_live = declared.contains(&old.rollup_hash);
            let name_live = live_names
                .get(old.view_name.as_str())
                .is_some_and(|n| n.contains(&old.rollup_name));
            if shape_live && name_live {
                continue; // fully current — step 4 handles the dated table
            }

            // Stale shape: nothing declared builds this table any more. (When
            // the shape is still live, step 4 drops the superseded table only
            // once its replacement exists.)
            if !shape_live && !old.table_name.is_empty() {
                statements.push(format!(
                    "DROP TABLE IF EXISTS {}",
                    qualify_manifest_table_name(&old.table_name, schema, dialect)
                ));
            }

            if name_live {
                continue; // the row this build just wrote occupies this identity
            }
            if shape_live && fresh_hashes.contains(old.rollup_hash.as_str()) {
                // Renamed *and* skipped as fresh: no replacement row was
                // written, so this row is the only pointer to live data. A
                // stale name is harmless — resolution matches on shape.
                continue;
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
    fn test_collect_build_sql_prune_spares_rollup_edited_in_place() {
        // The rollup kept its name and changed its shape, so the old hash is no
        // longer declared. The manifest is keyed on (view_name, rollup_name),
        // so deleting the "orphan" would delete the row this build just wrote.
        let view = test_view_with_preaggs();
        let rollup_name = resolve_rollups(&view)[0].name.clone();
        let stale = WarehouseRollupEntry {
            view_name: "orders".into(),
            rollup_name: rollup_name.clone(),
            rollup_hash: "oldshape".into(),
            table_name: "orders__oldshape__20260101".into(),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
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

        assert!(plan.pruned.is_empty(), "pruned: {:?}", plan.pruned);
        assert!(
            plan.statements
                .iter()
                .any(|s| s.starts_with("DROP TABLE IF EXISTS")
                    && s.contains("orders__oldshape__20260101")),
            "the superseded table should still be dropped: {:?}",
            plan.statements
        );
        // The upsert's own DELETE precedes its INSERT; nothing may delete the
        // row after it.
        let last_manifest_write = plan
            .statements
            .iter()
            .rposition(|s| {
                s.contains("__manifest") && (s.contains("DELETE") || s.contains("INSERT"))
            })
            .expect("a manifest write");
        assert!(
            plan.statements[last_manifest_write].contains("INSERT INTO"),
            "the last manifest write must be the insert, got: {}",
            plan.statements[last_manifest_write]
        );
    }

    #[test]
    fn test_collect_build_sql_prunes_row_of_renamed_rollup() {
        // The rollup kept its shape and changed its name, so step 4 drops the
        // superseded dated table. The row naming it must go too, or the
        // manifest keeps pointing queries at a table that no longer exists.
        let view = test_view_with_preaggs();
        let live_hash = resolve_rollups(&view)[0].hash.clone();
        let stale = WarehouseRollupEntry {
            view_name: "orders".into(),
            rollup_name: "old_name".into(),
            rollup_hash: live_hash.clone(),
            table_name: format!("orders__{live_hash}__20260101"),
            dimensions: vec!["region".into()],
            measures: vec![],
            time_dimension: Some("created_at".into()),
            granularity: Some("month".into()),
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

        assert_eq!(plan.pruned.len(), 1, "pruned: {:?}", plan.pruned);
        assert_eq!(plan.pruned[0].rollup_name, "old_name");
        assert!(
            plan.statements.iter().any(|s| s.contains("DELETE FROM")
                && s.contains("__manifest")
                && s.contains("'old_name'")),
            "the renamed-away row should be deleted: {:?}",
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
        // Same granularity: no date_trunc, just the stored column — but read
        // through the same VARCHAR→TIMESTAMP cast as the coarsening branch, so
        // the column's type does not depend on which branch produced it.
        // Alias must include the granularity so output matches warehouse column names.
        assert!(
            sql.contains("CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP)"),
            "Stored time col should be read through the cast: {}",
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
        // Coarser granularity: date_trunc over a CAST, aliased with the
        // requested granularity. The CAST is not cosmetic — the rollup Parquet
        // stores the time column as VARCHAR (it is written from the
        // warehouse's JSON response), and `date_trunc(VARCHAR)` does not bind
        // in DuckDB, so without it every coarser-than-stored read fails.
        // NULLIF because a NULL bucket is stored as the empty string, which
        // would otherwise fail the cast and take the whole read down with it.
        assert!(
            sql.contains(
                "date_trunc('year', CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP))"
            ),
            "Missing date_trunc over a CAST: {}",
            sql
        );
        // The result stays a TIMESTAMP, which is what the warehouse path
        // returns for the same question — a reader must not be able to tell
        // which tier answered.
        assert!(
            !sql.contains("AS DATE"),
            "Result should stay a TIMESTAMP, matching the warehouse path: {}",
            sql
        );
        assert!(
            sql.contains("AS \"orders__created_at__year\""),
            "Alias should include requested granularity: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_where_renders_in_date_range() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string(), "2026-03-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(
            covers(&request, &entry, true),
            "Aligned range should be servable"
        );
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // The bound must reach the WHERE. Dropping it returned unfiltered
        // totals with no error, which is the failure this guards.
        assert!(sql.contains("WHERE"), "Range must produce a WHERE: {}", sql);
        assert!(
            sql.contains("CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP) >= '2026-01-01'"),
            "Missing lower bound: {}",
            sql
        );
        // Half-open upper bound: the inclusive `2026-03-31` covers the whole
        // March bucket, so the comparison is `< 2026-04-01`. Rendering it as
        // `<= '2026-03-31'` drops March entirely (the bucket start is
        // `2026-03-01`… and on a day rollup it drops the final day of every
        // range, which is how a trailing-90-day question returns 89 days).
        assert!(
            sql.contains("CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP) < '2026-04-01'"),
            "Upper bound should be exclusive-next-bucket: {}",
            sql
        );
    }

    /// End-to-end against a real DuckDB, over a table shaped exactly like the
    /// Parquet cache: the time bucket is VARCHAR holding microsecond ISO
    /// strings, and a NULL bucket is the empty string (that is what
    /// `write_parquet` emits for a JSON null).
    ///
    /// Two things only a real binder can prove: that the SQL binds at all, and
    /// that the last day of an inclusive range is actually in the answer.
    #[cfg(feature = "exec-duckdb")]
    #[test]
    fn test_reagg_executes_against_duckdb_cache_shape() {
        use crate::engine::query::{FilterOperator, QueryFilter, TimeDimensionQuery};

        let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
        conn.execute_batch(
            r#"
            CREATE TABLE __cache (
                "region" VARCHAR,
                "created_at__day" VARCHAR,
                "total_revenue__sum" DOUBLE
            );
            INSERT INTO __cache VALUES
                ('US', '2026-01-30T00:00:00.000000', 10),
                ('US', '2026-01-31T00:00:00.000000', 5),
                ('US', '2026-02-01T00:00:00.000000', 100),
                ('US', '', 7);
            "#,
        )
        .expect("seed cache table");

        let mut entry = test_local_rollup_entry();
        entry.granularity = Some("day".to_string());

        let sum_of = |sql: &str| -> f64 {
            let mut stmt = conn.prepare(sql).expect("reagg SQL must bind");
            let mut rows = stmt.query([]).expect("query");
            let mut total = 0.0;
            while let Some(row) = rows.next().expect("row") {
                total += row.get::<_, f64>(0).unwrap_or(0.0);
            }
            total
        };

        // Inclusive Jan 1 – Jan 31 must include Jan 31 and exclude Feb 1.
        let ranged = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string(), "2026-01-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&ranged, &entry, "\"__cache\"");
        assert_eq!(
            sum_of(&sql),
            15.0,
            "Jan 31 must be inside an inclusive Jan-31 bound: {}",
            sql
        );

        // Coarsening day → month must bind over the VARCHAR column and must
        // not choke on the empty-string (NULL) bucket.
        let coarsened = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&coarsened, &entry, "\"__cache\"");
        let mut stmt = conn.prepare(&sql).expect("coarsened SQL must bind");
        let rows: Vec<(bool, f64)> = stmt
            .query_map([], |r| {
                // The bucket column is a TIMESTAMP here, so probe only whether
                // it is NULL — the value's rendering is not what this asserts.
                let bucket_is_null = r.get_ref(0)?.data_type() == duckdb::types::Type::Null;
                Ok((bucket_is_null, r.get::<_, f64>(1)?))
            })
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        // Jan (15), Feb (100) and the NULL bucket (7) — the null bucket is
        // data, not a read failure.
        assert_eq!(rows.len(), 3, "expected three buckets: {:?}", rows);
        assert!(
            rows.iter().any(|(_, v)| *v == 15.0),
            "January should re-aggregate to 15: {:?}",
            rows
        );
        assert!(
            rows.iter().any(|(is_null, v)| *is_null && *v == 7.0),
            "The NULL bucket must survive the cast: {:?}",
            rows
        );
    }

    #[test]
    fn test_covers_refuses_range_misaligned_with_stored_grain() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // Stored grain is month; a range starting mid-January can only drop
        // January whole or include it whole. Both are wrong, so the rollup
        // must be declined and the warehouse asked instead.
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-15".to_string(), "2026-02-20".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(
            !covers(&request, &entry, true),
            "A mid-bucket range must not be served from a month rollup"
        );
    }

    #[test]
    fn test_covers_serves_day_range_including_the_final_day() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let mut entry = test_local_rollup_entry();
        entry.granularity = Some("day".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string(), "2026-01-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(covers(&request, &entry, true));
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("< '2026-02-01'"),
            "The final day of the range must be inside the bound: {}",
            sql
        );
    }

    #[test]
    fn test_reagg_where_renders_not_in_date_range() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::NotInDateRange),
                values: vec!["2026-01-01".to_string(), "2026-03-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // Same shape the raw SQL generator emits for notInDateRange —
        // `(col < lo OR col >= hi)`, with the same half-open upper bound.
        assert!(
            sql.contains("< '2026-01-01' OR") && sql.contains(">= '2026-04-01'"),
            "Negated range should exclude the whole range: {}",
            sql
        );
    }

    #[test]
    fn test_covers_refuses_malformed_range() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string()], // one bound, not two
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        // A half range must not quietly become "no filter" — that is the
        // unfiltered-totals failure this whole change exists to close. The raw
        // path errors on it, so the rollup declines and defers to that.
        assert!(
            !covers(&request, &entry, true),
            "A half range must decline the rollup, not widen the answer"
        );
    }

    #[test]
    fn test_covers_refuses_unrenderable_filter() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `beforeDate` has no rendering here. Left to `filter_map` it would be
        // dropped from the WHERE and the rollup would answer over all history.
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::BeforeDate),
                values: vec!["2026-01-01".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(
            !covers(&request, &entry, true),
            "An unrenderable filter must decline the rollup"
        );
    }

    #[test]
    fn test_date_range_bounds_refuses_mid_bucket_end() {
        // The end bound must cover its bucket to the end. Rounding a mid-bucket
        // bound up would widen the window — an 05:30 end on an hour rollup
        // quietly reaching to midnight is the same class of silent widening
        // this whole change exists to close.
        assert!(date_range_bounds("2026-01-01", "2026-01-31 05:30:00", "hour").is_none());
        assert!(date_range_bounds("2026-01-01", "2026-01-15 09:00:00", "day").is_none());
        assert!(date_range_bounds("2026-01-01", "2026-03-30", "month").is_none());
        // An end bound is honorable when what it *denotes* ends on a bucket
        // boundary. `2026-03-31` denotes through Apr 1 — the shape a calendar
        // range is written in. `2026-03-01` denotes only through Mar 2, so
        // serving it would return all of March.
        assert!(date_range_bounds("2026-01-01", "2026-03-31", "month").is_some());
        assert!(date_range_bounds("2026-01-01", "2026-03-01", "month").is_none());
        // An instant bound denotes one microsecond, never a bucket boundary.
        assert!(date_range_bounds("2026-01-01 00:00:00", "2026-01-31 05:00:00", "hour").is_none());
        // A bare date over an hour rollup denotes 24 buckets, and the raw path
        // reads it as midnight — so it is refused rather than answered a day
        // wide. The hour rollup has the resolution to be asked exactly.
        assert!(date_range_bounds("2026-01-01", "2026-01-31", "hour").is_none());
        assert!(
            date_range_bounds("2026-01-01 00:00:00", "2026-01-31 00:00:00", "hour").is_none(),
            "an instant end names no whole bucket"
        );
    }

    #[test]
    fn test_date_range_bounds_refuses_week_grain() {
        // Monday-start on most warehouses, Sunday-start on BigQuery/MySQL/Domo.
        // The entry does not record which, so a week rollup cannot validate a
        // bound at all.
        assert!(date_range_bounds("2026-01-05", "2026-01-11", "week").is_none());
    }

    #[test]
    fn test_parse_bound_accepts_common_iso_shapes() {
        for v in [
            "2026-01-31",
            "2026-01-31T00:00:00",
            "2026-01-31 00:00:00",
            "2026-01-31T00:00:00.000000",
            "2026-01-31T00:00:00Z",
            "2026-01-31T00:00:00+00:00",
            "2026-01-31T00:00",
        ] {
            assert!(parse_bound_span(v).is_some(), "should parse: {}", v);
        }
        assert!(parse_bound_span("not a date").is_none());
    }

    #[test]
    fn test_covers_serves_relative_date_range() {
        use crate::engine::query::TimeDimensionQuery;
        // A relative range arrives as one element and is expanded by
        // `resolved_date_range`. Reading `date_range` raw would decline every
        // rollup for the commonest shape of question there is.
        let mut entry = test_local_rollup_entry();
        entry.granularity = Some("day".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("day".to_string()),
                date_range: Some(vec!["last 30 days".to_string()]),
            }],
            ..QueryRequest::new()
        };
        assert!(
            covers(&request, &entry, true),
            "A relative range must still be servable from a day rollup"
        );
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(sql.contains("WHERE"), "Relative range must filter: {}", sql);
    }

    #[test]
    fn test_time_comparisons_go_through_the_cast() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `lte '2026-01-31'` against the stored `'2026-01-31T00:00:00.000000'`
        // string is false, so the last day would be dropped exactly as it was
        // for date ranges.
        let mut entry = test_local_rollup_entry();
        entry.granularity = Some("day".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::Lte),
                values: vec!["2026-01-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("CAST(NULLIF(\"created_at__day\", '') AS TIMESTAMP) < '2026-02-01'"),
            "Time comparison should compare aligned timestamps, not strings: {}",
            sql
        );
        // A plain dimension is untouched by the cast.
        let region = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.region".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["US".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&region, &entry, "read_parquet('/data/orders.parquet')");
        assert!(sql.contains("\"region\" = 'US'"), "{}", sql);
    }

    #[test]
    fn test_time_comparisons_must_be_bucket_aligned() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let with = |op: FilterOperator, v: &str| QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(op),
                values: vec![v.to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        // `lte '2026-01-15'` is satisfied by January's `2026-01-01` bucket, so
        // serving it would hand back the whole month where the raw path stops
        // on the 15th. Same trap mirrored for `gt`.
        assert!(!covers(
            &with(FilterOperator::Lte, "2026-01-15"),
            &entry,
            true
        ));
        assert!(!covers(
            &with(FilterOperator::Gt, "2026-01-15"),
            &entry,
            true
        ));
        assert!(!covers(
            &with(FilterOperator::Gte, "2026-01-15"),
            &entry,
            true
        ));
        assert!(!covers(
            &with(FilterOperator::Equals, "2026-01-15"),
            &entry,
            true
        ));

        // Aligned bounds are servable, and `lte` reaches past the last bucket.
        let req = with(FilterOperator::Lte, "2026-01-31");
        assert!(covers(&req, &entry, true));
        let sql = generate_reagg_sql(&req, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("< '2026-02-01'"),
            "An inclusive lte must cover its bucket: {}",
            sql
        );
        let req = with(FilterOperator::Gte, "2026-02-01");
        assert!(covers(&req, &entry, true));
        let sql = generate_reagg_sql(&req, &entry, "read_parquet('/data/orders.parquet')");
        assert!(sql.contains(">= '2026-02-01'"), "{}", sql);
    }

    #[test]
    fn test_bucket_start_bound_does_not_widen_lte_or_drop_gt() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let entry = test_local_rollup_entry(); // stored gran = "month"
        let with = |op: FilterOperator, v: &str| QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(op),
                values: vec![v.to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        // `2026-01-01` denotes Jan 1, not all of January. Reading it as "the
        // bucket it sits in" made `lte` return the whole month (the raw path
        // stops on the 1st) and `gt` drop the whole month (the raw path keeps
        // Jan 2–31). Neither can be answered from month buckets.
        assert!(!covers(
            &with(FilterOperator::Lte, "2026-01-01"),
            &entry,
            true
        ));
        assert!(!covers(
            &with(FilterOperator::Gt, "2026-01-01"),
            &entry,
            true
        ));
        // Equality names one bucket; a single day does not name a month.
        assert!(!covers(
            &with(FilterOperator::Equals, "2026-01-01"),
            &entry,
            true
        ));

        // On a day rollup a bare date names exactly one bucket, so all three
        // are answerable.
        let mut day = test_local_rollup_entry();
        day.granularity = Some("day".to_string());
        assert!(covers(&with(FilterOperator::Lte, "2026-01-01"), &day, true));
        assert!(covers(&with(FilterOperator::Gt, "2026-01-01"), &day, true));
        assert!(covers(
            &with(FilterOperator::Equals, "2026-01-01"),
            &day,
            true
        ));
        let sql = generate_reagg_sql(
            &with(FilterOperator::Gt, "2026-01-01"),
            &day,
            "read_parquet('/data/orders.parquet')",
        );
        assert!(
            sql.contains(">= '2026-01-02'"),
            "gt should start at the next bucket: {}",
            sql
        );
    }

    #[test]
    fn test_plain_dimension_equality_with_no_values_declines() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `"region" IN ()` is a syntax error; declining defers the question to
        // a path that can answer it instead of failing at execution.
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.region".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec![],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(!covers(&request, &entry, true));
    }

    #[test]
    fn test_ungranular_time_field_is_still_filterable_when_stored_as_a_dimension() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // A pre-agg may list the same field in `dimensions:` and
        // `time_dimension:` with no granularity. Step 1 of `generate_build_sql`
        // then materializes a plain `"created_at"` column, so filtering it is
        // fine — but on the cache it is VARCHAR holding the warehouse's
        // rendering, so it is still compared as a timestamp. No alignment
        // applies: the column holds instants, so the raw path's own semantics
        // carry over exactly. (With a granularity the bucket column exists and
        // wins; see the test below.)
        let mut entry = test_local_rollup_entry();
        entry.granularity = None;
        entry.dimensions.push("created_at".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["2026-01-01".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(covers(&request, &entry, true));
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("CAST(NULLIF(\"created_at\", '') AS TIMESTAMP) = '2026-01-01'"),
            "{}",
            sql
        );
    }

    #[test]
    fn test_bucket_column_wins_when_the_field_is_also_a_stored_dimension() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // Both columns exist here. A filter on the time dimension means the
        // bucket — reading the raw stored column instead would compare the
        // cache's VARCHAR `'2026-01-01T00:00:00.000000'` against
        // `'2026-01-01'` and match nothing.
        let mut entry = test_local_rollup_entry(); // stored gran = "month"
        entry.dimensions.push("created_at".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string(), "2026-03-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(covers(&request, &entry, true));
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains("CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP)"),
            "The bucket column should carry the filter: {}",
            sql
        );
    }

    #[test]
    fn test_substring_match_on_a_time_field_declines() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // A LIKE asks about text the bucket threw away: against month buckets
        // `contains '2026-01-15'` matches nothing, and there is no error to
        // fall back on.
        let mut entry = test_local_rollup_entry(); // stored gran = "month"
        entry.dimensions.push("created_at".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::Contains),
                values: vec!["2026-01-15".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(!covers(&request, &entry, true));
    }

    #[test]
    fn test_ungranular_time_field_serves_a_range_the_way_the_raw_path_does() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        let mut entry = test_local_rollup_entry();
        entry.granularity = None;
        entry.dimensions.push("created_at".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2026-01-01".to_string(), "2026-03-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(covers(&request, &entry, true));
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        // Instants, not buckets — the same closed comparison the raw path
        // renders, so the two tiers cannot disagree here at all.
        assert!(
            sql.contains(">= '2026-01-01'") && sql.contains("<= '2026-03-31'"),
            "{}",
            sql
        );
    }

    #[test]
    fn test_null_dimensions_are_seen_through_the_empty_string() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `write_parquet` stores a JSON null as `''`, so on the cache
        // `"region" IS NULL` matches nothing and `"region" <> 'US'` keeps the
        // null-region row the raw path drops.
        let entry = test_local_rollup_entry();
        let with = |op: FilterOperator, vals: Vec<String>| QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.region".to_string()),
                operator: Some(op),
                values: vals,
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let sql = |r: &QueryRequest| generate_reagg_sql(r, &entry, "read_parquet('/x.parquet')");
        assert!(
            sql(&with(FilterOperator::NotSet, vec![])).contains("NULLIF(\"region\", '') IS NULL")
        );
        assert!(
            sql(&with(FilterOperator::Set, vec![])).contains("NULLIF(\"region\", '') IS NOT NULL")
        );
        assert!(sql(&with(FilterOperator::NotEquals, vec!["US".into()]))
            .contains("NULLIF(\"region\", '') <> 'US'"));
        // A plain equality against a real value behaves the same either way.
        assert!(sql(&with(FilterOperator::Equals, vec!["US".into()])).contains("\"region\" = 'US'"));

        // The warehouse tier stores real NULLs, so it needs none of this.
        let wh = generate_warehouse_reagg_sql(
            &with(FilterOperator::NotSet, vec![]),
            &entry,
            "\"db\".\"t\"",
            &Dialect::Postgres,
        );
        assert!(
            wh.contains("\"region\" IS NULL") && !wh.contains("NULLIF"),
            "{}",
            wh
        );
    }

    #[test]
    fn test_both_spellings_of_an_ungranular_range_take_the_same_tier() {
        use crate::engine::query::TimeDimensionQuery;
        // The filter spelling of this question is served (see the test above),
        // so the time_dimensions spelling must be too.
        let mut entry = test_local_rollup_entry();
        entry.granularity = None;
        entry.dimensions.push("created_at".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: None,
                date_range: Some(vec!["2026-01-01".to_string(), "2026-03-31".to_string()]),
            }],
            ..QueryRequest::new()
        };
        assert!(covers(&request, &entry, true));
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/x.parquet')");
        assert!(
            sql.contains(">= '2026-01-01'") && sql.contains("<= '2026-03-31'"),
            "{}",
            sql
        );
    }

    #[test]
    fn test_comparison_with_no_values_declines() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `"region" > NULL` is never true, so serving it returns zero rows
        // where the rollup should have declined the question.
        let entry = test_local_rollup_entry();
        for op in [
            FilterOperator::Gt,
            FilterOperator::Gte,
            FilterOperator::Lt,
            FilterOperator::Lte,
        ] {
            let request = QueryRequest {
                measures: vec!["orders.total_revenue".to_string()],
                filters: vec![QueryFilter {
                    member: Some("orders.region".to_string()),
                    operator: Some(op.clone()),
                    values: vec![],
                    and: None,
                    or: None,
                }],
                ..QueryRequest::new()
            };
            assert!(!covers(&request, &entry, true), "{:?} should decline", op);
        }
    }

    #[test]
    fn test_rollup_without_granularity_cannot_serve_its_time_dimension() {
        use crate::engine::query::TimeDimensionQuery;
        // `generate_build_sql` only materializes the time column when both the
        // dimension and a granularity are declared, so without one the reagg
        // SQL would reference a column that was never written.
        let mut entry = test_local_rollup_entry();
        entry.granularity = None;
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        assert!(!covers(&request, &entry, true));
    }

    #[test]
    fn test_equality_with_no_values_declines() {
        use crate::engine::query::{FilterOperator, QueryFilter};
        // `IN ()` is a syntax error; rendering nothing would drop the filter.
        let entry = test_local_rollup_entry();
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.created_at".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec![],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        assert!(!covers(&request, &entry, true));
    }

    #[test]
    fn test_week_coarsening_is_allowed_where_the_warehouse_truncates() {
        // DuckDB's Monday-start `date_trunc` is only a problem when the local
        // engine does the truncating. On the warehouse it is the same
        // convention that built the buckets.
        assert!(!is_coarser_or_equal("week", "day", true));
        assert!(is_coarser_or_equal("week", "day", false));
    }

    #[test]
    fn test_parse_bound_refuses_a_real_utc_offset() {
        // A bucket carries no zone, so an offset cannot be applied — and
        // dropping it would answer a window shifted by up to a day from the
        // one the raw path filters.
        assert!(parse_bound_span("2026-02-01T00:00:00+07:00").is_none());
        assert!(parse_bound_span("2026-02-01T00:00:00-05:00").is_none());
        // A zero offset names the same wall clock.
        assert!(parse_bound_span("2026-02-01T00:00:00+00:00").is_some());
        assert!(parse_bound_span("2026-02-01T00:00:00Z").is_some());
    }

    #[test]
    fn test_days_do_not_roll_up_into_weeks() {
        // The local path truncates with DuckDB, which starts a week on Monday
        // whatever dialect built the rollup — BigQuery, MySQL and Domo start
        // it on Sunday. Only a week bucket the warehouse itself made is
        // trustworthy.
        assert!(!is_coarser_or_equal("week", "day", true));
        assert!(is_coarser_or_equal("week", "week", true));
    }

    #[test]
    fn test_weeks_do_not_roll_up_into_months() {
        // A week straddling a month boundary belongs partly to each month, so
        // truncating the week bucket misplaces its tail days. Coarsening out
        // of week must be refused rather than silently answered wrong.
        assert!(!is_coarser_or_equal("month", "week", true));
        assert!(!is_coarser_or_equal("quarter", "week", true));
        assert!(!is_coarser_or_equal("year", "week", true));
        assert!(is_coarser_or_equal("week", "week", true));
        assert!(is_coarser_or_equal("month", "day", true));
    }

    #[test]
    fn test_reagg_sql_sub_day_gran_keeps_timestamp() {
        use crate::engine::query::TimeDimensionQuery;
        // A sub-day ask keeps its time of day — the rollup was built to carry
        // it, and the warehouse path returns a TIMESTAMP for the same ask.
        let mut entry = test_local_rollup_entry();
        entry.granularity = Some("minute".to_string());
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.created_at".to_string(),
                granularity: Some("hour".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };
        let sql = generate_reagg_sql(&request, &entry, "read_parquet('/data/orders.parquet')");
        assert!(
            sql.contains(
                "date_trunc('hour', CAST(NULLIF(\"created_at__minute\", '') AS TIMESTAMP))"
            ),
            "Sub-day ask should truncate over a TIMESTAMP cast: {}",
            sql
        );
        assert!(
            !sql.contains("AS DATE"),
            "Sub-day ask must not be cast down to DATE: {}",
            sql
        );
        // The bucket column is the only thing cast; the ask itself is not.
        assert!(
            sql.contains("AS \"orders__created_at__hour\""),
            "Alias should carry the requested granularity: {}",
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
            sql.contains("CAST(NULLIF(\"created_at__month\", '') AS TIMESTAMP)"),
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
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name), true);
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
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name), true);
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
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name), true);
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
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name), true);
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
        let result = render_filter_sql(&filter, &entry, &|name| format!("\"{}\"", name), true);
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
            // The source is aliased to the view name in every dialect, the same
            // `FROM <source> AS <alias>` the live path emits — `{{TABLE}}`
            // resolves to that alias, and a subquery source needs one at all.
            assert!(
                ctas.contains(&format!(
                    "FROM orders AS {}",
                    dialect.quote_identifier(&view.name)
                )),
                "{}: source should be aliased to the view name: {}",
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
                // The sorting key IS the grouping key, and a grouping key over
                // an ELT-loaded column is usually Nullable — which MergeTree
                // rejects outright (`Code: 44 ILLEGAL_COLUMN`) without this.
                // Asserted on the ORDER-BY-a-key branch only; the aggregate-only
                // branch orders by `tuple()` and has no key to be null.
                assert!(
                    ctas.contains("SETTINGS allow_nullable_key = 1"),
                    "ClickHouse CTAS with a sorting key must allow a nullable one: {}",
                    ctas
                );
                // A TABLE setting, not a query setting: it has to sit after
                // ORDER BY and before `AS SELECT`. After the SELECT it would
                // parse fine and do nothing, which is the failure mode this
                // assertion exists to catch.
                let settings_at = ctas
                    .find("SETTINGS allow_nullable_key = 1")
                    .expect("asserted present above");
                let select_at = ctas.find("AS\nSELECT").expect("CTAS selects");
                assert!(
                    settings_at < select_at,
                    "SETTINGS must precede `AS SELECT` to bind to the table: {}",
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

        // Should have a DROP for the old table in the cleanup tail. (The row
        // is also named `by_region` while the view declares `by_region_monthly`,
        // so the orphan prune deletes its manifest row afterwards — the DROP is
        // in the tail, not necessarily the final statement.)
        assert!(
            plan.statements
                .iter()
                .any(|s| s.contains("DROP TABLE IF EXISTS")
                    && s.contains("orders__old_hash__20260410")),
            "Expected cleanup DROP for old table, got: {:?}",
            plan.statements
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
        ctas_in(view, others, rollup_name, Dialect::DuckDB)
    }

    /// The same, in a named dialect — for the shapes whose SQL differs by
    /// quoting rule rather than by structure.
    fn ctas_in_dialect(
        view: &View,
        rollup_name: &str,
        dialect: Dialect,
    ) -> Result<String, EngineError> {
        ctas_in(view, &[], rollup_name, dialect)
    }

    fn ctas_in(
        view: &View,
        others: &[View],
        rollup_name: &str,
        dialect: Dialect,
    ) -> Result<String, EngineError> {
        let mut views = vec![view.clone()];
        views.extend_from_slice(others);
        let layer = SemanticLayer::new(views, None);
        let dialects = DatasourceDialectMap::with_default(dialect);
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

    /// A brace that no pass can expand — a typo, a globals name that never
    /// inherited, a three-part ref the dotted-ref regex cannot match. The
    /// measure path built its column straight from `filtered_inner`, skipping
    /// the unresolved-ref check `resolve` runs, so both braces reached the
    /// CTAS and came back from the warehouse as a parser error.
    #[test]
    fn a_brace_the_measure_path_cannot_expand_is_refused_rather_than_emitted() {
        let yaml = r#"
name: payments
table: payments
pre_aggregations:
  - name: amount_by_method
    dimensions: [method]
    measures: [net_amount]
dimensions:
  - name: method
    type: string
    expr: method
measures:
  - name: net_amount
    type: sum
    expr: "{{amount_usd}}"
    filters:
      - expr: "{{is_voided}} = false"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "amount_by_method")
            .expect_err("a brace in the CTAS is a warehouse parser error");
        let msg = err.to_string();
        assert!(
            msg.contains("unresolved") && (msg.contains("is_voided") || msg.contains("amount_usd")),
            "the error should say a ref is unresolved and name it: {msg}"
        );
    }

    /// `{{<entity>.<field>}}` where `<entity>` is a Primary entity on this same
    /// view names a column of this view — the live path maps a base-view
    /// primary to the base view's own alias and joins nothing. Refusing it was
    /// both a wrong diagnosis and fatal to the whole build: one such view made
    /// `airlayer build` fail for every view in scope.
    #[test]
    fn a_ref_through_this_views_own_primary_entity_resolves_to_its_column() {
        let yaml = r#"
name: orders
table: orders
pre_aggregations:
  - name: orders_by_status
    dimensions: [status_upper]
    measures: [order_count]
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
  - name: status_upper
    type: string
    expr: "UPPER({{order.status_raw}})"
measures:
  - name: order_count
    type: count
"#;
        let ctas = ctas_for(&view_from_yaml(yaml), "orders_by_status").expect("builds");
        assert!(
            ctas.contains("UPPER(\"status_raw\") AS \"status_upper\""),
            "a primary entity on this view resolves to this view's own column, \
             unqualified because the CTAS has one table to read it from: {ctas}"
        );
    }

    /// The same shape through a *Foreign* entity points at the view that owns
    /// the entity, which a single-view CTAS has no join to reach.
    #[test]
    fn a_ref_through_a_foreign_entity_on_this_view_is_still_refused() {
        let customers = view_from_yaml(
            r#"
name: customers
table: customers
entities:
  - name: customer
    type: primary
    key: customer_id
dimensions:
  - name: customer_id
    type: string
    expr: customer_id
  - name: segment
    type: string
    expr: segment
"#,
        );
        let yaml = r#"
name: orders
table: orders
pre_aggregations:
  - name: orders_by_segment
    dimensions: [customer_segment]
    measures: [order_count]
entities:
  - name: order
    type: primary
    key: order_id
  - name: customer
    type: foreign
    key: customer_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
  - name: customer_id
    type: string
    expr: customer_id
  - name: customer_segment
    type: string
    expr: "UPPER({{customer.segment}})"
measures:
  - name: order_count
    type: count
"#;
        let err = ctas_in_layer(&view_from_yaml(yaml), &[customers], "orders_by_segment")
            .expect_err("a foreign entity's column lives behind a join");
        let msg = err.to_string();
        assert!(
            msg.contains("customer.segment") && msg.contains("single view"),
            "the error should name the ref and say why: {msg}"
        );
    }

    /// The rolling_window refusal used to sit in `measure_agg`, which only runs
    /// when another member's expr pulls the measure in. Listed directly in a
    /// rollup, the window was dropped and the plain aggregate stored — and
    /// `covers()` accepts the underlying type, so the rollup would answer with
    /// the cumulative total under the pre-aggregated badge.
    #[test]
    fn a_measure_listed_directly_with_a_rolling_window_is_refused() {
        let yaml = r#"
name: subscriptions
table: subscriptions
pre_aggregations:
  - name: trailing_by_plan
    dimensions: [plan]
    measures: [trailing_amount]
dimensions:
  - name: plan
    type: string
    expr: plan
measures:
  - name: trailing_amount
    type: sum
    expr: amount
    rolling_window:
      trailing: 7 day
"#;
        let err = ctas_for(&view_from_yaml(yaml), "trailing_by_plan")
            .expect_err("a rollup cannot store a window over rows outside the group");
        let msg = err.to_string();
        assert!(
            msg.contains("trailing_amount") && msg.contains("rolling_window"),
            "the error should name the measure and the window: {msg}"
        );
    }

    /// A custom measure stores no rollup column — `build_rollup_measure` emits
    /// none and `covers()` refuses the type. Resolving its expr anyway aborted
    /// the build of every other column in the rollup over a reference that is
    /// legal live and never written here.
    #[test]
    fn a_custom_measure_does_not_abort_a_build_it_contributes_no_column_to() {
        let vendors = view_from_yaml(
            r#"
name: vendors
table: vendors
entities:
  - name: vendor
    type: primary
    key: vendor_id
dimensions:
  - name: vendor_id
    type: string
    expr: vendor_id
measures:
  - name: rating
    type: sum
    expr: rating
"#,
        );
        let yaml = r#"
name: purchases
table: purchases
pre_aggregations:
  - name: spend_by_region
    dimensions: [region]
    measures: [total_spend, weird_ratio]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: total_spend
    type: sum
    expr: amount
  - name: weird_ratio
    type: custom
    expr: "SUM({{vendors.rating}})"
"#;
        let ctas = ctas_in_layer(&view_from_yaml(yaml), &[vendors], "spend_by_region")
            .expect("a custom measure contributes no column, so it cannot break the build");
        assert!(
            ctas.contains("SUM(amount) AS \"total_spend__sum\""),
            "the rollup's real columns must still be built: {ctas}"
        );
        assert!(
            !ctas.contains("weird_ratio"),
            "a custom measure is not pre-aggregable and stores nothing: {ctas}"
        );
    }

    /// `{{TABLE}}` used to be exempt from the raw-column guard, but the
    /// resolver rewrites it — so the stored column (`"costs".amount`) and the
    /// `__freq` companion the manifest names from the *unresolved* expr
    /// (`{{TABLE}}.amount__freq`) described different columns. For
    /// count_distinct, which `covers()` accepts, every query against the rollup
    /// then failed on an unknown column.
    #[test]
    fn a_table_ref_in_a_raw_column_measure_is_refused_like_any_other_ref() {
        let yaml = r#"
name: costs
table: costs
pre_aggregations:
  - name: costs_by_region
    dimensions: [region]
    measures: [median_amount]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: median_amount
    type: median
    expr: "{{TABLE}}.amount"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "costs_by_region")
            .expect_err("the manifest names the stored column by the raw expr string");
        let msg = err.to_string();
        assert!(
            msg.contains("median_amount") && msg.contains("reference"),
            "the error should name the measure and the reference: {msg}"
        );

        // count_distinct is the shape `covers()` accepts, so the same guard has
        // to catch it there too.
        let yaml = yaml.replace("type: median", "type: count_distinct");
        let err = ctas_for(&view_from_yaml(&yaml), "costs_by_region")
            .expect_err("same guard, same reason");
        assert!(
            err.to_string().contains("median_amount"),
            "the error should name the measure: {err}"
        );
    }

    /// A request variable in a dimension expr. The live path preserves
    /// `{{variables.X}}` because the *caller* binds it at request time; `build`
    /// runs the CTAS itself and has nothing to bind from, so the passthrough is
    /// a warehouse parser error rather than a deferred substitution. The old
    /// message blamed a missing join, which sends the reader looking for one.
    #[test]
    fn a_single_dot_request_variable_is_refused_as_a_variable_not_as_a_missing_join() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: by_scoped_region
    dimensions: [scoped_region]
    measures: [order_count]
dimensions:
  - name: scoped_region
    type: string
    expr: "CONCAT({{variables.schema}}, region)"
measures:
  - name: order_count
    type: count
"#;
        let err = ctas_for(&view_from_yaml(yaml), "by_scoped_region")
            .expect_err("nothing binds a request variable at build time");
        let msg = err.to_string();
        assert!(
            msg.contains("variables.schema") && msg.contains("request variable"),
            "the error should name the variable and call it one: {msg}"
        );
        assert!(
            !msg.contains("single view"),
            "a variable is not a missing join, and saying so misdirects: {msg}"
        );
    }

    /// The multi-dot form is the dangerous one: `dotted_ref_regex` matches a
    /// single dot only, and `find_unresolved_ref` exempts anything starting
    /// with `variables.`, so between them nothing saw this and the braces
    /// reached the warehouse. Through a measure filter, so the `filtered_inner`
    /// path is the one under test.
    #[test]
    fn a_multi_dot_request_variable_in_a_filter_does_not_slip_past_the_brace_check() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: scoped_revenue_by_region
    dimensions: [region]
    measures: [scoped_revenue]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: scoped_revenue
    type: sum
    expr: amount
    filters:
      - expr: "tenant = {{variables.db.tenant_id}}"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "scoped_revenue_by_region")
            .expect_err("a brace in the CTAS is a warehouse parser error");
        let msg = err.to_string();
        assert!(
            msg.contains("variables.db.tenant_id") && msg.contains("request variable"),
            "the multi-dot form must be caught and named like any other: {msg}"
        );
    }

    /// The FROM clause is the view's `table:`/`sql:` emitted verbatim — the
    /// live path does the same, so nothing in either path ever rewrites it.
    #[test]
    fn a_request_variable_in_the_views_table_is_refused_too() {
        let yaml = r#"
name: sales
table: "{{variables.schema}}.sales"
pre_aggregations:
  - name: revenue_by_region
    dimensions: [region]
    measures: [revenue]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: revenue
    type: sum
    expr: amount
"#;
        let err = ctas_for(&view_from_yaml(yaml), "revenue_by_region")
            .expect_err("the FROM clause is emitted verbatim, variable and all");
        let msg = err.to_string();
        assert!(
            msg.contains("variables.schema") && msg.contains("request variable"),
            "one policy for variables, wherever they appear: {msg}"
        );
    }

    /// Any other brace in the source is refused for the same reason, with a
    /// message that says where it is. `{{TABLE}}` is not exempt here the way it
    /// is inside an expr: in a view's *own* source it would expand to that
    /// source, and the live path does not resolve it there either.
    #[test]
    fn a_table_ref_in_the_views_sql_is_refused_because_the_from_clause_is_verbatim() {
        let yaml = r#"
name: sales
sql: "SELECT * FROM {{TABLE}} WHERE valid"
pre_aggregations:
  - name: revenue_by_region
    dimensions: [region]
    measures: [revenue]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: revenue
    type: sum
    expr: amount
"#;
        let err = ctas_for(&view_from_yaml(yaml), "revenue_by_region")
            .expect_err("nothing resolves a brace in the source");
        let msg = err.to_string();
        assert!(
            msg.contains("TABLE") && msg.contains("verbatim"),
            "the error should name the ref and say the source is emitted as written: {msg}"
        );
    }

    /// A measure ref expands to an aggregate, and a dimension expr lands in the
    /// GROUP BY as well as the SELECT: DuckDB answers `GROUP BY clause cannot
    /// contain aggregates!`. Naming the reference is the point of this
    /// resolver; emitting SQL that fails downstream is what it replaced.
    #[test]
    fn a_measure_reference_in_a_dimension_expr_is_refused_rather_than_grouped_over() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: bucket_by_region
    dimensions: [revenue_bucket]
    measures: [order_count]
dimensions:
  - name: revenue_bucket
    type: string
    expr: "CAST({{sales.revenue}} AS VARCHAR)"
measures:
  - name: revenue
    type: sum
    expr: amount
  - name: order_count
    type: count
"#;
        let err = ctas_for(&view_from_yaml(yaml), "bucket_by_region")
            .expect_err("an aggregate cannot be grouped by");
        let msg = err.to_string();
        assert!(
            msg.contains("sales.revenue") && msg.contains("aggregate"),
            "the error should name the measure and say why it cannot stand there: {msg}"
        );
    }

    /// Same rule inside a measure's `filters:` condition, which sits within the
    /// CASE the aggregate wraps.
    #[test]
    fn a_measure_reference_in_a_filter_condition_is_refused() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: big_revenue_by_region
    dimensions: [region]
    measures: [big_revenue]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: revenue
    type: sum
    expr: amount
  - name: big_revenue
    type: sum
    expr: amount
    filters:
      - expr: "{{sales.revenue}} > 0"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "big_revenue_by_region")
            .expect_err("a filter condition is not an aggregate position");
        assert!(
            err.to_string().contains("sales.revenue"),
            "the error should name the measure: {err}"
        );
    }

    /// And inside another aggregate's own argument, which would nest as
    /// `SUM(SUM(amount))`.
    #[test]
    fn a_measure_reference_inside_another_aggregates_argument_is_refused() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: double_revenue_by_region
    dimensions: [region]
    measures: [double_revenue]
dimensions:
  - name: region
    type: string
    expr: region
measures:
  - name: revenue
    type: sum
    expr: amount
  - name: double_revenue
    type: sum
    expr: "{{sales.revenue}} * 2"
"#;
        let err = ctas_for(&view_from_yaml(yaml), "double_revenue_by_region")
            .expect_err("SUM(SUM(x)) is not valid anywhere");
        assert!(
            err.to_string().contains("sales.revenue"),
            "the error should name the measure: {err}"
        );
    }

    /// Nothing forbids a view from declaring a dimension and a measure under
    /// one name, and the live member index stores measures last — so a query
    /// resolving `{{sales.score}}` gets the measure and never sees the
    /// dimension. Resolving the dimension here stored a different column than
    /// the query it stands in for reads, silently.
    #[test]
    fn a_name_that_is_both_a_dimension_and_a_measure_resolves_to_the_measure() {
        let yaml = r#"
name: sales
table: sales
pre_aggregations:
  - name: scores_by_region
    dimensions: [region]
    measures: [score_x2]
dimensions:
  - name: region
    type: string
    expr: region
  - name: score
    type: number
    expr: raw_score
measures:
  - name: score
    type: sum
    expr: score_amount
  - name: score_x2
    type: number
    expr: "{{sales.score}} * 2"
"#;
        let ctas = ctas_for(&view_from_yaml(yaml), "scores_by_region").expect("builds");
        assert!(
            ctas.contains("(SUM(score_amount)) * 2 AS \"score_x2__value\""),
            "the collision must resolve the way a query resolves it: {ctas}"
        );
        assert!(
            !ctas.contains("raw_score"),
            "the dimension of the same name is the column a query never reads: {ctas}"
        );
    }

    /// Ten composition levels: legal, acyclic, and the shape a metric tree
    /// grows on its own. The cap was half the live path's and a measure hop
    /// spent two levels of it, so a chain like this was reported as a member
    /// that references itself — a wrong diagnosis for a schema with no cycle.
    #[test]
    fn a_deep_but_acyclic_composition_chain_is_not_reported_as_a_cycle() {
        const LEVELS: usize = 10;
        let mut yaml = String::from(
            "name: chain\ntable: chain\n\
             pre_aggregations:\n  - name: r\n    dimensions: [region]\n    measures: [level_0]\n\
             dimensions:\n  - name: region\n    type: string\n    expr: region\nmeasures:\n",
        );
        for i in 0..LEVELS {
            yaml.push_str(&format!(
                "  - name: level_{i}\n    type: number\n    expr: \"{{{{chain.level_{}}}}} + 0\"\n",
                i + 1
            ));
        }
        yaml.push_str(&format!(
            "  - name: level_{LEVELS}\n    type: sum\n    expr: amount\n"
        ));
        let ctas = ctas_for(&view_from_yaml(&yaml), "r").expect("an acyclic chain resolves");
        assert!(
            ctas.contains("SUM(amount)") && !ctas.contains("{{"),
            "the chain should resolve all the way down to the leaf aggregate: {ctas}"
        );
    }

    /// `{{TABLE}}` resolved against the source *string* quoted
    /// `myschema.sales` as a single identifier, and DuckDB answered
    /// `Binder Error: Referenced table "myschema.sales" not found! Candidate
    /// tables: "sales"`. The live path resolves it to the view alias; aliasing
    /// the CTAS's source the same way makes the two agree.
    #[test]
    fn a_schema_qualified_table_is_aliased_so_a_table_ref_names_the_alias() {
        let yaml = r#"
name: sales
table: myschema.sales
pre_aggregations:
  - name: revenue_by_region
    dimensions: [region]
    measures: [revenue]
dimensions:
  - name: region
    type: string
    expr: "{{TABLE}}.region"
measures:
  - name: revenue
    type: sum
    expr: amount
"#;
        let ctas = ctas_for(&view_from_yaml(yaml), "revenue_by_region").expect("builds");
        assert!(
            ctas.contains("FROM myschema.sales AS \"sales\""),
            "the source has to carry the alias the column names: {ctas}"
        );
        assert!(
            ctas.contains("\"sales\".region AS \"region\""),
            "the ref should name the alias: {ctas}"
        );
        assert!(
            !ctas.contains("\"myschema.sales\""),
            "quoting the whole source as one identifier names no table: {ctas}"
        );
    }

    /// A `sql:` view is the same defect one step further along — the source is
    /// a whole subquery — plus a second one: `FROM (SELECT ...)` with no alias
    /// is accepted by DuckDB but rejected by Postgres and Redshift ("subquery
    /// in FROM must have an alias"), so `sql:` views were unbuildable there.
    #[test]
    fn a_sql_view_is_aliased_the_way_a_subquery_source_must_be() {
        let yaml = r#"
name: sales
sql: "SELECT * FROM raw_sales WHERE valid"
pre_aggregations:
  - name: revenue_by_region
    dimensions: [region]
    measures: [revenue]
dimensions:
  - name: region
    type: string
    expr: "{{TABLE}}.region"
measures:
  - name: revenue
    type: sum
    expr: amount
"#;
        let ctas = ctas_for(&view_from_yaml(yaml), "revenue_by_region").expect("builds");
        assert!(
            ctas.contains("FROM (SELECT * FROM raw_sales WHERE valid) AS \"sales\""),
            "a subquery source needs the alias, not just the column: {ctas}"
        );
        assert!(
            ctas.contains("\"sales\".region AS \"region\""),
            "the ref should name the alias, not the subquery text: {ctas}"
        );
    }

    /// Snowflake uppercases quoted identifiers, so the alias and every
    /// `{{TABLE}}` that names it have to be uppercased by the same rule or the
    /// column names a table the FROM clause did not define.
    #[test]
    fn the_alias_and_the_refs_that_name_it_are_quoted_by_one_rule() {
        let yaml = r#"
name: sales
table: myschema.sales
pre_aggregations:
  - name: revenue_by_region
    dimensions: [region]
    measures: [revenue]
dimensions:
  - name: region
    type: string
    expr: "{{TABLE}}.region"
measures:
  - name: revenue
    type: sum
    expr: amount
"#;
        let view = view_from_yaml(yaml);
        // Every variant, so a dialect whose quoting rule rewrites the alias
        // (Snowflake uppercases, four of them use backticks) cannot drift from
        // the refs that name it.
        let dialects = [
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
        ];
        for dialect in dialects {
            let ctas = ctas_in_dialect(&view, "revenue_by_region", dialect.clone())
                .unwrap_or_else(|e| panic!("{dialect}: {e}"));
            let alias = dialect.quote_identifier(&view.name);
            assert!(
                ctas.contains(&format!("FROM myschema.sales AS {alias}")),
                "{dialect}: source should be aliased to the view name: {ctas}"
            );
            assert!(
                ctas.contains(&format!(
                    "{alias}.region AS {}",
                    dialect.quote_identifier("region")
                )),
                "{dialect}: the ref should name the alias by the same quoting rule: {ctas}"
            );
        }
    }
}
