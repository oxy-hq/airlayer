pub mod catalog;
pub mod evaluator;
pub mod join_graph;
pub mod member_sql;
pub mod metric_tree;
pub mod metric_tree_fit;
pub mod metric_tree_ops;
pub mod motifs;
pub mod preagg;
pub mod profiler;
pub mod promotions;
pub mod query;
pub mod response;
pub mod shift;
pub mod sql_generator;

mod error;

pub use error::EngineError;

/// Default row limit applied to a compiled query when the caller specifies no
/// `limit`. Without this, a dimension-only query over a large fact table
/// compiles to an unbounded `SELECT col FROM table`, which streams the entire
/// table back and can OOM the server / time out the gateway. An *explicit*
/// limit — of any size — is always honored as-is; this only fills the `None`
/// case. Rollup builds construct their own SQL and never flow through
/// `compile_query`, so they are unaffected.
pub const DEFAULT_QUERY_LIMIT: u64 = 10_000;

/// The largest `QueryRequest.limit` value that's safe to emit verbatim into a
/// `LIMIT` clause. Every mainstream SQL dialect airlayer targets accepts a
/// signed 64-bit integer literal there; `i64::MAX` is the largest value
/// guaranteed to be in range everywhere. Pass this (or anything at or above
/// it) as an explicit `limit` to request an effectively unbounded query —
/// `compile_query` clamps any caller-supplied limit down to this value rather
/// than emitting it raw, so a genuinely unbounded `u64::MAX` can't overflow
/// the dialect's `BIGINT` range and fail at the database with an opaque
/// "out of range" error instead of a query that just runs long.
pub const UNBOUNDED_QUERY_LIMIT: u64 = i64::MAX as u64;

use crate::dialect::Dialect;
use crate::schema::models::{SemanticLayer, View};
use crate::schema::parser::SchemaParser;
use crate::schema::validator::SchemaValidator;
use evaluator::SchemaEvaluator;
use join_graph::JoinGraph;
use query::{QueryRequest, QueryResult};
use sql_generator::SqlGenerator;
use std::collections::HashMap;
use std::path::Path;

/// Maps datasource names to SQL dialects.
/// Built from config.yml `databases` entries or passed explicitly.
#[derive(Debug, Clone, Default)]
pub struct DatasourceDialectMap {
    map: HashMap<String, Dialect>,
    default: Option<Dialect>,
    /// Whether the default was explicitly set (via CLI -d flag or config.yml),
    /// as opposed to being inferred from view-level dialect fields.
    explicit_default: bool,
}

impl DatasourceDialectMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            default: None,
            explicit_default: false,
        }
    }

    /// Create a map with a single default dialect for all datasources.
    pub fn with_default(dialect: Dialect) -> Self {
        Self {
            map: HashMap::new(),
            default: Some(dialect),
            explicit_default: true,
        }
    }

    /// Add a datasource -> dialect mapping.
    pub fn insert(&mut self, datasource: &str, dialect: Dialect) {
        self.map.insert(datasource.to_string(), dialect);
    }

    /// Set the default dialect (used when a view has no datasource or when
    /// the datasource isn't in the map).
    pub fn set_default(&mut self, dialect: Dialect) {
        self.default = Some(dialect);
        self.explicit_default = true;
    }

    /// Set the default dialect inferred from view-level fields (lower priority than explicit).
    fn set_inferred_default(&mut self, dialect: Dialect) {
        self.default = Some(dialect);
        // Don't set explicit_default — this is a soft/inferred default
    }

    /// Resolve the dialect for a given datasource name.
    pub fn resolve(&self, datasource: Option<&str>) -> Result<&Dialect, EngineError> {
        if let Some(ds) = datasource {
            if let Some(d) = self.map.get(ds) {
                return Ok(d);
            }
        }
        self.default.as_ref().ok_or_else(|| {
            let ds_name = datasource.unwrap_or("<none>");
            EngineError::SchemaError(format!(
                "No dialect configured for datasource '{}' and no default dialect set",
                ds_name
            ))
        })
    }

    /// Check whether a datasource name is explicitly mapped in this config.
    pub fn has_datasource(&self, datasource: &str) -> bool {
        self.map.contains_key(datasource)
    }

    /// Load from a config.yml databases section.
    pub fn from_config_databases(databases: &[DatabaseConfig]) -> Self {
        let mut m = Self::new();
        for db in databases {
            if let Some(dialect) = Dialect::from_str(&db.db_type) {
                m.insert(&db.name, dialect);
            }
        }
        // Use the first database as default if there is one
        if let Some(first) = databases.first() {
            if let Some(dialect) = Dialect::from_str(&first.db_type) {
                m.set_default(dialect);
            }
        }
        m
    }
}

/// A database entry from config.yml.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub db_type: String,
}

/// Pre-aggregation configuration from config.yml.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PreAggConfig {
    /// Schema/database name for pre-aggregated tables. Default: "AIRLAYER".
    pub schema: Option<String>,
    /// Which database to use for pre-aggregation. Default: first database.
    pub database: Option<String>,
}

/// Partial config.yml — only the fields we need.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PartialConfig {
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,
    #[serde(default)]
    pub pre_aggregations: Option<PreAggConfig>,
}

/// The main semantic engine. Load .view.yml files, compile queries to SQL.
pub struct SemanticEngine {
    semantic_layer: SemanticLayer,
    evaluator: SchemaEvaluator,
    join_graph: JoinGraph,
    dialects: DatasourceDialectMap,
    promotions: promotions::Promotions,
}

impl SemanticEngine {
    /// Load a semantic layer from a directory containing .view.yml and .topic.yml files.
    #[cfg(feature = "cli")]
    pub fn load(
        views_dir: &Path,
        topics_dir: Option<&Path>,
        dialects: DatasourceDialectMap,
    ) -> Result<Self, EngineError> {
        let parser = SchemaParser::new();
        let semantic_layer = parser.parse_directory(views_dir, topics_dir)?;
        Self::from_semantic_layer(semantic_layer, dialects)
    }

    /// Build from an already-parsed SemanticLayer.
    pub fn from_semantic_layer(
        semantic_layer: SemanticLayer,
        mut dialects: DatasourceDialectMap,
    ) -> Result<Self, EngineError> {
        SchemaValidator::validate(&semantic_layer)?;

        // If no default dialect is set, try to infer from view-level dialect fields.
        // If all views with a dialect field agree, use that as the default.
        if dialects.default.is_none() {
            let mut view_dialect: Option<Dialect> = None;
            let mut conflict = false;
            for view in &semantic_layer.views {
                // Skip views whose datasource is already mapped
                if let Some(ref ds) = view.datasource {
                    if dialects.has_datasource(ds) {
                        continue;
                    }
                }
                if let Some(ref dialect_str) = view.dialect {
                    if let Some(d) = Dialect::from_str(dialect_str) {
                        if let Some(ref existing) = view_dialect {
                            if std::mem::discriminant(existing) != std::mem::discriminant(&d) {
                                conflict = true;
                                break;
                            }
                        } else {
                            view_dialect = Some(d);
                        }
                    }
                }
            }
            // Only set the default if all views agree (conflict is checked at query time)
            if !conflict {
                if let Some(d) = view_dialect {
                    dialects.set_inferred_default(d);
                }
            }
        }

        let join_graph = JoinGraph::build(&semantic_layer.views)?;
        let evaluator = SchemaEvaluator::new(&semantic_layer, &join_graph)?;
        let promotions = promotions::Promotions::build(&semantic_layer.views)?;
        Ok(Self {
            semantic_layer,
            evaluator,
            join_graph,
            dialects,
            promotions,
        })
    }

    /// Compile a query request into SQL.
    /// The dialect is resolved from the views' datasources.
    pub fn compile_query(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        // Rewrite induced (promoted) measure references into their source
        // measure equivalents BEFORE resolving the dialect, so that the
        // dialect resolver sees the source view (e.g. `tx`) rather than the
        // target view (e.g. `stores`). In a multi-datasource setup the two
        // views can have different dialects; the source view's dialect is the
        // correct one to use for the generated SQL.
        let (rewritten, restorations) = self.rewrite_induced_measures(request)?;
        let request_ref: &QueryRequest = rewritten.as_ref().unwrap_or(request);

        // Fill the `limit: None` case with a default so the semantic layer never
        // emits an unbounded full-table scan. An explicit limit — including
        // `UNBOUNDED_QUERY_LIMIT` — is honored, but clamped down to
        // `UNBOUNDED_QUERY_LIMIT` if larger, so a caller-supplied `u64::MAX`
        // (or similar) can't overflow the dialect's signed 64-bit `BIGINT`
        // range and fail at the database instead of compiling cleanly. See
        // `DEFAULT_QUERY_LIMIT` / `UNBOUNDED_QUERY_LIMIT`.
        let default_limit_applied = request_ref.limit.is_none();
        let limited;
        let request_ref: &QueryRequest = if request_ref.limit.is_none() {
            let mut r = request_ref.clone();
            r.limit = Some(DEFAULT_QUERY_LIMIT);
            limited = r;
            &limited
        } else if request_ref.limit.is_some_and(|l| l > UNBOUNDED_QUERY_LIMIT) {
            let mut r = request_ref.clone();
            r.limit = Some(UNBOUNDED_QUERY_LIMIT);
            limited = r;
            &limited
        } else {
            request_ref
        };

        let dialect = self.resolve_dialect_for_query(request_ref)?;
        let generator = SqlGenerator::new(
            &self.evaluator,
            &self.join_graph,
            dialect,
            &self.semantic_layer,
        );
        let mut result = generator.generate(request_ref)?;
        result.default_limit_applied = default_limit_applied;
        // Patch back the user-facing member names on the result column
        // metadata. Each queue entry corresponds to one occurrence of that
        // rewritten name in the SELECT (in declaration order), so popping
        // FIFO gives each column slot the right user-facing label even when
        // the same rewritten name appears for both an explicit measure and an
        // induced measure.
        let mut restorations = restorations;
        for col in &mut result.columns {
            if let Some(queue) = restorations.get_mut(&col.member) {
                if let Some(original) = queue.pop_front() {
                    col.member = original;
                }
            }
        }
        Ok(result)
    }

    /// Detect promoted measures in the request and rewrite them to their
    /// source equivalents. Returns `(Some(rewritten), restorations)` when at
    /// least one measure was rewritten; `(None, empty)` otherwise.
    ///
    /// Restorations map: `rewritten_member` → ordered queue of user-facing
    /// member names. The queue is parallel to the occurrences of that member
    /// name in `new_measures` (both explicit and induced entries are enqueued
    /// in the same order they appear in the measure list). After SQL
    /// generation the restoration loop pops one entry per column occurrence,
    /// so explicit and induced slots each get the right label even when they
    /// share the same rewritten name.
    fn rewrite_induced_measures(
        &self,
        request: &QueryRequest,
    ) -> Result<
        (
            Option<QueryRequest>,
            std::collections::HashMap<String, std::collections::VecDeque<String>>,
        ),
        EngineError,
    > {
        use std::collections::{HashMap, VecDeque};
        let mut new_measures: Vec<String> = Vec::with_capacity(request.measures.len());
        // Queue-based restorations: every measure slot (explicit and induced)
        // pushes its user-facing name onto the queue for the rewritten key.
        // The restoration loop pops in FIFO order, so the i-th occurrence of
        // a given rewritten name gets the i-th original name regardless of
        // whether it was explicit or induced.
        let mut restorations: HashMap<String, VecDeque<String>> = HashMap::new();
        let mut any_rewritten = false;
        for original in &request.measures {
            // Explicit measure → no change. Enqueue the identity restoration
            // so that explicit slots correctly advance the queue counter when
            // an induced slot for the same name follows (or precedes) them.
            if self.evaluator.is_measure(original) {
                restorations
                    .entry(original.clone())
                    .or_default()
                    .push_back(original.clone());
                new_measures.push(original.clone());
                continue;
            }
            // Try the promotion closure.
            let parts: Vec<&str> = original.splitn(2, '.').collect();
            if parts.len() != 2 {
                new_measures.push(original.clone());
                continue;
            }
            let (target_view, measure_name) = (parts[0], parts[1]);
            let candidates = self.promotions.candidates(target_view, measure_name);
            if candidates.is_empty() {
                // Not induced either — let the SQL generator surface the
                // "measure not found" error so the message stays consistent.
                new_measures.push(original.clone());
                continue;
            }
            let selected: &crate::engine::promotions::InducedMeasure = if candidates.len() == 1 {
                &candidates[0]
            } else {
                // Ambiguous induced name. Use `request.through` as a hint.
                // Two-phase matching to avoid false positives when an entity
                // name equals a view name of another candidate:
                //   Phase 1 — source-view name match (explicit, unambiguous)
                //   Phase 2 — entity in hierarchy path (fallback, only when
                //              no candidate matched by source-view name)
                // Separating the phases prevents `through: ["x"]` from
                // matching both the candidate whose source_view IS "x" AND
                // candidates whose hierarchy path CONTAINS entity "x".
                let hint: &[String] = &request.through;
                let by_view: Vec<&crate::engine::promotions::InducedMeasure> = candidates
                    .iter()
                    .filter(|c| hint.iter().any(|h| h == &c.source_view))
                    .collect();
                let matched: Vec<&crate::engine::promotions::InducedMeasure> =
                    if !by_view.is_empty() {
                        by_view
                    } else {
                        candidates
                            .iter()
                            .filter(|c| hint.iter().any(|h| c.path.contains(h)))
                            .collect()
                    };
                match matched.len() {
                    1 => matched[0],
                    0 => {
                        let srcs: Vec<&str> =
                            candidates.iter().map(|c| c.source_view.as_str()).collect();
                        return Err(EngineError::QueryError(format!(
                            "Induced measure '{}' is ambiguous: reachable from {:?}. \
                             Disambiguate by qualifying the measure with its source view \
                             (e.g. '{}.{}') or by setting `through:` to a source view name \
                             or an entity in the desired path.",
                            original, srcs, candidates[0].source_view, candidates[0].source_measure,
                        )));
                    }
                    _ => {
                        let srcs: Vec<&str> =
                            matched.iter().map(|c| c.source_view.as_str()).collect();
                        return Err(EngineError::QueryError(format!(
                            "Induced measure '{}' is still ambiguous after applying \
                             `through: {:?}`: {:?} all match. Tighten the hint or qualify \
                             the measure with a source view.",
                            original, hint, srcs,
                        )));
                    }
                }
            };
            // All three additivity classes route through the same
            // source-measure rewrite. The correctness conditions differ:
            //
            // - Additive (SUM/COUNT/MIN/MAX): re-foldable. Per-join-key
            //   pre-aggregation and single-stage GROUP BY both give the
            //   right answer; either base choice works.
            //
            // - Non-additive (AVG/COUNT_DISTINCT/MEDIAN/…): the source view
            //   *must* be the base so the single-stage GROUP BY at target
            //   grain aggregates source rows directly. The `pick_base_view`
            //   tiebreaker now respects "measure-owning view wins on ties,"
            //   which gives us the right base for the typical shape of an
            //   induced query.
            //
            // - Passthrough (`number`/`custom`): the source expression
            //   embeds `{{view.measure}}` references that the SQL generator
            //   resolves to the referenced leaves' aggregated expressions.
            //   With the source view as base, the leaves naturally aggregate
            //   at the *requested* target grain (because the GROUP BY is at
            //   target grain) and the wrapping expression — typically a
            //   ratio — is computed over those aggregates. That is the
            //   correct semantics for a ratio at a coarser grain (SUM(x) /
            //   SUM(y), not SUM(x/y)).
            let _additivity = selected.additivity; // kept for future per-class branching
            let rewritten = format!("{}.{}", selected.source_view, selected.source_measure);
            restorations
                .entry(rewritten.clone())
                .or_default()
                .push_back(original.clone());
            new_measures.push(rewritten);
            any_rewritten = true;
        }
        if !any_rewritten {
            return Ok((None, restorations));
        }
        let mut rewritten = request.clone();
        rewritten.measures = new_measures;
        Ok((Some(rewritten), restorations))
    }

    /// Resolve which dialect to use for a query by looking at the datasources
    /// of the referenced views, falling back to view-level `dialect` fields.
    ///
    /// Priority chain (highest to lowest):
    /// 1. CLI `-d` flag (stored as the default on DatasourceDialectMap)
    /// 2. config.yml datasource mapping
    /// 3. View-level `dialect` field in .view.yml (injected as default at construction time)
    /// 4. Default: postgres (set by CLI when neither -d nor -c is given)
    fn resolve_dialect_for_query(&self, request: &QueryRequest) -> Result<&Dialect, EngineError> {
        let views = request.referenced_views();

        // Collect the datasources from all referenced views
        let mut datasources: Vec<Option<&str>> = Vec::new();
        for view_name in &views {
            if let Some(view) = self.semantic_layer.view_by_name(view_name) {
                datasources.push(view.datasource.as_deref());
            }
        }

        // All views in a single query should use the same dialect.
        // Use the first non-None datasource we find.
        let ds = datasources.iter().find_map(|d| *d);
        let dialect = self.dialects.resolve(ds)?;

        // Check for conflicting view-level dialect declarations,
        // but only when the default was NOT explicitly set (via CLI -d or config).
        // When an explicit default is set, it takes priority and view-level dialect is ignored.
        if !self.dialects.explicit_default {
            for view_name in &views {
                if let Some(view) = self.semantic_layer.view_by_name(view_name) {
                    // Skip views whose datasource is explicitly mapped in config
                    if let Some(ref ds_name) = view.datasource {
                        if self.dialects.has_datasource(ds_name) {
                            continue;
                        }
                    }
                    if let Some(ref dialect_str) = view.dialect {
                        if let Some(d) = Dialect::from_str(dialect_str) {
                            if std::mem::discriminant(&d) != std::mem::discriminant(dialect) {
                                return Err(EngineError::QueryError(format!(
                                    "Query spans multiple dialects: view '{}' declares dialect '{}' \
                                     but resolved dialect is '{}'. Cross-database queries are not supported.",
                                    view.name, dialect_str, dialect
                                )));
                            }
                        } else {
                            return Err(EngineError::SchemaError(format!(
                                "Unknown dialect '{}' in view '{}'",
                                dialect_str, view.name
                            )));
                        }
                    }
                }
            }
        }

        // Verify all datasource-based views agree on the dialect
        for d in &datasources {
            let other = self.dialects.resolve(*d)?;
            if std::mem::discriminant(other) != std::mem::discriminant(dialect) {
                return Err(EngineError::QueryError(format!(
                    "Query spans multiple dialects: datasource {:?} uses {} but {:?} uses {}. \
                     Cross-database queries are not supported.",
                    ds, dialect, d, other
                )));
            }
        }

        Ok(dialect)
    }

    /// List all available views.
    pub fn views(&self) -> &[View] {
        &self.semantic_layer.views
    }

    /// Get a view by name.
    pub fn view(&self, name: &str) -> Option<&View> {
        self.semantic_layer.views.iter().find(|v| v.name == name)
    }

    /// Get the semantic layer.
    pub fn semantic_layer(&self) -> &SemanticLayer {
        &self.semantic_layer
    }

    /// Get the dialect map.
    pub fn dialects(&self) -> &DatasourceDialectMap {
        &self.dialects
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    #[test]
    fn test_partial_config_with_preagg() {
        let yaml = r#"
databases:
  - name: warehouse
    type: clickhouse
pre_aggregations:
  schema: MY_CACHE
  database: warehouse
"#;
        let config: PartialConfig = serde_yaml::from_str(yaml).expect("parse config");
        let preagg = config.pre_aggregations.as_ref().expect("has preagg");
        assert_eq!(preagg.schema.as_deref(), Some("MY_CACHE"));
        assert_eq!(preagg.database.as_deref(), Some("warehouse"));
    }

    #[test]
    fn test_partial_config_preagg_defaults() {
        let yaml = r#"
databases:
  - name: warehouse
    type: clickhouse
"#;
        let config: PartialConfig = serde_yaml::from_str(yaml).expect("parse config");
        assert!(config.pre_aggregations.is_none());
    }

    fn simple_view_with_dialect(name: &str, dialect: Option<&str>) -> View {
        View {
            name: name.to_string(),
            description: Some("test".to_string()),
            label: None,
            datasource: None,
            dialect: dialect.map(|s| s.to_string()),
            table: Some(name.to_string()),
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
                primary_key: None,
                sub_query: None,
                segmentable: None,
                inherits_from: None,
                meta: None,
            }],
            measures: Some(vec![Measure {
                name: "count".to_string(),
                measure_type: MeasureType::Count,
                description: None,
                expr: None,
                original_expr: None,
                filters: None,
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
                drivers: None,
                shift: None,
                meta: None,
            }]),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }
    }

    #[test]
    fn test_default_limit_applied_when_none() {
        let view = simple_view_with_dialect("orders", Some("clickhouse"));
        let layer = SemanticLayer::new(vec![view], None);
        let engine =
            SemanticEngine::from_semantic_layer(layer, DatasourceDialectMap::new()).unwrap();

        // Dimension-only query, no limit — must NOT compile to an unbounded scan.
        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        assert!(
            result.sql.contains(&format!("LIMIT {DEFAULT_QUERY_LIMIT}")),
            "expected default LIMIT {DEFAULT_QUERY_LIMIT} on a limit-less query, got:\n{}",
            result.sql
        );
        assert!(
            result.default_limit_applied,
            "default_limit_applied must be true when the caller left limit as None"
        );
    }

    #[test]
    fn test_explicit_limit_is_honored_not_overridden() {
        let view = simple_view_with_dialect("orders", Some("clickhouse"));
        let layer = SemanticLayer::new(vec![view], None);
        let engine =
            SemanticEngine::from_semantic_layer(layer, DatasourceDialectMap::new()).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            limit: Some(5),
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        assert!(
            result.sql.contains("LIMIT 5"),
            "explicit limit must be honored, got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains(&format!("LIMIT {DEFAULT_QUERY_LIMIT}")),
            "explicit limit must not be overridden by the default, got:\n{}",
            result.sql
        );
        assert!(
            !result.default_limit_applied,
            "default_limit_applied must be false when the caller passed an explicit limit"
        );
    }

    #[test]
    fn test_unbounded_query_limit_is_honored_verbatim() {
        let view = simple_view_with_dialect("orders", Some("clickhouse"));
        let layer = SemanticLayer::new(vec![view], None);
        let engine =
            SemanticEngine::from_semantic_layer(layer, DatasourceDialectMap::new()).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            limit: Some(UNBOUNDED_QUERY_LIMIT),
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        assert!(
            result
                .sql
                .contains(&format!("LIMIT {UNBOUNDED_QUERY_LIMIT}")),
            "UNBOUNDED_QUERY_LIMIT must be honored verbatim, got:\n{}",
            result.sql
        );
        assert!(!result.default_limit_applied);
    }

    #[test]
    fn test_limit_above_unbounded_is_clamped() {
        let view = simple_view_with_dialect("orders", Some("clickhouse"));
        let layer = SemanticLayer::new(vec![view], None);
        let engine =
            SemanticEngine::from_semantic_layer(layer, DatasourceDialectMap::new()).unwrap();

        // A raw u64::MAX would overflow every mainstream dialect's signed
        // 64-bit BIGINT range and fail at the database with an opaque
        // "out of range" error. compile_query must clamp it instead of
        // emitting it as-is.
        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            limit: Some(u64::MAX),
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        assert!(
            result
                .sql
                .contains(&format!("LIMIT {UNBOUNDED_QUERY_LIMIT}")),
            "limit above UNBOUNDED_QUERY_LIMIT must be clamped down to it, got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains("18446744073709551615"),
            "the raw u64::MAX must never be emitted verbatim, got:\n{}",
            result.sql
        );
        assert!(
            !result.default_limit_applied,
            "a caller-supplied (if oversized) limit is not the silent default"
        );
    }

    #[test]
    fn test_view_level_dialect_bigquery() {
        let view = simple_view_with_dialect("orders", Some("bigquery"));
        let layer = SemanticLayer::new(vec![view], None);
        // No default dialect set — view-level dialect should be used
        let dialects = DatasourceDialectMap::new();
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // BigQuery uses backtick quoting
        assert!(
            result.sql.contains('`'),
            "Expected BigQuery backtick quoting, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_view_level_dialect_conflict_error() {
        let view1 = simple_view_with_dialect("orders", Some("bigquery"));
        let mut view2 = simple_view_with_dialect("customers", Some("postgres"));
        // Give view2 a foreign entity pointing at orders so the query can reference both
        view2.entities.push(Entity {
            name: "order".to_string(),
            entity_type: EntityType::Foreign,
            lifespan: None,
            description: None,
            key: Some("id".to_string()),
            keys: None,
            inherits_from: None,
            meta: None,
            parent: None,
        });
        // Add primary entity to orders
        let mut view1_with_entity = view1;
        view1_with_entity.entities.push(Entity {
            name: "order".to_string(),
            entity_type: EntityType::Primary,
            lifespan: None,
            description: None,
            key: Some("id".to_string()),
            keys: None,
            inherits_from: None,
            meta: None,
            parent: None,
        });

        let layer = SemanticLayer::new(vec![view1_with_entity, view2], None);
        let dialects = DatasourceDialectMap::new();
        // Construction should still succeed (conflict only checked at query time via default)
        // But since views disagree, the engine won't set a default from views
        let engine = SemanticEngine::from_semantic_layer(layer, dialects);
        // With conflicting view dialects and no default, construction still works
        // but querying across both views should fail
        assert!(
            engine.is_err() || {
                let eng = engine.unwrap();
                let request = QueryRequest {
                    dimensions: vec!["orders.id".to_string(), "customers.id".to_string()],
                    measures: vec![],
                    ..QueryRequest::new()
                };
                eng.compile_query(&request).is_err()
            }
        );
    }

    #[test]
    fn test_cli_dialect_overrides_view_dialect() {
        let view = simple_view_with_dialect("orders", Some("bigquery"));
        let layer = SemanticLayer::new(vec![view], None);
        // CLI sets postgres as default, which should override view-level bigquery
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // Postgres uses double-quote quoting, not backticks
        assert!(
            !result.sql.contains('`'),
            "Expected Postgres quoting (no backticks), got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_view_without_dialect_uses_default() {
        let view = simple_view_with_dialect("orders", None);
        let layer = SemanticLayer::new(vec![view], None);
        let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
        let engine = SemanticEngine::from_semantic_layer(layer, dialects).unwrap();

        let request = QueryRequest {
            dimensions: vec!["orders.id".to_string()],
            measures: vec!["orders.count".to_string()],
            ..QueryRequest::new()
        };
        let result = engine.compile_query(&request).unwrap();
        // Should work fine with default postgres
        assert!(
            result.sql.contains("\"orders\""),
            "Expected Postgres quoting, got:\n{}",
            result.sql
        );
    }
}
