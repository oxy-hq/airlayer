use crate::dialect::Dialect;
use crate::engine::evaluator::SchemaEvaluator;
use crate::engine::join_graph::{JoinEdge, JoinGraph, JoinRelationship};
use crate::engine::member_sql::{dotted_ref_regex, param_ref_regex, MemberSqlResolver};
use crate::engine::query::*;
use crate::engine::EngineError;
use crate::schema::models::*;
use std::collections::{HashMap, HashSet};

/// Generates SQL from a QueryRequest using the schema evaluator and join graph.
pub struct SqlGenerator<'a> {
    evaluator: &'a SchemaEvaluator,
    join_graph: &'a JoinGraph,
    dialect: &'a Dialect,
    semantic_layer: &'a SemanticLayer,
    /// Recursion depth of `resolve_expression` (which is mutually recursive with
    /// `resolve_member_refs`). Guards against cyclic member definitions — e.g. a
    /// dimension whose `expr` references itself, directly or through a cycle —
    /// which would otherwise overflow the stack. On overflow the expr is left
    /// unresolved so it surfaces as a graceful "unresolved reference" error.
    resolve_depth: std::cell::Cell<u32>,
}

/// Maximum `resolve_expression` recursion depth before bailing out. Real metric
/// trees nest only a handful of levels; anything deeper is a definition cycle.
const MAX_RESOLVE_DEPTH: u32 = 64;

/// Internal state while building a query.
struct QueryBuilder {
    /// view_name -> alias
    view_aliases: HashMap<String, String>,
    /// Collected SELECT columns
    select_columns: Vec<SelectColumn>,
    /// JOIN clauses
    joins: Vec<JoinClause>,
    /// WHERE conditions
    where_conditions: Vec<String>,
    /// GROUP BY expressions (indices into select_columns)
    group_by_indices: Vec<usize>,
    /// HAVING conditions
    having_conditions: Vec<String>,
    /// ORDER BY clauses
    order_by: Vec<String>,
    /// Parameters for parameterized queries
    params: Vec<String>,
    /// Column metadata
    columns: Vec<ColumnMeta>,
    /// The base (root) view
    base_view: String,
    /// Views whose rows are multiplied by one-to-many joins
    multiplied_views: HashSet<String>,
}

struct SelectColumn {
    expr: String,
    alias: String,
    is_aggregate: bool,
}

#[allow(dead_code)]
struct JoinClause {
    join_type: String,
    table_expr: String,
    alias: String,
    condition: String,
    relationship: JoinRelationship,
}

impl<'a> SqlGenerator<'a> {
    pub fn new(
        evaluator: &'a SchemaEvaluator,
        join_graph: &'a JoinGraph,
        dialect: &'a Dialect,
        semantic_layer: &'a SemanticLayer,
    ) -> Self {
        Self {
            evaluator,
            join_graph,
            dialect,
            semantic_layer,
            resolve_depth: std::cell::Cell::new(0),
        }
    }

    /// Compile a query request into SQL.
    ///
    /// Wraps [`Self::generate_inner`] with a guard that rejects SQL still
    /// containing unresolved `{{ ... }}` placeholders (other than the
    /// intentional `{{ variables.X }}` passthrough). Emitting such SQL to the
    /// database produces a cryptic parser error far from the cause, so we fail
    /// here with an actionable message instead.
    pub fn generate(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        let result = self.generate_inner(request)?;
        if let Some(reference) = MemberSqlResolver::find_unresolved_ref(&result.sql) {
            return Err(EngineError::SqlGenerationError(format!(
                "unresolved reference `{{{{ {reference} }}}}` left in compiled SQL: a measure or \
                 dimension `expr` used a reference airlayer could not resolve. Reference raw \
                 columns directly, or use `{{{{ entity.field }}}}` with unquoted identifiers \
                 (`{{{{ variables.X }}}}` is the only placeholder preserved in output SQL)."
            )));
        }
        Ok(result)
    }

    fn generate_inner(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        // Determine which views are involved
        let referenced_views = request.referenced_views();
        if referenced_views.is_empty() {
            return Err(EngineError::QueryError(
                "Query must reference at least one view".to_string(),
            ));
        }

        // Validate all referenced members exist
        self.validate_members(request)?;

        // Route shift-derived queries (time-shifted comparisons, e.g. same-store
        // sales) to the dedicated multi-stage self-join compiler. The single-stage
        // builder below cannot express the shifted-bucket self-join or the
        // cohort-before-shift invariant.
        if self.query_uses_shift(request) {
            return self.generate_shift(request);
        }

        // Pick base view using join-tree cost optimization. Base selection
        // uses only the views named in the request, so expansion below can't
        // change which view anchors the FROM clause.
        let base_view = self.pick_base_view(request, &referenced_views)?;

        // Include views needed by {{view.field}} / {{entity.field}} references
        // inside requested member definitions — they compile to those views'
        // aliases and must be joined just like query-level references (#55).
        let referenced_views = self.expand_views_for_expr_refs(request, &referenced_views);

        let mut builder = QueryBuilder {
            view_aliases: HashMap::new(),
            select_columns: Vec::new(),
            joins: Vec::new(),
            where_conditions: Vec::new(),
            group_by_indices: Vec::new(),
            having_conditions: Vec::new(),
            order_by: Vec::new(),
            params: Vec::new(),
            columns: Vec::new(),
            base_view: base_view.clone(),
            multiplied_views: HashSet::new(),
        };

        // Assign alias to base view
        builder
            .view_aliases
            .insert(base_view.clone(), base_view.clone());

        // Build joins for all other referenced views
        let other_views: Vec<&str> = referenced_views
            .iter()
            .filter(|v| v.as_str() != base_view)
            .map(|v| v.as_str())
            .collect();

        if !other_views.is_empty() {
            self.build_joins(&mut builder, &base_view, &other_views, &request.through)?;
        }

        // Check if fan-out protection is needed
        let measure_views: HashSet<String> = request
            .measures
            .iter()
            .filter_map(|m| m.split('.').next().map(|v| v.to_string()))
            .collect();
        let needs_fanout_protection = measure_views
            .iter()
            .any(|v| builder.multiplied_views.contains(v));

        if needs_fanout_protection && !request.measures.is_empty() {
            let fanout_result =
                self.generate_with_fanout_protection(request, &base_view, &builder)?;
            // If a motif is also requested, wrap the fan-out result
            if let Some(ref motif_name) = request.motif {
                return self.apply_motif(motif_name, request, fanout_result);
            }
            return Ok(fanout_result);
        }

        // Build entity-to-alias map for cross-entity reference resolution
        let joined_views: Vec<&str> = other_views.to_vec();
        let entity_to_alias = self
            .evaluator
            .build_entity_to_alias_map(&base_view, &joined_views);

        // Add dimensions to SELECT and GROUP BY
        for dim_path in &request.dimensions {
            self.add_dimension(&mut builder, dim_path, &entity_to_alias)?;
        }

        // Add time dimensions
        for td in &request.time_dimensions {
            self.add_time_dimension(
                &mut builder,
                td,
                &entity_to_alias,
                request.timezone.as_deref(),
            )?;
        }

        // Add measures to SELECT
        for measure_path in &request.measures {
            self.add_measure(&mut builder, measure_path, &entity_to_alias)?;
        }

        // Add filters — route to WHERE or HAVING depending on member type
        for filter in &request.filters {
            let sql = self.compile_filter(filter, &mut builder, &entity_to_alias)?;
            if !sql.is_empty() {
                if self.is_measure_filter(filter) {
                    builder.having_conditions.push(sql);
                } else {
                    builder.where_conditions.push(sql);
                }
            }
        }

        // Add segment conditions as WHERE clauses
        for seg_path in &request.segments {
            let (view, name) = self.evaluator.parse_member_path(seg_path)?;
            let seg = self.evaluator.segment(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Segment '{}' not found", seg_path))
            })?;
            let alias = builder
                .view_aliases
                .get(&view)
                .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;
            let seg_expr = self.resolve_expression(alias, &seg.expr, &entity_to_alias);
            builder.where_conditions.push(seg_expr);
        }

        // Add time dimension date range filters (supports relative date ranges)
        for td in &request.time_dimensions {
            if let Some(date_range) = td.resolved_date_range() {
                if date_range.len() == 2 {
                    let (view, member) = self.evaluator.parse_member_path(&td.dimension)?;
                    let alias = builder.view_aliases.get(&view).ok_or_else(|| {
                        EngineError::QueryError(format!(
                            "View '{}' not found in query context",
                            view
                        ))
                    })?;
                    let dim = self.evaluator.dimension(&view, &member).ok_or_else(|| {
                        EngineError::QueryError(format!("Dimension '{}' not found", td.dimension))
                    })?;
                    let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);

                    let from_param = self.alloc_param(&date_range[0], &mut builder.params);
                    let to_param = self.alloc_param(&date_range[1], &mut builder.params);

                    builder.where_conditions.push(format!(
                        "{col} >= {from} AND {col} <= {to}",
                        col = col_expr,
                        from = from_param,
                        to = to_param,
                    ));
                }
            }
        }

        // Add ORDER BY
        for order in &request.order {
            let dir = if order.desc { "DESC" } else { "ASC" };
            if let Some(col) = builder.columns.iter().find(|c| c.member == order.id) {
                builder.order_by.push(format!(
                    "{} {}",
                    self.dialect.quote_identifier(&col.alias),
                    dir
                ));
            }
        }

        // If a motif is requested, compile the base query WITHOUT order/limit,
        // then wrap it with the motif CTE.
        if let Some(ref motif_name) = request.motif {
            // Assemble base SQL without ORDER BY / LIMIT
            let base_request = QueryRequest {
                order: vec![],
                limit: None,
                offset: None,
                motif: None,
                ..request.clone()
            };
            let base_sql = self.assemble_sql(&builder, &base_request)?;

            return self.apply_motif(
                motif_name,
                request,
                QueryResult {
                    sql: base_sql,
                    params: builder.params,
                    columns: builder.columns,
                    default_limit_applied: false,
                },
            );
        }

        // Build final SQL
        let sql = self.assemble_sql(&builder, request)?;

        Ok(QueryResult {
            sql,
            params: builder.params,
            columns: builder.columns,
            default_limit_applied: false,
        })
    }

    /// Apply a motif to the base query result, wrapping it in a CTE.
    fn apply_motif(
        &self,
        motif_name: &str,
        request: &QueryRequest,
        base_result: QueryResult,
    ) -> Result<QueryResult, EngineError> {
        use crate::engine::motifs;

        // 1. Look up motif: check semantic_layer first, then builtin catalog
        let motif = self
            .semantic_layer
            .motif_by_name(motif_name)
            .cloned()
            .or_else(|| {
                if motifs::is_builtin(motif_name) {
                    motifs::builtin_motifs()
                        .into_iter()
                        .find(|m| m.name == motif_name)
                } else {
                    None
                }
            })
            .ok_or_else(|| EngineError::QueryError(format!("Unknown motif: '{}'", motif_name)))?;

        // 2. Validate requirements
        motifs::validate_requirements(&motif, request, &base_result.columns)?;

        // 3. Resolve params
        let resolved = motifs::resolve_params(&motif, &base_result.columns, &request.motif_params)?;

        // 4. Wrap with motif
        let (sql, columns) = motifs::wrap_with_motif(
            &base_result.sql,
            &base_result.columns,
            &motif,
            &resolved,
            self.dialect,
            &request.order,
            request.limit,
            request.offset,
        )?;

        Ok(QueryResult {
            sql,
            params: base_result.params,
            columns,
            default_limit_applied: false,
        })
    }

    /// Generate a query with fan-out protection using CTEs.
    /// Pre-aggregates measures from multiplied views in separate subqueries.
    fn generate_with_fanout_protection(
        &self,
        request: &QueryRequest,
        base_view: &str,
        original_builder: &QueryBuilder,
    ) -> Result<QueryResult, EngineError> {
        // Group measures by their source view
        let mut measures_by_view: HashMap<String, Vec<&str>> = HashMap::new();
        for m in &request.measures {
            if let Some(v) = m.split('.').next() {
                measures_by_view.entry(v.to_string()).or_default().push(m);
            }
        }

        // If any measure in a multiplied view is non-additive (or
        // passthrough), route the whole query through user-grain CTEs:
        // each source view's CTE joins through the entity chain to the
        // user dim views and aggregates directly at the user-dim grain.
        // The outer SELECT is then a flat join (no GROUP BY) because each
        // CTE already produces one row per user dim combination.
        //
        // The join-key CTE shape used for the all-additive path is more
        // efficient but only correct for re-foldable measures
        // (`SUM(SUM(x))=SUM(x)`). User-grain CTEs are the universal fix.
        let any_non_additive = measures_by_view.iter().any(|(view_name, paths)| {
            if !original_builder.multiplied_views.contains(view_name) {
                return false;
            }
            paths.iter().any(|mp| {
                self.evaluator
                    .parse_member_path(mp)
                    .ok()
                    .and_then(|(_, n)| self.evaluator.measure(view_name, &n))
                    .is_some_and(|m| {
                        !matches!(
                            m.measure_type,
                            MeasureType::Sum
                                | MeasureType::Count
                                | MeasureType::Min
                                | MeasureType::Max,
                        )
                    })
            })
        });
        // Measures whose exprs reference other views row-level need those
        // views joined *inside* the CTE that compiles them. The additive
        // join-key CTE below scans the source table alone, so route such
        // queries through user-grain CTEs, which join the entity chain (#55).
        let any_cross_view_measure = measures_by_view.iter().any(|(view_name, paths)| {
            paths.iter().any(|mp| {
                self.evaluator
                    .parse_member_path(mp)
                    .ok()
                    .and_then(|(_, n)| self.evaluator.measure(view_name, &n))
                    .is_some_and(|m| self.measure_crosses_views(view_name, m))
            })
        });

        if any_non_additive || any_cross_view_measure {
            return self.generate_with_user_grain_ctes(
                request,
                base_view,
                original_builder,
                &measures_by_view,
            );
        }

        let mut params = Vec::new();
        let mut columns = Vec::new();
        let mut ctes: Vec<String> = Vec::new();

        // Collect all dimension expressions we need
        let mut dim_select_parts: Vec<String> = Vec::new();
        let mut dim_aliases: Vec<String> = Vec::new();

        let entity_to_alias = self.evaluator.build_entity_to_alias_map(
            base_view,
            &original_builder
                .joins
                .iter()
                .map(|j| j.alias.as_str())
                .collect::<Vec<_>>(),
        );

        for dim_path in &request.dimensions {
            let (view, name) = self.evaluator.parse_member_path(dim_path)?;
            let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Dimension not found: {}", dim_path))
            })?;
            let alias = original_builder
                .view_aliases
                .get(&view)
                .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;
            let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);
            let col_alias = self.member_alias(dim_path);
            dim_select_parts.push(format!(
                "{} AS {}",
                col_expr,
                self.dialect.quote_identifier(&col_alias)
            ));
            dim_aliases.push(col_alias.clone());
            columns.push(ColumnMeta {
                member: dim_path.clone(),
                alias: col_alias,
                kind: ColumnKind::Dimension,
            });
        }

        for td in &request.time_dimensions {
            let (view, name) = self.evaluator.parse_member_path(&td.dimension)?;
            let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Time dimension not found: {}", td.dimension))
            })?;
            let alias = original_builder
                .view_aliases
                .get(&view)
                .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;
            let mut col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);
            if let Some(tz) = request.timezone.as_deref() {
                if tz != "UTC" {
                    col_expr = self.dialect.convert_tz(&col_expr, tz);
                }
            }
            if let Some(ref granularity) = td.granularity {
                col_expr = self.dialect.date_trunc(granularity, &col_expr);
            }
            let member_path = if let Some(ref g) = td.granularity {
                format!("{}.{}", td.dimension, g)
            } else {
                td.dimension.clone()
            };
            let col_alias = self.member_alias(&member_path);
            dim_select_parts.push(format!(
                "{} AS {}",
                col_expr,
                self.dialect.quote_identifier(&col_alias)
            ));
            dim_aliases.push(col_alias.clone());
            columns.push(ColumnMeta {
                member: member_path,
                alias: col_alias,
                kind: ColumnKind::TimeDimension,
            });
        }

        // Build dimension spine CTE with all joins
        let base = self.evaluator.view(base_view).ok_or_else(|| {
            EngineError::SqlGenerationError(format!("Base view '{}' not found", base_view))
        })?;
        let from_expr = self.view_source_expr(base);
        let mut dim_spine_sql = format!(
            "SELECT DISTINCT\n    {}\n  FROM\n    {} AS {}",
            dim_select_parts.join(",\n    "),
            from_expr,
            self.dialect.quote_identifier(base_view)
        );

        for join in &original_builder.joins {
            dim_spine_sql.push_str(&format!(
                "\n  {} JOIN {} AS {} ON {}",
                join.join_type,
                join.table_expr,
                self.dialect.quote_identifier(&join.alias),
                join.condition
            ));
        }

        // Apply WHERE filters to the spine
        let mut spine_where: Vec<String> = Vec::new();
        for filter in &request.filters {
            if !self.is_measure_filter(filter) {
                let sql = self.compile_filter_for_context(
                    filter,
                    &original_builder.view_aliases,
                    &entity_to_alias,
                    &mut params,
                )?;
                if !sql.is_empty() {
                    spine_where.push(sql);
                }
            }
        }
        for seg_path in &request.segments {
            let (view, name) = self.evaluator.parse_member_path(seg_path)?;
            let seg = self.evaluator.segment(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Segment '{}' not found", seg_path))
            })?;
            let alias = original_builder
                .view_aliases
                .get(&view)
                .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;
            spine_where.push(self.resolve_expression(alias, &seg.expr, &entity_to_alias));
        }
        for td in &request.time_dimensions {
            if let Some(ref date_range) = td.date_range {
                if date_range.len() == 2 {
                    let (view, member) = self.evaluator.parse_member_path(&td.dimension)?;
                    let alias = original_builder.view_aliases.get(&view).ok_or_else(|| {
                        EngineError::QueryError(format!("View '{}' not in query", view))
                    })?;
                    let dim = self.evaluator.dimension(&view, &member).ok_or_else(|| {
                        EngineError::QueryError(format!("Dimension '{}' not found", td.dimension))
                    })?;
                    let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);
                    let from_param = self.alloc_param(&date_range[0], &mut params);
                    let to_param = self.alloc_param(&date_range[1], &mut params);
                    spine_where.push(format!(
                        "{} >= {} AND {} <= {}",
                        col_expr, from_param, col_expr, to_param
                    ));
                }
            }
        }
        if !spine_where.is_empty() {
            dim_spine_sql.push_str(&format!(
                "\n  WHERE\n    {}",
                spine_where.join("\n    AND ")
            ));
        }

        // Defer pushing __dim_spine to `ctes` until we know which join keys
        // the measure CTEs need; the spine has to project those keys so the
        // outer JOINs can match on them.

        // Build per-view measure CTEs.
        //
        // In the fan-out path, EVERY measure-owning view gets its own CTE
        // keyed by the columns that link it to the dim spine. The previous
        // "inline-when-not-multiplied" branch was unsound: the spine's
        // `SELECT DISTINCT` collapses rows that an inline measure
        // expression needs to scan, silently dropping data. Routing every
        // measure through a CTE means the spine joins on stable keys and
        // the outer SELECT re-aggregates cleanly.
        let mut measure_cte_names: Vec<String> = Vec::new();
        let mut measure_cte_join_keys: Vec<Vec<String>> = Vec::new();
        // Per-CTE outer-aggregation functions, parallel to measure_cte_names
        // (and to the measure_paths inside each CTE). Each inner Vec lines up
        // with the order in which measures were appended to that CTE.
        let mut measure_cte_outer_aggs: Vec<Vec<&'static str>> = Vec::new();
        let final_select_measures: Vec<String> = Vec::new();

        for (view_name, measure_paths) in &measures_by_view {
            let view = self.evaluator.view(view_name).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not found", view_name))
            })?;

            // Find the join keys for this view — the columns it uses to join to other views
            let join_keys: Vec<String> = if view_name == base_view {
                // Base view's join keys come from its entity keys used in joins
                original_builder
                    .joins
                    .iter()
                    .flat_map(|j| {
                        // Parse the ON condition to find base view columns
                        // Use the join graph edges instead
                        self.join_graph
                            .edges_from(base_view)
                            .into_iter()
                            .filter(|e| e.to_view == j.alias)
                            .flat_map(|e| e.conditions.iter().map(|c| c.from_column.clone()))
                            .collect::<Vec<_>>()
                    })
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            } else {
                // Foreign view's join keys come from the edge connecting it
                let mut keys = HashSet::new();
                for join in &original_builder.joins {
                    if join.alias == *view_name {
                        // Find the edge for this join
                        for edge in self.join_graph.edges_from(&join.alias) {
                            for cond in &edge.conditions {
                                keys.insert(cond.from_column.clone());
                            }
                        }
                        // Also check edges TO this view
                        for edge in self.join_graph.all_edges() {
                            if edge.to_view == *view_name {
                                for cond in &edge.conditions {
                                    keys.insert(cond.to_column.clone());
                                }
                            }
                        }
                    }
                }
                keys.into_iter().collect()
            };

            if join_keys.is_empty() {
                return Err(EngineError::SqlGenerationError(format!(
                    "Cannot pre-aggregate measures from view '{}' in a fan-out query: \
                     no join keys connect it to the dimension spine. Add an entity \
                     declaration linking '{}' to a parent view.",
                    view_name, view_name
                )));
            }

            let cte_name = format!("__measures_{}", view_name);
            let view_source = self.view_source_expr(view);
            let view_alias = view_name;
            let empty_entity_map: HashMap<String, String> = HashMap::new();

            // Build CTE: SELECT join_keys, AGG(measures) FROM view GROUP BY join_keys.
            // `k` is an entity key *name*, not necessarily a literal column —
            // resolve it through the view's own dimension definitions.
            let key_selects: Vec<String> = join_keys
                .iter()
                .map(|k| {
                    let col_expr = self.resolve_join_key_expr(view_alias, view_alias, k);
                    format!("{} AS {}", col_expr, self.dialect.quote_identifier(k))
                })
                .collect();

            let mut measure_selects: Vec<String> = Vec::new();
            // outer_aggs: for each measure in this view's CTE, the function
            // name to use in the outer SELECT (`SUM`/`MIN`/`MAX`). Captured
            // here while we still have the Measure struct in scope.
            let mut outer_aggs: Vec<&'static str> = Vec::new();
            for mp in measure_paths {
                let (_, name) = self.evaluator.parse_member_path(mp)?;
                let measure = self
                    .evaluator
                    .measure(view_name, &name)
                    .ok_or_else(|| EngineError::QueryError(format!("Measure not found: {}", mp)))?;
                // The CTE pre-aggregates at the join-key grain; the outer
                // SELECT re-aggregates to the user dim grain. That second
                // pass is only safe for additive measure types whose
                // self-composition is identity (`SUM(SUM(x))=SUM(x)`,
                // `SUM(COUNT(*))=COUNT_total`, `MIN(MIN(x))=MIN(x)`,
                // `MAX(MAX(x))=MAX(x)`). Anything else (avg, distinct,
                // median, number, custom) would silently produce a wrong
                // number, so we refuse rather than guess.
                let outer = match measure.measure_type {
                    MeasureType::Sum | MeasureType::Count => "SUM",
                    MeasureType::Min => "MIN",
                    MeasureType::Max => "MAX",
                    _ => {
                        return Err(EngineError::QueryError(format!(
                            "Cannot fan-out non-additive measure '{}' (type {}) across \
                             multiple multiplied views. Two source views attached to a \
                             shared parent need each side aggregated to the requested \
                             target grain directly; that path is not yet implemented \
                             for non-additive measures. Query the source view directly \
                             instead.",
                            mp, measure.measure_type
                        )));
                    }
                };
                outer_aggs.push(outer);
                let agg_expr = self.measure_agg_expr(view_alias, measure, &empty_entity_map)?;
                let col_alias = self.member_alias(mp);
                measure_selects.push(format!(
                    "{} AS {}",
                    agg_expr,
                    self.dialect.quote_identifier(&col_alias)
                ));
                columns.push(ColumnMeta {
                    member: mp.to_string(),
                    alias: col_alias,
                    kind: ColumnKind::Measure,
                });
            }

            let all_selects: Vec<String> = key_selects
                .iter()
                .chain(measure_selects.iter())
                .cloned()
                .collect();

            let group_by: Vec<String> = (1..=join_keys.len()).map(|i| i.to_string()).collect();

            let cte_sql = format!(
                "{} AS (\n  SELECT\n    {}\n  FROM\n    {} AS {}\n  GROUP BY\n    {}\n)",
                cte_name,
                all_selects.join(",\n    "),
                view_source,
                self.dialect.quote_identifier(view_alias),
                group_by.join(", ")
            );
            ctes.push(cte_sql);
            measure_cte_names.push(cte_name);
            measure_cte_join_keys.push(join_keys);
            measure_cte_outer_aggs.push(outer_aggs);
        }

        // Collect all join-key column names that the measure CTEs need to
        // appear on the spine. Each key must be projected from `__dim_spine`
        // (with the correct value) so the outer LEFT JOIN can match. Any
        // view in the join tree that declares the column as a dimension is
        // an equivalent source post-join; prefer the base view to keep the
        // qualifier stable.
        let mut spine_key_set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for keys in &measure_cte_join_keys {
            for k in keys {
                spine_key_set.insert(k.clone());
            }
        }
        let spine_key_parts: Vec<String> = spine_key_set
            .iter()
            .map(|k| {
                let qualifier: &str = if base.dimensions.iter().any(|d| d.name == *k) {
                    base_view
                } else {
                    original_builder
                        .joins
                        .iter()
                        .find_map(|j| {
                            self.evaluator.view(&j.alias).and_then(|v| {
                                v.dimensions
                                    .iter()
                                    .any(|d| d.name == *k)
                                    .then_some(j.alias.as_str())
                            })
                        })
                        .unwrap_or(base_view)
                };
                let col_expr = self.resolve_join_key_expr(qualifier, qualifier, k);
                format!("{} AS {}", col_expr, self.dialect.quote_identifier(k))
            })
            .collect();

        // Inject spine_key_parts into the spine's SELECT (right after the
        // user dim list). Done by re-rendering rather than string-patching
        // to keep the formatting consistent.
        let full_select_parts: Vec<String> = dim_select_parts
            .iter()
            .cloned()
            .chain(spine_key_parts.iter().cloned())
            .collect();
        let original_select_block = format!(
            "SELECT DISTINCT\n    {}\n  FROM",
            dim_select_parts.join(",\n    ")
        );
        let new_select_block = format!(
            "SELECT DISTINCT\n    {}\n  FROM",
            full_select_parts.join(",\n    ")
        );
        let dim_spine_sql = dim_spine_sql.replacen(&original_select_block, &new_select_block, 1);
        ctes.push(format!("__dim_spine AS (\n  {}\n)", dim_spine_sql));

        // Build final query. User dims project from the spine; each CTE
        // measure gets wrapped in its outer aggregation (sum/min/max — see
        // the type check inside the CTE loop) so per-key intermediate values
        // roll up to the user dim grain.
        let mut final_select: Vec<String> = dim_aliases
            .iter()
            .map(|a| format!("__dim_spine.{}", self.dialect.quote_identifier(a)))
            .collect();

        for ((cte_name, measure_paths), outer_aggs) in measure_cte_names
            .iter()
            .zip(measures_by_view.values())
            .zip(measure_cte_outer_aggs.iter())
        {
            for (mp, outer_agg) in measure_paths.iter().zip(outer_aggs.iter()) {
                let col_alias = self.member_alias(mp);
                let q = self.dialect.quote_identifier(&col_alias);
                final_select.push(format!("{}({}.{}) AS {}", outer_agg, cte_name, q, q));
            }
        }
        // Direct (non-CTE) measures — the inline path is no longer taken in
        // the fan-out generator, so this is empty in practice. Kept for
        // robustness against future code paths that might re-introduce it.
        final_select.extend(final_select_measures);

        let mut sql = format!(
            "WITH\n{}\nSELECT\n  {}\nFROM\n  __dim_spine",
            ctes.join(",\n"),
            final_select.join(",\n  ")
        );

        // Join measure CTEs to the dimension spine
        for (idx, cte_name) in measure_cte_names.iter().enumerate() {
            let join_keys = &measure_cte_join_keys[idx];
            let conditions: Vec<String> = join_keys
                .iter()
                .map(|k| {
                    format!(
                        "__dim_spine.{} = {}.{}",
                        self.dialect.quote_identifier(k),
                        cte_name,
                        self.dialect.quote_identifier(k)
                    )
                })
                .collect();
            sql.push_str(&format!(
                "\nLEFT JOIN {} ON {}",
                cte_name,
                conditions.join(" AND ")
            ));
        }

        // Outer GROUP BY on the user dim aliases (positional, 1..=N). The
        // outer SELECT wraps each CTE measure in its appropriate aggregation
        // (sum/min/max), so we need exactly one row per user dim combo. If
        // there are no user dims, this is a single-row total — no GROUP BY.
        if !dim_aliases.is_empty() {
            let group_by_indices: Vec<String> =
                (1..=dim_aliases.len()).map(|i| i.to_string()).collect();
            sql.push_str(&format!("\nGROUP BY\n  {}", group_by_indices.join(", ")));
        }

        // ORDER BY
        for order in &request.order {
            let dir = if order.desc { "DESC" } else { "ASC" };
            if let Some(col) = columns.iter().find(|c| c.member == order.id) {
                // First order clause gets ORDER BY, rest get commas
                if sql.contains("\nORDER BY") {
                    sql.push_str(&format!(
                        ", {} {}",
                        self.dialect.quote_identifier(&col.alias),
                        dir
                    ));
                } else {
                    sql.push_str(&format!(
                        "\nORDER BY\n  {} {}",
                        self.dialect.quote_identifier(&col.alias),
                        dir
                    ));
                }
            }
        }

        if let Some(limit) = request.limit {
            sql.push_str(&format!("\nLIMIT {}", limit));
        }
        if let Some(offset) = request.offset {
            sql.push_str(&format!("\nOFFSET {}", offset));
        }

        Ok(QueryResult {
            sql,
            params,
            columns,
            default_limit_applied: false,
        })
    }

    fn validate_members(&self, request: &QueryRequest) -> Result<(), EngineError> {
        for m in &request.measures {
            let (view, name) = self.evaluator.parse_member_path(m)?;
            if self.evaluator.measure(&view, &name).is_none() {
                return Err(EngineError::QueryError(format!(
                    "Measure '{}' not found in view '{}'",
                    name, view
                )));
            }
        }
        for d in &request.dimensions {
            let (view, name) = self.evaluator.parse_member_path(d)?;
            if self.evaluator.dimension(&view, &name).is_none() {
                return Err(EngineError::QueryError(format!(
                    "Dimension '{}' not found in view '{}'",
                    name, view
                )));
            }
        }
        for td in &request.time_dimensions {
            let (view, name) = self.evaluator.parse_member_path(&td.dimension)?;
            if self.evaluator.dimension(&view, &name).is_none() {
                return Err(EngineError::QueryError(format!(
                    "Time dimension '{}' not found in view '{}'",
                    name, view
                )));
            }
        }
        for s in &request.segments {
            let (view, name) = self.evaluator.parse_member_path(s)?;
            if self.evaluator.segment(&view, &name).is_none() {
                return Err(EngineError::QueryError(format!(
                    "Segment '{}' not found in view '{}'",
                    name, view
                )));
            }
        }
        Ok(())
    }

    /// Fan-out path for queries that include at least one non-additive
    /// measure on a multiplied source view. Each measure-owning view gets
    /// its own CTE that joins through the entity chain to the user dim
    /// views and aggregates *directly* at the user-dim grain. The outer
    /// SELECT just stitches the CTEs together — no second aggregation,
    /// no GROUP BY.
    ///
    /// This is the universal correct shape (it also gives correct answers
    /// for additive measures, just less efficiently than the join-key
    /// pre-agg). The all-additive path keeps the smaller CTEs.
    #[allow(clippy::too_many_arguments)]
    fn generate_with_user_grain_ctes(
        &self,
        request: &QueryRequest,
        base_view: &str,
        original_builder: &QueryBuilder,
        measures_by_view: &HashMap<String, Vec<&str>>,
    ) -> Result<QueryResult, EngineError> {
        let mut params: Vec<String> = Vec::new();
        let mut columns: Vec<ColumnMeta> = Vec::new();
        let mut ctes: Vec<String> = Vec::new();

        if !request.time_dimensions.is_empty() {
            return Err(EngineError::SqlGenerationError(
                "User-grain CTE path does not yet support time_dimensions in mixed \
                 non-additive fan-out queries. Query a single source view directly."
                    .into(),
            ));
        }

        // A composite (number/custom) top-level measure whose own expr
        // combines named measures from 2+ distinct views can't be computed
        // inline against its own view's single flat join — that join tree
        // may fan out relative to one of the OTHER referenced views, which
        // would silently multiply every term sharing it. Pull such measures
        // out of the normal per-view grouping entirely: each constituent
        // {{view.measure}} term is instead aggregated in its OWNING view's
        // own CTE (grouped to the same user dims as everything else here),
        // and the composite's final SELECT column is its own expr text with
        // each term substituted for that CTE's column — so every term rolls
        // up at its own correct grain and the composite's arithmetic only
        // combines already-correct scalars.
        let mut view_terms: HashMap<String, Vec<String>> = HashMap::new();
        let mut composite_substitutions: HashMap<String, String> = HashMap::new();
        for (view_name, measure_paths) in measures_by_view {
            for mp in measure_paths {
                let (_, name) = self.evaluator.parse_member_path(mp)?;
                let measure = self
                    .evaluator
                    .measure(view_name, &name)
                    .ok_or_else(|| EngineError::QueryError(format!("Measure not found: {}", mp)))?;

                let Some(terms) = self.composite_measure_needs_isolation(measure) else {
                    let entry = view_terms.entry(view_name.clone()).or_default();
                    if !entry.iter().any(|p| p == mp) {
                        entry.push(mp.to_string());
                    }
                    continue;
                };

                // Safety check: every `{{...}}` ref in the expr must resolve
                // to a measure (not a bare dimension/entity ref). Non-measure
                // cross-view content would have no isolated join context left
                // to resolve against once this measure is pulled out of its
                // view's inline computation. Transparent same-view composites
                // (intermediates whose own exprs only reference other measures)
                // are expanded recursively — they are not required to be leaf
                // terms themselves, so we check is_measure rather than
                // membership in the leaf set.
                let expr = measure.expr.as_deref().unwrap_or("");
                let has_extra_refs =
                    MemberSqlResolver::extract_entity_refs(expr)
                        .iter()
                        .any(|(first, second)| {
                            first != "variables"
                                && !self.evaluator.is_measure(&format!("{}.{}", first, second))
                        });
                if has_extra_refs {
                    let views_str = terms
                        .iter()
                        .map(|(v, _)| v.as_str())
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(EngineError::QueryError(format!(
                        "Measure '{}' combines named measures from multiple views ({}) with \
                         additional cross-view content in the same expression — this cannot be \
                         safely isolated per view (each term would need its own join grain). \
                         Rewrite the expression so every cross-view reference is a plain \
                         {{{{view.measure}}}} reference to an existing measure.",
                        mp, views_str
                    )));
                }

                // Build the substitution map recursively: transparent
                // intermediate composites are inlined (their exprs substituted
                // in place) rather than referenced via a CTE column.
                let leaf_set: HashSet<(String, String)> = terms.iter().cloned().collect();
                let mut sub_stack: HashSet<(String, String)> = HashSet::new();
                let sub_map = self.composite_substitution_map(expr, &leaf_set, &mut sub_stack);
                let substituted = dotted_ref_regex()
                    .replace_all(expr, |caps: &regex::Captures<'_>| {
                        let k = (caps[1].to_string(), caps[2].to_string());
                        sub_map
                            .get(&k)
                            .cloned()
                            .unwrap_or_else(|| caps[0].to_string())
                    })
                    .to_string();
                composite_substitutions.insert(mp.to_string(), substituted);

                for (tv, tn) in &terms {
                    let path = format!("{}.{}", tv, tn);
                    let entry = view_terms.entry(tv.clone()).or_default();
                    if !entry.iter().any(|p| p == &path) {
                        entry.push(path);
                    }
                }
            }
        }

        // For each source view that owns measures (or feeds a composite
        // term), build a CTE at user-dim grain. The CTE's FROM is the source
        // view; it joins through the entity chain to every user-dim view;
        // aggregates each measure; groups by the user dims.
        let mut measure_cte_names: Vec<String> = Vec::new();
        let mut measure_cte_dim_aliases: Vec<Vec<String>> = Vec::new();
        for (view_name, measure_paths) in &view_terms {
            let view = self.evaluator.view(view_name).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not found", view_name))
            })?;

            // Find join path from this source view to every user-dim view,
            // plus any view required by {{view.field}} / {{entity.field}}
            // references inside the members compiled into this CTE — the
            // user dims, this view's measures, and segments/filters (#55).
            let scoped_request = QueryRequest {
                measures: measure_paths.iter().map(|m| m.to_string()).collect(),
                dimensions: request.dimensions.clone(),
                segments: request.segments.clone(),
                filters: request.filters.clone(),
                ..QueryRequest::new()
            };
            // Seed with every view the scoped request names directly
            // (`referenced_views` already walks measures, dimensions,
            // segments, and and/or filter groups), then expand transitively
            // through `{{view.field}}` / `{{entity.field}}` refs inside those
            // members' definitions. Filters and segments name their view
            // directly rather than through a template, so without this
            // direct seed a filter on a view other than the measure's own
            // (e.g. a sibling attached to a shared hub) would compile a
            // WHERE clause against an alias never joined into this CTE.
            let seed_views = scoped_request.referenced_views();
            let user_dim_views: HashSet<String> = self
                .expand_views_for_expr_refs(&scoped_request, &seed_views)
                .into_iter()
                .collect();
            let target_views: Vec<&str> = user_dim_views
                .iter()
                .filter(|v| v.as_str() != view_name.as_str())
                .map(|s| s.as_str())
                .collect();
            let join_edges = if target_views.is_empty() {
                Vec::new()
            } else {
                self.join_graph.find_join_tree_with_hints(
                    view_name,
                    &target_views,
                    &request.through,
                )?
            };

            // This CTE aggregates every measure requested from `view_name` in
            // one SELECT with one shared FROM/JOIN/WHERE. If that join tree
            // fans out (a OneToMany hop — reachable here because a dimension,
            // filter, or segment pulled in a sibling view), an additive
            // measure (SUM/COUNT/MIN/MAX) sharing this CTE with a
            // non-additive one (which is why we're in this function at all)
            // would silently double-count per duplicated row, while the
            // non-additive measure (e.g. COUNT DISTINCT) stays correct —
            // there would be no signal anything was wrong. Refuse instead of
            // guessing; querying the additive measure separately (its own
            // request, routed through the additive fan-out CTE path, which
            // reconciles correctly) is unaffected.
            if join_edges
                .iter()
                .any(|e| e.relationship == JoinRelationship::OneToMany)
            {
                let mut saw_additive = false;
                let mut saw_non_additive = false;
                for mp in measure_paths {
                    let (_, name) = self.evaluator.parse_member_path(mp)?;
                    let measure = self.evaluator.measure(view_name, &name).ok_or_else(|| {
                        EngineError::QueryError(format!("Measure not found: {}", mp))
                    })?;
                    if matches!(
                        measure.measure_type,
                        MeasureType::Sum | MeasureType::Count | MeasureType::Min | MeasureType::Max
                    ) {
                        saw_additive = true;
                    } else {
                        saw_non_additive = true;
                    }
                }
                if saw_additive && saw_non_additive {
                    return Err(EngineError::QueryError(format!(
                        "Cannot combine additive (sum/count/min/max) and non-additive \
                         (avg/count_distinct/median/number/custom/etc.) measures from view '{}' in one \
                         query when a requested dimension, filter, or segment requires a \
                         one-to-many join into that view — the additive measure(s) would be \
                         double-counted by the fan-out. Query them in separate requests.",
                        view_name
                    )));
                }
            }

            // Local view_aliases: source view is the FROM root; joined views
            // are aliased to their own name (matches build_joins behaviour).
            let mut local_aliases: HashMap<String, String> = HashMap::new();
            local_aliases.insert(view_name.to_string(), view_name.to_string());
            for edge in &join_edges {
                local_aliases.insert(edge.to_view.clone(), edge.to_view.clone());
            }
            let joined_view_strs: Vec<&str> =
                join_edges.iter().map(|e| e.to_view.as_str()).collect();
            let entity_to_alias = self
                .evaluator
                .build_entity_to_alias_map(view_name, &joined_view_strs);

            // Resolve user dim projections in the CTE's local context.
            let mut dim_select_parts: Vec<String> = Vec::new();
            let mut dim_aliases: Vec<String> = Vec::new();
            for dim_path in &request.dimensions {
                let (dim_view, dim_name) = self.evaluator.parse_member_path(dim_path)?;
                let dim = self
                    .evaluator
                    .dimension(&dim_view, &dim_name)
                    .ok_or_else(|| {
                        EngineError::QueryError(format!("Dimension not found: {}", dim_path))
                    })?;
                let alias = local_aliases.get(&dim_view).ok_or_else(|| {
                    EngineError::QueryError(format!(
                        "Dimension '{}' is not reachable from source view '{}' via the \
                         entity graph",
                        dim_path, view_name
                    ))
                })?;
                let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);
                let col_alias = self.member_alias(dim_path);
                dim_select_parts.push(format!(
                    "{} AS {}",
                    col_expr,
                    self.dialect.quote_identifier(&col_alias)
                ));
                dim_aliases.push(col_alias);
            }

            // Build measure select parts. Resolve against the CTE's entity
            // map so {{entity.field}} refs in measure exprs hit the views
            // joined above instead of being left unresolved.
            let mut measure_selects: Vec<String> = Vec::new();
            for mp in measure_paths {
                let (_, name) = self.evaluator.parse_member_path(mp)?;
                let measure = self
                    .evaluator
                    .measure(view_name, &name)
                    .ok_or_else(|| EngineError::QueryError(format!("Measure not found: {}", mp)))?;
                let agg_expr = self.measure_agg_expr(view_name, measure, &entity_to_alias)?;
                let col_alias = self.member_alias(mp);
                measure_selects.push(format!(
                    "{} AS {}",
                    agg_expr,
                    self.dialect.quote_identifier(&col_alias)
                ));
            }

            // Apply filters/segments. Use the local aliases so view refs
            // resolve to the joined tables inside this CTE.
            let mut where_clauses: Vec<String> = Vec::new();
            for filter in &request.filters {
                if !self.is_measure_filter(filter) {
                    let sql = self.compile_filter_for_context(
                        filter,
                        &local_aliases,
                        &entity_to_alias,
                        &mut params,
                    )?;
                    if !sql.is_empty() {
                        where_clauses.push(sql);
                    }
                }
            }
            for seg_path in &request.segments {
                let (seg_view, seg_name) = self.evaluator.parse_member_path(seg_path)?;
                let seg = self
                    .evaluator
                    .segment(&seg_view, &seg_name)
                    .ok_or_else(|| {
                        EngineError::QueryError(format!("Segment '{}' not found", seg_path))
                    })?;
                if let Some(alias) = local_aliases.get(&seg_view) {
                    where_clauses.push(self.resolve_expression(alias, &seg.expr, &entity_to_alias));
                }
            }

            // Build the JOIN clauses inside the CTE.
            let mut join_sql = String::new();
            for edge in &join_edges {
                let alias = local_aliases.get(&edge.to_view).unwrap();
                let target_view = self.evaluator.view(&edge.to_view).ok_or_else(|| {
                    EngineError::JoinError(format!("View '{}' not found", edge.to_view))
                })?;
                let table_expr = self.view_source_expr(target_view);
                let conditions: Vec<String> = edge
                    .conditions
                    .iter()
                    .map(|c| {
                        let from_alias = local_aliases
                            .get(&edge.from_view)
                            .cloned()
                            .unwrap_or_else(|| edge.from_view.clone());
                        let from_resolved = self.resolve_join_key_expr(
                            &edge.from_view,
                            &from_alias,
                            &c.from_column,
                        );
                        let to_resolved =
                            self.resolve_join_key_expr(&edge.to_view, alias, &c.to_column);
                        format!("{} = {}", from_resolved, to_resolved)
                    })
                    .collect();
                let join_type = match edge.relationship {
                    JoinRelationship::OneToOne => "INNER",
                    _ => "LEFT",
                };
                join_sql.push_str(&format!(
                    "\n  {} JOIN {} AS {} ON {}",
                    join_type,
                    table_expr,
                    self.dialect.quote_identifier(alias),
                    conditions.join(" AND ")
                ));
            }

            let from_expr = self.view_source_expr(view);
            let all_selects: Vec<String> = dim_select_parts
                .iter()
                .chain(measure_selects.iter())
                .cloned()
                .collect();
            let cte_name = format!("__measures_{}", view_name);
            let group_by: Vec<String> = (1..=dim_select_parts.len())
                .map(|i| i.to_string())
                .collect();
            let where_block = if where_clauses.is_empty() {
                String::new()
            } else {
                format!("\n  WHERE\n    {}", where_clauses.join("\n    AND "))
            };
            let group_block = if group_by.is_empty() {
                String::new()
            } else {
                format!("\n  GROUP BY\n    {}", group_by.join(", "))
            };
            let cte_sql = format!(
                "{} AS (\n  SELECT\n    {}\n  FROM\n    {} AS {}{}{}{}\n)",
                cte_name,
                all_selects.join(",\n    "),
                from_expr,
                self.dialect.quote_identifier(view_name),
                join_sql,
                where_block,
                group_block,
            );
            ctes.push(cte_sql);
            measure_cte_names.push(cte_name);
            measure_cte_dim_aliases.push(dim_aliases);
        }

        // Dim spine: DISTINCT user dims from the original base + its joins
        // — so we get every valid combination even if some sources have no
        // matching rows (LEFT JOIN yields NULL for the missing measures).
        // With no user dims there is nothing to spine over (each measure CTE
        // is a single row), so the spine is skipped entirely and the outer
        // SELECT reads from the first measure CTE instead.
        let mut spine_dim_select_parts: Vec<String> = Vec::new();
        let mut spine_dim_aliases: Vec<String> = Vec::new();
        if !request.dimensions.is_empty() {
            let base = self.evaluator.view(base_view).ok_or_else(|| {
                EngineError::SqlGenerationError(format!("Base view '{}' not found", base_view))
            })?;
            let entity_to_alias = self.evaluator.build_entity_to_alias_map(
                base_view,
                &original_builder
                    .joins
                    .iter()
                    .map(|j| j.alias.as_str())
                    .collect::<Vec<_>>(),
            );
            for dim_path in &request.dimensions {
                let (view_n, name) = self.evaluator.parse_member_path(dim_path)?;
                let dim = self.evaluator.dimension(&view_n, &name).ok_or_else(|| {
                    EngineError::QueryError(format!("Dimension not found: {}", dim_path))
                })?;
                let alias = original_builder.view_aliases.get(&view_n).ok_or_else(|| {
                    EngineError::QueryError(format!("View '{}' not in query", view_n))
                })?;
                let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);
                let col_alias = self.member_alias(dim_path);
                spine_dim_select_parts.push(format!(
                    "{} AS {}",
                    col_expr,
                    self.dialect.quote_identifier(&col_alias)
                ));
                spine_dim_aliases.push(col_alias.clone());
                columns.insert(
                    spine_dim_aliases.len() - 1,
                    ColumnMeta {
                        member: dim_path.clone(),
                        alias: col_alias,
                        kind: ColumnKind::Dimension,
                    },
                );
            }

            let mut spine_sql = format!(
                "SELECT DISTINCT\n    {}\n  FROM\n    {} AS {}",
                spine_dim_select_parts.join(",\n    "),
                self.view_source_expr(base),
                self.dialect.quote_identifier(base_view)
            );
            for join in &original_builder.joins {
                spine_sql.push_str(&format!(
                    "\n  {} JOIN {} AS {} ON {}",
                    join.join_type,
                    join.table_expr,
                    self.dialect.quote_identifier(&join.alias),
                    join.condition
                ));
            }
            // Spine filters (same set the join-key path applies).
            let mut spine_where: Vec<String> = Vec::new();
            for filter in &request.filters {
                if !self.is_measure_filter(filter) {
                    let sql = self.compile_filter_for_context(
                        filter,
                        &original_builder.view_aliases,
                        &entity_to_alias,
                        &mut params,
                    )?;
                    if !sql.is_empty() {
                        spine_where.push(sql);
                    }
                }
            }
            for seg_path in &request.segments {
                let (view_n, name) = self.evaluator.parse_member_path(seg_path)?;
                let seg = self.evaluator.segment(&view_n, &name).ok_or_else(|| {
                    EngineError::QueryError(format!("Segment '{}' not found", seg_path))
                })?;
                if let Some(alias) = original_builder.view_aliases.get(&view_n) {
                    spine_where.push(self.resolve_expression(alias, &seg.expr, &entity_to_alias));
                }
            }
            if !spine_where.is_empty() {
                spine_sql.push_str(&format!(
                    "\n  WHERE\n    {}",
                    spine_where.join("\n    AND ")
                ));
            }
            ctes.push(format!("__dim_spine AS (\n  {}\n)", spine_sql));
        } // end spine (skipped when the query has no dimensions)

        // Outer SELECT: dims from spine + one column per originally requested
        // measure, in request order. A composite pulled out above gets its
        // substituted expr text (each term already at its own correct grain
        // via its own CTE); everything else references its own view's CTE
        // column directly. No GROUP BY: each CTE already aggregates to the
        // user-dim grain.
        let mut final_select: Vec<String> = spine_dim_aliases
            .iter()
            .map(|a| format!("__dim_spine.{}", self.dialect.quote_identifier(a)))
            .collect();
        for mp in &request.measures {
            let col_alias = self.member_alias(mp);
            if let Some(substituted) = composite_substitutions.get(mp) {
                final_select.push(format!(
                    "{} AS {}",
                    substituted,
                    self.dialect.quote_identifier(&col_alias)
                ));
            } else {
                let (view_name, _) = self.evaluator.parse_member_path(mp)?;
                let cte_name = format!("__measures_{}", view_name);
                final_select.push(format!(
                    "{}.{}",
                    cte_name,
                    self.dialect.quote_identifier(&col_alias)
                ));
            }
            columns.push(ColumnMeta {
                member: mp.clone(),
                alias: col_alias,
                kind: ColumnKind::Measure,
            });
        }
        let mut sql = if request.dimensions.is_empty() {
            // No spine: anchor on the first measure CTE (single row each).
            let mut s = format!(
                "WITH\n{}\nSELECT\n  {}\nFROM\n  {}",
                ctes.join(",\n"),
                final_select.join(",\n  "),
                measure_cte_names[0]
            );
            for cte_name in measure_cte_names.iter().skip(1) {
                s.push_str(&format!("\nLEFT JOIN {} ON TRUE", cte_name));
            }
            s
        } else {
            let mut s = format!(
                "WITH\n{}\nSELECT\n  {}\nFROM\n  __dim_spine",
                ctes.join(",\n"),
                final_select.join(",\n  ")
            );
            for (idx, cte_name) in measure_cte_names.iter().enumerate() {
                let dims = &measure_cte_dim_aliases[idx];
                let conditions: Vec<String> = dims
                    .iter()
                    .map(|a| {
                        let q = self.dialect.quote_identifier(a);
                        format!("__dim_spine.{} = {}.{}", q, cte_name, q)
                    })
                    .collect();
                let on_clause = if conditions.is_empty() {
                    "TRUE".to_string()
                } else {
                    conditions.join(" AND ")
                };
                s.push_str(&format!("\nLEFT JOIN {} ON {}", cte_name, on_clause));
            }
            s
        };

        if let Some(limit) = request.limit {
            sql.push_str(&format!("\nLIMIT {}", limit));
        }
        if let Some(offset) = request.offset {
            sql.push_str(&format!("\nOFFSET {}", offset));
        }

        Ok(QueryResult {
            sql,
            params,
            columns,
            default_limit_applied: false,
        })
    }

    /// Expand the query's referenced views with views required by cross-view
    /// references *inside the definitions* of requested members.
    ///
    /// `request.referenced_views()` answers "which views are named in the
    /// query?" — but a requested member's `expr` (or a measure's `filters`)
    /// may contain `{{view.field}}` / `{{entity.field}}` references that the
    /// resolver compiles to another view's alias. Without this expansion the
    /// join planner never includes that view and the generated SQL references
    /// an alias missing from the FROM clause (issue #55).
    ///
    /// Expansion is transitive — an inlined `{{view.dimension}}` substitutes
    /// the target dimension's expr, which may itself reference further views —
    /// and scans only members the query actually uses, so cross-view members
    /// that aren't requested don't force joins. A `visited` set makes cyclic
    /// references terminate.
    fn expand_views_for_expr_refs(
        &self,
        request: &QueryRequest,
        referenced_views: &[String],
    ) -> Vec<String> {
        fn collect_filter_members(filter: &QueryFilter, out: &mut Vec<(String, String)>) {
            if let Some(member) = &filter.member {
                if let Some((view, name)) = member.split_once('.') {
                    out.push((view.to_string(), name.to_string()));
                }
            }
            for nested in filter
                .and
                .iter()
                .flatten()
                .chain(filter.or.iter().flatten())
            {
                collect_filter_members(nested, out);
            }
        }

        let mut views = referenced_views.to_vec();
        let mut seen: HashSet<String> = views.iter().cloned().collect();
        let mut visited: HashSet<(String, String)> = HashSet::new();

        // Seed the worklist with every member the request names
        let mut work: Vec<(String, String)> = Vec::new();
        let member_paths = request
            .measures
            .iter()
            .chain(request.dimensions.iter())
            .chain(request.segments.iter())
            .chain(request.time_dimensions.iter().map(|td| &td.dimension));
        for path in member_paths {
            if let Some((view, name)) = path.split_once('.') {
                work.push((view.to_string(), name.to_string()));
            }
        }
        for filter in &request.filters {
            collect_filter_members(filter, &mut work);
        }

        while let Some((view, member)) = work.pop() {
            if !visited.insert((view.clone(), member.clone())) {
                continue;
            }

            let mut exprs: Vec<&str> = Vec::new();
            if let Some(dim) = self.evaluator.dimension(&view, &member) {
                // sub_query dimensions compile as correlated subqueries, not
                // joins — their cross-view refs must not pull views into the
                // join tree (an unreferenced one-to-many join multiplies rows).
                if dim.sub_query != Some(true) {
                    exprs.push(&dim.expr);
                }
            }
            if let Some(measure) = self.evaluator.measure(&view, &member) {
                if let Some(ref e) = measure.expr {
                    exprs.push(e);
                }
                for f in measure.filters.iter().flatten() {
                    exprs.push(&f.expr);
                }
            }
            if let Some(seg) = self.evaluator.segment(&view, &member) {
                exprs.push(&seg.expr);
            }

            for expr in exprs {
                for (first, second) in MemberSqlResolver::extract_entity_refs(expr) {
                    if first == "variables" {
                        continue;
                    }
                    if self.evaluator.view(&first).is_some() {
                        // {{view.member}} — the target member's expr is inlined
                        // against the view's alias; recurse into its definition
                        if seen.insert(first.clone()) {
                            views.push(first.clone());
                        }
                        work.push((first, second));
                    } else {
                        // {{entity.field}} — resolves to the alias of the view
                        // where the entity is Primary; `field` is a raw column,
                        // so no recursion. Prefer a view already in the set
                        // (matches build_entity_to_alias_map precedence).
                        let mut candidates: Vec<&str> = self
                            .evaluator
                            .all_views()
                            .filter(|v| {
                                v.entities.iter().any(|e| {
                                    e.name == first && e.entity_type == EntityType::Primary
                                })
                            })
                            .map(|v| v.name.as_str())
                            .collect();
                        if candidates.iter().any(|c| seen.contains(*c)) {
                            continue;
                        }
                        candidates.sort_unstable();
                        if let Some(c) = candidates.first() {
                            seen.insert(c.to_string());
                            views.push(c.to_string());
                        }
                    }
                }
            }
        }

        views
    }

    /// Whether a measure's expr (or measure-level filters) reference members
    /// or entities outside its own view. Such measures need their referenced
    /// views joined into whatever context compiles them.
    fn measure_crosses_views(&self, view_name: &str, measure: &Measure) -> bool {
        let mut exprs: Vec<&str> = Vec::new();
        if let Some(ref e) = measure.expr {
            exprs.push(e);
        }
        for f in measure.filters.iter().flatten() {
            exprs.push(&f.expr);
        }
        exprs.into_iter().any(|expr| {
            MemberSqlResolver::extract_entity_refs(expr)
                .into_iter()
                .any(|(first, _)| {
                    if first == "variables" || first == view_name {
                        return false;
                    }
                    if self.evaluator.view(&first).is_some() {
                        return true;
                    }
                    // Entity ref: stays local only when the entity is Primary
                    // on this view (resolves to the view's own alias).
                    !self.evaluator.view(view_name).is_some_and(|v| {
                        v.entities
                            .iter()
                            .any(|e| e.name == first && e.entity_type == EntityType::Primary)
                    })
                })
        })
    }

    /// Returns the transitive set of "leaf" (view, measure_name) pairs for a
    /// composite measure's expr. A leaf is any measure reference that is not
    /// itself a *transparent composite* — a `number`/`custom` measure whose
    /// own expr contains only measure refs (no raw dimension columns). Transparent
    /// intermediates are expanded recursively so that the returned set reflects
    /// the actual grain sources that must each have their own per-view CTE.
    ///
    /// Example: if `orders.net_revenue` expr is
    /// `{{orders.total_order_value}} - {{orders.total_tax_collected}}` and
    /// `total_order_value` is `{{order_items.total_revenue}}`, the leaf terms
    /// are `[(order_items, total_revenue), (orders, total_tax_collected)]` —
    /// not `[(orders, total_order_value), (orders, total_tax_collected)]` as the
    /// old single-level scan returned.
    fn composite_measure_ref_terms(&self, measure: &Measure) -> Vec<(String, String)> {
        let Some(ref expr) = measure.expr else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut on_stack: HashSet<(String, String)> = HashSet::new();
        self.collect_composite_leaf_terms_rec(expr, &mut on_stack, &mut result, &mut seen);
        result
    }

    /// Recursive DFS helper for `composite_measure_ref_terms`. Expands
    /// transparent intermediate composites and accumulates leaf terms.
    /// `on_stack` is the current DFS path for cycle detection; `seen` prevents
    /// duplicate entries in `result` across parallel expansion paths.
    fn collect_composite_leaf_terms_rec(
        &self,
        expr: &str,
        on_stack: &mut HashSet<(String, String)>,
        result: &mut Vec<(String, String)>,
        seen: &mut HashSet<(String, String)>,
    ) {
        for (view, name) in MemberSqlResolver::extract_entity_refs(expr)
            .into_iter()
            .filter(|(f, _)| f != "variables")
            .filter(|(f, s)| self.evaluator.is_measure(&format!("{}.{}", f, s)))
        {
            let key = (view.clone(), name.clone());
            // A transparent composite is a number/custom measure whose expr
            // contains at least one non-variable {{...}} ref and all such
            // refs are measures (not dimension columns). We recurse into
            // these to build the true transitive leaf set rather than
            // treating the intermediate as a monolithic CTE leaf.
            let is_transparent = self.evaluator.measure(&view, &name).is_some_and(|m| {
                matches!(m.measure_type, MeasureType::Number | MeasureType::Custom)
                    && m.expr.as_ref().is_some_and(|e| {
                        let refs = MemberSqlResolver::extract_entity_refs(e);
                        let non_var: Vec<_> =
                            refs.iter().filter(|(f, _)| f != "variables").collect();
                        !non_var.is_empty()
                            && non_var
                                .iter()
                                .all(|(f, s)| self.evaluator.is_measure(&format!("{}.{}", f, s)))
                    })
            });
            if is_transparent && !on_stack.contains(&key) {
                let sub_expr = self
                    .evaluator
                    .measure(&view, &name)
                    .and_then(|m| m.expr.clone());
                if let Some(sub_expr) = sub_expr {
                    on_stack.insert(key.clone());
                    self.collect_composite_leaf_terms_rec(&sub_expr, on_stack, result, seen);
                    on_stack.remove(&key);
                }
            } else if seen.insert(key.clone()) {
                result.push(key);
            }
        }
    }

    /// Pre-compute a token-to-SQL substitution map for a composite measure's
    /// expr, recursively expanding transparent intermediate composites. Each
    /// leaf `(view, name)` in `leaf_terms` is substituted with a reference to
    /// its owning `__measures_{view}` CTE column. Transparent intermediates
    /// (not in `leaf_terms`) have their own exprs recursively substituted
    /// and are inlined directly. `on_stack` guards against cycles.
    fn composite_substitution_map(
        &self,
        expr: &str,
        leaf_terms: &HashSet<(String, String)>,
        on_stack: &mut HashSet<(String, String)>,
    ) -> HashMap<(String, String), String> {
        let mut map: HashMap<(String, String), String> = HashMap::new();
        for (view, name) in MemberSqlResolver::extract_entity_refs(expr) {
            if view == "variables" {
                continue;
            }
            let key = (view.clone(), name.clone());
            if map.contains_key(&key) {
                continue;
            }
            if leaf_terms.contains(&key) {
                let cte_name = format!("__measures_{}", view);
                let col_alias = self.member_alias(&format!("{}.{}", view, name));
                map.insert(
                    key,
                    format!(
                        "({}.{})",
                        cte_name,
                        self.dialect.quote_identifier(&col_alias)
                    ),
                );
            } else if !on_stack.contains(&key) {
                // Transparent intermediate — inline its recursively-substituted expr
                if let Some(ref_measure) = self.evaluator.measure(&view, &name) {
                    if let Some(ref sub_expr) = ref_measure.expr.clone() {
                        on_stack.insert(key.clone());
                        let sub_map =
                            self.composite_substitution_map(sub_expr, leaf_terms, on_stack);
                        on_stack.remove(&key);
                        let substituted_sub = dotted_ref_regex()
                            .replace_all(sub_expr, |caps: &regex::Captures<'_>| {
                                let k = (caps[1].to_string(), caps[2].to_string());
                                sub_map
                                    .get(&k)
                                    .cloned()
                                    .unwrap_or_else(|| caps[0].to_string())
                            })
                            .to_string();
                        map.insert(key, format!("({})", substituted_sub));
                    }
                }
            }
        }
        map
    }

    /// A `number`/`custom` measure whose expr composes named measures from
    /// 2+ distinct views (including an explicit self-reference to its own
    /// view alongside a cross-view one) cannot be evaluated as one inline
    /// expression against a single flat join: if any of those views is
    /// reached via a one-to-many hop relative to another, every OTHER term
    /// sharing that same joined result gets silently multiplied by the
    /// fan-out. Returns the constituent leaf terms (computed transitively
    /// through transparent same-view intermediates — see
    /// `composite_measure_ref_terms`) when isolating each into its own
    /// per-view CTE both applies and is safe — safe meaning the expr
    /// contains no OTHER `{{...}}` content beyond measure refs, since
    /// non-measure cross-view content would have no isolated join context
    /// left to resolve against once split out. `None` means the existing
    /// flat-inline path is used unchanged (single-view composites, and
    /// composites whose transitive leaf views number fewer than two, are
    /// unaffected — e.g. a `SUM(CASE WHEN {{other.flag}} THEN 1 END)` style
    /// measure that only ever touches one join branch).
    fn composite_measure_needs_isolation(
        &self,
        measure: &Measure,
    ) -> Option<Vec<(String, String)>> {
        if !matches!(
            measure.measure_type,
            MeasureType::Number | MeasureType::Custom
        ) {
            return None;
        }
        let terms = self.composite_measure_ref_terms(measure);
        let distinct_views: HashSet<&str> = terms.iter().map(|(v, _)| v.as_str()).collect();
        if distinct_views.len() < 2 {
            return None;
        }
        Some(terms)
    }

    /// Pick the base view by trying all candidates and selecting the one
    /// that produces the shortest total join tree.
    fn pick_base_view(
        &self,
        request: &QueryRequest,
        views: &[String],
    ) -> Result<String, EngineError> {
        if views.len() == 1 {
            return Ok(views[0].clone());
        }

        // Count references per view for tiebreaking. Measures are tracked
        // separately so that, on cost+total-count ties, the view that owns a
        // measure beats a view that only owns dimensions. This matters for
        // induced (promoted) non-additive measures: the source view must be
        // the join base so the single-stage GROUP BY at target grain
        // aggregates source rows *directly*, not via a fan-out CTE that
        // pre-aggregates by an intermediate join key (which would silently
        // average-of-averages for AVG / break for COUNT_DISTINCT, etc.).
        let mut total_counts: HashMap<&str, usize> = HashMap::new();
        let mut measure_counts: HashMap<&str, usize> = HashMap::new();
        for m in &request.measures {
            if let Some(v) = m.split('.').next() {
                *total_counts.entry(v).or_default() += 1;
                *measure_counts.entry(v).or_default() += 1;
            }
        }
        for d in &request.dimensions {
            if let Some(v) = d.split('.').next() {
                *total_counts.entry(v).or_default() += 1;
            }
        }

        let other_views_for = |candidate: &str| -> Vec<&str> {
            views
                .iter()
                .filter(|v| v.as_str() != candidate)
                .map(|v| v.as_str())
                .collect()
        };

        // Try each view as root and pick the one with the shortest join tree.
        // Ranking: cost ↑, then measure_count ↓, then total_count ↓.
        let mut best: Option<(String, usize, usize, usize)> = None; // (view, cost, measure_count, total_count)
        for candidate in views {
            let others = other_views_for(candidate);
            if let Some(cost) = self.join_graph.join_tree_cost(candidate, &others) {
                let m_count = measure_counts.get(candidate.as_str()).copied().unwrap_or(0);
                let t_count = total_counts.get(candidate.as_str()).copied().unwrap_or(0);
                let better = match &best {
                    None => true,
                    Some((_, b_cost, b_m, b_t)) => {
                        cost < *b_cost
                            || (cost == *b_cost && m_count > *b_m)
                            || (cost == *b_cost && m_count == *b_m && t_count > *b_t)
                    }
                };
                if better {
                    best = Some((candidate.clone(), cost, m_count, t_count));
                }
            }
        }

        best.map(|(v, _, _, _)| v).ok_or_else(|| {
            // Fall back to reference count if no join tree is valid
            let fallback = total_counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(name, _)| name.to_string())
                .unwrap_or_else(|| views[0].clone());
            EngineError::QueryError(format!(
                "No valid join tree found; using '{}' as base view",
                fallback
            ))
        })
    }

    fn build_joins(
        &self,
        builder: &mut QueryBuilder,
        base_view: &str,
        target_views: &[&str],
        through: &[String],
    ) -> Result<(), EngineError> {
        let join_edges =
            self.join_graph
                .find_join_tree_with_hints(base_view, target_views, through)?;

        // Detect multiplied views: if a join edge is OneToMany, the source view's rows
        // are duplicated. Track which views get multiplied.
        self.detect_multiplied_views(builder, base_view, &join_edges);

        for edge in &join_edges {
            let alias = edge.to_view.clone();
            builder
                .view_aliases
                .insert(edge.to_view.clone(), alias.clone());

            let target_view = self.evaluator.view(&edge.to_view).ok_or_else(|| {
                EngineError::JoinError(format!("View '{}' not found", edge.to_view))
            })?;

            let table_expr = self.view_source_expr(target_view);

            let conditions: Vec<String> = edge
                .conditions
                .iter()
                .map(|c| {
                    let from_alias = builder
                        .view_aliases
                        .get(&edge.from_view)
                        .unwrap_or(&edge.from_view);

                    // Entity keys are dimension names — resolve them to actual
                    // column expressions via each view's dimension definitions.
                    let from_resolved =
                        self.resolve_join_key_expr(&edge.from_view, from_alias, &c.from_column);
                    let to_resolved =
                        self.resolve_join_key_expr(&edge.to_view, &alias, &c.to_column);

                    format!("{} = {}", from_resolved, to_resolved)
                })
                .collect();

            // Derive join type from relationship
            let join_type = match edge.relationship {
                JoinRelationship::OneToOne => "INNER",
                JoinRelationship::ManyToOne => "LEFT",
                JoinRelationship::OneToMany => "LEFT",
            };

            builder.joins.push(JoinClause {
                join_type: join_type.to_string(),
                table_expr,
                alias,
                condition: conditions.join(" AND "),
                relationship: edge.relationship.clone(),
            });
        }

        Ok(())
    }

    /// Detect which views get their rows multiplied by one-to-many joins.
    ///
    /// Two cases produce inflation that the fan-out CTE path must protect
    /// against:
    ///
    /// 1. **OneToMany from the base (or an ancestor)** — the existing case:
    ///    base joins downward to a child collection, base's rows appear once
    ///    per child row. Mark the "one" side.
    ///
    /// 2. **Chasm trap** — two distinct "many" sides hang off a shared "one"
    ///    hub. After joining all three, each many-row on side A gets paired
    ///    with each many-row on side B *per hub key*. Both sides' measures
    ///    inflate by the other side's row count per fiber. The existing
    ///    case-1 logic catches only the hub (the "one" side), missing both
    ///    "many" siblings — so an induced query like
    ///    `{measures: [stores.from_sales, stores.from_returns]}` would
    ///    silently cartesian-multiply both totals without this fix.
    fn detect_multiplied_views(
        &self,
        builder: &mut QueryBuilder,
        base_view: &str,
        join_edges: &[JoinEdge],
    ) {
        // Case 1 (original): mark the "one" side of every OneToMany edge.
        for edge in join_edges {
            if edge.relationship == JoinRelationship::OneToMany {
                builder.multiplied_views.insert(edge.from_view.clone());
                if edge.from_view == base_view || builder.view_aliases.contains_key(&edge.from_view)
                {
                    builder.multiplied_views.insert(base_view.to_string());
                }
            }
        }

        // Case 2 (chasm trap): group join-tree views by hub. A view V is a
        // "many" attachment to hub H iff there's an edge V→H ManyToOne or
        // H→V OneToMany in the tree. (Both are the same relationship in
        // different directions; `JoinGraph::build` materializes both edges
        // for each Foreign↔Primary pair.) If a hub has 2+ many siblings,
        // they all cross-inflate each other and all need fan-out CTEs.
        use std::collections::{HashMap, HashSet};
        let mut many_at_hub: HashMap<&str, HashSet<&str>> = HashMap::new();
        for edge in join_edges {
            match edge.relationship {
                JoinRelationship::ManyToOne => {
                    many_at_hub
                        .entry(edge.to_view.as_str())
                        .or_default()
                        .insert(edge.from_view.as_str());
                }
                JoinRelationship::OneToMany => {
                    many_at_hub
                        .entry(edge.from_view.as_str())
                        .or_default()
                        .insert(edge.to_view.as_str());
                }
                JoinRelationship::OneToOne => {}
            }
        }
        // The base view is a "many" attachment to whichever hub it joins to
        // via ManyToOne. (It doesn't get its own edge in the tree as a
        // many-child of itself.)
        for edge in join_edges {
            if edge.from_view == base_view && edge.relationship == JoinRelationship::ManyToOne {
                many_at_hub
                    .entry(edge.to_view.as_str())
                    .or_default()
                    .insert(base_view);
            }
        }
        for (_hub, siblings) in many_at_hub {
            if siblings.len() >= 2 {
                for v in siblings {
                    builder.multiplied_views.insert(v.to_string());
                }
            }
        }
    }

    fn add_dimension(
        &self,
        builder: &mut QueryBuilder,
        dim_path: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> Result<(), EngineError> {
        let (view, name) = self.evaluator.parse_member_path(dim_path)?;
        let dim = self
            .evaluator
            .dimension(&view, &name)
            .ok_or_else(|| EngineError::QueryError(format!("Dimension not found: {}", dim_path)))?;

        let alias = builder
            .view_aliases
            .get(&view)
            .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;

        let col_expr = if dim.sub_query.unwrap_or(false) {
            // Subquery dimension: the expr references a measure from a related view.
            // Generate a correlated subquery.
            self.build_subquery_dimension(alias, dim, entity_to_alias)?
        } else {
            self.resolve_expression(alias, &dim.expr, entity_to_alias)
        };
        let col_alias = self.member_alias(dim_path);

        let idx = builder.select_columns.len();
        builder.select_columns.push(SelectColumn {
            expr: col_expr,
            alias: col_alias.clone(),
            is_aggregate: false,
        });
        builder.group_by_indices.push(idx);
        builder.columns.push(ColumnMeta {
            member: dim_path.to_string(),
            alias: col_alias,
            kind: ColumnKind::Dimension,
        });

        Ok(())
    }

    /// Build a correlated subquery for a sub_query dimension.
    /// The dimension's expr should be a measure reference like "{{orders.total_revenue}}"
    /// or a view.measure path like "orders.total_revenue".
    fn build_subquery_dimension(
        &self,
        current_alias: &str,
        dim: &Dimension,
        _entity_to_alias: &HashMap<String, String>,
    ) -> Result<String, EngineError> {
        // Try to parse the expr as a measure reference
        let expr = &dim.expr;

        // Extract measure path — strip {{ }} if present
        let measure_path = if expr.starts_with("{{") && expr.ends_with("}}") {
            expr[2..expr.len() - 2].trim().to_string()
        } else {
            expr.to_string()
        };

        let (target_view, measure_name) = self.evaluator.parse_member_path(&measure_path)?;
        let measure = self
            .evaluator
            .measure(&target_view, &measure_name)
            .ok_or_else(|| {
                EngineError::QueryError(format!(
                    "Subquery dimension references measure '{}' which was not found",
                    measure_path
                ))
            })?;
        let target = self.evaluator.view(&target_view).ok_or_else(|| {
            EngineError::QueryError(format!(
                "View '{}' not found for subquery dimension",
                target_view
            ))
        })?;

        let empty_entity_map = HashMap::new();
        let agg_expr = self.measure_agg_expr(&target_view, measure, &empty_entity_map)?;
        let target_source = self.view_source_expr(target);

        // Find join condition: match entities between current view and target view
        let join_conditions = self.find_subquery_join_conditions(current_alias, &target_view)?;

        Ok(format!(
            "(SELECT {} FROM {} AS {} WHERE {})",
            agg_expr,
            target_source,
            self.dialect.quote_identifier(&target_view),
            join_conditions
        ))
    }

    /// Find the join conditions for a correlated subquery between two views.
    fn find_subquery_join_conditions(
        &self,
        outer_alias: &str,
        inner_view: &str,
    ) -> Result<String, EngineError> {
        // Use the join graph to find edges between the views. `c.from_column`
        // / `c.to_column` are entity key *names*, not necessarily literal
        // columns — resolve each through its own view's dimension
        // definitions rather than quoting them as raw identifiers.
        let edges = self.join_graph.edges_from(outer_alias);
        for edge in &edges {
            if edge.to_view == inner_view {
                let conditions: Vec<String> = edge
                    .conditions
                    .iter()
                    .map(|c| {
                        format!(
                            "{} = {}",
                            self.resolve_join_key_expr(inner_view, inner_view, &c.to_column),
                            self.resolve_join_key_expr(outer_alias, outer_alias, &c.from_column),
                        )
                    })
                    .collect();
                return Ok(conditions.join(" AND "));
            }
        }

        // Try reverse direction
        let edges = self.join_graph.edges_from(inner_view);
        for edge in &edges {
            if edge.to_view == outer_alias {
                let conditions: Vec<String> = edge
                    .conditions
                    .iter()
                    .map(|c| {
                        format!(
                            "{} = {}",
                            self.resolve_join_key_expr(inner_view, inner_view, &c.from_column),
                            self.resolve_join_key_expr(outer_alias, outer_alias, &c.to_column),
                        )
                    })
                    .collect();
                return Ok(conditions.join(" AND "));
            }
        }

        Err(EngineError::JoinError(format!(
            "No join path found between '{}' and '{}' for subquery dimension",
            outer_alias, inner_view
        )))
    }

    fn add_time_dimension(
        &self,
        builder: &mut QueryBuilder,
        td: &TimeDimensionQuery,
        entity_to_alias: &HashMap<String, String>,
        timezone: Option<&str>,
    ) -> Result<(), EngineError> {
        let (view, name) = self.evaluator.parse_member_path(&td.dimension)?;
        let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
            EngineError::QueryError(format!("Time dimension not found: {}", td.dimension))
        })?;

        let alias = builder
            .view_aliases
            .get(&view)
            .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;

        let mut col_expr = self.resolve_expression(alias, &dim.expr, entity_to_alias);

        if let Some(tz) = timezone {
            if tz != "UTC" {
                col_expr = self.dialect.convert_tz(&col_expr, tz);
            }
        }

        // Only include the time column in SELECT/GROUP BY when a granularity
        // is requested.  Without granularity the time dimension is filter-only
        // (the date_range WHERE clause is added separately).
        if let Some(ref granularity) = td.granularity {
            col_expr = self.dialect.date_trunc(granularity, &col_expr);

            let member_path = format!("{}.{}", td.dimension, granularity);
            let col_alias = self.member_alias(&member_path);

            let idx = builder.select_columns.len();
            builder.select_columns.push(SelectColumn {
                expr: col_expr,
                alias: col_alias.clone(),
                is_aggregate: false,
            });
            builder.group_by_indices.push(idx);
            builder.columns.push(ColumnMeta {
                member: member_path,
                alias: col_alias,
                kind: ColumnKind::TimeDimension,
            });
        }

        Ok(())
    }

    fn add_measure(
        &self,
        builder: &mut QueryBuilder,
        measure_path: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> Result<(), EngineError> {
        let (view, name) = self.evaluator.parse_member_path(measure_path)?;
        let measure = self.evaluator.measure(&view, &name).ok_or_else(|| {
            EngineError::QueryError(format!("Measure not found: {}", measure_path))
        })?;

        let alias = builder
            .view_aliases
            .get(&view)
            .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;

        let agg_expr = self.measure_agg_expr(alias, measure, entity_to_alias)?;
        let col_alias = self.member_alias(measure_path);

        builder.select_columns.push(SelectColumn {
            expr: agg_expr,
            alias: col_alias.clone(),
            is_aggregate: true,
        });
        builder.columns.push(ColumnMeta {
            member: measure_path.to_string(),
            alias: col_alias,
            kind: ColumnKind::Measure,
        });

        Ok(())
    }

    /// Build the aggregate expression for a measure.
    fn measure_agg_expr(
        &self,
        view_alias: &str,
        measure: &Measure,
        entity_to_alias: &HashMap<String, String>,
    ) -> Result<String, EngineError> {
        let inner_expr = if let Some(ref expr) = measure.expr {
            self.resolve_expression(view_alias, expr, entity_to_alias)
        } else {
            "*".to_string()
        };

        // Apply measure filters via CASE WHEN
        let has_filters = measure.filters.as_ref().is_some_and(|f| !f.is_empty());
        let filtered_expr = if has_filters {
            let filters = measure.filters.as_ref().unwrap();
            let conditions: Vec<String> = filters
                .iter()
                .map(|f| self.resolve_expression(view_alias, &f.expr, entity_to_alias))
                .collect();
            let condition = conditions.join(" AND ");
            if inner_expr == "*" {
                format!("CASE WHEN {} THEN 1 END", condition)
            } else {
                format!("CASE WHEN {} THEN {} END", condition, inner_expr)
            }
        } else {
            inner_expr
        };

        // Handle rolling window measures — wrap aggregate in a window function
        if let Some(ref rolling) = measure.rolling_window {
            let base_agg =
                self.base_aggregate_expr(view_alias, measure, &filtered_expr, entity_to_alias)?;
            let frame = self.build_window_frame(rolling);
            let window_expr = format!("{} OVER ({})", base_agg, frame);
            // Filtered SUMs return NULL when no rows match the CASE WHEN;
            // COALESCE must wrap the entire window expression (not the inner aggregate)
            // because OVER can only follow aggregate/window functions.
            if has_filters && measure.measure_type == MeasureType::Sum {
                return Ok(format!("COALESCE({}, 0)", window_expr));
            }
            return Ok(window_expr);
        }

        let agg = match measure.measure_type {
            MeasureType::Count => format!("COUNT({})", filtered_expr),
            MeasureType::Sum => {
                let sum = format!("SUM({})", filtered_expr);
                // Filtered SUMs return NULL when no rows match the CASE WHEN;
                // COALESCE to 0 so arithmetic expressions don't propagate NULL.
                // Note: only SUM gets this treatment — COALESCE(AVG/MIN/MAX(...), 0)
                // would be semantically misleading (0 is not a valid average/min/max).
                if has_filters {
                    format!("COALESCE({}, 0)", sum)
                } else {
                    sum
                }
            }
            MeasureType::Average => format!("AVG({})", filtered_expr),
            MeasureType::Min => format!("MIN({})", filtered_expr),
            MeasureType::Max => format!("MAX({})", filtered_expr),
            MeasureType::CountDistinct => {
                format!("COUNT(DISTINCT {})", filtered_expr)
            }
            MeasureType::CountDistinctApprox => self.dialect.count_distinct_approx(&filtered_expr),
            MeasureType::Number => {
                // Pass-through: expression already contains aggregation
                if let Some(ref expr) = measure.expr {
                    self.resolve_expression(view_alias, expr, entity_to_alias)
                } else {
                    return Err(EngineError::SqlGenerationError(
                        "Number measure requires an expr".to_string(),
                    ));
                }
            }
            MeasureType::Median => {
                format!(
                    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {})",
                    filtered_expr
                )
            }
            MeasureType::Custom => {
                if let Some(ref expr) = measure.expr {
                    self.resolve_expression(view_alias, expr, entity_to_alias)
                } else {
                    return Err(EngineError::SqlGenerationError(
                        "Custom measure requires an expr".to_string(),
                    ));
                }
            }
        };

        Ok(agg)
    }

    /// Resolve a join-condition key (`JoinCondition.from_column` /
    /// `.to_column`, sourced from an entity's declared `key:`/`keys:`) to its
    /// actual SQL column expression. Entity keys are DIMENSION NAMES, not
    /// necessarily literal column names — a Primary entity's key commonly
    /// follows a semantic naming convention (e.g. `fruit_id`) distinct from
    /// the underlying physical column (`expr: id`). Resolving through the
    /// dimension, exactly like `build_joins` already does for JOIN
    /// conditions, is required everywhere a `JoinCondition`'s column name is
    /// turned into a real SQL reference — treating it as a literal column
    /// name instead produces a "column does not exist" error (or, if the
    /// dimension-name string happens to coincidentally match some other real
    /// column, a silently wrong one).
    ///
    /// `dimension_view` is the view whose dimension list defines `key`;
    /// `alias` is the SQL alias to qualify the resolved expression with.
    /// They are the same view name at every call site in this codebase
    /// (aliases always equal view names) — kept as separate parameters only
    /// to mirror the from/to view distinction each call site already has.
    fn resolve_join_key_expr(&self, dimension_view: &str, alias: &str, key: &str) -> String {
        let col = self
            .evaluator
            .dimension(dimension_view, key)
            .map(|d| d.expr.as_str())
            .unwrap_or(key);
        self.resolve_expression(alias, col, &HashMap::new())
    }

    /// Unified expression resolver: handles {{TABLE}}, {{entity.field}}, {{view.measure}} references,
    /// and bare column qualification.
    fn resolve_expression(
        &self,
        view_alias: &str,
        expr: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> String {
        // Recursion guard: a cyclic member definition (a member expr that
        // references itself, directly or through a cycle) would recurse forever
        // through resolve_member_refs. Bail out by returning the expr unresolved
        // so the unresolved-ref check reports a clean error instead of crashing.
        let depth = self.resolve_depth.get();
        if depth >= MAX_RESOLVE_DEPTH {
            return expr.to_string();
        }
        self.resolve_depth.set(depth + 1);
        let result = self.resolve_expression_inner(view_alias, expr, entity_to_alias);
        self.resolve_depth.set(depth);
        result
    }

    fn resolve_expression_inner(
        &self,
        view_alias: &str,
        expr: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> String {
        let quote_fn = |s: &str| self.dialect.quote_identifier(s);

        // 0. Expand bare `{{member}}` refs (no view prefix) to `{{view.member}}`
        //    so an expr or measure filter can reference a sibling member by its
        //    bare name — e.g. a filter `{{is_voided}} = false` referencing the
        //    same view's `is_voided` dimension. Dotted refs, {{TABLE}}, and
        //    {{variables.X}} are untouched. Shadowing `expr` keeps the guard
        //    checks below operating on the expanded form.
        let expanded = self.expand_bare_member_refs(expr, view_alias);
        let expr = expanded.as_str();

        // 1. Resolve {{TABLE}} self-references
        let resolved = if MemberSqlResolver::has_table_ref(expr) {
            MemberSqlResolver::resolve_table_ref(expr, view_alias, &quote_fn)
        } else {
            expr.to_string()
        };

        // 2. Resolve {{X.Y}} patterns — could be entity refs or measure-to-measure refs
        let resolved = if MemberSqlResolver::has_entity_refs(&resolved) {
            self.resolve_member_refs(&resolved, view_alias, entity_to_alias)
        } else {
            resolved
        };

        // 3. For simple column names, qualify with view alias
        if is_simple_column_name(&resolved) {
            format!(
                "{}.{}",
                self.dialect.quote_identifier(view_alias),
                self.dialect.quote_identifier(&resolved)
            )
        } else if !MemberSqlResolver::has_entity_refs(expr)
            && !MemberSqlResolver::has_table_ref(expr)
            && !MemberSqlResolver::has_variable_refs(&resolved)
        {
            // 4. Complex expression — qualify bare column refs that match known dimension names
            self.qualify_bare_columns(&resolved, view_alias)
        } else {
            resolved
        }
    }

    /// Rewrite bare `{{member}}` references (a single identifier, no view prefix)
    /// into the fully-qualified `{{view.member}}` form when `member` is a
    /// dimension or measure of the current view. Other single-token braces
    /// (e.g. `{{TABLE}}`, motif params, or an unknown name) are left unchanged
    /// so their own resolvers can handle them. `{{view.member}}` and
    /// `{{variables.X}}` contain a dot and never match the single-token pattern.
    fn expand_bare_member_refs(&self, expr: &str, view_alias: &str) -> String {
        if !expr.contains("{{") {
            return expr.to_string();
        }
        param_ref_regex()
            .replace_all(expr, |caps: &regex::Captures<'_>| {
                let name = &caps[1];
                let path = format!("{}.{}", view_alias, name);
                if self.evaluator.is_dimension(&path) || self.evaluator.is_measure(&path) {
                    format!("{{{{{}.{}}}}}", view_alias, name)
                } else {
                    caps[0].to_string()
                }
            })
            .to_string()
    }

    /// Resolve a dimension's `expr` for use as the left-hand side of a filter
    /// comparison. A bare column resolves to a qualified identifier; a compound
    /// expression — e.g. a boolean dimension whose `expr` is `Holiday_Flag = 1`
    /// — is wrapped in parentheses so `<lhs> <op> <value>` cannot collapse into
    /// an invalid chained predicate like `Holiday_Flag = 1 = 'false'`.
    fn resolve_filter_lhs(
        &self,
        view_alias: &str,
        dim_expr: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> String {
        let resolved = self.resolve_expression(view_alias, dim_expr, entity_to_alias);
        if is_simple_column_name(dim_expr) {
            resolved
        } else {
            format!("({})", resolved)
        }
    }

    /// Resolve {{X.Y}} references that can be either:
    /// - entity references: {{entity_name.field}} -> qualified column
    /// - measure references: {{view_name.measure_name}} -> aggregate expression
    /// - dimension references: {{view_name.dimension_name}} -> dimension expression
    fn resolve_member_refs(
        &self,
        expr: &str,
        current_view_alias: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> String {
        let re = dotted_ref_regex();
        let quote_fn = |s: &str| self.dialect.quote_identifier(s);

        re.replace_all(expr, |caps: &regex::Captures<'_>| {
            let first = &caps[1];
            let second = &caps[2];

            // Skip variable references — they're preserved as-is
            if first == "variables" {
                return format!("{{{{{}.{}}}}}", first, second);
            }

            // Resolve the view alias for member references — use the view name
            // if it exists, otherwise fall back to the current view alias.
            let member_path = format!("{}.{}", first, second);
            let member_alias = if self.evaluator.view(first).is_some() {
                first.to_string()
            } else {
                current_view_alias.to_string()
            };

            // Check if it's a measure reference (view_name.measure_name).
            // Wrap in parentheses to preserve operator precedence when
            // the result is embedded in an outer arithmetic expression
            // (e.g., {{revenue.net_mrr}} * 12 must become (... + ... - ...) * 12).
            if self.evaluator.is_measure(&member_path) {
                if let Some(measure) = self.evaluator.measure(first, second) {
                    let alias = if self.evaluator.view(first).is_some() {
                        first.to_string()
                    } else {
                        current_view_alias.to_string()
                    };
                    if let Ok(agg) = self.measure_agg_expr(&alias, measure, entity_to_alias) {
                        return format!("({})", agg);
                    }
                }
            }

            // Check if it's a dimension reference (view_name.dimension_name).
            // Same parenthesization for precedence: {{view.margin}} * 100
            // where margin = "price - discount" must become (...) * 100.
            if self.evaluator.is_dimension(&member_path) {
                if let Some(dim) = self.evaluator.dimension(first, second) {
                    let resolved =
                        self.resolve_expression(&member_alias, &dim.expr, entity_to_alias);
                    return format!("({})", resolved);
                }
            }

            // Fall back to entity reference resolution
            if let Some(alias) = entity_to_alias.get(first) {
                format!("{}.{}", quote_fn(alias), quote_fn(second))
            } else {
                // Leave unresolved
                format!("{{{{{}.{}}}}}", first, second)
            }
        })
        .to_string()
    }

    /// Qualify bare column name tokens in a complex expression with the view alias.
    /// Handles both unquoted identifiers and double-quoted identifiers.
    /// Does not qualify SQL keywords, function names, or already-qualified references.
    fn qualify_bare_columns(&self, expr: &str, view_alias: &str) -> String {
        // Get dimension names for this view to know which unquoted tokens are columns
        let view = self.evaluator.view(view_alias);
        let dim_names: HashSet<&str> = view
            .map(|v| v.dimensions.iter().map(|d| d.name.as_str()).collect())
            .unwrap_or_default();

        let mut result = String::new();
        let chars: Vec<char> = expr.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            // Skip single-quoted strings (string literals)
            if chars[i] == '\'' {
                result.push(chars[i]);
                i += 1;
                while i < len && chars[i] != '\'' {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < len {
                    result.push(chars[i]);
                    i += 1;
                }
                continue;
            }

            // Handle quoted identifiers — double-quotes (Postgres/DuckDB/etc.)
            // and backticks (MySQL/BigQuery/Databricks)
            if chars[i] == '"' || chars[i] == '`' {
                let quote_char = chars[i];
                let start = i;
                i += 1; // skip opening quote
                let ident_start = i;
                while i < len && chars[i] != quote_char {
                    i += 1;
                }
                let identifier: String = chars[ident_start..i].iter().collect();
                if i < len {
                    i += 1; // skip closing quote
                }

                // Check if preceded by a dot (already qualified like "table"."column")
                let preceded_by_dot = start > 0 && chars[start - 1] == '.';
                // Check if followed by a dot (this is a qualifier itself like "schema"."col")
                let followed_by_dot = i < len && chars[i] == '.';

                if !preceded_by_dot && !followed_by_dot {
                    // Qualify this quoted identifier with the view alias
                    result.push_str(&format!(
                        "{}.{}",
                        self.dialect.quote_identifier(view_alias),
                        self.dialect.quote_identifier(&identifier)
                    ));
                } else {
                    // Already qualified, keep as-is
                    result.push_str(&self.dialect.quote_identifier(&identifier));
                }
                continue;
            }

            // Check for unquoted identifier tokens
            if chars[i].is_alphabetic() || chars[i] == '_' {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let token: String = chars[start..i].iter().collect();

                // Check if preceded by a dot (already qualified)
                let preceded_by_dot = start > 0 && chars[start - 1] == '.';
                // Check if followed by '(' (function call)
                let followed_by_paren = i < len && chars[i] == '(';

                if !preceded_by_dot && !followed_by_paren && dim_names.contains(token.as_str()) {
                    result.push_str(&format!(
                        "{}.{}",
                        self.dialect.quote_identifier(view_alias),
                        self.dialect.quote_identifier(&token)
                    ));
                } else {
                    result.push_str(&token);
                }
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }

        result
    }

    /// Build the base aggregate expression (without window frame) for rolling window measures.
    fn base_aggregate_expr(
        &self,
        view_alias: &str,
        measure: &Measure,
        filtered_expr: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> Result<String, EngineError> {
        Ok(match measure.measure_type {
            MeasureType::Count => format!("COUNT({})", filtered_expr),
            // Raw SUM here — when this aggregate is wrapped with OVER, the caller adds
            // the outer COALESCE around the whole window expression (OVER cannot follow
            // a COALESCE).
            MeasureType::Sum => format!("SUM({})", filtered_expr),
            MeasureType::Average => format!("AVG({})", filtered_expr),
            MeasureType::Min => format!("MIN({})", filtered_expr),
            MeasureType::Max => format!("MAX({})", filtered_expr),
            MeasureType::CountDistinct => format!("COUNT(DISTINCT {})", filtered_expr),
            MeasureType::CountDistinctApprox => self.dialect.count_distinct_approx(filtered_expr),
            MeasureType::Custom | MeasureType::Number => {
                if let Some(ref expr) = measure.expr {
                    self.resolve_expression(view_alias, expr, entity_to_alias)
                } else {
                    return Err(EngineError::SqlGenerationError(
                        "Pass-through measure requires an expr".to_string(),
                    ));
                }
            }
            MeasureType::Median => format!(
                "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {})",
                filtered_expr
            ),
        })
    }

    /// Build a SQL window frame clause from a RollingWindow config.
    fn build_window_frame(&self, rw: &RollingWindow) -> String {
        let trailing = rw.trailing.as_deref().unwrap_or("unbounded");
        let leading = rw.leading.as_deref().unwrap_or("current row");

        let start = if trailing == "unbounded" {
            "UNBOUNDED PRECEDING".to_string()
        } else if trailing == "current row" {
            "CURRENT ROW".to_string()
        } else {
            // Parse "N days/rows" etc.
            format!("{} PRECEDING", parse_window_interval(trailing))
        };

        let end = if leading == "unbounded" {
            "UNBOUNDED FOLLOWING".to_string()
        } else if leading == "current row" {
            "CURRENT ROW".to_string()
        } else {
            format!("{} FOLLOWING", parse_window_interval(leading))
        };

        format!("ORDER BY 1 ROWS BETWEEN {} AND {}", start, end)
    }

    /// Check if a filter targets a measure (should go to HAVING).
    fn is_measure_filter(&self, filter: &QueryFilter) -> bool {
        if let Some(ref member) = filter.member {
            return self.evaluator.is_measure(member);
        }
        if let Some(ref and_filters) = filter.and {
            return and_filters.iter().all(|f| self.is_measure_filter(f));
        }
        if let Some(ref or_filters) = filter.or {
            return or_filters.iter().all(|f| self.is_measure_filter(f));
        }
        false
    }

    fn compile_filter(
        &self,
        filter: &QueryFilter,
        builder: &mut QueryBuilder,
        entity_to_alias: &HashMap<String, String>,
    ) -> Result<String, EngineError> {
        // Handle AND/OR groups
        if let Some(ref and_filters) = filter.and {
            let parts: Result<Vec<String>, _> = and_filters
                .iter()
                .map(|f| self.compile_filter(f, builder, entity_to_alias))
                .collect();
            let parts = parts?;
            let non_empty: Vec<&str> = parts
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_str())
                .collect();
            return Ok(if non_empty.len() > 1 {
                format!("({})", non_empty.join(" AND "))
            } else {
                non_empty.first().map(|s| s.to_string()).unwrap_or_default()
            });
        }

        if let Some(ref or_filters) = filter.or {
            let parts: Result<Vec<String>, _> = or_filters
                .iter()
                .map(|f| self.compile_filter(f, builder, entity_to_alias))
                .collect();
            let parts = parts?;
            let non_empty: Vec<&str> = parts
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_str())
                .collect();
            return Ok(if non_empty.len() > 1 {
                format!("({})", non_empty.join(" OR "))
            } else {
                non_empty.first().map(|s| s.to_string()).unwrap_or_default()
            });
        }

        // Single filter
        let member = filter
            .member
            .as_ref()
            .ok_or_else(|| EngineError::QueryError("Filter must have a member".to_string()))?;
        let operator = filter
            .operator
            .as_ref()
            .ok_or_else(|| EngineError::QueryError("Filter must have an operator".to_string()))?;

        let (view, name) = self.evaluator.parse_member_path(member)?;
        let alias = builder
            .view_aliases
            .get(&view)
            .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;

        // Determine the column expression based on member type
        let col_expr = if self.evaluator.is_measure(member) {
            // Measure filter: use the aggregate expression
            let measure = self.evaluator.measure(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Measure '{}' not found", member))
            })?;
            self.measure_agg_expr(alias, measure, entity_to_alias)?
        } else {
            // Dimension filter: use the dimension expression
            let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
                EngineError::QueryError(format!("Filter member '{}' not found", member))
            })?;
            let col_expr = self.resolve_filter_lhs(alias, &dim.expr, entity_to_alias);
            // Boolean dimensions: render the comparison as an inline typed
            // literal (`(<expr>) = false`) rather than a string param — consumers
            // single-quote params into the invalid `(<expr>) = 'false'`.
            if dim.dimension_type == DimensionType::Boolean {
                if let Some(cond) = Self::try_boolean_filter(&col_expr, operator, &filter.values) {
                    return Ok(cond);
                }
            }
            col_expr
        };

        self.compile_filter_operator(&col_expr, operator, &filter.values, builder)
    }

    /// Compile a filter in a standalone context (for fan-out protection CTEs).
    fn compile_filter_for_context(
        &self,
        filter: &QueryFilter,
        view_aliases: &HashMap<String, String>,
        entity_to_alias: &HashMap<String, String>,
        params: &mut Vec<String>,
    ) -> Result<String, EngineError> {
        if let Some(ref and_filters) = filter.and {
            let parts: Result<Vec<String>, _> = and_filters
                .iter()
                .map(|f| self.compile_filter_for_context(f, view_aliases, entity_to_alias, params))
                .collect();
            let parts = parts?;
            let non_empty: Vec<&str> = parts
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_str())
                .collect();
            return Ok(if non_empty.len() > 1 {
                format!("({})", non_empty.join(" AND "))
            } else {
                non_empty.first().map(|s| s.to_string()).unwrap_or_default()
            });
        }
        if let Some(ref or_filters) = filter.or {
            let parts: Result<Vec<String>, _> = or_filters
                .iter()
                .map(|f| self.compile_filter_for_context(f, view_aliases, entity_to_alias, params))
                .collect();
            let parts = parts?;
            let non_empty: Vec<&str> = parts
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_str())
                .collect();
            return Ok(if non_empty.len() > 1 {
                format!("({})", non_empty.join(" OR "))
            } else {
                non_empty.first().map(|s| s.to_string()).unwrap_or_default()
            });
        }

        let member = filter
            .member
            .as_ref()
            .ok_or_else(|| EngineError::QueryError("Filter must have a member".to_string()))?;
        let operator = filter
            .operator
            .as_ref()
            .ok_or_else(|| EngineError::QueryError("Filter must have an operator".to_string()))?;

        let (view, name) = self.evaluator.parse_member_path(member)?;
        let alias = view_aliases
            .get(&view)
            .ok_or_else(|| EngineError::QueryError(format!("View '{}' not in query", view)))?;
        let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
            EngineError::QueryError(format!("Filter member '{}' not found", member))
        })?;
        let col_expr = self.resolve_filter_lhs(alias, &dim.expr, entity_to_alias);

        if dim.dimension_type == DimensionType::Boolean {
            if let Some(cond) = Self::try_boolean_filter(&col_expr, operator, &filter.values) {
                return Ok(cond);
            }
        }

        // Use parameterized values
        self.compile_filter_operator_parameterized(&col_expr, operator, &filter.values, params)
    }

    /// Render a boolean-dimension equality/inequality as an inline typed literal
    /// instead of a parameter. airlayer's `params` are a type-erased
    /// `Vec<String>`; consumers single-quote them when inlining, turning a
    /// boolean comparison into the invalid `(<expr>) = 'false'`. Only the
    /// canonical boolean tokens are inlined (never arbitrary input), so there is
    /// no injection surface — anything else returns `None` and falls back to the
    /// parameterized path.
    fn try_boolean_filter(col: &str, op: &FilterOperator, values: &[String]) -> Option<String> {
        if values.len() != 1 {
            return None;
        }
        let literal = match values[0].trim().to_ascii_lowercase().as_str() {
            "true" | "1" => "true",
            "false" | "0" => "false",
            _ => return None,
        };
        match op {
            FilterOperator::Equals => Some(format!("{} = {}", col, literal)),
            FilterOperator::NotEquals => Some(format!("{} <> {}", col, literal)),
            _ => None,
        }
    }

    /// Compile a filter operator using parameterized values.
    fn compile_filter_operator(
        &self,
        col: &str,
        op: &FilterOperator,
        values: &[String],
        builder: &mut QueryBuilder,
    ) -> Result<String, EngineError> {
        self.compile_filter_operator_parameterized(col, op, values, &mut builder.params)
    }

    /// Allocate a param placeholder for a value.
    fn alloc_param(&self, value: &str, params: &mut Vec<String>) -> String {
        let idx = params.len();
        params.push(value.to_string());
        self.dialect.param_placeholder(idx)
    }

    fn compile_filter_operator_parameterized(
        &self,
        col: &str,
        op: &FilterOperator,
        values: &[String],
        params: &mut Vec<String>,
    ) -> Result<String, EngineError> {
        match op {
            FilterOperator::Equals => {
                if values.len() == 1 {
                    let p = self.alloc_param(&values[0], params);
                    Ok(format!("{} = {}", col, p))
                } else {
                    let placeholders: Vec<String> =
                        values.iter().map(|v| self.alloc_param(v, params)).collect();
                    Ok(format!("{} IN ({})", col, placeholders.join(", ")))
                }
            }
            FilterOperator::NotEquals => {
                if values.len() == 1 {
                    let p = self.alloc_param(&values[0], params);
                    Ok(format!("{} <> {}", col, p))
                } else {
                    let placeholders: Vec<String> =
                        values.iter().map(|v| self.alloc_param(v, params)).collect();
                    Ok(format!("{} NOT IN ({})", col, placeholders.join(", ")))
                }
            }
            FilterOperator::Contains => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("%{}%", v), params);
                        format!("{} LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotContains => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("%{}%", v), params);
                        format!("{} NOT LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::StartsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("{}%", v), params);
                        format!("{} LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotStartsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("{}%", v), params);
                        format!("{} NOT LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::EndsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("%{}", v), params);
                        format!("{} LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotEndsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.alloc_param(&format!("%{}", v), params);
                        format!("{} NOT LIKE {}", col, p)
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::Gt => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} > {}", col, p))
            }
            FilterOperator::Gte => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} >= {}", col, p))
            }
            FilterOperator::Lt => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} < {}", col, p))
            }
            FilterOperator::Lte => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} <= {}", col, p))
            }
            FilterOperator::Set => Ok(format!("{} IS NOT NULL", col)),
            FilterOperator::NotSet => Ok(format!("{} IS NULL", col)),
            FilterOperator::InDateRange => {
                if values.len() == 2 {
                    let p0 = self.alloc_param(&values[0], params);
                    let p1 = self.alloc_param(&values[1], params);
                    Ok(format!("{} >= {} AND {} <= {}", col, p0, col, p1))
                } else {
                    Err(EngineError::QueryError(
                        "inDateRange requires exactly 2 values".to_string(),
                    ))
                }
            }
            FilterOperator::NotInDateRange => {
                if values.len() == 2 {
                    let p0 = self.alloc_param(&values[0], params);
                    let p1 = self.alloc_param(&values[1], params);
                    Ok(format!("({} < {} OR {} > {})", col, p0, col, p1))
                } else {
                    Err(EngineError::QueryError(
                        "notInDateRange requires exactly 2 values".to_string(),
                    ))
                }
            }
            FilterOperator::BeforeDate => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} < {}", col, p))
            }
            FilterOperator::BeforeOrOnDate => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} <= {}", col, p))
            }
            FilterOperator::AfterDate => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} > {}", col, p))
            }
            FilterOperator::AfterOrOnDate => {
                let p = self.alloc_param(&values[0], params);
                Ok(format!("{} >= {}", col, p))
            }
            FilterOperator::OnTheDate => {
                // Expand to date range for the full day
                let date = &values[0];
                let next_day = if let Ok(d) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
                    (d + chrono::Duration::days(1))
                        .format("%Y-%m-%d")
                        .to_string()
                } else {
                    // If not parseable, just use the date as-is for both bounds
                    date.clone()
                };
                let p0 = self.alloc_param(date, params);
                let p1 = self.alloc_param(&next_day, params);
                Ok(format!("{} >= {} AND {} < {}", col, p0, col, p1))
            }
        }
    }

    /// Build the full SQL string from the builder state.
    fn assemble_sql(
        &self,
        builder: &QueryBuilder,
        request: &QueryRequest,
    ) -> Result<String, EngineError> {
        let mut sql = String::new();

        // SELECT
        sql.push_str("SELECT\n");
        let select_parts: Vec<String> = builder
            .select_columns
            .iter()
            .map(|col| {
                format!(
                    "  {} AS {}",
                    col.expr,
                    self.dialect.quote_identifier(&col.alias)
                )
            })
            .collect();
        sql.push_str(&select_parts.join(",\n"));

        // FROM
        let base = self.evaluator.view(&builder.base_view).ok_or_else(|| {
            EngineError::SqlGenerationError(format!("Base view '{}' not found", builder.base_view))
        })?;
        let from_expr = self.view_source_expr(base);
        sql.push_str(&format!(
            "\nFROM\n  {} AS {}",
            from_expr,
            self.dialect.quote_identifier(&builder.base_view)
        ));

        // JOINs
        for join in &builder.joins {
            sql.push_str(&format!(
                "\n{} JOIN {} AS {} ON {}",
                join.join_type,
                join.table_expr,
                self.dialect.quote_identifier(&join.alias),
                join.condition
            ));
        }

        // WHERE
        if !builder.where_conditions.is_empty() {
            sql.push_str("\nWHERE\n  ");
            sql.push_str(&builder.where_conditions.join("\n  AND "));
        }

        // GROUP BY (only if there are aggregates and dimensions)
        if !builder.group_by_indices.is_empty()
            && builder.select_columns.iter().any(|c| c.is_aggregate)
            && !request.ungrouped
        {
            let group_refs: Vec<String> = builder
                .group_by_indices
                .iter()
                .map(|&idx| (idx + 1).to_string())
                .collect();
            sql.push_str(&format!("\nGROUP BY\n  {}", group_refs.join(", ")));
        }

        // HAVING
        if !builder.having_conditions.is_empty() {
            sql.push_str("\nHAVING\n  ");
            sql.push_str(&builder.having_conditions.join("\n  AND "));
        }

        // ORDER BY
        if !builder.order_by.is_empty() {
            sql.push_str(&format!("\nORDER BY\n  {}", builder.order_by.join(", ")));
        }

        // LIMIT
        if let Some(limit) = request.limit {
            sql.push_str(&format!("\nLIMIT {}", limit));
        }

        // OFFSET
        if let Some(offset) = request.offset {
            sql.push_str(&format!("\nOFFSET {}", offset));
        }

        Ok(sql)
    }

    /// Get the FROM expression for a view (table name or subquery).
    fn view_source_expr(&self, view: &View) -> String {
        if let Some(ref table) = view.table {
            self.quote_table_name(table)
        } else if let Some(ref sql) = view.sql {
            format!("(\n  {}\n)", sql)
        } else {
            view.name.clone()
        }
    }

    /// Quote a table name if it contains characters that require quoting.
    /// Built for Domo (where table names are UUIDs with hyphens), but handles
    /// the general case. Should not produce exceptions for other dialects since
    /// it only quotes names with special characters, but given this wasn't
    /// explicitly built for other DBs, be wary of unexpected behavior here.
    /// Handles schema-qualified names (e.g. "schema.table") by quoting each part.
    /// Leaves simple identifiers (alphanumeric + underscore) unquoted.
    fn quote_table_name(&self, table: &str) -> String {
        // If already quoted (starts with " or `), pass through as-is
        if table.starts_with('"') || table.starts_with('`') {
            return table.to_string();
        }
        let needs_quoting = |s: &str| {
            s.is_empty()
                || s.chars().next().is_some_and(|c| c.is_ascii_digit())
                || s.chars().any(|c| !c.is_alphanumeric() && c != '_')
        };
        let parts: Vec<&str> = table.split('.').collect();
        parts
            .iter()
            .map(|part| {
                if needs_quoting(part) {
                    self.dialect.quote_identifier(part)
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Generate a column alias from a member path.
    fn member_alias(&self, path: &str) -> String {
        path.replace('.', "__")
    }

    // ── Shift (time-shifted comparison) compilation ────────────────────────
    //
    // A `shift` measure re-evaluates a base measure over a window obtained by
    // shifting the query's current time window, optionally restricted to a
    // lifespan-derived cohort. Same-store sales is the proving case:
    //   same_store_sales = net_sales / net_sales_prior - 1
    // where `net_sales_prior` is `shift { measure: net_sales, by: 1 year, prior,
    // comparable_by: store_id }`.
    //
    // Lowering (three CTE stages):
    //   __shift_base    — base measures grouped by dims + time bucket, over the
    //                     EXPANDED scan window, with the cohort predicate applied
    //                     here (before the shift) so both windows see the same
    //                     entity set. (cohort-before-shift invariant)
    //   __shift_aligned — self-join of __shift_base on a shifted time key, so each
    //                     current bucket gets its prior bucket's value aligned as a
    //                     column. A self-join (not LAG) tolerates missing periods.
    //   outer SELECT    — ratio / compound measures over the aligned columns.

    /// Does this query select any shift-derived measure (a `shift` measure, or a
    /// composite measure that transitively references one)?
    fn query_uses_shift(&self, request: &QueryRequest) -> bool {
        request.measures.iter().any(|m| {
            if let Ok((view, name)) = self.evaluator.parse_member_path(m) {
                self.measure_is_shift_derived(&view, &name, &mut HashSet::new())
            } else {
                false
            }
        })
    }

    /// True if `view.name` is a `shift` measure or a `number`/`custom` measure
    /// whose expression transitively references a shift measure.
    fn measure_is_shift_derived(
        &self,
        view: &str,
        name: &str,
        visited: &mut HashSet<String>,
    ) -> bool {
        let key = format!("{}.{}", view, name);
        if !visited.insert(key) {
            return false;
        }
        let Some(measure) = self.evaluator.measure(view, name) else {
            return false;
        };
        if measure.shift.is_some() {
            return true;
        }
        if let Some(ref expr) = measure.expr {
            for cap in dotted_ref_regex().captures_iter(expr) {
                let (ref_view, ref_name) = (&cap[1], &cap[2]);
                if ref_view == "variables" {
                    continue;
                }
                if self
                    .evaluator
                    .is_measure(&format!("{}.{}", ref_view, ref_name))
                    && self.measure_is_shift_derived(ref_view, ref_name, visited)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Find a lifespan declaration for `entity_name` across all views.
    /// Returns the declaring view's name and the lifespan.
    fn find_lifespan(&self, entity_name: &str) -> Option<(String, Lifespan)> {
        for view in self.evaluator.all_views() {
            for entity in &view.entities {
                if entity.name == entity_name {
                    if let Some(ref ls) = entity.lifespan {
                        return Some((view.name.clone(), ls.clone()));
                    }
                }
            }
        }
        None
    }

    /// Compile a query that selects one or more shift-derived measures.
    fn generate_shift(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        use crate::engine::shift::{parse_iso_date, Interval};

        // SQLite has no date_trunc (and no interval arithmetic), so the time
        // bucketing the shift lowering depends on cannot be expressed. Fail with a
        // clear message rather than emitting SQL SQLite will reject.
        if *self.dialect == Dialect::SQLite {
            return Err(EngineError::QueryError(
                "shift (comparison) measures are not supported on SQLite: it has no date_trunc \
                 or interval arithmetic for the required time bucketing"
                    .to_string(),
            ));
        }

        // 1. Identify the shift measures used and the single fact view that owns
        //    them. Collect their (by, direction) so they can be unified.
        let mut fact_view: Option<String> = None;
        let mut shift_specs: Vec<(String, Shift)> = Vec::new(); // (measure name, shift)
        self.collect_shift_measures(request, &mut fact_view, &mut shift_specs)?;
        let fact_view = fact_view.ok_or_else(|| {
            EngineError::QueryError("shift query references no shift measure".to_string())
        })?;

        // 2. Unify shift configuration: all shifts in one query must agree on the
        //    interval and direction (a single self-join aligns one shifted key).
        let first = &shift_specs[0].1;
        let interval = Interval::parse(&first.by).map_err(EngineError::QueryError)?;
        let direction = first.direction.clone();
        for (mname, s) in &shift_specs {
            let i = Interval::parse(&s.by).map_err(EngineError::QueryError)?;
            if i != interval || s.direction != direction {
                return Err(EngineError::QueryError(format!(
                    "shift measure '{}' uses a different by/direction than another shift in the \
                     same query; mixing shift windows in one query is not supported (TODO)",
                    mname
                )));
            }
        }

        // Cohort: enforced if any used shift names a `comparable_by` entity. All
        // cohort shifts in one query must agree on that entity and on maturity.
        let cohort_shifts: Vec<&(String, Shift)> = shift_specs
            .iter()
            .filter(|(_, s)| s.comparable_by.is_some())
            .collect();
        let cohort_required = !cohort_shifts.is_empty();
        let (cohort_entity, maturity) = if cohort_required {
            let entity0 = cohort_shifts[0].1.comparable_by.clone();
            let m0 = cohort_shifts[0].1.maturity.clone();
            for (mname, s) in &cohort_shifts {
                if s.comparable_by != entity0 {
                    return Err(EngineError::QueryError(format!(
                        "shift measure '{}' is `comparable_by` a different entity than another \
                         cohort shift in the same query; they must agree",
                        mname
                    )));
                }
                if s.maturity != m0 {
                    return Err(EngineError::QueryError(format!(
                        "shift measure '{}' declares a different `maturity` than another cohort \
                         shift in the same query; they must agree",
                        mname
                    )));
                }
            }
            let maturity = match m0 {
                Some(ref s) => Some(Interval::parse(s).map_err(EngineError::QueryError)?),
                None => None,
            };
            (entity0, maturity)
        } else {
            (None, None)
        };

        // 3. Require a current time window: exactly one time dimension on the fact
        //    view with a resolved [start, end] date range.
        let td = request.time_dimensions.first().ok_or_else(|| {
            EngineError::QueryError(
                "a shift measure needs a time window: add a time_dimension with a date_range \
                 (there is no current window to shift from)"
                    .to_string(),
            )
        })?;
        let date_range = td
            .resolved_date_range()
            .filter(|r| r.len() == 2)
            .ok_or_else(|| {
                EngineError::QueryError(format!(
                    "shift measure requires a date_range on time dimension '{}'",
                    td.dimension
                ))
            })?;
        let c_start = parse_iso_date(&date_range[0]).map_err(EngineError::QueryError)?;
        let c_end = parse_iso_date(&date_range[1]).map_err(EngineError::QueryError)?;

        // Bucket granularity: the query's granularity, else derived from the unit.
        // The bucket must evenly divide the shift interval, or the shifted
        // self-join key lands off the bucket grid and silently matches no rows.
        let granularity = td
            .granularity
            .clone()
            .unwrap_or_else(|| interval.unit.default_granularity().to_string());
        interval
            .check_commensurable(&granularity)
            .map_err(EngineError::QueryError)?;

        // 4. Compute shifted window + expanded scan window as date literals.
        let (scan_start, scan_end) = match direction {
            ShiftDirection::Prior => (interval.subtract_from(c_start), c_end),
            ShiftDirection::Next => (c_start, interval.add_to(c_end)),
        };

        // 5. Cohort context (lifespan view/columns + the start-of-life cutoff).
        let cohort = match cohort_entity {
            Some(entity) => Some(self.build_cohort_context(
                &fact_view, &entity, &interval, &direction, maturity, c_start, c_end,
            )?),
            None => None,
        };

        // 6. Determine the base measures needed in the inner stage (the bases of
        //    every shift used, plus any plain fact measures referenced directly or
        //    by composite measures).
        let inner_bases = self.collect_inner_base_measures(request, &fact_view, &shift_specs)?;

        // 7. Build the inner stage (grouped base aggregation over the scan window).
        let inner = self.build_shift_inner_stage(
            request,
            &fact_view,
            td,
            &granularity,
            &inner_bases,
            &scan_start.format("%Y-%m-%d").to_string(),
            &scan_end.format("%Y-%m-%d").to_string(),
            cohort.as_ref(),
        )?;

        // 8. Assemble the aligned + outer stages around the inner SQL.
        self.assemble_shift_sql(
            request,
            &fact_view,
            td,
            &granularity,
            &interval,
            &direction,
            &inner,
            &c_start,
            &c_end,
        )
    }

    /// Populate `fact_view` and `shift_specs` with the shift measures the query
    /// references (transitively through composite measures).
    fn collect_shift_measures(
        &self,
        request: &QueryRequest,
        fact_view: &mut Option<String>,
        shift_specs: &mut Vec<(String, Shift)>,
    ) -> Result<(), EngineError> {
        let mut seen: HashSet<String> = HashSet::new();
        for m in &request.measures {
            let (view, name) = self.evaluator.parse_member_path(m)?;
            self.walk_shift_measures(&view, &name, fact_view, shift_specs, &mut seen)?;
        }
        if shift_specs.is_empty() {
            return Err(EngineError::QueryError(
                "internal: generate_shift called without a shift measure".to_string(),
            ));
        }
        Ok(())
    }

    fn walk_shift_measures(
        &self,
        view: &str,
        name: &str,
        fact_view: &mut Option<String>,
        shift_specs: &mut Vec<(String, Shift)>,
        seen: &mut HashSet<String>,
    ) -> Result<(), EngineError> {
        let key = format!("{}.{}", view, name);
        if !seen.insert(key) {
            return Ok(());
        }
        let Some(measure) = self.evaluator.measure(view, name) else {
            return Ok(());
        };
        if let Some(ref shift) = measure.shift {
            match fact_view {
                Some(fv) if fv != view => {
                    return Err(EngineError::QueryError(format!(
                        "shift measures span multiple views ('{}' and '{}'); a single shift query \
                         must stay within one fact view",
                        fv, view
                    )));
                }
                _ => *fact_view = Some(view.to_string()),
            }
            shift_specs.push((name.to_string(), shift.clone()));
            return Ok(());
        }
        if let Some(ref expr) = measure.expr {
            for cap in dotted_ref_regex().captures_iter(expr) {
                let (rv, rn) = (cap[1].to_string(), cap[2].to_string());
                if rv == "variables" {
                    continue;
                }
                if self.evaluator.is_measure(&format!("{}.{}", rv, rn)) {
                    self.walk_shift_measures(&rv, &rn, fact_view, shift_specs, seen)?;
                }
            }
        }
        Ok(())
    }

    /// Resolve the set of plain base-measure names (in the fact view) that the
    /// inner stage must aggregate: the base of every shift, plus any plain
    /// fact-view measures referenced directly or through composite measures.
    fn collect_inner_base_measures(
        &self,
        request: &QueryRequest,
        fact_view: &str,
        shift_specs: &[(String, Shift)],
    ) -> Result<Vec<String>, EngineError> {
        let mut bases: Vec<String> = Vec::new();
        let push = |name: &str, bases: &mut Vec<String>| {
            if !bases.iter().any(|b| b == name) {
                bases.push(name.to_string());
            }
        };
        // Bases of shift measures.
        for (_, s) in shift_specs {
            // The base must be a plain (non-shift) measure in the fact view.
            let base = self
                .evaluator
                .measure(fact_view, &s.measure)
                .ok_or_else(|| {
                    EngineError::QueryError(format!(
                        "shift base measure '{}.{}' not found",
                        fact_view, s.measure
                    ))
                })?;
            if base.shift.is_some() {
                return Err(EngineError::QueryError(format!(
                    "shift base '{}.{}' is itself a shift measure; the base must be a plain measure",
                    fact_view, s.measure
                )));
            }
            push(&s.measure, &mut bases);
        }
        // Plain fact-view measures referenced directly or via composites.
        let mut seen: HashSet<String> = HashSet::new();
        for m in &request.measures {
            let (view, name) = self.evaluator.parse_member_path(m)?;
            self.collect_plain_bases(&view, &name, fact_view, &mut bases, &mut seen);
        }
        Ok(bases)
    }

    fn collect_plain_bases(
        &self,
        view: &str,
        name: &str,
        fact_view: &str,
        bases: &mut Vec<String>,
        seen: &mut HashSet<String>,
    ) {
        if !seen.insert(format!("{}.{}", view, name)) {
            return;
        }
        let Some(measure) = self.evaluator.measure(view, name) else {
            return;
        };
        if measure.shift.is_some() {
            return; // handled via shift bases
        }
        // A plain (aggregate) measure in the fact view with no measure refs is a base.
        let refs: Vec<(String, String)> = measure
            .expr
            .as_ref()
            .map(|e| {
                dotted_ref_regex()
                    .captures_iter(e)
                    .filter(|c| &c[1] != "variables")
                    .filter(|c| self.evaluator.is_measure(&format!("{}.{}", &c[1], &c[2])))
                    .map(|c| (c[1].to_string(), c[2].to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if refs.is_empty() {
            if view == fact_view && !bases.iter().any(|b| b == name) {
                bases.push(name.to_string());
            }
            return;
        }
        for (rv, rn) in refs {
            self.collect_plain_bases(&rv, &rn, fact_view, bases, seen);
        }
    }
}

/// Cohort derivation context: the lifespan source (joined to the fact view) and
/// the pre-rendered cohort predicate fragments.
///
/// In the **direct** form, `lifespan_view` is a real view (e.g. `stores`)
/// joined through the normal entity graph; `start_col`/`end_col` are columns on
/// that view. In the **derived** form (`derived: Some(_)`), `lifespan_view` is
/// the alias of a synthesized CTE (`__lifespan_<entity>`); `start_col`/`end_col`
/// are its output columns (`lifespan_start` / `lifespan_end`), and the CTE
/// definition is prepended to the shift query's WITH clause.
struct CohortContext {
    /// View (or CTE alias) carrying the lifespan that the cohort predicate
    /// joins against. For derived lifespans this is the CTE alias.
    lifespan_view: String,
    /// Entity name linking fact view to lifespan view (e.g. `store_id`).
    #[allow(dead_code)]
    entity_name: String,
    /// Start column/dimension name on the lifespan source. For derived lifespans
    /// this is the CTE's output alias (`lifespan_start`).
    start_col: String,
    /// End column/dimension name on the lifespan source (None = no end column).
    /// For derived lifespans, `lifespan_end` when an end expr was provided.
    end_col: Option<String>,
    /// Start-of-life cutoff literal `YYYY-MM-DD`: lifespan.start must be <= this.
    start_cutoff: String,
    /// End-of-life floor literal `YYYY-MM-DD`: lifespan.end (if any) must be >= this.
    end_floor: String,
    /// Set when lifespan is derived via aggregation from another view.
    derived: Option<DerivedLifespan>,
}

/// A lifespan derived via aggregation rather than direct columns on the
/// entity's owning view. Emitted as a `__lifespan_<entity>` CTE that groups the
/// `from` view by the entity's key and exposes `lifespan_start` /
/// `lifespan_end` as aggregates. Used when the entity table doesn't carry
/// open/close columns and the lifespan must be inferred from another view's
/// activity (e.g. min/max of a transaction date).
struct DerivedLifespan {
    /// CTE body — the `SELECT ... FROM <from_view> GROUP BY <keys>` query.
    /// Indented and wrapped as `__lifespan_<entity> AS (...)` in the final SQL.
    cte_sql: String,
    /// Join key pairs `(fact_view_column, cte_column)`. The two sides may use
    /// different column names — the entity declaration on each view supplies
    /// its own `key`, and we don't assume they match.
    keys: Vec<(String, String)>,
}

/// The compiled inner stage of a shift query (the `__shift_base` CTE body) plus
/// the alias bookkeeping the later stages need.
struct ShiftInnerStage {
    sql: String,
    params: Vec<String>,
    /// Dimension + time-bucket columns (the non-aggregate selects), in order.
    dim_columns: Vec<ColumnMeta>,
    /// Alias of the time-bucket column (the shifted key).
    bucket_alias: String,
    /// Base measures, as (measure name in fact view, column alias).
    base_aliases: Vec<(String, String)>,
    /// Derived-lifespan CTE to prepend before `__shift_base`, as (alias, body).
    /// None when no cohort, or when the cohort uses a direct lifespan view.
    lifespan_cte: Option<(String, String)>,
}

impl<'a> SqlGenerator<'a> {
    /// Derive the cohort context for the `comparable_by` entity: resolve its
    /// `lifespan` and compute the start-of-life / end-of-life cutoffs.
    #[allow(clippy::too_many_arguments)]
    fn build_cohort_context(
        &self,
        fact_view: &str,
        comparable_by: &str,
        interval: &crate::engine::shift::Interval,
        direction: &ShiftDirection,
        maturity: Option<crate::engine::shift::Interval>,
        c_start: chrono::NaiveDate,
        c_end: chrono::NaiveDate,
    ) -> Result<CohortContext, EngineError> {
        let fv = self.evaluator.view(fact_view).ok_or_else(|| {
            EngineError::QueryError(format!("fact view '{}' not found", fact_view))
        })?;

        // The named entity must be reachable from the fact view (declared on it)
        // and must declare a lifespan (in whichever view owns it).
        if !fv.entities.iter().any(|e| e.name == comparable_by) {
            return Err(EngineError::QueryError(format!(
                "shift `comparable_by: {}` names an entity that is not on the queried view '{}'",
                comparable_by, fact_view
            )));
        }
        let (lifespan_view, lifespan) = self.find_lifespan(comparable_by).ok_or_else(|| {
            EngineError::QueryError(format!(
                "shift `comparable_by: {0}` requires entity '{0}' to declare a `lifespan`, but none \
                 is declared on any view",
                comparable_by
            ))
        })?;
        let entity_name = comparable_by.to_string();

        // Required coverage span. The entity must be live across both windows;
        // `maturity` (M) is a honeymoon offset that always pushes the required
        // start-of-life *earlier* (the entity must have existed M before the
        // earliest window start), never the end-of-life.
        //   prior: start <= (c_start - I - M),  end >= c_end
        //   next:  start <= (c_start - M),       end >= (c_end + I)
        let (start_cutoff, end_floor) = match direction {
            ShiftDirection::Prior => {
                let shifted_start = interval.subtract_from(c_start);
                let cutoff = match maturity {
                    Some(m) => m.subtract_from(shifted_start),
                    None => shifted_start,
                };
                (cutoff, c_end)
            }
            ShiftDirection::Next => {
                let cutoff = match maturity {
                    Some(m) => m.subtract_from(c_start),
                    None => c_start,
                };
                (cutoff, interval.add_to(c_end))
            }
        };

        // Derived form: lifespan.from names a view to aggregate over. The
        // engine synthesizes a `__lifespan_<entity>` CTE — grouping that view
        // by the entity's keys, exposing `lifespan_start` / `lifespan_end` as
        // aggregates — and the cohort predicate joins the fact view to it. The
        // `lifespan_view` field is repurposed as the CTE alias so the existing
        // alias-lookup path in build_shift_inner_stage works unchanged.
        if let Some(ref from_view_name) = lifespan.from {
            let cte_alias = format!("__lifespan_{}", comparable_by);
            let (cte_sql, keys) = self.build_lifespan_cte_sql(
                fact_view,
                from_view_name,
                comparable_by,
                &lifespan.start,
                lifespan.end.as_deref(),
            )?;
            return Ok(CohortContext {
                lifespan_view: cte_alias,
                entity_name,
                start_col: "lifespan_start".to_string(),
                end_col: lifespan.end.as_ref().map(|_| "lifespan_end".to_string()),
                start_cutoff: start_cutoff.format("%Y-%m-%d").to_string(),
                end_floor: end_floor.format("%Y-%m-%d").to_string(),
                derived: Some(DerivedLifespan { cte_sql, keys }),
            });
        }

        Ok(CohortContext {
            lifespan_view,
            entity_name,
            start_col: lifespan.start,
            end_col: lifespan.end,
            start_cutoff: start_cutoff.format("%Y-%m-%d").to_string(),
            end_floor: end_floor.format("%Y-%m-%d").to_string(),
            derived: None,
        })
    }

    /// Build the body SQL for a derived-lifespan CTE: group `from_view` by the
    /// `comparable_by` entity's keys and emit start/end aggregates aliased as
    /// `lifespan_start` / `lifespan_end`. Returns the CTE body and the
    /// `(fact_key, from_key)` column pairs used to join the fact view to the
    /// CTE. The two views' entity declarations supply their own key column
    /// names; they need not match.
    fn build_lifespan_cte_sql(
        &self,
        fact_view_name: &str,
        from_view_name: &str,
        comparable_by: &str,
        start_expr: &str,
        end_expr: Option<&str>,
    ) -> Result<(String, Vec<(String, String)>), EngineError> {
        let from_view = self.evaluator.view(from_view_name).ok_or_else(|| {
            EngineError::QueryError(format!(
                "lifespan `from: {}` names a view that does not exist (entity '{}')",
                from_view_name, comparable_by
            ))
        })?;
        let from_entity = from_view
            .entities
            .iter()
            .find(|e| e.name == comparable_by)
            .ok_or_else(|| {
                EngineError::QueryError(format!(
                    "lifespan `from: {0}` must declare the `{1}` entity (so its keys can group the \
                     aggregation), but '{0}' does not have entity '{1}'",
                    from_view_name, comparable_by
                ))
            })?;
        let from_keys = from_entity.get_keys();
        if from_keys.is_empty() {
            return Err(EngineError::QueryError(format!(
                "entity '{}' on view '{}' has no keys; cannot group lifespan aggregation",
                comparable_by, from_view_name
            )));
        }

        // Fact-side keys: the entity may use different column names on the
        // fact view than on the from view. (The entity is matched by name; the
        // key columns are per-view.) Pair them positionally — both sides must
        // expose the same number of keys for the entity.
        let fact_view = self.evaluator.view(fact_view_name).ok_or_else(|| {
            EngineError::QueryError(format!("fact view '{}' not found", fact_view_name))
        })?;
        let fact_entity = fact_view
            .entities
            .iter()
            .find(|e| e.name == comparable_by)
            .ok_or_else(|| {
                EngineError::QueryError(format!(
                    "fact view '{}' must declare the `{}` entity for a `comparable_by` shift",
                    fact_view_name, comparable_by
                ))
            })?;
        let fact_keys = fact_entity.get_keys();
        if fact_keys.len() != from_keys.len() {
            return Err(EngineError::QueryError(format!(
                "entity '{}' declares {} key(s) on fact view '{}' but {} on lifespan `from: {}`; \
                 both must expose the same number of keys",
                comparable_by,
                fact_keys.len(),
                fact_view_name,
                from_keys.len(),
                from_view_name,
            )));
        }
        let key_pairs: Vec<(String, String)> = fact_keys.into_iter().zip(from_keys).collect();

        let from_alias = from_view_name; // alias = view name (matches other shift CTEs)
        let q = |s: &str| self.dialect.quote_identifier(s);
        let from_table = self.view_source_expr(from_view);
        let empty = HashMap::new();

        // The CTE exposes the from view's key columns under their own names so
        // the JOIN condition on the right-hand side can reference them
        // verbatim. (Aliasing them to fact-side names would also work but
        // makes the CTE shape depend on the call site.)
        let key_select: Vec<String> = key_pairs
            .iter()
            .map(|(_, from_key)| format!("{}.{} AS {}", q(from_alias), q(from_key), q(from_key)))
            .collect();
        let key_group: Vec<String> = (1..=key_pairs.len()).map(|i| i.to_string()).collect();

        let start_sql = self.resolve_expression(from_alias, start_expr, &empty);
        let mut select_parts = key_select;
        select_parts.push(format!("{} AS {}", start_sql, q("lifespan_start")));
        if let Some(end) = end_expr {
            let end_sql = self.resolve_expression(from_alias, end, &empty);
            select_parts.push(format!("{} AS {}", end_sql, q("lifespan_end")));
        }

        let body = format!(
            "SELECT\n  {}\nFROM\n  {} AS {}\nGROUP BY\n  {}",
            select_parts.join(",\n  "),
            from_table,
            q(from_alias),
            key_group.join(", "),
        );

        Ok((body, key_pairs))
    }

    /// Build the `__shift_base` inner stage: base measures grouped by the query
    /// dimensions + time bucket, scanned over the EXPANDED window, with the
    /// cohort predicate applied here (the cohort-before-shift invariant).
    #[allow(clippy::too_many_arguments)]
    fn build_shift_inner_stage(
        &self,
        request: &QueryRequest,
        fact_view: &str,
        td: &TimeDimensionQuery,
        granularity: &str,
        inner_bases: &[String],
        scan_start: &str,
        scan_end: &str,
        cohort: Option<&CohortContext>,
    ) -> Result<ShiftInnerStage, EngineError> {
        // Referenced views: fact + dimension views + lifespan view (if cohort).
        let mut referenced: Vec<String> = vec![fact_view.to_string()];
        let add_view = |v: String, acc: &mut Vec<String>| {
            if !acc.contains(&v) {
                acc.push(v);
            }
        };
        for d in &request.dimensions {
            let (v, _) = self.evaluator.parse_member_path(d)?;
            add_view(v, &mut referenced);
        }
        {
            let (v, _) = self.evaluator.parse_member_path(&td.dimension)?;
            add_view(v, &mut referenced);
        }
        if let Some(c) = cohort {
            // Direct lifespan: the lifespan view goes through the regular join
            // graph. Derived lifespan: the CTE is wired in manually below, so
            // don't ask the join graph to find a path to it (it's not a view).
            if c.derived.is_none() {
                add_view(c.lifespan_view.clone(), &mut referenced);
            }
        }

        let mut builder = QueryBuilder {
            view_aliases: HashMap::new(),
            select_columns: Vec::new(),
            joins: Vec::new(),
            where_conditions: Vec::new(),
            group_by_indices: Vec::new(),
            having_conditions: Vec::new(),
            order_by: Vec::new(),
            params: Vec::new(),
            columns: Vec::new(),
            base_view: fact_view.to_string(),
            multiplied_views: HashSet::new(),
        };
        builder
            .view_aliases
            .insert(fact_view.to_string(), fact_view.to_string());

        let other_views: Vec<&str> = referenced
            .iter()
            .filter(|v| v.as_str() != fact_view)
            .map(|v| v.as_str())
            .collect();
        if !other_views.is_empty() {
            self.build_joins(&mut builder, fact_view, &other_views, &request.through)?;
        }

        // Derived lifespan: splice in a manual LEFT JOIN to the synthesized
        // CTE. The CTE is referenced by alias only (it appears in the WITH
        // clause, not in evaluator's view list), so this skips the join graph.
        // The fact and from views may each declare the entity under a
        // different column name; `keys` carries both sides as paired columns.
        if let Some(c) = cohort {
            if let Some(ref d) = c.derived {
                let q = |s: &str| self.dialect.quote_identifier(s);
                let cte_alias = &c.lifespan_view;
                // The CTE alias isn't a real view; inserting it into
                // view_aliases lets the existing predicate-resolution code
                // (which calls `view_aliases.get(...)` to find a qualifier)
                // work unchanged. Callers that look up the *view* via
                // `evaluator.view(alias)` will correctly get None.
                builder
                    .view_aliases
                    .insert(cte_alias.clone(), cte_alias.clone());
                let condition = d
                    .keys
                    .iter()
                    .map(|(fact_key, from_key)| {
                        format!(
                            "{}.{} = {}.{}",
                            q(fact_view),
                            q(fact_key),
                            q(cte_alias),
                            q(from_key)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                builder.joins.push(JoinClause {
                    join_type: "LEFT".to_string(),
                    table_expr: q(cte_alias),
                    alias: cte_alias.clone(),
                    condition,
                    relationship: JoinRelationship::ManyToOne,
                });
            }
        }

        // The shift inner stage has no fan-out protection: a one-to-many join
        // would multiply the fact rows and silently inflate the base aggregates
        // (and thus every shifted value). Refuse rather than miscompute.
        // TODO: extend fan-out protection (generate_with_fanout_protection) to the
        // shift inner stage so comps across one-to-many joins are supported.
        if builder.multiplied_views.contains(fact_view) {
            return Err(EngineError::QueryError(format!(
                "shift query joins a one-to-many relationship that multiplies '{}' rows; this would \
                 inflate the shifted base measure. Fan-out protection is not yet supported for shift \
                 measures — restrict the query to many-to-one joins.",
                fact_view
            )));
        }

        let entity_to_alias = self
            .evaluator
            .build_entity_to_alias_map(fact_view, &other_views);

        // Dimensions.
        for d in &request.dimensions {
            self.add_dimension(&mut builder, d, &entity_to_alias)?;
        }
        // Time bucket (always at the chosen granularity).
        let td_bucket = TimeDimensionQuery {
            dimension: td.dimension.clone(),
            granularity: Some(granularity.to_string()),
            date_range: None,
        };
        self.add_time_dimension(
            &mut builder,
            &td_bucket,
            &entity_to_alias,
            request.timezone.as_deref(),
        )?;
        let bucket_member = format!("{}.{}", td.dimension, granularity);
        let bucket_alias = self.member_alias(&bucket_member);
        // Normalize the bucket to DATE so the self-join key and window bounds are
        // all DATE-typed (uniform across dialects; avoids DATE/TIMESTAMP mismatch
        // and string-coercion failures, and turns MySQL's string bucket into a
        // real date). The bucket is always a period boundary, so DATE loses nothing.
        if let Some(col) = builder
            .select_columns
            .iter_mut()
            .find(|c| c.alias == bucket_alias)
        {
            col.expr = self.dialect.cast_to_date(&col.expr);
        }

        // Base measures.
        for base in inner_bases {
            self.add_measure(
                &mut builder,
                &format!("{}.{}", fact_view, base),
                &entity_to_alias,
            )?;
        }

        // Dimension filters (route measure filters to HAVING for parity, though
        // measure filters on a shift base are unusual).
        for filter in &request.filters {
            let sql = self.compile_filter(filter, &mut builder, &entity_to_alias)?;
            if !sql.is_empty() {
                if self.is_measure_filter(filter) {
                    builder.having_conditions.push(sql);
                } else {
                    builder.where_conditions.push(sql);
                }
            }
        }

        // Expanded scan window on the raw (un-truncated) time column.
        let (tv, tn) = self.evaluator.parse_member_path(&td.dimension)?;
        let tdim = self.evaluator.dimension(&tv, &tn).ok_or_else(|| {
            EngineError::QueryError(format!("time dimension '{}' not found", td.dimension))
        })?;
        let talias = builder
            .view_aliases
            .get(&tv)
            .ok_or_else(|| EngineError::QueryError(format!("view '{}' not in query", tv)))?;
        let tcol = self.dialect.cast_to_date(&self.resolve_expression(
            talias,
            &tdim.expr,
            &entity_to_alias,
        ));
        builder.where_conditions.push(format!(
            "{c} >= {s} AND {c} <= {e}",
            c = tcol,
            s = self.dialect.date_literal(scan_start),
            e = self.dialect.date_literal(scan_end),
        ));

        // Cohort predicate (entity-level, computed from window literals) — applied
        // here so both the current and shifted buckets inherit the same entities.
        if let Some(c) = cohort {
            let empty = HashMap::new();
            let lalias = builder.view_aliases.get(&c.lifespan_view).ok_or_else(|| {
                EngineError::QueryError(format!(
                    "lifespan view '{}' could not be joined for the cohort predicate",
                    c.lifespan_view
                ))
            })?;
            let start_expr =
                self.dialect
                    .cast_to_date(&self.resolve_expression(lalias, &c.start_col, &empty));
            let mut pred = format!(
                "{} <= {}",
                start_expr,
                self.dialect.date_literal(&c.start_cutoff)
            );
            if let Some(ref end_col) = c.end_col {
                let end_expr = self
                    .dialect
                    .cast_to_date(&self.resolve_expression(lalias, end_col, &empty));
                // Direct form: a NULL end column means "still active" — keep
                // the row. Derived form: end is an aggregate (e.g.
                // MAX(sale_date)); it is only NULL when every row in the
                // entity's group has a NULL date, which is "no signal" rather
                // than "still active" — exclude. Branch the predicate
                // accordingly.
                pred = if c.derived.is_some() {
                    format!(
                        "{} AND {} >= {}",
                        pred,
                        end_expr,
                        self.dialect.date_literal(&c.end_floor)
                    )
                } else {
                    format!(
                        "{} AND ({} IS NULL OR {} >= {})",
                        pred,
                        end_expr,
                        end_expr,
                        self.dialect.date_literal(&c.end_floor)
                    )
                };
            }
            builder.where_conditions.push(pred);
        }

        // Assemble the inner SELECT (grouped; no order/limit/motif). Force
        // `ungrouped: false` — the inner stage MUST aggregate base measures by
        // dims + bucket for the self-join to align; an ungrouped request would
        // otherwise drop the GROUP BY and emit raw rows.
        let inner_request = QueryRequest {
            order: vec![],
            limit: None,
            offset: None,
            motif: None,
            ungrouped: false,
            ..request.clone()
        };
        let sql = self.assemble_sql(&builder, &inner_request)?;

        // Collect alias bookkeeping for the later stages.
        let dim_columns: Vec<ColumnMeta> = builder
            .columns
            .iter()
            .filter(|c| matches!(c.kind, ColumnKind::Dimension | ColumnKind::TimeDimension))
            .cloned()
            .collect();
        let base_aliases: Vec<(String, String)> = builder
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Measure)
            .map(|c| {
                let name = c.member.rsplit('.').next().unwrap_or(&c.member).to_string();
                (name, c.alias.clone())
            })
            .collect();

        let lifespan_cte = cohort.and_then(|c| {
            c.derived
                .as_ref()
                .map(|d| (c.lifespan_view.clone(), d.cte_sql.clone()))
        });

        Ok(ShiftInnerStage {
            sql,
            params: builder.params,
            dim_columns,
            bucket_alias,
            base_aliases,
            lifespan_cte,
        })
    }

    /// Assemble the aligned self-join + outer SELECT around the inner stage.
    #[allow(clippy::too_many_arguments)]
    fn assemble_shift_sql(
        &self,
        request: &QueryRequest,
        fact_view: &str,
        td: &TimeDimensionQuery,
        granularity: &str,
        interval: &crate::engine::shift::Interval,
        direction: &ShiftDirection,
        inner: &ShiftInnerStage,
        c_start: &chrono::NaiveDate,
        c_end: &chrono::NaiveDate,
    ) -> Result<QueryResult, EngineError> {
        let q = |s: &str| self.dialect.quote_identifier(s);
        let bucket = &inner.bucket_alias;

        // ── Aligned stage ────────────────────────────────────────────────
        // ON: dimension equality + shifted bucket key. Dimension equality is
        // NULL-safe (`a = b OR (a IS NULL AND b IS NULL)`) so a segment whose
        // grouping value is NULL still aligns to its own prior bucket instead of
        // silently dropping out (plain `=` is never true for NULL). Written
        // longhand rather than `IS NOT DISTINCT FROM` for cross-dialect support.
        let mut on_conditions: Vec<String> = inner
            .dim_columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Dimension)
            .map(|c| {
                let a = q(&c.alias);
                format!("(cur.{a} = prior.{a} OR (cur.{a} IS NULL AND prior.{a} IS NULL))")
            })
            .collect();
        // Shifted bucket key, via dialect-aware DATE arithmetic (both buckets are
        // DATE-typed). `prior` sits one interval before `cur`, so advancing the
        // prior bucket by the interval must equal the current bucket.
        let (count, unit) = interval.base_parts();
        let bucket_join = match direction {
            // prior bucket sits one interval before the current bucket.
            ShiftDirection::Prior => format!(
                "cur.{b} = {adv}",
                b = q(bucket),
                adv = self
                    .dialect
                    .date_add(&format!("prior.{}", q(bucket)), count, unit),
            ),
            // next (shifted-forward) bucket sits one interval after the current.
            ShiftDirection::Next => format!(
                "prior.{b} = {adv}",
                b = q(bucket),
                adv = self
                    .dialect
                    .date_add(&format!("cur.{}", q(bucket)), count, unit),
            ),
        };
        on_conditions.push(bucket_join);

        // SELECT: dims + bucket (from cur), each base measure as current + prior.
        let mut aligned_select: Vec<String> = inner
            .dim_columns
            .iter()
            .map(|c| format!("cur.{a} AS {a}", a = q(&c.alias)))
            .collect();
        for (_, alias) in &inner.base_aliases {
            aligned_select.push(format!("cur.{a} AS {a}", a = q(alias)));
            let prior_alias = format!("{}__prior", alias);
            aligned_select.push(format!(
                "prior.{a} AS {pa}",
                a = q(alias),
                pa = q(&prior_alias)
            ));
        }

        // Restrict to current-window buckets (the prior cur-bucket is dropped).
        let cur_window = format!(
            "cur.{b} >= {s} AND cur.{b} <= {e}",
            b = q(bucket),
            s = self
                .dialect
                .date_literal(&c_start.format("%Y-%m-%d").to_string()),
            e = self
                .dialect
                .date_literal(&c_end.format("%Y-%m-%d").to_string()),
        );

        let aligned = format!(
            "SELECT\n  {}\nFROM __shift_base AS cur\nLEFT JOIN __shift_base AS prior ON {}\nWHERE {}",
            aligned_select.join(",\n  "),
            on_conditions.join(" AND "),
            cur_window,
        );

        // ── Outer stage ──────────────────────────────────────────────────
        let mut outer_select: Vec<String> = Vec::new();
        let mut columns: Vec<ColumnMeta> = Vec::new();

        // Dimensions + time bucket pass through from the aligned stage.
        for c in &inner.dim_columns {
            outer_select.push(format!("{a} AS {a}", a = q(&c.alias)));
            columns.push(c.clone());
        }
        // Requested measures, resolved against the aligned columns.
        for m in &request.measures {
            let (view, name) = self.evaluator.parse_member_path(m)?;
            let expr = self.resolve_outer_measure_expr(&view, &name, fact_view, inner)?;
            let alias = self.member_alias(m);
            outer_select.push(format!("{} AS {}", expr, q(&alias)));
            columns.push(ColumnMeta {
                member: m.clone(),
                alias,
                kind: ColumnKind::Measure,
            });
        }

        let mut outer = format!(
            "SELECT\n  {}\nFROM __shift_aligned",
            outer_select.join(",\n  ")
        );

        // ORDER BY (map member -> alias), LIMIT, OFFSET. Match the exact member,
        // or — for a time dimension ordered by its bare member (`sales.sale_date`)
        // rather than the bucketed member (`sales.sale_date.month`) — its prefix.
        let order_parts: Vec<String> = request
            .order
            .iter()
            .filter_map(|o| {
                let prefix = format!("{}.", o.id);
                columns
                    .iter()
                    .find(|c| c.member == o.id || c.member.starts_with(&prefix))
                    .map(|c| format!("{} {}", q(&c.alias), if o.desc { "DESC" } else { "ASC" }))
            })
            .collect();
        if !order_parts.is_empty() {
            outer.push_str(&format!("\nORDER BY\n  {}", order_parts.join(", ")));
        }
        if let Some(limit) = request.limit {
            outer.push_str(&format!("\nLIMIT {}", limit));
        }
        if let Some(offset) = request.offset {
            outer.push_str(&format!("\nOFFSET {}", offset));
        }

        let _ = (td, granularity); // window/granularity already baked into the stages
                                   // Prepend the derived-lifespan CTE when present. It's referenced only
                                   // by the inner stage's JOIN, so a single forward declaration suffices.
        let lifespan_prefix = match &inner.lifespan_cte {
            Some((alias, body)) => format!(
                "{} AS (\n{}\n),\n",
                self.dialect.quote_identifier(alias),
                indent_sql(body),
            ),
            None => String::new(),
        };
        let sql = format!(
            "WITH {}__shift_base AS (\n{}\n),\n__shift_aligned AS (\n{}\n)\n{}",
            lifespan_prefix,
            indent_sql(&inner.sql),
            indent_sql(&aligned),
            outer,
        );

        Ok(QueryResult {
            sql,
            params: inner.params.clone(),
            columns,
            default_limit_applied: false,
        })
    }

    /// Resolve a requested measure to a SQL expression over the aligned-stage
    /// columns. Shift measures map to their base's `__prior` column; plain base
    /// measures to their current column; composite measures recurse.
    fn resolve_outer_measure_expr(
        &self,
        view: &str,
        name: &str,
        fact_view: &str,
        inner: &ShiftInnerStage,
    ) -> Result<String, EngineError> {
        let q = |s: &str| self.dialect.quote_identifier(s);
        let measure = self.evaluator.measure(view, name).ok_or_else(|| {
            EngineError::QueryError(format!("measure '{}.{}' not found", view, name))
        })?;

        // Shift measure → the prior column of its base.
        if let Some(ref shift) = measure.shift {
            let base_alias = self.member_alias(&format!("{}.{}", fact_view, shift.measure));
            return Ok(q(&format!("{}__prior", base_alias)));
        }

        // Composite (number/custom) measure referencing other measures → recurse.
        if let Some(ref expr) = measure.expr {
            let has_measure_ref = dotted_ref_regex().captures_iter(expr).any(|c| {
                &c[1] != "variables" && self.evaluator.is_measure(&format!("{}.{}", &c[1], &c[2]))
            });
            if has_measure_ref {
                let mut err: Option<EngineError> = None;
                let resolved = dotted_ref_regex()
                    .replace_all(expr, |caps: &regex::Captures<'_>| {
                        let (rv, rn) = (&caps[1], &caps[2]);
                        if rv == "variables" {
                            return format!("{{{{{}.{}}}}}", rv, rn);
                        }
                        if self.evaluator.is_measure(&format!("{}.{}", rv, rn)) {
                            match self.resolve_outer_measure_expr(rv, rn, fact_view, inner) {
                                Ok(s) => format!("({})", s),
                                Err(e) => {
                                    err = Some(e);
                                    String::new()
                                }
                            }
                        } else {
                            format!("{{{{{}.{}}}}}", rv, rn)
                        }
                    })
                    .to_string();
                if let Some(e) = err {
                    return Err(e);
                }
                return Ok(resolved);
            }
        }

        // Plain base measure → its current column. Confirm it was materialised.
        let base_alias = self.member_alias(&format!("{}.{}", view, name));
        if inner.base_aliases.iter().any(|(_, a)| a == &base_alias) {
            Ok(q(&base_alias))
        } else {
            Err(EngineError::QueryError(format!(
                "measure '{}.{}' is not part of the shift query's fact view '{}' and cannot be \
                 aligned",
                view, name, fact_view
            )))
        }
    }
}

/// Indent a multi-line SQL fragment by two spaces for nesting inside a CTE.
fn indent_sql(sql: &str) -> String {
    sql.lines()
        .map(|l| format!("  {}", l))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse a window interval string like "7 days" or "3 rows" into SQL form.
fn parse_window_interval(s: &str) -> String {
    // Try to parse "N unit" format
    let parts: Vec<&str> = s.trim().splitn(2, ' ').collect();
    if parts.len() == 2 {
        if let Ok(n) = parts[0].parse::<i64>() {
            let unit = parts[1].to_uppercase();
            if unit.starts_with("ROW") {
                return n.to_string();
            }
            // For time-based intervals, use RANGE instead of ROWS
            return format!("{}", n);
        }
    }
    // Fall back to literal
    s.to_string()
}

/// Check if an expression is a simple, single-token column name — only word
/// characters (`[A-Za-z0-9_]`), no spaces, operators, or functions. Such an
/// expr is qualified with the view alias and quoted as one identifier.
///
/// A name that needs special quoting (spaces, reserved words, mixed case on a
/// case-sensitive dialect) must be quoted explicitly in the YAML — e.g.
/// `expr: '"Day of Week"'`. That routes through `qualify_bare_columns`, which
/// preserves the author's quoting. We deliberately do NOT try to auto-detect
/// bare spaced identifiers: a bare multi-word string like `col IS NOT NULL` is
/// indistinguishable from a spaced column name without a SQL parser, and
/// guessing wrong silently mis-quotes an expression as an identifier (#73).
fn is_simple_column_name(expr: &str) -> bool {
    let trimmed = expr.trim();
    !trimmed.is_empty() && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Wrap a SUM aggregate in COALESCE when the measure has filters.
/// Filtered SUMs use `SUM(CASE WHEN ... END)` which returns NULL when no
/// rows match; COALESCE to 0 prevents NULL propagation in arithmetic.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::evaluator::SchemaEvaluator;
    use crate::engine::join_graph::JoinGraph;

    fn make_test_engine() -> (SchemaEvaluator, JoinGraph, SemanticLayer) {
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "orders".to_string(),
                    description: Some("Orders".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("public.orders".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "customer".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("customer_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![
                        Dimension {
                            name: "order_id".to_string(),
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
                        },
                        Dimension {
                            name: "customer_id".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "customer_id".to_string(),
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
                            name: "status".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "status".to_string(),
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
                            name: "order_date".to_string(),
                            dimension_type: DimensionType::Date,
                            description: None,
                            expr: "order_date".to_string(),
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
                            name: "amount".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "amount".to_string(),
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
                            name: "day_of_week".to_string(),
                            dimension_type: DimensionType::String,
                            // Spaced column names must be quoted explicitly in the
                            // YAML; airlayer preserves the author's quoting rather
                            // than guessing whether a bare spaced string is an
                            // identifier or an expression (#73).
                            description: None,
                            expr: "\"Day of Week\"".to_string(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: None,
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        },
                        // Boolean dimension whose `expr` is a word-only predicate
                        // (`<col> IS NOT NULL`). Such an expr must NOT be quoted
                        // whole as an identifier — the bug fixed in #73.
                        Dimension {
                            name: "has_status".to_string(),
                            dimension_type: DimensionType::Boolean,
                            description: None,
                            expr: "status IS NOT NULL".to_string(),
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
                            name: "published_language".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "\"Published Language\"".to_string(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: None,
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        },
                        // Self-referential dimension (`expr` references itself by
                        // bare name) — an invalid cyclic definition that must be
                        // caught by the recursion guard, not overflow the stack.
                        Dimension {
                            name: "cyclic".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "{{cyclic}}".to_string(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: None,
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        },
                        // Boolean dimension whose `expr` is itself a comparison
                        // — the shape that previously produced an invalid
                        // chained predicate when filtered (see
                        // test_compound_boolean_dimension_filter_is_parenthesized).
                        Dimension {
                            name: "is_completed".to_string(),
                            dimension_type: DimensionType::Boolean,
                            description: None,
                            expr: "status = 'completed'".to_string(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: None,
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        },
                        // Fixture for the unresolved-`{{ }}` guard: a quoted,
                        // qualified column wrapped in braces is not a valid
                        // member ref, so it survives resolution unchanged.
                        Dimension {
                            name: "bad_templated_ref".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "{{ \"orders\".\"status\" }}".to_string(),
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
                        },
                        Measure {
                            name: "total_revenue".to_string(),
                            measure_type: MeasureType::Sum,
                            description: None,
                            expr: Some("amount".to_string()),
                            original_expr: None,
                            filters: None,
                            samples: None,
                            synonyms: None,
                            rolling_window: None,
                            inherits_from: None,
                            drivers: None,
                            shift: None,
                            meta: None,
                        },
                        // Measure whose filter uses a BARE same-view member ref
                        // `{{is_completed}}` (no view prefix). airlayer must expand
                        // it to `{{orders.is_completed}}` and resolve it.
                        Measure {
                            name: "completed_count".to_string(),
                            measure_type: MeasureType::Count,
                            description: None,
                            expr: None,
                            original_expr: None,
                            filters: Some(vec![MeasureFilter {
                                expr: "{{is_completed}} = true".to_string(),
                                description: None,
                                original_expr: None,
                            }]),
                            samples: None,
                            synonyms: None,
                            rolling_window: None,
                            inherits_from: None,
                            drivers: None,
                            shift: None,
                            meta: None,
                        },
                    ]),
                    segments: vec![Segment {
                        name: "is_active".to_string(),
                        expr: "status = 'active'".to_string(),
                        description: Some("Active orders".to_string()),
                        inherits_from: None,
                        meta: None,
                    }],
                    pre_aggregations: None,
                    refresh_key: None,
                    meta: None,
                },
                View {
                    name: "customers".to_string(),
                    description: Some("Customers".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("public.customers".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "customer".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("customer_id".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![
                        Dimension {
                            name: "customer_id".to_string(),
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
                        },
                        Dimension {
                            name: "name".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "name".to_string(),
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
                        name: "total_customers".to_string(),
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
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        (eval, jg, layer)
    }

    #[test]
    fn test_simple_select() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("COUNT(*)"));
        assert!(result.sql.contains("status"));
        assert!(result.sql.contains("GROUP BY"));
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_column_name_with_spaces_must_be_explicitly_quoted() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // The fixture `day_of_week` expr is the explicitly-quoted `"Day of Week"`.
        // airlayer must qualify it with the view alias and preserve the quoting.
        let request = QueryRequest {
            dimensions: vec!["orders.day_of_week".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("\"orders\".\"Day of Week\""),
            "Expected qualified quoted column name, got: {}",
            result.sql
        );
    }

    #[test]
    fn test_word_only_predicate_dimension_is_not_quoted_whole() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // `has_status` expr is `status IS NOT NULL` — a word-only predicate. It must
        // be emitted as a predicate, NOT quoted whole as `"status IS NOT NULL"` (#73).
        let request = QueryRequest {
            dimensions: vec!["orders.has_status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // The predicate operator survives, and the bare column inside it is
        // qualified (status is a dimension here) — not collapsed into a literal.
        assert!(
            result.sql.contains("IS NOT NULL"),
            "Expected predicate emitted as-is, got: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("\"status IS NOT NULL\""),
            "Predicate must not be quoted whole as one identifier, got: {}",
            result.sql
        );
    }

    #[test]
    fn test_is_simple_column_name() {
        // Single-token identifiers are simple.
        assert!(is_simple_column_name("status"));
        assert!(is_simple_column_name("is_modifier"));
        assert!(is_simple_column_name("notes"));
        // Anything with spaces is NOT simple — spaced names require explicit
        // quoting in the YAML, and word-only predicates are expressions (#73).
        assert!(!is_simple_column_name("Day of Week"));
        assert!(!is_simple_column_name("parent_selection_guid IS NOT NULL"));
        assert!(!is_simple_column_name("a and b"));
        // Operators / functions / empty are not simple.
        assert!(!is_simple_column_name("a + b"));
        assert!(!is_simple_column_name("COALESCE(x, y)"));
        assert!(!is_simple_column_name(""));
        // Explicitly quoted names contain quote chars → not simple; they route
        // through qualify_bare_columns which preserves the quoting.
        assert!(!is_simple_column_name("\"Published Language\""));
    }

    #[test]
    fn test_explicitly_quoted_column_name() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.published_language".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Explicitly quoted expr should be qualified with view alias
        assert!(
            result.sql.contains("\"orders\".\"Published Language\""),
            "Expected qualified quoted column, got: {}",
            result.sql
        );
    }

    #[test]
    fn test_cross_view_join() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["customers.name".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("JOIN"));
        assert!(result.sql.contains("customers"));
        // Entity key "customer_id" resolves through dimension expr:
        // customers.id (expr="id") and orders.customer_id (expr="customer_id")
        // Base view may be either, so check both possible orderings.
        let has_resolved_join = result
            .sql
            .contains(r#""orders"."customer_id" = "customers"."id""#)
            || result
                .sql
                .contains(r#""customers"."id" = "orders"."customer_id""#);
        assert!(
            has_resolved_join,
            "JOIN should use resolved dimension exprs, not raw key names. SQL: {}",
            result.sql
        );
        // Must NOT contain the raw key name as a column on the customers side
        assert!(
            !result.sql.contains(r#""customers"."customer_id""#),
            "JOIN should not use raw entity key name 'customer_id' for customers. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn test_time_dimension() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.order_date".to_string(),
                granularity: Some("month".to_string()),
                date_range: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("date_trunc"));
    }

    #[test]
    fn test_filter_parameterized() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["active".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("WHERE"));
        // Should use parameterized value, not inline
        assert!(result.sql.contains("$1"));
        assert_eq!(result.params, vec!["active".to_string()]);
    }

    #[test]
    fn test_compound_boolean_dimension_filter_is_parenthesized() {
        // Regression: a boolean dimension whose `expr` is itself a comparison
        // (`status = 'completed'`), filtered by equality, must parenthesize the
        // expr so the predicate is `(<expr>) = $1` — never the invalid chained
        // `status = 'completed' = $1` that crashed DuckDB with
        // "syntax error at or near =".
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.is_completed".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["false".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // The compound expr is wrapped, so the literal is immediately followed
        // by a closing paren before the outer comparison.
        assert!(
            result.sql.contains("'completed')"),
            "compound boolean dimension expr must be parenthesized, got:\n{}",
            result.sql
        );
        // Explicit guard against the chained-equals regression.
        assert!(
            !result.sql.contains("'completed' = "),
            "unparenthesized chained predicate regressed:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_measure_filter_bare_member_ref_is_resolved() {
        // Regression: a measure filter referencing a sibling member by bare name
        // `{{is_completed}}` (no view prefix) must resolve to the dimension's
        // expr — not be left as an unresolvable `{{ "orders"."is_completed" }}`.
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.completed_count".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // The bare ref expanded and the dimension's expr (status = 'completed')
        // was substituted into the CASE WHEN, with the column qualified.
        assert!(
            result.sql.contains("= 'completed'") && result.sql.contains("CASE WHEN"),
            "bare member ref must resolve to the dimension expr, got:\n{}",
            result.sql
        );
        // No template braces survive into the compiled SQL.
        assert!(
            !result.sql.contains("{{"),
            "unresolved template ref left in SQL:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_cyclic_member_ref_does_not_overflow_stack() {
        // A self-referential member definition (`cyclic` expr = `{{cyclic}}`) must
        // be caught by the recursion guard and surface as a clean unresolved-ref
        // error, never a stack overflow.
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.cyclic".to_string()],
            ..QueryRequest::new()
        };

        // Must return (Ok or Err) without overflowing the stack. The unresolved
        // brace is detected and reported as an error rather than compiled.
        let result = gen.generate(&request);
        assert!(
            result.is_err(),
            "cyclic definition should error, got: {:?}",
            result.map(|r| r.sql)
        );
    }

    #[test]
    fn test_boolean_dimension_filter_renders_typed_literal_not_quoted_param() {
        // A boolean dimension filtered by equality must compile to an inline
        // boolean literal — `(<expr>) = false` — not a string param. airlayer's
        // params are a type-erased Vec<String> that consumers single-quote when
        // inlining, producing `(<expr>) = 'false'`, which warehouses reject for
        // a boolean LHS (the reported HTTP 502 on `is_deleted` filters).
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.is_completed".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["false".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("= false"),
            "boolean filter must inline a typed literal, got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains("$1"),
            "boolean filter value must not be a string param, got:\n{}",
            result.sql
        );
        assert!(
            result.params.is_empty(),
            "no param should be allocated for an inlined boolean, got: {:?}",
            result.params
        );
    }

    #[test]
    fn test_unresolved_member_ref_fails_compilation() {
        // A dimension `expr` that wraps a quoted/qualified column in `{{ }}` is
        // not a resolvable member ref. Rather than emit the braces into SQL and
        // fail at the database with a cryptic parser error, compilation must
        // fail here with an actionable message.
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.bad_templated_ref".to_string()],
            ..QueryRequest::new()
        };

        let err = gen
            .generate(&request)
            .expect_err("compilation should fail on an unresolved {{ }} reference");
        assert!(
            err.to_string().contains("unresolved reference"),
            "expected an unresolved-reference error, got: {err}"
        );
    }

    #[test]
    fn test_limit_offset() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            limit: Some(10),
            offset: Some(20),
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("LIMIT 10"));
        assert!(result.sql.contains("OFFSET 20"));
    }

    #[test]
    fn test_measure_filter_goes_to_having() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.total_revenue".to_string()),
                operator: Some(FilterOperator::Gt),
                values: vec!["1000".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("HAVING"));
        assert!(result.sql.contains("SUM("));
    }

    #[test]
    fn test_segment() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            segments: vec!["orders.is_active".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("WHERE"));
        // The segment expression should contain the qualified status column
        assert!(result.sql.contains("status"));
        assert!(result.sql.contains("active"));
    }

    #[test]
    fn test_complex_expression_qualification() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "COALESCE(status, 'unknown')".to_string(),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Should qualify the bare 'status' column inside COALESCE
        assert!(result.sql.contains("\"orders\".\"status\""));
    }

    #[test]
    fn test_table_self_reference() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "total_amount".to_string(),
                    dimension_type: DimensionType::Number,
                    description: None,
                    expr: "{{TABLE}}.price * {{TABLE}}.quantity".to_string(),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.total_amount".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result
            .sql
            .contains("\"orders\".price * \"orders\".quantity"));
    }

    #[test]
    fn test_fanout_protection() {
        // orders (one) -> order_items (many)
        // Query: measures from orders AND order_items, with dimensions from both.
        // When orders is base, joining to order_items is OneToMany, which
        // multiplies orders' rows. Fan-out protection should pre-aggregate orders.
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "orders".to_string(),
                    description: Some("Orders".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("public.orders".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "order".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("id".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![
                        Dimension {
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
                        },
                        Dimension {
                            name: "status".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "status".to_string(),
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
                            name: "amount".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "amount".to_string(),
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
                            name: "total_revenue".to_string(),
                            measure_type: MeasureType::Sum,
                            description: None,
                            expr: Some("amount".to_string()),
                            original_expr: None,
                            filters: None,
                            samples: None,
                            synonyms: None,
                            rolling_window: None,
                            inherits_from: None,
                            drivers: None,
                            shift: None,
                            meta: None,
                        },
                        Measure {
                            name: "order_count".to_string(),
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
                        },
                    ]),
                    segments: vec![],
                    pre_aggregations: None,
                    refresh_key: None,
                    meta: None,
                },
                View {
                    name: "order_items".to_string(),
                    description: Some("Order line items".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("public.order_items".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order_item".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![
                        Dimension {
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
                        },
                        Dimension {
                            name: "product_name".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "product_name".to_string(),
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
                        name: "item_count".to_string(),
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
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Query with measures from orders and dimensions from order_items.
        // orders is forced as base (more measures), and the OneToMany join
        // to order_items would multiply orders' rows.
        let request = QueryRequest {
            measures: vec![
                "orders.total_revenue".to_string(),
                "orders.order_count".to_string(),
            ],
            dimensions: vec![
                "orders.status".to_string(),
                "order_items.product_name".to_string(),
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Should use CTEs for fan-out protection since orders is multiplied
        assert!(
            result.sql.contains("WITH"),
            "Expected CTE for fan-out protection, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("__dim_spine"),
            "Expected dimension spine CTE, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_join_type_respects_relationship() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["customers.name".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // orders -> customers is ManyToOne, should be LEFT JOIN
        assert!(result.sql.contains("LEFT JOIN"));
    }

    // ─── Nested AND/OR filters ───────────────────────────────────────

    #[test]
    fn test_nested_and_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            filters: vec![QueryFilter {
                member: None,
                operator: None,
                values: vec![],
                and: Some(vec![
                    QueryFilter {
                        member: Some("orders.status".to_string()),
                        operator: Some(FilterOperator::Equals),
                        values: vec!["active".to_string()],
                        and: None,
                        or: None,
                    },
                    QueryFilter {
                        member: Some("orders.amount".to_string()),
                        operator: Some(FilterOperator::Gt),
                        values: vec!["100".to_string()],
                        and: None,
                        or: None,
                    },
                ]),
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("AND"),
            "Expected AND in WHERE, got:\n{}",
            result.sql
        );
        assert_eq!(result.params.len(), 2);
        assert_eq!(result.params[0], "active");
        assert_eq!(result.params[1], "100");
    }

    #[test]
    fn test_nested_or_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: None,
                operator: None,
                values: vec![],
                and: None,
                or: Some(vec![
                    QueryFilter {
                        member: Some("orders.status".to_string()),
                        operator: Some(FilterOperator::Equals),
                        values: vec!["active".to_string()],
                        and: None,
                        or: None,
                    },
                    QueryFilter {
                        member: Some("orders.status".to_string()),
                        operator: Some(FilterOperator::Equals),
                        values: vec!["pending".to_string()],
                        and: None,
                        or: None,
                    },
                ]),
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("OR"),
            "Expected OR in WHERE, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["active", "pending"]);
    }

    #[test]
    fn test_deeply_nested_and_inside_or() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // OR( AND(status=active, amount>100), AND(status=pending, amount>200) )
        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: None,
                operator: None,
                values: vec![],
                and: None,
                or: Some(vec![
                    QueryFilter {
                        member: None,
                        operator: None,
                        values: vec![],
                        and: Some(vec![
                            QueryFilter {
                                member: Some("orders.status".to_string()),
                                operator: Some(FilterOperator::Equals),
                                values: vec!["active".to_string()],
                                and: None,
                                or: None,
                            },
                            QueryFilter {
                                member: Some("orders.amount".to_string()),
                                operator: Some(FilterOperator::Gt),
                                values: vec!["100".to_string()],
                                and: None,
                                or: None,
                            },
                        ]),
                        or: None,
                    },
                    QueryFilter {
                        member: None,
                        operator: None,
                        values: vec![],
                        and: Some(vec![
                            QueryFilter {
                                member: Some("orders.status".to_string()),
                                operator: Some(FilterOperator::Equals),
                                values: vec!["pending".to_string()],
                                and: None,
                                or: None,
                            },
                            QueryFilter {
                                member: Some("orders.amount".to_string()),
                                operator: Some(FilterOperator::Gt),
                                values: vec!["200".to_string()],
                                and: None,
                                or: None,
                            },
                        ]),
                        or: None,
                    },
                ]),
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("OR"),
            "Expected nested OR, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("AND"),
            "Expected nested AND, got:\n{}",
            result.sql
        );
        assert_eq!(result.params.len(), 4);
    }

    // ─── Mixed dimension + measure filters ─────────────────────────

    #[test]
    fn test_dimension_and_measure_filter_split() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // One dimension filter (→ WHERE) and one measure filter (→ HAVING)
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            filters: vec![
                QueryFilter {
                    member: Some("orders.status".to_string()),
                    operator: Some(FilterOperator::NotEquals),
                    values: vec!["cancelled".to_string()],
                    and: None,
                    or: None,
                },
                QueryFilter {
                    member: Some("orders.total_revenue".to_string()),
                    operator: Some(FilterOperator::Gte),
                    values: vec!["500".to_string()],
                    and: None,
                    or: None,
                },
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("WHERE"),
            "Expected WHERE clause, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("HAVING"),
            "Expected HAVING clause, got:\n{}",
            result.sql
        );
        // WHERE should have the status filter, HAVING should have the revenue filter
        let where_pos = result.sql.find("WHERE").unwrap();
        let having_pos = result.sql.find("HAVING").unwrap();
        assert!(where_pos < having_pos);
    }

    // ─── Multiple filter operators ─────────────────────────────────

    #[test]
    fn test_in_operator_multiple_values() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec![
                    "active".to_string(),
                    "pending".to_string(),
                    "shipped".to_string(),
                ],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("IN"),
            "Expected IN for multi-value equals, got:\n{}",
            result.sql
        );
        assert_eq!(result.params.len(), 3);
    }

    #[test]
    fn test_contains_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Contains),
                values: vec!["act".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("LIKE"),
            "Expected LIKE, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["%act%"]);
    }

    #[test]
    fn test_set_and_not_set_filters() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Set),
                values: vec![],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("IS NOT NULL"),
            "Expected IS NOT NULL, got:\n{}",
            result.sql
        );
        assert!(result.params.is_empty(), "Set filter should have no params");
    }

    #[test]
    fn test_date_range_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.order_date".to_string()),
                operator: Some(FilterOperator::InDateRange),
                values: vec!["2025-01-01".to_string(), "2025-12-31".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains(">="),
            "Expected >= for date range start, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("<="),
            "Expected <= for date range end, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["2025-01-01", "2025-12-31"]);
    }

    // ─── Time dimension with date_range ────────────────────────────

    #[test]
    fn test_time_dimension_with_date_range() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.order_date".to_string(),
                granularity: Some("day".to_string()),
                date_range: Some(vec!["2025-01-01".to_string(), "2025-03-31".to_string()]),
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("date_trunc"),
            "Expected date_trunc, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("WHERE"),
            "Expected WHERE for date range, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["2025-01-01", "2025-03-31"]);
    }

    #[test]
    fn test_time_dimension_multiple_granularities() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Same dimension at different granularities
        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            time_dimensions: vec![
                TimeDimensionQuery {
                    dimension: "orders.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                },
                TimeDimensionQuery {
                    dimension: "orders.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                },
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        let sql_lower = result.sql.to_lowercase();
        assert!(
            sql_lower.contains("'month'") || sql_lower.contains("month"),
            "Expected month granularity, got:\n{}",
            result.sql
        );
        assert!(
            sql_lower.contains("'year'") || sql_lower.contains("year"),
            "Expected year granularity, got:\n{}",
            result.sql
        );
    }

    // ─── Cross-view filters ───────────────────────────────────────

    #[test]
    fn test_filter_on_joined_view() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Filter on customers.name while selecting orders measures
        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["customers.name".to_string()],
            filters: vec![QueryFilter {
                member: Some("customers.name".to_string()),
                operator: Some(FilterOperator::StartsWith),
                values: vec!["A".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN"),
            "Expected JOIN, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("LIKE"),
            "Expected LIKE for startsWith, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["A%"]);
    }

    // ─── Dialect-specific output ──────────────────────────────────

    #[test]
    fn test_mysql_quoting() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::MySQL;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("`orders`"),
            "Expected backtick quoting for MySQL, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_bigquery_quoting() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::BigQuery;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("`orders`"),
            "Expected backtick quoting for BigQuery, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_mysql_param_placeholders() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::MySQL;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["active".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // MySQL uses ? placeholders
        assert!(
            result.sql.contains("?"),
            "Expected ? placeholder for MySQL, got:\n{}",
            result.sql
        );
        assert!(!result.sql.contains("$1"), "Should not have $1 for MySQL");
    }

    // ─── Ungrouped mode ───────────────────────────────────────────

    #[test]
    fn test_ungrouped_query() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ungrouped: true,
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            !result.sql.contains("GROUP BY"),
            "Expected no GROUP BY in ungrouped mode, got:\n{}",
            result.sql
        );
    }

    // ─── Measures only (no dimensions) ────────────────────────────

    #[test]
    fn test_measures_only_no_group_by() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec![
                "orders.count".to_string(),
                "orders.total_revenue".to_string(),
            ],
            dimensions: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // With aggregates but no dimensions, there's nothing to GROUP BY
        assert!(
            !result.sql.contains("GROUP BY"),
            "No GROUP BY needed with only measures, got:\n{}",
            result.sql
        );
        assert!(result.sql.contains("COUNT(*)"));
        assert!(result.sql.contains("SUM("));
    }

    // ─── SQL subquery view ────────────────────────────────────────

    #[test]
    fn test_sql_subquery_view() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "derived".to_string(),
                description: Some("Derived".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: None,
                sql: Some("SELECT * FROM raw_events WHERE valid = true".to_string()),
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "event_type".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "event_type".to_string(),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["derived.count".to_string()],
            dimensions: vec!["derived.event_type".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result
                .sql
                .contains("SELECT * FROM raw_events WHERE valid = true"),
            "Expected subquery in FROM, got:\n{}",
            result.sql
        );
    }

    // ─── Multiple segments ────────────────────────────────────────

    #[test]
    fn test_multiple_segments() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "status".to_string(),
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
                segments: vec![
                    Segment {
                        name: "is_active".to_string(),
                        expr: "status = 'active'".to_string(),
                        description: None,
                        inherits_from: None,
                        meta: None,
                    },
                    Segment {
                        name: "is_high_value".to_string(),
                        expr: "amount > 1000".to_string(),
                        description: None,
                        inherits_from: None,
                        meta: None,
                    },
                ],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            segments: vec![
                "orders.is_active".to_string(),
                "orders.is_high_value".to_string(),
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("active"),
            "Expected active segment, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("1000"),
            "Expected high_value segment, got:\n{}",
            result.sql
        );
        // Both should be in WHERE, combined with AND
        assert!(result.sql.contains("WHERE"));
    }

    // ─── Error cases ──────────────────────────────────────────────

    #[test]
    fn test_nonexistent_member_error() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.nonexistent_measure".to_string()],
            dimensions: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request);
        assert!(result.is_err(), "Expected error for nonexistent measure");
    }

    #[test]
    fn test_nonexistent_view_error() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["nonexistent_view.count".to_string()],
            dimensions: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request);
        assert!(result.is_err(), "Expected error for nonexistent view");
    }

    #[test]
    fn test_empty_query_error() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest::new();
        let result = gen.generate(&request);
        assert!(result.is_err(), "Expected error for empty query");
    }

    #[test]
    fn test_nonexistent_segment_error() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            segments: vec!["orders.nonexistent_segment".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request);
        assert!(result.is_err(), "Expected error for nonexistent segment");
    }

    // ─── Multi-hop transitive join ────────────────────────────────

    #[test]
    fn test_three_view_transitive_join() {
        // A -> B -> C: query dims from A and C, measures from B
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "departments".to_string(),
                    description: Some("Departments".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("hr.departments".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "dept".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("dept_id".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![Dimension {
                        name: "dept_name".to_string(),
                        dimension_type: DimensionType::String,
                        description: None,
                        expr: "dept_name".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                        sub_query: None,
                        segmentable: None,
                        inherits_from: None,
                        meta: None,
                    }],
                    measures: None,
                    segments: vec![],
                    pre_aggregations: None,
                    refresh_key: None,
                    meta: None,
                },
                View {
                    name: "employees".to_string(),
                    description: Some("Employees".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("hr.employees".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "emp".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("emp_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "dept".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("dept_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![Dimension {
                        name: "emp_name".to_string(),
                        dimension_type: DimensionType::String,
                        description: None,
                        expr: "emp_name".to_string(),
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
                        name: "headcount".to_string(),
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
                },
                View {
                    name: "timesheets".to_string(),
                    description: Some("Timesheets".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("hr.timesheets".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "timesheet".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("ts_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "emp".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("emp_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![Dimension {
                        name: "hours".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "hours".to_string(),
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
                        name: "total_hours".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("hours".to_string()),
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
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Query spans departments -> employees -> timesheets
        let request = QueryRequest {
            measures: vec!["timesheets.total_hours".to_string()],
            dimensions: vec!["departments.dept_name".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Should contain two JOINs for the transitive path
        let join_count = result.sql.matches("JOIN").count();
        assert!(
            join_count >= 2,
            "Expected at least 2 JOINs for transitive path, got {} in:\n{}",
            join_count,
            result.sql
        );
        assert!(
            result.sql.contains("departments"),
            "Expected departments in SQL"
        );
        assert!(
            result.sql.contains("timesheets"),
            "Expected timesheets in SQL"
        );
        assert!(
            result.sql.contains("employees"),
            "Expected employees as intermediate in SQL"
        );
    }

    // ─── Measure with filters (CASE WHEN) ─────────────────────────

    #[test]
    fn test_measure_with_inline_filter() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: Some("Events".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "event_type".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "event_type".to_string(),
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
                        name: "total_events".to_string(),
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
                    },
                    Measure {
                        name: "click_count".to_string(),
                        measure_type: MeasureType::Count,
                        description: None,
                        expr: None,
                        original_expr: None,
                        filters: Some(vec![crate::schema::models::MeasureFilter {
                            expr: "event_type = 'click'".to_string(),
                            original_expr: None,
                            description: None,
                        }]),
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: None,
                        shift: None,
                        meta: None,
                    },
                ]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec![
                "events.total_events".to_string(),
                "events.click_count".to_string(),
            ],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("COUNT(*)"),
            "Expected unfiltered COUNT, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("CASE WHEN") || result.sql.contains("case when"),
            "Expected CASE WHEN for filtered measure, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("click"),
            "Expected click filter in CASE WHEN"
        );
    }

    // ─── Custom measure type ──────────────────────────────────────

    #[test]
    fn test_custom_measure() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "region".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "region".to_string(),
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
                    name: "avg_order_value".to_string(),
                    measure_type: MeasureType::Custom,
                    description: None,
                    expr: Some("SUM(total) / NULLIF(COUNT(*), 0)".to_string()),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.avg_order_value".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("SUM(total) / NULLIF(COUNT(*), 0)"),
            "Expected custom expression verbatim, got:\n{}",
            result.sql
        );
    }

    // ─── Order by both dimension and measure ──────────────────────

    #[test]
    fn test_order_by_multiple_columns() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            order: vec![
                OrderBy {
                    id: "orders.status".to_string(),
                    desc: false,
                },
                OrderBy {
                    id: "orders.total_revenue".to_string(),
                    desc: true,
                },
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("ORDER BY"), "Expected ORDER BY");
        assert!(result.sql.contains("ASC"), "Expected ASC");
        assert!(result.sql.contains("DESC"), "Expected DESC");
    }

    // ─── Column metadata ──────────────────────────────────────────

    #[test]
    fn test_column_metadata_correct() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec![
                "orders.count".to_string(),
                "orders.total_revenue".to_string(),
            ],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert_eq!(result.columns.len(), 3);

        let dim_col = result
            .columns
            .iter()
            .find(|c| c.member == "orders.status")
            .unwrap();
        assert_eq!(dim_col.kind, ColumnKind::Dimension);

        let measure_col = result
            .columns
            .iter()
            .find(|c| c.member == "orders.count")
            .unwrap();
        assert_eq!(measure_col.kind, ColumnKind::Measure);

        // Aliases should use double-underscore convention
        assert_eq!(dim_col.alias, "orders__status");
        assert_eq!(measure_col.alias, "orders__count");
    }

    #[test]
    fn test_best_base_view_selection() {
        // With A-B-C chain, querying dims from A and C with measures from B,
        // B should be picked as base (shorter total tree)
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "a".to_string(),
                    description: Some("A".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("a".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "a_entity".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("id".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
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
                    measures: None,
                    segments: vec![],
                    pre_aggregations: None,
                    refresh_key: None,
                    meta: None,
                },
                View {
                    name: "b".to_string(),
                    description: Some("B".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("b".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "b_entity".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "a_entity".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("a_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
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
                },
                View {
                    name: "c".to_string(),
                    description: Some("C".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("c".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "c_entity".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "b_entity".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("b_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
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
                    measures: None,
                    segments: vec![],
                    pre_aggregations: None,
                    refresh_key: None,
                    meta: None,
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Query spans A, B, C. B is in the middle and should be chosen as base.
        let request = QueryRequest {
            measures: vec!["b.count".to_string()],
            dimensions: vec!["a.id".to_string(), "c.id".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // B as base means FROM b, then joins to a and c (2 joins total).
        // B should be chosen as base since it's in the middle.
        // The SQL may use CTEs if fan-out is detected, but b should still be
        // the base view in either case.
        assert!(
            result.sql.contains("b AS \"b\""),
            "Expected 'b' as base view, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_domo_quoting() {
        let (evaluator, join_graph, layer) = make_test_engine();
        let dialect = Dialect::Domo;
        let gen = SqlGenerator::new(&evaluator, &join_graph, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.status".into()],
            measures: vec!["orders.total_revenue".into()],
            filters: vec![],
            segments: vec![],
            time_dimensions: vec![],
            order: vec![],
            limit: Some(10),
            offset: None,
            timezone: None,
            ungrouped: false,
            through: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Domo uses backtick quoting like MySQL
        assert!(
            result.sql.contains("`orders`"),
            "Expected backtick-quoted identifiers for Domo, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("LIMIT 10"),
            "Expected LIMIT clause, got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains("\"orders\""),
            "Should not use double-quote identifiers for Domo, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_domo_param_placeholders() {
        let (evaluator, join_graph, layer) = make_test_engine();
        let dialect = Dialect::Domo;
        let gen = SqlGenerator::new(&evaluator, &join_graph, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.status".into()],
            measures: vec!["orders.total_revenue".into()],
            filters: vec![QueryFilter {
                member: Some("orders.status".into()),
                operator: Some(FilterOperator::Equals),
                values: vec!["active".into()],
                and: None,
                or: None,
            }],
            segments: vec![],
            time_dimensions: vec![],
            order: vec![],
            limit: None,
            offset: None,
            timezone: None,
            ungrouped: false,
            through: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Domo uses ? placeholders with params
        assert!(
            result.sql.contains("= ?"),
            "Expected ? placeholder for Domo, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["active"]);
    }

    #[test]
    fn test_domo_date_trunc() {
        let dialect = Dialect::Domo;
        // Domo uses MySQL-style DATE_FORMAT for date truncation
        let result = dialect.date_trunc("month", "`my_date`");
        assert!(
            result.contains("DATE_FORMAT"),
            "Expected DATE_FORMAT for Domo date_trunc, got: {}",
            result
        );
        assert!(
            result.contains("%Y-%m-01"),
            "Expected month format pattern, got: {}",
            result
        );
    }

    #[test]
    fn test_domo_date_filter_inlined() {
        let (evaluator, join_graph, layer) = make_test_engine();
        let dialect = Dialect::Domo;
        let gen = SqlGenerator::new(&evaluator, &join_graph, &dialect, &layer);

        let request = QueryRequest {
            dimensions: vec!["orders.status".into()],
            measures: vec!["orders.count".into()],
            filters: vec![QueryFilter {
                member: Some("orders.status".into()),
                operator: Some(FilterOperator::Gte),
                values: vec!["2026-02-01".into()],
                and: None,
                or: None,
            }],
            segments: vec![],
            time_dimensions: vec![],
            order: vec![],
            limit: None,
            offset: None,
            timezone: None,
            ungrouped: false,
            through: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Domo uses ? placeholders with params (substitution happens in oxy-internal)
        assert!(
            result.sql.contains(">= ?"),
            "Expected ? placeholder for Domo, got:\n{}",
            result.sql
        );
        assert_eq!(result.params, vec!["2026-02-01"]);
    }

    #[test]
    fn test_count_distinct_approx() {
        let (_eval, _jg, _layer) = make_test_engine();
        let dialect = Dialect::BigQuery;

        // Build a layer with count_distinct_approx measure
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: Some("Events".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "event_type".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "event_type".to_string(),
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
                    name: "unique_users".to_string(),
                    measure_type: MeasureType::CountDistinctApprox,
                    description: None,
                    expr: Some("user_id".to_string()),
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
            }],
            None,
        );
        let jg2 = JoinGraph::build(&layer.views).unwrap();
        let eval2 = SchemaEvaluator::new(&layer, &jg2).unwrap();
        let gen2 = SqlGenerator::new(&eval2, &jg2, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["events.unique_users".to_string()],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };
        let result = gen2.generate(&request).unwrap();
        assert!(
            result.sql.contains("APPROX_COUNT_DISTINCT"),
            "Expected APPROX_COUNT_DISTINCT for BigQuery, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_number_passthrough_measure() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "stats".to_string(),
                description: Some("Stats".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("stats".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "category".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "category".to_string(),
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
                    name: "ratio".to_string(),
                    measure_type: MeasureType::Number,
                    description: None,
                    expr: Some("SUM(a) / NULLIF(SUM(b), 0)".to_string()),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["stats.ratio".to_string()],
            dimensions: vec!["stats.category".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // Number measure should pass through the expression as-is
        assert!(
            result.sql.contains("SUM(a) / NULLIF(SUM(b), 0)"),
            "Number measure should pass through expression, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_on_the_date_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.order_date".to_string()),
                operator: Some(FilterOperator::OnTheDate),
                values: vec!["2024-01-15".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // onTheDate expands to >= date AND < next_day
        assert!(
            result.sql.contains(">= $1"),
            "Expected >= for onTheDate, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("< $2"),
            "Expected < for onTheDate next day, got:\n{}",
            result.sql
        );
        assert_eq!(result.params[0], "2024-01-15");
        assert_eq!(result.params[1], "2024-01-16");
    }

    #[test]
    fn test_rolling_window_cumulative() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "sales".to_string(),
                description: Some("Sales".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("sales".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "sale_date".to_string(),
                    dimension_type: DimensionType::Date,
                    description: None,
                    expr: "sale_date".to_string(),
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
                    name: "cumulative_revenue".to_string(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("amount".to_string()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: Some(RollingWindow {
                        trailing: Some("unbounded".to_string()),
                        leading: None,
                        offset: None,
                    }),
                    inherits_from: None,
                    drivers: None,
                    shift: None,
                    meta: None,
                }]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["sales.cumulative_revenue".to_string()],
            dimensions: vec!["sales.sale_date".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("OVER"),
            "Expected OVER clause for rolling window, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("UNBOUNDED PRECEDING"),
            "Expected UNBOUNDED PRECEDING, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("CURRENT ROW"),
            "Expected CURRENT ROW, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_measure_to_measure_reference() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "status".to_string(),
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
                        name: "total_revenue".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("amount".to_string()),
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: None,
                        shift: None,
                        meta: None,
                    },
                    Measure {
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
                    },
                    Measure {
                        name: "avg_order_value".to_string(),
                        measure_type: MeasureType::Number,
                        description: None,
                        expr: Some(
                            "{{orders.total_revenue}} / NULLIF({{orders.count}}, 0)".to_string(),
                        ),
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: None,
                        shift: None,
                        meta: None,
                    },
                ]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.avg_order_value".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // The {{orders.total_revenue}} should resolve to SUM(amount) and {{orders.count}} to COUNT(*)
        assert!(
            result.sql.contains("SUM("),
            "Expected SUM from resolved measure ref, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("COUNT("),
            "Expected COUNT from resolved measure ref, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("NULLIF"),
            "Expected NULLIF preserved, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_subquery_dimension() {
        // Build a schema with orders having a subquery dimension referencing customers
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "customers".to_string(),
                    description: Some("Customers".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("customers".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "customer".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("customer_id".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![
                        Dimension {
                            name: "customer_id".to_string(),
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
                        },
                        Dimension {
                            name: "order_count".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "orders.count".to_string(),
                            original_expr: None,
                            samples: None,
                            synonyms: None,
                            primary_key: None,
                            sub_query: Some(true),
                            segmentable: None,
                            inherits_from: None,
                            meta: None,
                        },
                    ],
                    measures: Some(vec![Measure {
                        name: "total_customers".to_string(),
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
                },
                View {
                    name: "orders".to_string(),
                    description: Some("Orders".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("orders".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "customer".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: Some("customer_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![
                        Dimension {
                            name: "order_id".to_string(),
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
                        },
                        Dimension {
                            name: "customer_id".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "customer_id".to_string(),
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
                },
            ],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec![],
            dimensions: vec![
                "customers.customer_id".to_string(),
                "customers.order_count".to_string(),
            ],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // Subquery dimension should generate a correlated subquery
        assert!(
            result.sql.contains("SELECT COUNT(*)"),
            "Expected correlated subquery with COUNT(*), got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("FROM orders AS"),
            "Expected FROM orders in subquery, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_relative_date_range_parsing() {
        use super::parse_relative_date_range;

        // "today" should return same date for both bounds
        let result = parse_relative_date_range("today").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], result[1]);

        // "yesterday" should return previous day
        let result = parse_relative_date_range("yesterday").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], result[1]);

        // "last 7 days" should return a 7-day range
        let result = parse_relative_date_range("last 7 days").unwrap();
        assert_eq!(result.len(), 2);
        // Start should be before end
        assert!(result[0] < result[1]);

        // Unknown string should return None
        let result = parse_relative_date_range("some random string");
        assert!(result.is_none());
    }

    #[test]
    fn test_window_interval_parsing() {
        assert_eq!(parse_window_interval("7 days"), "7");
        assert_eq!(parse_window_interval("1 month"), "1");
        assert_eq!(parse_window_interval("3 rows"), "3");
        // Fallback for unparseable
        assert_eq!(parse_window_interval("unbounded"), "unbounded");
    }

    // ─── Additional coverage tests ──────────────────────────────────

    #[test]
    fn test_timezone_conversion() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            time_dimensions: vec![TimeDimensionQuery {
                dimension: "orders.order_date".to_string(),
                granularity: Some("day".to_string()),
                date_range: None,
            }],
            timezone: Some("America/New_York".to_string()),
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("AT TIME ZONE"),
            "Expected AT TIME ZONE for Postgres timezone conversion, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("America/New_York"),
            "Expected timezone name in SQL, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_time_dimension_granularity_combinations() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            time_dimensions: vec![
                TimeDimensionQuery {
                    dimension: "orders.order_date".to_string(),
                    granularity: Some("month".to_string()),
                    date_range: None,
                },
                TimeDimensionQuery {
                    dimension: "orders.order_date".to_string(),
                    granularity: Some("year".to_string()),
                    date_range: None,
                },
            ],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        let sql_lower = result.sql.to_lowercase();
        // Both DATE_TRUNC calls should be present
        assert!(
            sql_lower.contains("date_trunc('month'"),
            "Expected date_trunc for month, got:\n{}",
            result.sql
        );
        assert!(
            sql_lower.contains("date_trunc('year'"),
            "Expected date_trunc for year, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_rolling_window_trailing_interval() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "sales".to_string(),
                description: Some("Sales".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("sales".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "sale_date".to_string(),
                    dimension_type: DimensionType::Date,
                    description: None,
                    expr: "sale_date".to_string(),
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
                    name: "rolling_sum".to_string(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("amount".to_string()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: Some(RollingWindow {
                        trailing: Some("7".to_string()),
                        leading: None,
                        offset: None,
                    }),
                    inherits_from: None,
                    drivers: None,
                    shift: None,
                    meta: None,
                }]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["sales.rolling_sum".to_string()],
            dimensions: vec!["sales.sale_date".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("7 PRECEDING"),
            "Expected 7 PRECEDING in window frame, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("OVER"),
            "Expected OVER clause, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_count_distinct_approx_clickhouse() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: Some("Events".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "event_type".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "event_type".to_string(),
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
                    name: "unique_users".to_string(),
                    measure_type: MeasureType::CountDistinctApprox,
                    description: None,
                    expr: Some("user_id".to_string()),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::ClickHouse;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["events.unique_users".to_string()],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("uniqHLL12"),
            "Expected uniqHLL12 for ClickHouse approx count distinct, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_count_distinct_approx_fallback() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: Some("Events".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "event_type".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "event_type".to_string(),
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
                    name: "unique_users".to_string(),
                    measure_type: MeasureType::CountDistinctApprox,
                    description: None,
                    expr: Some("user_id".to_string()),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::SQLite;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["events.unique_users".to_string()],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("COUNT(DISTINCT"),
            "Expected COUNT(DISTINCT ...) fallback for SQLite, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_starts_with_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::StartsWith),
                values: vec!["act".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("LIKE"),
            "Expected LIKE for StartsWith filter, got:\n{}",
            result.sql
        );
        assert_eq!(
            result.params,
            vec!["act%"],
            "StartsWith should append % to value"
        );
    }

    #[test]
    fn test_ends_with_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::EndsWith),
                values: vec!["ive".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("LIKE"),
            "Expected LIKE for EndsWith filter, got:\n{}",
            result.sql
        );
        assert_eq!(
            result.params,
            vec!["%ive"],
            "EndsWith should prepend % to value"
        );
    }

    #[test]
    fn test_not_contains_filter() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::NotContains),
                values: vec!["cancel".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("NOT LIKE"),
            "Expected NOT LIKE for NotContains filter, got:\n{}",
            result.sql
        );
        assert_eq!(
            result.params,
            vec!["%cancel%"],
            "NotContains should wrap value with %"
        );
    }

    #[test]
    fn test_composite_key_join() {
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "order_items".to_string(),
                    description: Some("Order Items".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("order_items".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "order_line".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: None,
                        keys: Some(vec!["order_id".to_string(), "line_num".to_string()]),
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![
                        Dimension {
                            name: "order_id".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "order_id".to_string(),
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
                            name: "line_num".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "line_num".to_string(),
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
                            name: "product".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "product".to_string(),
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
                },
                View {
                    name: "returns".to_string(),
                    description: Some("Returns".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("returns".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "return_item".to_string(),
                            entity_type: EntityType::Primary,
                            lifespan: None,
                            description: None,
                            key: Some("return_id".to_string()),
                            keys: None,
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                        Entity {
                            name: "order_line".to_string(),
                            entity_type: EntityType::Foreign,
                            lifespan: None,
                            description: None,
                            key: None,
                            keys: Some(vec!["order_id".to_string(), "line_num".to_string()]),
                            inherits_from: None,
                            meta: None,
                            parent: None,
                        },
                    ],
                    dimensions: vec![
                        Dimension {
                            name: "return_id".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "return_id".to_string(),
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
                            name: "order_id".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "order_id".to_string(),
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
                            name: "line_num".to_string(),
                            dimension_type: DimensionType::Number,
                            description: None,
                            expr: "line_num".to_string(),
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
                            name: "reason".to_string(),
                            dimension_type: DimensionType::String,
                            description: None,
                            expr: "reason".to_string(),
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
                        name: "return_count".to_string(),
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
                },
            ],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["returns.return_count".to_string()],
            dimensions: vec!["order_items.product".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // Composite key join should have AND connecting the two key conditions
        assert!(
            result.sql.contains("AND"),
            "Expected AND for composite key join, got:\n{}",
            result.sql
        );
        // Both key columns should appear in the join condition
        assert!(
            result.sql.contains("order_id") && result.sql.contains("line_num"),
            "Expected both composite key columns in join, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_ungrouped_with_joins() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["customers.name".to_string()],
            ungrouped: true,
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            !result.sql.contains("GROUP BY"),
            "Expected no GROUP BY in ungrouped mode, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("JOIN"),
            "Expected JOIN even in ungrouped mode, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("customers"),
            "Expected customers table in join, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_measure_with_multiple_filters() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: Some("Events".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "category".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "category".to_string(),
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
                    name: "filtered_count".to_string(),
                    measure_type: MeasureType::Count,
                    description: None,
                    expr: None,
                    original_expr: None,
                    filters: Some(vec![
                        MeasureFilter {
                            expr: "status = 'active'".to_string(),
                            original_expr: None,
                            description: None,
                        },
                        MeasureFilter {
                            expr: "region = 'US'".to_string(),
                            original_expr: None,
                            description: None,
                        },
                    ]),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["events.filtered_count".to_string()],
            dimensions: vec!["events.category".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("CASE WHEN") || result.sql.contains("case when"),
            "Expected CASE WHEN for filtered measure, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("AND"),
            "Expected AND combining both filter conditions, got:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("active") && result.sql.contains("US"),
            "Expected both filter values in CASE WHEN, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_variable_passthrough_in_expression() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "org_orders".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "CASE WHEN org_id = {{variables.org_id}} THEN 'yes' ELSE 'no' END"
                        .to_string(),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.org_orders".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("{{variables.org_id}}"),
            "Expected {{variables.org_id}} preserved in output SQL, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_table_self_reference_in_measure() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "status".to_string(),
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
                    name: "weighted_total".to_string(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("{{TABLE}}.amount * {{TABLE}}.weight".to_string()),
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
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.weighted_total".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("\"orders\".amount") && result.sql.contains("\"orders\".weight"),
            "Expected {{TABLE}} resolved to view alias \"orders\", got:\n{}",
            result.sql
        );
        assert!(
            !result.sql.contains("{{TABLE}}"),
            "Expected no raw {{{{TABLE}}}} in output SQL, got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_qualify_double_quoted_columns_in_multi_view_join() {
        // Regression test for ambiguous column references when multiple views have
        // the same column name. Double-quoted identifiers in expressions must be
        // qualified with the table alias.
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "macro".to_string(),
                    description: Some("Macro tracking".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("macro.csv".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "date_entity".to_string(),
                        entity_type: EntityType::Primary,
                        lifespan: None,
                        description: None,
                        key: Some("Date".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![Dimension {
                        name: "month".to_string(),
                        dimension_type: DimensionType::Datetime,
                        description: None,
                        // Double-quoted identifier that must be qualified
                        expr: "date_trunc('month', \"Date\")".to_string(),
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
                        name: "avg_calories".to_string(),
                        measure_type: MeasureType::Average,
                        description: None,
                        expr: Some("Calories".to_string()),
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
                },
                View {
                    name: "cardio".to_string(),
                    description: Some("Cardio tracking".to_string()),
                    label: None,
                    datasource: None,
                    dialect: None,
                    table: Some("cardio.csv".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "date_entity".to_string(),
                        entity_type: EntityType::Foreign,
                        lifespan: None,
                        description: None,
                        key: Some("Date".to_string()),
                        keys: None,
                        inherits_from: None,
                        meta: None,
                        parent: None,
                    }],
                    dimensions: vec![Dimension {
                        name: "month".to_string(),
                        dimension_type: DimensionType::Datetime,
                        description: None,
                        // Same double-quoted column name - would be ambiguous without qualification
                        expr: "date_trunc('month', \"Date\")".to_string(),
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
                        name: "session_count".to_string(),
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
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::DuckDB;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Query dimensions from both views - this triggers the join
        let request = QueryRequest {
            measures: vec![
                "macro.avg_calories".to_string(),
                "cardio.session_count".to_string(),
            ],
            dimensions: vec!["macro.month".to_string(), "cardio.month".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();

        // The SQL should qualify "Date" with the table alias in both expressions
        // Check that we don't have bare unqualified "Date" in the dimension expressions
        // within the __dim_spine CTE
        assert!(
            !result.sql.contains("date_trunc('month', \"Date\")"),
            "Expected double-quoted columns to be qualified with table alias. Got unqualified \"Date\" in:\n{}",
            result.sql
        );

        // Verify that the table-qualified version exists
        assert!(
            result.sql.contains("\"macro\".\"Date\"") || result.sql.contains("\"cardio\".\"Date\""),
            "Expected table-qualified column references like \"macro\".\"Date\" in:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_qualify_skips_already_qualified_double_quoted_identifiers() {
        // If a dimension expr already contains a fully-qualified reference like
        // "other_schema"."col", we should NOT re-qualify "other_schema" with the view alias.
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "region".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    // Already fully qualified with schema — should be left alone entirely
                    expr: "\"my_schema\".\"my_col\"".to_string(),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::DuckDB;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();

        // "my_schema" should NOT be prefixed with the view alias
        assert!(
            !result.sql.contains("\"orders\".\"my_schema\""),
            "Should not qualify schema qualifiers with view alias. Got:\n{}",
            result.sql
        );
        // The original qualified reference should be preserved
        assert!(
            result.sql.contains("\"my_schema\".\"my_col\""),
            "Expected original qualified reference to be preserved. Got:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_qualify_uses_dialect_quoting_for_already_qualified() {
        // When a double-quoted identifier is already qualified (preceded by dot),
        // it should use dialect.quote_identifier() — not hardcoded double quotes.
        // For MySQL dialect, this means backticks.
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    // A bare double-quoted column that will be qualified
                    expr: "UPPER(\"Status\")".to_string(),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();

        // Test with MySQL dialect — should use backticks
        let dialect = Dialect::MySQL;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();

        // MySQL should use backtick quoting for the qualified column
        assert!(
            result.sql.contains("`orders`.`Status`"),
            "Expected MySQL backtick quoting for qualified column. Got:\n{}",
            result.sql
        );
        // Should NOT have double quotes (that's Postgres/DuckDB style)
        assert!(
            !result.sql.contains("\"Status\""),
            "Should not have double-quoted identifiers in MySQL output. Got:\n{}",
            result.sql
        );
    }

    // ─── Operator precedence for measure references ──────────────

    #[test]
    fn test_measure_reference_precedence() {
        // net_mrr = total_mrr + expansion - churned_mrr
        // annualized = {{revenue.net_mrr}} * 12
        // Without parens: SUM(a) + SUM(b) - SUM(c) * 12 (wrong)
        // With parens:   (SUM(a) + SUM(b) - SUM(c)) * 12 (correct)
        let layer = SemanticLayer::new(
            vec![View {
                name: "revenue".to_string(),
                description: Some("Revenue".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.revenue".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "month".to_string(),
                    dimension_type: DimensionType::Date,
                    description: None,
                    expr: "month".to_string(),
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
                        name: "total_mrr".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("mrr".to_string()),
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
                        name: "expansion".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("expansion_amount".to_string()),
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
                        name: "churned_mrr".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("churned_amount".to_string()),
                        original_expr: None,
                        filters: Some(vec![MeasureFilter {
                            expr: "status = 'churned'".to_string(),
                            original_expr: None,
                            description: None,
                        }]),
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        meta: None,
                        drivers: None,
                        shift: None,
                    },
                    Measure {
                        name: "net_mrr".to_string(),
                        measure_type: MeasureType::Number,
                        description: None,
                        expr: Some(
                            "{{revenue.total_mrr}} + {{revenue.expansion}} - {{revenue.churned_mrr}}"
                                .to_string(),
                        ),
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
                        name: "annualized_mrr".to_string(),
                        measure_type: MeasureType::Number,
                        description: None,
                        expr: Some("{{revenue.net_mrr}} * 12".to_string()),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Test composite measure: net_mrr = total + expansion - churned
        let request = QueryRequest {
            measures: vec!["revenue.net_mrr".to_string()],
            dimensions: vec!["revenue.month".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Each component measure should be wrapped in parens
        assert!(
            result.sql.contains("(SUM("),
            "Measure references should be wrapped in parens. Got:\n{}",
            result.sql
        );
        // churned_mrr is filtered, should have COALESCE
        assert!(
            result.sql.contains("COALESCE(SUM("),
            "Filtered SUM should be wrapped in COALESCE. Got:\n{}",
            result.sql
        );

        // Test nested measure reference: annualized = net_mrr * 12
        let request2 = QueryRequest {
            measures: vec!["revenue.annualized_mrr".to_string()],
            dimensions: vec!["revenue.month".to_string()],
            ..QueryRequest::new()
        };

        let result2 = gen.generate(&request2).unwrap();
        // The * 12 should apply to the entire net_mrr expression, not just churned_mrr
        assert!(
            result2.sql.contains(") * 12"),
            "Multiplication should apply to entire wrapped expression. Got:\n{}",
            result2.sql
        );
    }

    // ─── Operator precedence for dimension references ────────────

    #[test]
    fn test_dimension_reference_precedence() {
        // margin = "price - discount"
        // A measure expr using {{view.margin}} * 100 should become
        // ("v"."price" - "v"."discount") * 100, NOT "v"."price" - "v"."discount" * 100
        let layer = SemanticLayer::new(
            vec![View {
                name: "products".to_string(),
                description: Some("Products".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.products".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![
                    Dimension {
                        name: "price".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "price".to_string(),
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
                        name: "discount".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "discount".to_string(),
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
                        name: "margin".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "price - discount".to_string(),
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
                    name: "margin_pct".to_string(),
                    measure_type: MeasureType::Number,
                    description: None,
                    expr: Some("{{products.margin}} * 100".to_string()),
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
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["products.margin_pct".to_string()],
            dimensions: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Must have parens around the resolved dimension expression
        // Correct: (...price - ...discount) * 100
        // Wrong:   ...price - ...discount * 100
        assert!(
            result.sql.contains(") * 100"),
            "Dimension reference should be wrapped in parens for precedence. Got:\n{}",
            result.sql
        );
        // The opening paren should come before price
        assert!(
            result.sql.contains("(\"products\".\"price\""),
            "Opening paren should wrap the dimension expression. Got:\n{}",
            result.sql
        );
    }

    // ─── COALESCE for filtered measures ──────────────────────────

    #[test]
    fn test_filtered_sum_coalesce() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "status".to_string(),
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
                        name: "total_revenue".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("amount".to_string()),
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
                        name: "refunded_revenue".to_string(),
                        measure_type: MeasureType::Sum,
                        description: None,
                        expr: Some("amount".to_string()),
                        original_expr: None,
                        filters: Some(vec![MeasureFilter {
                            expr: "status = 'refunded'".to_string(),
                            original_expr: None,
                            description: None,
                        }]),
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
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Filtered SUM should have COALESCE
        let request = QueryRequest {
            measures: vec!["orders.refunded_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("COALESCE(SUM("),
            "Filtered SUM should be wrapped in COALESCE. Got:\n{}",
            result.sql
        );

        // Unfiltered SUM should NOT have COALESCE
        let request2 = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result2 = gen.generate(&request2).unwrap();
        assert!(
            !result2.sql.contains("COALESCE"),
            "Unfiltered SUM should NOT have COALESCE. Got:\n{}",
            result2.sql
        );
    }

    #[test]
    fn test_filtered_rolling_window_coalesce_placement() {
        // COALESCE must wrap the entire window expression:
        //   COALESCE(SUM(...) OVER (...), 0)
        // NOT the inner aggregate:
        //   COALESCE(SUM(...), 0) OVER (...)  ← invalid SQL
        let layer = SemanticLayer::new(
            vec![View {
                name: "metrics".to_string(),
                description: Some("Metrics".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.metrics".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "date".to_string(),
                    dimension_type: DimensionType::Date,
                    description: None,
                    expr: "date".to_string(),
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
                    name: "filtered_cumulative".to_string(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("value".to_string()),
                    original_expr: None,
                    filters: Some(vec![MeasureFilter {
                        expr: "active = true".to_string(),
                        original_expr: None,
                        description: None,
                    }]),
                    samples: None,
                    synonyms: None,
                    rolling_window: Some(RollingWindow {
                        trailing: Some("unbounded".to_string()),
                        leading: None,
                        offset: None,
                    }),
                    inherits_from: None,
                    meta: None,
                    drivers: None,
                    shift: None,
                }]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["metrics.filtered_cumulative".to_string()],
            dimensions: vec!["metrics.date".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // COALESCE must wrap the entire OVER expression
        assert!(
            result.sql.contains("COALESCE(SUM(") && result.sql.contains("OVER ("),
            "Should have COALESCE wrapping the window function. Got:\n{}",
            result.sql
        );
        // The OVER should come BEFORE the closing of COALESCE
        let coalesce_pos = result.sql.find("COALESCE(SUM(").unwrap();
        let over_pos = result.sql.find("OVER (").unwrap();
        let coalesce_end = result.sql[coalesce_pos..].find(", 0)").unwrap() + coalesce_pos;
        assert!(
            over_pos < coalesce_end,
            "OVER clause should be inside COALESCE, not after it. Got:\n{}",
            result.sql
        );
    }

    // ─── Backtick-quoted identifier qualification ────────────────

    #[test]
    fn test_backtick_quoted_identifier_qualification() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "status".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    // Expression uses backtick-quoted identifier
                    expr: "COALESCE(`Status Column`, 'unknown')".to_string(),
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
                    meta: None,
                    drivers: None,
                    shift: None,
                }]),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();

        // Test with BigQuery dialect (backtick quoting)
        let dialect = Dialect::BigQuery;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // The backtick-quoted identifier should be qualified with the view alias
        assert!(
            result.sql.contains("`orders`.`Status Column`"),
            "Backtick-quoted identifier should be qualified. Got:\n{}",
            result.sql
        );
    }

    #[test]
    fn quotes_table_segments_starting_with_digit() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::DuckDB;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);
        assert_eq!(
            gen.quote_table_name("main.20250816_tamalpa_headlands_50k"),
            r#"main."20250816_tamalpa_headlands_50k""#
        );
        assert_eq!(gen.quote_table_name("20250816_foo"), r#""20250816_foo""#);
    }

    #[test]
    fn leaves_plain_identifiers_unquoted() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::DuckDB;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);
        assert_eq!(gen.quote_table_name("main.oxymart"), "main.oxymart");
    }

    #[test]
    fn still_passes_through_prequoted_values() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::DuckDB;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);
        assert_eq!(
            gen.quote_table_name(r#""main"."already_quoted""#),
            r#""main"."already_quoted""#
        );
    }

    #[test]
    fn quotes_snowflake_identifier_with_leading_digit_uppercased() {
        let (eval, jg, layer) = make_test_engine();
        let dialect = Dialect::Snowflake;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);
        // Snowflake's quote_identifier uppercases the content.
        assert_eq!(gen.quote_table_name("20250816_foo"), r#""20250816_FOO""#);
    }

    // -----------------------------------------------------------------------
    // Issue #55: cross-entity references in view-definition exprs must
    // trigger JOINs, exactly like the same reference at query level.
    // -----------------------------------------------------------------------

    fn engine_from_yaml(yamls: &[&str]) -> (SchemaEvaluator, JoinGraph, SemanticLayer) {
        let parser = crate::schema::parser::SchemaParser::new();
        let views: Vec<View> = yamls
            .iter()
            .enumerate()
            .map(|(i, y)| {
                parser
                    .parse_view_str(y, &format!("<test_view_{}>", i))
                    .expect("parse test view")
            })
            .collect();
        let layer = SemanticLayer::new(views, None);
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        (eval, jg, layer)
    }

    const ISSUE_55_ORDERS: &str = r#"
name: orders
table: public.orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: flag_from_other_view
    type: boolean
    expr: "{{order_flags.is_flagged}}"
measures:
  - name: total_orders
    type: count_distinct
    expr: ORDER_ID
  - name: flagged_order_sum
    type: number
    expr: "SUM(CASE WHEN {{order_flags.is_flagged}} THEN 1 ELSE 0 END)"
  - name: total_flagged_orders
    type: count_distinct
    expr: ORDER_ID
    filters:
      - expr: "{{order_flags.is_flagged}}"
"#;

    const ISSUE_55_ORDER_FLAGS: &str = r#"
name: order_flags
table: public.order_flags
entities:
  - name: order
    type: foreign
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: is_flagged
    type: boolean
    expr: IS_FLAGGED
"#;

    #[test]
    fn test_issue_55_cross_view_ref_in_dimension_expr_joins() {
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, ISSUE_55_ORDER_FLAGS]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["orders.flag_from_other_view".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.order_flags"),
            "cross-view ref in dimension expr must join order_flags:\n{}",
            result.sql
        );
        assert!(result.sql.contains("\"order_flags\".\"IS_FLAGGED\""));
    }

    #[test]
    fn test_issue_55_cross_view_ref_in_measure_expr_joins() {
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, ISSUE_55_ORDER_FLAGS]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.flagged_order_sum".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.order_flags"),
            "cross-view ref in measure expr must join order_flags:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_cross_view_ref_in_measure_filter_joins() {
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, ISSUE_55_ORDER_FLAGS]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_flagged_orders".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.order_flags"),
            "cross-view ref in measure filter must join order_flags:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_no_spurious_join_when_ref_member_unused() {
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, ISSUE_55_ORDER_FLAGS]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        // Query touches only same-view members — the cross-ref dimension
        // exists on the view but isn't requested, so no join.
        let request = QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["orders.order_id".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            !result.sql.contains("JOIN"),
            "unrequested cross-ref members must not force joins:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_query_filter_on_cross_ref_member_joins() {
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, ISSUE_55_ORDER_FLAGS]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            filters: vec![QueryFilter {
                member: Some("orders.flag_from_other_view".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["true".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.order_flags"),
            "query filter on a cross-ref member must join order_flags:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_entity_style_ref_joins() {
        // The docs' canonical example: {{customer.name}} where `customer` is
        // an entity (Foreign here, Primary on the customers view).
        let orders = r#"
name: orders
table: public.orders
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
  - name: customer_name
    type: string
    expr: "{{customer.name}}"
measures:
  - name: count
    type: count
"#;
        let customers = r#"
name: customers
table: public.customers
entities:
  - name: customer
    type: primary
    key: customer_id
dimensions:
  - name: customer_id
    type: string
    expr: customer_id
  - name: name
    type: string
    expr: name
"#;
        let (eval, jg, layer) = engine_from_yaml(&[orders, customers]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.customer_name".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.customers"),
            "entity-style ref in dimension expr must join customers:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_transitive_refs_join_both_views() {
        // orders → order_flags → flag_meta: the inlined is_flagged expr
        // itself references a third view, which must also be joined.
        let order_flags = r#"
name: order_flags
table: public.order_flags
entities:
  - name: order
    type: foreign
    key: order_id
  - name: flag
    type: primary
    key: flag_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: flag_id
    type: string
    expr: FLAG_ID
  - name: is_flagged
    type: boolean
    expr: "{{flag_meta.code}} = 'F'"
"#;
        let flag_meta = r#"
name: flag_meta
table: public.flag_meta
entities:
  - name: flag
    type: foreign
    key: flag_id
dimensions:
  - name: flag_id
    type: string
    expr: FLAG_ID
  - name: code
    type: string
    expr: CODE
"#;
        let (eval, jg, layer) = engine_from_yaml(&[ISSUE_55_ORDERS, order_flags, flag_meta]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["orders.flag_from_other_view".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            result.sql.contains("JOIN public.order_flags"),
            "first hop must be joined:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("JOIN public.flag_meta"),
            "transitive ref target must be joined:\n{}",
            result.sql
        );
    }

    const COMPOSITE_MIXED_GRAIN_ORDERS: &str = r#"
name: orders
table: public.orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
measures:
  - name: total_tax_collected
    type: sum
    expr: tax_amount
  - name: net_revenue
    type: custom
    expr: "{{order_items.total_revenue}} - {{order_shipments.total_shipment_cost}} - {{orders.total_tax_collected}}"
"#;

    const COMPOSITE_MIXED_GRAIN_ORDER_ITEMS: &str = r#"
name: order_items
table: public.order_items
entities:
  - name: order
    type: foreign
    key: order_id
  - name: line_item
    type: primary
    key: line_item_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
  - name: line_item_id
    type: string
    expr: line_item_id
measures:
  - name: total_revenue
    type: sum
    expr: "quantity * unit_price"
"#;

    const COMPOSITE_MIXED_GRAIN_ORDER_SHIPMENTS: &str = r#"
name: order_shipments
table: public.order_shipments
entities:
  - name: order
    type: foreign
    key: order_id
  - name: line_item
    type: foreign
    key: line_item_id
  - name: shipment
    type: primary
    key: shipment_id
dimensions:
  - name: shipment_id
    type: string
    expr: shipment_id
  - name: order_id
    type: string
    expr: order_id
  - name: line_item_id
    type: string
    expr: line_item_id
measures:
  - name: total_shipment_cost
    type: sum
    expr: shipping_cost
"#;

    #[test]
    fn test_composite_measure_mixed_grain_isolates_each_term_into_its_own_cte() {
        // Bug repro: a `type: custom` measure combines a native SUM (its own
        // view) with two cross-view SUMs, where those views fan out
        // one-to-many relative to `orders`. Compiling it as one flat join
        // (the old behavior) would multiply `orders.tax_amount` by the
        // fan-out factor. Each referenced measure must instead get its own
        // isolated per-view CTE, joined into the outer query, and combined
        // only via the composite's arithmetic — never sharing a flat join.
        let (eval, jg, layer) = engine_from_yaml(&[
            COMPOSITE_MIXED_GRAIN_ORDERS,
            COMPOSITE_MIXED_GRAIN_ORDER_ITEMS,
            COMPOSITE_MIXED_GRAIN_ORDER_SHIPMENTS,
        ]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.net_revenue".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();

        // Each view gets its own isolated CTE...
        assert!(
            result.sql.contains("__measures_orders"),
            "expected an isolated orders CTE:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("__measures_order_items"),
            "expected an isolated order_items CTE:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("__measures_order_shipments"),
            "expected an isolated order_shipments CTE:\n{}",
            result.sql
        );
        // ...and no single CTE joins order_items together with
        // order_shipments (that flat join is exactly the fan-out that
        // inflated the native orders.tax_amount term).
        assert!(
            !result
                .sql
                .contains("JOIN public.order_shipments AS \"order_shipments\""),
            "order_shipments must not be joined into another view's CTE:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("orders__net_revenue"),
            "composite measure's own output column must still be present:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_composite_measure_with_extra_cross_view_content_errors_loudly() {
        // A composite mixing named measure refs across 2+ views (which we
        // isolate) with ADDITIONAL raw cross-view content (a bare dimension
        // ref not among the recognized measure terms) has no isolated join
        // context left to resolve that leftover content against once split
        // out — refuse rather than silently miscompute.
        let orders = r#"
name: orders
table: public.orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
measures:
  - name: total_tax_collected
    type: sum
    expr: tax_amount
  - name: net_revenue
    type: custom
    expr: "{{order_items.total_revenue}} - {{orders.total_tax_collected}} + CASE WHEN {{order_items.is_gift}} THEN 1 ELSE 0 END"
"#;
        let order_items = r#"
name: order_items
table: public.order_items
entities:
  - name: order
    type: foreign
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: order_id
  - name: is_gift
    type: boolean
    expr: is_gift
measures:
  - name: total_revenue
    type: sum
    expr: "quantity * unit_price"
"#;
        let (eval, jg, layer) = engine_from_yaml(&[orders, order_items]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.net_revenue".to_string()],
            ..QueryRequest::new()
        };
        let err = gen.generate(&request).unwrap_err();
        assert!(
            err.to_string().contains("cannot be safely isolated"),
            "expected a loud isolation error, got: {}",
            err
        );
    }

    #[test]
    fn test_issue_55_sub_query_dims_do_not_force_joins() {
        // sub_query dims compile as correlated subqueries — their cross-view
        // refs must not pull the target view into the join tree.
        let orders = r#"
name: orders
table: public.orders
entities:
  - name: order
    type: primary
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
  - name: flag_count
    type: number
    sub_query: true
    expr: "{{order_flags.flag_total}}"
measures:
  - name: total_orders
    type: count
"#;
        let order_flags = r#"
name: order_flags
table: public.order_flags
entities:
  - name: order
    type: foreign
    key: order_id
dimensions:
  - name: order_id
    type: string
    expr: ORDER_ID
measures:
  - name: flag_total
    type: count
"#;
        let (eval, jg, layer) = engine_from_yaml(&[orders, order_flags]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec!["orders.total_orders".to_string()],
            dimensions: vec!["orders.flag_count".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(
            !result.sql.contains("JOIN public.order_flags"),
            "sub_query dim must compile as a correlated subquery, not a join:\n{}",
            result.sql
        );
        assert!(
            result.sql.contains("SELECT"),
            "sanity: query compiled:\n{}",
            result.sql
        );
    }

    #[test]
    fn test_issue_55_cyclic_refs_terminate() {
        // a.x references b.y and b.y references a.x — expansion must
        // terminate (the resolver itself rejects such cycles separately).
        let view_a = r#"
name: a
table: public.a
entities:
  - name: shared
    type: primary
    key: id
dimensions:
  - name: id
    type: string
    expr: id
  - name: x
    type: string
    expr: "{{b.y}}"
"#;
        let view_b = r#"
name: b
table: public.b
entities:
  - name: shared
    type: foreign
    key: id
dimensions:
  - name: id
    type: string
    expr: id
  - name: y
    type: string
    expr: "{{a.x}}"
"#;
        let (eval, jg, layer) = engine_from_yaml(&[view_a, view_b]);
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect, &layer);

        let request = QueryRequest {
            measures: vec![],
            dimensions: vec!["a.x".to_string()],
            ..QueryRequest::new()
        };
        let expanded = gen.expand_views_for_expr_refs(&request, &["a".to_string()]);
        let mut sorted = expanded.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["a".to_string(), "b".to_string()]);
    }
}
