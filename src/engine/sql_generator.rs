use crate::dialect::Dialect;
use crate::engine::evaluator::SchemaEvaluator;
use crate::engine::join_graph::{JoinEdge, JoinGraph, JoinRelationship};
use crate::engine::member_sql::MemberSqlResolver;
use crate::engine::query::*;
use crate::engine::EngineError;
use crate::schema::models::*;
use std::collections::{HashMap, HashSet};

/// Generates SQL from a QueryRequest using the schema evaluator and join graph.
pub struct SqlGenerator<'a> {
    evaluator: &'a SchemaEvaluator,
    join_graph: &'a JoinGraph,
    dialect: &'a Dialect,
}

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
    ) -> Self {
        Self {
            evaluator,
            join_graph,
            dialect,
        }
    }

    pub fn generate(&self, request: &QueryRequest) -> Result<QueryResult, EngineError> {
        // Determine which views are involved
        let referenced_views = request.referenced_views();
        if referenced_views.is_empty() {
            return Err(EngineError::QueryError(
                "Query must reference at least one view".to_string(),
            ));
        }

        // Validate all referenced members exist
        self.validate_members(request)?;

        // Pick base view using join-tree cost optimization
        let base_view = self.pick_base_view(request, &referenced_views)?;

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
            return self.generate_with_fanout_protection(request, &base_view, &builder);
        }

        // Build entity-to-alias map for cross-entity reference resolution
        let joined_views: Vec<&str> = other_views.iter().copied().collect();
        let entity_to_alias =
            self.evaluator
                .build_entity_to_alias_map(&base_view, &joined_views);

        // Add dimensions to SELECT and GROUP BY
        for dim_path in &request.dimensions {
            self.add_dimension(&mut builder, dim_path, &entity_to_alias)?;
        }

        // Add time dimensions
        for td in &request.time_dimensions {
            self.add_time_dimension(&mut builder, td, &entity_to_alias, request.timezone.as_deref())?;
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
            let alias = builder.view_aliases.get(&view).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not in query", view))
            })?;
            let seg_expr = self.resolve_expression(alias, &seg.expr, &entity_to_alias);
            builder.where_conditions.push(seg_expr);
        }

        // Add time dimension date range filters (supports relative date ranges)
        for td in &request.time_dimensions {
            if let Some(date_range) = td.resolved_date_range() {
                if date_range.len() == 2 {
                    let (view, member) = self.evaluator.parse_member_path(&td.dimension)?;
                    let alias = builder
                        .view_aliases
                        .get(&view)
                        .ok_or_else(|| {
                            EngineError::QueryError(format!("View '{}' not found in query context", view))
                        })?;
                    let dim = self.evaluator.dimension(&view, &member).ok_or_else(|| {
                        EngineError::QueryError(format!("Dimension '{}' not found", td.dimension))
                    })?;
                    let col_expr = self.resolve_expression(alias, &dim.expr, &entity_to_alias);

                    let param_idx = builder.params.len();
                    builder.params.push(date_range[0].clone());
                    builder.params.push(date_range[1].clone());

                    let from_param = self.dialect.param_placeholder(param_idx);
                    let to_param = self.dialect.param_placeholder(param_idx + 1);

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
                builder
                    .order_by
                    .push(format!("{} {}", self.dialect.quote_identifier(&col.alias), dir));
            }
        }

        // Build final SQL
        let sql = self.assemble_sql(&builder, request)?;

        Ok(QueryResult {
            sql,
            params: builder.params,
            columns: builder.columns,
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
        let mut params = Vec::new();
        let mut columns = Vec::new();
        let mut ctes: Vec<String> = Vec::new();

        // Group measures by their source view
        let mut measures_by_view: HashMap<String, Vec<&str>> = HashMap::new();
        for m in &request.measures {
            if let Some(v) = m.split('.').next() {
                measures_by_view
                    .entry(v.to_string())
                    .or_default()
                    .push(m);
            }
        }

        // Identify join keys for each multiplied view
        // The join keys are the columns used in the join conditions connecting back to other views
        let mut view_join_keys: HashMap<String, Vec<(String, String)>> = HashMap::new(); // view -> [(local_col, remote_alias.remote_col)]
        for join in &original_builder.joins {
            // For multiplied views, we need to know what columns to GROUP BY
            // Parse the condition to extract column references
            // The join conditions are in the form: "alias1"."col1" = "alias2"."col2"
            // We stored them structured, so let's use the join graph edges instead
            if original_builder.multiplied_views.contains(&join.alias) {
                // This view is multiplied — we need its join key columns
                // Look up the edge from the join graph
                let edges = self.join_graph.edges_from(&join.alias);
                for edge in &edges {
                    for cond in &edge.conditions {
                        view_join_keys
                            .entry(join.alias.clone())
                            .or_default()
                            .push((cond.from_column.clone(), cond.to_column.clone()));
                    }
                }
            }
        }

        // Also check base view
        if original_builder.multiplied_views.contains(base_view) {
            // Find join keys for the base view from the join edges
            for join in &original_builder.joins {
                let edges = self.join_graph.edges_from(base_view);
                for edge in &edges {
                    if edge.to_view == join.alias {
                        for cond in &edge.conditions {
                            view_join_keys
                                .entry(base_view.to_string())
                                .or_default()
                                .push((cond.from_column.clone(), cond.to_column.clone()));
                        }
                    }
                }
            }
        }

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
            let alias = original_builder.view_aliases.get(&view).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not in query", view))
            })?;
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
            let alias = original_builder.view_aliases.get(&view).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not in query", view))
            })?;
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
                let sql = self.compile_filter_for_context(filter, &original_builder.view_aliases, &entity_to_alias, &mut params)?;
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
            let alias = original_builder.view_aliases.get(&view).ok_or_else(|| {
                EngineError::QueryError(format!("View '{}' not in query", view))
            })?;
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
                    let param_idx = params.len();
                    params.push(date_range[0].clone());
                    params.push(date_range[1].clone());
                    let from_param = self.dialect.param_placeholder(param_idx);
                    let to_param = self.dialect.param_placeholder(param_idx + 1);
                    spine_where.push(format!(
                        "{} >= {} AND {} <= {}",
                        col_expr, from_param, col_expr, to_param
                    ));
                }
            }
        }
        if !spine_where.is_empty() {
            dim_spine_sql.push_str(&format!("\n  WHERE\n    {}", spine_where.join("\n    AND ")));
        }

        ctes.push(format!("__dim_spine AS (\n  {}\n)", dim_spine_sql));

        // Build per-view measure CTEs for multiplied views
        let mut measure_cte_names: Vec<String> = Vec::new();
        let mut measure_cte_join_keys: Vec<Vec<String>> = Vec::new();
        let mut final_select_measures: Vec<String> = Vec::new();

        for (view_name, measure_paths) in &measures_by_view {
            if !original_builder.multiplied_views.contains(view_name) {
                // Not multiplied — measures can be computed directly from the spine
                // We'll handle these in the final SELECT
                for mp in measure_paths {
                    let (_, name) = self.evaluator.parse_member_path(mp)?;
                    let measure = self.evaluator.measure(view_name, &name).ok_or_else(|| {
                        EngineError::QueryError(format!("Measure not found: {}", mp))
                    })?;
                    let agg_expr = self.measure_agg_expr(view_name, measure, &entity_to_alias)?;
                    let col_alias = self.member_alias(mp);
                    final_select_measures.push(format!(
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
                continue;
            }

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
                // No join keys — can't pre-aggregate, fall back to direct computation
                for mp in measure_paths {
                    let (_, name) = self.evaluator.parse_member_path(mp)?;
                    let measure = self.evaluator.measure(view_name, &name).ok_or_else(|| {
                        EngineError::QueryError(format!("Measure not found: {}", mp))
                    })?;
                    let agg_expr = self.measure_agg_expr(view_name, measure, &entity_to_alias)?;
                    let col_alias = self.member_alias(mp);
                    final_select_measures.push(format!(
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
                continue;
            }

            let cte_name = format!("__measures_{}", view_name);
            let view_source = self.view_source_expr(view);
            let view_alias = view_name;
            let empty_entity_map: HashMap<String, String> = HashMap::new();

            // Build CTE: SELECT join_keys, AGG(measures) FROM view GROUP BY join_keys
            let key_selects: Vec<String> = join_keys
                .iter()
                .map(|k| {
                    format!(
                        "{}.{} AS {}",
                        self.dialect.quote_identifier(view_alias),
                        self.dialect.quote_identifier(k),
                        self.dialect.quote_identifier(k)
                    )
                })
                .collect();

            let mut measure_selects: Vec<String> = Vec::new();
            for mp in measure_paths {
                let (_, name) = self.evaluator.parse_member_path(mp)?;
                let measure = self.evaluator.measure(view_name, &name).ok_or_else(|| {
                    EngineError::QueryError(format!("Measure not found: {}", mp))
                })?;
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

            let group_by: Vec<String> = (1..=join_keys.len())
                .map(|i| i.to_string())
                .collect();

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
        }

        // Build final query: SELECT dims + measures FROM __dim_spine JOIN measure CTEs
        let mut final_select: Vec<String> = dim_aliases
            .iter()
            .map(|a| {
                format!(
                    "__dim_spine.{}",
                    self.dialect.quote_identifier(a)
                )
            })
            .collect();

        for (cte_name, measure_paths) in measure_cte_names.iter().zip(measures_by_view.values()) {
            for mp in measure_paths {
                let col_alias = self.member_alias(mp);
                final_select.push(format!(
                    "{}.{}",
                    cte_name,
                    self.dialect.quote_identifier(&col_alias)
                ));
            }
        }
        // Add direct (non-CTE) measures
        final_select.extend(final_select_measures);

        let mut sql = format!("WITH\n{}\nSELECT\n  {}\nFROM\n  __dim_spine", ctes.join(",\n"), final_select.join(",\n  "));

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

        // ORDER BY
        for order in &request.order {
            let dir = if order.desc { "DESC" } else { "ASC" };
            if let Some(col) = columns.iter().find(|c| c.member == order.id) {
                // First order clause gets ORDER BY, rest get commas
                if sql.contains("\nORDER BY") {
                    sql.push_str(&format!(", {} {}", self.dialect.quote_identifier(&col.alias), dir));
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

        // Count references per view for tiebreaking
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for m in &request.measures {
            if let Some(v) = m.split('.').next() {
                *counts.entry(v).or_default() += 1;
            }
        }
        for d in &request.dimensions {
            if let Some(v) = d.split('.').next() {
                *counts.entry(v).or_default() += 1;
            }
        }

        let other_views_for = |candidate: &str| -> Vec<&str> {
            views.iter().filter(|v| v.as_str() != candidate).map(|v| v.as_str()).collect()
        };

        // Try each view as root and pick the one with the shortest join tree
        let mut best: Option<(String, usize, usize)> = None; // (view, cost, ref_count)
        for candidate in views {
            let others = other_views_for(candidate);
            if let Some(cost) = self.join_graph.join_tree_cost(candidate, &others) {
                let ref_count = counts.get(candidate.as_str()).copied().unwrap_or(0);
                if let Some(ref b) = best {
                    // Prefer lower cost, then higher ref count
                    if cost < b.1 || (cost == b.1 && ref_count > b.2) {
                        best = Some((candidate.clone(), cost, ref_count));
                    }
                } else {
                    best = Some((candidate.clone(), cost, ref_count));
                }
            }
        }

        best.map(|(v, _, _)| v).ok_or_else(|| {
            // Fall back to reference count if no join tree is valid
            let fallback = counts
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
        let join_edges = self.join_graph.find_join_tree_with_hints(base_view, target_views, through)?;

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
                    format!(
                        "{}.{} = {}.{}",
                        self.dialect.quote_identifier(from_alias),
                        self.dialect.quote_identifier(&c.from_column),
                        self.dialect.quote_identifier(&alias),
                        self.dialect.quote_identifier(&c.to_column),
                    )
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
    fn detect_multiplied_views(
        &self,
        builder: &mut QueryBuilder,
        base_view: &str,
        join_edges: &[JoinEdge],
    ) {
        // A view is "multiplied" if there's a OneToMany edge in the join tree
        // where that view is the source (from_view). The from_view's rows get
        // duplicated because the to_view has many matching rows.
        for edge in join_edges {
            if edge.relationship == JoinRelationship::OneToMany {
                // The from_view's rows get multiplied
                builder.multiplied_views.insert(edge.from_view.clone());
                // The base view also gets multiplied if it's an ancestor
                if edge.from_view == base_view || builder.view_aliases.contains_key(&edge.from_view) {
                    builder.multiplied_views.insert(base_view.to_string());
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
        entity_to_alias: &HashMap<String, String>,
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
        let measure = self.evaluator.measure(&target_view, &measure_name).ok_or_else(|| {
            EngineError::QueryError(format!(
                "Subquery dimension references measure '{}' which was not found",
                measure_path
            ))
        })?;
        let target = self.evaluator.view(&target_view).ok_or_else(|| {
            EngineError::QueryError(format!("View '{}' not found for subquery dimension", target_view))
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
        // Use the join graph to find edges between the views
        let edges = self.join_graph.edges_from(outer_alias);
        for edge in &edges {
            if edge.to_view == inner_view {
                let conditions: Vec<String> = edge
                    .conditions
                    .iter()
                    .map(|c| {
                        format!(
                            "{}.{} = {}.{}",
                            self.dialect.quote_identifier(inner_view),
                            self.dialect.quote_identifier(&c.to_column),
                            self.dialect.quote_identifier(outer_alias),
                            self.dialect.quote_identifier(&c.from_column),
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
                            "{}.{} = {}.{}",
                            self.dialect.quote_identifier(inner_view),
                            self.dialect.quote_identifier(&c.from_column),
                            self.dialect.quote_identifier(outer_alias),
                            self.dialect.quote_identifier(&c.to_column),
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

        let alias = builder.view_aliases.get(&view).ok_or_else(|| {
            EngineError::QueryError(format!("View '{}' not in query", view))
        })?;

        let mut col_expr = self.resolve_expression(alias, &dim.expr, entity_to_alias);

        if let Some(tz) = timezone {
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

        let alias = builder.view_aliases.get(&view).ok_or_else(|| {
            EngineError::QueryError(format!("View '{}' not in query", view))
        })?;

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
        let filtered_expr = if let Some(ref filters) = measure.filters {
            if !filters.is_empty() {
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
            }
        } else {
            inner_expr
        };

        // Handle rolling window measures — wrap aggregate in a window function
        if let Some(ref rolling) = measure.rolling_window {
            let base_agg = self.base_aggregate_expr(view_alias, measure, &filtered_expr, entity_to_alias)?;
            let frame = self.build_window_frame(rolling);
            return Ok(format!("{} OVER ({})", base_agg, frame));
        }

        let agg = match measure.measure_type {
            MeasureType::Count => format!("COUNT({})", filtered_expr),
            MeasureType::Sum => format!("SUM({})", filtered_expr),
            MeasureType::Average => format!("AVG({})", filtered_expr),
            MeasureType::Min => format!("MIN({})", filtered_expr),
            MeasureType::Max => format!("MAX({})", filtered_expr),
            MeasureType::CountDistinct => {
                format!("COUNT(DISTINCT {})", filtered_expr)
            }
            MeasureType::CountDistinctApprox => {
                self.dialect.count_distinct_approx(&filtered_expr)
            }
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

    /// Unified expression resolver: handles {TABLE}, {{entity.field}}, {{view.measure}} references,
    /// and bare column qualification.
    fn resolve_expression(
        &self,
        view_alias: &str,
        expr: &str,
        entity_to_alias: &HashMap<String, String>,
    ) -> String {
        let quote_fn = |s: &str| self.dialect.quote_identifier(s);

        // 1. Resolve {TABLE} self-references
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
        use regex::Regex;
        use std::sync::OnceLock;

        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"\{\{(\w+)\.(\w+)\}\}").unwrap());

        let quote_fn = |s: &str| self.dialect.quote_identifier(s);

        re.replace_all(expr, |caps: &regex::Captures<'_>| {
            let first = &caps[1];
            let second = &caps[2];

            // Skip variable references — they're preserved as-is
            if first == "variables" {
                return format!("{{{{{}.{}}}}}", first, second);
            }

            // Check if it's a measure reference (view_name.measure_name)
            let member_path = format!("{}.{}", first, second);
            if self.evaluator.is_measure(&member_path) {
                if let Some(measure) = self.evaluator.measure(first, second) {
                    let alias = if self.evaluator.view(first).is_some() {
                        first.to_string()
                    } else {
                        current_view_alias.to_string()
                    };
                    // Recursively resolve the measure's aggregate expression
                    // Use an empty entity map to avoid infinite recursion
                    if let Ok(agg) = self.measure_agg_expr(&alias, measure, entity_to_alias) {
                        return agg;
                    }
                }
            }

            // Check if it's a dimension reference (view_name.dimension_name)
            if self.evaluator.is_dimension(&member_path) {
                if let Some(dim) = self.evaluator.dimension(first, second) {
                    let alias = if self.evaluator.view(first).is_some() {
                        first.to_string()
                    } else {
                        current_view_alias.to_string()
                    };
                    return self.resolve_expression(&alias, &dim.expr, entity_to_alias);
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
    /// Only qualifies tokens that look like identifiers and are likely column references
    /// (not SQL keywords, not function names, not already qualified).
    fn qualify_bare_columns(&self, expr: &str, view_alias: &str) -> String {
        // Get dimension names for this view to know which tokens are columns
        let view = self.evaluator.view(view_alias);
        let dim_names: HashSet<&str> = view
            .map(|v| v.dimensions.iter().map(|d| d.name.as_str()).collect())
            .unwrap_or_default();

        if dim_names.is_empty() {
            return expr.to_string();
        }

        let mut result = String::new();
        let chars: Vec<char> = expr.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            // Skip quoted strings
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

            // Check for identifier tokens
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
            MeasureType::Median => format!("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {})", filtered_expr),
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
            let non_empty: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
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
            let non_empty: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
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
        let alias = builder.view_aliases.get(&view).ok_or_else(|| {
            EngineError::QueryError(format!("View '{}' not in query", view))
        })?;

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
            self.resolve_expression(alias, &dim.expr, entity_to_alias)
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
            let non_empty: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
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
            let non_empty: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
            return Ok(if non_empty.len() > 1 {
                format!("({})", non_empty.join(" OR "))
            } else {
                non_empty.first().map(|s| s.to_string()).unwrap_or_default()
            });
        }

        let member = filter.member.as_ref().ok_or_else(|| {
            EngineError::QueryError("Filter must have a member".to_string())
        })?;
        let operator = filter.operator.as_ref().ok_or_else(|| {
            EngineError::QueryError("Filter must have an operator".to_string())
        })?;

        let (view, name) = self.evaluator.parse_member_path(member)?;
        let alias = view_aliases.get(&view).ok_or_else(|| {
            EngineError::QueryError(format!("View '{}' not in query", view))
        })?;
        let dim = self.evaluator.dimension(&view, &name).ok_or_else(|| {
            EngineError::QueryError(format!("Filter member '{}' not found", member))
        })?;
        let col_expr = self.resolve_expression(alias, &dim.expr, entity_to_alias);

        // Use parameterized values
        self.compile_filter_operator_parameterized(&col_expr, operator, &filter.values, params)
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
                    let idx = params.len();
                    params.push(values[0].clone());
                    Ok(format!("{} = {}", col, self.dialect.param_placeholder(idx)))
                } else {
                    let placeholders: Vec<String> = values
                        .iter()
                        .map(|v| {
                            let idx = params.len();
                            params.push(v.clone());
                            self.dialect.param_placeholder(idx)
                        })
                        .collect();
                    Ok(format!("{} IN ({})", col, placeholders.join(", ")))
                }
            }
            FilterOperator::NotEquals => {
                if values.len() == 1 {
                    let idx = params.len();
                    params.push(values[0].clone());
                    Ok(format!("{} <> {}", col, self.dialect.param_placeholder(idx)))
                } else {
                    let placeholders: Vec<String> = values
                        .iter()
                        .map(|v| {
                            let idx = params.len();
                            params.push(v.clone());
                            self.dialect.param_placeholder(idx)
                        })
                        .collect();
                    Ok(format!("{} NOT IN ({})", col, placeholders.join(", ")))
                }
            }
            FilterOperator::Contains => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("%{}%", v));
                        format!("{} LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotContains => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("%{}%", v));
                        format!("{} NOT LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::StartsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("{}%", v));
                        format!("{} LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotStartsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("{}%", v));
                        format!("{} NOT LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::EndsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("%{}", v));
                        format!("{} LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" OR ")))
            }
            FilterOperator::NotEndsWith => {
                let conditions: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let idx = params.len();
                        params.push(format!("%{}", v));
                        format!("{} NOT LIKE {}", col, self.dialect.param_placeholder(idx))
                    })
                    .collect();
                Ok(format!("({})", conditions.join(" AND ")))
            }
            FilterOperator::Gt => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} > {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::Gte => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} >= {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::Lt => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} < {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::Lte => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} <= {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::Set => Ok(format!("{} IS NOT NULL", col)),
            FilterOperator::NotSet => Ok(format!("{} IS NULL", col)),
            FilterOperator::InDateRange => {
                if values.len() == 2 {
                    let idx0 = params.len();
                    params.push(values[0].clone());
                    let idx1 = params.len();
                    params.push(values[1].clone());
                    Ok(format!(
                        "{} >= {} AND {} <= {}",
                        col,
                        self.dialect.param_placeholder(idx0),
                        col,
                        self.dialect.param_placeholder(idx1)
                    ))
                } else {
                    Err(EngineError::QueryError(
                        "inDateRange requires exactly 2 values".to_string(),
                    ))
                }
            }
            FilterOperator::NotInDateRange => {
                if values.len() == 2 {
                    let idx0 = params.len();
                    params.push(values[0].clone());
                    let idx1 = params.len();
                    params.push(values[1].clone());
                    Ok(format!(
                        "({} < {} OR {} > {})",
                        col,
                        self.dialect.param_placeholder(idx0),
                        col,
                        self.dialect.param_placeholder(idx1)
                    ))
                } else {
                    Err(EngineError::QueryError(
                        "notInDateRange requires exactly 2 values".to_string(),
                    ))
                }
            }
            FilterOperator::BeforeDate => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} < {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::BeforeOrOnDate => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} <= {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::AfterDate => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} > {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::AfterOrOnDate => {
                let idx = params.len();
                params.push(values[0].clone());
                Ok(format!("{} >= {}", col, self.dialect.param_placeholder(idx)))
            }
            FilterOperator::OnTheDate => {
                // Expand to date range for the full day
                let date = &values[0];
                let next_day = if let Ok(d) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
                    (d + chrono::Duration::days(1)).format("%Y-%m-%d").to_string()
                } else {
                    // If not parseable, just use the date as-is for both bounds
                    date.clone()
                };
                let idx0 = params.len();
                params.push(date.clone());
                let idx1 = params.len();
                params.push(next_day);
                Ok(format!(
                    "{} >= {} AND {} < {}",
                    col,
                    self.dialect.param_placeholder(idx0),
                    col,
                    self.dialect.param_placeholder(idx1)
                ))
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
            table.clone()
        } else if let Some(ref sql) = view.sql {
            format!("(\n  {}\n)", sql)
        } else {
            view.name.clone()
        }
    }

    /// Generate a column alias from a member path.
    fn member_alias(&self, path: &str) -> String {
        path.replace('.', "__")
    }
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

/// Check if an expression is a simple column name (no operators, functions, etc.).
fn is_simple_column_name(expr: &str) -> bool {
    let trimmed = expr.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::evaluator::SchemaEvaluator;
    use crate::engine::join_graph::JoinGraph;

    fn make_test_engine() -> (SchemaEvaluator, JoinGraph) {
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "orders".to_string(),
                    description: "Orders".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("public.orders".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "customer".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("customer_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                        },
                    ]),
                    segments: vec![
                        Segment {
                            name: "is_active".to_string(),
                            expr: "status = 'active'".to_string(),
                            description: Some("Active orders".to_string()),
                            inherits_from: None,
                        },
                    ],
                },
                View {
                    name: "customers".to_string(),
                    description: "Customers".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("public.customers".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "customer".to_string(),
                        entity_type: EntityType::Primary,
                        description: None,
                        key: Some("customer_id".to_string()),
                        keys: None,
                        inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        (eval, jg)
    }

    #[test]
    fn test_simple_select() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
    fn test_cross_view_join() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["customers.name".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("JOIN"));
        assert!(result.sql.contains("customers"));
    }

    #[test]
    fn test_time_dimension() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
    fn test_limit_offset() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Orders".to_string(),
                label: None,
                datasource: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![
                    Dimension {
                        name: "status".to_string(),
                        dimension_type: DimensionType::String,
                        description: None,
                        expr: "COALESCE(status, 'unknown')".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                            sub_query: None,
                            inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Orders".to_string(),
                label: None,
                datasource: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![
                    Dimension {
                        name: "total_amount".to_string(),
                        dimension_type: DimensionType::Number,
                        description: None,
                        expr: "{TABLE}.price * {TABLE}.quantity".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                            sub_query: None,
                            inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.total_amount".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("\"orders\".price * \"orders\".quantity"));
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
                    description: "Orders".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("public.orders".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                        },
                    ]),
                    segments: vec![],
                },
                View {
                    name: "order_items".to_string(),
                    description: "Order line items".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("public.order_items".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order_item".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("AND"), "Expected AND in WHERE, got:\n{}", result.sql);
        assert_eq!(result.params.len(), 2);
        assert_eq!(result.params[0], "active");
        assert_eq!(result.params[1], "100");
    }

    #[test]
    fn test_nested_or_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("OR"), "Expected OR in WHERE, got:\n{}", result.sql);
        assert_eq!(result.params, vec!["active", "pending"]);
    }

    #[test]
    fn test_deeply_nested_and_inside_or() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("OR"), "Expected nested OR, got:\n{}", result.sql);
        assert!(result.sql.contains("AND"), "Expected nested AND, got:\n{}", result.sql);
        assert_eq!(result.params.len(), 4);
    }

    // ─── Mixed dimension + measure filters ─────────────────────────

    #[test]
    fn test_dimension_and_measure_filter_split() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("WHERE"), "Expected WHERE clause, got:\n{}", result.sql);
        assert!(result.sql.contains("HAVING"), "Expected HAVING clause, got:\n{}", result.sql);
        // WHERE should have the status filter, HAVING should have the revenue filter
        let where_pos = result.sql.find("WHERE").unwrap();
        let having_pos = result.sql.find("HAVING").unwrap();
        assert!(where_pos < having_pos);
    }

    // ─── Multiple filter operators ─────────────────────────────────

    #[test]
    fn test_in_operator_multiple_values() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            filters: vec![QueryFilter {
                member: Some("orders.status".to_string()),
                operator: Some(FilterOperator::Equals),
                values: vec!["active".to_string(), "pending".to_string(), "shipped".to_string()],
                and: None,
                or: None,
            }],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("IN"), "Expected IN for multi-value equals, got:\n{}", result.sql);
        assert_eq!(result.params.len(), 3);
    }

    #[test]
    fn test_contains_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("LIKE"), "Expected LIKE, got:\n{}", result.sql);
        assert_eq!(result.params, vec!["%act%"]);
    }

    #[test]
    fn test_set_and_not_set_filters() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("IS NOT NULL"), "Expected IS NOT NULL, got:\n{}", result.sql);
        assert!(result.params.is_empty(), "Set filter should have no params");
    }

    #[test]
    fn test_date_range_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains(">="), "Expected >= for date range start, got:\n{}", result.sql);
        assert!(result.sql.contains("<="), "Expected <= for date range end, got:\n{}", result.sql);
        assert_eq!(result.params, vec!["2025-01-01", "2025-12-31"]);
    }

    // ─── Time dimension with date_range ────────────────────────────

    #[test]
    fn test_time_dimension_with_date_range() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("date_trunc"), "Expected date_trunc, got:\n{}", result.sql);
        assert!(result.sql.contains("WHERE"), "Expected WHERE for date range, got:\n{}", result.sql);
        assert_eq!(result.params, vec!["2025-01-01", "2025-03-31"]);
    }

    #[test]
    fn test_time_dimension_multiple_granularities() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(sql_lower.contains("'month'") || sql_lower.contains("month"),
            "Expected month granularity, got:\n{}", result.sql);
        assert!(sql_lower.contains("'year'") || sql_lower.contains("year"),
            "Expected year granularity, got:\n{}", result.sql);
    }

    // ─── Cross-view filters ───────────────────────────────────────

    #[test]
    fn test_filter_on_joined_view() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("JOIN"), "Expected JOIN, got:\n{}", result.sql);
        assert!(result.sql.contains("LIKE"), "Expected LIKE for startsWith, got:\n{}", result.sql);
        assert_eq!(result.params, vec!["A%"]);
    }

    // ─── Dialect-specific output ──────────────────────────────────

    #[test]
    fn test_mysql_quoting() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::MySQL;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("`orders`"), "Expected backtick quoting for MySQL, got:\n{}", result.sql);
    }

    #[test]
    fn test_bigquery_quoting() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::BigQuery;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("`orders`"), "Expected backtick quoting for BigQuery, got:\n{}", result.sql);
    }

    #[test]
    fn test_mysql_param_placeholders() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::MySQL;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("?"), "Expected ? placeholder for MySQL, got:\n{}", result.sql);
        assert!(!result.sql.contains("$1"), "Should not have $1 for MySQL");
    }

    // ─── Ungrouped mode ───────────────────────────────────────────

    #[test]
    fn test_ungrouped_query() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ungrouped: true,
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(!result.sql.contains("GROUP BY"), "Expected no GROUP BY in ungrouped mode, got:\n{}", result.sql);
    }

    // ─── Measures only (no dimensions) ────────────────────────────

    #[test]
    fn test_measures_only_no_group_by() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string(), "orders.total_revenue".to_string()],
            dimensions: vec![],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // With aggregates but no dimensions, there's nothing to GROUP BY
        assert!(!result.sql.contains("GROUP BY"), "No GROUP BY needed with only measures, got:\n{}", result.sql);
        assert!(result.sql.contains("COUNT(*)"));
        assert!(result.sql.contains("SUM("));
    }

    // ─── SQL subquery view ────────────────────────────────────────

    #[test]
    fn test_sql_subquery_view() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "derived".to_string(),
                description: "Derived".to_string(),
                label: None,
                datasource: None,
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
                            inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["derived.count".to_string()],
            dimensions: vec!["derived.event_type".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("SELECT * FROM raw_events WHERE valid = true"),
            "Expected subquery in FROM, got:\n{}", result.sql);
    }

    // ─── Multiple segments ────────────────────────────────────────

    #[test]
    fn test_multiple_segments() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: "Orders".to_string(),
                label: None,
                datasource: None,
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
                            inherits_from: None,
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
                }]),
                segments: vec![
                    Segment {
                        name: "is_active".to_string(),
                        expr: "status = 'active'".to_string(),
                        description: None,
                        inherits_from: None,
                    },
                    Segment {
                        name: "is_high_value".to_string(),
                        expr: "amount > 1000".to_string(),
                        description: None,
                        inherits_from: None,
                    },
                ],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string()],
            dimensions: vec![],
            segments: vec!["orders.is_active".to_string(), "orders.is_high_value".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("active"), "Expected active segment, got:\n{}", result.sql);
        assert!(result.sql.contains("1000"), "Expected high_value segment, got:\n{}", result.sql);
        // Both should be in WHERE, combined with AND
        assert!(result.sql.contains("WHERE"));
    }

    // ─── Error cases ──────────────────────────────────────────────

    #[test]
    fn test_nonexistent_member_error() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest::new();
        let result = gen.generate(&request);
        assert!(result.is_err(), "Expected error for empty query");
    }

    #[test]
    fn test_nonexistent_segment_error() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                    description: "Departments".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("hr.departments".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "dept".to_string(),
                        entity_type: EntityType::Primary,
                        description: None,
                        key: Some("dept_id".to_string()),
                        keys: None,
                        inherits_from: None,
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
                            inherits_from: None,
                    }],
                    measures: None,
                    segments: vec![],
                },
                View {
                    name: "employees".to_string(),
                    description: "Employees".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("hr.employees".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "emp".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("emp_id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "dept".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("dept_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
                View {
                    name: "timesheets".to_string(),
                    description: "Timesheets".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("hr.timesheets".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "timesheet".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("ts_id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "emp".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("emp_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        // Query spans departments -> employees -> timesheets
        let request = QueryRequest {
            measures: vec!["timesheets.total_hours".to_string()],
            dimensions: vec!["departments.dept_name".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        // Should contain two JOINs for the transitive path
        let join_count = result.sql.matches("JOIN").count();
        assert!(join_count >= 2, "Expected at least 2 JOINs for transitive path, got {} in:\n{}", join_count, result.sql);
        assert!(result.sql.contains("departments"), "Expected departments in SQL");
        assert!(result.sql.contains("timesheets"), "Expected timesheets in SQL");
        assert!(result.sql.contains("employees"), "Expected employees as intermediate in SQL");
    }

    // ─── Measure with filters (CASE WHEN) ─────────────────────────

    #[test]
    fn test_measure_with_inline_filter() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: "Events".to_string(),
                label: None,
                datasource: None,
                table: Some("public.events".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![
                    Dimension {
                        name: "event_type".to_string(),
                        dimension_type: DimensionType::String,
                        description: None,
                        expr: "event_type".to_string(),
                        original_expr: None,
                        samples: None,
                        synonyms: None,
                        primary_key: None,
                            sub_query: None,
                            inherits_from: None,
                    },
                ],
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
                    },
                ]),
                segments: vec![],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["events.total_events".to_string(), "events.click_count".to_string()],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("COUNT(*)"), "Expected unfiltered COUNT, got:\n{}", result.sql);
        assert!(result.sql.contains("CASE WHEN") || result.sql.contains("case when"),
            "Expected CASE WHEN for filtered measure, got:\n{}", result.sql);
        assert!(result.sql.contains("click"), "Expected click filter in CASE WHEN");
    }

    // ─── Custom measure type ──────────────────────────────────────

    #[test]
    fn test_custom_measure() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: "Orders".to_string(),
                label: None,
                datasource: None,
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
                            inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.avg_order_value".to_string()],
            dimensions: vec!["orders.region".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("SUM(total) / NULLIF(COUNT(*), 0)"),
            "Expected custom expression verbatim, got:\n{}", result.sql);
    }

    // ─── Order by both dimension and measure ──────────────────────

    #[test]
    fn test_order_by_multiple_columns() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            order: vec![
                OrderBy { id: "orders.status".to_string(), desc: false },
                OrderBy { id: "orders.total_revenue".to_string(), desc: true },
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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.count".to_string(), "orders.total_revenue".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };

        let result = gen.generate(&request).unwrap();
        assert_eq!(result.columns.len(), 3);

        let dim_col = result.columns.iter().find(|c| c.member == "orders.status").unwrap();
        assert_eq!(dim_col.kind, ColumnKind::Dimension);

        let measure_col = result.columns.iter().find(|c| c.member == "orders.count").unwrap();
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
                    description: "A".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("a".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "a_entity".to_string(),
                        entity_type: EntityType::Primary,
                        description: None,
                        key: Some("id".to_string()),
                        keys: None,
                        inherits_from: None,
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
                            inherits_from: None,
                    }],
                    measures: None,
                    segments: vec![],
                },
                View {
                    name: "b".to_string(),
                    description: "B".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("b".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "b_entity".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "a_entity".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("a_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
                View {
                    name: "c".to_string(),
                    description: "C".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("c".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "c_entity".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "b_entity".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("b_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
                    }],
                    measures: None,
                    segments: vec![],
                },
            ],
            None,
        );

        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (evaluator, join_graph) = make_test_engine();
        let dialect = Dialect::Domo;
        let gen = SqlGenerator::new(&evaluator, &join_graph, &dialect);

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
        };

        let result = gen.generate(&request).unwrap();
        // Domo uses backtick quoting like MySQL
        assert!(result.sql.contains("`orders`"), "Expected backtick-quoted identifiers for Domo, got:\n{}", result.sql);
        assert!(result.sql.contains("LIMIT 10"), "Expected LIMIT clause, got:\n{}", result.sql);
        assert!(!result.sql.contains("\"orders\""), "Should not use double-quote identifiers for Domo, got:\n{}", result.sql);
    }

    #[test]
    fn test_domo_param_placeholders() {
        let (evaluator, join_graph) = make_test_engine();
        let dialect = Dialect::Domo;
        let gen = SqlGenerator::new(&evaluator, &join_graph, &dialect);

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
        };

        let result = gen.generate(&request).unwrap();
        // Domo uses ? param placeholders like MySQL
        assert!(result.sql.contains("= ?"), "Expected ? placeholder for Domo, got:\n{}", result.sql);
        assert!(!result.sql.contains("$1"), "Should not use $1 placeholder for Domo");
        assert_eq!(result.params, vec!["active"]);
    }

    #[test]
    fn test_domo_date_trunc() {
        let dialect = Dialect::Domo;
        // Domo uses MySQL-style DATE_FORMAT for date truncation
        let result = dialect.date_trunc("month", "`my_date`");
        assert!(result.contains("DATE_FORMAT"), "Expected DATE_FORMAT for Domo date_trunc, got: {}", result);
        assert!(result.contains("%Y-%m-01"), "Expected month format pattern, got: {}", result);
    }

    #[test]
    fn test_count_distinct_approx() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::BigQuery;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        // Build a layer with count_distinct_approx measure
        let layer = SemanticLayer::new(
            vec![View {
                name: "events".to_string(),
                description: "Events".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg2 = JoinGraph::build(&layer.views).unwrap();
        let eval2 = SchemaEvaluator::new(&layer, &jg2).unwrap();
        let gen2 = SqlGenerator::new(&eval2, &jg2, &dialect);

        let request = QueryRequest {
            measures: vec!["events.unique_users".to_string()],
            dimensions: vec!["events.event_type".to_string()],
            ..QueryRequest::new()
        };
        let result = gen2.generate(&request).unwrap();
        assert!(result.sql.contains("APPROX_COUNT_DISTINCT"), "Expected APPROX_COUNT_DISTINCT for BigQuery, got:\n{}", result.sql);
    }

    #[test]
    fn test_number_passthrough_measure() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "stats".to_string(),
                description: "Stats".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["stats.ratio".to_string()],
            dimensions: vec!["stats.category".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // Number measure should pass through the expression as-is
        assert!(result.sql.contains("SUM(a) / NULLIF(SUM(b), 0)"), "Number measure should pass through expression, got:\n{}", result.sql);
    }

    #[test]
    fn test_on_the_date_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains(">= $1"), "Expected >= for onTheDate, got:\n{}", result.sql);
        assert!(result.sql.contains("< $2"), "Expected < for onTheDate next day, got:\n{}", result.sql);
        assert_eq!(result.params[0], "2024-01-15");
        assert_eq!(result.params[1], "2024-01-16");
    }

    #[test]
    fn test_rolling_window_cumulative() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "sales".to_string(),
                description: "Sales".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["sales.cumulative_revenue".to_string()],
            dimensions: vec!["sales.sale_date".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        assert!(result.sql.contains("OVER"), "Expected OVER clause for rolling window, got:\n{}", result.sql);
        assert!(result.sql.contains("UNBOUNDED PRECEDING"), "Expected UNBOUNDED PRECEDING, got:\n{}", result.sql);
        assert!(result.sql.contains("CURRENT ROW"), "Expected CURRENT ROW, got:\n{}", result.sql);
    }

    #[test]
    fn test_measure_to_measure_reference() {
        let layer = SemanticLayer::new(
            vec![View {
                name: "orders".to_string(),
                description: "Orders".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                    },
                    Measure {
                        name: "avg_order_value".to_string(),
                        measure_type: MeasureType::Number,
                        description: None,
                        expr: Some("{{orders.total_revenue}} / NULLIF({{orders.count}}, 0)".to_string()),
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                    },
                ]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

        let request = QueryRequest {
            measures: vec!["orders.avg_order_value".to_string()],
            dimensions: vec!["orders.status".to_string()],
            ..QueryRequest::new()
        };
        let result = gen.generate(&request).unwrap();
        // The {{orders.total_revenue}} should resolve to SUM(amount) and {{orders.count}} to COUNT(*)
        assert!(result.sql.contains("SUM("), "Expected SUM from resolved measure ref, got:\n{}", result.sql);
        assert!(result.sql.contains("COUNT("), "Expected COUNT from resolved measure ref, got:\n{}", result.sql);
        assert!(result.sql.contains("NULLIF"), "Expected NULLIF preserved, got:\n{}", result.sql);
    }

    #[test]
    fn test_subquery_dimension() {
        // Build a schema with orders having a subquery dimension referencing customers
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "customers".to_string(),
                    description: "Customers".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("customers".to_string()),
                    sql: None,
                    entities: vec![Entity {
                        name: "customer".to_string(),
                        entity_type: EntityType::Primary,
                        description: None,
                        key: Some("customer_id".to_string()),
                        keys: None,
                        inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
                View {
                    name: "orders".to_string(),
                    description: "Orders".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("orders".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("order_id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "customer".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: Some("customer_id".to_string()),
                            keys: None,
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
            ],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert!(result.sql.contains("SELECT COUNT(*)"), "Expected correlated subquery with COUNT(*), got:\n{}", result.sql);
        assert!(result.sql.contains("FROM orders AS"), "Expected FROM orders in subquery, got:\n{}", result.sql);
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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Sales".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Events".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::ClickHouse;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Events".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::SQLite;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert_eq!(result.params, vec!["act%"], "StartsWith should append % to value");
    }

    #[test]
    fn test_ends_with_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert_eq!(result.params, vec!["%ive"], "EndsWith should prepend % to value");
    }

    #[test]
    fn test_not_contains_filter() {
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        assert_eq!(result.params, vec!["%cancel%"], "NotContains should wrap value with %");
    }

    #[test]
    fn test_composite_key_join() {
        let layer = SemanticLayer::new(
            vec![
                View {
                    name: "order_items".to_string(),
                    description: "Order Items".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("order_items".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "order_line".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: None,
                            keys: Some(vec!["order_id".to_string(), "line_num".to_string()]),
                            inherits_from: None,
                        },
                    ],
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
                View {
                    name: "returns".to_string(),
                    description: "Returns".to_string(),
                    label: None,
                    datasource: None,
                    table: Some("returns".to_string()),
                    sql: None,
                    entities: vec![
                        Entity {
                            name: "return_item".to_string(),
                            entity_type: EntityType::Primary,
                            description: None,
                            key: Some("return_id".to_string()),
                            keys: None,
                            inherits_from: None,
                        },
                        Entity {
                            name: "order_line".to_string(),
                            entity_type: EntityType::Foreign,
                            description: None,
                            key: None,
                            keys: Some(vec!["order_id".to_string(), "line_num".to_string()]),
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                            inherits_from: None,
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
                    }]),
                    segments: vec![],
                },
            ],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
        let (eval, jg) = make_test_engine();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Events".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Orders".to_string(),
                label: None,
                datasource: None,
                table: Some("public.orders".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![Dimension {
                    name: "org_orders".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "CASE WHEN org_id = {{variables.org_id}} THEN 'yes' ELSE 'no' END".to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
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
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
                description: "Orders".to_string(),
                label: None,
                datasource: None,
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
                    inherits_from: None,
                }],
                measures: Some(vec![Measure {
                    name: "weighted_total".to_string(),
                    measure_type: MeasureType::Sum,
                    description: None,
                    expr: Some("{TABLE}.amount * {TABLE}.weight".to_string()),
                    original_expr: None,
                    filters: None,
                    samples: None,
                    synonyms: None,
                    rolling_window: None,
                    inherits_from: None,
                }]),
                segments: vec![],
            }],
            None,
        );
        let jg = JoinGraph::build(&layer.views).unwrap();
        let eval = SchemaEvaluator::new(&layer, &jg).unwrap();
        let dialect = Dialect::Postgres;
        let gen = SqlGenerator::new(&eval, &jg, &dialect);

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
            !result.sql.contains("{TABLE}"),
            "Expected no raw {{TABLE}} in output SQL, got:\n{}",
            result.sql
        );
    }
}
