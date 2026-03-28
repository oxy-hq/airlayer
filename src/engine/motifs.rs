use crate::dialect::Dialect;
use crate::engine::query::{ColumnKind, ColumnMeta, OrderBy, QueryRequest};
use crate::engine::EngineError;
use crate::schema::models::{
    Motif, MotifConstraint, MotifKind, MotifOutputColumn, MotifParam, MotifParamType,
};
use std::collections::HashMap;

/// Return the builtin motif catalog.
pub fn builtin_motifs() -> Vec<Motif> {
    vec![
        pop_motif("yoy", "Year-over-year comparison"),
        pop_motif("qoq", "Quarter-over-quarter comparison"),
        pop_motif("mom", "Month-over-month comparison"),
        pop_motif("wow", "Week-over-week comparison"),
        pop_motif("dod", "Day-over-day comparison"),
        anomaly_motif(),
        contribution_motif(),
        trend_motif(),
        moving_average_motif(),
        rank_motif(),
        percent_of_total_motif(),
        cumulative_motif(),
    ]
}

/// Check if name is a builtin motif.
pub fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        "yoy" | "qoq" | "mom" | "wow" | "dod" | "anomaly" | "contribution"
            | "trend" | "moving_average" | "rank" | "percent_of_total" | "cumulative"
    )
}

/// A multi-stage motif plan. Most motifs have one stage (adds on top of __base).
/// Complex motifs like anomaly and trend use intermediate CTEs.
pub struct MotifPlan {
    /// Intermediate CTE stages. Each stage produces a CTE that the next stage reads from.
    /// Empty for single-stage motifs.
    pub intermediate_adds: Vec<Vec<MotifOutputColumn>>,
    /// Final stage adds columns (these are the user-visible output columns).
    pub final_adds: Vec<MotifOutputColumn>,
}

/// Get builtin motif plan for a given motif + dialect.
pub fn builtin_plan(name: &str, dialect: &Dialect) -> MotifPlan {
    match name {
        "yoy" | "qoq" | "mom" | "wow" | "dod" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "previous_value".into(),
                    expr: "LAG({{ measure }}, 1) OVER (ORDER BY {{ time }})".into(),
                },
                MotifOutputColumn {
                    name: "growth_rate".into(),
                    expr: "CASE WHEN LAG({{ measure }}, 1) OVER (ORDER BY {{ time }}) IS NOT NULL AND LAG({{ measure }}, 1) OVER (ORDER BY {{ time }}) != 0 THEN ({{ measure }} - LAG({{ measure }}, 1) OVER (ORDER BY {{ time }})) * 1.0 / ABS(LAG({{ measure }}, 1) OVER (ORDER BY {{ time }})) ELSE NULL END".into(),
                },
            ],
        },
        // Anomaly: two-stage CTE.
        // Stage 1 (__stats): compute mean_value, stddev_value as window functions.
        // Stage 2 (final): reference __stats columns to compute z_score, is_anomaly.
        "anomaly" => MotifPlan {
            intermediate_adds: vec![vec![
                MotifOutputColumn {
                    name: "mean_value".into(),
                    expr: "AVG({{ measure }}) OVER ()".into(),
                },
                MotifOutputColumn {
                    name: "stddev_value".into(),
                    expr: format!("{}({{{{ measure }}}}) OVER ()", dialect.stddev_pop()),
                },
            ]],
            final_adds: vec![
                MotifOutputColumn {
                    name: "mean_value".into(),
                    expr: "s.mean_value".into(),
                },
                MotifOutputColumn {
                    name: "stddev_value".into(),
                    expr: "s.stddev_value".into(),
                },
                MotifOutputColumn {
                    name: "z_score".into(),
                    expr: "CASE WHEN s.stddev_value > 0 THEN ({{ measure }} - s.mean_value) / s.stddev_value ELSE 0 END".into(),
                },
                MotifOutputColumn {
                    name: "is_anomaly".into(),
                    expr: "CASE WHEN s.stddev_value > 0 AND ABS(({{ measure }} - s.mean_value) / s.stddev_value) > {{ threshold }} THEN 1 ELSE 0 END".into(),
                },
            ],
        },
        "contribution" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "total".into(),
                    expr: "SUM({{ measure }}) OVER ()".into(),
                },
                MotifOutputColumn {
                    name: "share".into(),
                    expr: "{{ measure }} * 1.0 / NULLIF(SUM({{ measure }}) OVER (), 0)".into(),
                },
            ],
        },
        // Trend: two-stage CTE.
        // Stage 1 (__numbered): compute row_n = ROW_NUMBER().
        // Stage 2 (final): REGR_SLOPE/INTERCEPT on materialized row_n (no nested windows).
        "trend" => {
            if dialect.has_regression_functions() {
                MotifPlan {
                    intermediate_adds: vec![vec![
                        MotifOutputColumn {
                            name: "row_n".into(),
                            expr: "ROW_NUMBER() OVER (ORDER BY {{ time }})".into(),
                        },
                    ]],
                    final_adds: vec![
                        MotifOutputColumn {
                            name: "row_n".into(),
                            expr: "s.row_n".into(),
                        },
                        MotifOutputColumn {
                            name: "slope".into(),
                            expr: "REGR_SLOPE({{ measure }}, s.row_n) OVER ()".into(),
                        },
                        MotifOutputColumn {
                            name: "intercept".into(),
                            expr: "REGR_INTERCEPT({{ measure }}, s.row_n) OVER ()".into(),
                        },
                        MotifOutputColumn {
                            name: "trend_value".into(),
                            expr: "REGR_INTERCEPT({{ measure }}, s.row_n) OVER () + REGR_SLOPE({{ measure }}, s.row_n) OVER () * s.row_n".into(),
                        },
                    ],
                }
            } else {
                // Manual fallback: just compute row_n for non-regression dialects
                MotifPlan {
                    intermediate_adds: vec![],
                    final_adds: vec![
                        MotifOutputColumn {
                            name: "row_n".into(),
                            expr: "ROW_NUMBER() OVER (ORDER BY {{ time }})".into(),
                        },
                    ],
                }
            }
        }
        "moving_average" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "moving_avg".into(),
                    expr: "AVG({{ measure }}) OVER (ORDER BY {{ time }} ROWS BETWEEN {{ window }} PRECEDING AND CURRENT ROW)".into(),
                },
            ],
        },
        "rank" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "rank".into(),
                    expr: "RANK() OVER (ORDER BY {{ measure }} DESC)".into(),
                },
            ],
        },
        "percent_of_total" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "percent_of_total".into(),
                    expr: "100.0 * {{ measure }} / NULLIF(SUM({{ measure }}) OVER (), 0)".into(),
                },
            ],
        },
        "cumulative" => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![
                MotifOutputColumn {
                    name: "cumulative_value".into(),
                    expr: "SUM({{ measure }}) OVER (ORDER BY {{ time }} ROWS UNBOUNDED PRECEDING)".into(),
                },
            ],
        },
        _ => MotifPlan {
            intermediate_adds: vec![],
            final_adds: vec![],
        },
    }
}

/// Backward-compat wrapper: get the user-visible adds columns.
pub fn builtin_adds(name: &str, dialect: &Dialect) -> Vec<MotifOutputColumn> {
    builtin_plan(name, dialect).final_adds
}

/// Validate that the query satisfies motif requirements.
pub fn validate_requirements(
    motif: &Motif,
    _request: &QueryRequest,
    base_columns: &[ColumnMeta],
) -> Result<(), EngineError> {
    let has_measure = base_columns.iter().any(|c| c.kind == ColumnKind::Measure);
    let has_time = base_columns
        .iter()
        .any(|c| c.kind == ColumnKind::TimeDimension);

    // All motifs require at least one measure
    if !has_measure {
        return Err(EngineError::QueryError(format!(
            "Motif '{}' requires at least one measure in the query",
            motif.name
        )));
    }

    // Check param constraints for temporal requirement
    let needs_time = motif.params.values().any(|p| {
        p.constraints.contains(&MotifConstraint::Temporal)
    });
    // PoP motifs and time-series motifs need a time dimension
    let time_motifs = ["yoy", "qoq", "mom", "wow", "dod", "moving_average", "cumulative", "trend"];
    let needs_time = needs_time
        || (motif.motif_kind == MotifKind::Builtin && time_motifs.contains(&motif.name.as_str()));

    if needs_time && !has_time {
        return Err(EngineError::QueryError(format!(
            "Motif '{}' requires a time_dimension in the query",
            motif.name
        )));
    }

    Ok(())
}

/// Resolve {{ param }} references to actual column aliases from the base CTE.
/// Auto-binding: {{ measure }} -> first Measure column, {{ time }} -> first TimeDimension,
/// {{ dimensions }} -> comma-separated Dimension columns. Explicit motif_params override.
pub fn resolve_params(
    motif: &Motif,
    base_columns: &[ColumnMeta],
    motif_params: &HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, String>, EngineError> {
    let mut resolved = HashMap::new();

    // Auto-bind standard params
    let first_measure = base_columns
        .iter()
        .find(|c| c.kind == ColumnKind::Measure)
        .map(|c| c.alias.clone());
    let first_time = base_columns
        .iter()
        .find(|c| c.kind == ColumnKind::TimeDimension)
        .map(|c| c.alias.clone());
    let all_dims: Vec<String> = base_columns
        .iter()
        .filter(|c| c.kind == ColumnKind::Dimension)
        .map(|c| c.alias.clone())
        .collect();

    if let Some(ref m) = first_measure {
        resolved.insert("measure".to_string(), format!("b.{}", m));
    }
    if let Some(ref t) = first_time {
        resolved.insert("time".to_string(), format!("b.{}", t));
    }
    if !all_dims.is_empty() {
        resolved.insert(
            "dimensions".to_string(),
            all_dims.iter().map(|d| format!("b.{}", d)).collect::<Vec<_>>().join(", "),
        );
    }

    // Default values for special params
    resolved.insert("threshold".to_string(), "2".to_string());
    // window for moving_average: default to window-1 = 6 (7-period window)
    resolved.insert("window".to_string(), "6".to_string());

    // Apply explicit motif_params (override auto-bindings)
    for (key, value) in motif_params {
        let str_val = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            other => other.to_string(),
        };
        resolved.insert(key.clone(), str_val);
    }

    // Validate that auto-bound params actually exist
    if motif.motif_kind == MotifKind::Builtin {
        if first_measure.is_none() {
            return Err(EngineError::QueryError(format!(
                "Motif '{}' requires a measure column but none found",
                motif.name
            )));
        }
    }

    Ok(resolved)
}

/// Substitute {{ param }} in expr with resolved values.
fn substitute_expr(expr: &str, resolved: &HashMap<String, String>) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{\s*(\w+)\s*\}\}").unwrap());
    re.replace_all(expr, |caps: &regex::Captures| {
        let param_name = &caps[1];
        resolved
            .get(param_name)
            .cloned()
            .unwrap_or_else(|| format!("{{{{ {} }}}}", param_name))
    })
    .to_string()
}

/// Generate the full wrapped SQL, supporting multi-stage CTEs.
///
/// Single-stage (most motifs):
///   WITH __base AS (<base_sql>) SELECT b.*, <adds> FROM __base b
///
/// Multi-stage (anomaly, trend):
///   WITH __base AS (<base_sql>),
///        __stage1 AS (SELECT b.*, <intermediate_adds> FROM __base b)
///   SELECT s.*, <final_adds> FROM __stage1 s
pub fn wrap_with_motif(
    base_sql: &str,
    base_columns: &[ColumnMeta],
    motif: &Motif,
    resolved_params: &HashMap<String, String>,
    dialect: &Dialect,
    order: &[OrderBy],
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<(String, Vec<ColumnMeta>), EngineError> {
    // Get the motif plan — builtin or custom (custom motifs are always single-stage)
    let plan = if motif.motif_kind == MotifKind::Builtin && motif.adds.is_empty() {
        builtin_plan(&motif.name, dialect)
    } else {
        MotifPlan {
            intermediate_adds: vec![],
            final_adds: motif.adds.clone(),
        }
    };

    if plan.final_adds.is_empty() {
        return Err(EngineError::QueryError(format!(
            "Motif '{}' has no output columns defined",
            motif.name
        )));
    }

    let has_intermediate = !plan.intermediate_adds.is_empty();

    // Collect all measures for potential per-measure expansion
    let all_measures: Vec<&ColumnMeta> = base_columns
        .iter()
        .filter(|c| c.kind == ColumnKind::Measure)
        .collect();

    // Build CTE chain
    let mut cte_parts: Vec<String> = vec![format!("__base AS (\n{}\n)", base_sql)];

    // Check whether intermediates use {{ measure }} and need per-measure expansion
    static MEASURE_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let measure_re = MEASURE_RE.get_or_init(|| regex::Regex::new(r"\{\{\s*measure\s*\}\}").unwrap());
    let intermediate_uses_measure = plan.intermediate_adds.iter()
        .any(|stage| stage.iter().any(|c| measure_re.is_match(&c.expr)));
    let expand_intermediates = intermediate_uses_measure && all_measures.len() > 1;

    // Add intermediate CTEs
    for (i, stage_adds) in plan.intermediate_adds.iter().enumerate() {
        let prev_alias = if i == 0 { "b" } else { "s" };
        let prev_cte = if i == 0 {
            "__base"
        } else {
            "__stage"
        };
        let mut stage_select: Vec<String> = vec![format!("{}.*", prev_alias)];

        if expand_intermediates {
            // Expand intermediate columns per-measure
            for measure_col in &all_measures {
                let measure_short = measure_col.alias.split("__").last().unwrap_or(&measure_col.alias);
                let measure_ref = format!("{}.{}", prev_alias, measure_col.alias);
                let mut per_measure_resolved = resolved_params.clone();
                per_measure_resolved.insert("measure".to_string(), measure_ref);

                for col in stage_adds {
                    let col_name = format!("{}_{}", col.name, measure_short);
                    let resolved_expr = substitute_expr(&col.expr, &per_measure_resolved);
                    stage_select.push(format!(
                        "{} AS {}",
                        resolved_expr,
                        dialect.quote_identifier(&col_name)
                    ));
                }
            }
        } else {
            for col in stage_adds {
                let resolved_expr = substitute_expr(&col.expr, resolved_params);
                stage_select.push(format!(
                    "{} AS {}",
                    resolved_expr,
                    dialect.quote_identifier(&col.name)
                ));
            }
        }

        let stage_name = format!("__stage{}", i + 1);
        cte_parts.push(format!(
            "{} AS (\nSELECT\n  {}\nFROM {} {}\n)",
            stage_name,
            stage_select.join(",\n  "),
            prev_cte,
            prev_alias,
        ));
    }

    // Build final SELECT
    // The final SELECT reads from the last CTE stage
    let (final_from, final_alias) = if has_intermediate {
        (format!("__stage{}", plan.intermediate_adds.len()), "s")
    } else {
        ("__base".to_string(), "b")
    };

    // For multi-stage motifs, $measure/$time/$dimensions in the final stage
    // must reference the intermediate alias (s) instead of the base alias (b).
    let final_resolved = if has_intermediate {
        let mut r = resolved_params.clone();
        for (_, v) in r.iter_mut() {
            if v.starts_with("b.") {
                *v = format!("s.{}", &v[2..]);
            }
        }
        r
    } else {
        resolved_params.clone()
    };

    let mut select_parts: Vec<String> = vec![format!("{}.*", final_alias)];
    let mut motif_columns: Vec<ColumnMeta> = Vec::new();

    // Detect whether the plan uses {{ measure }} and there are multiple measures.
    // If so, expand the motif columns once per measure (e.g., total_revenue__share, total_orders__share).
    let plan_uses_measure = plan.final_adds.iter().any(|c| measure_re.is_match(&c.expr))
        || plan.intermediate_adds.iter().any(|stage| stage.iter().any(|c| measure_re.is_match(&c.expr)));
    let expand_per_measure = plan_uses_measure && all_measures.len() > 1;

    // Collect intermediate column names for multi-stage rewriting
    let intermediate_col_names: Vec<String> = plan.intermediate_adds.iter()
        .flat_map(|stage| stage.iter().map(|c| c.name.clone()))
        .collect();

    if expand_per_measure {
        // For multi-measure expansion, emit motif columns for each measure.
        // Column names become {measure_short_name}__{motif_col_name}, e.g. total_revenue__share.
        for measure_col in &all_measures {
            let measure_short = measure_col.alias.split("__").last().unwrap_or(&measure_col.alias);
            let measure_ref = format!("{}.{}", final_alias, measure_col.alias);
            let mut per_measure_resolved = final_resolved.clone();
            per_measure_resolved.insert("measure".to_string(), measure_ref);

            for col in &plan.final_adds {
                let col_name = format!("{}__{}", measure_short, col.name);
                let mut expr = substitute_expr(&col.expr, &per_measure_resolved);

                // For multi-stage motifs with expanded intermediates, rewrite
                // references to intermediate columns: s.mean_value -> s.mean_value_total_revenue
                if has_intermediate && expand_intermediates {
                    for int_col in &intermediate_col_names {
                        let old_ref = format!("s.{}", int_col);
                        let new_ref = format!("s.{}_{}", int_col, measure_short);
                        expr = expr.replace(&old_ref, &new_ref);
                    }
                }

                select_parts.push(format!(
                    "{} AS {}",
                    expr,
                    dialect.quote_identifier(&col_name)
                ));
                motif_columns.push(ColumnMeta {
                    member: format!("__motif.{}", col_name),
                    alias: col_name,
                    kind: ColumnKind::MotifComputed,
                });
            }
        }
    } else {
        for col in &plan.final_adds {
            let resolved_expr = substitute_expr(&col.expr, &final_resolved);
            select_parts.push(format!(
                "{} AS {}",
                resolved_expr,
                dialect.quote_identifier(&col.name)
            ));
            motif_columns.push(ColumnMeta {
                member: format!("__motif.{}", col.name),
                alias: col.name.clone(),
                kind: ColumnKind::MotifComputed,
            });
        }
    }

    let mut sql = format!(
        "WITH {}\nSELECT\n  {}\nFROM {} {}",
        cte_parts.join(",\n"),
        select_parts.join(",\n  "),
        final_from,
        final_alias,
    );

    // ORDER BY
    if !order.is_empty() {
        let order_parts: Vec<String> = order
            .iter()
            .filter_map(|o| {
                let dir = if o.desc { "DESC" } else { "ASC" };
                let col_name = if let Some(col) = base_columns.iter().find(|c| c.member == o.id) {
                    Some(col.alias.clone())
                } else if let Some(mc) = motif_columns.iter().find(|c| c.alias == o.id || c.member == o.id) {
                    Some(mc.alias.clone())
                } else {
                    let all_aliases: Vec<&str> = base_columns.iter().map(|c| c.alias.as_str())
                        .chain(motif_columns.iter().map(|c| c.alias.as_str()))
                        .collect();
                    if all_aliases.contains(&o.id.as_str()) {
                        Some(o.id.clone())
                    } else {
                        None
                    }
                };
                col_name.map(|name| format!("{} {}", dialect.quote_identifier(&name), dir))
            })
            .collect();
        if !order_parts.is_empty() {
            sql.push_str(&format!("\nORDER BY\n  {}", order_parts.join(", ")));
        }
    }

    // LIMIT
    if let Some(limit) = limit {
        sql.push_str(&format!("\nLIMIT {}", limit));
    }

    // OFFSET
    if let Some(offset) = offset {
        sql.push_str(&format!("\nOFFSET {}", offset));
    }

    // Combine base columns + motif columns
    let mut all_columns: Vec<ColumnMeta> = base_columns.to_vec();
    all_columns.extend(motif_columns);

    Ok((sql, all_columns))
}

// ── Builtin motif constructors ──────────────────────────

fn pop_motif(name: &str, description: &str) -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to compare".into()),
            values: None,
        },
    );
    params.insert(
        "time".to_string(),
        MotifParam {
            param_type: MotifParamType::Dimension,
            constraints: vec![MotifConstraint::Temporal],
            default: None,
            description: Some("Time dimension for ordering".into()),
            values: None,
        },
    );
    Motif {
        name: name.to_string(),
        description: Some(description.to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![], // filled at compile time via builtin_adds()
    }
}

fn anomaly_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to detect anomalies on".into()),
            values: None,
        },
    );
    params.insert(
        "threshold".to_string(),
        MotifParam {
            param_type: MotifParamType::Number,
            constraints: vec![],
            default: Some(serde_json::json!(2)),
            description: Some("Z-score threshold for anomaly detection".into()),
            values: None,
        },
    );
    Motif {
        name: "anomaly".to_string(),
        description: Some("Anomaly detection using z-score".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn contribution_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to analyze contribution for".into()),
            values: None,
        },
    );
    Motif {
        name: "contribution".to_string(),
        description: Some("Contribution analysis — share of each row's measure relative to total".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn trend_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to fit trend on".into()),
            values: None,
        },
    );
    params.insert(
        "time".to_string(),
        MotifParam {
            param_type: MotifParamType::Dimension,
            constraints: vec![MotifConstraint::Temporal],
            default: None,
            description: Some("Time dimension for ordering".into()),
            values: None,
        },
    );
    Motif {
        name: "trend".to_string(),
        description: Some("Linear trend (regression slope + intercept)".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn moving_average_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to average".into()),
            values: None,
        },
    );
    params.insert(
        "time".to_string(),
        MotifParam {
            param_type: MotifParamType::Dimension,
            constraints: vec![MotifConstraint::Temporal],
            default: None,
            description: Some("Time dimension for ordering".into()),
            values: None,
        },
    );
    params.insert(
        "window".to_string(),
        MotifParam {
            param_type: MotifParamType::Number,
            constraints: vec![],
            default: Some(serde_json::json!(6)),
            description: Some("Window size (ROWS BETWEEN N PRECEDING AND CURRENT ROW)".into()),
            values: None,
        },
    );
    Motif {
        name: "moving_average".to_string(),
        description: Some("Moving average over a sliding window".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn rank_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to rank by".into()),
            values: None,
        },
    );
    Motif {
        name: "rank".to_string(),
        description: Some("Rank rows by measure descending".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn percent_of_total_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to compute percentage for".into()),
            values: None,
        },
    );
    Motif {
        name: "percent_of_total".to_string(),
        description: Some("Each row's measure as a percentage of the total".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

fn cumulative_motif() -> Motif {
    let mut params = HashMap::new();
    params.insert(
        "measure".to_string(),
        MotifParam {
            param_type: MotifParamType::Measure,
            constraints: vec![MotifConstraint::Numeric],
            default: None,
            description: Some("Measure to accumulate".into()),
            values: None,
        },
    );
    params.insert(
        "time".to_string(),
        MotifParam {
            param_type: MotifParamType::Dimension,
            constraints: vec![MotifConstraint::Temporal],
            default: None,
            description: Some("Time dimension for ordering".into()),
            values: None,
        },
    );
    Motif {
        name: "cumulative".to_string(),
        description: Some("Running cumulative sum".to_string()),
        motif_kind: MotifKind::Builtin,
        params,
        returns: None,
        adds: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_catalog_completeness() {
        let builtins = builtin_motifs();
        assert_eq!(builtins.len(), 12);
        for m in &builtins {
            assert!(is_builtin(&m.name), "Expected {} to be builtin", m.name);
            let adds = builtin_adds(&m.name, &Dialect::Postgres);
            assert!(!adds.is_empty(), "Builtin '{}' should have adds columns", m.name);
        }
    }

    #[test]
    fn test_param_resolution_auto() {
        let motif = contribution_motif();
        let columns = vec![
            ColumnMeta {
                member: "orders.status".into(),
                alias: "orders__status".into(),
                kind: ColumnKind::Dimension,
            },
            ColumnMeta {
                member: "orders.total_revenue".into(),
                alias: "orders__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        assert_eq!(resolved.get("measure").unwrap(), "b.orders__total_revenue");
    }

    #[test]
    fn test_param_resolution_explicit() {
        let motif = anomaly_motif();
        let columns = vec![
            ColumnMeta {
                member: "orders.total_revenue".into(),
                alias: "orders__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let mut explicit = HashMap::new();
        explicit.insert("threshold".to_string(), serde_json::json!(3));
        let resolved = resolve_params(&motif, &columns, &explicit).unwrap();
        assert_eq!(resolved.get("threshold").unwrap(), "3");
    }

    #[test]
    fn test_substitute_expr() {
        let mut resolved = HashMap::new();
        resolved.insert("measure".to_string(), "b.revenue".to_string());
        resolved.insert("time".to_string(), "b.created_at".to_string());

        let result = substitute_expr("LAG({{ measure }}, 1) OVER (ORDER BY {{ time }})", &resolved);
        assert_eq!(result, "LAG(b.revenue, 1) OVER (ORDER BY b.created_at)");
    }

    #[test]
    fn test_requirement_validation_missing_measure() {
        let motif = contribution_motif();
        let columns = vec![ColumnMeta {
            member: "orders.status".into(),
            alias: "orders__status".into(),
            kind: ColumnKind::Dimension,
        }];
        let request = QueryRequest::new();
        let result = validate_requirements(&motif, &request, &columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires at least one measure"));
    }

    #[test]
    fn test_requirement_validation_missing_time() {
        let motif = pop_motif("yoy", "test");
        let columns = vec![ColumnMeta {
            member: "orders.total_revenue".into(),
            alias: "orders__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let request = QueryRequest::new();
        let result = validate_requirements(&motif, &request, &columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a time_dimension"));
    }

    #[test]
    fn test_yoy_generates_cte_with_lag() {
        let motif = pop_motif("yoy", "test");
        let columns = vec![
            ColumnMeta {
                member: "events.event_date".into(),
                alias: "events__event_date".into(),
                kind: ColumnKind::TimeDimension,
            },
            ColumnMeta {
                member: "events.total_revenue".into(),
                alias: "events__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, out_cols) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            None,
            None,
        )
        .unwrap();
        assert!(sql.contains("WITH __base AS"), "SQL: {}", sql);
        assert!(sql.contains("LAG("), "SQL: {}", sql);
        assert!(sql.contains("growth_rate"), "SQL: {}", sql);
        // Base columns + 2 motif columns
        assert_eq!(out_cols.len(), 4);
    }

    #[test]
    fn test_contribution_generates_sum_over() {
        let motif = contribution_motif();
        let columns = vec![
            ColumnMeta {
                member: "events.platform".into(),
                alias: "events__platform".into(),
                kind: ColumnKind::Dimension,
            },
            ColumnMeta {
                member: "events.total_revenue".into(),
                alias: "events__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            None,
            None,
        )
        .unwrap();
        assert!(sql.contains("SUM("), "SQL: {}", sql);
        assert!(sql.contains("OVER ()"), "SQL: {}", sql);
    }

    #[test]
    fn test_anomaly_generates_two_cte_stddev() {
        let motif = anomaly_motif();
        let columns = vec![ColumnMeta {
            member: "events.total_revenue".into(),
            alias: "events__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            None,
            None,
        )
        .unwrap();
        // Two-CTE pattern: __base + __stage1
        assert!(sql.contains("__base"), "SQL: {}", sql);
        assert!(sql.contains("__stage1"), "SQL: {}", sql);
        // Stage 1 computes STDDEV_POP
        assert!(sql.contains("STDDEV_POP"), "SQL: {}", sql);
        // Final SELECT references s.mean_value, s.stddev_value (no repeated window functions)
        assert!(sql.contains("s.mean_value"), "SQL: {}", sql);
        assert!(sql.contains("s.stddev_value"), "SQL: {}", sql);
        assert!(sql.contains("z_score"), "SQL: {}", sql);
        assert!(sql.contains("is_anomaly"), "SQL: {}", sql);
    }

    #[test]
    fn test_trend_generates_two_cte_regression() {
        let motif = trend_motif();
        let columns = vec![
            ColumnMeta {
                member: "events.event_date".into(),
                alias: "events__event_date".into(),
                kind: ColumnKind::TimeDimension,
            },
            ColumnMeta {
                member: "events.total_revenue".into(),
                alias: "events__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            None,
            None,
        )
        .unwrap();
        // Two-CTE pattern: __base + __stage1 (row_n) + final (REGR_*)
        assert!(sql.contains("__stage1"), "SQL should have intermediate CTE:\n{}", sql);
        assert!(sql.contains("ROW_NUMBER()"), "SQL should compute row_n in stage1:\n{}", sql);
        assert!(sql.contains("REGR_SLOPE"), "SQL should have REGR_SLOPE:\n{}", sql);
        assert!(sql.contains("s.row_n"), "SQL should reference materialized row_n:\n{}", sql);
        // Should NOT nest window functions
        assert!(!sql.contains("REGR_SLOPE($measure, ROW_NUMBER()"), "SQL must not nest window functions:\n{}", sql);
    }

    #[test]
    fn test_custom_motif_substitution() {
        let motif = Motif {
            name: "custom_test".into(),
            description: None,
            motif_kind: MotifKind::Custom,
            params: {
                let mut p = HashMap::new();
                p.insert("measure".into(), MotifParam {
                    param_type: MotifParamType::Measure,
                    constraints: vec![],
                    default: None,
                    description: None,
                    values: None,
                });
                p
            },
            returns: None,
            adds: vec![MotifOutputColumn {
                name: "doubled".into(),
                expr: "{{ measure }} * 2".into(),
            }],
        };
        let columns = vec![ColumnMeta {
            member: "orders.revenue".into(),
            alias: "orders__revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            None,
            None,
        )
        .unwrap();
        assert!(sql.contains("b.orders__revenue * 2"), "SQL: {}", sql);
    }

    #[test]
    fn test_motif_limit_on_outer_only() {
        let motif = contribution_motif();
        let columns = vec![ColumnMeta {
            member: "events.total_revenue".into(),
            alias: "events__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let base_sql = "SELECT 1";
        let (sql, _) = wrap_with_motif(
            base_sql,
            &columns,
            &motif,
            &resolved,
            &Dialect::Postgres,
            &[],
            Some(10),
            None,
        )
        .unwrap();
        // LIMIT should appear only once, on the outer query
        assert_eq!(sql.matches("LIMIT").count(), 1, "SQL: {}", sql);
        assert!(sql.ends_with("LIMIT 10"), "SQL: {}", sql);
    }

    #[test]
    fn test_motif_with_bigquery_dialect() {
        let motif = contribution_motif();
        let columns = vec![ColumnMeta {
            member: "events.total_revenue".into(),
            alias: "events__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1",
            &columns,
            &motif,
            &resolved,
            &Dialect::BigQuery,
            &[],
            None,
            None,
        )
        .unwrap();
        // BigQuery uses backtick quoting for identifiers
        assert!(sql.contains('`'), "SQL: {}", sql);
    }
}
