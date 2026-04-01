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

/// A multi-stage motif plan. Most motifs have one stage (outputs on top of __base).
/// Complex motifs like anomaly and trend use intermediate CTEs.
#[derive(Debug, Clone)]
pub struct MotifPlan {
    /// Intermediate CTE stages. Each stage produces a CTE that the next stage reads from.
    /// Empty for single-stage motifs.
    pub intermediate_outputs: Vec<Vec<MotifOutputColumn>>,
    /// Final stage output columns (these are the user-visible output columns).
    pub final_outputs: Vec<MotifOutputColumn>,
}

/// Get builtin motif plan for a given motif + dialect.
pub fn builtin_plan(name: &str, dialect: &Dialect) -> MotifPlan {
    match name {
        "yoy" | "qoq" | "mom" | "wow" | "dod" => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![
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
            intermediate_outputs: vec![vec![
                MotifOutputColumn {
                    name: "mean_value".into(),
                    expr: "AVG({{ measure }}) OVER ()".into(),
                },
                MotifOutputColumn {
                    name: "stddev_value".into(),
                    expr: format!("{}({{{{ measure }}}}) OVER ()", dialect.stddev_pop()),
                },
            ]],
            final_outputs: vec![
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
            intermediate_outputs: vec![],
            final_outputs: vec![
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
                    intermediate_outputs: vec![vec![
                        MotifOutputColumn {
                            name: "row_n".into(),
                            expr: "ROW_NUMBER() OVER (ORDER BY {{ time }})".into(),
                        },
                    ]],
                    final_outputs: vec![
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
                    intermediate_outputs: vec![],
                    final_outputs: vec![
                        MotifOutputColumn {
                            name: "row_n".into(),
                            expr: "ROW_NUMBER() OVER (ORDER BY {{ time }})".into(),
                        },
                    ],
                }
            }
        }
        "moving_average" => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![
                MotifOutputColumn {
                    name: "moving_avg".into(),
                    expr: "AVG({{ measure }}) OVER (ORDER BY {{ time }} ROWS BETWEEN {{ window }} PRECEDING AND CURRENT ROW)".into(),
                },
            ],
        },
        "rank" => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![
                MotifOutputColumn {
                    name: "rank".into(),
                    expr: "RANK() OVER (ORDER BY {{ measure }} DESC)".into(),
                },
            ],
        },
        "percent_of_total" => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![
                MotifOutputColumn {
                    name: "percent_of_total".into(),
                    expr: "100.0 * {{ measure }} / NULLIF(SUM({{ measure }}) OVER (), 0)".into(),
                },
            ],
        },
        "cumulative" => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![
                MotifOutputColumn {
                    name: "cumulative_value".into(),
                    expr: "SUM({{ measure }}) OVER (ORDER BY {{ time }} ROWS UNBOUNDED PRECEDING)".into(),
                },
            ],
        },
        _ => MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: vec![],
        },
    }
}

/// Backward-compat wrapper: get the user-visible output columns.
pub fn builtin_outputs(name: &str, dialect: &Dialect) -> Vec<MotifOutputColumn> {
    builtin_plan(name, dialect).final_outputs
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
///
/// Params of type Measure/Dimension are resolved from semantic member names
/// (e.g., "orders.total_revenue") to CTE column aliases (e.g., "b.orders__total_revenue").
/// Auto-binding only happens when exactly one column of the required kind exists
/// (unambiguous). If multiple exist and no explicit value is provided, an error
/// lists the available members.
///
/// Params of type Number/String/Enum use their default values when not explicitly provided.
///
/// `{{ dimensions }}` auto-binds to all dimension columns (rarely ambiguous).
pub fn resolve_params(
    motif: &Motif,
    base_columns: &[ColumnMeta],
    motif_params: &HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, String>, EngineError> {
    let mut resolved = HashMap::new();

    // Collect columns by kind for auto-binding
    let measures: Vec<&ColumnMeta> = base_columns
        .iter()
        .filter(|c| c.kind == ColumnKind::Measure)
        .collect();
    let time_dims: Vec<&ColumnMeta> = base_columns
        .iter()
        .filter(|c| c.kind == ColumnKind::TimeDimension)
        .collect();
    let all_dims: Vec<&ColumnMeta> = base_columns
        .iter()
        .filter(|c| c.kind == ColumnKind::Dimension)
        .collect();

    // Always auto-bind dimensions (rarely ambiguous, users want all of them)
    if !all_dims.is_empty() {
        resolved.insert(
            "dimensions".to_string(),
            all_dims.iter().map(|d| format!("b.{}", d.alias)).collect::<Vec<_>>().join(", "),
        );
    }

    // Process each declared motif param
    for (param_name, param_def) in &motif.params {
        if let Some(explicit_value) = motif_params.get(param_name) {
            // Explicit value provided — resolve it
            let str_val = resolve_explicit_param(param_name, explicit_value, param_def, base_columns)?;
            resolved.insert(param_name.clone(), str_val);
        } else {
            // No explicit value — try auto-bind or default
            match param_def.param_type {
                MotifParamType::Measure => {
                    match measures.len() {
                        0 => {
                            return Err(EngineError::QueryError(format!(
                                "Motif '{}' requires param '{}' (type: measure) but the query has no measures",
                                motif.name, param_name
                            )));
                        }
                        1 => {
                            // Unambiguous — auto-bind the single measure
                            resolved.insert(param_name.clone(), format!("b.{}", measures[0].alias));
                        }
                        _ => {
                            // Ambiguous — require explicit param
                            let available: Vec<&str> = measures.iter().map(|m| m.member.as_str()).collect();
                            return Err(EngineError::QueryError(format!(
                                "Motif '{}' requires param '{}' (type: measure) but the query has {} measures: [{}]. \
                                 Specify which measure via motif_params, e.g. \"motif_params\": {{\"{}\": \"{}\"}}",
                                motif.name, param_name, measures.len(),
                                available.join(", "), param_name, available[0]
                            )));
                        }
                    }
                }
                MotifParamType::Dimension => {
                    // For "time" params with temporal constraint, auto-bind from time_dims
                    if param_def.constraints.contains(&MotifConstraint::Temporal) {
                        match time_dims.len() {
                            0 => {
                                return Err(EngineError::QueryError(format!(
                                    "Motif '{}' requires param '{}' (type: time dimension) but the query has no time dimensions",
                                    motif.name, param_name
                                )));
                            }
                            1 => {
                                resolved.insert(param_name.clone(), format!("b.{}", time_dims[0].alias));
                            }
                            _ => {
                                let available: Vec<&str> = time_dims.iter().map(|t| t.member.as_str()).collect();
                                return Err(EngineError::QueryError(format!(
                                    "Motif '{}' requires param '{}' (type: time dimension) but the query has {} time dimensions: [{}]. \
                                     Specify which via motif_params, e.g. \"motif_params\": {{\"{}\": \"{}\"}}",
                                    motif.name, param_name, time_dims.len(),
                                    available.join(", "), param_name, available[0]
                                )));
                            }
                        }
                    } else {
                        // Non-temporal dimension param — auto-bind all dims
                        if !all_dims.is_empty() {
                            resolved.insert(
                                param_name.clone(),
                                all_dims.iter().map(|d| format!("b.{}", d.alias)).collect::<Vec<_>>().join(", "),
                            );
                        }
                    }
                }
                MotifParamType::Number | MotifParamType::String | MotifParamType::Enum => {
                    // Use default value if available
                    if let Some(ref default) = param_def.default {
                        let str_val = match default {
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
                            other => other.to_string(),
                        };
                        resolved.insert(param_name.clone(), str_val);
                    }
                    // If no default and not provided, it will be caught by substitute_expr
                    // as an unresolved param error.
                }
            }
        }
    }

    // Also apply any motif_params that are NOT declared in the motif's param list.
    // This supports ad-hoc overrides (e.g., passing "threshold" or "window" to builtins
    // that may not have them formally declared).
    for (key, value) in motif_params {
        if !motif.params.contains_key(key) {
            let str_val = validate_literal_param(key, value)?;
            resolved.insert(key.clone(), str_val);
        }
    }

    Ok(resolved)
}

/// Resolve an explicit motif_param value.
/// For Measure/Dimension types, the value is a semantic member name that gets resolved
/// to a CTE column alias. For Number/String/Enum, the value is used directly.
fn resolve_explicit_param(
    param_name: &str,
    value: &serde_json::Value,
    param_def: &MotifParam,
    base_columns: &[ColumnMeta],
) -> Result<String, EngineError> {
    match param_def.param_type {
        MotifParamType::Measure | MotifParamType::Dimension => {
            // Value should be a semantic member name (e.g., "orders.total_revenue")
            let member_name = match value {
                serde_json::Value::String(s) => s.clone(),
                other => {
                    return Err(EngineError::QueryError(format!(
                        "Motif param '{}' expects a member name (string), got: {}",
                        param_name, other
                    )));
                }
            };
            // Look up the member in base_columns
            let col = base_columns.iter().find(|c| c.member == member_name)
                .ok_or_else(|| {
                    let available: Vec<&str> = base_columns.iter()
                        .filter(|c| match param_def.param_type {
                            MotifParamType::Measure => c.kind == ColumnKind::Measure,
                            MotifParamType::Dimension => c.kind == ColumnKind::Dimension || c.kind == ColumnKind::TimeDimension,
                            _ => false,
                        })
                        .map(|c| c.member.as_str())
                        .collect();
                    EngineError::QueryError(format!(
                        "Motif param '{}' references '{}' which is not in the query. Available: [{}]",
                        param_name, member_name, available.join(", ")
                    ))
                })?;
            Ok(format!("b.{}", col.alias))
        }
        MotifParamType::Number | MotifParamType::String | MotifParamType::Enum => {
            validate_literal_param(param_name, value)
        }
    }
}

/// Validate and convert a literal (non-member) param value to a string safe for SQL interpolation.
fn validate_literal_param(
    param_name: &str,
    value: &serde_json::Value,
) -> Result<String, EngineError> {
    match value {
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => {
            // Allow numeric strings and simple identifiers
            if s.parse::<f64>().is_ok() || s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
                Ok(s.clone())
            } else {
                Err(EngineError::QueryError(format!(
                    "Motif param '{}' has unsafe value '{}' — only numbers and identifiers are allowed",
                    param_name, s
                )))
            }
        }
        serde_json::Value::Bool(b) => Ok(if *b { "1" } else { "0" }.to_string()),
        other => {
            Err(EngineError::QueryError(format!(
                "Motif param '{}' has unsupported type: {}",
                param_name, other
            )))
        }
    }
}

/// Substitute {{ param }} in expr with resolved values.
/// Returns an error if any param reference is not in the resolved map.
fn substitute_expr(expr: &str, resolved: &HashMap<String, String>) -> Result<String, String> {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{\s*(\w+)\s*\}\}").unwrap());
    let mut unresolved: Vec<String> = Vec::new();
    let result = re.replace_all(expr, |caps: &regex::Captures| {
        let param_name = &caps[1];
        match resolved.get(param_name) {
            Some(val) => val.clone(),
            None => {
                unresolved.push(param_name.to_string());
                format!("{{{{ {} }}}}", param_name)
            }
        }
    })
    .to_string();
    if unresolved.is_empty() {
        Ok(result)
    } else {
        Err(format!("unresolved param(s): {}", unresolved.join(", ")))
    }
}

/// Generate the full wrapped SQL, supporting multi-stage CTEs.
///
/// Single-stage (most motifs):
///   WITH __base AS (<base_sql>) SELECT b.*, <outputs> FROM __base b
///
/// Multi-stage (anomaly, trend):
///   WITH __base AS (<base_sql>),
///        __stage1 AS (SELECT b.*, <intermediate_outputs> FROM __base b)
///   SELECT s.*, <final_outputs> FROM __stage1 s
///
/// Each `{{ param }}` in output expressions is resolved to the single value
/// determined by `resolve_params`. There is no per-measure expansion — each
/// measure param resolves to exactly one column.
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
    let plan = if motif.motif_kind == MotifKind::Builtin && motif.outputs.is_empty() {
        builtin_plan(&motif.name, dialect)
    } else {
        MotifPlan {
            intermediate_outputs: vec![],
            final_outputs: motif.outputs.clone(),
        }
    };

    if plan.final_outputs.is_empty() {
        return Err(EngineError::QueryError(format!(
            "Motif '{}' has no output columns defined",
            motif.name
        )));
    }

    let has_intermediate = !plan.intermediate_outputs.is_empty();

    // Build CTE chain
    let mut cte_parts: Vec<String> = vec![format!("__base AS (\n{}\n)", base_sql)];

    // Add intermediate CTEs
    let mut prev_cte_name = "__base".to_string();
    for (i, stage_adds) in plan.intermediate_outputs.iter().enumerate() {
        let prev_alias = if i == 0 { "b" } else { "s" };
        let prev_cte = &prev_cte_name;
        let mut stage_select: Vec<String> = vec![format!("{}.*", prev_alias)];

        for col in stage_adds {
            let resolved_expr = substitute_expr(&col.expr, resolved_params)
                .map_err(|e| EngineError::QueryError(format!(
                    "Motif '{}' intermediate column '{}': {}", motif.name, col.name, e
                )))?;
            stage_select.push(format!(
                "{} AS {}",
                resolved_expr,
                dialect.quote_identifier(&col.name)
            ));
        }

        let stage_name = format!("__stage{}", i + 1);
        cte_parts.push(format!(
            "{} AS (\nSELECT\n  {}\nFROM {} {}\n)",
            stage_name,
            stage_select.join(",\n  "),
            prev_cte,
            prev_alias,
        ));
        prev_cte_name = stage_name;
    }

    // Build final SELECT
    // The final SELECT reads from the last CTE stage
    let (final_from, final_alias) = if has_intermediate {
        (format!("__stage{}", plan.intermediate_outputs.len()), "s")
    } else {
        ("__base".to_string(), "b")
    };

    // For multi-stage motifs, {{ measure }}/{{ time }}/{{ dimensions }} in the final stage
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

    for col in &plan.final_outputs {
        let resolved_expr = substitute_expr(&col.expr, &final_resolved)
            .map_err(|e| EngineError::QueryError(format!(
                "Motif '{}' output column '{}': {}", motif.name, col.name, e
            )))?;
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
        outputs: vec![], // filled at compile time via builtin_outputs()
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
        outputs: vec![],
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
        outputs: vec![],
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
        outputs: vec![],
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
        outputs: vec![],
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
        outputs: vec![],
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
        outputs: vec![],
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
        outputs: vec![],
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
            let outputs = builtin_outputs(&m.name, &Dialect::Postgres);
            assert!(!outputs.is_empty(), "Builtin '{}' should have output columns", m.name);
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

        let result = substitute_expr("LAG({{ measure }}, 1) OVER (ORDER BY {{ time }})", &resolved).unwrap();
        assert_eq!(result, "LAG(b.revenue, 1) OVER (ORDER BY b.created_at)");
    }

    #[test]
    fn test_substitute_expr_unresolved_errors() {
        let resolved = HashMap::new();
        let result = substitute_expr("{{ nonexistent }}", &resolved);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("nonexistent"));
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
        assert!(!sql.contains("REGR_SLOPE({{ measure }}, ROW_NUMBER()"), "SQL must not nest window functions:\n{}", sql);
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
            outputs: vec![MotifOutputColumn {
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

    #[test]
    fn test_moving_average_generates_window() {
        let motif = moving_average_motif();
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
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        assert!(sql.contains("ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"), "SQL: {}", sql);
        assert!(sql.contains("AVG("), "SQL: {}", sql);
        assert_eq!(out_cols.len(), 3); // 2 base + 1 motif
    }

    #[test]
    fn test_cumulative_generates_unbounded() {
        let motif = cumulative_motif();
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
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        assert!(sql.contains("UNBOUNDED PRECEDING"), "SQL: {}", sql);
        assert!(sql.contains("cumulative_value"), "SQL: {}", sql);
    }

    #[test]
    fn test_rank_generates_order_by() {
        let motif = rank_motif();
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
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        assert!(sql.contains("RANK()"), "SQL: {}", sql);
        assert!(sql.contains("ORDER BY b.events__total_revenue DESC"), "SQL: {}", sql);
    }

    #[test]
    fn test_percent_of_total_generates_ratio() {
        let motif = percent_of_total_motif();
        let columns = vec![ColumnMeta {
            member: "events.total_revenue".into(),
            alias: "events__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let resolved = resolve_params(&motif, &columns, &HashMap::new()).unwrap();
        let (sql, _) = wrap_with_motif(
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        assert!(sql.contains("100.0"), "SQL: {}", sql);
        assert!(sql.contains("NULLIF(SUM("), "SQL: {}", sql);
        assert!(sql.contains("percent_of_total"), "SQL: {}", sql);
    }

    #[test]
    fn test_multi_measure_requires_explicit_param() {
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
            ColumnMeta {
                member: "events.total_events".into(),
                alias: "events__total_events".into(),
                kind: ColumnKind::Measure,
            },
        ];
        // No explicit measure param with 2 measures → should error
        let result = resolve_params(&motif, &columns, &HashMap::new());
        assert!(result.is_err(), "Should require explicit measure param with multiple measures");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("events.total_revenue"), "Error should list available measures: {}", err_msg);
        assert!(err_msg.contains("events.total_events"), "Error should list available measures: {}", err_msg);
    }

    #[test]
    fn test_multi_measure_with_explicit_param() {
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
            ColumnMeta {
                member: "events.total_events".into(),
                alias: "events__total_events".into(),
                kind: ColumnKind::Measure,
            },
        ];
        // Explicit measure param → should work, resolving semantic name to alias
        let mut params = HashMap::new();
        params.insert("measure".to_string(), serde_json::json!("events.total_revenue"));
        let resolved = resolve_params(&motif, &columns, &params).unwrap();
        assert_eq!(resolved.get("measure").unwrap(), "b.events__total_revenue");

        let (sql, out_cols) = wrap_with_motif(
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        // 3 base + 2 motif (total, share) — no per-measure expansion
        assert_eq!(out_cols.len(), 5, "columns: {:?}", out_cols.iter().map(|c| &c.alias).collect::<Vec<_>>());
        assert!(sql.contains("total"), "SQL: {}", sql);
        assert!(sql.contains("share"), "SQL: {}", sql);
    }

    #[test]
    fn test_multi_measure_anomaly_with_explicit_param() {
        let motif = anomaly_motif();
        let columns = vec![
            ColumnMeta {
                member: "events.total_revenue".into(),
                alias: "events__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
            ColumnMeta {
                member: "events.total_events".into(),
                alias: "events__total_events".into(),
                kind: ColumnKind::Measure,
            },
        ];
        // Explicit measure → should work for one measure
        let mut params = HashMap::new();
        params.insert("measure".to_string(), serde_json::json!("events.total_events"));
        let resolved = resolve_params(&motif, &columns, &params).unwrap();
        assert_eq!(resolved.get("measure").unwrap(), "b.events__total_events");

        let (sql, out_cols) = wrap_with_motif(
            "SELECT 1", &columns, &motif, &resolved, &Dialect::Postgres, &[], None, None,
        ).unwrap();
        // 2 base + 4 motif (mean_value, stddev_value, z_score, is_anomaly)
        assert_eq!(out_cols.len(), 6, "columns: {:?}", out_cols.iter().map(|c| &c.alias).collect::<Vec<_>>());
        assert!(sql.contains("z_score"), "SQL:\n{}", sql);
        assert!(sql.contains("events__total_events"), "SQL should reference the specified measure:\n{}", sql);
    }

    #[test]
    fn test_explicit_param_invalid_member_errors() {
        let motif = contribution_motif();
        let columns = vec![
            ColumnMeta {
                member: "events.total_revenue".into(),
                alias: "events__total_revenue".into(),
                kind: ColumnKind::Measure,
            },
        ];
        let mut params = HashMap::new();
        params.insert("measure".to_string(), serde_json::json!("events.nonexistent"));
        let result = resolve_params(&motif, &columns, &params);
        assert!(result.is_err(), "Should error for non-existent member");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("events.nonexistent"), "Error should mention the bad member: {}", err_msg);
        assert!(err_msg.contains("events.total_revenue"), "Error should list available: {}", err_msg);
    }

    #[test]
    fn test_motif_params_injection_rejected() {
        let motif = anomaly_motif();
        let columns = vec![ColumnMeta {
            member: "orders.total_revenue".into(),
            alias: "orders__total_revenue".into(),
            kind: ColumnKind::Measure,
        }];
        let mut explicit = HashMap::new();
        explicit.insert("threshold".to_string(), serde_json::json!("1; DROP TABLE x--"));
        let result = resolve_params(&motif, &columns, &explicit);
        assert!(result.is_err(), "Should reject SQL injection attempt");
        assert!(result.unwrap_err().to_string().contains("unsafe value"));
    }

    #[test]
    fn test_stddev_pop_dialects() {
        // Verify anomaly uses STDDEV_POP (not STDDEV) for dialects where they differ
        assert_eq!(Dialect::BigQuery.stddev_pop(), "STDDEV_POP");
        assert_eq!(Dialect::Snowflake.stddev_pop(), "STDDEV_POP");
        assert_eq!(Dialect::DuckDB.stddev_pop(), "STDDEV_POP");
        assert_eq!(Dialect::Databricks.stddev_pop(), "STDDEV_POP");
        assert_eq!(Dialect::Postgres.stddev_pop(), "STDDEV_POP");
        assert_eq!(Dialect::ClickHouse.stddev_pop(), "stddevPop");
        assert_eq!(Dialect::MySQL.stddev_pop(), "STDDEV");
    }
}
