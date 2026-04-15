use crate::engine::metric_tree::{EdgeKind, MetricEdge, MetricTree};
use crate::engine::query::{FilterOperator, OrderBy, QueryRequest, TimeDimensionQuery};
use crate::engine::EngineError;
use crate::schema::models::{DriverDirection, DriverForm, DriverStrength};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};

// ── Sensitivity ──────────────────────────────────────────

/// A driver's influence on a target metric.
#[derive(Debug, Clone, Serialize)]
pub struct SensitivityDriver {
    /// Fully qualified measure ID.
    pub measure: String,
    /// Path from driver to target (list of measure IDs).
    pub path: Vec<String>,
    /// Edge kind at the direct connection.
    pub edge_kind: String,
    // -- Quantitative (if available) --
    /// Effective coefficient (product of coefficients along path, if all are quantitative).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_coefficient: Option<f64>,
    /// Functional form (only meaningful for direct single-hop drivers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub form: Option<DriverForm>,
    // -- Qualitative fallback --
    pub direction: DriverDirection,
    pub strength: DriverStrength,
    /// Lag in days (from the direct edge).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    /// Description from the direct edge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Result of a sensitivity analysis.
#[derive(Debug, Clone, Serialize)]
pub struct SensitivityResult {
    pub target: String,
    pub drivers: Vec<SensitivityDriver>,
}

/// Rank all drivers of a target metric by influence magnitude.
///
/// Walks the metric tree backward from the target, collecting all direct and
/// transitive drivers. For quantitative edges (with coefficients), the effective
/// coefficient is the product along the path (chain rule). Results are sorted by
/// |effective_coefficient| descending, then by qualitative strength.
pub fn sensitivity(tree: &MetricTree, target: &str) -> Result<SensitivityResult, EngineError> {
    if !tree.nodes.iter().any(|n| n.id == target) {
        return Err(EngineError::QueryError(format!(
            "Measure '{}' not found in metric tree",
            target
        )));
    }

    // Build reverse adjacency: target -> [(source, edge)]
    let mut rev_adj: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        rev_adj.entry(edge.to.as_str()).or_default().push(edge);
    }

    // BFS backward from target, tracking path and cumulative coefficient.
    // Each queue item carries the edge metadata from its direct connection,
    // avoiding a linear scan through tree.edges per node.
    struct QueueItem<'a> {
        node_id: String,
        path: Vec<String>,
        cumulative_coeff: Option<f64>,
        direct_edge: &'a MetricEdge,
    }

    let mut queue: VecDeque<QueueItem> = VecDeque::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(target.to_string());

    // Seed with direct inputs
    if let Some(edges) = rev_adj.get(target) {
        for edge in edges {
            let coeff = edge_coefficient(edge);
            queue.push_back(QueueItem {
                node_id: edge.from.clone(),
                path: vec![edge.from.clone(), target.to_string()],
                cumulative_coeff: coeff,
                direct_edge: edge,
            });
        }
    }

    let mut drivers = Vec::new();

    while let Some(item) = queue.pop_front() {
        let edge = item.direct_edge;
        let is_direct = item.path.len() == 2;

        drivers.push(SensitivityDriver {
            measure: item.node_id.clone(),
            path: item.path.clone(),
            edge_kind: edge.kind.to_string(),
            effective_coefficient: item.cumulative_coeff,
            form: if is_direct {
                Some(edge.form.clone())
            } else {
                None
            },
            direction: infer_direction(edge),
            strength: infer_strength(edge),
            lag: edge.lag,
            description: edge.description.clone(),
        });

        // Continue BFS backward (only if not visited)
        if visited.insert(item.node_id.clone()) {
            if let Some(edges) = rev_adj.get(item.node_id.as_str()) {
                for edge in edges {
                    if !visited.contains(&edge.from) {
                        let child_coeff = edge_coefficient(edge);
                        let cumulative = match (item.cumulative_coeff, child_coeff) {
                            (Some(c1), Some(c2)) => Some(c1 * c2),
                            _ => None,
                        };
                        let mut path = vec![edge.from.clone()];
                        path.extend(item.path.clone());
                        queue.push_back(QueueItem {
                            node_id: edge.from.clone(),
                            path,
                            cumulative_coeff: cumulative,
                            direct_edge: edge,
                        });
                    }
                }
            }
        }
    }

    // Sort: quantitative (by |coefficient|) first, then qualitative (by strength)
    drivers.sort_by(|a, b| {
        match (a.effective_coefficient, b.effective_coefficient) {
            (Some(ca), Some(cb)) => cb
                .abs()
                .partial_cmp(&ca.abs())
                .unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less, // quantitative first
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => strength_rank(&a.strength).cmp(&strength_rank(&b.strength)),
        }
    });

    Ok(SensitivityResult {
        target: target.to_string(),
        drivers,
    })
}

// ── Predict ──────────────────────────────────────────────

/// A predicted impact on a measure.
#[derive(Debug, Clone, Serialize)]
pub struct PredictImpact {
    /// Measure that is impacted.
    pub measure: String,
    /// Estimated change in the target.
    pub estimated_delta: f64,
    /// How confident the estimate is: "exact" for component edges, "estimated" for drivers.
    pub confidence: String,
    /// Path from the changed input to this measure.
    pub path: Vec<String>,
    /// Functional form used.
    pub form: DriverForm,
    /// Lag in days before the effect manifests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
}

/// Input change for a predict operation.
#[derive(Debug, Clone, Serialize)]
pub struct PredictInput {
    pub measure: String,
    pub delta: f64,
}

/// Result of a predict operation.
#[derive(Debug, Clone, Serialize)]
pub struct PredictResult {
    pub inputs: Vec<PredictInput>,
    pub impacts: Vec<PredictImpact>,
}

/// Propagate hypothetical changes upward through the metric tree.
///
/// For each input (measure, delta), follows outgoing edges and uses declared
/// coefficients to estimate the impact on parent metrics. Component edges
/// pass the delta through directly (exact). Driver edges with coefficients
/// apply the linear approximation (coefficient * delta). Impacts at the same
/// node from multiple paths are summed.
pub fn predict(tree: &MetricTree, changes: &[(String, f64)]) -> Result<PredictResult, EngineError> {
    // Validate all inputs exist
    for (measure, _) in changes {
        if !tree.nodes.iter().any(|n| n.id == *measure) {
            return Err(EngineError::QueryError(format!(
                "Measure '{}' not found in metric tree",
                measure
            )));
        }
    }

    // Build forward adjacency: source -> [(target, edge)]
    let mut fwd_adj: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        fwd_adj.entry(edge.from.as_str()).or_default().push(edge);
    }

    // Track cumulative impacts per node: measure_id -> (total_delta, paths)
    let mut impacts_map: HashMap<String, (f64, Vec<PredictImpact>)> = HashMap::new();

    // BFS forward from each input
    struct PropItem {
        node_id: String,
        delta: f64,
        path: Vec<String>,
        confidence: String,
        form: DriverForm,
        lag: Option<u64>,
    }

    let inputs: Vec<PredictInput> = changes
        .iter()
        .map(|(m, d)| PredictInput {
            measure: m.clone(),
            delta: *d,
        })
        .collect();

    for (input_measure, input_delta) in changes {
        let mut queue: VecDeque<PropItem> = VecDeque::new();

        // Seed: propagate from input to its direct parents
        if let Some(edges) = fwd_adj.get(input_measure.as_str()) {
            for edge in edges {
                let (delta, confidence, form) = propagate_delta(*input_delta, edge);
                // Skip zero-impact paths (qualitative-only drivers)
                if delta.abs() < f64::EPSILON {
                    continue;
                }
                queue.push_back(PropItem {
                    node_id: edge.to.clone(),
                    delta,
                    path: vec![input_measure.clone(), edge.to.clone()],
                    confidence,
                    form,
                    lag: edge.lag,
                });
            }
        }

        let mut visited: HashSet<String> = HashSet::new();
        visited.insert(input_measure.clone());

        while let Some(item) = queue.pop_front() {
            // Accumulate impact
            let entry = impacts_map
                .entry(item.node_id.clone())
                .or_insert_with(|| (0.0, Vec::new()));
            entry.0 += item.delta;
            entry.1.push(PredictImpact {
                measure: item.node_id.clone(),
                estimated_delta: item.delta,
                confidence: item.confidence.clone(),
                path: item.path.clone(),
                form: item.form.clone(),
                lag: item.lag,
            });

            // Continue propagating upward
            if visited.insert(item.node_id.clone()) {
                if let Some(edges) = fwd_adj.get(item.node_id.as_str()) {
                    for edge in edges {
                        if !visited.contains(edge.to.as_str()) {
                            let (delta, confidence, form) = propagate_delta(item.delta, edge);
                            if delta.abs() < f64::EPSILON {
                                continue;
                            }
                            let mut path = item.path.clone();
                            path.push(edge.to.clone());
                            queue.push_back(PropItem {
                                node_id: edge.to.clone(),
                                delta,
                                path,
                                confidence: if item.confidence == "estimated" {
                                    "estimated".to_string()
                                } else {
                                    confidence
                                },
                                form,
                                lag: match (item.lag, edge.lag) {
                                    (Some(a), Some(b)) => Some(a + b),
                                    (Some(a), None) => Some(a),
                                    (None, Some(b)) => Some(b),
                                    (None, None) => None,
                                },
                            });
                        }
                    }
                }
            }
        }
    }

    // Collapse to one impact per target (sum deltas from all paths)
    let mut impacts: Vec<PredictImpact> = Vec::new();
    for (measure, (total_delta, paths)) in &impacts_map {
        // Use the first path's metadata, but sum the delta
        if let Some(first) = paths.first() {
            impacts.push(PredictImpact {
                measure: measure.clone(),
                estimated_delta: *total_delta,
                confidence: if paths.iter().all(|p| p.confidence == "exact") {
                    "exact".to_string()
                } else {
                    "estimated".to_string()
                },
                path: first.path.clone(),
                form: first.form.clone(),
                lag: first.lag,
            });
        }
    }

    // Sort by |estimated_delta| descending
    impacts.sort_by(|a, b| {
        b.estimated_delta
            .abs()
            .partial_cmp(&a.estimated_delta.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(PredictResult { inputs, impacts })
}

// ── Helpers ──────────────────────────────────────────────

/// Extract the coefficient from an edge. For component edges, coefficient is 1.0
/// (direct pass-through). For driver edges, uses the declared coefficient.
fn edge_coefficient(edge: &MetricEdge) -> Option<f64> {
    match edge.kind {
        EdgeKind::Component => Some(1.0),
        EdgeKind::Driver => edge.coefficient,
    }
}

/// Propagate a delta through an edge, returning (output_delta, confidence, form).
fn propagate_delta(input_delta: f64, edge: &MetricEdge) -> (f64, String, DriverForm) {
    match edge.kind {
        EdgeKind::Component => {
            // Component edges pass through exactly
            (input_delta, "exact".to_string(), DriverForm::Linear)
        }
        EdgeKind::Driver => {
            if let Some(coeff) = edge.coefficient {
                // Linear approximation: output = coefficient * input_delta
                // For non-linear forms, this is a first-order approximation.
                let output = coeff * input_delta;
                (output, "estimated".to_string(), edge.form.clone())
            } else {
                // No coefficient — can't quantify, pass through with unknown magnitude
                (0.0, "qualitative".to_string(), DriverForm::Linear)
            }
        }
    }
}

/// Infer direction from an edge (quantitative coefficient takes precedence).
fn infer_direction(edge: &MetricEdge) -> DriverDirection {
    if let Some(coeff) = edge.coefficient {
        if coeff > 0.0 {
            DriverDirection::Positive
        } else if coeff < 0.0 {
            DriverDirection::Negative
        } else {
            DriverDirection::Unknown
        }
    } else {
        edge.direction.clone()
    }
}

/// Infer strength from an edge (quantitative coefficient takes precedence).
fn infer_strength(edge: &MetricEdge) -> DriverStrength {
    if let Some(coeff) = edge.coefficient {
        let abs = coeff.abs();
        if abs >= 0.5 {
            DriverStrength::Strong
        } else if abs >= 0.1 {
            DriverStrength::Moderate
        } else {
            DriverStrength::Weak
        }
    } else {
        edge.strength.clone()
    }
}

/// Convert strength to a numeric rank for sorting (lower = stronger).
fn strength_rank(s: &DriverStrength) -> u8 {
    match s {
        DriverStrength::Strong => 0,
        DriverStrength::Moderate => 1,
        DriverStrength::Weak => 2,
    }
}

/// Estimate the impact of a driver's change on the target, accounting for functional form.
fn compute_driver_impact(
    edge: &MetricEdge,
    driver_delta: f64,
    driver_baseline: f64,
    target_baseline: f64,
) -> Option<f64> {
    let coeff = edge.coefficient?;
    match edge.form {
        DriverForm::Linear => Some(coeff * driver_delta),
        DriverForm::LogLog => {
            // %Δtarget = coeff × %Δdriver → Δtarget = target × coeff × (Δdriver / driver)
            if driver_baseline.abs() > f64::EPSILON && target_baseline.abs() > f64::EPSILON {
                Some(target_baseline * coeff * (driver_delta / driver_baseline))
            } else {
                Some(coeff * driver_delta)
            }
        }
        DriverForm::LogLinear => {
            // %Δtarget = coeff × Δdriver → Δtarget = target × coeff × Δdriver
            if target_baseline.abs() > f64::EPSILON {
                Some(target_baseline * coeff * driver_delta)
            } else {
                Some(coeff * driver_delta)
            }
        }
        DriverForm::LinearLog => {
            // Δtarget = coeff × ln(1 + Δdriver/driver)
            if driver_baseline.abs() > f64::EPSILON {
                let ratio = 1.0 + driver_delta / driver_baseline;
                if ratio > 0.0 {
                    Some(coeff * ratio.ln())
                } else {
                    Some(0.0)
                }
            } else {
                Some(coeff * driver_delta)
            }
        }
    }
}

// ── Opportunity Sizing ──────────────────────────────────

use crate::engine::query::QueryFilter;
use crate::schema::models::{DimensionType, SemanticLayer};

/// A single segment-level opportunity (one dimension value that underperforms).
#[derive(Debug, Clone, Serialize)]
pub struct SegmentOpportunity {
    /// Dimension value (e.g., "android").
    pub segment: String,
    /// Current measure value for this segment.
    pub current_value: f64,
    /// Benchmark value (e.g., weighted average across all segments).
    pub benchmark: f64,
    /// Gap to benchmark (positive = upside).
    pub gap: f64,
    /// Share of total volume in this segment.
    pub segment_share: f64,
    /// Weighted gap: gap × share (contribution to overall improvement).
    pub weighted_gap: f64,
}

/// Opportunities found along one dimension.
#[derive(Debug, Clone, Serialize)]
pub struct DimensionOpportunity {
    /// Fully qualified dimension (e.g., "funnel.platform").
    pub dimension: String,
    /// Total weighted gap if all underperformers matched the benchmark.
    pub total_weighted_gap: f64,
    /// Per-segment detail, sorted by weighted_gap descending.
    pub segments: Vec<SegmentOpportunity>,
}

/// Full result of an opportunity sizing analysis.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityResult {
    pub target: String,
    pub period: (String, String),
    pub overall_value: f64,
    /// Opportunities per dimension, sorted by total_weighted_gap descending.
    pub dimensions: Vec<DimensionOpportunity>,
    /// Downstream impacts from the top opportunity, propagated via drivers.
    pub downstream: Vec<PredictImpact>,
}

/// Run opportunity sizing: find underperforming segments and size the upside.
///
/// For each dimension of the target measure's view, queries the measure broken
/// down by segment, compares each segment to the weighted-average benchmark,
/// and calculates the gap. The top opportunity's weighted gap is then
/// propagated through the metric tree via driver coefficients.
pub fn opportunity(
    tree: &MetricTree,
    layer: &SemanticLayer,
    target: &str,
    time_dimension: &str,
    period: (&str, &str),
    executor: &QueryExecutor,
) -> Result<OpportunityResult, EngineError> {
    let target_node = tree
        .nodes
        .iter()
        .find(|n| n.id == target)
        .ok_or_else(|| {
            EngineError::QueryError(format!("Measure '{}' not found in metric tree", target))
        })?;

    let target_view = target.split('.').next().unwrap_or("");

    // Additive measures (count/sum) use per-segment average as benchmark.
    // Ratios (type: number) use the overall value (true weighted average).
    let is_additive = matches!(
        target_node.measure_type.as_str(),
        "count" | "sum" | "count_distinct" | "avg" | "min" | "max"
    );

    // Date range filters (use plain filters, not time_dimensions, to avoid
    // adding the time column to GROUP BY which would give per-date rows).
    let date_filters = vec![
        QueryFilter {
            member: Some(time_dimension.to_string()),
            operator: Some(FilterOperator::AfterOrOnDate),
            values: vec![period.0.to_string()],
            and: None,
            or: None,
        },
        QueryFilter {
            member: Some(time_dimension.to_string()),
            operator: Some(FilterOperator::BeforeOrOnDate),
            values: vec![period.1.to_string()],
            and: None,
            or: None,
        },
    ];

    // 1) Query overall value (no dimension breakdown)
    let overall_query = QueryRequest {
        measures: vec![target.to_string()],
        filters: date_filters.clone(),
        ..QueryRequest::new()
    };
    let overall_rows = executor(&overall_query)?;
    let measure_alias = target.replace('.', "__");
    let overall_value = overall_rows
        .first()
        .map(|r| extract_measure_value(r, &measure_alias))
        .unwrap_or(0.0);

    // 2) Discover non-time dimensions
    let dims = discover_dimensions(layer, target_view);

    // 3) For each dimension, query breakdown and find gaps
    let mut dim_opps: Vec<DimensionOpportunity> = Vec::new();

    for dim in &dims {
        let breakdown_query = QueryRequest {
            measures: vec![target.to_string()],
            dimensions: vec![dim.clone()],
            filters: date_filters.clone(),
            ..QueryRequest::new()
        };
        let rows = match executor(&breakdown_query) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if rows.is_empty() {
            continue;
        }

        let dim_alias = dim.replace('.', "__");

        // Extract per-segment values
        struct SegRow {
            segment: String,
            value: f64,
        }
        let seg_rows: Vec<SegRow> = rows
            .iter()
            .map(|r| SegRow {
                segment: extract_dim_value(r, &dim_alias),
                value: extract_measure_value(r, &measure_alias),
            })
            .collect();

        let n_segments = seg_rows.len() as f64;
        if n_segments < 1.0 {
            continue;
        }

        // For additive measures, benchmark = fair share (overall / n).
        // For ratios, benchmark = overall value (weighted average).
        let benchmark = if is_additive {
            overall_value / n_segments
        } else {
            overall_value
        };
        let equal_share = 1.0 / n_segments;

        let mut segments: Vec<SegmentOpportunity> = seg_rows
            .iter()
            .filter(|s| s.value < benchmark)
            .map(|s| {
                let gap = benchmark - s.value;
                SegmentOpportunity {
                    segment: s.segment.clone(),
                    current_value: s.value,
                    benchmark,
                    gap,
                    segment_share: equal_share,
                    weighted_gap: gap * equal_share,
                }
            })
            .collect();

        segments.sort_by(|a, b| {
            b.weighted_gap
                .partial_cmp(&a.weighted_gap)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_weighted_gap: f64 = segments.iter().map(|s| s.weighted_gap).sum();
        if total_weighted_gap.abs() < f64::EPSILON {
            continue;
        }

        dim_opps.push(DimensionOpportunity {
            dimension: dim.clone(),
            total_weighted_gap,
            segments,
        });
    }

    // Sort dimensions by total weighted gap descending
    dim_opps.sort_by(|a, b| {
        b.total_weighted_gap
            .partial_cmp(&a.total_weighted_gap)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // 4) Propagate the top opportunity's gap through the metric tree
    let downstream: Vec<PredictImpact> = if let Some(top_dim) = dim_opps.first() {
        let total_gap = top_dim.total_weighted_gap;
        let predict_result = predict(tree, &[(target.to_string(), total_gap)])?;
        predict_result
            .impacts
            .into_iter()
            .filter(|i| i.measure != target)
            .collect()
    } else {
        Vec::new()
    };

    Ok(OpportunityResult {
        target: target.to_string(),
        period: (period.0.to_string(), period.1.to_string()),
        overall_value,
        dimensions: dim_opps,
        downstream,
    })
}

// ── Explain (Recursive RCA) ─────────────────────────────

/// Configuration for the recursive explain algorithm.
#[derive(Debug, Clone)]
pub struct ExplainConfig {
    /// Stop adding top-level splits when cumulative coverage reaches this (0.0..1.0).
    pub coverage_threshold: f64,
    /// Maximum recursion depth.
    pub max_depth: usize,
    /// Maximum number of dimension values to consider per split.
    pub max_dim_values: usize,
    /// Stop recursing when best child's concentration < this (local signal threshold).
    pub min_concentration: f64,
    /// Safety net: stop when root fraction drops below this (prevents 0.8^N decay).
    pub min_root_fraction: f64,
    /// Enable deep beam search mode.
    pub deep: bool,
    /// Beam width for deep search (candidates kept per level).
    pub beam_width: usize,
    /// Maximum alternative explanations to return.
    pub max_alternatives: usize,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            coverage_threshold: 0.80,
            max_depth: 10,
            max_dim_values: 20,
            min_concentration: 0.05,
            min_root_fraction: 0.005,
            deep: false,
            beam_width: 10,
            max_alternatives: 5,
        }
    }
}

/// The kind of split chosen at each step.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SplitKind {
    /// Narrowed to a child measure in the metric tree.
    Component { child_measure: String },
    /// Narrowed to a specific dimension value.
    Dimension { dimension: String, value: String },
    /// All segments degraded roughly uniformly (no single outlier).
    UniformDegradation { dimension: String, num_elements: usize },
    /// A dimension value appears as a driver across multiple measures.
    CrossCutting { dimension: String, value: String, measures: Vec<String> },
}

/// A non-recursed sibling shown for context alongside the recursed path.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainSibling {
    /// What split this represents.
    pub split: SplitKind,
    /// The measure at this node.
    pub measure: String,
    /// Delta observed.
    pub delta: f64,
    /// Cascaded root fraction (same formula as ExplainNode.root_fraction).
    pub root_fraction: f64,
}

/// A single node in the explain result tree.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainNode {
    /// What split was taken to reach this node.
    pub split: SplitKind,
    /// The measure being examined at this node.
    pub measure: String,
    /// Filters active at this node (accumulated dimension splits).
    pub filters: Vec<QueryFilter>,
    /// Delta observed for this split.
    pub delta: f64,
    /// Fraction of the parent's delta explained by this split (raw, for ranking).
    pub concentration: f64,
    /// Fraction of the root's delta explained by this split, cascaded through
    /// the tree and normalized for scaling factors (e.g. ×12 in `arr = net_mrr * 12`).
    pub root_fraction: f64,
    /// Non-recursed siblings at this split level (all components / top-N dimensions).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub siblings: Vec<ExplainSibling>,
    /// For dimension splits: total number of unique values (for "showing X of Y").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension_count: Option<usize>,
    /// Children (further splits).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ExplainNode>,
}

/// Attribution of the target's change to a declared driver measure.
#[derive(Debug, Clone, Serialize)]
pub struct DriverAttribution {
    /// Fully qualified driver measure ID.
    pub driver_measure: String,
    /// Driver's previous period value.
    pub driver_previous: f64,
    /// Driver's current period value.
    pub driver_current: f64,
    /// Driver's delta (current - previous).
    pub driver_delta: f64,
    /// Coefficient from the driver edge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coefficient: Option<f64>,
    /// Functional form of the driver relationship.
    pub form: DriverForm,
    /// Estimated impact on the target (using declared coefficient and form).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_target_impact: Option<f64>,
    /// Description from the driver edge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A complete explanation path found by the deep beam search.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainPath {
    /// Chain of splits from root to leaf.
    pub nodes: Vec<ExplainNode>,
    /// Total fraction of root delta explained by this path.
    pub root_fraction: f64,
    /// Which scoring strategy found this path.
    pub strategy: String,
    /// Statistical significance test result (deep mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub significance: Option<SignificanceTest>,
}

/// Result of a two-tailed t-test against historical period deltas.
#[derive(Debug, Clone, Serialize)]
pub struct SignificanceTest {
    pub p_value: f64,
    pub historical_periods: usize,
    pub historical_mean_delta: f64,
    pub historical_std_delta: f64,
}

/// Detection heuristic warnings (always checked, not just --deep).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ExplainWarning {
    SimpsonsParadox {
        dimension: String,
        aggregate_delta: f64,
        segment_directions: Vec<(String, f64)>,
    },
    OpposingOffset {
        component_a: String,
        component_b: String,
        delta_a: f64,
        delta_b: f64,
    },
}

/// Top-level result of the recursive explain.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainResult {
    /// The root measure that was explained.
    pub target: String,
    /// Overall delta (current - previous).
    pub target_delta: f64,
    /// Previous period value.
    pub target_previous: f64,
    /// Current period value.
    pub target_current: f64,
    /// Time dimension used.
    pub time_dimension: String,
    /// Current period range.
    pub current_period: (String, String),
    /// Previous period range.
    pub previous_period: (String, String),
    /// The tree of explanations.
    pub nodes: Vec<ExplainNode>,
    /// Total fraction of target_delta explained.
    pub coverage: f64,
    /// Driver attribution: how much each declared driver changed and its estimated
    /// contribution to the target's change. Only populated when the target has driver edges.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub driver_attribution: Vec<DriverAttribution>,
    /// Deep beam search results (empty unless deep mode enabled).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<ExplainPath>,
    /// Detection heuristic warnings (always populated).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ExplainWarning>,
}

/// A metric's change between two periods (used internally).
#[derive(Debug, Clone)]
struct MetricDelta {
    previous: f64,
    current: f64,
    delta: f64,
}

/// Callback type for executing a query and returning rows.
/// The explain algorithm is in the non-feature-gated engine module,
/// so actual database execution is injected via this callback.
pub type QueryExecutor =
    dyn Fn(&QueryRequest) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, EngineError>;

/// Immutable context shared across all recursion levels of the explain algorithm.
struct ExplainCtx<'a> {
    dim_cache: HashMap<&'a str, Vec<String>>,
    children_of: HashMap<&'a str, Vec<&'a MetricEdge>>,
    time_dimension: &'a str,
    current_period: (&'a str, &'a str),
    previous_period: (&'a str, &'a str),
    config: &'a ExplainConfig,
    executor: &'a QueryExecutor,
    warnings: std::cell::RefCell<Vec<ExplainWarning>>,
}

/// Detect Simpson's paradox: all dimension elements moved opposite to the aggregate.
fn detect_simpsons_paradox(
    parent_delta: f64,
    dim: &str,
    elements: &[ElementScore],
) -> Option<ExplainWarning> {
    if elements.is_empty() || parent_delta.abs() < f64::EPSILON {
        return None;
    }
    let parent_sign = parent_delta.signum();
    let all_opposing = elements.iter().all(|e| {
        if e.delta.abs() < f64::EPSILON {
            true // zero delta is neutral
        } else {
            e.delta.signum() != parent_sign
        }
    });
    let has_meaningful = elements
        .iter()
        .any(|e| e.delta.abs() > f64::EPSILON && e.delta.signum() != parent_sign);
    if all_opposing && has_meaningful {
        Some(ExplainWarning::SimpsonsParadox {
            dimension: dim.to_string(),
            aggregate_delta: parent_delta,
            segment_directions: elements
                .iter()
                .map(|e| (e.value.clone(), e.delta))
                .collect(),
        })
    } else {
        None
    }
}

/// Detect opposing offsets: two components with deltas that substantially cancel.
fn detect_opposing_offsets(
    component_deltas: &[(String, f64)], // (measure, signed_delta)
) -> Vec<ExplainWarning> {
    let mut warnings = Vec::new();
    for i in 0..component_deltas.len() {
        for j in (i + 1)..component_deltas.len() {
            let (ref a_name, a_delta) = component_deltas[i];
            let (ref b_name, b_delta) = component_deltas[j];
            if a_delta.signum() != b_delta.signum()
                && a_delta.abs() > f64::EPSILON
                && b_delta.abs() > f64::EPSILON
            {
                let masking_ratio =
                    a_delta.abs().min(b_delta.abs()) / a_delta.abs().max(b_delta.abs());
                if masking_ratio > 0.3 {
                    warnings.push(ExplainWarning::OpposingOffset {
                        component_a: a_name.clone(),
                        component_b: b_name.clone(),
                        delta_a: a_delta,
                        delta_b: b_delta,
                    });
                }
            }
        }
    }
    warnings
}

/// A measure identified as searchable for the deep beam pass.
#[allow(dead_code)]
struct SearchableMeasure {
    measure: String,
    /// Product of edge signs from root to this measure.
    cumulative_sign: f64,
    /// Available non-time dimensions for this measure's view.
    dimensions: Vec<String>,
}

/// Build reverse adjacency map: to_measure -> [edges pointing to it].
#[allow(dead_code)]
fn build_children_of<'a>(tree: &'a MetricTree) -> HashMap<&'a str, Vec<&'a MetricEdge>> {
    let mut children_of: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        children_of.entry(edge.to.as_str()).or_default().push(edge);
    }
    children_of
}

/// Decompose a target measure into searchable measures by walking component edges.
///
/// A measure is searchable if:
/// - It's a leaf (no component children), OR
/// - It's an intermediate composite that has its own dimensions.
///
/// The target itself is excluded.
#[allow(dead_code)]
fn decompose_to_searchable(
    _tree: &MetricTree,
    layer: &SemanticLayer,
    target: &str,
    children_of: &HashMap<&str, Vec<&MetricEdge>>,
) -> Vec<SearchableMeasure> {
    let mut result = Vec::new();
    let mut stack: Vec<(&str, f64)> = vec![(target, 1.0)];
    let mut visited: HashSet<&str> = HashSet::new();

    while let Some((measure, cum_sign)) = stack.pop() {
        if !visited.insert(measure) {
            continue;
        }

        let component_children: Vec<(&str, f64)> = children_of
            .get(measure)
            .map(|edges| {
                edges
                    .iter()
                    .filter(|e| e.kind == EdgeKind::Component)
                    .map(|e| (e.from.as_str(), cum_sign * e.sign))
                    .collect()
            })
            .unwrap_or_default();

        if component_children.is_empty() {
            // Leaf measure
            let view_name = measure.split('.').next().unwrap_or("");
            let dims = discover_dimensions(layer, view_name);
            result.push(SearchableMeasure {
                measure: measure.to_string(),
                cumulative_sign: cum_sign,
                dimensions: dims,
            });
        } else {
            // Intermediate composite
            let view_name = measure.split('.').next().unwrap_or("");
            let dims = discover_dimensions(layer, view_name);
            if !dims.is_empty() && measure != target {
                result.push(SearchableMeasure {
                    measure: measure.to_string(),
                    cumulative_sign: cum_sign,
                    dimensions: dims,
                });
            }
            for (child, child_sign) in component_children {
                stack.push((child, child_sign));
            }
        }
    }

    result
}

/// Detect cross-cutting patterns: same base-dimension-name=value appearing across multiple measures
/// from different views. Groups by (bare_dim_name, value) so that `ads.region` and `subs.region`
/// are treated as the same dimension name "region".
fn detect_cross_cutting(paths: &[(ExplainPath, f64)]) -> Vec<ExplainPath> {
    // Group by (bare_dimension_name, value) -> list of (fully_qualified_dim, measure, root_fraction)
    let mut dim_val_groups: HashMap<(String, String), Vec<(String, String, f64)>> = HashMap::new();
    for (path, _leaf_share) in paths {
        for node in &path.nodes {
            if let SplitKind::Dimension { dimension, value } = &node.split {
                // Use bare name (after last '.') so ads.region and subs.region both map to "region"
                let bare_dim = dimension.split('.').last().unwrap_or(dimension.as_str()).to_string();
                dim_val_groups
                    .entry((bare_dim, value.clone()))
                    .or_default()
                    .push((dimension.clone(), node.measure.clone(), path.root_fraction));
            }
        }
    }

    let mut cross_cutting_paths = Vec::new();
    for ((bare_dim, value), entries) in &dim_val_groups {
        if entries.len() < 2 {
            continue;
        }
        // Deduplicate measures (same measure from different strategies)
        let unique_measures: HashSet<&str> = entries.iter().map(|(_, m, _)| m.as_str()).collect();
        if unique_measures.len() < 2 {
            continue;
        }
        let combined_fraction: f64 = {
            // Sum the max root_fraction per unique measure
            let mut per_measure: HashMap<&str, f64> = HashMap::new();
            for (_, m, rf) in entries {
                let entry = per_measure.entry(m.as_str()).or_insert(0.0_f64);
                *entry = entry.max(*rf);
            }
            per_measure.values().sum()
        };
        let measure_names: Vec<String> = unique_measures.into_iter().map(|s| s.to_string()).collect();

        cross_cutting_paths.push(ExplainPath {
            nodes: vec![ExplainNode {
                split: SplitKind::CrossCutting {
                    dimension: bare_dim.clone(),
                    value: value.clone(),
                    measures: measure_names,
                },
                measure: String::new(),
                filters: vec![],
                delta: 0.0,
                concentration: combined_fraction,
                root_fraction: combined_fraction,
                siblings: vec![],
                dimension_count: None,
                children: vec![],
            }],
            root_fraction: combined_fraction,
            strategy: "cross_cutting".to_string(),
            significance: None,
        });
    }
    cross_cutting_paths
}

/// Run the recursive root-cause analysis.
///
/// Executes queries to find the smallest (component, dimension-segment) pairs
/// that explain why a metric changed between two time periods.
pub fn explain(
    tree: &MetricTree,
    layer: &SemanticLayer,
    target: &str,
    time_dimension: &str,
    current_period: (&str, &str),
    previous_period: (&str, &str),
    config: &ExplainConfig,
    executor: &QueryExecutor,
) -> Result<ExplainResult, EngineError> {
    if !tree.nodes.iter().any(|n| n.id == target) {
        return Err(EngineError::QueryError(format!(
            "Measure '{}' not found in metric tree",
            target
        )));
    }

    // Build reverse adjacency: child -> parent edges (for looking up children of a measure)
    let mut children_of: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        children_of.entry(edge.to.as_str()).or_default().push(edge);
    }

    // Execute target aggregate to get overall delta
    let target_query = make_period_query(
        target,
        time_dimension,
        previous_period.0,
        current_period.1,
        &[],
        &[],
    );
    let target_rows = executor(&target_query)?;
    let target_md = extract_delta(target, &target_rows);

    if target_md.delta.abs() < f64::EPSILON {
        return Ok(ExplainResult {
            target: target.to_string(),
            target_delta: 0.0,
            target_previous: target_md.previous,
            target_current: target_md.current,
            time_dimension: time_dimension.to_string(),
            current_period: (current_period.0.to_string(), current_period.1.to_string()),
            previous_period: (previous_period.0.to_string(), previous_period.1.to_string()),
            nodes: vec![],
            coverage: 1.0,
            driver_attribution: vec![],
            alternatives: vec![],
            warnings: vec![],
        });
    }

    // Pre-compute dimensions per view to avoid repeated scans
    let dim_cache: HashMap<&str, Vec<String>> = layer
        .views
        .iter()
        .map(|v| (v.name.as_str(), discover_dimensions(layer, &v.name)))
        .collect();

    let target_view = target.split('.').next().unwrap_or("");
    let available_dims = dim_cache.get(target_view).cloned().unwrap_or_default();

    let ctx = ExplainCtx {
        dim_cache,
        children_of,
        time_dimension,
        current_period,
        previous_period,
        config,
        executor,
        warnings: std::cell::RefCell::new(Vec::new()),
    };

    // Recursive search
    let mut nodes = Vec::new();
    let mut covered = 0.0_f64;

    recurse(
        &ctx,
        target,
        target_md.delta,
        &[], // no filters yet
        &available_dims,
        0,
        true, // top level — coverage accrues here
        1.0,  // root explains 100% of itself
        &mut nodes,
        &mut covered,
    )?;

    // Driver attribution: for each driver edge pointing to the target,
    // query the driver's change and estimate its impact on the target.
    let mut driver_attribution = Vec::new();
    for edge in &tree.edges {
        if edge.to != target || edge.kind != EdgeKind::Driver {
            continue;
        }
        let driver_query = make_period_query(
            &edge.from,
            time_dimension,
            previous_period.0,
            current_period.1,
            &[],
            &[],
        );
        if let Ok(rows) = executor(&driver_query) {
            let md = extract_delta(&edge.from, &rows);
            let estimated_impact = compute_driver_impact(
                edge,
                md.delta,
                md.previous,
                target_md.previous,
            );
            driver_attribution.push(DriverAttribution {
                driver_measure: edge.from.clone(),
                driver_previous: md.previous,
                driver_current: md.current,
                driver_delta: md.delta,
                coefficient: edge.coefficient,
                form: edge.form.clone(),
                estimated_target_impact: estimated_impact,
                description: edge.description.clone(),
            });
        }
    }
    driver_attribution.sort_by(|a, b| {
        let a_imp = a.estimated_target_impact.unwrap_or(0.0).abs();
        let b_imp = b.estimated_target_impact.unwrap_or(0.0).abs();
        b_imp
            .partial_cmp(&a_imp)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Opposing offset detection: check component children of the target
    let mut component_deltas: Vec<(String, f64)> = Vec::new();
    if let Some(edges) = ctx.children_of.get(target) {
        for edge in edges {
            if edge.kind != EdgeKind::Component {
                continue;
            }
            let q = make_period_query(
                &edge.from,
                time_dimension,
                previous_period.0,
                current_period.1,
                &[],
                &[],
            );
            if let Ok(rows) = executor(&q) {
                let md = extract_delta(&edge.from, &rows);
                component_deltas.push((edge.from.clone(), md.delta * edge.sign));
            }
        }
    }
    let offset_warnings = detect_opposing_offsets(&component_deltas);

    let mut warnings = ctx.warnings.into_inner();
    warnings.extend(offset_warnings);

    // ── Deep pass (beam search) ──────────────────────────
    let mut alternatives = Vec::new();
    if config.deep {
        // Phase 1: decompose to searchable measures
        let searchable = decompose_to_searchable(tree, layer, target, &ctx.children_of);

        // Query aggregate deltas for each searchable measure
        let mut measure_deltas: Vec<(String, f64, f64, Vec<String>)> = Vec::new();
        for sm in &searchable {
            let q = make_period_query(
                &sm.measure, time_dimension,
                previous_period.0, current_period.1, &[], &[],
            );
            if let Ok(rows) = executor(&q) {
                let md = extract_delta(&sm.measure, &rows);
                let leaf_share = if target_md.delta.abs() > f64::EPSILON {
                    (md.delta * sm.cumulative_sign) / target_md.delta
                } else {
                    0.0
                };
                measure_deltas.push((sm.measure.clone(), md.delta, leaf_share, sm.dimensions.clone()));
            }
        }

        // If target itself has dimensions and isn't in searchable, also search it
        if !available_dims.is_empty() {
            let already_included = measure_deltas.iter().any(|(m, _, _, _)| m == target);
            if !already_included {
                measure_deltas.push((target.to_string(), target_md.delta, 1.0, available_dims.clone()));
            }
        }

        // Phase 2: per-measure beam search
        let mut all_paths: Vec<(ExplainPath, f64)> = Vec::new();
        for (measure, delta, leaf_share, dims) in &measure_deltas {
            if dims.is_empty() || delta.abs() < f64::EPSILON {
                continue;
            }
            let paths = beam_search_measure(
                measure, *delta, dims, &[], time_dimension,
                previous_period, current_period, config, executor,
            )?;
            for mut path in paths {
                path.root_fraction *= leaf_share.abs();
                all_paths.push((path, *leaf_share));
            }
        }

        // Phase 3: cross-cutting detection
        let cross_cutting = detect_cross_cutting(&all_paths);
        for cc in cross_cutting {
            all_paths.push((cc, 1.0));
        }

        // Sort and truncate
        all_paths.sort_by(|a, b| b.0.root_fraction.partial_cmp(&a.0.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
        let mut top_paths: Vec<ExplainPath> = all_paths.into_iter().take(config.max_alternatives).map(|(p, _)| p).collect();

        // Phase 5: Statistical significance — test each top-K path against historical variance.
        // Query 12 months of monthly data before the previous period to get historical deltas.
        // Dedup by (measure, filters) to avoid redundant queries when paths share terminal nodes.
        let mut hist_cache: HashMap<String, Option<Vec<f64>>> = HashMap::new();
        for path in &mut top_paths {
            if let Some(last_node) = path.nodes.last() {
                let cache_key = dedup_key(&last_node.measure, &last_node.filters);
                let historical_deltas = hist_cache.entry(cache_key).or_insert_with(|| {
                    let hist_q = make_historical_query(
                        &last_node.measure, time_dimension, previous_period.0,
                        &last_node.filters,
                    );
                    let hist_rows = executor(&hist_q).ok()?;
                    let measure_alias = last_node.measure.replace('.', "__");
                    let monthly_values: Vec<f64> = hist_rows
                        .iter()
                        .map(|r| extract_measure_value(r, &measure_alias))
                        .collect();
                    if monthly_values.len() >= 2 {
                        Some(monthly_values.windows(2).map(|w| w[1] - w[0]).collect())
                    } else {
                        None
                    }
                });
                if let Some(deltas) = historical_deltas {
                    path.significance = compute_significance(last_node.delta, deltas);
                }
            }
        }

        alternatives = top_paths;
    }

    Ok(ExplainResult {
        target: target.to_string(),
        target_delta: target_md.delta,
        target_previous: target_md.previous,
        target_current: target_md.current,
        time_dimension: time_dimension.to_string(),
        current_period: (current_period.0.to_string(), current_period.1.to_string()),
        previous_period: (previous_period.0.to_string(), previous_period.1.to_string()),
        nodes,
        coverage: covered,
        driver_attribution,
        alternatives,
        warnings,
    })
}

/// Discover non-time dimensions from a view (string, number, boolean).
fn discover_dimensions(layer: &SemanticLayer, view_name: &str) -> Vec<String> {
    layer
        .views
        .iter()
        .find(|v| v.name == view_name)
        .map(|v| {
            v.dimensions
                .iter()
                .filter(|d| {
                    matches!(
                        d.dimension_type,
                        DimensionType::String | DimensionType::Number | DimensionType::Boolean
                    )
                })
                .map(|d| format!("{}.{}", view_name, d.name))
                .collect()
        })
        .unwrap_or_default()
}

/// Build a QueryRequest that spans two periods with optional dimensions and filters.
fn make_period_query(
    measure: &str,
    time_dimension: &str,
    period_start: &str,
    period_end: &str,
    extra_dimensions: &[String],
    filters: &[QueryFilter],
) -> QueryRequest {
    QueryRequest {
        measures: vec![measure.to_string()],
        dimensions: extra_dimensions.to_vec(),
        filters: filters.to_vec(),
        time_dimensions: vec![TimeDimensionQuery {
            dimension: time_dimension.to_string(),
            granularity: Some("month".to_string()),
            date_range: Some(vec![period_start.to_string(), period_end.to_string()]),
        }],
        order: vec![OrderBy {
            id: format!("{}.month", time_dimension),
            desc: false,
        }],
        ..QueryRequest::new()
    }
}

/// Build a query for ~12 months of historical monthly data ending before `before_date`.
/// Used by Phase 5 significance testing to gather historical variance.
fn make_historical_query(
    measure: &str,
    time_dimension: &str,
    before_date: &str,
    filters: &[QueryFilter],
) -> QueryRequest {
    // Parse the before_date to compute 12 months prior.
    // Expected format: "YYYY-MM-DD". If parsing fails, use a safe fallback.
    let hist_start = if before_date.len() >= 10 {
        if let Ok(year) = before_date[..4].parse::<i32>() {
            format!("{}-{}", year - 1, &before_date[4..])
        } else {
            "2000-01-01".to_string()
        }
    } else {
        "2000-01-01".to_string()
    };
    make_period_query(measure, time_dimension, &hist_start, before_date, &[], filters)
}

/// Extract previous/current delta from 2 rows ordered by time ASC.
fn extract_delta(
    measure: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> MetricDelta {
    let measure_alias = measure.replace('.', "__");
    let (prev, curr) = match rows.len() {
        0 => (0.0, 0.0),
        1 => (0.0, extract_measure_value(&rows[0], &measure_alias)),
        _ => (
            extract_measure_value(&rows[0], &measure_alias),
            extract_measure_value(&rows[1], &measure_alias),
        ),
    };
    MetricDelta {
        previous: prev,
        current: curr,
        delta: curr - prev,
    }
}

/// Extract a numeric value from a row's measure column.
fn extract_measure_value(
    row: &serde_json::Map<String, serde_json::Value>,
    measure_alias: &str,
) -> f64 {
    row.get(measure_alias).map(json_to_f64).unwrap_or(0.0)
}

fn json_to_f64(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Extract a string dimension value from a row, coercing Null and non-string types.
fn extract_dim_value(
    row: &serde_json::Map<String, serde_json::Value>,
    dim_alias: &str,
) -> String {
    row.get(dim_alias)
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => "NULL".to_string(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| "NULL".to_string())
}

/// Candidate split evaluated during recursion.
struct Candidate {
    split: SplitKind,
    /// The measure to recurse on after this split.
    next_measure: String,
    /// Filters to apply after this split.
    next_filters: Vec<QueryFilter>,
    /// Available dimensions for further recursion.
    next_dims: Vec<String>,
    /// Observed delta for this candidate.
    delta: f64,
    /// Signed fraction of parent_delta (explanatory power).
    concentration: f64,
    /// Normalized share of parent's change, accounting for scaling factors.
    /// For dimensions: same as concentration.
    /// For components: normalized by total_attributed (strips out e.g. ×12 in `arr = net_mrr * 12`).
    parent_share: f64,
    /// Per-element JSD surprise (only meaningful for dimension splits).
    _surprise: f64,
}

/// Per-element JSD contribution: measures how much this element's share shifted
/// between the prior (previous period) and posterior (current period) distributions.
fn jsd_element(p: f64, q: f64) -> f64 {
    let m = (p + q) / 2.0;
    if m < f64::EPSILON {
        return 0.0;
    }
    let mut s = 0.0;
    if p > 0.0 {
        s += p * (p / m).ln();
    }
    if q > 0.0 {
        s += q * (q / m).ln();
    }
    0.5 * s
}

/// JSD with Laplace smoothing to handle zero shares.
fn jsd_element_smoothed(p: f64, q: f64, epsilon: f64) -> f64 {
    let p_s = p + epsilon;
    let q_s = q + epsilon;
    let m = (p_s + q_s) / 2.0;
    if m < f64::EPSILON {
        return 0.0;
    }
    let mut s = 0.0;
    if p_s > 0.0 {
        s += p_s * (p_s / m).ln();
    }
    if q_s > 0.0 {
        s += q_s * (q_s / m).ln();
    }
    0.5 * s
}

/// Compute Information Value for a dimension.
/// Input: slice of (prev_share, curr_share) per element, with Laplace epsilon.
fn compute_iv(elements: &[(f64, f64)], epsilon: f64) -> f64 {
    elements
        .iter()
        .map(|(p, q)| {
            let p_s = p + epsilon;
            let q_s = q + epsilon;
            let woe = (q_s / p_s).ln();
            (q_s - p_s) * woe
        })
        .sum()
}

/// Laplace smoothing epsilon: 1 / total (or 1e-10 if total ≈ 0).
fn laplace_epsilon(total: f64) -> f64 {
    if total.abs() > f64::EPSILON { 1.0 / total } else { 1e-10 }
}

/// Find the index of the element with the highest |WOE| value.
/// WOE = ln(curr_share / prev_share). Returns None if elements is empty.
fn best_woe_index(
    elements: &[ElementScore],
    prev_denom: f64,
    curr_denom: f64,
    epsilon: f64,
) -> Option<usize> {
    elements
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let p = (e.previous + epsilon) / prev_denom;
            let q = (e.current + epsilon) / curr_denom;
            let woe = (q / p).ln();
            (i, woe.abs())
        })
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}

/// Per-element scores for Adtributor-style dimension evaluation.
struct ElementScore {
    value: String,
    previous: f64,
    current: f64,
    delta: f64,
    /// Explanatory power: delta_i / delta_total.
    ep: f64,
    /// Per-element JSD surprise.
    surprise: f64,
}

/// Compute per-element EP and JSD surprise from breakdown rows.
///
/// Based on the Adtributor algorithm (Bhagwan et al., NSDI 2014):
/// - EP_i = (current_i - previous_i) / (current_total - previous_total)
/// - surprise_i = JSD(p_i, q_i) where p_i = prev_i/prev_total, q_i = curr_i/curr_total
fn compute_element_scores(
    measure: &str,
    dim: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
    parent_delta: f64,
) -> Vec<ElementScore> {
    let measure_alias = measure.replace('.', "__");
    let dim_alias = dim.replace('.', "__");

    // Group rows by dimension value, extract (previous, current)
    let mut groups: HashMap<String, Vec<&serde_json::Map<String, serde_json::Value>>> =
        HashMap::new();
    for row in rows {
        let dim_val = extract_dim_value(row, &dim_alias);
        groups.entry(dim_val).or_default().push(row);
    }

    let mut elements: Vec<ElementScore> = groups
        .into_iter()
        .map(|(value, group_rows)| {
            let (previous, current) = match group_rows.len() {
                0 => (0.0, 0.0),
                1 => (0.0, extract_measure_value(group_rows[0], &measure_alias)),
                _ => (
                    extract_measure_value(group_rows[0], &measure_alias),
                    extract_measure_value(group_rows[1], &measure_alias),
                ),
            };
            ElementScore {
                value,
                previous,
                current,
                delta: current - previous,
                ep: 0.0,
                surprise: 0.0,
            }
        })
        .collect();

    // Compute totals for prior/posterior distributions
    let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
    let total_curr: f64 = elements.iter().map(|e| e.current).sum();

    // Compute EP and surprise per element
    for elem in &mut elements {
        elem.ep = if parent_delta.abs() > f64::EPSILON {
            elem.delta / parent_delta
        } else {
            0.0
        };
        let p = if total_prev.abs() > f64::EPSILON {
            elem.previous / total_prev
        } else {
            0.0
        };
        let q = if total_curr.abs() > f64::EPSILON {
            elem.current / total_curr
        } else {
            0.0
        };
        elem.surprise = jsd_element(p, q);
    }

    elements
}

/// Signed fraction: `delta / reference`, positive when same direction, negative when opposing.
fn signed_fraction(delta: f64, reference: f64) -> f64 {
    if reference.abs() > f64::EPSILON {
        (delta * reference.signum()) / reference.abs()
    } else {
        0.0
    }
}

/// Adaptive EP threshold scaled by cardinality.
/// Base = 0.05; scales as 0.05 / sqrt(n) so high-cardinality dimensions
/// don't filter out all elements in uniform degradation scenarios.
fn adaptive_ep_threshold(num_elements: usize) -> f64 {
    const BASE_EP: f64 = 0.05;
    BASE_EP / (num_elements as f64).sqrt()
}

/// Detect uniform degradation: no element passes the base EP threshold (0.05),
/// but collectively they explain > 50% of the parent delta.
/// The `threshold` parameter (typically `adaptive_ep_threshold(n)`) is used as
/// the collective coverage floor — any dimension whose per-element EPs are all
/// below the base and whose total concentration exceeds this floor is flagged.
#[allow(dead_code)]
fn detect_uniform_degradation(
    dim: &str,
    elements: &[ElementScore],
    parent_delta: f64,
    _threshold: f64,
) -> Option<SplitKind> {
    const BASE_EP: f64 = 0.05;
    if parent_delta.abs() < f64::EPSILON || elements.is_empty() {
        return None;
    }
    // No single element should be individually dominant (above the base EP threshold).
    let any_significant = elements.iter().any(|e| e.ep.abs() >= BASE_EP);
    if any_significant {
        return None;
    }
    // But collectively they should explain the majority of the parent delta.
    let total_unsigned_concentration: f64 = elements
        .iter()
        .map(|e| signed_fraction(e.delta, parent_delta).abs())
        .sum();
    if total_unsigned_concentration > 0.50 {
        Some(SplitKind::UniformDegradation {
            dimension: dim.to_string(),
            num_elements: elements.len(),
        })
    } else {
        None
    }
}

/// Strategy 1: rank dimension by its top element's signed concentration.
#[allow(dead_code)]
fn strategy_max_concentration(elements: &[ElementScore], parent_delta: f64) -> (f64, String) {
    elements
        .iter()
        .map(|e| {
            let conc = signed_fraction(e.delta, parent_delta);
            (conc, e.value.clone())
        })
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or((0.0, String::new()))
}

/// Strategy 2: rank dimension by sum of top-k elements' |concentration|.
#[allow(dead_code)]
fn strategy_topk_concentration_sum(elements: &[ElementScore], parent_delta: f64, k: usize) -> f64 {
    let mut concentrations: Vec<f64> = elements
        .iter()
        .map(|e| signed_fraction(e.delta, parent_delta).abs())
        .collect();
    concentrations.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    concentrations.iter().take(k).sum()
}

/// Strategy 3: Total JSD surprise with Laplace smoothing (for dimension-level ranking in tests).
#[allow(dead_code)]
fn strategy_jsd_smoothed(elements: &[ElementScore], threshold: f64) -> f64 {
    let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
    let total_curr: f64 = elements.iter().map(|e| e.current).sum();
    let epsilon = laplace_epsilon(total_prev + total_curr);
    let num = elements.len() as f64;
    let prev_denom = total_prev + epsilon * num;
    let curr_denom = total_curr + epsilon * num;
    elements
        .iter()
        .filter(|e| e.ep.abs() >= threshold)
        .map(|e| {
            let p = (e.previous + epsilon) / prev_denom;
            let q = (e.current + epsilon) / curr_denom;
            jsd_element_smoothed(p, q, 0.0)
        })
        .sum()
}

/// Strategy 4: Information Value (IV) for dimension-level ranking (used in tests).
#[allow(dead_code)]
fn strategy_iv(elements: &[ElementScore]) -> f64 {
    let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
    let total_curr: f64 = elements.iter().map(|e| e.current).sum();
    let epsilon = laplace_epsilon(total_prev + total_curr);
    let num = elements.len() as f64;
    let shares: Vec<(f64, f64)> = elements
        .iter()
        .map(|e| {
            let p = (e.previous + epsilon) / (total_prev + epsilon * num);
            let q = (e.current + epsilon) / (total_curr + epsilon * num);
            (p, q)
        })
        .collect();
    compute_iv(&shares, 0.0)
}

// ── Beam search core ──────────────────────────────────────────────────────

/// A beam entry: a partial explanation path being explored.
#[derive(Clone)]
struct BeamEntry {
    nodes: Vec<ExplainNode>,
    measure: String,
    filters: Vec<QueryFilter>,
    remaining_dims: Vec<String>,
    root_fraction: f64,
    strategy: String,
}

/// Deduplication key for beam entries.
fn dedup_key(measure: &str, filters: &[QueryFilter]) -> String {
    let mut parts: Vec<String> = filters
        .iter()
        .filter_map(|f| {
            let m = f.member.as_deref()?;
            let v = f.values.first()?;
            Some(format!("{}={}", m, v))
        })
        .collect();
    parts.sort();
    format!("{}|{}", measure, parts.join("&"))
}

/// Create a BeamEntry from a strategy's pick.
fn make_beam_entry(
    measure: &str,
    dim: &str,
    elem: &ElementScore,
    parent_delta: f64,
    filters: &[QueryFilter],
    remaining_dims: &[String],
    strategy: &str,
    dim_count: usize,
    existing_nodes: &[ExplainNode],
    existing_root_fraction: f64,
) -> BeamEntry {
    let concentration = signed_fraction(elem.delta, parent_delta);
    // existing_root_fraction is always > 0 by construction (1.0 for seeds,
    // prior root_fraction for extensions), so this is simply a product.
    let new_root_fraction = existing_root_fraction * concentration.abs();

    let mut new_filters = filters.to_vec();
    new_filters.push(QueryFilter {
        member: Some(dim.to_string()),
        operator: Some(crate::engine::query::FilterOperator::Equals),
        values: vec![elem.value.clone()],
        and: None,
        or: None,
    });

    let mut new_nodes = existing_nodes.to_vec();
    new_nodes.push(ExplainNode {
        split: SplitKind::Dimension {
            dimension: dim.to_string(),
            value: elem.value.clone(),
        },
        measure: measure.to_string(),
        filters: new_filters.clone(),
        delta: elem.delta,
        concentration,
        root_fraction: new_root_fraction,
        siblings: vec![],
        dimension_count: Some(dim_count),
        children: vec![],
    });

    let new_remaining: Vec<String> = remaining_dims
        .iter()
        .filter(|d| d.as_str() != dim)
        .cloned()
        .collect();

    BeamEntry {
        nodes: new_nodes,
        measure: measure.to_string(),
        filters: new_filters,
        remaining_dims: new_remaining,
        root_fraction: new_root_fraction,
        strategy: strategy.to_string(),
    }
}

/// Evaluate all scoring strategies for one (measure, delta, filters, dims) and produce beam entries.
fn evaluate_beam_candidates(
    measure: &str,
    parent_delta: f64,
    filters: &[QueryFilter],
    available_dims: &[String],
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    executor: &QueryExecutor,
    existing_nodes: &[ExplainNode],
    existing_root_fraction: f64,
) -> Result<Vec<BeamEntry>, EngineError> {
    let mut all_candidates: Vec<BeamEntry> = Vec::new();

    for dim in available_dims {
        // make_period_query spans prev_start → curr_end in one query; extract_delta
        // splits into previous/current periods internally via the time column.
        let q = make_period_query(
            measure, time_dimension, previous_period.0, current_period.1,
            &[dim.clone()], filters,
        );
        let rows = match executor(&q) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let elements = compute_element_scores(measure, dim, &rows, parent_delta);
        if elements.is_empty() {
            continue;
        }

        let ep_threshold = adaptive_ep_threshold(elements.len());
        let remaining: Vec<String> = available_dims.iter().filter(|d| d.as_str() != dim.as_str()).cloned().collect();

        // Check for uniform degradation
        if let Some(uniform_split) = detect_uniform_degradation(dim, &elements, parent_delta, ep_threshold) {
            all_candidates.push(BeamEntry {
                nodes: {
                    let mut n = existing_nodes.to_vec();
                    n.push(ExplainNode {
                        split: uniform_split,
                        measure: measure.to_string(),
                        filters: filters.to_vec(),
                        delta: parent_delta,
                        concentration: 1.0,
                        root_fraction: existing_root_fraction,
                        siblings: vec![],
                        dimension_count: Some(elements.len()),
                        children: vec![],
                    });
                    n
                },
                measure: measure.to_string(),
                filters: filters.to_vec(),
                remaining_dims: vec![],
                root_fraction: existing_root_fraction,
                strategy: "uniform_degradation".to_string(),
            });
            continue;
        }

        // Strategy 1: max concentration
        let (max_conc, max_val) = strategy_max_concentration(&elements, parent_delta);
        if max_conc > 0.0 {
            if let Some(elem) = elements.iter().find(|e| e.value == max_val) {
                all_candidates.push(make_beam_entry(
                    measure, dim, elem, parent_delta, filters,
                    &remaining, "max_concentration", elements.len(),
                    existing_nodes, existing_root_fraction,
                ));
            }
        }

        // Strategy 2: highest |concentration| — picks the element with the largest
        // absolute share of the parent delta, regardless of direction.
        // Diverges from Strategy 1 when opposing elements exist (e.g. some segments
        // rising while others fall): Strategy 1 picks max signed, Strategy 2 picks max |delta|.
        if let Some(top_elem) = elements.iter().max_by(|a, b| {
            signed_fraction(a.delta, parent_delta).abs()
                .partial_cmp(&signed_fraction(b.delta, parent_delta).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            all_candidates.push(make_beam_entry(
                measure, dim, top_elem, parent_delta, filters,
                &remaining, "topk_concentration", elements.len(),
                existing_nodes, existing_root_fraction,
            ));
        }

        // Shared Laplace smoothing params for strategies 3 and 4.
        let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
        let total_curr: f64 = elements.iter().map(|e| e.current).sum();
        let epsilon = laplace_epsilon(total_prev + total_curr);
        let num = elements.len() as f64;
        let prev_denom = total_prev + epsilon * num;
        let curr_denom = total_curr + epsilon * num;

        // Strategy 3: JSD smoothed — pick element with the highest Laplace-smoothed JSD surprise.
        if let Some((best_idx, best_jsd)) = elements
            .iter()
            .enumerate()
            .filter(|(_, e)| e.ep.abs() >= ep_threshold)
            .map(|(i, e)| {
                let p = (e.previous + epsilon) / prev_denom;
                let q = (e.current + epsilon) / curr_denom;
                (i, jsd_element_smoothed(p, q, 0.0))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        {
            if best_jsd > 0.0 {
                all_candidates.push(make_beam_entry(
                    measure, dim, &elements[best_idx], parent_delta, filters,
                    &remaining, "jsd_smoothed", elements.len(),
                    existing_nodes, existing_root_fraction,
                ));
            }
        }

        // Strategy 4: IV/WOE — pick the element with the highest |WOE| value.
        if let Some(best_idx) = best_woe_index(&elements, prev_denom, curr_denom, epsilon) {
            all_candidates.push(make_beam_entry(
                measure, dim, &elements[best_idx], parent_delta, filters,
                &remaining, "iv_woe", elements.len(),
                existing_nodes, existing_root_fraction,
            ));
        }
    }

    all_candidates.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
    Ok(all_candidates)
}

/// Run beam search on a single measure to find the best explanation paths.
#[allow(dead_code)]
fn beam_search_measure(
    measure: &str,
    measure_delta: f64,
    available_dims: &[String],
    initial_filters: &[QueryFilter],
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    config: &ExplainConfig,
    executor: &QueryExecutor,
) -> Result<Vec<ExplainPath>, EngineError> {
    if measure_delta.abs() < f64::EPSILON || available_dims.is_empty() {
        return Ok(vec![]);
    }

    // Seed beam: evaluate all dims with all strategies
    let seed_candidates = evaluate_beam_candidates(
        measure, measure_delta, initial_filters, available_dims,
        time_dimension, previous_period, current_period, executor,
        &[], 1.0,
    )?;

    let mut beam: Vec<BeamEntry> = seed_candidates
        .into_iter()
        .take(config.beam_width)
        .collect();

    let mut completed: Vec<ExplainPath> = Vec::new();

    for _depth in 0..config.max_depth {
        if beam.is_empty() {
            break;
        }

        let mut next_beam: Vec<BeamEntry> = Vec::new();

        for entry in &beam {
            if entry.remaining_dims.is_empty()
                || entry.root_fraction < config.min_root_fraction
            {
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            // Get the delta for this entry's current state (filtered measure)
            let q = make_period_query(
                &entry.measure, time_dimension,
                previous_period.0, current_period.1,
                &[], &entry.filters,
            );
            let entry_delta = match executor(&q) {
                Ok(rows) => extract_delta(&entry.measure, &rows).delta,
                Err(_) => {
                    completed.push(ExplainPath {
                        nodes: entry.nodes.clone(),
                        root_fraction: entry.root_fraction,
                        strategy: entry.strategy.clone(),
                        significance: None,
                    });
                    continue;
                }
            };

            if entry_delta.abs() < f64::EPSILON {
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            let candidates = evaluate_beam_candidates(
                &entry.measure, entry_delta, &entry.filters,
                &entry.remaining_dims, time_dimension,
                previous_period, current_period, executor,
                &entry.nodes, entry.root_fraction,
            )?;

            // Always emit the current path as a completed alternative,
            // regardless of whether we can extend further. This ensures
            // that high-concentration single-dimension paths are captured
            // even when additional dimensions are available.
            completed.push(ExplainPath {
                nodes: entry.nodes.clone(),
                root_fraction: entry.root_fraction,
                strategy: entry.strategy.clone(),
                significance: None,
            });

            if !candidates.is_empty() {
                next_beam.extend(candidates);
            }
        }

        // Dedup by (measure, filter_set), keep highest root_fraction
        next_beam.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen: HashSet<String> = HashSet::new();
        next_beam.retain(|e| {
            let key = dedup_key(&e.measure, &e.filters);
            seen.insert(key)
        });
        next_beam.truncate(config.beam_width);

        beam = next_beam;
    }

    // Remaining beam entries become completed paths
    for entry in beam {
        completed.push(ExplainPath {
            nodes: entry.nodes.clone(),
            root_fraction: entry.root_fraction,
            strategy: entry.strategy.clone(),
            significance: None,
        });
    }

    // Sort by root_fraction descending, dedup, truncate
    completed.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
    // Dedup completed paths by (measure, filter_set) so that different measures
    // with identical filter combinations are not incorrectly conflated.
    let mut seen_completed: HashSet<String> = HashSet::new();
    completed.retain(|p| {
        let key = if let Some(last) = p.nodes.last() {
            dedup_key(&last.measure, &last.filters)
        } else {
            String::new()
        };
        seen_completed.insert(key)
    });
    completed.truncate(config.max_alternatives);
    Ok(completed)
}

/// Result of candidate evaluation at one recursion level.
struct EvalResult {
    /// ALL candidates of the winning type, sorted by concentration desc.
    /// Includes insignificant/negative entries for display context.
    candidates: Vec<Candidate>,
    /// For dimension splits: total unique values for the chosen dimension.
    dimension_count: Option<usize>,
}

/// Evaluate candidates and select the best split type (component vs dimension).
///
/// Returns ALL candidates of the winning type (for context display),
/// sorted by concentration descending.
fn evaluate_candidates(
    ctx: &ExplainCtx,
    measure: &str,
    parent_delta: f64,
    filters: &[QueryFilter],
    available_dims: &[String],
) -> Result<EvalResult, EngineError> {
    let parent_sign = parent_delta.signum();

    // Dimensions already constrained by active filters
    let filtered_members: HashSet<&str> =
        filters.iter().filter_map(|f| f.member.as_deref()).collect();

    // 1) Component candidates — query all children first, then normalize.
    //
    // total_attributed = Σ (child_delta × edge_sign) across ALL components.
    // This strips out scaling factors (e.g., ×12 in `arr = net_mrr * 12`).
    // parent_share = (delta × sign) / total_attributed → always sums to 1.0.
    struct ComponentQuery {
        child: String,
        delta: f64,
        sign: f64,
        child_dims: Vec<String>,
    }
    let mut component_queries: Vec<ComponentQuery> = Vec::new();
    if let Some(edges) = ctx.children_of.get(measure) {
        for edge in edges {
            // Only component edges represent mathematical identity (parent = f(children)).
            // Driver edges are correlative/causal and don't compose into the parent's delta.
            if edge.kind != EdgeKind::Component {
                continue;
            }
            let child = &edge.from;
            let q = make_period_query(
                child,
                ctx.time_dimension,
                ctx.previous_period.0,
                ctx.current_period.1,
                &[],
                filters,
            );
            match (ctx.executor)(&q) {
                Ok(rows) => {
                    let md = extract_delta(child, &rows);
                    let child_view = child.split('.').next().unwrap_or("");
                    let child_dims: Vec<String> = ctx
                        .dim_cache
                        .get(child_view)
                        .map(|dims| {
                            dims.iter()
                                .filter(|d| !filtered_members.contains(d.as_str()))
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default();
                    component_queries.push(ComponentQuery {
                        child: child.clone(),
                        delta: md.delta,
                        sign: edge.sign,
                        child_dims,
                    });
                }
                Err(_) => continue,
            }
        }
    }
    let total_attributed: f64 = component_queries.iter().map(|cq| cq.delta * cq.sign).sum();
    let mut component_cands: Vec<Candidate> = Vec::new();
    for cq in component_queries {
        // Concentration uses parent_delta (for ranking against dimension candidates)
        let concentration = if parent_delta.abs() > f64::EPSILON {
            (cq.delta * cq.sign * parent_sign) / parent_delta.abs()
        } else {
            0.0
        };
        // parent_share uses total_attributed (strips scaling factors for display)
        let parent_share = if total_attributed.abs() > f64::EPSILON {
            signed_fraction(cq.delta * cq.sign, total_attributed)
        } else {
            0.0
        };
        component_cands.push(Candidate {
            split: SplitKind::Component {
                child_measure: cq.child.clone(),
            },
            next_measure: cq.child,
            next_filters: filters.to_vec(),
            next_dims: cq.child_dims,
            delta: cq.delta,
            concentration,
            parent_share,
            _surprise: 0.0,
        });
    }
    component_cands.sort_by(|a, b| {
        b.concentration
            .partial_cmp(&a.concentration)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // 2) Dimension candidates — Adtributor-style surprise ranking.
    //    For each dimension, compute per-element EP and JSD surprise. Pick the
    //    dimension with the highest accumulated surprise (distributional shift).
    //    Elements with |EP| below threshold are noise and excluded from ranking.
    const MIN_ELEMENT_EP: f64 = 0.05; // 5% per-element explanatory power threshold

    let mut best_dim: Option<(f64, f64, Vec<Candidate>, usize)> = None; // (surprise, top_conc, candidates, total_count)
    let remaining_dims_for = |dim: &str| -> Vec<String> {
        available_dims
            .iter()
            .filter(|d| d.as_str() != dim)
            .cloned()
            .collect()
    };
    for dim in available_dims {
        let q = make_period_query(
            measure,
            ctx.time_dimension,
            ctx.previous_period.0,
            ctx.current_period.1,
            &[dim.clone()],
            filters,
        );
        match (ctx.executor)(&q) {
            Ok(rows) => {
                let mut elements =
                    compute_element_scores(measure, dim, &rows, parent_delta);
                let total_count = elements.len();
                if total_count == 0 {
                    continue;
                }

                // Check for Simpson's paradox before sorting/truncating
                if let Some(w) = detect_simpsons_paradox(parent_delta, dim, &elements) {
                    ctx.warnings.borrow_mut().push(w);
                }

                // Sort elements by surprise descending (most unexpected first)
                elements.sort_by(|a, b| {
                    b.surprise
                        .partial_cmp(&a.surprise)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                // Dimension surprise = sum of significant elements' surprises.
                // Only elements with |EP| >= threshold contribute (noise filter).
                let dim_surprise: f64 = elements
                    .iter()
                    .filter(|e| e.ep.abs() >= MIN_ELEMENT_EP)
                    .map(|e| e.surprise)
                    .sum();

                // Truncate to max display values
                elements.truncate(ctx.config.max_dim_values);

                let remaining = remaining_dims_for(dim);
                let mut dim_cands: Vec<Candidate> = Vec::new();
                for elem in &elements {
                    let concentration = signed_fraction(elem.delta, parent_delta);
                    let mut new_filters = filters.to_vec();
                    new_filters.push(QueryFilter {
                        member: Some(dim.clone()),
                        operator: Some(crate::engine::query::FilterOperator::Equals),
                        values: vec![elem.value.clone()],
                        and: None,
                        or: None,
                    });
                    dim_cands.push(Candidate {
                        split: SplitKind::Dimension {
                            dimension: dim.clone(),
                            value: elem.value.clone(),
                        },
                        next_measure: measure.to_string(),
                        next_filters: new_filters,
                        next_dims: remaining.clone(),
                        delta: elem.delta,
                        concentration,
                        parent_share: concentration,
                        _surprise: elem.surprise,
                    });
                }

                // Sort candidates by concentration (EP) for recursion ordering.
                // Surprise ranked dimensions; EP ranks elements within the winner.
                dim_cands.sort_by(|a, b| {
                    b.concentration
                        .partial_cmp(&a.concentration)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                // Top element's concentration (for comparison against components)
                let top_conc = dim_cands
                    .first()
                    .map(|c| c.concentration)
                    .unwrap_or(0.0);

                let is_better = match &best_dim {
                    None => true,
                    Some((existing_surprise, _, _, _)) => dim_surprise > *existing_surprise,
                };
                if is_better && dim_surprise > 0.0 {
                    best_dim = Some((dim_surprise, top_conc, dim_cands, total_count));
                }
            }
            Err(_) => continue,
        }
    }

    // 3) Pick the type with highest max concentration.
    //    Dimension candidates use surprise for inter-dimension ranking, but
    //    the top element's EP (concentration) is compared against components.
    let comp_max = component_cands
        .first()
        .map(|c| c.concentration)
        .unwrap_or(f64::NEG_INFINITY);
    let dim_max = best_dim
        .as_ref()
        .map(|(_, top_conc, _, _)| *top_conc)
        .unwrap_or(f64::NEG_INFINITY);

    if comp_max >= dim_max {
        Ok(EvalResult {
            candidates: component_cands,
            dimension_count: None,
        })
    } else {
        let (_, _, cands, total) = best_dim.unwrap_or((0.0, 0.0, Vec::new(), 0));
        Ok(EvalResult {
            candidates: cands,
            dimension_count: Some(total),
        })
    }
}

/// Recursive explain: at each level pick the best split type, emit candidates,
/// and recurse into each for more detail.
///
/// - **Top level**: emit multiple candidates (coverage accumulates).
/// - **Non-top levels**: emit the single best candidate only.
/// - **Stopping**: concentration < threshold, root fraction < floor, or max depth.
fn recurse(
    ctx: &ExplainCtx,
    measure: &str,
    parent_delta: f64,
    filters: &[QueryFilter],
    available_dims: &[String],
    depth: usize,
    is_top_level: bool,
    parent_root_fraction: f64,
    nodes: &mut Vec<ExplainNode>,
    covered: &mut f64,
) -> Result<(), EngineError> {
    if depth >= ctx.config.max_depth || *covered >= ctx.config.coverage_threshold {
        return Ok(());
    }
    if parent_delta.abs() < f64::EPSILON {
        return Ok(());
    }

    let eval = evaluate_candidates(ctx, measure, parent_delta, filters, available_dims)?;

    if eval.candidates.is_empty() {
        return Ok(());
    }

    // Check stopping: best child below min_concentration
    if eval.candidates[0].concentration < ctx.config.min_concentration {
        return Ok(());
    }

    // Separate significant candidates (recurse) from context-only (siblings).
    // For components: show ALL as siblings, recurse only significant ones.
    // For dimensions: show top N as siblings, recurse only the top one.
    let max_display_dims: usize = 5;

    // Only recurse into the top candidate; show the rest as siblings for context.
    let top = &eval.candidates[0];

    let root_fraction = parent_root_fraction * top.parent_share;
    if root_fraction < ctx.config.min_root_fraction {
        return Ok(());
    }

    // Build siblings: all other candidates at this level (for context display)
    let siblings: Vec<ExplainSibling> = eval
        .candidates
        .iter()
        .skip(1)
        .enumerate()
        .filter(|(i, _)| {
            // For dimensions, limit context to top N
            if eval.dimension_count.is_some() {
                *i < max_display_dims
            } else {
                true // components: show all
            }
        })
        .map(|(_, c)| ExplainSibling {
            split: c.split.clone(),
            measure: c.next_measure.clone(),
            delta: c.delta,
            root_fraction: parent_root_fraction * c.parent_share,
        })
        .collect();

    let mut node = ExplainNode {
        split: top.split.clone(),
        measure: top.next_measure.clone(),
        filters: top.next_filters.clone(),
        delta: top.delta,
        concentration: top.concentration,
        root_fraction,
        siblings,
        dimension_count: eval.dimension_count,
        children: Vec::new(),
    };

    recurse(
        ctx,
        &top.next_measure,
        top.delta,
        &top.next_filters,
        &top.next_dims,
        depth + 1,
        false,
        root_fraction,
        &mut node.children,
        covered,
    )?;

    // Coverage tracking at top level
    if is_top_level {
        *covered += root_fraction;
    }

    nodes.push(node);

    Ok(())
}

// ── Statistical significance ─────────────────────────────

/// Compute statistical significance of a delta relative to historical deltas.
/// Returns None if fewer than 6 historical periods.
#[allow(dead_code)]
fn compute_significance(current_delta: f64, historical_deltas: &[f64]) -> Option<SignificanceTest> {
    use statrs::distribution::{ContinuousCDF, StudentsT};

    const MIN_PERIODS: usize = 6;
    if historical_deltas.len() < MIN_PERIODS {
        return None;
    }

    let n = historical_deltas.len() as f64;
    let mean: f64 = historical_deltas.iter().sum::<f64>() / n;
    let variance: f64 = historical_deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let std = variance.sqrt();

    if std < f64::EPSILON {
        let p_value = if (current_delta - mean).abs() < f64::EPSILON { 1.0 } else { 0.0 };
        return Some(SignificanceTest {
            p_value,
            historical_periods: historical_deltas.len(),
            historical_mean_delta: mean,
            historical_std_delta: std,
        });
    }

    let t_stat = (current_delta - mean) / (std / n.sqrt());
    let df = n - 1.0;

    let t_dist = StudentsT::new(0.0, 1.0, df).ok()?;
    let p_value = 2.0 * (1.0 - t_dist.cdf(t_stat.abs()));

    Some(SignificanceTest {
        p_value,
        historical_periods: historical_deltas.len(),
        historical_mean_delta: mean,
        historical_std_delta: std,
    })
}

// ── Tests ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::metric_tree::MetricTree;
    use crate::schema::models::*;

    fn make_view(name: &str, measures: Vec<Measure>) -> View {
        View {
            name: name.to_string(),
            description: format!("{} view", name),
            label: None,
            datasource: None,
            dialect: None,
            table: Some(format!("public.{}", name)),
            sql: None,
            entities: vec![],
            dimensions: vec![],
            measures: Some(measures),
            segments: vec![],
            meta: None,
        }
    }

    fn atomic_measure(name: &str, mt: MeasureType) -> Measure {
        Measure {
            name: name.to_string(),
            measure_type: mt,
            description: None,
            expr: Some(name.to_string()),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            meta: None,
        }
    }

    fn composite_measure(name: &str, expr: &str) -> Measure {
        Measure {
            name: name.to_string(),
            measure_type: MeasureType::Number,
            description: None,
            expr: Some(expr.to_string()),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            meta: None,
        }
    }

    fn make_layer(views: Vec<View>) -> SemanticLayer {
        SemanticLayer {
            views,
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        }
    }

    /// Build a simple tree: revenue = new_mrr + expansion_mrr - churned_mrr
    fn saas_tree() -> (SemanticLayer, MetricTree) {
        let revenue_view = make_view(
            "revenue",
            vec![
                atomic_measure("new_mrr", MeasureType::Sum),
                atomic_measure("expansion_mrr", MeasureType::Sum),
                atomic_measure("churned_mrr", MeasureType::Sum),
                composite_measure(
                    "net_mrr",
                    "{{revenue.new_mrr}} + {{revenue.expansion_mrr}} - {{revenue.churned_mrr}}",
                ),
                composite_measure("arr", "{{revenue.net_mrr}} * 12"),
            ],
        );
        let layer = make_layer(vec![revenue_view]);
        let tree = MetricTree::build(&layer);
        (layer, tree)
    }

    /// SaaS tree with quantitative drivers on arr.
    fn saas_tree_with_drivers() -> (SemanticLayer, MetricTree) {
        let mut revenue_view = make_view(
            "revenue",
            vec![
                atomic_measure("new_mrr", MeasureType::Sum),
                atomic_measure("expansion_mrr", MeasureType::Sum),
                atomic_measure("churned_mrr", MeasureType::Sum),
                composite_measure(
                    "net_mrr",
                    "{{revenue.new_mrr}} + {{revenue.expansion_mrr}} - {{revenue.churned_mrr}}",
                ),
                composite_measure("arr", "{{revenue.net_mrr}} * 12"),
                atomic_measure("churn_rate", MeasureType::Average),
            ],
        );
        // Add quantitative driver: churn_rate -> arr
        if let Some(ref mut measures) = revenue_view.measures {
            let arr = measures.iter_mut().find(|m| m.name == "arr").unwrap();
            arr.drivers = Some(vec![Driver {
                measure: "revenue.churn_rate".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(-120_000.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: Some(30),
                description: Some("Each 1% increase in churn reduces ARR by $120K".to_string()),
                refs: None,
            }]);
        }
        let layer = make_layer(vec![revenue_view]);
        let tree = MetricTree::build(&layer);
        (layer, tree)
    }

    // ── Sensitivity tests ─────────────────────────

    #[test]
    fn test_sensitivity_component_tree() {
        let (_, tree) = saas_tree();
        let result = sensitivity(&tree, "revenue.arr").unwrap();
        assert_eq!(result.target, "revenue.arr");
        // Should find: net_mrr (direct), new_mrr, expansion_mrr, churned_mrr (transitive)
        assert_eq!(result.drivers.len(), 4);
        // All should have effective_coefficient = Some(1.0) since all are component edges
        for d in &result.drivers {
            assert!(d.effective_coefficient.is_some());
            assert_eq!(d.effective_coefficient.unwrap(), 1.0);
        }
    }

    #[test]
    fn test_sensitivity_with_quantitative_driver() {
        let (_, tree) = saas_tree_with_drivers();
        let result = sensitivity(&tree, "revenue.arr").unwrap();
        // Should find churn_rate as a driver with coefficient -120000
        let churn = result
            .drivers
            .iter()
            .find(|d| d.measure == "revenue.churn_rate")
            .expect("churn_rate should be a driver of arr");
        assert_eq!(churn.effective_coefficient, Some(-120_000.0));
        assert_eq!(churn.lag, Some(30));
    }

    #[test]
    fn test_sensitivity_not_found() {
        let (_, tree) = saas_tree();
        let result = sensitivity(&tree, "nonexistent.metric");
        assert!(result.is_err());
    }

    #[test]
    fn test_sensitivity_leaf_node() {
        let (_, tree) = saas_tree();
        let result = sensitivity(&tree, "revenue.new_mrr").unwrap();
        // Leaf node has no drivers
        assert!(result.drivers.is_empty());
    }

    // ── Predict tests ─────────────────────────────

    #[test]
    fn test_predict_single_hop() {
        let (_, tree) = saas_tree();
        // If net_mrr increases by 100, arr should increase by 100 (component pass-through)
        let result = predict(&tree, &[("revenue.net_mrr".to_string(), 100.0)]).unwrap();
        let arr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");
        assert_eq!(arr_impact.estimated_delta, 100.0);
        assert_eq!(arr_impact.confidence, "exact");
    }

    #[test]
    fn test_predict_multi_hop() {
        let (_, tree) = saas_tree();
        // If new_mrr increases by 50, it flows through net_mrr to arr
        let result = predict(&tree, &[("revenue.new_mrr".to_string(), 50.0)]).unwrap();
        let arr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");
        assert_eq!(arr_impact.estimated_delta, 50.0);
        assert_eq!(arr_impact.confidence, "exact");

        let net_mrr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr should be impacted");
        assert_eq!(net_mrr_impact.estimated_delta, 50.0);
    }

    #[test]
    fn test_predict_with_driver_coefficient() {
        let (_, tree) = saas_tree_with_drivers();
        // churn_rate increases by 0.01 (1%), should impact arr by -120000 * 0.01 = -1200
        let result = predict(&tree, &[("revenue.churn_rate".to_string(), 0.01)]).unwrap();
        let arr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted by churn_rate");
        assert!((arr_impact.estimated_delta - (-1200.0)).abs() < 0.01);
        assert_eq!(arr_impact.confidence, "estimated");
    }

    #[test]
    fn test_predict_multiple_inputs() {
        let (_, tree) = saas_tree();
        // new_mrr +100 and expansion_mrr +50 both flow into net_mrr and arr
        let result = predict(
            &tree,
            &[
                ("revenue.new_mrr".to_string(), 100.0),
                ("revenue.expansion_mrr".to_string(), 50.0),
            ],
        )
        .unwrap();
        let net_mrr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr should be impacted");
        assert_eq!(net_mrr.estimated_delta, 150.0); // 100 + 50
    }

    #[test]
    fn test_predict_not_found() {
        let (_, tree) = saas_tree();
        let result = predict(&tree, &[("nonexistent.metric".to_string(), 100.0)]);
        assert!(result.is_err());
    }

    // ── Additional sensitivity tests ─────────────

    #[test]
    fn test_sensitivity_coefficient_chain() {
        // Multi-hop: A --driver:2--> B --driver:3--> C (all driver edges)
        // Effective coefficient from A to C should be 2 * 3 = 6.0
        let a_measure = atomic_measure("a", MeasureType::Sum);
        let mut b_measure = atomic_measure("b", MeasureType::Sum);
        let mut c_measure = atomic_measure("c", MeasureType::Sum);

        b_measure.drivers = Some(vec![Driver {
            measure: "chain.a".to_string(),
            direction: DriverDirection::default(),
            strength: DriverStrength::default(),
            confidence: DriverConfidence::default(),
            coefficient: Some(2.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);
        c_measure.drivers = Some(vec![Driver {
            measure: "chain.b".to_string(),
            direction: DriverDirection::default(),
            strength: DriverStrength::default(),
            confidence: DriverConfidence::default(),
            coefficient: Some(3.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("chain", vec![a_measure, b_measure, c_measure]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "chain.c").unwrap();
        // Should find B (direct, coeff=3) and A (transitive, coeff=6)
        let a_driver = result
            .drivers
            .iter()
            .find(|d| d.measure == "chain.a")
            .expect("A should be a transitive driver of C");
        assert_eq!(a_driver.effective_coefficient, Some(6.0));
        assert_eq!(a_driver.path.len(), 3); // [a, b, c]
    }

    #[test]
    fn test_sensitivity_mixed_quant_qualitative() {
        // A --qualitative--> B --coeff:5--> C
        // Effective coefficient from A to C should be None (qualitative breaks the chain)
        let a_measure = atomic_measure("a", MeasureType::Sum);
        let mut b_measure = atomic_measure("b", MeasureType::Sum);
        let mut c_measure = atomic_measure("c", MeasureType::Sum);

        b_measure.drivers = Some(vec![Driver {
            measure: "mix.a".to_string(),
            direction: DriverDirection::Positive,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: None, // qualitative only
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);
        c_measure.drivers = Some(vec![Driver {
            measure: "mix.b".to_string(),
            direction: DriverDirection::default(),
            strength: DriverStrength::default(),
            confidence: DriverConfidence::default(),
            coefficient: Some(5.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("mix", vec![a_measure, b_measure, c_measure]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "mix.c").unwrap();
        let a_driver = result
            .drivers
            .iter()
            .find(|d| d.measure == "mix.a")
            .expect("A should be a transitive driver of C");
        // Qualitative edge breaks the coefficient chain
        assert_eq!(a_driver.effective_coefficient, None);
    }

    #[test]
    fn test_sensitivity_direction_inference() {
        // Negative coefficient should infer Negative direction
        // Positive coefficient should infer Positive direction
        let mut target = atomic_measure("target", MeasureType::Sum);
        let pos_driver = atomic_measure("pos_driver", MeasureType::Sum);
        let neg_driver = atomic_measure("neg_driver", MeasureType::Sum);

        target.drivers = Some(vec![
            Driver {
                measure: "dir.pos_driver".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(10.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
            Driver {
                measure: "dir.neg_driver".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(-5.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
        ]);

        let view = make_view("dir", vec![target, pos_driver, neg_driver]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "dir.target").unwrap();
        let pos = result
            .drivers
            .iter()
            .find(|d| d.measure == "dir.pos_driver")
            .unwrap();
        assert_eq!(pos.direction, DriverDirection::Positive);

        let neg = result
            .drivers
            .iter()
            .find(|d| d.measure == "dir.neg_driver")
            .unwrap();
        assert_eq!(neg.direction, DriverDirection::Negative);
    }

    #[test]
    fn test_sensitivity_diamond_graph() {
        // Diamond: D -> B -> A and D -> C -> A
        // The BFS adds drivers before checking visited, so D can appear
        // via both paths. However, visited prevents further propagation
        // from D being explored more than once.
        let d_measure = atomic_measure("d", MeasureType::Sum);
        let mut b_measure = atomic_measure("b", MeasureType::Sum);
        let mut c_measure = atomic_measure("c", MeasureType::Sum);
        let a_measure = composite_measure("a", "{{diamond.b}} + {{diamond.c}}");

        b_measure.drivers = Some(vec![Driver {
            measure: "diamond.d".to_string(),
            direction: DriverDirection::Positive,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: Some(2.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);
        c_measure.drivers = Some(vec![Driver {
            measure: "diamond.d".to_string(),
            direction: DriverDirection::Positive,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: Some(3.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view(
            "diamond",
            vec![d_measure, b_measure, c_measure, a_measure],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "diamond.a").unwrap();
        // D appears via two paths (B->A and C->A). The BFS adds a driver entry
        // before the visited check, so D appears twice — one per path. The visited
        // set prevents D from being explored further more than once.
        let d_entries: Vec<_> = result
            .drivers
            .iter()
            .filter(|d| d.measure == "diamond.d")
            .collect();
        assert_eq!(d_entries.len(), 2, "D appears via both paths in the diamond");
        // The two entries should have different effective coefficients (2.0 and 3.0)
        let coeffs: Vec<f64> = d_entries
            .iter()
            .map(|d| d.effective_coefficient.unwrap())
            .collect();
        assert!(coeffs.contains(&2.0));
        assert!(coeffs.contains(&3.0));
    }

    #[test]
    fn test_sensitivity_zero_coefficient() {
        // Edge with coefficient=0.0 should still appear with effective_coefficient=Some(0.0)
        let mut target = atomic_measure("target", MeasureType::Sum);
        let zero_driver = atomic_measure("zero_driver", MeasureType::Sum);

        target.drivers = Some(vec![Driver {
            measure: "zero.zero_driver".to_string(),
            direction: DriverDirection::default(),
            strength: DriverStrength::default(),
            confidence: DriverConfidence::default(),
            coefficient: Some(0.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("zero", vec![target, zero_driver]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "zero.target").unwrap();
        let driver = result
            .drivers
            .iter()
            .find(|d| d.measure == "zero.zero_driver")
            .expect("zero-coeff driver should appear");
        assert_eq!(driver.effective_coefficient, Some(0.0));
        assert_eq!(driver.strength, DriverStrength::Weak);
    }

    #[test]
    fn test_sensitivity_ordering() {
        // Multiple drivers with different |coefficient|. Verify descending sort.
        let mut target = atomic_measure("target", MeasureType::Sum);
        let small = atomic_measure("small", MeasureType::Sum);
        let medium = atomic_measure("medium", MeasureType::Sum);
        let large = atomic_measure("large", MeasureType::Sum);

        target.drivers = Some(vec![
            Driver {
                measure: "order.small".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(1.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
            Driver {
                measure: "order.medium".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(50.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
            Driver {
                measure: "order.large".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(-100.0),
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
        ]);

        let view = make_view("order", vec![target, small, medium, large]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "order.target").unwrap();
        assert_eq!(result.drivers.len(), 3);
        // Should be sorted by |coefficient| descending: large (100), medium (50), small (1)
        assert_eq!(result.drivers[0].measure, "order.large");
        assert_eq!(result.drivers[1].measure, "order.medium");
        assert_eq!(result.drivers[2].measure, "order.small");
    }

    #[test]
    fn test_sensitivity_qualitative_after_quantitative() {
        // Qualitative-only drivers sort after all quantitative drivers
        let mut target = atomic_measure("target", MeasureType::Sum);
        let quant_driver = atomic_measure("quant", MeasureType::Sum);
        let qual_driver = atomic_measure("qual", MeasureType::Sum);

        target.drivers = Some(vec![
            Driver {
                measure: "sorttest.qual".to_string(),
                direction: DriverDirection::Positive,
                strength: DriverStrength::Strong,
                confidence: DriverConfidence::High,
                coefficient: None, // qualitative
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
            Driver {
                measure: "sorttest.quant".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(0.01), // very small but quantitative
                form: DriverForm::Linear,
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            },
        ]);

        let view = make_view("sorttest", vec![target, quant_driver, qual_driver]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "sorttest.target").unwrap();
        assert_eq!(result.drivers.len(), 2);
        // Quantitative first, even with tiny coefficient
        assert_eq!(result.drivers[0].measure, "sorttest.quant");
        assert_eq!(result.drivers[1].measure, "sorttest.qual");
        assert!(result.drivers[1].effective_coefficient.is_none());
    }

    #[test]
    fn test_sensitivity_self_referential() {
        // A measure that references itself in expr should not cause infinite loop
        // (In practice this creates a component edge from self to self)
        let self_measure = composite_measure("self_ref", "{{selfview.self_ref}} + 1");
        let view = make_view("selfview", vec![self_measure]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Should complete without infinite loop
        let result = sensitivity(&tree, "selfview.self_ref").unwrap();
        // The self-edge appears as a driver (self -> self), but visited set prevents infinite loop
        // The exact count depends on whether the tree builder creates the self-edge
        assert!(result.drivers.len() <= 1);
    }

    // ── Additional predict tests ─────────────────────────────

    #[test]
    fn test_predict_zero_delta() {
        let (_, tree) = saas_tree();
        // Zero delta should produce no impacts (filtered out because delta.abs() < EPSILON)
        let result = predict(&tree, &[("revenue.new_mrr".to_string(), 0.0)]).unwrap();
        assert!(
            result.impacts.is_empty(),
            "zero delta should produce no impacts"
        );
    }

    #[test]
    fn test_predict_negative_delta() {
        let (_, tree) = saas_tree();
        // Negative delta flows correctly through component edges
        let result = predict(&tree, &[("revenue.new_mrr".to_string(), -200.0)]).unwrap();
        let net_mrr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr should be impacted");
        assert_eq!(net_mrr.estimated_delta, -200.0);

        let arr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");
        assert_eq!(arr.estimated_delta, -200.0);
        assert_eq!(arr.confidence, "exact");
    }

    #[test]
    fn test_predict_qualitative_driver_skipped() {
        // Driver with no coefficient should produce no impact (delta=0 -> skipped)
        let mut target = atomic_measure("target", MeasureType::Sum);
        let qual_driver = atomic_measure("qual_driver", MeasureType::Sum);

        target.drivers = Some(vec![Driver {
            measure: "qualskip.qual_driver".to_string(),
            direction: DriverDirection::Positive,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: None, // qualitative only
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("qualskip", vec![qual_driver, target]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Propagate from qual_driver -> should not reach target (no coefficient)
        let result =
            predict(&tree, &[("qualskip.qual_driver".to_string(), 100.0)]).unwrap();
        let target_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "qualskip.target");
        assert!(
            target_impact.is_none(),
            "qualitative driver should produce no impact"
        );
    }

    #[test]
    fn test_predict_diamond_accumulation() {
        // Diamond: A -> C and B -> C both component.
        // Predict A=100, B=50. C should accumulate both.
        let a = atomic_measure("a", MeasureType::Sum);
        let b = atomic_measure("b", MeasureType::Sum);
        let c = composite_measure("c", "{{diacc.a}} + {{diacc.b}}");

        let view = make_view("diacc", vec![a, b, c]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = predict(
            &tree,
            &[
                ("diacc.a".to_string(), 100.0),
                ("diacc.b".to_string(), 50.0),
            ],
        )
        .unwrap();

        let c_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "diacc.c")
            .expect("C should be impacted");
        assert!(
            (c_impact.estimated_delta - 150.0).abs() < 0.01,
            "C should accumulate both: got {}",
            c_impact.estimated_delta
        );
        assert_eq!(c_impact.confidence, "exact");
    }

    #[test]
    fn test_predict_mixed_confidence() {
        // Two separate inputs to the same target through different edge types.
        // Input 1 (a) reaches target via component edge (exact).
        // Input 2 (b) reaches target via driver edge (estimated).
        // Combined confidence should be "estimated" since not all paths are exact.
        let a = atomic_measure("a", MeasureType::Sum);
        let b = atomic_measure("b", MeasureType::Sum);
        let mut target = composite_measure("target", "{{mixconf.a}} + 0");

        // target also has a driver from b
        target.drivers = Some(vec![Driver {
            measure: "mixconf.b".to_string(),
            direction: DriverDirection::Positive,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: Some(2.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("mixconf", vec![a, b, target]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Two inputs: a (component path) and b (driver path) both hitting target
        let result = predict(
            &tree,
            &[
                ("mixconf.a".to_string(), 10.0),
                ("mixconf.b".to_string(), 5.0),
            ],
        )
        .unwrap();
        let target_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "mixconf.target")
            .expect("target should be impacted");
        // Path 1: a -> target (component, delta=10, exact)
        // Path 2: b -> target (driver, delta=2.0*5.0=10, estimated)
        // Total: 20, confidence: "estimated" because not all paths are exact
        assert!((target_impact.estimated_delta - 20.0).abs() < 0.01);
        assert_eq!(target_impact.confidence, "estimated");
    }

    #[test]
    fn test_predict_negative_coefficient() {
        // Driver with coefficient=-1000. Verify sign of impact.
        let mut target = atomic_measure("target", MeasureType::Sum);
        let driver = atomic_measure("driver", MeasureType::Sum);

        target.drivers = Some(vec![Driver {
            measure: "negcoeff.driver".to_string(),
            direction: DriverDirection::Negative,
            strength: DriverStrength::Strong,
            confidence: DriverConfidence::High,
            coefficient: Some(-1000.0),
            form: DriverForm::Linear,
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("negcoeff", vec![driver, target]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result =
            predict(&tree, &[("negcoeff.driver".to_string(), 5.0)]).unwrap();
        let impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "negcoeff.target")
            .expect("target should be impacted");
        assert!(
            (impact.estimated_delta - (-5000.0)).abs() < 0.01,
            "impact should be -5000, got {}",
            impact.estimated_delta
        );
    }

    #[test]
    fn test_predict_deep_chain() {
        // 4-level chain: A -> B -> C -> D (all component edges)
        // delta=10 at A should arrive at D=10
        let a = atomic_measure("a", MeasureType::Sum);
        let b = composite_measure("b", "{{deep.a}} + 0");
        let c = composite_measure("c", "{{deep.b}} + 0");
        let d = composite_measure("d", "{{deep.c}} + 0");

        let view = make_view("deep", vec![a, b, c, d]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = predict(&tree, &[("deep.a".to_string(), 10.0)]).unwrap();
        let d_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "deep.d")
            .expect("D should be impacted");
        assert!(
            (d_impact.estimated_delta - 10.0).abs() < 0.01,
            "delta should pass through all component edges unchanged"
        );
        assert_eq!(d_impact.confidence, "exact");
        assert_eq!(d_impact.path, vec!["deep.a", "deep.b", "deep.c", "deep.d"]);
    }

    // ── Opportunity tests ─────────────────────────────

    /// Helper to build a view with measures and dimensions for opportunity tests.
    fn make_opp_view(
        name: &str,
        measures: Vec<Measure>,
        dim_names: &[&str],
    ) -> View {
        View {
            name: name.to_string(),
            description: Some(format!("{} view", name)),
            label: None,
            datasource: None,
            dialect: None,
            table: Some(format!("public.{}", name)),
            sql: None,
            entities: vec![],
            dimensions: dim_names
                .iter()
                .map(|d| crate::schema::models::Dimension {
                    name: d.to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: d.to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    inherits_from: None,
                    primary_key: None,
                    sub_query: None,
                    meta: None,
                })
                .collect(),
            measures: Some(measures),
            segments: vec![],
            meta: None,
        }
    }

    #[test]
    fn test_opportunity_additive_basic() {
        // Sum measure with 3 segments [100, 200, 300]. Overall=600.
        // Additive benchmark = 600 / 3 = 200.
        // Segment "a" (100) is underperforming. Gap=100, weighted_gap=100*(1/3)=33.33.
        let view = make_opp_view(
            "opp",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let measure_alias = "opp__revenue";
        let dim_alias = "opp__region";

        let mut data = HashMap::new();
        // Overall query (no dimensions)
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[(measure_alias, jn(600.0))])],
        );
        // Breakdown by region
        data.insert(
            "opp.revenue:opp.region".to_string(),
            vec![
                row(&[(dim_alias, js("a")), (measure_alias, jn(100.0))]),
                row(&[(dim_alias, js("b")), (measure_alias, jn(200.0))]),
                row(&[(dim_alias, js("c")), (measure_alias, jn(300.0))]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        assert_eq!(result.target, "opp.revenue");
        assert!((result.overall_value - 600.0).abs() < 0.01);
        assert_eq!(result.dimensions.len(), 1);

        let dim_opp = &result.dimensions[0];
        assert_eq!(dim_opp.dimension, "opp.region");
        // Only segment "a" (100) is below benchmark (200)
        assert_eq!(dim_opp.segments.len(), 1);
        assert_eq!(dim_opp.segments[0].segment, "a");
        assert!((dim_opp.segments[0].current_value - 100.0).abs() < 0.01);
        assert!((dim_opp.segments[0].benchmark - 200.0).abs() < 0.01);
        assert!((dim_opp.segments[0].gap - 100.0).abs() < 0.01);
        // segment_share = 1/3, weighted_gap = 100 * (1/3) ≈ 33.33
        assert!((dim_opp.segments[0].segment_share - 1.0 / 3.0).abs() < 0.01);
        assert!((dim_opp.segments[0].weighted_gap - 100.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_opportunity_ratio_basic() {
        // Number/ratio measure with 3 segments [0.10, 0.30, 0.25].
        // Overall (weighted avg) = 0.22.
        // Benchmark for ratios = overall = 0.22.
        // Segment "a" (0.10) is underperforming. Gap = 0.12.
        // The expr references funnel.conversions and funnel.visits which don't exist
        // on this view. That's fine — MetricTree::build creates dangling component edges
        // harmlessly, and opportunity() only cares about the measure_type being Number.
        let view = make_opp_view(
            "funnel",
            vec![composite_measure("conversion_rate", "{{funnel.conversions}} / NULLIF({{funnel.visits}}, 0)")],
            &["platform"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let measure_alias = "funnel__conversion_rate";
        let dim_alias = "funnel__platform";

        let mut data = HashMap::new();
        data.insert(
            "funnel.conversion_rate".to_string(),
            vec![row(&[(measure_alias, jn(0.22))])],
        );
        data.insert(
            "funnel.conversion_rate:funnel.platform".to_string(),
            vec![
                row(&[(dim_alias, js("android")), (measure_alias, jn(0.10))]),
                row(&[(dim_alias, js("ios")), (measure_alias, jn(0.30))]),
                row(&[(dim_alias, js("web")), (measure_alias, jn(0.25))]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "funnel.conversion_rate",
            "funnel.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        assert!((result.overall_value - 0.22).abs() < 0.01);
        assert_eq!(result.dimensions.len(), 1);

        let dim_opp = &result.dimensions[0];
        // For ratio measures, benchmark = overall = 0.22
        // Segments below 0.22: android (0.10)
        assert_eq!(dim_opp.segments.len(), 1);
        assert_eq!(dim_opp.segments[0].segment, "android");
        assert!((dim_opp.segments[0].benchmark - 0.22).abs() < 0.01);
        assert!((dim_opp.segments[0].gap - 0.12).abs() < 0.01);
        // For ratios, segment_share = equal_share = 1/3 (documented heuristic:
        // each segment has equal opportunity to improve)
        assert!((dim_opp.segments[0].segment_share - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_opportunity_no_underperformers() {
        // All segments equal. No opportunities should be found.
        let view = make_opp_view(
            "equal",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "equal.revenue".to_string(),
            vec![row(&[("equal__revenue", jn(300.0))])],
        );
        data.insert(
            "equal.revenue:equal.region".to_string(),
            vec![
                row(&[("equal__region", js("a")), ("equal__revenue", jn(100.0))]),
                row(&[("equal__region", js("b")), ("equal__revenue", jn(100.0))]),
                row(&[("equal__region", js("c")), ("equal__revenue", jn(100.0))]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "equal.revenue",
            "equal.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        // Benchmark = 300/3 = 100. All segments == benchmark. No underperformers.
        assert!(
            result.dimensions.is_empty(),
            "no underperformers should yield empty dimensions"
        );
    }

    #[test]
    fn test_opportunity_single_segment() {
        // Only one segment. Benchmark = overall = segment value. No gap.
        let view = make_opp_view(
            "single",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "single.revenue".to_string(),
            vec![row(&[("single__revenue", jn(500.0))])],
        );
        data.insert(
            "single.revenue:single.region".to_string(),
            vec![row(&[
                ("single__region", js("only")),
                ("single__revenue", jn(500.0)),
            ])],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "single.revenue",
            "single.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        // Benchmark = 500/1 = 500. Single segment = 500. No gap.
        assert!(result.dimensions.is_empty());
    }

    #[test]
    fn test_opportunity_downstream_propagation() {
        // Opportunity on a leaf measure that has a parent via component edge.
        // Verify downstream impacts are populated.
        let leaf = atomic_measure("new_mrr", MeasureType::Sum);
        let parent = composite_measure("net_mrr", "{{prop.new_mrr}} + 0");

        let view = make_opp_view("prop", vec![leaf, parent], &["region"]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "prop.new_mrr".to_string(),
            vec![row(&[("prop__new_mrr", jn(300.0))])],
        );
        data.insert(
            "prop.new_mrr:prop.region".to_string(),
            vec![
                row(&[
                    ("prop__region", js("a")),
                    ("prop__new_mrr", jn(50.0)),
                ]),
                row(&[
                    ("prop__region", js("b")),
                    ("prop__new_mrr", jn(100.0)),
                ]),
                row(&[
                    ("prop__region", js("c")),
                    ("prop__new_mrr", jn(150.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "prop.new_mrr",
            "prop.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        // Benchmark = 300/3 = 100. Segment "a" (50) underperforms by 50.
        assert!(!result.dimensions.is_empty());
        // The top gap propagates to net_mrr via component edge
        assert!(
            !result.downstream.is_empty(),
            "should have downstream impacts from new_mrr to net_mrr"
        );
        let net_mrr_impact = result
            .downstream
            .iter()
            .find(|i| i.measure == "prop.net_mrr");
        assert!(
            net_mrr_impact.is_some(),
            "net_mrr should appear in downstream"
        );
    }

    #[test]
    fn test_opportunity_empty_dimensions() {
        // View with no string/number dimensions. Should return empty dimensions.
        let view = make_view(
            "nodim",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "nodim.revenue".to_string(),
            vec![row(&[("nodim__revenue", jn(1000.0))])],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "nodim.revenue",
            "nodim.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        assert!(
            result.dimensions.is_empty(),
            "no dimensions should yield empty result"
        );
    }

    #[test]
    fn test_opportunity_not_found() {
        let (layer, tree) = saas_tree();
        let data = HashMap::new();
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "nonexistent.metric",
            "revenue.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_opportunity_multiple_dimensions() {
        // View with 2 dimensions. Each should get its own DimensionOpportunity entry,
        // sorted by total_weighted_gap descending.
        let view = make_opp_view(
            "multi",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region", "channel"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "multi.revenue".to_string(),
            vec![row(&[("multi__revenue", jn(600.0))])],
        );
        // Region breakdown: segment "a" underperforms more
        data.insert(
            "multi.revenue:multi.region".to_string(),
            vec![
                row(&[
                    ("multi__region", js("a")),
                    ("multi__revenue", jn(50.0)),
                ]),
                row(&[
                    ("multi__region", js("b")),
                    ("multi__revenue", jn(250.0)),
                ]),
                row(&[
                    ("multi__region", js("c")),
                    ("multi__revenue", jn(300.0)),
                ]),
            ],
        );
        // Channel breakdown: smaller gap
        data.insert(
            "multi.revenue:multi.channel".to_string(),
            vec![
                row(&[
                    ("multi__channel", js("organic")),
                    ("multi__revenue", jn(180.0)),
                ]),
                row(&[
                    ("multi__channel", js("paid")),
                    ("multi__revenue", jn(200.0)),
                ]),
                row(&[
                    ("multi__channel", js("referral")),
                    ("multi__revenue", jn(220.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "multi.revenue",
            "multi.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        // Both dimensions should have opportunities
        assert_eq!(result.dimensions.len(), 2);
        // Region has bigger gap (a=50 vs benchmark=200, gap=150) than channel (organic=180 vs 200, gap=20)
        assert_eq!(result.dimensions[0].dimension, "multi.region");
        assert_eq!(result.dimensions[1].dimension, "multi.channel");
        assert!(
            result.dimensions[0].total_weighted_gap > result.dimensions[1].total_weighted_gap,
            "dimensions should be sorted by total_weighted_gap descending"
        );
    }

    #[test]
    fn test_opportunity_zero_overall() {
        // Overall value is 0. Benchmark = 0/3 = 0. All segments >= 0.
        let view = make_opp_view(
            "zeroval",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "zeroval.revenue".to_string(),
            vec![row(&[("zeroval__revenue", jn(0.0))])],
        );
        data.insert(
            "zeroval.revenue:zeroval.region".to_string(),
            vec![
                row(&[
                    ("zeroval__region", js("a")),
                    ("zeroval__revenue", jn(0.0)),
                ]),
                row(&[
                    ("zeroval__region", js("b")),
                    ("zeroval__revenue", jn(0.0)),
                ]),
                row(&[
                    ("zeroval__region", js("c")),
                    ("zeroval__revenue", jn(0.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "zeroval.revenue",
            "zeroval.created_at",
            ("2024-01-01", "2024-01-31"),
            &exec,
        )
        .unwrap();

        assert!((result.overall_value).abs() < 0.01);
        // Benchmark = 0, all segments = 0, no gap
        assert!(
            result.dimensions.is_empty(),
            "zero overall should yield no opportunities"
        );
    }

    // ── Explain tests ─────────────────────────────

    /// Helper to build a serde_json::Map row.
    fn row(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn jn(v: f64) -> serde_json::Value {
        serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }

    fn js(s: &str) -> serde_json::Value {
        serde_json::Value::String(s.to_string())
    }

    /// Build a mock executor that returns predefined rows per measure.
    fn mock_executor(
        data: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ) -> Box<QueryExecutor> {
        Box::new(move |q: &QueryRequest| {
            let measure = &q.measures[0];
            // If there are extra dimensions, look up "measure:dim" first
            if !q.dimensions.is_empty() {
                let dim = &q.dimensions[0];
                let key = format!("{}:{}", measure, dim);
                if let Some(rows) = data.get(&key) {
                    return Ok(rows.clone());
                }
            }
            // Fall back to measure-only lookup
            Ok(data.get(measure.as_str()).cloned().unwrap_or_default())
        })
    }

    #[test]
    fn test_explain_finds_component_splits() {
        let (layer, tree) = saas_tree();
        // arr = net_mrr * 12; net_mrr = new + expansion - churned
        // Scenario: arr dropped by 24K. net_mrr dropped 2K. churned_mrr spiked.
        let mut data = HashMap::new();
        data.insert(
            "revenue.arr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__arr", jn(120000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__arr", jn(96000.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.net_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__net_mrr", jn(10000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__net_mrr", jn(8000.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.churned_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__churned_mrr", jn(1000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__churned_mrr", jn(3400.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.new_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__new_mrr", jn(2000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__new_mrr", jn(1800.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.expansion_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__expansion_mrr", jn(500.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__expansion_mrr", jn(600.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        assert_eq!(result.target, "revenue.arr");
        assert!((result.target_delta - (-24000.0)).abs() < 0.01);
        // Should have at least one node (component split)
        assert!(!result.nodes.is_empty());
        // First node should be the component split with highest concentration
        // net_mrr has delta -2000, concentration = 2000/24000 ≈ 0.083
        // The algorithm should find component splits
        let has_component = result
            .nodes
            .iter()
            .any(|n| matches!(&n.split, SplitKind::Component { .. }));
        assert!(has_component, "Should find component splits");
    }

    #[test]
    fn test_explain_not_found() {
        let (layer, tree) = saas_tree();
        let data = HashMap::new();
        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "nonexistent.metric",
            "revenue.created_at",
            ("2024-02-01", "2024-02-29"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_explain_zero_delta() {
        let (layer, tree) = saas_tree();
        let mut data = HashMap::new();
        // Same value in both periods → zero delta → no splits needed
        data.insert(
            "revenue.arr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__arr", jn(100000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__arr", jn(100000.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        assert!((result.target_delta).abs() < 0.01);
        assert!(result.nodes.is_empty());
        assert!((result.coverage - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_explain_with_dimension_splits() {
        // Create a view with dimensions so the algorithm can try dimension splits
        let revenue_view = View {
            name: "revenue".to_string(),
            description: "revenue view".to_string(),
            label: None,
            datasource: None,
            dialect: None,
            table: Some("public.revenue".to_string()),
            sql: None,
            entities: vec![],
            dimensions: vec![crate::schema::models::Dimension {
                name: "plan".to_string(),
                dimension_type: DimensionType::String,
                description: None,
                expr: "plan".to_string(),
                original_expr: None,
                samples: None,
                synonyms: None,
                inherits_from: None,
                primary_key: None,
                sub_query: None,
                meta: None,
            }],
            measures: Some(vec![atomic_measure("mrr", MeasureType::Sum)]),
            segments: vec![],
            meta: None,
        };
        let layer = make_layer(vec![revenue_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        // Aggregate: mrr dropped by 1000
        data.insert(
            "revenue.mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__mrr", jn(10000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__mrr", jn(9000.0)),
                ]),
            ],
        );
        // Dimension breakdown: Enterprise accounts for 900 of the 1000 drop
        data.insert(
            "revenue.mrr:revenue.plan".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__plan", js("Enterprise")),
                    ("revenue__mrr", jn(5000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__plan", js("Pro")),
                    ("revenue__mrr", jn(5000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__plan", js("Enterprise")),
                    ("revenue__mrr", jn(4100.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__plan", js("Pro")),
                    ("revenue__mrr", jn(4900.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.mrr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        // Should find a dimension split for plan=Enterprise
        let has_dim_split = result.nodes.iter().any(|n| {
            matches!(&n.split, SplitKind::Dimension { dimension, value }
                if dimension == "revenue.plan" && value == "Enterprise")
        });
        assert!(has_dim_split, "Should find Enterprise dimension split");
    }

    #[test]
    fn test_explain_includes_drivers() {
        let (layer, tree) = saas_tree_with_drivers();
        let mut data = HashMap::new();
        data.insert(
            "revenue.arr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__arr", jn(120000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__arr", jn(96000.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.net_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__net_mrr", jn(10000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__net_mrr", jn(8000.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.churn_rate".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__churn_rate", jn(0.04)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__churn_rate", jn(0.16)),
                ]),
            ],
        );
        data.insert(
            "revenue.churned_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__churned_mrr", jn(1000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__churned_mrr", jn(3400.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.new_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__new_mrr", jn(2000.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__new_mrr", jn(1800.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.expansion_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__expansion_mrr", jn(500.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__expansion_mrr", jn(600.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        // Should find at least some splits (component or driver)
        assert!(!result.nodes.is_empty());
    }

    // ── Pathological explain tests ────────────────────────
    //
    // These tests construct scenarios where the greedy, single-path algorithm
    // is known to produce suboptimal results. Each test documents:
    //   (a) what the CURRENT algorithm produces
    //   (b) what the OPTIMAL algorithm SHOULD produce
    //
    // When improving the algorithm, flip the assertions from (a) → (b).

    /// Build a filter-aware mock executor.
    ///
    /// Keys:
    /// - `"measure"` → aggregate (no dims, no filters)
    /// - `"measure:dim"` → dimension breakdown (no filters)
    /// - `"measure:dim|member=val"` → filtered breakdown
    /// - `"measure|member=val"` → filtered aggregate
    ///
    /// Filter keys are sorted alphabetically and joined with `&`.
    fn filter_aware_mock(
        data: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ) -> Box<QueryExecutor> {
        Box::new(move |q: &QueryRequest| {
            let measure = &q.measures[0];

            // Build filter suffix from active (non-time) filters
            let mut filter_parts: Vec<String> = q
                .filters
                .iter()
                .filter_map(|f| {
                    let member = f.member.as_deref()?;
                    let val = f.values.first()?;
                    Some(format!("{}={}", member, val))
                })
                .collect();
            filter_parts.sort();
            let filter_suffix = if filter_parts.is_empty() {
                String::new()
            } else {
                format!("|{}", filter_parts.join("&"))
            };

            // Try most specific key first, fall back to less specific
            if !q.dimensions.is_empty() {
                let dim = &q.dimensions[0];
                // "measure:dim|filters"
                let key = format!("{}:{}{}", measure, dim, filter_suffix);
                if let Some(rows) = data.get(&key) {
                    return Ok(rows.clone());
                }
                // "measure:dim" (no filter)
                let key_no_filter = format!("{}:{}", measure, dim);
                if let Some(rows) = data.get(&key_no_filter) {
                    return Ok(rows.clone());
                }
            }
            // "measure|filters"
            if !filter_suffix.is_empty() {
                let key = format!("{}{}", measure, filter_suffix);
                if let Some(rows) = data.get(&key) {
                    return Ok(rows.clone());
                }
            }
            // "measure"
            Ok(data.get(measure.as_str()).cloned().unwrap_or_default())
        })
    }

    /// Make a view with named dimensions and measures.
    fn make_view_with_dims(
        name: &str,
        dim_names: &[&str],
        measure_names: &[(&str, MeasureType)],
    ) -> View {
        View {
            name: name.to_string(),
            description: Some(format!("{} view", name)),
            label: None,
            datasource: None,
            dialect: None,
            table: Some(format!("public.{}", name)),
            sql: None,
            entities: vec![],
            dimensions: dim_names
                .iter()
                .map(|d| crate::schema::models::Dimension {
                    name: d.to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: d.to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    inherits_from: None,
                    primary_key: None,
                    sub_query: None,
                    meta: None,
                })
                .collect(),
            measures: Some(
                measure_names
                    .iter()
                    .map(|(n, t)| atomic_measure(n, t.clone()))
                    .collect(),
            ),
            segments: vec![],
            meta: None,
        }
    }

    fn run_explain(
        layer: &SemanticLayer,
        tree: &MetricTree,
        target: &str,
        data: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ) -> ExplainResult {
        let exec = filter_aware_mock(data);
        explain(
            tree,
            layer,
            target,
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap()
    }

    /// Helper: create 2-row aggregate (prev, curr) for a measure.
    fn agg(measure: &str, prev: f64, curr: f64) -> (String, Vec<serde_json::Map<String, serde_json::Value>>) {
        let alias = measure.replace('.', "__");
        (
            measure.to_string(),
            vec![
                row(&[
                    (&format!("{}__created_at", measure.split('.').next().unwrap()), js("2024-01")),
                    (&alias, jn(prev)),
                ]),
                row(&[
                    (&format!("{}__created_at", measure.split('.').next().unwrap()), js("2024-02")),
                    (&alias, jn(curr)),
                ]),
            ],
        )
    }

    /// Helper: create dimension breakdown rows.
    /// `entries` is (dim_value, prev, curr) tuples.
    fn dim_breakdown(
        measure: &str,
        dim: &str,
        entries: &[(&str, f64, f64)],
    ) -> (String, Vec<serde_json::Map<String, serde_json::Value>>) {
        let key = format!("{}:{}", measure, dim);
        let measure_alias = measure.replace('.', "__");
        let dim_alias = dim.replace('.', "__");
        let time_col = format!("{}__created_at", measure.split('.').next().unwrap());
        let mut rows_vec = Vec::new();
        for (val, prev, curr) in entries {
            rows_vec.push(row(&[
                (&time_col, js("2024-01")),
                (&dim_alias, js(val)),
                (&measure_alias, jn(*prev)),
            ]));
            rows_vec.push(row(&[
                (&time_col, js("2024-02")),
                (&dim_alias, js(val)),
                (&measure_alias, jn(*curr)),
            ]));
        }
        (key, rows_vec)
    }

    /// Like dim_breakdown but with a filter qualifier in the key.
    fn dim_breakdown_filtered(
        measure: &str,
        dim: &str,
        filter_str: &str,
        entries: &[(&str, f64, f64)],
    ) -> (String, Vec<serde_json::Map<String, serde_json::Value>>) {
        let key = format!("{}:{}|{}", measure, dim, filter_str);
        let measure_alias = measure.replace('.', "__");
        let dim_alias = dim.replace('.', "__");
        let time_col = format!("{}__created_at", measure.split('.').next().unwrap());
        let mut rows_vec = Vec::new();
        for (val, prev, curr) in entries {
            rows_vec.push(row(&[
                (&time_col, js("2024-01")),
                (&dim_alias, js(val)),
                (&measure_alias, jn(*prev)),
            ]));
            rows_vec.push(row(&[
                (&time_col, js("2024-02")),
                (&dim_alias, js(val)),
                (&measure_alias, jn(*curr)),
            ]));
        }
        (key, rows_vec)
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 1: Checkerboard Interaction
    //
    // The root cause lives in a 2D interaction (platform × region) that
    // is invisible from either dimension alone. Each single-dimension
    // split shows a perfectly uniform 50/50 split.
    //
    // OPTIMAL: identify (Android, EU) and (iOS, US) as the pair that
    //          together explain 100% of the drop (each -100).
    // CURRENT: picks one dimension arbitrarily, gets 50% coverage, and
    //          the second dimension is only found at depth 2.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_checkerboard_interaction() {
        let view = make_view_with_dims(
            "sales",
            &["platform", "region"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        //          |  US  |  EU  | total
        //  iOS     | 250  | 250  |  500  → 300 + 150 = 450
        //  Android | 250  | 250  |  500  → 150 + 300 = 450
        //  total   | 500  | 500  | 1000  →  450 + 450 = 900
        //
        // Delta by cell: iOS,US=+50  iOS,EU=-100  Android,US=-100  Android,EU=+50
        // Per-dimension: each value shows -50 (uniform). No single dimension dominates.

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 1000.0, 900.0),
            dim_breakdown("sales.revenue", "sales.platform", &[
                ("iOS", 500.0, 450.0),
                ("Android", 500.0, 450.0),
            ]),
            dim_breakdown("sales.revenue", "sales.region", &[
                ("US", 500.0, 450.0),
                ("EU", 500.0, 450.0),
            ]),
            // Depth-2: after filtering to platform=iOS, split by region
            dim_breakdown_filtered(
                "sales.revenue", "sales.region", "sales.platform=iOS",
                &[("US", 250.0, 300.0), ("EU", 250.0, 150.0)],
            ),
            // Depth-2: after filtering to platform=Android, split by region
            dim_breakdown_filtered(
                "sales.revenue", "sales.region", "sales.platform=Android",
                &[("US", 250.0, 150.0), ("EU", 250.0, 300.0)],
            ),
            // Depth-2: after filtering to region=US, split by platform
            dim_breakdown_filtered(
                "sales.revenue", "sales.platform", "sales.region=US",
                &[("iOS", 250.0, 300.0), ("Android", 250.0, 150.0)],
            ),
            // Depth-2: after filtering to region=EU, split by platform
            dim_breakdown_filtered(
                "sales.revenue", "sales.platform", "sales.region=EU",
                &[("iOS", 250.0, 150.0), ("Android", 250.0, 300.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-100.0)).abs() < 0.01);

        // Current behavior: EVEN WORSE than expected!
        // Each dimension maintains exactly 50/50 proportions across periods:
        //   iOS: 500/1000 → 450/900 = 0.50 both periods
        //   Android: same
        // JSD = 0 for both dimensions (no distributional shift).
        // With zero surprise and no component edges, the algorithm finds
        // NO candidates at all. The interaction is completely invisible.
        assert!(
            result.nodes.is_empty(),
            "checkerboard interaction is invisible: JSD=0 for both dimensions"
        );
        assert!(
            result.coverage < 0.01,
            "zero coverage: algorithm cannot detect the interaction"
        );

        // OPTIMAL: a multi-dim aware algorithm would evaluate (platform, region) jointly
        // and find that (iOS,EU)=-100 and (Android,US)=-100 each have concentration 1.0,
        // rather than relying on single-dimension JSD which is blind to this pattern.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 2: JSD Distraction (Surprise vs Concentration)
    //
    // Dimension A (source) has high total JSD (many values shuffling)
    // but the top element only has ~50% concentration.
    // Dimension B (plan) has low JSD (proportions barely shift) but
    // the top element has 95% concentration — a clear, actionable answer.
    //
    // OPTIMAL: pick plan=Enterprise (95% concentration).
    // CURRENT: picks source (higher JSD surprise), recurses into a
    //          50%-concentration element, missing the simple answer.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_jsd_distraction() {
        let view = make_view_with_dims(
            "sales",
            &["source", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Dim A (source): 10 sources with large proportional shuffling.
        // 5 declining sources: 1000 → 500 each (delta -500, EP 0.50)
        // 5 rising sources:    1000 → 1300 each (delta +300, EP -0.30)
        // Total: 10000 → 9000, delta = -1000
        let source_entries: Vec<(&str, f64, f64)> = vec![
            ("src_1", 1000.0, 500.0),
            ("src_2", 1000.0, 500.0),
            ("src_3", 1000.0, 500.0),
            ("src_4", 1000.0, 500.0),
            ("src_5", 1000.0, 500.0),
            ("src_6", 1000.0, 1300.0),
            ("src_7", 1000.0, 1300.0),
            ("src_8", 1000.0, 1300.0),
            ("src_9", 1000.0, 1300.0),
            ("src_10", 1000.0, 1300.0),
        ];

        // Dim B (plan): 2 values, Enterprise concentrates the drop.
        // Enterprise: 8000 → 7050, delta = -950 (EP = 0.95)
        // Free:       2000 → 1950, delta = -50  (EP = 0.05)
        // Total:     10000 → 9000, delta = -1000
        //
        // JSD is low because proportions barely shift:
        //   Enterprise: 80% → 78.3%, Free: 20% → 21.7%

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // CURRENT BEHAVIOR: algorithm picks source (higher total JSD) over plan.
        // The top element is one of the declining sources with concentration ~0.50.
        //
        // Document which dimension was actually chosen:
        let chose_source = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.source");

        // The algorithm should ideally pick plan (top concentration 0.95 >> source's 0.50).
        // But JSD-based inter-dimension ranking may pick source instead.
        //
        // NOTE: After the bug-fix comparison step (comp_max vs dim_max), the algorithm
        // compares the TOP ELEMENT's concentration between the winning dimension and
        // components. But between dimensions, it uses surprise. So if source wins by
        // surprise, its top element (concentration 0.50) is used, not plan's (0.95).
        // VERIFIED: algorithm picks source (higher total JSD) over plan.
        // source's top element has concentration 0.50 — half the drop,
        // while plan=Enterprise would explain 95% at 0.95 concentration.
        assert!(chose_source, "algorithm picks source (higher JSD), not plan");
        assert!(
            (top.concentration - 0.50).abs() < 0.01,
            "source top concentration should be 0.50, got {}",
            top.concentration
        );
        // OPTIMAL: would choose plan=Enterprise at 0.95 concentration.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 3: Simpson's Paradox (Mix Shift)
    //
    // An average/rate metric drops at the aggregate level, but EVERY
    // segment's rate actually improved. The cause is a mix shift:
    // traffic moved from a high-converting segment to a low-converting one.
    //
    // The algorithm splits by device and finds NEGATIVE concentrations
    // (both segments improved, opposing the aggregate direction), so
    // it cannot explain the drop via dimension splits at all.
    //
    // OPTIMAL: detect the mix shift pattern and report it.
    // CURRENT: finds no useful splits (all concentrations are negative
    //          or zero), produces empty/low-coverage result.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_simpsons_paradox() {
        // Conversion rate (an average metric)
        // Aggregate: 5.0% → 3.92%, delta = -1.08%
        //
        // Device breakdown:
        //   Mobile:  3.0% → 3.5%  (rate UP, delta +0.50)
        //   Desktop: 5.5% → 6.0%  (rate UP, delta +0.50)
        //
        // Mix shift: Mobile went from 20% to 83% of traffic.
        // Weighted avg: 0.20*3.0 + 0.80*5.5 = 5.0 → 0.83*3.5 + 0.17*6.0 = 3.93

        let view = make_view_with_dims(
            "sales",
            &["device"],
            &[("conversion_rate", MeasureType::Average)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.conversion_rate", 5.0, 3.92),
            dim_breakdown("sales.conversion_rate", "sales.device", &[
                ("Mobile", 3.0, 3.5),   // rate UP
                ("Desktop", 5.5, 6.0),  // rate UP
            ]),
        ]);

        let result = run_explain(&layer, &tree, "sales.conversion_rate", data);

        assert!((result.target_delta - (-1.08)).abs() < 0.01);

        // Current behavior: both segments have POSITIVE deltas (+0.5 each),
        // which give NEGATIVE concentration relative to the negative parent delta.
        // The algorithm sees no useful candidates (best concentration < 0) and
        // produces an empty result.
        //
        // This is a fundamental limitation: the algorithm decomposes the value
        // by segment but doesn't account for mix-shift effects on weighted averages.
        assert!(
            result.nodes.is_empty(),
            "Simpson's paradox: no splits found (both segments improved individually)"
        );
        assert!(
            result.coverage < 0.01,
            "coverage should be ~0 for Simpson's paradox"
        );
        // OPTIMAL: a mix-shift-aware algorithm would decompose the aggregate change
        // into (a) within-segment rate changes and (b) between-segment mix changes,
        // identifying that the mix shift toward Mobile explains the aggregate decline.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 4: Death by a Thousand Cuts
    //
    // 200 products each drop by exactly 5 units. No single product
    // exceeds the 5% EP threshold (5/1000 = 0.5%), so ALL elements
    // are filtered as noise. The algorithm finds no root cause.
    //
    // OPTIMAL: detect the "uniform degradation" pattern and report
    //          that the drop is evenly distributed across all products.
    // CURRENT: filters all elements below MIN_ELEMENT_EP, gets zero
    //          dimension surprise, produces empty result.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_death_by_thousand_cuts() {
        let view = make_view_with_dims(
            "sales",
            &["product"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // 200 products, each: 50 → 45, delta = -5, EP = 5/1000 = 0.005
        let product_entries: Vec<(String, f64, f64)> = (1..=200)
            .map(|i| (format!("product_{}", i), 50.0, 45.0))
            .collect();
        let product_refs: Vec<(&str, f64, f64)> = product_entries
            .iter()
            .map(|(s, p, c)| (s.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.product", &product_refs),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);

        // Current behavior: all 200 products have EP = 0.005, well below
        // MIN_ELEMENT_EP = 0.05, so dimension surprise = 0. No candidates pass.
        assert!(
            result.nodes.is_empty(),
            "greedy algorithm should find no candidates when all elements are below EP threshold"
        );
        assert!(
            result.coverage < 0.01,
            "coverage should be ~0 (no candidates found)"
        );

        // OPTIMAL: detect that the drop is uniformly distributed and report
        // "all 200 products declined by ~0.5% each — likely a systemic issue,
        // not attributable to any specific product."
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 5: Decoy High-Cardinality Dimension
    //
    // Dim A (plan): 2 values, Enterprise accounts for 90% of the drop.
    //   Low cardinality → low total JSD even though signal is concentrated.
    // Dim B (user_id): 100 values with random variation.
    //   High cardinality → high total JSD from many small surprises.
    //
    // The algorithm picks the wrong dimension (user_id) by surprise,
    // missing the clear, actionable answer (plan=Enterprise).
    //
    // OPTIMAL: pick plan=Enterprise (0.90 concentration).
    // CURRENT: picks user_id (higher accumulated surprise), top element
    //          has much lower concentration.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_decoy_high_cardinality() {
        let view = make_view_with_dims(
            "sales",
            &["plan", "user_id"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Dim A (plan): clear signal, low JSD
        // Enterprise: 9000 → 8100, delta = -900 (EP = 0.90)
        // Free:       1000 →  900, delta = -100 (EP = 0.10)

        // Dim B (user_id): 100 users with random-looking variation.
        // 20 users drop significantly (pass EP threshold), 20 rise slightly,
        // 60 are near-flat. Total: 10000 → 9000, delta = -1000.
        //   20 × (150→85):  prev=3000, curr=1700, delta=-1300
        //   20 × (50→60):   prev=1000, curr=1200, delta=+200
        //   59 × (100→102): prev=5900, curr=6018, delta=+118
        //    1 × (100→82):  prev=100,  curr=82,   delta=-18
        //   Total:           prev=10000, curr=9000, delta=-1000 ✓
        let mut user_entries: Vec<(String, f64, f64)> = Vec::new();
        for i in 1..=20 {
            user_entries.push((format!("user_{}", i), 150.0, 85.0));
        }
        for i in 21..=40 {
            user_entries.push((format!("user_{}", i), 50.0, 60.0));
        }
        for i in 41..=99 {
            user_entries.push((format!("user_{}", i), 100.0, 102.0));
        }
        user_entries.push(("user_100".to_string(), 100.0, 82.0));

        let user_refs: Vec<(&str, f64, f64)> = user_entries
            .iter()
            .map(|(s, p, c)| (s.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 9000.0, 8100.0),
                ("Free", 1000.0, 900.0),
            ]),
            dim_breakdown("sales.revenue", "sales.user_id", &user_refs),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];
        let chose_user = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.user_id");

        // user_id has 20 elements with EP = -65/(-1000) = 0.065 > 0.05 threshold
        // (users 1-20 each drop 65). Their surprise values accumulate.
        // plan has 2 elements, both above EP threshold, but lower total surprise.
        //
        // Whether the algorithm picks the right dimension depends on the exact
        // JSD accumulation. Document the actual behavior:
        // VERIFIED: algorithm picks user_id (higher accumulated JSD) over plan.
        // user_id's top element has concentration 0.065 (6.5% of drop),
        // while plan=Enterprise would explain 90% at 0.90 concentration.
        assert!(chose_user, "algorithm picks user_id (higher JSD), not plan");
        assert!(
            (top.concentration - 0.065).abs() < 0.01,
            "user_id top concentration should be 0.065, got {}",
            top.concentration
        );
        // OPTIMAL: plan=Enterprise at 0.90 concentration is far more actionable.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 6: Component Hides Cross-Cutting Dimension
    //
    // Metric = A + B.  A dropped 600, B dropped 400.
    // Algorithm picks component A (concentration 0.60).
    // Within A, dim X = "foo" accounts for 100% of A's drop.
    // Within B, dim X = "foo" also accounts for 100% of B's drop.
    //
    // The REAL insight: X="foo" explains 100% of the total drop across
    // both components. But greedy only reports "A → A.X=foo" (60% coverage).
    //
    // OPTIMAL: detect that X="foo" is a cross-cutting root cause (100% coverage).
    // CURRENT: follows component A, finds X=foo at 60% of root, stops.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_component_hides_cross_cutting_dimension() {
        // Two views: "ads" and "subs", metric = ads.revenue + subs.revenue
        let ads_view = make_view_with_dims(
            "ads",
            &["region"],
            &[("revenue", MeasureType::Sum)],
        );
        let subs_view = make_view_with_dims(
            "subs",
            &["region"],
            &[("revenue", MeasureType::Sum)],
        );
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![
            composite_measure("revenue", "{{ads.revenue}} + {{subs.revenue}}"),
        ]);

        let layer = make_layer(vec![total_view, ads_view, subs_view]);
        let tree = MetricTree::build(&layer);

        // total.revenue: 10000 → 9000, delta = -1000
        // ads.revenue:   6000 → 5400,  delta = -600  (concentration 0.60)
        // subs.revenue:  4000 → 3600,  delta = -400  (concentration 0.40)
        //
        // Within ads, region breakdown:
        //   US:  5000 → 5000,  delta = 0
        //   EU:  1000 →  400,  delta = -600 (100% of ads drop)
        //
        // Within subs, region breakdown:
        //   US:  3500 → 3500,  delta = 0
        //   EU:   500 →  100,  delta = -400 (100% of subs drop)
        //
        // Cross-cutting: EU accounts for 100% of total drop across both components.

        let mut data = HashMap::new();
        data.extend([
            agg("total.revenue", 10000.0, 9000.0),
            agg("ads.revenue", 6000.0, 5400.0),
            agg("subs.revenue", 4000.0, 3600.0),
            // total has no dimensions, so no dimension breakdowns for total
            // Within ads (after component split):
            dim_breakdown("ads.revenue", "ads.region", &[
                ("US", 5000.0, 5000.0),
                ("EU", 1000.0, 400.0),
            ]),
            // Within subs:
            dim_breakdown("subs.revenue", "subs.region", &[
                ("US", 3500.0, 3500.0),
                ("EU", 500.0, 100.0),
            ]),
        ]);

        let result = run_explain(&layer, &tree, "total.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Current behavior: algorithm picks component ads.revenue (higher concentration
        // at 0.60 vs subs at 0.40), then recurses into ads and finds region=EU.
        let top = &result.nodes[0];
        let is_component = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "ads.revenue");
        assert!(is_component, "should pick ads component first: {:?}", top.split);

        // At depth 2, should find region=EU within ads
        assert!(!top.children.is_empty(), "should recurse into ads");
        let child = &top.children[0];
        let found_eu = matches!(&child.split, SplitKind::Dimension { dimension, value }
            if dimension == "ads.region" && value == "EU");
        assert!(found_eu, "should find ads.region=EU at depth 2: {:?}", child.split);

        // Coverage is only root_fraction of ads path: ~0.60
        // (ads is 60% of total, EU is 100% of ads → root_fraction = 0.60)
        assert!(
            result.coverage < 0.70,
            "coverage should be ~0.60, got {}",
            result.coverage
        );

        // OPTIMAL: would detect that "EU" is the cross-cutting cause across both
        // components and report 100% coverage: EU dropped 1000 total
        // (ads.EU=-600 + subs.EU=-400).
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 7: Greedy Picks Shallow Winner, Misses Deep Concentration
    //
    // Two components: A (concentration 0.55) and B (concentration 0.45).
    // The algorithm greedily picks A. Within A, the best dimension split
    // has only 40% concentration (diffuse across many segments).
    // Within B, one segment accounts for 95% of B's drop.
    //
    // Path via A: root_fraction = 0.55 × 0.40 = 0.22 (weak deep signal)
    // Path via B: root_fraction = 0.45 × 0.95 = 0.43 (strong deep signal)
    //
    // OPTIMAL: explore B first for its stronger deep concentration.
    // CURRENT: picks A (higher top-level concentration), gets worse deep result.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_greedy_shallow_winner() {
        let comp_a = make_view_with_dims(
            "comp_a",
            &["segment"],
            &[("metric", MeasureType::Sum)],
        );
        let comp_b = make_view_with_dims(
            "comp_b",
            &["segment"],
            &[("metric", MeasureType::Sum)],
        );
        let mut parent = make_view_with_dims("parent", &[], &[]);
        parent.measures = Some(vec![
            composite_measure("metric", "{{comp_a.metric}} + {{comp_b.metric}}"),
        ]);

        let layer = make_layer(vec![parent, comp_a, comp_b]);
        let tree = MetricTree::build(&layer);

        // parent.metric: 10000 → 9000, delta = -1000
        // comp_a.metric: 5500 → 4950,  delta = -550 (concentration 0.55)
        // comp_b.metric: 4500 → 4050,  delta = -450 (concentration 0.45)

        let mut data = HashMap::new();
        data.extend([
            agg("parent.metric", 10000.0, 9000.0),
            agg("comp_a.metric", 5500.0, 4950.0),
            agg("comp_b.metric", 4500.0, 4050.0),
            // Within comp_a: diffuse drop across 5 segments
            dim_breakdown("comp_a.metric", "comp_a.segment", &[
                ("seg_1", 1100.0, 880.0),   // delta -220, EP 0.40
                ("seg_2", 1100.0, 990.0),   // delta -110, EP 0.20
                ("seg_3", 1100.0, 1045.0),  // delta -55,  EP 0.10
                ("seg_4", 1100.0, 1017.5),  // delta -82.5, EP 0.15
                ("seg_5", 1100.0, 1017.5),  // delta -82.5, EP 0.15
            ]),
            // Within comp_b: concentrated drop in one segment
            dim_breakdown("comp_b.metric", "comp_b.segment", &[
                ("seg_critical", 2000.0, 1572.5),  // delta -427.5, EP 0.95
                ("seg_other_1", 1000.0, 1000.0),   // delta 0
                ("seg_other_2", 1000.0, 1000.0),   // delta 0
                ("seg_other_3", 500.0, 477.5),      // delta -22.5, EP 0.05
            ]),
        ]);

        let result = run_explain(&layer, &tree, "parent.metric", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Current behavior: picks comp_a (concentration 0.55 > 0.45)
        let top = &result.nodes[0];
        let picked_a = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "comp_a.metric");
        let _picked_b = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "comp_b.metric");

        assert!(picked_a, "greedy should pick comp_a (higher concentration): {:?}", top.split);

        // Within comp_a, best segment has concentration 0.40
        if !top.children.is_empty() {
            let depth2 = &top.children[0];
            assert!(
                depth2.concentration < 0.50,
                "comp_a's best segment should have concentration ~0.40, got {}",
                depth2.concentration
            );
            // Root fraction via A: 0.55 × 0.40 = 0.22
            assert!(
                depth2.root_fraction < 0.30,
                "root_fraction via A path should be ~0.22, got {}",
                depth2.root_fraction
            );
        }

        // OPTIMAL: would evaluate both paths' potential depth and pick B:
        // comp_b → seg_critical gives root_fraction = 0.45 × 0.95 = 0.4275
        // which is nearly double the A path (0.22).
        //
        // Siblings should show comp_b was available:
        assert!(
            top.siblings.iter().any(|s| {
                matches!(&s.split, SplitKind::Component { child_measure }
                    if child_measure == "comp_b.metric")
            }),
            "comp_b should appear as a sibling"
        );
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 8: Concentration Threshold Cliff
    //
    // The best candidate has concentration just below min_concentration
    // (0.049 < 0.05 threshold), so the algorithm stops immediately.
    // But there are 20 candidates at ~0.049 that TOGETHER explain 98%.
    //
    // OPTIMAL: recognize the set of similar-magnitude candidates
    //          collectively explain nearly all of the drop.
    // CURRENT: stops at the threshold, reports 0% coverage.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_concentration_threshold_cliff() {
        let view = make_view_with_dims(
            "sales",
            &["category"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // 21 categories. 20 drop by 49 each (EP=0.049, just below 0.05 threshold).
        // 1 drops by 20 (EP=0.020). Total delta = 20×(-49) + (-20) = -1000. ✓
        // Total prev = 21×500 = 10500, total curr = 20×451 + 480 = 9500. ✓
        let mut cat_entries: Vec<(String, f64, f64)> = Vec::new();
        for i in 1..=21 {
            if i <= 20 {
                cat_entries.push((format!("cat_{}", i), 500.0, 451.0)); // delta -49, EP 0.049
            } else {
                cat_entries.push((format!("cat_{}", i), 500.0, 480.0)); // delta -20, EP 0.020
            }
        }
        let cat_refs: Vec<(&str, f64, f64)> = cat_entries
            .iter()
            .map(|(s, p, c)| (s.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10500.0, 9500.0),
            dim_breakdown("sales.revenue", "sales.category", &cat_refs),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);

        // EP per category: max is 49/1000 = 0.049 < MIN_ELEMENT_EP (0.05).
        // ALL categories are filtered as noise → dim_surprise = 0.
        //
        // Even though concentration is calculated differently from EP for ranking,
        // the dimension won't be selected because its surprise score is 0
        // (all elements below EP threshold get filtered).
        // Then min_concentration check: the best candidate's concentration = 0.049 < 0.05.
        //
        // Result: no nodes emitted.
        assert!(
            result.nodes.is_empty(),
            "algorithm stops when best concentration is below threshold"
        );

        // OPTIMAL: would recognize that 20 categories each declining by 4.8%
        // collectively explain 96% of the drop — a uniform degradation pattern.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 9: Opposing Offsets in Different Components
    //
    // Metric = A - B (A is revenue, B is cost).
    // A dropped 100. B also dropped 200 (cost savings).
    // Net metric: (A'-B') - (A-B) = (4900-2800) - (5000-3000)
    //           = 2100 - 2000 = +100 (metric improved!)
    //
    // But within A, region=EU dropped 500 and US rose 400.
    // Within B (cost), region=EU dropped 300 and US rose 100.
    //
    // The EU revenue decline (-500) is partially masked by the EU cost
    // savings (-300). Net EU impact on metric: -500 - (-300) = -200.
    // US impact: +400 - (+100) = +300.
    //
    // The algorithm sees metric improved by 100, splits by component,
    // finds A at concentration -1.0 (opposing direction) and -B at
    // concentration +2.0. It misses that EU has an underlying problem.
    //
    // OPTIMAL: flag the EU revenue decline as a risk despite metric improvement.
    // CURRENT: follows the positive metric change, attributes to cost savings.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_opposing_offsets() {
        let rev_view = make_view_with_dims(
            "rev",
            &["region"],
            &[("amount", MeasureType::Sum)],
        );
        let cost_view = make_view_with_dims(
            "cost",
            &["region"],
            &[("amount", MeasureType::Sum)],
        );
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![
            composite_measure("net", "{{rev.amount}} - {{cost.amount}}"),
        ]);

        let layer = make_layer(vec![profit_view, rev_view, cost_view]);
        let tree = MetricTree::build(&layer);

        // profit.net: 2000 → 2100, delta = +100
        // rev.amount: 5000 → 4900, delta = -100, sign = +1
        //   → concentration = (-100 × 1 × 1) / 100 = -1.0 (opposing)
        // cost.amount: 3000 → 2800, delta = -200, sign = -1
        //   → concentration = (-200 × -1 × 1) / 100 = 2.0 (same direction, ×2)

        let mut data = HashMap::new();
        data.extend([
            agg("profit.net", 2000.0, 2100.0),
            agg("rev.amount", 5000.0, 4900.0),
            agg("cost.amount", 3000.0, 2800.0),
            dim_breakdown("rev.amount", "rev.region", &[
                ("US", 2000.0, 2400.0),  // delta +400
                ("EU", 3000.0, 2500.0),  // delta -500
            ]),
            dim_breakdown("cost.amount", "cost.region", &[
                ("US", 1000.0, 1100.0),   // delta +100 (cost rose)
                ("EU", 2000.0, 1700.0),   // delta -300 (cost fell)
            ]),
        ]);

        let result = run_explain(&layer, &tree, "profit.net", data);

        assert!((result.target_delta - 100.0).abs() < 0.01);

        // The metric IMPROVED. The algorithm finds cost as the dominant
        // component (concentration 2.0) since cost savings drove the improvement.
        assert!(!result.nodes.is_empty(), "should find component splits");
        let top = &result.nodes[0];
        let picked_cost = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "cost.amount");
        assert!(picked_cost, "should pick cost (concentration 2.0 > revenue's -1.0): {:?}", top.split);
        assert!(
            top.concentration > 1.5,
            "cost concentration should be ~2.0, got {}",
            top.concentration
        );

        // OPTIMAL: while the metric improved, the algorithm should surface
        // that EU revenue dropped 500 as a WARNING — it's masked by cost
        // savings that may not be sustainable.
    }

    // ─────────────────────────────────────────────────────────────
    // DEEP-FIXED variants: verify beam search handles each pathological case
    // ─────────────────────────────────────────────────────────────

    // ── Case 2: JSD Distraction — deep_fixed ──
    #[test]
    fn test_pathological_jsd_distraction_deep_fixed() {
        let view = make_view_with_dims("sales", &["source", "plan"], &[("revenue", MeasureType::Sum)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let source_entries: Vec<(&str, f64, f64)> = vec![
            ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
            ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
            ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
            ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
            ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
        ];

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "sales.revenue", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Deep pass should find plan=Enterprise (95% concentration) as a top alternative
        assert!(!result.alternatives.is_empty(), "deep pass should produce alternatives");
        let best = &result.alternatives[0];
        assert!(best.root_fraction > 0.90, "best alt should have root_fraction > 0.90, got {}", best.root_fraction);
        let found = best.nodes.iter().any(|n| matches!(&n.split,
            SplitKind::Dimension { dimension, value } if dimension == "sales.plan" && value == "Enterprise"));
        assert!(found, "deep pass should find plan=Enterprise");
    }

    // ── Case 3: Simpson's Paradox — deep_fixed ──
    #[test]
    fn test_pathological_simpsons_paradox_deep_fixed() {
        let view = make_view_with_dims("sales", &["device"], &[("conversion_rate", MeasureType::Average)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.conversion_rate", 5.0, 3.92),
            dim_breakdown("sales.conversion_rate", "sales.device", &[
                ("Mobile", 3.0, 3.5),
                ("Desktop", 5.5, 6.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "sales.conversion_rate", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Should still detect Simpson's paradox warning in deep mode
        assert!(result.warnings.iter().any(|w| matches!(w, ExplainWarning::SimpsonsParadox { .. })),
            "should detect Simpson's paradox even in deep mode");
    }

    // ── Case 4: Death by Thousand Cuts — deep_fixed ──
    #[test]
    fn test_pathological_death_by_thousand_cuts_deep_fixed() {
        let view = make_view_with_dims("sales", &["product"], &[("revenue", MeasureType::Sum)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let product_entries: Vec<(String, f64, f64)> = (0..200)
            .map(|i| (format!("prod_{}", i), 50.0, 45.0))
            .collect();
        let product_refs: Vec<(&str, f64, f64)> = product_entries.iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.product", &product_refs),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "sales.revenue", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Deep pass should find uniform degradation across 200 products
        let has_uniform = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| matches!(&n.split, SplitKind::UniformDegradation { .. }))
        }) || result.nodes.iter().any(|n| matches!(&n.split, SplitKind::UniformDegradation { .. }));
        assert!(has_uniform, "deep pass should detect uniform degradation across 200 products, alternatives: {:?}",
            result.alternatives.iter().map(|a| format!("{}: {:?}", a.strategy, a.nodes.iter().map(|n| format!("{:?}", n.split)).collect::<Vec<_>>())).collect::<Vec<_>>());
    }

    // ── Case 5: Decoy High Cardinality — deep_fixed ──
    #[test]
    fn test_pathological_decoy_high_cardinality_deep_fixed() {
        let view = make_view_with_dims("sales", &["user_id", "plan"], &[("revenue", MeasureType::Sum)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let user_entries: Vec<(String, f64, f64)> = (0..100)
            .map(|i| {
                let curr = if i < 50 { 80.0 } else { 120.0 };
                (format!("user_{}", i), 100.0, curr)
            })
            .collect();
        let user_refs: Vec<(&str, f64, f64)> = user_entries.iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.user_id", &user_refs),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "sales.revenue", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Deep pass should find plan=Enterprise as a top alternative, not get distracted by user_id
        assert!(!result.alternatives.is_empty(), "should have alternatives");
        let found_enterprise = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| matches!(&n.split,
                SplitKind::Dimension { dimension, value } if dimension == "sales.plan" && value == "Enterprise"))
        });
        assert!(found_enterprise, "deep pass should find plan=Enterprise despite user_id noise");
    }

    // ── Case 6: Component Hides Cross-Cutting Dimension — deep_fixed ──
    #[test]
    fn test_pathological_component_cross_cutting_deep_fixed() {
        let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
        let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![
            composite_measure("revenue", "{{ads.revenue}} + {{subs.revenue}}"),
        ]);

        let layer = make_layer(vec![total_view, ads_view, subs_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("total.revenue", 10000.0, 9000.0),
            agg("ads.revenue", 6000.0, 5400.0),
            agg("subs.revenue", 4000.0, 3600.0),
            dim_breakdown("ads.revenue", "ads.region", &[
                ("US", 3000.0, 3100.0),
                ("EU", 2000.0, 1500.0),
                ("APAC", 1000.0, 800.0),
            ]),
            dim_breakdown("subs.revenue", "subs.region", &[
                ("US", 2000.0, 2100.0),
                ("EU", 1500.0, 1100.0),
                ("APAC", 500.0, 400.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, max_alternatives: 5, ..Default::default() };
        let result = explain(&tree, &layer, "total.revenue", "total.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Deep pass should detect cross-cutting EU pattern across ads and subs
        let has_eu = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| match &n.split {
                SplitKind::CrossCutting { value, .. } => value == "EU",
                SplitKind::Dimension { value, .. } => value == "EU",
                _ => false,
            })
        });
        assert!(has_eu, "deep pass should find EU as a significant factor across components");
    }

    // ── Case 8: Concentration Threshold Cliff — deep_fixed ──
    #[test]
    fn test_pathological_concentration_threshold_cliff_deep_fixed() {
        let view = make_view_with_dims("sales", &["category"], &[("revenue", MeasureType::Sum)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // 20 categories each with ~4.9% concentration (just below 5% threshold)
        let mut cat_entries: Vec<(String, f64, f64)> = (0..20)
            .map(|i| (format!("cat_{}", i), 500.0, 451.0))
            .collect();
        cat_entries.push(("cat_tiny".to_string(), 100.0, 80.0));
        let cat_refs: Vec<(&str, f64, f64)> = cat_entries.iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10100.0, 9100.0),
            dim_breakdown("sales.revenue", "sales.category", &cat_refs),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "sales.revenue", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Deep pass should either find uniform degradation or have alternatives
        let has_result = !result.alternatives.is_empty() || !result.nodes.is_empty();
        assert!(has_result, "deep pass should handle sub-threshold concentration");
    }

    // ── Case 9: Opposing Offsets — deep_fixed ──
    #[test]
    fn test_pathological_opposing_offsets_deep_fixed() {
        let rev_view = make_view_with_dims("rev", &["region"], &[("amount", MeasureType::Sum)]);
        let cost_view = make_view_with_dims("cost", &["region"], &[("amount", MeasureType::Sum)]);
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![
            composite_measure("net", "{{rev.amount}} - {{cost.amount}}"),
        ]);

        let layer = make_layer(vec![profit_view, rev_view, cost_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("profit.net", 2000.0, 2100.0),
            agg("rev.amount", 5000.0, 4900.0),
            agg("cost.amount", 3000.0, 2800.0),
            dim_breakdown("rev.amount", "rev.region", &[
                ("US", 2000.0, 2400.0),
                ("EU", 3000.0, 2500.0),
            ]),
            dim_breakdown("cost.amount", "cost.region", &[
                ("US", 1000.0, 1100.0),
                ("EU", 2000.0, 1700.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(&tree, &layer, "profit.net", "profit.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"), &config, &exec).unwrap();

        // Should detect opposing offset warning even in deep mode
        assert!(result.warnings.iter().any(|w| matches!(w, ExplainWarning::OpposingOffset { .. })),
            "should detect opposing offset in deep mode");
        // Deep pass should drill into the components
        let has_component_analysis = !result.alternatives.is_empty() || !result.nodes.is_empty();
        assert!(has_component_analysis, "deep pass should analyze components");
    }

    // ─────────────────────────────────────────────────────────────
    // Heuristic detection tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_heuristic_simpsons_paradox_detected() {
        let view = make_view_with_dims(
            "sales",
            &["device"],
            &[("conversion_rate", MeasureType::Average)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.conversion_rate", 5.0, 3.92),
            dim_breakdown("sales.conversion_rate", "sales.device", &[
                ("Mobile", 3.0, 3.5),
                ("Desktop", 5.5, 6.0),
            ]),
        ]);

        let result = run_explain(&layer, &tree, "sales.conversion_rate", data);

        assert!(
            result.warnings.iter().any(|w| matches!(w, ExplainWarning::SimpsonsParadox { .. })),
            "should detect Simpson's paradox warning"
        );
    }

    #[test]
    fn test_heuristic_opposing_offset_detected() {
        let rev_view = make_view_with_dims(
            "rev",
            &["region"],
            &[("amount", MeasureType::Sum)],
        );
        let cost_view = make_view_with_dims(
            "cost",
            &["region"],
            &[("amount", MeasureType::Sum)],
        );
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![
            composite_measure("net", "{{rev.amount}} - {{cost.amount}}"),
        ]);

        let layer = make_layer(vec![profit_view, rev_view, cost_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("profit.net", 2000.0, 2100.0),
            agg("rev.amount", 5000.0, 4900.0),
            agg("cost.amount", 3000.0, 2800.0),
            dim_breakdown("rev.amount", "rev.region", &[
                ("US", 2000.0, 2400.0),
                ("EU", 3000.0, 2500.0),
            ]),
            dim_breakdown("cost.amount", "cost.region", &[
                ("US", 1000.0, 1100.0),
                ("EU", 2000.0, 1700.0),
            ]),
        ]);

        let result = run_explain(&layer, &tree, "profit.net", data);

        assert!(
            result.warnings.iter().any(|w| matches!(w, ExplainWarning::OpposingOffset { .. })),
            "should detect opposing offset warning"
        );
    }

    #[test]
    fn test_heuristic_no_false_positive() {
        let view = make_view_with_dims(
            "sales",
            &["plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!(
            result.warnings.is_empty(),
            "normal drop should produce no warnings, got {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_split_kind_serialization() {
        let uniform = SplitKind::UniformDegradation {
            dimension: "product".to_string(),
            num_elements: 200,
        };
        let json = serde_json::to_value(&uniform).unwrap();
        assert_eq!(json["type"], "uniform_degradation");
        assert_eq!(json["dimension"], "product");
        assert_eq!(json["num_elements"], 200);

        let cross = SplitKind::CrossCutting {
            dimension: "region".to_string(),
            value: "EU".to_string(),
            measures: vec!["ads.revenue".to_string(), "subs.revenue".to_string()],
        };
        let json = serde_json::to_value(&cross).unwrap();
        assert_eq!(json["type"], "cross_cutting");
        assert_eq!(json["measures"].as_array().unwrap().len(), 2);
    }

    // ── JSD smoothing and IV/WOE tests ────────────────────────

    #[test]
    fn test_jsd_element_with_smoothing() {
        let result = jsd_element_smoothed(0.0, 0.5, 1e-6);
        assert!(result.is_finite(), "smoothed JSD should be finite for zero share");
        assert!(result > 0.0, "new segment should have positive JSD");
    }

    #[test]
    fn test_jsd_element_smoothed_matches_original_for_nonzero() {
        let original = jsd_element(0.3, 0.2);
        let smoothed = jsd_element_smoothed(0.3, 0.2, 1e-10);
        assert!((original - smoothed).abs() < 1e-6, "smoothing should be negligible for nonzero shares");
    }

    #[test]
    fn test_woe_and_iv() {
        let elements = vec![
            (0.6_f64, 0.4_f64),
            (0.4, 0.6),
        ];
        let epsilon = 1e-10_f64;
        let woe_1 = ((0.4_f64 + epsilon) / (0.6_f64 + epsilon)).ln();
        let woe_2 = ((0.6_f64 + epsilon) / (0.4_f64 + epsilon)).ln();
        let iv = (0.4_f64 - 0.6_f64) * woe_1 + (0.6_f64 - 0.4_f64) * woe_2;
        assert!(iv > 0.0, "IV should be positive for shifted distribution");

        let computed = compute_iv(&elements, epsilon);
        assert!((computed - iv).abs() < 1e-6, "IV computation should match manual calc");
    }

    #[test]
    fn test_woe_zero_share_with_smoothing() {
        let elements = vec![(0.0_f64, 0.5_f64), (1.0, 0.5)];
        let iv = compute_iv(&elements, 1e-6);
        assert!(iv.is_finite(), "IV should be finite with smoothing");
        assert!(iv > 0.0, "distribution shift should produce positive IV");
    }

    // ── Adaptive EP threshold and uniform degradation tests ───────────────────

    #[test]
    fn test_adaptive_ep_threshold() {
        assert!((adaptive_ep_threshold(2) - 0.0354).abs() < 0.001);    // 0.05 / sqrt(2)
        assert!((adaptive_ep_threshold(200) - 0.00354).abs() < 0.001); // 0.05 / sqrt(200)
        assert!((adaptive_ep_threshold(1) - 0.05).abs() < 0.001);      // 0.05 / sqrt(1)
    }

    #[test]
    fn test_detect_uniform_degradation() {
        let elements: Vec<ElementScore> = (0..200)
            .map(|i| ElementScore {
                value: format!("item_{}", i),
                previous: 50.0,
                current: 45.0,
                delta: -5.0,
                ep: 0.005,
                surprise: 0.0,
            })
            .collect();
        let parent_delta = -1000.0;
        let threshold = adaptive_ep_threshold(200);
        let result = detect_uniform_degradation("sales.product", &elements, parent_delta, threshold);
        assert!(result.is_some(), "should detect uniform degradation");
        if let Some(SplitKind::UniformDegradation { dimension, num_elements }) = result {
            assert_eq!(dimension, "sales.product");
            assert_eq!(num_elements, 200);
        }
    }

    #[test]
    fn test_no_uniform_degradation_when_concentrated() {
        let elements = vec![
            ElementScore { value: "A".to_string(), previous: 8000.0, current: 7100.0, delta: -900.0, ep: 0.9, surprise: 0.01 },
            ElementScore { value: "B".to_string(), previous: 2000.0, current: 1900.0, delta: -100.0, ep: 0.1, surprise: 0.001 },
        ];
        let threshold = adaptive_ep_threshold(2);
        let result = detect_uniform_degradation("sales.plan", &elements, -1000.0, threshold);
        assert!(result.is_none(), "concentrated drop is not uniform degradation");
    }

    #[test]
    fn test_strategy_max_concentration() {
        let elements = vec![
            ElementScore { value: "A".to_string(), previous: 8000.0, current: 7050.0, delta: -950.0, ep: 0.95, surprise: 0.001 },
            ElementScore { value: "B".to_string(), previous: 2000.0, current: 1950.0, delta: -50.0, ep: 0.05, surprise: 0.0001 },
        ];
        let (score, top_value) = strategy_max_concentration(&elements, -1000.0);
        assert!((score - 0.95).abs() < 0.01);
        assert_eq!(top_value, "A");
    }

    #[test]
    fn test_strategy_topk_concentration_sum() {
        let elements = vec![
            ElementScore { value: "A".to_string(), previous: 0.0, current: 0.0, delta: -400.0, ep: 0.4, surprise: 0.0 },
            ElementScore { value: "B".to_string(), previous: 0.0, current: 0.0, delta: -350.0, ep: 0.35, surprise: 0.0 },
            ElementScore { value: "C".to_string(), previous: 0.0, current: 0.0, delta: -150.0, ep: 0.15, surprise: 0.0 },
            ElementScore { value: "D".to_string(), previous: 0.0, current: 0.0, delta: -100.0, ep: 0.10, surprise: 0.0 },
        ];
        let score = strategy_topk_concentration_sum(&elements, -1000.0, 3);
        assert!((score - 0.90).abs() < 0.01);
    }

    #[test]
    fn test_strategy_iv_ranking() {
        let elements_shifted = vec![
            ElementScore { value: "A".to_string(), previous: 6000.0, current: 3000.0, delta: -3000.0, ep: 0.6, surprise: 0.0 },
            ElementScore { value: "B".to_string(), previous: 4000.0, current: 6000.0, delta: 2000.0, ep: -0.4, surprise: 0.0 },
        ];
        let elements_stable = vec![
            ElementScore { value: "X".to_string(), previous: 5000.0, current: 4500.0, delta: -500.0, ep: 0.5, surprise: 0.0 },
            ElementScore { value: "Y".to_string(), previous: 5000.0, current: 4500.0, delta: -500.0, ep: 0.5, surprise: 0.0 },
        ];
        let iv_shifted = strategy_iv(&elements_shifted);
        let iv_stable = strategy_iv(&elements_stable);
        assert!(iv_shifted > iv_stable, "shifted distribution should have higher IV");
    }

    // ── Phase 1 tree decomposition tests ─────────────────────────────────────

    #[test]
    fn test_decompose_to_searchable_measures() {
        let (layer, tree) = saas_tree();
        let children_of = build_children_of(&tree);
        let result = decompose_to_searchable(&tree, &layer, "revenue.arr", &children_of);
        assert_eq!(result.len(), 3, "should find 3 leaf measures");
        let names: Vec<&str> = result.iter().map(|s| s.measure.as_str()).collect();
        assert!(names.contains(&"revenue.new_mrr"));
        assert!(names.contains(&"revenue.expansion_mrr"));
        assert!(names.contains(&"revenue.churned_mrr"));
        let churned = result.iter().find(|s| s.measure == "revenue.churned_mrr").unwrap();
        assert!((churned.cumulative_sign - (-1.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decompose_includes_intermediate_with_dims() {
        let leaf_a = make_view_with_dims("leaf_a", &[], &[("val", MeasureType::Sum)]);
        let leaf_b = make_view_with_dims("leaf_b", &[], &[("val", MeasureType::Sum)]);
        let mut mid = make_view_with_dims("mid", &["region"], &[]);
        mid.measures = Some(vec![composite_measure("total", "{{leaf_a.val}} + {{leaf_b.val}}")]);
        let mut top = make_view_with_dims("top", &[], &[]);
        top.measures = Some(vec![composite_measure("grand", "{{mid.total}} * 2")]);

        let layer = make_layer(vec![top, mid, leaf_a, leaf_b]);
        let tree = MetricTree::build(&layer);
        let children_of = build_children_of(&tree);
        let result = decompose_to_searchable(&tree, &layer, "top.grand", &children_of);

        let names: Vec<&str> = result.iter().map(|s| s.measure.as_str()).collect();
        assert!(names.contains(&"mid.total"), "intermediate with dims should be searchable");
        assert!(names.contains(&"leaf_a.val"), "leaf should be searchable");
        assert!(names.contains(&"leaf_b.val"), "leaf should be searchable");
    }

    // ── Beam search core tests ──────────────────────────────────────────────

    #[test]
    fn test_beam_search_finds_concentrated_path() {
        let view = make_view_with_dims(
            "sales",
            &["source", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);

        let source_entries: Vec<(&str, f64, f64)> = vec![
            ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
            ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
            ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
            ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
            ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
        ];

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let dims = vec!["sales.source".to_string(), "sales.plan".to_string()];
        let config = ExplainConfig { beam_width: 5, max_alternatives: 3, ..Default::default() };

        let paths = beam_search_measure(
            "sales.revenue",
            -1000.0,
            &dims,
            &[],
            "sales.created_at",
            ("2024-01-01", "2024-01-31"),
            ("2024-02-01", "2024-02-28"),
            &config,
            &exec,
        ).unwrap();

        assert!(!paths.is_empty(), "should find at least one path");
        let best = &paths[0];
        assert!(
            best.root_fraction > 0.90,
            "best path should have root_fraction > 0.90, got {}",
            best.root_fraction
        );
        let found_enterprise = best.nodes.iter().any(|n| {
            matches!(&n.split, SplitKind::Dimension { dimension, value }
                if dimension == "sales.plan" && value == "Enterprise")
        });
        assert!(found_enterprise, "best path should find plan=Enterprise");
    }

    #[test]
    fn test_deep_explain_jsd_distraction_fixed() {
        let view = make_view_with_dims(
            "sales",
            &["source", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let source_entries: Vec<(&str, f64, f64)> = vec![
            ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
            ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
            ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
            ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
            ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
        ];

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown("sales.revenue", "sales.plan", &[
                ("Enterprise", 8000.0, 7050.0),
                ("Free", 2000.0, 1950.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
        let result = explain(
            &tree, &layer, "sales.revenue", "sales.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"),
            &config, &exec,
        ).unwrap();

        assert!(!result.alternatives.is_empty(), "deep pass should produce alternatives");
        let best_alt = &result.alternatives[0];
        assert!(
            best_alt.root_fraction > 0.90,
            "best alternative should have root_fraction > 0.90, got {}",
            best_alt.root_fraction
        );
        let found_enterprise = best_alt.nodes.iter().any(|n| {
            matches!(&n.split, SplitKind::Dimension { dimension, value }
                if dimension == "sales.plan" && value == "Enterprise")
        });
        assert!(found_enterprise, "best alternative should find plan=Enterprise");
    }

    #[test]
    fn test_deep_explain_cross_cutting_detected() {
        let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
        let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![
            composite_measure("revenue", "{{ads.revenue}} + {{subs.revenue}}"),
        ]);

        let layer = make_layer(vec![total_view, ads_view, subs_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("total.revenue", 10000.0, 9000.0),
            agg("ads.revenue", 6000.0, 5400.0),
            agg("subs.revenue", 4000.0, 3600.0),
            dim_breakdown("ads.revenue", "ads.region", &[
                ("US", 5000.0, 5000.0),
                ("EU", 1000.0, 400.0),
            ]),
            dim_breakdown("subs.revenue", "subs.region", &[
                ("US", 3500.0, 3500.0),
                ("EU", 500.0, 100.0),
            ]),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig { deep: true, beam_width: 5, max_alternatives: 5, ..Default::default() };
        let result = explain(
            &tree, &layer, "total.revenue", "total.created_at",
            ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"),
            &config, &exec,
        ).unwrap();

        // Should find a CrossCutting alternative for region=EU
        let has_cross_cutting = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| matches!(&n.split, SplitKind::CrossCutting { value, .. } if value == "EU"))
        });
        assert!(has_cross_cutting, "should detect cross-cutting region=EU across ads and subs, got {:?}",
            result.alternatives.iter().map(|a| a.nodes.iter().map(|n| format!("{:?}", n.split)).collect::<Vec<_>>()).collect::<Vec<_>>());
    }

    // ── Statistical significance tests ───────────────────

    #[test]
    fn test_significance_test_detects_abnormal_delta() {
        let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
        let current_delta = -200.0;
        let result = compute_significance(current_delta, &historical);
        assert!(result.is_some(), "should compute significance with 6 periods");
        let sig = result.unwrap();
        assert!(sig.p_value < 0.01, "p-value should be very small for outlier delta, got {}", sig.p_value);
    }

    #[test]
    fn test_significance_test_normal_delta_not_significant() {
        let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
        let current_delta = -48.0;
        let result = compute_significance(current_delta, &historical);
        let sig = result.unwrap();
        assert!(sig.p_value > 0.05, "normal delta should not be significant, got {}", sig.p_value);
    }

    #[test]
    fn test_significance_insufficient_history() {
        let historical = vec![-50.0, -40.0];
        let result = compute_significance(-200.0, &historical);
        assert!(result.is_none(), "should return None with insufficient history");
    }
}
