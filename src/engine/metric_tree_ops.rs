use crate::engine::metric_tree::{EdgeKind, MetricEdge, MetricTree};
use crate::engine::query::{OrderBy, QueryRequest, TimeDimensionQuery};
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

// ── Explain (Recursive RCA) ─────────────────────────────

use crate::engine::query::QueryFilter;
use crate::schema::models::{DimensionType, SemanticLayer};

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
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            coverage_threshold: 0.80,
            max_depth: 10,
            max_dim_values: 20,
            min_concentration: 0.05,
            min_root_fraction: 0.005,
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
}

/// A metric's change between two periods (used internally).
#[derive(Debug, Clone)]
struct MetricDelta {
    previous: f64,
    current: f64,
    delta: f64,
}

/// A single dimension value's contribution to change (used internally).
#[derive(Debug, Clone)]
struct DimensionMover {
    value: String,
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

/// Extract dimension movers from breakdown rows.
fn extract_movers(
    measure: &str,
    dim: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
    max_values: usize,
) -> Vec<DimensionMover> {
    let measure_alias = measure.replace('.', "__");
    let dim_alias = dim.replace('.', "__");

    let mut groups: HashMap<String, Vec<&serde_json::Map<String, serde_json::Value>>> =
        HashMap::new();
    for row in rows {
        let dim_val = row
            .get(&dim_alias)
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => "NULL".to_string(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| "NULL".to_string());
        groups.entry(dim_val).or_default().push(row);
    }

    let mut movers: Vec<DimensionMover> = groups
        .into_iter()
        .map(|(value, group_rows)| {
            let (prev, curr) = match group_rows.len() {
                0 => (0.0, 0.0),
                1 => (0.0, extract_measure_value(group_rows[0], &measure_alias)),
                _ => (
                    extract_measure_value(group_rows[0], &measure_alias),
                    extract_measure_value(group_rows[1], &measure_alias),
                ),
            };
            DimensionMover {
                value,
                delta: curr - prev,
            }
        })
        .collect();

    // Sort by |delta| descending
    movers.sort_by(|a, b| {
        b.delta
            .abs()
            .partial_cmp(&a.delta.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    movers.truncate(max_values);
    movers
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
    /// Signed fraction of parent_delta (used for ranking/selection).
    concentration: f64,
    /// Normalized share of parent's change, accounting for scaling factors.
    /// For dimensions: same as concentration.
    /// For components: normalized by total_attributed (strips out e.g. ×12 in `arr = net_mrr * 12`).
    parent_share: f64,
}

/// Signed fraction: `delta / reference`, positive when same direction, negative when opposing.
fn signed_fraction(delta: f64, reference: f64) -> f64 {
    if reference.abs() > f64::EPSILON {
        (delta * reference.signum()) / reference.abs()
    } else {
        0.0
    }
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
        });
    }
    component_cands.sort_by(|a, b| {
        b.concentration
            .partial_cmp(&a.concentration)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // 2) Dimension candidates — for each dimension, collect ALL movers (for context).
    //    Keep only the best dimension (highest top-mover concentration).
    let mut best_dim: Option<(f64, Vec<Candidate>, usize)> = None; // (max_conc, candidates, total_count)
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
                let movers = extract_movers(measure, dim, &rows, ctx.config.max_dim_values);
                let total_count = movers.len();
                let remaining = remaining_dims_for(dim);
                let mut dim_cands: Vec<Candidate> = Vec::new();
                for mover in &movers {
                    let concentration = signed_fraction(mover.delta, parent_delta);
                    let mut new_filters = filters.to_vec();
                    new_filters.push(QueryFilter {
                        member: Some(dim.clone()),
                        operator: Some(crate::engine::query::FilterOperator::Equals),
                        values: vec![mover.value.clone()],
                        and: None,
                        or: None,
                    });
                    dim_cands.push(Candidate {
                        split: SplitKind::Dimension {
                            dimension: dim.clone(),
                            value: mover.value.clone(),
                        },
                        next_measure: measure.to_string(),
                        next_filters: new_filters,
                        next_dims: remaining.clone(),
                        delta: mover.delta,
                        concentration,
                        parent_share: concentration,
                    });
                }
                dim_cands.sort_by(|a, b| {
                    b.concentration
                        .partial_cmp(&a.concentration)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                if let Some(top) = dim_cands.first() {
                    let max_conc = top.concentration;
                    let is_better = match &best_dim {
                        None => true,
                        Some((existing, _, _)) => max_conc > *existing,
                    };
                    if is_better {
                        best_dim = Some((max_conc, dim_cands, total_count));
                    }
                }
            }
            Err(_) => continue,
        }
    }

    // 3) Pick the type with highest max concentration
    let comp_max = component_cands
        .first()
        .map(|c| c.concentration)
        .unwrap_or(f64::NEG_INFINITY);
    let dim_max = best_dim
        .as_ref()
        .map(|(m, _, _)| *m)
        .unwrap_or(f64::NEG_INFINITY);

    if comp_max >= dim_max {
        Ok(EvalResult {
            candidates: component_cands,
            dimension_count: None,
        })
    } else {
        let (_, cands, total) = best_dim.unwrap_or((0.0, Vec::new(), 0));
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

    // Collect emitted nodes with their root fractions for deferred coverage tracking
    let mut emitted: Vec<(ExplainNode, f64)> = Vec::new();

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

// ── Tests ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::metric_tree::MetricTree;
    use crate::schema::models::*;

    fn make_view(name: &str, measures: Vec<Measure>) -> View {
        View {
            name: name.to_string(),
            description: Some(format!("{} view", name)),
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
            description: Some("revenue view".to_string()),
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
}
