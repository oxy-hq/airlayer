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
pub fn predict(
    tree: &MetricTree,
    changes: &[(String, f64)],
) -> Result<PredictResult, EngineError> {
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

// ── Explain ─────────────────────────────────────────────

/// The role a step plays in the explain plan.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainRole {
    /// The target metric itself.
    Overview,
    /// A direct mathematical component of the target.
    Component,
    /// A driver (correlative/causal) of the target.
    Driver,
    /// A dimensional breakdown of a metric.
    Breakdown,
}

/// A single step in an explain plan — one compilable query.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainStep {
    /// Human-readable step name.
    pub name: String,
    /// What this step shows.
    pub description: String,
    /// Role of this step in the plan.
    pub role: ExplainRole,
    /// The measure being examined.
    pub measure: String,
    /// Breakdown dimension (only for Breakdown steps).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdown_dimension: Option<String>,
    /// The query to compile/execute.
    pub query: QueryRequest,
}

/// A plan of queries that explain why a metric changed between two periods.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainPlan {
    pub target: String,
    pub time_dimension: String,
    pub current_period: (String, String),
    pub previous_period: (String, String),
    pub granularity: String,
    pub steps: Vec<ExplainStep>,
}

/// Generate a plan of queries that explain why a target metric changed.
///
/// Walks the metric tree backward from the target, generating comparison
/// queries for each component and driver. Each step is a standard
/// `QueryRequest` that can be compiled to SQL by the semantic engine.
///
/// The plan covers both periods in a single query per step (using
/// the time dimension with the specified granularity), so you get
/// two rows per step — one per period.
pub fn explain(
    tree: &MetricTree,
    target: &str,
    time_dimension: &str,
    current_period: (&str, &str),
    previous_period: (&str, &str),
    granularity: &str,
    breakdown_dimensions: &[String],
) -> Result<ExplainPlan, EngineError> {
    if !tree.nodes.iter().any(|n| n.id == target) {
        return Err(EngineError::QueryError(format!(
            "Measure '{}' not found in metric tree",
            target
        )));
    }

    // Build reverse adjacency: target -> sources
    let mut rev_adj: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        rev_adj.entry(edge.to.as_str()).or_default().push(edge);
    }

    let mut steps = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(target.to_string());

    // Helper to push aggregate + breakdown steps for a measure
    let add_steps =
        |measure: &str, role: ExplainRole, description: &str, steps: &mut Vec<ExplainStep>| {
            let role_label = match role {
                ExplainRole::Overview => "overview",
                ExplainRole::Component => "component",
                ExplainRole::Driver => "driver",
                ExplainRole::Breakdown => "breakdown",
            };
            // Aggregate step
            steps.push(ExplainStep {
                name: format!("{}_{}", role_label, measure.replace('.', "_")),
                description: description.to_string(),
                role: role.clone(),
                measure: measure.to_string(),
                breakdown_dimension: None,
                query: make_period_query(
                    measure,
                    time_dimension,
                    previous_period.0,
                    current_period.1,
                    granularity,
                    &[],
                ),
            });
            // Breakdown steps
            for dim in breakdown_dimensions {
                steps.push(ExplainStep {
                    name: format!(
                        "breakdown_{}_by_{}",
                        measure.replace('.', "_"),
                        dim.replace('.', "_")
                    ),
                    description: format!("{} by {}", measure, dim),
                    role: ExplainRole::Breakdown,
                    measure: measure.to_string(),
                    breakdown_dimension: Some(dim.clone()),
                    query: make_period_query(
                        measure,
                        time_dimension,
                        previous_period.0,
                        current_period.1,
                        granularity,
                        &[dim.clone()],
                    ),
                });
            }
        };

    // Step 1: target overview
    add_steps(
        target,
        ExplainRole::Overview,
        &format!("Overall change in {}", target),
        &mut steps,
    );

    // BFS backward: collect components and drivers
    let mut queue: VecDeque<(String, ExplainRole)> = VecDeque::new();
    if let Some(edges) = rev_adj.get(target) {
        for edge in edges {
            let role = match edge.kind {
                EdgeKind::Component => ExplainRole::Component,
                EdgeKind::Driver => ExplainRole::Driver,
            };
            queue.push_back((edge.from.clone(), role));
        }
    }

    while let Some((node_id, role)) = queue.pop_front() {
        if !visited.insert(node_id.clone()) {
            continue;
        }

        let role_name = match role {
            ExplainRole::Component => "component",
            ExplainRole::Driver => "driver",
            _ => "input",
        };
        add_steps(
            &node_id,
            role,
            &format!("Change in {} ({} of {})", node_id, role_name, target),
            &mut steps,
        );

        // Continue backward
        if let Some(edges) = rev_adj.get(node_id.as_str()) {
            for edge in edges {
                if !visited.contains(&edge.from) {
                    let child_role = match edge.kind {
                        EdgeKind::Component => ExplainRole::Component,
                        EdgeKind::Driver => ExplainRole::Driver,
                    };
                    queue.push_back((edge.from.clone(), child_role));
                }
            }
        }
    }

    Ok(ExplainPlan {
        target: target.to_string(),
        time_dimension: time_dimension.to_string(),
        current_period: (current_period.0.to_string(), current_period.1.to_string()),
        previous_period: (previous_period.0.to_string(), previous_period.1.to_string()),
        granularity: granularity.to_string(),
        steps,
    })
}

/// Build a QueryRequest that compares a measure across two periods.
fn make_period_query(
    measure: &str,
    time_dimension: &str,
    period_start: &str,
    period_end: &str,
    granularity: &str,
    extra_dimensions: &[String],
) -> QueryRequest {
    QueryRequest {
        measures: vec![measure.to_string()],
        dimensions: extra_dimensions.to_vec(),
        time_dimensions: vec![TimeDimensionQuery {
            dimension: time_dimension.to_string(),
            granularity: Some(granularity.to_string()),
            date_range: Some(vec![period_start.to_string(), period_end.to_string()]),
        }],
        order: vec![OrderBy {
            id: time_dimension.to_string(),
            desc: false,
        }],
        ..QueryRequest::new()
    }
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

    // ── Explain tests ─────────────────────────────

    #[test]
    fn test_explain_generates_steps_for_components() {
        let (_, tree) = saas_tree();
        let plan = explain(
            &tree,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-29"),
            ("2024-01-01", "2024-01-31"),
            "month",
            &[],
        )
        .unwrap();
        assert_eq!(plan.target, "revenue.arr");
        assert_eq!(plan.granularity, "month");
        // Should have steps for: arr (overview), net_mrr, new_mrr, expansion_mrr, churned_mrr
        let measures: Vec<&str> = plan.steps.iter().map(|s| s.measure.as_str()).collect();
        assert!(measures.contains(&"revenue.arr"));
        assert!(measures.contains(&"revenue.net_mrr"));
        assert!(measures.contains(&"revenue.new_mrr"));
        // Each step should have the time dimension
        for step in &plan.steps {
            assert_eq!(step.query.time_dimensions.len(), 1);
            assert_eq!(step.query.time_dimensions[0].dimension, "revenue.created_at");
        }
    }

    #[test]
    fn test_explain_with_breakdowns() {
        let (_, tree) = saas_tree();
        let plan = explain(
            &tree,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-29"),
            ("2024-01-01", "2024-01-31"),
            "month",
            &["revenue.plan".to_string()],
        )
        .unwrap();
        // Should have breakdown steps
        let breakdown_steps: Vec<&ExplainStep> = plan
            .steps
            .iter()
            .filter(|s| matches!(s.role, ExplainRole::Breakdown))
            .collect();
        assert!(!breakdown_steps.is_empty());
        // Each breakdown step should include the dimension
        for step in &breakdown_steps {
            assert_eq!(step.query.dimensions, vec!["revenue.plan".to_string()]);
            assert!(step.breakdown_dimension.is_some());
        }
    }

    #[test]
    fn test_explain_includes_drivers() {
        let (_, tree) = saas_tree_with_drivers();
        let plan = explain(
            &tree,
            "revenue.arr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-29"),
            ("2024-01-01", "2024-01-31"),
            "month",
            &[],
        )
        .unwrap();
        let measures: Vec<&str> = plan.steps.iter().map(|s| s.measure.as_str()).collect();
        assert!(measures.contains(&"revenue.churn_rate"));
    }

    #[test]
    fn test_explain_not_found() {
        let (_, tree) = saas_tree();
        let result = explain(
            &tree,
            "nonexistent.metric",
            "revenue.created_at",
            ("2024-02-01", "2024-02-29"),
            ("2024-01-01", "2024-01-31"),
            "month",
            &[],
        );
        assert!(result.is_err());
    }
}
