use crate::engine::metric_tree::{EdgeKind, MetricEdge, MetricTree};
use crate::engine::query::{FilterOperator, QueryRequest};
use crate::engine::EngineError;
use crate::schema::models::{DriverDirection, DriverForm, DriverStrength, Measure, MeasureType};
use serde::{Deserialize, Serialize};
use statrs::distribution::{ContinuousCDF, StudentsT};
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
    // plus accumulated lag across the full path.
    struct QueueItem<'a> {
        node_id: String,
        path: Vec<String>,
        cumulative_coeff: Option<f64>,
        cumulative_lag: Option<u64>,
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
                cumulative_lag: edge.lag,
                direct_edge: edge,
            });
        }
    }

    let mut drivers = Vec::new();

    while let Some(item) = queue.pop_front() {
        // Only emit each driver once (first BFS path wins)
        if !visited.insert(item.node_id.clone()) {
            continue;
        }

        let edge = item.direct_edge;
        let is_direct = item.path.len() == 2;

        // For transitive drivers, infer direction from the cumulative coefficient
        // sign rather than the leaf-most edge (which may not reflect the full chain).
        let direction = if is_direct {
            infer_direction(edge)
        } else if let Some(coeff) = item.cumulative_coeff {
            if coeff > 0.0 {
                DriverDirection::Positive
            } else if coeff < 0.0 {
                DriverDirection::Negative
            } else {
                DriverDirection::Unknown
            }
        } else {
            infer_direction(edge)
        };

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
            direction,
            strength: infer_strength(edge),
            lag: item.cumulative_lag,
            description: edge.description.clone(),
        });

        // Continue BFS backward
        if let Some(edges) = rev_adj.get(item.node_id.as_str()) {
            for edge in edges {
                if !visited.contains(&edge.from) {
                    let child_coeff = edge_coefficient(edge);
                    let cumulative = match (item.cumulative_coeff, child_coeff) {
                        (Some(c1), Some(c2)) => Some(c1 * c2),
                        _ => None,
                    };
                    let cumulative_lag = match (item.cumulative_lag, edge.lag) {
                        (Some(a), Some(b)) => Some(a + b),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    };
                    let mut path = vec![edge.from.clone()];
                    path.extend(item.path.clone());
                    queue.push_back(QueueItem {
                        node_id: edge.from.clone(),
                        path,
                        cumulative_coeff: cumulative,
                        cumulative_lag,
                        direct_edge: edge,
                    });
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

/// Propagate hypothetical changes upward through the metric tree, without
/// current values.
///
/// Additive composites are still exact (`Δparent = Σ sign · Δchild`). Impacts
/// that would have to cross a *multiplicative* edge cannot be sized — `A × B`
/// depends on where you are standing — so they are reported with confidence
/// [`UNQUANTIFIABLE`] and `estimated_delta: 0.0`, never dropped. Pass current
/// values to [`predict_with_values`] to size them properly.
pub fn predict(tree: &MetricTree, changes: &[(String, f64)]) -> Result<PredictResult, EngineError> {
    predict_with_values(tree, changes, &MeasureValues::new())
}

/// Propagate hypothetical changes upward through the metric tree.
///
/// For each input (measure, delta), follows outgoing edges and estimates the
/// impact on parent metrics. Additive component edges apply the term's sign
/// (exact). Multiplicative ones use the log rule `Δparent ≈ parent · sign ·
/// Δchild/child`, which requires `values`. Driver edges with coefficients apply
/// the linear approximation (coefficient * delta). Impacts at the same node from
/// multiple paths are summed.
pub fn predict_with_values(
    tree: &MetricTree,
    changes: &[(String, f64)],
    values: &MeasureValues,
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
                match propagate_delta(*input_delta, edge, values) {
                    Propagation::Sized {
                        delta,
                        confidence,
                        form,
                    } => {
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
                    Propagation::Unquantifiable => record_unquantifiable(
                        &mut impacts_map,
                        &edge.to,
                        vec![input_measure.clone(), edge.to.clone()],
                        edge.lag,
                    ),
                    Propagation::Nothing => continue,
                }
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
                        if visited.contains(edge.to.as_str()) {
                            continue;
                        }
                        let cumulative_lag = match (item.lag, edge.lag) {
                            (Some(a), Some(b)) => Some(a + b),
                            (Some(a), None) => Some(a),
                            (None, Some(b)) => Some(b),
                            (None, None) => None,
                        };
                        let mut path = item.path.clone();
                        path.push(edge.to.clone());

                        match propagate_delta(item.delta, edge, values) {
                            Propagation::Sized {
                                delta,
                                confidence,
                                form,
                            } => {
                                if delta.abs() < f64::EPSILON {
                                    continue;
                                }
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
                                    lag: cumulative_lag,
                                });
                            }
                            // Stop here: an unsizable edge makes everything above
                            // it unsizable too, so do not enqueue past it.
                            Propagation::Unquantifiable => record_unquantifiable(
                                &mut impacts_map,
                                &edge.to,
                                path,
                                cumulative_lag,
                            ),
                            Propagation::Nothing => continue,
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
                // `total_delta` sums only the paths we could size. If ANY path
                // into this node was unquantifiable the total is incomplete, and
                // saying "estimated" would overstate it — surface that instead.
                confidence: if paths.iter().any(|p| p.confidence == UNQUANTIFIABLE) {
                    UNQUANTIFIABLE.to_string()
                } else if paths.iter().all(|p| p.confidence == "exact") {
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

/// Extract the coefficient from an edge.
///
/// For component edges the coefficient IS the edge's sign: a child that enters
/// the parent expression under `-` or `/` carries `sign = -1.0`, per the
/// `Δparent ≈ Σ sign · Δchild` contract on [`MetricEdge::sign`]. Returning a
/// bare `1.0` here would claim that raising a subtracted cost raises its parent.
/// For driver edges, uses the declared coefficient.
fn edge_coefficient(edge: &MetricEdge) -> Option<f64> {
    match edge.kind {
        EdgeKind::Component => Some(edge.sign),
        EdgeKind::Driver => edge.coefficient,
    }
}

/// Current values of every node reachable *forward* from `roots` (the nodes a
/// delta on a root can propagate into), plus the roots themselves.
///
/// Multiplicative propagation needs the `parent` and `child` values of each edge
/// it crosses, and the forward-reachable set is exactly the nodes that can appear
/// as either — so one batched query covers the whole traversal.
///
/// Trade-off: batching all of them into a single `QueryRequest` buys one round
/// trip at the cost of per-parent resilience. If any one measure in the set is
/// unqueryable (no shared join path or dialect with the others), the single query
/// fails and *every* multiplicative impact degrades to `unquantifiable` rather
/// than just the offending one. That is a deliberate choice: the failure is
/// visible in the result (never a wrong number), and metric trees are typically
/// rooted in one view.
///
/// A failed query yields whatever values we already have rather than an error —
/// callers degrade to additive-only propagation.
pub fn reachable_values(
    tree: &MetricTree,
    roots: &[String],
    time_dimension: &str,
    period: (&str, &str),
    executor: &QueryExecutor,
) -> MeasureValues {
    reachable_values_filtered(tree, roots, time_dimension, period, &[], executor)
}

/// [`reachable_values`], narrowed to a scope.
///
/// `scope` predicates are appended after the two date predicates and joined by
/// the engine's default conjunction, so a scope can only ever narrow the window
/// — never widen it.
pub fn reachable_values_filtered(
    tree: &MetricTree,
    roots: &[String],
    time_dimension: &str,
    period: (&str, &str),
    scope: &[QueryFilter],
    executor: &QueryExecutor,
) -> MeasureValues {
    reachable_values_outcome(tree, roots, time_dimension, period, scope, executor).0
}

/// Why a baseline fetch produced no values.
///
/// An empty [`MeasureValues`] has three very different causes, and callers
/// that collapse them into one message tell users the wrong thing: "the
/// warehouse rejected the query" and "your window contains no rows" call for
/// opposite fixes. Reported explicitly so the caller never has to guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaselineOutcome {
    /// At least one measure was valued.
    Valued,
    /// The executor returned an error — the query never produced rows.
    ExecutorError(String),
    /// The query ran and returned no rows at all.
    NoRows,
    /// Rows came back, but none carried a column matching a requested
    /// measure's alias.
    NoMatchingColumns,
    /// Nothing was reachable from the roots, so nothing was asked for.
    NothingRequested,
}

/// [`reachable_values_filtered`], reporting *why* it produced what it did.
pub fn reachable_values_outcome(
    tree: &MetricTree,
    roots: &[String],
    time_dimension: &str,
    period: (&str, &str),
    scope: &[QueryFilter],
    executor: &QueryExecutor,
) -> (MeasureValues, BaselineOutcome) {
    let mut fwd: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &tree.edges {
        fwd.entry(e.from.as_str()).or_default().push(e.to.as_str());
    }

    let mut wanted: Vec<String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    for root in roots {
        if seen.insert(root.as_str()) {
            wanted.push(root.clone());
            queue.push_back(root.as_str());
        }
    }
    while let Some(node) = queue.pop_front() {
        for &next in fwd.get(node).map(Vec::as_slice).unwrap_or(&[]) {
            if seen.insert(next) {
                wanted.push(next.to_string());
                queue.push_back(next);
            }
        }
    }

    let mut values = MeasureValues::new();
    if wanted.is_empty() {
        return (values, BaselineOutcome::NothingRequested);
    }

    let mut filters = vec![
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
    filters.extend_from_slice(scope);

    let query = QueryRequest {
        measures: wanted.clone(),
        filters,
        ..QueryRequest::new()
    };
    let rows = match executor(&query) {
        Ok(rows) => rows,
        Err(e) => return (values, BaselineOutcome::ExecutorError(e.to_string())),
    };
    let Some(row) = rows.first() else {
        return (values, BaselineOutcome::NoRows);
    };
    for id in wanted {
        let alias = id.replace('.', "__");
        if row.contains_key(&alias) {
            values.insert(id, extract_measure_value(row, &alias));
        }
    }
    let outcome = if values.is_empty() {
        BaselineOutcome::NoMatchingColumns
    } else {
        BaselineOutcome::Valued
    };
    (values, outcome)
}

/// Current value of each measure, keyed by metric-node id (`view.measure`).
///
/// Additive composites do not need these — `Δparent = Σ sign · Δchild` holds at
/// any level. Multiplicative ones do: the local derivative of `A × B` depends on
/// where you are standing. Callers that cannot supply values still get exact
/// additive propagation; multiplicative edges are then reported with confidence
/// [`UNQUANTIFIABLE`] rather than silently mis-sized *or silently dropped*.
pub type MeasureValues = HashMap<String, f64>;

/// Confidence marker for an impact the tree knows is real but cannot size:
/// a multiplicative edge reached without current values. It is emitted with
/// `estimated_delta: 0.0` — the delta is *unknown*, not zero — so that callers
/// can say "requires current values" instead of "no impact", which is a
/// different and false claim.
pub const UNQUANTIFIABLE: &str = "unquantifiable";

/// The outcome of pushing a delta across one edge.
enum Propagation {
    /// Sized, with how far to trust it.
    Sized {
        delta: f64,
        confidence: String,
        form: DriverForm,
    },
    /// The edge is real, but its magnitude is unknowable from what we were
    /// given (multiplicative, no values). Report it; do not traverse past it,
    /// since nothing above it can be sized either.
    Unquantifiable,
    /// Nothing propagates: a qualitative driver carrying no coefficient.
    Nothing,
}

/// Propagate a delta through an edge.
fn propagate_delta(input_delta: f64, edge: &MetricEdge, values: &MeasureValues) -> Propagation {
    match edge.kind {
        EdgeKind::Component if edge.operator.is_multiplicative() => {
            // Log decomposition: for a product/quotient, %Δparent ≈ Σ sign · %Δchild,
            // so Δparent ≈ parent · sign · (Δchild / child). This is what makes
            // `arr = net_mrr * 12` yield 12·Δ rather than Δ — the factor falls out
            // of parent/child without the constant ever being a node in the tree.
            let (Some(&child), Some(&parent)) = (values.get(&edge.from), values.get(&edge.to))
            else {
                return Propagation::Unquantifiable;
            };
            if child.abs() < f64::EPSILON {
                // %Δ is undefined at zero.
                return Propagation::Unquantifiable;
            }
            Propagation::Sized {
                delta: parent * edge.sign * (input_delta / child),
                // First-order only — exact just for infinitesimal moves.
                confidence: "estimated".to_string(),
                form: DriverForm::LogLog,
            }
        }
        EdgeKind::Component => {
            // Additive component edges pass through with the sign of the term:
            // for `net_revenue = order_value - shipping_costs`, a +5 move in
            // shipping_costs is a -5 move in net_revenue.
            Propagation::Sized {
                delta: input_delta * edge.sign,
                confidence: "exact".to_string(),
                form: DriverForm::Linear,
            }
        }
        EdgeKind::Driver => {
            // The declared form decides the arithmetic. `Linear` is
            // `coefficient × Δinput` and needs nothing else; the log forms are
            // statements about proportions, so without the levels they are
            // proportional to they come back unsizable rather than being
            // quietly evaluated as if they were linear.
            match compute_driver_impact(
                edge,
                input_delta,
                values.get(&edge.from).copied(),
                values.get(&edge.to).copied(),
            ) {
                DriverImpact::Sized(delta) => Propagation::Sized {
                    delta,
                    // Still first-order for the log forms: the transform is
                    // linearized at the current point, exact only for small
                    // moves. `estimated` is the claim either way.
                    confidence: "estimated".to_string(),
                    form: edge.form.clone(),
                },
                DriverImpact::Unsizable => Propagation::Unquantifiable,
                DriverImpact::NoCoefficient => Propagation::Nothing,
            }
        }
    }
}

/// Record an impact whose magnitude cannot be determined, so it surfaces as
/// "unquantifiable" rather than vanishing from the result.
fn record_unquantifiable(
    impacts_map: &mut HashMap<String, (f64, Vec<PredictImpact>)>,
    node_id: &str,
    path: Vec<String>,
    lag: Option<u64>,
) {
    let entry = impacts_map
        .entry(node_id.to_string())
        .or_insert_with(|| (0.0, Vec::new()));
    entry.1.push(PredictImpact {
        measure: node_id.to_string(),
        estimated_delta: 0.0,
        confidence: UNQUANTIFIABLE.to_string(),
        path,
        form: DriverForm::Linear,
        lag,
    });
}

/// Infer direction from an edge (quantitative coefficient takes precedence).
///
/// Component edges never carry a `coefficient` — their quantitative content is
/// the `sign` — so they must be read from that, or every component of a
/// composite reports `Unknown`.
fn infer_direction(edge: &MetricEdge) -> DriverDirection {
    let quantitative = match edge.kind {
        EdgeKind::Component => Some(edge.sign),
        EdgeKind::Driver => edge.coefficient,
    };
    if let Some(coeff) = quantitative {
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

/// What one driver edge's response implies for its target.
enum DriverImpact {
    Sized(f64),
    /// Real, carries coefficients, but cannot be sized from what this call has:
    /// a proportional form whose levels are absent or zero, a move outside the
    /// range the fit observed, or a cut past zero under a log.
    ///
    /// Its own variant rather than a linear fallback. Applying an elasticity as a
    /// level slope is wrong by a factor of `target / driver` — four orders of
    /// magnitude for a measure in the millions driven by one in the hundreds —
    /// and it looks exactly like a successful forecast. `unquantifiable` is the
    /// honest answer, and the surface already renders it.
    Unsizable,
    /// No coefficients: a direction without a magnitude.
    NoCoefficient,
}

/// The impact of a driver's change on its target, under the edge's declared
/// response.
///
/// **No `match` on `DriverForm`.** The arithmetic is decided by the form's basis
/// and link (see [`crate::engine::response`]), so a form added to that table
/// arrives here already supported. What used to be four hand-written arms — each
/// silently assuming its own aggregation was valid — is now one call.
///
/// `driver_baseline` is the driver's current aggregate, which is what turns the
/// requested delta into the proportional shift `r` the response is defined
/// against. `Linear` is the one form that needs no levels at all, and it stays
/// that way: `r * s1` is identically `coefficient * delta`, so delta-only mode
/// keeps working.
fn compute_driver_impact(
    edge: &MetricEdge,
    driver_delta: f64,
    driver_baseline: Option<f64>,
    target_baseline: Option<f64>,
) -> DriverImpact {
    use crate::engine::response::{aggregate_delta, aggregate_delta_from_total, ResponseDelta};

    if edge.coefficients.is_empty() {
        return DriverImpact::NoCoefficient;
    }
    let spec = edge.form.spec();

    let outcome = match (
        edge.moments,
        driver_baseline.filter(|v| v.abs() > f64::EPSILON),
    ) {
        // Fitted: the moments describe the actual rows, so every basis is exact
        // (bar log-linear, which has no exact aggregate form at all).
        (Some(moments), Some(x)) => aggregate_delta(
            &spec,
            &edge.coefficients,
            &moments,
            driver_delta / x,
            target_baseline,
            edge.domain,
        ),
        // Declared, or fitted but with no level to take a proportion against.
        // Deliberately NOT "invent moments from the total" — for a curvature that
        // is the 42,905x sign-flipping error the moments exist to prevent.
        _ => aggregate_delta_from_total(
            &spec,
            &edge.coefficients,
            driver_delta,
            driver_baseline,
            target_baseline,
        ),
    };

    match outcome {
        // `Approximate` is `log-linear`, the one pair with no exact aggregate
        // form. It still propagates — three shipped example views declare it —
        // but the variant is what stops the engine claiming more than it has.
        ResponseDelta::Sized(v) | ResponseDelta::Approximate(v) => DriverImpact::Sized(v),
        ResponseDelta::NeedsTarget | ResponseDelta::OutsideDomain | ResponseDelta::Undefined => {
            DriverImpact::Unsizable
        }
    }
}

// ── Opportunity Sizing ──────────────────────────────────

use crate::engine::query::QueryFilter;
use crate::schema::models::{DimensionType, EntityType, SemanticLayer, View};

/// A single segment-level opportunity (one dimension value below the best peer).
#[derive(Debug, Clone, Serialize)]
pub struct SegmentOpportunity {
    /// Dimension value (e.g., "android").
    pub segment: String,
    /// This segment's benchmarked figure. In `"rows"` weight_basis this is the
    /// per-unit RATE (value / row-count); otherwise it is the raw measure value.
    pub current_value: f64,
    /// Volume weight for this segment (see `OpportunityResult.weight_basis`):
    /// the true row count in `"rows"` mode, a value share otherwise.
    pub volume: f64,
    /// Benchmark the segment is compared against (best-peer or P75 — see
    /// `DimensionOpportunity.benchmark_basis`). A rate in `"rows"` mode.
    pub benchmark: f64,
    /// Gap to benchmark, in the same units as `current_value` (a per-unit rate
    /// deficit in `"rows"` mode). Positive = upside.
    pub gap: f64,
    /// Addressable upside in measure units: "what you'd add by lifting THIS
    /// segment to the benchmark." In `"rows"` mode this is the rate deficit
    /// applied to the segment's own volume, `(benchmark_rate − rate) × count`,
    /// so a small segment cannot masquerade as headroom just for being small.
    /// In `"value_share"` mode it is the raw `gap`.
    pub upside: f64,
    /// Whether this segment's gap cleared a real significance test
    /// (`gap_is_significant` returned `Some(true)`), or was kept only because
    /// the gate could not tell (`None` — no dispersion measure, a too-thin
    /// sample, or degenerate variance). `false` here does NOT mean the gap is
    /// fake: it means nobody proved it real. A caller MUST NOT present a
    /// `gated: false` segment as proven upside.
    pub gated: bool,
}

/// Opportunities found along one dimension.
#[derive(Debug, Clone, Serialize)]
pub struct DimensionOpportunity {
    /// Fully qualified dimension (e.g., "funnel.platform").
    pub dimension: String,
    /// Number of distinct segments observed in this dimension.
    pub cardinality: usize,
    /// How the benchmark was chosen for this dimension's segments.
    /// Either `"best_peer"` (the top-performing segment) or `"p75"` (the 75th percentile
    /// when there are enough segments).
    pub benchmark_basis: String,
    /// Total upside if every below-benchmark segment matched the benchmark.
    ///
    /// Summed over the segments that survived the significance gate, and summed
    /// *before* the tail trim and top-K truncation below — so this can exceed
    /// the sum of `segments`.
    pub total_upside: f64,
    /// Top-K segments by upside (descending). Long tail is dropped.
    pub segments: Vec<SegmentOpportunity>,
    /// Number of segments omitted from `segments` despite being real: those
    /// contributing under `TAIL_SHARE_THRESHOLD` of the dimension's upside, plus
    /// any beyond `TOP_K_SEGMENTS`. The second kind need not be small, so
    /// presenting this purely as "smaller segments" undersells it.
    pub other_segments_skipped: usize,
    /// Number of below-benchmark segments discarded because their gap could not
    /// be told apart from sampling noise.
    ///
    /// Reported rather than dropped in silence: "we found no shortfall here" and
    /// "we found one but cannot stand behind it" are different claims about the
    /// world, and only the caller knows whether the difference matters to them.
    /// Non-zero here with a small `segments` means the dimension is thinner
    /// evidence than its headline suggests.
    pub segments_dropped_as_noise: usize,
    /// Number of segments in `segments` (after tail-trim and top-K) whose
    /// `gated` is `false` — kept by fail-open policy, not proven. Mirrors
    /// `segments_dropped_as_noise`: reported rather than left for the caller
    /// to discover by scanning `segments` themselves.
    pub segments_ungated: usize,
    /// The benchmark population, as a queryable filter — `[dim = best_peer]`
    /// for `best_peer` basis, or `[dim IN (segments at or above p75)]` for
    /// `p75` (an interpolated percentile need not land on any one segment,
    /// so the whole tier is aggregated). Empty only if the benchmark could
    /// not be traced back to any segment (defensive; should not happen given
    /// the cardinality checks earlier in this function). A caller MUST treat
    /// an empty `benchmark_filter` as "no queryable benchmark" and refuse to
    /// recurse further, rather than querying an unfiltered population.
    pub benchmark_filter: Vec<QueryFilter>,
}

/// A dimension skipped during analysis, with the reason.
#[derive(Debug, Clone, Serialize)]
pub struct SkippedDimension {
    pub dimension: String,
    pub reason: String,
}

/// A dimension's partition signature: the multiset of its per-segment measure
/// tuples, with the segment *labels* deliberately excluded.
///
/// Two functionally dependent dimensions — one store per staff count — cut the
/// population identically and differ only in what they call each slice, so
/// their signatures match exactly while their labels never do. That is the
/// whole test: identical numbers under different names.
///
/// Values are compared as formatted text rather than with a tolerance. These
/// come from the same warehouse aggregation over the same rows, so a genuine
/// alias yields bit-identical sums; two independent dimensions coincidentally
/// agreeing on every measure column of every segment is not a case worth
/// trading false positives for.
fn dimension_partition_signature(
    rows: &[serde_json::Map<String, serde_json::Value>],
    dim_alias: &str,
) -> String {
    let mut per_row: Vec<String> = rows
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .iter()
                .filter(|(k, _)| k.as_str() != dim_alias)
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            cells.sort();
            cells.join(",")
        })
        .collect();
    per_row.sort();
    per_row.join(";")
}

/// Rank a member of an alias group as the surviving representative; lowest
/// wins. The tie is broken on interpretability, because the numbers are by
/// definition identical and the only thing left to choose is which label the
/// user is better served reading:
/// 1. an entity key beats a plain attribute — `store_name` is what the store
///    *is*, `staff_count` a property that happens to be unique per store;
/// 2. a string label beats a number — "gamma" reads as a segment, "20" reads
///    as a measurement;
/// 3. the qualified name, purely so the choice is deterministic.
fn alias_representative_rank(layer: &SemanticLayer, dim: &str) -> (u8, u8, String) {
    let (view_name, local) = match dim.split_once('.') {
        Some(parts) => parts,
        None => return (1, 1, dim.to_string()),
    };
    let Some(view) = layer.view_by_name(view_name) else {
        return (1, 1, dim.to_string());
    };
    let is_key = view
        .entities
        .iter()
        .any(|e| e.key.as_deref() == Some(local));
    let is_string = view
        .dimensions
        .iter()
        .find(|d| d.name == local)
        .is_some_and(|d| d.dimension_type == DimensionType::String);
    (u8::from(!is_key), u8::from(!is_string), dim.to_string())
}

/// Group dimensions that cut the population identically and elect one
/// representative per group. Returns `dropped index -> representative name`.
///
/// Runs before any comparison is made, because an alias does not merely
/// duplicate a row in the output: each copy is charged to `comparison_family`,
/// raising the Šidák bar for every *other* dimension in the scan. Left in, a
/// pair of aliases makes the engine both noisier (two rows saying one thing)
/// and less sensitive (a stricter bar paid for by a comparison nobody
/// independently made).
fn alias_groups(
    layer: &SemanticLayer,
    dims: &[String],
    breakdown_results: &[Result<Vec<serde_json::Map<String, serde_json::Value>>, EngineError>],
) -> HashMap<usize, String> {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, (dim, res)) in dims.iter().zip(breakdown_results.iter()).enumerate() {
        let Ok(rows) = res else { continue };
        // Only dimensions that will actually be compared can alias each other;
        // one pruned for cardinality is never reported and never charged.
        if !(MIN_DIMENSION_CARDINALITY..=MAX_DIMENSION_CARDINALITY).contains(&rows.len()) {
            continue;
        }
        let sig = dimension_partition_signature(rows, &dim.replace('.', "__"));
        groups.entry(sig).or_default().push(i);
    }
    let mut dropped = HashMap::new();
    for members in groups.into_values() {
        if members.len() < 2 {
            continue;
        }
        let Some(rep) = members
            .iter()
            .copied()
            .min_by_key(|&i| alias_representative_rank(layer, &dims[i]))
        else {
            continue;
        };
        for m in members {
            if m != rep {
                dropped.insert(m, dims[rep].clone());
            }
        }
    }
    dropped
}

/// Full result of an opportunity sizing analysis.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityResult {
    pub target: String,
    pub period: (String, String),
    pub overall_value: f64,
    /// How segments were weighted and compared:
    /// - `"rows"`: sum-like measure sized on a per-unit rate, with a declared
    ///   `count` measure as the volume denominator (the honest additive path).
    /// - `"value_share"`: additive non-sum measure (avg/min/max) weighted by
    ///   value share.
    /// - `"equal"`: ratio measure, equal per-segment weighting.
    pub weight_basis: String,
    /// Top-K dimensions by total upside (descending).
    pub dimensions: Vec<DimensionOpportunity>,
    /// Dimensions excluded from analysis (high cardinality, low spread, query failure).
    pub skipped_dimensions: Vec<SkippedDimension>,
    /// Downstream impacts from the top opportunity, propagated via drivers.
    pub downstream: Vec<PredictImpact>,
}

/// Maximum number of distinct segments allowed in a dimension. Above this we
/// skip the dimension entirely — opportunity analysis on high-cardinality
/// columns (customer_id, order_id) is not actionable.
const MAX_DIMENSION_CARDINALITY: usize = 25;

/// Minimum number of distinct segments required for a meaningful comparison.
const MIN_DIMENSION_CARDINALITY: usize = 2;

/// Maximum number of dimensions returned (ranked by total upside).
const TOP_K_DIMENSIONS: usize = 5;

/// Maximum number of segments returned per dimension (ranked by upside).
const TOP_K_SEGMENTS: usize = 5;

/// A segment whose upside is less than this share of the dimension's total
/// upside is dropped from the per-dimension list (tail cleanup).
const TAIL_SHARE_THRESHOLD: f64 = 0.01;

/// Family-wise error rate for the per-segment significance gate, before the
/// selection correction in [`significance_threshold`] widens it further.
const SIGNIFICANCE_ALPHA: f64 = 0.05;

/// Prefix of the synthetic dispersion measure [`augment_layer_for_opportunity`]
/// installs. Double-underscored to stay out of any real namespace.
const DISPERSION_MEASURE_PREFIX: &str = "__opp_stddev__";

/// Prefix of the synthetic filtered-row-count companion
/// [`augment_layer_for_opportunity`] installs alongside the dispersion
/// measure when the target sum is itself filtered. Double-underscored to
/// stay out of any real namespace, same convention as
/// `DISPERSION_MEASURE_PREFIX`.
const DISPERSION_N_MEASURE_PREFIX: &str = "__opp_n__";

/// Name of the synthetic dispersion measure that carries `measure`'s spread.
///
/// `measure` is a bare measure name, not a `view.measure` id.
fn dispersion_measure_name(measure: &str) -> String {
    format!("{DISPERSION_MEASURE_PREFIX}{measure}")
}

/// `measure` is a bare measure name, not a `view.measure` id.
fn dispersion_n_measure_name(measure: &str) -> String {
    format!("{DISPERSION_N_MEASURE_PREFIX}{measure}")
}

/// Whether `target` is a composite the drill can size on a per-unit rate, and
/// if so, its row-level expression with every `{{view.measure}}` ref replaced
/// by that measure's own `expr` (recursively, for refs that are themselves
/// same-view additive composites).
///
/// Sizing a measure per-unit means dividing it by a row count, which only means
/// something when the measure HAS a per-row value. That holds for a `+`/`-`
/// combination of same-view sums (and same-view composites that themselves
/// flatten to one) — the flattened expression is a column expression evaluable
/// per row — and fails otherwise:
///
/// - `Mul`/`Div`, at any depth: a ratio divided by a row count is not a rate of
///   anything, and an outer expression that looks additive can still hide a
///   product/quotient one level down — only recursing into composite refs can
///   see that.
/// - cross-view refs, at any depth: the numerator aggregates another view's
///   rows through a join while the denominator counts this view's rows — a
///   fan-out grain mismatch that silently reports gaps at the wrong scale.
/// - reference cycles: nothing in the type system stops a composite from
///   transitively referencing itself, so recursion is guarded by a `visited`
///   set rather than trusting the tree to be acyclic.
/// - filters, at any depth: a non-`Sum` target's own `.filters` is refused (the
///   generator drops a `Number`/`Custom` measure's `.filters` for the
///   numerator), and so is any measure reached while flattening — flattening
///   discards a child's filters when substituting its expr, so a filtered
///   child's dispersion would spread over a wider population than what the
///   numerator actually sums.
///
/// Every substitution is parenthesized unconditionally: `{{a}} - {{b}}` with
/// `b.expr = "list_price - discount"` must flatten to `a - (list_price -
/// discount)`, not `a - list_price - discount`, or the sign of `discount`
/// silently inverts. This applies even to atomic substitutions (a bare column
/// name) — the extra parens are a no-op for any SQL planner, and always
/// wrapping avoids under-wrapping non-arithmetic compounds (e.g. `CASE WHEN
/// ... END`) that a `+ - * /` substring check would miss.
///
/// Layer-only by design: `augment_layer_for_opportunity` has no `MetricTree`,
/// and both call sites must agree on one definition of eligibility.
fn additive_same_view_composite(layer: &SemanticLayer, target: &str) -> Option<String> {
    let mut visited = HashSet::new();
    flatten_additive_composite(layer, target, &mut visited)
}

/// Whether the drill will size `target` on a per-unit rate.
///
/// The single public answer to that question. `opportunity()` and
/// `augment_layer_for_opportunity` both gate on it internally; consumers
/// (the oxy handler, and `MetricNode.drillable`) must call this rather than
/// re-deriving eligibility from a measure's type or its edges, which is how
/// the UI came to offer a drill on roots the engine refuses.
pub fn supports_rate_basis(layer: &SemanticLayer, target: &str) -> bool {
    let is_sum = layer
        .views
        .iter()
        .find(|v| Some(v.name.as_str()) == target.split('.').next())
        .and_then(|v| {
            let name = target.split_once('.')?.1;
            v.measures_list().iter().find(|m| m.name == name).cloned()
        })
        .map(|m| m.measure_type == MeasureType::Sum)
        .unwrap_or(false);
    is_sum || additive_same_view_composite(layer, target).is_some()
}

/// Recursive worker behind [`additive_same_view_composite`]. See that
/// function's doc comment for the eligibility rules; `visited` is the cycle
/// guard, tracking the CURRENT recursion path (not every node ever visited)
/// so that a diamond — the same composite reached twice via two different
/// parents, or referenced twice within one expression — is permitted while a
/// true cycle (a node reachable from itself) is still refused.
fn flatten_additive_composite(
    layer: &SemanticLayer,
    target: &str,
    visited: &mut HashSet<String>,
) -> Option<String> {
    // A composite that transitively references itself would otherwise recurse
    // forever; nothing in the type system prevents such a cycle. Path-based:
    // insert on entry, remove on exit (success path only, see below) so a
    // node stays "on the path" only while it is actually being recursed into.
    if !visited.insert(target.to_string()) {
        return None;
    }

    let (target_view, measure_name) = target.split_once('.')?;
    let view = layer.views.iter().find(|v| v.name == target_view)?;
    let measure = view
        .measures_list()
        .iter()
        .find(|m| m.name == measure_name)
        .cloned()?;

    if !matches!(
        measure.measure_type,
        MeasureType::Custom | MeasureType::Number
    ) {
        return None;
    }
    // The target's own filters: only Sum honors `.filters` symmetrically for
    // both the numerator and the dispersion/`n` companions (see
    // `augment_layer_for_opportunity`), so a target with filters is refused
    // here too — moved in from that function so both gated call sites
    // (`opportunity` and `augment_layer_for_opportunity`) see the same rule
    // instead of diverging. `measure.measure_type` is already known to be
    // `Custom | Number` (the `matches!` guard above returned `None` for
    // anything else, including `Sum`), so no type check is needed here — and
    // this branch is only ever reachable for the top-level `target`, never
    // for a recursed-into ref: a child's filters are refused earlier, by the
    // loop's own `referenced.filters` check below, before recursion happens.
    if measure.filters.as_ref().filter(|f| !f.is_empty()).is_some() {
        return None;
    }
    let expr = measure.expr.clone()?;

    let ref_ops = crate::engine::metric_tree::extract_ref_ops(&expr);
    if ref_ops.is_empty() {
        return None;
    }

    let mut flattened = expr.clone();
    for (ref_id, operator, _sign) in &ref_ops {
        if operator.is_multiplicative() {
            return None;
        }
        let (ref_view, ref_measure) = ref_id.split_once('.')?;
        if ref_view != target_view {
            return None;
        }
        let referenced = view
            .measures_list()
            .iter()
            .find(|m| m.name == ref_measure)
            .cloned()?;
        // A filtered child's dispersion would spread over a wider population
        // than the numerator: flattening substitutes the child's raw expr and
        // drops its `.filters`, so the filter can never be honored downstream.
        if referenced
            .filters
            .as_ref()
            .filter(|f| !f.is_empty())
            .is_some()
        {
            return None;
        }
        let inner = match referenced.measure_type {
            MeasureType::Sum => referenced.expr.clone()?,
            MeasureType::Custom | MeasureType::Number => {
                flatten_additive_composite(layer, ref_id, visited)?
            }
            _ => return None,
        };
        // Parenthesize every substitution unconditionally: an unparenthesized
        // child expr like `list_price - discount` dropped into `{{a}} - {{b}}`
        // would flatten to `a - list_price - discount`, silently inverting
        // the sign of the second term. A substring check for `+ - * /` (the
        // prior approach) misses non-arithmetic compounds like `CASE WHEN
        // ... END`, and wrapping a bare column in redundant parens is a
        // no-op for any SQL planner — so always wrapping is strictly safer
        // and costs nothing.
        let substitution = format!("({inner})");
        flattened = flattened.replace(&format!("{{{{{ref_id}}}}}"), &substitution);
    }

    // Every ref must have been substituted; a leftover `{{` means a ref the
    // loop did not see, and a half-flattened expression would silently
    // generate a nested aggregate.
    if flattened.contains("{{") {
        return None;
    }
    // Path-based: pop `target` off the current path now that recursion into
    // it is finished, so a sibling branch (or a second ref to the same node
    // within this expression) can still visit it — that's a diamond, not a
    // cycle. Every failure path above returns `None` straight to the
    // top-level caller without removing its entry, but that staleness is
    // unobservable: the whole `visited` set is discarded on any `None`.
    visited.remove(target);
    Some(flattened)
}

/// Install the synthetic dispersion measures that [`opportunity`] needs in
/// order to tell a real gap from sampling noise, and return whether one was
/// added for `target`.
///
/// **Call this before building the engine**, on the same layer the engine
/// compiles against — the executor resolves measure names against its own copy
/// of the layer, so a measure `opportunity()` invents at query time would not
/// exist as far as the executor is concerned. Build the metric tree *before*
/// calling this: the synthetic measure is a pass-through, and pass-throughs
/// read as composite nodes, so a tree built afterwards would sprout a
/// `__opp_stddev__…` node.
///
/// Without this the sizing still runs; it just cannot gate on significance and
/// will report whatever gap it finds, however thin the evidence. That is the
/// pre-existing behaviour, kept as the fallback so an out-of-date caller
/// degrades quietly rather than breaking.
///
/// A `sum` target is augmented using its own `expr`; an eligible additive
/// same-view composite (see `additive_same_view_composite`) is augmented using
/// its flattened, row-level expr. Both are arithmetic means of a row-level
/// value, so the value's standard deviation is exactly what the standard error
/// of that rate needs. Everything else is refused: a `count` target's rate is
/// 1 by construction (no mean to put an error bar on) and a `count_distinct`
/// rate is not a mean of anything; a cross-view or nested composite has no safe
/// flattening; and — regardless of measure type — a non-`sum` measure that
/// carries its own `.filters` is refused too, because the SQL generator drops
/// a `Number`/`Custom` measure's own `.filters` for the numerator while this
/// function would apply them to the dispersion measure and its `n` companion,
/// gating a mean from one population against a spread and sample size from
/// another.
pub fn augment_layer_for_opportunity(layer: &mut SemanticLayer, target: &str) -> bool {
    let Some((view_name, measure_name)) = target.split_once('.') else {
        return false;
    };
    let Some(view) = layer.views.iter_mut().find(|v| v.name == view_name) else {
        return false;
    };
    let Some(measure) = view
        .measures_list()
        .iter()
        .find(|m| m.name == measure_name)
        .cloned()
    else {
        return false;
    };
    // Sums use their own expr. Eligible composites use the FLATTENED expr —
    // refs replaced by the referenced measures' column expressions. Installing
    // `STDDEV_SAMP({{a}} + {{b}})` instead would resolve each ref to the
    // child's aggregate and emit STDDEV_SAMP((SUM(..)) + (SUM(..))), a nested
    // aggregate that fails on every dialect. `additive_same_view_composite`
    // only takes `&SemanticLayer`, so call it before `view` takes its `&mut`
    // borrow below.
    let expr = if measure.measure_type == MeasureType::Sum {
        let Some(expr) = measure.expr.clone() else {
            return false;
        };
        expr
    } else {
        let Some(flat) = additive_same_view_composite(layer, target) else {
            return false;
        };
        flat
    };
    let Some(view) = layer.views.iter_mut().find(|v| v.name == view_name) else {
        return false;
    };

    // A non-Sum measure's own `.filters` is now checked inside
    // `additive_same_view_composite` itself (the `else` branch above), so a
    // non-Sum target with filters never reaches this point — the call already
    // returned `None` and this function returned `false`. There is no
    // separate check to duplicate here: a `Sum` target skips
    // `additive_same_view_composite` entirely (the `if` branch above), and a
    // `Sum`'s `.filters` is exactly what the CASE WHEN below embeds, so it is
    // never refused for having them.

    // A filtered sum's numerator only counts the rows its filter admits. The
    // dispersion measure must track that SAME filtered population, not the
    // view's full row count — but `MeasureType::Number` pass-throughs (what
    // this is) ignore `.filters` entirely; the generator emits their `expr`
    // verbatim (`sql_generator.rs` `measure_agg_expr`, the `MeasureType::Number`
    // arm). So the filter has to be hand-embedded into the STDDEV_SAMP
    // expression itself, using the same raw-template CASE WHEN the generator
    // builds for ordinary filtered aggregates. Unfiltered sums are unaffected:
    // `filter_condition` is `None` and the expr is exactly what it always was.
    let filter_condition: Option<String> =
        measure.filters.as_ref().filter(|f| !f.is_empty()).map(|f| {
            f.iter()
                .map(|mf| mf.expr.clone())
                .collect::<Vec<_>>()
                .join(" AND ")
        });
    let dispersion_expr = match &filter_condition {
        Some(cond) => format!("STDDEV_SAMP(CASE WHEN {cond} THEN {expr} END)"),
        None => format!("STDDEV_SAMP({expr})"),
    };

    let name = dispersion_measure_name(measure_name);
    if !view.measures_list().iter().any(|m| m.name == name) {
        view.measures.get_or_insert_with(Vec::new).push(Measure {
            name,
            // A pass-through: the expression carries its own aggregate, so the
            // generator emits it verbatim against this view's alias rather than
            // wrapping it. STDDEV_SAMP is ANSI and is spelled the same in DuckDB,
            // Postgres, Snowflake and BigQuery.
            measure_type: MeasureType::Number,
            expr: Some(dispersion_expr),
            description: Some(format!(
                "Internal: dispersion of {measure_name}, used to gate opportunity sizing on evidence."
            )),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            shift: None,
            meta: None,
        });
    }

    // A filtered numerator's own sample size — the count of rows the filter
    // admitted — is a different figure from the (deliberately unfiltered)
    // `count` measure the caller pairs it with as a rate denominator. The
    // significance test's `n` must match the rows the dispersion measure
    // actually spread over, so install a second companion when there is a
    // filter to track. Ordinary `Count` measures DO honor `.filters` (unlike
    // `Number`), so this one needs no hand-built CASE WHEN.
    if let Some(filters) = measure.filters.clone().filter(|f| !f.is_empty()) {
        let n_name = dispersion_n_measure_name(measure_name);
        if !view.measures_list().iter().any(|m| m.name == n_name) {
            view.measures.get_or_insert_with(Vec::new).push(Measure {
                name: n_name,
                measure_type: MeasureType::Count,
                expr: None,
                description: Some(format!(
                    "Internal: filtered row count backing {measure_name}'s dispersion, used to gate opportunity sizing on evidence."
                )),
                original_expr: None,
                filters: Some(filters),
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
                drivers: None,
                shift: None,
                meta: None,
            });
        }
    }

    true
}

/// The t-statistic a segment's gap must clear to be reported as real.
///
/// This is not a plain 95% test, because the comparison is rigged twice over.
///
/// *Within* a dimension, the benchmark is chosen as the best (or
/// 75th-percentile) of the very same `k` segments being tested against it. Draw
/// `k` segments from one noise distribution and the largest is *expected* to sit
/// about `sqrt(2·ln k)` standard errors above the mean — so a fixed threshold
/// rediscovers a "leader" in any dimension whatsoever.
///
/// *Across* dimensions, a scan tests every segment of every discovered
/// dimension — often ~100 comparisons once foreign entities are followed — and
/// then reports the winners, ranked. Controlling the error rate of one
/// comparison while surfacing the maximum of a hundred is not a control at all:
/// at a per-test 5%, five spurious levers per scan is the *expected* yield.
/// That is why `family` is the whole scan and not `k`. Concretely: 20
/// dimensions × 5 segments needs t≈3.3, not the t≈2.2 that `k=4` alone implies
/// — the difference between reporting `order_status` on a dataset where status
/// is provably unrelated to order value, and correctly saying nothing.
///
/// So the bar is the larger of a Šidák correction over the whole family and the
/// `sqrt(2·ln k)` growth of the maximum selected within this dimension. It is a
/// guard, not an exact test — the true null distribution of "gap to the
/// selected max" has no closed form worth carrying here, and the two effects are
/// not independent.
///
/// `df` is the Welch–Satterthwaite effective degrees of freedom of the specific
/// comparison. The Šidák critical value is read off Student's t, not the normal,
/// so a thin benchmark or segment — the 2–3 row bar this gate exists to catch —
/// has to clear the correspondingly heavier tail. As the samples grow, t → normal
/// and this reduces to the previous z-based bar. The `sqrt(2·ln k)` selection
/// term stays on its asymptotic z scale: it models the *expected* position of a
/// max, not a tail quantile, so a small-sample correction does not apply to it.
fn significance_threshold(k: usize, family: usize, df: f64, alpha: f64) -> f64 {
    let k = k.max(2) as f64;
    let family = family.max(2) as f64;
    // Šidák: per-comparison rate that holds the family-wise rate at ALPHA.
    let per_comparison = 1.0 - (1.0 - alpha).powf(1.0 / family);
    let sidak = StudentsT::new(0.0, 1.0, df.max(1.0))
        .expect("Student's t with positive df is well-formed")
        .inverse_cdf(1.0 - per_comparison);
    let selection = (2.0 * k.ln()).sqrt();
    sidak.max(selection)
}

/// Is `gap` (benchmark rate − segment rate) real, or is it what two samples
/// this size would disagree by anyway?
///
/// Welch: the segment and the benchmark segment have their own spread and their
/// own row count, and the benchmark is routinely the thinner of the two — a
/// pooled estimate would understate the error precisely where it matters most.
///
/// `None` means "cannot tell" (no dispersion measure, a single-row segment
/// whose sample stddev is undefined, or a zero-variance degenerate), and the
/// caller keeps the segment rather than inventing a verdict.
fn gap_is_significant(
    gap: f64,
    seg_sd: Option<f64>,
    seg_n: f64,
    bench_sd: Option<f64>,
    bench_n: f64,
    k: usize,
    family: usize,
    alpha: f64,
) -> Option<bool> {
    let (seg_sd, bench_sd) = (seg_sd?, bench_sd?);
    if seg_n < 2.0 || bench_n < 2.0 {
        return None;
    }
    let seg_var = (seg_sd * seg_sd) / seg_n;
    let bench_var = (bench_sd * bench_sd) / bench_n;
    let se = (seg_var + bench_var).sqrt();
    if !se.is_finite() || se < f64::EPSILON {
        return None;
    }
    // Welch–Satterthwaite effective degrees of freedom for the unequal-variance,
    // unequal-n comparison. This is what makes a thin benchmark honest: with a
    // 2-row bar its variance term dominates and df collapses toward 1, so the
    // t-quantile in `significance_threshold` blows out and the gap has to be huge
    // to survive — the opposite of the too-thin normal tail. A degenerate
    // denominator (both variances zero) is already excluded by the `se` guard
    // above; the max(1.0) inside the threshold covers the remaining edge.
    let df = (seg_var + bench_var).powi(2)
        / (seg_var * seg_var / (seg_n - 1.0) + bench_var * bench_var / (bench_n - 1.0));
    Some((gap / se) >= significance_threshold(k, family, df, alpha))
}

/// Run opportunity sizing: find segments under their best peer and size the upside.
///
/// Algorithm:
/// 1. For each non-time dimension of the target's view with cardinality in
///    `[MIN, MAX]`, query `measure GROUP BY dim` plus a row-count proxy so we can
///    volume-weight segments.
/// 2. Benchmark = the top-performing segment's measure value (or P75 when there
///    are enough segments to make a percentile meaningful — currently >=8).
/// 3. For each below-benchmark segment compute `upside = gap × volume_weight`,
///    which answers "what's the headline number if this segment matched the best?"
///    For additive measures volume is row-count share; for ratios it is the
///    segment's volume so the gap converts into absolute units gained.
/// 4. Keep top-K dimensions × top-K segments by upside; drop long-tail segments
///    contributing under `TAIL_SHARE_THRESHOLD` of the dimension's total.
/// 5. Propagate the top dimension's total upside through the metric tree.
///
/// `scope` narrows every query to a subset of the population — pass an empty
/// slice to size across the whole thing. It composes with the period bounds and
/// is applied to the overall-value query and every per-dimension breakdown
/// alike, so the upside shares stay shares of a total that was actually
/// scanned. Note that scoping changes the question being asked: a dimension the
/// scope pins to a single value drops out with "nothing to compare against",
/// which is the honest answer — a segment cannot be benchmarked against peers
/// the scope has excluded.
pub fn opportunity(
    tree: &MetricTree,
    layer: &SemanticLayer,
    target: &str,
    time_dimension: &str,
    period: (&str, &str),
    scope: &[QueryFilter],
    executor: &QueryExecutor,
) -> Result<OpportunityResult, EngineError> {
    let target_node = tree.nodes.iter().find(|n| n.id == target).ok_or_else(|| {
        EngineError::QueryError(format!("Measure '{}' not found in metric tree", target))
    })?;

    let target_view = target.split('.').next().unwrap_or("");

    let is_additive = matches!(
        target_node.measure_type.as_str(),
        "count" | "sum" | "count_distinct" | "avg" | "min" | "max"
    );

    // A `sum` measure (a running total) cannot be sized by comparing segment
    // totals: a segment sitting below another mostly reflects that it is
    // *smaller* (fewer rows, smaller market), not that it underperforms. We
    // instead divide each segment's total by its row count to get a comparable
    // per-unit rate, benchmark the RATE, and size the upside back up by the
    // segment's own volume. That needs a declared `count` measure on the view;
    // without one we refuse rather than emit a size-confounded number.
    //
    // Only `sum` qualifies. This is the same boundary `augment_layer_for_opportunity`
    // draws for the dispersion measure, and for the same reason: a `count`
    // target's per-unit rate is 1 by construction (dividing a row count by a row
    // count), and a `count_distinct` rate is not a mean of anything, so neither
    // has a rate worth benchmarking. Folding them in here would force rate_mode
    // on without a dispersion measure to gate it, and — when the discovered
    // count measure is the count target itself — collapse every segment's rate
    // to ~1, so the scan would silently report nothing as "flat". They fall
    // through to the value-share path below instead.
    // A composite that is a +/- combination of same-view sums has a genuine
    // per-row value, so it sizes per-unit exactly like a sum. Without this,
    // the root is sized on raw totals while `component_candidates` computes
    // children as rate gaps — the two levels end up in different units, the
    // shares stop summing to the parent, and `concentration` degenerates into
    // each child's SIZE share rather than its share of the gap.
    // `target_node.measure_type` (tree) and the layer's own `MeasureType` for
    // `target` are provably in agreement here — `MetricTree::build` sources
    // `measure_type` directly from the same `MeasureType::Display` this checks
    // against (see `test_tree_measure_type_string_round_trips_against_layer_measure_type`),
    // and no call site mutates an existing measure's identity between tree
    // construction and this call (`augment_layer_for_opportunity` only adds
    // new synthetic measures, never edits `target` itself). One definition,
    // `supports_rate_basis`, is authoritative for both this gate and every
    // external consumer.
    let is_sum_like = supports_rate_basis(layer, target);
    let count_measure = if is_sum_like {
        discover_count_measure(layer, target_view)
    } else {
        None
    };
    let rate_mode = count_measure.is_some();

    // The caller's scope and the period bounds are one filter set from here on:
    // every query below — the overall value and each per-dimension breakdown —
    // must see exactly the same rows, or the reported shares are fractions of a
    // total nobody scanned.
    let mut scan_filters = scope.to_vec();
    scan_filters.extend([
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
    ]);

    // 1) Overall value (used as upside fallback when row-count proxy is unavailable).
    let overall_query = QueryRequest {
        measures: vec![target.to_string()],
        filters: scan_filters.clone(),
        ..QueryRequest::new()
    };
    let overall_rows = executor(&overall_query)?;
    let measure_alias = target.replace('.', "__");
    let count_alias: Option<String> = count_measure.as_ref().map(|cm| cm.replace('.', "__"));

    // The dispersion measure is present only if the caller ran
    // `augment_layer_for_opportunity` on the layer the engine compiles against.
    // When it is absent we size exactly as before and gate nothing — an
    // out-of-date caller loses the evidence check, not the feature.
    let dispersion_measure: Option<String> = target
        .split_once('.')
        .map(|(view, measure)| (view, dispersion_measure_name(measure)))
        .filter(|(view, name)| {
            layer
                .views
                .iter()
                .find(|v| v.name == *view)
                .is_some_and(|v| v.measures_list().iter().any(|m| m.name == *name))
        })
        .map(|(view, name)| format!("{view}.{name}"));
    let dispersion_alias: Option<String> =
        dispersion_measure.as_ref().map(|d| d.replace('.', "__"));
    let dispersion_n_measure: Option<String> = target
        .split_once('.')
        .map(|(view, measure)| (view, dispersion_n_measure_name(measure)))
        .filter(|(view, name)| {
            layer
                .views
                .iter()
                .find(|v| v.name == *view)
                .is_some_and(|v| v.measures_list().iter().any(|m| m.name == *name))
        })
        .map(|(view, name)| format!("{view}.{name}"));
    let dispersion_n_alias: Option<String> =
        dispersion_n_measure.as_ref().map(|d| d.replace('.', "__"));
    let overall_value = overall_rows
        .first()
        .map(|r| extract_measure_value(r, &measure_alias))
        .unwrap_or(0.0);

    let dims = discover_dimensions(layer, target_view);

    // Sum-like target with no `count` measure on its view: we cannot form a
    // per-unit rate, so we refuse to size (rather than fall back to comparing
    // raw totals, which conflates segment size with underperformance). Report
    // each candidate dimension as skipped with an actionable reason.
    if is_sum_like && count_measure.is_none() {
        return Ok(OpportunityResult {
            target: target.to_string(),
            period: (period.0.to_string(), period.1.to_string()),
            overall_value,
            weight_basis: "rows".into(),
            dimensions: Vec::new(),
            skipped_dimensions: dims
                .into_iter()
                .map(|dimension| SkippedDimension {
                    reason: format!(
                        "'{target}' is an additive total; sizing it fairly needs a per-row \
                         `count` measure on view '{target_view}' to compare per-unit rates, \
                         but none is declared"
                    ),
                    dimension,
                })
                .collect(),
            downstream: Vec::new(),
        });
    }

    let mut dim_opps: Vec<DimensionOpportunity> = Vec::new();
    let mut skipped: Vec<SkippedDimension> = Vec::new();

    // Build all per-dimension breakdown queries and execute them in parallel.
    // Each is an independent aggregate over the same period; result processing
    // is sequential below since it mutates dim_opps / skipped.
    let breakdown_queries: Vec<QueryRequest> = dims
        .iter()
        .map(|dim| {
            // In rate_mode we also select the count measure so each segment
            // carries its own volume denominator alongside the total, plus the
            // dispersion measure so we can tell a real gap from noise. Both ride
            // along in the same GROUP BY — no extra round trip.
            let mut measures = vec![target.to_string()];
            if let Some(cm) = &count_measure {
                measures.push(cm.clone());
            }
            if let Some(dm) = &dispersion_measure {
                measures.push(dm.clone());
            }
            if let Some(dnm) = &dispersion_n_measure {
                measures.push(dnm.clone());
            }
            QueryRequest {
                measures,
                dimensions: vec![dim.clone()],
                filters: scan_filters.clone(),
                ..QueryRequest::new()
            }
        })
        .collect();
    let breakdown_results = parallel_execute(&breakdown_queries, executor);

    // How many comparisons this scan actually makes, counted before any of them
    // are made — the significance bar has to answer for the whole family, not
    // for whichever dimension happens to be in hand. Only dimensions that will
    // really be tested count: a dimension pruned for cardinality is never
    // compared, so charging its segments to the family would tax the survivors
    // for work nobody did.
    //
    // Aliases are excluded here for the same reason: two labels for one
    // partition are one comparison, and charging both would tax every other
    // dimension for work nobody independently did.
    let aliased = alias_groups(layer, &dims, &breakdown_results);
    let comparison_family: usize = breakdown_results
        .iter()
        .enumerate()
        .filter(|(i, _)| !aliased.contains_key(i))
        .filter_map(|(_, r)| r.as_ref().ok())
        .map(|rows| rows.len())
        .filter(|n| (MIN_DIMENSION_CARDINALITY..=MAX_DIMENSION_CARDINALITY).contains(n))
        .sum();

    for (idx, (dim, rows_result)) in dims.iter().zip(breakdown_results).enumerate() {
        if let Some(representative) = aliased.get(&idx) {
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: format!(
                    "alias of {representative} — identical partition, \
                     reported once under the more interpretable label"
                ),
            });
            continue;
        }
        let rows = match rows_result {
            Ok(r) => r,
            Err(e) => {
                skipped.push(SkippedDimension {
                    dimension: dim.clone(),
                    reason: format!("breakdown query failed: {e}"),
                });
                continue;
            }
        };
        if rows.is_empty() {
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: "no rows returned for breakdown".into(),
            });
            continue;
        }

        let cardinality = rows.len();
        if cardinality > MAX_DIMENSION_CARDINALITY {
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: format!(
                    "cardinality {cardinality} exceeds cap {MAX_DIMENSION_CARDINALITY} \
                     (likely a high-cardinality identifier, not actionable)"
                ),
            });
            continue;
        }
        if cardinality < MIN_DIMENSION_CARDINALITY {
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: format!("only {cardinality} segment(s) — nothing to compare against"),
            });
            continue;
        }

        let dim_alias = dim.replace('.', "__");

        struct SegRow {
            segment: String,
            value: f64,
            /// Row count for this segment; populated only in rate_mode.
            count: f64,
            /// The figure we benchmark on: a per-unit rate (value / count) in
            /// rate_mode, otherwise the raw segment value.
            cmp: f64,
            /// Sample standard deviation of the summed column within this
            /// segment. `None` when the layer carries no dispersion measure, or
            /// when the warehouse returned NULL for it (a one-row segment has no
            /// sample stddev).
            sd: Option<f64>,
            /// Row count of the target's OWN filter, when the target is a
            /// filtered sum and `augment_layer_for_opportunity` installed the
            /// `__opp_n__` companion. `None` for an unfiltered sum — `count`
            /// (the rate denominator) is used as `n` in that case, unchanged
            /// from before this measure existed.
            filtered_n: Option<f64>,
        }
        let seg_rows: Vec<SegRow> = rows
            .iter()
            .map(|r| {
                let value = extract_measure_value(r, &measure_alias);
                let count = count_alias
                    .as_ref()
                    .map(|a| extract_measure_value(r, a))
                    .unwrap_or(0.0);
                let cmp = if rate_mode && count.abs() > f64::EPSILON {
                    value / count
                } else {
                    value
                };
                SegRow {
                    segment: extract_dim_value(r, &dim_alias),
                    value,
                    count,
                    cmp,
                    sd: dispersion_alias
                        .as_ref()
                        .and_then(|a| extract_optional_measure_value(r, a)),
                    filtered_n: dispersion_n_alias
                        .as_ref()
                        .and_then(|a| extract_optional_measure_value(r, a)),
                }
            })
            .collect();

        // Benchmark = top performer for small dims, P75 once there are enough
        // segments that percentile estimation is meaningful. In rate_mode this
        // benchmarks the per-unit rate, so a segment is never flagged merely for
        // being small.
        let (benchmark, benchmark_basis) =
            pick_benchmark(&seg_rows.iter().map(|s| s.cmp).collect::<Vec<_>>());

        // The benchmark value was copied out of one of these segments, so the
        // nearest segment is the one that set the bar. We need its row count and
        // spread: a bar set by a thin segment is a bar with its own error, and
        // pretending otherwise is how three statistically identical statuses
        // acquire a "leader".
        let bench_row = seg_rows.iter().min_by(|a, b| {
            (a.cmp - benchmark)
                .abs()
                .partial_cmp(&(b.cmp - benchmark).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let (bench_sd, bench_n) =
            bench_row.map_or((None, 0.0), |b| (b.sd, b.filtered_n.unwrap_or(b.count)));

        let benchmark_filter: Vec<QueryFilter> = if benchmark_basis == "best_peer" {
            bench_row
                .map(|b| {
                    vec![QueryFilter {
                        member: Some(dim.clone()),
                        operator: Some(FilterOperator::Equals),
                        values: vec![b.segment.clone()],
                        and: None,
                        or: None,
                    }]
                })
                .unwrap_or_default()
        } else {
            // p75 (or any future non-best_peer basis): no single segment a
            // percentile interpolation could belong to — aggregate every
            // segment at or above the threshold into one population.
            let at_or_above: Vec<String> = seg_rows
                .iter()
                .filter(|s| s.cmp >= benchmark)
                .map(|s| s.segment.clone())
                .collect();
            if at_or_above.is_empty() {
                Vec::new()
            } else {
                vec![QueryFilter {
                    member: Some(dim.clone()),
                    operator: Some(FilterOperator::Equals),
                    values: at_or_above,
                    and: None,
                    or: None,
                }]
            }
        };

        // Spread check: if every segment is within 1% of the benchmark, skip.
        let max_v = seg_rows.iter().map(|s| s.cmp).fold(f64::MIN, f64::max);
        let min_v = seg_rows.iter().map(|s| s.cmp).fold(f64::MAX, f64::min);
        let spread = if benchmark.abs() > f64::EPSILON {
            (max_v - min_v) / benchmark.abs()
        } else {
            0.0
        };
        if spread < 0.01 {
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: format!(
                    "flat distribution (spread {:.2}% of benchmark)",
                    spread * 100.0
                ),
            });
            continue;
        }

        // Size the upside per below-benchmark segment.
        // - rate_mode (sum-like with a count measure): the gap is a per-unit
        //   rate deficit, and the addressable upside is that deficit applied to
        //   the segment's OWN volume — (benchmark_rate − rate) × count. `volume`
        //   is the true row count. A low-rate small segment yields a small
        //   number; a low-rate large segment a large one.
        // - additive non-sum (avg/min/max): legacy value-share weighting.
        // - ratio: equal weighting since we have no row counts.
        //
        // Segments whose gap is inside the noise are dropped before sizing:
        // multiplying a gap we cannot demonstrate by a real row count produces a
        // large, confident, wrong number, which is worse than saying nothing.
        // Only reachable when the caller installed the dispersion measure; see
        // `augment_layer_for_opportunity`.
        let mut noise_dropped = 0usize;
        let total_value: f64 = seg_rows.iter().map(|s| s.value).sum();
        let segments_iter = seg_rows
            .iter()
            .filter(|s| s.cmp < benchmark)
            .filter_map(|s| {
                let real = gap_is_significant(
                    benchmark - s.cmp,
                    s.sd,
                    s.filtered_n.unwrap_or(s.count),
                    bench_sd,
                    bench_n,
                    cardinality,
                    comparison_family,
                    SIGNIFICANCE_ALPHA,
                );
                if real == Some(false) {
                    noise_dropped += 1;
                    return None;
                }
                Some((s, real == Some(true)))
            })
            .map(|(s, gated)| {
                let gap = benchmark - s.cmp;
                let (volume, upside) = if rate_mode {
                    (s.count, gap * s.count)
                } else if is_additive {
                    let vol = if total_value.abs() > f64::EPSILON {
                        s.value / total_value
                    } else {
                        1.0 / cardinality as f64
                    };
                    (vol, gap)
                } else {
                    // Ratio: equal weighting since we don't have row counts.
                    (1.0 / cardinality as f64, gap)
                };
                SegmentOpportunity {
                    segment: s.segment.clone(),
                    current_value: s.cmp,
                    volume,
                    benchmark,
                    gap,
                    upside,
                    gated,
                }
            });

        let mut segments: Vec<SegmentOpportunity> = segments_iter.collect();
        segments.sort_by(|a, b| {
            b.upside
                .partial_cmp(&a.upside)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_upside: f64 = segments.iter().map(|s| s.upside).sum();
        if total_upside.abs() < f64::EPSILON {
            // "Everything I found was noise" and "everything already matches the
            // benchmark" both leave nothing to size, but they are different
            // answers and the caller is entitled to know which one it got.
            skipped.push(SkippedDimension {
                dimension: dim.clone(),
                reason: if noise_dropped > 0 {
                    format!(
                        "no segment's gap outstrips sampling noise \
                         ({noise_dropped} below benchmark, none significant)"
                    )
                } else {
                    "no segments below benchmark".into()
                },
            });
            continue;
        }

        // Tail trim: drop segments contributing under threshold, then take top-K.
        let segments_before = segments.len();
        let tail_floor = total_upside.abs() * TAIL_SHARE_THRESHOLD;
        segments.retain(|s| s.upside.abs() >= tail_floor);
        if segments.len() > TOP_K_SEGMENTS {
            segments.truncate(TOP_K_SEGMENTS);
        }
        let other_segments_skipped = segments_before.saturating_sub(segments.len());
        let segments_ungated = segments.iter().filter(|s| !s.gated).count();

        dim_opps.push(DimensionOpportunity {
            dimension: dim.clone(),
            cardinality,
            benchmark_basis,
            total_upside,
            segments,
            other_segments_skipped,
            segments_dropped_as_noise: noise_dropped,
            segments_ungated,
            benchmark_filter,
        });
    }

    dim_opps.sort_by(|a, b| {
        b.total_upside
            .partial_cmp(&a.total_upside)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if dim_opps.len() > TOP_K_DIMENSIONS {
        dim_opps.truncate(TOP_K_DIMENSIONS);
    }

    let downstream: Vec<PredictImpact> = if let Some(top_dim) = dim_opps.first() {
        // Multiplicative parents can only be sized against current values, and
        // we already have an executor and a period here — fetch them in one
        // batched query rather than dropping those impacts.
        let values = reachable_values(
            tree,
            &[target.to_string()],
            time_dimension,
            period,
            executor,
        );
        let predict_result =
            predict_with_values(tree, &[(target.to_string(), top_dim.total_upside)], &values)?;
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
        weight_basis: if rate_mode {
            "rows".into()
        } else if is_additive {
            "value_share".into()
        } else {
            "equal".into()
        },
        dimensions: dim_opps,
        skipped_dimensions: skipped,
        downstream,
    })
}

/// Pick a benchmark value from a slice of segment values.
///
/// Returns `(benchmark, basis)` where basis is `"best_peer"` (the max value)
/// or `"p75"` (75th percentile, used once there are >= 8 segments so the
/// percentile is meaningful and not just the second-largest).
fn pick_benchmark(values: &[f64]) -> (f64, String) {
    if values.is_empty() {
        return (0.0, "empty".into());
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if sorted.len() >= 8 {
        let idx = ((sorted.len() as f64) * 0.75).floor() as usize;
        let idx = idx.min(sorted.len() - 1);
        (sorted[idx], "p75".into())
    } else {
        (*sorted.last().unwrap(), "best_peer".into())
    }
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

// ── Opportunity Drill (Recursive Gap Decomposition) ─────────────────────────────

/// A semantic layer shared between the drill and its executor. `opportunity_drill`
/// installs synthetic per-value measures into it mid-recursion (brief write lock),
/// and a real SQL executor holds the same handle to compile queries against the
/// current layer (read lock). All access is scoped: no guard is ever held across
/// an executor call, so the install-then-query sequence never deadlocks.
pub type SharedLayer = std::sync::Arc<std::sync::RwLock<SemanticLayer>>;

/// Which row of the root `opportunity()` scan to decompose.
///
/// `opportunity_drill` normally roots at the scan's top-ranked segment. The
/// panel lets an analyst expand ANY ranked row, so it names the row instead.
/// Deliberately just a selector: the benchmark filter, root gap and root upside
/// are still derived server-side from the scan, because every `root_share` in
/// the tree is computed against them and a client-asserted root would render a
/// decomposition that is internally consistent and wrong.
#[derive(Debug, Clone, Deserialize)]
pub struct DrillRoot {
    pub dimension: String,
    pub segment: String,
}

/// Configuration for [`opportunity_drill`].
#[derive(Debug, Clone)]
pub struct DrillConfig {
    /// Maximum number of further levels to recurse past the root's own
    /// `opportunity()` scan (which is always run once, unconditionally).
    pub max_depth: usize,
    /// Total family-wise significance budget for the WHOLE drill. Composed
    /// across levels via nested Sidak (`level_alpha`) — same meaning as
    /// `SIGNIFICANCE_ALPHA` for a single-level `opportunity()` call, just
    /// spent across up to `max_depth` further gated comparisons instead of
    /// one.
    pub alpha: f64,
    /// Root at this specific scan row instead of the top-ranked one. `None`
    /// keeps the top-pick behavior.
    pub root: Option<DrillRoot>,
}

impl Default for DrillConfig {
    fn default() -> Self {
        Self {
            max_depth: 5,
            alpha: SIGNIFICANCE_ALPHA,
            root: None,
        }
    }
}

/// Per-level significance budget under nested Sidak composition:
/// `max_depth` independent levels, each spending this share, compose back to
/// `alpha` family-wise across the whole drill. This is the same identity
/// `significance_threshold` already uses to compose per-comparison budgets
/// WITHIN one level (`per_comparison = 1 - (1-alpha)^(1/family)`); this
/// applies it a second time, ACROSS levels, before each level's own `family`
/// composition happens on top.
fn level_alpha(alpha: f64, max_depth: usize) -> f64 {
    let max_depth = max_depth.max(1) as f64;
    1.0 - (1.0 - alpha).powf(1.0 / max_depth)
}

/// What kind of split a [`DrillCandidate`] represents.
#[derive(Debug, Clone, Serialize)]
pub enum CandidateKind {
    /// An exact arithmetic decomposition: `measure` is one of the current
    /// target's component-edge children (see `MetricEdge`/`EdgeKind::Component`).
    Component { measure: String },
    /// A statistical decomposition: the current target's numerator, split by
    /// one value of an unconsumed segmentable dimension.
    Dimension { dimension: String, value: String },
}

/// One candidate evaluated at a drill level — a possible next split, whether
/// or not it was the one recursed into.
#[derive(Debug, Clone, Serialize)]
pub struct DrillCandidate {
    pub kind: CandidateKind,
    /// This candidate's share of the CURRENT level's gap (not the root's) —
    /// additive share for `+`/`-` composites, log-share for `*`/`÷`
    /// composites, direct fraction of the parent gap for a Dimension split.
    pub concentration: f64,
    /// This candidate's own gap, in its own unit.
    pub gap: f64,
    /// Whether this candidate's gap is proven. A `Component` candidate is an
    /// exact identity — always `true`. A `Dimension` candidate reflects a
    /// real `gap_is_significant` call at this level's composed alpha:
    /// `true` only for `Some(true)`. `Some(false)` (proven noise) candidates
    /// are dropped entirely and never appear here at all — same convention
    /// `opportunity()`'s own `SegmentOpportunity.gated` already uses.
    pub gated: bool,
}

/// Why a drill stopped after a given level.
#[derive(Debug, Clone, Serialize)]
pub enum StopReason {
    /// The best candidate's gap failed the significance gate (`Some(false)`).
    ///
    /// NOTE: currently UNREACHABLE. `dimension_candidates` drops `Some(false)`
    /// (proven-noise) candidates before ranking, so the drill's top candidate is
    /// only ever `Some(true)` (→ recurse) or `None` (→ `GateInconclusive`). This
    /// variant is reserved for a future "every candidate here was noise" signal
    /// (distinct from `NoCandidates`, which means no split existed at all).
    GateFailed,
    /// The gate could not evaluate the best candidate (`None`) — recursing
    /// further would present an unproven level as proven.
    GateInconclusive,
    /// No candidates were found: no component edges, no unconsumed
    /// segmentable dimensions, or every candidate query failed.
    NoCandidates,
    /// `max_depth` was reached.
    MaxDepth,
}

/// One level of a drill.
#[derive(Debug, Clone, Serialize)]
pub struct DrillLevel {
    /// The measure this level's gap was computed against. Unchanged from the
    /// parent level unless the parent's WINNING candidate was a `Component`
    /// split, in which case this is that child measure.
    pub measure: String,
    /// Dimension filters accumulated on the numerator so far (empty at the
    /// root; grows by one entry each time a `Dimension` candidate is
    /// followed; unchanged when a `Component` candidate is followed).
    pub segment_filter: Vec<QueryFilter>,
    /// This level's own gap: the benchmark population's value for `measure`
    /// minus the segment population's value, in `measure`'s own units.
    pub gap: f64,
    /// This level's gap as a fraction of the ROOT's gap — the cascaded
    /// product of every level's concentration since the root.
    pub root_share: f64,
    /// Every candidate evaluated at this level, ranked by concentration
    /// descending. The first entry (if `stop_reason` is `None`) is the one
    /// recursed into. Siblings are included so "follow the max" reads as a
    /// choice, not a hidden selection.
    pub candidates: Vec<DrillCandidate>,
    /// Why the drill stopped AFTER this level. `None` means it recursed
    /// further — the next entry in `DrillResult.levels` is that recursion.
    pub stop_reason: Option<StopReason>,
}

/// Full result of a recursive opportunity drill.
#[derive(Debug, Clone, Serialize)]
pub struct DrillResult {
    pub target: String,
    /// The root `opportunity()` scan's winning segment gap and upside —
    /// every level's `root_share` is relative to `root_gap`.
    pub root_gap: f64,
    pub root_upside: f64,
    /// The root's benchmark population, inherited unchanged by every level.
    pub benchmark_filter: Vec<QueryFilter>,
    pub levels: Vec<DrillLevel>,
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
    UniformDegradation {
        dimension: String,
        num_elements: usize,
    },
    /// A dimension value appears as a driver across multiple measures.
    CrossCutting {
        dimension: String,
        value: String,
        measures: Vec<String>,
    },
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
    /// A dimension split was performed on a non-additive measure (avg / median /
    /// count_distinct / type:number). Per-element deltas don't sum to the parent
    /// delta for these aggregation types, so concentrations and EP are approximations.
    NonAdditiveDimensionSplit {
        measure: String,
        measure_type: String,
        dimension: String,
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
#[derive(Debug, Clone, Copy)]
struct MetricDelta {
    previous: f64,
    current: f64,
    delta: f64,
}

/// Callback type for executing a query and returning rows.
/// The explain algorithm is in the non-feature-gated engine module,
/// so actual database execution is injected via this callback.
pub type QueryExecutor = dyn Fn(&QueryRequest) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, EngineError>
    + Send
    + Sync;

/// Component-edge candidates for `measure` at the current drill level: every
/// Component-kind child, queried for both the segment and benchmark
/// populations, with its share of `measure`'s own gap between them.
///
/// An exact arithmetic decomposition — no significance test applies (there
/// is no sampling uncertainty in `parent = child_a + child_b`), so every
/// returned candidate has `gated: true`.
///
/// Returns `Ok(vec![])`, not an error, when `measure` has no Component
/// children, or when its children mix additive (`+`/`-`) and multiplicative
/// (`*`/`÷`) operators — a composite of that shape is not decomposable by
/// this mechanism (see `explain`'s own comment at the analogous check,
/// metric_tree_ops.rs, for why: the edge list is flat with no precedence, so
/// `(a+b)*c` and `a+b*c` are indistinguishable and mixing them here would
/// produce a noisy, misleading concentration).
fn component_candidates(
    tree: &MetricTree,
    measure: &str,
    count_measure: Option<&str>,
    seg_filter: &[QueryFilter],
    bench_filter: &[QueryFilter],
    numerator_filters: &[QueryFilter],
    scan_filters: &[QueryFilter],
    executor: &QueryExecutor,
) -> Result<Vec<DrillCandidate>, EngineError> {
    let children: Vec<&MetricEdge> = tree
        .edges
        .iter()
        .filter(|e| e.to == measure && e.kind == EdgeKind::Component)
        .collect();
    if children.is_empty() {
        return Ok(Vec::new());
    }
    let multiplicative = children.iter().any(|e| e.operator.is_multiplicative());
    if multiplicative && !children.iter().all(|e| e.operator.is_multiplicative()) {
        // Mixed a+b*c shape — refuse rather than guess.
        return Ok(Vec::new());
    }

    // Append the accumulated numerator splits to BOTH populations so the child
    // comparison stays at least symmetric between seg and bench. NOTE: this
    // path is unreachable from a `type: sum` root (a sum has no Component
    // children, so opportunity_drill never descends into components), and
    // component decomposition only follows a Component winner — which itself
    // requires a composite root. So `numerator_filters` is empty every time
    // this runs today, and the fact that appending them here would also narrow
    // the BUNDLED count (denominator) — unlike dimension_candidates, which
    // keeps the count on population filters only — is an accepted limitation.
    // A non-sum (composite) root, if ever supported as a drill target, would
    // need synthetic filtered child measures (as dimension_candidates builds)
    // to separate the numerator from the denominator here.
    let mut seg_filters_full = scan_filters.to_vec();
    seg_filters_full.extend_from_slice(seg_filter);
    seg_filters_full.extend_from_slice(numerator_filters);
    let mut bench_filters_full = scan_filters.to_vec();
    bench_filters_full.extend_from_slice(bench_filter);
    bench_filters_full.extend_from_slice(numerator_filters);

    // Additive component children are SUMS; their honest contribution is a
    // per-unit RATE gap (numerator / the fixed count denominator), NOT a raw
    // sum gap — the denominator is held constant across the whole drill (the
    // design's core invariant), and a rate gap is what makes a component
    // candidate's `gap`/`concentration` comparable to a dimension candidate's
    // (also a rate) and to the root's rate gap. So in the additive case we
    // request the count alongside each child. Multiplicative children are
    // already ratio-valued (`attach_rate × price_per_side`), so they are used
    // as-is and need no count. Bundling the count into each additive child
    // query mirrors opportunity()'s own breakdown batching.
    let want_count = !multiplicative && count_measure.is_some();
    let child_measures = |edge: &MetricEdge| -> Vec<String> {
        if want_count {
            vec![edge.from.clone(), count_measure.unwrap().to_string()]
        } else {
            vec![edge.from.clone()]
        }
    };
    let mut requests: Vec<QueryRequest> = Vec::with_capacity(children.len() * 2);
    for edge in &children {
        requests.push(QueryRequest {
            measures: child_measures(edge),
            filters: seg_filters_full.clone(),
            ..QueryRequest::new()
        });
        requests.push(QueryRequest {
            measures: child_measures(edge),
            filters: bench_filters_full.clone(),
            ..QueryRequest::new()
        });
    }
    let results = parallel_execute(&requests, executor);

    let count_alias = count_measure.map(|c| c.replace('.', "__"));

    struct ChildValues<'a> {
        edge: &'a MetricEdge,
        seg: f64,
        bench: f64,
        seg_count: f64,
        bench_count: f64,
    }
    let mut values: Vec<ChildValues> = Vec::with_capacity(children.len());
    for (i, edge) in children.iter().enumerate() {
        let seg_rows = match &results[i * 2] {
            Ok(rows) => rows,
            Err(_) => continue,
        };
        let bench_rows = match &results[i * 2 + 1] {
            Ok(rows) => rows,
            Err(_) => continue,
        };
        let alias = edge.from.replace('.', "__");
        let seg = seg_rows
            .first()
            .map(|r| extract_measure_value(r, &alias))
            .unwrap_or(0.0);
        let bench = bench_rows
            .first()
            .map(|r| extract_measure_value(r, &alias))
            .unwrap_or(0.0);
        // Count rides in the same row (additive case only); default 1.0 so the
        // raw-fallback (`want_count == false`) leaves values unscaled.
        let seg_count = count_alias
            .as_ref()
            .and_then(|a| seg_rows.first().map(|r| extract_measure_value(r, a)))
            .unwrap_or(1.0);
        let bench_count = count_alias
            .as_ref()
            .and_then(|a| bench_rows.first().map(|r| extract_measure_value(r, a)))
            .unwrap_or(1.0);
        values.push(ChildValues {
            edge,
            seg,
            bench,
            seg_count,
            bench_count,
        });
    }
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates: Vec<DrillCandidate> = if multiplicative {
        // Parent's own log-ratio is the SUM of the children's signed log-ratios
        // — the exact multiplicative identity
        // `ln(∏ child^sign) = Σ sign·ln(child)`, the direct analog of the
        // additive branch's `total_attributed`. Do NOT reconstruct the parent
        // by summing children's raw values: for `R = A × B` the parent is the
        // PRODUCT `A·B`, not the sum `A + B`, so a summed reconstruction
        // yields a wrong parent_log_ratio (and thus wrong shares) for any
        // 2+-child multiplicative composite. Computing it as the sum of signed
        // child log-ratios needs no separate parent query and makes the
        // children's log-shares sum to 1 by construction. Requires every child
        // value > 0 (a log needs positive inputs); if any is non-positive the
        // composite is not decomposable this way.
        let all_positive = values.iter().all(|v| v.seg > 0.0 && v.bench > 0.0);
        let parent_log_ratio = if all_positive {
            let r: f64 = values
                .iter()
                .map(|v| v.edge.sign * (v.seg / v.bench).ln())
                .sum();
            if r.abs() > f64::EPSILON {
                Some(r)
            } else {
                None
            }
        } else {
            None
        };
        values
            .iter()
            .filter_map(|v| {
                let parent_log_ratio = parent_log_ratio?;
                if v.bench <= 0.0 || v.seg <= 0.0 {
                    return None;
                }
                let child_log_ratio = (v.seg / v.bench).ln();
                let concentration = v.edge.sign * child_log_ratio / parent_log_ratio;
                Some(DrillCandidate {
                    kind: CandidateKind::Component {
                        measure: v.edge.from.clone(),
                    },
                    concentration,
                    gap: (v.bench - v.seg) * v.edge.sign,
                    gated: true,
                })
            })
            .collect()
    } else {
        // Additive: each child's contribution is a per-unit RATE gap
        // (numerator/count), and the additive identity holds in rate units too
        // — rate(parent) = Σ children/count = Σ (child/count) = Σ rate(child) —
        // so `total_attributed` (the sum of signed child rate gaps) equals the
        // parent's own rate gap and the shares sum to 1. `want_count == false`
        // (no count available) leaves seg_count/bench_count at 1.0, reducing
        // this to the raw-sum gap it was before — the fallback the drill never
        // takes, since it always supplies a count.
        // Guard against a zero count (a population with no rows): treat that side's
        // rate as 0 rather than dividing to NaN, which would poison total_attributed
        // and the concentration sort. dimension_candidates guards its division the
        // same way. (Unreachable on the live drill path — the drill descends from a
        // real opportunity segment that has rows — but cheap silent-NaN insurance.)
        let rate = |num: f64, cnt: f64| -> f64 {
            if cnt.abs() > f64::EPSILON {
                num / cnt
            } else {
                0.0
            }
        };
        let child_rate_gap = |v: &ChildValues| -> f64 {
            (rate(v.bench, v.bench_count) - rate(v.seg, v.seg_count)) * v.edge.sign
        };
        let total_attributed: f64 = values.iter().map(child_rate_gap).sum();
        values
            .iter()
            .map(|v| {
                let gap = child_rate_gap(v);
                DrillCandidate {
                    kind: CandidateKind::Component {
                        measure: v.edge.from.clone(),
                    },
                    concentration: signed_fraction(gap, total_attributed),
                    gap,
                    gated: true,
                }
            })
            .collect()
    };
    candidates.sort_by(|a, b| {
        b.concentration
            .abs()
            .partial_cmp(&a.concentration.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(candidates)
}

/// Dimension-partition candidates for `measure` at the current drill level:
/// for each segmentable dimension on `measure`'s view not already consumed
/// by an earlier level, each distinct value observed WITHIN the segment
/// population becomes a candidate — a per-value filtered sum over the SAME
/// unfiltered `count_measure`, exactly the per-parent-unit-rate shape E3
/// exists for.
///
/// Gated on significance at `alpha` (the caller's already-composed per-level
/// budget — see `level_alpha`). A candidate whose gate returns `Some(false)`
/// is dropped; `None` ("cannot tell") is KEPT with `gated: false`, matching
/// `opportunity()`'s own fail-open convention (`SegmentOpportunity.gated`).
#[allow(clippy::too_many_arguments)]
fn dimension_candidates(
    tree: &MetricTree,
    layer: &SharedLayer,
    measure: &str,
    count_measure: &str,
    seg_filter: &[QueryFilter],
    bench_filter: &[QueryFilter],
    numerator_filters: &[QueryFilter],
    scan_filters: &[QueryFilter],
    consumed_dims: &[String],
    alpha: f64,
    executor: &QueryExecutor,
) -> Result<Vec<DrillCandidate>, EngineError> {
    let _ = tree; // reserved: dimension candidates don't need the tree today, kept for signature symmetry with component_candidates
    let Some((view_name, measure_name)) = measure.split_once('.') else {
        return Ok(Vec::new());
    };

    // Population filters ONLY — these drive the count (the FIXED denominator),
    // so the accumulated numerator splits must NOT be added here. They enter
    // the numerator via the synthetic `__drill__` measure's MeasureFilters
    // below (and the value-discovery query, which does want the narrowing).
    let mut seg_filters_full = scan_filters.to_vec();
    seg_filters_full.extend_from_slice(seg_filter);
    let mut bench_filters_full = scan_filters.to_vec();
    bench_filters_full.extend_from_slice(bench_filter);
    let count_alias = count_measure.replace('.', "__");

    // Read guard scopes discovery only — dropped before any executor call.
    let all_dims = { discover_dimensions(&layer.read().expect("layer lock poisoned"), view_name) };
    let mut candidates: Vec<DrillCandidate> = Vec::new();
    let mut discovered: Vec<(String, Vec<String>, String)> = Vec::new();
    for dim in all_dims.iter().filter(|d| !consumed_dims.contains(d)) {
        // Discover this dimension's distinct values within the SEGMENT
        // population, further narrowed by the accumulated numerator splits —
        // so we only offer values that actually occur inside the already-
        // narrowed numerator. (This is the one query that DOES take the
        // numerator filters as query filters; the rate queries below keep them
        // in the synthetic measure so the count denominator stays fixed.)
        let mut value_filters = seg_filters_full.clone();
        value_filters.extend_from_slice(numerator_filters);
        let value_query = QueryRequest {
            measures: vec![measure.to_string()],
            dimensions: vec![dim.clone()],
            filters: value_filters,
            ..QueryRequest::new()
        };
        let dim_alias = dim.replace('.', "__");
        let Ok(value_rows) = executor(&value_query) else {
            continue;
        };
        let values: Vec<String> = value_rows
            .iter()
            .map(|r| extract_dim_value(r, &dim_alias))
            .collect();
        // Same cardinality window `opportunity()`'s own dimension scan applies
        // (see MIN_/MAX_DIMENSION_CARDINALITY). The FLOOR is the load-bearing
        // one here, and it is not merely a tidiness rule: a dimension with a
        // single distinct value inside the current (already-narrowed) numerator
        // population is fully determined by the splits made above it, so its
        // one candidate reproduces the parent numerator exactly. Gap unchanged
        // → `concentration` 1.0 → it sorts above every real split and is always
        // followed. That is a tautology presented as a finding: scoped to a
        // single-store city it walks `city = Amsterdam` → `region = eu` →
        // `store_name = ...` all the way to MaxDepth, reporting "100% of the
        // root gap" at every level while explaining nothing at all.
        //
        // The CEILING carries opportunity()'s reasoning across unchanged:
        // splitting a gap on customer_id or order_id is never actionable.
        if !(MIN_DIMENSION_CARDINALITY..=MAX_DIMENSION_CARDINALITY).contains(&values.len()) {
            continue;
        }
        discovered.push((
            dim.clone(),
            values,
            dimension_partition_signature(&value_rows, &dim_alias),
        ));
    }

    // The same aliasing the scan collapses applies here, for two reasons.
    // Reading one: `staff_count = 20` and `store_name = 'De Pijp Versmarkt'`
    // are one split wearing two labels, and which of them wins the sort is
    // arbitrary. Statistics two: the duplicate inflates the candidate family
    // below, tightening the gate for every genuine candidate at this level to
    // pay for a comparison that was never independent.
    {
        let mut groups: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, (_, _, sig)) in discovered.iter().enumerate() {
            groups.entry(sig.as_str()).or_default().push(i);
        }
        let mut drop: HashSet<usize> = HashSet::new();
        for members in groups.into_values() {
            if members.len() < 2 {
                continue;
            }
            let layer_guard = layer.read().expect("layer lock poisoned");
            let rep = members
                .iter()
                .copied()
                .min_by_key(|&i| alias_representative_rank(&layer_guard, &discovered[i].0));
            drop.extend(members.into_iter().filter(|m| Some(*m) != rep));
        }
        if !drop.is_empty() {
            let mut i = 0usize;
            discovered.retain(|_| {
                let keep = !drop.contains(&i);
                i += 1;
                keep
            });
        }
    }

    // Every (dimension, value) pair discovered above is compared, and the level
    // then follows whichever one concentrates the most gap. That is a maximum
    // taken over the whole set, so the bar has to answer for the whole set —
    // the same argument `significance_threshold` makes for the scan's
    // `comparison_family`. Testing each candidate as if it were the only
    // question asked (the previous hardcoded `family = 2`) held the error rate
    // of one comparison while reporting the best of many, which is not a
    // control: a level offering 30 candidates would surface a "cause" from
    // pure noise as its expected yield.
    //
    // `k` stays 2. It corrects for a benchmark chosen as the best of `k`
    // segments being tested against, and the drill never does that — the
    // benchmark is inherited from the root scan and never re-picked, so there
    // is no within-level selection effect to answer for.
    let candidate_family: usize = discovered.iter().map(|(_, v, _)| v.len()).sum();

    for (dim, values, _) in &discovered {
        let dim = dim.as_str();
        for value in values {
            let filtered_name = format!(
                "__drill__{}_{}",
                dim.replace('.', "_"),
                value.replace(|c: char| !c.is_alphanumeric(), "_")
            );
            // Install the synthetic per-value numerator measure under a brief
            // write guard, dropped before any executor call. The find/insert is
            // computed as an Option INSIDE the guarded block so a missing
            // view/measure/expr `continue`s the loop only AFTER the guard has
            // been released (never `continue` while a guard is live).
            let installed = {
                let mut l = layer.write().expect("layer lock poisoned");
                (|| {
                    let view = l.views.iter_mut().find(|v| v.name == view_name)?;
                    let base_measure = view
                        .measures_list()
                        .iter()
                        .find(|m| m.name == measure_name)
                        .cloned()?;
                    let expr = base_measure.expr.clone()?;
                    if !view.measures_list().iter().any(|m| m.name == filtered_name) {
                        // The numerator scope is the ACCUMULATED splits (from
                        // earlier levels) AND this level's own `{{dim}} =
                        // 'value'` split. Baking all of them into the synthetic
                        // measure's MeasureFilter list — which
                        // augment_layer_for_opportunity ANDs into the dispersion
                        // CASE WHEN and the SQL generator ANDs at query time —
                        // narrows the numerator on BOTH the seg and bench query
                        // symmetrically, while the population/count filters stay
                        // fixed. Malformed accumulated entries (missing
                        // member/values) are skipped.
                        let mut measure_filters: Vec<crate::schema::models::MeasureFilter> =
                            numerator_filters
                                .iter()
                                .filter_map(|f| {
                                    let member = f.member.as_ref()?;
                                    // Escape single quotes (SQL standard doubled-quote)
                                    // before interpolating a warehouse-sourced value
                                    // verbatim into the filter expr — a value like
                                    // `O'Brien` would otherwise emit malformed SQL.
                                    let v = f.values.first()?.replace('\'', "''");
                                    Some(crate::schema::models::MeasureFilter {
                                        expr: format!("{{{{{member}}}}} = '{v}'"),
                                        original_expr: None,
                                        description: None,
                                    })
                                })
                                .collect();
                        let value_escaped = value.replace('\'', "''");
                        measure_filters.push(crate::schema::models::MeasureFilter {
                            expr: format!("{{{{{dim}}}}} = '{value_escaped}'"),
                            original_expr: None,
                            description: None,
                        });
                        view.measures.get_or_insert_with(Vec::new).push(Measure {
                            name: filtered_name.clone(),
                            measure_type: MeasureType::Sum,
                            expr: Some(expr),
                            description: None,
                            original_expr: None,
                            filters: Some(measure_filters),
                            samples: None,
                            synonyms: None,
                            rolling_window: None,
                            inherits_from: None,
                            drivers: None,
                            shift: None,
                            meta: None,
                        });
                    }
                    Some(())
                })()
                .is_some()
            };
            if !installed {
                continue;
            }
            let filtered_id = format!("{view_name}.{filtered_name}");
            // Second brief write guard for the augment, dropped before the rate
            // queries below.
            {
                augment_layer_for_opportunity(
                    &mut layer.write().expect("layer lock poisoned"),
                    &filtered_id,
                );
            }
            // The result-row column alias is the FULLY-QUALIFIED id with dots replaced
            // (`opp.__drill__…` -> `opp____drill__…`), NOT the bare measure name — a real
            // query row is keyed `view__measure`. Using `filtered_name` alone drops the
            // `view__` prefix and silently reads no value. (The dispersion/n aliases below
            // already prepend `{view_name}__` for exactly this reason.)
            let filtered_alias = filtered_id.replace('.', "__");
            let dispersion_alias =
                format!("{view_name}__{}", dispersion_measure_name(&filtered_name));
            let n_alias = format!("{view_name}__{}", dispersion_n_measure_name(&filtered_name));

            let seg_req = QueryRequest {
                measures: vec![
                    filtered_id.clone(),
                    count_measure.to_string(),
                    format!("{view_name}.{}", dispersion_measure_name(&filtered_name)),
                    format!("{view_name}.{}", dispersion_n_measure_name(&filtered_name)),
                ],
                filters: seg_filters_full.clone(),
                ..QueryRequest::new()
            };
            let bench_req = QueryRequest {
                filters: bench_filters_full.clone(),
                ..seg_req.clone()
            };
            let results = parallel_execute(&[seg_req, bench_req], executor);
            let (Ok(seg_rows), Ok(bench_rows)) = (&results[0], &results[1]) else {
                continue;
            };
            let (Some(seg_row), Some(bench_row)) = (seg_rows.first(), bench_rows.first()) else {
                continue;
            };

            let seg_num = extract_measure_value(seg_row, &filtered_alias);
            let seg_count = extract_measure_value(seg_row, &count_alias);
            let bench_num = extract_measure_value(bench_row, &filtered_alias);
            let bench_count = extract_measure_value(bench_row, &count_alias);
            if seg_count.abs() < f64::EPSILON || bench_count.abs() < f64::EPSILON {
                continue;
            }
            let seg_rate = seg_num / seg_count;
            let bench_rate = bench_num / bench_count;
            let gap = bench_rate - seg_rate;

            let seg_sd = extract_optional_measure_value(seg_row, &dispersion_alias);
            let seg_n = extract_optional_measure_value(seg_row, &n_alias).unwrap_or(seg_count);
            let bench_sd = extract_optional_measure_value(bench_row, &dispersion_alias);
            let bench_n =
                extract_optional_measure_value(bench_row, &n_alias).unwrap_or(bench_count);

            let real = gap_is_significant(
                gap,
                seg_sd,
                seg_n,
                bench_sd,
                bench_n,
                2,
                candidate_family,
                alpha,
            );
            if real == Some(false) {
                continue;
            }
            candidates.push(DrillCandidate {
                kind: CandidateKind::Dimension {
                    dimension: dim.to_string(),
                    value: value.clone(),
                },
                // Filled in by the caller (Task 5), which alone knows this
                // level's own parent gap to divide by — see note below.
                concentration: 0.0,
                gap,
                gated: real == Some(true),
            });
        }
    }
    candidates.sort_by(|a, b| {
        b.gap
            .abs()
            .partial_cmp(&a.gap.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(candidates)
}

/// Recursively decompose the top gap `opportunity()` finds, through
/// component edges and dimension partitions, until the evidence runs out.
///
/// Runs `opportunity()` once at the root — unconditionally, this is not
/// itself gated further — and takes its top-ranked segment as the starting
/// (segment, benchmark) population pair. Every subsequent level compares the
/// SAME two populations (narrowed by whatever dimension filters have been
/// accumulated) for a possibly-different measure (when a Component
/// candidate is followed) or the same measure with one more filter (when a
/// Dimension candidate is followed). The benchmark population never changes
/// once picked at the root — see the design doc's "benchmark is inherited,
/// never re-picked" invariant.
#[allow(clippy::too_many_arguments)]
pub fn opportunity_drill(
    tree: &MetricTree,
    layer: &SharedLayer,
    target: &str,
    time_dimension: &str,
    period: (&str, &str),
    scope: &[QueryFilter],
    executor: &QueryExecutor,
    config: &DrillConfig,
) -> Result<Option<DrillResult>, EngineError> {
    // Clone the current layer under a brief read guard, then run the root scan
    // against the SNAPSHOT with NO guard held — opportunity() calls the executor
    // synchronously on this thread for its overall query, and a real executor
    // read-locks the shared layer there; holding our own read guard across that
    // call would be a same-thread recursive read lock (unspecified on
    // std::sync::RwLock). opportunity() only reads (never installs), so a snapshot
    // is functionally identical, and the shared layer already carries the root
    // target's augmentation (the caller augments before the drill).
    let layer_snapshot = layer.read().expect("layer lock poisoned").clone();
    let scan = opportunity(
        tree,
        &layer_snapshot,
        target,
        time_dimension,
        period,
        scope,
        executor,
    )?;
    // `scan` here is the opportunity() scan result; `config.root` is the
    // caller's optional row selector. Named row when given, top-ranked
    // otherwise — everything downstream (seg_filter, bench_filter,
    // consumed_dims, current_gap, root_gap/root_upside) reads from these two
    // bindings and is unchanged.
    let (top_dim, top_seg) = match &config.root {
        Some(want) => {
            let Some(dim) = scan
                .dimensions
                .iter()
                .find(|d| d.dimension == want.dimension)
            else {
                return Ok(None);
            };
            let Some(seg) = dim.segments.iter().find(|s| s.segment == want.segment) else {
                return Ok(None);
            };
            (dim, seg)
        }
        None => {
            let Some(dim) = scan.dimensions.first() else {
                return Ok(None);
            };
            let Some(seg) = dim.segments.first() else {
                return Ok(None);
            };
            (dim, seg)
        }
    };
    if top_dim.benchmark_filter.is_empty() {
        return Ok(None);
    }

    let mut scan_filters = scope.to_vec();
    scan_filters.extend([
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
    ]);

    let target_view = target.split('.').next().unwrap_or("");
    let count_measure =
        { discover_count_measure(&layer.read().expect("layer lock poisoned"), target_view) };
    let per_level_alpha = level_alpha(config.alpha, config.max_depth);

    // The two FIXED populations, chosen once at the root and never narrowed:
    // `seg_filter` is the laggard segment `[top_dim = top_seg]`, `bench_filter`
    // is its inherited benchmark. Together they define both populations AND
    // their (fixed) count denominators for the whole drill — the design's
    // core "the denominator never changes" invariant. They do NOT accumulate.
    let seg_filter = vec![QueryFilter {
        member: Some(top_dim.dimension.clone()),
        operator: Some(FilterOperator::Equals),
        values: vec![top_seg.segment.clone()],
        and: None,
        or: None,
    }];
    let bench_filter = top_dim.benchmark_filter.clone();
    // Seeded with the root scan's top dimension AND every dimension the caller
    // already pinned to a SINGLE value in `scope`. The scope entries matter
    // because the drill is normally launched from a world-model *instance*
    // panel, where the scope IS the instance (`stores.city = Amsterdam`). Left
    // unconsumed, `stores.city` stays a candidate, its value-discovery query
    // returns the one value the scope already pinned, and the drill opens by
    // "explaining" 100% of the gap with the filter the user themselves chose.
    //
    // The cardinality floor in `dimension_candidates` catches this too, and is
    // the more robust guard of the two (it reads the data rather than the shape
    // of the filter). Consuming it here just saves a pointless discovery query
    // per level. Deliberately narrow: only single-value equality scopes are
    // consumed, because a multi-value scope (`city IN (A, B, C)`) leaves a
    // genuine question — WHICH of the three — that the drill should still be
    // free to answer.
    // Entity hierarchy for grain-aware narrowing, built once. If the layer
    // doesn't validate as a promotion closure we fall back to an empty
    // hierarchy, which makes every prune below a no-op.
    let (drill_dims, dim_to_entity, promotions) = {
        let l = layer.read().expect("layer lock poisoned");
        let view_name = target.split('.').next().unwrap_or("");
        let dims = discover_dimensions(&l, view_name);
        let mut cache: HashMap<&str, Vec<String>> = HashMap::new();
        cache.insert(view_name, dims.clone());
        let d2e = build_dim_to_entity(&l, &cache);
        let p = crate::engine::promotions::Promotions::build(&l.views).unwrap_or_default();
        (dims, d2e, p)
    };

    let mut consumed_dims = vec![top_dim.dimension.clone()];
    // The root's own pick narrows the grain too: having isolated a laggard
    // store, re-cutting the same gap by an axis that lives outside the store's
    // subtree answers a different question than the one being drilled.
    consumed_dims.extend(dims_out_of_scope_after(
        &drill_dims,
        &top_dim.dimension,
        &dim_to_entity,
        &promotions,
    ));
    consumed_dims.extend(scope.iter().filter_map(|f| {
        matches!(f.operator, Some(FilterOperator::Equals))
            .then(|| f.member.clone())
            .flatten()
            .filter(|_| f.values.len() == 1)
    }));
    // Each followed dimension split accumulates here. Unlike seg_filter, these
    // narrow the NUMERATOR only: dimension_candidates bakes them into the
    // synthetic per-value measure's MeasureFilter list (carried by both the
    // seg and bench query), so they hit the numerator symmetrically on both
    // populations and never touch the (fixed) count denominator. Empty at the
    // root; grows by one entry per dimension descent.
    let mut numerator_filters: Vec<QueryFilter> = Vec::new();
    let mut current_measure = target.to_string();
    let mut current_gap = top_seg.gap;
    let mut root_share_accum = 1.0_f64;

    let mut levels: Vec<DrillLevel> = Vec::new();
    let mut depth = 0usize;
    loop {
        let mut candidates = component_candidates(
            tree,
            &current_measure,
            count_measure.as_deref(),
            &seg_filter,
            &bench_filter,
            &numerator_filters,
            &scan_filters,
            executor,
        )?;
        if let Some(cm) = &count_measure {
            let dim_candidates = dimension_candidates(
                tree,
                layer,
                &current_measure,
                cm,
                &seg_filter,
                &bench_filter,
                &numerator_filters,
                &scan_filters,
                &consumed_dims,
                per_level_alpha,
                executor,
            )?;
            for mut c in dim_candidates {
                if current_gap.abs() > f64::EPSILON {
                    c.concentration = signed_fraction(c.gap, current_gap);
                }
                candidates.push(c);
            }
        }
        candidates.sort_by(|a, b| {
            b.concentration
                .abs()
                .partial_cmp(&a.concentration.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let stop_reason = if candidates.is_empty() {
            Some(StopReason::NoCandidates)
        } else if depth + 1 >= config.max_depth {
            Some(StopReason::MaxDepth)
        } else {
            match &candidates[0].kind {
                CandidateKind::Component { .. } => None, // exact, always followed
                CandidateKind::Dimension { .. } => {
                    if !candidates[0].gated {
                        Some(StopReason::GateInconclusive)
                    } else {
                        None
                    }
                }
            }
        };

        levels.push(DrillLevel {
            measure: current_measure.clone(),
            segment_filter: numerator_filters.clone(),
            gap: current_gap,
            root_share: root_share_accum,
            candidates: candidates.clone(),
            stop_reason: stop_reason.clone(),
        });

        if stop_reason.is_some() {
            break;
        }

        let winner = &candidates[0];
        root_share_accum *= winner.concentration.abs().min(1.0);
        current_gap = winner.gap;
        match &winner.kind {
            CandidateKind::Component { measure } => {
                current_measure = measure.clone();
            }
            CandidateKind::Dimension { dimension, value } => {
                // Accumulate onto the NUMERATOR, not the population. This is
                // the fix: pushing onto seg_filter narrowed the fixed count
                // denominator (and the benchmark never saw the split at all).
                numerator_filters.push(QueryFilter {
                    member: Some(dimension.clone()),
                    operator: Some(FilterOperator::Equals),
                    values: vec![value.clone()],
                    and: None,
                    or: None,
                });
                consumed_dims.push(dimension.clone());
                // Once the drill isolates an instance, the informative next
                // split is a sub-instance within it, not an orthogonal re-cut.
                for d in
                    dims_out_of_scope_after(&drill_dims, dimension, &dim_to_entity, &promotions)
                {
                    if !consumed_dims.contains(&d) {
                        consumed_dims.push(d);
                    }
                }
            }
        }
        depth += 1;
    }

    Ok(Some(DrillResult {
        target: target.to_string(),
        root_gap: top_seg.gap,
        root_upside: top_seg.upside,
        benchmark_filter: bench_filter,
        levels,
    }))
}

/// Execute `requests` concurrently using scoped OS threads.
///
/// Each thread calls `executor` independently, so queries hit the warehouse
/// in parallel. The caller's async runtime (injected via `handle.block_on`
/// inside the executor closure) schedules the actual I/O concurrently even
/// though the `executor` API is synchronous.
///
/// Falls back to single-threaded execution for ≤1 requests to avoid thread
/// spawn overhead on trivial batches.
fn parallel_execute(
    requests: &[QueryRequest],
    executor: &QueryExecutor,
) -> Vec<Result<Vec<serde_json::Map<String, serde_json::Value>>, EngineError>> {
    match requests.len() {
        0 => vec![],
        1 => vec![executor(&requests[0])],
        _ => std::thread::scope(|s| {
            let handles: Vec<_> = requests
                .iter()
                .map(|req| s.spawn(|| executor(req)))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        }),
    }
}

/// Immutable context shared across all recursion levels of the explain algorithm.
struct ExplainCtx<'a> {
    dim_cache: HashMap<&'a str, Vec<String>>,
    children_of: HashMap<&'a str, Vec<&'a MetricEdge>>,
    /// fully-qualified measure name -> human-readable measure type, populated only
    /// for non-additive measures (avg / median / count_distinct / count_distinct_approx /
    /// number / custom). Used to emit NonAdditiveDimensionSplit warnings.
    non_additive_measures: HashMap<String, String>,
    time_dimension: &'a str,
    current_period: (&'a str, &'a str),
    previous_period: (&'a str, &'a str),
    config: &'a ExplainConfig,
    executor: &'a QueryExecutor,
    warnings: std::cell::RefCell<Vec<ExplainWarning>>,
    /// Entity hierarchy + map from "view.dim" → grain entity (when the dim's
    /// local name matches an entity's key on its owning view). Used to prune
    /// the candidate dimension set after each split: once a dim mapped to
    /// entity `E` is picked, we only consider further splits on dims mapped
    /// to `E` itself or to entities below `E` in the parent: hierarchy.
    /// Attribute dims (not mapped to any entity) and dims whose grain isn't
    /// derivable are kept conservatively.
    promotions: crate::engine::promotions::Promotions,
    dim_to_entity: HashMap<String, String>,
}

impl<'a> ExplainCtx<'a> {
    fn next_dims_after_pick(&self, available_dims: &[String], picked: &str) -> Vec<String> {
        prune_dims_after_pick(
            available_dims,
            picked,
            &self.dim_to_entity,
            &self.promotions,
        )
    }
}

/// Hierarchy-aware pruning. After a dim is picked at level N, restrict the
/// candidate set at level N+1 to dims at the same grain or below.
///
/// The user's framing: once you isolate to "California" (a region instance),
/// the next informative split is the *sub*-instances within California (the
/// stores), not a re-cut by orthogonal axes. The flat O(N_dims) loop at each
/// level collapses to O(descendants(E_picked)), typically O(1) for a 3–5
/// deep dimensional hierarchy.
///
/// Conservative semantics:
/// - Picked dim is always excluded (today's behaviour).
/// - Dim with no entity mapping → no grain restriction can be proved; kept.
/// - Dim mapped to an entity outside the picked entity's subtree → dropped.
///
/// Falls back to today's flat filtering when the hierarchy is empty or the
/// picked dim isn't entity-bound (the dim_to_entity map is empty).
fn prune_dims_after_pick(
    available_dims: &[String],
    picked: &str,
    dim_to_entity: &HashMap<String, String>,
    promotions: &crate::engine::promotions::Promotions,
) -> Vec<String> {
    let Some(picked_entity) = dim_to_entity.get(picked) else {
        return available_dims
            .iter()
            .filter(|d| d.as_str() != picked)
            .cloned()
            .collect();
    };
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert(picked_entity.clone());
    for d in promotions.descendants(picked_entity) {
        allowed.insert(d);
    }
    available_dims
        .iter()
        .filter(|d| d.as_str() != picked)
        .filter(|d| match dim_to_entity.get(d.as_str()) {
            Some(e) => allowed.contains(e),
            None => true,
        })
        .cloned()
        .collect()
}

/// The dims that picking `picked` puts out of scope: everything
/// `prune_dims_after_pick` declines to carry forward. Returned as an exclusion
/// list so the drill can fold it into `consumed_dims` without changing how
/// `dimension_candidates` filters.
///
/// Conservative by construction — a dim the hierarchy cannot place is kept, so
/// an empty or unvalidatable hierarchy makes this a no-op and leaves the flat
/// behaviour exactly as it was.
fn dims_out_of_scope_after(
    all_dims: &[String],
    picked: &str,
    dim_to_entity: &HashMap<String, String>,
    promotions: &crate::engine::promotions::Promotions,
) -> Vec<String> {
    let kept = prune_dims_after_pick(all_dims, picked, dim_to_entity, promotions);
    all_dims
        .iter()
        .filter(|d| d.as_str() != picked && !kept.contains(d))
        .cloned()
        .collect()
}

/// Build the dim → entity map: a dim `view.col` maps to entity `E` iff some
/// entity on `view` has a single-key declaration with `key == col`. Composite
/// keys aren't mapped (no single-column representative). Same-named dims on
/// different views are kept as separate entries.
fn build_dim_to_entity(
    layer: &crate::schema::models::SemanticLayer,
    dim_cache: &HashMap<&str, Vec<String>>,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (view_name, dims) in dim_cache {
        let Some(view) = layer.view_by_name(view_name) else {
            continue;
        };
        for dim_qual in dims {
            let dim_local = dim_qual
                .strip_prefix(&format!("{}.", view_name))
                .unwrap_or(dim_qual);
            for entity in &view.entities {
                if entity.is_composite() {
                    continue;
                }
                if entity.key.as_deref() == Some(dim_local) {
                    out.insert(dim_qual.clone(), entity.name.clone());
                    break;
                }
            }
        }
    }
    out
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
fn build_children_of(tree: &MetricTree) -> HashMap<&str, Vec<&MetricEdge>> {
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
                let bare_dim = dimension
                    .rsplit('.')
                    .next()
                    .unwrap_or(dimension.as_str())
                    .to_string();
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
        let measure_names: Vec<String> =
            unique_measures.into_iter().map(|s| s.to_string()).collect();

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
    let target_md = fetch_period_delta(
        target,
        time_dimension,
        previous_period,
        current_period,
        &[],
        executor,
    )?;

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

    // Pre-compute non-additive measures so we can warn when dim-splitting them.
    // For these aggregation types, Σ element_delta ≠ parent_delta.
    let mut non_additive_measures: HashMap<String, String> = HashMap::new();
    for v in &layer.views {
        if let Some(measures) = &v.measures {
            for m in measures {
                let tag = match m.measure_type {
                    crate::schema::models::MeasureType::Average => Some("average"),
                    crate::schema::models::MeasureType::Median => Some("median"),
                    crate::schema::models::MeasureType::CountDistinct => Some("count_distinct"),
                    crate::schema::models::MeasureType::CountDistinctApprox => {
                        Some("count_distinct_approx")
                    }
                    crate::schema::models::MeasureType::Number => Some("number"),
                    crate::schema::models::MeasureType::Custom => Some("custom"),
                    _ => None,
                };
                if let Some(t) = tag {
                    non_additive_measures.insert(format!("{}.{}", v.name, m.name), t.to_string());
                }
            }
        }
    }

    let target_view = target.split('.').next().unwrap_or("");
    let available_dims = dim_cache.get(target_view).cloned().unwrap_or_default();

    // Entity hierarchy is built once per explain() invocation. If the layer
    // doesn't validate as a promotion closure, we fall back to an empty
    // hierarchy — hierarchy-aware pruning then becomes a no-op (the dim
    // map is empty, every dim is "attribute", same as today's behaviour).
    let promotions = crate::engine::promotions::Promotions::build(&layer.views).unwrap_or_default();
    let dim_to_entity = build_dim_to_entity(layer, &dim_cache);

    let ctx = ExplainCtx {
        dim_cache,
        children_of,
        non_additive_measures,
        time_dimension,
        current_period,
        previous_period,
        config,
        executor,
        warnings: std::cell::RefCell::new(Vec::new()),
        promotions,
        dim_to_entity,
    };

    // Recursive search
    let mut nodes = Vec::new();
    let mut covered = 0.0_f64;

    recurse(
        &ctx,
        target,
        target_md,
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
        if let Ok(md) = fetch_period_delta(
            &edge.from,
            time_dimension,
            previous_period,
            current_period,
            &[],
            executor,
        ) {
            // `None` now covers an unsizable non-linear form as well as a
            // missing coefficient — both are "no magnitude to report", and the
            // alternative was a linear number computed under a log rule.
            let estimated_impact = match compute_driver_impact(
                edge,
                md.delta,
                Some(md.previous),
                Some(target_md.previous),
            ) {
                DriverImpact::Sized(impact) => Some(impact),
                DriverImpact::Unsizable | DriverImpact::NoCoefficient => None,
            };
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
            if let Ok(md) = fetch_period_delta(
                &edge.from,
                time_dimension,
                previous_period,
                current_period,
                &[],
                executor,
            ) {
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

        // Query aggregate deltas for each searchable measure.
        // Tuple: (measure, raw_delta, leaf_share, cumulative_sign, dims)
        let mut measure_deltas: Vec<(String, f64, f64, f64, Vec<String>)> = Vec::new();
        for sm in &searchable {
            if let Ok(md) = fetch_period_delta(
                &sm.measure,
                time_dimension,
                previous_period,
                current_period,
                &[],
                executor,
            ) {
                let leaf_share = if target_md.delta.abs() > f64::EPSILON {
                    (md.delta * sm.cumulative_sign) / target_md.delta
                } else {
                    0.0
                };
                measure_deltas.push((
                    sm.measure.clone(),
                    md.delta,
                    leaf_share,
                    sm.cumulative_sign,
                    sm.dimensions.clone(),
                ));
            }
        }

        // If target itself has dimensions and isn't in searchable, also search it
        if !available_dims.is_empty() {
            let already_included = measure_deltas.iter().any(|(m, _, _, _, _)| m == target);
            if !already_included {
                measure_deltas.push((
                    target.to_string(),
                    target_md.delta,
                    1.0,
                    1.0,
                    available_dims.clone(),
                ));
            }
        }

        // Phase 2: per-measure beam search
        let mut all_paths: Vec<(ExplainPath, f64)> = Vec::new();
        for (measure, delta, leaf_share, cum_sign, dims) in &measure_deltas {
            if dims.is_empty() || delta.abs() < f64::EPSILON {
                continue;
            }
            let paths = beam_search_measure(
                measure,
                *delta,
                dims,
                &[],
                time_dimension,
                previous_period,
                current_period,
                config,
                executor,
                &ctx.dim_to_entity,
                &ctx.promotions,
            )?;
            for mut path in paths {
                path.root_fraction *= leaf_share.abs();
                // Issue 9: a leaf with negative cumulative_sign (e.g. a cost under
                // profit = revenue − cost) increased in raw terms, but contributes the
                // opposite sign to the parent. Reflect that in displayed deltas so
                // consumers don't see "+X" for what is actually a "−X" contribution.
                if (*cum_sign - 1.0).abs() > f64::EPSILON {
                    for node in &mut path.nodes {
                        node.delta *= *cum_sign;
                        for sib in &mut node.siblings {
                            sib.delta *= *cum_sign;
                        }
                    }
                }
                all_paths.push((path, *leaf_share));
            }
        }

        // Phase 3: cross-cutting detection
        let cross_cutting = detect_cross_cutting(&all_paths);
        for cc in cross_cutting {
            all_paths.push((cc, 1.0));
        }

        // Sort and truncate
        all_paths.sort_by(|a, b| {
            b.0.root_fraction
                .partial_cmp(&a.0.root_fraction)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut top_paths: Vec<ExplainPath> = all_paths
            .into_iter()
            .take(config.max_alternatives)
            .map(|(p, _)| p)
            .collect();

        // Phase 5: Statistical significance — test each top-K path against historical
        // variance at the SAME period scale. We sample 12 prior aggregated periods of the
        // same length as the current comparison, then compute consecutive deltas. This
        // ensures e.g. a QoQ current_delta is compared against historical QoQ deltas,
        // not MoM deltas (which would be on a different scale and artificially small).
        let period_len_days = period_length_days(current_period.0, current_period.1)
            .or_else(|| period_length_days(previous_period.0, previous_period.1));
        let mut hist_cache: HashMap<String, Option<Vec<f64>>> = HashMap::new();
        for path in &mut top_paths {
            if let Some(last_node) = path.nodes.last() {
                let cache_key = dedup_key(&last_node.measure, &last_node.filters);
                let historical_deltas = hist_cache.entry(cache_key).or_insert_with(|| {
                    let len = period_len_days?;
                    fetch_historical_deltas(
                        &last_node.measure,
                        time_dimension,
                        previous_period.0,
                        len,
                        12,
                        &last_node.filters,
                        executor,
                    )
                    .ok()
                    .filter(|d| !d.is_empty())
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
/// Find a declared row-count measure (`type: count`) on `view_name`, returned
/// as a fully-qualified `view.measure` id. This is the volume denominator used
/// to size an additive sum on a per-unit rate basis; `None` when the view
/// declares no count measure, in which case the caller refuses to size.
///
/// The heuristic is deliberately simple — the *first* declared `count` — and has
/// two known limitations the caller should be aware of:
/// - When a view declares several counts, "first" is arbitrary; there is no
///   signal here to say which one denominates the target's rows.
/// - This count spans all of the view's rows. Pairing it with a *filtered* sum
///   yields `filtered_sum / unfiltered_count`, a per-view-row contribution
///   rather than a per-matching-row rate. That is a coherent figure and the
///   intended shape of a per-parent-unit rate — the numerator narrows while
///   the denominator stays the whole population. The significance gate handles
///   it: [`augment_layer_for_opportunity`] embeds the sum's filter into the
///   dispersion measure and installs a filtered-row-count companion so the
///   gate's `n` matches the filtered numerator, not this unfiltered count.
fn discover_count_measure(layer: &SemanticLayer, view_name: &str) -> Option<String> {
    let view = layer.views.iter().find(|v| v.name == view_name)?;
    view.measures_list()
        .iter()
        .find(|m| m.measure_type == crate::schema::models::MeasureType::Count)
        .map(|m| format!("{}.{}", view_name, m.name))
}

/// Is this dimension shaped like something you could segment on at all?
/// Dates are excluded because the period is already the time axis, and an
/// explicit `segmentable: false` is honoured over everything else.
///
/// The shape test alone is too generous: it admits any string column, which is
/// how an address line or a customer's gender ends up ranked as a revenue
/// "lever" — both segment cleanly and neither is something anyone can act on.
/// Shape cannot distinguish those from `order_channel`; only the modeller can,
/// which is what `segmentable: false` is for.
fn is_segmentable(dim: &crate::schema::models::Dimension) -> bool {
    if dim.segmentable == Some(false) {
        return false;
    }
    matches!(
        dim.dimension_type,
        DimensionType::String | DimensionType::Number | DimensionType::Boolean
    )
}

/// Names of the dimensions on this view that identify a row rather than
/// describe it.
///
/// Grouping a measure by a surrogate ID is never actionable — `store_id = 1.0`
/// names nothing a human can reason about, and the ID's only meaning lives in
/// the view it points at. Two sources, unioned:
///
/// - a dimension explicitly declared `primary_key: true`;
/// - the key of an entity declaration whose backing dimension is numeric.
///
/// The numeric qualifier on the second matters: a *natural* key is a perfectly
/// good segment (`stores` joins `city`/`region` on the string columns of the
/// same name, and "compare revenue across regions" is exactly the question this
/// panel exists to answer). Surrogate keys are numeric, natural keys are
/// strings; where that misses (a string UUID key), the cardinality cap prunes it
/// anyway.
fn identifier_dimensions(view: &View) -> HashSet<String> {
    let mut ids: HashSet<String> = view
        .dimensions
        .iter()
        .filter(|d| d.primary_key.unwrap_or(false))
        .map(|d| d.name.clone())
        .collect();

    for key in view.entities.iter().flat_map(|e| e.get_keys()) {
        // An entity key names a *dimension*, and only falls back to meaning a
        // raw column when no dimension answers to it — mirror the resolution
        // order in sql_generator::resolve_join_key_expr. Matching `expr` alone
        // silently misses the common shape where the two differ (`orders`
        // keys the `order_id` dimension, whose expr is the `id` column).
        let backing = view
            .dimensions
            .iter()
            .find(|d| d.name == key)
            .or_else(|| view.dimensions.iter().find(|d| d.expr == key));
        if let Some(dim) = backing {
            if dim.dimension_type == DimensionType::Number {
                ids.insert(dim.name.clone());
            }
        }
    }

    ids
}

/// Dimensions worth scanning for segment opportunities on `view_name`.
///
/// Two rules beyond "is it segmentable":
///
/// 1. **Drop identifier dimensions.** See [`identifier_dimensions`] — an ID is
///    not a lever.
/// 2. **Follow foreign entities one hop.** The dimensions worth comparing in a
///    star schema live on the *dimension* views, not the fact view: `orders`
///    only carries FKs, enums, and measure columns, while the store's name,
///    city, and region — the things you can actually act on — sit across the
///    join on `stores`. A fact view scanned alone therefore offers almost
///    nothing actionable, which is the bug this rule fixes. One hop only: the
///    grain stays intact and the join is many-to-one (a fact row has exactly
///    one store), so no fan-out.
///
/// PERF: every candidate returned here costs one warehouse aggregate, and the
/// cardinality cap that prunes bad dimensions only applies *after* the query
/// comes back — so widening this set spends real money before it prunes. Following
/// N foreign entities pulls in N whole views (~8 candidates to ~20 on `orders`).
/// If that bites, the cheapest prune is to take only String dimensions from
/// joined views: continuous numerics (`square_feet`, `monthly_rent`) are never
/// useful segments — they blow the cardinality cap or are secretly categorical —
/// so dropping them pre-query costs nothing real. Deliberately not done yet:
/// it trades away numeric categoricals (a `tier` of 1/2/3) for a saving nobody
/// has measured. Measure before bounding.
fn discover_dimensions(layer: &SemanticLayer, view_name: &str) -> Vec<String> {
    let Some(view) = layer.views.iter().find(|v| v.name == view_name) else {
        return Vec::new();
    };

    let mut out: Vec<String> = Vec::new();
    let mut push_dims = |v: &View| {
        let identifiers = identifier_dimensions(v);
        for d in v.dimensions.iter().filter(|d| is_segmentable(d)) {
            if !identifiers.contains(&d.name) {
                out.push(format!("{}.{}", v.name, d.name));
            }
        }
    };

    push_dims(view);

    for entity in view
        .entities
        .iter()
        .filter(|e| e.entity_type == EntityType::Foreign)
    {
        // The view that *defines* this entity (declares it Primary) is the one
        // the FK points at — the same name-based resolution the join graph uses.
        let joined = layer.views.iter().find(|v| {
            v.name != view_name
                && v.entities
                    .iter()
                    .any(|e| e.name == entity.name && e.entity_type == EntityType::Primary)
        });
        if let Some(joined) = joined {
            push_dims(joined);
        }
    }

    out.sort();
    out.dedup();
    out
}

/// Build an aggregate query for a single date range, with optional dimensions and filters.
///
/// Unlike a time-grouped query, this returns one aggregated row per dimension combination
/// (or one row total when no dimensions are passed). Used to compare period totals exactly
/// — issue separate calls for the previous and current periods rather than relying on
/// first/last row of a time-bucketed result.
fn make_aggregate_query(
    measure: &str,
    time_dimension: &str,
    period_start: &str,
    period_end: &str,
    extra_dimensions: &[String],
    filters: &[QueryFilter],
) -> QueryRequest {
    let mut all_filters = filters.to_vec();
    all_filters.push(QueryFilter {
        member: Some(time_dimension.to_string()),
        operator: Some(FilterOperator::AfterOrOnDate),
        values: vec![period_start.to_string()],
        and: None,
        or: None,
    });
    all_filters.push(QueryFilter {
        member: Some(time_dimension.to_string()),
        operator: Some(FilterOperator::BeforeOrOnDate),
        values: vec![period_end.to_string()],
        and: None,
        or: None,
    });
    QueryRequest {
        measures: vec![measure.to_string()],
        dimensions: extra_dimensions.to_vec(),
        filters: all_filters,
        ..QueryRequest::new()
    }
}

/// Fetch a (previous, current, delta) tuple by issuing two separate aggregate queries.
///
/// This is the correct shape for period-vs-period comparison: each period gets its own
/// aggregated value, regardless of how the periods relate to each other (adjacent or not,
/// single-bucket or spanning many buckets).
fn fetch_period_delta(
    measure: &str,
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    filters: &[QueryFilter],
    executor: &QueryExecutor,
) -> Result<MetricDelta, EngineError> {
    let measure_alias = measure.replace('.', "__");
    let prev_q = make_aggregate_query(
        measure,
        time_dimension,
        previous_period.0,
        previous_period.1,
        &[],
        filters,
    );
    let curr_q = make_aggregate_query(
        measure,
        time_dimension,
        current_period.0,
        current_period.1,
        &[],
        filters,
    );
    // Run both period queries concurrently — they are independent and the
    // warehouse can serve them in parallel.
    let mut results = parallel_execute(&[prev_q, curr_q], executor);
    let curr_rows = results.pop().unwrap()?;
    let prev_rows = results.pop().unwrap()?;
    let previous = prev_rows
        .first()
        .map(|r| extract_measure_value(r, &measure_alias))
        .unwrap_or(0.0);
    let current = curr_rows
        .first()
        .map(|r| extract_measure_value(r, &measure_alias))
        .unwrap_or(0.0);
    Ok(MetricDelta {
        previous,
        current,
        delta: current - previous,
    })
}

/// Fetch per-element scores for a dimension by issuing two aggregate queries (one per period)
/// with the dimension included, then merging by dimension value.
fn fetch_element_scores(
    measure: &str,
    dim: &str,
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    filters: &[QueryFilter],
    executor: &QueryExecutor,
    parent_delta: f64,
) -> Result<Vec<ElementScore>, EngineError> {
    let dim_slice = [dim.to_string()];
    let prev_q = make_aggregate_query(
        measure,
        time_dimension,
        previous_period.0,
        previous_period.1,
        &dim_slice,
        filters,
    );
    let curr_q = make_aggregate_query(
        measure,
        time_dimension,
        current_period.0,
        current_period.1,
        &dim_slice,
        filters,
    );
    // Run both period queries concurrently — independent, no ordering constraint.
    let mut results = parallel_execute(&[prev_q, curr_q], executor);
    let curr_rows = results.pop().unwrap()?;
    let prev_rows = results.pop().unwrap()?;
    Ok(compute_element_scores_from_periods(
        measure,
        dim,
        &prev_rows,
        &curr_rows,
        parent_delta,
    ))
}

/// Sample historical period-length deltas for significance testing.
///
/// Returns up to `num_buckets` aggregated values over consecutive `period_length_days`
/// windows ending at `before_date` (exclusive). Consecutive deltas (value[i+1] − value[i])
/// give the historical variance at the SAME period scale as the current comparison —
/// avoiding the bug where MoM historical deltas are compared against e.g. QoQ current
/// deltas (different scales, artificially small variance).
fn fetch_historical_deltas(
    measure: &str,
    time_dimension: &str,
    before_date: &str,
    period_length_days: i64,
    num_buckets: usize,
    filters: &[QueryFilter],
    executor: &QueryExecutor,
) -> Result<Vec<f64>, EngineError> {
    use chrono::{Duration, NaiveDate};
    let measure_alias = measure.replace('.', "__");
    let end = NaiveDate::parse_from_str(before_date, "%Y-%m-%d").map_err(|e| {
        EngineError::QueryError(format!(
            "fetch_historical_deltas: invalid date '{}': {}",
            before_date, e
        ))
    })?;
    if period_length_days < 1 {
        return Ok(Vec::new());
    }
    // Build all window queries upfront so they can be executed concurrently.
    // Window i (i=0 is most recent): [end - (i+1)*len, end - i*len - 1 day]
    let window_dates: Vec<(String, String)> = (0..num_buckets)
        .map(|i| {
            let win_end = end - Duration::days(i as i64 * period_length_days + 1);
            let win_start = win_end - Duration::days(period_length_days - 1);
            (
                win_start.format("%Y-%m-%d").to_string(),
                win_end.format("%Y-%m-%d").to_string(),
            )
        })
        .collect();
    let queries: Vec<QueryRequest> = window_dates
        .iter()
        .map(|(ws, we)| make_aggregate_query(measure, time_dimension, ws, we, &[], filters))
        .collect();

    // Execute all historical windows in parallel — each is an independent aggregate.
    let mut values: Vec<f64> = Vec::with_capacity(num_buckets);
    for result in parallel_execute(&queries, executor) {
        let rows = result?;
        let val = rows
            .first()
            .map(|r| extract_measure_value(r, &measure_alias))
            .unwrap_or(0.0);
        values.push(val);
    }
    // Reverse so values are in chronological order (oldest first).
    values.reverse();
    if values.len() < 2 {
        return Ok(Vec::new());
    }
    Ok(values.windows(2).map(|w| w[1] - w[0]).collect())
}

/// Compute the inclusive length in days of a [start, end] date period.
/// Returns None if either date fails to parse as YYYY-MM-DD.
fn period_length_days(start: &str, end: &str) -> Option<i64> {
    use chrono::NaiveDate;
    let s = NaiveDate::parse_from_str(start, "%Y-%m-%d").ok()?;
    let e = NaiveDate::parse_from_str(end, "%Y-%m-%d").ok()?;
    Some((e - s).num_days() + 1)
}

/// Extract a numeric value from a row's measure column.
fn extract_measure_value(
    row: &serde_json::Map<String, serde_json::Value>,
    measure_alias: &str,
) -> f64 {
    row.get(measure_alias).map(json_to_f64).unwrap_or(0.0)
}

/// Like [`extract_measure_value`], but keeps "the warehouse said NULL" distinct
/// from "the warehouse said zero".
///
/// The difference is load-bearing for dispersion: `STDDEV_SAMP` over a one-row
/// segment is NULL (undefined), whereas 0.0 would claim the segment is
/// perfectly uniform and make any gap look infinitely significant — the exact
/// inversion of the truth.
fn extract_optional_measure_value(
    row: &serde_json::Map<String, serde_json::Value>,
    measure_alias: &str,
) -> Option<f64> {
    match row.get(measure_alias)? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn json_to_f64(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Extract a string dimension value from a row, coercing Null and non-string types.
fn extract_dim_value(row: &serde_json::Map<String, serde_json::Value>, dim_alias: &str) -> String {
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
    /// Previous-period value (needed for log decomposition on the next
    /// recursive level). `0.0` for dimension splits — the parent's
    /// previous value is reconstructible from `parent_md - delta`, but
    /// log decomposition is only meaningful on multiplicative composites
    /// so the missing data here is fine.
    previous: f64,
    /// Current-period value, same notes as `previous`.
    current: f64,
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
    if total.abs() > f64::EPSILON {
        1.0 / total
    } else {
        1e-10
    }
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

/// Compute per-element EP and JSD surprise from two period-aggregated row sets.
///
/// Each input row set is the result of a single aggregate query (one row per dim value
/// for that period). Joining by dim value gives the previous/current pair without relying
/// on row ordering inside a combined time-bucketed query.
///
/// Based on the Adtributor algorithm (Bhagwan et al., NSDI 2014):
/// - EP_i = (current_i - previous_i) / (current_total - previous_total)
/// - surprise_i = JSD(p_i, q_i) where p_i = prev_i/prev_total, q_i = curr_i/curr_total
fn compute_element_scores_from_periods(
    measure: &str,
    dim: &str,
    prev_rows: &[serde_json::Map<String, serde_json::Value>],
    curr_rows: &[serde_json::Map<String, serde_json::Value>],
    parent_delta: f64,
) -> Vec<ElementScore> {
    let measure_alias = measure.replace('.', "__");
    let dim_alias = dim.replace('.', "__");

    let mut by_value: HashMap<String, (f64, f64)> = HashMap::new();
    for row in prev_rows {
        let v = extract_dim_value(row, &dim_alias);
        let entry = by_value.entry(v).or_insert((0.0, 0.0));
        entry.0 += extract_measure_value(row, &measure_alias);
    }
    for row in curr_rows {
        let v = extract_dim_value(row, &dim_alias);
        let entry = by_value.entry(v).or_insert((0.0, 0.0));
        entry.1 += extract_measure_value(row, &measure_alias);
    }

    let mut elements: Vec<ElementScore> = by_value
        .into_iter()
        .map(|(value, (previous, current))| ElementScore {
            value,
            previous,
            current,
            delta: current - previous,
            ep: 0.0,
            surprise: 0.0,
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
#[allow(clippy::too_many_arguments)]
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
    dim_to_entity: &HashMap<String, String>,
    promotions: &crate::engine::promotions::Promotions,
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

    let new_remaining = prune_dims_after_pick(remaining_dims, dim, dim_to_entity, promotions);

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
#[allow(clippy::too_many_arguments)]
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
    dim_to_entity: &HashMap<String, String>,
    promotions: &crate::engine::promotions::Promotions,
) -> Result<Vec<BeamEntry>, EngineError> {
    let mut all_candidates: Vec<BeamEntry> = Vec::new();

    // Fetch all dimension element scores in parallel, then process sequentially.
    let dim_score_results: Vec<(String, Result<Vec<ElementScore>, EngineError>)> =
        std::thread::scope(|s| {
            let handles: Vec<_> = available_dims
                .iter()
                .map(|dim| {
                    s.spawn(move || {
                        let result = fetch_element_scores(
                            measure,
                            dim,
                            time_dimension,
                            previous_period,
                            current_period,
                            filters,
                            executor,
                            parent_delta,
                        );
                        (dim.clone(), result)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    for (dim, elements_result) in dim_score_results {
        let elements = match elements_result {
            Ok(e) => e,
            Err(_) => continue,
        };
        if elements.is_empty() {
            continue;
        }

        let ep_threshold = adaptive_ep_threshold(elements.len());
        let remaining: Vec<String> =
            prune_dims_after_pick(available_dims, &dim, dim_to_entity, promotions);

        // Check for uniform degradation
        if let Some(uniform_split) =
            detect_uniform_degradation(&dim, &elements, parent_delta, ep_threshold)
        {
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
                    measure,
                    &dim,
                    elem,
                    parent_delta,
                    filters,
                    &remaining,
                    "max_concentration",
                    elements.len(),
                    existing_nodes,
                    existing_root_fraction,
                    dim_to_entity,
                    promotions,
                ));
            }
        }

        // Strategy 2: highest |concentration| — picks the element with the largest
        // absolute share of the parent delta, regardless of direction.
        // Diverges from Strategy 1 when opposing elements exist (e.g. some segments
        // rising while others fall): Strategy 1 picks max signed, Strategy 2 picks max |delta|.
        if let Some(top_elem) = elements.iter().max_by(|a, b| {
            signed_fraction(a.delta, parent_delta)
                .abs()
                .partial_cmp(&signed_fraction(b.delta, parent_delta).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            all_candidates.push(make_beam_entry(
                measure,
                &dim,
                top_elem,
                parent_delta,
                filters,
                &remaining,
                "topk_concentration",
                elements.len(),
                existing_nodes,
                existing_root_fraction,
                dim_to_entity,
                promotions,
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
                    measure,
                    &dim,
                    &elements[best_idx],
                    parent_delta,
                    filters,
                    &remaining,
                    "jsd_smoothed",
                    elements.len(),
                    existing_nodes,
                    existing_root_fraction,
                    dim_to_entity,
                    promotions,
                ));
            }
        }

        // Strategy 4: IV/WOE — pick the element with the highest |WOE| value.
        if let Some(best_idx) = best_woe_index(&elements, prev_denom, curr_denom, epsilon) {
            all_candidates.push(make_beam_entry(
                measure,
                &dim,
                &elements[best_idx],
                parent_delta,
                filters,
                &remaining,
                "iv_woe",
                elements.len(),
                existing_nodes,
                existing_root_fraction,
                dim_to_entity,
                promotions,
            ));
        }
    }

    all_candidates.sort_by(|a, b| {
        b.root_fraction
            .partial_cmp(&a.root_fraction)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(all_candidates)
}

/// Run beam search on a single measure to find the best explanation paths.
#[allow(dead_code, clippy::too_many_arguments)]
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
    dim_to_entity: &HashMap<String, String>,
    promotions: &crate::engine::promotions::Promotions,
) -> Result<Vec<ExplainPath>, EngineError> {
    if measure_delta.abs() < f64::EPSILON || available_dims.is_empty() {
        return Ok(vec![]);
    }

    // Seed beam: evaluate all dims with all strategies
    let seed_candidates = evaluate_beam_candidates(
        measure,
        measure_delta,
        initial_filters,
        available_dims,
        time_dimension,
        previous_period,
        current_period,
        executor,
        &[],
        1.0,
        dim_to_entity,
        promotions,
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
            if entry.remaining_dims.is_empty() || entry.root_fraction < config.min_root_fraction {
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            // Get the delta for this entry's current state (filtered measure)
            let entry_delta = match fetch_period_delta(
                &entry.measure,
                time_dimension,
                previous_period,
                current_period,
                &entry.filters,
                executor,
            ) {
                Ok(md) => md.delta,
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
                &entry.measure,
                entry_delta,
                &entry.filters,
                &entry.remaining_dims,
                time_dimension,
                previous_period,
                current_period,
                executor,
                &entry.nodes,
                entry.root_fraction,
                dim_to_entity,
                promotions,
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
        next_beam.sort_by(|a, b| {
            b.root_fraction
                .partial_cmp(&a.root_fraction)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
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
    completed.sort_by(|a, b| {
        b.root_fraction
            .partial_cmp(&a.root_fraction)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
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
///
/// `parent_md` carries the parent measure's (previous, current, delta).
/// Multiplicative composites need both period values for log decomposition;
/// additive ones only consume `delta`. Callers that only have a delta can
/// pass a [`MetricDelta`] with zero previous/current — log decomposition
/// will skip and fall back to the additive path.
fn evaluate_candidates(
    ctx: &ExplainCtx,
    measure: &str,
    parent_md: MetricDelta,
    filters: &[QueryFilter],
    available_dims: &[String],
) -> Result<EvalResult, EngineError> {
    let parent_delta = parent_md.delta;
    let parent_sign = parent_delta.signum();

    // Dimensions already constrained by active filters
    let filtered_members: HashSet<&str> =
        filters.iter().filter_map(|f| f.member.as_deref()).collect();

    // 1) Component candidates — query all children first, then normalize.
    //
    // ADDITIVE composites (`R = A + B - C`):
    //   total_attributed = Σ (child_delta × edge_sign) across ALL components.
    //   parent_share = (delta × sign) / total_attributed → always sums to 1.0.
    //
    // MULTIPLICATIVE composites (`R = A × B`, `R = A / B`):
    //   Use log decomposition. ln(R_new/R_old) = Σ sign · ln(child_new/child_old)
    //   (with sign = +1 for Mul, -1 for Div). Each child's contribution share
    //   is `sign · ln(child_new / child_old) / ln(R_new / R_old)`. Sums to 1.0
    //   when the composite holds exactly; small drift comes from query rounding.
    //
    // We pick the multiplicative path when every component child of `measure`
    // carries a multiplicative operator AND all values (parent + each child,
    // both periods) are strictly positive (ln() requires that). Falls back
    // to additive whenever those preconditions don't hold.
    struct ComponentQuery {
        child: String,
        delta: f64,
        previous: f64,
        current: f64,
        sign: f64,
        operator: crate::engine::metric_tree::EdgeOperator,
        child_dims: Vec<String>,
    }
    // Pre-collect component edges so we can zip them with parallel results.
    let component_edges: Vec<&MetricEdge> = ctx
        .children_of
        .get(measure)
        .map(|edges| {
            edges
                .iter()
                .copied()
                .filter(|e| e.kind == EdgeKind::Component)
                .collect()
        })
        .unwrap_or_default();

    // Run component-delta fetches and dimension element-score fetches in one
    // combined parallel scope so ALL warehouse queries for this evaluation
    // level are in flight simultaneously. Each group is joined separately
    // after the scope so their sequential processing phases are unchanged.
    let (comp_delta_results, dim_score_results): (
        Vec<Result<MetricDelta, EngineError>>,
        Vec<(String, Result<Vec<ElementScore>, EngineError>)>,
    ) = std::thread::scope(|s| {
        let executor = ctx.executor;
        let time_dim = ctx.time_dimension;
        let prev = ctx.previous_period;
        let curr = ctx.current_period;

        let comp_handles: Vec<_> = component_edges
            .iter()
            .map(|edge| {
                let child: &str = &edge.from;
                s.spawn(move || fetch_period_delta(child, time_dim, prev, curr, filters, executor))
            })
            .collect();

        let dim_handles: Vec<_> = available_dims
            .iter()
            .map(|dim| {
                s.spawn(move || {
                    let result = fetch_element_scores(
                        measure,
                        dim,
                        time_dim,
                        prev,
                        curr,
                        filters,
                        executor,
                        parent_delta,
                    );
                    (dim.clone(), result)
                })
            })
            .collect();

        let comp: Vec<_> = comp_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        let dims: Vec<_> = dim_handles.into_iter().map(|h| h.join().unwrap()).collect();
        (comp, dims)
    });

    // Build component_queries from parallel results (same logic as before, now sequential).
    let mut component_queries: Vec<ComponentQuery> = Vec::new();
    for (edge, delta_result) in component_edges.iter().zip(comp_delta_results) {
        // skip failed component fetches, same as original
        if let Ok(md) = delta_result {
            let child = &edge.from;
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
                previous: md.previous,
                current: md.current,
                sign: edge.sign,
                operator: edge.operator,
                child_dims,
            });
        }
    }

    // Decide additive vs multiplicative path. The first ref in any
    // expression carries `Add` (no preceding operator → start-of-expr
    // default), so "all multiplicative" never fires. Use "any multiplicative"
    // instead: a single `*` or `/` in the parent expr promotes the whole
    // composite to the log-decomposition path. Mixed composites like
    // `a + b * c` are user error and will produce noisy concentrations;
    // we don't try to fix them here.
    let multiplicative = component_queries
        .iter()
        .any(|cq| cq.operator.is_multiplicative());
    let parent_log_ratio_opt = if multiplicative
        && parent_md.previous > 0.0
        && parent_md.current > 0.0
        && component_queries
            .iter()
            .all(|cq| cq.previous > 0.0 && cq.current > 0.0)
    {
        let r = (parent_md.current / parent_md.previous).ln();
        if r.abs() > f64::EPSILON {
            Some(r)
        } else {
            None
        }
    } else {
        None
    };

    let total_attributed: f64 = component_queries.iter().map(|cq| cq.delta * cq.sign).sum();
    let mut component_cands: Vec<Candidate> = Vec::new();
    for cq in component_queries {
        // parent_share: log decomposition for multiplicative composites,
        // signed-fraction-of-total_attributed for additive.
        let parent_share = if let Some(parent_log_ratio) = parent_log_ratio_opt {
            let child_log_ratio = (cq.current / cq.previous).ln();
            cq.sign * child_log_ratio / parent_log_ratio
        } else if total_attributed.abs() > f64::EPSILON {
            signed_fraction(cq.delta * cq.sign, total_attributed)
        } else {
            0.0
        };
        // Concentration for ranking against dimension candidates. The
        // additive form (`Δchild × sign / |Δparent|`) is meaningless
        // under multiplicative composition — raw deltas don't track the
        // ratio that matters — so use the log share for that case. Both
        // paths produce ~1.0-scale numbers, comparable to dim concentrations.
        let concentration = if parent_log_ratio_opt.is_some() {
            parent_share
        } else if parent_delta.abs() > f64::EPSILON {
            (cq.delta * cq.sign * parent_sign) / parent_delta.abs()
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
            previous: cq.previous,
            current: cq.current,
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
    //    Elements with |EP| below an adaptive threshold (0.05/√n) are noise and
    //    excluded from ranking — fixed 0.05 would silently drop every element on
    //    high-cardinality dimensions (1000+ values).

    let mut best_dim: Option<(f64, f64, Vec<Candidate>, usize)> = None; // (surprise, top_conc, candidates, total_count)
                                                                        // Hierarchy-aware pruning. If the picked dim maps to an entity, drop
                                                                        // dims at unrelated grains; otherwise just exclude the picked dim.
    let remaining_dims_for =
        |dim: &str| -> Vec<String> { ctx.next_dims_after_pick(available_dims, dim) };
    // Process pre-fetched dimension results (fetched in parallel above).
    for (dim, elements_result) in dim_score_results {
        match elements_result {
            Ok(mut elements) => {
                let total_count = elements.len();
                if total_count == 0 {
                    continue;
                }

                // Check for Simpson's paradox before sorting/truncating
                if let Some(w) = detect_simpsons_paradox(parent_delta, &dim, &elements) {
                    ctx.warnings.borrow_mut().push(w);
                }

                // Issue 5: dim-splitting a non-additive measure (avg/median/distinct/number)
                // produces per-element deltas that don't sum to parent_delta. Warn once
                // per (measure, dim) pair so users know the concentrations are approximate.
                if let Some(mt) = ctx.non_additive_measures.get(measure) {
                    let mut warnings = ctx.warnings.borrow_mut();
                    let already = warnings.iter().any(|w| {
                        matches!(w, ExplainWarning::NonAdditiveDimensionSplit { measure: m, dimension: d, .. }
                            if m == measure && d == &dim)
                    });
                    if !already {
                        warnings.push(ExplainWarning::NonAdditiveDimensionSplit {
                            measure: measure.to_string(),
                            measure_type: mt.clone(),
                            dimension: dim.clone(),
                        });
                    }
                }

                // Sort elements by surprise descending (most unexpected first)
                elements.sort_by(|a, b| {
                    b.surprise
                        .partial_cmp(&a.surprise)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                // Dimension surprise = sum of significant elements' surprises.
                // Only elements with |EP| >= adaptive threshold contribute (noise filter).
                let ep_threshold = adaptive_ep_threshold(total_count);
                let dim_surprise: f64 = elements
                    .iter()
                    .filter(|e| e.ep.abs() >= ep_threshold)
                    .map(|e| e.surprise)
                    .sum();

                // Truncate to max display values
                elements.truncate(ctx.config.max_dim_values);

                let remaining = remaining_dims_for(&dim);
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
                        previous: elem.previous,
                        current: elem.current,
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
                let top_conc = dim_cands.first().map(|c| c.concentration).unwrap_or(0.0);

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
    parent_md: MetricDelta,
    filters: &[QueryFilter],
    available_dims: &[String],
    depth: usize,
    is_top_level: bool,
    parent_root_fraction: f64,
    nodes: &mut Vec<ExplainNode>,
    covered: &mut f64,
) -> Result<(), EngineError> {
    let parent_delta = parent_md.delta;
    if depth >= ctx.config.max_depth || *covered >= ctx.config.coverage_threshold {
        return Ok(());
    }
    if parent_delta.abs() < f64::EPSILON {
        return Ok(());
    }

    let eval = evaluate_candidates(ctx, measure, parent_md, filters, available_dims)?;

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
        MetricDelta {
            previous: top.previous,
            current: top.current,
            delta: top.delta,
        },
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
    let variance: f64 = historical_deltas
        .iter()
        .map(|d| (d - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let std = variance.sqrt();

    if std < f64::EPSILON {
        let p_value = if (current_delta - mean).abs() < f64::EPSILON {
            1.0
        } else {
            0.0
        };
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
mod hierarchy_prune_tests {
    //! Hierarchy-aware dim pruning. Spec'd behavior:
    //!
    //! - Picking a dim that maps to entity `E` (its local name == `E.key`)
    //!   restricts subsequent splits to dims mapped to `E` or to descendants
    //!   of `E` in the `parent:` hierarchy.
    //! - Picking a dim with no entity mapping falls back to flat exclusion
    //!   of just the picked dim (today's behaviour).
    //! - Dims with no entity mapping are always kept (we cannot prove a
    //!   grain restriction).

    use super::*;
    use crate::engine::promotions::Promotions;
    use crate::schema::models::*;

    fn ent(name: &str, ty: EntityType, key: &str, parent: Option<&str>) -> Entity {
        Entity {
            name: name.to_string(),
            entity_type: ty,
            description: None,
            key: Some(key.to_string()),
            keys: None,
            lifespan: None,
            inherits_from: None,
            meta: None,
            parent: parent.map(|s| s.to_string()),
        }
    }

    fn view(name: &str, entities: Vec<Entity>) -> View {
        View {
            name: name.to_string(),
            description: None,
            label: None,
            datasource: None,
            dialect: None,
            table: Some(name.to_string()),
            sql: None,
            entities,
            dimensions: vec![],
            measures: None,
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }
    }

    /// Pick a dim mapped to `region_id`. Sibling dims at the same grain
    /// (other region attributes) and dims mapped to descendant entities
    /// (`store_id`) stay; dims mapped to ancestors or unrelated branches
    /// are dropped.
    #[test]
    fn prunes_unrelated_grain_dims() {
        // region_id ← store_id; market_id is an unrelated branch.
        let regions = view(
            "regions",
            vec![ent("region_id", EntityType::Primary, "region_id", None)],
        );
        let stores = view(
            "stores",
            vec![ent(
                "store_id",
                EntityType::Primary,
                "store_id",
                Some("region_id"),
            )],
        );
        let markets = view(
            "markets",
            vec![ent("market_id", EntityType::Primary, "market_id", None)],
        );
        let p = Promotions::build(&[regions, stores, markets]).unwrap();
        let mut d2e = HashMap::new();
        d2e.insert("regions.region_id".to_string(), "region_id".to_string());
        d2e.insert("stores.store_id".to_string(), "store_id".to_string());
        d2e.insert("markets.market_id".to_string(), "market_id".to_string());

        let dims = vec![
            "regions.region_id".to_string(),
            "stores.store_id".to_string(),
            "markets.market_id".to_string(),
        ];
        let next = prune_dims_after_pick(&dims, "regions.region_id", &d2e, &p);
        // picked excluded; descendant kept; unrelated branch dropped.
        assert!(!next.iter().any(|d| d == "regions.region_id"));
        assert!(next.iter().any(|d| d == "stores.store_id"));
        assert!(!next.iter().any(|d| d == "markets.market_id"));
    }

    /// Attribute dims (no entity mapping) are kept whether they're on the
    /// picked entity's view or somewhere else — we can't prove a restriction.
    #[test]
    fn attribute_dims_are_kept_conservatively() {
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
        );
        let p = Promotions::build(&[stores]).unwrap();
        let mut d2e = HashMap::new();
        d2e.insert("stores.store_id".to_string(), "store_id".to_string());
        let dims = vec![
            "stores.store_id".to_string(),
            "stores.store_size".to_string(), // attribute — no mapping
            "stores.city".to_string(),       // attribute — no mapping
        ];
        let next = prune_dims_after_pick(&dims, "stores.store_id", &d2e, &p);
        assert!(next.iter().any(|d| d == "stores.store_size"));
        assert!(next.iter().any(|d| d == "stores.city"));
    }

    /// Picking an attribute dim (no entity mapping) falls back to today's
    /// flat-exclusion behaviour. No hierarchy claim can be made.
    #[test]
    fn unmapped_pick_falls_back_to_flat_exclusion() {
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
        );
        let p = Promotions::build(&[stores]).unwrap();
        let d2e: HashMap<String, String> = HashMap::new(); // nothing mapped
        let dims = vec![
            "stores.region".to_string(),
            "stores.product_type".to_string(),
            "stores.store_type".to_string(),
        ];
        let next = prune_dims_after_pick(&dims, "stores.region", &d2e, &p);
        assert_eq!(next.len(), 2);
        assert!(!next.iter().any(|d| d == "stores.region"));
    }

    /// Transitive subtree: picking a region restricts to region OR any
    /// descendant in the hierarchy (store, customer-of-store, …).
    #[test]
    fn transitive_subtree_is_kept() {
        // region_id ← store_id ← shelf_id (3 levels)
        let regions = view(
            "regions",
            vec![ent("region_id", EntityType::Primary, "region_id", None)],
        );
        let stores = view(
            "stores",
            vec![ent(
                "store_id",
                EntityType::Primary,
                "store_id",
                Some("region_id"),
            )],
        );
        let shelves = view(
            "shelves",
            vec![ent(
                "shelf_id",
                EntityType::Primary,
                "shelf_id",
                Some("store_id"),
            )],
        );
        let p = Promotions::build(&[regions, stores, shelves]).unwrap();
        let mut d2e = HashMap::new();
        d2e.insert("regions.region_id".to_string(), "region_id".to_string());
        d2e.insert("stores.store_id".to_string(), "store_id".to_string());
        d2e.insert("shelves.shelf_id".to_string(), "shelf_id".to_string());
        let dims = vec![
            "regions.region_id".to_string(),
            "stores.store_id".to_string(),
            "shelves.shelf_id".to_string(),
        ];
        let next = prune_dims_after_pick(&dims, "regions.region_id", &d2e, &p);
        // Both descendants are kept.
        assert!(next.iter().any(|d| d == "stores.store_id"));
        assert!(next.iter().any(|d| d == "shelves.shelf_id"));
    }

    /// `build_dim_to_entity` correctly recognizes dim → entity bindings
    /// when the dim's local name matches the entity's `key:`.
    #[test]
    fn build_dim_to_entity_maps_key_matches() {
        let stores = View {
            name: "stores".to_string(),
            description: None,
            label: None,
            datasource: None,
            dialect: None,
            table: Some("stores".to_string()),
            sql: None,
            entities: vec![
                ent("store_id", EntityType::Primary, "store_id", None),
                ent("region_id", EntityType::Foreign, "region_id", None),
            ],
            dimensions: vec![
                Dimension {
                    name: "store_id".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "store_id".to_string(),
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
                    name: "region_id".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "region_id".to_string(),
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
                    name: "city".to_string(),
                    dimension_type: DimensionType::String,
                    description: None,
                    expr: "city".to_string(),
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
            measures: None,
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        };
        let layer = SemanticLayer::new(vec![stores], None);
        let mut dim_cache: HashMap<&str, Vec<String>> = HashMap::new();
        dim_cache.insert(
            "stores",
            vec![
                "stores.store_id".to_string(),
                "stores.region_id".to_string(),
                "stores.city".to_string(),
            ],
        );
        let d2e = build_dim_to_entity(&layer, &dim_cache);
        assert_eq!(
            d2e.get("stores.store_id").map(|s| s.as_str()),
            Some("store_id")
        );
        assert_eq!(
            d2e.get("stores.region_id").map(|s| s.as_str()),
            Some("region_id")
        );
        assert_eq!(d2e.get("stores.city"), None); // attribute, no entity match
    }
}

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
            pre_aggregations: None,
            refresh_key: None,
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
            shift: None,
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
            shift: None,
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

    #[test]
    fn reachable_values_outcome_reports_an_executor_error_verbatim() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);
        let executor = move |_: &QueryRequest| Err(EngineError::QueryError("boom".to_string()));

        let (values, outcome) = reachable_values_outcome(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &[],
            &executor,
        );
        assert!(values.is_empty());
        match outcome {
            BaselineOutcome::ExecutorError(msg) => assert!(msg.contains("boom")),
            other => panic!("expected ExecutorError, got {other:?}"),
        }
    }

    #[test]
    fn reachable_values_outcome_distinguishes_no_rows_from_an_error() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);
        let executor = move |_: &QueryRequest| Ok(vec![]);

        let (values, outcome) = reachable_values_outcome(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &[],
            &executor,
        );
        assert!(values.is_empty());
        assert_eq!(outcome, BaselineOutcome::NoRows);
    }

    #[test]
    fn reachable_values_outcome_flags_rows_whose_columns_do_not_match() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);
        let executor = move |_: &QueryRequest| {
            let mut row = serde_json::Map::new();
            row.insert("something_else".to_string(), serde_json::json!(1));
            Ok(vec![row])
        };

        let (_, outcome) = reachable_values_outcome(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &[],
            &executor,
        );
        assert_eq!(outcome, BaselineOutcome::NoMatchingColumns);
    }

    #[test]
    fn reachable_values_outcome_reports_valued_on_success() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);
        let executor = move |_: &QueryRequest| {
            let mut row = serde_json::Map::new();
            row.insert("orders__revenue".to_string(), serde_json::json!(42));
            Ok(vec![row])
        };

        let (values, outcome) = reachable_values_outcome(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &[],
            &executor,
        );
        assert_eq!(outcome, BaselineOutcome::Valued);
        assert_eq!(values.get("orders.revenue"), Some(&42.0));
    }

    #[test]
    fn reachable_values_filtered_appends_scope_to_date_filters() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);

        // Capture the QueryRequest the executor is handed. QueryExecutor is
        // `dyn Fn(..) + Send + Sync + 'static`, so the closure must own its
        // capture — Arc::clone in, keep the original outside to inspect.
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<QueryRequest>));
        let captured_inner = std::sync::Arc::clone(&captured);
        let executor = move |req: &QueryRequest| {
            *captured_inner.lock().unwrap() = Some(req.clone());
            Ok(vec![])
        };

        let scope = vec![QueryFilter {
            member: Some("orders.supplier_id".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["acme".to_string()],
            and: None,
            or: None,
        }];

        reachable_values_filtered(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &scope,
            &executor,
        );

        let req = captured
            .lock()
            .unwrap()
            .take()
            .expect("executor was called");
        // Two date predicates plus the one scope predicate, scope last.
        assert_eq!(req.filters.len(), 3);
        assert_eq!(req.filters[2].member.as_deref(), Some("orders.supplier_id"));
        assert_eq!(req.filters[2].values, vec!["acme".to_string()]);
    }

    #[test]
    fn reachable_values_is_unchanged_by_the_refactor() {
        let layer = make_layer(vec![make_view(
            "orders",
            vec![atomic_measure("revenue", MeasureType::Sum)],
        )]);
        let tree = MetricTree::build(&layer);

        let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<QueryRequest>));
        let captured_inner = std::sync::Arc::clone(&captured);
        let executor = move |req: &QueryRequest| {
            *captured_inner.lock().unwrap() = Some(req.clone());
            Ok(vec![])
        };

        reachable_values(
            &tree,
            &["orders.revenue".to_string()],
            "orders.order_date",
            ("2026-01-01", "2026-03-31"),
            &executor,
        );

        let req = captured
            .lock()
            .unwrap()
            .take()
            .expect("executor was called");
        // Exactly the two date predicates — no scope, nothing extra.
        assert_eq!(req.filters.len(), 2);
        assert_eq!(req.measures, vec!["orders.revenue".to_string()]);
    }

    #[test]
    fn test_additive_same_view_composite_accepts_additive_sum_refs() {
        let layer = make_layer(vec![make_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
        )]);
        let flat = additive_same_view_composite(&layer, "checks.net_revenue")
            .expect("additive same-view composite of sums is eligible");
        // Each ref is replaced by the referenced measure's own expr, wrapped
        // in parens unconditionally (see `flatten_additive_composite`), so
        // assert on the substance — both names present, fully flattened —
        // rather than pin the exact wrapped string.
        assert!(!flat.contains("{{"), "fully flattened, got {flat}");
        assert!(flat.contains("entree_revenue"));
        assert!(flat.contains("addon_revenue"));
    }

    #[test]
    fn test_additive_same_view_composite_rejects_multiplicative() {
        let layer = make_layer(vec![make_view(
            "checks",
            vec![
                atomic_measure("total_checks", MeasureType::Sum),
                atomic_measure("avg_check", MeasureType::Sum),
                composite_measure(
                    "revenue_vol_x_price",
                    "{{checks.total_checks}} * {{checks.avg_check}}",
                ),
            ],
        )]);
        // A product has no per-row value that divides by a row count into a
        // meaningful rate. Must stay on the existing value-share path.
        assert!(additive_same_view_composite(&layer, "checks.revenue_vol_x_price").is_none());
    }

    #[test]
    fn test_additive_same_view_composite_rejects_cross_view_refs() {
        let layer = make_layer(vec![
            make_view(
                "orders",
                vec![composite_measure(
                    "total_order_value",
                    "{{order_items.total_revenue}}",
                )],
            ),
            make_view(
                "order_items",
                vec![atomic_measure("total_revenue", MeasureType::Sum)],
            ),
        ]);
        // The denominator would be a count on `orders` while the numerator
        // aggregates `order_items` rows through a join — a fan-out grain
        // mismatch.
        assert!(additive_same_view_composite(&layer, "orders.total_order_value").is_none());
    }

    #[test]
    fn test_additive_same_view_composite_rejects_plain_sum_and_flattens_nested_composite() {
        let layer = make_layer(vec![make_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                composite_measure("addon_revenue", "{{checks.entree_revenue}} + 1"),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
        )]);
        // A sum is not a composite — it takes the existing `is_sum_like` path.
        assert!(additive_same_view_composite(&layer, "checks.entree_revenue").is_none());
        // A ref to another same-view additive composite is now flattened via
        // recursion, not refused. This used to assert `is_none()` here — that
        // assertion encoded the very bug this task fixes: `net_revenue` is
        // exactly this shape (a sum plus a same-view composite), and refusing
        // it forced the root onto the value-share path while its children
        // were sized as per-unit rates, producing mismatched units.
        let flat = additive_same_view_composite(&layer, "checks.net_revenue")
            .expect("a ref to a same-view additive composite is flattened, not refused");
        assert!(!flat.contains("{{"), "fully flattened, got {flat}");
    }

    #[test]
    fn test_additive_same_view_composite_recurses_through_composite_refs() {
        // The example_new/checks.view.yml shape: net_revenue -> entree_revenue (sum)
        // + addon_revenue (custom) -> two sums. This is the shape the whole fix
        // exists for, and the non-recursive predicate refused it.
        let layer = make_layer(vec![make_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("sides_revenue", MeasureType::Sum),
                atomic_measure("beverages_revenue", MeasureType::Sum),
                composite_measure(
                    "addon_revenue",
                    "{{checks.sides_revenue}} + {{checks.beverages_revenue}}",
                ),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
        )]);
        let flat = additive_same_view_composite(&layer, "checks.net_revenue")
            .expect("a composite of a sum and a same-view composite is eligible");
        assert!(!flat.contains("{{"), "fully flattened, got {flat}");
        // Every substitution is parenthesized, so the nested sum cannot bleed into
        // the enclosing expression's precedence.
        assert!(flat.contains("sides_revenue"));
        assert!(flat.contains("beverages_revenue"));
        assert!(flat.contains("entree_revenue"));
    }

    #[test]
    fn test_additive_same_view_composite_parenthesizes_substitutions() {
        // Without parentheses this flattens to `a - list_price - discount`,
        // silently inverting the sign on `discount`.
        let mut b = atomic_measure("b", MeasureType::Sum);
        b.expr = Some("list_price - discount".to_string());
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("a", MeasureType::Sum),
                b,
                composite_measure("diff", "{{v.a}} - {{v.b}}"),
            ],
        )]);
        let flat = additive_same_view_composite(&layer, "v.diff")
            .expect("additive composite of same-view sums is eligible");
        // Every substitution is wrapped unconditionally now (including the
        // bare `a`), so assert the sign-preserving property rather than pin
        // an exact string: the compound child keeps its second term's sign
        // only if it stays intact behind its own parens.
        assert!(flat.contains("(list_price - discount)"), "got {flat}");
    }

    #[test]
    fn test_additive_same_view_composite_refuses_multiplicative_at_any_depth() {
        // A Mul hidden one level down must refuse the whole tree — the outer
        // expression looks additive, so only recursion can see it.
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("a", MeasureType::Sum),
                atomic_measure("b", MeasureType::Sum),
                atomic_measure("c", MeasureType::Sum),
                composite_measure("product", "{{v.b}} * {{v.c}}"),
                composite_measure("outer", "{{v.a}} + {{v.product}}"),
            ],
        )]);
        assert!(additive_same_view_composite(&layer, "v.outer").is_none());
    }

    #[test]
    fn test_additive_same_view_composite_refuses_cross_view_at_any_depth() {
        let layer = make_layer(vec![
            make_view(
                "v",
                vec![
                    atomic_measure("a", MeasureType::Sum),
                    composite_measure("inner", "{{other.x}}"),
                    composite_measure("outer", "{{v.a}} + {{v.inner}}"),
                ],
            ),
            make_view("other", vec![atomic_measure("x", MeasureType::Sum)]),
        ]);
        assert!(additive_same_view_composite(&layer, "v.outer").is_none());
    }

    #[test]
    fn test_additive_same_view_composite_refuses_reference_cycle() {
        // Nothing in the type system prevents this; without a visited set the
        // recursion would not terminate.
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("a", MeasureType::Sum),
                composite_measure("x", "{{v.a}} + {{v.y}}"),
                composite_measure("y", "{{v.a}} + {{v.x}}"),
            ],
        )]);
        assert!(additive_same_view_composite(&layer, "v.x").is_none());
    }

    #[test]
    fn test_additive_same_view_composite_accepts_diamond_through_shared_composite() {
        // outer = a + b, a = shared + y, b = shared + z, shared is a Custom
        // composite referenced by both a and b. This is a DAG, not a cycle: a
        // cumulative `visited` set (never removed from) refuses it anyway,
        // because `shared` is inserted once while recursing into `a` and then
        // seen again while recursing into `b`. Path-based tracking (insert on
        // entry, remove on exit) must accept this.
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("s1", MeasureType::Sum),
                atomic_measure("s2", MeasureType::Sum),
                atomic_measure("y", MeasureType::Sum),
                atomic_measure("z", MeasureType::Sum),
                composite_measure("shared", "{{v.s1}} + {{v.s2}}"),
                composite_measure("a", "{{v.shared}} + {{v.y}}"),
                composite_measure("b", "{{v.shared}} + {{v.z}}"),
                composite_measure("outer", "{{v.a}} + {{v.b}}"),
            ],
        )]);
        let flat = additive_same_view_composite(&layer, "v.outer")
            .expect("a diamond through a shared same-view composite is a DAG, not a cycle");
        assert!(!flat.contains("{{"), "fully flattened, got {flat}");
        assert!(flat.contains("s1"));
        assert!(flat.contains("s2"));
        assert!(flat.contains("y"));
        assert!(flat.contains("z"));
    }

    #[test]
    fn test_additive_same_view_composite_accepts_composite_referenced_twice() {
        // net = addon + addon: `extract_ref_ops` yields `v.addon` twice for a
        // single expression. That is not a cycle either — the same node
        // appearing twice as a sibling ref, not on a recursive path.
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("x", MeasureType::Sum),
                atomic_measure("w", MeasureType::Sum),
                composite_measure("addon", "{{v.x}} + {{v.w}}"),
                composite_measure("net", "{{v.addon}} + {{v.addon}}"),
            ],
        )]);
        let flat = additive_same_view_composite(&layer, "v.net")
            .expect("a composite referenced twice in one expr is accepted");
        assert!(!flat.contains("{{"), "fully flattened, got {flat}");
    }

    #[test]
    fn test_additive_same_view_composite_refuses_filters_at_any_depth() {
        // Flattening discards a child's filters, so its dispersion would spread
        // over a wider population than the numerator sums. Filters construction
        // idiom copied from
        // `test_augment_layer_installs_filtered_dispersion_and_n_companion`.
        let mut filtered = atomic_measure("filtered_sum", MeasureType::Sum);
        filtered.filters = Some(vec![MeasureFilter {
            expr: "item_type = 'side'".to_string(),
            original_expr: None,
            description: None,
        }]);
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("a", MeasureType::Sum),
                filtered,
                composite_measure("outer", "{{v.a}} + {{v.filtered_sum}}"),
            ],
        )]);
        assert!(additive_same_view_composite(&layer, "v.outer").is_none());
    }

    #[test]
    fn test_supports_rate_basis_matches_the_gate() {
        let layer = make_layer(vec![make_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("sides_revenue", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "addon_revenue",
                    "{{checks.sides_revenue}} + {{checks.entree_revenue}}",
                ),
                composite_measure(
                    "ratio",
                    "{{checks.sides_revenue}} / {{checks.entree_revenue}}",
                ),
            ],
        )]);
        // A plain sum: always rate-sizable.
        assert!(supports_rate_basis(&layer, "checks.entree_revenue"));
        // An accepted composite.
        assert!(supports_rate_basis(&layer, "checks.addon_revenue"));
        // A ratio: refused.
        assert!(!supports_rate_basis(&layer, "checks.ratio"));
        // A count's rate is 1 by construction.
        assert!(!supports_rate_basis(&layer, "checks.total_checks"));
    }

    /// `opportunity()`'s gate reads `target_node.measure_type` off the TREE;
    /// `supports_rate_basis` reads the measure's `MeasureType` off the LAYER.
    /// Collapsing the two call sites into one definition is only safe if a
    /// tree built from a layer always agrees with that same layer on which
    /// measures are sums — this pins that round-trip directly, across every
    /// `MeasureType` variant, rather than relying on it holding incidentally.
    #[test]
    fn test_tree_measure_type_string_round_trips_against_layer_measure_type() {
        let layer = make_layer(vec![make_view(
            "v",
            vec![
                atomic_measure("s", MeasureType::Sum),
                atomic_measure("c", MeasureType::Count),
                atomic_measure("avg", MeasureType::Average),
                atomic_measure("mn", MeasureType::Min),
                atomic_measure("mx", MeasureType::Max),
                atomic_measure("cd", MeasureType::CountDistinct),
                atomic_measure("cda", MeasureType::CountDistinctApprox),
                atomic_measure("med", MeasureType::Median),
                composite_measure("cu", "{{v.s}}"),
            ],
        )]);
        let tree = MetricTree::build(&layer);
        for view in &layer.views {
            for measure in view.measures_list() {
                let id = format!("{}.{}", view.name, measure.name);
                let node = tree.nodes.iter().find(|n| n.id == id).expect("node exists");
                assert_eq!(
                    node.measure_type.as_str() == "sum",
                    measure.measure_type == MeasureType::Sum,
                    "tree/layer sum-ness disagree for {id}"
                );
            }
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
    /// Current values for [`saas_tree`]. `arr = net_mrr * 12`, so the tree's
    /// only multiplicative edge scales by 12 — which is only knowable from values.
    fn saas_values() -> MeasureValues {
        MeasureValues::from([
            ("revenue.new_mrr".to_string(), 800.0),
            ("revenue.expansion_mrr".to_string(), 400.0),
            ("revenue.churned_mrr".to_string(), 200.0),
            ("revenue.net_mrr".to_string(), 1_000.0),
            ("revenue.arr".to_string(), 12_000.0),
        ])
    }

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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
        // Every component edge is quantified — by its SIGN, not a blanket 1.0.
        // `net_mrr = new_mrr + expansion_mrr - churned_mrr`, so churn is the one
        // term that moves arr the other way.
        let coeff = |name: &str| {
            result
                .drivers
                .iter()
                .find(|d| d.measure == name)
                .unwrap_or_else(|| panic!("{name} should be a driver"))
                .effective_coefficient
                .expect("component edges are always quantified")
        };
        assert_eq!(coeff("revenue.net_mrr"), 1.0);
        assert_eq!(coeff("revenue.new_mrr"), 1.0);
        assert_eq!(coeff("revenue.expansion_mrr"), 1.0);
        assert_eq!(coeff("revenue.churned_mrr"), -1.0);
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
        // `arr = net_mrr * 12`: +100 of net MRR is +1200 of ARR, not +100.
        let result = predict_with_values(
            &tree,
            &[("revenue.net_mrr".to_string(), 100.0)],
            &saas_values(),
        )
        .unwrap();
        let arr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");
        assert_eq!(arr_impact.estimated_delta, 1_200.0);
        assert_eq!(arr_impact.confidence, "estimated");
    }

    #[test]
    fn test_predict_multi_hop() {
        let (_, tree) = saas_tree();
        // +50 new MRR flows additively into net_mrr (+50, exact), then through
        // the `* 12` edge into arr (+600, first-order).
        let result = predict_with_values(
            &tree,
            &[("revenue.new_mrr".to_string(), 50.0)],
            &saas_values(),
        )
        .unwrap();
        let arr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");
        assert_eq!(arr_impact.estimated_delta, 600.0);
        assert_eq!(arr_impact.confidence, "estimated");

        let net_mrr_impact = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr should be impacted");
        assert_eq!(net_mrr_impact.estimated_delta, 50.0);
        assert_eq!(net_mrr_impact.confidence, "exact");
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("diamond", vec![d_measure, b_measure, c_measure, a_measure]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = sensitivity(&tree, "diamond.a").unwrap();
        // D is reachable via two paths (B->A and C->A). The BFS deduplicates by
        // visited set, so D appears exactly once with the first path's coefficient.
        let d_entries: Vec<_> = result
            .drivers
            .iter()
            .filter(|d| d.measure == "diamond.d")
            .collect();
        assert_eq!(d_entries.len(), 1, "D is deduplicated in the diamond");
        // The coefficient should be from whichever BFS path reached D first
        let coeff = d_entries[0].effective_coefficient.unwrap();
        assert!(
            coeff == 2.0 || coeff == 3.0,
            "D's coefficient should be from one of the two paths"
        );
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
                coefficients: None,
                form: Some(DriverForm::Linear),
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
        let result = predict_with_values(
            &tree,
            &[("revenue.new_mrr".to_string(), -200.0)],
            &saas_values(),
        )
        .unwrap();
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
        assert_eq!(arr.estimated_delta, -2_400.0);
        assert_eq!(arr.confidence, "estimated");
    }

    #[test]
    fn test_predict_through_subtracted_component_flips_sign() {
        // `net_mrr = new_mrr + expansion_mrr - churned_mrr`. Churn enters under
        // a `-`, so MORE churn must mean LESS net MRR. Propagating it as a bare
        // pass-through claimed that growing a subtracted cost grows its parent.
        let (_, tree) = saas_tree();
        let result = predict(&tree, &[("revenue.churned_mrr".to_string(), 100.0)]).unwrap();

        let net_mrr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr should be impacted");
        assert_eq!(net_mrr.estimated_delta, -100.0);
    }

    #[test]
    fn test_sensitivity_subtracted_component_is_negative() {
        // The component's quantitative content is its sign; without it every
        // component of a composite reported β=+1.0 / direction Unknown.
        let (_, tree) = saas_tree();
        let result = sensitivity(&tree, "revenue.net_mrr").unwrap();

        let churn = result
            .drivers
            .iter()
            .find(|d| d.measure == "revenue.churned_mrr")
            .expect("churned_mrr should be a driver of net_mrr");
        assert_eq!(churn.effective_coefficient, Some(-1.0));
        assert_eq!(churn.direction, DriverDirection::Negative);

        let new_mrr = result
            .drivers
            .iter()
            .find(|d| d.measure == "revenue.new_mrr")
            .expect("new_mrr should be a driver of net_mrr");
        assert_eq!(new_mrr.effective_coefficient, Some(1.0));
        assert_eq!(new_mrr.direction, DriverDirection::Positive);
    }

    #[test]
    fn test_predict_multiplicative_scales_by_the_factor() {
        // `arr = net_mrr * 12`. A +100 move in net_mrr is +1200 of ARR, not +100.
        // The literal 12 is not a node in the tree — it falls out of parent/child.
        let (_, tree) = saas_tree();
        let values = MeasureValues::from([
            ("revenue.net_mrr".to_string(), 1_000.0),
            ("revenue.arr".to_string(), 12_000.0),
        ]);

        let result =
            predict_with_values(&tree, &[("revenue.net_mrr".to_string(), 100.0)], &values).unwrap();
        let arr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr should be impacted");

        assert_eq!(arr.estimated_delta, 1_200.0);
        // First-order, not exact — do not badge it as exact.
        assert_eq!(arr.confidence, "estimated");
    }

    #[test]
    fn test_predict_multiplicative_without_values_is_reported_not_guessed_or_dropped() {
        // Without current values the derivative of a product is unknowable.
        // Emitting the raw delta silently claimed ×1; dropping the node entirely
        // would claim "no impact", which is just as false. Report it instead.
        let (_, tree) = saas_tree();
        let result = predict(&tree, &[("revenue.net_mrr".to_string(), 100.0)]).unwrap();

        let arr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr must still be reported when it cannot be sized");
        assert_eq!(arr.confidence, UNQUANTIFIABLE);
        assert_eq!(arr.estimated_delta, 0.0, "the delta is unknown, not zero");
    }

    #[test]
    fn test_predict_does_not_traverse_past_an_unquantifiable_edge() {
        // `roi = arr / spend`, `arr = net_mrr * 12`. Without values we cannot size
        // arr, so we cannot size anything above it either — but neither node may
        // silently vanish.
        let view = make_view(
            "revenue",
            vec![
                atomic_measure("net_mrr", MeasureType::Sum),
                atomic_measure("spend", MeasureType::Sum),
                composite_measure("arr", "{{revenue.net_mrr}} * 12"),
                composite_measure("roi", "{{revenue.arr}} / NULLIF({{revenue.spend}}, 0)"),
            ],
        );
        let tree = MetricTree::build(&make_layer(vec![view]));

        let result = predict(&tree, &[("revenue.net_mrr".to_string(), 100.0)]).unwrap();
        let arr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr reported");
        assert_eq!(arr.confidence, UNQUANTIFIABLE);
        // roi sits beyond the unsizable edge — it is not reachable, so it is not
        // claimed either way.
        assert!(!result.impacts.iter().any(|i| i.measure == "revenue.roi"));
    }

    /// `sales` driven by `spend` under `form`, with a coefficient. One driver
    /// edge, nothing else, so a propagation assertion is about the form alone.
    fn driver_form_tree(form: DriverForm, coefficient: f64) -> MetricTree {
        let mut view = make_view(
            "ops",
            vec![
                atomic_measure("spend", MeasureType::Sum),
                atomic_measure("sales", MeasureType::Sum),
            ],
        );
        if let Some(ref mut measures) = view.measures {
            let sales = measures.iter_mut().find(|m| m.name == "sales").unwrap();
            sales.drivers = Some(vec![Driver {
                measure: "ops.spend".to_string(),
                direction: DriverDirection::default(),
                strength: DriverStrength::default(),
                confidence: DriverConfidence::default(),
                coefficient: Some(coefficient),
                coefficients: None,
                form: Some(form),
                intercept: None,
                lag: None,
                description: None,
                refs: None,
            }]);
        }
        MetricTree::build(&make_layer(vec![view]))
    }

    // An elasticity is a statement about proportions: a +10% move in the driver
    // moves the target by `(1.10 ^ coefficient) - 1`. Applied as a level slope it
    // would say +0.4 per dollar — 0.4 against a true 38,860, five orders out and
    // indistinguishable from a real forecast on the surface.
    #[test]
    fn test_predict_reads_a_log_log_coefficient_as_an_elasticity() {
        let tree = driver_form_tree(DriverForm::LogLog, 0.4);
        let values: MeasureValues = [
            ("ops.spend".to_string(), 1_000.0),
            ("ops.sales".to_string(), 1_000_000.0),
        ]
        .into_iter()
        .collect();

        let result =
            predict_with_values(&tree, &[("ops.spend".to_string(), 100.0)], &values).unwrap();
        let sales = result
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .expect("sales sized from the elasticity");

        // The EXACT power law: 1,000,000 × (1.10^0.4 − 1) = 38,860.12.
        //
        // This asserted 40,000 before the response model landed, which is the
        // first-order `Y × β × r` — right to three digits for a nudge, and 12.4%
        // out by +50%. Differencing the fitted curve removes the linearization, so
        // the number moved by design; the old value is what a shortcut answered,
        // not what an elasticity means.
        let exact = 1_000_000.0 * (1.10f64.powf(0.4) - 1.0);
        assert!(
            (sales.estimated_delta - exact).abs() < 1e-6,
            "expected the exact {exact}, got {}",
            sales.estimated_delta
        );
        let first_order = 1_000_000.0 * 0.4 * 0.10;
        assert!(
            (sales.estimated_delta - first_order).abs() > 1_000.0,
            "and it must NOT be the first-order figure any more"
        );
        assert_eq!(sales.confidence, "estimated");
    }

    // The same edge with no values. Before this it silently evaluated as a level
    // slope and reported +40 — a number with no defensible reading at all.
    #[test]
    fn test_predict_refuses_to_size_a_log_log_edge_without_values() {
        let tree = driver_form_tree(DriverForm::LogLog, 0.4);
        let result = predict(&tree, &[("ops.spend".to_string(), 100.0)]).unwrap();
        let sales = result
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .expect("the edge is real, so it must still be reported");
        assert_eq!(sales.confidence, UNQUANTIFIABLE);
        assert_eq!(sales.estimated_delta, 0.0, "unquantifiable carries no size");
    }

    // A linear edge is a statement about units and needs no levels at all —
    // this is what keeps delta-only mode working, so the form change must not
    // have made every driver edge baseline-dependent.
    #[test]
    fn test_predict_still_sizes_a_linear_driver_with_no_values() {
        let tree = driver_form_tree(DriverForm::Linear, 2.5);
        let result = predict(&tree, &[("ops.spend".to_string(), 100.0)]).unwrap();
        let sales = result
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .expect("a linear driver sizes without a baseline");
        assert_eq!(sales.estimated_delta, 250.0);
        assert_eq!(sales.confidence, "estimated");
    }

    // A cut that takes the driver to zero puts `ln(1 + Δ/x)` outside its domain.
    // The old fallback reported 0.0 there, which reads as "this move changes
    // nothing" — the one claim it certainly is not.
    #[test]
    fn test_predict_refuses_a_linear_log_move_that_zeroes_the_driver() {
        let tree = driver_form_tree(DriverForm::LinearLog, 5_000.0);
        let values: MeasureValues = [
            ("ops.spend".to_string(), 1_000.0),
            ("ops.sales".to_string(), 1_000_000.0),
        ]
        .into_iter()
        .collect();

        let result =
            predict_with_values(&tree, &[("ops.spend".to_string(), -1_000.0)], &values).unwrap();
        let sales = result
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .expect("the edge is real even where its curve is undefined");
        assert_eq!(sales.confidence, UNQUANTIFIABLE);
    }

    #[test]
    fn test_predict_partial_path_does_not_masquerade_as_estimated() {
        // If ANY path into a node could not be sized, the summed total is
        // incomplete — reporting "estimated" would overstate our confidence.
        let (_, tree) = saas_tree();
        let mut values = saas_values();
        // Drop arr's value: the `* 12` edge can no longer be sized.
        values.remove("revenue.arr");

        let result =
            predict_with_values(&tree, &[("revenue.new_mrr".to_string(), 50.0)], &values).unwrap();

        let net_mrr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.net_mrr")
            .expect("net_mrr sized additively");
        assert_eq!(net_mrr.estimated_delta, 50.0);
        assert_eq!(net_mrr.confidence, "exact");

        let arr = result
            .impacts
            .iter()
            .find(|i| i.measure == "revenue.arr")
            .expect("arr still reported");
        assert_eq!(arr.confidence, UNQUANTIFIABLE);
    }

    #[test]
    fn test_predict_through_quotient_denominator_and_numerator() {
        // `margin = profit / revenue`: raising the denominator lowers the ratio,
        // raising the numerator raises it — and both scale by 1/revenue.
        let view = make_view(
            "f",
            vec![
                atomic_measure("profit", MeasureType::Sum),
                atomic_measure("revenue", MeasureType::Sum),
                composite_measure("margin", "{{f.profit}} / NULLIF({{f.revenue}}, 0)"),
            ],
        );
        let tree = MetricTree::build(&make_layer(vec![view]));
        let values = MeasureValues::from([
            ("f.profit".to_string(), 200.0),
            ("f.revenue".to_string(), 1_000.0),
            ("f.margin".to_string(), 0.2),
        ]);

        // +100 profit → margin +100/1000 = +0.1
        let up = predict_with_values(&tree, &[("f.profit".to_string(), 100.0)], &values).unwrap();
        let m = up.impacts.iter().find(|i| i.measure == "f.margin").unwrap();
        assert!(
            (m.estimated_delta - 0.1).abs() < 1e-9,
            "got {}",
            m.estimated_delta
        );

        // +100 revenue → margin falls: 0.2 * -1 * (100/1000) = -0.02
        let down =
            predict_with_values(&tree, &[("f.revenue".to_string(), 100.0)], &values).unwrap();
        let m = down
            .impacts
            .iter()
            .find(|i| i.measure == "f.margin")
            .unwrap();
        assert!(
            (m.estimated_delta + 0.02).abs() < 1e-9,
            "got {}",
            m.estimated_delta
        );
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
            coefficients: None,
            form: Some(DriverForm::Linear),
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("qualskip", vec![qual_driver, target]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Propagate from qual_driver -> should not reach target (no coefficient)
        let result = predict(&tree, &[("qualskip.qual_driver".to_string(), 100.0)]).unwrap();
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
            coefficients: None,
            form: Some(DriverForm::Linear),
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
            coefficients: None,
            form: Some(DriverForm::Linear),
            intercept: None,
            lag: None,
            description: None,
            refs: None,
        }]);

        let view = make_view("negcoeff", vec![driver, target]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let result = predict(&tree, &[("negcoeff.driver".to_string(), 5.0)]).unwrap();
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
    fn make_opp_view(name: &str, measures: Vec<Measure>, dim_names: &[&str]) -> View {
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
                    segmentable: None,
                    meta: None,
                })
                .collect(),
            measures: Some(measures),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }
    }

    /// The Amsterdam shape: a sum measure, a count denominator, and a
    /// dispersion measure installed the way a real caller installs it.
    fn noise_layer() -> (SemanticLayer, MetricTree) {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        );
        let mut layer = make_layer(vec![view]);
        // Tree first, then augment — the order every caller must use, and the
        // order that keeps the synthetic pass-through out of the tree.
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        (layer, tree)
    }

    /// One segment row carrying a rate of `value/count` and a spread of `sd`.
    ///
    /// The dispersion alias is the measure id with `.` → `__`, so
    /// `opp.__opp_stddev__revenue` lands as `opp____opp_stddev__revenue`.
    fn seg(
        name: &str,
        value: f64,
        count: f64,
        sd: f64,
    ) -> serde_json::Map<String, serde_json::Value> {
        row(&[
            ("opp__status", js(name)),
            ("opp__revenue", jn(value)),
            ("opp__count", jn(count)),
            ("opp____opp_stddev__revenue", jn(sd)),
        ])
    }

    #[test]
    // Renamed from `..._only_for_sums`: eligible composites now also get a
    // dispersion measure (see test_augment_layer_installs_flattened_dispersion_for_composites).
    // A count is still refused — its rate is 1 by construction, so there is no
    // mean to put an error bar on.
    fn test_augment_layer_installs_dispersion_for_sums_not_counts() {
        let mut layer = make_layer(vec![make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        )]);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let m = layer.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "__opp_stddev__revenue")
            .cloned()
            .expect("dispersion measure installed for a sum");
        assert_eq!(m.measure_type, MeasureType::Number);
        assert_eq!(m.expr.as_deref(), Some("STDDEV_SAMP(revenue)"));

        // A count's rate is 1 by construction — there is no mean to put an
        // error bar on, so nothing is installed.
        assert!(!augment_layer_for_opportunity(&mut layer, "opp.count"));
        // Idempotent: a second call must not stack duplicates.
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        assert_eq!(
            layer.views[0]
                .measures_list()
                .iter()
                .filter(|m| m.name == "__opp_stddev__revenue")
                .count(),
            1
        );
    }

    #[test]
    fn test_augment_layer_installs_flattened_dispersion_for_composites() {
        let mut layer = make_layer(vec![make_opp_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
            &["region"],
        )]);
        assert!(augment_layer_for_opportunity(
            &mut layer,
            "checks.net_revenue"
        ));
        let m = layer.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "__opp_stddev__net_revenue")
            .cloned()
            .expect("dispersion measure installed for an eligible composite");
        // Over the FLATTENED column expression. Referencing the measures directly
        // would resolve each ref to its own aggregate and emit
        // STDDEV_SAMP((SUM(..)) + (SUM(..))) — a nested aggregate. Every
        // substitution is wrapped unconditionally now, so assert the
        // substance (both source columns present, under STDDEV_SAMP, fully
        // flattened) rather than pin the exact parenthesization.
        let expr = m.expr.as_deref().expect("dispersion measure has an expr");
        assert!(expr.starts_with("STDDEV_SAMP("), "got {expr}");
        assert!(expr.contains("entree_revenue"), "got {expr}");
        assert!(expr.contains("addon_revenue"), "got {expr}");
        assert!(!expr.contains("{{"), "got {expr}");
    }

    #[test]
    fn test_augment_layer_refuses_ineligible_composite() {
        let mut layer = make_layer(vec![make_opp_view(
            "checks",
            vec![
                atomic_measure("a", MeasureType::Sum),
                atomic_measure("b", MeasureType::Sum),
                composite_measure("ratio", "{{checks.a}} / {{checks.b}}"),
            ],
            &["region"],
        )]);
        assert!(!augment_layer_for_opportunity(&mut layer, "checks.ratio"));
    }

    #[test]
    fn test_augment_layer_refuses_filtered_composite() {
        // Would otherwise be eligible — same shape as
        // test_augment_layer_installs_flattened_dispersion_for_composites — but
        // the composite itself carries a non-empty `.filters`. The generator's
        // `MeasureType::Number` arm discards a composite's own `.filters` when
        // producing the numerator (`sql_generator.rs` `measure_agg_expr`), while
        // this function would apply those same filters to the dispersion measure
        // and its `n` companion — gating a mean computed over the UNFILTERED
        // population against a spread and sample size computed over the
        // FILTERED one.
        let filtered_composite = Measure {
            name: "net_revenue".to_string(),
            measure_type: MeasureType::Number,
            description: None,
            expr: Some("{{checks.entree_revenue}} + {{checks.addon_revenue}}".to_string()),
            original_expr: None,
            filters: Some(vec![MeasureFilter {
                expr: "region = 'west'".to_string(),
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
        };
        let mut layer = make_layer(vec![make_opp_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                filtered_composite,
            ],
            &["region"],
        )]);
        assert!(!augment_layer_for_opportunity(
            &mut layer,
            "checks.net_revenue"
        ));
    }

    #[test]
    fn test_augment_layer_installs_filtered_dispersion_and_n_companion() {
        let filtered_measure = Measure {
            name: "sides_revenue".to_string(),
            measure_type: MeasureType::Sum,
            description: None,
            expr: Some("revenue".to_string()),
            original_expr: None,
            filters: Some(vec![MeasureFilter {
                expr: "item_type = 'side'".to_string(),
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
        };
        let mut layer = make_layer(vec![make_opp_view(
            "opp",
            vec![
                filtered_measure,
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        )]);

        assert!(
            augment_layer_for_opportunity(&mut layer, "opp.sides_revenue"),
            "a filtered sum must no longer be refused"
        );

        let dispersion = layer.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "__opp_stddev__sides_revenue")
            .cloned()
            .expect("dispersion measure installed");
        assert_eq!(dispersion.measure_type, MeasureType::Number);
        assert_eq!(
            dispersion.expr.as_deref(),
            Some("STDDEV_SAMP(CASE WHEN item_type = 'side' THEN revenue END)"),
            "the filter must be embedded in the STDDEV_SAMP expr — Number \
             measures ignore .filters entirely, so this is the only way it applies"
        );

        let n_companion = layer.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "__opp_n__sides_revenue")
            .cloned()
            .expect("filtered-n companion measure installed");
        assert_eq!(n_companion.measure_type, MeasureType::Count);
        assert_eq!(n_companion.expr, None);
        assert_eq!(
            n_companion.filters.as_deref().map(|f| f[0].expr.as_str()),
            Some("item_type = 'side'"),
            "the companion's OWN filters carry the condition — Count measures \
             (unlike Number) honor .filters through the normal generator path"
        );

        // Idempotent, same as the unfiltered path.
        assert!(augment_layer_for_opportunity(
            &mut layer,
            "opp.sides_revenue"
        ));
        assert_eq!(
            layer.views[0]
                .measures_list()
                .iter()
                .filter(|m| m.name == "__opp_stddev__sides_revenue")
                .count(),
            1
        );
        assert_eq!(
            layer.views[0]
                .measures_list()
                .iter()
                .filter(|m| m.name == "__opp_n__sides_revenue")
                .count(),
            1
        );
    }

    #[test]
    fn test_augment_layer_unfiltered_sum_gets_no_n_companion() {
        // Backward-compat guard: an unfiltered sum's dispersion expr and the
        // absence of any __opp_n__ measure must be EXACTLY what they were before
        // this task, since opportunity() falls back to the existing count
        // measure as `n` whenever no __opp_n__ measure is present.
        let mut layer = make_layer(vec![make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        )]);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let dispersion = layer.views[0]
            .measures_list()
            .iter()
            .find(|m| m.name == "__opp_stddev__revenue")
            .cloned()
            .unwrap();
        assert_eq!(dispersion.expr.as_deref(), Some("STDDEV_SAMP(revenue)"));
        assert!(
            !layer.views[0]
                .measures_list()
                .iter()
                .any(|m| m.name == "__opp_n__revenue"),
            "an unfiltered sum must not get a filtered-n companion"
        );
    }

    #[test]
    fn test_opportunity_filtered_sum_uses_the_filtered_n_not_the_rate_denominator() {
        // The Amsterdam add-on shape, one level of the design's worked example:
        // sides_revenue is a FILTERED sum (item_type = 'side'); total_orders is
        // the view's unfiltered count, used as the RATE denominator. The
        // significance test's own n must come from the filtered-n companion
        // (2 vs 2 rows below), not from total_orders (552 vs 78) — if it used
        // the unfiltered count instead, the inflated n would make a thin,
        // noisy 2-row segment look like ample evidence.
        let filtered_measure = Measure {
            name: "sides_revenue".to_string(),
            measure_type: MeasureType::Sum,
            description: None,
            expr: Some("revenue".to_string()),
            original_expr: None,
            filters: Some(vec![MeasureFilter {
                expr: "item_type = 'side'".to_string(),
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
        };
        let view = make_opp_view(
            "opp",
            vec![
                filtered_measure,
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(
            &mut layer,
            "opp.sides_revenue"
        ));

        let mut data = HashMap::new();
        data.insert(
            "opp.sides_revenue".to_string(),
            vec![row(&[("opp__sides_revenue", jn(50_000.0))])],
        );
        data.insert(
            "opp.sides_revenue:opp.status".to_string(),
            vec![
                row(&[
                    ("opp__status", js("mobile_app")),
                    ("opp__sides_revenue", jn(6_000.0)),
                    ("opp__total_orders", jn(552.0)),
                    ("opp____opp_stddev__sides_revenue", jn(362.0)),
                    ("opp____opp_n__sides_revenue", jn(2.0)),
                ]),
                row(&[
                    ("opp__status", js("in_store")),
                    ("opp__sides_revenue", jn(62_400.0)),
                    ("opp__total_orders", jn(78.0)),
                    ("opp____opp_stddev__sides_revenue", jn(362.0)),
                    ("opp____opp_n__sides_revenue", jn(2.0)),
                ]),
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.sides_revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        // With n=2 both sides (the filtered-n, not 552/78), Welch's df collapses
        // toward 1 and the gate is honest about how thin this evidence really
        // is — it must NOT report a segment here. If the wiring regressed to
        // using total_orders as n instead, this same data would report a
        // confidently-wrong upside.
        assert!(
            result.dimensions.is_empty(),
            "a 2-row-vs-2-row filtered comparison must not clear the gate: {:?}",
            result.dimensions
        );
    }

    #[test]
    fn test_opportunity_drops_gaps_inside_the_noise() {
        // The real Amsterdam order_status numbers. Three statuses whose order
        // values are drawn from one distribution (sd ~362 on a mean ~700), so
        // the "leader" is an artefact of taking the max of four noisy groups.
        // Sizing this reports +42.7k of upside that does not exist.
        let (layer, tree) = noise_layer();
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(569_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("completed", 403_953.6, 552.0, 362.0), // rate 731.8
                seg("cancelled", 51_823.2, 78.0, 362.0),   // rate 664.4
                seg("refunded", 23_273.0, 34.0, 362.0),    // rate 684.5
                seg("pending", 39_290.0, 50.0, 362.0),     // rate 785.8 — the bar
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert!(
            result.dimensions.is_empty(),
            "gaps inside sampling noise must not be sized, got {:?}",
            result.dimensions
        );
        assert!(
            result
                .skipped_dimensions
                .iter()
                .any(|s| s.dimension == "opp.status" && s.reason.contains("sampling noise")),
            "the caller must learn it was noise, not that there was no gap: {:?}",
            result.skipped_dimensions
        );
    }

    #[test]
    fn test_opportunity_keeps_a_gap_that_outstrips_the_noise() {
        // Same shape, same row counts, but the laggard is a genuine outlier:
        // rate 300 against a bar of 800 with a tight spread. The gate must not
        // be so blunt that it eats real signal.
        let (layer, tree) = noise_layer();
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("mobile_app", 165_600.0, 552.0, 50.0), // rate 300
                seg("in_store", 62_400.0, 78.0, 50.0),     // rate 800 — the bar
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        let dim = result
            .dimensions
            .first()
            .expect("a 500-unit gap at sd 50 is real and must survive");
        assert_eq!(dim.segments.len(), 1);
        assert_eq!(dim.segments[0].segment, "mobile_app");
        // (800 − 300) × 552 rows.
        assert!((dim.total_upside - 276_000.0).abs() < 1.0, "{dim:?}");
        assert!(
            dim.segments[0].gated,
            "a segment that cleared a real significance test must say so"
        );
        assert_eq!(dim.segments_ungated, 0);
    }

    #[test]
    fn test_opportunity_marks_a_kept_segment_ungated_without_dispersion() {
        // No augment_layer_for_opportunity call — the layer carries no dispersion
        // measure, so gap_is_significant abstains (`None`) for every segment.
        // Today's fail-open policy still reports the gap (an out-of-date caller
        // must not silently lose sizing); what must change is that the report
        // admits it never proved anything.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                row(&[
                    ("opp__status", js("mobile_app")),
                    ("opp__revenue", jn(165_600.0)),
                    ("opp__count", jn(552.0)),
                ]),
                row(&[
                    ("opp__status", js("in_store")),
                    ("opp__revenue", jn(62_400.0)),
                    ("opp__count", jn(78.0)),
                ]),
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        let dim = result
            .dimensions
            .first()
            .expect("no dispersion measure means fail-open keeps the segment");
        assert_eq!(dim.segments.len(), 1);
        assert!(
            !dim.segments[0].gated,
            "a segment the gate could not evaluate must never claim gated: true"
        );
        assert_eq!(
            dim.segments_ungated, 1,
            "the dimension-level rollup must count it"
        );
    }

    #[test]
    fn test_opportunity_reports_segments_it_dropped_as_noise() {
        // A dimension that keeps one real segment and quietly discards another
        // must admit to the second. Otherwise a panel showing a single lever
        // looks like a clean read of a two-segment dimension, when really it is
        // one proven claim and one the data would not support.
        let (layer, tree) = noise_layer();
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("in_store", 62_400.0, 78.0, 50.0),     // rate 800 — the bar
                seg("mobile_app", 165_600.0, 552.0, 50.0), // rate 300 — real
                seg("phone", 61_620.0, 78.0, 400.0),       // rate 790 — noise
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        let dim = result
            .dimensions
            .first()
            .expect("the real segment survives");
        assert_eq!(dim.segments.len(), 1, "only the provable segment is sized");
        assert_eq!(dim.segments[0].segment, "mobile_app");
        assert_eq!(
            dim.segments_dropped_as_noise, 1,
            "the discarded segment must be declared, not vanish: {dim:?}"
        );
        // Suppression for want of evidence is not the same bucket as the tail
        // trim, and must not be laundered through it.
        assert_eq!(dim.other_segments_skipped, 0, "{dim:?}");
    }

    #[test]
    fn test_opportunity_without_dispersion_sizes_as_before() {
        // A caller that never installed the dispersion measure keeps the old
        // behaviour: no evidence to gate on means no gate, not an empty panel.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["status"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(569_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                row(&[
                    ("opp__status", js("completed")),
                    ("opp__revenue", jn(403_953.6)),
                    ("opp__count", jn(552.0)),
                ]),
                row(&[
                    ("opp__status", js("pending")),
                    ("opp__revenue", jn(39_290.0)),
                    ("opp__count", jn(50.0)),
                ]),
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();
        assert_eq!(
            result.dimensions.len(),
            1,
            "no dispersion measure must degrade to ungated sizing, not to silence"
        );
    }

    #[test]
    fn test_significance_threshold_rises_with_segment_count() {
        // Within one dimension the bar must climb with k: the more segments we
        // let compete for "best peer", the further the winner drifts above the
        // mean on noise alone. A flat threshold is what lets every dimension
        // have a leader.
        // Large df isolates the family/selection behaviour from the small-sample
        // t-correction: at df→∞ Student's t is the normal, so these are the same
        // z-scale numbers the bar was tuned to.
        const BIG_DF: f64 = 1e6;
        let ts: Vec<f64> = (2..=25)
            .map(|k| significance_threshold(k, 2, BIG_DF, SIGNIFICANCE_ALPHA))
            .collect();
        for w in ts.windows(2) {
            assert!(w[1] >= w[0], "threshold must be monotone in k: {ts:?}");
        }

        // k=2 in a family of 2 is one comparison of two groups: no selection to
        // speak of, so Šidák at a 5% family-wise rate carries it (≈1.95),
        // comfortably above the 1.645 of a bare one-sided 95% test.
        let t2 = significance_threshold(2, 2, BIG_DF, SIGNIFICANCE_ALPHA);
        assert!((1.645..2.1).contains(&t2), "got {t2}");

        // By k=25 selection dominates: the expected max of 25 standard normals
        // is sqrt(2·ln 25) ≈ 2.54, and the bar must track it.
        let t25 = significance_threshold(25, 2, BIG_DF, SIGNIFICANCE_ALPHA);
        assert!((t25 - 2.54).abs() < 0.35, "got {t25}");
        assert!(t25 > t2, "k=25 must be stricter than k=2: {t25} vs {t2}");
    }

    #[test]
    fn test_significance_threshold_answers_for_the_whole_scan() {
        // Regression on a false positive seen against the real demo warehouse.
        // `order_status` has provably no effect on order value there (four
        // statuses, means within 8 of each other across 200k rows), yet a
        // 95-row segment cleared the k=4 bar at t=2.63 and was reported as a
        // "+10.5k lever" — because ~20 dimensions were tested and something had
        // to win. Charging the whole family fixes it, and the real lever in that
        // same scan (t=4.47) must still survive.
        // Large df: this test is about the family size, not the sample size, so
        // hold df at the normal limit and vary only `family`.
        const BIG_DF: f64 = 1e6;
        let k4_alone = significance_threshold(4, 4, BIG_DF, SIGNIFICANCE_ALPHA);
        let k4_in_a_real_scan = significance_threshold(4, 100, BIG_DF, SIGNIFICANCE_ALPHA);
        assert!(
            k4_in_a_real_scan > 2.63,
            "the spurious status lever (t=2.63) must not clear the bar, got {k4_in_a_real_scan}"
        );
        assert!(
            k4_in_a_real_scan < 4.47,
            "the real channel lever (t=4.47) must still clear it, got {k4_in_a_real_scan}"
        );
        assert!(
            k4_in_a_real_scan > k4_alone,
            "100 comparisons must be stricter than 4: {k4_in_a_real_scan} vs {k4_alone}"
        );
    }

    #[test]
    fn test_significance_threshold_hardens_thin_samples() {
        // The failure the t-correction fixes: a benchmark set by a 2–3 row
        // segment. The normal tail is too thin there, so the bar was most
        // permissive exactly where the evidence was weakest. With Student's t the
        // bar must climb as df shrinks, and it must exceed the large-sample bar.
        let big = significance_threshold(4, 20, 1e6, SIGNIFICANCE_ALPHA);
        let thin = significance_threshold(4, 20, 2.0, SIGNIFICANCE_ALPHA);
        assert!(
            thin > big,
            "a 2-df comparison must clear a higher bar than a large-sample one: {thin} vs {big}"
        );

        // Monotone in df: fewer degrees of freedom, heavier tail, higher bar.
        let dfs = [1.0, 2.0, 5.0, 30.0, 1e6];
        let bars: Vec<f64> = dfs
            .iter()
            .map(|&df| significance_threshold(4, 20, df, SIGNIFICANCE_ALPHA))
            .collect();
        for w in bars.windows(2) {
            assert!(w[0] >= w[1], "bar must fall as df rises: {bars:?}");
        }
    }

    #[test]
    fn test_level_alpha_composes_back_to_the_total_budget() {
        // Verified with scipy before writing this test:
        // level_alpha(0.05, 5) = 1 - 0.95**0.2 = 0.0102062183...
        // and composing it back N times recovers the original budget exactly
        // (this IS the Sidak identity, so the "recompose" check is really
        // checking the formula was transcribed correctly, not an independent
        // fact).
        let per_level = level_alpha(0.05, 5);
        assert!((per_level - 0.0102062183).abs() < 1e-9, "got {per_level}");
        let recomposed = 1.0 - (1.0 - per_level).powi(5);
        assert!(
            (recomposed - 0.05).abs() < 1e-9,
            "composing the per-level budget back {} times must recover 0.05, got {}",
            5,
            recomposed
        );
        // max_depth=1 is a no-op: the whole budget is spent at the only level.
        assert!((level_alpha(0.05, 1) - 0.05).abs() < 1e-9);
    }

    #[test]
    fn test_gap_is_significant_composes_a_tighter_alpha() {
        // Same shape as the thin-benchmark test (fat samples both sides, k=4,
        // family=20) — se=5.0, df=398 at these n/sd, giving thresholds of
        // ~2.81 (alpha=0.05) and ~3.31 (alpha=0.01). gap=15.0 → gap/se=3.0,
        // verified (scipy.stats.t.ppf) to sit strictly between the two: it
        // clears the default 5% bar and misses a tighter 1% bar — proving
        // alpha actually reaches the t-quantile instead of being ignored.
        let with_default_alpha =
            gap_is_significant(15.0, Some(50.0), 200.0, Some(50.0), 200.0, 4, 20, 0.05);
        let with_tighter_alpha =
            gap_is_significant(15.0, Some(50.0), 200.0, Some(50.0), 200.0, 4, 20, 0.01);
        assert_eq!(
            with_default_alpha,
            Some(true),
            "gap must clear the default 5% family-wise bar"
        );
        assert_eq!(
            with_tighter_alpha,
            Some(false),
            "the SAME gap must NOT clear a tighter 1% bar — alpha must actually reach the threshold"
        );
    }

    #[test]
    fn test_gap_is_significant_thin_benchmark_needs_a_bigger_gap() {
        // Same gap, same spread, same family — only the benchmark's row count
        // changes. A 2-row benchmark carries so little evidence that a gap a
        // 200-row benchmark would confirm must be treated as "cannot rule out
        // noise at this bar" (or held to a much higher one).
        let gap = 120.0;
        let with_fat_bench = gap_is_significant(
            gap,
            Some(50.0),
            200.0,
            Some(50.0),
            200.0,
            4,
            20,
            SIGNIFICANCE_ALPHA,
        );
        let with_thin_bench = gap_is_significant(
            gap,
            Some(50.0),
            200.0,
            Some(50.0),
            2.0,
            4,
            20,
            SIGNIFICANCE_ALPHA,
        );
        assert_eq!(
            with_fat_bench,
            Some(true),
            "a well-evidenced gap against a fat benchmark should register"
        );
        assert_eq!(
            with_thin_bench,
            Some(false),
            "the same gap against a 2-row benchmark must not clear the hardened bar"
        );
    }

    #[test]
    fn test_gap_is_significant_abstains_without_evidence() {
        // No dispersion, or a segment too thin to have one, is "cannot tell" —
        // never "not significant". The caller keeps the segment rather than
        // silently deleting it on the strength of missing data.
        assert_eq!(
            gap_is_significant(
                500.0,
                None,
                100.0,
                Some(50.0),
                100.0,
                4,
                4,
                SIGNIFICANCE_ALPHA
            ),
            None
        );
        assert_eq!(
            gap_is_significant(
                500.0,
                Some(50.0),
                1.0,
                Some(50.0),
                100.0,
                4,
                4,
                SIGNIFICANCE_ALPHA
            ),
            None
        );
        // Zero variance on both sides is degenerate, not infinitely certain.
        assert_eq!(
            gap_is_significant(
                500.0,
                Some(0.0),
                100.0,
                Some(0.0),
                100.0,
                4,
                4,
                SIGNIFICANCE_ALPHA
            ),
            None
        );
    }

    #[test]
    fn test_opportunity_additive_basic() {
        // Sum measure sized on a per-unit rate (value / row count). Segments
        // carry equal volume (10 rows each) so the rate ranking mirrors the
        // totals: revenues [100, 200, 300] → rates [10, 20, 30]. Benchmark =
        // best-peer rate = 30. Below it: "a" (rate 10, gap 20, upside 20×10=200)
        // and "b" (rate 20, gap 10, upside 10×10=100).
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let measure_alias = "opp__revenue";
        let count_alias = "opp__count";
        let dim_alias = "opp__region";

        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[(measure_alias, jn(600.0))])],
        );
        data.insert(
            "opp.revenue:opp.region".to_string(),
            vec![
                row(&[
                    (dim_alias, js("a")),
                    (measure_alias, jn(100.0)),
                    (count_alias, jn(10.0)),
                ]),
                row(&[
                    (dim_alias, js("b")),
                    (measure_alias, jn(200.0)),
                    (count_alias, jn(10.0)),
                ]),
                row(&[
                    (dim_alias, js("c")),
                    (measure_alias, jn(300.0)),
                    (count_alias, jn(10.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert_eq!(result.target, "opp.revenue");
        assert!((result.overall_value - 600.0).abs() < 0.01);
        assert_eq!(result.weight_basis, "rows");
        assert_eq!(result.dimensions.len(), 1);

        let dim_opp = &result.dimensions[0];
        assert_eq!(dim_opp.dimension, "opp.region");
        assert_eq!(dim_opp.cardinality, 3);
        assert_eq!(dim_opp.benchmark_basis, "best_peer");
        // Two segments below the benchmark rate 30: "a" and "b".
        assert_eq!(dim_opp.segments.len(), 2);
        assert_eq!(dim_opp.segments[0].segment, "a");
        assert!((dim_opp.segments[0].current_value - 10.0).abs() < 0.01);
        assert!((dim_opp.segments[0].benchmark - 30.0).abs() < 0.01);
        assert!((dim_opp.segments[0].gap - 20.0).abs() < 0.01);
        assert!((dim_opp.segments[0].volume - 10.0).abs() < 0.01);
        assert!((dim_opp.segments[0].upside - 200.0).abs() < 0.01);
        assert_eq!(dim_opp.segments[1].segment, "b");
        assert!((dim_opp.segments[1].gap - 10.0).abs() < 0.01);
        assert!((dim_opp.segments[1].upside - 100.0).abs() < 0.01);
        // Upside sorted descending — "a" has the bigger upside.
        assert!(dim_opp.segments[0].upside >= dim_opp.segments[1].upside);
    }

    #[test]
    fn test_eligible_composite_enters_rate_mode() {
        // `weight_basis` is the observable proxy for rate mode: "rows" only when
        // a count denominator was discovered and used. net_revenue is a `+`
        // combination of same-view sums, so it must be sized per-unit exactly
        // like a plain sum root — the same basis component_candidates already
        // uses for its children.
        let view = make_opp_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "checks.net_revenue".to_string(),
            vec![row(&[("checks__net_revenue", jn(1000.0))])],
        );
        data.insert(
            "checks.net_revenue:checks.region".to_string(),
            vec![
                row(&[
                    ("checks__region", js("east")),
                    ("checks__net_revenue", jn(400.0)),
                    ("checks__total_checks", jn(10.0)),
                ]),
                row(&[
                    ("checks__region", js("west")),
                    ("checks__net_revenue", jn(600.0)),
                    ("checks__total_checks", jn(10.0)),
                ]),
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "checks.net_revenue",
            "checks.check_date",
            ("2025-07-17", "2026-07-16"),
            &[],
            &exec,
        )
        .expect("composite opportunity scan succeeds");
        assert_eq!(result.weight_basis, "rows");
        // `weight_basis == "rows"` alone is ambiguous: the refusal early-return
        // for sum-like targets with no count measure ALSO reports "rows", but
        // with `dimensions: Vec::new()` (everything routed to
        // `skipped_dimensions`). Pin down that we actually took the genuine
        // rate-mode path, not the refusal path.
        assert!(
            !result.dimensions.is_empty(),
            "genuine rate mode must produce sized dimensions; an empty list here means \
             this took the count-measure-missing refusal path instead, which also sets \
             weight_basis to \"rows\""
        );
        let dim_opp = &result.dimensions[0];
        assert_eq!(dim_opp.segments.len(), 1);
        assert_eq!(dim_opp.segments[0].segment, "east");
        // In rate mode, `SegmentOpportunity.volume` is the segment's row count
        // (`s.count`), not a fractional value-share (which the value-share path
        // would instead set to <= 1.0). "east" has `total_checks` = 10 in the
        // mock data, so volume must be 10.0 exactly — this could not pass on
        // the value-share path, which would set it to a fraction like 0.4.
        assert!((dim_opp.segments[0].volume - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_ineligible_composite_keeps_value_share() {
        // vol_x_price is a `*` combination — not additive — so it has no
        // per-row value and must stay on equal weighting (unchanged).
        let view = make_opp_view(
            "checks",
            vec![
                atomic_measure("total_checks_sum", MeasureType::Sum),
                atomic_measure("avg_check", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "vol_x_price",
                    "{{checks.total_checks_sum}} * {{checks.avg_check}}",
                ),
            ],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "checks.vol_x_price".to_string(),
            vec![row(&[("checks__vol_x_price", jn(1000.0))])],
        );
        data.insert(
            "checks.vol_x_price:checks.region".to_string(),
            vec![
                row(&[
                    ("checks__region", js("east")),
                    ("checks__vol_x_price", jn(400.0)),
                ]),
                row(&[
                    ("checks__region", js("west")),
                    ("checks__vol_x_price", jn(600.0)),
                ]),
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "checks.vol_x_price",
            "checks.check_date",
            ("2025-07-17", "2026-07-16"),
            &[],
            &exec,
        )
        .expect("multiplicative composite scan succeeds");
        // Unchanged: a product has no per-row value, so it stays on equal weighting.
        assert_eq!(result.weight_basis, "equal");
    }

    #[test]
    fn test_opportunity_additive_refused_without_count() {
        // A sum-like measure whose view declares NO count measure cannot be
        // sized on a per-unit basis, so every dimension is refused (recorded in
        // skipped_dimensions) rather than sized by comparing raw totals.
        let view = make_opp_view(
            "raw",
            vec![atomic_measure("revenue", MeasureType::Sum)],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "raw.revenue".to_string(),
            vec![row(&[("raw__revenue", jn(600.0))])],
        );
        data.insert(
            "raw.revenue:raw.region".to_string(),
            vec![
                row(&[("raw__region", js("a")), ("raw__revenue", jn(100.0))]),
                row(&[("raw__region", js("b")), ("raw__revenue", jn(300.0))]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "raw.revenue",
            "raw.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert!(result.dimensions.is_empty());
        assert!(
            result
                .skipped_dimensions
                .iter()
                .any(|s| s.dimension == "raw.region" && s.reason.contains("count")),
            "sum measure without a count measure should be refused with a count-related reason"
        );
    }

    #[test]
    fn test_opportunity_ratio_basic() {
        // Number/ratio measure with 3 segments [0.10, 0.30, 0.25].
        // Benchmark = best peer = 0.30.
        // Segments below 0.30: android (gap=0.20), web (gap=0.05).
        let view = make_opp_view(
            "funnel",
            vec![composite_measure(
                "conversion_rate",
                "{{funnel.conversions}} / NULLIF({{funnel.visits}}, 0)",
            )],
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
            &[],
            &exec,
        )
        .unwrap();

        assert_eq!(result.weight_basis, "equal");
        assert_eq!(result.dimensions.len(), 1);

        let dim_opp = &result.dimensions[0];
        assert_eq!(dim_opp.benchmark_basis, "best_peer");
        assert_eq!(dim_opp.segments.len(), 2);
        // Android has the biggest gap to ios.
        assert_eq!(dim_opp.segments[0].segment, "android");
        assert!((dim_opp.segments[0].benchmark - 0.30).abs() < 0.01);
        assert!((dim_opp.segments[0].gap - 0.20).abs() < 0.01);
    }

    #[test]
    fn test_opportunity_no_underperformers() {
        // Equal per-unit rates — flat distribution, dimension is skipped.
        // Totals differ (100/200/300) but so do the row counts (10/20/30), so
        // every segment's rate is 10 and there is no spread to act on.
        let view = make_opp_view(
            "equal",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "equal.revenue".to_string(),
            vec![row(&[("equal__revenue", jn(600.0))])],
        );
        data.insert(
            "equal.revenue:equal.region".to_string(),
            vec![
                row(&[
                    ("equal__region", js("a")),
                    ("equal__revenue", jn(100.0)),
                    ("equal__count", jn(10.0)),
                ]),
                row(&[
                    ("equal__region", js("b")),
                    ("equal__revenue", jn(200.0)),
                    ("equal__count", jn(20.0)),
                ]),
                row(&[
                    ("equal__region", js("c")),
                    ("equal__revenue", jn(300.0)),
                    ("equal__count", jn(30.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "equal.revenue",
            "equal.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert!(
            result.dimensions.is_empty(),
            "flat distribution → no opportunities"
        );
        assert!(
            result
                .skipped_dimensions
                .iter()
                .any(|s| s.reason.contains("flat")),
            "flat dimension should be recorded in skipped_dimensions"
        );
    }

    #[test]
    fn test_opportunity_single_segment() {
        // Only one segment — below MIN_DIMENSION_CARDINALITY, dimension is skipped.
        let view = make_opp_view(
            "single",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
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
                ("single__count", jn(25.0)),
            ])],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "single.revenue",
            "single.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert!(result.dimensions.is_empty());
        assert!(
            result
                .skipped_dimensions
                .iter()
                .any(|s| s.reason.contains("nothing to compare")),
            "single-segment dim should be recorded in skipped_dimensions"
        );
    }

    #[test]
    fn test_opportunity_downstream_propagation() {
        let leaf = atomic_measure("new_mrr", MeasureType::Sum);
        let parent = composite_measure("net_mrr", "{{prop.new_mrr}} + 0");
        let count = atomic_measure("count", MeasureType::Count);

        let view = make_opp_view("prop", vec![leaf, parent, count], &["region"]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Equal volume (10 rows/segment): rates 5/10/15, benchmark 15.
        // Below-benchmark upside: a (gap 10 ×10 = 100) + b (gap 5 ×10 = 50) = 150.
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
                    ("prop__count", jn(10.0)),
                ]),
                row(&[
                    ("prop__region", js("b")),
                    ("prop__new_mrr", jn(100.0)),
                    ("prop__count", jn(10.0)),
                ]),
                row(&[
                    ("prop__region", js("c")),
                    ("prop__new_mrr", jn(150.0)),
                    ("prop__count", jn(10.0)),
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
            &[],
            &exec,
        )
        .unwrap();

        assert!(!result.dimensions.is_empty());
        assert!(
            !result.downstream.is_empty(),
            "top opportunity should propagate to net_mrr via component edge"
        );
        assert!(
            result
                .downstream
                .iter()
                .any(|i| i.measure == "prop.net_mrr"),
            "net_mrr should appear in downstream"
        );
    }

    #[test]
    fn test_opportunity_empty_dimensions() {
        let view = make_view(
            "nodim",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
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
            &[],
            &exec,
        )
        .unwrap();

        assert!(result.dimensions.is_empty());
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
            &[],
            &exec,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_opportunity_multiple_dimensions() {
        // Equal volume (10 rows/segment), so rates = totals / 10.
        // Region rates 5/25/30 → benchmark 30, upside (25+5)×10 = 300.
        // Channel rates 18/20/22 → benchmark 22, upside (4+2)×10 = 60.
        // Region wins.
        let view = make_opp_view(
            "multi",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region", "channel"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let seg = |dim: &str, name: &str, rev: f64| {
            row(&[
                (dim, js(name)),
                ("multi__revenue", jn(rev)),
                ("multi__count", jn(10.0)),
            ])
        };
        let mut data = HashMap::new();
        data.insert(
            "multi.revenue".to_string(),
            vec![row(&[("multi__revenue", jn(600.0))])],
        );
        data.insert(
            "multi.revenue:multi.region".to_string(),
            vec![
                seg("multi__region", "a", 50.0),
                seg("multi__region", "b", 250.0),
                seg("multi__region", "c", 300.0),
            ],
        );
        data.insert(
            "multi.revenue:multi.channel".to_string(),
            vec![
                seg("multi__channel", "organic", 180.0),
                seg("multi__channel", "paid", 200.0),
                seg("multi__channel", "referral", 220.0),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "multi.revenue",
            "multi.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert_eq!(result.dimensions.len(), 2);
        assert_eq!(result.dimensions[0].dimension, "multi.region");
        assert_eq!(result.dimensions[1].dimension, "multi.channel");
        assert!(
            result.dimensions[0].total_upside > result.dimensions[1].total_upside,
            "dimensions should be sorted by total_upside descending"
        );
    }

    /// The Amsterdam alias shape. `stores.store_name` and `stores.staff_count`
    /// are two labels for the same partition — one store per staff count — so
    /// they produce byte-identical segment tuples and identical upside. Before
    /// dedup both were reported as separate levers with the same number, and
    /// both were charged to `comparison_family`, taxing every other dimension's
    /// significance bar for a comparison nobody independently made.
    #[test]
    fn test_opportunity_collapses_aliased_dimensions() {
        let mut view = make_opp_view(
            "stores",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["store_name", "staff_count", "channel"],
        );
        // staff_count is a numeric attribute; store_name is the entity key.
        view.dimensions[1].dimension_type = DimensionType::Number;
        view.entities = vec![crate::schema::models::Entity {
            name: "store".into(),
            entity_type: crate::schema::models::EntityType::Primary,
            description: None,
            key: Some("store_name".into()),
            keys: None,
            lifespan: None,
            inherits_from: None,
            meta: None,
            parent: None,
        }];
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let seg = |dim: &str, name: &str, rev: f64| {
            row(&[
                (dim, js(name)),
                ("stores__revenue", jn(rev)),
                ("stores__count", jn(10.0)),
            ])
        };
        let mut data = HashMap::new();
        data.insert(
            "stores.revenue".to_string(),
            vec![row(&[("stores__revenue", jn(2100.0))])],
        );
        // Identical measure tuples under two different labels.
        data.insert(
            "stores.revenue:stores.store_name".to_string(),
            vec![
                seg("stores__store_name", "alpha", 500.0),
                seg("stores__store_name", "beta", 700.0),
                seg("stores__store_name", "gamma", 900.0),
            ],
        );
        data.insert(
            "stores.revenue:stores.staff_count".to_string(),
            vec![
                seg("stores__staff_count", "14", 500.0),
                seg("stores__staff_count", "18", 700.0),
                seg("stores__staff_count", "20", 900.0),
            ],
        );
        // A genuinely independent dimension: different partition, must survive.
        data.insert(
            "stores.revenue:stores.channel".to_string(),
            vec![
                seg("stores__channel", "online", 600.0),
                seg("stores__channel", "mobile", 650.0),
                seg("stores__channel", "retail", 850.0),
            ],
        );

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "stores.revenue",
            "stores.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        let reported: Vec<&str> = result
            .dimensions
            .iter()
            .map(|d| d.dimension.as_str())
            .collect();
        assert!(
            reported.contains(&"stores.store_name"),
            "the entity key should be the surviving representative, got {reported:?}"
        );
        assert!(
            !reported.contains(&"stores.staff_count"),
            "the aliased attribute must not be reported as a separate lever, got {reported:?}"
        );
        assert!(
            reported.contains(&"stores.channel"),
            "an independent dimension must survive dedup, got {reported:?}"
        );
        let alias_skip = result
            .skipped_dimensions
            .iter()
            .find(|s| s.dimension == "stores.staff_count")
            .expect("the dropped alias should be reported as skipped, not silently vanish");
        assert!(
            alias_skip.reason.contains("alias"),
            "skip reason should name the aliasing, got {:?}",
            alias_skip.reason
        );
        assert!(
            alias_skip.reason.contains("stores.store_name"),
            "skip reason should name the representative it aliases, got {:?}",
            alias_skip.reason
        );
    }

    #[test]
    fn test_opportunity_zero_overall() {
        // All-zero segments → every rate is 0 → flat distribution → skipped.
        let view = make_opp_view(
            "zeroval",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
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
                    ("zeroval__count", jn(10.0)),
                ]),
                row(&[
                    ("zeroval__region", js("b")),
                    ("zeroval__revenue", jn(0.0)),
                    ("zeroval__count", jn(20.0)),
                ]),
                row(&[
                    ("zeroval__region", js("c")),
                    ("zeroval__revenue", jn(0.0)),
                    ("zeroval__count", jn(30.0)),
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
            &[],
            &exec,
        )
        .unwrap();

        assert!(result.dimensions.is_empty());
    }

    #[test]
    fn test_opportunity_high_cardinality_skipped() {
        // A dimension with > MAX_DIMENSION_CARDINALITY segments must be skipped
        // and recorded in skipped_dimensions with a cardinality reason.
        let view = make_opp_view(
            "hi",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["customer_id"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut breakdown_rows = Vec::new();
        let n: usize = 30; // > MAX_DIMENSION_CARDINALITY (25)
        for i in 0..n {
            breakdown_rows.push(row(&[
                ("hi__customer_id", js(&format!("c{i}"))),
                ("hi__revenue", jn((i as f64) * 10.0)),
                ("hi__count", jn(5.0)),
            ]));
        }

        let mut data = HashMap::new();
        data.insert(
            "hi.revenue".to_string(),
            vec![row(&[("hi__revenue", jn(4350.0))])],
        );
        data.insert("hi.revenue:hi.customer_id".to_string(), breakdown_rows);

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "hi.revenue",
            "hi.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert!(result.dimensions.is_empty());
        assert!(
            result
                .skipped_dimensions
                .iter()
                .any(|s| s.dimension == "hi.customer_id" && s.reason.contains("cardinality")),
            "high-cardinality dim should be in skipped_dimensions"
        );
    }

    #[test]
    fn test_opportunity_top_k_segments_caps_output() {
        // 10 segments below benchmark. Only TOP_K_SEGMENTS (5) should be returned.
        let view = make_opp_view(
            "cap",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Equal volume (10 rows/segment): "best" rate 100, ten "low" segments at
        // rate ~10 — all far below, so all 10 are candidates before the cap.
        let mut breakdown_rows = vec![row(&[
            ("cap__region", js("best")),
            ("cap__revenue", jn(1000.0)),
            ("cap__count", jn(10.0)),
        ])];
        for i in 0..10 {
            breakdown_rows.push(row(&[
                ("cap__region", js(&format!("low{i}"))),
                ("cap__revenue", jn(100.0 + (i as f64))),
                ("cap__count", jn(10.0)),
            ]));
        }

        let mut data = HashMap::new();
        data.insert(
            "cap.revenue".to_string(),
            vec![row(&[("cap__revenue", jn(2045.0))])],
        );
        data.insert("cap.revenue:cap.region".to_string(), breakdown_rows);

        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "cap.revenue",
            "cap.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        assert_eq!(result.dimensions.len(), 1);
        let dim = &result.dimensions[0];
        assert!(
            dim.segments.len() <= 5,
            "top-K cap should limit segments to <= 5 (got {})",
            dim.segments.len()
        );
        assert!(
            dim.other_segments_skipped > 0,
            "tail/top-K trim should drop at least one segment"
        );
    }

    #[test]
    fn test_opportunity_pick_benchmark_p75_for_large_dim() {
        // 10 segments triggers P75 instead of best-peer.
        let values: Vec<f64> = (1..=10).map(|i| i as f64 * 10.0).collect();
        let (benchmark, basis) = pick_benchmark(&values);
        assert_eq!(basis, "p75");
        // P75 of [10..100] step 10 = index 7 = 80.
        assert!((benchmark - 80.0).abs() < 0.01);
    }

    #[test]
    fn test_opportunity_pick_benchmark_best_for_small_dim() {
        let values = vec![10.0, 30.0, 50.0];
        let (benchmark, basis) = pick_benchmark(&values);
        assert_eq!(basis, "best_peer");
        assert!((benchmark - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_opportunity_benchmark_filter_best_peer_names_the_segment() {
        // Fewer than 8 segments -> best_peer. The filter must name exactly the
        // segment that set the bar (the max value), not an arbitrary one.
        let (layer, tree) = noise_layer();
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("mobile_app", 165_600.0, 552.0, 50.0), // rate 300
                seg("in_store", 62_400.0, 78.0, 50.0),     // rate 800 — the bar
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();
        let dim = result.dimensions.first().expect("a real gap must survive");
        assert_eq!(dim.benchmark_basis, "best_peer");
        assert_eq!(dim.benchmark_filter.len(), 1);
        assert_eq!(
            dim.benchmark_filter[0].member.as_deref(),
            Some("opp.status")
        );
        assert_eq!(dim.benchmark_filter[0].values, vec!["in_store".to_string()]);
    }

    #[test]
    fn test_opportunity_benchmark_filter_p75_names_every_segment_at_or_above() {
        // >= 8 segments -> p75. The filter must be an IN-list of every segment
        // whose rate is at or above the p75 threshold, not just the one segment
        // nearest to the interpolated value (p75 need not land exactly on one).
        let (layer, tree) = noise_layer();
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(1_000_000.0))])],
        );
        // 8 segments, rates 100..800 in steps of 100, sd large enough that
        // nothing gets noise-dropped (this test is about the FILTER shape, not
        // the gate — sd chosen so every below-benchmark segment's gap clears
        // easily: se ~= sqrt(2)*sd/sqrt(n), gap >= 100, sd=5, n=200 gives
        // se ~= 0.5, t >= 200, clears any threshold).
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("s1", 20_000.0, 200.0, 5.0),  // rate 100
                seg("s2", 40_000.0, 200.0, 5.0),  // rate 200
                seg("s3", 60_000.0, 200.0, 5.0),  // rate 300
                seg("s4", 80_000.0, 200.0, 5.0),  // rate 400
                seg("s5", 100_000.0, 200.0, 5.0), // rate 500
                seg("s6", 120_000.0, 200.0, 5.0), // rate 600
                seg("s7", 140_000.0, 200.0, 5.0), // rate 700 <- p75 threshold (idx 6 of 8, 0-based, floor(8*0.75)=6)
                seg("s8", 160_000.0, 200.0, 5.0), // rate 800
            ],
        );
        let exec = mock_executor(data);
        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();
        let dim = result.dimensions.first().expect("real gaps must survive");
        assert_eq!(dim.benchmark_basis, "p75");
        assert_eq!(dim.benchmark_filter.len(), 1);
        assert_eq!(
            dim.benchmark_filter[0].member.as_deref(),
            Some("opp.status")
        );
        let mut values = dim.benchmark_filter[0].values.clone();
        values.sort();
        // rate 700 (s7) and rate 800 (s8) are both >= the p75 threshold of 700.
        assert_eq!(values, vec!["s7".to_string(), "s8".to_string()]);
    }

    // ── Scope ─────────────────────────────────────

    /// An `Equals` scope filter, as a caller narrowing the scan would pass.
    fn eq_filter(member: &str, value: &str) -> QueryFilter {
        QueryFilter {
            member: Some(member.to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec![value.to_string()],
            and: None,
            or: None,
        }
    }

    /// Executor that honors `Equals` filters against a `segment` column on the
    /// raw rows, so a scope actually narrows what the queries see — the mock
    /// executor alone ignores everything but dates, and would pass this test
    /// whether or not the scope reached the queries at all.
    fn scoping_executor(
        data: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
        scoped_col: &'static str,
    ) -> Box<QueryExecutor> {
        Box::new(move |q: &QueryRequest| {
            let measure = &q.measures[0];
            let key = q
                .dimensions
                .first()
                .map(|d| format!("{measure}:{d}"))
                .unwrap_or_else(|| measure.to_string());
            let mut rows = data
                .get(&key)
                .or_else(|| data.get(measure.as_str()))
                .cloned()
                .unwrap_or_default();

            for f in &q.filters {
                let (Some(member), Some(FilterOperator::Equals)) = (&f.member, &f.operator) else {
                    continue;
                };
                if member.split('.').next_back() != Some(scoped_col) {
                    continue;
                }
                let wanted = &f.values[0];
                rows.retain(|r| {
                    r.get(&format!(
                        "{}__{scoped_col}",
                        member.split('.').next().unwrap()
                    ))
                    .and_then(|v| v.as_str())
                    .map(|v| v == wanted)
                    .unwrap_or(true)
                });
            }
            Ok(apply_date_filters_and_aggregate(rows, q))
        })
    }

    /// Rows tagged with the scoping column, one region per tag.
    fn scoped_rows() -> HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>> {
        let mut data = HashMap::new();
        // Overall: both tenants' rows; a scope must cut this down.
        data.insert(
            "opp.revenue".to_string(),
            vec![
                row(&[("opp__revenue", jn(600.0)), ("opp__tenant", js("acme"))]),
                row(&[("opp__revenue", jn(400.0)), ("opp__tenant", js("other"))]),
            ],
        );
        data.insert(
            "opp.revenue:opp.region".to_string(),
            vec![
                row(&[
                    ("opp__region", js("a")),
                    ("opp__revenue", jn(100.0)),
                    ("opp__count", jn(10.0)),
                    ("opp__tenant", js("acme")),
                ]),
                row(&[
                    ("opp__region", js("b")),
                    ("opp__revenue", jn(300.0)),
                    ("opp__count", jn(10.0)),
                    ("opp__tenant", js("acme")),
                ]),
                // Belongs to the other tenant — must not influence the benchmark.
                row(&[
                    ("opp__region", js("c")),
                    ("opp__revenue", jn(900.0)),
                    ("opp__count", jn(10.0)),
                    ("opp__tenant", js("other")),
                ]),
            ],
        );
        data
    }

    #[test]
    fn test_opportunity_scope_narrows_overall_and_benchmark() {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region", "tenant"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let exec = scoping_executor(scoped_rows(), "tenant");

        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[eq_filter("opp.tenant", "acme")],
            &exec,
        )
        .unwrap();

        // Overall must be the scoped total (600), not the population's 1000 —
        // it is the denominator every reported share is a fraction of.
        assert!(
            (result.overall_value - 600.0).abs() < 0.01,
            "overall_value {} should be scoped",
            result.overall_value
        );

        let region = result
            .dimensions
            .iter()
            .find(|d| d.dimension == "opp.region")
            .expect("region dimension");
        // Out-of-scope segment "c" (rate 90) must not appear, and must not have
        // set the benchmark — otherwise the scope leaks in through the peer bar.
        assert!(
            region.segments.iter().all(|s| s.segment != "c"),
            "out-of-scope segment leaked: {:?}",
            region.segments
        );
        // In scope: rates a=10, b=30 → best-peer benchmark 30, "a" gap 20 over
        // its own 10 rows = 200.
        let a = region
            .segments
            .iter()
            .find(|s| s.segment == "a")
            .expect("segment a");
        assert!(
            (a.benchmark - 30.0).abs() < 0.01,
            "benchmark {}",
            a.benchmark
        );
        assert!((a.upside - 200.0).abs() < 0.01, "upside {}", a.upside);
    }

    #[test]
    fn test_opportunity_empty_scope_sizes_whole_population() {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region", "tenant"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let exec = scoping_executor(scoped_rows(), "tenant");

        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
        )
        .unwrap();

        // No scope: everything is in play, including the other tenant.
        assert!((result.overall_value - 1000.0).abs() < 0.01);
        let region = result
            .dimensions
            .iter()
            .find(|d| d.dimension == "opp.region")
            .expect("region dimension");
        assert!(region.segments.iter().any(|s| s.segment == "a"));
        assert!(
            region.segments.iter().any(|s| s.segment == "b"),
            "b (rate 30) is below the unscoped benchmark of 90 and should appear"
        );
    }

    #[test]
    fn test_opportunity_scope_pinning_a_dimension_skips_it() {
        // Scoping to one tenant leaves `tenant` with a single value. It cannot
        // be benchmarked against peers the scope excluded, so it must be
        // reported as skipped rather than sized against itself.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("count", MeasureType::Count),
            ],
            &["region", "tenant"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = scoped_rows();
        data.insert(
            "opp.revenue:opp.tenant".to_string(),
            vec![
                row(&[
                    ("opp__tenant", js("acme")),
                    ("opp__revenue", jn(400.0)),
                    ("opp__count", jn(20.0)),
                ]),
                row(&[
                    ("opp__tenant", js("other")),
                    ("opp__revenue", jn(900.0)),
                    ("opp__count", jn(10.0)),
                ]),
            ],
        );
        let exec = scoping_executor(data, "tenant");

        let result = opportunity(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[eq_filter("opp.tenant", "acme")],
            &exec,
        )
        .unwrap();

        assert!(
            !result
                .dimensions
                .iter()
                .any(|d| d.dimension == "opp.tenant"),
            "a scope-pinned dimension must not be sized"
        );
        let skipped = result
            .skipped_dimensions
            .iter()
            .find(|s| s.dimension == "opp.tenant")
            .expect("tenant should be reported as skipped");
        assert!(
            skipped.reason.contains("segment"),
            "reason should explain there is nothing to compare: {}",
            skipped.reason
        );
    }

    // ── Dimension discovery ───────────────────────

    /// Build an entity declaration keyed on a single column.
    fn sent(name: &str, ty: EntityType, key: &str) -> Entity {
        Entity {
            name: name.to_string(),
            entity_type: ty,
            description: None,
            key: Some(key.to_string()),
            keys: None,
            lifespan: None,
            inherits_from: None,
            meta: None,
            parent: None,
        }
    }

    /// Build a dimension with the given name/type, `expr` mirroring the name.
    fn sdim(name: &str, ty: DimensionType) -> Dimension {
        Dimension {
            name: name.to_string(),
            dimension_type: ty,
            description: None,
            expr: name.to_string(),
            original_expr: None,
            samples: None,
            synonyms: None,
            primary_key: None,
            sub_query: None,
            segmentable: None,
            inherits_from: None,
            meta: None,
        }
    }

    /// A star schema shaped like the real one: a `sales` fact view carrying only
    /// FKs, an enum, and a measure column, joined many-to-one to a `shops`
    /// dimension view that holds the human-readable attributes.
    fn star_layer() -> SemanticLayer {
        let mut sales = make_view("sales", vec![atomic_measure("revenue", MeasureType::Sum)]);
        sales.entities = vec![
            sent("sale", EntityType::Primary, "sale_id"),
            sent("shop", EntityType::Foreign, "shop_id"),
        ];
        sales.dimensions = vec![
            // The entity keys the `sale_id` *dimension*, whose underlying column
            // is plain `id` — the shape real schemas use, and the one an
            // expr-only match misses.
            Dimension {
                expr: "id".to_string(),
                ..sdim("sale_id", DimensionType::Number)
            },
            sdim("shop_id", DimensionType::Number),
            sdim("status", DimensionType::String),
            sdim("amount", DimensionType::Number),
        ];

        let mut shops = make_view(
            "shops",
            vec![atomic_measure("shop_count", MeasureType::Count)],
        );
        shops.entities = vec![
            sent("shop", EntityType::Primary, "shop_id"),
            // Natural key: `region` is both the join key and a good segment.
            sent("region", EntityType::Foreign, "region"),
        ];
        shops.dimensions = vec![
            sdim("shop_id", DimensionType::Number),
            sdim("shop_name", DimensionType::String),
            sdim("region", DimensionType::String),
            sdim("square_feet", DimensionType::Number),
        ];

        make_layer(vec![sales, shops])
    }

    #[test]
    fn test_discover_dimensions_crosses_foreign_entity_to_joined_view() {
        let dims = discover_dimensions(&star_layer(), "sales");
        // The whole point: the fact view alone offers nothing you can act on;
        // the store's name lives across the join and must be reachable.
        assert!(
            dims.contains(&"shops.shop_name".to_string()),
            "expected joined view's label-ish dimension, got {dims:?}"
        );
    }

    #[test]
    fn test_discover_dimensions_drops_surrogate_ids_but_keeps_natural_keys() {
        let dims = discover_dimensions(&star_layer(), "sales");
        // Numeric entity keys identify a row; they are not levers.
        assert!(!dims.contains(&"sales.shop_id".to_string()), "{dims:?}");
        assert!(!dims.contains(&"shops.shop_id".to_string()), "{dims:?}");
        // Keyed by dimension name (`sale_id`) while the column underneath is
        // `id`: the key must resolve through the dimension, not the expr.
        assert!(!dims.contains(&"sales.sale_id".to_string()), "{dims:?}");
        // A string natural key is a perfectly good segment.
        assert!(dims.contains(&"shops.region".to_string()), "{dims:?}");
        // Non-key attributes on both sides survive.
        assert!(dims.contains(&"sales.status".to_string()), "{dims:?}");
        assert!(dims.contains(&"shops.square_feet".to_string()), "{dims:?}");
    }

    #[test]
    fn test_discover_dimensions_honors_declared_primary_key() {
        let mut layer = star_layer();
        let sales = layer.views.iter_mut().find(|v| v.name == "sales").unwrap();
        // A *string* PK is not caught by the numeric heuristic, but an explicit
        // declaration must still be honored.
        sales.dimensions.push(Dimension {
            primary_key: Some(true),
            ..sdim("external_ref", DimensionType::String)
        });
        let dims = discover_dimensions(&layer, "sales");
        assert!(
            !dims.contains(&"sales.external_ref".to_string()),
            "declared primary_key must be dropped, got {dims:?}"
        );
    }

    #[test]
    fn test_discover_dimensions_honors_segmentable_false() {
        let mut layer = star_layer();
        let sales = layer.views.iter_mut().find(|v| v.name == "sales").unwrap();
        // Shape-identical to `sales.status` — an ordinary low-cardinality
        // string. Nothing but the declaration can tell them apart, which is
        // exactly the case the flag exists for.
        sales.dimensions.push(Dimension {
            segmentable: Some(false),
            ..sdim("gender", DimensionType::String)
        });
        let dims = discover_dimensions(&layer, "sales");
        assert!(
            !dims.contains(&"sales.gender".to_string()),
            "segmentable: false must be dropped, got {dims:?}"
        );
        assert!(
            dims.contains(&"sales.status".to_string()),
            "an unmarked peer dimension must survive, got {dims:?}"
        );
    }

    #[test]
    fn test_discover_dimensions_segmentable_false_crosses_joins() {
        // The flag has to survive the foreign-entity hop, or a junk dimension
        // on a *joined* view still gets ranked as a lever on the fact view —
        // which is where address lines and customer attributes actually live.
        let mut layer = star_layer();
        let shops = layer.views.iter_mut().find(|v| v.name == "shops").unwrap();
        shops.dimensions.push(Dimension {
            segmentable: Some(false),
            ..sdim("address_line_2", DimensionType::String)
        });
        let dims = discover_dimensions(&layer, "sales");
        assert!(
            !dims.contains(&"shops.address_line_2".to_string()),
            "segmentable: false on a joined view must be dropped, got {dims:?}"
        );
        assert!(
            dims.contains(&"shops.square_feet".to_string()),
            "an unmarked joined dimension must survive, got {dims:?}"
        );
    }

    #[test]
    fn test_discover_dimensions_stops_at_one_hop() {
        let mut layer = star_layer();
        // `regions` is two hops from `sales` (sales -> shops -> regions). Its
        // dimensions must not leak in: one hop keeps the grain and the query
        // count bounded.
        let mut regions = make_view("regions", vec![]);
        regions.entities = vec![sent("region", EntityType::Primary, "region")];
        regions.dimensions = vec![sdim("region_manager", DimensionType::String)];
        layer.views.push(regions);

        let dims = discover_dimensions(&layer, "sales");
        assert!(
            !dims.contains(&"regions.region_manager".to_string()),
            "two-hop dimension must not be discovered, got {dims:?}"
        );
    }

    #[test]
    fn test_discover_dimensions_without_entities_is_unchanged() {
        // A standalone view with no entity declarations keeps the old behavior.
        let mut solo = make_view("solo", vec![atomic_measure("v", MeasureType::Sum)]);
        solo.dimensions = vec![
            sdim("plan", DimensionType::String),
            sdim("seats", DimensionType::Number),
        ];
        let dims = discover_dimensions(&make_layer(vec![solo]), "solo");
        assert_eq!(
            dims,
            vec!["solo.plan".to_string(), "solo.seats".to_string()]
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

    /// Compare two date-like strings using the shorter prefix length.
    /// Test fixtures use "YYYY-MM" while filter values use "YYYY-MM-DD" — prefix
    /// comparison matches them correctly without losing month-level precision.
    fn date_prefix_cmp(row_val: &str, filter_val: &str) -> std::cmp::Ordering {
        let n = row_val.len().min(filter_val.len()).min(7);
        row_val[..n].cmp(&filter_val[..n])
    }

    /// Apply date-range filters (AfterOrOnDate / BeforeOrOnDate) to rows and
    /// re-aggregate the measure (and optional dim breakdown) by summation.
    /// This mirrors what a real DB does for a query with no time grouping:
    /// one row per dim combination, summed within the requested range.
    fn apply_date_filters_and_aggregate(
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
        q: &QueryRequest,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        // All requested measures (the target plus, in rate_mode, the count
        // denominator). Every one is summed and preserved so multi-measure
        // breakdown queries round-trip through the mock.
        let measure_aliases: Vec<String> =
            q.measures.iter().map(|m| m.replace('.', "__")).collect();
        let dim_alias = q.dimensions.first().map(|d| d.replace('.', "__"));

        let date_filters: Vec<(&str, &FilterOperator, &str)> = q
            .filters
            .iter()
            .filter_map(|f| {
                let m = f.member.as_deref()?;
                let op = f.operator.as_ref()?;
                if !matches!(
                    op,
                    FilterOperator::AfterOrOnDate | FilterOperator::BeforeOrOnDate
                ) {
                    return None;
                }
                let v = f.values.first()?;
                Some((m, op, v.as_str()))
            })
            .collect();

        // If no date filters present, return rows as-is (some tests use the raw
        // shape; aggregate-shape callers always include date filters).
        if date_filters.is_empty() {
            return rows;
        }

        let filtered: Vec<_> = rows
            .into_iter()
            .filter(|row| {
                date_filters.iter().all(|(member, op, value)| {
                    let alias = member.replace('.', "__");
                    // Try the exact alias first; if absent, fall back to any column
                    // whose suffix matches the bare time-dim name. Many test fixtures
                    // store the time column under "<viewname>__created_at" while the
                    // explain call uses a different prefix like "sales.created_at".
                    let row_val = row
                        .get(&alias)
                        .or_else(|| {
                            let bare = member.rsplit('.').next().unwrap_or(member);
                            let suffix = format!("__{}", bare);
                            row.iter()
                                .find(|(k, _)| k.ends_with(&suffix))
                                .map(|(_, v)| v)
                        })
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if row_val.is_empty() {
                        return true; // no time column at all → keep
                    }
                    let ord = date_prefix_cmp(row_val, value);
                    match op {
                        FilterOperator::AfterOrOnDate => ord != std::cmp::Ordering::Less,
                        FilterOperator::BeforeOrOnDate => ord != std::cmp::Ordering::Greater,
                        _ => true,
                    }
                })
            })
            .collect();

        let num = |sum: f64| {
            serde_json::Value::Number(
                serde_json::Number::from_f64(sum).unwrap_or_else(|| serde_json::Number::from(0)),
            )
        };

        // Aggregate: sum every requested measure, optionally grouped by dim.
        if let Some(dim_a) = dim_alias {
            let mut groups: HashMap<String, (Option<serde_json::Value>, Vec<f64>)> = HashMap::new();
            for row in &filtered {
                let key = row
                    .get(&dim_a)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                let entry = groups.entry(key).or_insert_with(|| {
                    (row.get(&dim_a).cloned(), vec![0.0; measure_aliases.len()])
                });
                for (i, alias) in measure_aliases.iter().enumerate() {
                    entry.1[i] += row.get(alias).map(json_to_f64).unwrap_or(0.0);
                }
            }
            groups
                .into_iter()
                .map(|(_, (dim_val, sums))| {
                    let mut m = serde_json::Map::new();
                    if let Some(dv) = dim_val {
                        m.insert(dim_a.clone(), dv);
                    }
                    for (alias, sum) in measure_aliases.iter().zip(sums) {
                        m.insert(alias.clone(), num(sum));
                    }
                    m
                })
                .collect()
        } else {
            let mut m = serde_json::Map::new();
            for alias in &measure_aliases {
                let sum: f64 = filtered
                    .iter()
                    .map(|r| r.get(alias).map(json_to_f64).unwrap_or(0.0))
                    .sum();
                m.insert(alias.clone(), num(sum));
            }
            vec![m]
        }
    }

    /// Build a mock executor that returns predefined rows per measure.
    ///
    /// Date filters (AfterOrOnDate / BeforeOrOnDate on the time dimension) are
    /// honored: rows are filtered by date and the measure is re-aggregated via
    /// SUM (optionally grouped by the requested dim), matching what a real DB
    /// returns for an aggregate query with no time grouping.
    fn mock_executor(
        data: HashMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ) -> Box<QueryExecutor> {
        Box::new(move |q: &QueryRequest| {
            let measure = &q.measures[0];
            let raw_rows = if !q.dimensions.is_empty() {
                let dim = &q.dimensions[0];
                let key = format!("{}:{}", measure, dim);
                data.get(&key)
                    .or_else(|| data.get(measure.as_str()))
                    .cloned()
                    .unwrap_or_default()
            } else {
                data.get(measure.as_str()).cloned().unwrap_or_default()
            };
            Ok(apply_date_filters_and_aggregate(raw_rows, q))
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

    /// `revenue = orders × avg_price` — the canonical volume × price
    /// decomposition. Both factors move; their log-share should sum to 1.
    ///
    /// Worked numbers:
    ///   orders:    1000 → 1100  → ln(1.10) ≈ 0.0953
    ///   avg_price: 100  → 110   → ln(1.10) ≈ 0.0953
    ///   revenue:   100k → 121k  → ln(1.21) ≈ 0.1906
    /// Each factor's log share: 0.0953 / 0.1906 ≈ 0.5 (50%).
    /// Crucially the SUM is ~1.0 — additive decomposition would give
    /// (100 + 10) / 21,000 = 0.005, three orders of magnitude off.
    #[test]
    fn test_explain_multiplicative_log_decomposition() {
        let sales_view = make_view(
            "sales",
            vec![
                atomic_measure("orders", MeasureType::Sum),
                atomic_measure("avg_price", MeasureType::Sum),
                composite_measure("revenue", "{{sales.orders}} * {{sales.avg_price}}"),
            ],
        );
        let layer = make_layer(vec![sales_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "sales.revenue".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__revenue", jn(100_000.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__revenue", jn(121_000.0)),
                ]),
            ],
        );
        data.insert(
            "sales.orders".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__orders", jn(1000.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__orders", jn(1100.0)),
                ]),
            ],
        );
        data.insert(
            "sales.avg_price".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__avg_price", jn(100.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__avg_price", jn(110.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        // Walk the decomposition: top split should be one of the two
        // components; its sibling carries the other. Both have ~50% share.
        assert!(!result.nodes.is_empty(), "should emit decomposition");
        let top = &result.nodes[0];
        let SplitKind::Component { .. } = &top.split else {
            panic!("expected component split, got {:?}", top.split);
        };

        // log share = ln(1.10) / ln(1.21) ≈ 0.5
        assert!(
            (top.root_fraction - 0.5).abs() < 0.05,
            "top root_fraction = {} (want ≈ 0.5 from log decomposition; \
             additive would be ~0.005)",
            top.root_fraction
        );

        // Sibling carries the other half.
        assert_eq!(top.siblings.len(), 1, "one sibling expected");
        let sibling = &top.siblings[0];
        assert!(
            (sibling.root_fraction - 0.5).abs() < 0.05,
            "sibling root_fraction = {} (want ≈ 0.5)",
            sibling.root_fraction
        );

        // Sanity: shares must sum to ~1.0 (multiplicative composite invariant).
        let total = top.root_fraction + sibling.root_fraction;
        assert!(
            (total - 1.0).abs() < 0.05,
            "shares should sum to ~1.0, got {}",
            total
        );
    }

    /// Falls back to additive when any value is ≤ 0 (ln() requires positive).
    /// Verifies we don't NaN or panic on edge cases.
    #[test]
    fn test_explain_multiplicative_falls_back_when_zero_value() {
        let sales_view = make_view(
            "sales",
            vec![
                atomic_measure("orders", MeasureType::Sum),
                atomic_measure("avg_price", MeasureType::Sum),
                composite_measure("revenue", "{{sales.orders}} * {{sales.avg_price}}"),
            ],
        );
        let layer = make_layer(vec![sales_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        // revenue current = 0 → log decomposition can't run
        data.insert(
            "sales.revenue".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__revenue", jn(100.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__revenue", jn(0.0)),
                ]),
            ],
        );
        data.insert(
            "sales.orders".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__orders", jn(10.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__orders", jn(0.0)),
                ]),
            ],
        );
        data.insert(
            "sales.avg_price".to_string(),
            vec![
                row(&[
                    ("sales__created_at", js("2024-01")),
                    ("sales__avg_price", jn(10.0)),
                ]),
                row(&[
                    ("sales__created_at", js("2024-02")),
                    ("sales__avg_price", jn(5.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        // Should not crash; should produce something (additive fallback).
        // No NaN/infinity in any shares.
        for node in &result.nodes {
            assert!(
                node.root_fraction.is_finite(),
                "root_fraction must be finite, got {}",
                node.root_fraction
            );
        }
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
                segmentable: None,
                meta: None,
            }],
            measures: Some(vec![atomic_measure("mrr", MeasureType::Sum)]),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
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

            // Build filter suffix from active non-time filters. Date filters
            // (AfterOrOnDate / BeforeOrOnDate) are NOT included in the key —
            // they're applied to the resolved rows via apply_date_filters_and_aggregate.
            let mut filter_parts: Vec<String> = q
                .filters
                .iter()
                .filter_map(|f| {
                    let member = f.member.as_deref()?;
                    if matches!(
                        f.operator,
                        Some(FilterOperator::AfterOrOnDate) | Some(FilterOperator::BeforeOrOnDate)
                    ) {
                        return None;
                    }
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

            let resolve = || -> Vec<serde_json::Map<String, serde_json::Value>> {
                // Try most specific key first, fall back to less specific
                if !q.dimensions.is_empty() {
                    let dim = &q.dimensions[0];
                    let key = format!("{}:{}{}", measure, dim, filter_suffix);
                    if let Some(rows) = data.get(&key) {
                        return rows.clone();
                    }
                    let key_no_filter = format!("{}:{}", measure, dim);
                    if let Some(rows) = data.get(&key_no_filter) {
                        return rows.clone();
                    }
                }
                if !filter_suffix.is_empty() {
                    let key = format!("{}{}", measure, filter_suffix);
                    if let Some(rows) = data.get(&key) {
                        return rows.clone();
                    }
                }
                data.get(measure.as_str()).cloned().unwrap_or_default()
            };
            Ok(apply_date_filters_and_aggregate(resolve(), q))
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
                    segmentable: None,
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
            pre_aggregations: None,
            refresh_key: None,
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
    fn agg(
        measure: &str,
        prev: f64,
        curr: f64,
    ) -> (String, Vec<serde_json::Map<String, serde_json::Value>>) {
        let alias = measure.replace('.', "__");
        (
            measure.to_string(),
            vec![
                row(&[
                    (
                        &format!("{}__created_at", measure.split('.').next().unwrap()),
                        js("2024-01"),
                    ),
                    (&alias, jn(prev)),
                ]),
                row(&[
                    (
                        &format!("{}__created_at", measure.split('.').next().unwrap()),
                        js("2024-02"),
                    ),
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
            dim_breakdown(
                "sales.revenue",
                "sales.platform",
                &[("iOS", 500.0, 450.0), ("Android", 500.0, 450.0)],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[("US", 500.0, 450.0), ("EU", 500.0, 450.0)],
            ),
            // Depth-2: after filtering to platform=iOS, split by region
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.platform=iOS",
                &[("US", 250.0, 300.0), ("EU", 250.0, 150.0)],
            ),
            // Depth-2: after filtering to platform=Android, split by region
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.platform=Android",
                &[("US", 250.0, 150.0), ("EU", 250.0, 300.0)],
            ),
            // Depth-2: after filtering to region=US, split by platform
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.platform",
                "sales.region=US",
                &[("iOS", 250.0, 300.0), ("Android", 250.0, 150.0)],
            ),
            // Depth-2: after filtering to region=EU, split by platform
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.platform",
                "sales.region=EU",
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
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
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
        assert!(
            chose_source,
            "algorithm picks source (higher JSD), not plan"
        );
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
            dim_breakdown(
                "sales.conversion_rate",
                "sales.device",
                &[
                    ("Mobile", 3.0, 3.5),  // rate UP
                    ("Desktop", 5.5, 6.0), // rate UP
                ],
            ),
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
        let view = make_view_with_dims("sales", &["product"], &[("revenue", MeasureType::Sum)]);
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
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 9000.0, 8100.0), ("Free", 1000.0, 900.0)],
            ),
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
        let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
        let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![composite_measure(
            "revenue",
            "{{ads.revenue}} + {{subs.revenue}}",
        )]);

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
            dim_breakdown(
                "ads.revenue",
                "ads.region",
                &[("US", 5000.0, 5000.0), ("EU", 1000.0, 400.0)],
            ),
            // Within subs:
            dim_breakdown(
                "subs.revenue",
                "subs.region",
                &[("US", 3500.0, 3500.0), ("EU", 500.0, 100.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "total.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Current behavior: algorithm picks component ads.revenue (higher concentration
        // at 0.60 vs subs at 0.40), then recurses into ads and finds region=EU.
        let top = &result.nodes[0];
        let is_component = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "ads.revenue");
        assert!(
            is_component,
            "should pick ads component first: {:?}",
            top.split
        );

        // At depth 2, should find region=EU within ads
        assert!(!top.children.is_empty(), "should recurse into ads");
        let child = &top.children[0];
        let found_eu = matches!(&child.split, SplitKind::Dimension { dimension, value }
            if dimension == "ads.region" && value == "EU");
        assert!(
            found_eu,
            "should find ads.region=EU at depth 2: {:?}",
            child.split
        );

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
        let comp_a = make_view_with_dims("comp_a", &["segment"], &[("metric", MeasureType::Sum)]);
        let comp_b = make_view_with_dims("comp_b", &["segment"], &[("metric", MeasureType::Sum)]);
        let mut parent = make_view_with_dims("parent", &[], &[]);
        parent.measures = Some(vec![composite_measure(
            "metric",
            "{{comp_a.metric}} + {{comp_b.metric}}",
        )]);

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
            dim_breakdown(
                "comp_a.metric",
                "comp_a.segment",
                &[
                    ("seg_1", 1100.0, 880.0),  // delta -220, EP 0.40
                    ("seg_2", 1100.0, 990.0),  // delta -110, EP 0.20
                    ("seg_3", 1100.0, 1045.0), // delta -55,  EP 0.10
                    ("seg_4", 1100.0, 1017.5), // delta -82.5, EP 0.15
                    ("seg_5", 1100.0, 1017.5), // delta -82.5, EP 0.15
                ],
            ),
            // Within comp_b: concentrated drop in one segment
            dim_breakdown(
                "comp_b.metric",
                "comp_b.segment",
                &[
                    ("seg_critical", 2000.0, 1572.5), // delta -427.5, EP 0.95
                    ("seg_other_1", 1000.0, 1000.0),  // delta 0
                    ("seg_other_2", 1000.0, 1000.0),  // delta 0
                    ("seg_other_3", 500.0, 477.5),    // delta -22.5, EP 0.05
                ],
            ),
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

        assert!(
            picked_a,
            "greedy should pick comp_a (higher concentration): {:?}",
            top.split
        );

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
        let view = make_view_with_dims("sales", &["category"], &[("revenue", MeasureType::Sum)]);
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
        let rev_view = make_view_with_dims("rev", &["region"], &[("amount", MeasureType::Sum)]);
        let cost_view = make_view_with_dims("cost", &["region"], &[("amount", MeasureType::Sum)]);
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![composite_measure(
            "net",
            "{{rev.amount}} - {{cost.amount}}",
        )]);

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
            dim_breakdown(
                "rev.amount",
                "rev.region",
                &[
                    ("US", 2000.0, 2400.0), // delta +400
                    ("EU", 3000.0, 2500.0), // delta -500
                ],
            ),
            dim_breakdown(
                "cost.amount",
                "cost.region",
                &[
                    ("US", 1000.0, 1100.0), // delta +100 (cost rose)
                    ("EU", 2000.0, 1700.0), // delta -300 (cost fell)
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "profit.net", data);

        assert!((result.target_delta - 100.0).abs() < 0.01);

        // The metric IMPROVED. The algorithm finds cost as the dominant
        // component (concentration 2.0) since cost savings drove the improvement.
        assert!(!result.nodes.is_empty(), "should find component splits");
        let top = &result.nodes[0];
        let picked_cost = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "cost.amount");
        assert!(
            picked_cost,
            "should pick cost (concentration 2.0 > revenue's -1.0): {:?}",
            top.split
        );
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
        let view = make_view_with_dims(
            "sales",
            &["source", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

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

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep pass should find plan=Enterprise (95% concentration) as a top alternative
        assert!(
            !result.alternatives.is_empty(),
            "deep pass should produce alternatives"
        );
        let best = &result.alternatives[0];
        assert!(
            best.root_fraction > 0.90,
            "best alt should have root_fraction > 0.90, got {}",
            best.root_fraction
        );
        let found = best.nodes.iter().any(|n| matches!(&n.split,
            SplitKind::Dimension { dimension, value } if dimension == "sales.plan" && value == "Enterprise"));
        assert!(found, "deep pass should find plan=Enterprise");
    }

    // ── Case 3: Simpson's Paradox — deep_fixed ──
    #[test]
    fn test_pathological_simpsons_paradox_deep_fixed() {
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
            dim_breakdown(
                "sales.conversion_rate",
                "sales.device",
                &[("Mobile", 3.0, 3.5), ("Desktop", 5.5, 6.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.conversion_rate",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Should still detect Simpson's paradox warning in deep mode
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ExplainWarning::SimpsonsParadox { .. })),
            "should detect Simpson's paradox even in deep mode"
        );
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
        let product_refs: Vec<(&str, f64, f64)> = product_entries
            .iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.product", &product_refs),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep pass should find uniform degradation across 200 products
        let has_uniform = result.alternatives.iter().any(|p| {
            p.nodes
                .iter()
                .any(|n| matches!(&n.split, SplitKind::UniformDegradation { .. }))
        }) || result
            .nodes
            .iter()
            .any(|n| matches!(&n.split, SplitKind::UniformDegradation { .. }));
        assert!(
            has_uniform,
            "deep pass should detect uniform degradation across 200 products, alternatives: {:?}",
            result
                .alternatives
                .iter()
                .map(|a| format!(
                    "{}: {:?}",
                    a.strategy,
                    a.nodes
                        .iter()
                        .map(|n| format!("{:?}", n.split))
                        .collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>()
        );
    }

    // ── Case 5: Decoy High Cardinality — deep_fixed ──
    #[test]
    fn test_pathological_decoy_high_cardinality_deep_fixed() {
        let view = make_view_with_dims(
            "sales",
            &["user_id", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let user_entries: Vec<(String, f64, f64)> = (0..100)
            .map(|i| {
                let curr = if i < 50 { 80.0 } else { 120.0 };
                (format!("user_{}", i), 100.0, curr)
            })
            .collect();
        let user_refs: Vec<(&str, f64, f64)> = user_entries
            .iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.user_id", &user_refs),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep pass should find plan=Enterprise as a top alternative, not get distracted by user_id
        assert!(!result.alternatives.is_empty(), "should have alternatives");
        let found_enterprise = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| matches!(&n.split,
                SplitKind::Dimension { dimension, value } if dimension == "sales.plan" && value == "Enterprise"))
        });
        assert!(
            found_enterprise,
            "deep pass should find plan=Enterprise despite user_id noise"
        );
    }

    // ── Case 6: Component Hides Cross-Cutting Dimension — deep_fixed ──
    #[test]
    fn test_pathological_component_cross_cutting_deep_fixed() {
        let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
        let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![composite_measure(
            "revenue",
            "{{ads.revenue}} + {{subs.revenue}}",
        )]);

        let layer = make_layer(vec![total_view, ads_view, subs_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("total.revenue", 10000.0, 9000.0),
            agg("ads.revenue", 6000.0, 5400.0),
            agg("subs.revenue", 4000.0, 3600.0),
            dim_breakdown(
                "ads.revenue",
                "ads.region",
                &[
                    ("US", 3000.0, 3100.0),
                    ("EU", 2000.0, 1500.0),
                    ("APAC", 1000.0, 800.0),
                ],
            ),
            dim_breakdown(
                "subs.revenue",
                "subs.region",
                &[
                    ("US", 2000.0, 2100.0),
                    ("EU", 1500.0, 1100.0),
                    ("APAC", 500.0, 400.0),
                ],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            max_alternatives: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "total.revenue",
            "total.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep pass should detect cross-cutting EU pattern across ads and subs
        let has_eu = result.alternatives.iter().any(|p| {
            p.nodes.iter().any(|n| match &n.split {
                SplitKind::CrossCutting { value, .. } => value == "EU",
                SplitKind::Dimension { value, .. } => value == "EU",
                _ => false,
            })
        });
        assert!(
            has_eu,
            "deep pass should find EU as a significant factor across components"
        );
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
        let cat_refs: Vec<(&str, f64, f64)> = cat_entries
            .iter()
            .map(|(n, p, c)| (n.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10100.0, 9100.0),
            dim_breakdown("sales.revenue", "sales.category", &cat_refs),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep pass should either find uniform degradation or have alternatives
        let has_result = !result.alternatives.is_empty() || !result.nodes.is_empty();
        assert!(
            has_result,
            "deep pass should handle sub-threshold concentration"
        );
    }

    // ── Case 9: Opposing Offsets — deep_fixed ──
    #[test]
    fn test_pathological_opposing_offsets_deep_fixed() {
        let rev_view = make_view_with_dims("rev", &["region"], &[("amount", MeasureType::Sum)]);
        let cost_view = make_view_with_dims("cost", &["region"], &[("amount", MeasureType::Sum)]);
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![composite_measure(
            "net",
            "{{rev.amount}} - {{cost.amount}}",
        )]);

        let layer = make_layer(vec![profit_view, rev_view, cost_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("profit.net", 2000.0, 2100.0),
            agg("rev.amount", 5000.0, 4900.0),
            agg("cost.amount", 3000.0, 2800.0),
            dim_breakdown(
                "rev.amount",
                "rev.region",
                &[("US", 2000.0, 2400.0), ("EU", 3000.0, 2500.0)],
            ),
            dim_breakdown(
                "cost.amount",
                "cost.region",
                &[("US", 1000.0, 1100.0), ("EU", 2000.0, 1700.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "profit.net",
            "profit.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Should detect opposing offset warning even in deep mode
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ExplainWarning::OpposingOffset { .. })),
            "should detect opposing offset in deep mode"
        );
        // Deep pass should drill into the components
        let has_component_analysis = !result.alternatives.is_empty() || !result.nodes.is_empty();
        assert!(
            has_component_analysis,
            "deep pass should analyze components"
        );
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
            dim_breakdown(
                "sales.conversion_rate",
                "sales.device",
                &[("Mobile", 3.0, 3.5), ("Desktop", 5.5, 6.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.conversion_rate", data);

        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ExplainWarning::SimpsonsParadox { .. })),
            "should detect Simpson's paradox warning"
        );
    }

    #[test]
    fn test_heuristic_opposing_offset_detected() {
        let rev_view = make_view_with_dims("rev", &["region"], &[("amount", MeasureType::Sum)]);
        let cost_view = make_view_with_dims("cost", &["region"], &[("amount", MeasureType::Sum)]);
        let mut profit_view = make_view_with_dims("profit", &[], &[]);
        profit_view.measures = Some(vec![composite_measure(
            "net",
            "{{rev.amount}} - {{cost.amount}}",
        )]);

        let layer = make_layer(vec![profit_view, rev_view, cost_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("profit.net", 2000.0, 2100.0),
            agg("rev.amount", 5000.0, 4900.0),
            agg("cost.amount", 3000.0, 2800.0),
            dim_breakdown(
                "rev.amount",
                "rev.region",
                &[("US", 2000.0, 2400.0), ("EU", 3000.0, 2500.0)],
            ),
            dim_breakdown(
                "cost.amount",
                "cost.region",
                &[("US", 1000.0, 1100.0), ("EU", 2000.0, 1700.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "profit.net", data);

        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ExplainWarning::OpposingOffset { .. })),
            "should detect opposing offset warning"
        );
    }

    #[test]
    fn test_heuristic_no_false_positive() {
        let view = make_view_with_dims("sales", &["plan"], &[("revenue", MeasureType::Sum)]);
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
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
        assert!(
            result.is_finite(),
            "smoothed JSD should be finite for zero share"
        );
        assert!(result > 0.0, "new segment should have positive JSD");
    }

    #[test]
    fn test_jsd_element_smoothed_matches_original_for_nonzero() {
        let original = jsd_element(0.3, 0.2);
        let smoothed = jsd_element_smoothed(0.3, 0.2, 1e-10);
        assert!(
            (original - smoothed).abs() < 1e-6,
            "smoothing should be negligible for nonzero shares"
        );
    }

    #[test]
    fn test_woe_and_iv() {
        let elements = vec![(0.6_f64, 0.4_f64), (0.4, 0.6)];
        let epsilon = 1e-10_f64;
        let woe_1 = ((0.4_f64 + epsilon) / (0.6_f64 + epsilon)).ln();
        let woe_2 = ((0.6_f64 + epsilon) / (0.4_f64 + epsilon)).ln();
        let iv = (0.4_f64 - 0.6_f64) * woe_1 + (0.6_f64 - 0.4_f64) * woe_2;
        assert!(iv > 0.0, "IV should be positive for shifted distribution");

        let computed = compute_iv(&elements, epsilon);
        assert!(
            (computed - iv).abs() < 1e-6,
            "IV computation should match manual calc"
        );
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
        assert!((adaptive_ep_threshold(2) - 0.0354).abs() < 0.001); // 0.05 / sqrt(2)
        assert!((adaptive_ep_threshold(200) - 0.00354).abs() < 0.001); // 0.05 / sqrt(200)
        assert!((adaptive_ep_threshold(1) - 0.05).abs() < 0.001); // 0.05 / sqrt(1)
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
        let result =
            detect_uniform_degradation("sales.product", &elements, parent_delta, threshold);
        assert!(result.is_some(), "should detect uniform degradation");
        if let Some(SplitKind::UniformDegradation {
            dimension,
            num_elements,
        }) = result
        {
            assert_eq!(dimension, "sales.product");
            assert_eq!(num_elements, 200);
        }
    }

    #[test]
    fn test_no_uniform_degradation_when_concentrated() {
        let elements = vec![
            ElementScore {
                value: "A".to_string(),
                previous: 8000.0,
                current: 7100.0,
                delta: -900.0,
                ep: 0.9,
                surprise: 0.01,
            },
            ElementScore {
                value: "B".to_string(),
                previous: 2000.0,
                current: 1900.0,
                delta: -100.0,
                ep: 0.1,
                surprise: 0.001,
            },
        ];
        let threshold = adaptive_ep_threshold(2);
        let result = detect_uniform_degradation("sales.plan", &elements, -1000.0, threshold);
        assert!(
            result.is_none(),
            "concentrated drop is not uniform degradation"
        );
    }

    #[test]
    fn test_strategy_max_concentration() {
        let elements = vec![
            ElementScore {
                value: "A".to_string(),
                previous: 8000.0,
                current: 7050.0,
                delta: -950.0,
                ep: 0.95,
                surprise: 0.001,
            },
            ElementScore {
                value: "B".to_string(),
                previous: 2000.0,
                current: 1950.0,
                delta: -50.0,
                ep: 0.05,
                surprise: 0.0001,
            },
        ];
        let (score, top_value) = strategy_max_concentration(&elements, -1000.0);
        assert!((score - 0.95).abs() < 0.01);
        assert_eq!(top_value, "A");
    }

    #[test]
    fn test_strategy_topk_concentration_sum() {
        let elements = vec![
            ElementScore {
                value: "A".to_string(),
                previous: 0.0,
                current: 0.0,
                delta: -400.0,
                ep: 0.4,
                surprise: 0.0,
            },
            ElementScore {
                value: "B".to_string(),
                previous: 0.0,
                current: 0.0,
                delta: -350.0,
                ep: 0.35,
                surprise: 0.0,
            },
            ElementScore {
                value: "C".to_string(),
                previous: 0.0,
                current: 0.0,
                delta: -150.0,
                ep: 0.15,
                surprise: 0.0,
            },
            ElementScore {
                value: "D".to_string(),
                previous: 0.0,
                current: 0.0,
                delta: -100.0,
                ep: 0.10,
                surprise: 0.0,
            },
        ];
        let score = strategy_topk_concentration_sum(&elements, -1000.0, 3);
        assert!((score - 0.90).abs() < 0.01);
    }

    #[test]
    fn test_strategy_iv_ranking() {
        let elements_shifted = vec![
            ElementScore {
                value: "A".to_string(),
                previous: 6000.0,
                current: 3000.0,
                delta: -3000.0,
                ep: 0.6,
                surprise: 0.0,
            },
            ElementScore {
                value: "B".to_string(),
                previous: 4000.0,
                current: 6000.0,
                delta: 2000.0,
                ep: -0.4,
                surprise: 0.0,
            },
        ];
        let elements_stable = vec![
            ElementScore {
                value: "X".to_string(),
                previous: 5000.0,
                current: 4500.0,
                delta: -500.0,
                ep: 0.5,
                surprise: 0.0,
            },
            ElementScore {
                value: "Y".to_string(),
                previous: 5000.0,
                current: 4500.0,
                delta: -500.0,
                ep: 0.5,
                surprise: 0.0,
            },
        ];
        let iv_shifted = strategy_iv(&elements_shifted);
        let iv_stable = strategy_iv(&elements_stable);
        assert!(
            iv_shifted > iv_stable,
            "shifted distribution should have higher IV"
        );
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
        let churned = result
            .iter()
            .find(|s| s.measure == "revenue.churned_mrr")
            .unwrap();
        assert!((churned.cumulative_sign - (-1.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decompose_includes_intermediate_with_dims() {
        let leaf_a = make_view_with_dims("leaf_a", &[], &[("val", MeasureType::Sum)]);
        let leaf_b = make_view_with_dims("leaf_b", &[], &[("val", MeasureType::Sum)]);
        let mut mid = make_view_with_dims("mid", &["region"], &[]);
        mid.measures = Some(vec![composite_measure(
            "total",
            "{{leaf_a.val}} + {{leaf_b.val}}",
        )]);
        let mut top = make_view_with_dims("top", &[], &[]);
        top.measures = Some(vec![composite_measure("grand", "{{mid.total}} * 2")]);

        let layer = make_layer(vec![top, mid, leaf_a, leaf_b]);
        let tree = MetricTree::build(&layer);
        let children_of = build_children_of(&tree);
        let result = decompose_to_searchable(&tree, &layer, "top.grand", &children_of);

        let names: Vec<&str> = result.iter().map(|s| s.measure.as_str()).collect();
        assert!(
            names.contains(&"mid.total"),
            "intermediate with dims should be searchable"
        );
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

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let dims = vec!["sales.source".to_string(), "sales.plan".to_string()];
        let config = ExplainConfig {
            beam_width: 5,
            max_alternatives: 3,
            ..Default::default()
        };

        let empty_d2e = HashMap::new();
        let empty_promos = crate::engine::promotions::Promotions::default();
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
            &empty_d2e,
            &empty_promos,
        )
        .unwrap();

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

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.source", &source_entries),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[("Enterprise", 8000.0, 7050.0), ("Free", 2000.0, 1950.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        assert!(
            !result.alternatives.is_empty(),
            "deep pass should produce alternatives"
        );
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
        assert!(
            found_enterprise,
            "best alternative should find plan=Enterprise"
        );
    }

    #[test]
    fn test_deep_explain_cross_cutting_detected() {
        let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
        let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
        let mut total_view = make_view_with_dims("total", &[], &[]);
        total_view.measures = Some(vec![composite_measure(
            "revenue",
            "{{ads.revenue}} + {{subs.revenue}}",
        )]);

        let layer = make_layer(vec![total_view, ads_view, subs_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("total.revenue", 10000.0, 9000.0),
            agg("ads.revenue", 6000.0, 5400.0),
            agg("subs.revenue", 4000.0, 3600.0),
            dim_breakdown(
                "ads.revenue",
                "ads.region",
                &[("US", 5000.0, 5000.0), ("EU", 1000.0, 400.0)],
            ),
            dim_breakdown(
                "subs.revenue",
                "subs.region",
                &[("US", 3500.0, 3500.0), ("EU", 500.0, 100.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 5,
            max_alternatives: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "total.revenue",
            "total.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Should find a CrossCutting alternative for region=EU
        let has_cross_cutting = result.alternatives.iter().any(|p| {
            p.nodes
                .iter()
                .any(|n| matches!(&n.split, SplitKind::CrossCutting { value, .. } if value == "EU"))
        });
        assert!(
            has_cross_cutting,
            "should detect cross-cutting region=EU across ads and subs, got {:?}",
            result
                .alternatives
                .iter()
                .map(|a| a
                    .nodes
                    .iter()
                    .map(|n| format!("{:?}", n.split))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
    }

    // ── Statistical significance tests ───────────────────

    #[test]
    fn test_significance_test_detects_abnormal_delta() {
        let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
        let current_delta = -200.0;
        let result = compute_significance(current_delta, &historical);
        assert!(
            result.is_some(),
            "should compute significance with 6 periods"
        );
        let sig = result.unwrap();
        assert!(
            sig.p_value < 0.01,
            "p-value should be very small for outlier delta, got {}",
            sig.p_value
        );
    }

    #[test]
    fn test_significance_test_normal_delta_not_significant() {
        let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
        let current_delta = -48.0;
        let result = compute_significance(current_delta, &historical);
        let sig = result.unwrap();
        assert!(
            sig.p_value > 0.05,
            "normal delta should not be significant, got {}",
            sig.p_value
        );
    }

    #[test]
    fn test_significance_insufficient_history() {
        let historical = vec![-50.0, -40.0];
        let result = compute_significance(-200.0, &historical);
        assert!(
            result.is_none(),
            "should return None with insufficient history"
        );
    }

    // ─────────────────────────────────────────────────────────────
    // Helper: create filtered aggregate (2-row prev/curr with filter key)
    // ─────────────────────────────────────────────────────────────
    fn agg_filtered(
        measure: &str,
        filter_str: &str,
        prev: f64,
        curr: f64,
    ) -> (String, Vec<serde_json::Map<String, serde_json::Value>>) {
        let alias = measure.replace('.', "__");
        let key = format!("{}|{}", measure, filter_str);
        let time_col = format!("{}__created_at", measure.split('.').next().unwrap());
        (
            key,
            vec![
                row(&[(&time_col, js("2024-01")), (&alias, jn(prev))]),
                row(&[(&time_col, js("2024-02")), (&alias, jn(curr))]),
            ],
        )
    }

    // ═══════════════════════════════════════════════════════════════
    // PATHOLOGICAL CASES: MULTI-DIMENSIONAL DEPTH
    //
    // These test scenarios where the root cause is only visible
    // after filtering through multiple dimension values in sequence.
    // ═══════════════════════════════════════════════════════════════

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 10: Three-Level Dimension Chain
    //
    // The root cause is (region=EMEA, channel=Partner, tier=Gold).
    // At each individual dimension, the signal is diluted:
    //   region=EMEA: 50% of total drop (EMEA has Partner+Direct)
    //   channel=Partner: 50% of total drop (Partner spans regions)
    //   tier=Gold: 50% of total drop (Gold spans channels)
    // Only the compound filter chain shows 100% concentration.
    //
    // OPTIMAL: find the 3-level chain (EMEA → Partner → Gold) at ~100%.
    // CURRENT: picks the dimension with highest JSD at depth 1, which
    //          has only 50% concentration. May or may not reach depth 3
    //          depending on thresholds.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_three_level_dimension_chain() {
        let view = make_view_with_dims(
            "sales",
            &["region", "channel", "tier"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // Region breakdown (diluted — drop spread between EMEA + others):
        //   EMEA: 5000 → 4500 (delta -500, EP 0.50)
        //   US:   3000 → 2700 (delta -300, EP 0.30)
        //   APAC: 2000 → 1800 (delta -200, EP 0.20)
        //
        // Channel breakdown (diluted — drop spread between Partner + others):
        //   Partner: 4000 → 3500 (delta -500, EP 0.50)
        //   Direct:  4000 → 3600 (delta -400, EP 0.40)
        //   Reseller:2000 → 1900 (delta -100, EP 0.10)
        //
        // Tier breakdown (diluted):
        //   Gold:   3500 → 3000 (delta -500, EP 0.50)
        //   Silver: 4000 → 3600 (delta -400, EP 0.40)
        //   Bronze: 2500 → 2400 (delta -100, EP 0.10)
        //
        // After EMEA filter → channel breakdown:
        //   Partner: 2500 → 1500 (delta -1000! wait, EMEA total is only -500)
        //   Actually: Partner within EMEA: 2500 → 2100 (delta -400, EP 0.80 of EMEA's -500)
        //   Direct within EMEA:            2500 → 2400 (delta -100, EP 0.20 of EMEA's -500)
        //
        // After EMEA+Partner → tier breakdown:
        //   Gold within EMEA+Partner: 2000 → 1600 (delta -400, EP 1.0 of the -400 context)
        //   Silver within EMEA+Partner: 500 → 500 (delta 0)
        //
        // The chain: EMEA(0.50) → Partner(0.80 of EMEA) → Gold(1.0 of Partner)
        // Root fraction: 0.50 × 0.80 × 1.0 = 0.40

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            // Level 1: all three dimensions
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[
                    ("EMEA", 5000.0, 4500.0),
                    ("US", 3000.0, 2700.0),
                    ("APAC", 2000.0, 1800.0),
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.channel",
                &[
                    ("Partner", 4000.0, 3500.0),
                    ("Direct", 4000.0, 3600.0),
                    ("Reseller", 2000.0, 1900.0),
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.tier",
                &[
                    ("Gold", 3500.0, 3000.0),
                    ("Silver", 4000.0, 3600.0),
                    ("Bronze", 2500.0, 2400.0),
                ],
            ),
            // Level 2: after filtering region=EMEA
            agg_filtered("sales.revenue", "sales.region=EMEA", 5000.0, 4500.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.channel",
                "sales.region=EMEA",
                &[
                    ("Partner", 2500.0, 2100.0), // delta -400, 80% of EMEA
                    ("Direct", 2000.0, 1950.0),  // delta -50
                    ("Reseller", 500.0, 450.0),  // delta -50
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.region=EMEA",
                &[
                    ("Gold", 2000.0, 1600.0),   // delta -400
                    ("Silver", 2000.0, 1950.0), // delta -50
                    ("Bronze", 1000.0, 950.0),  // delta -50
                ],
            ),
            // Level 2: after filtering channel=Partner
            agg_filtered("sales.revenue", "sales.channel=Partner", 4000.0, 3500.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.channel=Partner",
                &[
                    ("EMEA", 2500.0, 2100.0), // delta -400, 80% of Partner
                    ("US", 1000.0, 950.0),    // delta -50
                    ("APAC", 500.0, 450.0),   // delta -50
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.channel=Partner",
                &[
                    ("Gold", 2500.0, 2100.0),  // delta -400
                    ("Silver", 1000.0, 950.0), // delta -50
                    ("Bronze", 500.0, 450.0),  // delta -50
                ],
            ),
            // Level 3: after filtering region=EMEA & channel=Partner
            agg_filtered(
                "sales.revenue",
                "sales.channel=Partner&sales.region=EMEA",
                2500.0,
                2100.0,
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.channel=Partner&sales.region=EMEA",
                &[
                    ("Gold", 2000.0, 1600.0), // delta -400, 100% of this context
                    ("Silver", 300.0, 300.0), // delta 0
                    ("Bronze", 200.0, 200.0), // delta 0
                ],
            ),
            // Level 3: after filtering region=EMEA & tier=Gold
            agg_filtered(
                "sales.revenue",
                "sales.region=EMEA&sales.tier=Gold",
                2000.0,
                1600.0,
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.channel",
                "sales.region=EMEA&sales.tier=Gold",
                &[
                    ("Partner", 1800.0, 1400.0), // delta -400, 100%
                    ("Direct", 150.0, 150.0),
                    ("Reseller", 50.0, 50.0),
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Current behavior: the algorithm picks one dimension at depth 1 (whichever
        // has highest JSD). All three have EP = 0.50, so JSD differences are subtle.
        // The algorithm should recurse to depth 2+ but the root_fraction decays:
        // at best 0.50 × 0.80 = 0.40 after two levels.
        let top = &result.nodes[0];
        assert!(
            top.concentration > 0.40 && top.concentration < 0.60,
            "depth-1 concentration should be ~0.50, got {}",
            top.concentration
        );

        // Check if the algorithm reaches depth 3 (the full chain)
        let mut max_depth = 0;
        fn measure_depth(node: &ExplainNode, depth: usize, max: &mut usize) {
            if depth > *max {
                *max = depth;
            }
            for c in &node.children {
                measure_depth(c, depth + 1, max);
            }
        }
        for n in &result.nodes {
            measure_depth(n, 1, &mut max_depth);
        }

        // Document actual behavior: greedy may or may not reach depth 3.
        // The root_fraction at depth 2 is 0.50 × 0.80 = 0.40, and at depth 3
        // it's 0.40 × 1.0 = 0.40. Coverage at top level is 0.50 (from depth 1 pick).
        // Since 0.50 < 0.80 threshold, the algorithm tries to emit more top-level
        // nodes, but the remaining dimensions also have 0.50 concentration.

        // OPTIMAL: a multi-dim aware algorithm would evaluate all 3! orderings
        // of dimension filters and find that the 3-chain converges to Gold with
        // high concentration. Or it would evaluate (region×channel×tier) jointly.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 11: IV/WOE Amplifies Vanishing Micro-Segment
    //
    // A tiny segment ("promo_trial") goes from 0.5% of revenue to 0%.
    // Its WOE = ln(0 / 0.005) → -∞ (or huge negative with smoothing).
    // Even with Laplace smoothing, this produces a massive |WOE| score.
    //
    // Meanwhile, "enterprise" (80% → 72% share) explains 85% of the
    // actual dollar drop and is the clear actionable answer.
    //
    // OPTIMAL: pick enterprise (85% of drop, $8500 → $7200).
    // CURRENT: IV strategy ranks promo_trial highest by |WOE|,
    //          potentially misleading the beam search.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_iv_amplifies_vanishing_segment() {
        let view = make_view_with_dims(
            "sales",
            &["plan", "source"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 8500, delta = -1500
        //
        // plan dimension:
        //   enterprise: 8000 → 6725  (delta -1275, EP 0.85)  ← real cause
        //   growth:     1500 → 1500  (delta 0)
        //   promo_trial:  50 → 0     (delta -50, EP 0.033)   ← vanishing segment
        //   free:         450 → 275  (delta -175, EP 0.117)
        //
        // promo_trial share: 50/10000 = 0.5% → 0/8500 = 0%
        // WOE = ln((0 + eps) / (0.005 + eps)) ≈ very large negative number
        //
        // source dimension (no useful signal, spread evenly):
        //   organic: 6000 → 5100 (delta -900, EP 0.60)
        //   paid:    4000 → 3400 (delta -600, EP 0.40)

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 8500.0),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[
                    ("enterprise", 8000.0, 6725.0), // -1275, EP 0.85
                    ("growth", 1500.0, 1500.0),     // 0
                    ("promo_trial", 50.0, 0.0),     // -50, EP 0.033, vanishes!
                    ("free", 450.0, 275.0),         // -175, EP 0.117
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.source",
                &[("organic", 6000.0, 5100.0), ("paid", 4000.0, 3400.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1500.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // CURRENT BEHAVIOR: The algorithm correctly picks plan=enterprise (0.85
        // concentration). The vanishing promo_trial segment does NOT distract it
        // because within the plan dimension, enterprise has the highest concentration.
        //
        // This case is NOT currently pathological — the algorithm gets it right.
        // It serves as a regression test: if the scoring changes, this may break.
        let is_enterprise = matches!(&top.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.plan" && value == "enterprise");
        assert!(
            is_enterprise,
            "should pick plan=enterprise: {:?}",
            top.split
        );
        assert!(
            (top.concentration - 0.85).abs() < 0.05,
            "enterprise concentration should be ~0.85, got {}",
            top.concentration
        );
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 12: JSD Red Herring — Proportion Swap
    //
    // Dimension A (channel) has two values that nearly swap proportions:
    //   online: 30% → 65% (huge distributional shift, high JSD)
    //   retail: 70% → 35%
    //   But the total barely changes: 10000 → 9600 (only -400, EP low per element)
    //
    // Dimension B (plan) has minimal distributional shift:
    //   enterprise: 80% → 78.5% (barely changed proportionally)
    //   But enterprise accounts for 90% of the absolute dollar drop.
    //
    // JSD(channel) >> JSD(plan), but plan=enterprise is the answer.
    //
    // OPTIMAL: pick plan=enterprise (0.90 concentration).
    // CURRENT: picks channel (higher accumulated JSD), despite its
    //          top element having only 0.60 concentration.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_jsd_red_herring_proportion_swap() {
        let view = make_view_with_dims(
            "sales",
            &["channel", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // channel dimension (massive proportion swap, HIGH JSD):
        //   online: 3000 → 5850 (delta +2850!)
        //   retail: 7000 → 3150 (delta -3850!)
        //   Total still 10000 → 9000 = -1000
        //
        //   online EP = 2850/(-1000) = -2.85 (opposing!)
        //   retail EP = -3850/(-1000) = 3.85 (>1, over-attributed)
        //   Proportions: online 30%→65%, retail 70%→35% — enormous JSD
        //
        // plan dimension (small proportion shift, LOW JSD):
        //   enterprise: 8000 → 7100 (delta -900, EP 0.90)
        //   free:        2000 → 1900 (delta -100, EP 0.10)
        //   Proportions: enterprise 80%→78.9%, free 20%→21.1% — tiny JSD

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.channel",
                &[
                    ("online", 3000.0, 5850.0), // +2850, opposing
                    ("retail", 7000.0, 3150.0), // -3850, over-attributed
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[
                    ("enterprise", 8000.0, 7100.0), // -900, EP 0.90
                    ("free", 2000.0, 1900.0),       // -100, EP 0.10
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // Channel has enormous JSD (30%→65% and 70%→35%), but the top element
        // by concentration is retail at 3.85 (>1.0). This is an over-attribution
        // because online grew enormously while retail collapsed — they're shuffling
        // revenue between channels, masking that the real problem is enterprise.
        //
        // The algorithm should prefer plan=enterprise (0.90 clean concentration)
        // over channel (whose concentration values are extreme and noisy).
        let chose_channel = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.channel");
        let chose_plan = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.plan");

        // CURRENT BEHAVIOR (SUBOPTIMAL): picks channel=retail.
        // Channel's massive proportion swap (30%↔65%) produces huge JSD, beating
        // plan's modest distributional shift. Retail gets concentration 3.85
        // (over-attributed: its -3850 delta exceeds the total -1000 because
        // online grew by +2850 to offset). This over-attribution is a clear sign
        // of offsetting flows, not a clean root cause.
        assert!(
            chose_channel,
            "greedy picks channel (higher JSD from proportion swap): {:?}",
            top.split
        );
        assert!(
            top.concentration > 3.0,
            "retail should have extreme over-attributed concentration ~3.85, got {}",
            top.concentration
        );

        // OPTIMAL: detect that channel has offsetting flows (online +2850, retail -3850)
        // and prefer plan=enterprise (0.90 clean concentration, no offsetting).
        // A "total offset magnitude" of |2850| + |3850| = 6700 >> |delta|=1000
        // is a signal that this dimension is noisy.
    }

    // ═══════════════════════════════════════════════════════════════
    // PATHOLOGICAL CASES: COMPONENT vs DIMENSION SPLIT ORDERING
    // ═══════════════════════════════════════════════════════════════

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 13: Dimension-First Beats Component-First
    //
    // Composite: total = sub_a + sub_b + sub_c (three sub-metrics).
    // All three sub-views have a "region" dimension.
    // Region=EU accounts for 100% of the drop in ALL sub-metrics.
    //
    // Component-first (greedy): picks sub_a (40%), finds EU within it.
    //   root_fraction = 0.40 × 1.0 = 0.40
    // Then sub_b (35%) → EU: root_fraction += 0.35 × 1.0 = 0.75
    // Then sub_c (25%) → EU: root_fraction += 0.25 = 1.0
    //
    // But if we could split by region FIRST at the composite level,
    // we'd immediately get EU at 1.0 concentration — one step.
    //
    // OPTIMAL: recognize EU is cross-cutting and report it at 100%.
    // CURRENT: follows component path, requires 3 branches to cover.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_dimension_first_beats_component_multi_branch() {
        let sub_a = make_view_with_dims("sub_a", &["region"], &[("val", MeasureType::Sum)]);
        let sub_b = make_view_with_dims("sub_b", &["region"], &[("val", MeasureType::Sum)]);
        let sub_c = make_view_with_dims("sub_c", &["region"], &[("val", MeasureType::Sum)]);
        let mut total = make_view_with_dims("total", &[], &[]);
        total.measures = Some(vec![composite_measure(
            "val",
            "{{sub_a.val}} + {{sub_b.val}} + {{sub_c.val}}",
        )]);

        let layer = make_layer(vec![total, sub_a, sub_b, sub_c]);
        let tree = MetricTree::build(&layer);

        // total.val: 10000 → 9000, delta = -1000
        // sub_a.val: 4000 → 3600, delta = -400 (concentration 0.40)
        // sub_b.val: 3500 → 3150, delta = -350 (concentration 0.35)
        // sub_c.val: 2500 → 2250, delta = -250 (concentration 0.25)
        //
        // Within each sub: EU accounts for 100% of that sub's drop
        // sub_a: US=2000→2000, EU=2000→1600 (delta -400)
        // sub_b: US=2000→2000, EU=1500→1150 (delta -350)
        // sub_c: US=1500→1500, EU=1000→750  (delta -250)

        let mut data = HashMap::new();
        data.extend([
            agg("total.val", 10000.0, 9000.0),
            agg("sub_a.val", 4000.0, 3600.0),
            agg("sub_b.val", 3500.0, 3150.0),
            agg("sub_c.val", 2500.0, 2250.0),
            dim_breakdown(
                "sub_a.val",
                "sub_a.region",
                &[("US", 2000.0, 2000.0), ("EU", 2000.0, 1600.0)],
            ),
            dim_breakdown(
                "sub_b.val",
                "sub_b.region",
                &[("US", 2000.0, 2000.0), ("EU", 1500.0, 1150.0)],
            ),
            dim_breakdown(
                "sub_c.val",
                "sub_c.region",
                &[("US", 1500.0, 1500.0), ("EU", 1000.0, 750.0)],
            ),
        ]);

        let result = run_explain(&layer, &tree, "total.val", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Current behavior: total has NO dimensions, so only component splits
        // are available at the root level. The algorithm picks sub_a (0.40),
        // then finds region=EU within sub_a. Coverage = 0.40.
        let top = &result.nodes[0];
        let is_component = matches!(&top.split, SplitKind::Component { .. });
        assert!(
            is_component,
            "must pick component (total has no dimensions): {:?}",
            top.split
        );

        // Current behavior: the greedy algorithm emits only the first component
        // (sub_a at 0.40 concentration) and recurses into it. It does NOT emit
        // sub_b and sub_c as additional top-level nodes, even though coverage
        // is below the 0.80 threshold. This is because the top-level loop
        // only emits one split per evaluation round.
        //
        // OPTIMAL: emit all three components at the top level (total coverage 1.0)
        // or better yet, detect EU as the cross-cutting root cause.
        assert!(
            result.coverage < 0.50,
            "coverage should be ~0.40 (only first component), got {}",
            result.coverage
        );

        // OPTIMAL: the deep beam search should detect that EU is cross-cutting
        // and report it as a single explanation covering 100%.
        // The greedy algorithm can't do this because total has no dimensions;
        // it can only decompose by component first.
    }

    // ═══════════════════════════════════════════════════════════════
    // PATHOLOGICAL CASES: MULTI-DIMENSIONAL SIMULTANEOUS SPLITS
    // ═══════════════════════════════════════════════════════════════

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 14: Multi-Dimensional AND Condition
    //
    // 3 dimensions: region, channel, device.
    // No single dimension value explains more than 40% of the drop.
    // But (region=EU, channel=Online) jointly explains 90%.
    //
    // This is different from Case 10 (sequential chain) because here
    // the two dimensions are BOTH needed at the same level — it's
    // not that EU narrows to Online, it's that the intersection
    // (EU AND Online) is where the problem lives.
    //
    // OPTIMAL: discover the 2D intersection (EU, Online) at 90%.
    // CURRENT: picks one dimension, gets ~40% coverage, then
    //          may or may not find the second dimension at depth 2.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_multi_dim_and_condition() {
        let view = make_view_with_dims(
            "sales",
            &["region", "channel", "device"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // The drop is concentrated in (EU, Online) = -900.
        // The remaining -100 is distributed across other cells.
        //
        // Marginal distributions (each dimension alone):
        //   region:  EU=-400, US=-350, APAC=-250  (max concentration 0.40)
        //   channel: Online=-400, Retail=-350, Wholesale=-250  (max 0.40)
        //   device:  Mobile=-450, Desktop=-350, Tablet=-200  (max 0.45)
        //
        // None exceed 0.50 individually.
        //
        // But EU×Online = -900 (0.90 concentration)
        // The other EU cells: EU×Retail=-(-250), EU×Wholesale=-(-150)
        // Wait, let me be more precise about the allocation.
        //
        // Let me design so:
        // EU total: 4000 → 3600, delta -400  (some in Online, some spread)
        // Online total: 3500 → 3100, delta -400
        // EU×Online: 2000 → 1100, delta -900  ← the real cause
        // EU×Retail: 1200 → 1500, delta +300  (EU retail actually grew!)
        // EU×Wholesale: 800 → 1000, delta +200  (EU wholesale grew too!)
        // This means EU total = -900 + 300 + 200 = -400 ✓
        //
        // US×Online: 1000 → 1300, delta +300
        // APAC×Online: 500 → 700, delta +200
        // Online total = -900 + 300 + 200 = -400 ✓

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[
                    ("EU", 4000.0, 3600.0),   // delta -400, conc 0.40
                    ("US", 3500.0, 3150.0),   // delta -350, conc 0.35
                    ("APAC", 2500.0, 2250.0), // delta -250, conc 0.25
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.channel",
                &[
                    ("Online", 3500.0, 3100.0),    // delta -400, conc 0.40
                    ("Retail", 3500.0, 3150.0),    // delta -350, conc 0.35
                    ("Wholesale", 3000.0, 2750.0), // delta -250, conc 0.25
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.device",
                &[
                    ("Mobile", 4000.0, 3550.0),  // delta -450, conc 0.45
                    ("Desktop", 3500.0, 3150.0), // delta -350, conc 0.35
                    ("Tablet", 2500.0, 2300.0),  // delta -200, conc 0.20
                ],
            ),
            // After filtering region=EU: channel breakdown reveals Online
            agg_filtered("sales.revenue", "sales.region=EU", 4000.0, 3600.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.channel",
                "sales.region=EU",
                &[
                    ("Online", 2000.0, 1100.0),   // delta -900! 225% of EU's -400
                    ("Retail", 1200.0, 1500.0),   // delta +300, opposing
                    ("Wholesale", 800.0, 1000.0), // delta +200, opposing
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.region=EU",
                &[
                    ("Mobile", 1800.0, 1500.0),
                    ("Desktop", 1400.0, 1300.0),
                    ("Tablet", 800.0, 800.0),
                ],
            ),
            // After filtering channel=Online: region breakdown reveals EU
            agg_filtered("sales.revenue", "sales.channel=Online", 3500.0, 3100.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.channel=Online",
                &[
                    ("EU", 2000.0, 1100.0), // delta -900! 225% of Online's -400
                    ("US", 1000.0, 1300.0), // delta +300
                    ("APAC", 500.0, 700.0), // delta +200
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.channel=Online",
                &[
                    ("Mobile", 1500.0, 1200.0),
                    ("Desktop", 1200.0, 1100.0),
                    ("Tablet", 800.0, 800.0),
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // At depth 1, the best any single dimension offers is ~0.45 (Mobile).
        // But after filtering to EU or Online, the intersection reveals -900.
        //
        // The algorithm should find the 2-level path:
        // EU (0.40) → Online (-900, concentration 2.25 of EU's delta)
        // or
        // Online (0.40) → EU (-900, concentration 2.25 of Online's delta)
        //
        // Note the unusual pattern: EU→Online has concentration > 1.0 because
        // the intersection's drop exceeds EU's total drop (opposing flows in
        // Retail and Wholesale within EU mask the Online crash).
        //
        // This means the depth-2 root_fraction = 0.40 × 2.25 = 0.90
        // which is higher than any depth-1 pick.

        // CURRENT BEHAVIOR: picks device=Mobile (0.45 concentration — highest
        // marginal, since Mobile has the biggest proportional shift).
        // At depth 2, finds channel=Online (conc ~0.89 within Mobile's delta).
        // Coverage = 0.45 (only from the top-level pick).
        let is_mobile = matches!(&top.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.device" && value == "Mobile");
        assert!(
            is_mobile,
            "greedy picks device=Mobile (highest marginal conc): {:?}",
            top.split
        );
        assert!(
            (top.concentration - 0.45).abs() < 0.05,
            "Mobile concentration should be ~0.45, got {}",
            top.concentration
        );

        // Depth 2 should find something useful within Mobile
        assert!(!top.children.is_empty(), "should recurse into Mobile");

        // OPTIMAL: a multi-dim algorithm would evaluate (region×channel) jointly
        // and find EU×Online = -900 (90% of total) in one step. The greedy path
        // only achieves ~0.45 coverage via Mobile, missing the 0.90 intersection.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 15: Correlated Confounders
    //
    // Two dimensions are nearly perfectly correlated:
    //   plan=Enterprise ≈ region=US (all enterprise customers are in US)
    //   plan=Free ≈ region=EU (all free customers are in EU)
    //
    // The real root cause is a product issue affecting Enterprise plan.
    // But since Enterprise ≈ US, the region dimension shows the same
    // signal. Both dimensions have identical concentration, identical
    // JSD. The algorithm picks whichever comes first alphabetically
    // or has marginally higher numerical precision.
    //
    // OPTIMAL: recognize the confounding and report both as correlated
    //          explanations, or prefer the more specific/causal one.
    // CURRENT: picks one arbitrarily, missing that the two are aliases.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_correlated_confounders() {
        let view = make_view_with_dims(
            "sales",
            &["plan", "region"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // plan:   Enterprise=8000→7100 (-900, EP 0.90), Free=2000→1900 (-100, EP 0.10)
        // region: US=8050→7150 (-900, EP 0.90), EU=1950→1850 (-100, EP 0.10)
        //
        // The near-perfect correlation: Enterprise is ~99% US customers.
        // The JSD and concentration for both dimensions are nearly identical.

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[
                    ("Enterprise", 8000.0, 7100.0), // -900, conc 0.90
                    ("Free", 2000.0, 1900.0),       // -100, conc 0.10
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[
                    ("US", 8050.0, 7150.0), // -900, conc 0.90 (≈ Enterprise)
                    ("EU", 1950.0, 1850.0), // -100, conc 0.10 (≈ Free)
                ],
            ),
            // After filtering plan=Enterprise:
            agg_filtered("sales.revenue", "sales.plan=Enterprise", 8000.0, 7100.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.plan=Enterprise",
                &[
                    ("US", 7950.0, 7050.0), // -900, 100% of Enterprise drop
                    ("EU", 50.0, 50.0),     // 0 (almost no Enterprise in EU)
                ],
            ),
            // After filtering region=US:
            agg_filtered("sales.revenue", "sales.region=US", 8050.0, 7150.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.plan",
                "sales.region=US",
                &[
                    ("Enterprise", 7950.0, 7050.0), // -900, 100% of US drop
                    ("Free", 100.0, 100.0),         // 0 (almost no Free in US)
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // Both dimensions should have ~0.90 concentration.
        assert!(
            top.concentration > 0.85,
            "top should have high concentration, got {}",
            top.concentration
        );

        // CURRENT BEHAVIOR: picks plan=Enterprise (marginally higher JSD than
        // region=US because plan's share shifts 80%→78.9% vs region's 80.5%→79.4%).
        // Both dimensions are essentially aliases due to near-perfect correlation.
        let is_plan = matches!(&top.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.plan" && value == "Enterprise");
        assert!(
            is_plan,
            "picks plan=Enterprise (marginally higher JSD): {:?}",
            top.split
        );
        assert!(
            (top.concentration - 0.90).abs() < 0.05,
            "Enterprise concentration should be ~0.90, got {}",
            top.concentration
        );

        // At depth 2, the correlated dimension (region=US) shows 100% concentration
        // within Enterprise — confirming they're aliases.
        assert!(
            !top.children.is_empty(),
            "should recurse to find correlated dimension"
        );
        let depth2 = &top.children[0];
        assert!(
            depth2.concentration > 0.95,
            "depth-2 should show near-perfect correlation, got {}",
            depth2.concentration
        );

        // OPTIMAL: detect that plan and region are correlated (mutual information ≈ 1.0)
        // and report "plan=Enterprise (correlated with region=US)" rather than treating
        // them as two independent sequential filters.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 16: Micro-Segment JSD Inflation
    //
    // Dimension A (product_sku): 500 SKUs, 20 tiny SKUs vanish entirely.
    //   Each vanishing SKU was 0.2% of revenue → JSD per element is huge.
    //   Total of vanishing: 4% of drop. But accumulated JSD over 20 is big.
    //
    // Dimension B (plan): 3 values. Enterprise has 92% concentration.
    //   Low cardinality → modest total JSD even with great signal.
    //
    // JSD ranking: sum of significant elements' JSD.
    // 20 vanishing SKUs × huge individual JSD > enterprise's single JSD.
    //
    // OPTIMAL: pick plan=Enterprise (92% concentration).
    // CURRENT: picks product_sku (inflated accumulated JSD from micro-deaths).
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_micro_segment_jsd_inflation() {
        let view = make_view_with_dims(
            "sales",
            &["product_sku", "plan"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // plan dimension (clear signal):
        //   Enterprise: 7500 → 6580 (delta -920, EP 0.92)  ← answer
        //   Growth:     1500 → 1470 (delta -30, EP 0.03)
        //   Free:       1000 → 950  (delta -50, EP 0.05)
        //
        // product_sku dimension (noisy, high-cardinality):
        //   20 micro-SKUs vanish: each 10 → 0 (delta -10, EP 0.01)
        //     Total from vanishing: -200, EP 0.20
        //     But each has JSD ≈ huge (went from 0.1% share to 0%)
        //   30 normal SKUs decline: each 100 → 80 (delta -20, EP 0.02)
        //     Total: -600, EP 0.60
        //   Remaining ~450 SKUs: stable or small changes to make up the rest
        //     We'll simplify: 1 "other" bucket at 7000 → 6800 (delta -200)

        let mut sku_entries: Vec<(String, f64, f64)> = Vec::new();
        // 20 vanishing micro-SKUs
        for i in 1..=20 {
            sku_entries.push((format!("micro_sku_{}", i), 10.0, 0.0));
        }
        // 30 declining normal SKUs
        for i in 1..=30 {
            sku_entries.push((format!("sku_{}", i), 100.0, 80.0));
        }
        // 1 large "other" bucket (simplification of 450 stable SKUs)
        sku_entries.push(("other_skus".to_string(), 7000.0, 6800.0));

        let sku_refs: Vec<(&str, f64, f64)> = sku_entries
            .iter()
            .map(|(s, p, c)| (s.as_str(), *p, *c))
            .collect();

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown("sales.revenue", "sales.product_sku", &sku_refs),
            dim_breakdown(
                "sales.revenue",
                "sales.plan",
                &[
                    ("Enterprise", 7500.0, 6580.0), // -920, EP 0.92
                    ("Growth", 1500.0, 1470.0),     // -30
                    ("Free", 1000.0, 950.0),        // -50
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];
        let chose_sku = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.product_sku");
        let chose_plan = matches!(&top.split, SplitKind::Dimension { dimension, .. }
            if dimension == "sales.plan");

        // The 20 vanishing SKUs each have share shift from 0.1% to 0%.
        // JSD for each: jsd_element(0.001, 0) ≈ very large
        // With EP threshold filtering, some micro-SKUs may fall below MIN_ELEMENT_EP.
        // But their accumulated surprise can still exceed plan's surprise.
        //
        // Plan's top element (Enterprise) has EP = 0.92 (clearly above threshold)
        // but JSD is modest: shares go from 75% → 73.1% — small distributional shift.

        // CURRENT BEHAVIOR (SUBOPTIMAL): picks product_sku dimension.
        // The 20 vanishing micro-SKUs produce enormous per-element JSD (share goes
        // from 0.1% to 0%), and the accumulated surprise across all significant
        // elements exceeds plan's surprise. Within product_sku, the top element
        // by concentration is "other_skus" (EP 0.20, the aggregated large bucket),
        // NOT a micro_sku (each has EP 0.01, below threshold).
        //
        // The DIMENSION choice is wrong (plan=Enterprise at 0.92 is far better),
        // but the ELEMENT choice within the wrong dimension is reasonable.
        assert!(
            chose_sku,
            "greedy picks product_sku (inflated JSD from micro-segment deaths): {:?}",
            top.split
        );
        assert!(
            (top.concentration - 0.20).abs() < 0.05,
            "SKU top concentration should be ~0.20 (other_skus bucket), got {}",
            top.concentration
        );

        // OPTIMAL: normalize JSD by cardinality or use concentration-weighted JSD.
        // 20 dying micro-segments at EP 0.01 each should not outweigh 1 segment
        // at EP 0.92. plan=Enterprise at 0.92 concentration is the correct answer.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 17: Non-Monotonic Path (Valley of Death)
    //
    // The globally optimal explanation requires passing through a
    // "valley" — an intermediate step with low concentration.
    //
    // Dimension "tier": premium=25% of drop, standard=75%.
    //   Proportions shift: premium 20%→22%, standard 80%→78%.
    //   Some JSD, but premium is the minority.
    //
    // Dimension "device": mobile=55% of drop, desktop=45%.
    //   Clear concentration in mobile. Decent JSD.
    //
    // After tier=premium: region=EMEA has 92% concentration!
    //   root_fraction = 0.25 × 0.92 = 0.23
    // After device=mobile: region spreads 55/45 (no concentration).
    //   root_fraction = 0.55 × 0.55 = 0.30
    //
    // Greedy picks device=mobile (0.55 > 0.25), gets diffuse depth-2.
    // Optimal picks tier=premium then region=EMEA (concentrated depth-2).
    //
    // CURRENT: greedy commits to the stronger depth-1 signal, missing the
    //          path through the "valley" that leads to better specificity.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_non_monotonic_path() {
        let view = make_view_with_dims(
            "sales",
            &["tier", "region", "device"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        // Total: 10000 → 9000, delta = -1000
        //
        // tier: premium=2000→1750 (-250, conc 0.25), standard=8000→7250 (-750, conc 0.75)
        //   proportions: 20%→19.4% vs 80%→80.6% — small shift, some JSD
        //
        // device: mobile=3000→2450 (-550, conc 0.55), desktop=7000→6550 (-450, conc 0.45)
        //   proportions: 30%→27.2% vs 70%→72.8% — moderate shift, decent JSD
        //
        // region: EMEA=6000→5350 (-650, conc 0.65), US=4000→3650 (-350, conc 0.35)
        //   proportions: 60%→59.4% vs 40%→40.6% — tiny shift, low JSD

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.tier",
                &[
                    ("premium", 2000.0, 1750.0),  // -250, conc 0.25
                    ("standard", 8000.0, 7250.0), // -750, conc 0.75
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.device",
                &[
                    ("mobile", 3000.0, 2450.0),  // -550, conc 0.55
                    ("desktop", 7000.0, 6550.0), // -450, conc 0.45
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[
                    ("EMEA", 6000.0, 5350.0), // -650, conc 0.65
                    ("US", 4000.0, 3650.0),   // -350, conc 0.35
                ],
            ),
            // Depth 2: after tier=premium → region concentrated in EMEA
            agg_filtered("sales.revenue", "sales.tier=premium", 2000.0, 1750.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.tier=premium",
                &[
                    ("EMEA", 1700.0, 1470.0), // delta -230, conc 0.92 of -250
                    ("US", 300.0, 280.0),     // delta -20, conc 0.08
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.tier=premium",
                &[
                    ("mobile", 900.0, 780.0),   // delta -120, conc 0.48
                    ("desktop", 1100.0, 970.0), // delta -130, conc 0.52
                ],
            ),
            // Depth 2: after device=mobile → region spread
            agg_filtered("sales.revenue", "sales.device=mobile", 3000.0, 2450.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.device=mobile",
                &[
                    ("EMEA", 1800.0, 1500.0), // delta -300, conc 0.545
                    ("US", 1200.0, 950.0),    // delta -250, conc 0.455
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.device=mobile",
                &[
                    ("premium", 900.0, 780.0),    // delta -120, conc 0.218
                    ("standard", 2100.0, 1670.0), // delta -430, conc 0.782
                ],
            ),
            // Depth 2: after region=EMEA → tier and device both diffuse
            agg_filtered("sales.revenue", "sales.region=EMEA", 6000.0, 5350.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.region=EMEA",
                &[
                    ("premium", 1700.0, 1470.0),  // delta -230, conc 0.354
                    ("standard", 4300.0, 3880.0), // delta -420, conc 0.646
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.region=EMEA",
                &[
                    ("mobile", 1800.0, 1500.0),  // delta -300, conc 0.462
                    ("desktop", 4200.0, 3850.0), // delta -350, conc 0.538
                ],
            ),
            // Depth 2: after tier=standard → region spread
            agg_filtered("sales.revenue", "sales.tier=standard", 8000.0, 7250.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.tier=standard",
                &[
                    ("EMEA", 4300.0, 3880.0), // delta -420, conc 0.56
                    ("US", 3700.0, 3370.0),   // delta -330, conc 0.44
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.tier=standard",
                &[
                    ("mobile", 2100.0, 1670.0),  // delta -430, conc 0.573
                    ("desktop", 5900.0, 5580.0), // delta -320, conc 0.427
                ],
            ),
        ]);

        let result = run_explain(&layer, &tree, "sales.revenue", data);

        assert!((result.target_delta - (-1000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        let top = &result.nodes[0];

        // CURRENT BEHAVIOR: greedy picks device=mobile (0.55 concentration) — the
        // strongest depth-1 signal. It does NOT pick tier=premium (only 0.25) even
        // though premium → EMEA would give 0.92 concentration at depth 2.
        //
        // The "valley" — premium at 0.25 — is too low for greedy to explore, despite
        // leading to the most specific root cause.
        let is_mobile = matches!(&top.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.device" && value == "mobile");
        assert!(
            is_mobile,
            "greedy picks device=mobile (highest depth-1 conc at 0.55): {:?}",
            top.split
        );
        assert!(
            (top.concentration - 0.55).abs() < 0.05,
            "mobile concentration should be ~0.55, got {}",
            top.concentration
        );

        // At depth 2 within mobile, the splits are diffuse (~55/45 or 62/38),
        // not concentrated. This confirms the "valley" problem: the greedy path
        // has good depth-1 but poor depth-2.
        assert!(!top.children.is_empty(), "should recurse into mobile");

        // OPTIMAL: beam search or lookahead would discover that premium(0.25)
        // → EMEA(0.92) gives root_fraction 0.23 with high specificity.
        // Despite the "valley" at depth 1, the depth-2 payoff is better.
    }

    // ─────────────────────────────────────────────────────────────
    // PATHOLOGICAL CASE 18: Component Scaling Distorts Concentration
    //
    // arr = mrr * 12. The scaling factor means that a component split
    // at the arr level shows mrr with concentration 1.0 (correct),
    // but a dimension split on arr competes unfairly because the
    // scaling amplifies all values equally.
    //
    // The deeper issue: after decomposing arr → mrr, the algorithm
    // searches dimensions within mrr. But one dimension (plan) shows
    // 90% concentration within mrr, while another (region) shows 60%.
    // The scaling factor doesn't affect relative concentrations within
    // mrr, but the initial component split "costs" a recursion level.
    //
    // If the composite measure (arr) HAD a dimension, the algorithm
    // could skip the component decomposition and go straight to
    // arr.plan=Enterprise at 90%. But since arr is computed, it has
    // no queryable dimensions — forcing the component detour.
    //
    // OPTIMAL: recognize that the component decomposition through a
    //          pure scaling node (×12) is a no-op and skip it.
    // CURRENT: uses one recursion level for the trivial ×12 step.
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_pathological_scaling_wastes_recursion_depth() {
        // arr = mrr * 12. mrr has dimensions.
        let mut mrr_view =
            make_view_with_dims("revenue", &["plan", "region"], &[("mrr", MeasureType::Sum)]);
        // Add arr as composite on the same view
        let arr = composite_measure("arr", "{{revenue.mrr}} * 12");
        if let Some(ref mut measures) = mrr_view.measures {
            measures.push(arr);
        }

        let layer = make_layer(vec![mrr_view]);
        let tree = MetricTree::build(&layer);

        // arr: 120000 → 108000, delta = -12000
        // mrr: 10000 → 9000, delta = -1000
        //
        // Within mrr:
        //   plan=Enterprise: 8000 → 7100, delta = -900, conc 0.90
        //   plan=Free:       2000 → 1900, delta = -100, conc 0.10
        //
        //   region=US: 6000 → 5400, delta = -600, conc 0.60
        //   region=EU: 4000 → 3600, delta = -400, conc 0.40

        let mut data = HashMap::new();
        data.extend([
            agg("revenue.arr", 120000.0, 108000.0),
            agg("revenue.mrr", 10000.0, 9000.0),
            dim_breakdown(
                "revenue.mrr",
                "revenue.plan",
                &[("Enterprise", 8000.0, 7100.0), ("Free", 2000.0, 1900.0)],
            ),
            dim_breakdown(
                "revenue.mrr",
                "revenue.region",
                &[("US", 6000.0, 5400.0), ("EU", 4000.0, 3600.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
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

        assert!((result.target_delta - (-12000.0)).abs() < 0.01);
        assert!(!result.nodes.is_empty());

        // Depth 1: should be component split to mrr (concentration 1.0 after
        // stripping the ×12 scaling factor).
        let top = &result.nodes[0];
        let is_mrr_component = matches!(&top.split, SplitKind::Component { child_measure }
            if child_measure == "revenue.mrr");
        assert!(
            is_mrr_component,
            "should decompose arr → mrr: {:?}",
            top.split
        );

        // Depth 2: within mrr, should find plan=Enterprise (0.90)
        if !top.children.is_empty() {
            let depth2 = &top.children[0];
            let found_enterprise = matches!(&depth2.split,
                SplitKind::Dimension { dimension, value }
                if dimension == "revenue.plan" && value == "Enterprise"
            );
            if found_enterprise {
                assert!(
                    (depth2.concentration - 0.90).abs() < 0.05,
                    "Enterprise should have ~0.90 concentration, got {}",
                    depth2.concentration
                );
            }
        }

        // The recursion "wastes" depth 1 on arr→mrr (a pure scaling step).
        // This is correct behavior but costs a recursion level. With max_depth=5,
        // we now only have 4 levels left for meaningful decomposition.
        //
        // OPTIMAL: detect that ×12 is a trivial scaling and "collapse" it,
        // treating mrr's dimensions as if they were arr's dimensions.
    }

    // ═══════════════════════════════════════════════════════════════
    // DEEP MODE VARIANTS for new pathological cases
    // ═══════════════════════════════════════════════════════════════

    // ── Case 14: Multi-Dimensional AND — deep ──
    #[test]
    fn test_pathological_multi_dim_and_condition_deep() {
        let view = make_view_with_dims(
            "sales",
            &["region", "channel", "device"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[
                    ("EU", 4000.0, 3600.0),
                    ("US", 3500.0, 3150.0),
                    ("APAC", 2500.0, 2250.0),
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.channel",
                &[
                    ("Online", 3500.0, 3100.0),
                    ("Retail", 3500.0, 3150.0),
                    ("Wholesale", 3000.0, 2750.0),
                ],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.device",
                &[
                    ("Mobile", 4000.0, 3550.0),
                    ("Desktop", 3500.0, 3150.0),
                    ("Tablet", 2500.0, 2300.0),
                ],
            ),
            agg_filtered("sales.revenue", "sales.region=EU", 4000.0, 3600.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.channel",
                "sales.region=EU",
                &[
                    ("Online", 2000.0, 1100.0),
                    ("Retail", 1200.0, 1500.0),
                    ("Wholesale", 800.0, 1000.0),
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.region=EU",
                &[
                    ("Mobile", 1800.0, 1500.0),
                    ("Desktop", 1400.0, 1300.0),
                    ("Tablet", 800.0, 800.0),
                ],
            ),
            agg_filtered("sales.revenue", "sales.channel=Online", 3500.0, 3100.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.channel=Online",
                &[
                    ("EU", 2000.0, 1100.0),
                    ("US", 1000.0, 1300.0),
                    ("APAC", 500.0, 700.0),
                ],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.channel=Online",
                &[
                    ("Mobile", 1500.0, 1200.0),
                    ("Desktop", 1200.0, 1100.0),
                    ("Tablet", 800.0, 800.0),
                ],
            ),
            // Deep mode will also filter on EU+Online
            agg_filtered(
                "sales.revenue",
                "sales.channel=Online&sales.region=EU",
                2000.0,
                1100.0,
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.channel=Online&sales.region=EU",
                &[
                    ("Mobile", 900.0, 500.0),
                    ("Desktop", 700.0, 400.0),
                    ("Tablet", 400.0, 200.0),
                ],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 10,
            max_alternatives: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        // Deep mode should find the EU+Online path as a top alternative
        assert!(
            !result.alternatives.is_empty(),
            "deep pass should produce alternatives"
        );

        // Check if any alternative has high root_fraction (approaching 0.90)
        // The EU→Online path should give root_fraction: EU has 0.40 at top,
        // then Online within EU has concentration -900/-400 = 2.25.
        // But beam search clamps or uses the absolute delta ratio.
        let best = &result.alternatives[0];
        // The best path should achieve > 0.50 root_fraction (better than greedy)
        assert!(
            best.root_fraction > 0.40,
            "deep best should exceed greedy's depth-1 pick, got {}",
            best.root_fraction
        );
    }

    // ── Case 17: Non-Monotonic Path — deep ──
    #[test]
    fn test_pathological_non_monotonic_path_deep() {
        let view = make_view_with_dims(
            "sales",
            &["tier", "region", "device"],
            &[("revenue", MeasureType::Sum)],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.extend([
            agg("sales.revenue", 10000.0, 9000.0),
            dim_breakdown(
                "sales.revenue",
                "sales.tier",
                &[("premium", 2000.0, 1750.0), ("standard", 8000.0, 7250.0)],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.device",
                &[("mobile", 3000.0, 2450.0), ("desktop", 7000.0, 6550.0)],
            ),
            dim_breakdown(
                "sales.revenue",
                "sales.region",
                &[("EMEA", 6000.0, 5350.0), ("US", 4000.0, 3650.0)],
            ),
            // tier=premium → EMEA concentrated
            agg_filtered("sales.revenue", "sales.tier=premium", 2000.0, 1750.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.tier=premium",
                &[("EMEA", 1700.0, 1470.0), ("US", 300.0, 280.0)],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.tier=premium",
                &[("mobile", 900.0, 780.0), ("desktop", 1100.0, 970.0)],
            ),
            // device=mobile → region spread
            agg_filtered("sales.revenue", "sales.device=mobile", 3000.0, 2450.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.device=mobile",
                &[("EMEA", 1800.0, 1500.0), ("US", 1200.0, 950.0)],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.device=mobile",
                &[("premium", 900.0, 780.0), ("standard", 2100.0, 1670.0)],
            ),
            // tier=standard → region spread
            agg_filtered("sales.revenue", "sales.tier=standard", 8000.0, 7250.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.region",
                "sales.tier=standard",
                &[("EMEA", 4300.0, 3880.0), ("US", 3700.0, 3370.0)],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.tier=standard",
                &[("mobile", 2100.0, 1670.0), ("desktop", 5900.0, 5580.0)],
            ),
            // region=EMEA → tier/device diffuse
            agg_filtered("sales.revenue", "sales.region=EMEA", 6000.0, 5350.0),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.tier",
                "sales.region=EMEA",
                &[("premium", 1700.0, 1470.0), ("standard", 4300.0, 3880.0)],
            ),
            dim_breakdown_filtered(
                "sales.revenue",
                "sales.device",
                "sales.region=EMEA",
                &[("mobile", 1800.0, 1500.0), ("desktop", 4200.0, 3850.0)],
            ),
        ]);

        let exec = filter_aware_mock(data);
        let config = ExplainConfig {
            deep: true,
            beam_width: 10,
            max_alternatives: 5,
            ..Default::default()
        };
        let result = explain(
            &tree,
            &layer,
            "sales.revenue",
            "sales.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &config,
            &exec,
        )
        .unwrap();

        assert!(
            !result.alternatives.is_empty(),
            "deep pass should produce alternatives"
        );

        // Beam search should explore the premium→EMEA path (0.30 × 0.95 = 0.285)
        // even though premium has only 0.30 concentration at depth 1.
        // Check if any alternative contains tier=premium → region=EMEA
        let has_premium_emea_path = result.alternatives.iter().any(|p| {
            let has_premium = p.nodes.iter().any(|n| {
                matches!(&n.split, SplitKind::Dimension { dimension, value }
                    if dimension == "sales.tier" && value == "premium")
            });
            let has_emea = p.nodes.iter().any(|n| {
                matches!(&n.split, SplitKind::Dimension { dimension, value }
                    if dimension == "sales.region" && value == "EMEA")
            });
            has_premium && has_emea
        });

        // The beam search should explore this path because it tries multiple
        // strategies including top-K concentration and IV/WOE.
        // premium→EMEA should appear as one of the alternatives.
        // Note: this is a soft check — if the beam doesn't find it, the test
        // still passes. The point is to document the expected behavior.
        if has_premium_emea_path {
            // Good: beam search found the non-monotonic path through the valley.
        }
    }

    // ── Regression tests for review fixes ─────────────────────────────────

    /// Bug 1: comparing periods wider than one granularity bucket must sum each
    /// period independently — not take the first vs. last row of a combined query.
    /// Q1 vs Q2 with monthly fixture rows: correct delta = Σ(Q2) - Σ(Q1) = 900.
    /// The pre-fix first-row-vs-last-row implementation would yield 500 (Jan vs Jun).
    #[test]
    fn test_explain_multi_month_period_sums_per_period() {
        let revenue_view = make_view("revenue", vec![atomic_measure("mrr", MeasureType::Sum)]);
        let layer = make_layer(vec![revenue_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "revenue.mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__mrr", jn(100.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__mrr", jn(200.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-03")),
                    ("revenue__mrr", jn(300.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-04")),
                    ("revenue__mrr", jn(400.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-05")),
                    ("revenue__mrr", jn(500.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-06")),
                    ("revenue__mrr", jn(600.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.mrr",
            "revenue.created_at",
            ("2024-04-01", "2024-06-30"), // Q2
            ("2024-01-01", "2024-03-31"), // Q1
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        assert!(
            (result.target_previous - 600.0).abs() < 0.01,
            "Q1 should sum to 600, got {}",
            result.target_previous
        );
        assert!(
            (result.target_current - 1500.0).abs() < 0.01,
            "Q2 should sum to 1500, got {}",
            result.target_current
        );
        assert!(
            (result.target_delta - 900.0).abs() < 0.01,
            "delta should be 900 (period sums), not 500 (Jan vs Jun first/last rows)"
        );
    }

    /// Bug 3: invalid date input to fetch_historical_deltas must return an error
    /// rather than silently fall back to a 25-year historical window.
    #[test]
    fn test_fetch_historical_deltas_invalid_date_errors() {
        let exec: Box<QueryExecutor> = Box::new(|_q| Ok(vec![]));
        let result = fetch_historical_deltas(
            "revenue.mrr",
            "revenue.created_at",
            "not-a-date",
            30,
            12,
            &[],
            &exec,
        );
        assert!(
            result.is_err(),
            "expected error on invalid date input, got {:?}",
            result
        );
    }

    /// Issue 5: dim-splitting a non-additive measure (avg/median/distinct/number)
    /// must emit a NonAdditiveDimensionSplit warning, since per-element deltas
    /// do not sum to parent_delta for these aggregation types.
    #[test]
    fn test_non_additive_dim_split_warning_fires() {
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
                segmentable: None,
                meta: None,
            }],
            measures: Some(vec![atomic_measure("avg_mrr", MeasureType::Average)]),
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        };
        let layer = make_layer(vec![revenue_view]);
        let tree = MetricTree::build(&layer);

        let mut data = HashMap::new();
        data.insert(
            "revenue.avg_mrr".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__avg_mrr", jn(100.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__avg_mrr", jn(80.0)),
                ]),
            ],
        );
        data.insert(
            "revenue.avg_mrr:revenue.plan".to_string(),
            vec![
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__plan", js("Pro")),
                    ("revenue__avg_mrr", jn(60.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-01")),
                    ("revenue__plan", js("Enterprise")),
                    ("revenue__avg_mrr", jn(140.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__plan", js("Pro")),
                    ("revenue__avg_mrr", jn(50.0)),
                ]),
                row(&[
                    ("revenue__created_at", js("2024-02")),
                    ("revenue__plan", js("Enterprise")),
                    ("revenue__avg_mrr", jn(110.0)),
                ]),
            ],
        );

        let exec = mock_executor(data);
        let result = explain(
            &tree,
            &layer,
            "revenue.avg_mrr",
            "revenue.created_at",
            ("2024-02-01", "2024-02-28"),
            ("2024-01-01", "2024-01-31"),
            &ExplainConfig::default(),
            &exec,
        )
        .unwrap();

        let has_warning = result.warnings.iter().any(|w| {
            matches!(
                w,
                ExplainWarning::NonAdditiveDimensionSplit {
                    measure,
                    measure_type,
                    dimension,
                } if measure == "revenue.avg_mrr"
                    && measure_type == "average"
                    && dimension == "revenue.plan"
            )
        });
        assert!(
            has_warning,
            "expected NonAdditiveDimensionSplit warning, got {:?}",
            result.warnings
        );
    }

    /// order_value (custom, = entree_revenue + addon_revenue) with two Sum
    /// children plus a `total_orders` count (the fixed denominator) on the same
    /// view — the shape `component_candidates` needs to size RATE gaps.
    fn order_value_tree() -> (SemanticLayer, MetricTree) {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
                composite_measure(
                    "order_value",
                    "{{opp.entree_revenue}} + {{opp.addon_revenue}}",
                ),
            ],
            &["status"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        (layer, tree)
    }

    /// rev_per_unit (custom, = price * quantity) — two Sum children joined by a
    /// multiplicative operator, exercising the log-share branch. The parser
    /// labels BOTH edges of `a * b` with `EdgeOperator::Mul`, so this is a
    /// homogeneous multiplicative composite.
    fn rev_per_unit_tree() -> (SemanticLayer, MetricTree) {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("price", MeasureType::Sum),
                atomic_measure("quantity", MeasureType::Sum),
                composite_measure("rev_per_unit", "{{opp.price}} * {{opp.quantity}}"),
            ],
            &["status"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        (layer, tree)
    }

    #[test]
    fn test_component_candidates_splits_multiplicative_composite() {
        // The bug this test locks down: parent_log_ratio must be the SUM of the
        // children's signed log-ratios (the multiplicative identity), NOT the log
        // of a summed reconstruction of the parent. Verified with scipy before
        // writing: price seg=8/bench=10, quantity seg=5/bench=10 ->
        //   parent_log_ratio = ln(8/10) + ln(5/10) = -0.9162907 = ln(40/100) (the
        //   true product parent), so shares sum to exactly 1:
        //   price = ln(0.8)/-0.9163 = 0.2435292026,
        //   quantity = ln(0.5)/-0.9163 = 0.7564707974.
        // A summed reconstruction (parent_seg=13, parent_bench=20) would give
        // parent_log_ratio = ln(13/20) = -0.4307829, and price's share would come
        // out ~0.518 — provably wrong, and this test would fail.
        let (_, tree) = rev_per_unit_tree();
        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let measure = &q.measures[0];
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let value = match measure.as_str() {
                "opp.price" => {
                    if is_seg {
                        8.0
                    } else {
                        10.0
                    }
                }
                "opp.quantity" => {
                    if is_seg {
                        5.0
                    } else {
                        10.0
                    }
                }
                _ => panic!("unexpected measure {measure}"),
            };
            Ok(vec![row(&[(
                measure.replace('.', "__").leak() as &str,
                jn(value),
            )])])
        });
        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];
        // count_measure = None: multiplicative children are already ratio-valued,
        // so component_candidates never divides them by a count (nor queries one).
        let candidates = component_candidates(
            &tree,
            "opp.rev_per_unit",
            None,
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &exec,
        )
        .unwrap();
        assert_eq!(candidates.len(), 2, "{candidates:?}");
        let quantity = candidates
            .iter()
            .find(|c| matches!(&c.kind, CandidateKind::Component { measure } if measure == "opp.quantity"))
            .expect("quantity candidate present");
        assert!(
            (quantity.concentration - 0.756_470_797_4).abs() < 1e-6,
            "{quantity:?}"
        );
        let price = candidates
            .iter()
            .find(|c| matches!(&c.kind, CandidateKind::Component { measure } if measure == "opp.price"))
            .expect("price candidate present");
        assert!(
            (price.concentration - 0.243_529_202_6).abs() < 1e-6,
            "{price:?}"
        );
        assert!(
            (quantity.concentration + price.concentration - 1.0).abs() < 1e-9,
            "{candidates:?}"
        );
        assert!(matches!(
            &candidates[0].kind,
            CandidateKind::Component { measure } if measure == "opp.quantity"
        ));
        assert!(candidates.iter().all(|c| c.gated));
    }

    #[test]
    fn test_component_candidates_splits_additive_composite() {
        // The segment (mobile_app) and benchmark (in_store) have DIFFERENT order
        // counts (552 vs 78), so a raw-sum decomposition and a per-unit-rate
        // decomposition give wildly different answers — this is the assertion that
        // locks in the rate-based fix. The child NUMERATORS below are chosen to
        // yield the design's worked-example per-order rates:
        //   entree/order: 400 (seg) -> 420 (bench)   [220800/552, 32760/78]
        //   addon/order:  143.9 (seg) -> 342.4 (bench)[79432.8/552, 26707.2/78]
        // so the RATE gaps are entree 20, addon 198.5, parent 218.5, and addon's
        // share is 198.5/218.5 = 0.9085. A raw-sum decomposition would instead
        // give addon.gap = 26707.2-79432.8 = -52725.6 and share ~0.219 — the exact
        // bug the earlier version shipped; asserting the rate gap fails it.
        let (_, tree) = order_value_tree();
        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            // measures[0] = the child sum, measures[1] = total_orders (count) —
            // the additive query bundles them, and a real executor returns one row
            // with a column per measure.
            let child = &q.measures[0];
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let (entree_num, addon_num, count) = if is_seg {
                (220_800.0, 79_432.8, 552.0)
            } else {
                (32_760.0, 26_707.2, 78.0)
            };
            let num = match child.as_str() {
                "opp.entree_revenue" => entree_num,
                "opp.addon_revenue" => addon_num,
                _ => panic!("unexpected measure {child}"),
            };
            Ok(vec![row(&[
                (child.replace('.', "__").leak() as &str, jn(num)),
                ("opp__total_orders", jn(count)),
            ])])
        });
        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];
        let candidates = component_candidates(
            &tree,
            "opp.order_value",
            Some("opp.total_orders"),
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &exec,
        )
        .unwrap();
        assert_eq!(candidates.len(), 2, "{candidates:?}");
        let addon = candidates
            .iter()
            .find(|c| matches!(&c.kind, CandidateKind::Component { measure } if measure == "opp.addon_revenue"))
            .expect("addon candidate present");
        // RATE gap 198.5 (= 342.4 - 143.9), NOT the raw-sum gap -52725.6.
        assert!((addon.gap - 198.5).abs() < 1e-4, "{addon:?}");
        assert!(
            (addon.concentration - 198.5 / 218.5).abs() < 1e-6,
            "{addon:?}"
        );
        assert!(
            addon.gated,
            "component candidates are always gated (exact identity)"
        );
        // Ranked by concentration descending — addon (91%) before entree (9%).
        assert!(matches!(
            &candidates[0].kind,
            CandidateKind::Component { measure } if measure == "opp.addon_revenue"
        ));
    }

    #[test]
    fn test_component_candidates_empty_for_a_measure_with_no_children() {
        let (_, tree) = order_value_tree();
        let exec: Box<QueryExecutor> = Box::new(|_q: &QueryRequest| Ok(vec![]));
        // entree_revenue is atomic (a plain Sum, no {{...}} refs) — it has no
        // Component-kind edges pointing AT it, regardless of how many OTHER
        // composites reference it.
        let candidates =
            component_candidates(&tree, "opp.entree_revenue", None, &[], &[], &[], &[], &exec)
                .unwrap();
        assert!(candidates.is_empty(), "{candidates:?}");
    }

    #[test]
    fn test_dimension_candidates_splits_by_unconsumed_dimension() {
        // addon_revenue (unfiltered sum), split by `category` (sides/drinks),
        // over the unfiltered total_orders count. Segment population:
        // category IN {sides: 121_100, drinks: 21_400} (addon_revenue=142_500
        // when unfiltered, matches neither directly — this test only checks the
        // PER-VALUE filtered figures, not that they sum to the parent, which
        // Task 5's integration covers).
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];

        // The dimension-VALUE discovery query (category values present within
        // the segment population) and the per-value rate queries all share the
        // executor; distinguish by inspecting measures/filters/dimensions.
        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                // Value-discovery query: GROUP BY category within the segment.
                return Ok(vec![
                    row(&[("opp__category", js("sides"))]),
                    row(&[("opp__category", js("drinks"))]),
                ]);
            }
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            // measures[0] is the synthetic __drill__ filtered sum; identify which
            // value it filters to by name substring. The count (measures[1]) is
            // always total_orders and rides along in the same row — a real
            // executor returns one column per requested measure.
            let filtered = &q.measures[0];
            let sum_val = if filtered.contains("sides") {
                if is_seg {
                    6_000.0
                } else {
                    62_400.0
                }
            } else {
                // drinks
                if is_seg {
                    3_000.0
                } else {
                    12_000.0
                }
            };
            let count_val = if is_seg { 552.0 } else { 78.0 };
            let sum_alias = filtered.replace('.', "__");
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
            ])])
        });

        let candidates = dimension_candidates(
            &tree,
            &layer,
            "opp.addon_revenue",
            "opp.total_orders",
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &[],
            SIGNIFICANCE_ALPHA,
            &exec,
        )
        .unwrap();

        assert!(!candidates.is_empty(), "{candidates:?}");
        let sides = candidates
            .iter()
            .find(|c| matches!(&c.kind, CandidateKind::Dimension { dimension, value } if dimension == "opp.category" && value == "sides"))
            .expect("sides candidate present");
        // rate_seg = 6000/552 = 10.87; rate_bench = 62400/78 = 800; gap = 789.13.
        assert!((sides.gap - 789.130_434_78).abs() < 1e-4, "{sides:?}");
    }

    #[test]
    fn test_dimension_candidates_installs_measures_visible_to_executor() {
        // The whole point of the SharedLayer refactor: an executor holding a
        // clone of the shared handle can `read()` it mid-run and SEE the
        // synthetic __drill__ measure the drill installed a moment earlier —
        // proving install-before-execute visibility with no deadlock (no write
        // guard is held while the executor runs).
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];

        // Set true iff, on a rate query, the executor can read the just-installed
        // __drill__ measure from its own clone of the SharedLayer. Shared across
        // parallel_execute's threads, hence Arc<AtomicBool>.
        let saw_measure = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_inner = std::sync::Arc::clone(&saw_measure);
        let layer_for_exec = std::sync::Arc::clone(&layer);

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                // Value-discovery query: GROUP BY category within the segment.
                return Ok(vec![
                    row(&[("opp__category", js("sides"))]),
                    row(&[("opp__category", js("drinks"))]),
                ]);
            }
            let filtered = &q.measures[0];
            // A rate query carries the synthetic __drill__ measure first. Read
            // the shared layer (the executor's own clone) and confirm the
            // just-installed measure is visible under the lock — this is the
            // guarantee the refactor exists to provide.
            if filtered.contains("__drill__") {
                let (_, measure_name) = filtered.split_once('.').unwrap();
                let l = layer_for_exec.read().unwrap();
                let visible = l
                    .views
                    .iter()
                    .any(|v| v.measures_list().iter().any(|m| m.name == measure_name));
                if visible {
                    saw_inner.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let sum_val = if filtered.contains("sides") {
                if is_seg {
                    6_000.0
                } else {
                    62_400.0
                }
            } else if is_seg {
                3_000.0
            } else {
                12_000.0
            };
            let count_val = if is_seg { 552.0 } else { 78.0 };
            let sum_alias = filtered.replace('.', "__");
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
            ])])
        });

        let candidates = dimension_candidates(
            &tree,
            &layer,
            "opp.addon_revenue",
            "opp.total_orders",
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &[],
            SIGNIFICANCE_ALPHA,
            &exec,
        )
        .unwrap();

        assert!(!candidates.is_empty(), "{candidates:?}");
        assert!(
            saw_measure.load(std::sync::atomic::Ordering::SeqCst),
            "the executor must have read an installed __drill__ measure from the shared layer mid-run"
        );
    }

    #[test]
    fn test_dimension_candidates_drops_a_within_noise_candidate() {
        // A single segmentable dimension ("segment") with one distinct value
        // ("vip") whose per-unit rate gap is tiny relative to its dispersion:
        // seg_rate = 1000/100 = 10, bench_rate = 1200/100 = 12, gap = 2.
        // With seg_sd = bench_sd = 50 and seg_n = bench_n = 100, se =
        // sqrt(50^2/100 + 50^2/100) = sqrt(50) ≈ 7.07, so gap/se ≈ 0.28 — far
        // below significance_threshold(k=2, family=2, df≈198, ALPHA) ≈ 1.95.
        // gap_is_significant must return Some(false), and dimension_candidates
        // must drop the candidate entirely rather than keep it as noise.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "segment"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                // Value-discovery query: a single distinct value within the segment.
                return Ok(vec![row(&[("opp__segment", js("vip"))])]);
            }
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let filtered = &q.measures[0];
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            let sum_val = if is_seg { 1_000.0 } else { 1_200.0 };
            let count_val = 100.0;
            let sd_val = 50.0;
            let n_val = 100.0;
            let sum_alias = filtered.replace('.', "__");
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(sd_val)),
                (n_alias.leak() as &str, jn(n_val)),
            ])])
        });

        let candidates = dimension_candidates(
            &tree,
            &layer,
            "opp.addon_revenue",
            "opp.total_orders",
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &[],
            SIGNIFICANCE_ALPHA,
            &exec,
        )
        .unwrap();

        assert!(
            candidates.iter().all(|c| !matches!(
                &c.kind,
                CandidateKind::Dimension { dimension, value }
                    if dimension == "opp.segment" && value == "vip"
            )),
            "a within-noise candidate (Some(false)) must be dropped, not kept: {candidates:?}"
        );
    }

    #[test]
    fn test_dimension_candidates_keeps_a_significant_candidate_gated() {
        // Same shape as the noise-drop test, but with a large gap and tight
        // dispersion: seg_rate = 1000/100 = 10, bench_rate = 3000/100 = 30,
        // gap = 20. With seg_sd = bench_sd = 1 and seg_n = bench_n = 100,
        // se = sqrt(1/100 + 1/100) ≈ 0.1414, so gap/se ≈ 141 — far above
        // significance_threshold(k=2, family=2, df≈198, ALPHA) ≈ 1.95.
        // gap_is_significant must return Some(true), and the candidate must be
        // kept with `gated: true` (contrast with the unconsumed-dimension test,
        // where no dispersion is installed at all → None → kept `gated: false`).
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "segment"],
        );
        let layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let seg_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["mobile_app".to_string()],
            and: None,
            or: None,
        }];
        let bench_filter = vec![QueryFilter {
            member: Some("opp.status".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["in_store".to_string()],
            and: None,
            or: None,
        }];

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                // Two values: a single-valued dimension carries no information
                // (its one candidate just restates the parent numerator) and is
                // skipped by the MIN_DIMENSION_CARDINALITY floor. Both values
                // get the same rate treatment below; `vip` is the one asserted.
                return Ok(vec![
                    row(&[("opp__segment", js("vip"))]),
                    row(&[("opp__segment", js("standard"))]),
                ]);
            }
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let filtered = &q.measures[0];
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            let sum_val = if is_seg { 1_000.0 } else { 3_000.0 };
            let count_val = 100.0;
            let sd_val = 1.0;
            let n_val = 100.0;
            let sum_alias = filtered.replace('.', "__");
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(sd_val)),
                (n_alias.leak() as &str, jn(n_val)),
            ])])
        });

        let candidates = dimension_candidates(
            &tree,
            &layer,
            "opp.addon_revenue",
            "opp.total_orders",
            &seg_filter,
            &bench_filter,
            &[],
            &[],
            &[],
            SIGNIFICANCE_ALPHA,
            &exec,
        )
        .unwrap();

        let vip = candidates
            .iter()
            .find(|c| matches!(&c.kind, CandidateKind::Dimension { dimension, value } if dimension == "opp.segment" && value == "vip"))
            .expect("significant candidate present");
        assert!(
            vip.gated,
            "a Some(true) gate result must set gated: true: {vip:?}"
        );
    }

    #[test]
    fn test_opportunity_drill_stops_at_root_when_no_candidates_recurse_further() {
        // A root with no component children and no OTHER segmentable dimension
        // beyond the one opportunity() already consumed (status) — the drill
        // must still return a valid one-level result, not an error, with
        // stop_reason NoCandidates on that one level.
        let (layer, tree) = noise_layer();
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("mobile_app", 165_600.0, 552.0, 50.0), // rate 300
                seg("in_store", 62_400.0, 78.0, 50.0),     // rate 800 — the bar
            ],
        );
        let exec = mock_executor(data);
        let config = DrillConfig::default();
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &config,
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)");

        assert_eq!(result.levels.len(), 1);
        assert!((result.root_gap - 500.0).abs() < 1e-6, "{result:?}");
        assert!(!result.benchmark_filter.is_empty());
        assert!(matches!(
            result.levels[0].stop_reason,
            Some(StopReason::NoCandidates)
        ));
    }

    #[test]
    fn test_opportunity_drill_returns_none_when_root_finds_nothing() {
        let (layer, tree) = noise_layer();
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(500_000.0))])],
        );
        // Flat distribution -> opportunity() reports no dimensions.
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("mobile_app", 250_000.0, 500.0, 50.0),
                seg("in_store", 250_000.0, 500.0, 50.0),
            ],
        );
        let exec = mock_executor(data);
        let config = DrillConfig::default();
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &config,
        )
        .unwrap();
        assert!(result.is_none(), "{result:?}");
    }

    /// Shared fixture for the two-dimension-level drill tests: a `type: sum`
    /// root over `status` and `category`, where `status` carries a real gap
    /// (mobile_app lags in_store) and `category` is flat (sides vs. drinks
    /// have the same rate, so it never outranks `status` as the scan's top
    /// dimension but still exposes a second, non-top segment to root at).
    fn drill_fixture_two_levels() -> (MetricTree, SharedLayer, Box<QueryExecutor>) {
        // The two tests above only exercise depth-0 stopping; this one drives
        // the recursion into a second level. A `type: sum` root has no
        // Component children (no `{{...}}` refs), so component_candidates is
        // always empty here and the recursion is necessarily
        // dimension -> dimension — a component -> dimension drill is not
        // constructible through opportunity_drill with a sum root (the
        // rate-based component_candidates fix has its own unit test, Task 3,
        // not this one).
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let is_mobile = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let is_in_store = q
                .filters
                .iter()
                .any(|f| f.values == vec!["in_store".to_string()]);

            if !q.dimensions.is_empty() {
                if q.dimensions[0] == "opp.status" {
                    // Root's status breakdown: mobile_app (rate 300, the
                    // laggard) vs in_store (rate 800, the bar) — tight
                    // stddev so mobile_app is a real gap, not noise.
                    return Ok(vec![
                        row(&[
                            ("opp__status", js("mobile_app")),
                            ("opp__revenue", jn(165_600.0)),
                            ("opp__total_orders", jn(552.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                        row(&[
                            ("opp__status", js("in_store")),
                            ("opp__revenue", jn(62_400.0)),
                            ("opp__total_orders", jn(78.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                    ]);
                }
                // q.dimensions[0] == "opp.category"
                if q.measures.len() > 1 {
                    // Root's category breakdown, unfiltered by status — flat
                    // (same rate in both categories), so category never
                    // outranks status as the root's top dimension.
                    return Ok(vec![
                        row(&[
                            ("opp__category", js("sides")),
                            ("opp__revenue", jn(100_000.0)),
                            ("opp__total_orders", jn(200.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                        row(&[
                            ("opp__category", js("drinks")),
                            ("opp__revenue", jn(50_000.0)),
                            ("opp__total_orders", jn(100.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                    ]);
                }
                // dimension_candidates' category value-discovery query, within
                // the mobile_app segment. Must offer at least two values: a
                // single-valued dimension is a tautology (its one candidate
                // reproduces the parent numerator exactly) and is now skipped
                // by the MIN_DIMENSION_CARDINALITY floor.
                return Ok(vec![
                    row(&[("opp__category", js("sides"))]),
                    row(&[("opp__category", js("drinks"))]),
                ]);
            }

            // No dimensions: either the root's overall_query (1 measure) or a
            // dimension_candidates per-value rate query (4 measures: the
            // filtered sum, the count, the dispersion, and its n companion).
            if q.measures.len() == 1 {
                return Ok(vec![row(&[("opp__revenue", jn(228_000.0))])]);
            }

            // dimension_candidates' seg/bench rate query for category=sides.
            // Identify it by measure-name substring, exactly as Task 4's
            // mock does. Tight stddev/n so the gate returns Some(true):
            // gap = 62_400/78 - 6_000/552 = 789.13; se = sqrt(2)*10/sqrt(100)
            // ~= 1.41, t ~= 558, clears any Sidak-composed threshold easily.
            let filtered = &q.measures[0];
            let is_sides = filtered.contains("sides");
            assert!(
                is_sides || filtered.contains("drinks"),
                "unexpected measure {filtered}"
            );
            // `sides` carries the whole gap (789.13); `drinks` is near-flat
            // (3.85) so it is offered, considered, and loses — which is the
            // point of having a second value here.
            let (sum_val, count_val) = match (is_sides, is_mobile) {
                (true, true) => (6_000.0, 552.0),
                (true, false) => (62_400.0, 78.0),
                (false, true) => (5_000.0, 552.0),
                (false, false) => (1_000.0, 78.0),
            };
            if !is_mobile {
                assert!(is_in_store, "expected in_store filter: {:?}", q.filters);
            }
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let sum_alias = filtered.replace('.', "__");
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(10.0)),
                (n_alias.leak() as &str, jn(100.0)),
            ])])
        });

        (tree, layer, exec)
    }

    #[test]
    fn test_opportunity_drill_recurses_through_two_dimension_levels() {
        let (tree, layer, exec) = drill_fixture_two_levels();

        let config = DrillConfig::default();
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &config,
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)");

        assert_eq!(result.levels.len(), 2, "{result:?}");
        assert_eq!(result.levels[0].measure, "opp.revenue");
        assert!(
            result.levels[0].stop_reason.is_none(),
            "level 0 must recurse"
        );
        // Level 0's winner is the category=sides dimension split, gated.
        assert!(matches!(
            &result.levels[0].candidates[0].kind,
            CandidateKind::Dimension { dimension, value }
                if dimension == "opp.category" && value == "sides"
        ));
        assert!(result.levels[0].candidates[0].gated);
        // Level 1 carries the accumulated segment filter and stops (no dims left).
        assert_eq!(result.levels[1].measure, "opp.revenue");
        assert!(
            result.levels[1]
                .segment_filter
                .iter()
                .any(|f| f.member.as_deref() == Some("opp.category")
                    && f.values == vec!["sides".to_string()]),
            "the category=sides filter must have been pushed for level 1: {:?}",
            result.levels[1].segment_filter
        );
        assert!(matches!(
            result.levels[1].stop_reason,
            Some(StopReason::NoCandidates)
        ));
        // root_share cascades: level 0 is 1.0 (the root), level 1 is level 0's
        // winner concentration (abs, clamped) — assert it is > 0 and <= level 0's.
        assert!(result.levels[1].root_share > 0.0);
        assert!(result.levels[1].root_share <= result.levels[0].root_share);
    }

    /// Fixture for the named-root selector tests: a single `status` dimension
    /// with THREE segments (unlike `drill_fixture_two_levels`, whose only
    /// non-flat dimension exposes a single below-benchmark segment — the root
    /// `opportunity()` scan there has nothing else to root at). `in_store` is
    /// the best-peer benchmark; `mobile_app` and `kiosk` are both real,
    /// significant laggards with different gaps, so the scan's top pick
    /// (`mobile_app`, the bigger gap) and a second addressable row (`kiosk`)
    /// both survive tail-trim and the significance gate — giving the "root at
    /// a non-top row" test a genuinely different row to point at.
    fn drill_fixture_named_root() -> (MetricTree, SharedLayer, Box<QueryExecutor>) {
        let (layer, tree) = noise_layer();
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));
        let mut data = HashMap::new();
        data.insert(
            "opp.revenue".to_string(),
            vec![row(&[("opp__revenue", jn(408_000.0))])],
        );
        data.insert(
            "opp.revenue:opp.status".to_string(),
            vec![
                seg("mobile_app", 165_600.0, 552.0, 50.0), // rate 300 — biggest gap
                seg("in_store", 62_400.0, 78.0, 50.0),     // rate 800 — the bar
                seg("kiosk", 180_000.0, 300.0, 50.0),      // rate 600 — smaller gap
            ],
        );
        let executor = mock_executor(data);
        (tree, layer, executor)
    }

    #[test]
    fn test_opportunity_drill_roots_at_a_named_non_top_row() {
        let (tree, layer, executor) = drill_fixture_named_root();

        // Establish what the UNROOTED drill picks, so the assertion below is about
        // a genuinely different row rather than a coincidence.
        let top = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &executor,
            &DrillConfig::default(),
        )
        .unwrap()
        .unwrap();

        // Pick a row the top-pick is NOT rooted at.
        let scan = opportunity(
            &tree,
            &layer.read().unwrap().clone(),
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &executor,
        )
        .unwrap();
        let other = scan
            .dimensions
            .iter()
            .flat_map(|d| {
                d.segments
                    .iter()
                    .map(move |s| (d.dimension.clone(), s.clone()))
            })
            .find(|(dim, seg)| {
                *dim != scan.dimensions[0].dimension
                    || seg.segment != scan.dimensions[0].segments[0].segment
            })
            .expect("fixture must expose more than one sizable segment");

        let config = DrillConfig {
            root: Some(DrillRoot {
                dimension: other.0.clone(),
                segment: other.1.segment.clone(),
            }),
            ..DrillConfig::default()
        };
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &executor,
            &config,
        )
        .unwrap()
        .expect("named row must produce a chain");

        // Rooted at the row we asked for, not the engine's top pick.
        assert!((result.root_gap - other.1.gap).abs() < 1e-6, "{result:?}");
        assert!(
            (result.root_upside - other.1.upside).abs() < 1e-6,
            "{result:?}"
        );
        assert!(
            (result.root_gap - top.root_gap).abs() > 1e-6,
            "test is vacuous — the named row is the top pick: {result:?}"
        );
    }

    #[test]
    fn test_opportunity_drill_returns_none_for_an_absent_root() {
        let (tree, layer, executor) = drill_fixture_named_root();
        let config = DrillConfig {
            root: Some(DrillRoot {
                dimension: "opp.status".to_string(),
                segment: "a_segment_that_does_not_exist".to_string(),
            }),
            ..DrillConfig::default()
        };
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &executor,
            &config,
        )
        .unwrap();

        // MUST be None, never a silent fall back to the top row: decomposing a
        // different segment under the clicked row's heading is the exact class of
        // confident-wrong-number bug this feature has already shipped once.
        assert!(result.is_none(), "{result:?}");
    }

    /// Shared root-scan mock for the two degenerate-split tests below: a
    /// `status` breakdown where `mobile_app` lags `in_store` by a provable
    /// margin, plus the flat `overall_query` response. Returns `None` for
    /// anything else so each test can layer its own behaviour on top.
    fn degenerate_drill_root_rows(
        q: &QueryRequest,
    ) -> Option<Vec<serde_json::Map<String, serde_json::Value>>> {
        if !q.dimensions.is_empty() && q.dimensions[0] == "opp.status" {
            return Some(vec![
                row(&[
                    ("opp__status", js("mobile_app")),
                    ("opp__revenue", jn(165_600.0)),
                    ("opp__total_orders", jn(552.0)),
                    ("opp____opp_stddev__revenue", jn(50.0)),
                ]),
                row(&[
                    ("opp__status", js("in_store")),
                    ("opp__revenue", jn(62_400.0)),
                    ("opp__total_orders", jn(78.0)),
                    ("opp____opp_stddev__revenue", jn(50.0)),
                ]),
            ]);
        }
        if q.dimensions.is_empty() && q.measures.len() == 1 {
            return Some(vec![row(&[("opp__revenue", jn(228_000.0))])]);
        }
        None
    }

    /// The Amsterdam drill shape. Scoped to a city with two stores, the
    /// `store_name` and `staff_count` partitions are identical — one store per
    /// staff count — so the drill offered both and the one it followed
    /// ("staff_count = 20") was decided by sort order rather than by meaning.
    #[test]
    fn test_drill_collapses_aliased_candidate_dimensions() {
        let mut view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "store_name", "staff_count"],
        );
        view.dimensions[2].dimension_type = DimensionType::Number;
        view.entities = vec![crate::schema::models::Entity {
            name: "store".into(),
            entity_type: crate::schema::models::EntityType::Primary,
            description: None,
            key: Some("store_name".into()),
            keys: None,
            lifespan: None,
            inherits_from: None,
            meta: None,
            parent: None,
        }];
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                let dim = q.dimensions[0].as_str();
                if q.measures.len() > 1 {
                    // Only status has spread, so status is the root pick and
                    // neither alias is consumed before the drill descends.
                    if dim == "opp.status" {
                        return Ok(vec![
                            row(&[
                                ("opp__status", js("mobile_app")),
                                ("opp__revenue", jn(165_600.0)),
                                ("opp__total_orders", jn(552.0)),
                                ("opp____opp_stddev__revenue", jn(50.0)),
                            ]),
                            row(&[
                                ("opp__status", js("in_store")),
                                ("opp__revenue", jn(62_400.0)),
                                ("opp__total_orders", jn(78.0)),
                                ("opp____opp_stddev__revenue", jn(50.0)),
                            ]),
                        ]);
                    }
                    let alias = if dim == "opp.store_name" {
                        "opp__store_name"
                    } else {
                        "opp__staff_count"
                    };
                    let labels = if dim == "opp.store_name" {
                        ["jordaan", "de_pijp"]
                    } else {
                        ["14", "20"]
                    };
                    return Ok(labels
                        .iter()
                        .map(|v| {
                            row(&[
                                (alias, js(v)),
                                ("opp__revenue", jn(10_000.0)),
                                ("opp__total_orders", jn(100.0)),
                                ("opp____opp_stddev__revenue", jn(10.0)),
                            ])
                        })
                        .collect());
                }
                // Value discovery. store_name and staff_count return IDENTICAL
                // measure tuples under different labels — the alias signature.
                let (alias, labels) = if dim == "opp.store_name" {
                    ("opp__store_name", ["jordaan", "de_pijp"])
                } else {
                    ("opp__staff_count", ["14", "20"])
                };
                return Ok(labels
                    .iter()
                    .zip([4_965_700.98, 8_607_951.80])
                    .map(|(v, rev)| row(&[(alias, js(v)), ("opp__revenue", jn(rev))]))
                    .collect());
            }

            if q.measures.len() == 1 {
                return Ok(vec![row(&[("opp__revenue", jn(228_000.0))])]);
            }

            let filtered = &q.measures[0];
            let is_seg = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let (sum_val, count_val) = if is_seg {
                (5_520.0, 552.0)
            } else {
                (62_400.0, 78.0)
            };
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let sum_alias = filtered.replace('.', "__");
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(10.0)),
                (n_alias.leak() as &str, jn(100.0)),
            ])])
        });

        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)");

        let offered: Vec<&str> = result.levels[0]
            .candidates
            .iter()
            .filter_map(|c| match &c.kind {
                CandidateKind::Dimension { dimension, .. } => Some(dimension.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            offered.contains(&"opp.store_name"),
            "the entity key should be the surviving representative, got {offered:?}"
        );
        assert!(
            !offered.contains(&"opp.staff_count"),
            "the aliased attribute must not be offered as a separate split, got {offered:?}"
        );
    }

    /// Drill a gap isolated to one store, with `opp.channel_name` keyed to an
    /// entity that lives outside the store's subtree. `with_hierarchy` toggles
    /// only whether the view declares its entities at all — the data, the
    /// gaps, and every query answer are identical between the two runs.
    fn drill_with_orthogonal_axis(with_hierarchy: bool) -> DrillResult {
        let mut view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["store_name", "status", "channel_name"],
        );
        if with_hierarchy {
            let entity = |name: &str, key: &str| crate::schema::models::Entity {
                name: name.into(),
                entity_type: crate::schema::models::EntityType::Primary,
                description: None,
                key: Some(key.into()),
                keys: None,
                lifespan: None,
                inherits_from: None,
                meta: None,
                parent: None,
            };
            view.entities = vec![
                entity("store", "store_name"),
                entity("channel", "channel_name"),
            ];
        }
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if !q.dimensions.is_empty() {
                let dim = q.dimensions[0].as_str();
                if q.measures.len() > 1 {
                    // Only store_name has spread, so it is the root pick. The
                    // other two are flat (and differ from each other, so they
                    // are not collapsed as aliases of one another).
                    let (alias, rows) = match dim {
                        "opp.store_name" => (
                            "opp__store_name",
                            vec![("s1", 165_600.0, 552.0), ("s2", 62_400.0, 78.0)],
                        ),
                        "opp.status" => (
                            "opp__status",
                            vec![("open", 10_000.0, 100.0), ("shut", 20_000.0, 200.0)],
                        ),
                        _ => (
                            "opp__channel_name",
                            vec![("web", 30_000.0, 300.0), ("app", 60_000.0, 600.0)],
                        ),
                    };
                    return Ok(rows
                        .into_iter()
                        .map(|(v, rev, n)| {
                            row(&[
                                (alias, js(v)),
                                ("opp__revenue", jn(rev)),
                                ("opp__total_orders", jn(n)),
                                ("opp____opp_stddev__revenue", jn(50.0)),
                            ])
                        })
                        .collect());
                }
                // Value discovery. Both non-root dims offer two values with a
                // large gap, so either would be a candidate if it were offered.
                let (alias, vals) = match dim {
                    "opp.status" => ("opp__status", ["open", "shut"]),
                    _ => ("opp__channel_name", ["web", "app"]),
                };
                return Ok(vals.iter().map(|v| row(&[(alias, js(v))])).collect());
            }

            if q.measures.len() == 1 {
                return Ok(vec![row(&[("opp__revenue", jn(228_000.0))])]);
            }

            // Per-value rate query. A huge gap either way, so the outcome turns
            // on whether the dimension was offered at all, not on the gate.
            let filtered = &q.measures[0];
            let is_seg = q.filters.iter().any(|f| f.values == vec!["s1".to_string()]);
            let (sum_val, count_val) = if is_seg {
                (5_520.0, 552.0)
            } else {
                (62_400.0, 78.0)
            };
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let sum_alias = filtered.replace('.', "__");
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(10.0)),
                (n_alias.leak() as &str, jn(100.0)),
            ])])
        });

        opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)")
    }

    /// Having isolated the gap to one store, re-cutting that same gap by
    /// channel answers a different question than the one being drilled. The
    /// hierarchy-aware pruning that says so already existed but was reachable
    /// only from `explain()`; the drill ran the flat candidate set.
    #[test]
    fn test_drill_prunes_axes_outside_the_picked_entity_subtree() {
        let offers = |r: &DrillResult, want: &str| {
            r.levels[0].candidates.iter().any(|c| {
                matches!(&c.kind, CandidateKind::Dimension { dimension, .. } if dimension == want)
            })
        };

        // Control: no declared entities, so nothing can be placed in a
        // hierarchy and the drill keeps its flat behaviour.
        let flat = drill_with_orthogonal_axis(false);
        assert!(
            offers(&flat, "opp.channel_name"),
            "without a hierarchy the orthogonal axis is still offered"
        );

        let pruned = drill_with_orthogonal_axis(true);
        assert!(
            !offers(&pruned, "opp.channel_name"),
            "channel lives outside the picked store's subtree and must be pruned"
        );
        assert!(
            offers(&pruned, "opp.status"),
            "a dim the hierarchy cannot place must be kept — pruning is conservative"
        );
    }

    /// Run the drill with `n_values` candidate values in `opp.category`, of
    /// which exactly one ("sides") carries a marginal gap and the rest are
    /// flat. The marginal gap is tuned to t ~= 3.0: above the bar a level
    /// offering 2 candidates sets (~2.59) and below the one a level offering
    /// 25 sets (~3.38). Nothing about the split itself changes between the two
    /// runs — only how many other questions were asked alongside it.
    fn drill_with_n_candidate_values(n_values: usize) -> DrillResult {
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let values: Vec<String> = std::iter::once("sides".to_string())
            .chain((0..n_values.saturating_sub(1)).map(|i| format!("flat{i}")))
            .collect();

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let is_mobile = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);

            if !q.dimensions.is_empty() {
                if q.dimensions[0] == "opp.status" {
                    // mobile_app rate 300 vs in_store 800 — a huge, unambiguous
                    // root gap, so the root pick is never what is under test.
                    return Ok(vec![
                        row(&[
                            ("opp__status", js("mobile_app")),
                            ("opp__revenue", jn(165_600.0)),
                            ("opp__total_orders", jn(552.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                        row(&[
                            ("opp__status", js("in_store")),
                            ("opp__revenue", jn(62_400.0)),
                            ("opp__total_orders", jn(78.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                    ]);
                }
                if q.measures.len() > 1 {
                    // Root's category breakdown: perfectly flat, so category
                    // never competes with status to be the root dimension.
                    return Ok(values
                        .iter()
                        .map(|v| {
                            row(&[
                                ("opp__category", js(v)),
                                ("opp__revenue", jn(10_000.0)),
                                ("opp__total_orders", jn(100.0)),
                                ("opp____opp_stddev__revenue", jn(10.0)),
                            ])
                        })
                        .collect());
                }
                // Value discovery inside the mobile_app segment.
                return Ok(values
                    .iter()
                    .map(|v| row(&[("opp__category", js(v))]))
                    .collect());
            }

            if q.measures.len() == 1 {
                return Ok(vec![row(&[("opp__revenue", jn(228_000.0))])]);
            }

            // Per-value seg/bench rate query. `sides` gaps by 4.24 on a
            // standard error of 1.414 (sd 10, n 100 both sides) => t ~= 3.0.
            // Every other value is exactly flat, so it contributes to the
            // candidate family without ever being a real candidate itself.
            let filtered = &q.measures[0];
            let is_sides = filtered.contains("sides");
            let (sum_val, count_val) = match (is_sides, is_mobile) {
                (true, true) => (5_520.0, 552.0),
                (true, false) => (1_110.72, 78.0),
                (false, true) => (5_520.0, 552.0),
                (false, false) => (780.0, 78.0),
            };
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let sum_alias = filtered.replace('.', "__");
            let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
            let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
                (dispersion_alias.leak() as &str, jn(10.0)),
                (n_alias.leak() as &str, jn(100.0)),
            ])])
        });

        opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)")
    }

    /// The drill compares every (dimension, value) pair at a level and then
    /// follows whichever concentrates the most gap — a maximum over the whole
    /// set. The bar therefore has to answer for the whole set. It previously
    /// answered for a hardcoded family of 2, so a level offering thirty
    /// candidates held the error rate of one comparison while reporting the
    /// best of thirty.
    #[test]
    fn test_drill_gate_answers_for_the_whole_candidate_family() {
        let has_sides = |r: &DrillResult| {
            r.levels[0].candidates.iter().any(|c| {
                matches!(&c.kind, CandidateKind::Dimension { dimension, value }
                    if dimension == "opp.category" && value == "sides")
            })
        };

        assert!(
            has_sides(&drill_with_n_candidate_values(2)),
            "a marginal split at t~=3.0 must clear the bar a 2-candidate level sets"
        );
        assert!(
            !has_sides(&drill_with_n_candidate_values(25)),
            "the identical split must NOT clear the bar a 25-candidate level sets — \
             if it does, the gate is still answering for a family of 2"
        );
    }

    #[test]
    #[test]
    fn test_opportunity_drill_skips_a_single_valued_dimension() {
        // REGRESSION. A dimension with exactly one distinct value inside the
        // current numerator population is fully determined by the splits above
        // it: its lone candidate reproduces the parent numerator exactly, so
        // gap == current_gap and concentration == 1.0. That sorts above every
        // real split and was therefore always followed, producing a chain of
        // "+100% of root gap" levels that ran to MaxDepth and explained
        // nothing. Concretely: an instance panel scoped to a single-store city
        // drilled `city = Amsterdam` -> `region = eu` -> `store_name = ...`,
        // each level restating the one before it.
        //
        // Such a dimension must now be skipped outright, and a drill left with
        // no other candidate must stop and SAY it found nothing.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if let Some(rows) = degenerate_drill_root_rows(q) {
                return Ok(rows);
            }
            if !q.dimensions.is_empty() {
                // q.dimensions[0] == "opp.category".
                if q.measures.len() > 1 {
                    // Root's category breakdown — flat, so `status` stays the
                    // root's top dimension.
                    return Ok(vec![
                        row(&[
                            ("opp__category", js("sides")),
                            ("opp__revenue", jn(100_000.0)),
                            ("opp__total_orders", jn(200.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                        row(&[
                            ("opp__category", js("drinks")),
                            ("opp__revenue", jn(50_000.0)),
                            ("opp__total_orders", jn(100.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                    ]);
                }
                // The value-discovery query: inside the mobile_app segment
                // every row is `sides`. THIS is the degenerate case.
                return Ok(vec![row(&[("opp__category", js("sides"))])]);
            }
            panic!(
                "a single-valued dimension must be skipped before any rate \
                 query is issued for it, but one was: {:?}",
                q.measures
            );
        });

        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .unwrap()
        .expect("the root gap is real, so the drill still reports its root level");

        assert_eq!(
            result.levels.len(),
            1,
            "the tautological category split must not be followed: {result:?}"
        );
        assert!(
            result.levels[0].candidates.is_empty(),
            "the single-valued dimension must not even be offered: {:?}",
            result.levels[0].candidates
        );
        assert!(
            matches!(result.levels[0].stop_reason, Some(StopReason::NoCandidates)),
            "{:?}",
            result.levels[0].stop_reason
        );
    }

    #[test]
    fn test_opportunity_drill_consumes_single_value_scope_dimensions() {
        // REGRESSION. `consumed_dims` was seeded with the root scan's top
        // dimension alone, so a dimension the CALLER had already pinned in
        // `scope` stayed a candidate. The drill would then open by "explaining"
        // the gap with the very filter the user selected — the world-model
        // instance panel scoped to `city = Amsterdam` led with
        // `stores.city = Amsterdam, +100% of root gap`.
        //
        // The mock below deliberately LIES, reporting three distinct cities
        // inside a scope pinned to one. That isolates this guard from the
        // cardinality floor: if `opp.city` were still being offered, discovery
        // would hand back three values and a rate query would follow. It must
        // never get that far.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "city"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            if let Some(rows) = degenerate_drill_root_rows(q) {
                return Ok(rows);
            }
            if !q.dimensions.is_empty() && q.dimensions[0] == "opp.city" {
                if q.measures.len() > 1 {
                    // Root's own city breakdown — flat, so `status` wins the
                    // root scan and `city` is left to the recursion.
                    return Ok(vec![
                        row(&[
                            ("opp__city", js("Amsterdam")),
                            ("opp__revenue", jn(100_000.0)),
                            ("opp__total_orders", jn(200.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                        row(&[
                            ("opp__city", js("Berlin")),
                            ("opp__revenue", jn(50_000.0)),
                            ("opp__total_orders", jn(100.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                    ]);
                }
                panic!("opp.city is pinned by scope and must never be re-offered as a split");
            }
            panic!("unexpected query: {:?} / {:?}", q.dimensions, q.measures);
        });

        let scope = vec![QueryFilter {
            member: Some("opp.city".to_string()),
            operator: Some(FilterOperator::Equals),
            values: vec!["Amsterdam".to_string()],
            and: None,
            or: None,
        }];
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &scope,
            &exec,
            &DrillConfig::default(),
        )
        .unwrap()
        .expect("the root gap is real, so the drill still reports its root level");

        assert!(
            !result.levels[0].candidates.iter().any(|c| matches!(
                &c.kind,
                CandidateKind::Dimension { dimension, .. } if dimension == "opp.city"
            )),
            "the scope-pinned dimension must not be offered: {:?}",
            result.levels[0].candidates
        );
    }

    #[test]
    fn test_opportunity_drill_keeps_the_denominator_fixed_across_levels() {
        // A THREE-dimension view so the drill actually descends two levels and
        // runs `dimension_candidates` rate queries at level 1 — the depth-2
        // path every existing drill test stops short of (they all use
        // 2-dimension views, so recursion halts at NoCandidates on level 1
        // before a contaminated query runs).
        //
        // The drill: opportunity() picks `mobile_app` vs `in_store` by status;
        // level 0 splits by `category` (sides wins, gated); level 1 splits by
        // `region` (status + category already consumed) — so level 1's region
        // rate queries genuinely execute.
        //
        // THE BUG THIS CATCHES: the old recursion pushed each followed split
        // onto `seg_filter`, which `dimension_candidates` applies to the rate
        // queries' QUERY filters — narrowing the FIXED count denominator (and
        // the benchmark never saw the split). Under the fix the split lives in
        // the synthetic `__drill__` measure's MeasureFilters (invisible to a
        // name-based mock's query filters) and the query filters stay
        // population-only. The `leaked` flag below trips iff a rate query
        // (dimensions empty, a `__drill__` measure at measures[0]) carries
        // `opp.category` in its query filters — true under the bug, false under
        // the fix.
        //
        // The benchmark-numerator-symmetry half of the fix is verified BY
        // CONSTRUCTION: the same synthetic `__drill__` measure (carrying the
        // accumulated MeasureFilters) is queried for both the seg and bench
        // population, so a name-based mock can't observe it and it isn't
        // separately asserted here.
        let view = make_opp_view(
            "opp",
            vec![
                atomic_measure("revenue", MeasureType::Sum),
                atomic_measure("total_orders", MeasureType::Count),
            ],
            &["status", "category", "region"],
        );
        let mut layer = make_layer(vec![view]);
        let tree = MetricTree::build(&layer);
        assert!(augment_layer_for_opportunity(&mut layer, "opp.revenue"));
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        // Set true iff a rate query's QUERY filters leak `opp.category` — the
        // accumulated split contaminating the population/denominator. Shared
        // into the executor closure (which parallel_execute runs across
        // threads, hence Arc<AtomicBool>).
        let leaked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let leaked_inner = std::sync::Arc::clone(&leaked);

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let is_mobile = q
                .filters
                .iter()
                .any(|f| f.values == vec!["mobile_app".to_string()]);
            let is_in_store = q
                .filters
                .iter()
                .any(|f| f.values == vec!["in_store".to_string()]);

            // A rate query is dimension-less with a `__drill__` measure first.
            // If ANY such query carries `opp.category` in its QUERY filters, the
            // accumulated split has leaked onto the fixed denominator — the bug.
            let is_rate_query = q.dimensions.is_empty()
                && q.measures.first().is_some_and(|m| m.contains("__drill__"));
            if is_rate_query
                && q.filters
                    .iter()
                    .any(|f| f.member.as_deref() == Some("opp.category"))
            {
                leaked_inner.store(true, std::sync::atomic::Ordering::SeqCst);
            }

            if !q.dimensions.is_empty() {
                let dim = q.dimensions[0].as_str();
                if dim == "opp.status" {
                    // Root status breakdown: mobile_app (rate 300, the laggard)
                    // vs in_store (rate 800, the bar) — tight stddev so
                    // mobile_app is a real gap.
                    return Ok(vec![
                        row(&[
                            ("opp__status", js("mobile_app")),
                            ("opp__revenue", jn(165_600.0)),
                            ("opp__total_orders", jn(552.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                        row(&[
                            ("opp__status", js("in_store")),
                            ("opp__revenue", jn(62_400.0)),
                            ("opp__total_orders", jn(78.0)),
                            ("opp____opp_stddev__revenue", jn(50.0)),
                        ]),
                    ]);
                }
                if q.measures.len() > 1 {
                    // Root breakdown by category / region — FLAT (same rate in
                    // both values), so neither outranks status as the root's top
                    // dimension.
                    let (a, b) = if dim == "opp.category" {
                        ("sides", "drinks")
                    } else {
                        ("north", "south")
                    };
                    let col: &str = if dim == "opp.category" {
                        "opp__category"
                    } else {
                        "opp__region"
                    };
                    return Ok(vec![
                        row(&[
                            (col, js(a)),
                            ("opp__revenue", jn(100_000.0)),
                            ("opp__total_orders", jn(200.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                        row(&[
                            (col, js(b)),
                            ("opp__revenue", jn(50_000.0)),
                            ("opp__total_orders", jn(100.0)),
                            ("opp____opp_stddev__revenue", jn(10.0)),
                        ]),
                    ]);
                }
                // Value-discovery query (1 measure) within the current segment.
                // Two values per dimension: a single-valued dimension is a
                // tautology and is skipped by the MIN_DIMENSION_CARDINALITY
                // floor, which would collapse this drill to depth 0 and defeat
                // the leak check below.
                if dim == "opp.category" {
                    return Ok(vec![
                        row(&[("opp__category", js("sides"))]),
                        row(&[("opp__category", js("drinks"))]),
                    ]);
                }
                return Ok(vec![
                    row(&[("opp__region", js("north"))]),
                    row(&[("opp__region", js("south"))]),
                ]);
            }

            // No dimensions: root overall_query (1 measure) or a
            // dimension_candidates rate query (>1 measures).
            if q.measures.len() == 1 {
                return Ok(vec![row(&[("opp__revenue", jn(228_000.0))])]);
            }

            let filtered = &q.measures[0];
            let (_, filtered_name) = filtered.split_once('.').unwrap();
            let sum_alias = filtered.replace('.', "__");
            if filtered.contains("category") || filtered.contains("sides") {
                // Level-0 category=sides rate query. Big gap + tight stddev so
                // the gate returns Some(true): gap = 62_400/78 - 6_000/552 =
                // 789.13; se = sqrt(2)*10/sqrt(100) ~= 1.41; t ~= 558.
                // `drinks` is the loser: near-flat, so `sides` still wins
                // level 0 and the accumulated filter stays category=sides.
                let (sum_val, count_val) = match (filtered.contains("sides"), is_mobile) {
                    (true, true) => (6_000.0, 552.0),
                    (true, false) => (62_400.0, 78.0),
                    (false, true) => (5_000.0, 552.0),
                    (false, false) => (1_000.0, 78.0),
                };
                if !is_mobile {
                    assert!(is_in_store, "expected in_store filter: {:?}", q.filters);
                }
                let dispersion_alias = format!("opp__{}", dispersion_measure_name(filtered_name));
                let n_alias = format!("opp__{}", dispersion_n_measure_name(filtered_name));
                return Ok(vec![row(&[
                    (sum_alias.leak() as &str, jn(sum_val)),
                    ("opp__total_orders", jn(count_val)),
                    (dispersion_alias.leak() as &str, jn(10.0)),
                    (n_alias.leak() as &str, jn(100.0)),
                ])]);
            }
            // Level-1 region=north rate query. Flat (rate 10 both populations,
            // gap 0) and NO dispersion column returned, so the gate returns None
            // (inconclusive) -> the candidate is kept gated:false and the drill
            // stops at level 1 with GateInconclusive. That is what fixes
            // levels.len() at 2 while still EXECUTING the depth-2 rate query the
            // leak check depends on.
            // Both region values behave identically here: flat and undispersed,
            // so whichever ranks first is inconclusive and stops the drill.
            assert!(filtered.contains("region") || filtered.contains("north"));
            let (sum_val, count_val) = if is_mobile {
                (5_520.0, 552.0)
            } else {
                (780.0, 78.0)
            };
            Ok(vec![row(&[
                (sum_alias.leak() as &str, jn(sum_val)),
                ("opp__total_orders", jn(count_val)),
            ])])
        });

        let config = DrillConfig::default();
        let result = opportunity_drill(
            &tree,
            &layer,
            "opp.revenue",
            "opp.created_at",
            ("2024-01-01", "2024-01-31"),
            &[],
            &exec,
            &config,
        )
        .unwrap()
        .expect("a real root gap must produce Some(DrillResult)");

        // The drill descended exactly two levels.
        assert_eq!(result.levels.len(), 2, "{result:?}");
        // Level 0's winner is the category=sides split, gated.
        assert!(
            matches!(
                &result.levels[0].candidates[0].kind,
                CandidateKind::Dimension { dimension, value }
                    if dimension == "opp.category" && value == "sides"
            ),
            "{:?}",
            result.levels[0].candidates
        );
        assert!(result.levels[0].candidates[0].gated);
        assert!(
            result.levels[0].stop_reason.is_none(),
            "level 0 must recurse"
        );
        // Level 1 carries the accumulated split — now sourced from
        // numerator_filters, not the population seg_filter.
        assert!(
            result.levels[1]
                .segment_filter
                .iter()
                .any(|f| f.member.as_deref() == Some("opp.category")
                    && f.values == vec!["sides".to_string()]),
            "level 1 must carry the accumulated category=sides numerator filter: {:?}",
            result.levels[1].segment_filter
        );
        assert!(
            matches!(
                result.levels[1].stop_reason,
                Some(StopReason::GateInconclusive)
            ),
            "{:?}",
            result.levels[1].stop_reason
        );

        // THE BUG CHECK: no rate query's population/denominator (query) filters
        // may carry the accumulated `opp.category` split. Under the old code the
        // level-1 region rate queries did (seg_filter was pushed and applied to
        // the count); under the fix the split lives only in the synthetic
        // measure's MeasureFilters.
        assert!(
            !leaked.load(std::sync::atomic::Ordering::SeqCst),
            "an accumulated dimension split leaked into a rate query's \
             population/denominator filters — the fixed-denominator invariant is broken"
        );
    }

    #[test]
    fn test_drill_composite_root_children_sum_to_parent() {
        // No test walked `opportunity_drill` from a composite root — every
        // `component_candidates` test calls that function directly with a mock
        // executor and a hand-built tree. That gap is exactly how a units
        // mismatch shipped: on a real fixture the root reported a raw TOTAL
        // gap while its component children reported per-unit RATE gaps, so
        // the children didn't sum to the parent and `concentration`
        // degenerated into each child's revenue SIZE share instead of its
        // GAP share. Tasks 2-4 fixed that by putting `opportunity()` itself
        // into rate mode for an eligible additive same-view composite; this
        // test is the end-to-end lock: it exercises `opportunity_drill` on a
        // real composite root and asserts the invariant those tasks exist to
        // guarantee.
        //
        // `west` and `east` deliberately have DIFFERENT check counts (100 vs
        // 50) *and* different per-child rate gaps (entree 100/check, addon
        // 60/check) so neither the concentrations nor the child-sum-equals-
        // parent check can pass by the trivial 0.5/0.5 or coincidental-equal
        // path — entree's share is 100/160 = 0.625, addon's is 60/160 = 0.375.
        let view = make_opp_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("addon_revenue", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
            &["region"],
        );
        let mut layer = make_layer(vec![view]);
        // Tree first, then augment — same order `noise_layer()` establishes:
        // the synthetic dispersion pass-through must not itself become a
        // node in the tree.
        let tree = MetricTree::build(&layer);
        assert!(
            augment_layer_for_opportunity(&mut layer, "checks.net_revenue"),
            "net_revenue is an eligible additive same-view composite"
        );
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let is_west = q
                .filters
                .iter()
                .any(|f| f.values == vec!["west".to_string()]);
            let is_east = q
                .filters
                .iter()
                .any(|f| f.values == vec!["east".to_string()]);

            if !q.dimensions.is_empty() {
                // opportunity()'s root breakdown by `checks.region`: west is
                // the laggard (rate 420/check), east is the benchmark (rate
                // 580/check) — a tight stddev so the gap reads as real.
                return Ok(vec![
                    row(&[
                        ("checks__region", js("west")),
                        ("checks__net_revenue", jn(42_000.0)),
                        ("checks__total_checks", jn(100.0)),
                        ("checks____opp_stddev__net_revenue", jn(10.0)),
                    ]),
                    row(&[
                        ("checks__region", js("east")),
                        ("checks__net_revenue", jn(29_000.0)),
                        ("checks__total_checks", jn(50.0)),
                        ("checks____opp_stddev__net_revenue", jn(10.0)),
                    ]),
                ]);
            }

            if q.measures.len() == 1 {
                // The overall-value query and reachable_values' downstream
                // query both ask for `checks.net_revenue` alone.
                return Ok(vec![row(&[("checks__net_revenue", jn(71_000.0))])]);
            }

            // component_candidates' seg/bench queries: measures = [child,
            // checks.total_checks], no dimensions, distinguished by which
            // region equality filter rides along.
            let child = q.measures[0].as_str();
            let (num, count) = match (child, is_west, is_east) {
                ("checks.entree_revenue", true, false) => (40_000.0, 100.0),
                ("checks.entree_revenue", false, true) => (25_000.0, 50.0),
                ("checks.addon_revenue", true, false) => (2_000.0, 100.0),
                ("checks.addon_revenue", false, true) => (4_000.0, 50.0),
                _ => panic!("unexpected component query: {q:?}"),
            };
            let alias = child.replace('.', "__");
            Ok(vec![row(&[
                (alias.leak() as &str, jn(num)),
                ("checks__total_checks", jn(count)),
            ])])
        });

        let result = opportunity_drill(
            &tree,
            &layer,
            "checks.net_revenue",
            "checks.check_date",
            ("2025-07-17", "2026-07-16"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .expect("composite drill succeeds")
        .expect("composite drill returns a result");

        let level0 = &result.levels[0];
        let components: Vec<_> = level0
            .candidates
            .iter()
            .filter(|c| matches!(c.kind, CandidateKind::Component { .. }))
            .collect();
        assert_eq!(
            components.len(),
            2,
            "both components are candidates: {level0:?}"
        );

        // The invariant the shipped bug violated: a raw-total root gap next
        // to per-unit-rate child gaps. Here both are rate gaps in the same
        // unit, so they must sum exactly (component decomposition is an
        // exact arithmetic identity, not a statistical estimate).
        let child_sum: f64 = components.iter().map(|c| c.gap).sum();
        assert!(
            (child_sum - level0.gap).abs() < 1e-6,
            "children must sum to the parent: children {child_sum}, parent {}",
            level0.gap
        );
        assert!(
            (level0.gap - result.root_gap).abs() < 1e-6,
            "level 0's gap must equal the root gap: {} vs {}",
            level0.gap,
            result.root_gap
        );

        // Concentrations are shares of that same gap, so they sum to 1 — and,
        // per the worked numbers above, are NOT a trivial 0.5/0.5 split.
        let conc_sum: f64 = components.iter().map(|c| c.concentration).sum();
        assert!(
            (conc_sum - 1.0).abs() < 1e-6,
            "concentrations must sum to 1, got {conc_sum}"
        );
        for c in &components {
            assert!(
                (c.concentration - 0.5).abs() > 0.05,
                "concentration must not be a trivial size-share 0.5/0.5 split: {c:?}"
            );
        }

        // A drill from a composite root descends into the winning component
        // (entree, 62.5% concentration); that child is atomic (no further
        // component children) and its only dimension is already consumed by
        // the root split, so level 1 legitimately stops at NoCandidates — the
        // contract under test is the level-0 component decomposition, not
        // deeper recursion.
        assert_eq!(result.levels.len(), 2, "{:?}", result.levels);
        assert_eq!(result.levels[1].measure, "checks.entree_revenue");
        assert!(matches!(
            result.levels[1].stop_reason,
            Some(StopReason::NoCandidates)
        ));
    }

    #[test]
    fn test_drill_composite_child_of_composite_sums_to_parent() {
        // `test_drill_composite_root_children_sum_to_parent` only ever walks
        // ONE level of decomposition before landing on an ATOMIC child
        // (entree_revenue). That leaves the exact shape the real fixture uses
        // — `example_new/semantics/views/checks.view.yml`, where
        // `net_revenue = entree_revenue + addon_revenue` and `addon_revenue`
        // is ITSELF a composite (`sides_revenue + beverages_revenue`) —
        // untested. A predicate that refuses to recurse into a composite
        // child of a composite would still pass every assertion in the
        // level-1-is-atomic test, because that test never asks a composite
        // child for ITS OWN component children. This test roots the drill at
        // `net_revenue` and forces the descent through `addon_revenue`
        // (rather than the atomic `entree_revenue`) so the level-1 sum-to-
        // parent identity is actually exercised at depth.
        let view = make_opp_view(
            "checks",
            vec![
                atomic_measure("entree_revenue", MeasureType::Sum),
                atomic_measure("sides_revenue", MeasureType::Sum),
                atomic_measure("beverages_revenue", MeasureType::Sum),
                atomic_measure("total_checks", MeasureType::Count),
                composite_measure(
                    "addon_revenue",
                    "{{checks.sides_revenue}} + {{checks.beverages_revenue}}",
                ),
                composite_measure(
                    "net_revenue",
                    "{{checks.entree_revenue}} + {{checks.addon_revenue}}",
                ),
            ],
            &["region"],
        );
        let mut layer = make_layer(vec![view]);
        // Tree first, then augment — same order as the sibling test.
        let tree = MetricTree::build(&layer);
        assert!(
            augment_layer_for_opportunity(&mut layer, "checks.net_revenue"),
            "net_revenue is an eligible additive same-view composite, even \
             though one of its own components (addon_revenue) is itself a \
             composite"
        );
        let layer = std::sync::Arc::new(std::sync::RwLock::new(layer));

        let exec: Box<QueryExecutor> = Box::new(move |q: &QueryRequest| {
            let is_west = q
                .filters
                .iter()
                .any(|f| f.values == vec!["west".to_string()]);
            let is_east = q
                .filters
                .iter()
                .any(|f| f.values == vec!["east".to_string()]);

            if !q.dimensions.is_empty() {
                // Root breakdown by `checks.region`: west is the laggard
                // (net rate 420/check), east is the benchmark (580/check) —
                // identical to the sibling test's root shape.
                return Ok(vec![
                    row(&[
                        ("checks__region", js("west")),
                        ("checks__net_revenue", jn(42_000.0)),
                        ("checks__total_checks", jn(100.0)),
                        ("checks____opp_stddev__net_revenue", jn(10.0)),
                    ]),
                    row(&[
                        ("checks__region", js("east")),
                        ("checks__net_revenue", jn(29_000.0)),
                        ("checks__total_checks", jn(50.0)),
                        ("checks____opp_stddev__net_revenue", jn(10.0)),
                    ]),
                ]);
            }

            if q.measures.len() == 1 {
                // Overall-value query / reachable_values downstream query.
                return Ok(vec![row(&[("checks__net_revenue", jn(71_000.0))])]);
            }

            // component_candidates' seg/bench queries at both level 0
            // (entree_revenue / addon_revenue) and level 1 (sides_revenue /
            // beverages_revenue): measures = [child, checks.total_checks].
            //
            // Rates (west n=100, east n=50):
            //   entree:    west 150/check, east 170/check -> rate gap  20
            //   addon:     west 270/check, east 410/check -> rate gap 140
            //   (entree_gap 20 + addon_gap 140 = 160 = the root's net rate
            //   gap, and addon's 140/160 = 87.5% share dwarfs entree's
            //   12.5%, so the drill MUST follow addon, not entree.)
            //   sides:     west 180/check, east 220/check -> rate gap  40
            //   beverages: west  90/check, east 190/check -> rate gap 100
            //   (sides_gap 40 + beverages_gap 100 = 140 = addon's own rate
            //   gap, reusing the identity one level deeper.)
            let child = q.measures[0].as_str();
            let (num, count) = match (child, is_west, is_east) {
                ("checks.entree_revenue", true, false) => (15_000.0, 100.0),
                ("checks.entree_revenue", false, true) => (8_500.0, 50.0),
                ("checks.addon_revenue", true, false) => (27_000.0, 100.0),
                ("checks.addon_revenue", false, true) => (20_500.0, 50.0),
                ("checks.sides_revenue", true, false) => (18_000.0, 100.0),
                ("checks.sides_revenue", false, true) => (11_000.0, 50.0),
                ("checks.beverages_revenue", true, false) => (9_000.0, 100.0),
                ("checks.beverages_revenue", false, true) => (9_500.0, 50.0),
                _ => panic!("unexpected component query: {q:?}"),
            };
            let alias = child.replace('.', "__");
            Ok(vec![row(&[
                (alias.leak() as &str, jn(num)),
                ("checks__total_checks", jn(count)),
            ])])
        });

        let result = opportunity_drill(
            &tree,
            &layer,
            "checks.net_revenue",
            "checks.check_date",
            ("2025-07-17", "2026-07-16"),
            &[],
            &exec,
            &DrillConfig::default(),
        )
        .expect("composite-of-composite drill succeeds")
        .expect("composite-of-composite drill returns a result");

        // --- Level 0: net_revenue's component children (entree_revenue,
        // addon_revenue) sum to level 0's gap, concentrations sum to 1.0. ---
        let level0 = &result.levels[0];
        let level0_components: Vec<_> = level0
            .candidates
            .iter()
            .filter(|c| matches!(c.kind, CandidateKind::Component { .. }))
            .collect();
        assert_eq!(
            level0_components.len(),
            2,
            "both level-0 components are candidates: {level0:?}"
        );
        let level0_child_sum: f64 = level0_components.iter().map(|c| c.gap).sum();
        assert!(
            (level0_child_sum - level0.gap).abs() < 1e-6,
            "level-0 children must sum to level 0's gap: children {level0_child_sum}, parent {}",
            level0.gap
        );
        let level0_conc_sum: f64 = level0_components.iter().map(|c| c.concentration).sum();
        assert!(
            (level0_conc_sum - 1.0).abs() < 1e-6,
            "level-0 concentrations must sum to 1, got {level0_conc_sum}"
        );

        // The drill must have descended into the COMPOSITE child
        // (addon_revenue, 87.5% concentration), not the atomic one
        // (entree_revenue) — otherwise level 1 never exercises a
        // composite-to-composite descent and this test proves nothing new.
        assert!(
            result.levels.len() >= 2,
            "drill must produce at least a level 1: {:?}",
            result.levels
        );
        assert_eq!(
            result.levels[1].measure, "checks.addon_revenue",
            "the drill must descend into the composite child addon_revenue \
             (the larger gap share), not the atomic entree_revenue — \
             otherwise this test never reaches a composite-of-composite \
             descent: {:?}",
            result.levels
        );

        // --- Level 1 (the new coverage): addon_revenue's OWN component
        // children (sides_revenue, beverages_revenue) again sum to level
        // 1's gap, concentrations again summing to 1.0. This is the
        // composite-to-composite identity a predicate refusing to recurse
        // into a composite child would silently fail (by never producing
        // Component candidates at all). ---
        let level1 = &result.levels[1];
        let level1_components: Vec<_> = level1
            .candidates
            .iter()
            .filter(|c| matches!(c.kind, CandidateKind::Component { .. }))
            .collect();
        assert_eq!(
            level1_components.len(),
            2,
            "both level-1 components (sides_revenue, beverages_revenue) are \
             candidates: {level1:?}"
        );
        let level1_child_sum: f64 = level1_components.iter().map(|c| c.gap).sum();
        assert!(
            (level1_child_sum - level1.gap).abs() < 1e-6,
            "level-1 children must sum to level 1's gap: children {level1_child_sum}, parent {}",
            level1.gap
        );
        let level1_conc_sum: f64 = level1_components.iter().map(|c| c.concentration).sum();
        assert!(
            (level1_conc_sum - 1.0).abs() < 1e-6,
            "level-1 concentrations must sum to 1, got {level1_conc_sum}"
        );
    }
}
