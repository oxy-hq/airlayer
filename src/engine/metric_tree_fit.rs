//! Runtime fitting of driver coefficients from historical data.
//!
//! A driver edge declared without a `coefficient:` is a claim of *direction*
//! without a claim of *magnitude*, and `predict` correctly propagates nothing
//! across it. This module is the other half of that contract: given a query
//! executor and a window, measure the magnitude from history so the scenario
//! can forecast across the edge anyway — and *refuse* when the data does not
//! support one, so a coefficient the warehouse cannot substantiate never
//! silently becomes a forecast.
//!
//! A declared coefficient always wins. It may come from an experiment or a
//! causal model — knowledge the warehouse does not carry — so fitting only
//! considers driver edges whose `coefficient` is `None`.
//!
//! ## Why within-panel, not pooled
//!
//! The fit demeans every observation against its own panel (entity) before
//! pooling. This is not a stylistic choice: an un-demeaned slope absorbs the
//! level differences between panels. A larger store both sells more and spends
//! more on everything, so the between-panel contrast is really the budget
//! ratio — how big a store is, not what a marginal dollar does — and a pooled
//! regression mixes that into the answer. How badly depends on how much of the
//! driver's variance is within-panel rather than between; on airlayer's own
//! restaurant fixture the between-panel slope is 11.79 and the pooled slope
//! 8.09 against a true within-panel 5.78, so pooling overstates the lever by
//! 40% there and the pure between contrast by 2x. Demeaning removes exactly
//! that component.
//!
//! ## Which curve, and why no new estimator
//!
//! The edge's declared `form:` chooses the transformation the slope is measured
//! in — [`DriverForm::LogLog`] regresses `ln y` on `ln x` and the coefficient is
//! an elasticity, and so on for the other two. Every declared form is linear
//! *in its parameters*, so all four are this same within-panel OLS on
//! transformed columns: no new model class, no optimizer, and — decisively — the
//! slope keeps a standard error, which is the only reason the refusal gate below
//! can exist. A learner that predicts the target well but cannot say whether its
//! own derivative is distinguishable from zero has nothing to refuse with.
//!
//! The form is a *declaration*, never inferred. Fitting several forms and
//! keeping whichever fits best is model selection, and it would let the engine
//! pick the shape of a causal claim from observational data — the same thing
//! the refusal gate and the un-fitted `lag` exist to prevent. A human states
//! the shape; this module only measures its magnitude.
//!
//! Note what that does *not* buy: `t` says the slope is not zero, never that
//! the form is right. A saturating relationship declared `linear` will fit with
//! a large `t` and overstate a big lever. Nothing here computes residual
//! curvature, so a misdeclared form is invisible to the gate.
//!
//! ## The refusal gate
//!
//! An observational slope is already a generous stand-in for an
//! interventional coefficient; an *insignificant* observational slope is
//! noise wearing a number. Below [`MIN_FIT_T`] (or [`MIN_FIT_OBSERVATIONS`])
//! the fit returns a [`FittedDriver`] with `coefficient: None` and a
//! `refusal` naming why, and the edge stays qualitative — the same posture as
//! the opportunity drill's significance gate: an empty answer over a
//! confident wrong one.

use super::metric_tree::{EdgeKind, MetricEdge, MetricTree};
use super::metric_tree_ops::QueryExecutor;
use super::query::{FilterOperator, QueryFilter, QueryRequest};
use super::response::{
    candidate_aic, BasisMoments, CandidateScore, FormSource, Link, ResponseSpec,
    INFERENCE_CANDIDATES, MIN_AIC_IMPROVEMENT,
};
use super::EngineError;
use crate::schema::models::{DriverForm, EntityType, SemanticLayer};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// Minimum |t| for a fitted slope to become a coefficient. 2.0 is the usual
/// ~95% two-sided bar; below it the slope is indistinguishable from zero and
/// forecasting with it would manufacture a confident number out of noise.
pub const MIN_FIT_T: f64 = 2.0;

/// Minimum paired observations. Below this the t-statistic itself is not
/// trustworthy, so the gate refuses before consulting it.
pub const MIN_FIT_OBSERVATIONS: usize = 30;

/// The outcome of fitting one driver edge — either a usable coefficient or a
/// named refusal, never a silent absence. Serialized into API responses, so
/// field names are a wire contract.
///
/// `Deserialize` because the fit is produced by the baseline call and consumed
/// by a later, separate `predict` call: a UI re-runs propagation on every
/// keystroke and must not re-query the warehouse each time, so it echoes what
/// the baseline handed it. Every field but `from`/`to` defaults, so a client
/// that only round-trips the identity and the coefficient still applies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FittedDriver {
    /// Source (driver) measure id.
    pub from: String,
    /// Target (driven) measure id.
    pub to: String,
    /// The lag (days) the pairs were built at — the edge's declared lag, or 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    /// The functional form the slope was measured in — the edge's declared
    /// `form:`, carried so a number can never be applied under a different
    /// rule than it was fitted under. An elasticity read as a level slope is
    /// wrong by a factor of `target / driver`, silently.
    ///
    /// Defaults to `Linear` on deserialize, so a `FittedDriver` serialized
    /// before this field existed still round-trips — and reads as exactly what
    /// it was, since a fit produced then could only have been linear.
    #[serde(default)]
    pub form: DriverForm,
    /// Paired observations the fit used.
    #[serde(default)]
    pub n: usize,
    /// Panels (entities) those observations spanned.
    #[serde(default)]
    pub n_panels: usize,
    /// Pairs dropped because a logged axis had a non-positive value. Reported
    /// rather than silently narrowing the window: it moves `n`, and `n` is what
    /// [`MIN_FIT_OBSERVATIONS`] gates on, so a refusal can otherwise be caused
    /// by data nothing on the surface mentions. Always 0 for `Linear`.
    #[serde(default)]
    pub n_nonpositive: usize,
    /// The headline within-panel slope — the FIRST basis coefficient — present
    /// only when the gate passed. Kept because it is what every existing consumer
    /// reads, and it is the whole answer for the four single-term forms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coefficient: Option<f64>,
    /// One coefficient per basis term, in basis order. This is what propagation
    /// evaluates. A `quadratic` is `[slope, curvature]`; the single-term forms
    /// carry one element and `coefficient` mirrors it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coefficients: Vec<f64>,
    /// Standard error of the headline slope.
    #[serde(default)]
    pub se: f64,
    /// Standard error per basis term.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub se_terms: Vec<f64>,
    /// Headline `slope / se`. On a refusal this is the term that FAILED, so the
    /// number beside the reason is the one that caused it.
    #[serde(default)]
    pub t_stat: f64,
    /// `t` per basis term. Every one of them has to clear [`MIN_FIT_T`], so this
    /// is what a reader checks to see whether a curvature was real.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub t_stats: Vec<f64>,
    /// Whether `form` was declared in the YAML or measured from history.
    #[serde(default)]
    pub form_source: FormSource,
    /// Every candidate shape considered, with its comparable score. Empty when the
    /// form was declared (nothing was searched).
    ///
    /// Reported so an inferred shape is auditable and arguable: a modeller can see
    /// that a curve beat a line by 40 AIC rather than being told a form and having
    /// to trust it. This is what makes `form:` an override rather than a mystery.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<CandidateScore>,
    /// Basis moments over the rows this fit used — the sufficient statistics that
    /// let a curved response be applied to an aggregate lever exactly.
    ///
    /// This is the piece that makes a turning point possible. Without it the
    /// forecast has only the window total, and a per-row curvature applied to a
    /// total is 42,905x out on this project's own fixture, with the sign flipped.
    /// Carried on the wire because `predict` must not re-query.
    #[serde(default)]
    pub moments: crate::engine::response::BasisMoments,
    /// `(min, max)` of the driver values observed. The backstop against a lever
    /// so large that every row lands outside anything the fit ever saw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<(f64, f64)>,
    /// Why no coefficient was produced. `None` exactly when `coefficients` is
    /// non-empty — the two are one enum flattened for serialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

/// Panel dimensions for fitting: each `from`-view's foreign-entity keys.
///
/// The foreign entities of a view are its natural panel identifiers — for a
/// store-day view keyed by (location, date), the location key is what "its
/// own panel" means. Views with no foreign entity contribute nothing and the
/// fit degrades to a single pooled panel, which is correct for a view that
/// genuinely has one time series.
pub fn fit_panel_dimensions(layer: &SemanticLayer, edges: &[&MetricEdge]) -> Vec<String> {
    let from_views: HashSet<&str> = edges
        .iter()
        .filter_map(|e| e.from.split('.').next())
        .collect();
    let mut dims: Vec<String> = Vec::new();
    for view in &layer.views {
        if !from_views.contains(view.name.as_str()) {
            continue;
        }
        for entity in &view.entities {
            if entity.entity_type != EntityType::Foreign {
                continue;
            }
            for key in entity.key.iter().chain(entity.keys.iter().flatten()) {
                let dim = format!("{}.{}", view.name, key);
                if !dims.contains(&dim) {
                    dims.push(dim);
                }
            }
        }
    }
    dims
}

/// Driver edges a fit could size: `kind: driver`, no declared coefficient,
/// and reachable forward from `roots` (a lever's delta would actually cross
/// them). Declared coefficients are never refitted — see the module docs.
pub fn fittable_edges<'t>(tree: &'t MetricTree, roots: &[String]) -> Vec<&'t MetricEdge> {
    let mut fwd: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &tree.edges {
        fwd.entry(e.from.as_str()).or_default().push(e.to.as_str());
    }
    let mut reachable: HashSet<&str> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    for root in roots {
        if reachable.insert(root.as_str()) {
            queue.push_back(root.as_str());
        }
    }
    while let Some(node) = queue.pop_front() {
        for &next in fwd.get(node).map(Vec::as_slice).unwrap_or(&[]) {
            if reachable.insert(next) {
                queue.push_back(next);
            }
        }
    }
    tree.edges
        .iter()
        .filter(|e| {
            e.kind == EdgeKind::Driver
                && e.coefficient.is_none()
                && reachable.contains(e.from.as_str())
        })
        .collect()
}

/// Write fitted coefficients onto the tree's edges, so `predict` propagates
/// across them exactly as it would a declared coefficient. Refused fits are
/// skipped — their edges stay qualitative.
pub fn apply_fitted_coefficients(tree: &mut MetricTree, fits: &[FittedDriver]) {
    for fit in fits {
        // `coefficients` is the load-bearing field; the scalar is its mirror. A
        // refusal has neither, so it can never arrive as a forecast of zero.
        let coefficients = if fit.coefficients.is_empty() {
            match fit.coefficient {
                // An older client round-trips only the scalar. That is a
                // single-term shape by construction, so read it as one.
                Some(c) => vec![c],
                None => continue,
            }
        } else {
            fit.coefficients.clone()
        };
        for edge in &mut tree.edges {
            if edge.kind == EdgeKind::Driver
                && edge.coefficients.is_empty()
                && edge.from == fit.from
                && edge.to == fit.to
            {
                // A slope measured in one space must not be applied in another.
                // The two normally agree by construction — the fit reads the
                // form off this same edge — but a baseline and the `predict`
                // calls that echo it are separate requests, and the YAML can be
                // edited (or the branch switched) in between. An elasticity
                // applied as a level slope is wrong by a factor of
                // `target / driver` with nothing to show it happened, so the
                // stale fit is dropped and the edge stays qualitative.
                if edge.form_declared {
                    // A slope measured in one space must not be applied in another.
                    // The two normally agree — the fit read the form off this same
                    // edge — but a baseline and the predicts that echo it are
                    // separate requests over an editable workspace.
                    if edge.form != fit.form {
                        continue;
                    }
                } else {
                    // The edge declared no shape, so the fit CHOSE one. Adopt it:
                    // the coefficients are meaningless under any other, and this is
                    // what makes `form:` an override rather than a prerequisite.
                    edge.form = fit.form.clone();
                }
                // Same argument one level down: a vector of the wrong width for
                // this edge's basis is not a shape we can evaluate, and padding
                // or truncating it would apply a curve nobody declared.
                if coefficients.len() != edge.form.spec().width() {
                    continue;
                }
                edge.coefficient = coefficients.first().copied();
                edge.coefficients = coefficients.clone();
                // The moments and the observed range travel WITH the
                // coefficients, because a curved response is meaningless without
                // them: they are what makes applying it to an aggregate lever
                // exact rather than 42,905x out.
                edge.moments = Some(fit.moments);
                edge.domain = fit.domain;
            }
        }
    }
}

/// Fit every fittable driver edge from one batched panel query.
///
/// A single query at (panel, day) grain covers every candidate edge — the
/// same one-round-trip trade-off `reachable_values` makes, with the same
/// consequence: if the batched query fails, every candidate is refused (with
/// the error as the reason) rather than one. The failure is visible in the
/// result, never a wrong number.
///
/// Returns `Ok(vec![])` when there is nothing to fit, which is the common
/// case and deliberately free.
pub fn fit_driver_coefficients(
    tree: &MetricTree,
    roots: &[String],
    panel_dimensions: &[String],
    time_dimension: &str,
    period: (&str, &str),
    scope: &[QueryFilter],
    executor: &QueryExecutor,
) -> Result<Vec<FittedDriver>, EngineError> {
    let candidates = fittable_edges(tree, roots);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut measures: Vec<String> = Vec::new();
    for e in &candidates {
        for m in [&e.from, &e.to] {
            if !measures.contains(m) {
                measures.push(m.clone());
            }
        }
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

    let mut dimensions: Vec<String> = panel_dimensions.to_vec();
    dimensions.push(time_dimension.to_string());
    let query = QueryRequest {
        measures,
        dimensions,
        filters,
        // The fit consumes every panel-day in the window; the default 10k cap
        // would silently truncate the panel and bias the slope toward
        // whatever rows happened to come back first.
        limit: Some(super::UNBOUNDED_QUERY_LIMIT),
        ..QueryRequest::new()
    };

    let rows = match executor(&query) {
        Ok(rows) => rows,
        Err(e) => {
            let reason = format!("panel query failed: {e}");
            return Ok(candidates
                .iter()
                .map(|edge| {
                    refused(
                        &FitContext {
                            edge,
                            n: 0,
                            n_panels: 0,
                            n_nonpositive: 0,
                            moments: BasisMoments::default(),
                            domain: None,
                        },
                        &reason,
                    )
                })
                .collect());
        }
    };

    let panel = PanelData::from_rows(&rows, panel_dimensions, time_dimension);
    let mut fits: Vec<FittedDriver> = candidates
        .iter()
        .map(|edge| fit_one_edge(edge, &panel))
        .collect();
    fits.sort_by(|a, b| (&a.to, &a.from).cmp(&(&b.to, &b.from)));
    Ok(fits)
}

/// Rows reshaped for pairing: per panel, day-ordinal → row values.
struct PanelData {
    /// panel key → (day ordinal → row index).
    panels: HashMap<String, HashMap<i64, usize>>,
    /// Measure alias → value, per row.
    rows: Vec<HashMap<String, f64>>,
}

impl PanelData {
    fn from_rows(
        rows: &[serde_json::Map<String, serde_json::Value>],
        panel_dimensions: &[String],
        time_dimension: &str,
    ) -> Self {
        let time_alias = time_dimension.replace('.', "__");
        let panel_aliases: Vec<String> = panel_dimensions
            .iter()
            .map(|d| d.replace('.', "__"))
            .collect();

        let mut panels: HashMap<String, HashMap<i64, usize>> = HashMap::new();
        let mut kept: Vec<HashMap<String, f64>> = Vec::new();
        for row in rows {
            let Some(day) = row.get(&time_alias).and_then(json_to_day_ordinal) else {
                continue;
            };
            let key = panel_aliases
                .iter()
                .map(|a| json_to_key(row.get(a)))
                .collect::<Vec<_>>()
                .join("\u{1f}");
            let values: HashMap<String, f64> = row
                .iter()
                .filter_map(|(k, v)| json_to_f64_opt(v).map(|f| (k.clone(), f)))
                .collect();
            let idx = kept.len();
            kept.push(values);
            panels.entry(key).or_default().insert(day, idx);
        }
        Self { panels, rows: kept }
    }
}

/// One row's design entries and response, in the space the edge's form is linear
/// in, or `None` where any transform is undefined there.
///
/// A log needs a strictly positive input. A row with a non-positive value on a
/// logged axis carries no information the transform can represent — a zero
/// marketing-spend day says nothing about an elasticity — so it is dropped, and
/// the caller counts it. Substituting a small epsilon would invent an enormous
/// negative log and let one closed day dominate the slope.
///
/// Note what is NOT here: a `match` on the form. The basis decides the columns,
/// so a shape added to the response table needs no change in this file.
fn design_row(spec: &ResponseSpec, x: f64, y: f64) -> Option<(Vec<f64>, f64)> {
    let mut row = Vec::with_capacity(spec.width());
    for term in spec.basis {
        row.push(term.apply(x)?);
    }
    let response = match spec.link {
        Link::Identity => y,
        Link::Log => (y > 0.0).then(|| y.ln())?,
    };
    Some((row, response))
}

/// Solve `A b = c` for a small symmetric system by Gaussian elimination with
/// partial pivoting. `k` is the basis width — 1 or 2 today, never more than a
/// handful — so this is cheaper and more auditable than a linear-algebra
/// dependency, and it returns `None` on a singular system rather than a NaN.
fn solve(mut a: Vec<Vec<f64>>, mut c: Vec<f64>) -> Option<Vec<f64>> {
    let k = c.len();
    for col in 0..k {
        let (pivot, _) = (col..k)
            .map(|r| (r, a[r][col].abs()))
            .max_by(|x, y| x.1.partial_cmp(&y.1).unwrap_or(std::cmp::Ordering::Equal))?;
        if a[pivot][col].abs() < 1e-12 {
            return None; // collinear basis: the terms cannot be told apart
        }
        a.swap(col, pivot);
        c.swap(col, pivot);
        for r in (col + 1)..k {
            let f = a[r][col] / a[col][col];
            for x in col..k {
                a[r][x] -= f * a[col][x];
            }
            c[r] -= f * c[col];
        }
    }
    let mut out = vec![0.0; k];
    for r in (0..k).rev() {
        let mut v = c[r];
        for x in (r + 1)..k {
            v -= a[r][x] * out[x];
        }
        out[r] = v / a[r][r];
    }
    Some(out)
}

/// Invert a small symmetric matrix, for the standard errors: `se_j` is
/// `sqrt(sigma^2 * (X'X)^-1_jj)`. Same size regime as [`solve`].
fn invert(a: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
    let k = a.len();
    let mut cols = Vec::with_capacity(k);
    for j in 0..k {
        let mut e = vec![0.0; k];
        e[j] = 1.0;
        cols.push(solve(a.to_vec(), e)?);
    }
    // `cols[j]` is column j of the inverse; transpose into row-major.
    Some(
        (0..k)
            .map(|r| (0..k).map(|j| cols[j][r]).collect())
            .collect(),
    )
}

/// The raw `(x, y)` pairs an edge's lag produces, grouped by panel.
///
/// Extracted from the transform step so the same pairs can be fitted under
/// several candidate shapes without re-walking the panel. `restrict_positive`
/// keeps only rows every candidate can use: inference compares shapes by
/// likelihood, and a likelihood computed over a different row set per candidate
/// is not a comparison. Declaring a `form:` skips the restriction, which is one
/// of the concrete things declaring buys.
fn raw_pairs(
    edge: &MetricEdge,
    panel: &PanelData,
    restrict_positive: bool,
) -> (Vec<Vec<(f64, f64)>>, usize) {
    let lag = edge.lag.unwrap_or(0) as i64;
    let x_alias = edge.from.replace('.', "__");
    let y_alias = edge.to.replace('.', "__");
    let mut groups: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut dropped = 0usize;
    for days in panel.panels.values() {
        let mut pts: Vec<(f64, f64)> = Vec::new();
        for (&day, &row_idx) in days {
            let Some(&y_idx) = days.get(&(day + lag)) else {
                continue;
            };
            let (Some(&x), Some(&y)) = (
                panel.rows[row_idx].get(&x_alias),
                panel.rows[y_idx].get(&y_alias),
            ) else {
                continue;
            };
            if restrict_positive && (x <= 0.0 || y <= 0.0) {
                dropped += 1;
                continue;
            }
            pts.push((x, y));
        }
        if pts.len() >= 2 {
            groups.push(pts);
        }
    }
    (groups, dropped)
}

/// One candidate shape fitted to a set of pairs. `None` when the system is
/// singular or there are too few rows for the basis.
struct BasisFit {
    beta: Vec<f64>,
    se: Vec<f64>,
    t: Vec<f64>,
    rss: f64,
    n: usize,
    n_panels: usize,
    n_nonpositive: usize,
    moments: BasisMoments,
    domain: Option<(f64, f64)>,
    /// `SUM ln y` over the rows used — the Jacobian a log link needs to make its
    /// likelihood comparable with an identity link's.
    sum_ln_y: f64,
}

impl BasisFit {
    fn all_significant(&self) -> bool {
        self.t.iter().all(|t| t.abs() >= MIN_FIT_T)
    }

    fn aic(&self, link: Link) -> f64 {
        candidate_aic(link, self.n, self.beta.len(), self.rss, self.sum_ln_y)
    }
}

fn fit_one_edge(edge: &MetricEdge, panel: &PanelData) -> FittedDriver {
    if edge.form_declared {
        return fit_declared(edge, panel);
    }
    fit_inferred(edge, panel)
}

/// Fit one candidate shape to a set of raw pairs.
///
/// Everything shape-specific is in `spec`; this function only ever sees columns.
/// `None` when the basis is collinear over these rows or there is no degree of
/// freedom left after charging one per panel mean and one per term.
fn fit_basis(spec: &ResponseSpec, groups: &[Vec<(f64, f64)>]) -> Option<BasisFit> {
    let k = spec.width();
    let mut design: Vec<Vec<(Vec<f64>, f64)>> = Vec::new();
    let mut raw_x: Vec<f64> = Vec::new();
    let mut sum_ln_y = 0.0;
    let mut n_nonpositive = 0usize;
    for pts in groups {
        let mut rows: Vec<(Vec<f64>, f64)> = Vec::new();
        let mut xs_here: Vec<f64> = Vec::new();
        let mut lny_here = 0.0;
        for &(x, y) in pts {
            match design_row(spec, x, y) {
                Some(pair) => {
                    rows.push(pair);
                    xs_here.push(x);
                    if y > 0.0 {
                        lny_here += y.ln();
                    }
                }
                None => n_nonpositive += 1,
            }
        }
        // A panel needs more rows than terms, or it contributes only its own mean.
        if rows.len() > k {
            raw_x.extend(xs_here);
            sum_ln_y += lny_here;
            design.push(rows);
        }
    }

    let n: usize = design.iter().map(Vec::len).sum();
    let n_panels = design.len();
    if n == 0 {
        return None;
    }

    // Within-panel OLS: demean every column and the response against the panel's
    // own mean, then pool. The panel intercepts drop out, which is why the
    // response has no intercept to report and why propagation only ever needs a
    // DIFFERENCE.
    let mut xs: Vec<Vec<f64>> = Vec::with_capacity(n);
    let mut ys: Vec<f64> = Vec::with_capacity(n);
    for pts in &design {
        let inv = 1.0 / pts.len() as f64;
        let means: Vec<f64> = (0..k)
            .map(|j| pts.iter().map(|(row, _)| row[j]).sum::<f64>() * inv)
            .collect();
        let my = pts.iter().map(|(_, y)| y).sum::<f64>() * inv;
        for (row, y) in pts {
            xs.push((0..k).map(|j| row[j] - means[j]).collect());
            ys.push(y - my);
        }
    }

    let mut xtx = vec![vec![0.0; k]; k];
    let mut xty = vec![0.0; k];
    for (row, y) in xs.iter().zip(&ys) {
        for a in 0..k {
            xty[a] += row[a] * y;
            for b in a..k {
                xtx[a][b] += row[a] * row[b];
            }
        }
    }
    for a in 0..k {
        for b in 0..a {
            xtx[a][b] = xtx[b][a];
        }
    }

    let beta = solve(xtx.clone(), xty)?;
    let dof = n.checked_sub(n_panels + k).filter(|d| *d > 0)?;
    let rss: f64 = xs
        .iter()
        .zip(&ys)
        .map(|(row, y)| {
            let fitted: f64 = row.iter().zip(&beta).map(|(x, b)| x * b).sum();
            (y - fitted) * (y - fitted)
        })
        .sum();
    let sigma2 = rss / dof as f64;
    let inv = invert(&xtx)?;
    let se: Vec<f64> = (0..k).map(|j| (sigma2 * inv[j][j]).abs().sqrt()).collect();
    let t: Vec<f64> = beta
        .iter()
        .zip(&se)
        .map(|(b, s)| {
            if *s > 0.0 {
                b / s
            } else if b.abs() < f64::EPSILON {
                // 0/0. A coefficient of zero with no residual variance explains
                // NOTHING perfectly; reading it as infinitely significant is how a
                // curvature of zero would sail through the gate and arrive with a
                // turning point attached.
                0.0
            } else {
                f64::INFINITY
            }
        })
        .collect();

    Some(BasisFit {
        beta,
        se,
        t,
        rss,
        n,
        n_panels,
        n_nonpositive,
        moments: BasisMoments::from_values(&raw_x),
        domain: (!raw_x.is_empty()).then(|| {
            (
                raw_x.iter().copied().fold(f64::INFINITY, f64::min),
                raw_x.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            )
        }),
        sum_ln_y,
    })
}

/// Assemble a `FittedDriver` from a successful or gated candidate fit.
fn finish(
    edge: &MetricEdge,
    form: DriverForm,
    source: FormSource,
    fit: &BasisFit,
    candidates: Vec<CandidateScore>,
) -> FittedDriver {
    let ctx = FitContext {
        edge,
        n: fit.n,
        n_panels: fit.n_panels,
        n_nonpositive: fit.n_nonpositive,
        moments: fit.moments,
        domain: fit.domain,
    };
    // EVERY basis coefficient must clear the bar. At k = 1 this is bit-identical
    // to the single-slope gate it replaces. At k = 2 it is what stops a turning
    // point being invented from noise: a curvature indistinguishable from zero is
    // a claim the data does not support.
    if let Some(j) = fit.t.iter().position(|t| t.abs() < MIN_FIT_T) {
        return FittedDriver {
            form,
            form_source: source,
            candidates,
            se: fit.se[j],
            se_terms: fit.se.clone(),
            t_stat: fit.t[j],
            t_stats: fit.t.clone(),
            refusal: Some(format!(
                "no reliable relationship in this window (term {} of {}: t = {:.2}, \
                 need |t| >= {MIN_FIT_T})",
                j + 1,
                fit.beta.len(),
                fit.t[j]
            )),
            ..base(&ctx)
        };
    }
    FittedDriver {
        form,
        form_source: source,
        candidates,
        coefficient: fit.beta.first().copied(),
        coefficients: fit.beta.clone(),
        se: fit.se[0],
        se_terms: fit.se.clone(),
        t_stat: fit.t[0],
        t_stats: fit.t.clone(),
        refusal: None,
        ..base(&ctx)
    }
}

/// The shape was declared, so measure that one and gate it. No search, and no
/// row restriction — every row this basis can use is used.
fn fit_declared(edge: &MetricEdge, panel: &PanelData) -> FittedDriver {
    let spec = edge.form.spec();
    let (groups, _) = raw_pairs(edge, panel, false);
    let n_seen: usize = groups.iter().map(Vec::len).sum();

    let Some(fit) = fit_basis(&spec, &groups) else {
        return refused(
            &FitContext {
                edge,
                n: n_seen,
                n_panels: groups.len(),
                n_nonpositive: 0,
                moments: BasisMoments::default(),
                domain: None,
            },
            "the driver does not vary within any panel, or its basis terms are collinear",
        );
    };
    if fit.n < MIN_FIT_OBSERVATIONS {
        let dropped = if fit.n_nonpositive > 0 {
            format!(
                ", after dropping {} pair(s) with a non-positive value on a log axis \
                 ({} form)",
                fit.n_nonpositive, edge.form
            )
        } else {
            String::new()
        };
        return refused(
            &FitContext {
                edge,
                n: fit.n,
                n_panels: fit.n_panels,
                n_nonpositive: fit.n_nonpositive,
                moments: fit.moments,
                domain: fit.domain,
            },
            &format!(
                "only {} paired observations in the window, need {MIN_FIT_OBSERVATIONS}{dropped}",
                fit.n
            ),
        );
    }
    finish(
        edge,
        edge.form.clone(),
        FormSource::Declared,
        &fit,
        Vec::new(),
    )
}

/// No `form:` was declared, so measure the shape as well as the magnitude.
///
/// Every candidate is fitted to the SAME rows — those where all of them are
/// defined — because a likelihood computed over a different row set per candidate
/// is not a comparison. Scores are AIC in y-space, which is what makes a model of
/// `ln y` comparable with a model of `y` (see [`candidate_aic`]).
///
/// `linear` is the null. A more elaborate shape is adopted only if it beats the
/// null by [`MIN_AIC_IMPROVEMENT`] *and* every one of its terms is significant;
/// otherwise the straight line wins. Preferring a curve that merely ties with a
/// line is how observational data talks you into a shape it cannot support.
fn fit_inferred(edge: &MetricEdge, panel: &PanelData) -> FittedDriver {
    let (groups, restricted) = raw_pairs(edge, panel, true);
    let n_seen: usize = groups.iter().map(Vec::len).sum();
    let ctx_empty = FitContext {
        edge,
        n: n_seen,
        n_panels: groups.len(),
        n_nonpositive: restricted,
        moments: BasisMoments::default(),
        domain: None,
    };

    if n_seen < MIN_FIT_OBSERVATIONS {
        let dropped = if restricted > 0 {
            format!(
                ", after dropping {restricted} pair(s) with a non-positive value \
                 (inference needs rows every candidate shape can use)"
            )
        } else {
            String::new()
        };
        return refused(
            &ctx_empty,
            &format!(
                "only {n_seen} paired observations in the window, need \
                 {MIN_FIT_OBSERVATIONS}{dropped}"
            ),
        );
    }

    let mut fits: Vec<(DriverForm, BasisFit)> = Vec::new();
    let mut scores: Vec<CandidateScore> = Vec::new();
    for form in INFERENCE_CANDIDATES {
        let spec = form.spec();
        let Some(fit) = fit_basis(&spec, &groups) else {
            continue;
        };
        scores.push(CandidateScore {
            form: form.clone(),
            aic: fit.aic(spec.link),
            all_terms_significant: fit.all_significant(),
        });
        fits.push((form.clone(), fit));
    }

    // The null. If a straight line cannot be fitted at all, nothing here can.
    let Some(null_idx) = fits.iter().position(|(f, _)| *f == DriverForm::Linear) else {
        return refused(
            &ctx_empty,
            "no candidate shape could be fitted over this window (the driver does not \
             vary within any panel, or every basis was collinear)",
        );
    };
    let null_aic = scores[null_idx].aic;

    // Eligible = significant in every term, and either the null itself or a
    // decisive improvement on it.
    let mut best = null_idx;
    for i in 0..fits.len() {
        if !scores[i].all_terms_significant {
            continue;
        }
        if fits[i].0 != DriverForm::Linear && scores[i].aic > null_aic - MIN_AIC_IMPROVEMENT {
            continue;
        }
        if scores[i].aic < scores[best].aic {
            best = i;
        }
    }

    let (form, fit) = &fits[best];
    finish(edge, form.clone(), FormSource::Inferred, fit, scores)
}

/// Everything about a fit that does not depend on whether it succeeded.
struct FitContext<'e> {
    edge: &'e MetricEdge,
    n: usize,
    n_panels: usize,
    n_nonpositive: usize,
    moments: BasisMoments,
    domain: Option<(f64, f64)>,
}

fn base(ctx: &FitContext<'_>) -> FittedDriver {
    FittedDriver {
        from: ctx.edge.from.clone(),
        to: ctx.edge.to.clone(),
        lag: ctx.edge.lag,
        form: ctx.edge.form.clone(),
        form_source: if ctx.edge.form_declared {
            FormSource::Declared
        } else {
            FormSource::Inferred
        },
        candidates: Vec::new(),
        n: ctx.n,
        n_panels: ctx.n_panels,
        n_nonpositive: ctx.n_nonpositive,
        moments: ctx.moments,
        domain: ctx.domain,
        coefficient: None,
        coefficients: Vec::new(),
        se: 0.0,
        se_terms: Vec::new(),
        t_stat: 0.0,
        t_stats: Vec::new(),
        refusal: None,
    }
}

fn refused(ctx: &FitContext<'_>, reason: &str) -> FittedDriver {
    FittedDriver {
        refusal: Some(reason.to_string()),
        ..base(ctx)
    }
}

/// Days since the epoch, from whatever shape the warehouse returned the date
/// in. Only the leading `YYYY-MM-DD` is read, so timestamps pass too.
fn json_to_day_ordinal(v: &serde_json::Value) -> Option<i64> {
    let s = v.as_str()?;
    let date_part = s.get(..10)?;
    use chrono::Datelike;
    let parsed = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?;
    Some(parsed.num_days_from_ce() as i64)
}

fn json_to_key(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn json_to_f64_opt(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::{Driver, Measure, MeasureType, View};

    fn measure(name: &str, drivers: Option<Vec<Driver>>) -> Measure {
        Measure {
            name: name.to_string(),
            measure_type: MeasureType::Sum,
            description: None,
            expr: Some(name.to_string()),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers,
            shift: None,
            meta: None,
        }
    }

    fn driver(from: &str, coefficient: Option<f64>, lag: Option<u64>) -> Driver {
        Driver {
            measure: from.to_string(),
            direction: Default::default(),
            strength: Default::default(),
            confidence: Default::default(),
            coefficient,
            coefficients: None,
            form: Some(Default::default()),
            intercept: None,
            lag,
            description: None,
            refs: None,
        }
    }

    fn layer_with(measures: Vec<Measure>) -> SemanticLayer {
        SemanticLayer {
            views: vec![View {
                name: "ops".to_string(),
                description: None,
                label: None,
                datasource: None,
                dialect: None,
                table: Some("public.ops".to_string()),
                sql: None,
                entities: vec![],
                dimensions: vec![],
                measures: Some(measures),
                segments: vec![],
                pre_aggregations: None,
                refresh_key: None,
                meta: None,
            }],
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        }
    }

    /// `n_panels` x `n_days` rows where `y = slope * x (+ noise)`, x varying
    /// within each panel around a panel-specific level. The level offsets are
    /// deliberately huge relative to the within-panel variation — that is the
    /// between-panel confound a pooled regression would report instead of
    /// `slope`.
    fn panel_rows(
        n_panels: usize,
        n_days: usize,
        slope: f64,
        noise: f64,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let mut rows = Vec::new();
        for p in 0..n_panels {
            let level = 1_000.0 * (p as f64 + 1.0);
            for d in 0..n_days {
                let x = level + (d % 7) as f64;
                // Noise alternates sign so it cannot drift the slope, only
                // inflate its standard error.
                let wobble = if d % 2 == 0 { noise } else { -noise };
                let y = 10.0 * level + slope * (d % 7) as f64 + wobble;
                let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .checked_add_signed(chrono::Duration::days(d as i64))
                    .unwrap();
                let mut row = serde_json::Map::new();
                row.insert("ops__loc".into(), serde_json::json!(p as i64));
                row.insert("ops__day".into(), serde_json::json!(date.to_string()));
                row.insert("ops__spend".into(), serde_json::json!(x));
                row.insert("ops__sales".into(), serde_json::json!(y));
                rows.push(row);
            }
        }
        rows
    }

    fn fit_with(
        tree: &MetricTree,
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> Vec<FittedDriver> {
        let executor = move |_: &QueryRequest| Ok(rows.clone());
        fit_driver_coefficients(
            tree,
            &["ops.spend".to_string()],
            &["ops.loc".to_string()],
            "ops.day",
            ("2026-01-01", "2026-12-31"),
            &[],
            &executor,
        )
        .unwrap()
    }

    fn spend_drives_sales_tree(lag: Option<u64>) -> MetricTree {
        MetricTree::build(&layer_with(vec![
            measure("spend", None),
            measure("sales", Some(vec![driver("ops.spend", None, lag)])),
        ]))
    }

    #[test]
    fn recovers_the_within_panel_slope_not_the_between_panel_ratio() {
        let tree = spend_drives_sales_tree(None);
        let fits = fit_with(&tree, panel_rows(6, 40, 3.5, 0.0));

        assert_eq!(fits.len(), 1);
        let fit = &fits[0];
        let coefficient = fit.coefficient.expect("a clean linear panel must fit");
        // Between panels, y/x is ~10 (level vs 10*level). Within a panel it is
        // 3.5. Recovering 10 here would mean the demeaning never happened.
        assert!(
            (coefficient - 3.5).abs() < 1e-6,
            "expected the within-panel slope 3.5, got {coefficient}"
        );
        assert_eq!(fit.n_panels, 6);
        assert!(fit.refusal.is_none());
    }

    #[test]
    fn refuses_a_driver_that_does_not_move_the_target() {
        // y carries no signal from x at all — the fixture's weather case. A
        // fitter that reports whatever slope it measured would hand the
        // simulation a confident number manufactured out of noise.
        let tree = spend_drives_sales_tree(None);
        let mut rows = panel_rows(6, 40, 0.0, 0.0);
        for (i, row) in rows.iter_mut().enumerate() {
            let jitter = if i % 2 == 0 { 5.0 } else { -5.0 };
            let base = row["ops__sales"].as_f64().unwrap();
            row.insert("ops__sales".into(), serde_json::json!(base + jitter));
        }
        let fits = fit_with(&tree, rows);

        assert!(fits[0].coefficient.is_none(), "must refuse, not report ~0");
        let refusal = fits[0].refusal.as_deref().unwrap();
        assert!(
            refusal.contains("no reliable relationship"),
            "refusal must name the reason, got: {refusal}"
        );
    }

    #[test]
    fn refuses_when_the_window_is_too_short_to_trust() {
        let tree = spend_drives_sales_tree(None);
        // 2 panels x 4 days = 8 pairs, well under MIN_FIT_OBSERVATIONS.
        let fits = fit_with(&tree, panel_rows(2, 4, 3.5, 0.0));

        assert!(fits[0].coefficient.is_none());
        assert!(
            fits[0]
                .refusal
                .as_deref()
                .unwrap()
                .contains("paired observations"),
            "a thin window must be refused for being thin, not for insignificance"
        );
    }

    #[test]
    fn a_declared_coefficient_is_never_refitted() {
        // The declared number may come from an experiment — knowledge the
        // warehouse does not carry. Fitting over it would silently replace a
        // causal estimate with an observational one.
        let tree = MetricTree::build(&layer_with(vec![
            measure("spend", None),
            measure("sales", Some(vec![driver("ops.spend", Some(9.9), None)])),
        ]));
        assert!(fittable_edges(&tree, &["ops.spend".to_string()]).is_empty());

        let fits = fit_with(&tree, panel_rows(6, 40, 3.5, 0.0));
        assert!(fits.is_empty(), "nothing to fit, so no query and no result");
    }

    #[test]
    fn only_edges_a_lever_can_reach_are_fitted() {
        // Fitting is a warehouse query. Fitting edges the pinned lever cannot
        // propagate into spends that query for a number nobody will read.
        let tree = spend_drives_sales_tree(None);
        assert!(fittable_edges(&tree, &["ops.sales".to_string()]).is_empty());
        assert_eq!(fittable_edges(&tree, &["ops.spend".to_string()]).len(), 1);
    }

    #[test]
    fn pairs_are_built_at_the_edges_declared_lag() {
        // The same rows fit differently at lag 0 and lag 1, which is the whole
        // point of a lag: a driver that acts a day later must be paired a day
        // later or its effect is measured against the wrong outcome.
        let rows = panel_rows(6, 40, 3.5, 0.0);
        let at_0 = fit_with(&spend_drives_sales_tree(Some(0)), rows.clone());
        let at_1 = fit_with(&spend_drives_sales_tree(Some(1)), rows);

        let c0 = at_0[0].coefficient.expect("lag 0 fits");
        assert!((c0 - 3.5).abs() < 1e-6);
        assert_eq!(at_1[0].lag, Some(1));
        // The x=day%7 sawtooth means a 1-day shift genuinely changes the
        // relationship; asserting only that it differs keeps this a test of
        // pairing rather than of the sawtooth's arithmetic.
        match at_1[0].coefficient {
            Some(c1) => assert!(
                (c1 - c0).abs() > 1e-6,
                "lag must change which rows are paired"
            ),
            None => {} // a refusal at lag 1 is also evidence the shift applied
        }
    }

    fn driver_with_form(from: &str, form: DriverForm) -> Driver {
        Driver {
            form: Some(form),
            ..driver(from, None, None)
        }
    }

    fn log_log_tree() -> MetricTree {
        MetricTree::build(&layer_with(vec![
            measure("spend", None),
            measure(
                "sales",
                Some(vec![driver_with_form("ops.spend", DriverForm::LogLog)]),
            ),
        ]))
    }

    /// `y = c_p · x^elasticity` exactly, with a per-panel constant. In logs that
    /// is `ln y = ln c_p + elasticity · ln x`, so within-panel demeaning must
    /// recover `elasticity` and nothing else — the panel constant is precisely
    /// what the demeaning removes.
    ///
    /// `zero_days` rows per panel get `x = 0`, which has no log. They exist to
    /// pin down what happens at the edge of the transform's domain.
    fn power_law_rows(
        n_panels: usize,
        n_days: usize,
        elasticity: f64,
        zero_days: usize,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let mut rows = Vec::new();
        for p in 0..n_panels {
            let c = 10.0 * (p as f64 + 1.0);
            for d in 0..n_days {
                // Vary x multiplicatively: a log fit needs spread in logs, and
                // an additive wobble on a large level gives almost none.
                let x = if d < zero_days {
                    0.0
                } else {
                    100.0 * (1.0 + (d % 7) as f64)
                };
                let y = c * x.powf(elasticity);
                let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .checked_add_signed(chrono::Duration::days(d as i64))
                    .unwrap();
                let mut row = serde_json::Map::new();
                row.insert("ops__loc".into(), serde_json::json!(p as i64));
                row.insert("ops__day".into(), serde_json::json!(date.to_string()));
                row.insert("ops__spend".into(), serde_json::json!(x));
                row.insert("ops__sales".into(), serde_json::json!(y));
                rows.push(row);
            }
        }
        rows
    }

    // The point of declaring a form: the number that comes back is an
    // elasticity, not a level slope. Here the level slope dy/dx is around 0.6
    // and the elasticity is 0.4 — a fit that ignored the form would return the
    // former and every forecast across the edge would be wrong by y/x.
    #[test]
    fn a_log_log_edge_is_fitted_as_an_elasticity() {
        let tree = log_log_tree();
        let fits = fit_with(&tree, power_law_rows(3, 14, 0.4, 0));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(fit.form, DriverForm::LogLog, "the form is part of the fit");
        let coefficient = fit.coefficient.expect("a clean power law must fit");
        assert!(
            (coefficient - 0.4).abs() < 1e-9,
            "expected the 0.4 elasticity, got {coefficient}"
        );
        assert_eq!(fit.n_nonpositive, 0);
    }

    // A closed day is not evidence about an elasticity, and it has no log. It
    // must leave the fit and be counted — `n` is what the observation gate
    // reads, so a silent drop moves the gate for a reason nothing reports.
    #[test]
    fn a_log_fit_drops_non_positive_rows_and_counts_them() {
        let tree = log_log_tree();
        let fits = fit_with(&tree, power_law_rows(3, 16, 0.4, 2));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(fit.n_nonpositive, 6, "two zero days across three panels");
        assert_eq!(fit.n, 42, "16 days less the 2 zeroes, times 3 panels");
        let coefficient = fit.coefficient.expect("the surviving rows still fit");
        assert!((coefficient - 0.4).abs() < 1e-9);
    }

    // Same data, too few survivors: the refusal has to name the dropped rows,
    // or "only 12 paired observations" reads as a short window rather than as
    // a column that is mostly zero.
    #[test]
    fn a_refusal_names_the_rows_a_log_dropped() {
        let tree = log_log_tree();
        let fits = fit_with(&tree, power_law_rows(2, 20, 0.4, 14));
        let fit = fits.first().expect("one fittable edge");

        assert!(fit.coefficient.is_none());
        let refusal = fit.refusal.as_deref().unwrap_or_default();
        assert!(
            refusal.contains("non-positive") && refusal.contains("log-log"),
            "refusal must name the cause and the form, got: {refusal}"
        );
    }

    // The round trip is two requests, and the YAML can change between them. A
    // slope measured in logs applied as a level slope is wrong by target/driver
    // with nothing on the surface to show it — so a stale fit is dropped.
    #[test]
    fn a_fit_is_not_applied_under_a_form_it_was_not_measured_in() {
        let log_fit = fit_with(&log_log_tree(), power_law_rows(3, 14, 0.4, 0));
        assert!(log_fit[0].coefficient.is_some(), "precondition: it fitted");

        // The edge now declares `linear` — someone edited the view, or the
        // branch moved, after the baseline was taken.
        let mut linear_tree = spend_drives_sales_tree(None);
        apply_fitted_coefficients(&mut linear_tree, &log_fit);

        assert!(
            linear_tree.edges[0].coefficient.is_none(),
            "an elasticity must not be applied to a linear edge"
        );
    }

    #[test]
    fn a_fit_applies_when_the_form_still_matches() {
        let log_fit = fit_with(&log_log_tree(), power_law_rows(3, 14, 0.4, 0));
        let mut tree = log_log_tree();
        apply_fitted_coefficients(&mut tree, &log_fit);
        assert_eq!(tree.edges[0].coefficient, log_fit[0].coefficient);
    }

    fn quadratic_tree() -> MetricTree {
        MetricTree::build(&layer_with(vec![
            measure("spend", None),
            measure(
                "sales",
                Some(vec![driver_with_form("ops.spend", DriverForm::Quadratic)]),
            ),
        ]))
    }

    /// `y = b1*x + b2*x^2` per panel, plus a panel level the demeaning must
    /// remove. `x` sweeps a wide range so the curvature is identifiable.
    fn turning_rows(
        n_panels: usize,
        n_days: usize,
        b1: f64,
        b2: f64,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let mut rows = Vec::new();
        for p in 0..n_panels {
            let level = 5_000.0 * (p as f64 + 1.0);
            for d in 0..n_days {
                let x = 30.0 + (d % 20) as f64 * 6.0;
                // Alternating noise: cannot drift a coefficient, only inflate its
                // standard error — which is what gives the curvature term an
                // honestly small t when there is no curvature to find.
                // Deterministic, and NOT aligned to d's parity: an alternating
                // sign would sit on the same cycle as `x` and bias the curvature.
                // It is still not perfectly orthogonal to that cycle, which is why
                // the tolerances below are 1% rather than exact — a real random
                // draw would be, but a test must be reproducible.
                let wobble = ((d * 7 % 5) as f64 - 2.0) * 0.5;
                let y = level + b1 * x + b2 * x * x + wobble;
                let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .checked_add_signed(chrono::Duration::days(d as i64))
                    .unwrap();
                let mut row = serde_json::Map::new();
                row.insert("ops__loc".into(), serde_json::json!(p as i64));
                row.insert("ops__day".into(), serde_json::json!(date.to_string()));
                row.insert("ops__spend".into(), serde_json::json!(x));
                row.insert("ops__sales".into(), serde_json::json!(y));
                rows.push(row);
            }
        }
        rows
    }

    // The shape that was undeclarable before the coefficient became a vector.
    // Both terms must come back, in basis order, with the curvature's own t.
    #[test]
    fn a_quadratic_edge_is_fitted_as_two_coefficients() {
        let tree = quadratic_tree();
        let fits = fit_with(&tree, turning_rows(3, 40, 0.8, -0.0015));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(fit.form, DriverForm::Quadratic);
        assert_eq!(fit.coefficients.len(), 2, "slope and curvature");
        assert!(
            (fit.coefficients[0] - 0.8).abs() < 0.01,
            "{:?}",
            fit.coefficients
        );
        assert!(
            (fit.coefficients[1] - -0.0015).abs() < 3e-5,
            "{:?}",
            fit.coefficients
        );
        // The curvature is the term that matters, so its own t is asserted: this
        // is what a reader checks to see whether the turn is real.
        assert!(
            fit.t_stats[1].abs() > 10.0,
            "curvature t: {:?}",
            fit.t_stats
        );
        // The scalar mirrors the first term, so an older consumer still reads
        // something meaningful rather than nothing.
        assert_eq!(fit.coefficient, Some(fit.coefficients[0]));
        assert_eq!(fit.t_stats.len(), 2);
        // The moments are what let this be applied to an aggregate lever at all.
        assert!(fit.moments.s2 > 0.0 && fit.moments.n > 0.0);
        assert!(fit.domain.is_some());
    }

    // Every term has to clear the bar, not just the first. A curvature drawn from
    // noise is exactly the "confident number manufactured out of nothing" the gate
    // exists to stop — and it would come with a turning point attached.
    #[test]
    fn a_quadratic_with_no_real_curvature_is_refused_by_name() {
        let tree = quadratic_tree();
        // Genuinely linear data: the x^2 term has nothing to explain.
        let fits = fit_with(&tree, turning_rows(3, 40, 0.8, 0.0));
        let fit = fits.first().expect("one fittable edge");

        assert!(
            fit.coefficients.is_empty(),
            "must not fit a shape it cannot support"
        );
        let refusal = fit.refusal.as_deref().unwrap_or_default();
        assert!(
            refusal.contains("term 2 of 2"),
            "the refusal must name the term that failed, got: {refusal}"
        );
    }

    // A fitted quadratic must propagate through the moments — helping, then
    // saturating, then hurting — which is the whole point of carrying them.
    #[test]
    fn a_fitted_quadratic_turns_around_when_propagated() {
        let mut tree = quadratic_tree();
        let fits = fit_with(&tree, turning_rows(3, 40, 0.8, -0.0015));
        apply_fitted_coefficients(&mut tree, &fits);
        let edge = &tree.edges[0];
        assert_eq!(edge.coefficients.len(), 2);
        assert!(
            edge.moments.is_some(),
            "moments must travel with the coefficients"
        );

        let x = edge.moments.unwrap().s1;
        let values: crate::engine::metric_tree_ops::MeasureValues =
            [("ops.spend".to_string(), x), ("ops.sales".to_string(), 1e6)]
                .into_iter()
                .collect();
        let at = |r: f64| {
            crate::engine::metric_tree_ops::predict_with_values(
                &tree,
                &[("ops.spend".to_string(), x * r)],
                &values,
            )
            .unwrap()
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .map(|i| i.estimated_delta)
            .unwrap_or(0.0)
        };
        // This data's aggregate sits below the vertex, so it climbs first. The
        // turn is at +165% and break-even at +329%; +350% is past both and still
        // inside the observed spread (4.8x), so the domain backstop does not
        // swallow the case being tested.
        let (small, mid, big) = (at(0.05), at(0.20), at(3.50));
        assert!(small > 0.0, "a small push helps: {small}");
        assert!(mid > small, "still climbing at +20%: {mid}");
        assert!(big < 0.0, "a big push must actually hurt: {big}");
        assert!(mid > big, "and it must be a turn, not a dip");
    }

    /// A driver that declares NO form — the shape is measured, not asserted.
    fn undeclared_tree() -> MetricTree {
        MetricTree::build(&layer_with(vec![
            measure("spend", None),
            measure(
                "sales",
                Some(vec![Driver {
                    form: None,
                    ..driver("ops.spend", None, None)
                }]),
            ),
        ]))
    }

    // The point of the whole exercise: `form:` is an optimization, not a
    // prerequisite. Given genuinely linear history and nothing declared, the fit
    // has to come back with `linear` rather than refusing or guessing a curve.
    #[test]
    fn an_undeclared_form_is_inferred_as_linear_from_linear_history() {
        let tree = undeclared_tree();
        assert!(
            !tree.edges[0].form_declared,
            "precondition: nothing declared"
        );
        let fits = fit_with(&tree, panel_rows(3, 40, 5.0, 2.0));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(fit.form, DriverForm::Linear);
        assert_eq!(fit.form_source, FormSource::Inferred);
        assert!(
            (fit.coefficient.expect("fitted") - 5.0).abs() < 0.5,
            "{:?}",
            fit.coefficient
        );
        // Every candidate is reported with a comparable score, so an inferred
        // shape is arguable rather than a mystery.
        assert!(fit.candidates.len() >= 3, "{:?}", fit.candidates);
    }

    // Same edge, curved history. Nothing declared, and the fit must FIND the
    // turning point — which is the thing a single coefficient cannot express.
    #[test]
    fn an_undeclared_form_is_inferred_as_quadratic_from_curved_history() {
        let tree = undeclared_tree();
        let fits = fit_with(&tree, turning_rows(3, 40, 0.8, -0.0015));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(
            fit.form,
            DriverForm::Quadratic,
            "candidates: {:?}",
            fit.candidates
        );
        assert_eq!(fit.form_source, FormSource::Inferred);
        assert_eq!(fit.coefficients.len(), 2);
        assert!(fit.coefficients[1] < 0.0, "curvature bends down");
        // And it beat the straight line decisively, not on a tie.
        let lin = fit
            .candidates
            .iter()
            .find(|c| c.form == DriverForm::Linear)
            .expect("linear was considered");
        let quad = fit
            .candidates
            .iter()
            .find(|c| c.form == DriverForm::Quadratic)
            .unwrap();
        assert!(
            lin.aic - quad.aic >= MIN_AIC_IMPROVEMENT,
            "must clear the margin: linear {} vs quadratic {}",
            lin.aic,
            quad.aic
        );
    }

    // A power law. This is the case a naive comparison gets wrong, because the
    // residuals of a `ln y` model and a `y` model are in different units — the
    // Jacobian in `candidate_aic` is what makes the two scores comparable.
    #[test]
    fn an_undeclared_form_is_inferred_as_log_log_from_a_power_law() {
        let tree = undeclared_tree();
        let fits = fit_with(&tree, power_law_rows(3, 40, 0.4, 0));
        let fit = fits.first().expect("one fittable edge");

        assert_eq!(
            fit.form,
            DriverForm::LogLog,
            "candidates: {:?}",
            fit.candidates
        );
        assert!(
            (fit.coefficient.expect("fitted") - 0.4).abs() < 0.02,
            "the elasticity, not a level slope: {:?}",
            fit.coefficient
        );
    }

    // The margin is load-bearing. A curve that merely ties with a line must lose,
    // because observational data will always let you add a term.
    #[test]
    fn inference_prefers_the_straight_line_unless_a_curve_decisively_beats_it() {
        let tree = undeclared_tree();
        let fits = fit_with(&tree, panel_rows(3, 40, 5.0, 2.0));
        let fit = fits.first().unwrap();
        let lin = fit
            .candidates
            .iter()
            .find(|c| c.form == DriverForm::Linear)
            .unwrap();
        for c in &fit.candidates {
            if c.form == DriverForm::Linear {
                continue;
            }
            // Either it was not significant in every term, or it failed to clear
            // the margin. Both are reasons to keep the line.
            assert!(
                !c.all_terms_significant || c.aic > lin.aic - MIN_AIC_IMPROVEMENT,
                "{:?} should not have been eligible on linear data",
                c
            );
        }
        assert_eq!(fit.form, DriverForm::Linear);
    }

    // Declaring a form skips the search entirely — that is what makes it an
    // optimization — and reports itself as declared.
    #[test]
    fn a_declared_form_is_not_searched() {
        let tree = spend_drives_sales_tree(None);
        assert!(tree.edges[0].form_declared);
        let fits = fit_with(&tree, panel_rows(3, 40, 5.0, 2.0));
        let fit = fits.first().unwrap();
        assert_eq!(fit.form_source, FormSource::Declared);
        assert!(fit.candidates.is_empty(), "nothing was compared");
    }

    // An inferred shape has to reach the edge, or propagation would evaluate the
    // coefficients under the placeholder form they were not measured in.
    #[test]
    fn an_inferred_form_is_written_onto_the_edge() {
        let mut tree = undeclared_tree();
        let fits = fit_with(&tree, turning_rows(3, 40, 0.8, -0.0015));
        apply_fitted_coefficients(&mut tree, &fits);
        assert_eq!(tree.edges[0].form, DriverForm::Quadratic);
        assert_eq!(tree.edges[0].coefficients.len(), 2);
        assert!(tree.edges[0].moments.is_some());
    }

    // Back-compat on the wire: a FittedDriver serialized before `form` existed
    // could only have been linear, and must still deserialize as such rather
    // than failing the whole predict call.
    #[test]
    fn a_fit_without_a_form_field_deserializes_as_linear() {
        let json = r#"{"from":"ops.spend","to":"ops.sales","coefficient":5.78,"n":100}"#;
        let fit: FittedDriver = serde_json::from_str(json).unwrap();
        assert_eq!(fit.form, DriverForm::Linear);
        assert_eq!(fit.coefficient, Some(5.78));
        assert_eq!(fit.n_nonpositive, 0);
    }

    #[test]
    fn a_failed_panel_query_refuses_every_candidate_by_name() {
        let tree = spend_drives_sales_tree(None);
        let executor = move |_: &QueryRequest| Err(EngineError::QueryError("boom".to_string()));
        let fits = fit_driver_coefficients(
            &tree,
            &["ops.spend".to_string()],
            &["ops.loc".to_string()],
            "ops.day",
            ("2026-01-01", "2026-12-31"),
            &[],
            &executor,
        )
        .unwrap();

        assert_eq!(fits.len(), 1, "a failed fit is reported, never dropped");
        assert!(fits[0].coefficient.is_none());
        assert!(fits[0].refusal.as_deref().unwrap().contains("boom"));
    }

    #[test]
    fn applying_a_fit_makes_predict_cross_the_edge() {
        let mut tree = spend_drives_sales_tree(None);
        let fits = fit_with(&tree, panel_rows(6, 40, 3.5, 0.0));
        apply_fitted_coefficients(&mut tree, &fits);

        let result =
            crate::engine::metric_tree_ops::predict(&tree, &[("ops.spend".to_string(), 100.0)])
                .unwrap();
        let sales = result
            .impacts
            .iter()
            .find(|i| i.measure == "ops.sales")
            .expect("the fitted edge must now propagate");
        assert!((sales.estimated_delta - 350.0).abs() < 1e-4);
        // A fitted coefficient is an observational estimate, so it must carry
        // the same hedged label a declared one does — never "exact".
        assert_eq!(sales.confidence, "estimated");
    }

    #[test]
    fn a_refused_fit_leaves_the_edge_inert() {
        let mut tree = spend_drives_sales_tree(None);
        let refusals = vec![refused(
            &FitContext {
                edge: tree.edges.first().unwrap(),
                n: 0,
                n_panels: 0,
                n_nonpositive: 0,
                moments: BasisMoments::default(),
                domain: None,
            },
            "no reliable relationship",
        )];
        apply_fitted_coefficients(&mut tree, &refusals);

        assert!(tree.edges[0].coefficient.is_none());
        let result =
            crate::engine::metric_tree_ops::predict(&tree, &[("ops.spend".to_string(), 100.0)])
                .unwrap();
        assert!(
            result.impacts.is_empty(),
            "a refused fit must propagate nothing, not zero"
        );
    }

    #[test]
    fn panel_dimensions_come_from_the_source_views_foreign_entities() {
        use crate::schema::models::Entity;
        let mut layer = layer_with(vec![
            measure("spend", None),
            measure("sales", Some(vec![driver("ops.spend", None, None)])),
        ]);
        layer.views[0].entities = vec![
            Entity {
                name: "op_day".into(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("op_day_id".into()),
                keys: None,
                lifespan: None,
                inherits_from: None,
                parent: None,
                meta: None,
            },
            Entity {
                name: "loc".into(),
                entity_type: EntityType::Foreign,
                description: None,
                key: Some("loc_id".into()),
                keys: None,
                lifespan: None,
                inherits_from: None,
                parent: None,
                meta: None,
            },
        ];
        let tree = MetricTree::build(&layer);
        let edges = fittable_edges(&tree, &["ops.spend".to_string()]);

        // The primary key is per-row — grouping by it makes every panel a
        // single observation and the fit degenerate. Only foreign keys name a
        // panel that spans time.
        assert_eq!(fit_panel_dimensions(&layer, &edges), vec!["ops.loc_id"]);
    }
}
