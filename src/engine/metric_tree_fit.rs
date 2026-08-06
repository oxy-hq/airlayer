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
    /// The fitted within-panel slope, present only when the gate passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coefficient: Option<f64>,
    /// Standard error of the slope.
    #[serde(default)]
    pub se: f64,
    /// slope / se.
    #[serde(default)]
    pub t_stat: f64,
    /// Why no coefficient was produced. `None` exactly when `coefficient` is
    /// `Some` — the two are one enum flattened for serialization.
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
        let Some(coefficient) = fit.coefficient else {
            continue;
        };
        for edge in &mut tree.edges {
            if edge.kind == EdgeKind::Driver
                && edge.coefficient.is_none()
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
                if edge.form != fit.form {
                    continue;
                }
                edge.coefficient = Some(coefficient);
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
                .map(|edge| refused(edge, 0, 0, 0, &reason))
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

/// One `(x, y)` pair mapped into the space the edge's form is linear in, or
/// `None` when the transform is undefined there.
///
/// A log needs a strictly positive input. A pair with a non-positive value on a
/// logged axis carries no information the transform can represent — a zero
/// marketing-spend day says nothing about an elasticity — so it is dropped, and
/// the caller counts it. Substituting a small epsilon instead would invent an
/// enormous negative log and let one closed day dominate the slope.
fn transform_pair(form: &DriverForm, x: f64, y: f64) -> Option<(f64, f64)> {
    let log = |v: f64| (v > 0.0).then(|| v.ln());
    match form {
        DriverForm::Linear => Some((x, y)),
        DriverForm::LogLog => Some((log(x)?, log(y)?)),
        DriverForm::LogLinear => Some((x, log(y)?)),
        DriverForm::LinearLog => Some((log(x)?, y)),
    }
}

fn fit_one_edge(edge: &MetricEdge, panel: &PanelData) -> FittedDriver {
    let lag = edge.lag.unwrap_or(0) as i64;
    let x_alias = edge.from.replace('.', "__");
    let y_alias = edge.to.replace('.', "__");

    // (x at day d, y at day d+lag), per panel, in the form's own space. Days
    // with no partner at d+lag carry no lead-lag information and are skipped,
    // not zero-filled. Transforming HERE rather than after grouping is what
    // makes the demeaning below a within-panel fit of the declared curve: for
    // log-log it demeans logs, so the slope is an elasticity net of panel
    // level, not the log of a level slope.
    let mut groups: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut n_nonpositive = 0usize;
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
            match transform_pair(&edge.form, x, y) {
                Some(pair) => pts.push(pair),
                None => n_nonpositive += 1,
            }
        }
        if pts.len() >= 2 {
            groups.push(pts);
        }
    }

    let n: usize = groups.iter().map(Vec::len).sum();
    let n_panels = groups.len();
    if n < MIN_FIT_OBSERVATIONS {
        // Name the dropped rows: under a log form they are the usual reason a
        // window that looks ample fails this gate.
        let dropped = if n_nonpositive > 0 {
            format!(
                ", after dropping {n_nonpositive} pair(s) with a non-positive value on a \
                 log axis ({} form)",
                edge.form
            )
        } else {
            String::new()
        };
        return refused(
            edge,
            n,
            n_panels,
            n_nonpositive,
            &format!(
                "only {n} paired observations in the window, need {MIN_FIT_OBSERVATIONS}{dropped}"
            ),
        );
    }

    // Within-panel OLS: demean each group against itself, then pool.
    let mut xs: Vec<f64> = Vec::with_capacity(n);
    let mut ys: Vec<f64> = Vec::with_capacity(n);
    for pts in &groups {
        let mx = pts.iter().map(|p| p.0).sum::<f64>() / pts.len() as f64;
        let my = pts.iter().map(|p| p.1).sum::<f64>() / pts.len() as f64;
        for (x, y) in pts {
            xs.push(x - mx);
            ys.push(y - my);
        }
    }
    let sxx: f64 = xs.iter().map(|x| x * x).sum();
    if sxx < f64::EPSILON {
        return refused(
            edge,
            n,
            n_panels,
            n_nonpositive,
            "the driver does not vary within any panel",
        );
    }
    let slope = xs.iter().zip(&ys).map(|(x, y)| x * y).sum::<f64>() / sxx;
    let dof = n.saturating_sub(n_panels + 1);
    if dof == 0 {
        return refused(
            edge,
            n,
            n_panels,
            n_nonpositive,
            "not enough observations per panel",
        );
    }
    let resid_ss: f64 = xs
        .iter()
        .zip(&ys)
        .map(|(x, y)| {
            let r = y - slope * x;
            r * r
        })
        .sum();
    let se = ((resid_ss / dof as f64) / sxx).sqrt();
    let t_stat = if se > 0.0 { slope / se } else { f64::INFINITY };

    if t_stat.abs() < MIN_FIT_T {
        return FittedDriver {
            from: edge.from.clone(),
            to: edge.to.clone(),
            lag: edge.lag,
            form: edge.form.clone(),
            n,
            n_panels,
            n_nonpositive,
            coefficient: None,
            se,
            t_stat,
            refusal: Some(format!(
                "no reliable relationship in this window (t = {t_stat:.2}, need |t| >= {MIN_FIT_T})"
            )),
        };
    }

    FittedDriver {
        from: edge.from.clone(),
        to: edge.to.clone(),
        lag: edge.lag,
        form: edge.form.clone(),
        n,
        n_panels,
        n_nonpositive,
        coefficient: Some(slope),
        se,
        t_stat,
        refusal: None,
    }
}

fn refused(
    edge: &MetricEdge,
    n: usize,
    n_panels: usize,
    n_nonpositive: usize,
    reason: &str,
) -> FittedDriver {
    FittedDriver {
        from: edge.from.clone(),
        to: edge.to.clone(),
        lag: edge.lag,
        form: edge.form.clone(),
        n,
        n_panels,
        n_nonpositive,
        coefficient: None,
        se: 0.0,
        t_stat: 0.0,
        refusal: Some(reason.to_string()),
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
            form: Default::default(),
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
            form,
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
            tree.edges.first().unwrap(),
            0,
            0,
            0,
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
