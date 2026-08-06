//! Driver response functions: one representation for every declared `form:`.
//!
//! A driver edge says "x moves y, and by this much". `form:` says in what shape.
//! Every shape wanted so far is the same model — a linear fit in a **basis** of
//! transformed regressors, under a **link** on the target:
//!
//! ```text
//! g(y) = a_panel + SUM_k beta_k * phi_k(x) + e
//! ```
//!
//! so a named form is a lookup, not a code path:
//!
//! | `form` | basis | link | `beta` means |
//! | --- | --- | --- | --- |
//! | `linear` | `[x]` | identity | units of y per unit of x |
//! | `log-log` | `[ln x]` | log | elasticity (% per %) |
//! | `log-linear` | `[x]` | log | proportional change in y per unit of x |
//! | `linear-log` | `[ln x]` | identity | units of y per log-point of x |
//! | `quadratic` | `[x, x^2]` | identity | slope, then curvature |
//!
//! Adding a shape is a row in [`DriverForm::spec`]. That is the whole point: the
//! fit ([`super::metric_tree_fit`]) and the forecast ([`aggregate_delta`]) never
//! learn that a new shape exists, because both are written against the basis
//! rather than against the form.
//!
//! ## Why the fit and the forecast disagree about grain, and what fixes it
//!
//! The fit is per row — one store-day. The lever is an **aggregate**: the
//! scenario baseline values each measure as one number over the whole window. So
//! a row-level response gets applied to a sum, and `SUM f(x_i) != f(SUM x_i)`
//! unless `f` is linear. Each form shipped before this module survived that for
//! an unrelated reason (`linear` because sums of linear are linear; `log-log`
//! because an elasticity is scale-free), which is exactly why there was no shared
//! rule and why a curvature could not be added.
//!
//! The rule: read a lever as a **uniform proportional shift** — moving the
//! aggregate by `dX` means every row goes to `x_i * (1 + r)` for `r = dX / X`.
//! It is the only reading that is well defined without assuming how the change
//! distributes across rows, and it is what the ratio-based `log-log` arithmetic
//! already assumed implicitly. Under it, an identity link aggregates **exactly**
//! through the moments of the basis:
//!
//! ```text
//! dY = SUM_k beta_k * [ M_k(r) - M_k(0) ]     M_k(r) = SUM_i phi_k(x_i * (1+r))
//! ```
//!
//! and each term's moment is closed-form in a statistic the fit already has the
//! rows to compute — see [`BasisTerm::moment_delta`]. That is what makes a
//! curvature usable at all, and it is exact rather than first-order.

use crate::schema::models::DriverForm;

/// One column of the design matrix: a function of the driver's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasisTerm {
    /// `x` — the raw level.
    Identity,
    /// `ln x`. Undefined at or below zero, which is what drops a closed day.
    Log,
    /// `x^2` — the curvature term. Only meaningful alongside [`Self::Identity`],
    /// since a pure square has its vertex pinned at the origin.
    Square,
}

/// How the target enters the regression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    /// `y` as-is. The response is in the target's own units, and aggregates
    /// exactly through the basis moments.
    Identity,
    /// `ln y`. The response is proportional, so it needs the target's current
    /// level — and aggregates exactly only when the basis is `[ln x]`, where the
    /// per-row change is the same for every row.
    Log,
}

/// The basis and link a `form:` compiles to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseSpec {
    pub basis: &'static [BasisTerm],
    pub link: Link,
}

impl ResponseSpec {
    /// How many coefficients this shape needs. The scalar `coefficient:` field
    /// can only ever express a spec of width 1 — which is why a turning point was
    /// undeclarable before the vector.
    pub fn width(&self) -> usize {
        self.basis.len()
    }

    /// Whether an aggregate lever can be honoured **exactly** for this spec.
    ///
    /// False only for a log link over a non-log basis (`log-linear`): its
    /// per-row change `beta * x_i * r` varies by row, so `SUM y_i * exp(...)` has
    /// no closed form in fixed moments. Reported rather than hidden — three
    /// shipped example views declare that form, so it stays first-order rather
    /// than being refused, but it stops pretending to be exact.
    pub fn aggregates_exactly(&self) -> bool {
        match self.link {
            Link::Identity => true,
            Link::Log => self.basis == [BasisTerm::Log],
        }
    }
}

const LINEAR: ResponseSpec = ResponseSpec {
    basis: &[BasisTerm::Identity],
    link: Link::Identity,
};
const LOG_LOG: ResponseSpec = ResponseSpec {
    basis: &[BasisTerm::Log],
    link: Link::Log,
};
const LOG_LINEAR: ResponseSpec = ResponseSpec {
    basis: &[BasisTerm::Identity],
    link: Link::Log,
};
const LINEAR_LOG: ResponseSpec = ResponseSpec {
    basis: &[BasisTerm::Log],
    link: Link::Identity,
};
const QUADRATIC: ResponseSpec = ResponseSpec {
    basis: &[BasisTerm::Identity, BasisTerm::Square],
    link: Link::Identity,
};

impl DriverForm {
    /// The one table. A new shape is a row here and nothing else.
    pub fn spec(&self) -> ResponseSpec {
        match self {
            DriverForm::Linear => LINEAR,
            DriverForm::LogLog => LOG_LOG,
            DriverForm::LogLinear => LOG_LINEAR,
            DriverForm::LinearLog => LINEAR_LOG,
            DriverForm::Quadratic => QUADRATIC,
        }
    }
}

/// The shapes the fit will consider when a driver declares no `form:`.
///
/// `form:` is an **override, not a requirement**. Left out, the shape is measured
/// from history like the magnitude is; declared, it pins the shape and skips the
/// search. That ordering matters: a modeller should not have to know the
/// functional form of a relationship before asking what it is.
///
/// Two shapes are deliberately absent. `log-linear` and a log-linked quadratic
/// cannot be honoured **exactly** on an aggregate lever (see
/// [`ResponseSpec::aggregates_exactly`]), and inference must not hand the forecast
/// a shape it can only approximate — the human can still declare either.
///
/// `Linear` is first because it is the null: anything more elaborate has to beat
/// it by a margin, not merely tie.
pub const INFERENCE_CANDIDATES: &[DriverForm] = &[
    DriverForm::Linear,
    DriverForm::LogLog,
    DriverForm::LinearLog,
    DriverForm::Quadratic,
];

/// Whether a form's coefficients came from the YAML or from the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FormSource {
    /// The edge declared `form:`. Nothing was searched.
    #[default]
    Declared,
    /// No `form:` was declared, so the shape was selected from history.
    Inferred,
}

/// How well one candidate shape fitted, on a scale comparable ACROSS shapes.
///
/// The comparison is the hard part. A model of `y` and a model of `ln y` have
/// residuals in different units, so their RSS cannot be compared directly — this
/// is the usual reason people compare shapes badly. The fix is the standard one:
/// score the likelihood **in y-space**, which for a log link means adding the
/// Jacobian of the transform (`- SUM ln y`). That is exactly Box-Cox's device,
/// restricted to the two transforms this grid has, so it stays plain OLS and the
/// coefficients keep the standard errors the refusal gate is built on.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CandidateScore {
    pub form: DriverForm,
    /// Akaike information criterion in y-space — LOWER is better. Comparable
    /// across links precisely because of the Jacobian term.
    pub aic: f64,
    /// Whether every basis term cleared the significance gate. A candidate that
    /// fits well but rests on an insignificant term is not eligible, however good
    /// its AIC: that is how a curvature gets invented from noise.
    pub all_terms_significant: bool,
}

/// AIC in y-space for a fitted candidate. `sum_ln_y` is the Jacobian correction
/// and must be 0 for an identity link.
///
/// `n * ln(RSS/n) + 2k` is the usual profile form; the `+ 2 * SUM ln y` is what
/// makes a `ln y` model's number mean the same thing as a `y` model's.
pub fn candidate_aic(link: Link, n: usize, k: usize, rss: f64, sum_ln_y: f64) -> f64 {
    if n == 0 || rss <= 0.0 {
        return f64::NEG_INFINITY;
    }
    let base = (n as f64) * (rss / n as f64).ln() + 2.0 * k as f64;
    match link {
        Link::Identity => base,
        Link::Log => base + 2.0 * sum_ln_y,
    }
}

/// How much better than the null a candidate must score to be adopted.
///
/// 10 is the conventional "very strong" threshold on an AIC difference. It is a
/// margin rather than a tie-break on purpose: the null here is `linear`, and
/// preferring a curve that merely ties with a straight line is how observational
/// data talks you into a shape it cannot support.
pub const MIN_AIC_IMPROVEMENT: f64 = 10.0;

impl BasisTerm {
    /// This term's value for one row, or `None` where the transform is undefined.
    ///
    /// A pair with a non-positive value on a logged axis carries no information
    /// the transform can represent — a closed day says nothing about an
    /// elasticity — so it is dropped, and the fit counts it. Substituting a small
    /// epsilon would invent an enormous negative log and let one closed day
    /// dominate the slope.
    pub fn apply(&self, x: f64) -> Option<f64> {
        match self {
            BasisTerm::Identity => Some(x),
            BasisTerm::Log => (x > 0.0).then(|| x.ln()),
            BasisTerm::Square => Some(x * x),
        }
    }

    /// `M_k(r) - M_k(0)` — how this term's total moves when every row is scaled
    /// by `(1 + r)`. Closed-form, which is what keeps the forecast database-free:
    ///
    /// - `x`     → `r * s1`
    /// - `x^2`   → `((1+r)^2 - 1) * s2`
    /// - `ln x`  → `n * ln(1+r)` — note the `n`. The per-row change is the same
    ///   for every row, but there are `n` of them; dropping the factor was a real
    ///   bug in the hand-written `linear-log` arithmetic this replaces.
    ///
    /// `None` when the shift is outside the term's domain (`r <= -1` under a log,
    /// i.e. a cut that would take every row to zero or below).
    pub fn moment_delta(&self, m: &BasisMoments, r: f64) -> Option<f64> {
        match self {
            BasisTerm::Identity => Some(r * m.s1),
            BasisTerm::Square => Some(((1.0 + r) * (1.0 + r) - 1.0) * m.s2),
            BasisTerm::Log => (1.0 + r > 0.0).then(|| m.n * (1.0 + r).ln()),
        }
    }
}

/// The sufficient statistics of a basis over the rows a fit actually used.
///
/// Three numbers, computed once during the fit, and enough to evaluate any
/// polynomial-or-log basis under a proportional shift. `n` is the paired
/// observation count, so it matches the rows the coefficients were measured on —
/// not the window's row count, which differs whenever a lag or a dropped
/// non-positive value took rows out.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BasisMoments {
    /// Rows behind the fit.
    #[serde(default)]
    pub n: f64,
    /// `SUM x_i`.
    #[serde(default)]
    pub s1: f64,
    /// `SUM x_i^2`.
    #[serde(default)]
    pub s2: f64,
}

impl Default for BasisMoments {
    fn default() -> Self {
        Self {
            n: 0.0,
            s1: 0.0,
            s2: 0.0,
        }
    }
}

impl BasisMoments {
    pub fn from_values(xs: &[f64]) -> Self {
        Self {
            n: xs.len() as f64,
            s1: xs.iter().sum(),
            s2: xs.iter().map(|x| x * x).sum(),
        }
    }
}

/// What a response implies for its target, given a proportional shift.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResponseDelta {
    /// An exact aggregate change.
    Sized(f64),
    /// A change this spec cannot express exactly on an aggregate — currently only
    /// `log-linear`. The number is first-order; the variant is how a caller knows
    /// not to present it as more than that.
    Approximate(f64),
    /// A log link needs the target's current level and it was absent or zero.
    NeedsTarget,
    /// The shift leaves every observed value behind: `(1+r)` outside the spread
    /// the fit actually saw. A backstop against an absurd lever, not a tight
    /// bound.
    OutsideDomain,
    /// The shift is outside the basis' domain — a cut past zero under a log.
    Undefined,
}

/// The change in an aggregate target when its driver's aggregate moves by `r`
/// proportionally.
///
/// One expression for every shape. There is deliberately no `match` on
/// `DriverForm` here: the arithmetic is decided by the basis and the link, so a
/// new form reaches this function already supported.
///
/// `target` is the target's current aggregate, needed only under a log link.
/// `domain` is the `(min, max)` of the driver values the fit saw, when known.
pub fn aggregate_delta(
    spec: &ResponseSpec,
    coefficients: &[f64],
    moments: &BasisMoments,
    r: f64,
    target: Option<f64>,
    domain: Option<(f64, f64)>,
) -> ResponseDelta {
    if coefficients.len() != spec.width() {
        // A coefficient vector of the wrong width is not a shape we can evaluate;
        // guessing which terms were meant is how an elasticity gets applied as a
        // level slope.
        return ResponseDelta::Undefined;
    }
    if 1.0 + r <= 0.0 {
        return ResponseDelta::Undefined;
    }
    // Backstop: a shift bigger than the whole observed spread puts every row
    // somewhere the fit has never seen. Quadratics extrapolate catastrophically,
    // so this is checked for every basis rather than argued per form.
    if let Some((lo, hi)) = domain {
        if lo > 0.0 && hi > 0.0 {
            let spread = hi / lo;
            if 1.0 + r > spread || 1.0 + r < 1.0 / spread {
                return ResponseDelta::OutsideDomain;
            }
        }
    }

    // In link space: the sum of each term's moment change, weighted by its
    // coefficient. The panel intercepts cancel, which is why demeaning during the
    // fit costs nothing here.
    let mut delta_link = 0.0;
    for (term, coeff) in spec.basis.iter().zip(coefficients) {
        match term.moment_delta(moments, r) {
            Some(d) => delta_link += coeff * d,
            None => return ResponseDelta::Undefined,
        }
    }

    match spec.link {
        Link::Identity => ResponseDelta::Sized(delta_link),
        Link::Log => {
            let Some(y) = target.filter(|y| y.abs() > f64::EPSILON) else {
                return ResponseDelta::NeedsTarget;
            };
            if spec.aggregates_exactly() {
                // basis == [ln x], so the per-row log change is beta*ln(1+r) for
                // every row alike and the whole aggregate scales by it. Exact,
                // where the arithmetic this replaces was first-order: at +50% on
                // an elasticity of 0.45 the two differ by 12.4%.
                let per_row = coefficients[0] * (1.0 + r).ln();
                ResponseDelta::Sized(y * (per_row.exp() - 1.0))
            } else {
                // log-linear. `SUM y_i * exp(beta*x_i*r)` needs per-row targets;
                // this is the first-order stand-in, flagged as such.
                ResponseDelta::Approximate(y * (delta_link / moments.n.max(1.0)))
            }
        }
    }
}

/// [`aggregate_delta`] for an edge that was **never fitted**, so no moments exist
/// — a declared `coefficient:` and nothing else.
///
/// Only the specs whose moments are recoverable from the aggregate alone can be
/// served, and the rest must refuse rather than substitute:
///
/// | spec | servable? | why |
/// | --- | --- | --- |
/// | `[x]` identity | yes, exactly | `s1` *is* the aggregate, so `r*s1 == delta` |
/// | `[ln x]` log | yes, exactly | scale-free; the row count cancels |
/// | `[x]` log | first-order | preserved as shipped, still flagged |
/// | anything with `x^2` | **no** | needs `SUM x_i^2`, and `SUM x_i^2 != (SUM x_i)^2` |
/// | anything with `ln x` under identity | **no** | needs the row count `n` |
///
/// That last-but-one row is the whole reason a declared `quadratic` is refused
/// here instead of answered: substituting `(SUM x)^2` for `SUM x^2` on the
/// fixture is 42,905x out **with the sign flipped**, which would report a
/// money-making lever as a money-loser. A quadratic has to be fitted, because
/// only the fit sees the rows.
pub fn aggregate_delta_from_total(
    spec: &ResponseSpec,
    coefficients: &[f64],
    driver_delta: f64,
    driver_total: Option<f64>,
    target: Option<f64>,
) -> ResponseDelta {
    if coefficients.len() != spec.width() {
        return ResponseDelta::Undefined;
    }
    let beta = coefficients[0];
    match (spec.basis, spec.link) {
        // Exact: a sum of linear functions is linear in the sum, so no level is
        // needed at all. This is what keeps delta-only mode answering.
        ([BasisTerm::Identity], Link::Identity) => ResponseDelta::Sized(beta * driver_delta),
        // Exact: an elasticity is scale-free, so it needs the two aggregates and
        // nothing about the rows.
        ([BasisTerm::Log], Link::Log) => {
            let (Some(x), Some(y)) = (
                driver_total.filter(|v| v.abs() > f64::EPSILON),
                target.filter(|v| v.abs() > f64::EPSILON),
            ) else {
                return ResponseDelta::NeedsTarget;
            };
            let r = driver_delta / x;
            if 1.0 + r <= 0.0 {
                return ResponseDelta::Undefined;
            }
            ResponseDelta::Sized(y * ((beta * (1.0 + r).ln()).exp() - 1.0))
        }
        // First-order, as it has always been. Flagged, not silently exact.
        ([BasisTerm::Identity], Link::Log) => match target.filter(|v| v.abs() > f64::EPSILON) {
            Some(y) => ResponseDelta::Approximate(y * beta * driver_delta),
            None => ResponseDelta::NeedsTarget,
        },
        // Needs statistics only the rows carry.
        _ => ResponseDelta::OutsideDomain,
    }
}

/// The response sampled across the levers a user could plausibly pull.
///
/// This replaces a closed-form "where is the peak" solver, and the reason is
/// worth stating: that solver was `-b1*s1/(2*b2*s2) - 1`, which is the vertex of
/// a parabola and nothing else. It answers one question for one basis, so every
/// shape that could also turn — a cubic, a spline, a saturating curve with a
/// ceiling — needs its own solver and its own caller branch. Sampling answers
/// *every* question for *every* basis: the peak is the largest sample, break-even
/// is where the sign changes, "saturating" is a shrinking difference, and a shape
/// that does none of those simply has no such sample.
///
/// Returned as `(r, delta)` pairs over the valid range, so a caller reads
/// behaviour off the curve instead of being told a name. That is also what makes
/// the presentation layer form-free: "+10% -> +1,838" needs no unit vocabulary,
/// where "0.854 per unit" needs a different sentence per shape.
///
/// Both directions are sampled — a cut is a lever too. The range is bounded by
/// the fit's own observed spread when known (see [`aggregate_delta`]'s backstop),
/// so no sample is an extrapolation past the evidence.
pub fn response_profile(
    spec: &ResponseSpec,
    coefficients: &[f64],
    moments: &BasisMoments,
    target: Option<f64>,
    domain: Option<(f64, f64)>,
    samples: usize,
) -> Vec<(f64, f64)> {
    // Hard bounds on what counts as a plausible lever, so a 6x observed spread
    // does not produce a profile mostly made of +500% moves nobody would try.
    const R_MIN: f64 = -0.9;
    const R_MAX: f64 = 2.0;
    let (mut lo, mut hi) = (R_MIN, R_MAX);
    if let Some((dlo, dhi)) = domain {
        if dlo > 0.0 && dhi > 0.0 {
            let spread = dhi / dlo;
            lo = lo.max(1.0 / spread - 1.0);
            hi = hi.min(spread - 1.0);
        }
    }
    if samples < 2 || !(hi > lo) {
        return Vec::new();
    }
    (0..samples)
        .filter_map(|i| {
            let r = lo + (hi - lo) * (i as f64) / ((samples - 1) as f64);
            match aggregate_delta(spec, coefficients, moments, r, target, domain) {
                // An unsizable point is omitted rather than reported as zero: the
                // caller must not read a gap as "no effect here".
                ResponseDelta::Sized(d) | ResponseDelta::Approximate(d) => Some((r, d)),
                _ => None,
            }
        })
        .collect()
}

/// How many samples a profile carries.
///
/// 121 over the sampled range is ~2.4 percentage points of lever per step, which
/// is too coarse to quote a peak from directly — a caller wanting one should fit a
/// parabola through the three samples around the maximum. That is exact for a
/// quadratic response and O(h^3) for anything else smooth, and it needs no
/// knowledge of the basis, which is the whole point of reading the curve instead
/// of solving it.
pub const PROFILE_SAMPLES: usize = 121;

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixture's real delivery-spend moments, so these assertions are against
    /// the same numbers the design doc and the explainer quote.
    fn fixture() -> BasisMoments {
        BasisMoments {
            n: 8434.0,
            s1: 1_329_621.46,
            s2: 296_485_131.0,
        }
    }

    #[test]
    fn every_form_maps_to_a_spec_of_the_right_width() {
        assert_eq!(DriverForm::Linear.spec().width(), 1);
        assert_eq!(DriverForm::LogLog.spec().width(), 1);
        assert_eq!(DriverForm::LogLinear.spec().width(), 1);
        assert_eq!(DriverForm::LinearLog.spec().width(), 1);
        // The whole reason the scalar `coefficient:` had to become a vector.
        assert_eq!(DriverForm::Quadratic.spec().width(), 2);
    }

    #[test]
    fn only_log_over_a_non_log_basis_fails_to_aggregate() {
        assert!(DriverForm::Linear.spec().aggregates_exactly());
        assert!(DriverForm::LogLog.spec().aggregates_exactly());
        assert!(DriverForm::LinearLog.spec().aggregates_exactly());
        assert!(DriverForm::Quadratic.spec().aggregates_exactly());
        assert!(!DriverForm::LogLinear.spec().aggregates_exactly());
    }

    // A linear response needs no levels at all, which is what keeps delta-only
    // mode working: r*s1 is exactly beta*dX.
    #[test]
    fn a_linear_response_is_the_coefficient_times_the_move() {
        let m = fixture();
        let r = 0.10;
        let d = aggregate_delta(&DriverForm::Linear.spec(), &[2.5], &m, r, None, None);
        assert_eq!(d, ResponseDelta::Sized(2.5 * r * m.s1));
    }

    // The exact power law, not the first-order stand-in it replaces. Both are
    // quoted in internal-docs/driver-response-standardization.md.
    #[test]
    fn a_log_log_response_is_exact_where_the_old_arithmetic_was_first_order() {
        let m = fixture();
        let y = 288_557.0;
        let beta = 0.4508026316126415;
        let r = 0.50;
        let d = aggregate_delta(&DriverForm::LogLog.spec(), &[beta], &m, r, Some(y), None);
        let exact = y * ((1.0f64 + r).powf(beta) - 1.0);
        match d {
            ResponseDelta::Sized(v) => {
                assert!((v - exact).abs() < 1e-6, "{v} vs {exact}");
                let first_order = y * beta * r;
                assert!(
                    (first_order - exact).abs() / exact > 0.12,
                    "the first-order form should be >12% out at +50%, or this test \
                     is not guarding anything"
                );
            }
            other => panic!("expected Sized, got {other:?}"),
        }
    }

    // The `n` the hand-written linear-log arithmetic dropped. Without it the
    // answer is out by a factor of 8,434 on this fixture.
    #[test]
    fn a_linear_log_response_counts_every_row() {
        let m = fixture();
        let r = 0.10;
        let d = aggregate_delta(&DriverForm::LinearLog.spec(), &[500.0], &m, r, None, None);
        let expected = 500.0 * m.n * (1.0f64 + r).ln();
        match d {
            ResponseDelta::Sized(v) => assert!((v - expected).abs() < 1e-9),
            other => panic!("expected Sized, got {other:?}"),
        }
    }

    // The headline: a quadratic helps, helps less, then hurts — with no special
    // case anywhere. Figures match the explainer's chart.
    #[test]
    fn a_quadratic_response_helps_then_saturates_then_hurts() {
        let m = fixture();
        let spec = DriverForm::Quadratic.spec();
        let coeffs = [0.8, -0.0015];
        let at = |r: f64| match aggregate_delta(&spec, &coeffs, &m, r, None, None) {
            ResponseDelta::Sized(v) => v,
            other => panic!("expected Sized at r={r}, got {other:?}"),
        };
        let (a, b, c, d) = (at(0.05), at(0.10), at(0.20), at(0.50));
        assert!(a > 0.0 && b > a, "still climbing early");
        assert!(b - a > c - b, "the gain per step must shrink");
        assert!(d < 0.0, "past the crossing it must actually hurt, got {d}");
        // Beyond break-even the lever destroys value: -24,061 at +50%.
        assert!((d - -24_061.0).abs() < 1.0, "{d}");
    }

    // Exactness is the claim that makes the moments worth carrying, so it is
    // asserted against a brute-force shift of every row rather than a formula.
    #[test]
    fn the_moment_form_matches_shifting_every_row_individually() {
        // A spread of rows whose square-sum is nothing like the square of the sum.
        let xs: Vec<f64> = (1..=500).map(|i| 40.0 + (i as f64) * 0.7).collect();
        let m = BasisMoments::from_values(&xs);
        let (b1, b2) = (0.8, -0.0015);
        let r = 0.10;
        let f = |x: f64| b1 * x + b2 * x * x;
        let truth: f64 = xs.iter().map(|x| f(x * (1.0 + r))).sum::<f64>()
            - xs.iter().map(|x| f(*x)).sum::<f64>();
        match aggregate_delta(&DriverForm::Quadratic.spec(), &[b1, b2], &m, r, None, None) {
            ResponseDelta::Sized(v) => {
                assert!(
                    (v - truth).abs() / truth.abs() < 1e-12,
                    "moment form {v} vs brute force {truth}"
                );
            }
            other => panic!("expected Sized, got {other:?}"),
        }
        // And the mistake the moments exist to prevent: the same curvature applied
        // to the total instead of the rows. Here it is 12,228x out.
        let naive = b1 * (m.s1 * r) + b2 * ((m.s1 * (1.0 + r)).powi(2) - m.s1.powi(2));
        assert!(
            (naive / truth).abs() > 100.0,
            "the naive aggregate should be orders out, got {naive} vs {truth}"
        );
    }

    /// The same error on the project's real fixture, where it is worse than a
    /// magnitude problem: it reports a lever that makes money as one that loses
    /// it. Asserted separately from the brute-force test above because the sign
    /// flip depends on the actual spread of rows — on a tighter synthetic set the
    /// naive answer is merely thousands of times too large, which is a weaker
    /// thing to promise.
    #[test]
    fn on_the_real_fixture_the_naive_aggregate_flips_the_sign() {
        let m = fixture();
        let (b1, b2, r) = (0.8, -0.0015, 0.10);
        let correct =
            match aggregate_delta(&DriverForm::Quadratic.spec(), &[b1, b2], &m, r, None, None) {
                ResponseDelta::Sized(v) => v,
                other => panic!("expected Sized, got {other:?}"),
            };
        let naive = b1 * (m.s1 * r) + b2 * ((m.s1 * (1.0 + r)).powi(2) - m.s1.powi(2));
        assert!(correct > 0.0, "the lever gains here: {correct}");
        assert!(naive < 0.0, "the naive answer claims a loss: {naive}");
        assert!(
            (naive / correct).abs() > 40_000.0,
            "documented at 42,905x, got {:.0}x",
            (naive / correct).abs()
        );
    }

    #[test]
    fn a_log_link_without_a_target_says_so() {
        let m = fixture();
        assert_eq!(
            aggregate_delta(&DriverForm::LogLog.spec(), &[0.45], &m, 0.1, None, None),
            ResponseDelta::NeedsTarget
        );
        // Zero is as unusable as absent: it is what the proportion scales.
        assert_eq!(
            aggregate_delta(
                &DriverForm::LogLog.spec(),
                &[0.45],
                &m,
                0.1,
                Some(0.0),
                None
            ),
            ResponseDelta::NeedsTarget
        );
    }

    #[test]
    fn log_linear_is_reported_as_approximate_not_exact() {
        let m = fixture();
        match aggregate_delta(
            &DriverForm::LogLinear.spec(),
            &[0.001],
            &m,
            0.1,
            Some(1000.0),
            None,
        ) {
            ResponseDelta::Approximate(_) => {}
            other => panic!("log-linear has no exact aggregate form; got {other:?}"),
        }
    }

    #[test]
    fn a_coefficient_vector_of_the_wrong_width_is_refused() {
        let m = fixture();
        // A single number cannot describe a turning point, and picking one of the
        // two terms would be a silent misapplication.
        assert_eq!(
            aggregate_delta(&DriverForm::Quadratic.spec(), &[0.8], &m, 0.1, None, None),
            ResponseDelta::Undefined
        );
    }

    #[test]
    fn a_shift_past_the_observed_spread_is_refused() {
        let m = fixture();
        let spec = DriverForm::Quadratic.spec();
        // Observed $40..$400 is a 10x spread, so +200% is inside the backstop and
        // +2000% is not.
        assert!(matches!(
            aggregate_delta(&spec, &[0.8, -0.0015], &m, 2.0, None, Some((40.0, 400.0))),
            ResponseDelta::Sized(_)
        ));
        assert_eq!(
            aggregate_delta(&spec, &[0.8, -0.0015], &m, 20.0, None, Some((40.0, 400.0))),
            ResponseDelta::OutsideDomain
        );
    }

    #[test]
    fn a_cut_past_zero_is_undefined_not_zero() {
        let m = fixture();
        assert_eq!(
            aggregate_delta(
                &DriverForm::LinearLog.spec(),
                &[500.0],
                &m,
                -1.5,
                None,
                None
            ),
            ResponseDelta::Undefined
        );
    }

    /// Read behaviour off a profile the way a caller has to: find the largest
    /// sample and the sign change. No basis-specific arithmetic anywhere.
    fn peak_and_crossing(profile: &[(f64, f64)]) -> (Option<f64>, Option<f64>) {
        let peak = profile
            .iter()
            .filter(|(r, _)| *r > 0.0)
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(r, _)| *r);
        let crossing = profile
            .windows(2)
            .find(|w| w[0].0 > 0.0 && w[0].1 > 0.0 && w[1].1 <= 0.0)
            .map(|w| w[1].0);
        (peak, crossing)
    }

    // The generic replacement for a vertex solver. Same answer on a quadratic,
    // and it works without knowing the basis: +36% peak, break-even near +73%.
    #[test]
    fn a_profile_locates_a_turn_without_knowing_the_shape() {
        let m = fixture();
        let p = response_profile(
            &DriverForm::Quadratic.spec(),
            &[0.8, -0.0015],
            &m,
            None,
            None,
            PROFILE_SAMPLES,
        );
        let (peak, crossing) = peak_and_crossing(&p);
        assert!((peak.expect("a peak") - 0.20).abs() < 0.06, "{peak:?}");
        assert!(
            (crossing.expect("a crossing") - 0.39).abs() < 0.06,
            "{crossing:?}"
        );
    }

    // A shape that only ever rises has no interior peak and no crossing, and the
    // caller learns that from the samples rather than from a form name.
    #[test]
    fn a_profile_of_a_monotone_shape_reports_no_turn() {
        let m = fixture();
        for (form, coeffs) in [
            (DriverForm::Linear, vec![2.5]),
            (DriverForm::Quadratic, vec![0.8, 0.0015]), // curvature agrees with slope
        ] {
            let p = response_profile(&form.spec(), &coeffs, &m, None, None, PROFILE_SAMPLES);
            let (_, crossing) = peak_and_crossing(&p);
            assert!(
                crossing.is_none(),
                "{form} should never cross zero going up"
            );
            // Its "peak" is just the largest lever sampled — the boundary, not a
            // turn. A caller distinguishes the two by whether it sits at the edge.
            let last = p.last().unwrap().0;
            let (peak, _) = peak_and_crossing(&p);
            assert!((peak.unwrap() - last).abs() < 1e-9, "peak is the boundary");
        }
    }

    // A log link needs the target, so a profile without one is empty rather than
    // silently linear.
    #[test]
    fn a_profile_needs_what_its_link_needs() {
        let m = fixture();
        let spec = DriverForm::LogLog.spec();
        assert!(response_profile(&spec, &[0.45], &m, None, None, PROFILE_SAMPLES).is_empty());
        let p = response_profile(&spec, &[0.45], &m, Some(288_557.0), None, PROFILE_SAMPLES);
        assert!(!p.is_empty());
        // Saturating: rising throughout, with a shrinking step.
        let ups: Vec<f64> = p
            .iter()
            .filter(|(r, _)| *r >= 0.0)
            .map(|(_, d)| *d)
            .collect();
        assert!(ups.windows(2).all(|w| w[1] >= w[0]), "monotone up");
        let d1 = ups[1] - ups[0];
        let dn = ups[ups.len() - 1] - ups[ups.len() - 2];
        assert!(dn < d1, "each step buys less: {d1} then {dn}");
    }

    // The samples stay inside the evidence: an observed 6.6x spread must not
    // produce a profile point at +2000%.
    #[test]
    fn a_profile_never_extrapolates_past_the_observed_spread() {
        let m = fixture();
        let p = response_profile(
            &DriverForm::Quadratic.spec(),
            &[0.8, -0.0015],
            &m,
            None,
            Some((6.0, 39.6)),
            PROFILE_SAMPLES,
        );
        let spread = 39.6 / 6.0;
        assert!(p.iter().all(|(r, _)| 1.0 + r <= spread + 1e-9));
        assert!(p.iter().all(|(r, _)| 1.0 + r >= 1.0 / spread - 1e-9));
    }

    #[test]
    fn a_log_basis_drops_a_non_positive_row_rather_than_inventing_one() {
        assert_eq!(BasisTerm::Log.apply(0.0), None);
        assert_eq!(BasisTerm::Log.apply(-5.0), None);
        assert_eq!(BasisTerm::Identity.apply(0.0), Some(0.0));
        assert_eq!(BasisTerm::Square.apply(-3.0), Some(9.0));
    }
}
