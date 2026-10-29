//! Synthetic control for a campaign whose units were CHOSEN, not drawn: each
//! treated unit against a weighted combination of untouched donors, with
//! placebo-in-space inference.
//!
//! - **Estimate**: one augmented synthetic control per treated unit
//!   (`synthetic_fit`), reported as the unweighted mean of their post-window
//!   gaps — the same estimand as the waves path, per treated unit-day.
//! - **Test**: every donor is refitted as if it had been treated, against the
//!   rest of the pool. The statistic is the treated set's mean gap over its
//!   mean holdout pre-window error; its null is the same statistic over every k-subset of
//!   donors (exact below `ENUMERATE_BELOW`, else `PERMUTATIONS` seeded draws).
//! - **Interval**: the constant effects that test does not reject. Subtracting
//!   τ from the treated post window moves every treated gap by exactly τ and
//!   touches neither the weights (fitted on the pre window) nor any placebo, so
//!   the inversion is closed-form: `estimate ± c · mean holdout error`.
//!
//! Nothing here is randomised, so the result is never a registered random
//! experiment's: it assumes no spillover into donors and donors untouched by
//! the change, and the host says so beside every number.

use crate::engine::experiment::dropping::DroppedUnit;
use crate::engine::experiment::estimate::{Decision, EffectResult};
use crate::engine::experiment::permutation::{
    at_least, choose, min_attainable_p, ENUMERATE_BELOW, PERMUTATIONS,
};
use crate::engine::experiment::synthetic_fit::{fit, Fit, Pools};
use crate::engine::experiment::{
    fmt_p, per_comparison_alpha, validate_rates, windows_around, PanelMatrix, SplitMix64,
};
use std::collections::BTreeSet;

pub const SYNTHETIC_ESTIMAND: &str =
    "mean effect per treated unit-day against its synthetic control, unweighted across treated units";

/// Fewer donors than this and no pre-fit is worth trusting, whatever the
/// attainable p says: the pool cannot span the treated unit's movements.
pub const MIN_DONORS: usize = 10;

/// Fewer retained pre days than this leave the fit, or the days it holds out
/// to judge itself, too short to mean anything; refused up front rather than
/// blamed on the donors.
pub const MIN_PRE_DAYS: usize = 8;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SyntheticSpec {
    /// The units the campaign runs in, by name.
    pub treated: Vec<String>,
    /// Day ordinal the lever went on.
    pub start_on: i64,
    /// The donor pool by name; `None` = every panel unit neither treated nor excluded.
    pub donors: Option<Vec<String>>,
    /// Units kept out of the pool (spillover risk). Applied to an explicit pool too.
    pub exclude: Vec<String>,
    pub pre_days: usize,
    pub post_days: usize,
    pub anticipation_days: usize,
    pub washout_days: usize,
    pub coverage_floor: f64,
    pub alpha: f64,
    pub family: usize,
    /// Refusal threshold on each treated unit's pre-fit ratio — its holdout
    /// pre-window error (the augmented fit's, as the post window is predicted)
    /// over its own pre-window volatility, reported as `UnitFit::pre_fit`. Set
    /// at pricing and frozen at registration (`price_synthetic` proposes it).
    pub max_pre_fit: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DonorWeight {
    pub unit: String,
    pub weight: f64,
}

/// One day of the treated-versus-synthetic chart, in levels.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SeriesPoint {
    pub day: i64,
    pub actual: f64,
    pub synthetic: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UnitFit {
    pub unit: String,
    pub effect: f64,
    /// In-sample pre-window error of the weights alone; for display and the
    /// RMSPE ratio. The threshold and the test read `pre_fit` instead.
    pub pre_rmspe: f64,
    /// Holdout pre-window error over the unit's own pre-window volatility: 0 is a perfect
    /// track, 1 is no better than a flat line at the unit's mean.
    pub pre_fit: f64,
    pub post_rmspe: f64,
    /// Abadie's post/pre RMSPE ratio and its rank among the donors' placebos,
    /// as a p-value. Per unit, for display; the decision uses the set test.
    pub rmspe_ratio: f64,
    pub rank_p: f64,
    /// Non-zero weights only, largest first.
    pub weights: Vec<DonorWeight>,
    /// Pre window then post window; the washout is not drawn.
    pub series: Vec<SeriesPoint>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SyntheticResult {
    pub effect: EffectResult,
    pub fits: Vec<UnitFit>,
    pub donors: Vec<String>,
    /// Placebo sets behind the p-value, and whether they were all of them.
    pub placebo_sets: usize,
    pub exact: bool,
}

pub(crate) fn refused_synthetic(reason: impl Into<String>) -> EffectResult {
    let mut r = EffectResult::refused(reason);
    r.estimand = SYNTHETIC_ESTIMAND;
    r
}

/// Rows the spec reads: the treated units and the resolved pool. `fit_core`
/// moves a donor missing a day of its windows from `donors` to `dropped`.
pub(crate) struct Resolved {
    pub(crate) treated: Vec<usize>,
    pub(crate) donors: Vec<usize>,
    pub(crate) dropped: Vec<usize>,
}

/// The pool is large enough to fit on and to draw a placebo set from.
fn check_pool(treated: usize, donors: usize, left: &str) -> Result<(), String> {
    if donors < MIN_DONORS {
        return Err(format!(
            "only {donors} donor units are left once {left} are removed; a synthetic control \
             needs at least {MIN_DONORS}"
        ));
    }
    if treated > donors {
        return Err(format!(
            "{treated} treated units but only {donors} donors; the placebo test draws as many \
             donors as there are treated units"
        ));
    }
    Ok(())
}

fn row(m: &PanelMatrix, name: &str, role: &str) -> Result<usize, String> {
    m.unit_index(name)
        .ok_or_else(|| format!("the {role} unit '{name}' is not in the panel"))
}

/// Names checked, roles disjoint, pool large enough. Shared by the estimate,
/// the ratio's panel check and pricing.
pub(crate) fn resolve(m: &PanelMatrix, s: &SyntheticSpec) -> Result<Resolved, String> {
    validate_rates(s.alpha, s.coverage_floor)?;
    if !(s.max_pre_fit.is_finite() && s.max_pre_fit > 0.0) {
        return Err(format!(
            "max_pre_fit must be a finite number above 0; got {}",
            s.max_pre_fit
        ));
    }
    if s.treated.is_empty() {
        return Err("no unit was treated, so there is nothing to estimate".into());
    }
    let treated: BTreeSet<usize> = s
        .treated
        .iter()
        .map(|n| row(m, n, "treated"))
        .collect::<Result<_, _>>()?;
    let excluded: BTreeSet<usize> = s
        .exclude
        .iter()
        .map(|n| row(m, n, "excluded"))
        .collect::<Result<_, _>>()?;
    let pool: Vec<usize> = match &s.donors {
        Some(names) => {
            let mut rows = Vec::new();
            for n in names {
                let r = row(m, n, "donor")?;
                if treated.contains(&r) {
                    return Err(format!("'{n}' is named both treated and a donor"));
                }
                rows.push(r);
            }
            rows.sort_unstable();
            rows.dedup();
            rows
        }
        None => (0..m.n_units()).filter(|u| !treated.contains(u)).collect(),
    };
    let donors: Vec<usize> = pool.into_iter().filter(|u| !excluded.contains(u)).collect();
    check_pool(
        treated.len(),
        donors.len(),
        "the treated and excluded units",
    )?;
    Ok(Resolved {
        treated: treated.into_iter().collect(),
        donors,
        dropped: Vec::new(),
    })
}

/// Every fit the test reads: the treated units' and each donor's placebo.
pub(crate) struct Core {
    pub(crate) treated: Vec<Fit>,
    pub(crate) placebos: Vec<Fit>,
    pub(crate) pre: (usize, usize),
    pub(crate) post: (usize, usize),
}

pub(crate) fn fit_core(
    m: &PanelMatrix,
    s: &SyntheticSpec,
    r: &mut Resolved,
) -> Result<Core, String> {
    let w = windows_around(
        m,
        s.start_on,
        s.pre_days,
        s.post_days,
        s.anticipation_days,
        s.washout_days,
        s.coverage_floor,
    )
    .ok_or_else(|| {
        format!(
            "the {}-day pre window and {}-day post window around the start do not fit inside \
             the data at the registered coverage floor {}",
            s.pre_days, s.post_days, s.coverage_floor
        )
    })?;
    let pre = m.ordinal_range(w.pre.0, w.pre.1);
    let post = m.ordinal_range(w.post.0, w.post.1);
    if pre.1 - pre.0 < MIN_PRE_DAYS {
        return Err(format!(
            "the pre window keeps {} days at the registered coverage floor; a synthetic control \
             needs at least {MIN_PRE_DAYS} to fit on and hold some out to judge the fit",
            pre.1 - pre.0
        ));
    }
    // A unit missing a day of the windows would carry NaN into the shared
    // Gram, and so into every fit: a treated one is refused by name, a donor
    // leaves the pool (and is listed), as the waves path leaves out a thin unit.
    let complete = |u: usize| {
        (pre.0..pre.1)
            .chain(post.0..post.1)
            .all(|d| m.get(u, d).is_finite())
    };
    if let Some(u) = r.treated.iter().find(|u| !complete(**u)) {
        return Err(format!(
            "the treated unit '{}' holds a missing or non-numeric value in the pre or post \
             window, so its gap to the synthetic is undefined",
            m.units[*u]
        ));
    }
    let (kept, dropped): (Vec<usize>, Vec<usize>) = r.donors.iter().partition(|u| complete(**u));
    if !dropped.is_empty() {
        check_pool(
            r.treated.len(),
            kept.len(),
            "the treated, excluded and incomplete units",
        )?;
        r.donors = kept;
        r.dropped = dropped;
    }
    // Pool rows: the treated units first, then the donors.
    let k = r.treated.len();
    let rows: Vec<usize> = r.treated.iter().chain(&r.donors).copied().collect();
    let pools = Pools::new(m, &rows, pre, post);
    let donors: Vec<usize> = (k..rows.len()).collect();
    let treated = (0..k).map(|t| fit(&pools, t, &donors)).collect();
    let placebos = donors
        .iter()
        .map(|d| {
            let rest: Vec<usize> = donors.iter().copied().filter(|x| x != d).collect();
            fit(&pools, *d, &rest)
        })
        .collect();
    Ok(Core {
        treated,
        placebos,
        pre,
        post,
    })
}

fn mean(v: impl Iterator<Item = f64>) -> f64 {
    let (mut s, mut n) = (0.0, 0usize);
    for x in v {
        s += x;
        n += 1;
    }
    s / n as f64
}

fn binomial(n: usize, k: usize) -> f64 {
    (0..k).fold(1.0, |acc, i| acc * (n - i) as f64 / (i + 1) as f64)
}

/// The null: `|mean gap / mean pre error|` over every k-subset of placebos, or
/// `PERMUTATIONS` seeded subsets once there are more than `ENUMERATE_BELOW`.
pub(crate) fn placebo_null(placebos: &[Fit], k: usize, seed: u64) -> (Vec<f64>, bool) {
    let stat = |set: &[usize]| {
        let e = mean(set.iter().map(|i| placebos[*i].effect));
        let s = mean(set.iter().map(|i| placebos[*i].holdout_rmspe));
        (e / s).abs()
    };
    let j = placebos.len();
    if binomial(j, k) <= ENUMERATE_BELOW {
        let mut out = Vec::new();
        let all: Vec<usize> = (0..j).collect();
        choose(&all, k, 0, &mut Vec::new(), &mut |set| out.push(stat(set)));
        return (out, true);
    }
    let mut rng = SplitMix64::new(seed);
    let mut all: Vec<usize> = (0..j).collect();
    let out = (0..PERMUTATIONS)
        .map(|_| {
            rng.partial_shuffle(&mut all, k);
            stat(&all[..k])
        })
        .collect();
    (out, false)
}

/// The test and its closed-form inversion over a fitted core.
pub(crate) struct Tested {
    pub(crate) estimate: f64,
    pub(crate) scale: f64,
    pub(crate) critical: f64,
    pub(crate) p_value: f64,
    pub(crate) significant: bool,
    pub(crate) sets: usize,
    pub(crate) exact: bool,
}

pub(crate) fn test_core(core: &Core, s: &SyntheticSpec, seed: u64) -> Result<Tested, String> {
    let k = core.treated.len();
    let alpha = per_comparison_alpha(s.alpha, s.family);
    let distinct = binomial(core.placebos.len(), k);
    let floor = min_attainable_p(1.0 / (distinct + 1.0), distinct);
    if floor > alpha {
        return Err(format!(
            "{} donors give {} placebo sets for {k} treated, so the smallest attainable p is {}, \
             above the per-comparison alpha {}; add donors or register fewer outcomes",
            core.placebos.len(),
            distinct,
            fmt_p(floor),
            fmt_p(alpha)
        ));
    }
    let estimate = mean(core.treated.iter().map(|f| f.effect));
    let scale = mean(core.treated.iter().map(|f| f.holdout_rmspe));
    if !(scale.is_finite() && scale > 0.0) {
        return Err("the treated units' pre-window fit error is zero or undefined".into());
    }
    if !estimate.is_finite() {
        return Err(
            "a treated unit's post window holds a missing or non-numeric value, so its gap \
             to the synthetic is undefined"
                .into(),
        );
    }
    let (null, exact) = placebo_null(&core.placebos, k, seed);
    // A set with no finite statistic (a placebo the pool tracks with no
    // holdout error, or a NaN gap) stays in the denominator as extreme, as the
    // waves test keeps such draws: that can only raise p.
    let mut null: Vec<f64> = null
        .into_iter()
        .map(|v| if v.is_finite() { v } else { f64::INFINITY })
        .collect();
    let n = null.len();
    if n == 0 {
        return Err("no placebo set produced a usable statistic".into());
    }
    let observed = (estimate / scale).abs();
    let extreme = null.iter().filter(|v| at_least(**v, observed)).count();
    let p_value = (1 + extreme) as f64 / (n + 1) as f64;
    null.sort_by(|a, b| b.total_cmp(a));
    // p <= alpha iff at most m − 1 placebos reach the statistic, i.e. iff it
    // exceeds the m-th largest: that threshold IS the interval's half-width
    // scale, and `significant` is read off it so the interval excludes zero
    // exactly when the test rejects.
    let m = ((alpha * (n + 1) as f64) + 1e-9).floor() as usize;
    let critical = if m == 0 {
        f64::INFINITY
    } else {
        null[(m - 1).min(n - 1)]
    };
    let significant = m > 0 && !at_least(critical, observed);
    Ok(Tested {
        estimate,
        scale,
        critical,
        p_value,
        significant,
        sets: n,
        exact,
    })
}

/// Treated units whose pre fit is worse than the frozen threshold, by name.
fn pre_fit_refusal(
    m: &PanelMatrix,
    r: &Resolved,
    core: &Core,
    s: &SyntheticSpec,
) -> Option<String> {
    let bad: Vec<String> = r
        .treated
        .iter()
        .zip(&core.treated)
        .filter(|(_, f)| f.pre_fit().is_nan() || f.pre_fit() > s.max_pre_fit)
        .map(|(u, f)| format!("{} ({:.2})", m.units[*u], f.pre_fit()))
        .collect();
    (!bad.is_empty()).then(|| {
        format!(
            "no combination of donors tracks {} closely enough before the start: pre-fit error \
             over the unit's own volatility exceeds the registered {:.2}, so the estimate would \
             be the fit's error",
            bad.join(", "),
            s.max_pre_fit
        )
    })
}

fn unit_fit(m: &PanelMatrix, unit: usize, donors: &[usize], f: &Fit, core: &Core) -> UnitFit {
    let ratio = |x: &Fit| x.post_rmspe / x.pre_rmspe;
    let r = ratio(f);
    let above = core
        .placebos
        .iter()
        .filter(|p| at_least(ratio(p), r))
        .count();
    let mut weights: Vec<DonorWeight> = donors
        .iter()
        .zip(&f.weights)
        .filter(|(_, w)| **w > 1e-6)
        .map(|(d, w)| DonorWeight {
            unit: m.units[*d].clone(),
            weight: *w,
        })
        .collect();
    weights.sort_by(|a, b| b.weight.total_cmp(&a.weight));
    let window = |(lo, hi): (usize, usize), synth: &[f64]| -> Vec<SeriesPoint> {
        (lo..hi)
            .zip(synth)
            .map(|(d, s)| SeriesPoint {
                day: m.days[d],
                actual: m.get(unit, d),
                synthetic: *s,
            })
            .collect()
    };
    let mut series = window(core.pre, &f.synthetic_pre);
    series.extend(window(core.post, &f.synthetic_post));
    UnitFit {
        unit: m.units[unit].clone(),
        effect: f.effect,
        pre_rmspe: f.pre_rmspe,
        pre_fit: f.pre_fit(),
        post_rmspe: f.post_rmspe,
        rmspe_ratio: r,
        rank_p: (1 + above) as f64 / (core.placebos.len() + 1) as f64,
        weights,
        series,
    }
}

fn refused_result(reason: String) -> SyntheticResult {
    SyntheticResult {
        effect: refused_synthetic(reason),
        fits: Vec::new(),
        donors: Vec::new(),
        placebo_sets: 0,
        exact: false,
    }
}

pub fn estimate_synthetic(m: &PanelMatrix, s: &SyntheticSpec, seed: u64) -> SyntheticResult {
    let mut r = match resolve(m, s) {
        Ok(r) => r,
        Err(e) => return refused_result(e),
    };
    let core = match fit_core(m, s, &mut r) {
        Ok(c) => c,
        Err(e) => return refused_result(e),
    };
    if let Some(e) = pre_fit_refusal(m, &r, &core, s) {
        return refused_result(e);
    }
    let t = match test_core(&core, s, seed) {
        Ok(t) => t,
        Err(e) => return refused_result(e),
    };
    let half = t.critical * t.scale;
    let fits = r
        .treated
        .iter()
        .zip(&core.treated)
        .map(|(u, f)| unit_fit(m, *u, &r.donors, f, &core))
        .collect();
    SyntheticResult {
        effect: EffectResult {
            estimate: t.estimate,
            // A randomisation test has neither; t_stat is the test's own statistic.
            se: f64::NAN,
            t_stat: t.estimate / t.scale,
            df: f64::NAN,
            ci_low: t.estimate - half,
            ci_high: t.estimate + half,
            p_value: t.p_value,
            n_treated: r.treated.len(),
            n_control: r.donors.len(),
            significant: t.significant,
            estimand: SYNTHETIC_ESTIMAND,
            design: "synthetic control",
            dropped_waves: Vec::new(),
            dropped_units: r
                .dropped
                .iter()
                .map(|d| DroppedUnit {
                    unit: m.units[*d].clone(),
                    reason: "missing a day of the pre or post window; left out of the donor pool"
                        .into(),
                })
                .collect(),
            waves: Vec::new(),
            pre_trend: None,
            size_bias: None,
            retained_on_days: None,
            refusal: None,
        },
        fits,
        donors: r.donors.iter().map(|d| m.units[*d].clone()).collect(),
        placebo_sets: t.sets,
        exact: t.exact,
    }
}

/// Exactly the decision `estimate_synthetic` reports, without building the
/// per-unit series and weights the ratio's many probes never read.
pub(crate) fn decide_synthetic(m: &PanelMatrix, s: &SyntheticSpec, seed: u64) -> Decision {
    let tested = resolve(m, s).and_then(|mut r| {
        let core = fit_core(m, s, &mut r)?;
        if let Some(e) = pre_fit_refusal(m, &r, &core, s) {
            return Err(e);
        }
        test_core(&core, s, seed)
    });
    match tested {
        Err(reason) => Decision::Refused(reason),
        Ok(t) => Decision::Tested {
            significant: t.significant,
            p_value: t.p_value,
            estimate: t.estimate,
        },
    }
}

/// The same spec with the pre-fit gate open. The ratio's first stage and its
/// Anderson–Rubin probes read other series than the target (the driver, and
/// `target − β·driver`), which fit their donors differently for reasons that
/// say nothing about identification; the gate belongs to the target's own
/// estimate, which keeps it. A unit flat over the pre window is still refused.
pub(crate) fn without_pre_fit_gate(s: &SyntheticSpec) -> SyntheticSpec {
    SyntheticSpec {
        max_pre_fit: f64::MAX,
        ..s.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::design::{estimate, ExperimentDesign};
    use crate::engine::experiment::power_synthetic::{price_synthetic, PRE_FIT_CEILING};
    use crate::engine::experiment::ratio::estimate_ratio;
    use crate::engine::experiment::testkit::uniform;

    /// `donors` units on two shared factors plus noise, and `treated` units
    /// whose loadings are a mix of the first three donors' — at a level far
    /// above every donor, so only the demeaned fit can track them. Treated
    /// units gain `effect` from `start` on. Days `1..=days`.
    fn geo_panel(
        donors: usize,
        treated: usize,
        days: i64,
        start: i64,
        effect: f64,
        seed: u64,
    ) -> PanelMatrix {
        let mut rng = SplitMix64::new(seed);
        let f1 = |t: i64| (t as f64 * 0.21).sin() * 30.0;
        let f2 = |t: i64| [0.0, 4.0, 9.0, 7.0, 3.0, -8.0, -15.0][(t % 7) as usize] * 3.0;
        let loads: Vec<(f64, f64)> = (0..donors)
            .map(|_| (0.5 + uniform(&mut rng, 1.0), 0.5 + uniform(&mut rng, 1.0)))
            .collect();
        let mut rows = Vec::new();
        for (u, (a, b)) in loads.iter().enumerate() {
            for t in 1..=days {
                let v = 100.0 + 40.0 * u as f64 + a * f1(t) + b * f2(t) + uniform(&mut rng, 6.0);
                rows.push((format!("d{u:02}"), t, v));
            }
        }
        let (a, b) = loads[..3]
            .iter()
            .fold((0.0, 0.0), |(x, y), (p, q)| (x + p / 3.0, y + q / 3.0));
        for i in 0..treated {
            for t in 1..=days {
                let lift = if t >= start { effect } else { 0.0 };
                let v = 5000.0 + a * f1(t) + b * f2(t) + uniform(&mut rng, 6.0) + lift;
                rows.push((format!("t{i}"), t, v));
            }
        }
        PanelMatrix::from_triples(rows)
    }

    fn spec(treated: &[&str], start: i64) -> SyntheticSpec {
        SyntheticSpec {
            treated: treated.iter().map(|s| s.to_string()).collect(),
            start_on: start,
            donors: None,
            exclude: Vec::new(),
            pre_days: 56,
            post_days: 21,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.1,
            family: 1,
            max_pre_fit: 0.5,
        }
    }

    #[test]
    fn experiment_synthetic_recovers_a_planted_effect_off_the_donors_hull() {
        let m = geo_panel(20, 2, 120, 90, 25.0, 4);
        let r = estimate_synthetic(&m, &spec(&["t0", "t1"], 90), 1);
        let e = &r.effect;
        assert!(e.refusal.is_none(), "{:?}", e.refusal);
        assert!((e.estimate - 25.0).abs() < 5.0, "estimate {}", e.estimate);
        assert!(
            e.ci_low < 25.0 && 25.0 < e.ci_high,
            "[{}, {}]",
            e.ci_low,
            e.ci_high
        );
        assert!(e.significant && e.ci_low > 0.0, "p {}", e.p_value);
        assert_eq!(
            (e.n_treated, e.n_control, e.design),
            (2, 20, "synthetic control")
        );
        assert!(
            r.exact && r.placebo_sets == 190,
            "C(20, 2) sets, got {}",
            r.placebo_sets
        );
        let f = &r.fits[0];
        assert!(
            f.pre_fit < 0.3,
            "the demeaned fit tracks a 5000-level unit: {}",
            f.pre_fit
        );
        assert!((f.weights.iter().map(|w| w.weight).sum::<f64>() - 1.0).abs() < 1e-6);
        assert_eq!(f.series.len(), 56 + 21);
        assert!(
            f.series.iter().all(|p| p.synthetic > 4000.0),
            "series are in levels"
        );
    }

    #[test]
    fn experiment_synthetic_null_is_not_significant_and_its_interval_matches_its_test() {
        let mut rejections = 0;
        for seed in 0..10 {
            let m = geo_panel(20, 1, 120, 90, 0.0, 100 + seed);
            let e = estimate_synthetic(&m, &spec(&["t0"], 90), seed).effect;
            assert!(e.refusal.is_none(), "{:?}", e.refusal);
            // The inversion is closed-form, so the interval excludes zero
            // exactly when the test rejects.
            assert_eq!(
                e.significant,
                e.ci_low > 0.0 || e.ci_high < 0.0,
                "seed {seed}"
            );
            rejections += e.significant as usize;
        }
        assert!(
            rejections <= 3,
            "{rejections} of 10 nulls rejected at alpha 0.1"
        );
    }

    #[test]
    fn experiment_synthetic_design_entry_point_is_the_same_estimate() {
        let m = geo_panel(15, 1, 120, 90, 25.0, 9);
        let s = spec(&["t0"], 90);
        let direct = estimate_synthetic(&m, &s, 4).effect;
        let via = estimate(&m, &ExperimentDesign::Synthetic(s.clone()), 4);
        assert_eq!(direct.estimate.to_bits(), via.estimate.to_bits());
        assert_eq!(direct.p_value.to_bits(), via.p_value.to_bits());
        let v = serde_json::to_value(ExperimentDesign::Synthetic(s)).expect("serializes");
        assert!(
            v.get("Synthetic").is_some(),
            "externally tagged like the others: {v}"
        );
    }

    #[test]
    fn experiment_synthetic_refuses_by_name() {
        let m = geo_panel(12, 1, 120, 90, 0.0, 5);
        let refusal = |s: SyntheticSpec| {
            estimate_synthetic(&m, &s, 0)
                .effect
                .refusal
                .unwrap_or_default()
        };
        let mut s = spec(&["nope"], 90);
        assert!(refusal(s.clone()).contains("'nope' is not in the panel"));
        s = spec(&["t0"], 90);
        s.donors = Some(vec!["t0".into()]);
        assert!(refusal(s).contains("both treated and a donor"));
        s = spec(&["t0"], 90);
        s.exclude = vec!["d00".into(), "d01".into(), "d02".into()];
        assert!(
            refusal(s).contains("only 9 donor units"),
            "needs {MIN_DONORS}"
        );
        // 12 donors, one treated: p can reach 1/13 = 0.077, not alpha 0.05.
        s = spec(&["t0"], 90);
        s.alpha = 0.05;
        assert!(refusal(s).contains("smallest attainable p"));
        s = spec(&["t0"], 110);
        assert!(refusal(s).contains("do not fit inside the data"));
        let e = estimate_synthetic(&m, &spec(&["t0"], 90), 0).effect;
        assert!(e.estimate.is_finite() || e.refusal.is_some());
    }

    #[test]
    fn experiment_synthetic_refuses_a_unit_no_donor_mix_tracks() {
        // A treated unit of pure noise shares no factor with the donors.
        let mut rows: Vec<(String, i64, f64)> = Vec::new();
        let m0 = geo_panel(12, 0, 120, 90, 0.0, 6);
        for (u, name) in m0.units().iter().enumerate() {
            for (d, day) in m0.days.iter().enumerate() {
                rows.push((name.clone(), *day, m0.get(u, d)));
            }
        }
        let mut rng = SplitMix64::new(77);
        rows.extend((1..=120).map(|t| ("t0".to_string(), t, 300.0 + uniform(&mut rng, 60.0))));
        let m = PanelMatrix::from_triples(rows);
        let e = estimate_synthetic(&m, &spec(&["t0"], 90), 0).effect;
        let reason = e.refusal.expect("refused");
        assert!(
            reason.contains("no combination of donors tracks t0"),
            "{reason}"
        );
        assert!(e.estimate.is_nan() && !e.significant);
    }

    #[test]
    fn experiment_synthetic_prices_on_past_dates_and_proposes_a_threshold() {
        let m = geo_panel(20, 2, 400, 400, 0.0, 8);
        let p = price_synthetic(&m, &spec(&["t0", "t1"], 400), 0.8, Some(401), 2);
        assert!(p.power.refusal.is_none(), "{:?}", p.power.refusal);
        assert!(
            p.power.mde > 0.0 && p.power.mde < 40.0,
            "mde {}",
            p.power.mde
        );
        assert_eq!(p.power.independent_stretches, 400 / 77);
        assert!(p.pre_fit < p.max_pre_fit && p.max_pre_fit <= PRE_FIT_CEILING);
        // The proposed threshold is registrable: the estimate at it does not
        // refuse for its pre fit on the same panel.
        let mut s = spec(&["t0", "t1"], 300);
        s.max_pre_fit = p.max_pre_fit;
        assert!(estimate_synthetic(&m, &s, 0).effect.refusal.is_none());
        // One span of history is one look: refused.
        let short = geo_panel(20, 2, 100, 100, 0.0, 8);
        let q = price_synthetic(&short, &spec(&["t0"], 100), 0.8, None, 0);
        assert!(q
            .power
            .refusal
            .expect("refused")
            .contains("non-overlapping"));
    }

    /// `m` with every cell passed through `f(unit, day, value)`.
    fn remap(m: &PanelMatrix, f: impl Fn(&str, i64, f64) -> f64) -> PanelMatrix {
        let mut rows = Vec::new();
        for (u, name) in m.units().iter().enumerate() {
            for (d, day) in m.days.iter().enumerate() {
                rows.push((name.clone(), *day, f(name, *day, m.get(u, d))));
            }
        }
        PanelMatrix::from_triples(rows)
    }

    #[test]
    fn experiment_synthetic_refuses_a_missing_post_value_rather_than_rejecting() {
        // NaN only in the post window: the pre fit is fine, so before the
        // check the NaN statistic beat no placebo and read as significant.
        let m = geo_panel(20, 1, 120, 90, 0.0, 11);
        let m = remap(
            &m,
            |u, t, v| if u == "t0" && t == 100 { f64::NAN } else { v },
        );
        let e = estimate_synthetic(&m, &spec(&["t0"], 90), 0).effect;
        let reason = e.refusal.expect("refused");
        assert!(reason.contains("missing or non-numeric"), "{reason}");
        assert!(!e.significant);
        assert!(matches!(
            decide_synthetic(&m, &spec(&["t0"], 90), 0),
            Decision::Refused(_)
        ));
    }

    fn bare_fit(effect: f64, holdout_rmspe: f64) -> Fit {
        Fit {
            weights: Vec::new(),
            effect,
            pre_rmspe: holdout_rmspe,
            pre_sd: 1.0,
            post_rmspe: effect.abs(),
            holdout_rmspe,
            level: 0.0,
            synthetic_pre: Vec::new(),
            synthetic_post: Vec::new(),
        }
    }

    /// Placebos with no holdout error score +inf (a gap) or NaN (no gap): the
    /// most extreme sets there are, or ones with no statistic. Both stay in
    /// the null as extreme, as the waves test keeps them; dropping them read
    /// p = 1/10 here and rejected.
    #[test]
    fn experiment_synthetic_null_counts_unscorable_placebo_sets_as_extreme() {
        let mut placebos: Vec<Fit> = (0..9).map(|_| bare_fit(1.0, 1.0)).collect();
        placebos.extend([bare_fit(5.0, 0.0), bare_fit(5.0, 0.0), bare_fit(0.0, 0.0)]);
        let core = Core {
            treated: vec![bare_fit(10.0, 1.0)],
            placebos,
            pre: (0, 0),
            post: (0, 0),
        };
        let t = test_core(&core, &spec(&["t0"], 90), 0).expect("tested");
        assert_eq!(t.sets, 12);
        assert!((t.p_value - 4.0 / 13.0).abs() < 1e-12, "p {}", t.p_value);
        assert!(!t.significant);
        assert!(t.critical.is_infinite(), "critical {}", t.critical);
    }

    /// One missing cell in a donor's pre window and one in another's post
    /// window: those two donors leave the pool, by name, instead of turning
    /// every fit NaN and blaming the fit (pre) or the treated unit (post).
    #[test]
    fn experiment_synthetic_drops_a_donor_missing_a_window_day() {
        let m = geo_panel(20, 1, 120, 90, 25.0, 4);
        let m = remap(&m, |u, t, v| match (u, t) {
            ("d05", 50) | ("d07", 100) => f64::NAN,
            _ => v,
        });
        let r = estimate_synthetic(&m, &spec(&["t0"], 90), 1);
        assert!(r.effect.refusal.is_none(), "{:?}", r.effect.refusal);
        assert!((r.effect.estimate - 25.0).abs() < 5.0, "{}", r.effect.estimate);
        let dropped: Vec<&str> = r
            .effect
            .dropped_units
            .iter()
            .map(|d| d.unit.as_str())
            .collect();
        assert_eq!(dropped, ["d05", "d07"]);
        assert_eq!((r.donors.len(), r.effect.n_control), (18, 18));
        assert!(!r.donors.iter().any(|d| d == "d05" || d == "d07"));
        assert!(matches!(
            decide_synthetic(&m, &spec(&["t0"], 90), 1),
            Decision::Tested { .. }
        ));
    }

    /// A treated unit missing a pre-window day is refused by name for the
    /// missing value, not for a pre fit no donor could match.
    #[test]
    fn experiment_synthetic_names_a_treated_unit_missing_a_pre_day() {
        let m = geo_panel(20, 1, 120, 90, 0.0, 4);
        let m = remap(&m, |u, t, v| if u == "t0" && t == 50 { f64::NAN } else { v });
        let reason = estimate_synthetic(&m, &spec(&["t0"], 90), 1)
            .effect
            .refusal
            .expect("refused");
        assert!(
            reason.contains("'t0'") && reason.contains("missing or non-numeric"),
            "{reason}"
        );
    }

    /// Pricing reads past windows of the same panel: a donor gap on one day
    /// must cost that donor in the date it falls in, not the whole date.
    #[test]
    fn experiment_synthetic_pricing_survives_a_donor_gap() {
        let clean = geo_panel(20, 1, 400, 400, 0.0, 8);
        let m = remap(&clean, |u, t, v| if u == "d03" && t == 200 { f64::NAN } else { v });
        let s = spec(&["t0"], 400);
        let p = price_synthetic(&m, &s, 0.8, Some(401), 2);
        assert!(p.power.refusal.is_none(), "{:?}", p.power.refusal);
        let all = price_synthetic(&clean, &s, 0.8, Some(401), 2).power.iterations;
        assert_eq!(p.power.iterations, all, "every past date is still priced");
    }

    #[test]
    fn experiment_synthetic_refuses_a_pre_window_too_short_to_judge() {
        let m = geo_panel(20, 1, 120, 90, 0.0, 12);
        let mut s = spec(&["t0"], 90);
        s.pre_days = MIN_PRE_DAYS - 1;
        let reason = estimate_synthetic(&m, &s, 0)
            .effect
            .refusal
            .expect("refused");
        assert!(
            reason.contains(&format!("needs at least {MIN_PRE_DAYS}")),
            "the window is blamed, not the donors: {reason}"
        );
        s.pre_days = MIN_PRE_DAYS;
        let e = estimate_synthetic(&m, &s, 0).effect;
        assert!(
            !e.refusal.unwrap_or_default().contains("needs at least"),
            "{MIN_PRE_DAYS} days is enough to try"
        );
    }

    #[test]
    fn experiment_synthetic_pricing_refuses_a_flat_treated_unit() {
        // Zero sales throughout: 0/0 pre fit. Folding with f64::max read it
        // as a perfect fit and proposed a threshold the run would then refuse.
        let m = geo_panel(20, 1, 400, 400, 0.0, 13);
        let m = remap(&m, |u, _, v| if u == "t0" { 0.0 } else { v });
        let p = price_synthetic(&m, &spec(&["t0"], 400), 0.8, Some(401), 0);
        let reason = p.power.refusal.expect("refused");
        assert!(reason.contains("flat over a past pre window"), "{reason}");
        assert!(p.max_pre_fit.is_nan());
    }

    #[test]
    fn experiment_synthetic_pricing_defaults_to_history_before_the_campaign() {
        // A real lift from day 300 on: priced without `history_to`, no past
        // span may reach into it, so the price is the one cut at the start.
        let m = geo_panel(20, 1, 400, 300, 40.0, 14);
        let mut s = spec(&["t0"], 300);
        s.anticipation_days = 3;
        let default = price_synthetic(&m, &s, 0.8, None, 5);
        let cut = price_synthetic(&m, &s, 0.8, Some(297), 5);
        assert!(
            default.power.refusal.is_none(),
            "{:?}",
            default.power.refusal
        );
        assert_eq!(default.power.mde.to_bits(), cut.power.mde.to_bits());
        assert_eq!(default.max_pre_fit.to_bits(), cut.max_pre_fit.to_bits());
    }

    #[test]
    fn experiment_synthetic_ratio_gates_the_target_only() {
        // The driver is half the target plus noise only t0 carries, so it
        // tracks its donors worse than the target's registered threshold.
        let target = geo_panel(20, 1, 120, 90, 25.0, 15);
        let mut rng = SplitMix64::new(99);
        let noise: Vec<f64> = (0..=120).map(|_| uniform(&mut rng, 60.0)).collect();
        let driver = remap(&target, |u, t, v| {
            if u == "t0" {
                v / 2.0 + noise[t as usize]
            } else {
                v / 2.0
            }
        });
        let mut s = spec(&["t0"], 90);
        s.max_pre_fit = PRE_FIT_CEILING;
        let fit_of = |m: &PanelMatrix| estimate_synthetic(m, &s, 0).fits[0].pre_fit;
        let (t_fit, d_fit) = (fit_of(&target), fit_of(&driver));
        s.max_pre_fit = (t_fit + d_fit) / 2.0;
        assert!(
            t_fit < s.max_pre_fit && s.max_pre_fit < d_fit,
            "fixture: target {t_fit}, driver {d_fit}"
        );
        let r = estimate_ratio(&target, &driver, &ExperimentDesign::Synthetic(s), 0);
        assert!(
            r.first_stage.refusal.is_none(),
            "{:?}",
            r.first_stage.refusal
        );
        assert!(
            r.target_effect.refusal.is_none(),
            "{:?}",
            r.target_effect.refusal
        );
        assert!(r.coefficient.is_finite());
        assert!(
            !r.refusal
                .unwrap_or_default()
                .contains("no combination of donors"),
            "the driver's fit must not refuse the ratio"
        );
    }

    /// Whole simulated campaigns: the 90% interval must cover the planted
    /// effect about 90% of the time, for one treated unit and for two. Scaling
    /// the test by the IN-SAMPLE pre error covered 80% here (an overfit unit
    /// looks precise); the holdout error is what brings it back.
    #[test]
    #[ignore = "slow in a debug build: 200 whole estimates"]
    fn experiment_synthetic_calibration_covers_the_planted_effect() {
        for k in [1usize, 2] {
            let mut covered = 0;
            for seed in 0..100u64 {
                let m = geo_panel(20, k, 120, 90, 25.0, 1000 + seed);
                let mut s = spec(&[], 90);
                s.treated = (0..k).map(|i| format!("t{i}")).collect();
                let e = estimate_synthetic(&m, &s, seed).effect;
                assert!(e.refusal.is_none(), "seed {seed}: {:?}", e.refusal);
                covered += (e.ci_low <= 25.0 && 25.0 <= e.ci_high) as usize;
            }
            assert!(
                covered >= 82,
                "k = {k}: {covered} of 100 intervals covered the effect"
            );
        }
    }
}
