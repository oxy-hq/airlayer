//! The common-switch-date estimator and the public result types.

use crate::engine::experiment::diagnostics::{
    pre_trend, pre_trend_by_wave, size_bias, size_bias_by_wave, PreTrend, SizeBias,
};
use crate::engine::experiment::staggered::{build_waves, estimate_staggered, test_at_zero};
use crate::engine::experiment::{
    collapse, t_quantile, t_two_sided_p, welch, windows_around, PanelMatrix, UnitDelta,
    WelchRefusal, Windows,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

/// Which units were treated and when, keyed by unit NAME. A unit mapped to
/// `None` is a control for the whole run. A panel unit ABSENT from the map is
/// in neither arm and excluded from every computation (see `scope`); a key the
/// panel does not hold is refused.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Assignment {
    pub switch_day: HashMap<String, Option<i64>>,
    /// The blocks the assignment was randomised within; `None` = unblocked.
    /// Read by the staggered path, which permutes only within a stratum; the
    /// common-date path keeps Welch, and every unit `switch_day` names must sit
    /// in exactly one stratum.
    pub strata: Option<Vec<Vec<String>>>,
    pub pre_days: usize,
    pub post_days: usize,
    /// Days before the switch excluded from the baseline (pre-announcement).
    pub anticipation_days: usize,
    /// Days after the switch excluded from the effect window (ramp-up).
    pub washout_days: usize,
    /// Minimum share of a window's calendar days present. Below it the window
    /// is not the length it was registered as — refused, never shortened.
    pub coverage_floor: f64,
    pub alpha: f64,
    /// Outcomes registered in advance. 1 = a single pre-registered hypothesis.
    pub family: usize,
}

pub const ESTIMAND: &str = "mean effect per treated unit-day, unweighted across units";

#[derive(Debug, Clone, serde::Serialize)]
pub struct EffectResult {
    pub estimate: f64,
    pub se: f64,
    pub t_stat: f64,
    pub df: f64,
    /// Either endpoint may be ±inf: an interval the engine cannot bound is
    /// reported as unbounded, never invented. `serde_json` writes it as `null`.
    pub ci_low: f64,
    pub ci_high: f64,
    /// Per-comparison two-sided p: Welch on the common-date path, the
    /// permutation p on the staggered path. Not adjusted for `family`.
    pub p_value: f64,
    pub n_treated: usize,
    pub n_control: usize,
    pub significant: bool,
    pub estimand: &'static str,
    /// "common switch date", "staggered", "switchback" or "refused".
    pub design: &'static str,
    /// Waves that could not be estimated, with the reason. Empty on the
    /// common-switch-date path.
    pub dropped_waves: Vec<String>,
    pub pre_trend: Option<PreTrend>,
    pub size_bias: Option<SizeBias>,
    /// Switchback only: retained post-washout days across the retained ON
    /// periods — what the treated total multiplies by. `None` for waves designs.
    pub retained_on_days: Option<usize>,
    pub refusal: Option<String>,
}

impl EffectResult {
    /// A refusal: the reason is set and every number is `NaN`. A zero here would
    /// read as a finding — a zero effect, or worse a zero p-value.
    pub(crate) fn refused(reason: impl Into<String>) -> Self {
        Self {
            estimate: f64::NAN,
            se: f64::NAN,
            t_stat: f64::NAN,
            df: f64::NAN,
            ci_low: f64::NAN,
            ci_high: f64::NAN,
            p_value: f64::NAN,
            n_treated: 0,
            n_control: 0,
            significant: false,
            estimand: ESTIMAND,
            design: "refused",
            dropped_waves: Vec::new(),
            pre_trend: None,
            size_bias: None,
            retained_on_days: None,
            refusal: Some(reason.into()),
        }
    }
}

/// Every named unit must exist. Shared by BOTH paths: the staggered one once
/// skipped an unknown name silently, leaving a misspelled treated unit sitting
/// in the control pool — a plausible wrong number, which is the failure class
/// this module exists to avoid.
pub fn validate_assignment(m: &PanelMatrix, a: &Assignment) -> Result<(), String> {
    let mut names: Vec<&String> = a.switch_day.keys().collect();
    names.sort(); // deterministic: the FIRST unknown name, alphabetically
    for name in names {
        if m.unit_index(name).is_none() {
            return Err(format!(
                "assignment names unit '{name}', which is not in the panel"
            ));
        }
    }
    crate::engine::experiment::strata::validate_strata(a)
}

/// Validate the assignment and return the switch's day ORDINAL. Ordered so each
/// message states only what its own guard tested: staggering is checked before
/// the control check, so a staggered rollout with no controls is named for the
/// reason that would still refuse it if controls existed.
fn resolve_switch(m: &PanelMatrix, a: &Assignment) -> Result<i64, String> {
    validate_assignment(m, a)?;
    let mut days: Vec<i64> = a.switch_day.values().flatten().copied().collect();
    if days.is_empty() {
        return Err("no unit was treated, so there is nothing to estimate".into());
    }
    days.sort_unstable();
    days.dedup();
    if days.len() > 1 {
        return Err(format!(
            "staggered rollout: treated units switched on {} distinct switch dates, and \
             this estimator requires a common one",
            days.len()
        ));
    }
    // Only a NAMED `None` is a control: an unnamed panel unit is in neither arm.
    if !a.switch_day.values().any(Option::is_none) {
        return Err(
            "no unit stayed untreated, so there is no counterfactual to compare against".into(),
        );
    }
    if days[0] > *m.days.last().unwrap_or(&i64::MIN) {
        return Err("the switch date falls after the last day of history".into());
    }
    Ok(days[0])
}

/// The panel the estimator actually reads: only the units `switch_day` names.
/// A panel unit absent from the map is in NEITHER arm (index C1) and is dropped
/// here, before any collapse, Welch, wave pool, permutation or diagnostic sees
/// it. Borrowed when every unit is named. Callers validate first, so every key
/// is a panel unit. A day the absent unit lacked was already dropped by
/// `from_triples` and cannot be restored — the host builds the panel from the
/// pinned pool, and this guard is what keeps a stray unit out of the arms.
pub(crate) fn scope<'a>(m: &'a PanelMatrix, a: &Assignment) -> Cow<'a, PanelMatrix> {
    if a.switch_day.len() == m.n_units() {
        return Cow::Borrowed(m);
    }
    let mut rows: Vec<usize> = a
        .switch_day
        .keys()
        .filter_map(|n| m.unit_index(n))
        .collect();
    rows.sort_unstable();
    let values = rows
        .iter()
        .flat_map(|r| (0..m.n_days()).map(move |i| m.get(*r, i)))
        .collect();
    let units = rows.iter().map(|r| m.units[*r].clone()).collect();
    Cow::Owned(PanelMatrix {
        units,
        days: m.days.clone(),
        values,
    })
}

/// Treated and control deltas in `w`, arms resolved by NAME through
/// `unit_index`. A control is a unit NAMED with `None`; a unit the assignment
/// does not name is in neither arm (callers also pass a scoped panel).
pub(crate) fn split_arms(
    m: &PanelMatrix,
    a: &Assignment,
    w: &Windows,
) -> (Vec<f64>, Vec<f64>, Vec<UnitDelta>) {
    let named = |treated: bool| -> HashSet<usize> {
        a.switch_day
            .iter()
            .filter(|(_, s)| s.is_some() == treated)
            .filter_map(|(n, _)| m.unit_index(n))
            .collect()
    };
    let (treated, control) = (named(true), named(false));
    let deltas = collapse(m, w);
    let (mut t, mut c) = (Vec::new(), Vec::new());
    for d in &deltas {
        if treated.contains(&d.index) {
            t.push(d.delta)
        } else if control.contains(&d.index) {
            c.push(d.delta)
        }
    }
    (t, c, deltas)
}

fn window_refusal(s: i64, a: &Assignment) -> String {
    format!(
        "the window layout does not fit or is too sparsely covered: a switch on day {s} \
         against a design needing {}+{} calendar days before and {}+{} after, each at \
         least {:.0}% covered",
        a.anticipation_days,
        a.pre_days,
        a.washout_days,
        a.post_days,
        a.coverage_floor * 100.0
    )
}

/// The difference-in-differences estimate: each unit collapses to one pre/post
/// difference, then the two arms are compared with Welch.
pub fn estimate_simple(m: &PanelMatrix, a: &Assignment) -> EffectResult {
    let s = match resolve_switch(m, a) {
        Ok(s) => s,
        Err(reason) => return EffectResult::refused(reason),
    };
    let scoped = scope(m, a);
    let m = scoped.as_ref();
    let Some(w) = windows_around(
        m,
        s,
        a.pre_days,
        a.post_days,
        a.anticipation_days,
        a.washout_days,
        a.coverage_floor,
    ) else {
        return EffectResult::refused(window_refusal(s, a));
    };
    let (treated, control, deltas) = split_arms(m, a, &w);
    let test = match welch(&treated, &control) {
        Ok(t) => t,
        Err(WelchRefusal::ThinArm { treated, control }) => {
            return EffectResult::refused(format!(
                "a difference-in-differences needs at least 2 units per arm with a usable \
                 window; got {treated} treated and {control} control"
            ))
        }
        Err(WelchRefusal::NoSpread) => {
            return EffectResult::refused(
                "every unit moved identically, so there is no spread to measure the \
                 difference against",
            )
        }
    };
    let crit = t_quantile(test.df, a.alpha, a.family);
    EffectResult {
        estimate: test.diff,
        se: test.se,
        t_stat: test.t,
        df: test.df,
        ci_low: test.diff - crit * test.se,
        ci_high: test.diff + crit * test.se,
        p_value: t_two_sided_p(test.t, test.df),
        n_treated: treated.len(),
        n_control: control.len(),
        significant: test.t.abs() >= crit,
        estimand: ESTIMAND,
        design: "common switch date",
        dropped_waves: Vec::new(),
        pre_trend: pre_trend(m, a, s),
        size_bias: size_bias(m, a, &deltas, &w),
        retained_on_days: None,
        refusal: None,
    }
}

/// Distinct switch dates the assignment names.
fn switch_dates(a: &Assignment) -> usize {
    let mut days: Vec<i64> = a.switch_day.values().flatten().copied().collect();
    days.sort_unstable();
    days.dedup();
    days.len()
}

/// Route on the number of distinct switch dates, after validating the assignment
/// — an unknown name is refused whichever path would have run.
pub fn estimate_effect(m: &PanelMatrix, a: &Assignment, seed: u64) -> EffectResult {
    if let Err(reason) = validate_assignment(m, a) {
        return EffectResult::refused(reason);
    }
    // Every path below reads the scoped panel: an unnamed unit is in neither arm.
    let scoped = scope(m, a);
    let m = scoped.as_ref();
    if switch_dates(a) <= 1 {
        return estimate_simple(m, a);
    }
    let r = estimate_staggered(m, a, seed);
    if let Some(reason) = r.refusal {
        // Rebuilt from the reason alone: a guard refusal inside the staggered
        // estimator carries a finite att and p, and neither may reach the caller.
        let mut out = EffectResult::refused(reason);
        out.dropped_waves = r.dropped;
        return out;
    }
    let (waves, _) = build_waves(m, a).unwrap_or_default();
    EffectResult {
        estimate: r.att,
        se: f64::NAN,
        t_stat: f64::NAN,
        df: f64::NAN, // a permutation test has neither
        ci_low: r.ci_low,
        ci_high: r.ci_high,
        p_value: r.p_value,
        n_treated: r.waves.iter().map(|w| w.n_treated).sum(),
        n_control: r.waves.iter().map(|w| w.n_control).max().unwrap_or(0),
        significant: r.significant,
        estimand: ESTIMAND,
        design: "staggered",
        dropped_waves: r.dropped,
        pre_trend: pre_trend_by_wave(m, a, &waves),
        size_bias: size_bias_by_wave(m, a, &waves),
        retained_on_days: None,
        refusal: None,
    }
}

/// The estimator's accept/reject decision at zero effect, without its interval.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Decision {
    Refused(String),
    Tested {
        significant: bool,
        p_value: f64,
        estimate: f64,
    },
}

/// Exactly the decision `estimate_effect` reports, on either path. The common
/// path IS `estimate_simple` (cheap: no inversion to skip); the staggered path
/// is `test_at_zero`, the call `estimate_staggered` takes its `significant` from.
pub(crate) fn decide(m: &PanelMatrix, a: &Assignment, seed: u64) -> Decision {
    if let Err(reason) = validate_assignment(m, a) {
        return Decision::Refused(reason);
    }
    if switch_dates(a) <= 1 {
        let r = estimate_simple(m, a);
        return match r.refusal {
            Some(reason) => Decision::Refused(reason),
            None => Decision::Tested {
                significant: r.significant,
                p_value: r.p_value,
                estimate: r.estimate,
            },
        };
    }
    match test_at_zero(m, a, seed) {
        Ok(z) => Decision::Tested {
            significant: z.significant,
            p_value: z.p_value,
            estimate: z.att,
        },
        Err(reason) => Decision::Refused(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::t_two_sided_p;
    use crate::engine::experiment::testkit::{assign, fixture, fixture_rows};

    #[test]
    fn experiment_estimate_recovers_a_planted_effect() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let r = estimate_simple(&m, &a);
        assert!(r.refusal.is_none(), "unexpected refusal: {:?}", r.refusal);
        assert!(
            (r.estimate - 20.0).abs() < 5.0,
            "estimate was {}",
            r.estimate
        );
        assert_eq!((r.n_treated, r.n_control), (6, 6));
        assert!(
            r.ci_low < 20.0 && 20.0 < r.ci_high,
            "CI missed truth: {:?}",
            r
        );
        assert!(r.significant);
    }

    #[test]
    fn experiment_estimate_is_immune_to_unit_ordering() {
        // "t*" sorts AFTER "c*", so the matrix order is the reverse of the
        // caller's. A positional match reports −20 instead of +20.
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        assert!(
            m.units()[0].starts_with('c'),
            "fixture must exercise re-ordering"
        );
        let r = estimate_simple(&m, &assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31));
        assert!(
            r.estimate > 0.0,
            "sign flipped: arms were matched positionally"
        );
    }

    #[test]
    fn experiment_estimate_refuses_when_no_unit_stayed_untreated() {
        let m = fixture(12, 60, 12, 20.0, 31, 4);
        let all: Vec<String> = m.units().to_vec();
        let a = assign(&m, &all.iter().map(String::as_str).collect::<Vec<_>>(), 31);
        let r = estimate_simple(&m, &a);
        let reason = r.refusal.expect("must refuse");
        assert!(reason.contains("untreated"), "reason was: {reason}");
        assert!(
            !reason.contains("same date"),
            "must not assert an unchecked claim"
        );
        assert!(
            r.estimate.is_nan() && r.p_value.is_nan(),
            "a refusal carries NaN, never a zero"
        );
    }

    /// The SIMPLE path refuses staggered input; routing a staggered assignment to
    /// the wave estimator is the dispatcher's job, not this function's.
    #[test]
    fn experiment_estimate_simple_refuses_a_staggered_rollout() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.switch_day.insert("t0".into(), Some(30)); // one unit a day early
        let reason = estimate_simple(&m, &a).refusal.expect("must refuse");
        assert!(
            reason.contains("2 distinct switch dates"),
            "reason was: {reason}"
        );
    }

    #[test]
    fn experiment_estimate_refuses_an_unknown_unit_name() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.switch_day.insert("typo_store".into(), Some(31));
        let reason = estimate_simple(&m, &a).refusal.expect("must refuse");
        assert!(reason.contains("typo_store"), "reason was: {reason}");
    }

    #[test]
    fn experiment_estimate_refuses_windows_history_cannot_hold() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.pre_days = 40; // only 30 days exist before the switch
        let reason = estimate_simple(&m, &a).refusal.expect("must refuse");
        assert!(reason.contains("window"), "reason was: {reason}");
    }

    #[test]
    fn experiment_estimate_reports_the_welch_p_value() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let r = estimate_simple(&m, &assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31));
        assert!((r.p_value - t_two_sided_p(r.t_stat, r.df)).abs() < 1e-12);
        assert!(
            r.p_value < 0.05,
            "a planted 20 must read significant, p = {}",
            r.p_value
        );
        assert_eq!(
            r.p_value <= 0.05,
            r.significant,
            "with family = 1 the p-value and the decision must agree"
        );
        let null = fixture(12, 60, 6, 0.0, 31, 4);
        let rn = estimate_simple(
            &null,
            &assign(&null, &["t0", "t1", "t2", "t3", "t4", "t5"], 31),
        );
        assert!(
            rn.p_value > 0.0 && rn.p_value <= 1.0,
            "p was {}",
            rn.p_value
        );
    }

    /// The window check reads the REGISTERED floor. Days 13, 17 and 21 missing
    /// for one unit are missing for the whole panel, so the pre window [11, 31)
    /// holds 17 of its 20 calendar days — 85%.
    #[test]
    fn experiment_estimate_refuses_a_window_below_the_assignment_coverage_floor() {
        let rows: Vec<(String, i64, f64)> = fixture_rows(12, 60, 6, 20.0, 31, 4)
            .into_iter()
            .filter(|(u, d, _)| !(u == "c6" && [13, 17, 21].contains(d)))
            .collect();
        let m = PanelMatrix::from_triples(rows);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.coverage_floor = 0.9;
        let reason = estimate_simple(&m, &a)
            .refusal
            .expect("85% must not pass a 90% floor");
        assert!(
            reason.contains("90%"),
            "the message must name the floor it tested: {reason}"
        );
        a.coverage_floor = 0.8;
        let r = estimate_simple(&m, &a);
        assert!(
            r.refusal.is_none(),
            "85% passes an 80% floor: {:?}",
            r.refusal
        );
    }

    /// Index C1: a panel unit ABSENT from `switch_day` is in neither arm. A store
    /// opened after the control pool was pinned — here a wildly trending one,
    /// sorting into the middle of the unit order — must leave the estimate
    /// bit-identical, not join the controls.
    #[test]
    fn experiment_unit_absent_from_assignment_is_in_neither_arm() {
        let rows = fixture_rows(12, 60, 6, 20.0, 31, 4);
        let m = PanelMatrix::from_triples(rows.clone());
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let mut wild = rows;
        wild.extend((1..=60i64).map(|d| ("m_new".to_string(), d, 500.0 + 40.0 * d as f64)));
        let m_wild = PanelMatrix::from_triples(wild);
        assert_eq!(
            m_wild.n_units(),
            13,
            "precondition: the panel holds the unnamed unit"
        );
        assert_eq!(
            format!("{:?}", estimate_simple(&m_wild, &a)),
            format!("{:?}", estimate_simple(&m, &a)),
            "an unnamed unit leaked into an arm or a diagnostic"
        );
        // Naming only the treated units leaves no control, however many unnamed
        // units the panel holds.
        let mut only_treated = a.clone();
        only_treated.switch_day.retain(|_, s| s.is_some());
        let reason = estimate_simple(&m_wild, &only_treated)
            .refusal
            .expect("must refuse");
        assert!(reason.contains("untreated"), "{reason}");
    }

    use crate::engine::experiment::staggered::estimate_staggered;
    use crate::engine::experiment::testkit::{
        staggered_fixture, staggered_fixture_no_holdout, triples_of,
    };

    #[test]
    fn experiment_dispatch_routes_on_the_number_of_switch_dates() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let simple = estimate_effect(
            &m,
            &assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31),
            1,
        );
        assert_eq!(simple.design, "common switch date");
        let (ms, a) = staggered_fixture(25.0, 7);
        assert_eq!(estimate_effect(&ms, &a, 1).design, "staggered");
    }

    #[test]
    fn experiment_dispatch_refuses_an_unknown_name_on_both_paths() {
        for staggered in [false, true] {
            let (m, mut a) = if staggered {
                staggered_fixture(25.0, 7)
            } else {
                let m = fixture(12, 60, 6, 20.0, 31, 4);
                let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
                (m, a)
            };
            a.switch_day.insert("typo_store".into(), Some(31));
            let r = estimate_effect(&m, &a, 1);
            assert!(
                r.refusal.expect("must refuse").contains("typo_store"),
                "staggered={staggered}: an unknown name must never be silently skipped"
            );
        }
    }

    /// A single wave is a special case of the permutation test. Welch and the
    /// INVERTED permutation interval must land close; the `att +/- crit` shortcut
    /// gives 0.34 here and fails.
    #[test]
    fn experiment_dispatch_paths_agree_on_a_single_wave() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let welch = estimate_simple(&m, &a);
        let perm = estimate_staggered(&m, &a, 3);
        assert!(
            (welch.estimate - perm.att).abs() < 1e-9,
            "same point estimate"
        );
        let ratio = (welch.ci_high - welch.estimate) / ((perm.ci_high - perm.att).max(1e-12));
        assert!(
            (0.7..=1.4).contains(&ratio),
            "interval widths diverged: welch {} vs inverted permutation {} (ratio {ratio})",
            welch.ci_high - welch.estimate,
            perm.ci_high - perm.att
        );
    }

    #[test]
    fn experiment_dispatch_reports_a_pre_trend_for_a_fully_staggered_rollout() {
        // No never-treated units at all: an ever-treated-vs-never-treated split
        // would report nothing here.
        let (m, a) = staggered_fixture_no_holdout(0.0, 11);
        assert!(
            estimate_effect(&m, &a, 1).pre_trend.is_some(),
            "per-wave diagnostics must survive the absence of a global control arm"
        );
    }

    #[test]
    fn experiment_dispatch_reports_the_permutation_p_value() {
        let (m, a) = staggered_fixture(25.0, 7);
        let r = estimate_effect(&m, &a, 1);
        let direct = estimate_staggered(&m, &a, 1);
        assert_eq!(
            r.p_value, direct.p_value,
            "the staggered p is the permutation p at tau = 0"
        );
        assert!(r.p_value > 0.0 && r.p_value < 0.05, "p was {}", r.p_value);
        assert_eq!(r.p_value <= 0.05, r.significant);
    }

    /// The staggered guard refusal carries a finite att and p (the smallest
    /// attainable p) inside `StaggeredResult`; the dispatcher must drop them,
    /// because a refusal reads as NaN everywhere, never as a finding.
    #[test]
    fn experiment_dispatch_staggered_refusal_carries_nan_numbers() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        a.strata = Some(
            [
                &["t0", "c6"][..],
                &["t1"],
                &["t2"],
                &["t3"],
                &["t4"],
                &["t5"],
                &["c7"],
                &["c8"],
                &["c9"],
                &["c10"],
                &["c11"],
            ]
            .iter()
            .map(|b| b.iter().map(|u| u.to_string()).collect())
            .collect(),
        );
        let direct = estimate_staggered(&m, &a, 1);
        assert!(
            direct.att.is_finite() && direct.p_value.is_finite(),
            "precondition: the inner guard refusal carries finite numbers"
        );
        let r = estimate_effect(&m, &a, 1);
        assert!(r.refusal.is_some());
        assert_eq!(r.design, "refused");
        assert!(
            r.estimate.is_nan()
                && r.p_value.is_nan()
                && r.ci_low.is_nan()
                && r.ci_high.is_nan()
                && r.se.is_nan(),
            "a refusal must carry NaN: {r:?}"
        );
        assert!(!r.significant);
        assert!(matches!(decide(&m, &a, 1), Decision::Refused(_)));
    }

    /// Review focus 2: the Anderson-Rubin inversion and the placebo read
    /// `decide`, so `decide` must BE the decision `estimate_effect` reports —
    /// same significance, same p, same estimate — on both paths, and refuse
    /// what it refuses.
    #[test]
    fn experiment_decide_is_the_decision_estimate_effect_reports() {
        let common = {
            let m = fixture(12, 60, 6, 3.0, 31, 4);
            let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
            (m, a)
        };
        for (m, a) in [
            common,
            staggered_fixture(0.0, 7),
            staggered_fixture(25.0, 7),
        ] {
            let full = estimate_effect(&m, &a, 5);
            match decide(&m, &a, 5) {
                Decision::Tested {
                    significant,
                    p_value,
                    estimate,
                } => {
                    assert_eq!(significant, full.significant, "design {}", full.design);
                    assert_eq!(p_value, full.p_value, "design {}", full.design);
                    assert_eq!(estimate, full.estimate, "design {}", full.design);
                }
                Decision::Refused(r) => panic!("unexpected refusal: {r}"),
            }
        }
        let m = fixture(12, 60, 6, 3.0, 31, 4);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.switch_day.insert("typo_store".into(), Some(31));
        assert!(matches!(decide(&m, &a, 5), Decision::Refused(r) if r.contains("typo_store")));
    }

    /// Index C1 on the staggered path: an unnamed, wildly trending unit must
    /// join no wave's control pool and no permutation group.
    #[test]
    fn experiment_unit_absent_from_assignment_is_in_neither_arm_staggered() {
        let (m, a) = staggered_fixture(25.0, 7);
        let mut rows = triples_of(&m);
        rows.extend((1..=100i64).map(|d| ("m_new".to_string(), d, 500.0 + 40.0 * d as f64)));
        let m_wild = PanelMatrix::from_triples(rows);
        assert_eq!(
            format!("{:?}", estimate_effect(&m_wild, &a, 1)),
            format!("{:?}", estimate_effect(&m, &a, 1)),
            "an unnamed unit joined a wave's control pool or the permutation groups"
        );
        assert_eq!(decide(&m_wild, &a, 1), decide(&m, &a, 1));
    }
}
