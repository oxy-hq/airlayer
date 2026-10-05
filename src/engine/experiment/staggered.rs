//! The staggered estimator: units grouped into WAVES by switch date, each wave
//! estimated against only the units still clean through its post window.
//!
//! A pooled two-way fixed-effects regression would use already-treated units as
//! controls for later waves, which under heterogeneous effects gives some
//! comparisons negative weight and can flip the sign of the aggregate. A clean
//! pool per wave avoids that. The last wave of a rollout with no permanent
//! holdout has an empty pool by construction: it is dropped and reported, never
//! folded in at zero.

use crate::engine::experiment::estimate::{scope, validate_assignment, Assignment};
use crate::engine::experiment::per_comparison_alpha;
use crate::engine::experiment::permutation::{
    permutation_groups, permutation_test, relabellings, Groups, PermOutcome, Structure,
};
use crate::engine::experiment::{collapse, windows_around, PanelMatrix, Windows};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Endpoint bisection steps. The statistic is monotone in tau, so this is plenty.
const INVERSION_STEPS: usize = 25;
const BRACKET_DOUBLINGS: usize = 40;
const TOO_FEW: &str = "too few permutations produced a usable aggregate";

/// The units switching on one day, with that wave's windows and its clean
/// control pool (matrix rows).
#[derive(Debug, Clone)]
pub struct Wave {
    pub switch_ord: i64,
    pub treated: Vec<usize>,
    pub controls: Vec<usize>,
    pub windows: Windows,
}

impl Wave {
    pub fn n_control_units(&self) -> usize {
        self.controls.len()
    }
}

#[derive(Debug, Clone)]
pub struct WaveAtt {
    pub switch_ord: i64,
    pub n_treated: usize,
    pub n_control: usize,
    pub att: f64,
}

/// Every unit's switch ordinal, by matrix row; `None` = never treated. Reads a
/// SCOPED panel, where every row is named — an unnamed unit must never reach
/// here, or it would pose as never-treated.
pub(crate) fn switch_by_row(m: &PanelMatrix, a: &Assignment) -> Vec<Option<i64>> {
    let mut switch_of: Vec<Option<i64>> = vec![None; m.n_units()];
    for (name, day) in &a.switch_day {
        if let Some(u) = m.unit_index(name) {
            switch_of[u] = *day;
        }
    }
    switch_of
}

/// Group units by switch DATE and attach each wave's clean control pool.
/// `Err` when the assignment names a unit the panel does not hold; otherwise the
/// waves that can be estimated plus one message per wave dropped. `m` must be
/// scoped (`scope(m, a)`): row indices in the waves refer to it. Crate-private
/// because an unscoped caller would make an unnamed unit a control.
pub(crate) fn build_waves(
    m: &PanelMatrix,
    a: &Assignment,
) -> Result<(Vec<Wave>, Vec<String>), String> {
    validate_assignment(m, a)?;
    debug_assert_eq!(
        a.switch_day.len(),
        m.n_units(),
        "build_waves reads a scoped panel: an unnamed unit would become a control"
    );
    let switch_of = switch_by_row(m, a);
    let mut groups: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (u, s) in switch_of.iter().enumerate() {
        if let Some(s) = s {
            groups.entry(*s).or_default().push(u);
        }
    }
    let (mut waves, mut dropped) = (Vec::new(), Vec::new());
    for (switch_ord, treated) in groups {
        match wave_at(m, a, &switch_of, switch_ord, treated) {
            Ok(w) => waves.push(w),
            Err(reason) => dropped.push(reason),
        }
    }
    Ok((waves, dropped))
}

/// One wave, or the reason it drops. Each message names only its own guard.
fn wave_at(
    m: &PanelMatrix,
    a: &Assignment,
    switch_of: &[Option<i64>],
    switch_ord: i64,
    treated: Vec<usize>,
) -> Result<Wave, String> {
    let Some(windows) = windows_around(
        m,
        switch_ord,
        a.pre_days,
        a.post_days,
        a.anticipation_days,
        a.washout_days,
        a.coverage_floor,
    ) else {
        return Err(format!(
            "wave switching on day {switch_ord}: its windows do not fit history or fall \
             under the {:.0}% coverage floor",
            a.coverage_floor * 100.0
        ));
    };
    if treated.len() < 2 {
        return Err(format!(
            "wave switching on day {switch_ord}: {} unit(s), needs at least 2",
            treated.len()
        ));
    }
    // Clean for the WHOLE post window, anticipation included: a unit whose
    // pre-announcement lands inside the window is already contaminated.
    let anticipation = a.anticipation_days as i64;
    let controls: Vec<usize> = (0..m.n_units())
        .filter(|u| switch_of[*u].is_none_or(|k| k - anticipation >= windows.post.1))
        .collect();
    if controls.len() < 2 {
        return Err(format!(
            "wave switching on day {switch_ord}: no clean control pool remains \
             ({} unit(s) untreated through its post window)",
            controls.len()
        ));
    }
    Ok(Wave {
        switch_ord,
        treated,
        controls,
        windows,
    })
}

pub fn wave_att(m: &PanelMatrix, w: &Wave) -> Option<WaveAtt> {
    let deltas = collapse(m, &w.windows);
    let mean_of = |members: &[usize]| -> Option<f64> {
        let v: Vec<f64> = deltas
            .iter()
            .filter(|d| members.contains(&d.index))
            .map(|d| d.delta)
            .collect();
        if v.is_empty() {
            None
        } else {
            Some(v.iter().sum::<f64>() / v.len() as f64)
        }
    };
    Some(WaveAtt {
        switch_ord: w.switch_ord,
        n_treated: w.treated.len(),
        n_control: w.controls.len(),
        att: mean_of(&w.treated)? - mean_of(&w.controls)?,
    })
}

#[derive(Debug, Clone)]
pub struct StaggeredResult {
    pub att: f64,
    /// A permutation test reports no standard error: always `NaN`.
    pub null_sd: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub p_value: f64,
    pub significant: bool,
    /// Relabellings behind `p_value` — all of them when enumerated exactly.
    pub permutations: usize,
    pub waves: Vec<WaveAtt>,
    pub dropped: Vec<String>,
    pub refusal: Option<String>,
}

impl StaggeredResult {
    fn refused(reason: impl Into<String>, dropped: Vec<String>) -> Self {
        Self {
            att: f64::NAN,
            null_sd: f64::NAN,
            ci_low: f64::NAN,
            ci_high: f64::NAN,
            p_value: f64::NAN,
            significant: false,
            permutations: 0,
            waves: Vec::new(),
            dropped,
            refusal: Some(reason.into()),
        }
    }
}

/// Wave ATTs combined by wave SIZE. An unweighted mean would let a one-unit
/// wave outvote a nine.
pub fn aggregate(atts: &[WaveAtt]) -> Option<f64> {
    let total: usize = atts.iter().map(|a| a.n_treated).sum();
    if total == 0 {
        return None;
    }
    Some(atts.iter().map(|a| a.n_treated as f64 * a.att).sum::<f64>() / total as f64)
}

/// Everything the permutation test needs, built once, over the SCOPED panel.
struct Setup<'a> {
    panel: Cow<'a, PanelMatrix>,
    waves: Vec<Wave>,
    dropped: Vec<String>,
    atts: Vec<WaveAtt>,
    att: f64,
    switch_of: Vec<Option<i64>>,
    groups: Groups,
    alpha: f64,
}

impl Setup<'_> {
    fn structure(&self) -> Structure<'_> {
        Structure {
            waves: &self.waves,
            switch_of: &self.switch_of,
            groups: &self.groups,
        }
    }
}

/// Every row's stratum. Unblocked for now: every unit sits in stratum 0.
fn stratum_rows(m: &PanelMatrix, _a: &Assignment) -> Vec<usize> {
    vec![0; m.n_units()]
}

/// Waves, the aggregate, and the reachability guard. `Err` carries the
/// finished refusal, so every caller reports it identically.
fn setup<'a>(m: &'a PanelMatrix, a: &Assignment) -> Result<Setup<'a>, Box<StaggeredResult>> {
    validate_assignment(m, a)
        .map_err(|reason| Box::new(StaggeredResult::refused(reason, Vec::new())))?;
    // An unnamed unit is in neither arm, so it is in no permutation group either.
    let panel = scope(m, a);
    let m = panel.as_ref();
    let (waves, dropped) = build_waves(m, a)
        .map_err(|reason| Box::new(StaggeredResult::refused(reason, Vec::new())))?;
    let atts: Vec<WaveAtt> = waves.iter().filter_map(|w| wave_att(m, w)).collect();
    let Some(att) = aggregate(&atts) else {
        return Err(Box::new(StaggeredResult::refused(
            "no wave could be estimated",
            dropped,
        )));
    };
    if !att.is_finite() {
        return Err(Box::new(StaggeredResult::refused(
            "a wave window holds a non-finite value (NaN or infinite), so the effect \
             cannot be estimated",
            dropped,
        )));
    }
    let switch_of = switch_by_row(m, a);
    let groups = permutation_groups(&switch_of, &stratum_rows(m, a));
    let alpha = per_comparison_alpha(a.alpha, a.family);
    // Can this design reach alpha at all? If not, the acceptance set is the
    // whole line and no bisection can honestly return an endpoint.
    let min_p = 1.0 / relabellings(&groups);
    if min_p > alpha {
        return Err(Box::new(StaggeredResult {
            att,
            null_sd: f64::NAN,
            ci_low: f64::NEG_INFINITY,
            ci_high: f64::INFINITY,
            p_value: min_p,
            significant: false,
            permutations: 0,
            waves: atts,
            dropped,
            refusal: Some(unreachable_refusal(m.n_units(), groups.len(), alpha, min_p)),
        }));
    }
    Ok(Setup {
        panel,
        waves,
        dropped,
        atts,
        att,
        switch_of,
        groups,
        alpha,
    })
}

/// Names what the guard tested — and, when the labels were permuted within
/// strata, that it counted within them.
fn unreachable_refusal(n_units: usize, n_strata: usize, alpha: f64, min_p: f64) -> String {
    let within = if n_strata > 1 {
        format!(", permuted within {n_strata} strata,")
    } else {
        String::new()
    };
    format!(
        "a permutation test over {n_units} units in this wave structure{within} cannot \
         reach alpha = {alpha:.3}: its smallest attainable p-value is {min_p:.3}"
    )
}

/// The estimator's decision at zero effect, without its interval.
pub(crate) struct ZeroTest {
    pub(crate) significant: bool,
    pub(crate) p_value: f64,
    pub(crate) att: f64,
}

/// The staggered decision exactly as `estimate_staggered` makes it: the same
/// setup, the same `permutation_test` call at `tau = 0`, the same seed.
pub(crate) fn test_at_zero(m: &PanelMatrix, a: &Assignment, seed: u64) -> Result<ZeroTest, String> {
    let s = setup(m, a).map_err(|r| r.refusal.unwrap_or_default())?;
    let o = permutation_test(&s.panel, &s.structure(), 0.0, s.alpha, seed)
        .ok_or_else(|| TOO_FEW.to_string())?;
    Ok(ZeroTest {
        significant: o.rejected,
        p_value: o.p_value,
        att: s.att,
    })
}

/// One endpoint of the acceptance set, searched on its OWN bracket. A search
/// that never finds rejection is an unbounded side, reported as such. Shared
/// with the switchback inversion.
pub(crate) fn endpoint(test: &dyn Fn(f64) -> Option<PermOutcome>, att: f64, direction: f64) -> f64 {
    let rejects = |tau: f64| test(tau).is_some_and(|o| o.rejected);
    let mut span = att.abs().max(1.0);
    let mut bracketed = false;
    for _ in 0..BRACKET_DOUBLINGS {
        if rejects(att + direction * span) {
            bracketed = true;
            break;
        }
        span *= 2.0;
    }
    if !bracketed {
        return direction * f64::INFINITY;
    }
    let (mut inside, mut outside) = (att, att + direction * span);
    for _ in 0..INVERSION_STEPS {
        let mid = (inside + outside) / 2.0;
        if rejects(mid) {
            outside = mid
        } else {
            inside = mid
        }
    }
    inside
}

pub fn estimate_staggered(m: &PanelMatrix, a: &Assignment, seed: u64) -> StaggeredResult {
    let s = match setup(m, a) {
        Ok(s) => s,
        Err(refused) => return *refused,
    };
    let structure = s.structure();
    let test = |tau: f64| permutation_test(&s.panel, &structure, tau, s.alpha, seed);
    let Some(at_zero) = test(0.0) else {
        return StaggeredResult::refused(TOO_FEW, s.dropped);
    };
    StaggeredResult {
        att: s.att,
        null_sd: f64::NAN,
        ci_low: endpoint(&test, s.att, -1.0),
        ci_high: endpoint(&test, s.att, 1.0),
        p_value: at_zero.p_value,
        significant: at_zero.rejected,
        permutations: at_zero.draws,
        waves: s.atts,
        dropped: s.dropped,
        refusal: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::{
        assign, fixture, ladder, staggered_fixture, staggered_fixture_no_holdout, triples_of,
    };
    use crate::engine::experiment::PanelMatrix;
    use std::collections::HashMap;

    /// `units` units, the first `treated` (t0…) switching on day 31, over 60 days.
    fn tiny_fixture(units: usize, treated: usize, seed: u64) -> (PanelMatrix, Assignment) {
        let m = fixture(units, 60, treated, 0.0, 31, seed);
        let names: Vec<String> = (0..treated).map(|u| format!("t{u}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let a = assign(&m, &refs, 31);
        (m, a)
    }

    /// Two early adopters (day 21) against five late (day 41), no holdout, 60
    /// days: the late wave drops and the early one has the five late as controls.
    fn two_vs_five_fixture(effect: f64, seed: u64) -> (PanelMatrix, Assignment) {
        let names: Vec<String> = (0..7)
            .map(|u| {
                if u < 2 {
                    format!("e{u}")
                } else {
                    format!("l{u}")
                }
            })
            .collect();
        ladder(
            &names,
            &|u| Some(if u < 2 { 21 } else { 41 }),
            60,
            effect,
            seed,
        )
    }

    /// Every wave drops: e0-e3 on day 21 and l4-l7 on day 31 leave neither a
    /// clean control through its post window.
    fn all_waves_drop_fixture(seed: u64) -> (PanelMatrix, Assignment) {
        let names: Vec<String> = (0..8)
            .map(|u| {
                if u < 4 {
                    format!("e{u}")
                } else {
                    format!("l{u}")
                }
            })
            .collect();
        ladder(
            &names,
            &|u| Some(if u < 4 { 21 } else { 31 }),
            60,
            0.0,
            seed,
        )
    }

    /// A single-wave, noise-free panel whose collapsed delta for unit `i` is
    /// exactly `deltas[i]`, so a test can pin an exact tail position.
    fn planted_delta_fixture(deltas: &[f64], treated: &[usize]) -> (PanelMatrix, Assignment) {
        let mut rows = Vec::new();
        let mut switch_day = HashMap::new();
        for (i, delta) in deltas.iter().enumerate() {
            let name = format!("u{i:02}");
            switch_day.insert(name.clone(), treated.contains(&i).then_some(21));
            for day in 1..=40i64 {
                rows.push((name.clone(), day, if day >= 21 { *delta } else { 0.0 }));
            }
        }
        let a = Assignment {
            switch_day,
            strata: None,
            pre_days: 20,
            post_days: 20,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        };
        (PanelMatrix::from_triples(rows), a)
    }

    #[test]
    fn experiment_staggered_recovers_a_planted_effect_and_rejects() {
        let (m, a) = staggered_fixture(25.0, 7);
        let r = estimate_staggered(&m, &a, 4);
        assert!(r.refusal.is_none(), "unexpected refusal: {:?}", r.refusal);
        assert!((r.att - 25.0).abs() < 8.0, "att was {}", r.att);
        assert!(r.significant && r.p_value < 0.05, "p was {}", r.p_value);
        assert!(
            r.ci_low < 25.0 && 25.0 < r.ci_high,
            "CI missed truth: {:?}",
            r
        );
    }

    /// The shortcut this replaces reported an interval ~3x too wide. Under a real
    /// effect a correct inversion must stay close to what Welch says on the same
    /// single-wave data — and must NOT scale with the effect.
    #[test]
    fn experiment_staggered_interval_does_not_widen_with_the_effect() {
        let widths: Vec<f64> = [0.0, 20.0, 200.0]
            .iter()
            .map(|eff| {
                let m = fixture(12, 60, 6, *eff, 31, 4);
                let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
                let r = estimate_staggered(&m, &a, 3);
                (r.ci_high - r.ci_low) / 2.0
            })
            .collect();
        let (lo, hi) = (
            widths.iter().cloned().fold(f64::MAX, f64::min),
            widths.iter().cloned().fold(0.0_f64, f64::max),
        );
        assert!(
            hi / lo < 1.6,
            "interval width tracked the effect size: {widths:?} — that is the \
             att +/- crit shortcut, not an inversion"
        );
    }

    /// Only the decision is under test, so this reads `test_at_zero` — the one
    /// call `estimate_staggered` takes its `significant` from — and skips the
    /// interval search 200 times over.
    #[test]
    fn experiment_staggered_holds_its_false_positive_rate_under_no_effect() {
        let rejected = (0..200)
            .filter(|k| {
                let (m, a) = staggered_fixture(0.0, 900 + *k as u64);
                test_at_zero(&m, &a, 17)
                    .expect("a usable design")
                    .significant
            })
            .count();
        assert!(
            (2..=18).contains(&rejected),
            "rejected {rejected}/200 under the null, want ~10"
        );
    }

    #[test]
    fn experiment_staggered_aggregate_weights_by_wave_size() {
        let atts = vec![
            WaveAtt {
                switch_ord: 10,
                n_treated: 9,
                n_control: 5,
                att: 10.0,
            },
            WaveAtt {
                switch_ord: 20,
                n_treated: 1,
                n_control: 5,
                att: 50.0,
            },
        ];
        assert!(
            (aggregate(&atts).unwrap() - 14.0).abs() < 1e-12,
            "(9*10 + 1*50)/10 = 14"
        );
    }

    /// The defect this guards against: a global ever-treated flag adjusts the
    /// later wave (a control for the earlier one) too, and the statistic never
    /// moves. With no holdout and both waves treated, the true effect must
    /// still be INSIDE the interval.
    #[test]
    fn experiment_staggered_inversion_adjusts_by_exposure_not_by_flag() {
        let (m, a) = staggered_fixture_no_holdout(25.0, 5); // 4 early, 4 late
        let r = estimate_staggered(&m, &a, 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            r.ci_low < 25.0 && 25.0 < r.ci_high,
            "true effect 25 outside [{}, {}]: the adjustment hit the wrong units",
            r.ci_low,
            r.ci_high
        );
    }

    #[test]
    fn experiment_staggered_refuses_a_design_that_cannot_reach_alpha() {
        // 2 treated, 2 controls: six relabellings, minimum p = 1/6 > 0.05.
        let (m, a) = tiny_fixture(4, 2, 11);
        let r = estimate_staggered(&m, &a, 3);
        let reason = r
            .refusal
            .expect("must refuse rather than report a finite interval");
        assert!(reason.contains("cannot reach"), "reason was: {reason}");
        assert!(
            r.ci_low.is_infinite() && r.ci_high.is_infinite(),
            "an unreachable alpha is an unbounded interval, not a number"
        );
    }

    /// The bound is 1/N, not 2/N. Two early against five late (last wave
    /// dropped) is 21 assignments; an extreme observed split reaches p = 1/21
    /// exactly, so the design must NOT be refused, and the exact p must be
    /// reported — this design is below the enumeration cap.
    #[test]
    fn experiment_staggered_min_p_bound_has_no_factor_of_two() {
        let (m, a) = two_vs_five_fixture(100.0, 3);
        let r = estimate_staggered(&m, &a, 3);
        assert!(r.refusal.is_none(), "wrongly refused: {:?}", r.refusal);
        assert!(
            (r.p_value - 1.0 / 21.0).abs() < 1e-9,
            "exact enumeration must give p = 1/21, got {}",
            r.p_value
        );
        assert!(r.significant);
    }

    /// A balanced tiny design clears the 1/N guard but its exact p is 2/N > alpha,
    /// so the bracket search never finds rejection: infinite endpoints, no refusal.
    #[test]
    fn experiment_staggered_reports_an_unbounded_interval_when_no_bracket_rejects() {
        let (m, a) = tiny_fixture(6, 3, 11); // C(6,3) = 20; 1/20 = 0.05 passes, 2/20 does not
        let r = estimate_staggered(&m, &a, 3);
        assert!(
            r.refusal.is_none(),
            "1/20 <= alpha, so the guard must not refuse: {:?}",
            r.refusal
        );
        assert!(!r.significant);
        assert!(
            r.ci_low.is_infinite() && r.ci_high.is_infinite(),
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    /// The decision must be the p-value, not a percentile of the sorted nulls.
    /// Nine units with two treated is 36 relabellings; these deltas put the
    /// observed pair SECOND most extreme, so its exact p is 2/36 = 0.0556 while
    /// the 95th percentile sits at the third (index round(0.95 * 35) = 33).
    #[test]
    fn experiment_staggered_significance_follows_the_exact_p_value() {
        let (m, a) =
            planted_delta_fixture(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 40.0, 41.0], &[6, 8]);
        let r = estimate_staggered(&m, &a, 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            (r.p_value - 2.0 / 36.0).abs() < 1e-9,
            "exact p was {}",
            r.p_value
        );
        assert!(
            !r.significant,
            "p = {:.4} exceeds alpha; only the percentile rule calls this significant",
            r.p_value
        );
    }

    /// `staggered_fixture` with one cell of `unit` on `day` replaced by NaN.
    fn with_nan_cell(effect: f64, seed: u64, unit: &str, day: i64) -> (PanelMatrix, Assignment) {
        let (m, a) = staggered_fixture(effect, seed);
        let rows = triples_of(&m).into_iter().map(|(u, d, v)| {
            if u == unit && d == day {
                (u, d, f64::NAN)
            } else {
                (u, d, v)
            }
        });
        (PanelMatrix::from_triples(rows), a)
    }

    /// A NaN cell in an observed treated unit's post window makes the observed
    /// aggregate NaN. Every comparison against NaN is false, so no relabelling
    /// is "as extreme" and the exact p collapses to 0/N = 0: a finding read off
    /// garbage. It must be a refusal, never a rejection.
    #[test]
    fn experiment_staggered_refuses_a_non_finite_observed_statistic() {
        let (m, a) = with_nan_cell(25.0, 7, "t0", 65);
        let r = estimate_staggered(&m, &a, 4);
        assert!(r.refusal.is_some(), "a NaN observation must refuse: {r:?}");
        assert!(!r.significant, "{r:?}");
        assert!(r.p_value.is_nan() || r.p_value > 0.0, "p was {}", r.p_value);
        let z = test_at_zero(&m, &a, 4);
        assert!(z.is_err(), "the zero test must refuse too");
    }

    /// A NaN in a unit that is NOT in the observed slots leaves the observed
    /// statistic finite, but any relabelling that moves it into a slot is NaN.
    /// Those draws must count as extreme (they can only raise p), not drop out of
    /// the tail while staying in the denominator.
    #[test]
    fn experiment_staggered_non_finite_null_draws_count_as_extreme() {
        // t3 switches on day 71, so in the day-61 wave it is neither treated nor
        // a control; day 45 sits only in that wave's pre window.
        let (m, a) = staggered_fixture(0.0, 7);
        let clean = test_at_zero(&m, &a, 4).expect("usable design");
        let (mn, an) = with_nan_cell(0.0, 7, "t3", 45);
        let dirty = test_at_zero(&mn, &an, 4).expect("observed statistic is finite");
        assert!(
            dirty.p_value >= clean.p_value,
            "NaN draws shrank p: {} < {}",
            dirty.p_value,
            clean.p_value
        );
    }

    #[test]
    fn experiment_staggered_refuses_when_every_wave_drops() {
        let (m, a) = all_waves_drop_fixture(11);
        let r = estimate_staggered(&m, &a, 4);
        assert!(r.refusal.expect("must refuse").contains("no wave"));
        assert!(
            !r.dropped.is_empty(),
            "the reasons must survive the refusal"
        );
    }

    #[test]
    fn experiment_waves_use_only_clean_controls() {
        // 3 switch on day 61, 3 on day 71, 6 never. With pre=post=20 the first
        // wave's post window is [61, 81): the day-71 wave switches INSIDE it,
        // so only the 6 never-treated qualify as its controls.
        let (m, a) = staggered_fixture(0.0, 7);
        let (waves, dropped) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(waves.len(), 2, "dropped: {dropped:?}");
        assert_eq!(waves[0].treated.len(), 3);
        assert_eq!(
            waves[0].n_control_units(),
            6,
            "an already-switching unit leaked into the control pool"
        );
    }

    /// Half-open boundary: a unit switching ON the day the post window ends
    /// (day 81 for [61, 81)) is untreated for every day inside it, so with no
    /// anticipation it IS a clean control — 6 never-treated plus the 3 late.
    #[test]
    fn experiment_waves_admit_a_control_switching_on_the_window_end() {
        let (m, mut a) = staggered_fixture(0.0, 7);
        for name in ["t3", "t4", "t5"] {
            a.switch_day.insert(name.into(), Some(81));
        }
        let (waves, _) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(waves[0].n_control_units(), 9, "day 81 is outside [61, 81)");
    }

    #[test]
    fn experiment_waves_exclude_a_control_whose_anticipation_overlaps() {
        // A control switching 3 days after the post window ends, with a 5-day
        // anticipation band, is ALREADY anticipating inside that window.
        let (m, mut a) = staggered_fixture(0.0, 7);
        a.anticipation_days = 5;
        let boundary_unit = a
            .switch_day
            .keys()
            .find(|k| a.switch_day[*k].is_none())
            .expect("a never-treated unit")
            .clone();
        // The first wave's post window is [61, 81) (washout 0). A switch on day
        // 84 with a 5-day anticipation band is contaminated from day 79 — inside it.
        a.switch_day.insert(boundary_unit.clone(), Some(84));
        let (waves, _) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(
            waves[0].switch_ord, 61,
            "the day-61 wave must still fit its windows"
        );
        assert!(
            !waves[0]
                .controls
                .iter()
                .any(|u| m.units()[*u] == boundary_unit),
            "a unit anticipating inside the post window is not a clean control"
        );
    }

    #[test]
    fn experiment_waves_refuse_an_unknown_unit_name() {
        let (m, mut a) = staggered_fixture(0.0, 7);
        a.switch_day.insert("typo_store".into(), Some(61));
        let err = build_waves(&m, &a).expect_err("must refuse, not silently skip");
        assert!(err.contains("typo_store"), "error was: {err}");
    }

    #[test]
    fn experiment_waves_drop_a_final_wave_with_no_holdout() {
        let (m, a) = staggered_fixture_no_holdout(0.0, 11);
        let (waves, dropped) = build_waves(&m, &a).expect("valid assignment");
        assert!(
            dropped.iter().any(|d| d.contains("no clean control")),
            "dropped: {dropped:?}"
        );
        assert!(!waves.is_empty(), "earlier waves still identify");
    }

    #[test]
    fn experiment_wave_att_recovers_a_planted_per_wave_effect() {
        let (m, a) = staggered_fixture(25.0, 7);
        let (waves, _) = build_waves(&m, &a).expect("valid");
        for w in &waves {
            let att = wave_att(&m, w).expect("both arms present");
            assert!(
                (att.att - 25.0).abs() < 8.0,
                "wave at {} gave {}",
                w.switch_ord,
                att.att
            );
        }
    }
}
