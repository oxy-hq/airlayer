//! The lever as an instrument: the Wald ratio ITT(target) / ITT(driver), with
//! an Anderson–Rubin interval built by inverting the unchanged estimator on
//! `target − β·driver`.

use crate::engine::experiment::design::{
    decide_design, estimate, with_family_one, ExperimentDesign,
};
use crate::engine::experiment::estimate::{validate_assignment, Decision, EffectResult};
use crate::engine::experiment::staggered::{bisect, bracket};
use crate::engine::experiment::PanelMatrix;
use std::collections::BTreeSet;

/// The far-tail probe, beyond the last bracket probe (`scale × 2^39`).
const FAR_TAIL: f64 = (1u64 << 41) as f64;

/// Restrict two panels to the units AND days both hold, matched by NAME and by
/// ORDINAL — never by position. A unit or day only one panel holds is dropped
/// from both: a ratio of two effects is only a ratio when both are measured on
/// the same units over the same days. `estimate_ratio` refuses by name any
/// assigned unit this would drop; dropped days surface as window coverage.
pub(crate) fn align(a: &PanelMatrix, b: &PanelMatrix) -> (PanelMatrix, PanelMatrix) {
    let units: Vec<String> = a
        .units
        .iter()
        .filter(|u| b.unit_index(u).is_some())
        .cloned()
        .collect();
    let b_days: BTreeSet<i64> = b.days.iter().copied().collect();
    let days: Vec<i64> = a
        .days
        .iter()
        .copied()
        .filter(|d| b_days.contains(d))
        .collect();
    (a.restrict(&units, &days), b.restrict(&units, &days))
}

/// `target − beta·driver`, cell by cell. Both panels come out of `align`, so
/// their units and days are identical and in the same order.
pub(crate) fn minus_scaled(target: &PanelMatrix, driver: &PanelMatrix, beta: f64) -> PanelMatrix {
    debug_assert!(
        target.units == driver.units && target.days == driver.days,
        "minus_scaled needs a pair that came out of align"
    );
    let values = target
        .values
        .iter()
        .zip(&driver.values)
        .map(|(t, d)| t - beta * d)
        .collect();
    PanelMatrix {
        units: target.units.clone(),
        days: target.days.clone(),
        values,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RatioResult {
    /// ITT(target) / ITT(driver).
    pub coefficient: f64,
    /// The Anderson–Rubin set. ±inf when it is unbounded — under a weak first
    /// stage, or when it is not a single interval (`refusal` says which).
    pub ci_low: f64,
    pub ci_high: f64,
    /// The lever's effect on the driver — a reported diagnostic, tested at family 1.
    pub first_stage: EffectResult,
    pub target_effect: EffectResult,
    pub refusal: Option<String>,
}

fn refused_ratio(
    reason: String,
    first_stage: EffectResult,
    target_effect: EffectResult,
) -> RatioResult {
    RatioResult {
        coefficient: f64::NAN,
        ci_low: f64::NAN,
        ci_high: f64::NAN,
        first_stage,
        target_effect,
        refusal: Some(reason),
    }
}

/// Does the estimator's own decision reject "no effect" on `target − beta·driver`?
/// A refusal is not a rejection: a β the estimator cannot test stays in the set.
fn rejects(t: &PanelMatrix, d: &PanelMatrix, x: &ExperimentDesign, seed: u64, beta: f64) -> bool {
    matches!(
        decide_design(&minus_scaled(t, d, beta), x, seed),
        Decision::Tested {
            significant: true,
            ..
        }
    )
}

/// One side of the acceptance set, searched outward from `centre` on its own
/// bracket with the same bracket-and-bisect the permutation inversions use.
/// What differs is the two-ray check: with one instrument the acceptance region
/// is a quadratic inequality in β, so after a probe rejects the far tail is
/// tested too, and a side that is accepted again out there is not one piece.
/// `Some(±inf)` when no probe rejects; `Some(endpoint)` otherwise; `None` for
/// the two-ray case.
fn ar_endpoint(
    rejects: &dyn Fn(f64) -> bool,
    centre: f64,
    direction: f64,
    scale: f64,
) -> Option<f64> {
    let Some(outside) = bracket(rejects, centre, direction, scale) else {
        return Some(direction * f64::INFINITY);
    };
    if !rejects(centre + direction * scale * FAR_TAIL) {
        return None;
    }
    Some(bisect(rejects, centre, outside))
}

/// The Anderson–Rubin set around the Wald estimate: `Some((low, high))` when it
/// is one (possibly unbounded) interval, `None` when it is not.
fn anderson_rubin(
    t: &PanelMatrix,
    d: &PanelMatrix,
    x: &ExperimentDesign,
    seed: u64,
    centre: f64,
) -> Option<(f64, f64)> {
    let test = |beta: f64| rejects(t, d, x, seed, beta);
    let scale = centre.abs().max(1.0);
    Some((
        ar_endpoint(&test, centre, -1.0, scale)?,
        ar_endpoint(&test, centre, 1.0, scale)?,
    ))
}

/// Each panel holds what the design reads: every unit a waves assignment names,
/// or — a switchback naming none — the same fleet as the other panel.
fn check_pair(
    target: &PanelMatrix,
    driver: &PanelMatrix,
    x: &ExperimentDesign,
) -> Result<(), String> {
    let both = [("target", target, driver), ("driver", driver, target)];
    for (label, m, other) in both {
        match x {
            ExperimentDesign::Waves(a) => {
                validate_assignment(m, a).map_err(|e| format!("the {label} panel: {e}"))?
            }
            ExperimentDesign::Switchback(_) => {
                if let Some(u) = other.units.iter().find(|u| m.unit_index(u).is_none()) {
                    return Err(format!(
                        "the {label} panel lacks unit '{u}', which the other panel holds; a \
                         switchback's fleet must be the same in both"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Wald ratio ITT(target)/ITT(driver); interval by Anderson–Rubin inversion of
/// `estimate`'s own decision on `target − β·driver`, for either design.
pub fn estimate_ratio(
    target: &PanelMatrix,
    driver: &PanelMatrix,
    x: &ExperimentDesign,
    seed: u64,
) -> RatioResult {
    if let Err(reason) = check_pair(target, driver, x) {
        return refused_ratio(
            reason.clone(),
            EffectResult::refused(reason.clone()),
            EffectResult::refused(reason),
        );
    }
    let (t, d) = align(target, driver);
    let first_stage = estimate(&d, &with_family_one(x), seed);
    let target_effect = estimate(&t, x, seed);
    if let Some(r) = first_stage.refusal.clone() {
        return refused_ratio(format!("first stage: {r}"), first_stage, target_effect);
    }
    if let Some(r) = target_effect.refusal.clone() {
        return refused_ratio(format!("target: {r}"), first_stage, target_effect);
    }
    let coefficient = target_effect.estimate / first_stage.estimate;
    if !coefficient.is_finite() {
        return refused_ratio(
            "the lever's estimated effect on the driver is exactly zero, so the ratio is undefined"
                .into(),
            first_stage,
            target_effect,
        );
    }
    if let Decision::Refused(r) = decide_design(&minus_scaled(&t, &d, coefficient), x, seed) {
        return refused_ratio(
            format!("the Anderson-Rubin test at the point estimate: {r}"),
            first_stage,
            target_effect,
        );
    }
    let mut reasons = Vec::new();
    if !first_stage.significant {
        reasons.push(format!(
            "the lever did not measurably move the driver (first-stage p = {:.3} at alpha {})",
            first_stage.p_value,
            alpha_of(x)
        ));
    }
    let ar = anderson_rubin(&t, &d, x, seed, coefficient);
    let (ci_low, ci_high) = ar.unwrap_or_else(|| {
        reasons.push("the acceptance set is not a single interval".to_string());
        (f64::NEG_INFINITY, f64::INFINITY)
    });
    // The first stage is judged at family 1 but the AR tails run at the
    // registered family, so a significant first stage does not make the set
    // bounded. An infinite endpoint is a refusal in its own right.
    if ar.is_some() && (ci_low.is_infinite() || ci_high.is_infinite()) {
        let side = match (ci_low.is_infinite(), ci_high.is_infinite()) {
            (true, true) => "on both sides",
            (true, false) => "below",
            _ => "above",
        };
        reasons.push(format!(
            "the Anderson-Rubin set is unbounded {side} at the registered alpha {} and \
             family {}",
            alpha_of(x),
            family_of(x)
        ));
    }
    let refusal = (!reasons.is_empty()).then(|| reasons.join("; "));
    RatioResult {
        coefficient,
        ci_low,
        ci_high,
        first_stage,
        target_effect,
        refusal,
    }
}

/// The registered family, for the unbounded-set message.
fn family_of(x: &ExperimentDesign) -> usize {
    match x {
        ExperimentDesign::Waves(a) => a.family,
        ExperimentDesign::Switchback(s) => s.family,
    }
}

/// The registered alpha, for the weak-first-stage message.
fn alpha_of(x: &ExperimentDesign) -> f64 {
    match x {
        ExperimentDesign::Waves(a) => a.alpha,
        ExperimentDesign::Switchback(s) => s.alpha,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::design::ExperimentDesign;
    use crate::engine::experiment::estimate::Assignment;
    use crate::engine::experiment::testkit::{
        ratio_panels, staggered_ratio_panels, switchback_ratio_panels, triples_of,
    };

    /// A unit only the target holds, sorting into the MIDDLE of the order:
    /// every unit after it sits one position later in the target than in the
    /// driver. Its values are wild, so any positional pairing shows at once.
    fn with_target_only_unit(t: &PanelMatrix) -> PanelMatrix {
        let mut rows = triples_of(t);
        rows.extend((1..=60).map(|day| ("s05x".to_string(), day, 1.0e6)));
        PanelMatrix::from_triples(rows)
    }

    #[test]
    fn experiment_ratio_align_matches_units_by_name_not_position() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let t_extra = with_target_only_unit(&t);
        assert_eq!(
            (t_extra.unit_index("s06"), d.unit_index("s06")),
            (Some(7), Some(6)),
            "precondition: the two panels disagree on positions"
        );
        let (ta, da) = align(&t_extra, &d);
        assert_eq!(ta.units, d.units, "the target-only unit is dropped");
        assert_eq!(da.units, d.units);
        for (u, name) in ta.units.iter().enumerate() {
            let src = t
                .unit_index(name)
                .expect("aligned units come from the target");
            for i in 0..ta.n_days() {
                assert_eq!(
                    ta.get(u, i),
                    t.get(src, i),
                    "{name} day {i} was paired by position"
                );
            }
        }
    }

    #[test]
    fn experiment_ratio_align_drops_days_one_panel_lacks() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let d_short = PanelMatrix::from_triples(
            triples_of(&d)
                .into_iter()
                .filter(|(_, day, _)| *day != 5 && *day != 60),
        );
        let (ta, da) = align(&t, &d_short);
        assert_eq!(ta.days, da.days, "both sides keep the same ordinals");
        assert_eq!(ta.n_days(), 58);
        assert!(!ta.days.contains(&5) && !ta.days.contains(&60));
    }

    #[test]
    fn experiment_ratio_minus_scaled_is_cellwise() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let c = minus_scaled(&t, &d, 0.35);
        for u in 0..c.n_units() {
            for i in 0..c.n_days() {
                assert!((c.get(u, i) - (t.get(u, i) - 0.35 * d.get(u, i))).abs() < 1e-9);
            }
        }
    }

    fn waves(a: Assignment) -> ExperimentDesign {
        ExperimentDesign::Waves(a)
    }

    /// The first of twenty seeds whose first stage is NOT significant, with the
    /// lever moving the driver by nothing. Twenty significant null first stages
    /// in a row is a 1-in-10^26 event.
    fn first_null_first_stage(direct: f64) -> (u64, RatioResult) {
        (0..20u64)
            .map(|s| {
                let (t, d, a) = ratio_panels(0.0, 0.35, direct, 700 + s);
                (s, estimate_ratio(&t, &d, &waves(a), s))
            })
            .find(|(_, r)| r.first_stage.refusal.is_none() && !r.first_stage.significant)
            .expect("twenty null first stages cannot all be significant")
    }

    #[test]
    fn experiment_ratio_bounds_a_strong_instrument() {
        let (t, d, a) = ratio_panels(40.0, 0.35, 0.0, 11);
        let r = estimate_ratio(&t, &d, &waves(a), 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(r.first_stage.significant);
        assert_eq!(
            r.coefficient,
            r.target_effect.estimate / r.first_stage.estimate,
            "the point estimate is the Wald ratio of the two reported effects"
        );
        assert!(
            (r.coefficient - 0.35).abs() < 0.2,
            "coefficient {}",
            r.coefficient
        );
        assert!(
            r.ci_low.is_finite() && r.ci_high.is_finite(),
            "{:?}",
            (r.ci_low, r.ci_high)
        );
        assert!(r.ci_low < r.coefficient && r.coefficient < r.ci_high);
    }

    #[test]
    fn experiment_ratio_weak_first_stage_is_unbounded_and_refused() {
        let (seed, r) = first_null_first_stage(0.0);
        let reason = r
            .refusal
            .clone()
            .unwrap_or_else(|| panic!("seed {seed}: must refuse"));
        assert!(
            reason.contains("the lever did not measurably move the driver"),
            "{reason}"
        );
        assert_eq!(
            (r.ci_low, r.ci_high),
            (f64::NEG_INFINITY, f64::INFINITY),
            "a weak first stage leaves the set unbounded — reported, not invented"
        );
        assert!(
            r.coefficient.is_finite(),
            "the Wald point estimate is still reported"
        );
    }

    /// A lever that moves the target directly (an exclusion violation) and the
    /// driver not at all: the target test rejects near beta = 0 while both far
    /// tails, governed by the null first stage, accept — two rays.
    #[test]
    fn experiment_ratio_refuses_a_two_ray_acceptance_set() {
        let (seed, r) = first_null_first_stage(30.0);
        assert!(
            r.target_effect.significant,
            "precondition (seed {seed}): the target moved"
        );
        let reason = r.refusal.clone().expect("must refuse");
        assert!(
            reason.contains("the acceptance set is not a single interval"),
            "{reason}"
        );
        assert!(
            reason.contains("did not measurably move the driver"),
            "both guards fired and both are named: {reason}"
        );
        assert!(
            r.ci_low.is_infinite() && r.ci_high.is_infinite(),
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    #[test]
    fn experiment_ratio_aligns_panels_by_unit_name_end_to_end() {
        let (t, d, a) = ratio_panels(40.0, 0.35, 0.0, 21);
        let x = waves(a);
        let base = estimate_ratio(&t, &d, &x, 3);
        assert!(base.refusal.is_none(), "{:?}", base.refusal);
        let r = estimate_ratio(&with_target_only_unit(&t), &d, &x, 3);
        assert_eq!(
            (r.coefficient, r.ci_low, r.ci_high),
            (base.coefficient, base.ci_low, base.ci_high),
            "a unit only one panel holds is dropped from both, never paired by position"
        );
    }

    #[test]
    fn experiment_ratio_names_the_panel_missing_an_assigned_unit() {
        let (t, d, a) = ratio_panels(40.0, 0.35, 0.0, 21);
        let d_missing =
            PanelMatrix::from_triples(triples_of(&d).into_iter().filter(|(u, _, _)| u != "s03"));
        let r = estimate_ratio(&t, &d_missing, &waves(a), 3);
        let reason = r.refusal.clone().expect("must refuse");
        assert!(
            reason.contains("driver panel") && reason.contains("s03"),
            "{reason}"
        );
        assert!(
            r.coefficient.is_nan(),
            "a refusal carries NaN, never a zero"
        );
    }

    /// A lever that LOWERS the driver. Both effects are negative, the slope is
    /// positive, and every bracket step is taken from a scale computed on the
    /// absolute value.
    #[test]
    fn experiment_ratio_handles_a_lever_that_lowers_the_driver() {
        let (t, d, a) = ratio_panels(-40.0, 0.35, 0.0, 11);
        let r = estimate_ratio(&t, &d, &waves(a), 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            r.first_stage.estimate < 0.0 && r.target_effect.estimate < 0.0,
            "the lever lowers both the driver and, through it, the target"
        );
        assert!(
            (r.coefficient - 0.35).abs() < 0.2,
            "two negative effects make a positive slope: {}",
            r.coefficient
        );
        assert!(
            r.ci_low.is_finite()
                && r.ci_high.is_finite()
                && r.ci_low < r.coefficient
                && r.coefficient < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    /// A unit BOTH panels hold but the assignment does not name survives
    /// `align` (it is in both) and must still pair into nothing —
    /// `estimate_effect` and `decide` scope it out.
    #[test]
    fn experiment_ratio_unit_absent_from_assignment_is_in_neither_arm() {
        let (t, d, a) = ratio_panels(40.0, 0.35, 0.0, 11);
        let wild = |m: &PanelMatrix, slope: f64| {
            let mut rows = triples_of(m);
            rows.extend(
                (1..=60i64).map(|day| ("s05x".to_string(), day, 100.0 + slope * day as f64)),
            );
            PanelMatrix::from_triples(rows)
        };
        let x = waves(a);
        let base = estimate_ratio(&t, &d, &x, 3);
        let r = estimate_ratio(&wild(&t, 40.0), &wild(&d, -25.0), &x, 3);
        assert_eq!(
            format!("{r:?}"),
            format!("{base:?}"),
            "an unnamed unit entered the first stage, the target or the AR set"
        );
    }

    #[test]
    fn experiment_ratio_bounds_a_strong_instrument_on_a_staggered_design() {
        let (t, d, a) = staggered_ratio_panels(40.0, 0.35, 5);
        let r = estimate_ratio(&t, &d, &waves(a), 3);
        assert_eq!(r.first_stage.design, "staggered");
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            (r.coefficient - 0.35).abs() < 0.2,
            "coefficient {}",
            r.coefficient
        );
        assert!(
            r.ci_low.is_finite()
                && r.ci_high.is_finite()
                && r.ci_low < r.coefficient
                && r.coefficient < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    /// The ratio and its AR set for a switchback, through `estimate` and
    /// `decide_design`. Eight pairs reach alpha (2/256), and a strong first
    /// stage bounds the set.
    #[test]
    fn experiment_ratio_bounds_a_strong_instrument_on_a_switchback() {
        let (t, d, s) = switchback_ratio_panels(40.0, 0.35, 5);
        let r = estimate_ratio(&t, &d, &ExperimentDesign::Switchback(s), 3);
        assert_eq!(r.first_stage.design, "switchback");
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            (r.coefficient - 0.35).abs() < 0.2,
            "coefficient {}",
            r.coefficient
        );
        assert!(
            r.ci_low.is_finite()
                && r.ci_high.is_finite()
                && r.ci_low < r.coefficient
                && r.coefficient < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    /// A switchback names no units, so the two panels must hold the same ones —
    /// a unit only the driver has would otherwise drop out of the fleet silently.
    #[test]
    fn experiment_ratio_switchback_refuses_panels_with_different_fleets() {
        let (t, d, s) = switchback_ratio_panels(40.0, 0.35, 5);
        let t_short =
            PanelMatrix::from_triples(triples_of(&t).into_iter().filter(|(u, _, _)| u != "s07"));
        let r = estimate_ratio(&t_short, &d, &ExperimentDesign::Switchback(s), 3);
        let reason = r.refusal.expect("must refuse");
        assert!(
            reason.contains("target panel") && reason.contains("s07"),
            "{reason}"
        );
        assert!(r.coefficient.is_nan());
    }

    /// The first stage is a family-1 diagnostic; the target and the AR tails
    /// use the registered family. With a big family a first stage can clear
    /// alpha while the AR set at the stricter per-comparison rate is the whole
    /// line, and the ratio then carried (-inf, +inf) with no refusal.
    #[test]
    fn experiment_ratio_never_returns_an_unbounded_set_without_saying_so() {
        let (mut unbounded, mut runs) = (0, 0);
        for k in [4.0, 6.0, 8.0, 10.0] {
            for s in 0..40u64 {
                let (t, d, mut a) = ratio_panels(k, 0.35, 0.0, 900 + s);
                a.family = 20;
                let r = estimate_ratio(&t, &d, &waves(a), s);
                runs += 1;
                if r.ci_low.is_infinite() || r.ci_high.is_infinite() {
                    let why = r.refusal.as_deref().unwrap_or("");
                    assert!(
                        !why.is_empty(),
                        "k {k} seed {s}: ci ({}, {}) with no refusal (first stage p {})",
                        r.ci_low,
                        r.ci_high,
                        r.first_stage.p_value
                    );
                    if r.first_stage.significant {
                        // Nothing else explains the set: it must name itself.
                        let named = why.contains("unbounded") || why.contains("single interval");
                        assert!(named, "k {k} seed {s}: {why}");
                        unbounded += usize::from(why.contains("unbounded"));
                    }
                }
            }
        }
        assert!(
            unbounded > 0,
            "the fixture must reach an unbounded set behind a significant first stage in \
             {runs} runs, or this pins nothing"
        );
    }
}
