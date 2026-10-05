//! The switchback arm of placebo power: the pair schedule slid through bounded
//! pre-history, oriented by coin, the effect injected into the on periods, and
//! the estimator's own sign-flip rule applied.

use crate::engine::experiment::power::{
    bounded_history, no_mde_refusal, sd, smallest_tau, usable_refusal, DesignSpec, History, Priced,
};
use crate::engine::experiment::switchback::{
    pair_diffs, propose_switchback, sign_flip_test, test_pairs, unreachable_pairs,
    validate_period_washout, SwitchbackSchedule,
};
use crate::engine::experiment::{per_comparison_alpha, PanelMatrix, SplitMix64};
use std::collections::BTreeSet;

/// Bisection steps on the switchback MDE; each re-tests every draw exactly.
const MDE_STEPS_SWITCHBACK: usize = 25;

/// The fields a switchback has no use for must be 0; nonzero is refused by name.
fn check_unused(d: &DesignSpec) -> Result<(), String> {
    if d.pre_days != 0 || d.post_days != 0 || d.anticipation_days != 0 {
        return Err(format!(
            "a switchback has no pre or post window: pre_days, post_days and \
             anticipation_days must be 0; got {}, {}, {}",
            d.pre_days, d.post_days, d.anticipation_days
        ));
    }
    if d.blocks != 0 {
        return Err(format!(
            "a switchback is not blocked: blocks must be 0; got {}",
            d.blocks
        ));
    }
    Ok(())
}

/// The switchback's own guards, in order, and the calendar it is priced over.
/// The alpha guard runs before any history is read: a design the estimator
/// would refuse is refused here, not priced on draws that can never reject.
pub(crate) fn check_switchback(
    m: &PanelMatrix,
    d: &DesignSpec,
    period_days: usize,
    pairs: usize,
) -> Result<History, String> {
    check_unused(d)?;
    validate_period_washout(period_days, d.washout_days)?;
    if pairs == 0 {
        return Err("a switchback needs at least 1 pair of periods; pairs was 0".into());
    }
    if d.iterations == 0 {
        return Err("a placebo needs at least 1 iteration".into());
    }
    let alpha = per_comparison_alpha(d.alpha, d.family);
    let min_p = 2.0 / 2f64.powi(pairs as i32);
    if min_p > alpha {
        return Err(unreachable_pairs(pairs, alpha, min_p));
    }
    let span = (2 * pairs * period_days) as i64;
    bounded_history(m, d, span, span)
}

/// Each draw's retained pair differences, under a schedule slid to a random
/// start and oriented by coin; draws the rule refuses are skipped.
fn switchback_draws(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    shape: (usize, usize),
    seed: u64,
) -> (Vec<Vec<f64>>, BTreeSet<i64>) {
    let (period_days, pairs) = shape;
    let alpha = per_comparison_alpha(d.alpha, d.family);
    let mut rng = SplitMix64::new(seed);
    let (mut draws, mut starts) = (Vec::with_capacity(d.iterations), BTreeSet::new());
    for _ in 0..d.iterations {
        // `h.ladder` is the whole span; a start at most `limit - span` keeps
        // every period inside `[first, limit)`.
        let s0 = h.first + rng.below((h.limit - h.ladder - h.first + 1) as usize) as i64;
        let s = SwitchbackSchedule {
            periods: propose_switchback(s0, period_days as i64, pairs, rng.next_u64()),
            period_days,
            washout_days: d.washout_days,
            coverage_floor: d.coverage_floor,
            alpha: d.alpha,
            family: d.family,
        };
        let diffs = pair_diffs(m, &s).diffs;
        if test_pairs(&diffs, 0.0, alpha, 0).is_err() {
            continue;
        }
        starts.insert(s0);
        draws.push(diffs);
    }
    (draws, starts)
}

/// Share of draws the sign-flip rule rejects once `tau` is in every on period.
fn switchback_power(draws: &[Vec<f64>], tau: f64, alpha: f64, seed: u64) -> f64 {
    let rejected = draws
        .iter()
        .enumerate()
        .filter(|(k, diffs)| {
            let shifted: Vec<f64> = diffs.iter().map(|x| x + tau).collect();
            sign_flip_test(&shifted, 0.0, alpha, seed.wrapping_add(*k as u64)).rejected
        })
        .count();
    rejected as f64 / draws.len().max(1) as f64
}

pub(crate) fn price_switchback(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    shape: (usize, usize),
    seed: u64,
) -> Result<Priced, String> {
    let (draws, starts) = switchback_draws(m, d, h, shape, seed);
    if draws.len() < d.iterations.div_ceil(2) {
        return Err(usable_refusal(draws.len(), d.iterations));
    }
    let null: Vec<f64> = draws
        .iter()
        .map(|x| x.iter().sum::<f64>() / x.len() as f64)
        .collect();
    let null_sd = sd(&null);
    let alpha = per_comparison_alpha(d.alpha, d.family);
    let mde = smallest_tau(
        &mut |tau| switchback_power(&draws, tau, alpha, seed),
        d.power,
        null_sd,
        MDE_STEPS_SWITCHBACK,
    )
    .ok_or_else(|| no_mde_refusal(d.power))?;
    Ok(Priced {
        mde,
        null_sd,
        usable: draws.len(),
        distinct_windows: starts.len(),
    })
}

#[cfg(test)]
mod tests {
    use crate::engine::experiment::power::placebo_power;
    use crate::engine::experiment::testkit::{noisy_panel, switchback_design};

    #[test]
    fn experiment_placebo_power_prices_a_switchback() {
        let p = placebo_power(&noisy_panel(24, 400, 11), &switchback_design(7, 6), 9);
        assert!(p.refusal.is_none(), "{:?}", p.refusal);
        assert!(p.mde > 0.0 && p.mde.is_finite());
        assert_eq!(
            p.independent_stretches,
            400 / 84,
            "6 pairs of 7 days is an 84-day span"
        );
    }

    /// The spec's BMG example, priced: four pairs cannot reach 0.05 and can 0.2.
    /// The refusal comes before any draw — it holds on a panel too short to
    /// draw from at all, which would otherwise refuse for its history.
    #[test]
    fn experiment_placebo_power_switchback_needs_enough_pairs_for_alpha() {
        let m = noisy_panel(24, 400, 11);
        let mut d = switchback_design(7, 4);
        let p = placebo_power(&m, &d, 9);
        let r = p.refusal.expect("4 pairs at 0.05");
        assert!(r.contains("cannot reach") && r.contains("2/2^4"), "{r}");
        assert!(
            p.mde.is_nan() && p.iterations == 0,
            "refused up front: nothing was drawn"
        );
        let short = placebo_power(&noisy_panel(24, 20, 11), &d, 9)
            .refusal
            .expect("refused");
        assert!(
            short.contains("cannot reach"),
            "the alpha guard runs before history: {short}"
        );
        d.alpha = 0.2;
        assert!(
            placebo_power(&m, &d, 9).refusal.is_none(),
            "4 pairs reach 0.2"
        );
    }

    #[test]
    fn experiment_placebo_power_switchback_shrinks_with_more_pairs() {
        let m = noisy_panel(24, 400, 11);
        let six = placebo_power(&m, &switchback_design(7, 6), 9);
        let ten = placebo_power(&m, &switchback_design(7, 10), 9);
        assert!(
            ten.mde < six.mde,
            "10 pairs {} must beat 6 pairs {}",
            ten.mde,
            six.mde
        );
    }

    #[test]
    fn experiment_placebo_power_switchback_refuses_fields_it_has_no_use_for() {
        let m = noisy_panel(24, 400, 11);
        let refuse = |f: &dyn Fn(&mut crate::engine::experiment::DesignSpec)| {
            let mut d = switchback_design(7, 6);
            f(&mut d);
            placebo_power(&m, &d, 9).refusal.expect("must refuse")
        };
        assert!(refuse(&|d| d.pre_days = 56).contains("must be 0; got 56, 0, 0"));
        assert!(refuse(&|d| d.blocks = 2).contains("blocks must be 0; got 2"));
        assert!(refuse(&|d| d.washout_days = 7).contains("shorter than period_days"));
        assert!(refuse(
            &|d| d.shape = crate::engine::experiment::DesignShape::Switchback {
                period_days: 7,
                pairs: 0
            }
        )
        .contains("pairs was 0"));
        let mut d = switchback_design(7, 6);
        d.blocks = 2;
        assert!(
            placebo_power(&m, &d, 9).mde.is_nan(),
            "a refusal carries NaN"
        );
    }
}
