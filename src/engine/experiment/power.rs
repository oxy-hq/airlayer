//! Placebo power: the minimum detectable effect of a proposed design.

use crate::engine::experiment::estimate::Assignment;
use crate::engine::experiment::power_staggered::{draw_assignment, price_staggered};
use crate::engine::experiment::{
    collapse, t_quantile, welch, windows_around, PanelMatrix, SplitMix64, Windows,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, serde::Serialize)]
pub enum DesignShape {
    CommonDate {
        n_treated: usize,
        n_control: usize,
    },
    Staggered {
        wave_sizes: Vec<usize>,
        spacing_days: usize,
        n_never_treated: usize,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DesignSpec {
    pub shape: DesignShape,
    pub pre_days: usize,
    pub post_days: usize,
    pub anticipation_days: usize,
    pub washout_days: usize,
    /// Minimum share of each placebo window's calendar days present.
    pub coverage_floor: f64,
    pub alpha: f64,
    pub family: usize,
    pub power: f64,
    pub iterations: usize,
    /// EXCLUSIVE end of usable history, as a day ordinal: no placebo window
    /// reaches it. At result time the host passes the first switch minus
    /// `anticipation_days`, so the null never contains the real effect. `None`
    /// prices over all of history.
    pub history_to: Option<i64>,
    /// Strata each placebo draw ranks its units into by the draw's own
    /// pre-window mean, mirroring `propose_strata`; 0 or 1 = unblocked. It must be
    /// 0 for a switchback.
    pub blocks: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PowerResult {
    pub mde: f64,
    pub mde_relative: f64,
    pub baseline: f64,
    pub null_sd: f64,
    /// Usable placebo draws behind the MDE.
    pub iterations: usize,
    /// Distinct placebo switch days drawn — the time component is thinner than
    /// the iteration count suggests.
    pub distinct_windows: usize,
    /// Non-overlapping design spans that fit in the bounded history.
    pub independent_stretches: usize,
    pub refusal: Option<String>,
}

impl PowerResult {
    fn refused(reason: impl Into<String>) -> Self {
        Self {
            mde: f64::NAN,
            mde_relative: f64::NAN,
            baseline: f64::NAN,
            null_sd: f64::NAN,
            iterations: 0,
            distinct_windows: 0,
            independent_stretches: 0,
            refusal: Some(reason.into()),
        }
    }
}

/// Fewer non-overlapping spans than this and the null is one look wearing a
/// large iteration count.
const MIN_INDEPENDENT_STRETCHES: usize = 2;
const MAX_DOUBLINGS: usize = 40;
const MDE_STEPS_COMMON: usize = 40;

/// The calendar a design is priced over: `[first, limit)` in day ordinals.
pub(crate) struct History {
    pub(crate) first: i64,
    pub(crate) limit: i64,
    /// Days from the first wave's switch to the last's (0 for a common date).
    pub(crate) ladder: i64,
    pub(crate) stretches: usize,
}

pub(crate) struct Priced {
    pub(crate) mde: f64,
    pub(crate) null_sd: f64,
    pub(crate) usable: usize,
    pub(crate) distinct_windows: usize,
}

/// Units the shape needs, and its ladder length — or the reason it cannot be priced.
fn shape_units(shape: &DesignShape) -> Result<(usize, i64), String> {
    match shape {
        DesignShape::CommonDate {
            n_treated,
            n_control,
        } => {
            if *n_treated < 2 || *n_control < 2 {
                return Err(format!(
                    "a common-date design needs at least 2 units per arm; got {n_treated} \
                     treated and {n_control} control"
                ));
            }
            Ok((n_treated + n_control, 0))
        }
        DesignShape::Staggered {
            wave_sizes,
            spacing_days,
            n_never_treated,
        } => {
            if wave_sizes.is_empty() {
                return Err("a staggered design needs at least one wave".into());
            }
            if wave_sizes.iter().any(|s| *s < 2) {
                return Err(format!(
                    "every wave needs at least 2 units; got wave sizes {wave_sizes:?}"
                ));
            }
            let ladder = ((wave_sizes.len() - 1) * spacing_days) as i64;
            Ok((wave_sizes.iter().sum::<usize>() + n_never_treated, ladder))
        }
    }
}

/// The shared guards, in order; each message names only what it tested.
fn check_design(m: &PanelMatrix, d: &DesignSpec) -> Result<History, String> {
    if d.pre_days == 0 || d.post_days == 0 {
        return Err(format!(
            "each window must be at least 1 day long; got {} pre and {} post",
            d.pre_days, d.post_days
        ));
    }
    if d.iterations == 0 {
        return Err("a placebo needs at least 1 iteration".into());
    }
    let (need, ladder) = shape_units(&d.shape)?;
    if d.blocks >= 2 && d.blocks > need / 2 {
        return Err(format!(
            "{} blocks over the design's {need} units leave a block with fewer than 2 units; \
             at most {} fit",
            d.blocks,
            need / 2
        ));
    }
    if need > m.n_units() {
        return Err(format!(
            "the design needs {need} units and the panel holds {}",
            m.n_units()
        ));
    }
    let (Some(&first), Some(&last)) = (m.days.first(), m.days.last()) else {
        return Err("the panel holds no days".into());
    };
    let limit = d.history_to.map_or(last + 1, |h| h.min(last + 1));
    let span = (d.anticipation_days + d.pre_days + d.washout_days + d.post_days) as i64 + ladder;
    let available = (limit - first).max(0);
    let stretches = (available / span) as usize;
    if stretches < MIN_INDEPENDENT_STRETCHES {
        return Err(format!(
            "fewer than {MIN_INDEPENDENT_STRETCHES} non-overlapping {span}-day spans fit in \
             {available} days of bounded history; got {stretches}"
        ));
    }
    Ok(History {
        first,
        limit,
        ladder,
        stretches,
    })
}

/// A placebo (first) switch day whose whole ladder of windows fits `[first, limit)`.
pub(crate) fn draw_switch(rng: &mut SplitMix64, h: &History, d: &DesignSpec) -> i64 {
    let lo = h.first + (d.anticipation_days + d.pre_days) as i64;
    let hi = h.limit - (d.washout_days + d.post_days) as i64 - h.ladder;
    lo + rng.below((hi - lo + 1) as usize) as i64
}

/// What the estimator decided on one common-date draw: its t, its se, and
/// its OWN critical value (this draw's df, the registered family).
struct CommonDraw {
    t: f64,
    se: f64,
    crit: f64,
}

fn common_draws(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    arms: (usize, usize),
    seed: u64,
) -> (Vec<CommonDraw>, BTreeSet<i64>) {
    let need = arms.0 + arms.1;
    let mut rng = SplitMix64::new(seed);
    let mut order: Vec<usize> = (0..m.n_units()).collect();
    let (mut draws, mut switches) = (Vec::with_capacity(d.iterations), BTreeSet::new());
    for _ in 0..d.iterations {
        let s = draw_switch(&mut rng, h, d);
        rng.partial_shuffle(&mut order, need);
        let names: Vec<String> = order[..need].iter().map(|u| m.units[*u].clone()).collect();
        let draw_seed = rng.next_u64();
        let Some(w) = windows_around(
            m,
            s,
            d.pre_days,
            d.post_days,
            d.anticipation_days,
            d.washout_days,
            d.coverage_floor,
        ) else {
            continue;
        };
        let Ok(a) = draw_assignment(m, d, &names, s, draw_seed) else {
            continue;
        };
        let Some(draw) = common_draw(m, d, &a, &w) else {
            continue;
        };
        switches.insert(s);
        draws.push(draw);
    }
    (draws, switches)
}

/// Welch on one draw's arms, with this draw's own critical value.
fn common_draw(m: &PanelMatrix, d: &DesignSpec, a: &Assignment, w: &Windows) -> Option<CommonDraw> {
    let deltas = collapse(m, w);
    let arm = |treated: bool| -> Vec<f64> {
        deltas
            .iter()
            .filter(|x| {
                a.switch_day
                    .get(&x.unit)
                    .is_some_and(|s| s.is_some() == treated)
            })
            .map(|x| x.delta)
            .collect()
    };
    let test = welch(&arm(true), &arm(false)).ok()?;
    Some(CommonDraw {
        t: test.t,
        se: test.se,
        crit: t_quantile(test.df, d.alpha, d.family),
    })
}

fn common_power(draws: &[CommonDraw], tau: f64) -> f64 {
    draws
        .iter()
        .filter(|k| (k.t + tau / k.se).abs() >= k.crit)
        .count() as f64
        / draws.len() as f64
}

/// Smallest `tau >= 0` whose power reaches `target`: doubling from `start`
/// until it does, then `steps` bisections. Every candidate re-uses the same
/// draws, so power is monotone in `tau`. `None` when no `tau` up to
/// `start × 2^MAX_DOUBLINGS` reaches `target`.
pub(crate) fn smallest_tau(
    power_at: &mut dyn FnMut(f64) -> f64,
    target: f64,
    start: f64,
    steps: usize,
) -> Option<f64> {
    let mut hi = if start.is_finite() && start > 0.0 {
        start
    } else {
        1.0
    };
    let mut reached = false;
    for _ in 0..MAX_DOUBLINGS {
        if power_at(hi) >= target {
            reached = true;
            break;
        }
        hi *= 2.0;
    }
    if !reached {
        return None;
    }
    let mut lo = 0.0;
    for _ in 0..steps {
        let mid = (lo + hi) / 2.0;
        if power_at(mid) >= target {
            hi = mid
        } else {
            lo = mid
        }
    }
    Some(hi)
}

pub(crate) fn sd(x: &[f64]) -> f64 {
    if x.len() < 2 {
        return f64::NAN;
    }
    let mean = x.iter().sum::<f64>() / x.len() as f64;
    (x.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (x.len() - 1) as f64).sqrt()
}

pub(crate) fn usable_refusal(usable: usize, iterations: usize) -> String {
    format!("only {usable} of {iterations} placebo draws produced a usable comparison")
}

pub(crate) fn no_mde_refusal(power: f64) -> String {
    format!(
        "no injected effect up to 2^{MAX_DOUBLINGS} times the null spread reached {:.0}% power",
        power * 100.0
    )
}

/// Mean of every value on a day before `limit` — the baseline the MDE is
/// quoted against, over the same bounded history the null was drawn from.
fn bounded_mean(m: &PanelMatrix, limit: i64) -> f64 {
    let (_, hi) = m.ordinal_range(i64::MIN, limit);
    let cells = m.n_units() * hi;
    if cells == 0 {
        return f64::NAN;
    }
    (0..m.n_units())
        .map(|u| (0..hi).map(|i| m.get(u, i)).sum::<f64>())
        .sum::<f64>()
        / cells as f64
}

fn price_common(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    arms: (usize, usize),
    seed: u64,
) -> Result<Priced, String> {
    let (draws, switches) = common_draws(m, d, h, arms, seed);
    if draws.len() < d.iterations.div_ceil(2) {
        return Err(usable_refusal(draws.len(), d.iterations));
    }
    let effects: Vec<f64> = draws.iter().map(|k| k.t * k.se).collect();
    let null_sd = sd(&effects);
    let mde = smallest_tau(
        &mut |tau| common_power(&draws, tau),
        d.power,
        null_sd,
        MDE_STEPS_COMMON,
    )
    .ok_or_else(|| no_mde_refusal(d.power))?;
    Ok(Priced {
        mde,
        null_sd,
        usable: draws.len(),
        distinct_windows: switches.len(),
    })
}

pub fn placebo_power(m: &PanelMatrix, d: &DesignSpec, seed: u64) -> PowerResult {
    let h = match check_design(m, d) {
        Ok(h) => h,
        Err(reason) => return PowerResult::refused(reason),
    };
    let priced = match &d.shape {
        DesignShape::CommonDate {
            n_treated,
            n_control,
        } => price_common(m, d, &h, (*n_treated, *n_control), seed),
        DesignShape::Staggered { .. } => price_staggered(m, d, &h, seed),
    };
    let p = match priced {
        Ok(p) => p,
        Err(reason) => return PowerResult::refused(reason),
    };
    let baseline = bounded_mean(m, h.limit);
    PowerResult {
        mde: p.mde,
        mde_relative: p.mde / baseline,
        baseline,
        null_sd: p.null_sd,
        iterations: p.usable,
        distinct_windows: p.distinct_windows,
        independent_stretches: h.stretches,
        refusal: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::power_staggered::draw_assignment;
    use crate::engine::experiment::testkit::{
        common, noisy_panel, power_law_panel, staggered_design, uniform,
    };

    #[test]
    fn experiment_placebo_power_prices_a_common_date_design() {
        let p = placebo_power(&noisy_panel(24, 400, 11), &common(12, 12, 56, 56), 99);
        assert!(p.refusal.is_none(), "unexpected refusal: {:?}", p.refusal);
        assert!(p.mde > 0.0 && p.mde.is_finite());
        assert_eq!(p.independent_stretches, 400 / 112);
    }

    /// Registering more outcomes raises every draw's own threshold, so the MDE
    /// must rise. If it does not, `family` is being ignored somewhere.
    #[test]
    fn experiment_placebo_power_charges_for_a_larger_registered_family() {
        let m = noisy_panel(24, 400, 11);
        let one = placebo_power(&m, &common(12, 12, 56, 56), 99);
        let mut four = common(12, 12, 56, 56);
        four.family = 4;
        let four = placebo_power(&m, &four, 99);
        assert!(
            four.mde > one.mde * 1.02,
            "family=4 MDE {} must exceed family=1 MDE {}",
            four.mde,
            one.mde
        );
    }

    #[test]
    #[ignore = "slow: staggered placebo; run with --run-ignored only"]
    fn experiment_placebo_power_prices_staggering_in_the_same_league() {
        let m = noisy_panel(24, 400, 11);
        let big = placebo_power(&m, &common(12, 12, 56, 56), 99);
        let thin = placebo_power(&m, &staggered_design(vec![3, 3], 20, 6), 99);
        assert!(
            thin.refusal.is_none(),
            "unexpected refusal: {:?}",
            thin.refusal
        );
        let ratio = thin.mde / big.mde;
        assert!(
            (0.9..=1.8).contains(&ratio),
            "3+3 staggered {} vs 12/12 {} — ratio {ratio}",
            thin.mde,
            big.mde
        );
    }

    #[test]
    fn experiment_placebo_power_shrinks_the_mde_as_the_window_grows() {
        let m = noisy_panel(24, 400, 11);
        assert!(
            placebo_power(&m, &common(12, 12, 84, 84), 99).mde
                < placebo_power(&m, &common(12, 12, 28, 28), 99).mde
        );
    }

    /// Bounded history must keep a later jump OUT of the null. Both designs have
    /// to be priced for the comparison to mean anything — an earlier version
    /// bounded 200 days against a 112-day span, which refuses, and passed on a
    /// default `mde` of 0.0. A refusal now carries NaN, which fails `<`.
    #[test]
    fn experiment_placebo_power_honours_the_history_bound() {
        let mut rows = Vec::new();
        let mut r = SplitMix64::new(5);
        for u in 0..24 {
            for day in 1..=900i64 {
                let jump = if day > 450 && u < 12 { 900.0 } else { 0.0 };
                rows.push((
                    format!("u{u:03}"),
                    day,
                    1000.0 + uniform(&mut r, 100.0) + jump,
                ));
            }
        }
        let m = PanelMatrix::from_triples(rows);
        let unbounded = placebo_power(&m, &common(12, 12, 56, 56), 7);
        let mut d = common(12, 12, 56, 56);
        d.history_to = Some(450); // [1, 450): 449 days / 112 = 4 stretches, well clear
        let bounded = placebo_power(&m, &d, 7);
        assert!(
            unbounded.refusal.is_none() && bounded.refusal.is_none(),
            "both must be priced: {:?} / {:?}",
            unbounded.refusal,
            bounded.refusal
        );
        assert!(
            bounded.mde < unbounded.mde,
            "bounded {} must beat contaminated {}",
            bounded.mde,
            unbounded.mde
        );
    }

    #[test]
    fn experiment_placebo_power_refuses_designs_it_cannot_price() {
        let m = noisy_panel(24, 400, 11);
        assert!(placebo_power(&m, &common(1, 12, 56, 56), 9)
            .refusal
            .expect("thin arm")
            .contains("2 units per arm"));
        assert!(placebo_power(&m, &common(20, 20, 56, 56), 9)
            .refusal
            .expect("too few units")
            .contains("40 units"));
        assert!(placebo_power(&m, &common(12, 12, 0, 56), 9)
            .refusal
            .expect("zero window")
            .contains("at least 1 day"));
        assert!(
            placebo_power(&noisy_panel(24, 150, 11), &common(12, 12, 56, 56), 9)
                .refusal
                .expect("thin history")
                .contains("non-overlapping")
        );
        assert!(placebo_power(&m, &staggered_design(vec![1, 1], 20, 6), 9)
            .refusal
            .expect("thin wave")
            .contains("2 units"));
        assert!(
            placebo_power(&m, &common(1, 12, 56, 56), 9).mde.is_nan(),
            "a refused design carries NaN, never a zero MDE"
        );
    }

    /// Spec: placebo power blocks every draw the way pricing blocks. On a
    /// heavy-tailed panel an unblocked draw often puts most large units in one
    /// arm, which drops Welch's df and raises that draw's own critical value;
    /// blocking makes those draws impossible, so the blocked MDE is lower. If a
    /// correct implementation fails this, the fixture lacks heterogeneity —
    /// steepen the power law, never drop the assertion.
    #[test]
    fn experiment_placebo_power_blocks_each_draw_like_pricing() {
        let m = power_law_panel(24, 400, 11);
        let plain = placebo_power(&m, &common(12, 12, 56, 56), 99);
        let mut d = common(12, 12, 56, 56);
        d.blocks = 4;
        let blocked = placebo_power(&m, &d, 99);
        assert!(
            plain.refusal.is_none() && blocked.refusal.is_none(),
            "{:?} / {:?}",
            plain.refusal,
            blocked.refusal
        );
        assert!(
            blocked.mde < plain.mde,
            "blocked {} must beat unblocked {}",
            blocked.mde,
            plain.mde
        );
    }

    #[test]
    fn experiment_placebo_power_refuses_blocks_it_cannot_fill() {
        let mut d = common(12, 12, 56, 56);
        d.blocks = 13;
        let r = placebo_power(&noisy_panel(24, 400, 11), &d, 9)
            .refusal
            .expect("must refuse");
        assert!(
            r.contains("fewer than 2 units") && r.contains("at most 12"),
            "{r}"
        );
    }

    /// A blocked staggered draw carries its strata, so `decide` permutes within
    /// them, and every wave holds its proportional share of every stratum.
    #[test]
    fn experiment_placebo_power_blocked_staggered_draw_carries_its_strata() {
        let m = noisy_panel(24, 400, 11);
        let mut d = staggered_design(vec![4, 4], 20, 4);
        d.blocks = 2;
        let names: Vec<String> = m.units()[..12].to_vec();
        let a = draw_assignment(&m, &d, &names, 200, 5).expect("fits");
        let strata = a.strata.clone().expect("a blocked draw records its strata");
        assert_eq!(strata.len(), 2);
        for s in &strata {
            for day in [200, 220] {
                let k = s
                    .iter()
                    .filter(|u| a.switch_day[u.as_str()] == Some(day))
                    .count();
                assert_eq!(k, 2, "a wave of 4 over two strata of 6 is 2 from each");
            }
        }
        // 60 draws keep this test fast: each is an exact 8,100-relabelling test.
        d.iterations = 60;
        let p = placebo_power(&m, &d, 3);
        assert!(p.refusal.is_none() && p.mde.is_finite(), "{:?}", p.refusal);
    }
}
