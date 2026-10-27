//! Each wave's own effect, beside the pooled one.
//!
//! The pooled estimate is a size-weighted mean over waves, so a wave that
//! clearly moved can be averaged away by waves that did not (BMG's May deals:
//! the May 6 first-aid kits read +11 units a day, the two waves of slow sellers
//! about 0, and the pool missed the 95% bar). The report needs the parts as
//! well as the sum. Each wave is collapsed exactly as the pooled estimate
//! collapses it — its own windows, its own clean controls, the same coverage
//! rule — and its treated and control deltas are compared with Welch.
//!
//! A wave's interval is a Welch interval at the registered per-comparison
//! alpha, NOT the permutation interval the staggered pool reports: it is a
//! description of one part, and with two or three treated units it is wide.
//! Its `p_value` is not adjusted for the number of waves, so a reader scanning
//! several waves for one that clears the bar is doing the multiple comparison
//! the registration exists to prevent. The report says so.

use crate::engine::experiment::estimate::{resolve_switch, scope, Assignment};
use crate::engine::experiment::staggered::build_waves;
use crate::engine::experiment::{
    collapse, t_quantile, t_two_sided_p, welch, windows_around, PanelMatrix, Windows,
};
use std::collections::HashSet;

/// One wave's effect. `switch_day` is the day ordinal its units switched on.
/// `refusal` is set, with every number `NaN`, when the wave cannot be
/// compared on its own (fewer than two units in an arm, or no spread).
#[derive(Debug, Clone, serde::Serialize)]
pub struct WaveEffect {
    pub switch_day: i64,
    pub n_treated: usize,
    pub n_control: usize,
    pub estimate: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub p_value: f64,
    pub refusal: Option<String>,
}

/// Every wave the estimate compared, in switch order: one entry on the common
/// switch-date path, one per estimated wave on the staggered path. Empty when
/// the assignment cannot be laid out at all — the estimate refuses then too.
pub fn wave_effects(m: &PanelMatrix, a: &Assignment) -> Vec<WaveEffect> {
    let scoped = scope(m, a);
    let m = scoped.as_ref();
    match resolve_switch(m, a) {
        Ok(s) => {
            let treated: HashSet<usize> = a
                .switch_day
                .iter()
                .filter(|(_, d)| d.is_some())
                .filter_map(|(n, _)| m.unit_index(n))
                .collect();
            let (t, c): (Vec<usize>, Vec<usize>) =
                (0..m.n_units()).partition(|u| treated.contains(u));
            windows_around(
                m,
                s,
                a.pre_days,
                a.post_days,
                a.anticipation_days,
                a.washout_days,
                a.coverage_floor,
            )
            .map(|w| vec![one_wave(m, a, s, &w, &t, &c)])
            .unwrap_or_default()
        }
        Err(_) => build_waves(m, a)
            .map(|(waves, _)| {
                waves
                    .iter()
                    .map(|w| one_wave(m, a, w.switch_ord, &w.windows, &w.treated, &w.controls))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn one_wave(
    m: &PanelMatrix,
    a: &Assignment,
    switch_day: i64,
    w: &Windows,
    treated: &[usize],
    controls: &[usize],
) -> WaveEffect {
    let deltas = collapse(m, w);
    let pick = |members: &[usize]| -> Vec<f64> {
        deltas
            .iter()
            .filter(|d| members.contains(&d.index))
            .map(|d| d.delta)
            .collect()
    };
    let (t, c) = (pick(treated), pick(controls));
    let mut out = WaveEffect {
        switch_day,
        n_treated: t.len(),
        n_control: c.len(),
        estimate: f64::NAN,
        ci_low: f64::NAN,
        ci_high: f64::NAN,
        p_value: f64::NAN,
        refusal: None,
    };
    match welch(&t, &c) {
        Ok(test) => {
            let crit = t_quantile(test.df, a.alpha, a.family);
            out.estimate = test.diff;
            out.ci_low = test.diff - crit * test.se;
            out.ci_high = test.diff + crit * test.se;
            out.p_value = t_two_sided_p(test.t, test.df);
        }
        Err(_) => {
            out.refusal = Some(format!(
                "a wave is compared on its own only with at least 2 units in each arm and \
                 some spread between them; it has {} treated and {} control",
                t.len(),
                c.len()
            ))
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::estimate::estimate_effect;
    use crate::engine::experiment::testkit::{assign, fixture, ladder, staggered_fixture};

    #[test]
    fn experiment_a_common_date_has_one_wave_equal_to_the_estimate() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let r = estimate_effect(&m, &a, 7);
        assert_eq!(r.waves.len(), 1);
        let w = &r.waves[0];
        assert_eq!((w.switch_day, w.n_treated, w.n_control), (31, 6, 6));
        assert!((w.estimate - r.estimate).abs() < 1e-12);
        assert!((w.ci_low - r.ci_low).abs() < 1e-12 && (w.p_value - r.p_value).abs() < 1e-12);
    }

    /// Two waves with different planted lifts: the pooled number sits between
    /// them, and each wave's own estimate finds its own lift.
    #[test]
    fn experiment_each_wave_reads_its_own_lift() {
        let names: Vec<String> = (0..16)
            .map(|u| match u {
                0..=3 => format!("a{u}"),
                4..=7 => format!("b{u}"),
                _ => format!("c{u}"),
            })
            .collect();
        let switch_of = |u: usize| match u {
            0..=3 => Some(41),
            4..=7 => Some(81),
            _ => None,
        };
        let (base, a) = ladder(&names, &switch_of, 120, 0.0, 5);
        // a* gain 30 from day 41; b* gain nothing.
        let rows = crate::engine::experiment::testkit::triples_of(&base)
            .into_iter()
            .map(|(u, d, v)| {
                let lift = if u.starts_with('a') && d >= 41 {
                    30.0
                } else {
                    0.0
                };
                (u, d, v + lift)
            });
        let m = PanelMatrix::from_triples(rows);
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.waves.len(), 2);
        let (early, late) = (&r.waves[0], &r.waves[1]);
        assert_eq!((early.switch_day, late.switch_day), (41, 81));
        assert!((early.estimate - 30.0).abs() < 5.0, "{}", early.estimate);
        assert!(late.estimate.abs() < 10.0, "{}", late.estimate);
        assert!(early.estimate - late.estimate > 20.0);
        assert!(early.p_value < 0.05 && early.ci_low > 0.0);
        assert!(r.estimate > late.estimate && r.estimate < early.estimate);
    }

    #[test]
    fn experiment_a_dropped_wave_is_not_listed() {
        let (m, mut a) = staggered_fixture(5.0, 3);
        // t3 alone in its own wave on day 75: the staggered estimate drops a
        // one-unit wave, so it is not a compared wave and is not listed.
        a.switch_day.insert("t3".into(), Some(75));
        let r = estimate_effect(&m, &a, 7);
        assert!(r.waves.iter().all(|w| w.switch_day != 75));
        assert!(r.waves.iter().all(|w| w.refusal.is_none()));
    }
}
