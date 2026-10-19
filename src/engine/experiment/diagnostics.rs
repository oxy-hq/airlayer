//! Diagnostics reported beside an estimate: the parallel-trends pre-check and
//! the size-heterogeneity evidence. Neither gates the estimate; both are read.

use crate::engine::experiment::estimate::{split_arms, Assignment};
use crate::engine::experiment::staggered::Wave;
use crate::engine::experiment::{collapse, t_quantile, welch, PanelMatrix, UnitDelta, Windows};
use std::collections::BTreeMap;

/// The same estimator run entirely inside the pre-period.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PreTrend {
    pub estimate: f64,
    pub t_stat: f64,
    pub diverged: bool,
}

/// Does a treated unit's SIZE predict its own measured effect, beyond what the
/// controls show for the same relationship? Evidence about whether the estimate
/// transports to untreated units — never a gate on totalling the treated units
/// themselves, which is exact arithmetic either way.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SizeBias {
    pub correlation_treated: f64,
    /// The same correlation among controls. It carries every artifact the
    /// treated figure does — serial correlation, regression to the mean — and
    /// none of the heterogeneity, which is why the decision is the DIFFERENCE.
    pub correlation_control: f64,
    pub z_stat: f64,
    pub significant: bool,
    pub size_window: (i64, i64),
    /// Distinct units behind each correlation — what Fisher's SE is computed on.
    pub n_treated: usize,
    pub n_control: usize,
}

/// The pre-period split in two halves ending at `end` (the switch minus the
/// anticipation band), so no new history is required and anticipation stays
/// excluded. `None` when the pre-period cannot be halved.
pub(crate) fn pre_halves(end: i64, pre_days: usize) -> Option<Windows> {
    let half = (pre_days / 2) as i64;
    if half < 1 {
        return None;
    }
    Some(Windows {
        pre: (end - 2 * half, end - half),
        post: (end - half, end),
    })
}

fn covered(m: &PanelMatrix, w: &Windows, floor: f64) -> bool {
    m.coverage(w.pre.0, w.pre.1) >= floor && m.coverage(w.post.0, w.post.1) >= floor
}

/// The estimator run inside the pre-period. `None` means the check could not
/// run (too short to halve, or too sparsely covered): absence, never a pass.
pub(crate) fn pre_trend(m: &PanelMatrix, a: &Assignment, switch_ord: i64) -> Option<PreTrend> {
    let w = pre_halves(switch_ord - a.anticipation_days as i64, a.pre_days)?;
    if !covered(m, &w, a.coverage_floor) {
        return None;
    }
    let (treated, control, _) = split_arms(m, a, &w);
    let test = welch(&treated, &control).ok()?;
    Some(PreTrend {
        estimate: test.diff,
        t_stat: test.t,
        // A diagnostic, not a registered outcome: family 1.
        diverged: test.t.abs() >= t_quantile(test.df, a.alpha, 1),
    })
}

fn pearson(pairs: &[(f64, f64)]) -> Option<f64> {
    let n = pairs.len();
    if n < 4 {
        return None; // Fisher z needs n - 3 > 0
    }
    let (mx, my) = (
        pairs.iter().map(|p| p.0).sum::<f64>() / n as f64,
        pairs.iter().map(|p| p.1).sum::<f64>() / n as f64,
    );
    let sxy: f64 = pairs.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
    let sxx: f64 = pairs.iter().map(|(x, _)| (x - mx) * (x - mx)).sum();
    let syy: f64 = pairs.iter().map(|(_, y)| (y - my) * (y - my)).sum();
    if sxx < f64::EPSILON || syy < f64::EPSILON {
        return None;
    }
    Some(sxy / (sxx * syy).sqrt())
}

/// z for H0: r_a == r_b, via Fisher's transform. Shared with the staggered path.
fn fisher_z_diff(r_a: f64, n_a: usize, r_b: f64, n_b: usize) -> Option<f64> {
    if n_a < 4 || n_b < 4 {
        return None;
    }
    let z = |r: f64| 0.5 * ((1.0 + r) / (1.0 - r)).ln();
    let se = (1.0 / (n_a - 3) as f64 + 1.0 / (n_b - 3) as f64).sqrt();
    Some((z(r_a.clamp(-0.999, 0.999)) - z(r_b.clamp(-0.999, 0.999))) / se)
}

/// Two-sided normal critical value; a diagnostic, not a registered outcome, so
/// no family correction applies.
const Z_DIAGNOSTIC: f64 = 1.96;

/// The size window for a pre-window: the same length, ending a FULL window
/// length before it starts. A gap, not merely disjoint, so persistent noise
/// has room to decay. `None` when it falls before history or under the floor.
fn size_window(m: &PanelMatrix, pre: (i64, i64), floor: f64) -> Option<(i64, i64)> {
    let len = pre.1 - pre.0;
    let win = (pre.0 - 2 * len, pre.0 - len);
    if win.0 < *m.days.first()? || m.coverage(win.0, win.1) < floor {
        return None;
    }
    Some(win)
}

fn size_bias_from(
    treated: &[(f64, f64)],
    control: &[(f64, f64)],
    window: (i64, i64),
) -> Option<SizeBias> {
    let (r_t, r_c) = (pearson(treated)?, pearson(control)?);
    let z = fisher_z_diff(r_t, treated.len(), r_c, control.len())?;
    Some(SizeBias {
        correlation_treated: r_t,
        correlation_control: r_c,
        z_stat: z,
        significant: z.abs() >= Z_DIAGNOSTIC,
        size_window: window,
        n_treated: treated.len(),
        n_control: control.len(),
    })
}

pub(crate) fn size_bias(
    m: &PanelMatrix,
    a: &Assignment,
    deltas: &[UnitDelta],
    w: &Windows,
) -> Option<SizeBias> {
    let win = size_window(m, w.pre, a.coverage_floor)?;
    let (lo, hi) = m.ordinal_range(win.0, win.1);
    let (mut treated, mut control) = (Vec::new(), Vec::new());
    for d in deltas {
        let Some(size) = m.window_mean(d.index, lo, hi) else {
            continue;
        };
        // A unit the assignment does not name is in neither arm (C1).
        match a.switch_day.get(&d.unit) {
            Some(Some(_)) => treated.push((size, d.delta)),
            Some(None) => control.push((size, d.delta)),
            None => continue,
        }
    }
    size_bias_from(&treated, &control, win)
}

/// The pre-trend check run PER WAVE, on each wave's own treated units, its own
/// clean controls and its own pre-period; combined wave-size-weighted, with
/// `diverged` when any wave diverges. `None` only when no wave's halves fit.
pub(crate) fn pre_trend_by_wave(
    m: &PanelMatrix,
    a: &Assignment,
    waves: &[Wave],
) -> Option<PreTrend> {
    let mut parts: Vec<(usize, PreTrend)> = Vec::new();
    for w in waves {
        // `windows.pre.1` is this wave's switch minus the anticipation band.
        let Some(halves) = pre_halves(w.windows.pre.1, a.pre_days) else {
            continue;
        };
        if !covered(m, &halves, a.coverage_floor) {
            continue;
        }
        let deltas = collapse(m, &halves);
        let pick = |members: &[usize]| -> Vec<f64> {
            deltas
                .iter()
                .filter(|d| members.contains(&d.index))
                .map(|d| d.delta)
                .collect()
        };
        let Ok(test) = welch(&pick(&w.treated), &pick(&w.controls)) else {
            continue;
        };
        let diverged = test.t.abs() >= t_quantile(test.df, a.alpha, 1);
        parts.push((
            w.treated.len(),
            PreTrend {
                estimate: test.diff,
                t_stat: test.t,
                diverged,
            },
        ));
    }
    let n: usize = parts.iter().map(|(k, _)| k).sum();
    if n == 0 {
        return None;
    }
    let weighted = |f: fn(&PreTrend) -> f64| {
        parts.iter().map(|(k, p)| *k as f64 * f(p)).sum::<f64>() / n as f64
    };
    Some(PreTrend {
        estimate: weighted(|p| p.estimate),
        t_stat: weighted(|p| p.t_stat),
        diverged: parts.iter().any(|(_, p)| p.diverged),
    })
}

/// Size evidence pooled across waves, each unit counted ONCE in ONE arm:
/// treated units from their own wave, never-treated units from the earliest
/// wave they control for, ever-treated units never as controls. Waves are
/// walked in switch order (`build_waves` returns them that way).
pub(crate) fn size_bias_by_wave(
    m: &PanelMatrix,
    a: &Assignment,
    waves: &[Wave],
) -> Option<SizeBias> {
    let never_treated = |row: usize| a.switch_day.get(&m.units[row]).is_none_or(Option::is_none);
    let mut treated: BTreeMap<usize, (f64, f64)> = BTreeMap::new();
    let mut control: BTreeMap<usize, (f64, f64)> = BTreeMap::new();
    let mut first_window = None;
    for w in waves {
        let Some(win) = size_window(m, w.windows.pre, a.coverage_floor) else {
            continue;
        };
        first_window.get_or_insert(win);
        let (lo, hi) = m.ordinal_range(win.0, win.1);
        for d in collapse(m, &w.windows) {
            let Some(size) = m.window_mean(d.index, lo, hi) else {
                continue;
            };
            if w.treated.contains(&d.index) {
                treated.insert(d.index, (size, d.delta));
            } else if w.controls.contains(&d.index) && never_treated(d.index) {
                control.entry(d.index).or_insert((size, d.delta));
            }
        }
    }
    let t: Vec<(f64, f64)> = treated.into_values().collect();
    let c: Vec<(f64, f64)> = control.into_values().collect();
    size_bias_from(&t, &c, first_window?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::estimate::estimate_simple;
    use crate::engine::experiment::testkit::{assign, fixture, uniform};
    use crate::engine::experiment::SplitMix64;

    /// A differential PRE-period trend that stops at the switch. The ramp
    /// flattens on day 31, so the headline estimate (~29, 10.5 x the mean
    /// slope) is just the level the ramp left behind, and reads as an effect
    /// of the switch, while the pre-trend reads the ramp itself and is loud.
    /// That is the shape that shows the check adds information the headline
    /// lacks.
    fn diverging_fixture(seed: u64) -> (PanelMatrix, Assignment) {
        let mut rng = SplitMix64::new(seed);
        let mut rows = Vec::new();
        for u in 0..12 {
            let name = if u < 6 {
                format!("t{u}")
            } else {
                format!("c{u}")
            };
            let slope = if u < 6 { 2.0 + u as f64 * 0.3 } else { 0.0 };
            for day in 1..=60i64 {
                let ramp = slope * (day.min(31) as f64); // flattens at the switch
                rows.push((name.clone(), day, 500.0 + uniform(&mut rng, 20.0) + ramp));
            }
        }
        let m = PanelMatrix::from_triples(rows);
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        (m, a)
    }

    #[test]
    fn experiment_pre_trend_flags_arms_that_were_already_diverging() {
        let (m, a) = diverging_fixture(9);
        let pre = estimate_simple(&m, &a).pre_trend.expect("must be reported");
        assert!(
            pre.diverged,
            "a differential pre-trend must be flagged, t = {}",
            pre.t_stat
        );
    }

    #[test]
    fn experiment_pre_trend_is_quiet_when_arms_track_each_other() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let r = estimate_simple(&m, &assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31));
        assert!(!r.pre_trend.expect("must be reported").diverged);
    }

    #[test]
    fn experiment_pre_trend_reports_absence_rather_than_a_pass() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.pre_days = 1; // cannot be split in half
        assert!(estimate_simple(&m, &a).pre_trend.is_none());
    }

    use crate::engine::experiment::windows_around;

    const TREATED_12: [&str; 12] = [
        "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11",
    ];

    /// Per-unit AR(1) noise with coefficient `rho` and shock sd 30, no effect.
    /// The first half of the units are named `t{u}` so `TREATED_12` addresses them.
    fn ar1_fixture(units: usize, days: usize, rho: f64, seed: u64) -> PanelMatrix {
        let mut rng = SplitMix64::new(seed);
        let half_width = 30.0 * 3f64.sqrt(); // uniform on +-30*sqrt(3) has sd 30
        let mut rows = Vec::new();
        for u in 0..units {
            let name = if u < units / 2 {
                format!("t{u}")
            } else {
                format!("c{u}")
            };
            let mut prev = 0.0;
            for day in 1..=days as i64 {
                prev = rho * prev + uniform(&mut rng, 2.0 * half_width) - half_width;
                rows.push((name.clone(), day, 1000.0 + u as f64 * 7.0 + prev));
            }
        }
        PanelMatrix::from_triples(rows)
    }

    /// 24 units over 300 days; t0-t11 switch on day 221 and gain 10% of their
    /// own level, so a treated unit's effect scales with its size.
    fn size_scaled_fixture(seed: u64) -> (PanelMatrix, Assignment) {
        let mut rng = SplitMix64::new(seed);
        let mut rows = Vec::new();
        for u in 0..24usize {
            let name = if u < 12 {
                format!("t{u}")
            } else {
                format!("c{u}")
            };
            let level = 500.0 + 40.0 * u as f64;
            for day in 1..=300i64 {
                let lift = if u < 12 && day >= 221 {
                    0.1 * level
                } else {
                    0.0
                };
                rows.push((name.clone(), day, level + uniform(&mut rng, 40.0) + lift));
            }
        }
        let m = PanelMatrix::from_triples(rows);
        let a = assign(&m, &TREATED_12, 221);
        (m, a)
    }

    #[test]
    fn experiment_size_bias_does_not_fire_on_pure_noise() {
        let mut fired = 0;
        for seed in 0..40u64 {
            let m = fixture(24, 300, 12, 0.0, 221, seed);
            let a = assign(&m, &TREATED_12, 221);
            if estimate_simple(&m, &a)
                .size_bias
                .is_some_and(|b| b.significant)
            {
                fired += 1;
            }
        }
        assert!(
            fired <= 6,
            "fired {fired}/40 times under a pure null, want ~2"
        );
    }

    /// The case a disjoint window alone does not handle: persistent noise makes
    /// the size window lean on the pre-period. Both arms carry that lean, so the
    /// treated-vs-control difference must stay quiet.
    #[test]
    fn experiment_size_bias_stays_quiet_under_serial_correlation() {
        let mut fired = 0;
        for seed in 0..40u64 {
            let m = ar1_fixture(24, 300, 0.95, seed);
            let a = assign(&m, &TREATED_12, 221);
            let sb = estimate_simple(&m, &a).size_bias.expect("reported");
            assert!(
                sb.correlation_control.is_finite(),
                "the control arm's r must be reported"
            );
            if sb.significant {
                fired += 1
            }
        }
        assert!(
            fired <= 6,
            "fired {fired}/40 under AR(1) with no heterogeneity"
        );
    }

    #[test]
    fn experiment_size_bias_flags_an_effect_that_scales_with_unit_size() {
        let (m, a) = size_scaled_fixture(9);
        let sb = estimate_simple(&m, &a).size_bias.expect("must be reported");
        assert!(
            sb.correlation_treated > 0.7,
            "treated r was {}",
            sb.correlation_treated
        );
        assert!(
            sb.significant,
            "treated r {} vs control r {} must differ",
            sb.correlation_treated, sb.correlation_control
        );
    }

    #[test]
    fn experiment_size_bias_leaves_a_gap_before_the_pre_window() {
        let m = fixture(24, 300, 12, 20.0, 221, 4);
        let a = assign(&m, &TREATED_12, 221);
        let sb = estimate_simple(&m, &a).size_bias.expect("reported");
        let w = windows_around(&m, 221, 20, 20, 0, 0, 0.9).expect("fits");
        assert!(
            sb.size_window.1 <= w.pre.0 - (w.pre.1 - w.pre.0),
            "size window {:?} must end a full window length before pre starts at {}",
            sb.size_window,
            w.pre.0
        );
    }

    use crate::engine::experiment::estimate::estimate_effect;
    use crate::engine::experiment::testkit::{staggered_fixture, staggered_fixture_no_holdout};
    use std::collections::HashMap;

    /// 24 units over 300 days: t0-t5 switch on day 221, t6-t11 on day 241,
    /// c12-c23 never; every treated unit gains 10% of its own level.
    fn staggered_size_scaled_fixture(seed: u64) -> (PanelMatrix, Assignment) {
        let mut rng = SplitMix64::new(seed);
        let mut rows = Vec::new();
        let mut switch_day = HashMap::new();
        for u in 0..24usize {
            let name = if u < 12 {
                format!("t{u}")
            } else {
                format!("c{u}")
            };
            let s = match u {
                0..=5 => Some(221),
                6..=11 => Some(241),
                _ => None,
            };
            switch_day.insert(name.clone(), s);
            let level = 500.0 + 40.0 * u as f64;
            for day in 1..=300i64 {
                let lift = if s.is_some_and(|k| day >= k) {
                    0.1 * level
                } else {
                    0.0
                };
                rows.push((name.clone(), day, level + uniform(&mut rng, 40.0) + lift));
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
    fn experiment_size_bias_is_reported_on_the_staggered_path_too() {
        let (m, a) = staggered_size_scaled_fixture(9);
        let sb = estimate_effect(&m, &a, 2)
            .size_bias
            .expect("staggered must report it too");
        assert!(sb.significant, "a size-scaled effect must be flagged");
    }

    #[test]
    fn experiment_size_bias_staggered_is_quiet_on_a_flat_effect() {
        let (m, a) = staggered_fixture(25.0, 7); // same +25 at every unit
        let sb = estimate_effect(&m, &a, 2).size_bias.expect("reported");
        assert!(
            !sb.significant,
            "a flat effect must not read as size-dependent"
        );
    }

    /// Six never-treated units serve both waves. They must count once.
    #[test]
    fn experiment_size_bias_staggered_counts_each_control_once() {
        let (m, a) = staggered_fixture(25.0, 7);
        let sb = estimate_effect(&m, &a, 2).size_bias.expect("reported");
        assert_eq!(sb.n_control, 6, "6 distinct controls, not 6 x waves");
        assert_eq!(sb.n_treated, 6, "3 + 3 treated, each in its own wave");
    }

    #[test]
    fn experiment_size_bias_is_absent_without_a_never_treated_arm() {
        let (m, a) = staggered_fixture_no_holdout(0.0, 11);
        assert!(
            estimate_effect(&m, &a, 2).size_bias.is_none(),
            "no never-treated units means no control arm - absent, not faked"
        );
    }
}
