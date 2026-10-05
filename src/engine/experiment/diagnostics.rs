//! Diagnostics reported beside an estimate: the parallel-trends pre-check and
//! the size-heterogeneity evidence. Neither gates the estimate; both are read.

use crate::engine::experiment::estimate::{split_arms, Assignment};
use crate::engine::experiment::{t_quantile, welch, PanelMatrix, UnitDelta, Windows};

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

pub(crate) fn size_bias(
    _m: &PanelMatrix,
    _a: &Assignment,
    _deltas: &[UnitDelta],
    _w: &Windows,
) -> Option<SizeBias> {
    None
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
}
