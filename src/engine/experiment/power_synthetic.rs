//! Pricing a synthetic control by placebo-in-time: the same fit and the same
//! placebo-in-space test, run at past start dates where nothing happened.
//!
//! Planting a constant effect τ in the treated units' post window moves their
//! mean gap by exactly τ and changes nothing else the test reads (weights are
//! fitted on the pre window, placebos are donors), so each past date yields
//! the exact τ it would first detect — no grid search. The MDE is the
//! `power` quantile of those thresholds, the larger of the two directions.

use crate::engine::experiment::power::{PowerResult, MIN_INDEPENDENT_STRETCHES};
use crate::engine::experiment::synthetic::{fit_core, resolve, test_core, SyntheticSpec};
use crate::engine::experiment::PanelMatrix;

/// A synthetic whose pre fit is no better than a flat line at the unit's own
/// mean identifies nothing; pricing refuses at or above it.
pub const PRE_FIT_CEILING: f64 = 1.0;
/// Headroom the proposed refusal threshold leaves over the worst pre fit seen
/// in history, so ordinary drift between pricing and the run does not refuse.
const PRE_FIT_HEADROOM: f64 = 1.25;
/// Past start dates priced at most: each costs a fit per donor.
const MAX_DATES: usize = 12;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SyntheticPower {
    #[serde(flatten)]
    pub power: PowerResult,
    /// Worst treated pre-fit ratio across the priced dates.
    pub pre_fit: f64,
    /// The threshold to register as `SyntheticSpec::max_pre_fit`: the worst
    /// observed fit with headroom, never above `PRE_FIT_CEILING`.
    pub max_pre_fit: f64,
}

fn refused(reason: impl Into<String>) -> SyntheticPower {
    SyntheticPower {
        power: PowerResult::refused(reason),
        pre_fit: f64::NAN,
        max_pre_fit: f64::NAN,
    }
}

/// Past start dates, latest first, each with its whole span inside
/// `[first, limit)` and none overlapping.
fn past_starts(m: &PanelMatrix, s: &SyntheticSpec, history_to: Option<i64>) -> Vec<i64> {
    let (Some(first), Some(last)) = (m.days.first(), m.days.last()) else {
        return Vec::new();
    };
    let limit = history_to.unwrap_or(last + 1);
    let after = (s.washout_days + s.post_days) as i64;
    let before = (s.anticipation_days + s.pre_days) as i64;
    (0..MAX_DATES as i64)
        .map(|k| limit - k * (before + after) - after)
        .take_while(|start| start - before >= *first)
        .collect()
}

/// One past date: the τ each direction first detects, the treated level and
/// pre fit, and the placebo effects' spread.
struct AtDate {
    need_up: f64,
    need_down: f64,
    level: f64,
    pre_fit: f64,
    placebo_sd: f64,
}

fn at_date(m: &PanelMatrix, s: &SyntheticSpec, start: i64, seed: u64) -> Result<AtDate, String> {
    let s = SyntheticSpec {
        start_on: start,
        ..s.clone()
    };
    let r = resolve(m, &s)?;
    let core = fit_core(m, &s, &r)?;
    let t = test_core(&core, &s, seed)?;
    let bound = t.critical * t.scale;
    let k = core.treated.len() as f64;
    let effects: Vec<f64> = core.placebos.iter().map(|p| p.effect).collect();
    let mu = effects.iter().sum::<f64>() / effects.len() as f64;
    let var = effects.iter().map(|e| (e - mu) * (e - mu)).sum::<f64>() / (effects.len() - 1) as f64;
    Ok(AtDate {
        need_up: (bound - t.estimate).max(0.0),
        need_down: (bound + t.estimate).max(0.0),
        level: core.treated.iter().map(|f| f.level).sum::<f64>() / k,
        pre_fit: core
            .treated
            .iter()
            .map(|f| f.holdout_rmspe / f.pre_sd)
            .fold(0.0, f64::max),
        placebo_sd: (var / k).sqrt(),
    })
}

/// The smallest τ detected at a `power` share of dates.
fn quantile(mut needs: Vec<f64>, power: f64) -> f64 {
    needs.sort_by(f64::total_cmp);
    let idx = ((power * needs.len() as f64).ceil() as usize).clamp(1, needs.len()) - 1;
    needs[idx]
}

/// Price `s` (its `start_on` and `max_pre_fit` are ignored) over history
/// strictly before `history_to`.
pub fn price_synthetic(
    m: &PanelMatrix,
    s: &SyntheticSpec,
    power: f64,
    history_to: Option<i64>,
    seed: u64,
) -> SyntheticPower {
    if !(power.is_finite() && power > 0.0 && power < 1.0) {
        return refused(format!(
            "power must be strictly between 0 and 1; got {power}"
        ));
    }
    let priced = SyntheticSpec {
        max_pre_fit: PRE_FIT_CEILING,
        ..s.clone()
    };
    if let Err(e) = resolve(m, &priced) {
        return refused(e);
    }
    let starts = past_starts(m, &priced, history_to);
    let mut dates = Vec::new();
    let mut last_err = None;
    for (i, start) in starts.iter().enumerate() {
        match at_date(m, &priced, *start, seed.wrapping_add(i as u64)) {
            Ok(d) => dates.push(d),
            Err(e) => last_err = Some(e),
        }
    }
    if dates.len() < MIN_INDEPENDENT_STRETCHES {
        return refused(match last_err {
            Some(e) if starts.len() >= MIN_INDEPENDENT_STRETCHES => e,
            _ => format!(
                "history holds {} non-overlapping {}-day spans before the start; pricing needs \
                 at least {MIN_INDEPENDENT_STRETCHES}",
                dates.len(),
                s.anticipation_days + s.pre_days + s.washout_days + s.post_days
            ),
        });
    }
    let pre_fit = dates.iter().map(|d| d.pre_fit).fold(0.0, f64::max);
    if pre_fit.is_nan() || pre_fit >= PRE_FIT_CEILING {
        return refused(format!(
            "no combination of donors tracks the treated units before past start dates (worst \
             pre-fit {pre_fit:.2}, where 1.00 is no better than a flat line), so a synthetic \
             control would measure its own error"
        ));
    }
    let up = quantile(dates.iter().map(|d| d.need_up).collect(), power);
    let down = quantile(dates.iter().map(|d| d.need_down).collect(), power);
    let mde = up.max(down);
    let baseline = dates[0].level;
    SyntheticPower {
        power: PowerResult {
            mde,
            mde_relative: mde / baseline,
            baseline,
            null_sd: dates[0].placebo_sd,
            iterations: dates.len(),
            distinct_windows: dates.len(),
            independent_stretches: dates.len(),
            refusal: None,
        },
        pre_fit,
        max_pre_fit: (pre_fit * PRE_FIT_HEADROOM).min(PRE_FIT_CEILING),
    }
}
