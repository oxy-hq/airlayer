//! Fixtures shared by the test modules of `engine::experiment`. Compiled only
//! under `cfg(test)`, so nothing here reaches the library build.

use crate::engine::experiment::{PanelMatrix, SplitMix64};

pub(crate) fn matrix_from(rows: &[(&str, i64, f64)]) -> PanelMatrix {
    PanelMatrix::from_triples(rows.iter().map(|(u, d, v)| (u.to_string(), *d, *v)))
}

/// Uniform on `[0, width)` from the top 24 bits of one draw.
pub(crate) fn uniform(rng: &mut SplitMix64, width: f64) -> f64 {
    (rng.next_u64() >> 40) as f64 / 16_777_216.0 * width
}

use crate::engine::experiment::estimate::Assignment;
use crate::engine::experiment::power::{DesignShape, DesignSpec};
use std::collections::HashMap;

/// Rows for `n_units` units over days `1..=n_days`. The first `n_treated` are
/// named `t{u}` and gain `effect` from `switch` on; the rest are `c{u}`.
/// Per-unit-DAY noise, not a per-unit level: a level term cancels in the
/// pre/post difference and leaves both arms with zero variance, which makes
/// welch refuse and tests nothing. Treated units are named first on purpose:
/// they sort LAST, so a positional arm match would invert the sign.
pub(crate) fn fixture_rows(
    n_units: usize,
    n_days: usize,
    n_treated: usize,
    effect: f64,
    switch: i64,
    seed: u64,
) -> Vec<(String, i64, f64)> {
    let mut rng = SplitMix64::new(seed);
    let mut rows = Vec::new();
    for u in 0..n_units {
        let name = if u < n_treated {
            format!("t{u}")
        } else {
            format!("c{u}")
        };
        for day in 1..=n_days as i64 {
            let lift = if u < n_treated && day >= switch {
                effect
            } else {
                0.0
            };
            rows.push((
                name.clone(),
                day,
                500.0 + u as f64 * 7.0 + uniform(&mut rng, 40.0) + lift,
            ));
        }
    }
    rows
}

pub(crate) fn fixture(
    n_units: usize,
    n_days: usize,
    n_treated: usize,
    effect: f64,
    switch: i64,
    seed: u64,
) -> PanelMatrix {
    PanelMatrix::from_triples(fixture_rows(
        n_units, n_days, n_treated, effect, switch, seed,
    ))
}

/// A common-date assignment over every unit in `m`: `treated` switch on
/// `switch`, the rest are controls. pre = post = 20, floor 0.9, alpha 0.05.
pub(crate) fn assign(m: &PanelMatrix, treated: &[&str], switch: i64) -> Assignment {
    let switch_day: HashMap<String, Option<i64>> = m
        .units
        .iter()
        .map(|u| (u.clone(), treated.contains(&u.as_str()).then_some(switch)))
        .collect();
    Assignment {
        switch_day,
        strata: None,
        pre_days: 20,
        post_days: 20,
        anticipation_days: 0,
        washout_days: 0,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    }
}

/// Every cell of `m` as `(unit, day, value)` — for building a variant panel.
pub(crate) fn triples_of(m: &PanelMatrix) -> Vec<(String, i64, f64)> {
    let mut rows = Vec::with_capacity(m.values.len());
    for (u, name) in m.units.iter().enumerate() {
        for (i, day) in m.days.iter().enumerate() {
            rows.push((name.clone(), *day, m.get(u, i)));
        }
    }
    rows
}

/// A panel where every unit gains `effect` from its OWN switch day on, and the
/// matching assignment (pre = post = 20, floor 0.9, alpha 0.05, family 1).
pub(crate) fn ladder(
    names: &[String],
    switch_of: &dyn Fn(usize) -> Option<i64>,
    days: i64,
    effect: f64,
    seed: u64,
) -> (PanelMatrix, Assignment) {
    let mut rng = SplitMix64::new(seed);
    let mut rows = Vec::new();
    let mut switch_day = HashMap::new();
    for (u, name) in names.iter().enumerate() {
        let s = switch_of(u);
        switch_day.insert(name.clone(), s);
        for day in 1..=days {
            let lift = if s.is_some_and(|k| day >= k) {
                effect
            } else {
                0.0
            };
            rows.push((
                name.clone(),
                day,
                500.0 + u as f64 * 7.0 + uniform(&mut rng, 40.0) + lift,
            ));
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

/// 12 units over days 1..=100: t0-t2 switch on day 61, t3-t5 on day 71,
/// c6-c11 never. With pre = post = 20 the day-71 wave switches INSIDE the
/// day-61 wave's post window [61, 81), so only the six never-treated units
/// are its clean controls.
pub(crate) fn staggered_fixture(effect: f64, seed: u64) -> (PanelMatrix, Assignment) {
    let names: Vec<String> = (0..12)
        .map(|u| {
            if u < 6 {
                format!("t{u}")
            } else {
                format!("c{u}")
            }
        })
        .collect();
    let switch_of = |u: usize| match u {
        0..=2 => Some(61),
        3..=5 => Some(71),
        _ => None,
    };
    ladder(&names, &switch_of, 100, effect, seed)
}

/// Every unit switches: e0-e3 on day 21, l4-l7 on day 41, over days 1..=60.
/// The day-41 wave clears the day-21 wave's post window [21, 41), so it is a
/// clean control there — and has no clean control of its own.
pub(crate) fn staggered_fixture_no_holdout(effect: f64, seed: u64) -> (PanelMatrix, Assignment) {
    let names: Vec<String> = (0..8)
        .map(|u| {
            if u < 4 {
                format!("e{u}")
            } else {
                format!("l{u}")
            }
        })
        .collect();
    let switch_of = |u: usize| Some(if u < 4 { 21 } else { 41 });
    ladder(&names, &switch_of, 60, effect, seed)
}

/// `units` units named `u000`… over days `1..=days`: level `1000 + 5·u` plus
/// uniform noise of width 100, no effect.
pub(crate) fn noisy_panel(units: usize, days: usize, seed: u64) -> PanelMatrix {
    let mut rng = SplitMix64::new(seed);
    let mut rows = Vec::with_capacity(units * days);
    for u in 0..units {
        for day in 1..=days as i64 {
            rows.push((
                format!("u{u:03}"),
                day,
                1000.0 + 5.0 * u as f64 + uniform(&mut rng, 100.0),
            ));
        }
    }
    PanelMatrix::from_triples(rows)
}

/// A 12/12-style common-date design spec: floor 0.9, alpha 0.05, family 1,
/// power 0.80, 4000 placebo draws over all of history.
pub(crate) fn common(nt: usize, nc: usize, pre: usize, post: usize) -> DesignSpec {
    DesignSpec {
        shape: DesignShape::CommonDate {
            n_treated: nt,
            n_control: nc,
        },
        pre_days: pre,
        post_days: post,
        anticipation_days: 0,
        washout_days: 0,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
        power: 0.80,
        iterations: 4000,
        history_to: None,
        blocks: 0,
    }
}

/// A staggered design spec with 56-day windows and the staggered arm's 300 draws.
pub(crate) fn staggered_design(sizes: Vec<usize>, spacing: usize, never: usize) -> DesignSpec {
    DesignSpec {
        shape: DesignShape::Staggered {
            wave_sizes: sizes,
            spacing_days: spacing,
            n_never_treated: never,
        },
        pre_days: 56,
        post_days: 56,
        anticipation_days: 0,
        washout_days: 0,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
        power: 0.80,
        iterations: 300,
        history_to: None,
        blocks: 0,
    }
}

/// A switchback design spec: 7-day-style periods with a 2-day washout, floor
/// 0.9, alpha 0.05, family 1, power 0.80, 1000 placebo draws over all history.
pub(crate) fn switchback_design(period_days: usize, pairs: usize) -> DesignSpec {
    DesignSpec {
        shape: DesignShape::Switchback { period_days, pairs },
        pre_days: 0,
        post_days: 0,
        anticipation_days: 0,
        washout_days: 2,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
        power: 0.80,
        iterations: 1000,
        history_to: None,
        blocks: 0,
    }
}

/// Heavy-tailed unit sizes: unit `u` has level `2000 · (u + 1)^-1.1` and
/// multiplicative daily noise of ±20%, so a unit's spread scales with its size —
/// the panel on which one unblocked draw can put every large unit in one arm.
pub(crate) fn power_law_panel(units: usize, days: usize, seed: u64) -> PanelMatrix {
    let mut rng = SplitMix64::new(seed);
    let mut rows = Vec::with_capacity(units * days);
    for u in 0..units {
        let level = 2000.0 * ((u + 1) as f64).powf(-1.1);
        for day in 1..=days as i64 {
            rows.push((
                format!("u{u:03}"),
                day,
                level * (0.8 + uniform(&mut rng, 0.4)),
            ));
        }
    }
    PanelMatrix::from_triples(rows)
}

use crate::engine::experiment::switchback::{Period, SwitchbackSchedule};

/// A switchback schedule over `periods`: floor 0.9, alpha 0.05, family 1.
pub(crate) fn schedule(
    periods: Vec<Period>,
    period_days: usize,
    washout_days: usize,
) -> SwitchbackSchedule {
    SwitchbackSchedule {
        periods,
        period_days,
        washout_days,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    }
}

/// Is the lever live on `day`: inside an on period, or within the first
/// `carry` days of the period after one (carryover). Outside the schedule, no.
pub(crate) fn lever_live(s: &SwitchbackSchedule, day: i64, carry: usize) -> bool {
    let p = s.period_days as i64;
    let Some(k) = s
        .periods
        .iter()
        .position(|x| x.from_day <= day && day < x.from_day + p)
    else {
        return false;
    };
    s.periods[k].on || (k > 0 && s.periods[k - 1].on && day - s.periods[k].from_day < carry as i64)
}

/// `m` with `effect` added to every unit on every day the lever is live.
pub(crate) fn inject_switchback(
    m: &PanelMatrix,
    s: &SwitchbackSchedule,
    effect: f64,
    carry: usize,
) -> PanelMatrix {
    let mut out = m.clone();
    for (i, day) in m.days.iter().enumerate() {
        if lever_live(s, *day, carry) {
            for u in 0..m.n_units() {
                out.values[u * m.n_days() + i] += effect;
            }
        }
    }
    out
}

/// Units `s00`… with the given switch days over `1..=days`. `effects` is
/// `(k, beta, direct)`: the lever moves the driver by `k`; the target is
/// `50 + beta·driver + direct·(lever on) + noise`, so `direct` is a lever
/// effect that bypasses the driver. Returns `(target, driver, assignment)`,
/// pre = post = 20, floor 0.9, alpha 0.05, family 1.
pub(crate) fn paired_panels(
    switch_of: &[Option<i64>],
    days: i64,
    effects: (f64, f64, f64),
    seed: u64,
) -> (PanelMatrix, PanelMatrix, Assignment) {
    let (k, beta, direct) = effects;
    let mut rng = SplitMix64::new(seed);
    let (mut target, mut driver) = (Vec::new(), Vec::new());
    let mut switch_day = HashMap::new();
    for (u, s) in switch_of.iter().enumerate() {
        let name = format!("s{u:02}");
        switch_day.insert(name.clone(), *s);
        for day in 1..=days {
            let on = s.is_some_and(|x| day >= x);
            let dv = 100.0 + 5.0 * u as f64 + uniform(&mut rng, 40.0) + if on { k } else { 0.0 };
            let tv = 50.0 + beta * dv + uniform(&mut rng, 40.0) + if on { direct } else { 0.0 };
            driver.push((name.clone(), day, dv));
            target.push((name.clone(), day, tv));
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
    (
        PanelMatrix::from_triples(target),
        PanelMatrix::from_triples(driver),
        a,
    )
}

/// 24 units over 60 days; 12 drawn at random switch on day 31.
pub(crate) fn ratio_panels(
    k: f64,
    beta: f64,
    direct: f64,
    seed: u64,
) -> (PanelMatrix, PanelMatrix, Assignment) {
    let mut order: Vec<usize> = (0..24).collect();
    SplitMix64::new(seed ^ 0xA11C).partial_shuffle(&mut order, 12);
    let mut switch_of = vec![None; 24];
    for u in &order[..12] {
        switch_of[*u] = Some(31);
    }
    paired_panels(&switch_of, 60, (k, beta, direct), seed)
}

/// 24 units over 100 days; of 12 drawn at random, 6 switch on day 61 and 6 on
/// day 71; 12 never switch.
pub(crate) fn staggered_ratio_panels(
    k: f64,
    beta: f64,
    seed: u64,
) -> (PanelMatrix, PanelMatrix, Assignment) {
    let mut order: Vec<usize> = (0..24).collect();
    SplitMix64::new(seed ^ 0xA11C).partial_shuffle(&mut order, 12);
    let mut switch_of = vec![None; 24];
    for (i, u) in order[..12].iter().enumerate() {
        switch_of[*u] = Some(if i < 6 { 61 } else { 71 });
    }
    paired_panels(&switch_of, 100, (k, beta, 0.0), seed)
}
