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
