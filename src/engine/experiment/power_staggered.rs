//! The staggered arm of placebo power, and the design layout it and the
//! calibration tests share: which unit switches when, and a panel with an
//! effect injected from each treated unit's own switch day.

use crate::engine::experiment::estimate::{decide, Assignment, Decision};
use crate::engine::experiment::power::{
    draw_switch, no_mde_refusal, sd, smallest_tau, usable_refusal, DesignShape, DesignSpec,
    History, Priced,
};
use crate::engine::experiment::strata::blocked_layout;
use crate::engine::experiment::{PanelMatrix, SplitMix64};
use std::collections::{BTreeSet, HashMap};

/// Bisection steps on the staggered MDE: resolution `hi / 4096`. Each step
/// re-runs the permutation decision in every draw.
const MDE_STEPS_STAGGERED: usize = 12;

/// Switch days for `names` laid out as consecutive waves of `sizes`, `spacing`
/// days apart from `first_switch`; every name after them is a control.
pub(crate) fn lay_out(
    names: &[String],
    sizes: &[usize],
    first_switch: i64,
    spacing: usize,
) -> HashMap<String, Option<i64>> {
    let mut switch_day = HashMap::new();
    let mut at = 0;
    for (i, size) in sizes.iter().enumerate() {
        for name in &names[at..at + size] {
            switch_day.insert(name.clone(), Some(first_switch + (i * spacing) as i64));
        }
        at += size;
    }
    for name in &names[at..] {
        switch_day.insert(name.clone(), None);
    }
    switch_day
}

/// The assignment the estimator would receive for this design.
pub(crate) fn assignment_for(
    d: &DesignSpec,
    switch_day: HashMap<String, Option<i64>>,
) -> Assignment {
    Assignment {
        switch_day,
        strata: None, // a blocked draw sets it
        pre_days: d.pre_days,
        post_days: d.post_days,
        anticipation_days: d.anticipation_days,
        washout_days: d.washout_days,
        coverage_floor: d.coverage_floor,
        alpha: d.alpha,
        family: d.family,
    }
}

/// The design's wave sizes and spacing: a common date is one wave.
pub(crate) fn waves_of(shape: &DesignShape) -> (Vec<usize>, usize) {
    match shape {
        DesignShape::CommonDate { n_treated, .. } => (vec![*n_treated], 0),
        DesignShape::Staggered {
            wave_sizes,
            spacing_days,
            ..
        } => (wave_sizes.clone(), *spacing_days),
        // A switchback lays out no waves; `draw_assignment` is never called for one.
        DesignShape::Switchback { .. } => (Vec::new(), 0),
    }
}

/// The assignment one draw lays over `names` from `first_switch`: plain
/// `lay_out`, or — with `blocks >= 2` — strata ranked on THIS draw's pre-window
/// and every wave allocated within them, exactly as pricing does. The strata
/// travel on the assignment, so `decide` permutes within them.
pub(crate) fn draw_assignment(
    m: &PanelMatrix,
    d: &DesignSpec,
    names: &[String],
    first_switch: i64,
    seed: u64,
) -> Result<Assignment, String> {
    let (sizes, spacing) = waves_of(&d.shape);
    if d.blocks < 2 {
        return Ok(assignment_for(
            d,
            lay_out(names, &sizes, first_switch, spacing),
        ));
    }
    let pre_end = first_switch - d.anticipation_days as i64;
    let pre = (pre_end - d.pre_days as i64, pre_end);
    let (waves, strata) = blocked_layout(m, names, &sizes, d.blocks, pre, seed)?;
    let mut switch_day: HashMap<String, Option<i64>> =
        names.iter().map(|n| (n.clone(), None)).collect();
    for (i, wave) in waves.iter().enumerate() {
        for n in wave {
            switch_day.insert(n.clone(), Some(first_switch + (i * spacing) as i64));
        }
    }
    let mut a = assignment_for(d, switch_day);
    a.strata = Some(strata);
    Ok(a)
}

/// `m` with `tau` added to every treated unit from its own switch day on.
pub(crate) fn inject(m: &PanelMatrix, a: &Assignment, tau: f64) -> PanelMatrix {
    let mut out = m.clone();
    let n_days = m.days.len();
    for (u, name) in m.units.iter().enumerate() {
        let Some(Some(s)) = a.switch_day.get(name) else {
            continue;
        };
        let from = m.days.partition_point(|d| d < s);
        for v in &mut out.values[u * n_days + from..(u + 1) * n_days] {
            *v += tau;
        }
    }
    out
}

/// One placebo draw: the sub-panel of exactly the design's units over bounded
/// history, and the assignment laid over it.
struct StaggeredDraw {
    panel: PanelMatrix,
    assignment: Assignment,
}

fn staggered_draws(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    seed: u64,
) -> (Vec<StaggeredDraw>, BTreeSet<i64>) {
    let DesignShape::Staggered {
        wave_sizes,
        n_never_treated,
        ..
    } = &d.shape
    else {
        return (Vec::new(), BTreeSet::new());
    };
    let need = wave_sizes.iter().sum::<usize>() + n_never_treated;
    let days: Vec<i64> = m.days.iter().copied().filter(|x| *x < h.limit).collect();
    let mut rng = SplitMix64::new(seed);
    let mut order: Vec<usize> = (0..m.n_units()).collect();
    let (mut draws, mut switches) = (Vec::with_capacity(d.iterations), BTreeSet::new());
    for _ in 0..d.iterations {
        let s0 = draw_switch(&mut rng, h, d);
        rng.partial_shuffle(&mut order, need);
        let names: Vec<String> = order[..need].iter().map(|u| m.units[*u].clone()).collect();
        let Ok(assignment) = draw_assignment(m, d, &names, s0, rng.next_u64()) else {
            continue;
        };
        switches.insert(s0);
        draws.push(StaggeredDraw {
            panel: m.restrict(&names, &days),
            assignment,
        });
    }
    (draws, switches)
}

/// Share of draws the estimator's own decision rejects once `tau` is injected.
/// Draws it refuses are left out of numerator and denominator alike.
fn staggered_power(draws: &[StaggeredDraw], tau: f64, seed: u64) -> f64 {
    let (mut tested, mut rejected) = (0usize, 0usize);
    for (k, dr) in draws.iter().enumerate() {
        let panel = inject(&dr.panel, &dr.assignment, tau);
        if let Decision::Tested { significant, .. } =
            decide(&panel, &dr.assignment, seed.wrapping_add(k as u64))
        {
            tested += 1;
            rejected += usize::from(significant);
        }
    }
    if tested == 0 {
        0.0
    } else {
        rejected as f64 / tested as f64
    }
}

pub(crate) fn price_staggered(
    m: &PanelMatrix,
    d: &DesignSpec,
    h: &History,
    seed: u64,
) -> Result<Priced, String> {
    let (draws, switches) = staggered_draws(m, d, h, seed);
    let null: Vec<f64> = draws
        .iter()
        .enumerate()
        .filter_map(|(k, dr)| {
            match decide(&dr.panel, &dr.assignment, seed.wrapping_add(k as u64)) {
                Decision::Tested { estimate, .. } => Some(estimate),
                Decision::Refused(_) => None,
            }
        })
        .collect();
    if null.len() < d.iterations.div_ceil(2) {
        return Err(usable_refusal(null.len(), d.iterations));
    }
    let null_sd = sd(&null);
    let mde = smallest_tau(
        &mut |tau| staggered_power(&draws, tau, seed),
        d.power,
        null_sd,
        MDE_STEPS_STAGGERED,
    )
    .ok_or_else(|| no_mde_refusal(d.power))?;
    Ok(Priced {
        mde,
        null_sd,
        usable: null.len(),
        distinct_windows: switches.len(),
    })
}
