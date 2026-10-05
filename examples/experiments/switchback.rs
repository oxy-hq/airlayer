//! Switchback: the whole fleet alternates on and off in week-long periods.
//! `propose_switchback` flips one seeded coin per pair to decide which half is
//! on, and `estimate_switchback` tests with the exact sign-flip test that coin
//! defines.
//!
//! Three pairs can never reach p <= 0.05 (the smallest attainable p is
//! 2/2^3 = 0.25), so that schedule is refused up front.
//!
//! Run: `cargo run --example experiment_switchback`

use airlayer::engine::experiment::{
    day_ordinal, estimate_switchback, propose_switchback, PanelMatrix, Period, SwitchbackSchedule,
};

/// Deterministic noise in [-1, 1) per (unit, day), so the output is stable.
fn noise(unit: usize, day: i64) -> f64 {
    let mut z = (unit as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (day as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z ^= z >> 31;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 29;
    (z >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
}

const PERIOD_DAYS: i64 = 7;

/// Eight kitchens; while the lever is on, each does +5/day.
fn panel_for(periods: &[Period], start: i64) -> PanelMatrix {
    let is_on = |day: i64| {
        periods
            .iter()
            .rev()
            .find(|p| p.from_day <= day)
            .is_some_and(|p| p.on)
    };
    let days = periods.len() as i64 * PERIOD_DAYS;
    let mut rows = Vec::new();
    for i in 0..8 {
        for day in start..start + days {
            let lift = if is_on(day) { 5.0 } else { 0.0 };
            rows.push((
                format!("kitchen{i}"),
                day,
                120.0 + 10.0 * i as f64 + 8.0 * noise(i, day) + lift,
            ));
        }
    }
    PanelMatrix::from_triples(rows)
}

fn run(label: &str, pairs: usize, start: i64) {
    println!("== {label}");
    let periods = propose_switchback(start, PERIOD_DAYS, pairs, 11);
    let pattern: String = periods
        .iter()
        .map(|p| if p.on { '1' } else { '0' })
        .collect();
    println!("schedule ({pairs} pairs, 1 = on): {pattern}");

    let panel = panel_for(&periods, start);
    let schedule = SwitchbackSchedule {
        periods,
        period_days: PERIOD_DAYS as usize,
        washout_days: 2,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    };
    let r = estimate_switchback(&panel, &schedule, 42);
    match &r.refusal {
        Some(why) => println!("refused (design = {:?}): {why}", r.design),
        None => println!(
            "{}: {:+.2}/unit-day  95% CI [{:+.2}, {:+.2}]  sign-flip p={:.4}  significant={}  retained ON days={}",
            r.design,
            r.estimate,
            r.ci_low,
            r.ci_high,
            r.p_value,
            r.significant,
            r.retained_on_days.unwrap_or(0)
        ),
    }
    println!();
}

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2026, 2, 2).unwrap());
    run("eight pairs of weeks, +5/day while on", 8, start);
    run("three pairs: cannot reach alpha", 3, start);
}
