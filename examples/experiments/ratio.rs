//! The lever as an instrument: the lever adds staffed hours (the driver) and
//! you want revenue (the target) per extra hour. `estimate_ratio` returns the
//! Wald ratio ITT(revenue) / ITT(hours) with an Anderson–Rubin set, which stays
//! valid when the lever barely moves the driver.
//!
//! Three runs: a strong lever; a weak lever, where the coefficient and the set
//! are kept but `refusal` says the first stage is not significant (a soft
//! refusal); and a driver panel missing a treated store (a hard refusal).
//!
//! Run: `cargo run --example experiment_ratio`

use airlayer::engine::experiment::{
    day_ordinal, estimate_ratio, Assignment, ExperimentDesign, PanelMatrix, RatioResult,
};
use std::collections::HashMap;

/// Deterministic noise in [-1, 1) per (unit, day, stream), so the output is stable.
fn noise(unit: usize, day: i64, stream: u64) -> f64 {
    let mut z = (unit as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (day as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9)
        ^ stream.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    z ^= z >> 31;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 29;
    (z >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
}

const REVENUE_PER_HOUR: f64 = 2.0;

/// (target, driver) panels: treated stores gain `extra_hours`/day after the
/// switch, and revenue follows hours at `REVENUE_PER_HOUR`.
fn panels(
    stores: &[String],
    start: i64,
    switch: i64,
    extra_hours: f64,
) -> (PanelMatrix, PanelMatrix) {
    let (mut target, mut driver) = (Vec::new(), Vec::new());
    for (i, s) in stores.iter().enumerate() {
        for day in start..start + 70 {
            let lever = if i % 2 == 0 && day >= switch {
                extra_hours
            } else {
                0.0
            };
            let hours = 40.0 + 2.0 * i as f64 + 1.5 * noise(i, day, 1) + lever;
            let revenue = 300.0 + REVENUE_PER_HOUR * hours + 4.0 * noise(i, day, 2);
            driver.push((s.clone(), day, hours));
            target.push((s.clone(), day, revenue));
        }
    }
    (
        PanelMatrix::from_triples(target),
        PanelMatrix::from_triples(driver),
    )
}

fn report(label: &str, r: &RatioResult) {
    println!("== {label}");
    let fs = &r.first_stage;
    let te = &r.target_effect;
    println!(
        "first stage (hours): {:+.2}/day p={:.4} significant={}",
        fs.estimate, fs.p_value, fs.significant
    );
    println!(
        "target effect (revenue): {:+.2}/day p={:.4} significant={}",
        te.estimate, te.p_value, te.significant
    );
    println!(
        "revenue per hour: {:.2}  Anderson–Rubin 95% set [{:.2}, {:.2}]",
        r.coefficient, r.ci_low, r.ci_high
    );
    match &r.refusal {
        Some(why) => println!("refusal: {why}"),
        None => println!("refusal: none"),
    }
    println!();
}

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
    let switch = start + 35;
    let stores: Vec<String> = (0..12).map(|i| format!("store{i:02}")).collect();
    let design = ExperimentDesign::Waves(Assignment {
        switch_day: stores
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), (i % 2 == 0).then_some(switch)))
            .collect::<HashMap<_, _>>(),
        strata: None,
        pre_days: 28,
        post_days: 28,
        anticipation_days: 0,
        washout_days: 7,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    });

    let (target, driver) = panels(&stores, start, switch, 3.0);
    report(
        "strong lever: +3 staffed hours/day",
        &estimate_ratio(&target, &driver, &design, 42),
    );

    let (target, driver) = panels(&stores, start, switch, 0.1);
    report(
        "weak lever: +0.1 staffed hours/day (soft refusal, numbers kept)",
        &estimate_ratio(&target, &driver, &design, 42),
    );

    // The driver feed lost store00, a treated store: refused by name.
    let (target, _) = panels(&stores, start, switch, 3.0);
    let (_, driver) = panels(&stores[1..], start, switch, 3.0);
    report(
        "driver panel missing a treated store (hard refusal)",
        &estimate_ratio(&target, &driver, &design, 42),
    );
}
