//! Common switch date: six of twelve stores turn the lever on the same day.
//!
//! Each store collapses to one pre/post difference and the arms are compared
//! with Welch. The result carries the pre-trend check and the size-bias
//! diagnostic, and a misspelled store name is refused rather than silently
//! landing in the control pool.
//!
//! Run: `cargo run --example experiment_common_date`

use airlayer::engine::experiment::{
    day_ordinal, estimate, Assignment, EffectResult, ExperimentDesign, PanelMatrix,
};
use std::collections::HashMap;

/// Deterministic noise in [-1, 1) per (unit, day), so the output is stable.
fn noise(unit: usize, day: i64) -> f64 {
    let mut z = (unit as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (day as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z ^= z >> 31;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 29;
    (z >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
}

fn report(label: &str, r: &EffectResult) {
    println!("== {label}");
    if let Some(why) = &r.refusal {
        println!("refused (design = {:?}): {why}", r.design);
        return;
    }
    println!(
        "{}: {:+.2}/day  95% CI [{:+.2}, {:+.2}]  se={:.2} t={:.2} df={:.1}  p={:.4}  significant={}",
        r.design, r.estimate, r.ci_low, r.ci_high, r.se, r.t_stat, r.df, r.p_value, r.significant
    );
    println!("arms: {} treated, {} control", r.n_treated, r.n_control);
    match &r.pre_trend {
        Some(p) => println!(
            "pre_trend: {:+.2} (t={:.2}) diverged={}",
            p.estimate, p.t_stat, p.diverged
        ),
        None => println!("pre_trend: could not run (absence, not a pass)"),
    }
    match &r.size_bias {
        Some(s) => println!(
            "size_bias: corr treated={:+.2} control={:+.2} z={:.2} significant={}",
            s.correlation_treated, s.correlation_control, s.z_stat, s.significant
        ),
        None => println!("size_bias: could not run"),
    }
}

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2026, 3, 2).unwrap());
    let switch = start + 84;
    let stores: Vec<String> = (0..12).map(|i| format!("store{i:02}")).collect();

    // Seventeen weeks of daily sales: the size-bias check reads a window a full
    // pre-period before the pre-period, so it needs 3 x pre_days of history.
    // Stores differ in size; the even-numbered six gain +8/day once the lever is on.
    let mut rows = Vec::new();
    for (i, s) in stores.iter().enumerate() {
        for day in start..start + 119 {
            let base = 200.0 + 15.0 * i as f64;
            let lift = if i % 2 == 0 && day >= switch {
                8.0
            } else {
                0.0
            };
            rows.push((s.clone(), day, base + 6.0 * noise(i, day) + lift));
        }
    }
    let panel = PanelMatrix::from_triples(rows);

    let assignment = |switch_day: HashMap<String, Option<i64>>| {
        ExperimentDesign::Waves(Assignment {
            switch_day,
            strata: None,
            pre_days: 28,
            post_days: 28,
            anticipation_days: 0,
            washout_days: 7,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        })
    };

    let switch_day: HashMap<String, Option<i64>> = stores
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), (i % 2 == 0).then_some(switch)))
        .collect();
    report(
        "six treated stores, +8/day",
        &estimate(&panel, &assignment(switch_day.clone()), 42),
    );

    // A typo in the assignment: the panel has no "store99". Refused by name,
    // with every number NaN — never a zero that would read as a finding.
    let mut typo = switch_day;
    typo.remove("store00");
    typo.insert("store99".to_string(), Some(switch));
    report(
        "misspelled treated store",
        &estimate(&panel, &assignment(typo), 42),
    );
}
