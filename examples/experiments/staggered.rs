//! Staggered rollout: `propose_waves` splits twelve stores into waves two weeks
//! apart, and `estimate` compares each wave only with stores still untreated
//! through its post window.
//!
//! With a permanent holdout every wave is estimated. `propose_waves` refuses
//! to plan a rollout with no holdout; read after the fact, such a rollout's
//! last wave has no clean controls left, so it is dropped and named in
//! `dropped_waves`, never folded in at zero.
//!
//! Run: `cargo run --example experiment_staggered`

use airlayer::engine::experiment::{
    day_ordinal, estimate, propose_waves, Assignment, ExperimentDesign, PanelMatrix, ProposedWave,
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

/// Daily sales where every store in a wave gains +8/day from its switch day.
fn panel_for(stores: &[String], waves: &[ProposedWave], start: i64, days: i64) -> PanelMatrix {
    let mut rows = Vec::new();
    for (i, s) in stores.iter().enumerate() {
        let switch = waves
            .iter()
            .find(|w| w.units.contains(s))
            .map(|w| w.switch_day);
        for day in start..start + days {
            let lift = match switch {
                Some(sw) if day >= sw => 8.0,
                _ => 0.0,
            };
            rows.push((
                s.clone(),
                day,
                200.0 + 15.0 * i as f64 + 6.0 * noise(i, day) + lift,
            ));
        }
    }
    PanelMatrix::from_triples(rows)
}

fn run(label: &str, stores: &[String], waves: &[ProposedWave], start: i64) {
    println!("== {label}");
    let mut switch_day: HashMap<String, Option<i64>> =
        stores.iter().map(|s| (s.clone(), None)).collect();
    for (k, w) in waves.iter().enumerate() {
        println!(
            "wave {} switches on day +{}: {}",
            k + 1,
            w.switch_day - start,
            w.units.join(", ")
        );
        for u in &w.units {
            switch_day.insert(u.clone(), Some(w.switch_day));
        }
    }
    let holdout: Vec<&str> = stores
        .iter()
        .filter(|s| switch_day[*s].is_none())
        .map(String::as_str)
        .collect();
    println!(
        "holdout: {}",
        if holdout.is_empty() {
            "none".to_string()
        } else {
            holdout.join(", ")
        }
    );

    let panel = panel_for(stores, waves, start, 28 + 14 * waves.len() as i64 + 14);
    let design = ExperimentDesign::Waves(Assignment {
        switch_day,
        strata: None,
        pre_days: 21,
        post_days: 14,
        anticipation_days: 0,
        washout_days: 0,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    });
    let r = estimate(&panel, &design, 42);
    match &r.refusal {
        Some(why) => println!("refused: {why}"),
        None => println!(
            "{}: {:+.2}/day  95% CI [{:+.2}, {:+.2}]  permutation p={:.4}  significant={}  (se/t/df are NaN here: {})",
            r.design,
            r.estimate,
            r.ci_low,
            r.ci_high,
            r.p_value,
            r.significant,
            r.se.is_nan()
        ),
    }
    println!("arms: {} treated, {} control", r.n_treated, r.n_control);
    for d in &r.dropped_waves {
        println!("dropped: {d}");
    }
    println!();
}

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
    let stores: Vec<String> = (0..12).map(|i| format!("store{i:02}")).collect();
    let first_switch = start + 28;

    let waves = propose_waves(&stores, &[3, 3, 3], first_switch, 14, None, 7)
        .expect("nine of twelve stores leaves a holdout");
    run(
        "three seeded waves of 3, three-store holdout",
        &stores,
        &waves,
        start,
    );

    // propose_waves will not plan a rollout that leaves no control at all...
    match propose_waves(&stores, &[4, 4, 4], first_switch, 14, None, 7) {
        Ok(_) => unreachable!("a plan with no control is refused"),
        Err(why) => println!("== propose_waves([4, 4, 4]) refused: {why}\n"),
    }

    // ...but a rollout that already happened to everyone can still be read.
    // The last wave has no store left untreated through its post window, so
    // it is dropped and named, and the estimate covers the first two waves.
    let full_rollout: Vec<ProposedWave> = stores
        .chunks(4)
        .enumerate()
        .map(|(k, units)| ProposedWave {
            switch_day: first_switch + 14 * k as i64,
            units: units.to_vec(),
        })
        .collect();
    run(
        "full rollout in waves of 4, no holdout",
        &stores,
        &full_rollout,
        start,
    );
}
