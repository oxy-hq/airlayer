//! Pricing a design before running it: `placebo_power` replays the proposed
//! design many times on history where nothing happened and reports the
//! minimum detectable effect (MDE) at the requested power.
//!
//! The same design is then priced against a history cut off too early: fewer
//! than two non-overlapping design spans fit, so it is refused instead of
//! passing one look off as hundreds of iterations.
//!
//! Run: `cargo run --example experiment_placebo_power`

use airlayer::engine::experiment::{
    day_ordinal, placebo_power, DesignShape, DesignSpec, PanelMatrix, PowerResult,
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

fn report(label: &str, p: &PowerResult) {
    println!("== {label}");
    match &p.refusal {
        Some(why) => println!("refused: {why}"),
        None => println!(
            "MDE at 80% power: {:.2}/day ({:.1}% of baseline {:.1})  null sd={:.2}\n\
             iterations={} distinct placebo windows={} independent stretches={}",
            p.mde,
            100.0 * p.mde_relative,
            p.baseline,
            p.null_sd,
            p.iterations,
            p.distinct_windows,
            p.independent_stretches
        ),
    }
    println!();
}

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2025, 7, 1).unwrap());

    // Half a year of daily sales for twenty stores, with a weekly cycle and
    // store-level noise. No intervention anywhere: this is the null.
    let mut rows = Vec::new();
    for i in 0..20 {
        for day in start..start + 182 {
            let weekly = if day.rem_euclid(7) >= 5 { 25.0 } else { 0.0 };
            rows.push((
                format!("store{i:02}"),
                day,
                200.0 + 10.0 * i as f64 + weekly + 12.0 * noise(i, day),
            ));
        }
    }
    let history = PanelMatrix::from_triples(rows);

    let spec = |shape: DesignShape, history_to: Option<i64>| DesignSpec {
        shape,
        pre_days: 28,
        post_days: 28,
        anticipation_days: 0,
        washout_days: 7,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
        power: 0.8,
        // Small so a debug build stays quick; real pricing uses more.
        iterations: 200,
        history_to,
        blocks: 0,
    };
    let ten_vs_ten = || DesignShape::CommonDate {
        n_treated: 10,
        n_control: 10,
    };

    report(
        "common date, 10 treated vs 10 control, 4-week pre/post",
        &placebo_power(&history, &spec(ten_vs_ten(), None), 42),
    );
    report(
        "same design, two blocks ranked by pre-period size",
        &placebo_power(
            &history,
            &DesignSpec {
                blocks: 2,
                ..spec(ten_vs_ten(), None)
            },
            42,
        ),
    );
    report(
        "same design, history cut at day +90",
        &placebo_power(&history, &spec(ten_vs_ten(), Some(start + 90)), 42),
    );
}
