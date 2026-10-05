//! Whole-experiment calibration of the switchback, and its carryover bias.
//! Test-only (`#[cfg(test)] mod calibration_switchback;`).

use crate::engine::experiment::power::placebo_power;
use crate::engine::experiment::switchback::{
    estimate_switchback, propose_switchback, SwitchbackSchedule,
};
use crate::engine::experiment::testkit::{
    inject_switchback, noisy_panel, schedule, switchback_design,
};
use crate::engine::experiment::PanelMatrix;

struct Sim {
    reject_rate: f64,
    coverage: f64,
}

fn simulate(effect: f64, reps: usize, base_seed: u64) -> Sim {
    let (mut rejected, mut covered) = (0usize, 0usize);
    for k in 0..reps {
        let seed = base_seed + k as u64;
        let s = schedule(propose_switchback(200, 7, 8, seed ^ 0x5EED), 7, 2);
        let m = inject_switchback(&noisy_panel(24, 400, seed), &s, effect, 0);
        let r = estimate_switchback(&m, &s, seed);
        assert!(r.refusal.is_none(), "rep {k}: {:?}", r.refusal);
        rejected += usize::from(r.significant);
        covered += usize::from(r.ci_low <= effect && effect <= r.ci_high);
    }
    Sim {
        reject_rate: rejected as f64 / reps as f64,
        coverage: covered as f64 / reps as f64,
    }
}

/// Bands centred on the attained size 6/128 = 0.0469, not on alpha.
#[test]
fn experiment_calibration_switchback_simulates_whole_experiments() {
    let mde = placebo_power(&noisy_panel(24, 400, 11), &switchback_design(7, 8), 1).mde;
    let null = simulate(0.0, 1000, 15_000);
    let at_mde = simulate(mde, 1000, 16_000);
    assert!(
        (0.027..=0.067).contains(&null.reject_rate),
        "switchback rejected {:.3} of null experiments; the attained size is 0.047 — near \
         0.094 means the mirror tie was dropped",
        null.reject_rate
    );
    assert!(
        (0.765..=0.835).contains(&at_mde.reject_rate),
        "switchback detected {:.3} at the reported MDE, want ~0.80",
        at_mde.reject_rate
    );
    assert!(
        (0.933..=0.973).contains(&at_mde.coverage),
        "switchback interval covered {:.3}, want 1 − 0.047",
        at_mde.coverage
    );
}

/// Pairs whose off period immediately follows an on period.
fn off_after_on(s: &SwitchbackSchedule) -> usize {
    s.periods
        .iter()
        .enumerate()
        .filter(|(i, p)| !p.on && *i > 0 && s.periods[i - 1].on)
        .count()
}

#[test]
fn experiment_switchback_under_carryover_reads_low_by_the_expected_amount() {
    let flat = PanelMatrix::from_triples(
        (0..6).flat_map(|u| (1..=100i64).map(move |d| (format!("u{u}"), d, 100.0))),
    );
    let s = schedule(propose_switchback(1, 7, 6, 5), 7, 1);
    let (tau, pairs) = (10.0, 6.0);
    let k = off_after_on(&s) as f64;
    assert!(
        k >= 1.0 && k < pairs,
        "the fixture must have some off periods after on ones"
    );

    let r = estimate_switchback(&inject_switchback(&flat, &s, tau, 3), &s, 1);
    let expected = tau - tau * (3.0 - 1.0) / (7.0 - 1.0) * k / pairs;
    assert!(
        (r.estimate - expected).abs() < 1e-9,
        "carryover 3 past washout 1: estimate {} want {expected}",
        r.estimate
    );
    assert!(r.estimate < tau, "carryover biases toward zero");

    let r = estimate_switchback(&inject_switchback(&flat, &s, tau, 1), &s, 1);
    assert!(
        (r.estimate - tau).abs() < 1e-9,
        "a washout as long as the carryover absorbs it"
    );
}
