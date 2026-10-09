//! Whole-experiment calibration: fresh panels, random designs, the shipped
//! entry point, and counts. Test-only (`#[cfg(test)] mod calibration;`).

use crate::engine::experiment::estimate::{estimate_effect, Assignment};
use crate::engine::experiment::power::{placebo_power, DesignShape, DesignSpec};
use crate::engine::experiment::power_staggered::{assignment_for, inject, lay_out};
use crate::engine::experiment::testkit::{common, noisy_panel, staggered_design};
use crate::engine::experiment::{PanelMatrix, SplitMix64};

struct Sim {
    reject_rate: f64,
    coverage: f64,
}

/// The design's units drawn at random from `base` and laid out from
/// `first_switch`; the panel is restricted to exactly those units, so an
/// unassigned unit can never leak into the control pool.
fn random_design(
    base: &PanelMatrix,
    d: &DesignSpec,
    first_switch: i64,
    seed: u64,
) -> (PanelMatrix, Assignment) {
    let (sizes, spacing, never) = match &d.shape {
        DesignShape::CommonDate {
            n_treated,
            n_control,
        } => (vec![*n_treated], 0, *n_control),
        DesignShape::Staggered {
            wave_sizes,
            spacing_days,
            n_never_treated,
        } => (wave_sizes.clone(), *spacing_days, *n_never_treated),
        DesignShape::Switchback { .. } => panic!(
            "random_design lays out waves; the switchback calibration builds its own schedule"
        ),
    };
    let need = sizes.iter().sum::<usize>() + never;
    let mut order: Vec<usize> = (0..base.n_units()).collect();
    SplitMix64::new(seed ^ 0x5EED).partial_shuffle(&mut order, need);
    let names: Vec<String> = order[..need]
        .iter()
        .map(|u| base.units[*u].clone())
        .collect();
    let panel = base.restrict(&names, &base.days);
    (
        panel,
        assignment_for(d, lay_out(&names, &sizes, first_switch, spacing)),
    )
}

/// Fresh panel per replication; a random design in the given shape; `effect`
/// injected from each unit's own switch day. Returns how often the SHIPPED
/// estimator rejected and how often its interval held the injected effect.
fn simulate(d: &DesignSpec, effect: f64, reps: usize, base_seed: u64) -> Sim {
    let (mut rejected, mut covered) = (0usize, 0usize);
    for k in 0..reps {
        let seed = base_seed + k as u64;
        let (panel, a) = random_design(&noisy_panel(24, 400, seed), d, 200, seed);
        let m = inject(&panel, &a, effect);
        let r = estimate_effect(&m, &a, seed);
        assert!(r.refusal.is_none(), "rep {k}: {:?}", r.refusal);
        if r.significant {
            rejected += 1
        }
        if r.ci_low <= effect && effect <= r.ci_high {
            covered += 1
        }
    }
    Sim {
        reject_rate: rejected as f64 / reps as f64,
        coverage: covered as f64 / reps as f64,
    }
}

#[test]
fn experiment_calibration_common_date_simulates_whole_experiments() {
    let d = common(12, 12, 56, 56);
    let mde = placebo_power(&noisy_panel(24, 400, 11), &d, 1).mde;
    let null = simulate(&d, 0.0, 1000, 7_000);
    let at_mde = simulate(&d, mde, 1000, 8_000);
    assert!(
        (0.03..=0.07).contains(&null.reject_rate),
        "rejected {:.3} of null experiments, want ~0.05",
        null.reject_rate
    );
    assert!(
        (0.765..=0.835).contains(&at_mde.reject_rate),
        "detected {:.3} at the reported MDE, want ~0.80 — 0.75 is the raw-scale \
             null's signature and this band is sized to catch it",
        at_mde.reject_rate
    );
    assert!(
        (0.925..=0.975).contains(&at_mde.coverage),
        "interval covered the truth {:.3} of the time, want ~0.95",
        at_mde.coverage
    );
}

/// 400 reps: se at 0.80 is 0.020, so 0.75 is 2.5 se out — looser than the
/// common-date band, and stated as such rather than hidden. Each replication
/// runs a full staggered inversion, so this is the slowest test in the crate.
#[test]
#[ignore = "slow: 800 staggered inversions; run with --run-ignored only"]
fn experiment_calibration_staggered_simulates_whole_experiments() {
    let d = staggered_design(vec![4, 4], 20, 8);
    let mde = placebo_power(&noisy_panel(24, 400, 11), &d, 1).mde;
    let null = simulate(&d, 0.0, 400, 9_000);
    let at_mde = simulate(&d, mde, 400, 10_000);
    assert!(
        (0.02..=0.08).contains(&null.reject_rate),
        "staggered rejected {:.3} under the null",
        null.reject_rate
    );
    assert!(
        (0.75..=0.85).contains(&at_mde.reject_rate),
        "staggered detected {:.3} at its MDE",
        at_mde.reject_rate
    );
    assert!(
        (0.91..=0.98).contains(&at_mde.coverage),
        "staggered interval covered the truth {:.3}",
        at_mde.coverage
    );
}

/// AR(1) within unit plus a weekly seasonal with per-unit loadings.
fn seasonal_panel(units: usize, days: usize, seed: u64) -> PanelMatrix {
    let mut r = SplitMix64::new(seed);
    let mut rows = Vec::new();
    for u in 0..units {
        let load = 0.5 + (u % 5) as f64 * 0.4;
        let mut prev = 0.0;
        for day in 1..=days as i64 {
            let shock = crate::engine::experiment::testkit::uniform(&mut r, 60.0) - 30.0;
            prev = 0.85 * prev + shock;
            let weekly = load * 120.0 * ((day as f64) * std::f64::consts::TAU / 7.0).sin();
            rows.push((format!("u{u:03}"), day, 1000.0 + prev + weekly));
        }
    }
    PanelMatrix::from_triples(rows)
}

#[test]
fn experiment_single_window_analytic_crit_is_fragile_where_the_placebo_is_not() {
    use crate::engine::experiment::{
        collapse, t_power_quantile, t_quantile, welch, windows_around,
    };
    let m = seasonal_panel(24, 400, 17);
    let p = placebo_power(&m, &common(12, 12, 56, 56), 3);
    assert!(p.refusal.is_none(), "unexpected refusal: {:?}", p.refusal);

    // The analytic 80%-POWER MDE implied by a SINGLE window, at eight
    // placements over the same history. A significance threshold
    // (`t_quantile × se`) is the effect detected half the time; the like-for-
    // like figure adds the one-sided power quantile — ~1.4× further out.
    let singles: Vec<f64> = (0..8)
        .map(|k| {
            let switch = *m.days.first().expect("days") + (k * 30 + 56) as i64;
            let w = windows_around(&m, switch, 56, 56, 0, 0, 0.9).expect("fits");
            let d: Vec<f64> = collapse(&m, &w).iter().map(|u| u.delta).collect();
            let t = welch(&d[..12], &d[12..24]).expect("two full arms");
            (t_quantile(t.df, 0.05, 1) + t_power_quantile(t.df, 0.80)) * t.se
        })
        .collect();
    let (lo, hi) = (
        singles.iter().cloned().fold(f64::MAX, f64::min),
        singles.iter().cloned().fold(0.0_f64, f64::max),
    );
    assert!(
        hi / lo > 1.5,
        "the fixture must make single-window estimates swing: {lo}..{hi}"
    );

    // The placebo averages over placement, so its MDE sits inside that spread
    // rather than tracking whichever window happened to be picked.
    assert!(
        lo <= p.mde && p.mde <= hi,
        "placebo mde {} should lie within the single-window spread {lo}..{hi}",
        p.mde
    );
}

#[test]
fn experiment_ratio_calibration_covers_beta_with_a_strong_instrument() {
    use crate::engine::experiment::design::ExperimentDesign;
    use crate::engine::experiment::ratio::estimate_ratio;
    use crate::engine::experiment::testkit::ratio_panels;
    const REPS: usize = 600;
    const BETA: f64 = 0.35;
    let mut covered = 0usize;
    for k in 0..REPS {
        let (t, d, a) = ratio_panels(40.0, BETA, 0.0, 50_000 + k as u64);
        let r = estimate_ratio(&t, &d, &ExperimentDesign::Waves(a), k as u64);
        assert!(r.refusal.is_none(), "rep {k}: {:?}", r.refusal);
        assert!(
            r.ci_low.is_finite() && r.ci_high.is_finite(),
            "rep {k}: a strong instrument must bound the set"
        );
        if r.ci_low <= BETA && BETA <= r.ci_high {
            covered += 1
        }
    }
    let rate = covered as f64 / REPS as f64;
    assert!(
        (0.93..=0.97).contains(&rate),
        "Anderson-Rubin covered beta {rate:.3} of the time over {REPS} experiments; \
         the band excludes 0.90 at 2.45 SE — do not widen it"
    );
}
