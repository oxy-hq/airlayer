//! Effect estimation for a deliberate intervention.
//!
//! See `internal-docs/experiments.md` in oxygen-internal for the reasoning.
//! The short version: each unit collapses to ONE pre/post difference before
//! any test is run, so serial correlation inside a unit cannot inflate `t`,
//! and the two arms are compared with the same Welch machinery
//! `metric_tree_ops::gap_is_significant` uses. Units switching on the same
//! day form a WAVE — airlayer's `engine::cohort` is a different thing (peer
//! groups on an entity) and nothing here touches it.

#[cfg(test)]
mod calibration;
#[cfg(test)]
mod calibration_switchback;
pub mod design;
pub mod diagnostics;
pub mod estimate;
pub mod panel;
pub mod permutation;
pub mod power;
pub mod power_staggered;
pub mod power_switchback;
pub mod propose;
pub mod ratio;
pub mod staggered;
pub mod strata;
pub mod switchback;
#[cfg(test)]
pub(crate) mod testkit;

pub use design::{estimate, ExperimentDesign};
pub use diagnostics::{PreTrend, SizeBias};
pub use estimate::{estimate_effect, Assignment, EffectResult};
pub use panel::{collapse, day_ordinal, windows_around, PanelMatrix, UnitDelta, Windows};
pub use power::{placebo_power, DesignShape, DesignSpec, PowerResult};
pub use propose::{propose_waves, ProposedWave};
pub use ratio::{estimate_ratio, RatioResult};
pub use strata::propose_strata;
pub use switchback::{estimate_switchback, propose_switchback, Period, SwitchbackSchedule};

/// SplitMix64. Inlined rather than taking a `rand` dependency: `rand` is not a
/// direct dependency of this crate (only transitive through `statrs`), and
/// `Cargo.toml`'s wasm/getrandom target block makes adding it a bigger change
/// than the 20 lines it would save. Seeded explicitly at every call site so a
/// placebo run is reproducible.
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`, and `0` when `n == 0` rather than a modulo panic —
    /// callers guard their own ranges, but a panic reachable from a `pub(crate)`
    /// helper is a landmine for the next one. The modulo bias is under 2^-52
    /// for the day counts and unit counts this module sees (both well under
    /// 2^12), so rejection sampling would buy nothing measurable.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() % n as u64) as usize
    }

    /// Fisher-Yates over the first `k` positions only — enough to draw a
    /// treated/control assignment without shuffling the whole vector. The two
    /// arms are disjoint slices of the shuffled prefix, so no unit can land in
    /// both.
    pub(crate) fn partial_shuffle(&mut self, v: &mut [usize], k: usize) {
        let n = v.len();
        for i in 0..k.min(n) {
            let j = i + self.below(n - i);
            v.swap(i, j);
        }
    }
}

use crate::engine::metric_tree_ops::welch_se_df;
use statrs::distribution::{ContinuousCDF, StudentsT};

#[derive(Debug, Clone, PartialEq)]
pub enum WelchRefusal {
    ThinArm { treated: usize, control: usize },
    NoSpread,
}

#[derive(Debug, Clone, Copy)]
pub struct WelchTest {
    pub diff: f64,
    pub se: f64,
    pub df: f64,
    pub t: f64,
}

fn mean_sd(x: &[f64]) -> (f64, f64) {
    let n = x.len() as f64;
    let mean = x.iter().sum::<f64>() / n;
    let var = x.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / (n - 1.0);
    (mean, var.sqrt())
}

pub(crate) fn welch(a: &[f64], b: &[f64]) -> Result<WelchTest, WelchRefusal> {
    if a.len() < 2 || b.len() < 2 {
        return Err(WelchRefusal::ThinArm {
            treated: a.len(),
            control: b.len(),
        });
    }
    let (mean_a, sd_a) = mean_sd(a);
    let (mean_b, sd_b) = mean_sd(b);
    let (se, df) =
        welch_se_df(sd_a, a.len() as f64, sd_b, b.len() as f64).ok_or(WelchRefusal::NoSpread)?;
    let diff = mean_a - mean_b;
    Ok(WelchTest {
        diff,
        se,
        df,
        t: diff / se,
    })
}

fn student(df: f64) -> StudentsT {
    StudentsT::new(0.0, 1.0, df.max(1.0)).expect("Student's t with positive df is well-formed")
}

/// Šidák per-comparison rate for `family` outcomes registered IN ADVANCE — and
/// `alpha` itself for one, in which case no multiplicity correction applies at
/// all. That is the whole statistical dividend of pre-registration, and it is
/// why opportunity sizing's `significance_threshold` (which also carries a
/// selection term for benchmarking against the max of k segments) is
/// deliberately not reused. Shared by the Welch path and the permutation path.
pub(crate) fn per_comparison_alpha(alpha: f64, family: usize) -> f64 {
    if family <= 1 {
        alpha
    } else {
        1.0 - (1.0 - alpha).powf(1.0 / family as f64)
    }
}

/// The registered rates every test path reads, refused by name when unusable.
/// `alpha` must be finite and strictly inside (0, 1): NaN made every `p <= alpha`
/// comparison quietly false, 5.0 panicked inside the t quantile, and 1.5 made
/// pure noise significant. `coverage_floor` must be finite and inside [0, 1]
/// (both edges are real policies: 0 accepts any window, 1 demands every day);
/// NaN made `coverage < floor` false for every window. `family` is deliberately
/// NOT validated here: `per_comparison_alpha` treats 0 like 1 (no correction),
/// and that behaviour is kept. Each message states only what it tested.
pub(crate) fn validate_rates(alpha: f64, coverage_floor: f64) -> Result<(), String> {
    if !(alpha.is_finite() && alpha > 0.0 && alpha < 1.0) {
        return Err(format!(
            "alpha must be a finite number strictly between 0 and 1; got {alpha}"
        ));
    }
    if !(coverage_floor.is_finite() && (0.0..=1.0).contains(&coverage_floor)) {
        return Err(format!(
            "coverage_floor must be a finite number between 0 and 1; got {coverage_floor}"
        ));
    }
    Ok(())
}

/// A rate for a message: three decimals, or scientific once three decimals
/// would print 0.000 (a Šidák rate under a big family, a sampled p floor).
pub(crate) fn fmt_p(p: f64) -> String {
    if p >= 0.001 || p == 0.0 || !p.is_finite() {
        format!("{p:.3}")
    } else {
        format!("{p:.2e}")
    }
}

/// Two-sided critical t at the per-comparison rate.
pub(crate) fn t_quantile(df: f64, alpha: f64, family: usize) -> f64 {
    student(df).inverse_cdf(1.0 - per_comparison_alpha(alpha, family) / 2.0)
}

/// One-sided quantile at `power`. A significance threshold is the effect a test
/// detects HALF the time; the 80%-power MDE sits `t_power_quantile(df, 0.8) × se`
/// further out — about 1.4× the threshold under normal noise.
pub fn t_power_quantile(df: f64, power: f64) -> f64 {
    student(df).inverse_cdf(power)
}

/// Raw two-sided p-value of `t` at `df` — per comparison, NOT adjusted for
/// `family`. The decision applies the family through `t_quantile`; with
/// `family == 1` the two agree: `p ≤ alpha` exactly when `|t| ≥ t_quantile`.
pub(crate) fn t_two_sided_p(t: f64, df: f64) -> f64 {
    // The lower tail at -|t|, not `1 - cdf(|t|)`: that subtraction rounds to an
    // exact 0.0 once the upper tail drops below 1e-16, and a zero p reads as a finding.
    (2.0 * student(df).cdf(-t.abs())).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn experiment_rng_is_deterministic_and_in_range() {
        let (mut a, mut b) = (SplitMix64::new(42), SplitMix64::new(42));
        assert_eq!(
            a.next_u64(),
            b.next_u64(),
            "same seed must give same stream"
        );

        let mut r = SplitMix64::new(7);
        assert!((0..1000).all(|_| r.below(24) < 24));
        assert_eq!(r.below(1), 0, "a single-choice draw is always index 0");
        assert_eq!(r.below(0), 0, "below(0) must not divide by zero");

        // The assignment draw depends on a partial shuffle neither dropping nor
        // duplicating a unit — a duplicate would put one unit in both arms.
        let mut v: Vec<usize> = (0..24).collect();
        SplitMix64::new(1).partial_shuffle(&mut v, 12);
        let mut seen = v.clone();
        seen.sort_unstable();
        assert_eq!(seen, (0..24).collect::<Vec<_>>());
    }

    #[test]
    fn experiment_welch_matches_a_known_two_sample_case() {
        // Different spread and different n — the case a pooled variance gets wrong.
        let a = [10.0, 12.0, 14.0, 16.0]; // mean 13, s^2 = 20/3, s^2/n = 5/3
        let b = [8.0, 9.0, 10.0]; // mean  9, s^2 = 1,    s^2/n = 1/3
        let w = welch(&a, &b).expect("both arms have spread");
        assert!((w.diff - 4.0).abs() < 1e-12);
        assert!((w.se - 2.0_f64.sqrt()).abs() < 1e-12, "se was {}", w.se);
        // df = (5/3 + 1/3)^2 / ((5/3)^2/3 + (1/3)^2/2) = 4 / 0.9814815 = 4.075472
        assert!(
            (w.df - 4.075472).abs() < 1e-5,
            "satterthwaite df was {}",
            w.df
        );
    }

    #[test]
    fn experiment_welch_names_its_two_refusals_apart() {
        assert!(matches!(
            welch(&[1.0, 2.0], &[3.0]),
            Err(WelchRefusal::ThinArm {
                treated: 2,
                control: 1
            })
        ));
        // Both arms move identically: two observations, but no VARIANCE.
        assert!(matches!(
            welch(&[5.0, 5.0], &[1.0, 1.0]),
            Err(WelchRefusal::NoSpread)
        ));
    }

    #[test]
    fn experiment_t_quantile_tightens_only_when_more_outcomes_registered() {
        let one = t_quantile(20.0, 0.05, 1);
        assert!((one - 2.086).abs() < 0.01, "two-sided t at 20 df was {one}");
        assert!(
            t_quantile(20.0, 0.05, 4) > one,
            "registering 4 outcomes raises the bar"
        );
        // 80% power at 20 df sits at t = 0.860: the analytic MDE is (2.086 + 0.860) se.
        assert!((t_power_quantile(20.0, 0.80) - 0.860).abs() < 0.01);
    }

    #[test]
    fn experiment_t_two_sided_p_inverts_the_critical_value() {
        let crit = t_quantile(20.0, 0.05, 1);
        assert!(
            (t_two_sided_p(crit, 20.0) - 0.05).abs() < 1e-6,
            "p at the critical t is alpha"
        );
        assert!(
            (t_two_sided_p(-crit, 20.0) - 0.05).abs() < 1e-6,
            "two-sided: the sign is irrelevant"
        );
        assert!(
            (t_two_sided_p(0.0, 20.0) - 1.0).abs() < 1e-9,
            "t = 0 is p = 1"
        );
        assert!(t_two_sided_p(40.0, 20.0) < 1e-12);
    }

    /// Every C1 item, named through the module root with its C1 signature. A
    /// rename or a re-shape fails to compile here before it fails in oxy.
    #[test]
    fn experiment_contract_c1_surface_resolves_at_the_module_root() {
        use crate::engine::experiment as x;
        use std::collections::HashMap;
        let _: fn(chrono::NaiveDate) -> i64 = x::day_ordinal;
        let _: for<'a> fn(&'a x::PanelMatrix) -> &'a [String] = x::PanelMatrix::units;
        let _: fn(&x::PanelMatrix, &x::Assignment, u64) -> x::EffectResult = x::estimate_effect;
        let _: fn(&x::PanelMatrix, &x::DesignSpec, u64) -> x::PowerResult = x::placebo_power;
        let _: for<'a> fn(
            &[String],
            &[usize],
            i64,
            i64,
            Option<&'a [Vec<String>]>,
            u64,
        ) -> Result<Vec<x::ProposedWave>, String> = x::propose_waves;
        let _: fn(&x::PanelMatrix, usize, i64, i64) -> Result<Vec<Vec<String>>, String> =
            x::propose_strata;
        let _: fn(i64, i64, usize, u64) -> Vec<x::Period> = x::propose_switchback;
        let _: fn(&x::PanelMatrix, &x::SwitchbackSchedule, u64) -> x::EffectResult =
            x::estimate_switchback;
        let _: fn(&x::PanelMatrix, &x::ExperimentDesign, u64) -> x::EffectResult = x::estimate;
        let _: fn(&x::PanelMatrix, &x::PanelMatrix, &x::ExperimentDesign, u64) -> x::RatioResult =
            x::estimate_ratio;

        // One unit, one day: degenerate for every estimator below.
        let m = x::PanelMatrix::from_triples(vec![("a".to_string(), 1_i64, 1.0_f64)]);
        assert_eq!(m.units(), ["a".to_string()]);
        let d0 = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(
            x::day_ordinal(d0 + chrono::Duration::days(1)),
            x::day_ordinal(d0) + 1
        );

        let a = x::Assignment {
            switch_day: HashMap::from([("a".to_string(), None)]),
            strata: None,
            pre_days: 1,
            post_days: 1,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        };
        let e = x::estimate_effect(&m, &a, 0);
        let _: (f64, f64, f64, f64, f64, f64, f64) = (
            e.estimate, e.se, e.t_stat, e.df, e.ci_low, e.ci_high, e.p_value,
        );
        let _: (usize, usize, bool, &str, &str) = (
            e.n_treated,
            e.n_control,
            e.significant,
            e.estimand,
            e.design,
        );
        let _: (
            &Vec<String>,
            &Option<x::PreTrend>,
            &Option<x::SizeBias>,
            Option<usize>,
            &Option<String>,
        ) = (
            &e.dropped_waves,
            &e.pre_trend,
            &e.size_bias,
            e.retained_on_days,
            &e.refusal,
        );
        let _: &Option<Vec<Vec<String>>> = &a.strata;
        // A lone untreated unit has no treated arm: refused, with nothing to report.
        assert!(e.refusal.is_some(), "no treated unit is a refusal");
        assert!(!e.significant && e.retained_on_days.is_none());
        assert_eq!((e.n_treated, e.n_control), (0, 0));

        let _ = x::DesignShape::CommonDate {
            n_treated: 2,
            n_control: 2,
        };
        let _ = x::DesignShape::Switchback {
            period_days: 7,
            pairs: 6,
        };
        let sched = x::SwitchbackSchedule {
            periods: vec![
                x::Period {
                    from_day: 1,
                    on: true,
                },
                x::Period {
                    from_day: 8,
                    on: false,
                },
            ],
            period_days: 7,
            washout_days: 2,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        };
        let se = x::estimate_switchback(&m, &sched, 0);
        assert!(
            se.refusal.is_some(),
            "one unit-day cannot cover two periods"
        );
        let _ = x::ExperimentDesign::Switchback(sched);
        let _ = x::ExperimentDesign::Waves(a.clone());
        let blocks: usize = x::DesignSpec {
            shape: x::DesignShape::CommonDate {
                n_treated: 2,
                n_control: 2,
            },
            pre_days: 1,
            post_days: 1,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
            power: 0.8,
            iterations: 1,
            history_to: None,
            blocks: 0,
        }
        .blocks;
        assert_eq!(blocks, 0);
        let spec = x::DesignSpec {
            shape: x::DesignShape::Staggered {
                wave_sizes: vec![2],
                spacing_days: 7,
                n_never_treated: 2,
            },
            pre_days: 1,
            post_days: 1,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
            power: 0.8,
            iterations: 1,
            history_to: None,
            blocks: 0,
        };
        let p = x::placebo_power(&m, &spec, 0);
        let _: (f64, f64, f64, f64, usize, usize, usize, &Option<String>) = (
            p.mde,
            p.mde_relative,
            p.baseline,
            p.null_sd,
            p.iterations,
            p.distinct_windows,
            p.independent_stretches,
            &p.refusal,
        );
        // Four units asked for, one in the panel.
        assert!(
            p.refusal.is_some(),
            "a design larger than the panel is refused"
        );
        assert!(p.mde.is_nan());
        let w = x::ProposedWave {
            switch_day: 1,
            units: Vec::new(),
        };
        assert_eq!((w.switch_day, w.units.len()), (1, 0));
        let r = x::estimate_ratio(&m, &m, &x::ExperimentDesign::Waves(a), 0);
        let _: (
            f64,
            f64,
            f64,
            &x::EffectResult,
            &x::EffectResult,
            &Option<String>,
        ) = (
            r.coefficient,
            r.ci_low,
            r.ci_high,
            &r.first_stage,
            &r.target_effect,
            &r.refusal,
        );
        assert!(
            r.refusal.is_some(),
            "a degenerate pair of panels is refused"
        );
        assert!(r.first_stage.refusal.is_some() && r.target_effect.refusal.is_some());

        // The proposers return values, not just types.
        let units: Vec<String> = (0..4).map(|i| format!("u{i}")).collect();
        let waves = x::propose_waves(&units, &[2], 1, 7, None, 1).expect("a feasible plan");
        assert_eq!(waves.len(), 1);
        assert_eq!(waves[0].units.len(), 2);
        assert_eq!(x::propose_switchback(1, 7, 3, 1).len(), 6);
    }

    /// Index C2b: the host masks seeds to 53 bits so they survive JSON and
    /// JavaScript. Every seeded proposal is deterministic at the largest such
    /// seed — the engine takes the full `u64` and does not mask it again.
    #[test]
    fn experiment_seeded_proposals_are_deterministic_at_the_53_bit_seed() {
        let seed = (1u64 << 53) - 1;
        let units: Vec<String> = (0..12).map(|i| format!("s{i:02}")).collect();
        let strata: Vec<Vec<String>> = units.chunks(6).map(<[String]>::to_vec).collect();
        let waves = propose::propose_waves(&units, &[3], 1, 7, Some(&strata), seed);
        assert_eq!(
            waves,
            propose::propose_waves(&units, &[3], 1, 7, Some(&strata), seed)
        );
        let periods = switchback::propose_switchback(1, 7, 6, seed);
        assert_eq!(periods, switchback::propose_switchback(1, 7, 6, seed));
        assert_ne!(
            periods,
            switchback::propose_switchback(1, 7, 6, seed - 1),
            "neighbouring seeds draw different schedules (1 in 64 to coincide)"
        );
        // Regression pin for stored seeds: a schedule persisted under this seed must
        // keep replaying as the same assignment. Snapshotted once from the implementation.
        let wave = |switch_day, names: &[&str]| propose::ProposedWave {
            switch_day,
            units: names.iter().map(|n| n.to_string()).collect(),
        };
        assert_eq!(waves, Ok(vec![wave(1, &["s02", "s06", "s10"])]));
        let on = [
            false, true, false, true, true, false, false, true, false, true, true, false,
        ];
        let expected: Vec<switchback::Period> = on
            .iter()
            .enumerate()
            .map(|(i, &on)| switchback::Period {
                from_day: 1 + 7 * i as i64,
                on,
            })
            .collect();
        assert_eq!(periods, expected);
    }

    /// Review focus 1: PR 5 stores these as jsonb. Non-finite numbers must
    /// serialize, and they arrive as `null` — `ci_low: null` with no refusal
    /// means −inf, `ci_high: null` means +inf.
    #[test]
    fn experiment_unbounded_results_serialize_as_json() {
        let mut e = estimate::EffectResult::refused("placeholder reason for the fixture");
        e.ci_low = f64::NEG_INFINITY;
        e.ci_high = f64::INFINITY;
        e.refusal = None;
        let v = serde_json::to_value(&e).expect("a non-finite interval must still serialize");
        assert!(
            v["ci_low"].is_null() && v["ci_high"].is_null(),
            "±inf is written as null: {v}"
        );
        assert!(
            v["estimate"].is_null() && v["p_value"].is_null(),
            "NaN is written as null: {v}"
        );
        assert_eq!(v["design"], "refused");

        let r = ratio::RatioResult {
            coefficient: 0.35,
            ci_low: f64::NEG_INFINITY,
            ci_high: f64::INFINITY,
            first_stage: e.clone(),
            target_effect: e,
            refusal: Some("the lever did not measurably move the driver".into()),
        };
        let text = serde_json::to_string(&r).expect("an unbounded ratio serializes");
        assert!(
            text.contains("\"ci_low\":null") && text.contains("\"coefficient\":0.35"),
            "{text}"
        );

        let m = PanelMatrix::from_triples(vec![("a".to_string(), 1_i64, 1.0_f64)]);
        let spec = power::DesignSpec {
            shape: power::DesignShape::CommonDate {
                n_treated: 2,
                n_control: 2,
            },
            pre_days: 0,
            post_days: 1,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
            power: 0.8,
            iterations: 1,
            history_to: None,
            blocks: 0,
        };
        let p =
            serde_json::to_value(power::placebo_power(&m, &spec, 0)).expect("a refusal serializes");
        assert!(p["mde"].is_null() && p["refusal"].is_string(), "{p}");
        serde_json::to_string(&spec).expect("a DesignSpec serializes");
        let w = propose::ProposedWave {
            switch_day: 739_677,
            units: vec!["bondi".into()],
        };
        assert_eq!(
            serde_json::to_value(&w).expect("serializes")["units"][0],
            "bondi"
        );

        // retained_on_days: null on a waves result, a number on a switchback.
        assert!(v["retained_on_days"].is_null(), "{v}");
        let sched = switchback::SwitchbackSchedule {
            periods: switchback::propose_switchback(739_677, 7, 6, 1),
            period_days: 7,
            washout_days: 2,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        };
        let sv = serde_json::to_value(&sched).expect("a schedule serializes");
        assert_eq!(sv["periods"][0]["from_day"], 739_677);
        assert!(sv["periods"][0]["on"].is_boolean());
        // Externally tagged (serde's default) — PR 3 stores, PR 5 reads, this shape.
        let dv =
            serde_json::to_value(design::ExperimentDesign::Switchback(sched)).expect("serializes");
        assert!(dv["Switchback"]["period_days"] == 7, "{dv}");
        let shape = serde_json::to_value(power::DesignShape::Switchback {
            period_days: 7,
            pairs: 6,
        })
        .expect("serializes");
        assert_eq!(shape["Switchback"]["pairs"], 6, "{shape}");
        let mut sb = estimate::EffectResult::refused("placeholder");
        sb.retained_on_days = Some(30);
        assert_eq!(
            serde_json::to_value(&sb).expect("serializes")["retained_on_days"],
            30
        );
        let mut a = estimate::Assignment {
            switch_day: std::collections::HashMap::new(),
            strata: None,
            pre_days: 1,
            post_days: 1,
            anticipation_days: 0,
            washout_days: 0,
            coverage_floor: 0.9,
            alpha: 0.05,
            family: 1,
        };
        assert!(serde_json::to_value(&a).expect("serializes")["strata"].is_null());
        a.strata = Some(vec![vec!["bondi".into()]]);
        let av = serde_json::to_value(design::ExperimentDesign::Waves(a)).expect("serializes");
        assert_eq!(av["Waves"]["strata"][0][0], "bondi", "{av}");
    }
}
