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
    (2.0 * (1.0 - student(df).cdf(t.abs()))).clamp(0.0, 1.0)
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
}
