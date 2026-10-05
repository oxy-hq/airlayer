//! Effect estimation for a deliberate intervention.
//!
//! See `internal-docs/experiments.md` in oxygen-internal for the reasoning.
//! The short version: each unit collapses to ONE pre/post difference before
//! any test is run, so serial correlation inside a unit cannot inflate `t`,
//! and the two arms are compared with the same Welch machinery
//! `metric_tree_ops::gap_is_significant` uses. Units switching on the same
//! day form a WAVE — airlayer's `engine::cohort` is a different thing (peer
//! groups on an entity) and nothing here touches it.

pub mod estimate;
pub mod panel;
pub mod power;
#[cfg(test)]
pub(crate) mod testkit;

pub use panel::{collapse, day_ordinal, windows_around, PanelMatrix, UnitDelta, Windows};

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
}
