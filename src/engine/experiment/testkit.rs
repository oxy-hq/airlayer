//! Fixtures shared by the test modules of `engine::experiment`. Compiled only
//! under `cfg(test)`, so nothing here reaches the library build.

use crate::engine::experiment::{PanelMatrix, SplitMix64};

pub(crate) fn matrix_from(rows: &[(&str, i64, f64)]) -> PanelMatrix {
    PanelMatrix::from_triples(rows.iter().map(|(u, d, v)| (u.to_string(), *d, *v)))
}

/// Uniform on `[0, width)` from the top 24 bits of one draw.
pub(crate) fn uniform(rng: &mut SplitMix64, width: f64) -> f64 {
    (rng.next_u64() >> 40) as f64 / 16_777_216.0 * width
}
