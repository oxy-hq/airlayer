//! Diagnostics reported beside an estimate: the parallel-trends pre-check and
//! the size-heterogeneity evidence. Neither gates the estimate; both are read.

use crate::engine::experiment::estimate::Assignment;
use crate::engine::experiment::{PanelMatrix, UnitDelta, Windows};

/// The same estimator run entirely inside the pre-period.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PreTrend {
    pub estimate: f64,
    pub t_stat: f64,
    pub diverged: bool,
}

/// Does a treated unit's SIZE predict its own measured effect, beyond what the
/// controls show for the same relationship? Evidence about whether the estimate
/// transports to untreated units — never a gate on totalling the treated units
/// themselves, which is exact arithmetic either way.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SizeBias {
    pub correlation_treated: f64,
    /// The same correlation among controls. It carries every artifact the
    /// treated figure does — serial correlation, regression to the mean — and
    /// none of the heterogeneity, which is why the decision is the DIFFERENCE.
    pub correlation_control: f64,
    pub z_stat: f64,
    pub significant: bool,
    pub size_window: (i64, i64),
    /// Distinct units behind each correlation — what Fisher's SE is computed on.
    pub n_treated: usize,
    pub n_control: usize,
}

pub(crate) fn pre_trend(_m: &PanelMatrix, _a: &Assignment, _switch_ord: i64) -> Option<PreTrend> {
    None
}

pub(crate) fn size_bias(
    _m: &PanelMatrix,
    _a: &Assignment,
    _deltas: &[UnitDelta],
    _w: &Windows,
) -> Option<SizeBias> {
    None
}
