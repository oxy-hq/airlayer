//! The staggered estimator: units grouped into WAVES by switch date, each wave
//! estimated against only the units still clean through its post window.
//!
//! A pooled two-way fixed-effects regression would use already-treated units as
//! controls for later waves, which under heterogeneous effects gives some
//! comparisons negative weight and can flip the sign of the aggregate. A clean
//! pool per wave avoids that. The last wave of a rollout with no permanent
//! holdout has an empty pool by construction: it is dropped and reported, never
//! folded in at zero.

use crate::engine::experiment::estimate::{validate_assignment, Assignment};
use crate::engine::experiment::{collapse, windows_around, PanelMatrix, Windows};
use std::collections::BTreeMap;

/// The units switching on one day, with that wave's windows and its clean
/// control pool (matrix rows).
#[derive(Debug, Clone)]
pub struct Wave {
    pub switch_ord: i64,
    pub treated: Vec<usize>,
    pub controls: Vec<usize>,
    pub windows: Windows,
}

impl Wave {
    pub fn n_control_units(&self) -> usize {
        self.controls.len()
    }
}

#[derive(Debug, Clone)]
pub struct WaveAtt {
    pub switch_ord: i64,
    pub n_treated: usize,
    pub n_control: usize,
    pub att: f64,
}

/// Every unit's switch ordinal, by matrix row; `None` = never treated. Reads a
/// SCOPED panel, where every row is named — an unnamed unit must never reach
/// here, or it would pose as never-treated.
pub(crate) fn switch_by_row(m: &PanelMatrix, a: &Assignment) -> Vec<Option<i64>> {
    let mut switch_of: Vec<Option<i64>> = vec![None; m.n_units()];
    for (name, day) in &a.switch_day {
        if let Some(u) = m.unit_index(name) {
            switch_of[u] = *day;
        }
    }
    switch_of
}

/// Group units by switch DATE and attach each wave's clean control pool.
/// `Err` when the assignment names a unit the panel does not hold; otherwise the
/// waves that can be estimated plus one message per wave dropped. `m` must be
/// scoped (`scope(m, a)`): row indices in the waves refer to it. Crate-private
/// because an unscoped caller would make an unnamed unit a control.
pub(crate) fn build_waves(
    m: &PanelMatrix,
    a: &Assignment,
) -> Result<(Vec<Wave>, Vec<String>), String> {
    validate_assignment(m, a)?;
    debug_assert_eq!(
        a.switch_day.len(),
        m.n_units(),
        "build_waves reads a scoped panel: an unnamed unit would become a control"
    );
    let switch_of = switch_by_row(m, a);
    let mut groups: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (u, s) in switch_of.iter().enumerate() {
        if let Some(s) = s {
            groups.entry(*s).or_default().push(u);
        }
    }
    let (mut waves, mut dropped) = (Vec::new(), Vec::new());
    for (switch_ord, treated) in groups {
        match wave_at(m, a, &switch_of, switch_ord, treated) {
            Ok(w) => waves.push(w),
            Err(reason) => dropped.push(reason),
        }
    }
    Ok((waves, dropped))
}

/// One wave, or the reason it drops. Each message names only its own guard.
fn wave_at(
    m: &PanelMatrix,
    a: &Assignment,
    switch_of: &[Option<i64>],
    switch_ord: i64,
    treated: Vec<usize>,
) -> Result<Wave, String> {
    let Some(windows) = windows_around(
        m,
        switch_ord,
        a.pre_days,
        a.post_days,
        a.anticipation_days,
        a.washout_days,
        a.coverage_floor,
    ) else {
        return Err(format!(
            "wave switching on day {switch_ord}: its windows do not fit history or fall \
             under the {:.0}% coverage floor",
            a.coverage_floor * 100.0
        ));
    };
    if treated.len() < 2 {
        return Err(format!(
            "wave switching on day {switch_ord}: {} unit(s), needs at least 2",
            treated.len()
        ));
    }
    // Clean for the WHOLE post window, anticipation included: a unit whose
    // pre-announcement lands inside the window is already contaminated.
    let anticipation = a.anticipation_days as i64;
    let controls: Vec<usize> = (0..m.n_units())
        .filter(|u| switch_of[*u].is_none_or(|k| k - anticipation >= windows.post.1))
        .collect();
    if controls.len() < 2 {
        return Err(format!(
            "wave switching on day {switch_ord}: no clean control pool remains \
             ({} unit(s) untreated through its post window)",
            controls.len()
        ));
    }
    Ok(Wave {
        switch_ord,
        treated,
        controls,
        windows,
    })
}

pub fn wave_att(m: &PanelMatrix, w: &Wave) -> Option<WaveAtt> {
    let deltas = collapse(m, &w.windows);
    let mean_of = |members: &[usize]| -> Option<f64> {
        let v: Vec<f64> = deltas
            .iter()
            .filter(|d| members.contains(&d.index))
            .map(|d| d.delta)
            .collect();
        if v.is_empty() {
            None
        } else {
            Some(v.iter().sum::<f64>() / v.len() as f64)
        }
    };
    Some(WaveAtt {
        switch_ord: w.switch_ord,
        n_treated: w.treated.len(),
        n_control: w.controls.len(),
        att: mean_of(&w.treated)? - mean_of(&w.controls)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::{staggered_fixture, staggered_fixture_no_holdout};

    #[test]
    fn experiment_waves_use_only_clean_controls() {
        // 3 switch on day 61, 3 on day 71, 6 never. With pre=post=20 the first
        // wave's post window is [61, 81): the day-71 wave switches INSIDE it,
        // so only the 6 never-treated qualify as its controls.
        let (m, a) = staggered_fixture(0.0, 7);
        let (waves, dropped) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(waves.len(), 2, "dropped: {dropped:?}");
        assert_eq!(waves[0].treated.len(), 3);
        assert_eq!(
            waves[0].n_control_units(),
            6,
            "an already-switching unit leaked into the control pool"
        );
    }

    /// Half-open boundary: a unit switching ON the day the post window ends
    /// (day 81 for [61, 81)) is untreated for every day inside it, so with no
    /// anticipation it IS a clean control — 6 never-treated plus the 3 late.
    #[test]
    fn experiment_waves_admit_a_control_switching_on_the_window_end() {
        let (m, mut a) = staggered_fixture(0.0, 7);
        for name in ["t3", "t4", "t5"] {
            a.switch_day.insert(name.into(), Some(81));
        }
        let (waves, _) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(waves[0].n_control_units(), 9, "day 81 is outside [61, 81)");
    }

    #[test]
    fn experiment_waves_exclude_a_control_whose_anticipation_overlaps() {
        // A control switching 3 days after the post window ends, with a 5-day
        // anticipation band, is ALREADY anticipating inside that window.
        let (m, mut a) = staggered_fixture(0.0, 7);
        a.anticipation_days = 5;
        let boundary_unit = a
            .switch_day
            .keys()
            .find(|k| a.switch_day[*k].is_none())
            .expect("a never-treated unit")
            .clone();
        // The first wave's post window is [61, 81) (washout 0). A switch on day
        // 84 with a 5-day anticipation band is contaminated from day 79 — inside it.
        a.switch_day.insert(boundary_unit.clone(), Some(84));
        let (waves, _) = build_waves(&m, &a).expect("valid assignment");
        assert_eq!(
            waves[0].switch_ord, 61,
            "the day-61 wave must still fit its windows"
        );
        assert!(
            !waves[0]
                .controls
                .iter()
                .any(|u| m.units()[*u] == boundary_unit),
            "a unit anticipating inside the post window is not a clean control"
        );
    }

    #[test]
    fn experiment_waves_refuse_an_unknown_unit_name() {
        let (m, mut a) = staggered_fixture(0.0, 7);
        a.switch_day.insert("typo_store".into(), Some(61));
        let err = build_waves(&m, &a).expect_err("must refuse, not silently skip");
        assert!(err.contains("typo_store"), "error was: {err}");
    }

    #[test]
    fn experiment_waves_drop_a_final_wave_with_no_holdout() {
        let (m, a) = staggered_fixture_no_holdout(0.0, 11);
        let (waves, dropped) = build_waves(&m, &a).expect("valid assignment");
        assert!(
            dropped.iter().any(|d| d.contains("no clean control")),
            "dropped: {dropped:?}"
        );
        assert!(!waves.is_empty(), "earlier waves still identify");
    }

    #[test]
    fn experiment_wave_att_recovers_a_planted_per_wave_effect() {
        let (m, a) = staggered_fixture(25.0, 7);
        let (waves, _) = build_waves(&m, &a).expect("valid");
        for w in &waves {
            let att = wave_att(&m, w).expect("both arms present");
            assert!(
                (att.att - 25.0).abs() < 8.0,
                "wave at {} gave {}",
                w.switch_ord,
                att.att
            );
        }
    }
}
