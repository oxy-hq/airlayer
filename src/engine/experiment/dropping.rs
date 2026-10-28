//! Dropping a unit instead of everyone's days.
//!
//! [`PanelMatrix::from_triples`] keeps a day only when every unit has a value
//! on it. That is right for a dense panel, but one unit with a gap in its data
//! then deletes those days for every unit, and enough of them push a window
//! under its coverage floor: a refusal that names no unit and blames the
//! layout. And a unit the assignment names that the panel never received
//! refuses the whole estimate. Both are reasonable for a registered run whose
//! data the platform watched; for a reading of past data they turn one bad
//! unit into no answer. These entry points drop the unit instead, and say so:
//! every dropped unit comes back named, with its reason, beside the result.

use crate::engine::experiment::estimate::{estimate_effect, Assignment, EffectResult};
use crate::engine::experiment::panel::covers;
use crate::engine::experiment::wave_effects::Comparison;
use crate::engine::experiment::{PanelMatrix, Windows};
use std::collections::{BTreeMap, BTreeSet};

/// A unit left out of the estimate, and why.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DroppedUnit {
    pub unit: String,
    pub reason: String,
}

/// [`PanelMatrix::from_triples_keeping_gaps`], after dropping (and listing) a
/// unit holding a value on fewer than `min_unit_coverage` of the days any unit
/// holds. No day is dropped for everyone: a kept unit's gaps cost only itself,
/// and only in a window it covers below the floor (`left_out_by_windows`).
pub fn panel_dropping_sparse_units(
    rows: impl IntoIterator<Item = (String, i64, f64)>,
    min_unit_coverage: f64,
) -> (PanelMatrix, Vec<DroppedUnit>) {
    let rows: Vec<(String, i64, f64)> = rows.into_iter().collect();
    let days: BTreeSet<i64> = rows.iter().map(|(_, d, _)| *d).collect();
    let mut per_unit: BTreeMap<&str, BTreeSet<i64>> = BTreeMap::new();
    for (u, d, _) in &rows {
        per_unit.entry(u.as_str()).or_default().insert(*d);
    }
    let total = days.len();
    let dropped: Vec<DroppedUnit> = per_unit
        .iter()
        .filter(|(_, held)| (held.len() as f64) < min_unit_coverage * total as f64)
        .map(|(u, held)| DroppedUnit {
            unit: u.to_string(),
            reason: format!(
                "holds {} of the panel's {total} days, under the {:.0}% a unit needs",
                held.len(),
                min_unit_coverage * 100.0
            ),
        })
        .collect();
    let gone: BTreeSet<&str> = dropped.iter().map(|d| d.unit.as_str()).collect();
    let kept = rows
        .iter()
        .filter(|(u, _, _)| !gone.contains(u.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    (PanelMatrix::from_triples_keeping_gaps(kept), dropped)
}

/// On a panel with gaps, every unit the estimate leaves out of one of its
/// comparisons (`comparisons`, on the scoped panel) because it holds too little
/// of that comparison's windows — named, with each short window and its count,
/// one entry per unit. Empty on a dense panel, where the windows' own coverage
/// check already covers every unit.
pub(crate) fn left_out_by_windows(m: &PanelMatrix, cmps: &[Comparison]) -> Vec<DroppedUnit> {
    if !m.has_gaps() {
        return Vec::new();
    }
    let mut reasons: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in cmps {
        for u in c
            .treated
            .iter()
            .chain(&c.controls)
            .filter(|u| !covers(m, **u, &c.windows))
        {
            reasons
                .entry(m.units()[*u].clone())
                .or_default()
                .push(thin_window(m, *u, c.switch_day, &c.windows));
        }
    }
    reasons
        .into_iter()
        .map(|(unit, why)| DroppedUnit {
            unit,
            reason: why.join("; "),
        })
        .collect()
}

/// "left out of the comparison switching on 2026-05-14: holds 4 of the 10 days
/// in its after window, under the 90% coverage floor" — naming each window the
/// unit holds too little of, both when both are short.
fn thin_window(m: &PanelMatrix, u: usize, switch: i64, w: &Windows) -> String {
    let short: Vec<String> = [("before", w.pre), ("after", w.post)]
        .into_iter()
        .filter(|(_, span)| m.unit_coverage(u, span.0, span.1) < w.floor)
        .map(|(which, span)| {
            let days = span.1 - span.0;
            let held = (m.unit_coverage(u, span.0, span.1) * days as f64).round();
            format!("{held} of the {days} days in its {which} window")
        })
        .collect();
    let date = i32::try_from(switch)
        .ok()
        .and_then(chrono::NaiveDate::from_num_days_from_ce_opt)
        .map_or_else(|| format!("day {switch}"), |d| d.to_string());
    format!(
        "left out of the comparison switching on {date}: holds {}, under the {:.0}% \
         coverage floor",
        short.join(" and "),
        w.floor * 100.0
    )
}

/// [`estimate_effect`] after leaving out every unit the assignment names that
/// the panel does not hold (from its arm, and from its stratum), each listed in
/// the result's `dropped_units`. With none missing it is `estimate_effect`.
pub fn estimate_effect_dropping_missing(
    m: &PanelMatrix,
    a: &Assignment,
    seed: u64,
) -> EffectResult {
    let missing: BTreeSet<String> = a
        .switch_day
        .keys()
        .filter(|n| m.unit_index(n).is_none())
        .cloned()
        .collect();
    if missing.is_empty() {
        return estimate_effect(m, a, seed);
    }
    let mut kept = a.clone();
    kept.switch_day.retain(|n, _| !missing.contains(n));
    if let Some(strata) = kept.strata.as_mut() {
        for s in strata.iter_mut() {
            s.retain(|n| !missing.contains(n));
        }
        strata.retain(|s| !s.is_empty());
    }
    let mut r = estimate_effect(m, &kept, seed);
    let mut listed: Vec<DroppedUnit> = missing
        .into_iter()
        .map(|unit| {
            let arm = match a.switch_day.get(&unit) {
                Some(Some(_)) => "treated",
                _ => "control",
            };
            DroppedUnit {
                reason: format!("a {arm} unit with no rows in the panel"),
                unit,
            }
        })
        .collect();
    listed.append(&mut r.dropped_units);
    r.dropped_units = listed;
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::{assign, fixture_rows, staggered_fixture, triples_of};

    #[test]
    fn experiment_a_sparse_unit_is_dropped_instead_of_everyones_days() {
        // c6 misses days 13, 17 and 21: from_triples would drop those days for
        // all twelve units; here c6 alone goes and every day stays.
        let rows: Vec<(String, i64, f64)> = fixture_rows(12, 60, 6, 20.0, 31, 4)
            .into_iter()
            .filter(|(u, d, _)| !(u == "c6" && [13, 17, 21].contains(d)))
            .collect();
        let (m, dropped) = panel_dropping_sparse_units(rows, 1.0);
        assert_eq!(m.n_units(), 11);
        assert_eq!(m.n_days(), 60);
        assert_eq!(
            dropped,
            vec![DroppedUnit {
                unit: "c6".into(),
                reason: "holds 57 of the panel's 60 days, under the 100% a unit needs".into()
            }]
        );
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
    }

    #[test]
    fn experiment_a_unit_above_the_floor_is_kept() {
        let rows: Vec<(String, i64, f64)> = fixture_rows(12, 60, 6, 20.0, 31, 4)
            .into_iter()
            .filter(|(u, d, _)| !(u == "c6" && *d == 13))
            .collect();
        let (m, dropped) = panel_dropping_sparse_units(rows, 0.9);
        assert!(dropped.is_empty());
        assert_eq!(m.n_units(), 12);
        assert_eq!(m.n_days(), 60, "c6's missing day is no one else's");
        assert!(m.has_gaps());
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(
            (r.n_treated, r.n_control),
            (6, 6),
            "19 of 20 before-days clears the 90% floor, so c6 is still compared"
        );
        assert!(r.dropped_units.is_empty(), "{:?}", r.dropped_units);
    }

    /// The BMG May-deal failure: one comparison unit out of stock for six days
    /// of the second wave's after window. `from_triples` deleted those days for
    /// every unit and dropped the whole wave; keeping gaps leaves out only that
    /// unit, only from that wave, and says so.
    #[test]
    fn experiment_one_units_gap_costs_only_that_unit_and_only_that_wave() {
        let (dense, a) = staggered_fixture(5.0, 3);
        let rows: Vec<(String, i64, f64)> = triples_of(&dense)
            .into_iter()
            .filter(|(u, d, _)| !(u == "c6" && (85..=90).contains(d)))
            .collect();
        let strict = estimate_effect(&PanelMatrix::from_triples(rows.clone()), &a, 7);
        assert_eq!(
            strict.dropped_waves.len(),
            1,
            "the old panel drops the day-71 wave: {:?}",
            strict.dropped_waves
        );
        let (m, dropped) = panel_dropping_sparse_units(rows, 0.9);
        assert!(dropped.is_empty(), "c6 holds 94 of 100 days: {dropped:?}");
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(r.dropped_waves.is_empty(), "{:?}", r.dropped_waves);
        assert_eq!(r.n_treated, 6, "both waves are estimated");
        assert_eq!(r.dropped_units.len(), 1, "{:?}", r.dropped_units);
        let left = &r.dropped_units[0];
        assert_eq!(left.unit, "c6");
        assert!(
            left.reason
                .contains("holds 14 of the 20 days in its after window")
                && left.reason.contains("90% coverage floor"),
            "{}",
            left.reason
        );
        assert!(
            (r.estimate - 5.0).abs() < 2.0,
            "the planted effect is recovered: {}",
            r.estimate
        );
    }

    /// The same panel gap, missed rows for the WHOLE comparison: a unit the
    /// permutation relabels into a wave it holds too little of is left out of
    /// that slot's mean under every relabelling alike, so the test still runs.
    #[test]
    fn experiment_a_thin_unit_does_not_void_the_permutation() {
        let (dense, a) = staggered_fixture(0.0, 11);
        let rows: Vec<(String, i64, f64)> = triples_of(&dense)
            .into_iter()
            .filter(|(u, d, _)| !(u == "t4" && (61..=66).contains(d)))
            .collect();
        let (m, _) = panel_dropping_sparse_units(rows, 0.9);
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(r.p_value.is_finite() && r.p_value > 0.0);
        assert!(r.ci_low.is_finite() && r.ci_high.is_finite());
    }

    fn thinned(
        effect: f64,
        seed: u64,
        out: &[&str],
        days: std::ops::RangeInclusive<i64>,
    ) -> (PanelMatrix, Assignment) {
        let (dense, a) = staggered_fixture(effect, seed);
        let rows: Vec<(String, i64, f64)> = triples_of(&dense)
            .into_iter()
            .filter(|(u, d, _)| !(out.contains(&u.as_str()) && days.contains(d)))
            .collect();
        (panel_dropping_sparse_units(rows, 0.5).0, a)
    }

    /// Two of the day-71 wave's three treated units hold 13 of its 20 after-days:
    /// one unit is left to compare, so the wave is dropped and named, and the
    /// estimate runs on the day-61 wave instead of refusing outright.
    #[test]
    fn experiment_a_wave_with_too_few_units_compared_is_dropped_not_refused() {
        let (m, a) = thinned(5.0, 3, &["t3", "t4"], 82..=88);
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.n_treated, 3, "only the day-61 wave is estimated");
        assert_eq!(r.dropped_waves.len(), 1, "{:?}", r.dropped_waves);
        assert!(
            r.dropped_waves[0].contains("only 1 treated"),
            "{:?}",
            r.dropped_waves
        );
        assert_eq!(
            r.waves.len(),
            1,
            "the per-wave list matches what was compared"
        );
        assert!(
            r.dropped_units.is_empty(),
            "no compared wave left a unit out: {:?}",
            r.dropped_units
        );
    }

    /// c6..c8 are thin in both waves' windows, so a relabelling that puts all three
    /// in the day-61 wave's treated slots has no treated mean. It stays in the
    /// exact null (scored as extreme) instead of shrinking it below `N`.
    #[test]
    fn experiment_a_relabelling_with_an_empty_arm_stays_in_the_exact_null() {
        let (m, a) = thinned(0.0, 11, &["c6", "c7", "c8"], 62..=68);
        let r = crate::engine::experiment::staggered::estimate_staggered(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(
            r.permutations, 18_480,
            "12! / (3! 3! 6!) relabellings, none dropped"
        );
        assert!(r.p_value >= 1.0 / 18_480.0);
    }

    /// A refused estimate ran no comparison, so it names no unit as left out of one.
    #[test]
    fn experiment_a_refused_estimate_lists_no_left_out_units() {
        let (m, mut a) = thinned(5.0, 3, &["c6"], 85..=90);
        a.alpha = 1e-9;
        let r = estimate_effect(&m, &a, 7);
        assert!(r.refusal.is_some());
        assert!(r.dropped_units.is_empty(), "{:?}", r.dropped_units);
        assert!(r.waves.is_empty());
    }

    #[test]
    fn experiment_a_unit_short_in_both_windows_names_both() {
        let (m, a) = thinned(5.0, 3, &["c6"], 66..=79);
        let r = estimate_effect(&m, &a, 7);
        let c6 = r
            .dropped_units
            .iter()
            .find(|d| d.unit == "c6")
            .expect("c6 is listed");
        assert!(
            c6.reason.contains("in its before window and")
                && c6.reason.contains("in its after window"),
            "{}",
            c6.reason
        );
    }

    #[test]
    fn experiment_a_named_unit_with_no_rows_is_dropped_and_listed() {
        let rows = fixture_rows(12, 60, 6, 20.0, 31, 4);
        let m = PanelMatrix::from_triples(rows);
        let mut a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        a.switch_day.insert("ghost".into(), Some(31));
        assert!(
            estimate_effect(&m, &a, 7).refusal.is_some(),
            "the strict entry still refuses"
        );
        let r = estimate_effect_dropping_missing(&m, &a, 7);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.n_treated, 6);
        assert_eq!(
            r.dropped_units,
            vec![DroppedUnit {
                unit: "ghost".into(),
                reason: "a treated unit with no rows in the panel".into()
            }]
        );
    }

    #[test]
    fn experiment_nothing_missing_is_exactly_the_strict_estimate() {
        let m = PanelMatrix::from_triples(fixture_rows(12, 60, 6, 20.0, 31, 4));
        let a = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let strict = estimate_effect(&m, &a, 7);
        let lenient = estimate_effect_dropping_missing(&m, &a, 7);
        assert_eq!(strict.estimate, lenient.estimate);
        assert!(lenient.dropped_units.is_empty());
    }
}
