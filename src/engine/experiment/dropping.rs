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
use crate::engine::experiment::PanelMatrix;
use std::collections::{BTreeMap, BTreeSet};

/// A unit left out of the estimate, and why.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DroppedUnit {
    pub unit: String,
    pub reason: String,
}

/// [`PanelMatrix::from_triples`], except that a unit holding a value on fewer
/// than `min_unit_coverage` of the days any unit holds is dropped (and listed)
/// before incomplete days are dropped, so its gaps cost only itself.
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
    (PanelMatrix::from_triples(kept), dropped)
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
    r.dropped_units = missing
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
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::{assign, fixture_rows};

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
        assert_eq!(m.n_days(), 59, "the one missing day still goes for all");
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
