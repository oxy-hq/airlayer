//! The lever as an instrument: the Wald ratio ITT(target) / ITT(driver), with
//! an Anderson–Rubin interval built by inverting the unchanged estimator on
//! `target − β·driver`.

use crate::engine::experiment::PanelMatrix;
use std::collections::BTreeSet;

/// Restrict two panels to the units AND days both hold, matched by NAME and by
/// ORDINAL — never by position. A unit or day only one panel holds is dropped
/// from both: a ratio of two effects is only a ratio when both are measured on
/// the same units over the same days. `estimate_ratio` refuses by name any
/// assigned unit this would drop; dropped days surface as window coverage.
pub(crate) fn align(a: &PanelMatrix, b: &PanelMatrix) -> (PanelMatrix, PanelMatrix) {
    let units: Vec<String> = a
        .units
        .iter()
        .filter(|u| b.unit_index(u).is_some())
        .cloned()
        .collect();
    let b_days: BTreeSet<i64> = b.days.iter().copied().collect();
    let days: Vec<i64> = a
        .days
        .iter()
        .copied()
        .filter(|d| b_days.contains(d))
        .collect();
    (a.restrict(&units, &days), b.restrict(&units, &days))
}

/// `target − beta·driver`, cell by cell. Both panels come out of `align`, so
/// their units and days are identical and in the same order.
pub(crate) fn minus_scaled(target: &PanelMatrix, driver: &PanelMatrix, beta: f64) -> PanelMatrix {
    debug_assert!(
        target.units == driver.units && target.days == driver.days,
        "minus_scaled needs a pair that came out of align"
    );
    let values = target
        .values
        .iter()
        .zip(&driver.values)
        .map(|(t, d)| t - beta * d)
        .collect();
    PanelMatrix {
        units: target.units.clone(),
        days: target.days.clone(),
        values,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::{ratio_panels, triples_of};

    /// A unit only the target holds, sorting into the MIDDLE of the order:
    /// every unit after it sits one position later in the target than in the
    /// driver. Its values are wild, so any positional pairing shows at once.
    fn with_target_only_unit(t: &PanelMatrix) -> PanelMatrix {
        let mut rows = triples_of(t);
        rows.extend((1..=60).map(|day| ("s05x".to_string(), day, 1.0e6)));
        PanelMatrix::from_triples(rows)
    }

    #[test]
    fn experiment_ratio_align_matches_units_by_name_not_position() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let t_extra = with_target_only_unit(&t);
        assert_eq!(
            (t_extra.unit_index("s06"), d.unit_index("s06")),
            (Some(7), Some(6)),
            "precondition: the two panels disagree on positions"
        );
        let (ta, da) = align(&t_extra, &d);
        assert_eq!(ta.units, d.units, "the target-only unit is dropped");
        assert_eq!(da.units, d.units);
        for (u, name) in ta.units.iter().enumerate() {
            let src = t
                .unit_index(name)
                .expect("aligned units come from the target");
            for i in 0..ta.n_days() {
                assert_eq!(
                    ta.get(u, i),
                    t.get(src, i),
                    "{name} day {i} was paired by position"
                );
            }
        }
    }

    #[test]
    fn experiment_ratio_align_drops_days_one_panel_lacks() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let d_short = PanelMatrix::from_triples(
            triples_of(&d)
                .into_iter()
                .filter(|(_, day, _)| *day != 5 && *day != 60),
        );
        let (ta, da) = align(&t, &d_short);
        assert_eq!(ta.days, da.days, "both sides keep the same ordinals");
        assert_eq!(ta.n_days(), 58);
        assert!(!ta.days.contains(&5) && !ta.days.contains(&60));
    }

    #[test]
    fn experiment_ratio_minus_scaled_is_cellwise() {
        let (t, d, _) = ratio_panels(40.0, 0.35, 0.0, 21);
        let c = minus_scaled(&t, &d, 0.35);
        for u in 0..c.n_units() {
            for i in 0..c.n_days() {
                assert!((c.get(u, i) - (t.get(u, i) - 0.35 * d.get(u, i))).abs() < 1e-9);
            }
        }
    }
}
