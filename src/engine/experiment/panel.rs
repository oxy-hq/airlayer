//! The panel: one measure over a (unit, day) grid, and the calendar
//! arithmetic every window in this module is laid out with.

use chrono::Datelike;
use std::collections::{BTreeMap, BTreeSet};

/// The panel's day ordinal: `NaiveDate::num_days_from_ce()`, the convention
/// `metric_tree_fit`'s `json_to_day_ordinal` already uses. The host never
/// re-derives it.
pub fn day_ordinal(d: chrono::NaiveDate) -> i64 {
    i64::from(d.num_days_from_ce())
}

/// One measure over a (unit, day) grid. Row-major, dense, complete: every unit
/// has a value on every retained day. Fields are crate-private; the host builds
/// panels with `from_triples` and reads the sorted unit list with `units()`.
#[derive(Debug, Clone)]
pub struct PanelMatrix {
    pub(crate) units: Vec<String>,
    pub(crate) days: Vec<i64>,
    pub(crate) values: Vec<f64>,
}

impl PanelMatrix {
    /// `(unit name, day ordinal, value)`. Units come out sorted by name.
    pub fn from_triples(rows: impl IntoIterator<Item = (String, i64, f64)>) -> Self {
        let mut cells: BTreeMap<(String, i64), f64> = BTreeMap::new();
        let mut units: BTreeSet<String> = BTreeSet::new();
        let mut all_days: BTreeSet<i64> = BTreeSet::new();
        for (u, d, v) in rows {
            units.insert(u.clone());
            all_days.insert(d);
            cells.insert((u, d), v);
        }
        let units: Vec<String> = units.into_iter().collect();
        // A day any unit is missing is a day no unit can use: a
        // difference-in-differences compares the SAME calendar days across units.
        let days: Vec<i64> = all_days
            .into_iter()
            .filter(|d| units.iter().all(|u| cells.contains_key(&(u.clone(), *d))))
            .collect();
        let mut values = Vec::with_capacity(units.len() * days.len());
        for u in &units {
            for d in &days {
                values.push(cells[&(u.clone(), *d)]);
            }
        }
        Self {
            units,
            days,
            values,
        }
    }

    /// The unit names, sorted — the order every index in this module refers to.
    pub fn units(&self) -> &[String] {
        &self.units
    }

    pub fn n_units(&self) -> usize {
        self.units.len()
    }

    pub fn n_days(&self) -> usize {
        self.days.len()
    }

    pub fn get(&self, unit: usize, day: usize) -> f64 {
        self.values[unit * self.days.len() + day]
    }

    /// The row this unit name occupies AFTER sorting. Arms are matched through
    /// this, never through the caller's own ordering.
    pub fn unit_index(&self, name: &str) -> Option<usize> {
        self.units.binary_search_by(|u| u.as_str().cmp(name)).ok()
    }

    pub fn mean(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        self.values.iter().sum::<f64>() / self.values.len() as f64
    }

    /// Mean over the half-open INDEX range `from..to`. `None` when empty or out of range.
    pub fn window_mean(&self, unit: usize, from: usize, to: usize) -> Option<f64> {
        if to <= from || to > self.n_days() {
            return None;
        }
        Some((from..to).map(|d| self.get(unit, d)).sum::<f64>() / (to - from) as f64)
    }

    /// Retained index range inside a half-open CALENDAR span. Every window in
    /// this module is expressed as ordinals and converted here, because retained
    /// positions are a subset of the calendar and counting in them stretches a
    /// window silently.
    pub fn ordinal_range(&self, from_ord: i64, to_ord: i64) -> (usize, usize) {
        let lo = self.days.partition_point(|d| *d < from_ord);
        let hi = self.days.partition_point(|d| *d < to_ord);
        (lo, hi)
    }

    /// Retained days divided by calendar days in that span. A caller refuses a
    /// window whose coverage is below its own floor rather than shortening it.
    pub fn coverage(&self, from_ord: i64, to_ord: i64) -> f64 {
        if to_ord <= from_ord {
            return 0.0;
        }
        let (lo, hi) = self.ordinal_range(from_ord, to_ord);
        (hi - lo) as f64 / (to_ord - from_ord) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::matrix_from;

    #[test]
    fn experiment_panel_matrix_drops_incomplete_days() {
        // unit "b" has no value on day 2, so day 2 must vanish for BOTH units.
        let m = matrix_from(&[
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 3, 30.0),
            ("b", 1, 40.0),
            ("b", 3, 60.0),
        ]);
        assert_eq!(m.n_units(), 2);
        assert_eq!(m.days, vec![1, 3]);
        assert_eq!(m.get(0, 1), 30.0);
        assert_eq!(m.mean(), (10.0 + 30.0 + 40.0 + 60.0) / 4.0);
    }

    #[test]
    fn experiment_panel_matrix_indexes_units_by_name_not_insertion_order() {
        // Inserted t,t,c — stored c,t,t. Anything matching arms by the
        // caller's order would swap them and invert the sign of the effect.
        let m = matrix_from(&[("t1", 1, 1.0), ("t2", 1, 1.0), ("c1", 1, 1.0)]);
        assert_eq!(
            m.units().to_vec(),
            vec!["c1", "t1", "t2"],
            "units() is the sorted list"
        );
        assert_eq!(m.unit_index("t1"), Some(1));
        assert_eq!(m.unit_index("nope"), None);
    }

    #[test]
    fn experiment_panel_matrix_window_mean() {
        let m = matrix_from(&[
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 3, 30.0),
            ("a", 4, 40.0),
        ]);
        assert_eq!(m.window_mean(0, 0, 2), Some(15.0));
        assert_eq!(m.window_mean(0, 2, 2), None, "an empty window has no mean");
        assert_eq!(
            m.window_mean(0, 2, 9),
            None,
            "a window past the end has no mean"
        );
    }

    #[test]
    fn experiment_panel_matrix_measures_windows_in_calendar_days() {
        // Days 1,2,3 then a gap to 20,21,22. A 3-index window starting at day 1
        // must NOT be allowed to reach day 20 — that is a 20-day calendar span.
        let m = matrix_from(&[
            ("a", 1, 1.0),
            ("a", 2, 1.0),
            ("a", 3, 1.0),
            ("a", 20, 1.0),
            ("a", 21, 1.0),
            ("a", 22, 1.0),
        ]);
        assert_eq!(
            m.ordinal_range(1, 4),
            (0, 3),
            "days 1..4 are the first three rows"
        );
        // Half-open: [1, 21) holds days 1, 2, 3 and 20 — day 21 is excluded.
        assert_eq!(
            m.ordinal_range(1, 21),
            (0, 4),
            "days 1..21 hold four retained rows"
        );
        assert!(
            (m.coverage(1, 4) - 1.0).abs() < 1e-12,
            "a dense span is fully covered"
        );
        assert!(
            (m.coverage(1, 21) - 4.0 / 20.0).abs() < 1e-12,
            "4 retained days across a 20-day calendar span is 20% coverage"
        );
    }

    #[test]
    fn experiment_day_ordinal_matches_the_metric_tree_panel_convention() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 3, 2).expect("a valid date");
        assert_eq!(
            day_ordinal(d),
            739_677,
            "num_days_from_ce: 0001-01-01 is day 1"
        );
        let from_fit = crate::engine::metric_tree_fit::json_to_day_ordinal(&serde_json::json!(
            "2026-03-02T00:00:00Z"
        ));
        assert_eq!(
            Some(day_ordinal(d)),
            from_fit,
            "the host's ordinal and the metric-tree panel's must be the same day"
        );
    }
}
