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

    /// The sub-panel over `units` (by name) and `days` (by ordinal). Both are
    /// sorted and de-duplicated first, so the result keeps the sorted-units
    /// invariant `unit_index` relies on. Every name and ordinal must already be
    /// in this panel — callers pass subsets of `units` / `days`, never input.
    pub(crate) fn restrict(&self, units: &[String], days: &[i64]) -> PanelMatrix {
        let mut units = units.to_vec();
        units.sort();
        units.dedup();
        let mut days = days.to_vec();
        days.sort_unstable();
        days.dedup();
        let rows: Vec<usize> = units
            .iter()
            .map(|u| self.unit_index(u).expect("restrict: a unit of this panel"))
            .collect();
        let cols: Vec<usize> = days
            .iter()
            .map(|d| {
                self.days
                    .binary_search(d)
                    .expect("restrict: a day of this panel")
            })
            .collect();
        let values = rows
            .iter()
            .flat_map(|r| cols.iter().map(move |c| self.get(*r, *c)))
            .collect();
        PanelMatrix {
            units,
            days,
            values,
        }
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

/// The two comparison windows as half-open **day-ordinal** spans. Ordinals, not
/// retained-row indices: dropping incomplete days makes the retained rows a
/// subset of the calendar, so a window counted in rows silently spans longer
/// than it claims and prices a design nobody ran.
#[derive(Debug, Clone, Copy)]
pub struct Windows {
    pub pre: (i64, i64),
    pub post: (i64, i64),
}

/// Lay the windows out around a switch DATE, excluding an anticipation band
/// before it and a washout band after it. `None` when a window would be empty,
/// fall outside history, or hold less than `min_coverage` of its calendar span —
/// the caller turns that into its own refusal, because only it knows whether
/// the shortfall is the design's or the data's. `min_coverage` is always the
/// registered `coverage_floor`.
pub fn windows_around(
    m: &PanelMatrix,
    switch_ord: i64,
    pre_days: usize,
    post_days: usize,
    anticipation: usize,
    washout: usize,
    min_coverage: f64,
) -> Option<Windows> {
    if pre_days == 0 || post_days == 0 {
        return None;
    }
    let (first, last) = (*m.days.first()?, *m.days.last()?);
    let pre_end = switch_ord - anticipation as i64;
    let pre_start = pre_end - pre_days as i64;
    let post_start = switch_ord + washout as i64;
    let post_end = post_start + post_days as i64;
    if pre_start < first || post_end > last + 1 {
        return None;
    }
    if m.coverage(pre_start, pre_end) < min_coverage
        || m.coverage(post_start, post_end) < min_coverage
    {
        return None;
    }
    Some(Windows {
        pre: (pre_start, pre_end),
        post: (post_start, post_end),
    })
}

/// One unit's collapsed pre/post difference. `index` is its row in the matrix.
#[derive(Debug, Clone)]
pub struct UnitDelta {
    pub unit: String,
    pub index: usize,
    pub pre_mean: f64,
    pub post_mean: f64,
    pub delta: f64,
}

/// Collapse every unit to a single pre/post difference. Units missing either
/// window are dropped rather than zero-filled.
pub fn collapse(m: &PanelMatrix, w: &Windows) -> Vec<UnitDelta> {
    let (p0, p1) = m.ordinal_range(w.pre.0, w.pre.1);
    let (q0, q1) = m.ordinal_range(w.post.0, w.post.1);
    (0..m.n_units())
        .filter_map(|u| {
            let pre = m.window_mean(u, p0, p1)?;
            let post = m.window_mean(u, q0, q1)?;
            Some(UnitDelta {
                unit: m.units[u].clone(),
                index: u,
                pre_mean: pre,
                post_mean: post,
                delta: post - pre,
            })
        })
        .collect()
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

    fn dense(days: i64) -> PanelMatrix {
        matrix_from(&(1..=days).map(|d| ("a", d, 1.0)).collect::<Vec<_>>())
    }

    #[test]
    fn experiment_windows_exclude_anticipation_and_washout() {
        // switch on day 10, 3 pre days, 3 post, 2 days anticipation before and
        // 2 days washout after: the bands next to the switch are excluded, and
        // every boundary is a CALENDAR day, not a retained-row position.
        let m = dense(30);
        let w = windows_around(&m, 10, 3, 3, 2, 2, 0.9).expect("fits");
        assert_eq!(w.pre, (5, 8), "pre ends 2 days BEFORE the switch");
        assert_eq!(w.post, (12, 15), "post starts 2 days AFTER the switch");
        assert!(
            windows_around(&m, 3, 3, 3, 2, 2, 0.9).is_none(),
            "not enough history before"
        );
        assert!(
            windows_around(&dense(13), 10, 3, 3, 2, 2, 0.9).is_none(),
            "not enough history after"
        );
        assert!(
            windows_around(&m, 10, 0, 3, 0, 0, 0.9).is_none(),
            "a zero-length window"
        );
    }

    #[test]
    fn experiment_windows_refuse_a_span_the_calendar_does_not_fill() {
        // Days 1-3 present, 4-18 missing, 19-40 present. A 10-day pre window
        // ending at day 11 holds 3 of 10 calendar days: the window is not the
        // length it was registered as, so it is refused rather than shortened.
        let mut rows: Vec<(&str, i64, f64)> = (1..=3).map(|d| ("a", d, 1.0)).collect();
        rows.extend((19..=40).map(|d| ("a", d, 1.0)));
        let m = matrix_from(&rows);
        assert!(
            windows_around(&m, 11, 10, 10, 0, 0, 0.9).is_none(),
            "30% coverage must not pass a 90% floor"
        );
        assert!(
            windows_around(&m, 30, 10, 10, 0, 0, 0.9).is_some(),
            "a window inside the dense stretch is fine"
        );
    }

    /// The floor is the REGISTERED one: the same 85%-covered window is refused
    /// at `coverage_floor: 0.9` and accepted at `0.8`.
    #[test]
    fn experiment_windows_honour_the_registered_coverage_floor() {
        let rows: Vec<(&str, i64, f64)> = (1..=60)
            .filter(|d| ![13, 17, 21].contains(d))
            .map(|d| ("a", d, 1.0))
            .collect();
        let m = matrix_from(&rows);
        assert!(
            (m.coverage(11, 31) - 0.85).abs() < 1e-12,
            "17 of 20 calendar days"
        );
        assert!(
            windows_around(&m, 31, 20, 20, 0, 0, 0.9).is_none(),
            "85% refused at a 90% floor"
        );
        assert!(
            windows_around(&m, 31, 20, 20, 0, 0, 0.8).is_some(),
            "85% accepted at an 80% floor"
        );
    }

    #[test]
    fn experiment_collapse_gives_one_number_per_unit() {
        let m = matrix_from(&[
            ("a", 1, 10.0),
            ("a", 2, 10.0),
            ("a", 3, 14.0),
            ("a", 4, 16.0),
            ("b", 1, 50.0),
            ("b", 2, 50.0),
            ("b", 3, 50.0),
            ("b", 4, 50.0),
        ]);
        let d = collapse(&m, &windows_around(&m, 3, 2, 2, 0, 0, 1.0).expect("fits"));
        assert_eq!(d.len(), 2, "one observation per unit, never per day");
        assert_eq!((d[0].unit.as_str(), d[0].index), ("a", 0));
        assert_eq!(d[0].delta, 5.0);
        assert_eq!(d[1].delta, 0.0, "a flat unit contributes a zero difference");
    }

    #[test]
    fn experiment_panel_matrix_restrict_keeps_names_and_ordinals() {
        let m = matrix_from(&[
            ("a", 1, 1.0),
            ("a", 2, 2.0),
            ("a", 3, 3.0),
            ("b", 1, 4.0),
            ("b", 2, 5.0),
            ("b", 3, 6.0),
            ("c", 1, 7.0),
            ("c", 2, 8.0),
            ("c", 3, 9.0),
        ]);
        let r = m.restrict(&["c".to_string(), "a".to_string()], &[3, 1]);
        assert_eq!(
            r.units,
            vec!["a", "c"],
            "restricted units stay sorted, so unit_index still works"
        );
        assert_eq!(r.days, vec![1, 3]);
        assert_eq!(
            (r.get(0, 0), r.get(0, 1), r.get(1, 0), r.get(1, 1)),
            (1.0, 3.0, 7.0, 9.0)
        );
        assert_eq!(r.unit_index("c"), Some(1));
    }
}
