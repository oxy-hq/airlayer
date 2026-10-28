//! The proportional scale: an outcome read as `ln(1 + y)`.
//!
//! The estimators average absolute differences, so a lift is in the outcome's
//! own units per unit-day. When units differ in size by orders of magnitude
//! that answers the wrong question: a first-aid kit going from 15 to 40 a day
//! (+167%) is a +25 that ordinary swings in a 100-a-day comparison SKU drown
//! out, while ten slow sellers that cannot move more than one unit a day pull
//! the mean toward zero. On the log scale every unit's change is a ratio, so a
//! big and a small unit moving by the same share move by the same amount.
//!
//! `ln(1 + y)` rather than `ln(y)` so a day with zero sales stays readable. The
//! effect `x` on that scale is reported back as `exp(x) − 1`, the proportional
//! lift; it is approximate for outcomes near zero, where the `+ 1` dominates.
//! The transform is monotone, so an interval maps endpoint by endpoint and a
//! test's verdict is unchanged by reporting it back.

use crate::engine::experiment::PanelMatrix;

pub const PROPORTIONAL_ESTIMAND: &str =
    "mean effect on ln(1 + outcome) per treated unit-day, unweighted across units; \
     exp(effect) − 1 is the approximate proportional lift";

impl PanelMatrix {
    /// The panel with every held value replaced by `ln(1 + value)`. `Err` names
    /// the first unit and day holding a value the log cannot read (negative
    /// or non-finite): a refund-heavy day is a real value, not one to clamp.
    pub fn log1p(&self) -> Result<PanelMatrix, String> {
        let n_days = self.days.len();
        let mut out = self.clone();
        for (i, v) in out.values.iter_mut().enumerate() {
            let (u, d) = (i / n_days.max(1), i % n_days.max(1));
            if !self.is_present(u, d) {
                continue;
            }
            if !(v.is_finite() && *v >= 0.0) {
                return Err(format!(
                    "unit '{}' holds {v} on day {}: a proportional lift needs every value \
                     to be zero or more",
                    self.units[u], self.days[d]
                ));
            }
            *v = v.ln_1p();
        }
        Ok(out)
    }
}

/// An effect on the log scale as a proportional lift: `0.5` → `+64.9%` as
/// `0.649`. Infinite endpoints stay infinite (an unbounded interval is still
/// unbounded); `NaN` stays `NaN`.
pub fn proportional(log_effect: f64) -> f64 {
    log_effect.exp_m1()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::estimate::estimate_effect;
    use crate::engine::experiment::testkit::assign;

    #[test]
    fn experiment_log1p_maps_held_values_and_keeps_gaps() {
        let m = PanelMatrix::from_triples_keeping_gaps(
            [("a", 1, 0.0), ("a", 2, 3.0), ("b", 1, 1.0)].map(|(u, d, v)| (u.to_string(), d, v)),
        );
        let l = m.log1p().expect("all non-negative");
        assert_eq!(l.get(0, 0), 0.0);
        assert!((l.get(0, 1) - 4f64.ln()).abs() < 1e-12);
        assert!(!l.is_present(1, 1), "a gap stays a gap");
    }

    #[test]
    fn experiment_log1p_refuses_a_negative_value_by_name() {
        let m = PanelMatrix::from_triples([("a".to_string(), 7, -2.0)]);
        let e = m.log1p().expect_err("negative");
        assert!(e.contains("'a'") && e.contains("day 7"), "{e}");
    }

    #[test]
    fn experiment_proportional_maps_back() {
        assert!(
            (proportional(2f64.ln()) - 1.0).abs() < 1e-12,
            "ln 2 is +100%"
        );
        assert_eq!(proportional(0.0), 0.0);
        assert_eq!(proportional(f64::INFINITY), f64::INFINITY);
        assert_eq!(proportional(f64::NEG_INFINITY), -1.0);
    }

    /// The scale question in one panel: three treated units selling 30, 8 and
    /// 3 a day each gain about 150%, beside 37 controls sized 1 to 100 a day
    /// whose level drifts by up to 40% between the windows, as real SKUs do.
    /// In units the lifts are +45, +12 and +4.5 — so uneven, beside a 100-a-day
    /// control's ±40 drift, that they barely clear the bar (p ≈ 0.05). As shares
    /// they are all near ln 2.5 = 0.92 against drifts of ±0.4, and the lift
    /// stands out by orders of magnitude.
    #[test]
    fn experiment_a_proportional_lift_on_few_units_reads_on_the_log_scale() {
        let mut rows = Vec::new();
        let mut rng = crate::engine::experiment::SplitMix64::new(9);
        let mut draw = |width: f64| ((rng.next_u64() % 10_000) as f64 / 10_000.0 - 0.5) * width;
        for u in 0..40 {
            let treated = u < 3;
            let (name, level) = if treated {
                (format!("t{u}"), [30.0, 8.0, 3.0][u])
            } else {
                (format!("c{u}"), 1.0 + (u as f64 - 3.0) * 2.7)
            };
            let after = if treated {
                2.5 * (1.0 + draw(0.2))
            } else {
                1.0 + draw(0.8)
            };
            for day in 1..=60_i64 {
                let shift = if day >= 31 { after } else { 1.0 };
                let noise = 1.0 + draw(0.2);
                rows.push((name.clone(), day, (level * shift * noise).round()));
            }
        }
        let m = PanelMatrix::from_triples(rows);
        let a = assign(&m, &["t0", "t1", "t2"], 31);
        let units = estimate_effect(&m, &a, 7);
        let log = estimate_effect(&m.log1p().expect("counts"), &a, 7);
        // In units the biggest treated unit's lift dominates and the small ones
        // vanish into the controls' spread; on the log scale all three read alike.
        assert!(log.significant, "log p {}", log.p_value);
        assert!(
            log.p_value < units.p_value / 10.0,
            "log p {} against units p {}",
            log.p_value,
            units.p_value
        );
        let lift = proportional(log.estimate);
        assert!((lift - 1.5).abs() < 0.4, "read {lift}");
    }
}
