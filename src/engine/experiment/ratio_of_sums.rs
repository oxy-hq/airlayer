//! A ratio outcome: a share or a rate per unit, such as merchant-fulfilled units
//! over all units sold.
//!
//! A ratio cannot be averaged over days — a day with one sale would weigh as much
//! as a day with a hundred — so each unit's window is the ratio of its SUMS over
//! the window, `Σ numerator / Σ denominator`. Each unit then collapses to one
//! post − pre difference of that ratio and the arms are compared with Welch, as
//! the common-switch-date path does for a plain measure. A unit whose window has
//! no denominator (nothing sold) has no ratio there: it is left out and listed in
//! `dropped_units`, never read as zero. One switch date only in v1.

use crate::engine::experiment::dropping::DroppedUnit;
use crate::engine::experiment::estimate::{
    resolve_switch, window_refusal, Assignment, EffectResult,
};
use crate::engine::experiment::{
    t_quantile, t_two_sided_p, welch, windows_around, PanelMatrix, WelchRefusal, Windows,
};

pub const RATIO_ESTIMAND: &str =
    "mean change in each unit's ratio of sums (post window − pre window), unweighted across units";

/// The effect on `Σ numerator / Σ denominator` per unit. Both panels are read
/// over the days and units they share.
pub fn estimate_ratio_of_sums(
    numerator: &PanelMatrix,
    denominator: &PanelMatrix,
    a: &Assignment,
) -> EffectResult {
    let (num, den) = common(numerator, denominator);
    let s = match resolve_switch(&num, a) {
        Ok(s) => s,
        Err(reason) => return EffectResult::refused(reason),
    };
    let Some(w) = windows_around(
        &num,
        s,
        a.pre_days,
        a.post_days,
        a.anticipation_days,
        a.washout_days,
        a.coverage_floor,
    ) else {
        return EffectResult::refused(window_refusal(s, a));
    };
    let (treated, control, dropped) = arms(&num, &den, a, &w);
    let test = match welch(&treated, &control) {
        Ok(t) => t,
        Err(WelchRefusal::ThinArm { treated, control }) => {
            let mut r = EffectResult::refused(format!(
                "a difference-in-differences needs at least 2 units per arm with a ratio in \
                 both windows; got {treated} treated and {control} control"
            ));
            r.dropped_units = dropped;
            return r;
        }
        Err(WelchRefusal::NoSpread) => {
            return EffectResult::refused(
                "every unit's ratio moved identically, so there is no spread to measure the \
                 difference against",
            )
        }
    };
    let crit = t_quantile(test.df, a.alpha, a.family);
    EffectResult {
        estimate: test.diff,
        se: test.se,
        t_stat: test.t,
        df: test.df,
        ci_low: test.diff - crit * test.se,
        ci_high: test.diff + crit * test.se,
        p_value: t_two_sided_p(test.t, test.df),
        n_treated: treated.len(),
        n_control: control.len(),
        significant: test.t.abs() >= crit,
        estimand: RATIO_ESTIMAND,
        design: "common switch date",
        dropped_waves: Vec::new(),
        pre_trend: None,
        size_bias: None,
        retained_on_days: None,
        dropped_units: dropped,
        waves: Vec::new(),
        refusal: None,
    }
}

/// Both panels cut to the units and days each holds.
fn common(n: &PanelMatrix, d: &PanelMatrix) -> (PanelMatrix, PanelMatrix) {
    let units: Vec<String> = n
        .units()
        .iter()
        .filter(|u| d.unit_index(u).is_some())
        .cloned()
        .collect();
    let days: Vec<i64> = n
        .days
        .iter()
        .filter(|day| d.days.binary_search(day).is_ok())
        .copied()
        .collect();
    (n.restrict(&units, &days), d.restrict(&units, &days))
}

/// Each named unit's post − pre ratio change, by arm; units with no ratio in a
/// window are listed instead.
fn arms(
    num: &PanelMatrix,
    den: &PanelMatrix,
    a: &Assignment,
    w: &Windows,
) -> (Vec<f64>, Vec<f64>, Vec<DroppedUnit>) {
    let (p0, p1) = num.ordinal_range(w.pre.0, w.pre.1);
    let (q0, q1) = num.ordinal_range(w.post.0, w.post.1);
    let (mut treated, mut control, mut dropped) = (Vec::new(), Vec::new(), Vec::new());
    let mut names: Vec<&String> = a.switch_day.keys().collect();
    names.sort();
    for name in names {
        let Some(u) = num.unit_index(name) else {
            continue;
        };
        // On panels with gaps, a day counts only where the unit holds both sides,
        // and a window it holds too little of is not compared.
        let held = |from: usize, to: usize| -> Vec<usize> {
            (from..to)
                .filter(|i| num.is_present(u, *i) && den.is_present(u, *i))
                .collect()
        };
        let thin = |days: &[usize], span: (i64, i64)| {
            (days.len() as f64) < w.floor * (span.1 - span.0) as f64
        };
        let (pre_days, post_days) = (held(p0, p1), held(q0, q1));
        if num.has_gaps() || den.has_gaps() {
            let short = [("pre", &pre_days, w.pre), ("post", &post_days, w.post)]
                .into_iter()
                .find(|(_, d, span)| thin(d, *span));
            if let Some((which, d, span)) = short {
                dropped.push(DroppedUnit {
                    unit: name.clone(),
                    reason: format!(
                        "holds {} of the {} days in its {which} window, under the {:.0}% \
                         coverage floor",
                        d.len(),
                        span.1 - span.0,
                        w.floor * 100.0
                    ),
                });
                continue;
            }
        }
        let ratio = |days: &[usize]| {
            let n: f64 = days.iter().map(|i| num.get(u, *i)).sum();
            let d: f64 = days.iter().map(|i| den.get(u, *i)).sum();
            (d != 0.0 && d.is_finite() && n.is_finite()).then(|| n / d)
        };
        match (ratio(&pre_days), ratio(&post_days)) {
            (Some(pre), Some(post)) => match a.switch_day[name] {
                Some(_) => treated.push(post - pre),
                None => control.push(post - pre),
            },
            (pre, _) => dropped.push(DroppedUnit {
                unit: name.clone(),
                reason: format!(
                    "no denominator in its {} window, so it has no ratio there",
                    if pre.is_none() { "pre" } else { "post" }
                ),
            }),
        }
    }
    (treated, control, dropped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::assign;
    use crate::engine::experiment::SplitMix64;

    /// 8 treated `t*` and 8 control `c*` units over days 1..=40, switch on 21.
    /// Each unit sells 50–150 a day; its share is 0.1, and a treated unit's
    /// rises to `0.1 + effect` from the switch on, with per-day noise.
    fn panels(effect: f64) -> (PanelMatrix, PanelMatrix) {
        let mut rng = SplitMix64::new(11);
        let (mut num, mut den) = (Vec::new(), Vec::new());
        for u in 0..16 {
            let name = if u < 8 {
                format!("t{u}")
            } else {
                format!("c{u}")
            };
            for day in 1..=40_i64 {
                let total = 50.0 + (rng.next_u64() % 100) as f64;
                let noise = ((rng.next_u64() % 1000) as f64 / 1000.0 - 0.5) * 0.04;
                let share = 0.1 + noise + if u < 8 && day >= 21 { effect } else { 0.0 };
                num.push((name.clone(), day, (total * share).max(0.0)));
                den.push((name.clone(), day, total));
            }
        }
        (
            PanelMatrix::from_triples(num),
            PanelMatrix::from_triples(den),
        )
    }

    fn treated() -> Vec<&'static str> {
        vec!["t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"]
    }

    #[test]
    fn experiment_a_planted_share_change_is_read_as_a_ratio_of_sums() {
        let (num, den) = panels(0.2);
        let r = estimate_ratio_of_sums(&num, &den, &assign(&num, &treated(), 21));
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!((r.estimate - 0.2).abs() < 0.02, "read {}", r.estimate);
        assert!(r.significant);
        assert_eq!((r.n_treated, r.n_control), (8, 8));
        assert_eq!(r.estimand, RATIO_ESTIMAND);
    }

    #[test]
    fn experiment_no_planted_change_reads_near_zero() {
        let (num, den) = panels(0.0);
        let r = estimate_ratio_of_sums(&num, &den, &assign(&num, &treated(), 21));
        assert!(r.estimate.abs() < 0.02, "read {}", r.estimate);
    }

    #[test]
    fn experiment_a_unit_with_no_denominator_in_a_window_is_listed_not_zeroed() {
        let (num, den) = panels(0.2);
        let zeroed: Vec<(String, i64, f64)> = crate::engine::experiment::testkit::triples_of(&den)
            .into_iter()
            .map(|(u, d, v)| {
                if u == "c9" && d >= 21 {
                    (u, d, 0.0)
                } else {
                    (u, d, v)
                }
            })
            .collect();
        let den = PanelMatrix::from_triples(zeroed);
        let r = estimate_ratio_of_sums(&num, &den, &assign(&num, &treated(), 21));
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.n_control, 7);
        assert_eq!(
            r.dropped_units,
            vec![DroppedUnit {
                unit: "c9".into(),
                reason: "no denominator in its post window, so it has no ratio there".into()
            }]
        );
    }

    /// Panels with gaps: c9 lacks six post days and is listed; c10 lacks one
    /// and is still compared over the days it holds.
    #[test]
    fn experiment_a_ratio_over_panels_with_gaps_leaves_out_only_the_thin_unit() {
        let (num, den) = panels(0.2);
        let gaps = |m: &PanelMatrix| {
            PanelMatrix::from_triples_keeping_gaps(
                crate::engine::experiment::testkit::triples_of(m)
                    .into_iter()
                    .filter(|(u, d, _)| {
                        !(u == "c9" && (30..=35).contains(d)) && !(u == "c10" && *d == 5)
                    }),
            )
        };
        let (num, den) = (gaps(&num), gaps(&den));
        let r = estimate_ratio_of_sums(&num, &den, &assign(&num, &treated(), 21));
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!((r.n_treated, r.n_control), (8, 7));
        assert_eq!(
            r.dropped_units,
            vec![DroppedUnit {
                unit: "c9".into(),
                reason: "holds 14 of the 20 days in its post window, under the 90% coverage \
                         floor"
                    .into()
            }]
        );
        assert!((r.estimate - 0.2).abs() < 0.02, "read {}", r.estimate);
    }

    #[test]
    fn experiment_a_staggered_ratio_is_refused() {
        let (num, den) = panels(0.2);
        let mut a = assign(&num, &treated(), 21);
        a.switch_day.insert("t7".into(), Some(25));
        let r = estimate_ratio_of_sums(&num, &den, &a);
        assert!(r.refusal.unwrap().contains("staggered"));
    }
}
