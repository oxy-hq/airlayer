//! The paired switchback: the whole fleet alternates on and off over periods in
//! a seeded random order. The coin is the randomisation, so the coin is the test.

use crate::engine::experiment::estimate::{Decision, EffectResult};
use crate::engine::experiment::permutation::{
    min_attainable_p, PermOutcome, ENUMERATE_BELOW, PERMUTATIONS,
};
use crate::engine::experiment::staggered::endpoint;
use crate::engine::experiment::{
    fmt_p, per_comparison_alpha, validate_rates, PanelMatrix, SplitMix64,
};

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct Period {
    pub from_day: i64,
    pub on: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SwitchbackSchedule {
    /// Consecutive, each `period_days` long, in pairs; one on + one off per pair.
    pub periods: Vec<Period>,
    pub period_days: usize,
    /// Excluded at the start of EVERY period — each one starts with a switch.
    pub washout_days: usize,
    pub coverage_floor: f64,
    pub alpha: f64,
    pub family: usize,
}

/// Seeded: for each pair a coin decides which half is on. Empty when `pairs` is
/// 0 or `period_days` is below 1 — the estimator and the placebo refuse that.
pub fn propose_switchback(
    first_day: i64,
    period_days: i64,
    pairs: usize,
    seed: u64,
) -> Vec<Period> {
    if period_days < 1 {
        return Vec::new();
    }
    let mut rng = SplitMix64::new(seed);
    (0..pairs)
        .flat_map(|k| {
            let first_on = rng.next_u64() >> 63 == 1;
            let from = first_day + 2 * k as i64 * period_days;
            [
                Period {
                    from_day: from,
                    on: first_on,
                },
                Period {
                    from_day: from + period_days,
                    on: !first_on,
                },
            ]
        })
        .collect()
}

/// A period must be at least a day, and the washout shorter than it. Shared by
/// every check that takes a `period_days` / `washout_days` pair, so they all
/// refuse in the same words.
pub(crate) fn validate_period_washout(
    period_days: usize,
    washout_days: usize,
) -> Result<(), String> {
    if period_days == 0 {
        return Err("a switchback period must be at least 1 day long; period_days was 0".into());
    }
    if washout_days >= period_days {
        return Err(format!(
            "washout_days ({washout_days}) must be shorter than period_days ({period_days}), \
             or no day of a period is settled"
        ));
    }
    Ok(())
}

/// The schedule is consecutive pairs of `period_days`, one on and one off each,
/// with a washout shorter than a period. Each message names only its own guard.
pub(crate) fn validate_schedule(s: &SwitchbackSchedule) -> Result<(), String> {
    validate_rates(s.alpha, s.coverage_floor)?;
    validate_period_washout(s.period_days, s.washout_days)?;
    let Some(first) = s.periods.first().map(|p| p.from_day) else {
        return Err("a switchback needs at least one pair of periods; none were given".into());
    };
    if s.periods.len() % 2 == 1 {
        return Err(format!(
            "a switchback's periods come in consecutive pairs; got {} periods",
            s.periods.len()
        ));
    }
    for (k, p) in s.periods.iter().enumerate() {
        let expected = first + (k * s.period_days) as i64;
        if p.from_day != expected {
            return Err(format!(
                "period {} starts on day {}; consecutive {}-day periods put it on day {expected}",
                k + 1,
                p.from_day,
                s.period_days
            ));
        }
    }
    for (k, pair) in s.periods.chunks(2).enumerate() {
        if pair[0].on == pair[1].on {
            return Err(format!(
                "pair {} has both periods {}; each pair holds one on and one off period",
                k + 1,
                if pair[0].on { "on" } else { "off" }
            ));
        }
    }
    Ok(())
}

/// The estimand this design reports. It extends the interface's one-line
/// statement ("short-run effect of on against off under alternation") with the
/// unit it is measured in.
pub const SWITCHBACK_ESTIMAND: &str =
    "short-run effect of on against off under alternation, per unit-day, fleet mean";

/// The retained pairs: their on-minus-off differences, the retained days of
/// their on periods, and one message per pair dropped.
pub(crate) struct Pairs {
    pub(crate) diffs: Vec<f64>,
    pub(crate) on_days: usize,
    pub(crate) dropped: Vec<String>,
}

/// The fleet mean over a period's retained post-washout days, and how many
/// days that was - or why the period cannot be read.
fn period_mean(m: &PanelMatrix, from: i64, s: &SwitchbackSchedule) -> Result<(f64, usize), String> {
    let (a, b) = (from + s.washout_days as i64, from + s.period_days as i64);
    let (lo, hi) = m.ordinal_range(a, b);
    let cov = m.coverage(a, b);
    if cov < s.coverage_floor || hi == lo || m.n_units() == 0 {
        return Err(format!(
            "the period starting on day {from} holds {:.0}% of its {} post-washout days, under \
             the {:.0}% coverage floor",
            cov * 100.0,
            b - a,
            s.coverage_floor * 100.0
        ));
    }
    let sum: f64 = (0..m.n_units())
        .map(|u| (lo..hi).map(|i| m.get(u, i)).sum::<f64>())
        .sum();
    Ok((sum / (m.n_units() * (hi - lo)) as f64, hi - lo))
}

/// One `on - off` difference per pair whose two periods both clear the floor.
pub(crate) fn pair_diffs(m: &PanelMatrix, s: &SwitchbackSchedule) -> Pairs {
    let mut out = Pairs {
        diffs: Vec::new(),
        on_days: 0,
        dropped: Vec::new(),
    };
    for (k, pair) in s.periods.chunks(2).enumerate() {
        let (on, off) = if pair[0].on {
            (&pair[0], &pair[1])
        } else {
            (&pair[1], &pair[0])
        };
        match (
            period_mean(m, on.from_day, s),
            period_mean(m, off.from_day, s),
        ) {
            (Ok((v_on, days)), Ok((v_off, _))) => {
                out.diffs.push(v_on - v_off);
                out.on_days += days;
            }
            (Err(why), _) | (_, Err(why)) => out.dropped.push(format!("pair {}: {why}", k + 1)),
        }
    }
    out
}

/// `|sum of the signed terms|` for one sign vector.
fn flipped(x: &[f64], flip: &dyn Fn(usize) -> bool) -> f64 {
    x.iter()
        .enumerate()
        .map(|(k, v)| if flip(k) { -v } else { *v })
        .sum::<f64>()
        .abs()
}

/// Is a relabelled statistic at least as extreme as the observed one? A NaN or
/// infinite value compares false against everything, so it is counted extreme
/// explicitly: that can only raise p, never collapse it to zero.
fn extreme(v: f64, obs: f64) -> bool {
    !v.is_finite() || !obs.is_finite() || v >= obs
}

/// The sign-flip randomisation test of "every pair difference is `tau` plus a
/// symmetric coin": exact over all `2^P` sign vectors below the cap, otherwise
/// `PERMUTATIONS` seeded ones with the Monte Carlo p. Decided on the p-value.
pub(crate) fn sign_flip_test(diffs: &[f64], tau: f64, alpha: f64, seed: u64) -> PermOutcome {
    let x: Vec<f64> = diffs.iter().map(|d| d - tau).collect();
    let obs = flipped(&x, &|_| false);
    let (p_value, draws) = if 2f64.powi(x.len() as i32) <= ENUMERATE_BELOW {
        let n = 1usize << x.len();
        let extreme = (0..n)
            .filter(|mask| extreme(flipped(&x, &|k| mask >> k & 1 == 1), obs))
            .count();
        (extreme as f64 / n as f64, n)
    } else {
        let mut rng = SplitMix64::new(seed);
        let extreme = (0..PERMUTATIONS)
            .filter(|_| {
                let signs: Vec<bool> = (0..x.len()).map(|_| rng.next_u64() >> 63 == 1).collect();
                extreme(flipped(&x, &|k| signs[k]), obs)
            })
            .count();
        (
            (extreme + 1) as f64 / (PERMUTATIONS + 1) as f64,
            PERMUTATIONS,
        )
    };
    PermOutcome {
        rejected: p_value <= alpha,
        p_value,
        draws,
    }
}

/// The guard, then the test: refused when no pair survived, or when `2 / 2^P`
/// exceeds `alpha` (the per-comparison rate). Shared with the placebo.
pub(crate) fn test_pairs(
    diffs: &[f64],
    tau: f64,
    alpha: f64,
    seed: u64,
) -> Result<PermOutcome, String> {
    if diffs.is_empty() {
        return Err("every pair was dropped, so there is nothing to estimate".into());
    }
    if diffs.iter().any(|d| !d.is_finite()) {
        return Err(
            "a pair difference is non-finite (a NaN or infinite value sits in a \
                    retained period), so the effect cannot be estimated"
                .into(),
        );
    }
    let n = diffs.len();
    let min_p = switchback_min_p(n);
    if min_p > alpha {
        return Err(unreachable_pairs(n, alpha, min_p));
    }
    Ok(sign_flip_test(diffs, tau, alpha, seed))
}

/// The smallest p the sign-flip test over `pairs` pairs can report: `2/2^P`
/// when it enumerates (flipping every sign mirrors the observed statistic, so
/// the floor is 2, not 1), the sampled floor above the enumeration cap.
pub(crate) fn switchback_min_p(pairs: usize) -> f64 {
    let n = 2f64.powi(pairs as i32);
    min_attainable_p(2.0 / n, n)
}

/// Shared with the placebo's design check.
pub(crate) fn unreachable_pairs(n: usize, alpha: f64, min_p: f64) -> String {
    format!(
        "a sign-flip test over {n} pair(s) cannot reach alpha = {}: its smallest \
         attainable p-value is {} = {}",
        fmt_p(alpha),
        if 2f64.powi(n as i32) <= ENUMERATE_BELOW {
            format!("2/2^{n}")
        } else {
            format!("1/{} (sampled)", PERMUTATIONS + 1)
        },
        fmt_p(min_p)
    )
}

fn mean(x: &[f64]) -> f64 {
    x.iter().sum::<f64>() / x.len() as f64
}

/// Per-period fleet means, pair differences, the sign-flip decision at zero
/// and its constant-effect inversion.
pub fn estimate_switchback(m: &PanelMatrix, s: &SwitchbackSchedule, seed: u64) -> EffectResult {
    if let Err(reason) = validate_schedule(s) {
        return EffectResult::refused(reason);
    }
    let pairs = pair_diffs(m, s);
    let alpha = per_comparison_alpha(s.alpha, s.family);
    let at_zero = match test_pairs(&pairs.diffs, 0.0, alpha, seed) {
        Ok(o) => o,
        Err(reason) => {
            let mut r = EffectResult::refused(reason);
            r.dropped_waves = pairs.dropped;
            return r;
        }
    };
    let estimate = mean(&pairs.diffs);
    let test = |tau: f64| Some(sign_flip_test(&pairs.diffs, tau, alpha, seed));
    EffectResult {
        estimate,
        // A randomisation test has neither.
        se: f64::NAN,
        t_stat: f64::NAN,
        df: f64::NAN,
        ci_low: endpoint(&test, estimate, -1.0),
        ci_high: endpoint(&test, estimate, 1.0),
        p_value: at_zero.p_value,
        n_treated: pairs.diffs.len(),
        n_control: pairs.diffs.len(),
        significant: at_zero.rejected,
        estimand: SWITCHBACK_ESTIMAND,
        design: "switchback",
        dropped_waves: pairs.dropped,
        pre_trend: None,
        size_bias: None,
        retained_on_days: Some(pairs.on_days),
        refusal: None,
    }
}

/// Exactly the decision `estimate_switchback` reports, without its interval.
pub(crate) fn decide_switchback(m: &PanelMatrix, s: &SwitchbackSchedule, seed: u64) -> Decision {
    if let Err(reason) = validate_schedule(s) {
        return Decision::Refused(reason);
    }
    let pairs = pair_diffs(m, s);
    match test_pairs(
        &pairs.diffs,
        0.0,
        per_comparison_alpha(s.alpha, s.family),
        seed,
    ) {
        Ok(o) => Decision::Tested {
            significant: o.rejected,
            p_value: o.p_value,
            estimate: mean(&pairs.diffs),
        },
        Err(reason) => Decision::Refused(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::schedule;

    use crate::engine::experiment::estimate::Decision;
    use crate::engine::experiment::testkit::{inject_switchback, noisy_panel, triples_of};
    use crate::engine::experiment::PanelMatrix;

    /// 8 pairs of 7-day periods from day 200, washout 2, on a 24 x 400 panel.
    fn planted(effect: f64, pairs: usize, seed: u64) -> (PanelMatrix, SwitchbackSchedule) {
        let s = schedule(propose_switchback(200, 7, pairs, seed), 7, 2);
        (
            inject_switchback(&noisy_panel(24, 400, seed), &s, effect, 0),
            s,
        )
    }

    #[test]
    fn experiment_switchback_recovers_a_planted_effect() {
        let (m, s) = planted(20.0, 8, 4);
        let r = estimate_switchback(&m, &s, 1);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!((r.design, r.estimand), ("switchback", SWITCHBACK_ESTIMAND));
        assert!((r.estimate - 20.0).abs() < 5.0, "estimate {}", r.estimate);
        assert!(
            r.ci_low < 20.0 && 20.0 < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
        assert!(r.significant && r.p_value <= 0.05);
        assert_eq!(
            (r.n_treated, r.n_control),
            (8, 8),
            "both counts are retained pairs"
        );
        assert_eq!(
            r.retained_on_days,
            Some(8 * 5),
            "8 on periods x 5 post-washout days"
        );
        assert!(r.pre_trend.is_none() && r.size_bias.is_none());
        assert!(
            r.se.is_nan() && r.df.is_nan(),
            "a randomisation test has no se or df"
        );
    }

    /// The estimand string extends the phrase the interface contract fixes.
    #[test]
    fn experiment_switchback_estimand_names_the_contract_phrase() {
        assert!(
            SWITCHBACK_ESTIMAND.contains("short-run effect of on against off under alternation")
        );
    }

    /// The p-value counts sign flips exactly, mirror ties included. Six pairs
    /// with one small negative: the observed |sum| 14.5 is matched by itself,
    /// by flipping the -0.5 (15.5), and by both mirrors - p = 4/64, not
    /// significant at 0.05. All positive is the extreme pair only: 2/64.
    #[test]
    fn experiment_switchback_p_value_counts_sign_flips_exactly() {
        let o = sign_flip_test(&[1.0, 2.0, 3.0, 4.0, 5.0, -0.5], 0.0, 0.05, 0);
        assert!(
            (o.p_value - 4.0 / 64.0).abs() < 1e-12 && !o.rejected,
            "p {}",
            o.p_value
        );
        let o = sign_flip_test(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 0.0, 0.05, 0);
        assert!(
            (o.p_value - 2.0 / 64.0).abs() < 1e-12 && o.rejected,
            "p {}",
            o.p_value
        );
        assert_eq!(o.draws, 64, "enumerated exactly below the cap");
    }

    /// The spec's BMG arithmetic: four pairs cannot reach 0.05 (2/16 = 0.125)
    /// and can reach 0.2.
    #[test]
    fn experiment_switchback_refuses_too_few_pairs_for_alpha() {
        let (m, mut s) = planted(20.0, 4, 4);
        let r = estimate_switchback(&m, &s, 1);
        let reason = r.refusal.expect("4 pairs at alpha 0.05 must refuse");
        assert!(
            reason.contains("cannot reach") && reason.contains("2/2^4"),
            "{reason}"
        );
        assert!(r.estimate.is_nan(), "a refusal carries NaN");
        s.alpha = 0.2;
        assert!(
            estimate_switchback(&m, &s, 1).refusal.is_none(),
            "4 pairs reach alpha 0.2"
        );
    }

    #[test]
    fn experiment_switchback_drops_a_pair_under_the_coverage_floor() {
        let (m, s) = planted(20.0, 8, 4);
        let hole = s.periods[4].from_day; // the first period of pair 3
        let gappy = PanelMatrix::from_triples(
            triples_of(&m)
                .into_iter()
                .filter(|(_, d, _)| !(hole..hole + 7).contains(d)),
        );
        let r = estimate_switchback(&gappy, &s, 1);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.dropped_waves.len(), 1);
        assert!(
            r.dropped_waves[0].starts_with("pair 3:") && r.dropped_waves[0].contains("90%"),
            "{:?}",
            r.dropped_waves
        );
        assert_eq!((r.n_treated, r.retained_on_days), (7, Some(7 * 5)));
    }

    /// The inversion is a real test inversion: on eight pairs its half-width
    /// lands near a t-interval on the same pair differences, and it does not
    /// widen with the effect.
    #[test]
    fn experiment_switchback_interval_is_a_test_inversion() {
        let width = |effect: f64| {
            let (m, s) = planted(effect, 8, 6);
            let r = estimate_switchback(&m, &s, 1);
            (r.ci_high - r.ci_low) / 2.0
        };
        let (w0, w1) = (width(0.0), width(200.0));
        assert!(
            (w1 / w0 - 1.0).abs() < 0.05,
            "width tracked the effect: {w0} vs {w1}"
        );
        let (m, s) = planted(0.0, 8, 6);
        let d = pair_diffs(&m, &s).diffs;
        let mean = d.iter().sum::<f64>() / 8.0;
        let sd = (d.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / 7.0).sqrt();
        let t_half = crate::engine::experiment::t_quantile(7.0, 0.05, 1) * sd / 8f64.sqrt();
        assert!(
            (0.7..=1.4).contains(&(w0 / t_half)),
            "sign-flip {w0} vs t {t_half}"
        );
    }

    /// A non-finite cell in a retained period must never read as a finding: the
    /// NaN pair difference compares false against every relabelling, which
    /// would otherwise collapse the p-value to zero.
    #[test]
    fn experiment_switchback_refuses_a_non_finite_cell() {
        let (m, s) = planted(20.0, 8, 4);
        let on = s.periods.iter().find(|p| p.on).expect("an on period");
        let day = on.from_day + 3; // retained, past the washout
        let poisoned = |v: f64| {
            PanelMatrix::from_triples(
                triples_of(&m)
                    .into_iter()
                    .map(|(u, d, x)| (u.clone(), d, if u == "u000" && d == day { v } else { x })),
            )
        };
        for bad in [f64::NAN, f64::INFINITY] {
            let m = poisoned(bad);
            let r = estimate_switchback(&m, &s, 1);
            let reason = r.refusal.as_deref().expect("a non-finite cell must refuse");
            assert!(reason.contains("non-finite"), "{reason}");
            assert!(
                !r.significant && r.p_value != 0.0,
                "{:?}",
                (r.significant, r.p_value)
            );
            assert!(r.estimate.is_nan() && r.p_value.is_nan());
            assert!(matches!(decide_switchback(&m, &s, 1), Decision::Refused(_)));
        }
    }

    /// inf - inf is NaN: a pair whose two periods both carry +inf.
    #[test]
    fn experiment_switchback_refuses_an_infinite_pair_difference() {
        let reason = test_pairs(&[1.0, 2.0, f64::NAN, 3.0, 4.0, 5.0], 0.0, 0.05, 0)
            .err()
            .expect("a NaN difference must refuse");
        assert!(reason.contains("non-finite"), "{reason}");
        let (m, s) = planted(20.0, 8, 4);
        let (a, b) = (s.periods[0].from_day, s.periods[1].from_day);
        let both = PanelMatrix::from_triples(triples_of(&m).into_iter().map(|(u, d, x)| {
            let hit = u == "u000" && (d == a + 3 || d == b + 3);
            (u, d, if hit { f64::INFINITY } else { x })
        }));
        assert!(pair_diffs(&both, &s).diffs[0].is_nan(), "inf - inf is NaN");
        let r = estimate_switchback(&both, &s, 1);
        assert!(r.refusal.is_some() && !r.significant, "{:?}", r.refusal);
    }

    /// A non-finite statistic counts as extreme, never as a zero p-value.
    #[test]
    fn experiment_switchback_non_finite_statistic_is_never_significant() {
        let o = sign_flip_test(&[1.0, 2.0, f64::NAN, 3.0, 4.0, 5.0], 0.0, 0.05, 0);
        assert!(o.p_value == 1.0 && !o.rejected, "p {}", o.p_value);
    }

    /// `decide_switchback` is the decision `estimate_switchback` reports.
    #[test]
    fn experiment_switchback_decision_is_the_one_reported() {
        for effect in [0.0, 3.0, 20.0] {
            let (m, s) = planted(effect, 8, 7);
            let r = estimate_switchback(&m, &s, 2);
            assert_eq!(
                decide_switchback(&m, &s, 2),
                Decision::Tested {
                    significant: r.significant,
                    p_value: r.p_value,
                    estimate: r.estimate,
                }
            );
        }
    }

    #[test]
    fn experiment_switchback_schedule_is_consecutive_pairs_one_on_one_off() {
        let p = propose_switchback(739_677, 7, 4, 8_841_207);
        assert_eq!(p.len(), 8);
        assert!(
            p.iter()
                .enumerate()
                .all(|(k, x)| x.from_day == 739_677 + 7 * k as i64),
            "consecutive 7-day periods: {p:?}"
        );
        assert!(
            p.chunks(2).all(|pair| pair[0].on != pair[1].on),
            "one on, one off per pair"
        );
        assert_eq!(
            propose_switchback(739_677, 7, 4, 8_841_207),
            p,
            "the seed reproduces it"
        );
        assert_eq!(validate_schedule(&schedule(p, 7, 2)), Ok(()));
    }

    /// One coin per pair: over 64 pairs both orientations occur often, and two
    /// seeds give two schedules.
    #[test]
    fn experiment_switchback_schedule_flips_a_coin_per_pair() {
        let p = propose_switchback(1, 7, 64, 3);
        let on_first = p.chunks(2).filter(|pair| pair[0].on).count();
        assert!(
            (20..=44).contains(&on_first),
            "{on_first} of 64 pairs start on"
        );
        assert_ne!(
            propose_switchback(1, 7, 8, 1),
            propose_switchback(1, 7, 8, 2)
        );
        assert!(propose_switchback(1, 7, 0, 1).is_empty());
        assert!(
            propose_switchback(1, 0, 4, 1).is_empty(),
            "no period of zero days"
        );
    }

    #[test]
    fn experiment_switchback_schedule_refuses_what_is_not_a_paired_schedule() {
        let good = propose_switchback(1, 7, 3, 4);
        let refuse = |s: SwitchbackSchedule| validate_schedule(&s).expect_err("must refuse");
        assert!(refuse(schedule(good.clone(), 0, 0)).contains("at least 1 day"));
        assert!(refuse(schedule(good.clone(), 7, 7)).contains("shorter than period_days"));
        assert!(refuse(schedule(Vec::new(), 7, 2)).contains("at least one pair"));
        assert!(refuse(schedule(good[..5].to_vec(), 7, 2)).contains("got 5 periods"));
        let mut gap = good.clone();
        gap[3].from_day += 1;
        let e = refuse(schedule(gap, 7, 2));
        assert!(
            e.contains("period 4") && e.contains("put it on day 22"),
            "{e}"
        );
        let mut both = good;
        both[3].on = both[2].on;
        assert!(refuse(schedule(both, 7, 2)).contains("pair 2 has both periods"));
    }

    #[test]
    fn experiment_switchback_refuses_an_unusable_alpha_or_coverage_floor() {
        let (m, good) = planted(20.0, 8, 4);
        for bad in [f64::NAN, f64::INFINITY, 5.0, 1.5, 1.0, 0.0, -0.05] {
            let mut s = good.clone();
            s.alpha = bad;
            let reason = estimate_switchback(&m, &s, 1)
                .refusal
                .unwrap_or_else(|| panic!("alpha {bad} must refuse"));
            assert!(reason.contains("alpha"), "{bad}: {reason}");
            assert!(
                matches!(decide_switchback(&m, &s, 1), Decision::Refused(r) if r.contains("alpha"))
            );
        }
        for bad in [f64::NAN, f64::INFINITY, 1.5, -0.1] {
            let mut s = good.clone();
            s.coverage_floor = bad;
            let reason = estimate_switchback(&m, &s, 1)
                .refusal
                .unwrap_or_else(|| panic!("coverage_floor {bad} must refuse"));
            assert!(reason.contains("coverage_floor"), "{bad}: {reason}");
        }
    }

    #[test]
    fn experiment_switchback_guard_counts_the_sampled_floor_not_the_enumerated_one() {
        // 15 pairs is 32,768 sign vectors, over the enumeration cap, so the test
        // samples and its smallest p is 1/2001, not 2/2^15. At per-comparison
        // alpha 4.8e-4 no draw can ever reject.
        let s = schedule(propose_switchback(1, 7, 15, 3), 7, 1);
        let m = noisy_panel(12, 220, 9);
        let mut tight = s.clone();
        (tight.alpha, tight.family) = (0.01, 21);
        let r = estimate_switchback(&m, &tight, 1);
        let reason = r
            .refusal
            .expect("a design that can never reject must refuse");
        assert!(reason.contains("cannot reach"), "reason was: {reason}");
        assert!(matches!(
            decide_switchback(&m, &tight, 1),
            Decision::Refused(_)
        ));
        // At a reachable alpha the same sampled design is estimated.
        assert!(estimate_switchback(&m, &s, 1).refusal.is_none());
    }
}
