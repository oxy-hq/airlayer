//! The paired switchback: the whole fleet alternates on and off over periods in
//! a seeded random order. The coin is the randomisation, so the coin is the test.

use crate::engine::experiment::SplitMix64;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::testkit::schedule;

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
}
