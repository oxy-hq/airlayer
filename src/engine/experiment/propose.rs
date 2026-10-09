//! Randomisation: a seeded split of an entity's units into waves. The seed
//! reproduces the split; it does not prove the split was the first drawn —
//! that is the host's proposal log, not this function.

use crate::engine::experiment::strata::allocate;
use crate::engine::experiment::SplitMix64;
use std::collections::BTreeSet;

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct ProposedWave {
    pub switch_day: i64,
    pub units: Vec<String>,
}

/// Seeded, deterministic, and independent of the ORDER of `units` (it sorts
/// before shuffling). Units not placed in any wave are controls. With `strata`,
/// each wave's size is allocated across strata in proportion to stratum size,
/// remainders placed by the seed.
pub fn propose_waves(
    units: &[String],
    wave_sizes: &[usize],
    first_switch_day: i64,
    spacing_days: i64,
    strata: Option<&[Vec<String>]>,
    seed: u64,
) -> Result<Vec<ProposedWave>, String> {
    check_request(units, wave_sizes, spacing_days)?;
    let members = match strata {
        Some(strata) => {
            check_strata(units, strata)?;
            allocate(strata, wave_sizes, seed)
        }
        None => shuffled_waves(units, wave_sizes, seed),
    };
    Ok(members
        .into_iter()
        .enumerate()
        .map(|(i, units)| ProposedWave {
            switch_day: first_switch_day + i as i64 * spacing_days,
            units,
        })
        .collect())
}

/// The unblocked split: one seeded shuffle of the sorted units, cut into waves.
fn shuffled_waves(units: &[String], wave_sizes: &[usize], seed: u64) -> Vec<Vec<String>> {
    let mut sorted = units.to_vec();
    sorted.sort();
    let placed: usize = wave_sizes.iter().sum();
    let mut order: Vec<usize> = (0..sorted.len()).collect();
    SplitMix64::new(seed).partial_shuffle(&mut order, placed);
    let mut at = 0;
    wave_sizes
        .iter()
        .map(|size| {
            let mut members: Vec<String> = order[at..at + size]
                .iter()
                .map(|u| sorted[*u].clone())
                .collect();
            members.sort();
            at += size;
            members
        })
        .collect()
}

/// The strata partition `units` exactly. Each message names only its own guard.
fn check_strata(units: &[String], strata: &[Vec<String>]) -> Result<(), String> {
    let known: BTreeSet<&str> = units.iter().map(String::as_str).collect();
    for (k, s) in strata.iter().enumerate().map(|(k, s)| (k + 1, s)) {
        if s.is_empty() {
            return Err(format!("stratum {k} is empty"));
        }
        if let Some(u) = s.iter().find(|u| !known.contains(u.as_str())) {
            return Err(format!(
                "stratum {k} names unit '{u}', which is not among the units"
            ));
        }
        let mut seen = BTreeSet::new();
        if let Some(u) = s.iter().find(|u| !seen.insert(u.as_str())) {
            return Err(format!("stratum {k} lists unit '{u}' twice"));
        }
    }
    let mut sorted: Vec<&String> = units.iter().collect();
    sorted.sort();
    for u in sorted {
        match strata.iter().filter(|s| s.contains(u)).count() {
            1 => {}
            0 => return Err(format!("unit '{u}' sits in no stratum")),
            n => return Err(format!("unit '{u}' sits in {n} strata")),
        }
    }
    Ok(())
}

/// The guards, in order; each message names only what it tested.
fn check_request(units: &[String], wave_sizes: &[usize], spacing_days: i64) -> Result<(), String> {
    if wave_sizes.is_empty() {
        return Err("no wave sizes were requested".into());
    }
    if let Some(s) = wave_sizes.iter().find(|s| **s < 2) {
        return Err(format!(
            "every wave needs at least 2 units; a wave of {s} was requested"
        ));
    }
    if spacing_days < 1 {
        return Err(format!(
            "waves must be at least 1 day apart; spacing_days was {spacing_days}"
        ));
    }
    let mut seen = BTreeSet::new();
    if let Some(dup) = units.iter().find(|u| !seen.insert(u.as_str())) {
        return Err(format!("unit '{dup}' is listed twice"));
    }
    let placed: usize = wave_sizes.iter().sum();
    if placed >= units.len() {
        return Err(format!(
            "the waves place {placed} of {} units, leaving no control",
            units.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn stores(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("store_{i:02}")).collect()
    }

    #[test]
    fn experiment_propose_waves_is_deterministic_for_a_seed() {
        let u = stores(24);
        let first = propose_waves(&u, &[3, 3], 739_677, 14, None, 8_841_207).expect("valid");
        assert_eq!(
            propose_waves(&u, &[3, 3], 739_677, 14, None, 8_841_207),
            Ok(first),
            "the seed in the file must reproduce the waves exactly"
        );
    }

    /// The warehouse's key order is not a design input. Same seed, same unit
    /// SET in any order → the identical proposal.
    #[test]
    fn experiment_propose_waves_ignores_the_order_of_units() {
        let u = stores(24);
        let base = propose_waves(&u, &[4, 4], 739_677, 14, None, 42).expect("valid");
        let mut reversed = u.clone();
        reversed.reverse();
        let mut idx: Vec<usize> = (0..24).collect();
        SplitMix64::new(3).partial_shuffle(&mut idx, 24);
        let shuffled: Vec<String> = idx.iter().map(|i| u[*i].clone()).collect();
        assert_ne!(shuffled, u, "the fixture must actually reorder");
        assert_eq!(
            propose_waves(&reversed, &[4, 4], 739_677, 14, None, 42),
            Ok(base.clone())
        );
        assert_eq!(
            propose_waves(&shuffled, &[4, 4], 739_677, 14, None, 42),
            Ok(base)
        );
    }

    #[test]
    fn experiment_propose_waves_respects_sizes_and_spacing() {
        let u = stores(24);
        let w = propose_waves(&u, &[3, 2, 4], 739_677, 14, None, 7).expect("valid");
        assert_eq!(
            w.iter().map(|x| x.units.len()).collect::<Vec<_>>(),
            vec![3, 2, 4]
        );
        assert_eq!(
            w.iter().map(|x| x.switch_day).collect::<Vec<_>>(),
            vec![739_677, 739_691, 739_705],
            "switch days are first + i * spacing"
        );
        let placed: Vec<&String> = w.iter().flat_map(|x| &x.units).collect();
        let distinct: BTreeSet<&String> = placed.iter().copied().collect();
        assert_eq!(distinct.len(), placed.len(), "no unit sits in two waves");
        assert!(
            placed.iter().all(|p| u.contains(p)),
            "every placed unit came from the input"
        );
        assert!(
            w.iter().all(|x| x.units.windows(2).all(|p| p[0] < p[1])),
            "each wave lists its units sorted"
        );
        assert_eq!(
            u.len() - placed.len(),
            15,
            "every unplaced unit is a control"
        );
    }

    #[test]
    fn experiment_propose_waves_draws_differ_across_seeds() {
        let u = stores(24);
        let a = propose_waves(&u, &[6], 1, 1, None, 1).expect("valid");
        let b = propose_waves(&u, &[6], 1, 1, None, 2).expect("valid");
        assert_ne!(
            a, b,
            "two seeds drawing the same 6 of 24 is a 1-in-134,596 \
                          coincidence; a constant draw is the likelier cause"
        );
    }

    #[test]
    fn experiment_propose_waves_refuses_what_it_cannot_split() {
        let u = stores(6);
        let refuse = |units: &[String], sizes: &[usize], spacing: i64| {
            propose_waves(units, sizes, 100, spacing, None, 1).expect_err("must refuse")
        };
        let e = refuse(&u, &[], 7);
        assert!(e.contains("no wave sizes"), "{e}");
        let e = refuse(&u, &[3, 1], 7);
        assert!(
            e.contains("at least 2 units") && !e.contains("control"),
            "{e}"
        );
        let e = refuse(&u, &[2, 2], 0);
        assert!(e.contains("1 day apart") && !e.contains("control"), "{e}");
        let e = refuse(&u, &[3, 3], 7);
        assert!(e.contains("no control") && e.contains("6 of 6"), "{e}");
        let mut dup = stores(6);
        dup.push("store_01".into());
        let e = refuse(&dup, &[2], 7);
        assert!(e.contains("'store_01'") && e.contains("twice"), "{e}");
        assert!(
            propose_waves(&u, &[2, 2], 100, 7, None, 1).is_ok(),
            "4 of 6 leaves 2 controls"
        );
    }

    fn quartiles(u: &[String]) -> Vec<Vec<String>> {
        u.chunks(6).map(<[String]>::to_vec).collect()
    }

    #[test]
    fn experiment_propose_waves_allocates_each_wave_within_strata() {
        let u = stores(24);
        let strata = quartiles(&u);
        let w = propose_waves(&u, &[4, 4], 739_677, 14, Some(&strata), 8_841_207).expect("valid");
        for wave in &w {
            for s in &strata {
                assert_eq!(
                    wave.units.iter().filter(|x| s.contains(x)).count(),
                    1,
                    "4 of 24 over four strata of 6 is one from each: {wave:?}"
                );
            }
        }
        let placed: BTreeSet<&String> = w.iter().flat_map(|x| &x.units).collect();
        assert_eq!(placed.len(), 8, "no unit in two waves");
        assert_eq!(
            propose_waves(&u, &[4, 4], 739_677, 14, Some(&strata), 8_841_207),
            Ok(w),
            "the seed and the strata reproduce the waves exactly"
        );
    }

    #[test]
    fn experiment_propose_waves_with_strata_ignores_unit_and_stratum_order() {
        let u = stores(24);
        let strata = quartiles(&u);
        let base = propose_waves(&u, &[3, 3], 739_677, 14, Some(&strata), 5).expect("valid");
        let mut ru = u.clone();
        ru.reverse();
        let mut rs: Vec<Vec<String>> = strata.iter().rev().cloned().collect();
        for s in &mut rs {
            s.reverse();
        }
        assert_eq!(
            propose_waves(&ru, &[3, 3], 739_677, 14, Some(&rs), 5),
            Ok(base)
        );
    }

    #[test]
    fn experiment_propose_waves_refuses_strata_that_do_not_partition_the_units() {
        let u = stores(8);
        let refuse = |strata: Vec<Vec<String>>| {
            propose_waves(&u, &[2], 100, 7, Some(&strata), 1).expect_err("must refuse")
        };
        let s = |v: &[usize]| {
            v.iter()
                .map(|i| format!("store_{i:02}"))
                .collect::<Vec<_>>()
        };
        let e = refuse(vec![s(&[0, 1, 2, 3]), s(&[4, 5, 6])]);
        assert!(e.contains("'store_07'") && e.contains("no stratum"), "{e}");
        let e = refuse(vec![s(&[0, 1, 2, 3, 4]), s(&[4, 5, 6, 7])]);
        assert!(e.contains("'store_04'") && e.contains("2 strata"), "{e}");
        let e = refuse(vec![s(&[0, 1, 2, 3]), s(&[4, 5, 6, 7]), vec![]]);
        assert!(e.contains("stratum 3 is empty"), "{e}");
        let e = refuse(vec![s(&[0, 1, 2, 3]), s(&[4, 5, 6, 7, 9])]);
        assert!(e.contains("stratum 2") && e.contains("'store_09'"), "{e}");
        let e = refuse(vec![s(&[0, 1, 2, 3, 3]), s(&[4, 5, 6, 7])]);
        assert!(e.contains("'store_03' twice"), "{e}");
    }

    fn wave(switch_day: i64, units: &[&str]) -> ProposedWave {
        ProposedWave {
            switch_day,
            units: units.iter().map(|u| u.to_string()).collect(),
        }
    }

    /// Pinned so a stored seed reproduces its waves: the literal was produced by
    /// running this implementation once. A change that moves it breaks every
    /// seed already written into a committed experiment file.
    #[test]
    fn experiment_propose_waves_golden_unblocked() {
        let got = propose_waves(&stores(12), &[3, 2], 739_677, 14, None, 8_841_207).expect("valid");
        assert_eq!(
            got,
            vec![
                wave(739_677, &["store_04", "store_08", "store_09"]),
                wave(739_691, &["store_02", "store_05"]),
            ]
        );
    }

    /// Same pin for the blocked split: three strata of four, one unit per
    /// stratum in each wave of three.
    #[test]
    fn experiment_propose_waves_golden_stratified() {
        let u = stores(12);
        let strata: Vec<Vec<String>> = u.chunks(4).map(<[String]>::to_vec).collect();
        let got = propose_waves(&u, &[3, 3], 739_677, 14, Some(&strata), 8_841_207).expect("valid");
        assert_eq!(
            got,
            vec![
                wave(739_677, &["store_00", "store_05", "store_09"]),
                wave(739_691, &["store_01", "store_04", "store_08"]),
            ]
        );
    }
}
