//! Blocked assignment: validating the strata an assignment was randomised
//! within, and mapping panel rows to them. `propose_strata` and the
//! proportional allocation that pricing and the placebo share live here too.

use crate::engine::experiment::estimate::Assignment;
use crate::engine::experiment::{PanelMatrix, SplitMix64};

/// Every unit `switch_day` names sits in exactly one stratum. A stratum member
/// the assignment does not name is out of scope and ignored, like an unnamed
/// panel unit. Checked in sorted name order, so the first failure is stable.
pub(crate) fn validate_strata(a: &Assignment) -> Result<(), String> {
    let Some(strata) = &a.strata else {
        return Ok(());
    };
    let mut names: Vec<&String> = a.switch_day.keys().collect();
    names.sort();
    for name in names {
        match strata.iter().filter(|s| s.contains(name)).count() {
            1 => {}
            0 => return Err(format!("assignment unit '{name}' sits in no stratum")),
            k => return Err(format!("assignment unit '{name}' sits in {k} strata")),
        }
    }
    Ok(())
}

/// Each row's stratum index, for a SCOPED, validated panel; all zero when the
/// assignment is unblocked. The `unwrap_or(0)` is unreachable after validation
/// (every row of a scoped panel is named, and every named unit is in exactly
/// one stratum); it is a default, not a silent placement, because `setup`
/// validates first.
pub(crate) fn stratum_rows(m: &PanelMatrix, a: &Assignment) -> Vec<usize> {
    let Some(strata) = &a.strata else {
        return vec![0; m.n_units()];
    };
    m.units
        .iter()
        .map(|u| strata.iter().position(|s| s.contains(u)).unwrap_or(0))
        .collect()
}

/// Each named unit's mean over `[from, to)`, or the reason there is none.
fn means_over(
    m: &PanelMatrix,
    names: &[String],
    from: i64,
    to: i64,
) -> Result<Vec<(String, f64)>, String> {
    let (lo, hi) = m.ordinal_range(from, to);
    names
        .iter()
        .map(|n| {
            let u = m
                .unit_index(n)
                .ok_or_else(|| format!("unit '{n}' is not in the panel"))?;
            let v = m
                .window_mean(u, lo, hi)
                .ok_or_else(|| format!("no retained day in [{from}, {to}) to rank units by"))?;
            Ok((n.clone(), v))
        })
        .collect()
}

/// Rank largest-first (ties by name) and cut into `blocks` contiguous strata
/// whose sizes differ by at most one; each stratum sorted by name.
fn rank_into_blocks(
    mut means: Vec<(String, f64)>,
    blocks: usize,
) -> Result<Vec<Vec<String>>, String> {
    let n = means.len();
    if blocks < 2 {
        return Err(format!(
            "a stratified design needs at least 2 blocks; {blocks} were requested"
        ));
    }
    if blocks > n / 2 {
        return Err(format!(
            "{blocks} blocks over {n} units leave a block with fewer than 2 units; at most {} fit",
            n / 2
        ));
    }
    means.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let (base, extra) = (n / blocks, n % blocks);
    let mut at = 0;
    Ok((0..blocks)
        .map(|b| {
            let len = base + usize::from(b < extra);
            let mut s: Vec<String> = means[at..at + len].iter().map(|(u, _)| u.clone()).collect();
            at += len;
            s.sort();
            s
        })
        .collect())
}

/// Rank the panel's units by their mean over `[from, to)` and cut the ranking
/// into `blocks` contiguous strata (sizes differ by at most one). Deterministic;
/// ties broken by name.
pub fn propose_strata(
    m: &PanelMatrix,
    blocks: usize,
    from: i64,
    to: i64,
) -> Result<Vec<Vec<String>>, String> {
    rank_into_blocks(means_over(m, &m.units, from, to)?, blocks)
}

/// Each wave's members, allocated within strata in proportion to stratum size,
/// remainders placed by the seed; units no wave draws are controls. Requires
/// `sum(sizes) <= total units` (callers check). Order-independent: strata and
/// their members are put in canonical order before any draw.
pub(crate) fn allocate(strata: &[Vec<String>], sizes: &[usize], seed: u64) -> Vec<Vec<String>> {
    let mut strata: Vec<Vec<String>> = strata
        .iter()
        .map(|s| {
            let mut s = s.clone();
            s.sort();
            s
        })
        .collect();
    strata.sort();
    let mut rng = SplitMix64::new(seed);
    let counts = stratum_counts(&strata, sizes, &mut rng);
    let mut waves: Vec<Vec<String>> = vec![Vec::new(); sizes.len()];
    for (j, s) in strata.iter().enumerate() {
        let take: usize = counts.iter().map(|c| c[j]).sum();
        let mut order: Vec<usize> = (0..s.len()).collect();
        rng.partial_shuffle(&mut order, take);
        let mut at = 0;
        for (i, c) in counts.iter().enumerate() {
            waves[i].extend(order[at..at + c[j]].iter().map(|k| s[*k].clone()));
            at += c[j];
        }
    }
    for w in &mut waves {
        w.sort();
    }
    waves
}

/// `counts[wave][stratum]`: the floor of each proportional quota, then the
/// wave's remainder one unit at a time to a seeded stratum whose quota had a
/// fractional part and that still has room (any with room, if none).
fn stratum_counts(
    strata: &[Vec<String>],
    sizes: &[usize],
    rng: &mut SplitMix64,
) -> Vec<Vec<usize>> {
    let n: usize = strata.iter().map(Vec::len).sum();
    let mut room: Vec<usize> = strata.iter().map(Vec::len).collect();
    let mut counts = Vec::with_capacity(sizes.len());
    for &size in sizes {
        let quota = |j: usize| size * strata[j].len();
        let mut c: Vec<usize> = (0..strata.len())
            .map(|j| (quota(j) / n).min(room[j]))
            .collect();
        let mut short = size - c.iter().sum::<usize>();
        while short > 0 {
            let open = |j: &usize| room[*j] > c[*j];
            let mut pool: Vec<usize> = (0..c.len())
                .filter(|j| open(j) && !quota(*j).is_multiple_of(n) && c[*j] == quota(*j) / n)
                .collect();
            if pool.is_empty() {
                pool = (0..c.len()).filter(open).collect();
            }
            c[pool[rng.below(pool.len())]] += 1;
            short -= 1;
        }
        for (r, k) in room.iter_mut().zip(&c) {
            *r -= k;
        }
        counts.push(c);
    }
    counts
}

/// A placebo draw's (or a calibration replication's) mirror of pricing: rank
/// `names` by their mean over the draw's own `pre` window, cut `blocks` strata,
/// allocate `sizes` within them. Returns `(waves, strata)`.
pub(crate) fn blocked_layout(
    m: &PanelMatrix,
    names: &[String],
    sizes: &[usize],
    blocks: usize,
    pre: (i64, i64),
    seed: u64,
) -> Result<(Vec<Vec<String>>, Vec<Vec<String>>), String> {
    let strata = rank_into_blocks(means_over(m, names, pre.0, pre.1)?, blocks)?;
    Ok((allocate(&strata, sizes, seed), strata))
}

#[cfg(test)]
mod tests {
    use super::{allocate, blocked_layout, propose_strata};
    use crate::engine::experiment::estimate::{estimate_effect, estimate_simple};
    use crate::engine::experiment::permutation::{
        each_relabelling, permutation_groups, relabellings, shuffle_within,
    };
    use crate::engine::experiment::staggered::estimate_staggered;
    use crate::engine::experiment::testkit::{assign, fixture, matrix_from, staggered_fixture};
    use crate::engine::experiment::{PanelMatrix, SplitMix64};

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn strata_of(blocks: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        Some(
            blocks
                .iter()
                .map(|b| b.iter().map(|u| u.to_string()).collect())
                .collect(),
        )
    }

    /// Index Review Focus 7. Stratum A is rows 0-3, stratum B rows 4-7. Every
    /// enumerated AND every sampled relabelling keeps each row's data inside its
    /// own stratum, and the count is the product over strata — 12 × 12 = 144 —
    /// not the 8!/(3!2!3!) = 560 a pooled permutation counts.
    #[test]
    fn experiment_stratified_permutation_never_moves_a_unit_across_strata() {
        let switch_of = [
            Some(20),
            Some(20),
            Some(40),
            None,
            Some(20),
            Some(40),
            None,
            None,
        ];
        let stratum_of = [0, 0, 0, 0, 1, 1, 1, 1];
        let groups = permutation_groups(&switch_of, &stratum_of);
        assert_eq!(relabellings(&groups), 144.0, "4!/(2!1!1!) × 4!/(1!1!2!)");
        assert_eq!(
            relabellings(&permutation_groups(&switch_of, &[0; 8])),
            560.0
        );
        let mut seen = std::collections::HashSet::new();
        each_relabelling(&groups, &mut |perm| {
            for (slot, unit) in perm.iter().enumerate() {
                assert_eq!(
                    stratum_of[slot], stratum_of[*unit],
                    "enumeration moved unit {unit} into slot {slot} across strata"
                );
            }
            seen.insert(perm.to_vec());
        });
        assert_eq!(
            seen.len(),
            144,
            "every within-stratum relabelling, exactly once"
        );
        let (mut rng, mut perm) = (SplitMix64::new(9), (0..8).collect::<Vec<usize>>());
        for _ in 0..2000 {
            shuffle_within(&groups, &mut rng, &mut perm);
            assert!(
                perm.iter()
                    .enumerate()
                    .all(|(s, u)| stratum_of[s] == stratum_of[*u]),
                "the sampler moved a unit across strata: {perm:?}"
            );
        }
    }

    /// The guard reads the stratified count. Eleven strata, ten of them a single
    /// unit, leave 2 relabellings: min p = 1/2, refused — and the message says
    /// it counted within strata. Unblocked, the same design is 18,480 and fine.
    #[test]
    fn experiment_stratified_guard_refuses_a_design_blocked_too_finely() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        assert!(
            estimate_staggered(&m, &a, 3).refusal.is_none(),
            "precondition"
        );
        a.strata = strata_of(&[
            &["t0", "c6"],
            &["t1"],
            &["t2"],
            &["t3"],
            &["t4"],
            &["t5"],
            &["c7"],
            &["c8"],
            &["c9"],
            &["c10"],
            &["c11"],
        ]);
        let reason = estimate_staggered(&m, &a, 3).refusal.expect("must refuse");
        assert!(
            reason.contains("cannot reach") && reason.contains("within 11 strata"),
            "{reason}"
        );
    }

    /// Blocked into two strata of six that each hold a full wave structure, the
    /// planted effect is still recovered and its interval covers it.
    #[test]
    fn experiment_stratified_staggered_recovers_a_planted_effect() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        a.strata = strata_of(&[
            &["t0", "t1", "t3", "c6", "c7", "c8"],
            &["t2", "t4", "t5", "c9", "c10", "c11"],
        ]);
        let r = estimate_staggered(&m, &a, 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert!(
            r.ci_low < 25.0 && 25.0 < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    #[test]
    fn experiment_stratified_assignment_refuses_a_unit_in_no_stratum_or_two() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        let all: Vec<&str> = vec![
            "t0", "t1", "t2", "t3", "t4", "t5", "c6", "c7", "c8", "c9", "c10",
        ];
        a.strata = strata_of(&[&all]); // c11 is in none
        let r = estimate_staggered(&m, &a, 3).refusal.expect("must refuse");
        assert!(r.contains("'c11'") && r.contains("no stratum"), "{r}");
        a.strata = strata_of(&[&all, &["c11", "t0"]]); // t0 is in two
        let r = estimate_staggered(&m, &a, 3).refusal.expect("must refuse");
        assert!(r.contains("'t0'") && r.contains("2 strata"), "{r}");
        // A stratum member the assignment does not name is out of scope, not an error.
        a.strata = strata_of(&[&all, &["c11", "closed_store"]]);
        assert!(estimate_staggered(&m, &a, 3).refusal.is_none());
    }

    /// Common date keeps Welch: the strata are validated and otherwise unread,
    /// so the result is bit-identical to the unblocked one.
    #[test]
    fn experiment_stratified_common_date_keeps_welch() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let plain = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let mut blocked = plain.clone();
        blocked.strata = strata_of(&[
            &["t0", "t1", "t2", "c6", "c7", "c8"],
            &["t3", "t4", "t5", "c9", "c10", "c11"],
        ]);
        let (r, rb) = (estimate_simple(&m, &plain), estimate_simple(&m, &blocked));
        assert_eq!(rb.design, "common switch date");
        assert_eq!(format!("{r:?}"), format!("{rb:?}"));
        blocked.strata = strata_of(&[&["t0", "t1"]]);
        let reason = estimate_simple(&m, &blocked)
            .refusal
            .expect("validated on this path too");
        assert!(reason.contains("no stratum"), "{reason}");
    }

    // The same four guarantees, driven through the public dispatcher.

    #[test]
    fn experiment_stratified_guard_through_estimate_effect() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        a.strata = strata_of(&[
            &["t0", "c6"],
            &["t1"],
            &["t2"],
            &["t3"],
            &["t4"],
            &["t5"],
            &["c7"],
            &["c8"],
            &["c9"],
            &["c10"],
            &["c11"],
        ]);
        let r = estimate_effect(&m, &a, 3);
        let reason = r.refusal.expect("must refuse");
        assert!(
            reason.contains("cannot reach") && reason.contains("within 11 strata"),
            "{reason}"
        );
        assert!(r.estimate.is_nan() && r.p_value.is_nan());
    }

    #[test]
    fn experiment_stratified_recovers_a_planted_effect_through_estimate_effect() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        a.strata = strata_of(&[
            &["t0", "t1", "t3", "c6", "c7", "c8"],
            &["t2", "t4", "t5", "c9", "c10", "c11"],
        ]);
        let r = estimate_effect(&m, &a, 3);
        assert!(r.refusal.is_none(), "{:?}", r.refusal);
        assert_eq!(r.design, "staggered");
        assert!(
            r.ci_low < 25.0 && 25.0 < r.ci_high,
            "{:?}",
            (r.ci_low, r.ci_high)
        );
    }

    #[test]
    fn experiment_stratified_membership_refusals_through_estimate_effect() {
        let (m, mut a) = staggered_fixture(25.0, 7);
        let all: Vec<&str> = vec![
            "t0", "t1", "t2", "t3", "t4", "t5", "c6", "c7", "c8", "c9", "c10",
        ];
        a.strata = strata_of(&[&all]); // c11 is in none
        let r = estimate_effect(&m, &a, 3).refusal.expect("must refuse");
        assert!(r.contains("'c11'") && r.contains("no stratum"), "{r}");
        a.strata = strata_of(&[&all, &["c11", "t0"]]); // t0 is in two
        let r = estimate_effect(&m, &a, 3).refusal.expect("must refuse");
        assert!(r.contains("'t0'") && r.contains("2 strata"), "{r}");
        // A stratum member the assignment does not name is out of scope, not an error.
        a.strata = strata_of(&[&all, &["c11", "closed_store"]]);
        assert!(estimate_effect(&m, &a, 3).refusal.is_none());
    }

    #[test]
    fn experiment_stratified_common_date_keeps_welch_through_estimate_effect() {
        let m = fixture(12, 60, 6, 20.0, 31, 4);
        let plain = assign(&m, &["t0", "t1", "t2", "t3", "t4", "t5"], 31);
        let mut blocked = plain.clone();
        blocked.strata = strata_of(&[
            &["t0", "t1", "t2", "c6", "c7", "c8"],
            &["t3", "t4", "t5", "c9", "c10", "c11"],
        ]);
        let (r, rb) = (
            estimate_effect(&m, &plain, 3),
            estimate_effect(&m, &blocked, 3),
        );
        assert_eq!(rb.design, "common switch date");
        assert_eq!(format!("{r:?}"), format!("{rb:?}"));
        blocked.strata = strata_of(&[&["t0", "t1"]]);
        let reason = estimate_effect(&m, &blocked, 3)
            .refusal
            .expect("validated on this path too");
        assert!(reason.contains("no stratum"), "{reason}");
    }

    /// Seven units with means 70, 60, … 10; three blocks are sizes 3, 2, 2,
    /// largest first, contiguous in the ranking, each listed sorted.
    #[test]
    fn experiment_propose_strata_ranks_by_baseline_into_contiguous_blocks() {
        let rows: Vec<(String, i64, f64)> = ["g", "a", "f", "b", "e", "c", "d"]
            .iter()
            .enumerate()
            .flat_map(|(i, u)| (1..=10).map(move |d| (u.to_string(), d, 70.0 - 10.0 * i as f64)))
            .collect();
        let m = PanelMatrix::from_triples(rows);
        let s = propose_strata(&m, 3, 1, 11).expect("valid");
        assert_eq!(
            s,
            vec![
                names(&["a", "f", "g"]),
                names(&["b", "e"]),
                names(&["c", "d"])
            ]
        );
        assert_eq!(propose_strata(&m, 3, 1, 11), Ok(s), "deterministic");
    }

    #[test]
    fn experiment_propose_strata_breaks_ties_by_name_and_refuses_what_it_cannot_cut() {
        let m = matrix_from(&[("b", 1, 5.0), ("a", 1, 5.0), ("c", 1, 1.0), ("d", 1, 1.0)]);
        assert_eq!(
            propose_strata(&m, 2, 1, 2),
            Ok(vec![names(&["a", "b"]), names(&["c", "d"])])
        );
        let e = propose_strata(&m, 1, 1, 2).expect_err("one block is no blocking");
        assert!(e.contains("at least 2 blocks"), "{e}");
        let e = propose_strata(&m, 3, 1, 2).expect_err("a block of one");
        assert!(
            e.contains("fewer than 2 units") && e.contains("at most 2"),
            "{e}"
        );
        let e = propose_strata(&m, 2, 5, 9).expect_err("nothing to rank by");
        assert!(e.contains("no retained day"), "{e}");
    }

    #[test]
    fn experiment_allocate_gives_each_wave_its_share_of_every_stratum() {
        let strata: Vec<Vec<String>> = (0..4)
            .map(|b| (0..6).map(|i| format!("s{b}_{i}")).collect())
            .collect();
        let waves = allocate(&strata, &[4, 4], 7);
        for w in &waves {
            for s in &strata {
                assert_eq!(
                    w.iter().filter(|u| s.contains(u)).count(),
                    1,
                    "4 of 24 from a stratum of 6 is exactly 1: {w:?}"
                );
            }
        }
        let placed: std::collections::BTreeSet<&String> = waves.iter().flatten().collect();
        assert_eq!(placed.len(), 8, "no unit in two waves");
        // Uneven: 3 of 24 from two strata of 12 is 1.5 each — every wave's count
        // is 1 or 2 in each stratum, and the seed decides which.
        let two: Vec<Vec<String>> = vec![strata[0..2].concat(), strata[2..4].concat()];
        for seed in 0..20 {
            for w in allocate(&two, &[3, 3], seed) {
                for s in &two {
                    let k = w.iter().filter(|u| s.contains(u)).count();
                    assert!((1..=2).contains(&k), "seed {seed}: {k} from one stratum");
                }
            }
        }
        // Neither stratum order nor member order reaches the result.
        let mut shuffled: Vec<Vec<String>> = strata.iter().rev().cloned().collect();
        for s in &mut shuffled {
            s.reverse();
        }
        assert_eq!(allocate(&shuffled, &[4, 4], 7), waves);
    }

    /// The draw ranks on ITS OWN pre-window: units swap sizes at day 100, so a
    /// window before it and a window after it produce different strata.
    #[test]
    fn experiment_blocked_layout_ranks_on_the_draws_own_pre_window() {
        let rows: Vec<(String, i64, f64)> = (0..8usize)
            .flat_map(|u| {
                (1..=200i64).map(move |d| {
                    let big = if d < 100 { u < 4 } else { u >= 4 };
                    (
                        format!("u{u}"),
                        d,
                        if big { 100.0 } else { 10.0 } + u as f64,
                    )
                })
            })
            .collect();
        let m = PanelMatrix::from_triples(rows);
        let all: Vec<String> = m.units().to_vec();
        let (_, before) = blocked_layout(&m, &all, &[2], 2, (40, 90), 1).expect("fits");
        let (_, after) = blocked_layout(&m, &all, &[2], 2, (120, 170), 1).expect("fits");
        assert_eq!(before[0], names(&["u0", "u1", "u2", "u3"]));
        assert_eq!(after[0], names(&["u4", "u5", "u6", "u7"]));
    }
}
