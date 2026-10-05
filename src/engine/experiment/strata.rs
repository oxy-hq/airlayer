//! Blocked assignment: validating the strata an assignment was randomised
//! within, and mapping panel rows to them. `propose_strata` and the
//! proportional allocation that pricing and the placebo share live here too.

use crate::engine::experiment::estimate::Assignment;
use crate::engine::experiment::PanelMatrix;

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

#[cfg(test)]
mod tests {
    use crate::engine::experiment::estimate::estimate_simple;
    use crate::engine::experiment::permutation::{
        each_relabelling, permutation_groups, relabellings, shuffle_within,
    };
    use crate::engine::experiment::staggered::estimate_staggered;
    use crate::engine::experiment::testkit::{assign, fixture, staggered_fixture};
    use crate::engine::experiment::SplitMix64;

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
}
