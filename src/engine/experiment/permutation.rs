//! The label-permutation test behind the staggered estimator, and the
//! exposure-adjusted sharp null its interval inverts.

use crate::engine::experiment::staggered::Wave;
use crate::engine::experiment::{collapse, PanelMatrix, SplitMix64};
use std::collections::BTreeMap;

pub(crate) const PERMUTATIONS: usize = 2000;
/// Below this many distinct relabellings, enumerate them all: the p-value is
/// then exact, which matters precisely where the reachability guard is close.
/// Shared with the switchback's sign-flip test.
pub(crate) const ENUMERATE_BELOW: f64 = 20_000.0;

/// The smallest p a randomisation test can report, given the `exact` floor its
/// full enumeration would reach (`1/N` for a permutation test, `2/2^P` for the
/// sign-flip test) and how many distinct relabellings `n_distinct` there are.
/// Above `ENUMERATE_BELOW` the test SAMPLES, and a sampled p is `(k+1)/(B+1)`:
/// it can never fall below `1/(PERMUTATIONS+1)` however large `N` is. A guard
/// that checked only the exact floor passed designs that could never reject.
pub(crate) fn min_attainable_p(exact: f64, n_distinct: f64) -> f64 {
    if n_distinct <= ENUMERATE_BELOW {
        exact
    } else {
        exact.max(1.0 / (PERMUTATIONS + 1) as f64)
    }
}

/// Stratum → permutation group → unit rows. Units are exchangeable inside a
/// group's stratum and nowhere else.
pub(crate) type Groups = Vec<Vec<Vec<usize>>>;

/// The observed structure a relabelling permutes: the waves the statistic reads,
/// every row's observed switch, and the permutation groups.
pub(crate) struct Structure<'a> {
    pub(crate) waves: &'a [Wave],
    pub(crate) switch_of: &'a [Option<i64>],
    pub(crate) groups: &'a [Vec<Vec<usize>>],
}

/// Fraction of the RETAINED days in the half-open window `[a, b)` that fall on
/// or after `switch` — the same rows the window mean averaged, so the adjustment
/// removes exactly what the mean carries. Calendar-day exposure disagrees on any
/// window with gaps, and a window can have gaps and still clear the coverage floor.
pub(crate) fn exposure(m: &PanelMatrix, switch: Option<i64>, a: i64, b: i64) -> f64 {
    let Some(k) = switch else { return 0.0 };
    let (lo, hi) = m.ordinal_range(a, b);
    if hi <= lo {
        return 0.0;
    }
    // `ordinal_range` is two `partition_point`s and does not care that its
    // arguments are ordered, so a switch AFTER this window returns a start index
    // past `hi`. That is the common case, not a corner: every later adopter is a
    // control for an earlier wave and is scored against that wave's windows.
    // On `[1, 21)` with a switch on day 41 the two indices are 20 and 40, and the
    // unclamped subtraction is `20usize - 40usize` — a debug-build panic. Clamping
    // into `[lo, hi]` makes a future switch a zero exposure, which is what it is.
    let (klo, _) = m.ordinal_range(k.max(a), b);
    (hi - klo.clamp(lo, hi)) as f64 / (hi - lo) as f64
}

/// Every unit's collapsed delta in every wave's window, with `tau` removed in
/// proportion to that unit's exposure THERE: `tau × (exposure_post −
/// exposure_pre)`. Wave members lose the whole of `tau`; a clean control loses
/// nothing; a later adopter used as a control is untreated in this window and
/// keeps its delta whole — which a global treated flag gets wrong.
fn adjusted_deltas(
    m: &PanelMatrix,
    waves: &[Wave],
    switch_of: &[Option<i64>],
    tau: f64,
) -> Vec<Vec<Option<f64>>> {
    waves
        .iter()
        .map(|w| {
            let deltas = collapse(m, &w.windows);
            (0..m.n_units())
                .map(|u| {
                    let d = deltas.iter().find(|d| d.index == u)?;
                    let shift = exposure(m, switch_of[u], w.windows.post.0, w.windows.post.1)
                        - exposure(m, switch_of[u], w.windows.pre.0, w.windows.pre.1);
                    Some(d.delta - tau * shift)
                })
                .collect()
        })
        .collect()
}

/// The size-weighted aggregate under a relabelling, read off pre-adjusted deltas.
/// `perm[slot]` is the unit whose data fills `slot`.
fn aggregate_under(waves: &[Wave], adj: &[Vec<Option<f64>>], perm: &[usize]) -> Option<f64> {
    let (mut num, mut den) = (0.0, 0usize);
    for (w, deltas) in waves.iter().zip(adj) {
        let mean = |members: &[usize]| -> Option<f64> {
            let v: Vec<f64> = members.iter().filter_map(|s| deltas[perm[*s]]).collect();
            if v.len() < members.len() {
                return None;
            }
            Some(v.iter().sum::<f64>() / v.len() as f64)
        };
        num += w.treated.len() as f64 * (mean(&w.treated)? - mean(&w.controls)?);
        den += w.treated.len();
    }
    if den == 0 {
        None
    } else {
        Some(num / den as f64)
    }
}

pub(crate) struct PermOutcome {
    pub(crate) rejected: bool,
    pub(crate) p_value: f64,
    /// Relabellings the p-value was computed over.
    pub(crate) draws: usize,
}

/// Test the sharp null "every treated unit gained exactly `tau`": adjust once
/// with OBSERVED exposure, then permute labels over the adjusted data. The
/// decision is the p-value, never a percentile read off the sorted nulls.
pub(crate) fn permutation_test(
    m: &PanelMatrix,
    s: &Structure,
    tau: f64,
    alpha: f64,
    seed: u64,
) -> Option<PermOutcome> {
    let adj = adjusted_deltas(m, s.waves, s.switch_of, tau);
    let ident: Vec<usize> = (0..m.n_units()).collect();
    let obs = aggregate_under(s.waves, &adj, &ident)?;
    // A NaN or infinite observation compares false against everything, so no
    // relabelling would be "as extreme" and the p-value would collapse to zero:
    // a rejection read off garbage. There is nothing to test.
    if !obs.is_finite() {
        return None;
    }
    let (null, exact) = null_distribution(m.n_units(), s, &adj, seed)?;
    // A non-finite draw (a relabelling that pulled a NaN unit into a slot) stays
    // in the denominator, so it must count as extreme: that can only raise p.
    // Compared within rounding: a stratified relabelling sums the same deltas in
    // another order, so the mirror of the observed labelling can land an ulp
    // below |obs|. Dropping it would halve the exact p and reject a design whose
    // minimum attainable p is above alpha.
    let extreme = null
        .iter()
        .filter(|v| !v.is_finite() || at_least(v.abs(), obs.abs()))
        .count();
    let p_value = if exact {
        // Enumeration contains the observed labelling, so this count IS the exact
        // tail probability, and its floor of `1/N` is what the guard bounds.
        extreme as f64 / null.len() as f64
    } else {
        // Sampling does not contain it, and a bare `extreme / B` can report 0 — a
        // p-value no test may produce. `(1 + ·) / (1 + B)` is the valid Monte
        // Carlo p-value.
        (extreme + 1) as f64 / (null.len() + 1) as f64
    };
    Some(PermOutcome {
        rejected: p_value <= alpha,
        p_value,
        draws: null.len(),
    })
}

/// `v >= obs` up to floating-point rounding: a statistic equal to the observed
/// one mathematically, but summed in another order, still counts as extreme.
pub(crate) fn at_least(v: f64, obs: f64) -> bool {
    v >= obs - REL_TIE * obs.abs().max(f64::MIN_POSITIVE)
}

/// The relative slack within which two statistics are treated as a tie.
const REL_TIE: f64 = 1e-9;

/// The null: every distinct relabelling when there are few enough (`true`),
/// otherwise `PERMUTATIONS` seeded within-stratum shuffles (`false`). `None`
/// when too few relabellings produced a usable aggregate.
fn null_distribution(
    n_units: usize,
    s: &Structure,
    adj: &[Vec<Option<f64>>],
    seed: u64,
) -> Option<(Vec<f64>, bool)> {
    if relabellings(s.groups) <= ENUMERATE_BELOW {
        let mut out = Vec::new();
        each_relabelling(s.groups, &mut |perm| {
            if let Some(v) = aggregate_under(s.waves, adj, perm) {
                out.push(v)
            }
        });
        return (!out.is_empty()).then_some((out, true));
    }
    let mut rng = SplitMix64::new(seed);
    let mut perm: Vec<usize> = (0..n_units).collect();
    let mut out = Vec::with_capacity(PERMUTATIONS);
    for _ in 0..PERMUTATIONS {
        shuffle_within(s.groups, &mut rng, &mut perm);
        if let Some(v) = aggregate_under(s.waves, adj, &perm) {
            out.push(v)
        }
    }
    (out.len() >= PERMUTATIONS / 2).then_some((out, false))
}

/// One uniformly drawn relabelling: each stratum's units shuffled among that
/// stratum's own slots. Uniform over the stratum's permutations, hence over its
/// relabellings — and no unit ever lands in another stratum's slot.
pub(crate) fn shuffle_within(groups: &[Vec<Vec<usize>>], rng: &mut SplitMix64, perm: &mut [usize]) {
    for stratum in groups {
        let slots: Vec<usize> = stratum.iter().flatten().copied().collect();
        let mut units = slots.clone();
        let k = units.len();
        rng.partial_shuffle(&mut units, k);
        for (slot, unit) in slots.iter().zip(&units) {
            perm[*slot] = *unit;
        }
    }
}

/// The permutation groups: inside each stratum (`stratum_of[row]`), every unit
/// grouped by its OBSERVED switch day, with that stratum's never-treated pool as
/// one further group. **A dropped wave is its own group**, not part of the
/// never-treated pool: it still controls an earlier wave and is ineligible for a
/// later one, so it is not interchangeable with a unit that never switched
/// (merging them counts 210 relabellings where there are 630). Unblocked is
/// `stratum_of` all zero: one stratum.
pub(crate) fn permutation_groups(switch_of: &[Option<i64>], stratum_of: &[usize]) -> Groups {
    let mut strata: BTreeMap<usize, (BTreeMap<i64, Vec<usize>>, Vec<usize>)> = BTreeMap::new();
    for (u, s) in switch_of.iter().enumerate() {
        let (by_day, never) = strata.entry(stratum_of[u]).or_default();
        match s {
            Some(d) => by_day.entry(*d).or_default().push(u),
            None => never.push(u),
        }
    }
    strata
        .into_values()
        .map(|(by_day, never)| {
            let mut groups: Vec<Vec<usize>> = by_day.into_values().collect();
            if !never.is_empty() {
                groups.push(never)
            }
            groups
        })
        .collect()
}

/// Distinct relabellings of the observed structure: per stratum, the multinomial
/// coefficient over its group sizes; overall, the PRODUCT over strata — labels
/// never cross a stratum. The smallest p a permutation test can ever produce is
/// `1 / this` — NOT `2 /`. Each factor is rounded: a multinomial is an integer,
/// and the `exp(ln …)` error must not move `1/N` across alpha.
pub(crate) fn relabellings(groups: &[Vec<Vec<usize>>]) -> f64 {
    let ln_fact = |k: usize| (1..=k).map(|i| (i as f64).ln()).sum::<f64>();
    groups
        .iter()
        .map(|stratum| {
            let n: usize = stratum.iter().map(Vec::len).sum();
            (ln_fact(n) - stratum.iter().map(|g| ln_fact(g.len())).sum::<f64>())
                .exp()
                .round()
        })
        .product()
}

/// Call `f` once per distinct relabelling. `perm[slot]` is the unit whose data
/// fills `slot`. Strata are filled one after another, each from its OWN units;
/// inside one, group `g`'s slots receive a `|g|`-subset of the stratum's units
/// not yet placed, in increasing order — a group's internal order never reaches
/// the statistic, so each distinct membership pattern appears exactly once, the
/// identity included.
pub(crate) fn each_relabelling(groups: &[Vec<Vec<usize>>], f: &mut dyn FnMut(&[usize])) {
    let n: usize = groups.iter().flatten().map(Vec::len).sum();
    let mut perm: Vec<usize> = (0..n).collect();
    fill_stratum(groups, 0, &mut perm, f);
}

fn fill_stratum(
    groups: &[Vec<Vec<usize>>],
    j: usize,
    perm: &mut [usize],
    f: &mut dyn FnMut(&[usize]),
) {
    let Some(stratum) = groups.get(j) else {
        f(perm);
        return;
    };
    let mut units: Vec<usize> = stratum.iter().flatten().copied().collect();
    units.sort_unstable();
    fill_group(stratum, 0, &units, perm, &mut |p: &mut [usize]| {
        fill_stratum(groups, j + 1, p, f)
    });
}

fn fill_group(
    groups: &[Vec<usize>],
    g: usize,
    remaining: &[usize],
    perm: &mut [usize],
    next: &mut dyn FnMut(&mut [usize]),
) {
    let Some(slots) = groups.get(g) else {
        next(perm);
        return;
    };
    let mut picked = Vec::with_capacity(slots.len());
    choose(
        remaining,
        slots.len(),
        0,
        &mut picked,
        &mut |chosen: &[usize]| {
            for (slot, unit) in slots.iter().zip(chosen) {
                perm[*slot] = *unit;
            }
            let rest: Vec<usize> = remaining
                .iter()
                .copied()
                .filter(|u| !chosen.contains(u))
                .collect();
            fill_group(groups, g + 1, &rest, perm, next);
        },
    );
}

/// Every `k`-subset of `from`, in increasing order, handed to `f`.
fn choose(
    from: &[usize],
    k: usize,
    start: usize,
    acc: &mut Vec<usize>,
    f: &mut dyn FnMut(&[usize]),
) {
    if acc.len() == k {
        f(acc);
        return;
    }
    for (i, &u) in from.iter().enumerate().skip(start) {
        if from.len() - i < k - acc.len() {
            break;
        }
        acc.push(u);
        choose(from, k, i + 1, acc, f);
        acc.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A later adopter is a control for an earlier wave, so `exposure` is asked
    /// about a switch that falls PAST the window it is scoring. `ordinal_range`
    /// returns a start index beyond the window's end there, and the unguarded
    /// subtraction was `20usize - 40usize` — a panic on the ordinary path.
    #[test]
    fn experiment_staggered_exposure_is_zero_for_a_switch_after_the_window() {
        let m = PanelMatrix::from_triples(
            (1..=60)
                .map(|d| ("u".to_string(), d, 0.0))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            exposure(&m, Some(41), 1, 21),
            0.0,
            "switch well past the window end"
        );
        assert_eq!(
            exposure(&m, Some(21), 1, 21),
            0.0,
            "switch ON the half-open end"
        );
        assert!(
            (exposure(&m, Some(11), 1, 21) - 0.5).abs() < 1e-12,
            "half the window"
        );
        assert_eq!(
            exposure(&m, Some(1), 1, 21),
            1.0,
            "switched as the window opens"
        );
        assert_eq!(exposure(&m, None, 1, 21), 0.0, "never treated");
    }

    /// A dropped wave's units are not interchangeable with never-treated ones:
    /// the singleton still controls an earlier wave and is ineligible for a
    /// later one. Merging it into the pool counts 210 relabellings where there
    /// are 630, and enumerates a null centred off zero.
    #[test]
    fn experiment_staggered_enumeration_keeps_dropped_waves_distinct() {
        // Two units switch on day 20, two on day 40, one on day 60 (a singleton
        // wave, dropped for being under two units), two never switch.
        let switch_of = vec![Some(20), Some(20), Some(40), Some(40), Some(60), None, None];
        let groups = permutation_groups(&switch_of, &[0; 7]);
        assert_eq!(groups.len(), 1, "unblocked: one stratum");
        assert_eq!(
            groups[0].iter().map(|g| g.len()).collect::<Vec<_>>(),
            vec![2, 2, 1, 2],
            "the dropped singleton is its own pool, not part of never-treated"
        );
        assert_eq!(
            relabellings(&groups),
            630.0,
            "7!/(2!2!1!2!) = 630; folding the singleton into the pool gives 210"
        );
        let mut seen = std::collections::HashSet::new();
        each_relabelling(&groups, &mut |perm| {
            seen.insert(perm.to_vec());
        });
        assert_eq!(
            seen.len(),
            630,
            "every distinct membership pattern, exactly once"
        );
        assert_eq!(
            relabellings(&[vec![vec![0, 1, 2], vec![3, 4, 5]]]),
            20.0,
            "a multinomial is an integer — rounded, so 1/20 is exactly 0.05"
        );
    }
}
