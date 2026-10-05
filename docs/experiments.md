# Experiments (effect estimation)

`engine::experiment` estimates the effect of a **deliberate intervention**: a lever pulled on some units (stores, regions, accounts) and not on others, or switched on and off over time. It answers "did the lever move the measure, by how much, and how sure are we?" over a panel of daily values, and it prices a design before anyone runs it.

It is a library module with no CLI subcommand. Its host is Oxygen's pre-registered experiments; the reasoning behind each choice lives in oxygen-internal `internal-docs/experiments.md` ("The estimator"). This page is the airlayer-side reference: what each entry point takes, what it returns, and what it refuses.

> **Wave, not cohort.** Units that switch on the same day form a *wave*. `engine::cohort` (peer cohorts on an entity) is unrelated, and nothing here touches it.

## The shape of the problem

The input is always a `PanelMatrix`: one measure over a (unit, day) grid.

```rust
use airlayer::engine::experiment::{day_ordinal, PanelMatrix};

let d = |y, m, dd| day_ordinal(chrono::NaiveDate::from_ymd_opt(y, m, dd).unwrap());
let panel = PanelMatrix::from_triples(vec![
    ("bondi".to_string(), d(2026, 3, 1), 1520.0),
    ("manly".to_string(), d(2026, 3, 1), 980.0),
    // ... one row per (unit, day)
]);
```

- **Days are ordinals**, `NaiveDate::num_days_from_ce()`, which is the convention `metric_tree_fit` already uses. Every window, switch day and period start in this module is a day ordinal. The host never re-derives one.
- **Units are sorted by name.** Arms are always matched by unit *name* through `unit_index`, never by the caller's ordering.
- **The grid is dense.** `from_triples` drops any day on which some unit lacks a value. The retained days are therefore a subset of the calendar, which is why windows are laid out in calendar ordinals and then checked for **coverage** (retained days ÷ calendar days). A window under the registered `coverage_floor` is refused. It is never silently shortened, because a window counted in retained rows spans longer than it claims.

## Three designs, one entry point

```rust
pub enum ExperimentDesign {
    Waves(Assignment),               // common switch date, or staggered waves
    Switchback(SwitchbackSchedule),  // the whole fleet alternates on/off
}

pub fn estimate(m: &PanelMatrix, d: &ExperimentDesign, seed: u64) -> EffectResult;
```

`estimate` dispatches to `estimate_effect` (waves) or `estimate_switchback`. Both return the same `EffectResult`, so the ratio estimator and the host never branch on the design. `seed` makes every randomisation test reproducible.

### Waves: `Assignment`

```rust
pub struct Assignment {
    pub switch_day: HashMap<String, Option<i64>>, // unit → switch ordinal; None = control
    pub strata: Option<Vec<Vec<String>>>,         // blocks randomised within; None = unblocked
    pub pre_days: usize,
    pub post_days: usize,
    pub anticipation_days: usize,  // excluded before the switch (pre-announcement)
    pub washout_days: usize,       // excluded after the switch (ramp-up)
    pub coverage_floor: f64,       // [0, 1]
    pub alpha: f64,                // (0, 1)
    pub family: usize,             // outcomes registered in advance; 1 = one hypothesis
}
```

- A unit mapped to `None` is a control for the whole run.
- A panel unit **absent** from `switch_day` is in neither arm. It is dropped before any computation.
- A name the panel does not hold is refused. A misspelled treated unit must never sit in the control pool.

`estimate_effect` routes on the number of distinct switch dates.

**One switch date → `"common switch date"`.** Each unit collapses to **one** pre/post difference (`collapse`), so serial correlation inside a unit cannot inflate `t`, and no cluster-robust variance is needed. The two arms are then compared with Welch, using the same arithmetic as `metric_tree_ops::gap_is_significant` (shared as `welch_se_df`). The estimate carries `se`, `t_stat`, `df`, a t interval and the Welch p-value.

**Several switch dates → `"staggered"`.** Units are grouped into waves by switch date. Each wave is estimated against only the units still **clean** (not yet treated) through its post window. A pooled two-way fixed-effects regression would use already-treated units as controls and can flip the sign of the aggregate under heterogeneous effects. Then:

- Wave effects are combined **weighted by wave size**, so a one-unit wave cannot outvote a nine-unit one.
- A wave with no clean controls is dropped and listed in `dropped_waves`. This is typically the last wave of a rollout with no permanent holdout. It is never folded in at zero.
- The test permutes unit labels over the observed wave structure. It enumerates all relabellings exactly below 20,000 and otherwise samples 2,000 seeded ones.
- The interval inverts an exposure-adjusted sharp null ("every treated unit gained exactly τ"). An endpoint the inversion cannot bound is reported as ±∞.
- `se`, `t_stat` and `df` are `NaN` on this path, because a permutation test has none of them.
- A design whose smallest attainable p (`1/N` relabellings, or the sampled floor `1/(2000+1)`) is above the per-comparison alpha is refused up front. It could never reject.

**Strata.** With `strata` set, every unit `switch_day` names must sit in exactly one stratum. The staggered permutation then moves labels only *within* a stratum: both the relabelling count and the minimum-attainable-p guard are products over strata. The common-date path keeps Welch, which is conservative under blocking.

### Switchback: `SwitchbackSchedule`

```rust
pub struct SwitchbackSchedule {
    pub periods: Vec<Period>,   // consecutive, in pairs; one on + one off per pair
    pub period_days: usize,
    pub washout_days: usize,    // excluded at the start of EVERY period
    pub coverage_floor: f64,
    pub alpha: f64,
    pub family: usize,
}
pub struct Period { pub from_day: i64, pub on: bool }
```

The whole panel alternates on and off. `estimate_switchback`:

1. Takes the fleet mean of each period over its post-washout days.
2. Forms one `on − off` difference per pair. A pair with a period under the coverage floor is dropped.
3. Tests with an **exact sign-flip randomisation test**: the coin that ordered each pair *is* the randomisation, so it is the test. The smallest attainable p is `2/2^P` for `P` pairs, and a schedule where that is above alpha is refused.
4. Builds the interval as that test's constant-effect inversion.

`retained_on_days` reports the retained ON days the per-day effect multiplies by when the host totals it. The estimand is `SWITCHBACK_ESTIMAND`: short-run effect of on against off under alternation, per unit-day, fleet mean.

## Reading an `EffectResult`

| Field | Meaning |
|-------|---------|
| `estimate` | Mean effect per treated unit-day (`ESTIMAND`), or the switchback estimand |
| `se`, `t_stat`, `df` | Welch quantities on the common-date path; `NaN` on the permutation and sign-flip paths |
| `ci_low`, `ci_high` | Interval at the per-comparison rate. Either endpoint may be **±∞**: unbounded, never invented |
| `p_value` | Raw two-sided per-comparison p (Welch, permutation or sign-flip). **Not** adjusted for `family` |
| `significant` | The decision, with `family` applied via a Šidák per-comparison rate |
| `design` | `"common switch date"`, `"staggered"`, `"switchback"` or `"refused"` |
| `n_treated`, `n_control` | Units behind each arm |
| `dropped_waves` | Staggered only: waves that could not be estimated, with the reason |
| `pre_trend` | The same estimator run inside the pre-period (two halves). `diverged` flags a pre-existing gap. `None` means the check could not run, which is absence, not a pass. Per wave on the staggered path |
| `size_bias` | Does a treated unit's size predict its own effect, beyond what controls show for the same correlation? Evidence on whether the effect transports to untreated units. It never gates the estimate |
| `retained_on_days` | Switchback only |
| `refusal` | Why no estimate was made |

**Refusals are values, not errors.** A refused `EffectResult` has `refusal: Some(reason)`, every number `NaN`, `significant: false` and `design: "refused"`. A refused `PowerResult` likewise carries `NaN` numbers and a `refusal`. A zero would read as a finding: a zero effect, or worse, a zero p. Every message names the guard that fired. Refusals cover thin or spread-less arms, unknown unit names, windows under the coverage floor, non-finite panel cells, unreachable significance, and `alpha`/`coverage_floor`/`power` outside their ranges.

**`family` is the pre-registration dividend.** `family: 1` applies no multiplicity correction at all. `family: k` uses the Šidák rate `1 − (1 − α)^(1/k)`. This deliberately differs from opportunity sizing's threshold, which also carries a selection term.

**Serialization.** Every public result and input derives `serde::Serialize`. Non-finite numbers serialize as `null`: `ci_low: null` with no refusal means −∞, and `ci_high: null` means +∞. `ExperimentDesign` and `DesignShape` are externally tagged, which is serde's default (`{"Switchback": {...}}`).

## Pricing a design: `placebo_power`

```rust
pub fn placebo_power(m: &PanelMatrix, d: &DesignSpec, seed: u64) -> PowerResult;

pub enum DesignShape {
    CommonDate { n_treated: usize, n_control: usize },
    Staggered  { wave_sizes: Vec<usize>, spacing_days: usize, n_never_treated: usize },
    Switchback { period_days: usize, pairs: usize },
}
```

`placebo_power` returns the **minimum detectable effect** (MDE) of a proposed design at the requested `power`, by running the design many times on history where nothing happened.

- **Each draw runs that design's own decision.** A common-date draw uses Welch with its own critical value, a staggered draw uses the permutation test, and a switchback draw uses the sign-flip rule over slid, coin-oriented pair schedules. The MDE is therefore the effect *this* estimator detects, not an analytic approximation.
- **History is bounded.** `history_to` is an **exclusive** day ordinal that no placebo window reaches. At result time the host passes the first switch minus `anticipation_days`, so the null never contains the real effect. `None` prices over all of history.
- **Blocked draws.** `blocks > 1` ranks each draw's units by that draw's own pre-window mean into strata, the way `propose_strata` does, and allocates within them. It must be `0` for a switchback.
- **Too little history is refused.** Fewer than 2 non-overlapping design spans in the bounded history is refused, because the null would be one look wearing a large iteration count.

`PowerResult` carries `mde`, `mde_relative` (against `baseline`, the mean over the same bounded history), `null_sd`, the usable `iterations`, `distinct_windows` (distinct placebo switch days, since the time component is thinner than the iteration count) and `independent_stretches`.

## Proposing an assignment

These functions are all deterministic given their seed and independent of the order units arrive in. The seed reproduces a split. It does not prove the split was the first one drawn; that is the host's proposal log.

- **`propose_strata(m, blocks, from, to)`** ranks the panel's units by their mean over `[from, to)`, largest first with ties broken by name. It then cuts the ranking into `blocks` contiguous strata whose sizes differ by at most one.
- **`propose_waves(units, wave_sizes, first_switch_day, spacing_days, strata, seed)`** makes a seeded split into waves `spacing_days` apart. Units not placed in any wave are controls. Given `strata`, each wave is allocated across strata in proportion to stratum size, with remainders placed by the seed.
- **`propose_switchback(first_day, period_days, pairs, seed)`** flips one seeded coin per pair to decide which half is on. It returns `2 · pairs` consecutive periods, or an empty list for `pairs == 0` or `period_days < 1`, which the estimator then refuses.

Seeds are full `u64`. The host masks them to 53 bits so they survive JSON. A stored seed must keep replaying the same assignment, and tests pin that.

## The lever as an instrument: `estimate_ratio`

```rust
pub fn estimate_ratio(
    target: &PanelMatrix, driver: &PanelMatrix, d: &ExperimentDesign, seed: u64,
) -> RatioResult;
```

When the lever moves a *driver* (say, prep time) and you want its effect on a *target* (say, revenue) per unit of driver, the lever is an instrument. `estimate_ratio` returns the Wald ratio `ITT(target) / ITT(driver)` for either design.

- **The panels are aligned first.** The two panels are restricted to the units *and* days both hold, matched by name and ordinal. An assigned unit missing from either panel is refused by name.
- **The interval is an Anderson–Rubin set.** It inverts the *unchanged* estimator's decision on `target − β·driver`, so it is valid under a weak instrument.
- **Hard refusals stop early.** If either effect is refused, the driver effect is exactly zero, or the Anderson–Rubin test cannot run at the point estimate, the result is refused with `NaN` numbers.
- **Soft refusals keep the numbers.** The first stage is tested at family 1. If it is not significant ("the lever did not measurably move the driver"), the `coefficient` and the set are still reported. The same applies when the set is two rays rather than one interval (reported as ±∞) or unbounded on a side at the registered alpha and family. `refusal` names every reason that applies, joined with `; `, so a consumer must check `refusal` before trusting `ci_low`/`ci_high`.
- **Both effects are kept.** `first_stage` and `target_effect` are the two full `EffectResult`s.

## Example

```rust
use std::collections::HashMap;
use airlayer::engine::experiment::{
    day_ordinal, estimate, Assignment, ExperimentDesign, PanelMatrix,
};

fn main() {
    let start = day_ordinal(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
    let switch = start + 28;
    let units = ["s1", "s2", "s3", "s4", "s5", "s6"];

    // 8 weeks of daily sales; s1–s3 gain +12/day after the switch.
    let mut rows = Vec::new();
    for (i, u) in units.iter().enumerate() {
        for day in start..start + 56 {
            let noise = ((day * 7 + i as i64 * 13) % 11) as f64;
            let lift = if i < 3 && day >= switch { 12.0 } else { 0.0 };
            rows.push((u.to_string(), day, 100.0 + 10.0 * i as f64 + noise + lift));
        }
    }
    let panel = PanelMatrix::from_triples(rows);

    let switch_day: HashMap<String, Option<i64>> = units
        .iter()
        .enumerate()
        .map(|(i, u)| (u.to_string(), (i < 3).then_some(switch)))
        .collect();
    let design = ExperimentDesign::Waves(Assignment {
        switch_day,
        strata: None,
        pre_days: 21,
        post_days: 21,
        anticipation_days: 0,
        washout_days: 7,
        coverage_floor: 0.9,
        alpha: 0.05,
        family: 1,
    });

    let r = estimate(&panel, &design, 42);
    match &r.refusal {
        Some(why) => println!("refused: {why}"),
        None => println!(
            "{}: {:.2} [{:.2}, {:.2}] p={:.4} significant={}",
            r.design, r.estimate, r.ci_low, r.ci_high, r.p_value, r.significant
        ),
    }
}
// prints: common switch date: 12.17 [11.42, 12.93] p=0.0002 significant=true
```

## Module map

| File | Responsibility |
|------|----------------|
| `mod.rs` | Re-exports, the seeded `SplitMix64`, Welch, Šidák rate, rate validation |
| `panel.rs` | `PanelMatrix`, `day_ordinal`, `windows_around`, coverage, `collapse` |
| `estimate.rs` | `Assignment`, `EffectResult`, `estimate_effect` and the common-date path |
| `staggered.rs` | Waves, clean control pools, size-weighted aggregate, interval inversion |
| `permutation.rs` | The label-permutation test and the exposure-adjusted sharp null |
| `strata.rs` | Strata validation, `propose_strata`, proportional allocation |
| `switchback.rs` | `SwitchbackSchedule`, `propose_switchback`, the sign-flip test |
| `design.rs` | `ExperimentDesign` and `estimate` |
| `diagnostics.rs` | `PreTrend` and `SizeBias` |
| `power.rs`, `power_staggered.rs`, `power_switchback.rs` | `placebo_power` per design |
| `propose.rs` | `propose_waves` |
| `ratio.rs` | `estimate_ratio` and the Anderson–Rubin set |
| `calibration*.rs`, `testkit.rs` | Test-only: whole-experiment calibration and fixtures |

## Testing

All experiment tests are named `experiment_*`:

```bash
cargo nextest run --lib -E 'test(experiment)'                       # fast set
cargo nextest run --lib --run-ignored only -E 'test(experiment)'    # the two slow ones
```

Two tests are `#[ignore]`d for their debug-build runtime. `experiment_calibration_staggered_simulates_whole_experiments` takes about 330 s and `experiment_placebo_power_prices_staggering_in_the_same_league` about 255 s. CI's Tier 2 job runs them through `--include-ignored`.

Calibration tests run whole simulated experiments through the real estimator and check coverage and false-positive rates for all three designs. Stratified assignment is covered by unit and placebo tests. A stratified whole-experiment calibration is not yet included.
