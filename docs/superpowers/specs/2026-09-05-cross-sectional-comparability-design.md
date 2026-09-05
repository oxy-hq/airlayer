# Cross-sectional comparability

**Status:** BLOCKED — under revision after adversarial review
**Date:** 2026-09-05
**Repos touched:** `airlayer` (schema + engine), `oxy` (endpoints, DTOs, SDK)

> ## Read this first
>
> An adversarial review found nine design holes in this document. It is retained for its
> research — §3's extraction of the reference implementation's rules and §9's inventory of
> what is inexpressible are both verified and still good — but **the design in §5 and §7 does
> not work as written** and must not be implemented from.
>
> **Two defects moved out** into a separate, shippable spec,
> `2026-09-05-opportunity-benchmark-correctness-design.md`: metric polarity and the
> thin-segment benchmark floor. Neither needs cohorts, and both are live wrong answers in a
> shipped UI. `rate_denominator` also moved there in scope terms and is deferred until it can
> be specified correctly.
>
> **Two premises in this document are false and are being rewritten:**
>
> 1. §2.2 claims no live consumer. Verified false: 7 of 8 metric-tree endpoints are wired,
>    unflagged, with `/ide/world-model` as the default index route
>    (`o3/web-app/src/App.tsx:421,483`). Only `/distribution` has an unmounted panel.
> 2. §7.1's `Benchmark` is per-dimension. A cohort benchmark is **per-subject** — the
>    reference's cohort CTE is `GROUP BY a.restaurant_id`, one median per store. One
>    population and one scalar cannot represent N medians, and
>    `DimensionOpportunity.benchmark_filter` (one filter per dimension, whose own contract
>    makes an empty value mean "drill must not recurse") has no per-subject form.
>
> **Unresolved, and blocking:** `opportunity` builds `QueryRequest`s and calls
> `QueryExecutor = dyn Fn(&QueryRequest) -> ...` (`metric_tree_ops.rs:3501`); it never
> compiles SQL. §7.2 specifies a correlated self-join CTE that this architecture cannot
> express, while §8.1 implies a separate resolution query — two different architectures in one
> document. That question is under investigation and determines the shape of the rewrite.
>
> Other confirmed holes not yet fixed here: `per: day` divides by a constant and so
> re-introduces the tenure bug §3 rule 2 documents; §7.4's hierarchy direction is inverted
> (descendants are finer than the cohort's grain, not coarser) and has no defined behaviour
> for a dimension that resolves to no cohort entity, which §7.4 itself concedes is the
> majority path; the cohort self-join has no cardinality bound; the `require` join uses plain
> `=` where NULL needs `Dialect::null_safe_eq`; and a subject/peer asymmetry (a store excluded
> as both but still reported) has no representation in `__cohort_base`.

## 1. The gap

airlayer has **temporal** comparability. `Shift.comparable_by` (`src/schema/models.rs:740`)
plus entity `lifespan` (`models.rs:78`, `Lifespan` at `models.rs:44-56`) restricts a
period-over-period query to entities alive in both windows — same-store sales. The
vocabulary exists and works: `comparable_by`, `lifespan`, `maturity`.

There is no **cross-sectional** equivalent: nothing declares which instances may be
benchmarked against each other at one point in time. That absence produces wrong answers
today.

`pick_benchmark` (`src/engine/metric_tree_ops.rs:3046-3059`) takes the max over segments
below 8, or the p75 at 8 or more, over *every* segment of a dimension:

```rust
fn pick_benchmark(values: &[f64]) -> (f64, String) {
    if values.is_empty() { return (0.0, "empty".into()); }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if sorted.len() >= 8 {
        let idx = ((sorted.len() as f64) * 0.75).floor() as usize;
        let idx = idx.min(sorted.len() - 1);
        (sorted[idx], "p75".into())
    } else {
        (*sorted.last().unwrap(), "best_peer".into())
    }
}
```

Nothing here knows that some segments are not comparable. On a 23-restaurant workspace
this benchmarked Oregon (1 store) against California (21 stores). The `>= 8` threshold is
a bare literal at `:3052`; `p75` is hardcoded policy, not a caller's choice.

The same gap is reported from the consumer side. `internal-docs/world-model-opportunities.md:164-170`
(oxy repo):

> **The one real semantic-model gap: we cannot say "valid to group by, invalid to benchmark
> across."** `party_size` had to be deleted by hand — a 6-top spends more than a 2-top by
> arithmetic, not performance, and since upside is `gap × volume` it outranked the real
> signal by 52×. Deleting it also lost the ability to *group by* party size. The fix is a
> sibling flag to `segmentable`, and it must be separate: benchmarking across party size is
> invalid, but splitting an observed drop by it in `explain` is legitimate, and one flag
> serving both would silently break the second.

That doc's "three predicates that are not interchangeable" table (`:140-144`) wants a
fourth row. This spec adds it.

### Relationship to PR #110

oxy-hq/airlayer#110 (`474cb3a`, `3c0e542`) is adjacent and must not be re-litigated. It
fixed *how a target is sized*: refusing to size an additive total as if it were a ratio,
then classifying extensiveness by additive **term** rather than top-level operator. It
reported `total_upside = $3,363,216` on a measure whose actual value was `$1,865,406` —
a one-store-vs-twenty-one-store comparison surfaced as recoverable upside.

#110 stopped sizing a total as a ratio. It did **not** stop comparing a thin segment to a
thick one. This spec does that. The two are complementary and touch adjoining code in
`opportunity`; the implementation must leave #110's `is_extensive_composite` /
`supports_rate_basis` gating semantically unchanged.

## 2. Non-goals and the freedom boundary

### 2.1 Non-goals

Carried from the request, and binding on the design:

- **Thresholds do not move into the platform.** Band width, peer floor, severity tiers and
  all copy are judgements about restaurants and stay in the app. The platform owns *how* to
  form and query a cohort; the app owns *what* is comparable.
- **The Watchlist's SQL is not retired by this change.** Its guards (period completeness,
  posting spread, three cohort-validity checks) were each found by a user reading a wrong
  row. A v1 primitive will not carry them. Watchlist is the proving ground, not the first
  migration.
- **`useDistribution` is not a head start.** Its request is `{target, time_dimension, period}`
  with no segment or cohort (confirmed in three places: `crates/app/src/server/api/metric_tree.rs:1240`,
  `web-app/src/types/metricTree.ts:674-678`, `sdk/typescript/src/metricTree.ts:482-486`).
  It is left untouched.

### 2.2 What we are free to redesign, and what we are not

Nothing is consuming the world model or opportunity sizing yet. **Semantic query is the only
live surface.** That makes the benchmark machinery genuinely open to correction rather than
extension — but the freedom has a precise edge, and the edge is not "airlayer vs oxy".

**Free** — no live consumer, correct it outright:
- everything in `src/engine/metric_tree_ops.rs` (`opportunity`, `opportunity_drill`,
  `explain`, `pick_benchmark` and their result types);
- the oxy metric-tree endpoints and DTOs (`crates/app/src/server/api/metric_tree.rs`);
- all three hand-copied TS mirrors of those wire types.

**Not free** — semantic query depends on these:
- `.view.yml` **parsing**. A field can be added freely; renaming or removing one breaks
  existing schemas. This is why `segmentable` survives as a deprecated alias (§5.3) rather
  than being deleted.
- Pre-agg **rollup hashing**. Verified rather than assumed: `definition_fingerprint`
  (`src/engine/preagg.rs:94-152`) hashes view name, `source_sql`, each dimension's
  *name and expr*, and each measure's *name, type, expr and filters* — not a whole-struct
  serialization. None of the fields this spec adds participate, so **the rollup hash does
  not move** and no cached rollup is invalidated. Any future change that puts a new field
  into a dimension's `expr` or a measure's `type`/`filters` would break that and must
  re-check.

The practical consequence runs through the rest of this document: where the first draft
preserved existing benchmark behaviour for callers who don't opt in, it now **replaces** it.
Preserving the default was preserving the bug.

## 3. Reference implementation: what the rules actually are

Extracted from `customer-apps/apps/pokehouse/watchlist`. These are the acceptance criteria
for expressiveness — each is either expressible in the proposed shape or explicitly listed
in §9 as out of reach.

| Rule | Where | Detail |
|---|---|---|
| Size band measure | formula `peerCohortSql.ts:658,718,793,900`; rationale `:258-264` | **Average daily** net sales, `s_trailing_sales / nullIf(s_trailing_days, 0)` — not the trailing total. `salesCte` (`:266-290`) only builds the raw sums |
| Band window | call `peerCohortSql.ts:200`; helper `:219-223`; `thresholds.ts:198` | `[periodStart − 90d, periodEnd]`, anchored to the query period |
| Band shape | `peerCohortSql.ts:466-470` | Multiplicative, symmetric, **subject-centred**: two clauses, `b.trailing_sales >= a.trailing_sales * (1-pct)` and `<= a.trailing_sales * (1+pct)`, with `salesBandPct` parameterized (`thresholds.ts:132` = 0.35) |
| Exact-match key | `peerCohortSql.ts:509-519` | `INNER JOIN ... ON b.basis = a.basis`, applied before the size filter |
| Peer floor | `thresholds.ts:195`, `peerCohortSql.ts:554-563,604-608` | `minCohortSize = 3`, used as a **tier-selection predicate, deliberately not a gate** — "the CLIENT decides" |
| Ramp exclusion | `peerCohortSql.ts:311-319,661` | First-ever sale on/after period start ⇒ `drop_reason = 'opened_mid_period'`; excluded as subject *and* as peer |
| Statistic | `peerCohortSql.ts:500-517` | `quantileExactInclusive(0.5)` over peers, **excluding self** (`b.restaurant_id != a.restaurant_id`) |
| Gap pricing | `peerCohortSql.ts:598` | `(observed_pct − baseline_pct) / 100 × period_sales`, against the subject's **own** sales |
| Per-metric banding | `peerCohortSql.ts:162-164` | Only `wage_cost` and `give_away` are size-banded; `food_cost`, `void_rate`, `review_rating` deliberately are not |

Three properties of this are load-bearing and easy to get wrong:

1. **Asymmetry is deliberate and measured.** "It is also not reciprocal: A can be inside
   B's band while B is outside A's. Measured July 2026, 7 of 55 food pairs and 35 of 210
   labor pairs. ISS avoids this by centring the subject in its group rather than only
   filtering. Unfixed"
   (`thresholds.ts:127-130`). A cohort is therefore a correlated self-join per subject,
   **never** a bucketing or `NTILE` partition. Optimising it into buckets silently changes
   the answer.
2. **Banding on the total was a real bug.** Trailing totals conflate size with tenure — new
   stores' 90-day totals read as small stores'. Clovis went from 0 peers to 6 after the fix
   (`peerCohortSql.ts:258-264`, `STATE.md:1481-1487`). Hence a normaliser is part of the
   band declaration, not an afterthought.
3. **Comparability varies per measure, not per entity.** Size matters for labour and
   giveaway; it deliberately does not for food cost, justified by measured slope/R²
   (`peerCohortSql.ts:82-103`). This is the fact that rules out a single `comparable:` block
   on the entity and forces **named** cohorts.

## 4. Shape chosen

Three shapes were considered:

- **A — one `comparable:` block on the entity.** Cheapest, reads well beside `lifespan`.
  Rejected: one entity gets exactly one notion of comparability, which is already false in
  the proving case (rule 3 above).
- **B — named cohorts on the entity, selected at query time.** Chosen.
- **C — top-level `.cohort.yml` objects.** Most composable, but detaches the rule from the
  entity it describes and adds a file type and loader. Deferred; B does not preclude it.

## 5. Schema addition

### 5.1 `cohorts` on `Entity`

Declared beside `lifespan`, because comparability is intrinsic to the entity in the same
way its lifespan is — any view using the entity inherits it.

```yaml
entities:
  - name: restaurant_id
    type: primary
    key: restaurant_id
    lifespan: {start: opened_at, end: closed_at}
    cohorts:
      size_matched:
        band:
          measure: sales.net_sales
          per: day              # normaliser; omit for a raw total
          over: 90 days         # trailing window, anchored to the query period
          tolerance: 0.35       # multiplicative, subject-centred
        require: [restaurants.accounting_basis]   # exact-match keys
        min_peers: 3
      basis_only:
        require: [restaurants.accounting_basis]
        min_peers: 3
```

```rust
/// A named rule for which instances of this entity may be benchmarked against
/// each other at one point in time. The cross-sectional sibling of `lifespan`.
pub struct Cohort {
    /// Size-matching rule. `None` means membership is decided by `require` alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub band: Option<CohortBand>,
    /// Members that must match the subject exactly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub require: Vec<String>,
    /// Reported, never actioned. See §7.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_peers: Option<usize>,
    /// Exclude the subject from its own peer set. Defaults true.
    #[serde(default = "default_true")]
    pub exclude_self: bool,
}

pub struct CohortBand {
    pub measure: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per: Option<BandNormaliser>,   // Day | Week | Month
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub over: Option<String>,          // "<int> <unit>", parsed by shift::Interval
    pub tolerance: f64,
}

// on Entity, beside `lifespan`:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub cohorts: Option<BTreeMap<String, Cohort>>,
```

`BTreeMap`, not `HashMap`. Cohort iteration order reaches emitted SQL, and
`QueryRequest::referenced_views` (`src/engine/query.rs:75-79`) already documents why an
unstable order there is a correctness bug, not a cosmetic one: it "seeds `pick_base_view`'s
candidate scan and the join planner's target list, so an unstable one makes a cost tie
resolve differently run to run".

`over:` reuses `Interval::parse` (`src/engine/shift.rs:78`) verbatim — the same
`"<int> <unit>"` grammar as `by:`/`maturity:`, and the same calendar-aware `subtract_from`
(`shift.rs:177`). The trailing window is `[period_start − over, period_end]`, matching
`peerCohortSql.ts:219-223`. With `over:` omitted, the band measure is evaluated over the
query period itself.

### 5.2 `rate_denominator` on `Measure`

`opportunity`'s rate basis today is `value / row_count`, via `discover_count_measure`
(`metric_tree_ops.rs:5174`). None of the Watchlist's five metrics are that shape — they are
`cogs / net_sales`, `discounts / gross_sales`, `voided / total_rung`. Without a declarable
denominator the primitive cannot be proven against the case it was designed from.

```rust
// on Measure:
/// Measure to use as this measure's denominator when put on a per-unit basis.
/// Takes precedence over the discovered `type: count` measure.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub rate_denominator: Option<String>,
```

`supports_rate_basis` (`metric_tree_ops.rs:2081`) prefers it when present; absent it,
today's discovery path and #110's extensiveness gating are untouched.

This lands cleanly downstream: `OpportunityResponse` already carries a `rate_denominator`
field (`o3/crates/app/src/server/api/metric_tree.rs:386`) populated by `count_measure_id`
(`:406`). It begins carrying a measure id rather than only a count-measure id.

### 5.3 `analysis` on `Dimension`: one capability set, not a second boolean

The first draft added `benchmarkable` beside `segmentable`. Two booleans invite a third: the
next case that is groupable and benchmarkable but invalid to decompose has nowhere to go.
With the benchmark surface unused, this is the moment to say it once.

```yaml
dimensions:
  - name: party_size
    analysis:
      explain: true      # splitting an observed drop by it is legitimate
      benchmark: false   # a 6-top outspends a 2-top by arithmetic, not performance
```

```rust
/// What a dimension may be *used for* in analysis, beyond plain grouping.
///
/// Grouping is not represented: a dimension that cannot be grouped by is not a
/// dimension. These are the two analytical uses that are separately valid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DimensionAnalysis {
    /// May be used to decompose an observed change (`explain`). Default true.
    #[serde(default = "default_true")]
    pub explain: bool,
    /// May be *benchmarked across* — two of its segments held to the same
    /// standard (`opportunity`, `drill`). Default true.
    #[serde(default = "default_true")]
    pub benchmark: bool,
}

// on Dimension:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub analysis: Option<DimensionAnalysis>,
```

**Why these two axes and no others.** `segmentable` (`models.rs:186`) is documented at
`models.rs:170-186` as an **actionability** flag — "not a *lever*", for `address_line_2`,
`gender`, `total_amount`. Because it is applied inside `discover_dimensions` (§6), it today
suppresses a dimension in **both** `explain` and `opportunity`. `party_size` needs exactly
one of those two suppressed, which is the whole reason the world-model doc insists the flags
stay separate. Two independent booleans is therefore the true shape of the existing
behaviour, not a speculative generalisation.

**`segmentable` is kept as a deprecated alias.** It cannot be removed: `.view.yml` parsing is
outside the freedom boundary (§2.2), and there is a real producer —
`src/schema/foreign/cube.rs:342` maps Cube's `shown` onto it, so every converted Cube schema
carries it. Resolution rule, applied once at parse time:

- `segmentable: false` ⇒ `analysis: {explain: false, benchmark: false}` — its current meaning
  exactly, since `discover_dimensions` gates both paths today.
- `segmentable: true` / absent ⇒ no constraint.
- Both present and disagreeing ⇒ **validation error**, not a silent precedence rule.

`cube.rs:342` moves to producing `analysis` directly, preserving the documented intent at
`cube.rs:338-341` ("a dimension nobody is meant to see must not resurface as a suggested
lever"). `INIT_CLAUDE_MD` (`src/cli/mod.rs:6240`) documents `segmentable: false` and must be
updated in the same change, per the "Keeping init artifacts in sync" rule in `CLAUDE.md`;
so must any `.claude/skills/*/SKILL.md` mentioning it, since those are embedded at compile
time via `include_str!`.

### 5.4 `min_support`: a request-level floor, defaulting to 2

`min_support` is **not** a field on `Cohort`. It must apply whether or not a cohort is in
play (§7.4), so it cannot live inside one.

```rust
// on the opportunity/drill request:
/// Refuse to benchmark a segment whose own support falls below this.
/// Defaults to 2 — a single-instance segment has no defensible benchmark.
/// `Some(1)` explicitly opts back into the old unfloored behaviour.
#[serde(default = "default_min_support")]  // = 2
pub min_support: usize,
```

**The default is 2, not unset.** The first draft defaulted it off "preserving existing
behaviour" — but existing behaviour is the Oregon bug, and nobody is depending on it (§2.2).
Benchmarking a one-store region against a twenty-one-store region is not a judgement call
the platform should make silently; a segment of size 1 has no variance, no dispersion, and
nothing the t-test at `metric_tree_ops.rs:2419-2449` can evaluate. Refusing it is the honest
floor. An app that genuinely wants it passes `min_support: 1`.

An optional per-view default may be declared so a workspace can raise the floor once rather
than on every call; the request value wins when both are present. It lives on the view rather
than the entity because it is a property of the scan, not of an entity's comparability.

Validation: `min_support >= 1`.

### 5.5 Validation

Mirroring the `comparable_by` checks at `src/schema/validator.rs:293-311`, with the same
message style (name the view, the member, and what to do):

- `band.measure` resolves to a measure reachable from the entity's grain.
- Every `require` member resolves to a dimension reachable from the entity's grain.
- `tolerance` is finite and in `(0, 1)`.
- `min_peers >= 1`. (`min_support` is validated at the request level, §5.4.)
- `analysis` and a disagreeing `segmentable` on the same dimension is an error (§5.3).
- `over` parses as an `Interval`.
- Cohort names are unique within an entity (free via `BTreeMap`, but a duplicate key in
  YAML must be reported rather than silently last-wins).
- A cohort declared on a non-`Primary` entity is rejected, consistent with the existing
  `parent:` rule — a cohort needs a row identity to self-join on.

## 6. The dimension gate: wiring, not the flag

The flag is trivial; the wiring point is the whole risk, and it was verified rather than
assumed.

`is_segmentable` (`metric_tree_ops.rs:5191-5198`) is applied inside `discover_dimensions`
(`:5279`). `discover_dimensions` has **six production call sites**:

| Line | Enclosing function | Side |
|---|---|---|
| 2602 | `opportunity` (`:2475`) | benchmark |
| 3783 | `dimension_candidates` (`:3753`) | benchmark (drill-only) |
| 4189 | `opportunity_drill` (`:4071`) | benchmark |
| 4618 | `decompose_to_searchable` (`:4589`) | **explain** |
| 4627 | `decompose_to_searchable` (`:4589`) | **explain** |
| 4771 | `explain` (`:4717`) | **explain** |

`dimension_candidates` is reached only from `opportunity_drill` (`:4238`) in production.

So: **`analysis.benchmark` is applied at `:2602`, `:3783` and `:4189` only, never inside
`discover_dimensions` or `is_segmentable`.** Putting it in the shared helper would have
excluded `party_size` from `explain` too — precisely the silent breakage the world-model doc
predicted. This is the single most important implementation constraint in the spec.

A dimension excluded this way is reported through the existing
`SkippedDimension { dimension, reason }` (`:1586-1590`), so the caller learns *why*
`party_size` is absent rather than watching it vanish.

## 7. How it reaches benchmark selection

### 7.1 The benchmark is a population; the scalar is derived from it

The current code picks a scalar (`pick_benchmark`, `:3046-3059`) and then *reconstructs* a
filter from it at `:2829-2860` by re-scanning for `cmp >= benchmark`. That inversion is the
root of both defects: it only works because "p75" happens to be a threshold, and it leaves
the size-dependent policy buried where no caller can see or override it.

With no live consumer (§2.2), this is replaced rather than extended:

```rust
/// The population a segment is judged against, and the statistic taken over it.
pub struct Benchmark {
    /// The members of the comparison population. Always explicit — a cohort's
    /// membership is a per-subject peer list, not a threshold, so it can never
    /// be recovered by re-scanning for `cmp >= value`.
    pub population: Vec<String>,
    pub statistic: BenchmarkStatistic,   // Median | P75 | BestPeer
    pub value: f64,
    /// How the population was formed: a named cohort, or the full segment set.
    pub basis: BenchmarkBasis,
    /// Cohort size and whether it met `min_peers`. Reported, never actioned.
    pub sufficiency: Option<CohortSufficiency>,
}

fn select_benchmark(
    rows: &[SegRow],
    cohort: Option<&ResolvedCohort>,
    statistic: BenchmarkStatistic,
) -> Option<Benchmark>;
```

`benchmark_filter` is then built *from* `population`, not reverse-engineered back out of a
scalar. This is what the world-model doc already asks for — "the benchmark is a filter, not
a number" (`:28-31`) — and it keeps the drill's inherited-benchmark invariant (`:87-89`)
mechanical rather than incidental: the population is resolved once at the root and carried
down, because it is now a value rather than something each level re-derives.

**The `>= 8` heuristic is deleted, not defaulted.** `statistic` becomes a required argument.
The rule at `:3052` is a bare literal with no named constant, no test asserting the boundary
as intentional, and no justification anywhere in the codebase — it silently switches from
"the single best segment" to "an interpolated percentile" as a dimension's cardinality
crosses eight, which changes what the number *means* mid-scan. Keeping it as the no-opt-in
default would preserve exactly the hardcoded policy this work exists to remove. Callers
choose; the CLI's default is `median`, matching the Watchlist (`peerCohortSql.ts:500`) and
the reasoning at `:439-441` that the mean is computed but "NEVER used to judge".

`SegRow` (`metric_tree_ops.rs:2757+`, fields `segment, value, count, cmp, sd, filtered_n`)
gains no fields; the cohort is passed alongside.

### 7.2 Cohort resolution and SQL

A resolved cohort compiles to one **correlated self-join** at the entity's grain, emitted as
a CTE alongside the existing `__lifespan_<entity>` machinery (`build_lifespan_cte_sql`,
`src/engine/sql_generator.rs:4428-4520`; spliced into the WITH clause at `:4962-4979`):

```sql
__cohort_<entity>_<name> AS (
  SELECT a.<key> AS subject, b.<key> AS peer
  FROM   __cohort_base a
  JOIN   __cohort_base b
    ON   b.<require_i> = a.<require_i>            -- for each require member
   AND   b.<key> <> a.<key>                       -- when exclude_self
   AND   b.band_value BETWEEN a.band_value * (1 - tol)
                          AND a.band_value * (1 + tol)   -- when band is set
)
```

`__cohort_base` carries the entity keys, the `require` members, and the band value
(normalised per `per:`, over the `over:` window). Subject-centred by construction: the
bounds are computed from `a`, so membership is asymmetric, as §3 requires.

Percentile flavour is pinned per dialect (R-7 / `quantileExactInclusive`) rather than left
to the warehouse default. The difference is not academic: plain `quantileExact` returns the
upper of the two middle values on even-sized sets and cost $933 of understatement on a real
case (`peerCohortSql.ts:426-441`).

### 7.3 `min_peers` reports, never falls back

`ResolvedCohort` carries `peer_count` and `sufficient: bool`. The result is emitted either
way; the platform never silently substitutes a wider cohort.

This is deliberate and is the reason cohorts are **named**. The Watchlist's ladder
(±35% → ±70% for labour → whole-basis for food cost → unjudged) is judgement about
restaurants. An app that wants tier 2 makes a second call naming `basis_only`. Encoding the
ladder in the schema would import exactly the policy that §2 excludes, and the ladder is not
uniform across metrics anyway (`wholeBasisFallback: kind !== "wage_cost"`,
`peerCohortSql.ts:205`).

### 7.4 `min_support`: the part that actually fixes Oregon

**A store-level cohort does not fix the motivating example.** Oregon-vs-California is a
benchmark across a *region* dimension; a cohort declared on `restaurant_id` is at store
grain and never applies to it. Cohorts fix "who are store X's peers" — the Watchlist's
problem, not the one that produced the Oregon row.

Two mechanisms, both in v1:

1. **A cohort applies when the scanned dimension resolves to the cohort's entity grain**, or
   a descendant of it via the existing promotion/hierarchy machinery. Fixing Oregon
   *properly* means declaring a cohort on a `region` entity, banded on store count.
2. **`min_support`: a floor on a segment's own support**, applied whether or not a cohort is
   in play. A segment whose underlying entity/row count falls below it is not benchmarked
   and is reported as skipped. This is the general form of `min_peers`, and it is what
   catches a one-store Oregon in a schema that has no region entity.

Without (2), "we shipped cross-sectional comparability" and "Oregon is still benchmarked
against California" would both be true. `min_support` is a request-level parameter (§5.4),
not a cohort field, and defaults to 2 — the Oregon row stops being produced whether or not
anyone declares a cohort.

## 8. Query and endpoint surface

### 8.1 airlayer

`opportunity` (`metric_tree_ops.rs:2475-2483`) and `opportunity_drill` (`:4071`) gain
`cohort: Option<&str>`, a required `statistic: BenchmarkStatistic`, and `min_support`. A new public
`resolve_cohort` returns the peer set for a subject without sizing an opportunity.

CLI: `opportunity --cohort <name> --statistic median|p75|best_peer`, and a new
`cohort <entity> --name <cohort> --period start:end` subcommand.

`inspect --json` surfaces cohorts on each entity, and in the `ontology` block
(`src/cli/mod.rs:1707`, `build_ontology_json`) as a comparability relation beside the
existing containment and categorical promotion edges — a cohort is a *symmetric-intent,
asymmetric-in-fact* relation over one entity's instances, which is a new edge kind, not an
existing one.

No WASM/FFI work. `src/wasm.rs` and `src/ffi.rs` expose only `compile`, `validate`,
`catalog_list`, the `cache_*` family and `compile_foreign`; no metric-tree op crosses that
boundary, and `catalog::catalog()` does not carry `lifespan`/`shift` today either.

### 8.2 oxy

- `OpportunityRequest` (`crates/app/src/server/api/metric_tree.rs:339`) and `DrillRequest`
  (`:1040`) gain `cohort?` and `statistic?`. `DrillRequest` inherits them downward, per the
  inherited-benchmark invariant.
- New `POST /semantic/metric-tree/cohort` returning the resolved peer set, its size,
  sufficiency, and the benchmark value — so an app can ask "who are store X's peers?"
  without sizing an opportunity.
- `DistributionRequest` is left alone.

### 8.3 The hand-copy problem

Metric-tree wire types are hand-maintained in **three** places with nothing enforcing
agreement — airlayer's Rust structs (canonical), `web-app/src/types/metricTree.ts`, and
`sdk/typescript/src/metricTree.ts` — plus the oxy DTOs that `#[serde(flatten)]` over them.
There is no codegen: no `ts-rs`/`specta`, and `schemars` is present but not wired to a
TS-emitting step.

They are **already out of sync**: the server and web-app `OpportunityRequest` carry
`instance?: OpportunityInstance`; the SDK's (`sdk/typescript/src/metricTree.ts:454-458`)
does not. The `DriverForm` comment (`metricTree.ts:14-20`) records the failure mode: "the
last time one fell behind — `oxy-semantic`, five variants short — a valid `.view.yml`
stopped parsing."

Therefore: the new fields land in all three copies plus the oxy DTO **in one commit**, and
the plan adds a shape-assertion test on the SDK types. Fixing the pre-existing `instance`
drift is out of scope and noted separately.

## 9. What this shape cannot express

The design's actual test. Each of these is a real Watchlist rule that a v1 primitive does
not carry, stated so nobody discovers it later:

**Deliberately out (policy, per §2):**
- The tier ladder. Two named cohorts and a client decision, not a schema fallback list.
- All threshold values (`thresholds.ts:85-503`) — hand-tuned constants justified by
  narrative analysis of specific incidents, and interactively overridable per session
  without persisting (`thresholds.ts:8-10,541-560`).
- Flag gates, severity tiers, ranking and all copy (`peerCohort.ts:272-323`).

**Genuinely not expressible, and not attempted:**
- **Period-completeness guards.** `postingSpreadSql` (`peerCohortSql.ts:1754-1788`, with
  `maxPostingSpreadDays = 7` at `thresholds.ts:479`) and untagged-share (`:1814-1828`, with
  `maxUntaggedCostPct = 10` at `thresholds.ts:501`).
  These are properties of *data arrival*, not of the semantic model. Without them a month
  where one bookkeeper posted late reads as "+87% vs peers"; a uniformly-incomplete month
  passes the spread test with `spread_days = 0` and produced "$46,610 of overspend that does
  not exist" (`:1796-1801`).
- **The basis literal.** `if(m.map_company = 'pokehouse', 'direct_only', 'full')`
  (`:650`) — a company-name branch baked into SQL. Expressible only if the app first
  models it as a dimension.
- **Bridge tables.** `LOCATION_MAP`, `MOMOS_LOCATION_MAP` (`:56-72`), hand-curated with a
  `confidence` column and documented name mismatches.
- **Ramp exclusion, partially.** "First-ever sale on/after period start" is lifespan-shaped
  and *is* expressible once a derived `lifespan` (`from:` + `MIN(sale_date)`) is declared —
  the existing `__lifespan_<entity>` CTE already computes exactly this. But
  `minDaysWithSales = 20` and `minPlausibleWage = 13` are data-quality predicates, not
  comparability, and stay app-side as filters.
- **Per-dataset plausibility patches** (`maxPlausibleQuantity`, `maxPlausibleLineUsd`,
  `processing_state` rules) — forensic constants, not primitive behaviour.

**Known asymmetry, accepted:** cohort membership is non-reciprocal by design. This departs
from industry practice (ISS centres the subject in its own group) and the Watchlist
documents it as acknowledged and unfixed. The platform reproduces the Watchlist's behaviour
rather than silently "correcting" it; `exclude_self` and the correlated join make it
explicit rather than accidental.

**Stale source note:** `STATE.md:705` says the wage guard is "under $12"; the code
(`thresholds.ts:227`) and `STATE.md:1497-1504` say $13. STATE.md's "whole rule" paragraph is
stale — do not treat it as sole source of truth when checking rules.

## 10. Effect on existing callers

**There are none on the surface being changed.** Nothing consumes the world model or
opportunity sizing; semantic query is the only live surface (§2.2). This section therefore
states what *must not* move rather than what is preserved.

### 10.1 The criterion is inverted

The first draft's success measure — "every existing `opportunity` / `drill` / `explain` test
passes unmodified" — is wrong here. Those tests encode the behaviour being corrected: the
`>= 8` switch, the unfloored benchmark, `segmentable` suppressing `explain` and `opportunity`
together. A change that leaves them all green has not fixed anything.

Replacement criterion:

- **Semantic-query tests pass unmodified.** Non-negotiable — that surface is live. This
  includes SQL-generation, join-planning, promotion and pre-agg tests.
- **Pre-agg rollup hashes are unchanged.** Asserted directly, not inferred: a test computes
  `definition_fingerprint` for a fixture view before and after the new fields are added and
  requires equality. §2.2 establishes why this should hold; the test is what keeps it true.
- **Opportunity/drill tests are expected to change**, and every changed assertion carries a
  one-line note saying which corrected behaviour moved it. An assertion that changes for a
  reason nobody can name is a regression wearing a green tick.
- **`explain` behaviour changes in exactly one way:** a dimension previously suppressed by
  `segmentable: false` is still suppressed (via the alias, §5.3); one carrying
  `analysis: {explain: true, benchmark: false}` is newly visible to `explain`. Both are
  asserted.

### 10.2 The version bump

One line — `o3/Cargo.toml:591`, currently `rev = "b141d8d..."` (airlayer v0.4.0) — plus a
dated changelog entry, per the convention stated at `:286-289` and restated at
`:584-589` immediately before the pin.
`oxy-airlayer-compat` is the enforced sole dependent
(`crates/infrastructure/semantic/src/lib.rs:15-21`, CI test `this_is_the_sole_airlayer_dependent`
at `:701`), so any breakage localises there.

The changelog entry should say plainly that benchmark selection changed shape and that the
`>= 8` heuristic is gone, so a future reader doesn't diagnose it as drift.

## 11. Phasing

The first draft phased for safe incremental rollout to live consumers. There are none, so
phasing now serves reviewability instead — and the split that matters is *live surface vs
free surface*, not additive vs breaking.

1. **Schema + validator + inspect** (touches the live surface). `cohorts`, `analysis` with
   the `segmentable` alias, `rate_denominator`, the per-view `min_support` default; the
   `cube.rs:342` mapping; `INIT_CLAUDE_MD` and skill files. Includes the
   fingerprint-unchanged test (§10.1). No engine behaviour change. This phase must be
   reviewed on its own because it is the only one that can affect semantic query.
2. **Benchmark redesign** (free surface). `Benchmark` as a population, `select_benchmark`,
   deletion of the `>= 8` heuristic, `min_support` with its default of 2, and the
   `analysis.benchmark` gate at `:2602` / `:3783` / `:4189`. Lands as one coherent change —
   splitting it would leave the tree in a state where the old policy is half-removed.
3. **Cohort resolution + SQL.** The self-join CTE, `ResolvedCohort`, cohort-aware
   `select_benchmark`.
4. **`rate_denominator` in sizing.** Extends `supports_rate_basis` without disturbing #110's
   extensiveness gating.
5. **oxy surface.** Endpoint fields, the new `/cohort` route, all three TS copies in one
   commit, the SDK shape test, and the rev bump with its changelog entry.

## 12. Testing

- **Unit, schema:** parse/round-trip each new field; every validator rejection with its
  message; `BTreeMap` ordering stability.
- **Unit, selection:** `select_benchmark` for each statistic over a known population;
  `benchmark_filter` is built from `population` and round-trips (filter selects exactly the
  population); the empty-population case returns `None` rather than a zero.
- **Unit, no silent policy:** a test asserting that cardinality does **not** change which
  statistic is used — the direct inverse of the deleted `>= 8` rule, so it cannot creep back.
- **Unit, alias resolution:** `segmentable: false` resolves to both capabilities off;
  `analysis` disagreeing with `segmentable` is a validation error; Cube `shown: false`
  converts to the same suppression it produces today.
- **Unit, gating:** the `explain`-must-still-see-it test (phase 2), stated as the property it
  protects.
- **Unit, asymmetry:** a fixture where A is in B's cohort and B is not in A's, asserting both
  directions — this is the property most likely to be "optimised" away later.
- **Integration (DuckDB, tier 1):** a store fixture reproducing size band + exact-match
  requirement + median-excluding-self, checked against hand-computed expected values.
- **Regression:** the Oregon shape (1 store vs 21) sized under `min_support` and refused.
- **Fingerprint stability:** `definition_fingerprint` for a fixture view is byte-identical
  before and after the new fields (§10.1) — the test that keeps the rollup-hash claim true.
- **Semantic query:** the full SQL-generation, join, promotion and pre-agg suites, unmodified.
- **Opportunity/drill:** expected to change; each changed assertion annotated with the
  corrected behaviour that moved it.

## 13. Open questions for implementation

- Does the band's `over:` window interact correctly with a query whose period is shorter than
  the window? (Watchlist's window ends at `periodEnd`, so it always overlaps; confirm.)
- Should `min_support` count rows or distinct entities at the segment's grain? Rows is
  cheaper; entities is what Oregon actually means. Leaning entities where a grain is
  resolvable, rows otherwise, reported either way.
- Does a cohort compose with `Shift.comparable_by` on the same entity — a same-store
  comparison *within* a size-matched peer group? The primitives are orthogonal by
  construction, but the CTE interaction is unverified.
