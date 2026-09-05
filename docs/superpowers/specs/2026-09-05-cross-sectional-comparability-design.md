# Cross-sectional comparability: peer cohorts

**Status:** design, awaiting review — architecture resolved
**Date:** 2026-09-05
**Scope:** `airlayer` (schema + `metric_tree_ops` + CLI), `oxy` (endpoint, SDK)
**Companion:** `2026-09-05-opportunity-benchmark-correctness-design.md` fixes two live defects
in `opportunity` (metric polarity, the thin-segment benchmark floor) that need none of this
machinery. That spec ships first and independently.

## 1. The gap

airlayer has **temporal** comparability: `Shift.comparable_by` (`src/schema/models.rs:740`)
plus entity `lifespan` (`models.rs:78`, `Lifespan` at `:44-56`) restricts a period-over-period
query to entities alive in both windows — same-store sales.

There is no **cross-sectional** equivalent: nothing declares which instances may be
benchmarked against each other at one point in time. The proving case is a restaurant peer
cohort currently hand-rolled in ~1800 lines of generated SQL, which the platform should be
able to express.

`internal-docs/world-model-opportunities.md:164-170` (oxy) reports the same absence from the
consumer side, naming `party_size` as the canonical case. That half — "valid to group by,
invalid to benchmark across" — is handled by the companion spec's `analysis` capability set,
not here.

## 2. The decision that shapes everything: a cohort is a sibling result, not a benchmark

The first draft of this spec tried to make a peer cohort *be* `opportunity`'s benchmark. That
does not work, and understanding why is the whole design.

**A cohort benchmark is per-subject.** The reference's cohort CTE is `GROUP BY a.restaurant_id`
— one median per store, over that store's own peers. Two near-identical stores legitimately
get two different baselines.

**Every result type in `opportunity` is per-dimension.** `pick_benchmark` returns one scalar
(`metric_tree_ops.rs:3046-3059`). `DimensionOpportunity.benchmark_basis` (`:1545`) is one
string. `DimensionOpportunity.benchmark_filter` (`:1581`) is one queryable population, and
`opportunity_drill` hard-requires it — `if top_dim.benchmark_filter.is_empty() { return
Ok(None) }` (`:4127`) — using it as one of two fixed populations "chosen once at the root and
never narrowed". The significance gate reads `bench_sd`/`bench_n` off a single `bench_row`
(`:2817-2827`).

N per-subject medians cannot be poured into that shape. Forcing it means making
`benchmark_basis` and `benchmark_filter` optional, refusing drill in cohort mode, re-deriving
dispersion over peer sets rather than segments, and auditing every consumer that assumes
segments in a dimension share a benchmark — a fortnight inside a 19k-line file with ~86
tests, for no gain over the alternative.

**The alternative: emit `PeerCohortResult` alongside `OpportunityResult.dimensions`.** The
cohort answers a different question ("how does each store compare to stores like it?") than
the dimension scan ("which segment is furthest below the bar?"). Keeping them as separate
results is not a compromise — it is the honest modelling. It also dissolves the grain-mismatch
problem entirely: there is no need to fuse an entity-grain cohort with a segment-grain
breakdown, because they are never combined.

Consequences, stated up front:

- **`opportunity`'s benchmark selection is untouched by this spec.** No change to
  `pick_benchmark`, `benchmark_filter`, or the drill invariant.
- **Cohort results are not drillable in v1.** Drill recurses on a fixed benchmark population;
  a per-subject cohort has none. This is a stated product boundary, not an accident.
- **`--cohort` is not a modifier on `opportunity`.** It is its own operation.

## 3. Reference implementation: what the rules actually are

Extracted from `customer-apps/apps/pokehouse/watchlist`. These are the acceptance criteria
for expressiveness — each is either expressible in the design below or explicitly listed in
§9 as out of reach. Every citation in this table was independently verified by two reviewers.

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

## 4. Schema

Named cohorts, on the entity, beside `lifespan` — the same place and for the same reason the
codebase already puts `lifespan` and `parent:` there (`models.rs:85-89`): "who are my peers"
is intrinsic to the entity, so any view using it inherits the rule.

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
          per: sales.trading_days     # a MEASURE, not a calendar unit — see §4.1
          tolerance: 0.35             # multiplicative, subject-centred
        require: [restaurants.accounting_basis]
        min_peers: 3
      basis_only:
        require: [restaurants.accounting_basis]
        min_peers: 3
```

**Named, not a single block**, because comparability varies per *measure*, not per entity: in
the reference only wage cost and giveaway are size-banded; food cost, voids and review rating
deliberately are not, justified by measured slope/R² (§3, rule 3). One `comparable:` block on
the entity cannot say that.

```rust
pub struct Cohort {
    #[serde(default, skip_serializing_if = "Option::is_none")] pub band: Option<CohortBand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]   pub require: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub min_peers: Option<usize>,
    #[serde(default = "default_true")]                          pub exclude_self: bool,
}
pub struct CohortBand {
    pub measure: String,
    /// Divisor measure. The band compares `measure / per`, never a raw total.
    #[serde(default, skip_serializing_if = "Option::is_none")] pub per: Option<String>,
    pub tolerance: f64,
}
// on Entity, beside `lifespan`:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub cohorts: Option<BTreeMap<String, Cohort>>,
```

`BTreeMap` for determinism, consistent with the reasoning at `src/engine/query.rs:75-79`.

### 4.1 `per:` is a measure, not a calendar unit

The first draft specified `per: Day | Week | Month`. That is broken: dividing a 90-day
window's total by the constant 90 orders entities identically to the raw total, so the band is
mathematically unchanged — and banding on the total is precisely the bug §3 rule 2 records
(new stores' totals read as small stores'; Clovis went from 0 peers to 6 only after the fix).

The reference's divisor is `countIf(d BETWEEN ts AND pe)` — **days on which that store has
data**, a per-entity quantity. So `per:` names a measure. `per: sales.trading_days` divides by
each entity's own trading-day count; omitting `per:` bands on the raw measure and is the
caller's explicit choice.

There is no `over:` window field. The cohort is resolved for the period the caller asks about;
a trailing window is expressed by asking for the trailing period.

### 4.2 Validation

- `band.measure`, `band.per` and every `require` member resolve, and are reachable from the
  entity's grain.
- `tolerance` is finite and `> 0`. No upper bound — `tolerance: 1.0` ("up to 2×") is
  legitimate; the first draft's `(0,1)` was arbitrary.
- `min_peers >= 1`.
- Cohorts are declared only on `type: primary` entities, consistent with the existing `parent:`
  rule (`validator.rs:315-362`) — a cohort needs a row identity.
- **The entity's key must have a backing dimension.** `identifier_dimensions`
  (`metric_tree_ops.rs:5217`) resolves a key to a dimension by name, falling back to matching
  `expr`. If neither resolves, the entity-grain pull in §5 cannot be expressed, and the
  validator says so with an actionable message rather than failing at query time.

## 5. Resolution: one query, medians in Rust

`opportunity` and `opportunity_drill` do not compile SQL. They build `QueryRequest`s and call
`pub type QueryExecutor = dyn Fn(&QueryRequest) -> ...` (`metric_tree_ops.rs:3501`). A
correlated self-join with a band inequality is not expressible as a `QueryRequest`, and the
existing shift/lifespan CTE machinery does not help: `build_cohort_context`
(`sql_generator.rs:4330`) emits a *global* set-membership predicate from two date literals,
with no per-subject grouping anywhere, and `__shift_aligned` self-joins on **equality** of
shifted keys (`:3894-3897`), never on an inequality band. `build_lifespan_cte_sql` (`:4425`) is
a useful *template* for a grouped CTE, not reusable machinery.

So: **resolve in Rust over one entity-grain pull.**

```rust
pub fn resolve_cohort(
    layer: &SemanticLayer, entity: &str, cohort: &str,
    period: (&str, &str), executor: &QueryExecutor,
) -> Result<PeerCohortResult, EngineError>;
```

One `QueryRequest`: `dimensions: [entity_key, ...require]`, `measures: [target, band.measure,
band.per]`, filtered to the period. Then per subject: filter to the same `require` tuple, apply
the band against **that subject's own** value, exclude self, take the R-7 median.

**Query cost: 2 + N**, against today's 1 + N (one overall at `:2557`, N breakdowns fired
concurrently through `parallel_execute` at `:4347`). One extra query, one extra wave.

At 10²–10⁴ entities the naive O(N²) peer loop is free. Above that it needs a guard — see §6.

### 5.1 The truncation trap

`compile_query` fills `limit: None` with `DEFAULT_QUERY_LIMIT = 10_000`
(`src/engine/mod.rs:28`, `:180`). An entity-grain pull that silently truncates at 10k yields
**wrong medians with no error** — the single most likely way to ship a quiet correctness bug
here.

`resolve_cohort` therefore sets `limit: Some(UNBOUNDED_QUERY_LIMIT)` (`mod.rs:39`) explicitly
**and** cross-checks the returned row count against a separate `COUNT(DISTINCT entity_key)`,
refusing rather than computing a median over a truncated universe. Both halves: the explicit
limit prevents the common case, the assertion catches a warehouse-side cap we don't control.

### 5.2 R-7 in Rust, which removes a portability problem

The reference uses ClickHouse's `quantileExactInclusive(0.5)` (R-7). Pushing that into SQL
would not survive the dialect matrix: `PERCENTILE_CONT(0.5) WITHIN GROUP` on Postgres and
Snowflake, window-only `PERCENTILE_CONT` on BigQuery, `median()`/`quantile_cont` on DuckDB, and
nothing usable on MySQL or SQLite — with no agreement on interpolation for even-sized peer
sets. The reference itself documents that plain `quantileExact` (upper-of-two-middle) understated
a real gap by $933.

Computing R-7 in Rust gives one definition on every warehouse. This is a genuine advantage of
the chosen architecture, not a consolation.

`statistic` is the caller's choice — `median` (default) | `p75` | `best_peer` — never inferred
from cohort size. Note the reference's own caution: its ±35%/3-peer design is defensible
*because* it shows a dollar gap against the median of a named, listed group, and "showing a
rank or a percentile would break that defence". P75 over a 3-peer cohort is available and is
the thing the reference says not to do.

## 6. Refusals are reported, never silent

Three refusal channels, all reported per subject rather than filtered away in a `WHERE`:

- **`min_peers` not met** — the subject is returned with its peer count and
  `sufficient: false`. The platform never widens the band or substitutes a different cohort;
  the reference's tier ladder (±35% → ±70% → whole-basis → unjudged) is judgement about
  restaurants and stays in the app, expressed as a second call naming a second cohort.
- **Excluded as subject and as peer** — the reference's `drop_reason`. A store may be returned
  in the result while contributing to nobody's median. The Rust loop makes this trivially
  expressible where a `WHERE`-clause design could not.
- **NULL `require` value** — an entity whose exact-match attribute is NULL joins nothing. It is
  excluded and **reported**, not silently vanished. (In a SQL design this would have needed
  `Dialect::null_safe_eq`; in Rust it is an explicit branch.)

```rust
pub struct PeerCohortResult {
    pub entity: String, pub cohort: String, pub statistic: BenchmarkStatistic,
    pub subjects: Vec<CohortSubject>,
    pub excluded: Vec<ExcludedSubject>,   // { key, reason }
}
pub struct CohortSubject {
    pub key: String, pub value: f64, pub baseline: f64, pub gap: f64,
    pub peers: Vec<String>, pub peer_count: usize, pub sufficient: bool,
}
```

The reference moved its guards out of the `WHERE` into a reported column precisely because a
store "simply did not appear in the labor list and no screen said why". Reproducing a failure
the reference already fixed would be careless.

**Cardinality guard.** `opportunity` caps dimension cardinality at 25
(`MAX_DIMENSION_CARDINALITY`, `:1723`) because scans cost money. A cohort self-join is O(N²)
in entity count: 23 restaurants is 529 pairs, 2M customers is 4×10¹². `resolve_cohort` refuses
above a configurable entity ceiling with the count in the message, and the validator warns when
a cohort is declared on an entity whose grain is plainly unbounded.

## 7. Gap pricing

`gap = value − baseline` in the units of the measure. When the caller wants the reference's
dollar pricing — `(observed_pct − baseline_pct) × period_sales` — that is `gap × the
denominator's value for that subject`, which the caller has, because `PeerCohortResult` returns
per-subject values rather than a dimension-level aggregate.

Note this is *not* `gap × row_count`. `opportunity`'s `upside` multiplies a rate deficit by a
row count, which is only coherent because `cmp` and `upside` share the same discovered count
measure. A cohort on a ratio has a different denominator, and conflating them yields a number
in no unit at all. This is one of the three defects that kept `rate_denominator` out of the
companion spec, and it is why cohort pricing is expressed per subject rather than summed into a
`total_upside`.

## 8. Surface

**CLI:** `airlayer cohort <entity> --name <cohort> --period start:end [--statistic median]`.
`inspect --json` surfaces cohorts per entity and in the `ontology` block
(`cli/mod.rs:1707`) as a comparability relation — a symmetric-intent, asymmetric-in-fact
relation over one entity's instances, which is a new edge kind beside the existing containment
and categorical promotions.

**Rust:** `resolve_cohort` is public. `augment_layer_for_peer_cohort` follows the established
precedent — `run_opportunity` (`cli/mod.rs:2684`) already clones the layer, augments it, and
builds the engine from the augmented copy so the executor resolves the synthetic measures;
`dimension_candidates` (`:3894-3963`) does the same at *runtime* under a write guard, with a
test (`:17077`) proving install-before-execute visibility. Nothing new is invented.

**oxy:** a new `POST /semantic/metric-tree/cohort`. `OpportunityRequest` and `DrillRequest` are
**not** changed — the cohort is a separate operation, so neither the live World Model
components nor their tests are touched by this spec. Wire types land in all three
hand-maintained TS copies in one commit with a shape-assertion test, since the SDK is already
missing `instance` and has no codegen to catch drift.

**Pre-agg:** unaffected. `definition_fingerprint` (`src/engine/preagg.rs:94-152`) hashes view
name, `source_sql`, dimension name+expr, measure name/type/expr/filters; `cohorts` enters none
of them. Asserted by test.

## 9. What this design cannot express

The design's actual test. Each of these is a real Watchlist rule that a v1 primitive does
not carry, stated so nobody discovers it later:

**Deliberately out (policy):**
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
rather than silently "correcting" it; the per-subject band in §5 makes it explicit rather
than accidental.

**Structural limits, and what the architecture resolved.** An earlier draft listed only
*policy* exclusions here, which understated the case. Four structural rules were genuinely
inexpressible in that draft; §2's sibling-result decision resolves three:

| Rule | Status |
|---|---|
| Per-subject baselines (each store its own median) | **Resolved** — `PeerCohortResult.subjects` is per-subject by construction (§5) |
| Subject-vs-peer asymmetry (excluded from both, still reported) | **Resolved** — the Rust loop reports it (§6); a `WHERE`-clause design could not |
| Gap priced against the subject's own denominator | **Resolved** — per-subject values are returned, so the caller multiplies (§7) |
| Metric polarity (all five reference metrics are lower-is-better) | **Not here.** `opportunity` hardcodes higher-is-better in three places; fixed in the companion spec, and a prerequisite for this one to produce correct signs |

**Stale source note:** `STATE.md:705` says the wage guard is "under $12"; the code
(`thresholds.ts:227`) and `STATE.md:1497-1504` say $13. STATE.md's "whole rule" paragraph is
stale — do not treat it as sole source of truth when checking rules.

## 10. Testing

- **Asymmetry:** a fixture where A is in B's cohort and B is not in A's, asserting both
  directions. This is the property most likely to be "optimised" into a bucketing later.
- **Band normalisation:** two entities with equal totals but different trading-day counts land
  in different bands — the test that would have caught the `per: Day` mistake.
- **R-7:** even-sized peer sets against hand-computed values, including the case where
  upper-of-two-middle differs from interpolated.
- **Truncation:** a fixture exceeding `DEFAULT_QUERY_LIMIT` refuses rather than medianing a
  truncated universe (§5.1).
- **Refusal reporting:** `min_peers` unmet, NULL `require`, and excluded-as-peer each appear in
  the result with a reason; none silently vanish.
- **Integration (DuckDB, tier 1):** size band + exact-match requirement + median-excluding-self
  against hand-computed expected values.
- **Untouched:** the full `opportunity`/`drill` suite passes unmodified — this spec changes
  neither. Semantic query and the fingerprint likewise.

## 11. Phasing

1. **Schema + validator + inspect.** `cohorts`, `CohortBand`, the key-has-a-dimension check,
   ontology surfacing. No behaviour.
2. **`resolve_cohort`.** The entity-grain pull, the truncation guard, the peer loop, R-7,
   `PeerCohortResult` with its refusal channels.
3. **CLI `cohort` subcommand.**
4. **oxy endpoint + SDK.**

## 12. Open questions

- What is the entity-count ceiling in §6, and is it configurable per call or fixed?
- Should a `Measure` carry a `default_cohort:`? The reference binds cohort↔metric statically
  (`metricIsSizeBanded(kind)`) and its comment says why: when the UI and the query disagree
  about which group was used, the screen claims "stores its size" while the query compared
  everyone — "the one class of bug a reader can neither see nor check." A query-time-only
  `--cohort` re-opens exactly that.
- Does a cohort compose with `Shift.comparable_by` on the same entity — a same-store comparison
  *within* a size-matched peer group? The primitives are orthogonal by construction; the
  interaction is unverified.
