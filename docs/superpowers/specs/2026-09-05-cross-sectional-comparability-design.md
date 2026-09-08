# Cross-sectional comparability: peer cohorts

**Status:** approved; verified against main @ dc3f209 (2026-09-07). Phases 1-3 in scope.
**Date:** 2026-09-05
**Scope:** `airlayer` (schema + `metric_tree_ops` + CLI), `oxy` (endpoint, SDK)
**Companion:** the opportunity-benchmark-correctness spec fixed two live defects in
`opportunity` (metric polarity, the thin-segment benchmark floor) that need none of this
machinery. **It has shipped** — PR #112, merged 2026-09-07 as `dc3f209`. The polarity
prerequisite this design depends on is therefore satisfied, not pending.

**Naming note:** `pick_benchmark` was renamed `select_benchmark` in #112. All references
below use the old name; read them as `select_benchmark`.

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
| Band window | call `peerCohortSql.ts:200`; helper `:219-223`; sum `:278-279`; `thresholds.ts:198` | `[periodStart − 90d, periodEnd]` — anchored at the period **start**, so it is a lookback *extension* of the period and its length is `90 + periodLength`, not 90 |
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
    /// Lookback window the band is measured over, anchored at the period
    /// START — independent of the window the compared measure runs over. See §4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")] pub window: Option<String>,
}
// on Entity, beside `lifespan`:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub cohorts: Option<BTreeMap<String, Cohort>>,
```

`BTreeMap` for determinism, consistent with the reasoning at `src/engine/query.rs:75-79`.

**Decided (was §12 Q2): a measure may name its default cohort, and the caller may override.**

```rust
// on Measure, beside `direction`:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub default_cohort: Option<String>,   // "entity.cohort_name"
```

The reference binds cohort↔metric statically (`metricIsSizeBanded(kind)`) because a
query-time-only choice creates the one bug class a reader can neither see nor check: the
screen claims "stores its size" while the query compared everyone. A static-only binding
would answer that but makes a measure comparable exactly one way forever.

Both, with the safety carried by the *result* rather than the schema: `--cohort` overrides
`default_cohort`, and `PeerCohortResult.cohort` always names the cohort actually used, so
the answer is self-describing at every call site. A consumer that renders the cohort name
from the result cannot drift from the query. That — not the static binding — is what closes
the bug class.

### 4.1 `per:` is a measure, not a calendar unit

The first draft specified `per: Day | Week | Month`. That is broken: dividing a 90-day
window's total by the constant 90 orders entities identically to the raw total, so the band is
mathematically unchanged — and banding on the total is precisely the bug §3 rule 2 records
(new stores' totals read as small stores'; Clovis went from 0 peers to 6 only after the fix).

The reference's divisor is `countIf(d BETWEEN ts AND pe)` — **days on which that store has
data**, a per-entity quantity. So `per:` names a measure. `per: sales.trading_days` divides by
each entity's own trading-day count; omitting `per:` bands on the raw measure and is the
caller's explicit choice.

### 4.2 The band's window is its own

The first draft of this section ended by claiming a trailing window needs no field of its
own: "the cohort is resolved for the period the caller asks about; a trailing window is
expressed by asking for the trailing period." **That claim is wrong.** Asking for the trailing
period moves the compared *measure* too, not just the band — and that is the wrong answer, not
merely an inconvenience. The Watchlist bands `wage_cost` and `give_away` on a trailing average
daily sales figure while measuring the metric itself over the selected reporting period, often
a single month: a one-month sales figure is a noisy size proxy (a store having a slow March
does not make it a smaller store), so the band needs 90 days of history but the metric does
not. Widening the query period to get that history widens the metric's window along with it.
This is a different axis from `per:` (§4.1): `per:` stops a trailing total from conflating size
with tenure by normalising *within* one window; it cannot make the band and the metric span
*two* different windows. Before this field, a modeller writing a band got a plausible number
computed over the wrong window with nothing reporting a problem — the one silent failure in a
design that is otherwise emphatic that refusals are reported (§6).

```yaml
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      size_matched:
        band:
          measure: sales.net_sales
          per: sales.trading_days
          tolerance: 0.35
          # size measured over the trailing 90 days, independent of the period
          window: 90 days
        require: [stores.accounting_basis]
        min_peers: 3
```

**Value grammar:** the same interval grammar `shift.by` uses (`"90 days"`, `"3 months"`),
parsed by `Interval::parse` (`src/engine/shift.rs`) — chosen deliberately over a plain integer
day-count. A count-plus-unit makes "non-finite" and "fractional" *unrepresentable* rather than
merely rejected, so the validator's remaining job (§4.3) is only the two cases the grammar
itself cannot rule out: an unparseable string and a zero-length interval. A negative count or
an unknown unit is refused by `Interval::parse` itself, so the validator inherits both refusals
rather than restating them; and a non-finite or fractional half-width, the shape `tolerance`
has to guard against explicitly, cannot be written here at all.

**Semantics: a lookback extension, anchored at the period START.** For query period
`[start, end]` the band window is `[start − window, end]`, both bounds inclusive, with the
lower bound computed by `Interval::subtract_from` — the same calendar-arithmetic routine
`shift` uses. The compared measure and the `require` tuple are unaffected by any of this; they
stay on the query period. Omitting `window:` keeps today's behaviour exactly: the band is
measured over the same period as everything else.

**Parity with the reference.** The Watchlist's trailing window is
`[periodStart − trailingDays, periodEnd]` — a lookback *extension* of the period, not a
fixed-length window ending at the period's end (`trailingStart = daysBeforeIso(period.start,
trailingDays)` at `peerCohortSql.ts:200`, `daysBeforeIso` at `:219-223`, consumed by
`sumIf(..., d >= ts AND d <= pe)` at `:278-279`). airlayer anchors the same way, and must:
the reference's window spans `trailingDays + periodLength`, so reproducing it with an
end-anchored `window:` would need a *different declared value for every period length* — 121
days for a 31-day January, 118 for a 28-day February. No fixed end-anchored window expresses
the reference for two months of different lengths, which puts the anchor beyond anything a
caller can configure around.

The invariant this buys is worth having on its own terms: **the band window always contains
the period.** An entity is never banded on a window that excludes part of the performance
being judged, and "present in the period, absent from the band window" stops being a
window-length artifact. That branch still exists in §6 — the two pulls select *different
measures*, so a band measure living on a view with no rows for an entity can still leave it
unbanded — but containment of the windows is now a fact the reader can rely on rather than a
case to reason about.

### 4.3 Validation

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
- **`band.window`, if present, must parse.** An unparseable interval string is a hard error at
  validation time, not a runtime surprise. A negative count and an unknown unit are refused by
  `Interval::parse` and reported through this same rule (§4.2); the one case the parser accepts
  and a band cannot use is the next bullet.
- **`band.window`, if present, must be non-zero length.** `0 days` scans no rows in the band
  pull and would band every subject on an empty measure — a cohort that silently admits or
  rejects everyone, which is worse than the field not existing.
- `resolve_cohort` re-checks both `band.window` rules at runtime and REFUSES rather than
  falling back to the period, because a layer built programmatically (not parsed from YAML)
  can skip validation entirely.

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
the band against **that subject's own** value, exclude self, take the R-7 median. This holds
whenever the cohort has no band, or a band with no `window:` declared — still exactly ONE
pull, byte-for-byte the query it always compiled (pinned by a round-trip-counting test).

**When `band.window` is declared, the pull SPLITS in two.** The band's measure is drawn from a
different window than the compared metric, so one `QueryRequest` cannot express both — a
single GROUP BY cannot carry two different date-range filters over the same rows. `resolve_cohort`
therefore issues:

- **the period pull** — `[key] + require` dimensions, target measure only, filtered to the
  query period;
- **the band pull** — `[key]` dimension **alone** (no `require`), `band.measure` + `band.per`,
  filtered to the band window (§4.2).

The band pull carries no `require` members deliberately: `require` is a property of the
subject as read over the comparison period, and selecting it a second time over the band
window would fan the band pull's GROUP BY out — an entity whose `require` value differs (or is
NULL) in the band window would corrupt the per-entity sums the pull exists to compute. The two
pulls are joined per entity key in Rust, at the same place the peer loop already runs (§6).

**Query cost: 2 + N** without a band window, against today's 1 + N (one overall at `:2557`, N
breakdowns fired concurrently through `parallel_execute` at `:4347`). One extra query, one
extra wave. A declared `band.window` adds **two** more — the band pull and its own independent
`__cohort_total__` count (§5.1) — for 4 + N, and only when `window:` is actually declared.

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

**Every guard here extends unchanged to the band pull** when `window:` is declared: its own
explicit `limit: Some(UNBOUNDED_QUERY_LIMIT)`, and its own independent `COUNT(DISTINCT
entity_key)` cross-check — not a count shared with the period pull. The count must be per-pull
because the two pulls span different populations by construction: an entity that traded in the
trailing window but not in the reporting period (or the reverse) is *expected* to appear in one
pull and not the other (§6). Checking both pulls against a single shared count would refuse
that correct case as if it were a truncation. Two helpers were extracted for this —
`count_entities` and `cross_check_pull` — called once per pull rather than once per
`resolve_cohort` call.

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
- **In the band window but not the comparison period** (only when `band.window` is declared) —
  the entity has rows to compute a band value from but none to compare the target measure over.
  Reported excluded: "has rows in the band window but none in the comparison period, so
  `<measure>` cannot be read for it; it is neither a subject nor anyone's peer."
- **In the comparison period but not the band window** — the subject has a metric value but
  no size to band it by. Start-anchoring makes the band window *contain* the period (§4.2), so
  this is not a window-length artifact; it stays reachable because the two pulls select
  different measures, and the band's measure may come from a view with no rows for that entity
  over the band window. Reported excluded: "no row in the band window for
  this subject … excluded rather than banded on the comparison period instead." Falling back to
  the period row for the band value is explicitly refused: that would silently reintroduce the
  wrong-window defect §4.2 exists to close, just moved from the declaration down to the row.
- **A keyless row in the band pull**, when `window:` is declared — reported under the same
  synthesized null marker as a keyless row in the period pull, suffixed ` [band window]` so the
  two are distinguishable. The marker itself is now chosen over the union of BOTH pulls' keys,
  so a synthesized id can never collide with a real key from either side.

```rust
pub struct PeerCohortResult {
    pub entity: String, pub cohort: String, pub statistic: BenchmarkStatistic,
    pub period: (String, String),
    pub band_window: Option<(String, String)>,   // §8; Some iff a band is declared
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
a cohort is declared on an entity whose grain is plainly unbounded. The ceiling is applied to
each window's count separately, for the same reason §5.1's cross-check is: a band window
spanning years can reach a population the reporting period never does, and a ceiling read only
off the period would let it through.

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

**`band_window` on the result.** `PeerCohortResult.band_window: Option<(String, String)>` is
`Some` exactly when the cohort declares a band — equal to the query `period` when the band
declares no `window:` (because "the same as the period" is a fact about this run, not the
absence of one) — and `None` when the cohort has no band at all. Three states, not two.
Reported for the same reason `cohort` is (§4): a consumer rendering "compared against entities
of similar size" must not drift from the window the query actually banded on. `inspect --json`
carries it twice: verbatim under `views[].hierarchy[].cohorts` (the whole `Cohort` serialises,
`window` included, as the plain declared string), and as `band_window` in
`ontology.comparability`, emitted as the DECLARED interval string rather than a resolved date
pair (that block describes the schema; the resolved window depends on the period a query asks
for) — ABSENT from the JSON rather than `null` when the band has no window. The `airlayer
cohort` CLI prints a `band measured over <start> .. <end> (trailing, anchored at the period
end)` line only when the resolved band window differs from the query period.

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
| Metric polarity (all five reference metrics are lower-is-better) | **Delivered.** `opportunity` is polarity-aware end to end as of #112 (`select_benchmark`, the `is_underperforming`/`gap_of` closures, the benchmark tiers). What stays hardcoded higher-is-better is narrower: `opportunity_drill`'s `component_candidates`/`dimension_candidates`, which this design does not touch — drill refuses a `lower_is_better` target outright (`metric_tree_ops.rs:4907`) |

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
- **Band-window arithmetic:** trailing-from-period-end via `Interval::subtract_from`, checked
  against a hand-computed calendar date, not merely a day count.
- **Band-window round trip:** a bandless-or-unwindowed cohort still issues exactly ONE
  `QueryRequest` — the regression test that would catch an accidental split when `window` is
  absent.
- **Both window-exclusion directions:** an entity in the band window but not the comparison
  period, and (only reachable when `window` is shorter than the period) an entity in the
  comparison period but not the band window — each reported with a reason, never silently
  dropped or banded on the wrong window as a fallback.
- **Band-pull guards are independent:** the band pull's own truncation refusal and its own
  fan-out refusal, driven by its own cardinality count — not the period pull's.
- **Fingerprint immunity:** `definition_fingerprint_ignores_cohort_band_window`, alongside the
  existing `..._ignores_default_cohort`.
- **Integration (DuckDB, tier 1), band window:** two cohorts differing in exactly one field
  (`window: 90 days`) produce different peer sets for the same pair of entities, in both
  directions; an entity that traded only outside the reporting period is banded (present in the
  trailing pull) but never a subject (absent from the period pull) — kept in a separate fixture
  from the existing cohort seed so its hand-computed census stays pristine.

## 11. Phasing

1. **Schema + validator + inspect.** `cohorts`, `CohortBand`, the key-has-a-dimension check,
   ontology surfacing. No behaviour.
2. **`resolve_cohort`.** The entity-grain pull, the truncation guard, the peer loop, R-7,
   `PeerCohortResult` with its refusal channels.
3. **CLI `cohort` subcommand.**
4. **oxy endpoint + SDK.**

## 12. Decisions (was: open questions)

- **Entity-count ceiling (§6).** Configurable, defaulting to 5,000 subjects — below the
  10k `DEFAULT_QUERY_LIMIT` the pull must stay under anyway, and 25M pairs is still a
  sub-second Rust loop. `resolve_cohort` refuses above it with the count in the message.
- **`Measure.default_cohort`.** Yes, with `--cohort` override; the result names the cohort
  used. Rationale recorded in §4.
- **Composition with `Shift.comparable_by`.** Out of scope for v1 and explicitly untested.
  The primitives are orthogonal by construction (one restricts *which entities*, the other
  *which window*), but nothing verifies the interaction, so the plan neither claims it works
  nor forbids it.
- **Scope.** Phases 1-3 (airlayer). The oxy endpoint and the three hand-maintained TS
  mirrors are deferred to a follow-up, as the companion spec's oxy task was.
- **Start-anchoring, matching the reference (§4.2).** `band.window` is a lookback *extension*
  of the query period: `[start − window, end]`. An earlier draft anchored at the period end
  instead and recorded that as a deliberate divergence; that was wrong. The reference's window
  spans `trailingDays + periodLength`, so an end-anchored window cannot reproduce it for two
  months of different lengths under any single declared value (121 days for January, 118 for
  February) — the anchor is not something a caller can configure around. Start-anchoring also
  guarantees the band window contains the period, so an entity is never banded on a window that
  excludes part of the performance being judged. The comparison-period-but-not-band-window
  exclusion (§6) survives the change, because the two pulls select different measures — but its
  cause is the band measure's own view, not a short window.

## 13. Verification against main (2026-09-07, @ dc3f209)

Every citation in §§2-8 was re-checked after #112 and #115 landed. Line numbers moved (the
file grew from ~19k to ~22k lines); no claim changed in kind. The load-bearing results:

- **§2's spine holds.** `select_benchmark` (`:3712`) still returns one scalar + one basis
  per dimension; `benchmark_basis: String` (`:1557`); `benchmark_filter: Vec<QueryFilter>`
  (`:1596`); drill still bails on an empty filter (`:4956`) with the "two fixed populations,
  chosen once at the root and never narrowed" comment verbatim (`:4979-4981`); the
  significance gate still reads a single `bench_row` (`:3380-3397`). #112 made this
  machinery polarity-aware without changing its arity.
- **`quantile_r7` already exists** (`:3690`), a free function over a sorted `&[f64]`, called
  by `select_benchmark`. §5.2 is a call, not new logic. `BenchmarkStatistic` (`:3662`) has
  exactly `Median | P75 | BestPeer`.
- **`QueryExecutor`** (`:4218`), **`parallel_execute`** (`:5195`, `std::thread::scope`),
  **`identifier_dimensions`** (`:6077`), **`augment_layer_for_opportunity`** (`:2583`),
  **`run_opportunity`** (`cli/mod.rs:2683`) — all present, shapes unchanged.
- **`DEFAULT_QUERY_LIMIT = 10_000`** (`engine/mod.rs:28`) and **`UNBOUNDED_QUERY_LIMIT`**
  (`:39`) unchanged, at the same lines; `compile_query` still fills `None` with the default
  (`:180`). §5.1's trap is live.
- **Fingerprint immunity is structural, not incidental.** `definition_fingerprint`
  (`preagg.rs:94-152`) receives `view: &View` and never sees `Entity` at all, so a `cohorts`
  field cannot enter it by any path. #112 added `definition_fingerprint_ignores_measure_direction`
  (`preagg.rs:4820`) making the same point for `Measure.direction`. A `default_cohort` field
  on `Measure` needs the equivalent test.
- **Nothing pre-empts this design.** `PeerCohortResult`, `resolve_cohort`, `CohortSubject`,
  `struct Cohort` — zero hits tree-wide. No field named `cohorts` collides on `Entity`.
- **§5's "CTE machinery does not help" confirmed.** `build_cohort_context`
  (`sql_generator.rs:4330`) is two date literals with no per-subject grouping;
  `__shift_aligned` joins on equality only (`:4848`); `build_lifespan_cte_sql` (`:4428`) is
  a grouped-CTE template, not a band self-join.
- **`opportunity()` takes `&SemanticLayer`; `opportunity_drill` takes `&SharedLayer`**
  (`Arc<RwLock<..>>`) because it augments mid-call under a write guard. `resolve_cohort`
  follows the former — it needs no runtime dimension discovery.
- **Fingerprint immunity extended to `band.window`.** `CohortBand.window` is reached the same
  way `per`/`tolerance` are — through `Entity`, which `definition_fingerprint` never touches at
  all (`preagg.rs:94-152`). `definition_fingerprint_ignores_cohort_band_window` asserts this
  directly, alongside the existing `..._ignores_default_cohort`.
