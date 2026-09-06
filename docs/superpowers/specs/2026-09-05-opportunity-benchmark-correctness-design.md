# Correcting `opportunity`'s benchmark selection

**Status:** design, awaiting review
**Date:** 2026-09-05
**Scope:** `airlayer` (`src/engine/metric_tree_ops.rs`, schema), `oxy` (endpoint + web-app)
**Companion:** cross-sectional cohorts are a separate spec
(`2026-09-05-cross-sectional-comparability-design.md`). This one deliberately contains no
cohort machinery.

## 1. Why this is split out

An adversarial review of the cohort spec found nine design holes. Two of them are not about
cohorts at all — they are live defects in `opportunity` today, reachable in a shipped UI, and
they can be fixed without any of the cohort architecture that the companion spec is still
blocked on.

**This surface is live.** `web-app/src/App.tsx:421` mounts `/ide/world-model` and `:483`
makes it the default index redirect for that route group; the World Model is the first icon
in the IDE sidebar, behind no feature flag. `useOpportunityQuery` is called at
`WorldModelOpportunitiesSection.tsx:182`, `useDrillQuery` at `WorldModelSegmentDrill.tsx:66`.
Whatever these functions compute is on someone's screen.

## 2. The three defects

### 2.1 Polarity: `opportunity` assumes higher-is-better, and inverts every cost metric

There is no polarity concept anywhere in airlayer — `grep -rn "higher_is_better\|lower_is_better\|polarity\|is_desirable" src` returns nothing. It is hardcoded in three places:

- `pick_benchmark`'s `best_peer` arm takes `*sorted.last()`, i.e. the **max**
  (`metric_tree_ops.rs:3046-3059`);
- the sizing filter is `.filter(|s| s.cmp < benchmark)` (`:2900`);
- `gap_is_significant` is one-sided on `gap = benchmark − cmp` (`:2419-2449`).

For a cost or defect rate — `cogs / net_sales`, `wage / net_sales`, `discounts / gross_sales`,
`voided / rung`, or anything where less is better — this is inverted end to end. The benchmark
becomes the **worst** performer, the segments selected for "upside" are the **cheapest**
ones, and their sized upside is the cost of becoming average. Every metric in the reference
implementation this work was designed from is of this kind.

This is not a cohort problem and does not need cohorts to fix.

### 2.2 A thin segment cannot be a subject, but can still set the bar

This is the actual mechanism behind the motivating bug. With `best_peer` the benchmark is
`max` over segments, so a one-store Oregon at rate 0.91 **sets the bar** for a
twenty-one-store California at 0.30, which is then sized as `(0.91 − 0.30) × count_CA`.

The existing code already names the problem, at `:2814-2818`:

> "The benchmark value was copied out of one of these segments, so the nearest segment is the
> one that set the bar. We need its row count and spread: a bar set by a thin segment is a bar
> with its own error, and pretending otherwise is how three statistically identical statuses
> acquire a 'leader'."

The mitigation is statistical — `bench_sd` / `bench_n` feed the Welch test — and it **fails
open**: `gap_is_significant` returns `None` when `sd` is absent or `n < 2`, and only
`Some(false)` drops a segment. A one-row segment has no sample stddev, so the bar it sets is
never challenged.

Note carefully: any floor that filters *subjects* does not fix this. Oregon is not the
subject; it is the benchmark.

### 2.3 The `>= 8` switch is undefended policy

`pick_benchmark` switches from "the single best segment" to "an interpolated 75th percentile"
as a dimension's cardinality crosses eight (`:3052`). It is a bare literal with no named
constant, no test asserting the boundary is intentional, and no justification in the
codebase. It silently changes what the reported number *means* mid-scan, and it is policy the
caller can neither see nor override.

## 3. Non-goals

- **No cohorts.** No `entity.cohorts`, no peer sets, no per-subject benchmarks. Those need a
  result-shape change and an unresolved architecture question; they are the companion spec.
- **No `rate_denominator`.** The review found three independent breaks in it (unit
  incoherence between `cmp` and `upside`, desynchronisation of `supports_rate_basis` from
  `augment_layer_for_opportunity`, and a route to re-open the bug PR #110 closed). It belongs
  with cohorts, correctly specified, not smuggled in here.
- **No thresholds move into the platform.** Unchanged from the companion spec.

## 4. Polarity

```yaml
measures:
  - name: food_cost_pct
    type: number
    expr: "..."
    direction: lower_is_better    # higher_is_better (default) | lower_is_better
```

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MeasureDirection {
    #[default]
    HigherIsBetter,
    LowerIsBetter,
}

// on Measure:
#[serde(default, skip_serializing_if = "is_default")]
pub direction: MeasureDirection,
```

Threaded through exactly the three hardcoded sites:

| Site | Today | With polarity |
|---|---|---|
| `select_benchmark` best-peer arm | `sorted.last()` (max) | max, or `sorted.first()` (min) when lower-is-better |
| `select_benchmark` percentile arm | p75 | p75, or p25 when lower-is-better |
| sizing filter (`:2900`) | `s.cmp < benchmark` | `s.cmp < benchmark`, or `>` when lower-is-better |
| `gap_is_significant` (`:2419-2449`) | `gap = benchmark − cmp` | `gap = cmp − benchmark` when lower-is-better |

`gap` and `upside` stay **positive = opportunity** in both directions, so no consumer has to
learn a sign convention. `OpportunityResult` gains a `direction` field so a caller can render
"reduce to" rather than "lift to" — the web-app currently hardcodes improvement language.

**Default is `higher_is_better`**, which is today's behaviour, so an undeclared measure is
unaffected. This is the one place in this spec where preserving the default is right: a
measure whose polarity nobody has stated is not evidence of either polarity, and guessing
from the name (`cost`, `rate`, `churn`) would be worse than asking.

## 5. `min_support`, applied to both sides

```rust
// on the opportunity/drill request:
/// Refuse to benchmark a segment whose support falls below this — as a subject
/// AND as a candidate benchmark. Defaults to 2.
pub min_support: usize,
```

Two application points, and the second is the one that matters:

1. **Subject side** — a segment below the floor is not sized.
2. **Benchmark side** — a segment below the floor is excluded from the population
   `select_benchmark` picks from. Without this the floor does nothing about §2.2.

### 5.1 Support is a distinct-entity count, at the *dimension's* grain

Under a row-count reading, one-store Oregon has thousands of order rows and clears any
sane floor; the fix would be inert. Support is therefore `COUNT(DISTINCT <entity key>)` at
the grain the segment resolves to.

**The grain is the scanned dimension's, not the target measure's.** An earlier draft of this
section said "primary entity key of the target's view" two lines after saying "the grain the
segment resolves to", and the implementation followed the prescription rather than the
rationale. Those are not the same thing, and on the shape this floor exists for they are
opposites: a transaction-grain fact view's primary entity IS the row surrogate, so
`COUNT(DISTINCT sale_id)` is exactly the row count this section rejects. The pathology lives
on dimensions sitting *above* an entity grain — `stores.region`, whose owning view's primary
entity is `store_id` — and those dimensions belong to a different view from the measure.

`SegRow.count` cannot supply the number either: it derives from `count_alias`, which exists
only when `count_measure` was discovered, i.e. only in rate mode. A ratio target — which is
every metric in the proving case — has `count == 0.0` for every segment.

So `augment_layer_for_opportunity` installs a synthetic measure alongside `__opp_stddev__`
and `__opp_n__`, **one per distinct owning view in the scan**:

```
__opp_support__ = COUNT(DISTINCT <primary entity key of the DIMENSION's owning view>)
```

Expressed as a measure on the target's view referencing the owning view's key —
`COUNT(DISTINCT {{stores.store_id}})` — which `expand_views_for_expr_refs`
(`src/engine/sql_generator.rs:1950`, the issue-#55 machinery) resolves by pulling the join
in. Verified by spike: the join appears, the count is the true distinct-entity count rather
than the row count, and a `sum` selected alongside stays uninflated.

Support measures live in the **target's** view, one per distinct owning view among the
scanned dimensions, its name suffixed with the owning view (`__opp_support__revenue__stores`)
so that two distinct owning views installing alongside the same target measure never collide;
the degenerate same-view case (owning view == target view) keeps the unsuffixed name, which is
also what every existing single-view fixture and test already expects. `view_primary_entity_key`
is deterministic per view, so the existing idempotent "skip if already present" guard still
handles re-running the installer for the same target without duplicating a measure.

**The fact-view case degenerates correctly, and that is not a leak.** When the scanned
dimension belongs to the target's own view (`sales.channel`), the key resolves to that view's
primary entity and support becomes the row count. That is the honest answer: there is no
coarser entity, every row genuinely is its own instance, and the Oregon pathology cannot
arise. The floor only needs to bite where a segment covers few *things*.

**When the owning view declares no `type: primary` entity, or declares a composite key**,
there is no row identity to count and no honest support number. The floor is **not** silently
downgraded to rows: it is reported as inapplicable **for that dimension**, once, via
`support_floor_inapplicable` (§9) — not per segment via `skipped_segments` (§7), because those
segments are still benchmarked and still sized. A floor that quietly measures something else is
worse than no floor.

**Known gap.** The spike covered only the star topology (fact *many* → dimension *one*),
which is the Oregon shape. A dimension reached through a `OneToMany` hop via a shared hub
routes through the user-grain CTE machinery, where `count_distinct` is documented as immune
to fan-out but unverified here. Out of scope for this spec; revisit if a cohort or
sibling-view topology needs a floor.

### 5.2 Why 2, and why it differs from the reference's 3

The reference implementation's peer floor is 3, justified against external practice (Fannie
Mae's "minimum of three closed comparables", NCES's three-institution peer group) and against
the observation that at three peers the median *is* one store's number.

That floor governs **cohort size** — how many peers a median may be taken over. `min_support`
here governs **segment support** — whether a segment is substantial enough to be compared at
all. They are different quantities and correctly have different values. 2 is the point below
which no dispersion exists and the significance gate has nothing to evaluate; it is the floor
at which the existing fail-open behaviour becomes fail-closed. When the companion spec adds
cohorts it will carry its own `min_peers`, and 3 is the right default there.

## 6. `statistic` becomes explicit; the `>= 8` switch is deleted

```rust
pub enum BenchmarkStatistic { Median, P75, BestPeer }
```

Required on the request. No cardinality-dependent default. The CLI defaults to `median`.

`DimensionOpportunity.benchmark_basis` currently carries `"best_peer" | "p75"` as a string
and is consumed by the web-app; it keeps its type and gains `"median"` as a value.

A test asserts that cardinality does **not** influence which statistic is used — the direct
inverse of the deleted rule, so it cannot creep back in unnoticed.

## 7. Reporting refusals per segment

§5 refuses segments; §8 refuses dimensions. Dimension refusals already have a channel
(`SkippedDimension { dimension, reason }`, `:1586-1590`). **Segment refusals have none** —
`SegmentOpportunity` is only constructed for segments that are below the benchmark and
survived the noise gate, and it carries `gated: bool` with no reason.

The reference implementation hit this exact failure and fixed it by moving guards out of the
`WHERE` clause into a reported `drop_reason` column, because a store "simply did not appear in
the labor list and no screen said why". Reproducing the failure it already solved would be
careless.

```rust
pub struct SkippedSegment {
    pub segment: String,
    pub reason: String,     // "support 1 below floor of 2", "no entity grain to count support at"
}
// on DimensionOpportunity:
pub skipped_segments: Vec<SkippedSegment>,
```

`skipped_segments` carries **genuine exclusions only** — segments the floor actually removed
from the benchmarking population. §5.1's "floor inapplicable" case is *not* one of those: those
segments are still benchmarked and still sized (fail-open), so reporting them here would claim
they were excluded when they were not. That case is per-*dimension*, not per-segment, and is
carried once by `support_floor_inapplicable` (§9). An earlier draft of this section said
`SkippedSegment` was where it gets said; that was wrong, and shipping it produced segments
reported simultaneously as an opportunity and as excluded.

## 8. The `analysis` capability set

Replaces adding a second boolean beside `segmentable`.

```yaml
dimensions:
  - name: party_size
    analysis:
      explain: true      # decomposing an observed change or gap by it is legitimate
      benchmark: false   # a 6-top outspends a 2-top by arithmetic, not performance
```

```rust
pub struct DimensionAnalysis {
    #[serde(default = "default_true")] pub explain: bool,
    #[serde(default = "default_true")] pub benchmark: bool,
}
// on Dimension:
#[serde(default, skip_serializing_if = "Option::is_none")]
pub analysis: Option<DimensionAnalysis>,
```

### 8.1 Gate points — verified, not assumed

`is_segmentable` (`:5191-5199`) is applied inside `discover_dimensions` (`:5279`), which has
six production call sites. Gating a new flag there would hit all six. The audit:

| Line | Enclosing fn | Operation | Gated on |
|---|---|---|---|
| 2602 | `opportunity` (`:2475`) | scan for a benchmark axis | **`benchmark`** |
| 3783 | `dimension_candidates` (`:3753`) | decompose a *gap* | `explain` |
| 4189 | `opportunity_drill` (`:4071`) | decompose a *gap* | `explain` |
| 4618 | `decompose_to_searchable` (`:4589`) | decompose a *change* | `explain` |
| 4627 | `decompose_to_searchable` (`:4589`) | decompose a *change* | `explain` |
| 4771 | `explain` (`:4717`) | decompose a *change* | `explain` |

`dimension_candidates` is reached only from `opportunity_drill` (`:4238`) in production.

**Drill is gated on `explain`, not `benchmark`** — a deliberate call. Drill splits an already-
chosen gap by a further dimension; splitting a store-vs-peers gap by party mix is a legitimate
explanation of that gap, because mix shift is real. `benchmark: false` says "do not hold two
of these segments to the same standard", which is a statement about the *scan*, not about
decomposition.

So `analysis.benchmark` gates exactly one site (`:2602`) and `analysis.explain` gates the
other five. Neither is applied inside `discover_dimensions` or `is_segmentable`.

### 8.2 `segmentable` compatibility

`segmentable` cannot be removed: `.view.yml` parsing is a live surface, and
`src/schema/foreign/cube.rs:342` maps Cube's `shown` onto it (rationale at `:338-341`), so
every converted Cube schema carries it.

- `segmentable: false` ⇒ `{explain: false, benchmark: false}` — exactly its meaning today,
  since `discover_dimensions` gates all six sites.
- `segmentable: true` / absent ⇒ no constraint.
- **Both present ⇒ `analysis` wins, with a validator warning.** Not an error: `segmentable` is
  machine-generated by the Cube converter, so a user adding `analysis` to a converted schema
  would be forced to hand-edit generated output to satisfy a hard failure.

**Resolution is a function, not a parse-time rewrite.** Roughly forty sites construct
`Dimension` literals directly (`sql_generator`, `promotions`, `evaluator`, `catalog`,
`join_graph`, the foreign parsers) and never pass through the parser. A parse-time mutation
would silently lose the alias for any programmatically built layer. Instead:

```rust
impl Dimension {
    pub fn analysis_caps(&self) -> DimensionAnalysis { /* analysis, else segmentable, else default */ }
}
```

read at each gate point.

`cube.rs:342` keeps producing `segmentable` (no converter change needed, the alias handles
it). `INIT_CLAUDE_MD` (`src/cli/mod.rs:6240`) and any `.claude/skills/*/SKILL.md` mentioning
`segmentable` are updated per the sync rule in `CLAUDE.md`.

## 9. Effect on live consumers

Unlike the companion spec's first draft, this one does not assume the surface is dead.

**Rust:** `pick_benchmark` → `select_benchmark` with polarity and a required statistic;
`augment_layer_for_opportunity` gains `__opp_support__`; `DimensionOpportunity` gains
`skipped_segments` and `support_floor_inapplicable`; `OpportunityResult` gains `direction`.

**oxy server:** `OpportunityRequest` / `DrillRequest` gain `statistic` and `min_support`.
Both are required-with-a-default on the wire, so an old client keeps working and gets
`median` — which is a *behaviour change* for anyone currently seeing p75/best_peer. That is
the intended correction, but it must be called out in the changelog rather than discovered.

**web-app:** `WorldModelOpportunitiesSection`, `WorldModelSegmentDrill`,
`WorldModelSizedSegmentRow` and `WorldModelDetailPanel` consume `benchmark_basis` and the
segment list. New `benchmark_basis` values, a new `skipped_segments` array and a `direction`
field need rendering, and improvement copy that currently hardcodes "lift to" needs the
lower-is-better wording. Their component tests assert rendered output and will need updating.

`benchmark_basis` carries **four** values, not three, so the TypeScript union is
`"median" | "p75" | "best_peer" | "empty"`. `"empty"` is not a statistic: it means no
benchmark was computed because `min_support` excluded *every* segment in the dimension. The
dimension is still reported — "every segment here is too thin to judge" is a finding — but
its `benchmark` is a placeholder `0.0` rather than a measurement, its `segments` and
`benchmark_filter` are empty, and its `skipped_segments` says why each segment was dropped.
A consumer MUST NOT render `"empty"` as a benchmark or compare anything against it.

Alongside it, `support_floor_inapplicable: string | null` is a **per-dimension** reason
saying the `min_support` floor could not be evaluated here at all (no support measure rode
along — see §5.1). It is deliberately not a per-segment refusal: in that case the floor is
inapplicable rather than failed, every segment is still benchmarked and still sized
(fail-open), and none of them appear in `skipped_segments`. Render it once against the
dimension, never as a badge on rows that are simultaneously being reported as opportunities.

**SDK:** `sdk/typescript/src/metricTree.ts` is the third hand-maintained mirror with no
codegen, and is *already* missing the `instance` field the server and web-app carry. New
fields land in all three copies in one commit, with a shape-assertion test.

**Pre-agg:** unaffected. `definition_fingerprint` (`src/engine/preagg.rs:94-152`) hashes view
name, `source_sql`, dimension name+expr and measure name/type/expr/filters. `direction` and
`analysis` enter none of those, so no rollup hash moves. Asserted by test, not assumed.

## 10. Testing

- **Polarity:** a lower-is-better fixture where the correct answer is the *opposite* segment
  set from today's; both the benchmark pick and the sizing filter asserted. A
  higher-is-better fixture asserting today's behaviour is unchanged.
- **Thin bar:** the motivating shape — one segment with support 1 and an extreme rate, one
  large segment — asserting the thin segment neither is sized **nor sets the bar**, and
  appears in `skipped_segments` with a reason. This is the regression test for §2.2 and must
  fail against current `main`.
- **Support semantics:** a segment with many rows but one distinct entity is refused;
  the same fixture with a row-count reading would pass, so the test distinguishes them.
- **The generated measure, not an injected number.** At least one test must run the support
  measure `augment_layer_for_opportunity` actually generates, against a real star schema, and
  assert the distinct-entity count. Every support test in the first implementation injected
  support through a stub executor, which is precisely why the target-view/dimension-view
  confusion survived seven tasks and eight reviews: nothing ever exercised the measure the
  code emits.
- **No entity grain:** a view without a primary entity reports the floor inapplicable rather
  than falling back to rows.
- **Statistic:** each statistic over a known population; cardinality does not select it.
- **Alias:** `segmentable: false` suppresses all six sites; `{explain: true, benchmark: false}`
  suppresses only `:2602`; both-present warns and `analysis` wins; a programmatically built
  `Dimension` resolves correctly (guards the function-not-parse-mutation decision).
- **Fingerprint:** byte-identical before and after the new fields.
- **Semantic query:** full suite unmodified.
- **Opportunity/drill:** expected to change; each changed assertion annotated with the
  corrected behaviour that moved it.

## 11. Phasing

1. **Schema + alias.** `direction`, `analysis`, `analysis_caps()`, validator warning, init
   artifacts, fingerprint test. No behaviour change.
2. **Polarity.** The three hardcoded sites. Biggest correctness win; independently valuable.
3. **`select_benchmark`.** Required statistic, `>= 8` deleted, benchmark population made
   explicit.
4. **`min_support` + `__opp_support__` + `skipped_segments`.** Needs 3, because the floor
   applies to the population 3 introduces.
5. **Gate points.** `benchmark` at `:2602`, `explain` at the other five.
6. **oxy + web-app + SDK.** Endpoint fields, rendering, all three TS copies, rev bump.

## 12. Open questions

- Should `direction` be declarable per *view* as a default (a costs view is uniformly
  lower-is-better), or only per measure? Per measure is safer and more verbose.
- `min_support` on `drill`: the drill inherits a benchmark chosen at the root, so applying the
  floor again at each level may double-filter. Probably it applies only at the root; confirm
  against `opportunity_drill`'s inheritance of `benchmark_filter`.
- Does any existing `.view.yml` in oxy's examples or a customer workspace declare a
  cost-like measure that is currently being sized backwards? Worth checking before shipping
  polarity, because the fix will visibly change numbers those users have seen.
