# Opportunity Benchmark Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three live defects in `opportunity`'s benchmark selection — inverted polarity for cost metrics, thin segments setting the benchmark, and an undefended cardinality-dependent statistic — and add the `analysis` capability set that lets a dimension be grouped but not benchmarked across.

**Architecture:** All changes are additive schema fields (`Option`/`#[serde(default)]`) plus edits inside `src/engine/metric_tree_ops.rs`. No SQL generation changes, no `QueryRequest` changes, no pre-agg changes. Synthetic measures follow the existing `augment_layer_for_opportunity` pattern. Capability resolution is a *method* read at gate points, not a parse-time rewrite, because ~40 sites construct `Dimension` literals without going through the parser.

**Tech Stack:** Rust, `cargo test`, `just`. Schema in `src/schema/models.rs`, validation in `src/schema/validator.rs`, logic in `src/engine/metric_tree_ops.rs` (19k lines, ~86 metric-tree tests), CLI in `src/cli/mod.rs`.

**Spec:** `docs/superpowers/specs/2026-09-05-opportunity-benchmark-correctness-design.md`

## Global Constraints

- **This surface is live.** `/ide/world-model` is mounted and is the default index redirect (`o3/web-app/src/App.tsx:421,483`), behind no feature flag. Behaviour changes here change numbers on someone's screen.
- **Semantic-query tests must pass unmodified.** SQL generation, joins, promotions, pre-agg. That surface is live and is not in scope.
- **Pre-agg rollup hashes must not move.** `definition_fingerprint` (`src/engine/preagg.rs:94-152`) hashes view name, `source_sql`, dimension name+expr, measure name/type/expr/filters. No new field may enter those. Asserted by test in Task 1.
- **Opportunity/drill tests are expected to change.** Every changed assertion gets a one-line comment naming the corrected behaviour that moved it. An assertion that changes for a reason nobody can name is a regression wearing a green tick.
- **Defaults preserve today's behaviour** for `direction` (`higher_is_better`) and `analysis` (both true). The two places that deliberately do *not* preserve it are `min_support` (defaults 2) and `statistic` (no cardinality default) — both stated in the spec as intentional corrections.
- **Do not run `cargo check`/`test` after every edit** (slow crate). Batch edits per task, run once at the task's test step.
- No attribution lines, `Co-Authored-By`, or "Generated with" footers in any commit message.

---

### Task 1: `MeasureDirection` on `Measure`

**Files:**
- Modify: `src/schema/models.rs` (add enum + field on `Measure`)
- Test: `src/schema/models.rs` (inline `#[cfg(test)]`), `src/engine/preagg.rs` (fingerprint test)

**Interfaces:**
- Consumes: nothing.
- Produces: `pub enum MeasureDirection { HigherIsBetter, LowerIsBetter }` with `Default = HigherIsBetter`; `Measure.direction: MeasureDirection`. Tasks 3 and 8 read it.

- [ ] **Step 1: Write the failing tests**

In `src/schema/models.rs` test module:

```rust
#[test]
fn measure_direction_defaults_to_higher_is_better() {
    let m: Measure = serde_yaml::from_str("name: revenue\ntype: sum\nexpr: amount\n").unwrap();
    assert_eq!(m.direction, MeasureDirection::HigherIsBetter);
}

#[test]
fn measure_direction_parses_lower_is_better() {
    let m: Measure = serde_yaml::from_str(
        "name: food_cost_pct\ntype: sum\nexpr: cogs\ndirection: lower_is_better\n",
    )
    .unwrap();
    assert_eq!(m.direction, MeasureDirection::LowerIsBetter);
}

#[test]
fn measure_direction_round_trips_and_omits_default() {
    let m: Measure = serde_yaml::from_str("name: revenue\ntype: sum\nexpr: amount\n").unwrap();
    let out = serde_yaml::to_string(&m).unwrap();
    assert!(!out.contains("direction"), "default direction must not serialize: {out}");
}
```

In `src/engine/preagg.rs` test module:

```rust
/// Guards the plan's Global Constraint: new schema fields must not move the
/// rollup hash, or every cached rollup silently invalidates.
#[test]
fn definition_fingerprint_ignores_measure_direction() {
    let mut view = fingerprint_fixture_view();
    let before = definition_fingerprint(&view, &["region".into()], &fixture_rollup_measures(), None);
    for m in view.measures.get_or_insert_with(Vec::new).iter_mut() {
        m.direction = crate::schema::models::MeasureDirection::LowerIsBetter;
    }
    let after = definition_fingerprint(&view, &["region".into()], &fixture_rollup_measures(), None);
    assert_eq!(before, after, "direction must not enter the rollup fingerprint");
}
```

If `fingerprint_fixture_view` / `fixture_rollup_measures` do not exist, build them inline from an existing fixture in that module — read the surrounding tests first and follow their construction style.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib measure_direction definition_fingerprint_ignores -- --nocapture`
Expected: FAIL — `MeasureDirection` not found.

- [ ] **Step 3: Implement**

In `src/schema/models.rs`, above `Measure`:

```rust
/// Which way is "better" for this measure.
///
/// `opportunity` sizes the gap between a segment and a benchmark. For revenue,
/// better is larger and the benchmark is a high performer; for a cost or defect
/// rate, better is smaller and the benchmark is a low one. Without this the
/// engine assumes higher-is-better everywhere, which selects the *cheapest*
/// segments of a cost metric and sizes their "upside" as the cost of becoming
/// average — inverted end to end.
///
/// Defaults to `HigherIsBetter`, which is the engine's historical behaviour. A
/// measure whose polarity nobody has stated is not evidence of either polarity,
/// so this is never inferred from the measure's name.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MeasureDirection {
    #[default]
    HigherIsBetter,
    LowerIsBetter,
}
```

On `Measure`, beside `shift`:

```rust
    /// Which direction of movement is an improvement. See [`MeasureDirection`].
    #[serde(default, skip_serializing_if = "is_higher_is_better")]
    pub direction: MeasureDirection,
```

And a module-level helper:

```rust
fn is_higher_is_better(d: &MeasureDirection) -> bool {
    matches!(d, MeasureDirection::HigherIsBetter)
}
```

`Measure` has a hand-written `Deserialize` impl (it makes `type` optional when `shift` is present). Add `direction` to it, defaulting to `HigherIsBetter` when the key is absent. Then add `direction: MeasureDirection::default()` to every `Measure { .. }` struct literal the compiler flags — these are in `src/schema/parser.rs`, `src/schema/globals.rs`, `src/schema/foreign/{cube,lookml,dbt,omni,mod}.rs`, `src/engine/metric_tree_ops.rs`, `src/engine/sql_generator.rs`, and test modules.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib measure_direction definition_fingerprint_ignores`
Expected: PASS.

- [ ] **Step 5: Full unit suite — nothing else may move**

Run: `just test`
Expected: PASS, no test edited.

- [ ] **Step 6: Commit**

```bash
git add src/schema/models.rs src/schema/parser.rs src/schema/globals.rs src/schema/foreign src/engine/preagg.rs src/engine/metric_tree_ops.rs src/engine/sql_generator.rs
git commit -m "Add MeasureDirection to Measure

Declares whether higher or lower is better. Defaults to higher_is_better,
today's implicit assumption, so no existing schema changes meaning. Asserts
the field does not enter definition_fingerprint, so no rollup invalidates."
```

---

### Task 2: `DimensionAnalysis` and `analysis_caps()`

**Files:**
- Modify: `src/schema/models.rs` (struct + field + method on `Dimension`)
- Modify: `src/schema/validator.rs` (disagreement warning)
- Test: both, inline

**Interfaces:**
- Consumes: nothing.
- Produces: `pub struct DimensionAnalysis { pub explain: bool, pub benchmark: bool }`; `Dimension.analysis: Option<DimensionAnalysis>`; `Dimension::analysis_caps(&self) -> DimensionAnalysis`. Task 7 calls `analysis_caps()`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn analysis_caps_defaults_to_all_true() {
    let d: Dimension = serde_yaml::from_str("name: region\ntype: string\nexpr: region\n").unwrap();
    let caps = d.analysis_caps();
    assert!(caps.explain && caps.benchmark);
}

#[test]
fn segmentable_false_suppresses_both_capabilities() {
    // segmentable is applied inside discover_dimensions today, which gates all
    // six call sites, so `false` means both capabilities off. This preserves
    // the alias exactly.
    let d: Dimension =
        serde_yaml::from_str("name: gender\ntype: string\nexpr: g\nsegmentable: false\n").unwrap();
    let caps = d.analysis_caps();
    assert!(!caps.explain && !caps.benchmark);
}

#[test]
fn analysis_can_split_the_two_capabilities() {
    // The party_size case: legitimate to decompose by, invalid to benchmark across.
    let d: Dimension = serde_yaml::from_str(
        "name: party_size\ntype: number\nexpr: party_size\nanalysis:\n  explain: true\n  benchmark: false\n",
    )
    .unwrap();
    let caps = d.analysis_caps();
    assert!(caps.explain, "explain must survive");
    assert!(!caps.benchmark, "benchmark must be suppressed");
}

#[test]
fn analysis_wins_over_segmentable_when_both_present() {
    // Not an error: cube.rs machine-generates `segmentable`, so a hard failure
    // would force users to hand-edit generated output.
    let d: Dimension = serde_yaml::from_str(
        "name: party_size\ntype: number\nexpr: p\nsegmentable: false\nanalysis:\n  explain: true\n  benchmark: false\n",
    )
    .unwrap();
    assert!(d.analysis_caps().explain);
}

#[test]
fn analysis_caps_works_on_a_programmatically_built_dimension() {
    // Guards the decision that resolution is a method, not a parse-time rewrite:
    // ~40 sites build Dimension literals without touching the parser.
    let mut d: Dimension = serde_yaml::from_str("name: x\ntype: string\nexpr: x\n").unwrap();
    d.segmentable = Some(false);
    d.analysis = None;
    let caps = d.analysis_caps();
    assert!(!caps.explain && !caps.benchmark);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib analysis_caps segmentable_false_suppresses analysis_can_split analysis_wins`
Expected: FAIL — `DimensionAnalysis` not found.

- [ ] **Step 3: Implement**

In `src/schema/models.rs`:

```rust
/// What a dimension may be *used for* in analysis, beyond plain grouping.
///
/// Grouping is not represented: a dimension that cannot be grouped by is not a
/// dimension. These are the two analytical uses that are separately valid, and
/// they must stay separate — benchmarking across `party_size` is invalid (a
/// 6-top outspends a 2-top by arithmetic), while splitting an observed drop by
/// it is legitimate. One flag serving both silently breaks the second.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DimensionAnalysis {
    /// May be used to decompose an observed change or gap (`explain`, `drill`).
    #[serde(default = "default_true")]
    pub explain: bool,
    /// May be *benchmarked across* — two segments held to the same standard
    /// (`opportunity`'s scan).
    #[serde(default = "default_true")]
    pub benchmark: bool,
}

fn default_true() -> bool { true }

impl Default for DimensionAnalysis {
    fn default() -> Self { Self { explain: true, benchmark: true } }
}
```

On `Dimension`, beside `segmentable`:

```rust
    /// See [`DimensionAnalysis`]. Supersedes `segmentable` when both are set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis: Option<DimensionAnalysis>,
```

And the resolution method — **a method, not a parse-time mutation**, so programmatically built layers resolve identically:

```rust
impl Dimension {
    /// Resolve this dimension's analysis capabilities, honouring the deprecated
    /// `segmentable` alias.
    ///
    /// `segmentable: false` means both capabilities off, because it is applied
    /// inside `discover_dimensions`, which gates every analysis call site.
    /// `analysis` wins when both are present.
    pub fn analysis_caps(&self) -> DimensionAnalysis {
        if let Some(a) = self.analysis {
            return a;
        }
        if self.segmentable == Some(false) {
            return DimensionAnalysis { explain: false, benchmark: false };
        }
        DimensionAnalysis::default()
    }
}
```

Add `analysis: None` to every `Dimension { .. }` literal the compiler flags.

- [ ] **Step 4: Add the validator warning**

In `src/schema/validator.rs`, in the per-dimension loop, following the existing stderr-warning style used for promotion ambiguities:

```rust
if dim.analysis.is_some() && dim.segmentable.is_some() {
    eprintln!(
        "[{}] dimension '{}' declares both `analysis` and the deprecated \
         `segmentable`; `analysis` wins. Remove `segmentable` to silence this.",
        view.name, dim.name
    );
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib analysis_caps segmentable_false_suppresses analysis_can_split analysis_wins && just test`
Expected: PASS, no existing test edited.

- [ ] **Step 6: Commit**

```bash
git add src/schema/models.rs src/schema/validator.rs src/schema/foreign src/engine
git commit -m "Add DimensionAnalysis capability set with segmentable alias

Replaces adding a second boolean beside segmentable. Resolution is a method
rather than a parse-time rewrite because ~40 sites build Dimension literals
without going through the parser. segmentable is kept as a deprecated alias
and cannot be removed: cube.rs maps Cube's `shown` onto it, so every
converted schema carries it."
```

---

### Task 3: Polarity in benchmark selection, sizing and significance

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`pick_benchmark` → polarity-aware; sizing filter at ~`:2900`; `gap_is_significant` call sites)
- Test: same file, inline

**Interfaces:**
- Consumes: `MeasureDirection` (Task 1).
- Produces: `fn measure_direction(layer: &SemanticLayer, target: &str) -> MeasureDirection`; `pick_benchmark(values: &[f64], direction: MeasureDirection) -> (f64, String)`. Task 4 replaces this signature again — that is expected; the two changes are kept separate so each is reviewable.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn pick_benchmark_lower_is_better_takes_the_minimum() {
    // For a cost rate the bar is the CHEAPEST peer, not the priciest.
    let vals = vec![0.20, 0.30, 0.91];
    let (b, basis) = pick_benchmark(&vals, MeasureDirection::LowerIsBetter);
    assert_eq!(b, 0.20);
    assert_eq!(basis, "best_peer");
}

#[test]
fn pick_benchmark_higher_is_better_is_unchanged() {
    let vals = vec![0.20, 0.30, 0.91];
    let (b, basis) = pick_benchmark(&vals, MeasureDirection::HigherIsBetter);
    assert_eq!(b, 0.91);
    assert_eq!(basis, "best_peer");
}

#[test]
fn opportunity_on_a_lower_is_better_target_sizes_the_expensive_segments() {
    // The regression test for the inverted-polarity defect. Without polarity
    // this selects the CHEAP segments and sizes the cost of becoming average.
    let layer = lower_is_better_layer();          // food_cost_pct, direction: lower_is_better
    let tree = MetricTree::build(&layer);
    let exec = stub_executor_with_rates(&[("north", 0.20), ("south", 0.60)]);
    let res = opportunity(&tree, &layer, "food.cost_pct", "food.day", ("2026-01-01", "2026-01-31"), &[], &exec).unwrap();
    let dim = res.dimensions.iter().find(|d| d.dimension == "food.region").unwrap();
    let segs: Vec<&str> = dim.segments.iter().map(|s| s.segment.as_str()).collect();
    assert_eq!(segs, vec!["south"], "the expensive segment is the opportunity, not the cheap one");
    assert!(dim.segments[0].gap > 0.0, "gap stays positive = opportunity in both directions");
}
```

Build `lower_is_better_layer()` and `stub_executor_with_rates()` following the existing fixture helpers in the test module (read `star_layer()` and the executor stubs used by the current `opportunity` tests first — reuse them rather than inventing a parallel style).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib pick_benchmark_lower_is_better opportunity_on_a_lower_is_better`
Expected: FAIL — arity mismatch on `pick_benchmark`.

- [ ] **Step 3: Implement**

Replace `pick_benchmark`:

```rust
/// Pick a benchmark value from a slice of segment values.
///
/// `direction` decides which end of the sorted values is "best": the max for a
/// higher-is-better measure, the min for a cost or defect rate.
fn pick_benchmark(values: &[f64], direction: MeasureDirection) -> (f64, String) {
    if values.is_empty() {
        return (0.0, "empty".into());
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if sorted.len() >= 8 {
        let q = match direction {
            MeasureDirection::HigherIsBetter => 0.75,
            MeasureDirection::LowerIsBetter => 0.25,
        };
        let idx = ((sorted.len() as f64) * q).floor() as usize;
        let idx = idx.min(sorted.len() - 1);
        (sorted[idx], "p75".into())
    } else {
        let v = match direction {
            MeasureDirection::HigherIsBetter => *sorted.last().unwrap(),
            MeasureDirection::LowerIsBetter => *sorted.first().unwrap(),
        };
        (v, "best_peer".into())
    }
}
```

Add the lookup:

```rust
/// The declared polarity of `target` (`view.measure`), defaulting to
/// higher-is-better when the measure or its view cannot be resolved.
fn measure_direction(layer: &SemanticLayer, target: &str) -> MeasureDirection {
    let Some((view_name, measure_name)) = target.split_once('.') else {
        return MeasureDirection::HigherIsBetter;
    };
    layer
        .views
        .iter()
        .find(|v| v.name == view_name)
        .and_then(|v| v.measures_list().iter().find(|m| m.name == measure_name).map(|m| m.direction))
        .unwrap_or(MeasureDirection::HigherIsBetter)
}
```

In `opportunity`, resolve `let direction = measure_direction(layer, target);` once near the top, pass it to `pick_benchmark`, and make the sizing filter and gap polarity-aware. The filter at `~:2900` becomes:

```rust
        let is_below = |c: f64| match direction {
            MeasureDirection::HigherIsBetter => c < benchmark,
            MeasureDirection::LowerIsBetter => c > benchmark,
        };
        let gap_of = |c: f64| match direction {
            MeasureDirection::HigherIsBetter => benchmark - c,
            MeasureDirection::LowerIsBetter => c - benchmark,
        };
        let segments_iter = seg_rows
            .iter()
            .filter(|s| is_below(s.cmp))
            .filter_map(|s| {
                let real = gap_is_significant(
                    gap_of(s.cmp),
                    s.sd,
                    s.filtered_n.unwrap_or(s.count),
                    bench_sd,
                    bench_n,
                    cardinality,
                    comparison_family,
                    SIGNIFICANCE_ALPHA,
                );
                // ... unchanged below
```

Replace every other `benchmark - s.cmp` in the sizing body with `gap_of(s.cmp)`. `gap` and `upside` stay positive-means-opportunity in both directions, so no consumer learns a sign convention.

Also apply `direction` to the `benchmark_filter` construction: the `p75` arm's `.filter(|s| s.cmp >= benchmark)` becomes `<=` when lower-is-better, since the benchmark tier is the *good* tier.

Add `direction: MeasureDirection` to `OpportunityResult` so a caller can render "reduce to" rather than "lift to".

- [ ] **Step 4: Run tests**

Run: `cargo test --lib pick_benchmark opportunity_on_a_lower_is_better`
Expected: PASS.

- [ ] **Step 5: Run the metric-tree suite and triage**

Run: `cargo test --lib metric_tree_ops`
Expected: existing tests PASS unchanged — every fixture defaults to `higher_is_better`. **If any fails, stop**: it means a default was not preserved. Do not edit the test to match; find why the default moved.

- [ ] **Step 6: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "Size opportunity in the measure's declared direction

opportunity assumed higher-is-better in three places: the best-peer arm took
the max, the sizing filter was cmp < benchmark, and the gap was benchmark -
cmp. For a cost or defect rate that selects the cheapest segments and sizes
their upside as the cost of becoming average.

gap and upside stay positive = opportunity in both directions, so no consumer
has to learn a sign convention."
```

---

### Task 4: Explicit `statistic`; delete the `>= 8` switch

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`BenchmarkStatistic`, `pick_benchmark` → `select_benchmark`, `opportunity` signature)
- Modify: `src/cli/mod.rs` (`--statistic` flag, default `median`)
- Test: `src/engine/metric_tree_ops.rs`

**Interfaces:**
- Consumes: `MeasureDirection`, `measure_direction` (Task 3).
- Produces: `pub enum BenchmarkStatistic { Median, P75, BestPeer }`; `fn select_benchmark(values: &[f64], direction: MeasureDirection, statistic: BenchmarkStatistic) -> (f64, String)`. `opportunity` and `opportunity_drill` gain a `statistic: BenchmarkStatistic` parameter. Task 6 extends the same signatures.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn statistic_is_not_selected_by_cardinality() {
    // The direct inverse of the deleted `>= 8` rule, so it cannot creep back.
    let seven: Vec<f64> = (1..=7).map(|i| i as f64).collect();
    let eight: Vec<f64> = (1..=8).map(|i| i as f64).collect();
    let (_, b7) = select_benchmark(&seven, MeasureDirection::HigherIsBetter, BenchmarkStatistic::BestPeer);
    let (_, b8) = select_benchmark(&eight, MeasureDirection::HigherIsBetter, BenchmarkStatistic::BestPeer);
    assert_eq!(b7, "best_peer");
    assert_eq!(b8, "best_peer", "crossing 8 segments must not switch the statistic");
}

#[test]
fn select_benchmark_median_is_r7_interpolated() {
    let vals = vec![1.0, 2.0, 3.0, 4.0];
    let (v, basis) = select_benchmark(&vals, MeasureDirection::HigherIsBetter, BenchmarkStatistic::Median);
    assert_eq!(basis, "median");
    assert!((v - 2.5).abs() < 1e-9, "even-sized median interpolates, got {v}");
}

#[test]
fn select_benchmark_empty_returns_empty_basis() {
    let (v, basis) = select_benchmark(&[], MeasureDirection::HigherIsBetter, BenchmarkStatistic::Median);
    assert_eq!(v, 0.0);
    assert_eq!(basis, "empty");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib statistic_is_not_selected select_benchmark_`
Expected: FAIL — `select_benchmark` not found.

- [ ] **Step 3: Implement**

```rust
/// Which statistic over the benchmark population becomes the bar.
///
/// The caller's choice, always. This replaced a hardcoded rule that switched
/// from "the single best segment" to "an interpolated 75th percentile" as a
/// dimension crossed eight segments — a bare literal with no justification,
/// which silently changed what the reported number meant mid-scan.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkStatistic {
    Median,
    P75,
    BestPeer,
}

/// R-7 (PERCENTILE.INC) quantile over pre-sorted values.
fn quantile_r7(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let h = (sorted.len() as f64 - 1.0) * q;
    let lo = h.floor() as usize;
    let hi = h.ceil() as usize;
    sorted[lo] + (h - lo as f64) * (sorted[hi] - sorted[lo])
}

fn select_benchmark(
    values: &[f64],
    direction: MeasureDirection,
    statistic: BenchmarkStatistic,
) -> (f64, String) {
    if values.is_empty() {
        return (0.0, "empty".into());
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match statistic {
        BenchmarkStatistic::Median => (quantile_r7(&sorted, 0.5), "median".into()),
        BenchmarkStatistic::P75 => {
            let q = match direction {
                MeasureDirection::HigherIsBetter => 0.75,
                MeasureDirection::LowerIsBetter => 0.25,
            };
            (quantile_r7(&sorted, q), "p75".into())
        }
        BenchmarkStatistic::BestPeer => {
            let v = match direction {
                MeasureDirection::HigherIsBetter => *sorted.last().unwrap(),
                MeasureDirection::LowerIsBetter => *sorted.first().unwrap(),
            };
            (v, "best_peer".into())
        }
    }
}
```

Delete `pick_benchmark`. Add `statistic: BenchmarkStatistic` to `opportunity` and `opportunity_drill`, threading it to the call site. In `src/cli/mod.rs`, add `--statistic` (clap `value_enum`, default `median`) to the `opportunity` and `drill` subcommands and pass it through.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib statistic_is_not_selected select_benchmark_`
Expected: PASS.

- [ ] **Step 5: Triage the metric-tree suite**

Run: `cargo test --lib metric_tree_ops`
Expected: **failures here are expected**. Tests built on the `>= 8` switch or on `best_peer` defaults now get `median`. For each: confirm the new value is correct by hand, update the assertion, and add a one-line comment naming the corrected behaviour — e.g. `// was best_peer via the deleted >=8 rule; median is now the explicit default`.

- [ ] **Step 6: Commit**

```bash
git add src/engine/metric_tree_ops.rs src/cli/mod.rs
git commit -m "Make the benchmark statistic explicit; delete the >=8 switch

The cardinality rule was a bare literal with no named constant, no test
asserting the boundary was intentional, and no justification. It changed what
the number meant as a dimension crossed eight segments.

statistic is now a required argument; the CLI defaults to median. A test
asserts cardinality does not select the statistic, so the rule cannot return."
```

---

### Task 5: `__opp_support__`, a distinct-entity count

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`augment_layer_for_opportunity`, name helper, breakdown selection)
- Test: same file

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `const SUPPORT_MEASURE_PREFIX: &str = "__opp_support__";`, `fn support_measure_name(measure: &str) -> String`, and `fn view_primary_entity_key(view: &View) -> Option<String>`. Task 6 reads `SegRow.support`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn augment_installs_a_distinct_entity_support_measure() {
    let mut layer = star_layer();
    assert!(augment_layer_for_opportunity(&mut layer, "sales.revenue"));
    let view = layer.views.iter().find(|v| v.name == "sales").unwrap();
    let name = support_measure_name("revenue");
    let m = view.measures_list().iter().find(|m| m.name == name).cloned()
        .expect("support measure must be installed");
    assert_eq!(m.measure_type, MeasureType::CountDistinct);
    assert!(m.expr.is_some(), "support measure needs the entity key expr");
}

#[test]
fn no_support_measure_without_a_primary_entity() {
    // No row identity means no honest support number. The floor must report
    // itself inapplicable rather than quietly counting rows instead.
    let mut layer = layer_without_primary_entity();
    augment_layer_for_opportunity(&mut layer, "solo.amount");
    let view = layer.views.iter().find(|v| v.name == "solo").unwrap();
    let name = support_measure_name("amount");
    assert!(view.measures_list().iter().all(|m| m.name != name));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib augment_installs_a_distinct_entity no_support_measure_without`
Expected: FAIL — `support_measure_name` not found.

- [ ] **Step 3: Implement**

```rust
/// Prefix for the synthetic distinct-entity-count measure backing `min_support`.
const SUPPORT_MEASURE_PREFIX: &str = "__opp_support__";

/// `measure` is a bare measure name, not a `view.measure` id.
fn support_measure_name(measure: &str) -> String {
    format!("{SUPPORT_MEASURE_PREFIX}{measure}")
}

/// The column expression for a view's primary entity key, if it has one.
///
/// Resolves the key to a backing dimension by name, then by `expr`, mirroring
/// `identifier_dimensions` and `sql_generator::resolve_join_key_expr`. Returns
/// `None` for a view with no `type: primary` entity — there is no row identity
/// to count, so `min_support` has nothing honest to measure.
fn view_primary_entity_key(view: &View) -> Option<String> {
    let entity = view
        .entities
        .iter()
        .find(|e| e.entity_type == EntityType::Primary)?;
    let key = entity.get_keys().into_iter().next()?;
    let backing = view
        .dimensions
        .iter()
        .find(|d| d.name == key)
        .or_else(|| view.dimensions.iter().find(|d| d.expr == key));
    Some(backing.map(|d| d.expr.clone()).unwrap_or(key))
}
```

At the end of `augment_layer_for_opportunity`, before `true`, following the `__opp_n__` block's shape exactly:

```rust
    // Support: how many distinct entities stand behind a segment. `min_support`
    // needs this and cannot use `SegRow.count`, which is populated only in rate
    // mode — so a ratio target, which is the common case, has no count at all.
    // Row count is the wrong quantity anyway: a one-store region has thousands
    // of order rows and would clear any sane floor.
    if let Some(key_expr) = view_primary_entity_key(view) {
        let s_name = support_measure_name(measure_name);
        if !view.measures_list().iter().any(|m| m.name == s_name) {
            view.measures.get_or_insert_with(Vec::new).push(Measure {
                name: s_name,
                measure_type: MeasureType::CountDistinct,
                expr: Some(key_expr),
                description: Some(format!(
                    "Internal: distinct entities backing {measure_name}'s segments, used to floor opportunity sizing."
                )),
                original_expr: None,
                filters: None,
                samples: None,
                synonyms: None,
                rolling_window: None,
                inherits_from: None,
                drivers: None,
                shift: None,
                direction: MeasureDirection::default(),
                meta: None,
            });
        }
    }
```

In `opportunity`, look the measure up by name (mirroring how `dispersion_measure` is resolved at `~:2560-2589`), add it to each breakdown query's measures when present, and extend `SegRow` with `support: Option<f64>` read from the returned alias.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib augment_installs_a_distinct_entity no_support_measure_without && cargo test --lib metric_tree_ops`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "Install __opp_support__, a distinct-entity count per segment

min_support needs a support number and SegRow.count cannot supply it: it is
populated only in rate mode, so a ratio target has none. Row count is also the
wrong quantity — a one-store region has thousands of order rows.

A view with no primary entity gets no support measure; the floor reports
itself inapplicable rather than silently measuring rows instead."
```

---

### Task 6: `min_support` on both sides, and `SkippedSegment`

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`min_support` param, benchmark population filter, subject filter, `SkippedSegment`, `DimensionOpportunity`)
- Modify: `src/cli/mod.rs` (`--min-support`)
- Test: same file

**Interfaces:**
- Consumes: `SegRow.support` (Task 5), `select_benchmark` (Task 4).
- Produces: `pub struct SkippedSegment { pub segment: String, pub reason: String }`; `DimensionOpportunity.skipped_segments: Vec<SkippedSegment>`; `min_support: usize` on `opportunity`/`opportunity_drill`.

- [ ] **Step 1: Write the failing test — the motivating regression**

```rust
#[test]
fn a_thin_segment_neither_is_sized_nor_sets_the_bar() {
    // THE regression test. One-store Oregon at an extreme rate vs
    // twenty-one-store California. Oregon is not the subject — it is the
    // BENCHMARK, because best_peer is the max. A subject-side floor alone
    // leaves this bug fully intact.
    let layer = support_layer();
    let tree = MetricTree::build(&layer);
    let exec = stub_executor_with_support(&[
        ("OR", 0.91, 1.0),
        ("CA", 0.30, 21.0),
        ("WA", 0.31, 2.0),
    ]);
    let res = opportunity(
        &tree, &layer, "sales.rate", "sales.day",
        ("2026-01-01", "2026-01-31"), &[], BenchmarkStatistic::BestPeer, 2, &exec,
    ).unwrap();
    let dim = res.dimensions.iter().find(|d| d.dimension == "sales.region").unwrap();

    let segs: Vec<&str> = dim.segments.iter().map(|s| s.segment.as_str()).collect();
    assert!(!segs.contains(&"OR"), "OR must not be sized as a subject");
    for s in &dim.segments {
        assert!(s.benchmark <= 0.31 + 1e-9, "OR must not set the bar, got {}", s.benchmark);
    }
    assert!(
        dim.skipped_segments.iter().any(|s| s.segment == "OR" && s.reason.contains("support")),
        "OR's refusal must be reported, not silent: {:?}", dim.skipped_segments
    );
}

#[test]
fn support_floor_counts_entities_not_rows() {
    // Distinguishes the two readings: many rows, one entity => refused.
    let layer = support_layer();
    let tree = MetricTree::build(&layer);
    let exec = stub_executor_with_support_and_rows(&[("OR", 0.91, 1.0, 5000.0), ("CA", 0.30, 21.0, 6000.0)]);
    let res = opportunity(
        &tree, &layer, "sales.rate", "sales.day",
        ("2026-01-01", "2026-01-31"), &[], BenchmarkStatistic::BestPeer, 2, &exec,
    ).unwrap();
    let dim = res.dimensions.iter().find(|d| d.dimension == "sales.region").unwrap();
    assert!(dim.skipped_segments.iter().any(|s| s.segment == "OR"));
}

#[test]
fn support_floor_reports_itself_inapplicable_without_an_entity_grain() {
    let layer = layer_without_primary_entity();
    let tree = MetricTree::build(&layer);
    let exec = stub_executor_no_support();
    let res = opportunity(
        &tree, &layer, "solo.amount", "solo.day",
        ("2026-01-01", "2026-01-31"), &[], BenchmarkStatistic::Median, 2, &exec,
    ).unwrap();
    let dim = res.dimensions.first().unwrap();
    assert!(
        dim.skipped_segments.iter().any(|s| s.reason.contains("no entity grain")),
        "must say the floor could not be applied rather than falling back to rows"
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib a_thin_segment_neither support_floor_`
Expected: FAIL — arity mismatch and `skipped_segments` not found.

- [ ] **Step 3: Implement**

Add:

```rust
/// A segment excluded from benchmarking, with the reason.
///
/// Refusals are reported rather than filtered away in silence. The reference
/// implementation this design is drawn from moved its guards out of the WHERE
/// clause into a reported column for exactly this reason: a store "simply did
/// not appear in the list and no screen said why".
#[derive(Debug, Clone, Serialize)]
pub struct SkippedSegment {
    pub segment: String,
    pub reason: String,
}
```

Add `pub skipped_segments: Vec<SkippedSegment>` to `DimensionOpportunity`, and `min_support: usize` to `opportunity`/`opportunity_drill`.

In the per-dimension body, **before** `select_benchmark`:

```rust
        let mut skipped_segments: Vec<SkippedSegment> = Vec::new();

        // Support gates BOTH sides. Gating subjects alone leaves the motivating
        // bug intact: a one-instance segment with an extreme rate is not the
        // subject, it is the MAX, so it still sets the bar for everyone else.
        let eligible: Vec<&SegRow> = seg_rows
            .iter()
            .filter(|s| match s.support {
                Some(n) if n < min_support as f64 => {
                    skipped_segments.push(SkippedSegment {
                        segment: s.segment.clone(),
                        reason: format!("support {n} below floor of {min_support}"),
                    });
                    false
                }
                None => {
                    skipped_segments.push(SkippedSegment {
                        segment: s.segment.clone(),
                        reason: "no entity grain to count support at; floor not applied".into(),
                    });
                    true // fail open: report, do not silently drop
                }
                _ => true,
            })
            .collect();

        let (benchmark, benchmark_basis) = select_benchmark(
            &eligible.iter().map(|s| s.cmp).collect::<Vec<_>>(),
            direction,
            statistic,
        );
```

`bench_row`, `benchmark_filter` and the sizing iterator all read `eligible` rather than `seg_rows`, so a refused segment is neither sized nor able to set the bar. Populate `skipped_segments` into the emitted `DimensionOpportunity`.

Add `--min-support` (default `2`) to the CLI subcommands.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib a_thin_segment_neither support_floor_`
Expected: PASS.

- [ ] **Step 5: Confirm the regression test is real**

Run: `git stash push -u -m "minsupport-verify-$$" && cargo test --lib a_thin_segment_neither; git stash list --format='%H %gs' | head`
Expected: the test FAILS or does not compile against the pre-change tree. Restore with `git stash apply <sha>` then drop that entry by tag. A regression test that passes before the fix is testing nothing.

- [ ] **Step 6: Triage and commit**

Run: `just test`, updating opportunity assertions that moved, each with a one-line reason comment.

```bash
git add src/engine/metric_tree_ops.rs src/cli/mod.rs
git commit -m "Apply min_support to the benchmark population, not just subjects

A one-instance segment with an extreme rate is not the subject — under
best_peer it is the max, so it SETS the bar. Filtering subjects alone leaves
the one-store-vs-21-store bug fully intact.

Support is a distinct-entity count and defaults to a floor of 2. Where no
entity grain resolves the floor reports itself inapplicable rather than
quietly falling back to rows. Refusals land in skipped_segments with a
reason."
```

---

### Task 7: Gate points

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`:2602`, `:3783`, `:4189`, `:4618`, `:4627`, `:4771`)
- Test: same file

**Interfaces:**
- Consumes: `Dimension::analysis_caps()` (Task 2).
- Produces: nothing downstream.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn benchmark_false_hides_a_dimension_from_opportunity_only() {
    // The whole point of splitting the capabilities: party_size must vanish
    // from the benchmark scan and remain available to explain.
    let layer = layer_with_party_size_not_benchmarkable();
    let opp_dims = benchmark_dimensions(&layer, "checks");
    assert!(!opp_dims.iter().any(|d| d.ends_with(".party_size")));
    let exp_dims = explain_dimensions(&layer, "checks");
    assert!(exp_dims.iter().any(|d| d.ends_with(".party_size")),
        "explain must still see it: splitting an observed drop by party size is legitimate");
}

#[test]
fn segmentable_false_still_hides_a_dimension_from_both() {
    let layer = layer_with_segmentable_false();
    assert!(!benchmark_dimensions(&layer, "sales").iter().any(|d| d.ends_with(".gender")));
    assert!(!explain_dimensions(&layer, "sales").iter().any(|d| d.ends_with(".gender")));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib benchmark_false_hides segmentable_false_still_hides`
Expected: FAIL — helpers not found; `party_size` still present.

- [ ] **Step 3: Implement**

Leave `discover_dimensions` and `is_segmentable` untouched — gating there would hit all six sites and break `explain`. Add two wrappers:

```rust
/// Dimensions eligible as a benchmark axis. Used only by `opportunity`'s scan.
fn benchmark_dimensions(layer: &SemanticLayer, view_name: &str) -> Vec<String> {
    filter_by_caps(layer, discover_dimensions(layer, view_name), |c| c.benchmark)
}

/// Dimensions eligible to decompose a change or a gap. Used by `explain`,
/// `decompose_to_searchable`, `opportunity_drill` and `dimension_candidates`.
///
/// Drill is gated here, not on `benchmark`: it splits an already-chosen gap by
/// a further dimension, and splitting a store-vs-peers gap by party mix is a
/// legitimate explanation, because mix shift is real. `benchmark: false` is a
/// statement about the scan, not about decomposition.
fn explain_dimensions(layer: &SemanticLayer, view_name: &str) -> Vec<String> {
    filter_by_caps(layer, discover_dimensions(layer, view_name), |c| c.explain)
}

fn filter_by_caps(
    layer: &SemanticLayer,
    dims: Vec<String>,
    keep: impl Fn(&DimensionAnalysis) -> bool,
) -> Vec<String> {
    dims.into_iter()
        .filter(|qualified| {
            let Some((v, d)) = qualified.split_once('.') else { return true };
            layer
                .views
                .iter()
                .find(|view| view.name == v)
                .and_then(|view| view.dimensions.iter().find(|dim| dim.name == d))
                .map(|dim| keep(&dim.analysis_caps()))
                .unwrap_or(true)
        })
        .collect()
}
```

Replace the call at `:2602` with `benchmark_dimensions(...)`, and the calls at `:3783`, `:4189`, `:4618`, `:4627`, `:4771` with `explain_dimensions(...)`. Note `:3783` and `:4189` take a `SharedLayer`/`&SemanticLayer` respectively — match the existing borrow at each site.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib benchmark_false_hides segmentable_false_still_hides && just test`
Expected: PASS. The existing `test_discover_dimensions_honors_segmentable_false` still passes untouched — `discover_dimensions` was not changed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "Gate analysis capabilities per call site, never in discover_dimensions

discover_dimensions has six production call sites: one benchmark scan and
five decomposition paths. Gating inside it — the obvious implementation —
would have hidden party_size from explain too, the exact breakage the
capability split exists to prevent.

Drill is gated on explain rather than benchmark: it decomposes an already
chosen gap, and mix shift is a legitimate explanation of one."
```

---

### Task 8: oxy endpoint, SDK and web-app

**Files:**
- Modify: `o3/Cargo.toml:591` (rev bump + changelog entry), `o3/crates/app/src/server/api/metric_tree.rs` (`OpportunityRequest:339`, `DrillRequest:1040`, `OpportunityResponse:388`)
- Modify: `o3/web-app/src/types/metricTree.ts`, `o3/sdk/typescript/src/metricTree.ts`
- Modify: `o3/web-app/src/pages/ide/WorldModel/components/{WorldModelOpportunitiesSection,WorldModelSegmentDrill,WorldModelSizedSegmentRow}.tsx`

**Interfaces:**
- Consumes: all prior tasks, via the airlayer rev bump.
- Produces: wire fields `statistic`, `min_support`, `direction`, `skipped_segments`.

- [ ] **Step 1: Bump the airlayer rev**

Merge and tag airlayer first. Then in `o3/Cargo.toml:591` update `rev = "..."`, and add a dated changelog entry above it per the convention at `:286-289` (restated at `:584-589`). The entry must say plainly that benchmark selection changed shape, that the `>= 8` heuristic is gone, and that an old client now gets `median` where it previously got p75/best_peer — a visible number change, not drift.

- [ ] **Step 2: Add the server fields**

`OpportunityRequest` and `DrillRequest` gain:

```rust
    #[serde(default)]
    pub statistic: Option<BenchmarkStatistic>,
    #[serde(default)]
    pub min_support: Option<usize>,
```

Both default server-side (`median`, `2`) so an old client keeps working. `OpportunityResponse` flattens the new `direction` and `skipped_segments` from `OpportunityResult`.

- [ ] **Step 3: Update all three TS copies in one commit**

Add the fields to `web-app/src/types/metricTree.ts` and `sdk/typescript/src/metricTree.ts`. `benchmark_basis` gains `"median"`:

```ts
export type BenchmarkStatistic = "median" | "p75" | "best_peer";
export interface SkippedSegment { segment: string; reason: string }
```

Add a shape-assertion test in the SDK — there is no codegen, the file's own `DriverForm` comment (`:14-20`) records a prior drift breaking a valid `.view.yml`, and the SDK's `OpportunityRequest` is *already* missing the `instance` field the server and web-app carry.

- [ ] **Step 4: Render the new fields**

`WorldModelOpportunitiesSection` renders `skipped_segments` (so a refused segment says why rather than vanishing) and switches improvement copy on `direction` — "reduce to" for `lower_is_better`, not the currently hardcoded lift wording. `WorldModelSizedSegmentRow` and `WorldModelSegmentDrill` handle `benchmark_basis === "median"`.

- [ ] **Step 5: Run both suites**

Run: `cargo test` in `o3`, then `npm test` in `web-app` and `sdk/typescript`.
Expected: component tests asserting rendered benchmark copy will fail; update each with a one-line reason comment.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/app/src/server/api/metric_tree.rs web-app sdk/typescript
git commit -m "Carry benchmark statistic, min_support and direction through the API

Bumps airlayer. Old clients keep working and now get median where they
previously got p75 or best_peer — a visible number change, called out in the
changelog rather than left to be discovered.

Adds an SDK shape-assertion test: three hand-maintained mirrors with no
codegen, and the SDK is already missing OpportunityRequest.instance."
```

---

## Self-Review

**Spec coverage.** §4 polarity → Tasks 1, 3. §5 `min_support` both sides → Task 6; §5.1 entity-count and the no-entity case → Tasks 5, 6. §6 statistic → Task 4. §7 per-segment refusals → Task 6. §8 capability set → Tasks 2, 7; §8.1 gate points → Task 7; §8.2 alias and method-not-mutation → Task 2. §9 live consumers → Task 8. §10 testing → distributed. §11 phasing → task order.

**Two spec items deliberately not tasked:** §12's open questions (per-view `direction` default; whether `min_support` applies at each drill level or only the root) stay open — the second needs reading `opportunity_drill`'s inheritance with the code in hand, and is flagged in Task 6's triage step. §9's audit of existing customer `.view.yml` files for cost-like measures currently sized backwards is a pre-ship check, not a code change.

**Placeholders:** none. Every code step carries real code. Fixture helpers (`support_layer`, `stub_executor_with_support`, `lower_is_better_layer`) are named but not written out — each task says to follow the existing fixture style in that module, which is the honest instruction since those helpers must match conventions the executor will have in front of them.

**Type consistency:** `MeasureDirection` (T1) is consumed by T3, T4, T5's `Measure` literal. `select_benchmark` (T4) supersedes T3's `pick_benchmark` — deliberate and noted in T4's Interfaces. `SegRow.support: Option<f64>` (T5) is read by T6. `analysis_caps()` (T2) is called by T7's `filter_by_caps`. `BenchmarkStatistic` (T4) reaches the wire in T8.

**Ordering constraint:** Task 3 before Task 4 (both touch the same function; splitting keeps each reviewable). Task 5 before Task 6 (the floor needs the support number). Task 4 before Task 6 (the floor filters the population `select_benchmark` reads).

---

### Task 9: Resolve support at the dimension's grain

**Added after the final whole-branch review.** The support measure counted the *target
measure's* view's primary entity, which on a transaction-grain fact view is the row surrogate
— exactly the row count §5.1 rejects. The floor was therefore inert on the star schema it was
designed for. Spec §5.1 has been corrected; this task makes the code match it.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (`view_primary_entity_key`, `augment_layer_for_opportunity`, the support lookup in `opportunity`)
- Test: same file, plus `tests/integration_tests.rs` for the DuckDB star-schema test

**Interfaces:**
- Consumes: `SegRow.support: Option<f64>` and the `min_support` floor (Task 6) — both unchanged.
- Produces: `view_primary_entity_key(view: &View)` unchanged in signature but called per *owning* view; one `__opp_support__` measure per distinct owning view in the scan.

- [ ] **Step 1: Write the failing test — the one that would have caught this**

In `tests/integration_tests.rs`, against DuckDB, following the existing DuckDB test shape. A
star schema: `sales` (primary entity on a row surrogate, a `revenue` sum) joined to `stores`
(primary entity `store_id`, a `region` dimension). Seed one region with a single store and
many rows, another with several stores.

```rust
/// The support floor must count STORES, not rows. This is the test whose absence let the
/// target-view/dimension-view confusion survive seven tasks: every other support test
/// injects the number through a stub instead of running the generated measure.
#[test]
fn opportunity_support_counts_entities_of_the_dimensions_view_not_fact_rows() {
    // one-store region with many rows must be refused; multi-store region must survive
}
```

Assert the single-store region appears in `skipped_segments` with a support reason, and does
not set the benchmark.

- [ ] **Step 2: Run it — expect failure for the RIGHT reason**

Run: `cargo test --features exec-duckdb opportunity_support_counts_entities -- --nocapture`
Expected: FAIL because the single-store region's support is its *row* count and clears the
floor. Confirm that is the reason, not a fixture or compile error.

- [ ] **Step 3: Resolve the key per owning view**

`view_primary_entity_key` keeps its shape; the caller changes. In
`augment_layer_for_opportunity`, install one support measure per distinct owning view among
the scanned dimensions, expressed on the target's view as a cross-view ref so the join is
pulled in:

```rust
// COUNT(DISTINCT {{<owning_view>.<key>}}) — expand_views_for_expr_refs resolves the ref
// and adds the join (verified by spike; see spec §5.1).
```

The per-dimension breakdown query selects the support measure matching that dimension's
owning view. Where the owning view has no primary entity or a composite key, install nothing
and let the floor report itself inapplicable, exactly as today.

- [ ] **Step 4: Run the test and the suite**

Run: `cargo test --features exec-duckdb opportunity_support_counts_entities` then `just test`
Expected: PASS. `sales.channel` (a fact-view dimension) must still behave as before —
support degenerates to the row count, which §5.1 states is correct.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs tests/integration_tests.rs docs/superpowers/specs
git commit -m "Count support at the dimension's grain, not the target's

The support measure counted the target measure's view's primary entity, which
on a transaction-grain fact view is the row surrogate — the row count the
floor exists to reject. The pathology lives on dimensions above an entity
grain, which belong to a different view.

Adds the test that would have caught it: one that runs the generated measure
against a real star schema instead of injecting support through a stub."
```
