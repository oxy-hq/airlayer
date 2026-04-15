# Deep RCA: Multi-Strategy Beam Search for Explain

**Date:** 2026-04-15
**Status:** Draft

## Problem

The current `explain` algorithm is a greedy single-path search that fails on 9 documented pathological cases (tests in `src/engine/metric_tree_ops.rs::test_pathological_*`). The failures fall into three categories:

1. **Scoring failures** — JSD-based dimension ranking picks the wrong dimension when a high-cardinality shuffly dimension beats a low-cardinality concentrated one (tests 2, 5).
2. **Search failures** — greedy single-path misses better deep paths, cross-cutting dimensions, and multi-dimensional interactions (tests 1, 6, 7).
3. **Structural failures** — the decomposition model cannot handle uniform degradation, Simpson's paradox (mix shift), or masked opposing offsets (tests 3, 4, 8, 9).

## Design Goals

- Address all 9 pathological cases unambiguously.
- Two-tier architecture: fast pass (current algorithm, unchanged) + deep pass (`--deep` flag).
- Deep pass output: ranked list of independent explanation paths, each with statistical significance.
- Detection heuristics for patterns that no search algorithm can decompose (Simpson's, opposing offsets).

## Expected Outcome Per Pathological Case

| # | Test | Expected Deep-Pass Behavior |
|---|------|-----------------------------|
| 1 | Checkerboard interaction | Multi-dim pair strategy (Strategy 5) surfaces (Android, EU) and (iOS, US) cells directly. Cross-cutting detection in Phase 3 may also flag if the same pattern appears across components. |
| 2 | JSD distraction | Max-concentration strategy (Strategy 1) proposes plan=Enterprise at 0.95 concentration. It outranks source's 0.50 in the beam. Statistical significance further demotes source elements (normal variance) while Enterprise's drop is significant. |
| 3 | Simpson's paradox | Phase 4 detection heuristic fires: all device segments moved opposite to aggregate. Warning emitted. No dimension split is attempted for this pattern — it's flagged as a mix-shift effect. |
| 4 | Death by thousand cuts | Adaptive EP threshold allows high-cardinality elements through. Uniform-degradation detection fires: no single product is surfaced. Instead, a `UniformDegradation { dimension: "product", num_elements: 200 }` split is emitted, telling the user the drop is systemic, not attributable to any single product. |
| 5 | Decoy high-cardinality | Same as test 2: max-concentration strategy proposes plan=Enterprise at 0.90, outranking user_id's 0.065 in the beam. Statistical significance confirms Enterprise's drop is abnormal; individual user deltas are within normal variance. |
| 6 | Component hides cross-cutting dim | Decompose-then-search runs dimensional beam on both ads and subs independently. Phase 3 cross-cutting detection finds region=EU appears in both with combined root_fraction near 1.0. A `CrossCutting { dimension: "region", value: "EU", measures: ["ads.revenue", "subs.revenue"] }` explanation is emitted, ranked above the individual component paths. |
| 7 | Greedy shallow winner | Beam explores both comp_a and comp_b paths. comp_b → seg_critical achieves root_fraction 0.43, outranking comp_a → seg_1 at 0.22. The beam surfaces the better path. |
| 8 | Concentration threshold cliff | Adaptive EP threshold (scaled by √cardinality) lets elements through. If individual elements still don't pass, uniform-degradation detection fires, reporting the collective pattern rather than individual categories. No min_concentration gate in deep beam prevents premature stopping. |
| 9 | Opposing offsets | Phase 4 opposing-offset heuristic fires: rev.amount (-100) is partially offset by cost.amount (-200 with sign -1). Warning emitted identifying the masking relationship. The deep search still finds the cost path, but the warning alerts the user that EU revenue decline is hidden. |

## Architecture: Two-Tier

### Fast Pass (default)

The current greedy algorithm, unchanged. Single-path, O(components + dims) queries per level. Populates `ExplainResult.nodes`. Backward-compatible.

### Deep Pass (`--deep`)

Multi-strategy beam search. Runs after the fast pass. Uses a decompose-then-search architecture:

1. **Phase 1: Tree Decomposition** — graph traversal + aggregate queries.
2. **Phase 2: Per-Measure Dimensional Beam Search** — bulk of query cost.
3. **Phase 3: Global Merge & Rank** — cross-cutting detection, final ranking.
4. **Phase 4: Detection Heuristics** — Simpson's, opposing offsets. Zero extra queries.
5. **Phase 5: Statistical Significance** — t-test against historical data. One query per top candidate.

## Phase 1: Tree Decomposition

Walk component edges from the target measure to all leaf measures, tracking sign and path at each edge. Collect the set of **searchable measures**: all leaves, plus any intermediate composite that has its own dimensions.

Query each searchable measure's aggregate delta (one query each). Compute each measure's `leaf_share`:

```
leaf_share = (delta × cumulative_sign) / target_delta
```

Where `cumulative_sign` is the product of edge signs along the path from root to this measure.

**Cost:** O(searchable_measures) queries. Typically 3-8.

## Phase 2: Per-Measure Dimensional Beam Search

For each searchable measure independently:

### Query Phase

Fetch all dimension breakdowns: one query per available dimension. Results are stored in the query cache and shared across all scoring strategies.

### Scoring Strategies

All strategies operate on the same cached breakdown data. Each returns a ranked list of candidates.

**Strategy 1: Max-Element Concentration**
- For each dimension, rank elements by `|concentration|` descending.
- Dimension score = top element's concentration.
- Directly optimizes for "which single split explains the most."

**Strategy 2: Top-K Concentration Sum**
- Dimension score = sum of top-3 elements' `|concentration|`.
- Captures "a few big movers" where no single element dominates.

**Strategy 3: JSD Surprise**
- Current algorithm's JSD computation, unchanged.
- Laplace smoothing applied: add `ε = 1 / (total_prev + total_curr)` to all shares before computing JSD.

**Strategy 4: IV/WOE (Information Value / Weight of Evidence)**
- For each element: `WOE_i = ln(q_share_i / p_share_i)` where shares use Laplace smoothing.
- For each dimension: `IV = Σ (q_share_i - p_share_i) × WOE_i`.
- Dimension score = IV. Top element = highest `|WOE|` element with `|EP|` above noise threshold.

**Strategy 5: Multi-Dim Pairs** (conditional)
- Only activates when no single-dimension candidate achieves concentration > 0.60 after the first expansion level.
- For the top-k single dimensions by concentration (k=3), query all pairwise GROUP BY (dim_a, dim_b).
- Score each cell by concentration within the pair.
- Catches interaction effects invisible to single-dimension evaluation.
- Cost: up to k-choose-2 = 3 extra queries per level when activated.

*Total per-measure strategies: 5 (Strategies 1-5), with Strategy 5 conditional. Cross-component detection operates in Phase 3, not as a per-measure strategy.*

### Beam Mechanics

- **Width:** W (default 10) per searchable measure.
- **Initialization:** Top W candidates across all strategies, deduplicated by (dimension, value).
- **Expansion:** For each beam entry, apply accumulated filters, query remaining dimensions, score with all strategies, produce next-level candidates. All candidates enter a shared pool; top W survive after deduplication.
- **Deduplication:** Two beam entries with identical (measure, filter_set) are equivalent. Keep the one with higher root_fraction.
- **Termination:** A beam entry terminates (becomes a completed path) when:
  - No candidates above adaptive EP threshold
  - Root fraction below `min_root_fraction`
  - No remaining dimensions to split
- **No min_concentration gate** in deep beam. Weak candidates survive and compete; only beam width constrains exploration.

### Adaptive EP Threshold

Base `MIN_ELEMENT_EP = 0.05` (same as current). Scaled by cardinality:

```
effective_threshold = 0.05 / sqrt(num_elements)
```

- 2 elements → 0.035
- 20 elements → 0.011
- 200 elements → 0.0035

Preserves noise filtering for low-cardinality dimensions while allowing uniform patterns through.

### Uniform Degradation Detection

After filtering elements by adaptive EP, if no element passes the threshold AND the sum of all elements' `|concentration|` > 0.50:

- Emit a `UniformDegradation { dimension, num_elements }` candidate directly to the completed paths list (bypasses the beam — there is nothing to recurse into).
- This is a leaf explanation. It tells the user: "the drop is evenly distributed across all N values of this dimension — no single value is responsible."

## Phase 3: Global Merge & Rank

Collect all completed paths from all per-measure beam searches. For each path:

```
root_fraction = leaf_share × path_concentration
```

Where `path_concentration` is the product of concentrations along the path's dimension splits.

### Cross-Cutting Detection (Strategy 6)

After all per-measure beams complete, scan completed paths for dimension values that appear across multiple measures. Group paths by each (dimension, value) pair in their accumulated filters. For each dimension value that appears across 2+ measures:

```
combined_root_fraction = Σ root_fraction across all measures containing this filter
```

If `combined_root_fraction` exceeds any individual path's root_fraction, emit a `CrossCutting` explanation:

```
CrossCutting {
    dimension: "region",
    value: "EU",
    measures: ["ads.revenue", "subs.revenue"],
    root_fraction: 1.0,  // combined
}
```

### Final Ranking

Sort all explanations (individual paths + cross-cutting + uniform degradation) by root_fraction descending. Return top `max_alternatives`.

## Phase 4: Detection Heuristics

Run on every `explain` call (not just `--deep`). Zero extra queries — uses data already fetched.

### Simpson's Paradox

After evaluating dimension breakdowns, for each dimension:

```
element_deltas = [elem.delta for elem in breakdown]
if all(sign(d) != sign(parent_delta) for d in element_deltas where |d| > epsilon):
    emit SimpsonsParadox warning
```

### Opposing Offsets

After computing component deltas (Phase 1 or fast pass), for each component pair (A, B) where `sign(A.delta) != sign(B.delta)`:

```
masking_ratio = min(|A.delta|, |B.delta|) / max(|A.delta|, |B.delta|)
if masking_ratio > 0.3:
    emit OpposingOffset warning
```

## Phase 5: Statistical Significance

Runs after Phase 3 ranking, only in deep mode. Tests whether each candidate's delta is statistically significant relative to historical variance.

### Method

For each of the top-K paths (K = max_alternatives):

1. Issue one wide-range query per terminal (measure, dimension, filters) combination: 12 months of monthly granularity.
2. Extract per-period deltas for the relevant segment.
3. Compute a two-tailed t-test:

```
t_stat = (current_delta - mean_historical) / (std_historical / sqrt(n))
p_value = 2 × (1 - t_cdf(|t_stat|, df = n - 1))
```

4. Annotate the path with p_value.

### Gating

- Requires `historical_periods >= 6`. Below that, skip significance testing and note in output.
- The significance test demotes but does not remove paths. A path with p_value > 0.05 is flagged as "not statistically significant" but still shown — the user decides.

### Query Cost

One query per top-K candidate. With K=5, that's 5 additional queries.

## Data Types

```rust
pub enum SplitKind {
    Component { child_measure: String },
    Dimension { dimension: String, value: String },
    UniformDegradation { dimension: String, num_elements: usize },
    CrossCutting { dimension: String, value: String, measures: Vec<String> },
}

pub struct ExplainPath {
    pub nodes: Vec<ExplainNode>,
    pub root_fraction: f64,
    pub strategy: String,
    pub significance: Option<SignificanceTest>,
}

pub struct SignificanceTest {
    pub p_value: f64,
    pub historical_periods: usize,
    pub historical_mean_delta: f64,
    pub historical_std_delta: f64,
}

pub enum ExplainWarning {
    SimpsonsParadox {
        dimension: String,
        aggregate_delta: f64,
        segment_directions: Vec<(String, f64)>,
    },
    OpposingOffset {
        component_a: String,
        component_b: String,
        delta_a: f64,
        delta_b: f64,
    },
}

pub struct ExplainResult {
    pub target: String,
    pub target_delta: f64,
    pub target_previous: f64,
    pub target_current: f64,
    pub time_dimension: String,
    pub current_period: (String, String),
    pub previous_period: (String, String),
    pub nodes: Vec<ExplainNode>,            // fast pass (always populated)
    pub coverage: f64,
    pub driver_attribution: Vec<DriverAttribution>,
    pub alternatives: Vec<ExplainPath>,     // deep pass (empty unless --deep)
    pub warnings: Vec<ExplainWarning>,      // always populated
}

pub struct ExplainConfig {
    pub coverage_threshold: f64,     // 0.80
    pub max_depth: usize,            // 10
    pub max_dim_values: usize,       // 20
    pub min_concentration: f64,      // 0.05 (fast pass only)
    pub min_root_fraction: f64,      // 0.005
    pub deep: bool,                  // false
    pub beam_width: usize,           // 10
    pub max_alternatives: usize,     // 5
}
```

## CLI Interface

```
airlayer explain <measure> --time <dim> --current start:end --previous start:end [--deep] [--json]
    [--beam-width N]         # default 10, only with --deep
    [--max-alternatives N]   # default 5, only with --deep
```

### Text Output (--deep)

```
═══ Explain: revenue.arr ═══════════════════════════════

  Target: revenue.arr
  Delta:  -24,000 (120,000 → 96,000)
  Period: 2024-02 vs 2024-01

─── Fast Pass ───────────────────────────────────────────

  revenue.arr
  └── [component] revenue.net_mrr  delta: -2,000  (100%)
      └── [component] revenue.churned_mrr  delta: +2,400  (120%)
          siblings: revenue.new_mrr (-200, 10%), revenue.expansion_mrr (+100, -5%)

─── Alternative Explanations (deep) ─────────────────────

  #1  revenue.churned_mrr → churned_mrr.plan=Enterprise
      coverage: 0.95   p-value: 0.003

  #2  [cross-cutting] region=EU across ads.revenue, subs.revenue
      coverage: 1.00   p-value: 0.001

  #3  revenue.plan=Enterprise
      coverage: 0.87   p-value: 0.008

  #4  [uniform] revenue.product (200 products, none dominant)
      coverage: 0.96   p-value: n/a

  #5  revenue.source=src_1
      coverage: 0.50   p-value: 0.42 (not significant)

─── Warnings ────────────────────────────────────────────

  ⚠ Simpson's paradox on sales.device: all segments improved
    individually but aggregate declined (likely mix-shift)

  ⚠ Opposing offset: rev.amount (-100) partially masked by
    cost.amount savings (+200)
```

### JSON Output (--json)

Same `ExplainResult` struct serialized. `alternatives` is empty when `--deep` is not used. `warnings` is always populated.

## Query Optimization: GROUPING SETS

The key insight: all single-dimension breakdowns for a given (measure, filters) pair query the same base table with the same filters — they differ only in the GROUP BY clause. Rather than issuing D separate queries for D dimensions, use `GROUPING SETS` to get all breakdowns in a single round trip.

### Single-dimension breakdowns (1 query instead of D)

```sql
SELECT
    GROUPING_ID(dim_a, dim_b, dim_c) AS grp_id,
    dim_a, dim_b, dim_c,
    time_dim_month,
    SUM(measure) AS measure_value
FROM ...
WHERE <time_filter> AND <accumulated_filters>
GROUP BY GROUPING SETS (
    (time_dim_month, dim_a),
    (time_dim_month, dim_b),
    (time_dim_month, dim_c)
)
ORDER BY time_dim_month
```

Each grouping set produces rows for one dimension's breakdown. The `GROUPING_ID` (or NULL pattern in the non-active columns) identifies which set each row belongs to. Client-side, split the result by grouping set and feed each to the scoring strategies.

### With pairs (Strategy 5 activated, 1 query instead of D + k-choose-2)

```sql
GROUP BY GROUPING SETS (
    -- singles
    (time_dim_month, dim_a),
    (time_dim_month, dim_b),
    (time_dim_month, dim_c),
    -- pairs (top-k by single-dim concentration)
    (time_dim_month, dim_a, dim_b),
    (time_dim_month, dim_a, dim_c),
    (time_dim_month, dim_b, dim_c)
)
```

Singles + pairs in one round trip. Strategy 5's pair data is available immediately without a second query phase.

### Dialect support

GROUPING SETS is supported by all dialects airlayer targets:

| Dialect | Support |
|---------|---------|
| Postgres | Yes (9.5+) |
| Snowflake | Yes |
| BigQuery | Yes |
| Databricks | Yes |
| ClickHouse | Yes |
| DuckDB | Yes |
| MySQL | No — use UNION ALL of individual GROUP BYs as fallback |
| SQLite | No — fall back to individual queries |
| Presto/Trino | Yes |

**Fallback for unsupported dialects:** Issue individual queries per dimension, same as the fast pass does today. The query cache still deduplicates across beam entries.

### Implementation

The GROUPING SETS query bypasses the normal `QueryRequest` → `SemanticEngine::compile_query` pipeline since `QueryRequest` doesn't support grouping sets. Instead, `explain` constructs the SQL directly using the engine's existing helpers for table resolution, filter compilation, and measure expression expansion. This is scoped to the explain deep pass only — no changes to the general query API.

The function signature:

```rust
fn make_grouping_sets_query(
    engine: &SemanticEngine,
    measure: &str,
    time_dimension: &str,
    period_start: &str,
    period_end: &str,
    single_dims: &[String],       // all available dimensions
    pair_dims: Option<&[(String, String)]>,  // optional pairs for Strategy 5
    filters: &[QueryFilter],
) -> String  // raw SQL
```

Returns raw SQL. The executor runs it and the explain code parses the GROUPING_ID to split results.

### Impact on query count

Each unique (measure, filter_set) combination now requires exactly **1 query** for all single-dimension breakdowns (+ optionally pairs), instead of D queries. The query cache key becomes `(measure, "grouping_sets", filter_set)`.

## Query Cost (revised)

### Fast Pass (unchanged)

O(C + D) per level × depth. Typically 15-50 queries. The fast pass does NOT use GROUPING SETS — it stays backward-compatible with the existing query path.

### Deep Pass

| Phase | Cost | Notes |
|-------|------|-------|
| Phase 1: Decomposition | O(searchable_measures) | Typically 3-8 aggregate queries |
| Phase 2: Beam search | O(unique (measure, filter_set) combos) | 1 GROUPING SETS query per unique combo. Cache deduplicates convergent beams. |
| Phase 3: Merge | 0 | Pure computation |
| Phase 4: Heuristics | 0 | Uses cached data |
| Phase 5: Significance | O(max_alternatives) | 1 wide-range query per top candidate |

**Estimated totals with GROUPING SETS:**

| Scenario | Fast | Deep | Total |
|----------|------|------|-------|
| Simple (1 view, 5 dims) | ~15 | ~10-15 | ~25-30 |
| Medium (3 views, 10 dims) | ~40 | ~20-40 | ~60-80 |
| Complex (5 views, 15 dims) | ~60 | ~40-80 | ~100-140 |

The deep pass is now comparable in query count to the fast pass, since most of the cost was redundant per-dimension queries that GROUPING SETS eliminates.

## Query Cache

HashMap keyed by `(measure, query_type, Vec<QueryFilter>)` → parsed results. `query_type` is either `"aggregate"`, `"grouping_sets"`, or `"significance"`. Scoped to the explain call. Shared across:

- Fast pass and deep pass (fast pass aggregate results pre-warm the cache)
- All beam entries (convergent paths get cache hits)
- All scoring strategies (same GROUPING SETS result, different math)

## Laplace Smoothing

Applied to all distributional metrics (JSD, IV/WOE):

```
ε = 1.0 / (total_prev + total_curr)
smoothed_share_prev_i = (prev_i + ε) / (total_prev + ε × num_elements)
smoothed_share_curr_i = (curr_i + ε) / (total_curr + ε × num_elements)
```

Prevents division by zero for new/disappeared segments. Scales with data magnitude so ε is negligible for large segments.
