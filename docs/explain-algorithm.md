# Explain Algorithm

The `explain` command performs recursive root-cause analysis on a metric change between two time periods. It answers: **why did this metric change?**

Given a target measure and two time periods, explain decomposes the change into the smallest `(component, segment)` pairs that account for it. The output is a tree of splits — each level narrows the explanation by either decomposing a composite metric into its children (component split) or segmenting by a dimension value (dimension split).

## Two-pass architecture

Explain runs two passes:

1. **Fast pass (greedy)** — default. Single-path depth-first descent. Picks the locally best split at each level. Fast (O(dims × depth) queries), but can miss non-obvious causes.

2. **Deep pass (beam search)** — enabled with `--deep`. Explores multiple paths simultaneously using 4 independent scoring strategies. Slower (O(beam_width × dims × depth) queries), but surfaces alternative explanations the greedy pass misses.

Both passes produce the same output format. The fast pass populates the main result tree; the deep pass appends ranked alternatives.

## Fast pass: greedy descent

### Entry point

```
explain(target, time_dimension, current_period, previous_period)
```

1. Query the target measure's aggregate value in both periods → `target_delta`
2. Pre-compute available dimensions for every view (string, number, boolean — not time)
3. Call `recurse()` starting at the target

### Recursive search (`recurse`)

At each level, the algorithm:

1. **Evaluate candidates** via `evaluate_candidates()` (see below)
2. **Check stopping conditions:**
   - Depth ≥ `max_depth` (default 10)
   - Coverage ≥ `coverage_threshold` (default 80%)
   - Best candidate concentration < `min_concentration` (default 5%)
   - Root fraction < `min_root_fraction` (default 0.5%)
3. **Emit a node** for the best candidate, with siblings for context
4. **Recurse** into only the best candidate (depth + 1)

At the **top level**, the algorithm emits multiple candidates and accumulates coverage. At non-top levels, only the single best candidate is explored.

### Candidate evaluation (`evaluate_candidates`)

This is the core decision function. It considers two types of splits and picks the type with the higher maximum concentration.

#### Component candidates

For composite metrics (e.g., `arr = net_mrr × 12`), query each child component's aggregate delta. Components are scored by **concentration**:

```
concentration_i = (child_delta_i × edge_sign × parent_sign) / |parent_delta|
```

This measures what fraction of the parent's change is attributable to this child. A scaling factor like `× 12` is stripped out via normalization against `total_attributed = Σ(child_delta × sign)`.

#### Dimension candidates (Adtributor-style)

For each available dimension, query the GROUP BY breakdown to get per-segment values. Each segment is scored on two axes:

**Explanatory power (EP):** What fraction of the total change did this segment contribute?

```
EP_i = delta_i / parent_delta
```

EP ranges from −∞ to +∞. An EP of 0.90 means the segment explains 90% of the parent's change. Negative EP means the segment moved opposite to the aggregate.

**JSD surprise:** How much did this segment's share of the total shift between periods?

```
p_i = previous_i / total_previous    (prior share)
q_i = current_i / total_current      (posterior share)
surprise_i = JSD(p_i, q_i)           (Jensen-Shannon divergence element)
```

JSD is a symmetric measure of distributional shift. A segment that was 30% of revenue and is now 65% has high JSD regardless of whether the absolute amount changed.

#### Dimension ranking

Dimensions are ranked by **accumulated surprise** — the sum of JSD surprise across all segments with |EP| ≥ 5% (the noise threshold). This is the Adtributor algorithm: the dimension whose significant segments show the most unexpected distributional shift wins.

Within the winning dimension, segments are ranked by **concentration** (= EP) for recursion ordering.

#### Component vs. dimension decision

The split type with the higher **maximum concentration** wins:

- `comp_max` = concentration of the largest component
- `dim_max` = EP of the winning dimension's top segment

If `comp_max ≥ dim_max`, emit component candidates. Otherwise, emit dimension candidates.

This means surprise is used to choose *which* dimension, but concentration is used to choose *whether* to split by dimension or component.

## Deep pass: beam search

Enabled with `--deep`. Runs after the fast pass and appends ranked alternatives to the result.

### Phase 1: Decompose composite metrics

Walk component edges top-down from the target to find **searchable measures** — measures with non-empty dimension sets. Each leaf measure (no component children) or intermediate composite with its own dimensions is included. The target itself is excluded.

A `cumulative_sign` tracks the product of edge signs from root to each measure (e.g., `+1` for additive components, `-1` for subtractive).

### Phase 2: Per-measure beam search

For each searchable measure, run `beam_search_measure()`:

**Seeding:** Evaluate all dimensions × 4 strategies → up to `4 × |dims|` candidates. Keep the top `beam_width` (default 10) by root_fraction.

**Iteration:** For each depth level, process each beam entry:
1. Query the current (measure, filters) delta
2. Emit the current path as a completed alternative
3. Evaluate remaining dimensions × 4 strategies for next level
4. Deduplicate by `(measure, filter_set)` — highest root_fraction wins
5. Truncate to `beam_width`

Each path's `root_fraction` is multiplied by `leaf_share` — the measure's contribution to the root delta.

### The 4 scoring strategies

Each strategy picks one segment per dimension, producing diverse candidates:

| # | Strategy | Picks | Use case |
|---|----------|-------|----------|
| 1 | **Max concentration** | Segment with highest signed `delta / parent_delta` | Single dominant cause |
| 2 | **Max \|concentration\|** | Segment with highest absolute concentration | Opposing movements (some segments up, others down) |
| 3 | **JSD smoothed** | Segment with highest Laplace-smoothed JSD surprise, filtered by adaptive EP threshold | Unexpected share shifts (controls for noise) |
| 4 | **IV/WOE** | Segment with highest \|WOE\| where `WOE = ln(current_share / previous_share)` | Dramatic share multipliers (segment doubled, halved, vanished) |

**Laplace smoothing** (strategies 3 and 4): Adds `ε = 1 / (total_prev + total_curr)` to each element's share to handle zero-share segments without division by zero.

**Adaptive EP threshold** (strategy 3): `0.05 / √(num_elements)` — scales down for high-cardinality dimensions so valid segments aren't filtered as noise.

### Phase 3: Cross-cutting detection

After all per-measure beam searches complete, look for the same `(dimension_name, value)` appearing in multiple measures' paths. If "region=EU" appears in both `product_revenue` and `service_revenue` explanations, emit a special cross-cutting node listing the affected measures.

Bare dimension names are compared (after last `.`), so `ads.region` and `subs.region` are treated as the same dimension.

### Phase 4: Sort, truncate, significance test

1. Sort all paths by root_fraction descending
2. Truncate to `max_alternatives` (default 5)
3. For each top path, query 12 months of historical monthly data before the previous period. Compute month-to-month deltas and run a Student's t-test against the current delta. Store the p-value and significance flag.

## Detection heuristics

These run on every explain call (both fast and deep passes):

### Simpson's paradox

Checks if all segments within a dimension moved opposite to the aggregate. Example: every region's revenue increased, but total revenue decreased (due to mix shift). Emits a warning.

### Opposing offsets

Checks component children for two components with deltas that substantially cancel. If `|min(|a|, |b|) / max(|a|, |b|)| > 0.3`, warns that the net change masks internal movements. Example: product revenue +$500 and service revenue −$600, net −$100.

### Uniform degradation (deep pass only)

During beam search, if no single segment exceeds the adaptive EP threshold but the dimension collectively explains significant delta, emit a "uniform degradation" node. This means the decline is spread evenly across all segments — there's no single segment to blame.

## Scoring formulas

### Jensen-Shannon Divergence (per element)

```
JSD(p, q) = 0.5 × [p × ln(p/m) + q × ln(q/m)]
where m = (p + q) / 2
```

Symmetric. Zero when `p = q`. Higher when shares diverge.

### Weight of Evidence (WOE)

```
WOE_i = ln(current_share_i / previous_share_i)
```

Positive when share increased, negative when decreased. Magnitude reflects the multiplier. With Laplace smoothing, vanishing segments get large negative WOE but not infinity.

### Information Value (IV)

```
IV = Σ (current_share_i - previous_share_i) × WOE_i
```

Aggregate measure of distributional shift across all segments. Used in the beam search strategy ranking.

### Concentration / Explanatory Power

```
concentration_i = delta_i / parent_delta
```

The fraction of the parent's change attributable to this segment. Can exceed 1.0 if opposing flows exist (e.g., segment dropped by more than the total because other segments grew to partially offset).

### Root fraction

```
root_fraction = Π(parent_share_k) for k in path from root to node
```

Multiplicative cascade. A node at depth 3 with parent shares 0.50, 0.80, 1.0 has root_fraction = 0.40. This represents the fraction of the original metric change explained by this specific path.

## Configuration

| Parameter | Default | Effect |
|-----------|---------|--------|
| `coverage_threshold` | 0.80 | Stop greedy recursion when 80% of the change is explained |
| `max_depth` | 10 | Maximum recursion depth |
| `max_dim_values` | 20 | Truncate dimension breakdown to top 20 segments |
| `min_concentration` | 0.05 | Require best candidate to explain ≥5% of parent delta |
| `min_root_fraction` | 0.005 | Require ≥0.5% of root delta to recurse |
| `deep` | false | Enable beam search (--deep flag) |
| `beam_width` | 10 | Beam entries per level (--beam-width) |
| `max_alternatives` | 5 | Top-K alternatives from beam search (--max-alternatives) |

## Known limitations

These are documented in the pathological test suite (`src/engine/metric_tree_ops.rs`, Cases 10-18):

| Limitation | Description | Test case |
|------------|-------------|-----------|
| **JSD red herring** | Proportion swaps (30%↔65%) produce enormous JSD despite being a shuffle, not a root cause. Greedy picks the high-JSD dimension over the high-concentration one. | Case 12 |
| **Micro-segment JSD inflation** | Many vanishing micro-segments accumulate large total JSD, drowning out a single high-concentration cause in a low-cardinality dimension. | Case 16 |
| **Single-dim greedy misses intersections** | The real cause is an intersection (EU × Online = 90%), but neither dimension alone exceeds 45%. Greedy picks the best single dimension. | Case 14 |
| **Non-monotonic path** | The globally best explanation requires passing through a "valley" — a weak depth-1 split leading to a strong depth-2 split. Greedy commits to the stronger depth-1. | Case 17 |
| **Component-first wastes coverage** | A cross-cutting dimension (EU) is hidden behind a 3-way component split. Greedy emits only the first component. | Case 13 |
| **Scaling wastes recursion** | `arr = mrr × 12` uses a recursion level for a trivial identity. | Case 18 |

The deep beam search mitigates some of these by exploring multiple paths, but does not evaluate multi-dimensional intersections directly (no joint GROUP BY).

## CLI usage

```bash
# Fast pass (greedy)
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31

# Deep pass (beam search) with alternatives
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --deep --beam-width 15 --max-alternatives 8

# JSON output for programmatic consumption
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --deep --json
```
