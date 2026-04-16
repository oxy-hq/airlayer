# Pathological RCA Cases

Synthetic datasets designed to expose failure modes in root-cause analysis algorithms. Each dataset creates a scenario where naive greedy search, JSD ranking, or IV/WOE scoring leads to suboptimal explanations.

## Pathological patterns in the data

### `sales` view (`data/transactions.csv`)

~78 transactions across Jan-Feb 2024 with 4 dimensions: `region`, `plan`, `channel`, `product_sku`.

**Designed distractors:**

| Pattern | Why it's pathological |
|---------|----------------------|
| **Channel proportion swap** | `partner` and `direct` swap share in EU. Produces enormous JSD despite small absolute impact. Greedy JSD ranking may pick channel over plan. |
| **Vanishing segment** | `promo_trial` disappears in Feb (0.5%→0%). Infinite WOE, tiny volume. IV/WOE scoring overweights it. |
| **Micro-SKU proliferation** | 14 product SKUs, most with tiny revenue. Inflates the product_sku dimension's aggregate JSD score, drowning out the plan dimension. |
| **Correlated dimensions** | Enterprise customers are mostly US+partner. Region and channel partially explain the same drop, confusing single-dimension attribution. |
| **Multi-dim intersection** | The sharpest drop is `region=EU × plan=enterprise × channel=partner`, visible only after 2+ dimension filters. |

**Actual behavior:** Greedy picks `region=EU` (110% concentration) and drills into `channel=partner` then `product_sku`. The deep beam search surfaces alternatives like `plan=enterprise → channel=partner` (123% coverage) and `channel=partner → region=EU` (119% coverage), showing how different entry points lead to different decompositions of the same underlying cause.

### `total_revenue` composite (`data/revenue_by_line.csv`)

46 rows split into `product` and `service` revenue lines.

**True root cause:** `region=EU` drops across both product and service lines.

**Why it's interesting:**
- `total_revenue` aggregates both revenue lines, and `product_revenue` / `service_revenue` are filtered leaf views
- The metric tree has component edges from `total_revenue.amount` → `product_revenue.amount` + `service_revenue.amount`
- Running explain on the parent vs each leaf shows whether the algorithm can surface the cross-cutting `region=EU` cause
- All three views correctly identify `region=EU` as 100% of the drop

## Scripts

```bash
# 1. Greedy explain — watch the dimension ranking and drill-down path
./01_explain_greedy.sh

# 2. Deep beam search — surfaces alternative decomposition paths
./02_explain_deep.sh

# 3. Component split — compare parent vs leaf explanations
./03_explain_components.sh
```

## What to look for

**Greedy vs Deep on `sales.revenue`:**
- Greedy picks `region=EU` first (110% concentration), then drills into channel and product_sku
- Deep surfaces `plan=enterprise → channel=partner` as an alternative with even higher coverage (123%)
- The "correct" answer depends on what dimension the analyst cares about — the algorithm should surface multiple paths

**Component split on `total_revenue.total`:**
- The algorithm finds `region=EU` immediately (100% of drop)
- Compare with leaf explains on `product_revenue.amount` and `service_revenue.amount` — all find EU
- This is the "easy" case; the pathological version is when components have different root causes

## Corresponding unit tests

The `src/engine/metric_tree_ops.rs` file contains pathological test cases (Cases 10-18) that exercise these patterns with mock data, including cases the current algorithm handles suboptimally:
- **Case 12:** JSD red herring — channel proportion swap overrides plan=enterprise
- **Case 13:** Dimension-first beats component split on multi-branch composites
- **Case 14:** Multi-dimensional AND condition (EU×Online) missed by single-dim greedy
- **Case 16:** Micro-segment JSD inflation drowns out the real cause
- **Case 17:** Non-monotonic path through a concentration "valley"
