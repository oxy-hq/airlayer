# Metric Tree Example: SaaS Revenue Model

This directory contains a SaaS revenue model with composite measures, driver annotations, and sample data. The `07_explain.sh` script demonstrates recursive root-cause analysis.

## The Explain Algorithm

The `explain` command answers: **why did a metric change between two time periods?** It recursively decomposes a top-level delta into the smallest (sub-metric, segment) pairs that account for the change. Think of it as a decision tree where each level picks the split that best explains the remaining variance.

### Split Types

At each level, the algorithm evaluates two kinds of splits and picks whichever has the highest concentration:

**Component splits** drill into the sub-metrics of a composite measure. For `net_mrr = total_mrr + expansion_mrr + new_mrr - churned_mrr`, the algorithm queries each child measure independently and computes how much of the parent's delta each child accounts for. The relationship sign is inferred from the expression: `churned_mrr` is preceded by `-`, so its sign is -1. An *increase* in `churned_mrr` contributes to a *decrease* in `net_mrr`.

**Dimension splits** segment the current measure by a categorical dimension (e.g., region, plan). The algorithm queries the measure broken down by each available dimension, finds the single dimension value with the largest same-direction delta, and uses that as the split. Dimension values are mutually exclusive rows, so the attribution is clean.

### Concentration

Concentration is the signed fraction of the parent's delta explained by a child split:

```
concentration = (child_delta * sign(parent_delta)) / |parent_delta|
```

Positive concentration means the child contributes in the same direction as the parent's change. Candidates are sorted by concentration descending; only positive-concentration splits are pursued.

### Recursion Strategy

- **Top level**: emit all significant splits until cumulative coverage reaches the threshold. Multiple top-level nodes are allowed because they may represent independent causes.
- **Non-top levels**: emit only the single best split. Multiple sibling splits at the same depth create redundant, correlated paths (e.g., region=EMEA and plan=enterprise might explain the same underlying rows). The algorithm avoids this by picking one path and drilling deeper.

### Stopping Criteria

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `coverage_threshold` | 80% | Stop adding top-level splits once this fraction of the root delta is explained |
| `max_depth` | 5 | Maximum recursion depth |
| `root_fraction` cutoff | 1% | Skip candidates whose absolute delta is less than 1% of the root delta (prevents chasing noise from compounding signal decay) |
| Positive concentration | -- | Stop when no remaining candidate has positive concentration (i.e., all remaining splits oppose the parent's direction) |

### Example Walkthrough

The model in `views/revenue.view.yml` defines:

```
arr = net_mrr * 12
net_mrr = total_mrr + expansion_mrr + new_mrr - churned_mrr
```

The data in `data/subscriptions.csv` has a scenario where four enterprise EMEA customers churn in February. Running `07_explain.sh` produces output like:

```
revenue.arr: 276300 -> 149640 (-126660, -45.8%)
  Period: 2024-01-01 .. 2024-01-31 vs 2024-02-01 .. 2024-02-28

  1. region=EMEA                    -126000  (99% of total)
     1. plan=enterprise                -126000  (99% of total)
        1. revenue.net_mrr                -10500  (8% of total)
           1. revenue.churned_mrr            -10500  (8% of total)
```

Walking through each level:

1. **region=EMEA (-126000, 99%)** -- The algorithm evaluates component splits (into `net_mrr`) and dimension splits (by `region`, by `plan`). Region=EMEA has the highest concentration because nearly the entire ARR decline is localized there. This is selected as the top-level split.

2. **plan=enterprise (-126000, 100%)** -- Within EMEA, the algorithm again evaluates all candidate splits. The `plan` dimension split on `enterprise` explains 100% of the EMEA delta, beating the component split into `net_mrr`. Only one split is emitted (non-top level).

3. **revenue.net_mrr (-10500, 8%)** -- Now scoped to EMEA+enterprise with no remaining dimensions, the only option is a component split. `net_mrr` (the sole child of `arr`) is selected. The delta is -10500 at the MRR level (which becomes -126000 at the ARR level after the `* 12` in the parent expression). The 8% shown is relative to the *root* delta.

4. **revenue.churned_mrr (-10500, 100%)** -- Decomposing `net_mrr`, the algorithm finds that `churned_mrr` explains the change. The sign is -1 (subtracted in the expression), so the +10500 increase in churned MRR produces a -10500 decrease in net MRR. Recursion stops here as `churned_mrr` is a leaf measure with no further splits.

### Usage

Text output (human-readable tree):

```bash
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31
```

JSON output (machine-readable, for programmatic consumption):

```bash
airlayer explain revenue.arr \
  --time revenue.created_at \
  --current 2024-02-01:2024-02-28 \
  --previous 2024-01-01:2024-01-31 \
  --json
```

The `--time` flag specifies which time dimension to use for period comparison. The `--current` and `--previous` flags define the two periods as `start:end` date ranges. The command requires a `config.yml` (auto-detected from project root) with database connection details, since explain executes queries to compute deltas.
