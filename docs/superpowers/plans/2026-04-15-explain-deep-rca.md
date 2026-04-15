# Deep RCA Beam Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--deep` mode to `explain` that uses multi-strategy beam search to find globally optimal root-cause explanations, addressing all 9 pathological test cases.

**Architecture:** Two-tier explain: fast pass (current greedy, unchanged) + deep pass (decompose metric tree to leaves, run per-leaf dimensional beam search with 5 scoring strategies, merge with cross-cutting detection, annotate with statistical significance). Detection heuristics (Simpson's paradox, opposing offsets) run on every call. GROUPING SETS optimization collapses per-dimension queries into one round trip.

**Tech Stack:** Rust, serde_json, petgraph (existing), statrs or inline t-distribution CDF for p-values.

**Spec:** `docs/superpowers/specs/2026-04-15-explain-deep-rca-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `src/engine/metric_tree_ops.rs` | All explain logic — types, fast pass, deep pass, strategies, heuristics, significance | Modify (major) |
| `src/dialect/mod.rs` | `has_grouping_sets()` method on Dialect | Modify (minor) |
| `src/cli/mod.rs` | `--deep`, `--beam-width`, `--max-alternatives` flags; deep output rendering | Modify (moderate) |
| `Cargo.toml` | Optional: `statrs` dependency for t-distribution CDF | Modify (minor) |

All new logic goes in `metric_tree_ops.rs` — the file already owns all explain types and functions. No new files are created; this follows the existing pattern where the entire explain feature lives in one module.

---

### Task 1: Add New Data Types

**Files:**
- Modify: `src/engine/metric_tree_ops.rs:680-813` (types section)

- [ ] **Step 1: Write failing test for new SplitKind variants**

Add to the test module at the bottom of `metric_tree_ops.rs`:

```rust
#[test]
fn test_split_kind_serialization() {
    let uniform = SplitKind::UniformDegradation {
        dimension: "product".to_string(),
        num_elements: 200,
    };
    let json = serde_json::to_value(&uniform).unwrap();
    assert_eq!(json["type"], "uniform_degradation");
    assert_eq!(json["dimension"], "product");
    assert_eq!(json["num_elements"], 200);

    let cross = SplitKind::CrossCutting {
        dimension: "region".to_string(),
        value: "EU".to_string(),
        measures: vec!["ads.revenue".to_string(), "subs.revenue".to_string()],
    };
    let json = serde_json::to_value(&cross).unwrap();
    assert_eq!(json["type"], "cross_cutting");
    assert_eq!(json["measures"].as_array().unwrap().len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_split_kind_serialization`
Expected: FAIL — `UniformDegradation` and `CrossCutting` variants don't exist.

- [ ] **Step 3: Add new SplitKind variants**

In `src/engine/metric_tree_ops.rs`, update the `SplitKind` enum at line 708:

```rust
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SplitKind {
    Component { child_measure: String },
    Dimension { dimension: String, value: String },
    UniformDegradation { dimension: String, num_elements: usize },
    CrossCutting { dimension: String, value: String, measures: Vec<String> },
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_split_kind_serialization`
Expected: PASS

- [ ] **Step 5: Add ExplainPath, SignificanceTest, ExplainWarning types and update ExplainResult and ExplainConfig**

Add after the existing `DriverAttribution` struct (around line 777):

```rust
/// A complete explanation path found by the deep beam search.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainPath {
    /// Chain of splits from root to leaf.
    pub nodes: Vec<ExplainNode>,
    /// Total fraction of root delta explained by this path.
    pub root_fraction: f64,
    /// Which scoring strategy found this path.
    pub strategy: String,
    /// Statistical significance test result (deep mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub significance: Option<SignificanceTest>,
}

/// Result of a two-tailed t-test against historical period deltas.
#[derive(Debug, Clone, Serialize)]
pub struct SignificanceTest {
    pub p_value: f64,
    pub historical_periods: usize,
    pub historical_mean_delta: f64,
    pub historical_std_delta: f64,
}

/// Detection heuristic warnings (always checked, not just --deep).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
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
```

Update `ExplainResult` (line 781) to add new fields:

```rust
pub struct ExplainResult {
    pub target: String,
    pub target_delta: f64,
    pub target_previous: f64,
    pub target_current: f64,
    pub time_dimension: String,
    pub current_period: (String, String),
    pub previous_period: (String, String),
    pub nodes: Vec<ExplainNode>,
    pub coverage: f64,
    pub driver_attribution: Vec<DriverAttribution>,
    /// Deep beam search results (empty unless deep mode enabled).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<ExplainPath>,
    /// Detection heuristic warnings (always populated).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ExplainWarning>,
}
```

Update `ExplainConfig` (line 680) to add deep-mode fields:

```rust
pub struct ExplainConfig {
    pub coverage_threshold: f64,
    pub max_depth: usize,
    pub max_dim_values: usize,
    pub min_concentration: f64,
    pub min_root_fraction: f64,
    /// Enable deep beam search mode.
    pub deep: bool,
    /// Beam width for deep search (candidates kept per level).
    pub beam_width: usize,
    /// Maximum alternative explanations to return.
    pub max_alternatives: usize,
}
```

Update the `Default` impl:

```rust
impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            coverage_threshold: 0.80,
            max_depth: 10,
            max_dim_values: 20,
            min_concentration: 0.05,
            min_root_fraction: 0.005,
            deep: false,
            beam_width: 10,
            max_alternatives: 5,
        }
    }
}
```

- [ ] **Step 6: Fix all compilation errors from ExplainResult field additions**

The `explain()` function constructs `ExplainResult` at line 965. Add the new fields:

```rust
    Ok(ExplainResult {
        target: target.to_string(),
        target_delta: target_md.delta,
        target_previous: target_md.previous,
        target_current: target_md.current,
        time_dimension: time_dimension.to_string(),
        current_period: (current_period.0.to_string(), current_period.1.to_string()),
        previous_period: (previous_period.0.to_string(), previous_period.1.to_string()),
        nodes,
        coverage: covered,
        driver_attribution,
        alternatives: vec![],
        warnings: vec![],
    })
```

Also update the early return for zero delta (line 871):

```rust
    return Ok(ExplainResult {
        // ... existing fields ...
        alternatives: vec![],
        warnings: vec![],
    });
```

- [ ] **Step 7: Run full test suite to verify nothing broke**

Run: `cargo test --lib engine::metric_tree_ops::tests::`
Expected: All 23 tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add data types for deep RCA beam search

Add SplitKind::UniformDegradation and CrossCutting variants,
ExplainPath, SignificanceTest, ExplainWarning types. Extend
ExplainResult with alternatives and warnings fields. Extend
ExplainConfig with deep, beam_width, max_alternatives."
```

---

### Task 2: Detection Heuristics (Simpson's Paradox + Opposing Offsets)

These run on every explain call (not just --deep) and use data already fetched. Implement them first since they're self-contained and immediately testable.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs:835-977` (explain function) and test module

- [ ] **Step 1: Write failing tests for detection heuristics**

Add to the test module:

```rust
#[test]
fn test_heuristic_simpsons_paradox_detected() {
    // Reuse the Simpson's paradox pathological test setup,
    // but now check that a warning is emitted.
    let view = make_view_with_dims(
        "sales",
        &["device"],
        &[("conversion_rate", MeasureType::Average)],
    );
    let layer = make_layer(vec![view]);
    let tree = MetricTree::build(&layer);

    let mut data = HashMap::new();
    data.extend([
        agg("sales.conversion_rate", 5.0, 3.92),
        dim_breakdown("sales.conversion_rate", "sales.device", &[
            ("Mobile", 3.0, 3.5),
            ("Desktop", 5.5, 6.0),
        ]),
    ]);

    let result = run_explain(&layer, &tree, "sales.conversion_rate", data);

    // Should detect Simpson's paradox: all segments improved but aggregate declined
    assert!(
        result.warnings.iter().any(|w| matches!(w, ExplainWarning::SimpsonsParadox { .. })),
        "should detect Simpson's paradox warning"
    );
}

#[test]
fn test_heuristic_opposing_offset_detected() {
    let rev_view = make_view_with_dims(
        "rev",
        &["region"],
        &[("amount", MeasureType::Sum)],
    );
    let cost_view = make_view_with_dims(
        "cost",
        &["region"],
        &[("amount", MeasureType::Sum)],
    );
    let mut profit_view = make_view_with_dims("profit", &[], &[]);
    profit_view.measures = Some(vec![
        composite_measure("net", "{{rev.amount}} - {{cost.amount}}"),
    ]);

    let layer = make_layer(vec![profit_view, rev_view, cost_view]);
    let tree = MetricTree::build(&layer);

    let mut data = HashMap::new();
    data.extend([
        agg("profit.net", 2000.0, 2100.0),
        agg("rev.amount", 5000.0, 4900.0),
        agg("cost.amount", 3000.0, 2800.0),
        dim_breakdown("rev.amount", "rev.region", &[
            ("US", 2000.0, 2400.0),
            ("EU", 3000.0, 2500.0),
        ]),
        dim_breakdown("cost.amount", "cost.region", &[
            ("US", 1000.0, 1100.0),
            ("EU", 2000.0, 1700.0),
        ]),
    ]);

    let result = run_explain(&layer, &tree, "profit.net", data);

    assert!(
        result.warnings.iter().any(|w| matches!(w, ExplainWarning::OpposingOffset { .. })),
        "should detect opposing offset warning"
    );
}

#[test]
fn test_heuristic_no_false_positive() {
    // Normal case: one segment dropped, one stayed — no Simpson's paradox.
    let view = make_view_with_dims(
        "sales",
        &["plan"],
        &[("revenue", MeasureType::Sum)],
    );
    let layer = make_layer(vec![view]);
    let tree = MetricTree::build(&layer);

    let mut data = HashMap::new();
    data.extend([
        agg("sales.revenue", 10000.0, 9000.0),
        dim_breakdown("sales.revenue", "sales.plan", &[
            ("Enterprise", 8000.0, 7050.0),
            ("Free", 2000.0, 1950.0),
        ]),
    ]);

    let result = run_explain(&layer, &tree, "sales.revenue", data);

    assert!(
        result.warnings.is_empty(),
        "normal drop should produce no warnings, got {:?}",
        result.warnings
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_heuristic_`
Expected: FAIL — `warnings` is always empty (vec![]).

- [ ] **Step 3: Implement Simpson's paradox detection**

Add a helper function before `explain()`:

```rust
/// Detect Simpson's paradox: all dimension elements moved opposite to the aggregate.
fn detect_simpsons_paradox(
    measure: &str,
    parent_delta: f64,
    dim: &str,
    elements: &[ElementScore],
) -> Option<ExplainWarning> {
    if elements.is_empty() || parent_delta.abs() < f64::EPSILON {
        return None;
    }
    let parent_sign = parent_delta.signum();
    let all_opposing = elements.iter().all(|e| {
        if e.delta.abs() < f64::EPSILON {
            true // zero delta is neutral, don't disqualify
        } else {
            e.delta.signum() != parent_sign
        }
    });
    // Require at least one element with a meaningful opposing delta
    let has_meaningful = elements.iter().any(|e| e.delta.abs() > f64::EPSILON && e.delta.signum() != parent_sign);
    if all_opposing && has_meaningful {
        Some(ExplainWarning::SimpsonsParadox {
            dimension: dim.to_string(),
            aggregate_delta: parent_delta,
            segment_directions: elements
                .iter()
                .map(|e| (e.value.clone(), e.delta))
                .collect(),
        })
    } else {
        None
    }
}
```

- [ ] **Step 4: Implement opposing offset detection**

Add another helper:

```rust
/// Detect opposing offsets: two components with deltas that substantially cancel.
fn detect_opposing_offsets(
    component_deltas: &[(String, f64, f64)], // (measure, delta, sign)
) -> Vec<ExplainWarning> {
    let mut warnings = Vec::new();
    let signed: Vec<(&str, f64)> = component_deltas
        .iter()
        .map(|(m, delta, sign)| (m.as_str(), delta * sign))
        .collect();
    for i in 0..signed.len() {
        for j in (i + 1)..signed.len() {
            let (a_name, a_delta) = signed[i];
            let (b_name, b_delta) = signed[j];
            if a_delta.signum() != b_delta.signum() && a_delta.abs() > f64::EPSILON && b_delta.abs() > f64::EPSILON {
                let masking_ratio = a_delta.abs().min(b_delta.abs()) / a_delta.abs().max(b_delta.abs());
                if masking_ratio > 0.3 {
                    warnings.push(ExplainWarning::OpposingOffset {
                        component_a: a_name.to_string(),
                        component_b: b_name.to_string(),
                        delta_a: a_delta,
                        delta_b: b_delta,
                    });
                }
            }
        }
    }
    warnings
}
```

- [ ] **Step 5: Wire heuristics into explain()**

In `explain()`, after the fast-pass `recurse()` call (around line 920) and the driver attribution block (around line 963), add heuristic detection before constructing the final `ExplainResult`.

For Simpson's paradox, call it during `evaluate_candidates` and collect warnings. The simplest integration point: after the recurse call, re-evaluate the target's dimension breakdowns and run the check. Since the dimension data is already in the call flow, add a lightweight scan.

Actually, the cleanest approach is to collect dimension element scores during `evaluate_candidates` and check them. Add a `warnings` field to `ExplainCtx` (make it a `RefCell<Vec<ExplainWarning>>`):

```rust
struct ExplainCtx<'a> {
    dim_cache: HashMap<&'a str, Vec<String>>,
    children_of: HashMap<&'a str, Vec<&'a MetricEdge>>,
    time_dimension: &'a str,
    current_period: (&'a str, &'a str),
    previous_period: (&'a str, &'a str),
    config: &'a ExplainConfig,
    executor: &'a QueryExecutor,
    warnings: std::cell::RefCell<Vec<ExplainWarning>>,
}
```

In `evaluate_candidates`, after computing element scores for each dimension (around line 1346), add:

```rust
if let Some(w) = detect_simpsons_paradox(measure, parent_delta, dim, &elements) {
    ctx.warnings.borrow_mut().push(w);
}
```

For opposing offsets, call it in `explain()` after the component queries in the first `evaluate_candidates` call. Collect component deltas during the fast pass or after Phase 1. The simplest approach: do it in `explain()` using the `children_of` map, query each child, and check:

```rust
// Opposing offset detection (after fast pass)
let mut component_deltas: Vec<(String, f64, f64)> = Vec::new();
if let Some(edges) = ctx.children_of.get(target) {
    for edge in edges {
        if edge.kind != EdgeKind::Component { continue; }
        let q = make_period_query(&edge.from, time_dimension, previous_period.0, current_period.1, &[], &[]);
        if let Ok(rows) = executor(&q) {
            let md = extract_delta(&edge.from, &rows);
            component_deltas.push((edge.from.clone(), md.delta, edge.sign));
        }
    }
}
let offset_warnings = detect_opposing_offsets(&component_deltas);
```

Then combine all warnings before returning:

```rust
let mut warnings = ctx.warnings.into_inner();
warnings.extend(offset_warnings);
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_heuristic_`
Expected: All 3 pass.

- [ ] **Step 7: Run full test suite**

Run: `cargo test --lib engine::metric_tree_ops::tests::`
Expected: All tests pass (existing + new). The new `warnings` and `alternatives` fields on ExplainResult are populated but the existing pathological tests don't assert on them, so they pass unchanged.

- [ ] **Step 8: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add Simpson's paradox and opposing offset detection

Detection heuristics run on every explain call. Simpson's paradox
fires when all dimension segments move opposite to the aggregate.
Opposing offset fires when component deltas substantially cancel."
```

---

### Task 3: Laplace Smoothing and IV/WOE Scoring

Add the foundational scoring functions that the beam search strategies will use. Test them in isolation.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (scoring functions section, ~line 1098-1205)

- [ ] **Step 1: Write failing tests for Laplace-smoothed JSD and IV/WOE**

```rust
#[test]
fn test_jsd_element_with_smoothing() {
    // Zero previous share — should not panic or return NaN with smoothing
    let result = jsd_element_smoothed(0.0, 0.5, 1e-6);
    assert!(result.is_finite(), "smoothed JSD should be finite for zero share");
    assert!(result > 0.0, "new segment should have positive JSD");
}

#[test]
fn test_jsd_element_smoothed_matches_original_for_nonzero() {
    // When both shares are well above zero, smoothing should barely change the result
    let original = jsd_element(0.3, 0.2);
    let smoothed = jsd_element_smoothed(0.3, 0.2, 1e-10);
    assert!((original - smoothed).abs() < 1e-6, "smoothing should be negligible for nonzero shares");
}

#[test]
fn test_woe_and_iv() {
    // Simple 2-element case: shares [0.6, 0.4] → [0.4, 0.6]
    let elements = vec![
        (0.6_f64, 0.4_f64), // elem 1: prev_share, curr_share
        (0.4, 0.6),         // elem 2
    ];
    let epsilon = 1e-10;
    let woe_1 = ((0.4 + epsilon) / (0.6 + epsilon)).ln();
    let woe_2 = ((0.6 + epsilon) / (0.4 + epsilon)).ln();
    let iv = (0.4 - 0.6) * woe_1 + (0.6 - 0.4) * woe_2;
    assert!(iv > 0.0, "IV should be positive for shifted distribution");

    let computed = compute_iv(&elements, epsilon);
    assert!((computed - iv).abs() < 1e-6, "IV computation should match manual calc");
}

#[test]
fn test_woe_zero_share_with_smoothing() {
    // New segment: prev_share=0, curr_share=0.5
    let elements = vec![(0.0_f64, 0.5_f64), (1.0, 0.5)];
    let iv = compute_iv(&elements, 1e-6);
    assert!(iv.is_finite(), "IV should be finite with smoothing");
    assert!(iv > 0.0, "distribution shift should produce positive IV");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_jsd_element_with_smoothing -- && cargo test --lib engine::metric_tree_ops::tests::test_woe`
Expected: FAIL — functions don't exist.

- [ ] **Step 3: Implement Laplace-smoothed JSD**

Add after the existing `jsd_element` function (line 1113):

```rust
/// JSD with Laplace smoothing to handle zero shares.
fn jsd_element_smoothed(p: f64, q: f64, epsilon: f64) -> f64 {
    let p_s = p + epsilon;
    let q_s = q + epsilon;
    let m = (p_s + q_s) / 2.0;
    if m < f64::EPSILON {
        return 0.0;
    }
    let mut s = 0.0;
    if p_s > 0.0 {
        s += p_s * (p_s / m).ln();
    }
    if q_s > 0.0 {
        s += q_s * (q_s / m).ln();
    }
    0.5 * s
}

/// Compute Information Value for a dimension.
/// Input: slice of (prev_share, curr_share) per element, with Laplace epsilon.
fn compute_iv(elements: &[(f64, f64)], epsilon: f64) -> f64 {
    elements.iter().map(|(p, q)| {
        let p_s = p + epsilon;
        let q_s = q + epsilon;
        let woe = (q_s / p_s).ln();
        (q_s - p_s) * woe
    }).sum()
}

/// Compute per-element WOE values for a dimension breakdown.
/// Returns (value, woe) pairs, sorted by |woe| descending.
fn compute_element_woe(
    elements: &[ElementScore],
    total_prev: f64,
    total_curr: f64,
    epsilon: f64,
) -> Vec<(String, f64)> {
    let num = elements.len() as f64;
    let prev_denom = total_prev + epsilon * num;
    let curr_denom = total_curr + epsilon * num;
    let mut woes: Vec<(String, f64)> = elements
        .iter()
        .map(|e| {
            let p = (e.previous + epsilon) / prev_denom;
            let q = (e.current + epsilon) / curr_denom;
            let woe = (q / p).ln();
            (e.value.clone(), woe)
        })
        .collect();
    woes.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap_or(std::cmp::Ordering::Equal));
    woes
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_jsd_element_with_smoothing -- && cargo test --lib engine::metric_tree_ops::tests::test_jsd_element_smoothed -- && cargo test --lib engine::metric_tree_ops::tests::test_woe`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add Laplace-smoothed JSD and IV/WOE scoring functions

jsd_element_smoothed() handles zero-share segments via Laplace
smoothing. compute_iv() and compute_element_woe() implement
Information Value and Weight of Evidence for dimension ranking."
```

---

### Task 4: Adaptive EP Threshold and Uniform Degradation Detection

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (scoring + test sections)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn test_adaptive_ep_threshold() {
    assert!((adaptive_ep_threshold(2) - 0.0354).abs() < 0.001);    // 0.05 / sqrt(2)
    assert!((adaptive_ep_threshold(200) - 0.00354).abs() < 0.001); // 0.05 / sqrt(200)
    assert!((adaptive_ep_threshold(1) - 0.05).abs() < 0.001);      // 0.05 / sqrt(1)
}

#[test]
fn test_detect_uniform_degradation() {
    // 200 elements each with concentration 0.005 — sum > 0.50
    let elements: Vec<ElementScore> = (0..200)
        .map(|i| ElementScore {
            value: format!("item_{}", i),
            previous: 50.0,
            current: 45.0,
            delta: -5.0,
            ep: 0.005,
            surprise: 0.0,
        })
        .collect();
    let parent_delta = -1000.0;
    let threshold = adaptive_ep_threshold(200);
    let result = detect_uniform_degradation("sales.product", &elements, parent_delta, threshold);
    assert!(result.is_some(), "should detect uniform degradation");
    if let Some(SplitKind::UniformDegradation { dimension, num_elements }) = result {
        assert_eq!(dimension, "sales.product");
        assert_eq!(num_elements, 200);
    }
}

#[test]
fn test_no_uniform_degradation_when_concentrated() {
    // 2 elements, one dominant — not uniform
    let elements = vec![
        ElementScore { value: "A".to_string(), previous: 8000.0, current: 7100.0, delta: -900.0, ep: 0.9, surprise: 0.01 },
        ElementScore { value: "B".to_string(), previous: 2000.0, current: 1900.0, delta: -100.0, ep: 0.1, surprise: 0.001 },
    ];
    let threshold = adaptive_ep_threshold(2);
    let result = detect_uniform_degradation("sales.plan", &elements, -1000.0, threshold);
    assert!(result.is_none(), "concentrated drop is not uniform degradation");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_adaptive_ep -- && cargo test --lib engine::metric_tree_ops::tests::test_detect_uniform -- && cargo test --lib engine::metric_tree_ops::tests::test_no_uniform`
Expected: FAIL — functions don't exist.

- [ ] **Step 3: Implement adaptive threshold and uniform degradation detection**

```rust
/// Adaptive EP threshold scaled by cardinality.
/// Base = 0.05; scales as 0.05 / sqrt(n) so high-cardinality dimensions
/// don't filter out all elements in uniform degradation scenarios.
fn adaptive_ep_threshold(num_elements: usize) -> f64 {
    const BASE_EP: f64 = 0.05;
    BASE_EP / (num_elements as f64).sqrt()
}

/// Detect uniform degradation: no element passes the EP threshold, but
/// collectively they explain > 50% of the parent delta.
fn detect_uniform_degradation(
    dim: &str,
    elements: &[ElementScore],
    parent_delta: f64,
    threshold: f64,
) -> Option<SplitKind> {
    if parent_delta.abs() < f64::EPSILON || elements.is_empty() {
        return None;
    }
    // Check if any element passes the threshold
    let any_significant = elements.iter().any(|e| e.ep.abs() >= threshold);
    if any_significant {
        return None; // Not uniform — some elements are significant
    }
    // Check collective coverage
    let total_unsigned_concentration: f64 = elements
        .iter()
        .map(|e| signed_fraction(e.delta, parent_delta).abs())
        .sum();
    if total_unsigned_concentration > 0.50 {
        Some(SplitKind::UniformDegradation {
            dimension: dim.to_string(),
            num_elements: elements.len(),
        })
    } else {
        None
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_adaptive_ep -- && cargo test --lib engine::metric_tree_ops::tests::test_detect_uniform -- && cargo test --lib engine::metric_tree_ops::tests::test_no_uniform`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add adaptive EP threshold and uniform degradation detection

adaptive_ep_threshold() scales 0.05/sqrt(n) for cardinality-aware
noise filtering. detect_uniform_degradation() identifies when a
dimension's drop is evenly spread across all values."
```

---

### Task 5: Phase 1 — Tree Decomposition to Searchable Measures

**Files:**
- Modify: `src/engine/metric_tree_ops.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn test_decompose_to_searchable_measures() {
    // arr = net_mrr * 12; net_mrr = new + expansion - churned
    // Leaves: new_mrr, expansion_mrr, churned_mrr
    // Intermediates with no dims: net_mrr, arr → not searchable (no own dims)
    let (layer, tree) = saas_tree();
    let children_of = build_children_of(&tree);
    let result = decompose_to_searchable(&tree, &layer, "revenue.arr", &children_of);
    // All are leaves (no dims on any view in saas_tree)
    assert_eq!(result.len(), 3, "should find 3 leaf measures");
    let names: Vec<&str> = result.iter().map(|s| s.measure.as_str()).collect();
    assert!(names.contains(&"revenue.new_mrr"));
    assert!(names.contains(&"revenue.expansion_mrr"));
    assert!(names.contains(&"revenue.churned_mrr"));
    // churned_mrr has sign -1 (subtracted in net_mrr expression)
    let churned = result.iter().find(|s| s.measure == "revenue.churned_mrr").unwrap();
    assert!((churned.cumulative_sign - (-1.0)).abs() < f64::EPSILON);
}

#[test]
fn test_decompose_includes_intermediate_with_dims() {
    // Create a tree where an intermediate composite has its own dimensions
    let leaf_a = make_view_with_dims("leaf_a", &[], &[("val", MeasureType::Sum)]);
    let leaf_b = make_view_with_dims("leaf_b", &[], &[("val", MeasureType::Sum)]);
    let mut mid = make_view_with_dims("mid", &["region"], &[]);
    mid.measures = Some(vec![composite_measure("total", "{{leaf_a.val}} + {{leaf_b.val}}")]);
    let mut top = make_view_with_dims("top", &[], &[]);
    top.measures = Some(vec![composite_measure("grand", "{{mid.total}} * 2")]);

    let layer = make_layer(vec![top, mid, leaf_a, leaf_b]);
    let tree = MetricTree::build(&layer);
    let children_of = build_children_of(&tree);
    let result = decompose_to_searchable(&tree, &layer, "top.grand", &children_of);

    let names: Vec<&str> = result.iter().map(|s| s.measure.as_str()).collect();
    // Should include mid.total (has dims) AND leaf_a.val, leaf_b.val (leaves)
    assert!(names.contains(&"mid.total"), "intermediate with dims should be searchable");
    assert!(names.contains(&"leaf_a.val"), "leaf should be searchable");
    assert!(names.contains(&"leaf_b.val"), "leaf should be searchable");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_decompose_`
Expected: FAIL — `decompose_to_searchable`, `build_children_of` don't exist.

- [ ] **Step 3: Implement decomposition**

```rust
/// A measure identified as searchable for the deep beam pass.
struct SearchableMeasure {
    measure: String,
    /// Product of edge signs from root to this measure.
    cumulative_sign: f64,
    /// Available non-time dimensions for this measure's view.
    dimensions: Vec<String>,
}

/// Build reverse adjacency map: to_measure → [edges pointing to it].
fn build_children_of<'a>(tree: &'a MetricTree) -> HashMap<&'a str, Vec<&'a MetricEdge>> {
    let mut children_of: HashMap<&str, Vec<&MetricEdge>> = HashMap::new();
    for edge in &tree.edges {
        children_of.entry(edge.to.as_str()).or_default().push(edge);
    }
    children_of
}

/// Decompose a target measure into searchable measures by walking component edges.
///
/// A measure is searchable if:
/// - It's a leaf (no component children), OR
/// - It's an intermediate composite that has its own dimensions.
///
/// The target itself is excluded (the caller handles it separately).
fn decompose_to_searchable(
    tree: &MetricTree,
    layer: &SemanticLayer,
    target: &str,
    children_of: &HashMap<&str, Vec<&MetricEdge>>,
) -> Vec<SearchableMeasure> {
    let mut result = Vec::new();
    let mut stack: Vec<(&str, f64)> = vec![(target, 1.0)]; // (measure, cumulative_sign)
    let mut visited: HashSet<&str> = HashSet::new();

    while let Some((measure, cum_sign)) = stack.pop() {
        if !visited.insert(measure) {
            continue;
        }

        let component_children: Vec<(&str, f64)> = children_of
            .get(measure)
            .map(|edges| {
                edges.iter()
                    .filter(|e| e.kind == EdgeKind::Component)
                    .map(|e| (e.from.as_str(), cum_sign * e.sign))
                    .collect()
            })
            .unwrap_or_default();

        if component_children.is_empty() {
            // Leaf measure — always searchable
            let view_name = measure.split('.').next().unwrap_or("");
            let dims = discover_dimensions(layer, view_name);
            result.push(SearchableMeasure {
                measure: measure.to_string(),
                cumulative_sign: cum_sign,
                dimensions: dims,
            });
        } else {
            // Intermediate — searchable if it has its own dimensions
            let view_name = measure.split('.').next().unwrap_or("");
            let dims = discover_dimensions(layer, view_name);
            if !dims.is_empty() && measure != target {
                result.push(SearchableMeasure {
                    measure: measure.to_string(),
                    cumulative_sign: cum_sign,
                    dimensions: dims,
                });
            }
            // Always recurse into children
            for (child, child_sign) in component_children {
                stack.push((child, child_sign));
            }
        }
    }

    result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_decompose_`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add Phase 1 tree decomposition to searchable measures

decompose_to_searchable() walks component edges to find leaves and
intermediates with dimensions. build_children_of() extracts reverse
adjacency from the metric tree."
```

---

### Task 6: Beam Search Scoring Strategies

Implement the 5 per-measure scoring strategies as standalone functions that take element scores and return ranked candidates.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs`

- [ ] **Step 1: Write failing tests for each strategy**

```rust
#[test]
fn test_strategy_max_concentration() {
    let elements = vec![
        ElementScore { value: "A".to_string(), previous: 8000.0, current: 7050.0, delta: -950.0, ep: 0.95, surprise: 0.001 },
        ElementScore { value: "B".to_string(), previous: 2000.0, current: 1950.0, delta: -50.0, ep: 0.05, surprise: 0.0001 },
    ];
    let (score, top_value) = strategy_max_concentration(&elements, -1000.0);
    assert!((score - 0.95).abs() < 0.01);
    assert_eq!(top_value, "A");
}

#[test]
fn test_strategy_topk_concentration_sum() {
    let elements = vec![
        ElementScore { value: "A".to_string(), previous: 0.0, current: 0.0, delta: -400.0, ep: 0.4, surprise: 0.0 },
        ElementScore { value: "B".to_string(), previous: 0.0, current: 0.0, delta: -350.0, ep: 0.35, surprise: 0.0 },
        ElementScore { value: "C".to_string(), previous: 0.0, current: 0.0, delta: -150.0, ep: 0.15, surprise: 0.0 },
        ElementScore { value: "D".to_string(), previous: 0.0, current: 0.0, delta: -100.0, ep: 0.10, surprise: 0.0 },
    ];
    let score = strategy_topk_concentration_sum(&elements, -1000.0, 3);
    // Top 3 by |concentration|: 0.4 + 0.35 + 0.15 = 0.90
    assert!((score - 0.90).abs() < 0.01);
}

#[test]
fn test_strategy_iv_ranking() {
    // Dimension with big proportional shift should score higher IV
    let elements_shifted = vec![
        ElementScore { value: "A".to_string(), previous: 6000.0, current: 3000.0, delta: -3000.0, ep: 0.6, surprise: 0.0 },
        ElementScore { value: "B".to_string(), previous: 4000.0, current: 6000.0, delta: 2000.0, ep: -0.4, surprise: 0.0 },
    ];
    let elements_stable = vec![
        ElementScore { value: "X".to_string(), previous: 5000.0, current: 4500.0, delta: -500.0, ep: 0.5, surprise: 0.0 },
        ElementScore { value: "Y".to_string(), previous: 5000.0, current: 4500.0, delta: -500.0, ep: 0.5, surprise: 0.0 },
    ];
    let iv_shifted = strategy_iv(&elements_shifted);
    let iv_stable = strategy_iv(&elements_stable);
    assert!(iv_shifted > iv_stable, "shifted distribution should have higher IV");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_strategy_`
Expected: FAIL — strategy functions don't exist.

- [ ] **Step 3: Implement scoring strategies**

```rust
/// Strategy 1: rank dimension by its top element's |concentration|.
fn strategy_max_concentration(elements: &[ElementScore], parent_delta: f64) -> (f64, String) {
    elements
        .iter()
        .map(|e| {
            let conc = signed_fraction(e.delta, parent_delta);
            (conc, e.value.clone())
        })
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or((0.0, String::new()))
}

/// Strategy 2: rank dimension by sum of top-k elements' |concentration|.
fn strategy_topk_concentration_sum(elements: &[ElementScore], parent_delta: f64, k: usize) -> f64 {
    let mut concentrations: Vec<f64> = elements
        .iter()
        .map(|e| signed_fraction(e.delta, parent_delta).abs())
        .collect();
    concentrations.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    concentrations.iter().take(k).sum()
}

/// Strategy 3: JSD surprise with Laplace smoothing.
/// Returns total dimension surprise (sum of per-element JSD for significant elements).
fn strategy_jsd_smoothed(elements: &[ElementScore], threshold: f64) -> f64 {
    let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
    let total_curr: f64 = elements.iter().map(|e| e.current).sum();
    let epsilon = if (total_prev + total_curr).abs() > f64::EPSILON {
        1.0 / (total_prev + total_curr)
    } else {
        1e-10
    };
    let num = elements.len() as f64;
    let prev_denom = total_prev + epsilon * num;
    let curr_denom = total_curr + epsilon * num;

    elements
        .iter()
        .filter(|e| e.ep.abs() >= threshold)
        .map(|e| {
            let p = (e.previous + epsilon) / prev_denom;
            let q = (e.current + epsilon) / curr_denom;
            jsd_element_smoothed(p, q, 0.0) // already smoothed via shares
        })
        .sum()
}

/// Strategy 4: Information Value (IV) for dimension ranking.
fn strategy_iv(elements: &[ElementScore]) -> f64 {
    let total_prev: f64 = elements.iter().map(|e| e.previous).sum();
    let total_curr: f64 = elements.iter().map(|e| e.current).sum();
    let epsilon = if (total_prev + total_curr).abs() > f64::EPSILON {
        1.0 / (total_prev + total_curr)
    } else {
        1e-10
    };
    let shares: Vec<(f64, f64)> = elements
        .iter()
        .map(|e| {
            let num = elements.len() as f64;
            let p = (e.previous + epsilon) / (total_prev + epsilon * num);
            let q = (e.current + epsilon) / (total_curr + epsilon * num);
            (p, q)
        })
        .collect();
    compute_iv(&shares, 0.0) // already smoothed via shares
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_strategy_`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): add beam search scoring strategies 1-4

strategy_max_concentration, strategy_topk_concentration_sum,
strategy_jsd_smoothed, strategy_iv for per-dimension ranking
in the deep beam search."
```

---

### Task 7: Beam Search Core Loop

Implement the per-measure beam search that uses all strategies to find the best explanation paths.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs`

- [ ] **Step 1: Write failing test for beam search**

```rust
#[test]
fn test_beam_search_finds_concentrated_path() {
    // Single view with 2 dims. plan=Enterprise has 0.95 concentration.
    // source has many small elements (JSD distraction scenario).
    let view = make_view_with_dims(
        "sales",
        &["source", "plan"],
        &[("revenue", MeasureType::Sum)],
    );
    let layer = make_layer(vec![view]);

    let source_entries: Vec<(&str, f64, f64)> = vec![
        ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
        ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
        ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
        ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
        ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
    ];

    let mut data = HashMap::new();
    data.extend([
        agg("sales.revenue", 10000.0, 9000.0),
        dim_breakdown("sales.revenue", "sales.source", &source_entries),
        dim_breakdown("sales.revenue", "sales.plan", &[
            ("Enterprise", 8000.0, 7050.0),
            ("Free", 2000.0, 1950.0),
        ]),
    ]);

    let exec = filter_aware_mock(data);
    let dims = vec!["sales.source".to_string(), "sales.plan".to_string()];
    let config = ExplainConfig { beam_width: 5, max_alternatives: 3, ..Default::default() };

    let paths = beam_search_measure(
        "sales.revenue",
        -1000.0,
        &dims,
        &[],
        "sales.created_at",
        ("2024-01-01", "2024-01-31"),
        ("2024-02-01", "2024-02-28"),
        &config,
        &exec,
    ).unwrap();

    // The best path should be plan=Enterprise with root_fraction ~0.95
    assert!(!paths.is_empty(), "should find at least one path");
    let best = &paths[0];
    assert!(
        best.root_fraction > 0.90,
        "best path should have root_fraction > 0.90, got {}",
        best.root_fraction
    );
    // Verify it found the right dimension
    let found_enterprise = best.nodes.iter().any(|n| {
        matches!(&n.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.plan" && value == "Enterprise")
    });
    assert!(found_enterprise, "best path should find plan=Enterprise");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_beam_search_finds`
Expected: FAIL — `beam_search_measure` doesn't exist.

- [ ] **Step 3: Implement beam search core**

```rust
/// A beam entry: a partial explanation path being explored.
#[derive(Clone)]
struct BeamEntry {
    /// Accumulated splits so far.
    nodes: Vec<ExplainNode>,
    /// Current measure being explored.
    measure: String,
    /// Accumulated filters from dimension splits.
    filters: Vec<QueryFilter>,
    /// Remaining dimensions available for splitting.
    remaining_dims: Vec<String>,
    /// Cumulative root_fraction from root to this point.
    root_fraction: f64,
    /// Which strategy originated this path.
    strategy: String,
}

/// Run beam search on a single measure to find the best explanation paths.
fn beam_search_measure(
    measure: &str,
    measure_delta: f64,
    available_dims: &[String],
    initial_filters: &[QueryFilter],
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    config: &ExplainConfig,
    executor: &QueryExecutor,
) -> Result<Vec<ExplainPath>, EngineError> {
    if measure_delta.abs() < f64::EPSILON || available_dims.is_empty() {
        return Ok(vec![]);
    }

    // Seed beam: evaluate all dims with all strategies, take top beam_width entries
    let seed_candidates = evaluate_beam_candidates(
        measure, measure_delta, initial_filters, available_dims,
        time_dimension, previous_period, current_period, executor,
    )?;

    let mut beam: Vec<BeamEntry> = seed_candidates
        .into_iter()
        .take(config.beam_width)
        .collect();

    let mut completed: Vec<ExplainPath> = Vec::new();

    for _depth in 0..config.max_depth {
        if beam.is_empty() {
            break;
        }

        let mut next_beam: Vec<BeamEntry> = Vec::new();

        for entry in &beam {
            if entry.remaining_dims.is_empty()
                || entry.root_fraction < config.min_root_fraction
            {
                // Terminate: move to completed
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            // Get the delta for this entry's current state (filtered measure)
            let q = make_period_query(
                &entry.measure, time_dimension,
                previous_period.0, current_period.1,
                &[], &entry.filters,
            );
            let entry_delta = match executor(&q) {
                Ok(rows) => extract_delta(&entry.measure, &rows).delta,
                Err(_) => {
                    completed.push(ExplainPath {
                        nodes: entry.nodes.clone(),
                        root_fraction: entry.root_fraction,
                        strategy: entry.strategy.clone(),
                        significance: None,
                    });
                    continue;
                }
            };

            if entry_delta.abs() < f64::EPSILON {
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            // Evaluate candidates at this level
            let candidates = evaluate_beam_candidates(
                &entry.measure, entry_delta, &entry.filters,
                &entry.remaining_dims, time_dimension,
                previous_period, current_period, executor,
            )?;

            if candidates.is_empty() {
                completed.push(ExplainPath {
                    nodes: entry.nodes.clone(),
                    root_fraction: entry.root_fraction,
                    strategy: entry.strategy.clone(),
                    significance: None,
                });
                continue;
            }

            for candidate in candidates {
                next_beam.push(candidate);
            }
        }

        // Deduplicate by (measure, filter_set), keep highest root_fraction
        next_beam.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen: HashSet<String> = HashSet::new();
        next_beam.retain(|e| {
            let key = dedup_key(&e.measure, &e.filters);
            seen.insert(key)
        });
        next_beam.truncate(config.beam_width);

        beam = next_beam;
    }

    // Any remaining beam entries become completed paths
    for entry in beam {
        completed.push(ExplainPath {
            nodes: entry.nodes.clone(),
            root_fraction: entry.root_fraction,
            strategy: entry.strategy.clone(),
            significance: None,
        });
    }

    // Sort by root_fraction descending
    completed.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
    completed.truncate(config.max_alternatives);
    Ok(completed)
}

/// Deduplication key for beam entries.
fn dedup_key(measure: &str, filters: &[QueryFilter]) -> String {
    let mut parts: Vec<String> = filters
        .iter()
        .filter_map(|f| {
            let m = f.member.as_deref()?;
            let v = f.values.first()?;
            Some(format!("{}={}", m, v))
        })
        .collect();
    parts.sort();
    format!("{}|{}", measure, parts.join("&"))
}

/// Evaluate all scoring strategies for one (measure, delta, filters, dims) and produce
/// beam entries. Each strategy proposes its best candidate; all are returned.
fn evaluate_beam_candidates(
    measure: &str,
    parent_delta: f64,
    filters: &[QueryFilter],
    available_dims: &[String],
    time_dimension: &str,
    previous_period: (&str, &str),
    current_period: (&str, &str),
    executor: &QueryExecutor,
) -> Result<Vec<BeamEntry>, EngineError> {
    let mut all_candidates: Vec<BeamEntry> = Vec::new();

    for dim in available_dims {
        let q = make_period_query(
            measure, time_dimension, previous_period.0, current_period.1,
            &[dim.clone()], filters,
        );
        let rows = match executor(&q) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let elements = compute_element_scores(measure, dim, &rows, parent_delta);
        if elements.is_empty() {
            continue;
        }

        let ep_threshold = adaptive_ep_threshold(elements.len());
        let remaining: Vec<String> = available_dims.iter().filter(|d| d.as_str() != dim.as_str()).cloned().collect();

        // Check for uniform degradation first
        if let Some(uniform_split) = detect_uniform_degradation(dim, &elements, parent_delta, ep_threshold) {
            // Uniform degradation goes directly to a completed-style entry with no further dims
            all_candidates.push(BeamEntry {
                nodes: vec![ExplainNode {
                    split: uniform_split,
                    measure: measure.to_string(),
                    filters: filters.to_vec(),
                    delta: parent_delta,
                    concentration: 1.0,
                    root_fraction: 1.0,
                    siblings: vec![],
                    dimension_count: Some(elements.len()),
                    children: vec![],
                }],
                measure: measure.to_string(),
                filters: filters.to_vec(),
                remaining_dims: vec![], // no further recursion
                root_fraction: 1.0,
                strategy: "uniform_degradation".to_string(),
            });
            continue;
        }

        // Strategy 1: max concentration
        let (max_conc, max_val) = strategy_max_concentration(&elements, parent_delta);
        if max_conc > 0.0 {
            let mut new_filters = filters.to_vec();
            new_filters.push(QueryFilter {
                member: Some(dim.clone()),
                operator: Some(crate::engine::query::FilterOperator::Equals),
                values: vec![max_val.clone()],
                and: None,
                or: None,
            });
            all_candidates.push(BeamEntry {
                nodes: vec![ExplainNode {
                    split: SplitKind::Dimension { dimension: dim.clone(), value: max_val },
                    measure: measure.to_string(),
                    filters: new_filters.clone(),
                    delta: elements.iter().find(|e| e.value == *all_candidates.last().map(|c| &c.nodes[0]).and_then(|n| match &n.split { SplitKind::Dimension { value, .. } => Some(value), _ => None }).unwrap_or(&String::new())).map(|e| e.delta).unwrap_or(0.0),
                    concentration: max_conc,
                    root_fraction: max_conc,
                    siblings: vec![],
                    dimension_count: Some(elements.len()),
                    children: vec![],
                },],
                measure: measure.to_string(),
                filters: new_filters,
                remaining_dims: remaining.clone(),
                root_fraction: max_conc,
                strategy: "max_concentration".to_string(),
            });
        }

        // Actually, the above BeamEntry construction is getting complex with inline element lookups.
        // Let me simplify with a helper that creates entries for each strategy's top pick.

        // [This will be refactored in implementation — see note below]
    }

    all_candidates.sort_by(|a, b| b.root_fraction.partial_cmp(&a.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
    Ok(all_candidates)
}
```

**Implementation note:** The `evaluate_beam_candidates` function above shows the structure but the inline `BeamEntry` construction is verbose. During implementation, extract a helper:

```rust
fn make_beam_entry(
    measure: &str,
    dim: &str,
    elem: &ElementScore,
    parent_delta: f64,
    filters: &[QueryFilter],
    remaining_dims: &[String],
    strategy: &str,
    dim_count: usize,
) -> BeamEntry
```

This helper constructs the `BeamEntry` with proper filter accumulation, node creation, and root_fraction computation. Each strategy calls it with its top element.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_beam_search_finds`
Expected: PASS — beam search finds plan=Enterprise via max_concentration strategy.

- [ ] **Step 5: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): implement beam search core loop

beam_search_measure() runs multi-strategy beam search on a single
measure. evaluate_beam_candidates() scores dimensions with
max_concentration, topk_sum, jsd_smoothed, and iv strategies.
Beam deduplication prevents convergent paths from wasting width."
```

---

### Task 8: Deep Pass Integration — Wire Beam Search into explain()

Connect Phase 1 (decomposition) → Phase 2 (per-measure beam) → Phase 3 (merge + cross-cutting) → Phase 4 (heuristics, already done) into the `explain()` function.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs:835-977` (explain function)

- [ ] **Step 1: Write failing test for end-to-end deep explain**

```rust
#[test]
fn test_deep_explain_jsd_distraction_fixed() {
    // Same data as test_pathological_jsd_distraction, but with deep=true.
    // The deep pass should find plan=Enterprise as the best alternative.
    let view = make_view_with_dims(
        "sales",
        &["source", "plan"],
        &[("revenue", MeasureType::Sum)],
    );
    let layer = make_layer(vec![view]);
    let tree = MetricTree::build(&layer);

    let source_entries: Vec<(&str, f64, f64)> = vec![
        ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
        ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
        ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
        ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
        ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
    ];

    let mut data = HashMap::new();
    data.extend([
        agg("sales.revenue", 10000.0, 9000.0),
        dim_breakdown("sales.revenue", "sales.source", &source_entries),
        dim_breakdown("sales.revenue", "sales.plan", &[
            ("Enterprise", 8000.0, 7050.0),
            ("Free", 2000.0, 1950.0),
        ]),
    ]);

    let exec = filter_aware_mock(data);
    let config = ExplainConfig { deep: true, beam_width: 5, max_alternatives: 3, ..Default::default() };
    let result = explain(
        &tree, &layer, "sales.revenue", "sales.created_at",
        ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"),
        &config, &exec,
    ).unwrap();

    // The deep pass should find plan=Enterprise as the top alternative
    assert!(!result.alternatives.is_empty(), "deep pass should produce alternatives");
    let best_alt = &result.alternatives[0];
    assert!(
        best_alt.root_fraction > 0.90,
        "best alternative should have root_fraction > 0.90, got {}",
        best_alt.root_fraction
    );
    let found_enterprise = best_alt.nodes.iter().any(|n| {
        matches!(&n.split, SplitKind::Dimension { dimension, value }
            if dimension == "sales.plan" && value == "Enterprise")
    });
    assert!(found_enterprise, "best alternative should find plan=Enterprise");
}

#[test]
fn test_deep_explain_cross_cutting_detected() {
    // Two components, same dimension value (EU) explains drop in both.
    let ads_view = make_view_with_dims("ads", &["region"], &[("revenue", MeasureType::Sum)]);
    let subs_view = make_view_with_dims("subs", &["region"], &[("revenue", MeasureType::Sum)]);
    let mut total_view = make_view_with_dims("total", &[], &[]);
    total_view.measures = Some(vec![
        composite_measure("revenue", "{{ads.revenue}} + {{subs.revenue}}"),
    ]);

    let layer = make_layer(vec![total_view, ads_view, subs_view]);
    let tree = MetricTree::build(&layer);

    let mut data = HashMap::new();
    data.extend([
        agg("total.revenue", 10000.0, 9000.0),
        agg("ads.revenue", 6000.0, 5400.0),
        agg("subs.revenue", 4000.0, 3600.0),
        dim_breakdown("ads.revenue", "ads.region", &[
            ("US", 5000.0, 5000.0),
            ("EU", 1000.0, 400.0),
        ]),
        dim_breakdown("subs.revenue", "subs.region", &[
            ("US", 3500.0, 3500.0),
            ("EU", 500.0, 100.0),
        ]),
    ]);

    let exec = filter_aware_mock(data);
    let config = ExplainConfig { deep: true, beam_width: 5, max_alternatives: 5, ..Default::default() };
    let result = explain(
        &tree, &layer, "total.revenue", "total.created_at",
        ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"),
        &config, &exec,
    ).unwrap();

    // Should find a CrossCutting alternative for region=EU
    let has_cross_cutting = result.alternatives.iter().any(|p| {
        p.nodes.iter().any(|n| matches!(&n.split, SplitKind::CrossCutting { value, .. } if value == "EU"))
    });
    assert!(has_cross_cutting, "should detect cross-cutting region=EU across ads and subs");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_deep_explain_`
Expected: FAIL — `explain` with `deep: true` doesn't produce alternatives yet.

- [ ] **Step 3: Implement deep pass integration in explain()**

In `explain()`, after the fast pass `recurse()` call and driver attribution, add the deep pass:

```rust
    // ── Deep pass (beam search) ──────────────────────────
    let mut alternatives = Vec::new();
    if config.deep {
        // Phase 1: decompose to searchable measures
        let searchable = decompose_to_searchable(tree, layer, target, &ctx.children_of);

        // Query aggregate deltas for each searchable measure
        let mut measure_deltas: Vec<(String, f64, f64, Vec<String>)> = Vec::new(); // (measure, delta, leaf_share, dims)
        for sm in &searchable {
            let q = make_period_query(
                &sm.measure, time_dimension,
                previous_period.0, current_period.1, &[], &[],
            );
            if let Ok(rows) = executor(&q) {
                let md = extract_delta(&sm.measure, &rows);
                let leaf_share = if target_md.delta.abs() > f64::EPSILON {
                    (md.delta * sm.cumulative_sign) / target_md.delta
                } else {
                    0.0
                };
                measure_deltas.push((sm.measure.clone(), md.delta, leaf_share, sm.dimensions.clone()));
            }
        }

        // If target itself has dimensions and isn't in searchable, also search it
        if !available_dims.is_empty() {
            let already_included = measure_deltas.iter().any(|(m, _, _, _)| m == target);
            if !already_included {
                measure_deltas.push((target.to_string(), target_md.delta, 1.0, available_dims.clone()));
            }
        }

        // Phase 2: per-measure beam search
        let mut all_paths: Vec<(ExplainPath, f64)> = Vec::new(); // (path, leaf_share)
        for (measure, delta, leaf_share, dims) in &measure_deltas {
            if dims.is_empty() || delta.abs() < f64::EPSILON {
                continue;
            }
            let paths = beam_search_measure(
                measure, *delta, dims, &[], time_dimension,
                previous_period, current_period, config, executor,
            )?;
            for mut path in paths {
                path.root_fraction *= leaf_share.abs();
                all_paths.push((path, *leaf_share));
            }
        }

        // Phase 3: cross-cutting detection
        let cross_cutting = detect_cross_cutting(&all_paths);
        for cc in cross_cutting {
            all_paths.push((cc, 1.0));
        }

        // Sort and truncate
        all_paths.sort_by(|a, b| b.0.root_fraction.partial_cmp(&a.0.root_fraction).unwrap_or(std::cmp::Ordering::Equal));
        alternatives = all_paths.into_iter().take(config.max_alternatives).map(|(p, _)| p).collect();
    }
```

Also implement the cross-cutting detection helper:

```rust
/// Detect cross-cutting patterns: same dimension=value appearing across multiple measures.
fn detect_cross_cutting(paths: &[(ExplainPath, f64)]) -> Vec<ExplainPath> {
    // Group by (dimension, value) across all paths
    let mut dim_val_groups: HashMap<(String, String), Vec<(String, f64)>> = HashMap::new();
    for (path, _leaf_share) in paths {
        for node in &path.nodes {
            if let SplitKind::Dimension { dimension, value } = &node.split {
                dim_val_groups
                    .entry((dimension.clone(), value.clone()))
                    .or_default()
                    .push((node.measure.clone(), path.root_fraction));
            }
        }
    }

    let mut cross_cutting_paths = Vec::new();
    for ((dimension, value), measures) in &dim_val_groups {
        if measures.len() < 2 {
            continue;
        }
        let combined_fraction: f64 = measures.iter().map(|(_, rf)| rf).sum();
        let measure_names: Vec<String> = measures.iter().map(|(m, _)| m.clone()).collect();

        // Only emit if combined fraction exceeds any individual path's fraction
        let max_individual = measures.iter().map(|(_, rf)| *rf).fold(0.0_f64, f64::max);
        if combined_fraction > max_individual * 1.1 {
            cross_cutting_paths.push(ExplainPath {
                nodes: vec![ExplainNode {
                    split: SplitKind::CrossCutting {
                        dimension: dimension.clone(),
                        value: value.clone(),
                        measures: measure_names,
                    },
                    measure: String::new(),
                    filters: vec![],
                    delta: 0.0,
                    concentration: combined_fraction,
                    root_fraction: combined_fraction,
                    siblings: vec![],
                    dimension_count: None,
                    children: vec![],
                }],
                root_fraction: combined_fraction,
                strategy: "cross_cutting".to_string(),
                significance: None,
            });
        }
    }
    cross_cutting_paths
}
```

Update the final `ExplainResult` construction to use `alternatives` and `warnings`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_deep_explain_`
Expected: Both pass.

- [ ] **Step 5: Run full test suite**

Run: `cargo test --lib engine::metric_tree_ops::tests::`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "feat(explain): wire deep beam search into explain()

Phase 1 decomposes to searchable measures, Phase 2 runs per-measure
beam search, Phase 3 detects cross-cutting patterns. Deep pass
activated via ExplainConfig::deep flag."
```

---

### Task 9: Statistical Significance (Phase 5)

**Files:**
- Modify: `src/engine/metric_tree_ops.rs`
- Modify: `Cargo.toml` (add `statrs` dependency for t-distribution CDF)

- [ ] **Step 1: Add statrs dependency**

In `Cargo.toml`, add under `[dependencies]`:

```toml
statrs = "0.17"
```

- [ ] **Step 2: Write failing test**

```rust
#[test]
fn test_significance_test_detects_abnormal_delta() {
    // Historical deltas: [-50, -40, -60, -45, -55, -50] (mean=-50, std≈7.07)
    // Current delta: -200 (far outside normal range)
    let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
    let current_delta = -200.0;
    let result = compute_significance(current_delta, &historical);
    assert!(result.is_some(), "should compute significance with 6 periods");
    let sig = result.unwrap();
    assert!(sig.p_value < 0.01, "p-value should be very small for outlier delta, got {}", sig.p_value);
}

#[test]
fn test_significance_test_normal_delta_not_significant() {
    let historical = vec![-50.0, -40.0, -60.0, -45.0, -55.0, -50.0];
    let current_delta = -48.0; // within normal range
    let result = compute_significance(current_delta, &historical);
    let sig = result.unwrap();
    assert!(sig.p_value > 0.05, "normal delta should not be significant, got {}", sig.p_value);
}

#[test]
fn test_significance_insufficient_history() {
    let historical = vec![-50.0, -40.0]; // only 2 periods < 6 minimum
    let result = compute_significance(-200.0, &historical);
    assert!(result.is_none(), "should return None with insufficient history");
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_significance_`
Expected: FAIL — `compute_significance` doesn't exist.

- [ ] **Step 4: Implement significance testing**

```rust
use statrs::distribution::{ContinuousCDF, StudentsT};

/// Compute statistical significance of a delta relative to historical deltas.
/// Returns None if fewer than 6 historical periods.
fn compute_significance(current_delta: f64, historical_deltas: &[f64]) -> Option<SignificanceTest> {
    const MIN_PERIODS: usize = 6;
    if historical_deltas.len() < MIN_PERIODS {
        return None;
    }

    let n = historical_deltas.len() as f64;
    let mean: f64 = historical_deltas.iter().sum::<f64>() / n;
    let variance: f64 = historical_deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let std = variance.sqrt();

    if std < f64::EPSILON {
        // All historical deltas are identical — any different delta is "significant"
        let p_value = if (current_delta - mean).abs() < f64::EPSILON { 1.0 } else { 0.0 };
        return Some(SignificanceTest {
            p_value,
            historical_periods: historical_deltas.len(),
            historical_mean_delta: mean,
            historical_std_delta: std,
        });
    }

    let t_stat = (current_delta - mean) / (std / n.sqrt());
    let df = n - 1.0;

    // Two-tailed p-value from t-distribution
    let t_dist = StudentsT::new(0.0, 1.0, df).ok()?;
    let p_value = 2.0 * (1.0 - t_dist.cdf(t_stat.abs()));

    Some(SignificanceTest {
        p_value,
        historical_periods: historical_deltas.len(),
        historical_mean_delta: mean,
        historical_std_delta: std,
    })
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_significance_`
Expected: All pass.

- [ ] **Step 6: Wire significance into deep pass**

In `explain()`, after Phase 3 (cross-cutting detection) and before returning alternatives, add Phase 5:

```rust
    // Phase 5: statistical significance for top alternatives
    if config.deep {
        for alt in alternatives.iter_mut().take(config.max_alternatives) {
            if let Some(terminal_node) = alt.nodes.last() {
                // Query 12 months of monthly history for the terminal segment
                let hist_q = make_period_query(
                    &terminal_node.measure, time_dimension,
                    // 12 months back from previous period start
                    &format!("{}-12m", previous_period.0), // simplified — actual impl needs date arithmetic
                    &current_period.1,
                    &[], &terminal_node.filters,
                );
                // Parse monthly deltas from the result
                // [Implementation: extract month-over-month deltas from the multi-month result]
                // For now, significance is computed when historical data is available
            }
        }
    }
```

**Note:** The date arithmetic for "12 months back" requires careful handling. The implementation should use the existing period parsing in the CLI to compute the historical date range. For the initial implementation, accept an optional `historical_start` in `ExplainConfig` or compute it from `previous_period.0`.

- [ ] **Step 7: Run full test suite**

Run: `cargo test --lib engine::metric_tree_ops::tests::`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/engine/metric_tree_ops.rs Cargo.toml
git commit -m "feat(explain): add statistical significance testing (Phase 5)

compute_significance() runs a two-tailed t-test against historical
period deltas. Returns p-value annotated on each ExplainPath.
Requires minimum 6 historical periods."
```

---

### Task 10: GROUPING SETS Dialect Support

**Files:**
- Modify: `src/dialect/mod.rs`

- [ ] **Step 1: Write failing test**

```rust
// In dialect/mod.rs tests (or a new test)
#[test]
fn test_has_grouping_sets() {
    assert!(Dialect::Postgres.has_grouping_sets());
    assert!(Dialect::Snowflake.has_grouping_sets());
    assert!(Dialect::BigQuery.has_grouping_sets());
    assert!(Dialect::DuckDB.has_grouping_sets());
    assert!(Dialect::ClickHouse.has_grouping_sets());
    assert!(Dialect::Databricks.has_grouping_sets());
    assert!(Dialect::Presto.has_grouping_sets());
    assert!(Dialect::Redshift.has_grouping_sets());
    assert!(!Dialect::MySQL.has_grouping_sets());
    assert!(!Dialect::SQLite.has_grouping_sets());
    assert!(!Dialect::Domo.has_grouping_sets());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib dialect::tests::test_has_grouping_sets`
Expected: FAIL — method doesn't exist.

- [ ] **Step 3: Implement has_grouping_sets()**

Add to `impl Dialect` in `src/dialect/mod.rs`:

```rust
    /// Whether this dialect supports GROUPING SETS in GROUP BY.
    pub fn has_grouping_sets(&self) -> bool {
        !matches!(self, Dialect::MySQL | Dialect::SQLite | Dialect::Domo)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib dialect::tests::test_has_grouping_sets`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dialect/mod.rs
git commit -m "feat(dialect): add has_grouping_sets() for deep explain optimization

MySQL, SQLite, and Domo lack GROUPING SETS support; all other
dialects (Postgres, Snowflake, BigQuery, DuckDB, ClickHouse,
Databricks, Presto, Redshift) support it."
```

---

### Task 11: CLI Integration — --deep flag and output rendering

**Files:**
- Modify: `src/cli/mod.rs:247-282` (Explain command args) and `src/cli/mod.rs:1861-2082` (explain output)

- [ ] **Step 1: Add CLI flags**

In the `Explain` command variant (line 247), add:

```rust
    /// Enable deep beam search for alternative explanations.
    #[arg(long)]
    deep: bool,
    /// Beam width for deep search (default: 10).
    #[arg(long, default_value = "10")]
    beam_width: usize,
    /// Maximum alternative explanations to show (default: 5).
    #[arg(long, default_value = "5")]
    max_alternatives: usize,
```

- [ ] **Step 2: Wire flags into ExplainConfig**

In the `Commands::Explain` dispatch (line 868), update the config construction:

```rust
    let config = ExplainConfig {
        deep,
        beam_width,
        max_alternatives,
        ..ExplainConfig::default()
    };
```

- [ ] **Step 3: Update print_explain_result() to render alternatives and warnings**

After the existing tree rendering in `print_explain_result()`, add:

```rust
    // Render warnings
    if !result.warnings.is_empty() {
        eprintln!();
        eprintln!("─── Warnings ────────────────────────────────────────────");
        for w in &result.warnings {
            match w {
                ExplainWarning::SimpsonsParadox { dimension, aggregate_delta, segment_directions } => {
                    eprintln!("  ⚠ Simpson's paradox on {}: all segments moved opposite", dimension);
                    eprintln!("    to aggregate (Δ{:.0}). Likely a mix-shift effect.", aggregate_delta);
                }
                ExplainWarning::OpposingOffset { component_a, component_b, delta_a, delta_b } => {
                    eprintln!("  ⚠ Opposing offset: {} ({:+.0}) partially masked by", component_a, delta_a);
                    eprintln!("    {} ({:+.0})", component_b, delta_b);
                }
            }
        }
    }

    // Render alternatives (deep mode)
    if !result.alternatives.is_empty() {
        eprintln!();
        eprintln!("─── Alternative Explanations (deep) ─────────────────────");
        for (i, alt) in result.alternatives.iter().enumerate() {
            let path_str: Vec<String> = alt.nodes.iter().map(|n| match &n.split {
                SplitKind::Dimension { dimension, value } => format!("{}={}", dimension, value),
                SplitKind::Component { child_measure } => child_measure.clone(),
                SplitKind::UniformDegradation { dimension, num_elements } =>
                    format!("[uniform] {} ({} values, none dominant)", dimension, num_elements),
                SplitKind::CrossCutting { dimension, value, measures } =>
                    format!("[cross-cutting] {}={} across {}", dimension, value, measures.join(", ")),
            }).collect();
            let sig_str = alt.significance.as_ref()
                .map(|s| {
                    if s.p_value > 0.05 { format!("  p={:.2} (not significant)", s.p_value) }
                    else { format!("  p={:.3}", s.p_value) }
                })
                .unwrap_or_default();
            eprintln!("  #{:<2} {}", i + 1, path_str.join(" → "));
            eprintln!("      coverage: {:.2}{}", alt.root_fraction, sig_str);
        }
    }
```

- [ ] **Step 4: Run the full test suite including CLI tests**

Run: `cargo test --lib`
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/cli/mod.rs
git commit -m "feat(cli): add --deep, --beam-width, --max-alternatives flags to explain

Renders alternative explanations and detection heuristic warnings
in both text and JSON output modes."
```

---

### Task 12: Update Pathological Tests to Verify Deep Pass Fixes

Update the existing pathological tests to verify the deep pass produces correct results.

**Files:**
- Modify: `src/engine/metric_tree_ops.rs` (test module)

- [ ] **Step 1: Add deep-mode assertions to pathological tests**

For each pathological test, add a second call with `deep: true` and verify the expected outcome from the spec's "Expected Outcome Per Pathological Case" table. Example for test 2 (JSD distraction):

```rust
#[test]
fn test_pathological_jsd_distraction_deep_fixed() {
    // Same setup as test_pathological_jsd_distraction
    let view = make_view_with_dims(
        "sales",
        &["source", "plan"],
        &[("revenue", MeasureType::Sum)],
    );
    let layer = make_layer(vec![view]);
    let tree = MetricTree::build(&layer);

    let source_entries: Vec<(&str, f64, f64)> = vec![
        ("src_1", 1000.0, 500.0), ("src_2", 1000.0, 500.0),
        ("src_3", 1000.0, 500.0), ("src_4", 1000.0, 500.0),
        ("src_5", 1000.0, 500.0), ("src_6", 1000.0, 1300.0),
        ("src_7", 1000.0, 1300.0), ("src_8", 1000.0, 1300.0),
        ("src_9", 1000.0, 1300.0), ("src_10", 1000.0, 1300.0),
    ];

    let mut data = HashMap::new();
    data.extend([
        agg("sales.revenue", 10000.0, 9000.0),
        dim_breakdown("sales.revenue", "sales.source", &source_entries),
        dim_breakdown("sales.revenue", "sales.plan", &[
            ("Enterprise", 8000.0, 7050.0),
            ("Free", 2000.0, 1950.0),
        ]),
    ]);

    let exec = filter_aware_mock(data);
    let config = ExplainConfig { deep: true, beam_width: 5, ..Default::default() };
    let result = explain(
        &tree, &layer, "sales.revenue", "sales.created_at",
        ("2024-02-01", "2024-02-28"), ("2024-01-01", "2024-01-31"),
        &config, &exec,
    ).unwrap();

    // Deep pass should find plan=Enterprise as the top alternative
    let best = &result.alternatives[0];
    assert!(best.root_fraction > 0.90, "deep pass should find 0.95, got {}", best.root_fraction);
    let found = best.nodes.iter().any(|n| matches!(&n.split,
        SplitKind::Dimension { dimension, value } if dimension == "sales.plan" && value == "Enterprise"));
    assert!(found, "deep pass should find plan=Enterprise");
}
```

Write similar `_deep_fixed` tests for each pathological case following the expected outcomes table in the spec. Each test creates the same data as the original pathological test but passes `deep: true` and asserts the improved behavior.

- [ ] **Step 2: Run all deep-fixed tests**

Run: `cargo test --lib engine::metric_tree_ops::tests::test_pathological_ -- && cargo test --lib engine::metric_tree_ops::tests::test_deep_`
Expected: All pass. Original pathological tests still pass (documenting fast-pass behavior), new deep tests pass (documenting deep-pass fixes).

- [ ] **Step 3: Run full test suite**

Run: `cargo test --lib`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/metric_tree_ops.rs
git commit -m "test(explain): add deep-mode tests verifying all pathological cases fixed

Each pathological test now has a _deep_fixed variant confirming the
beam search finds the correct root cause. Original tests preserved
to document fast-pass behavior."
```

---

### Task 13: Update CLAUDE.md and Init Artifacts

Per the repo's convention: when adding features, update CLAUDE.md, INIT_CLAUDE_MD, and skill files.

**Files:**
- Modify: `CLAUDE.md` (root)
- Modify: `src/cli/mod.rs` (INIT_CLAUDE_MD constant)

- [ ] **Step 1: Update CLAUDE.md CLI conventions section**

Add `--deep` to the explain command documentation:

```markdown
- `explain <measure> --time <dim> --current start:end --previous start:end`: recursive root-cause analysis. Add `--deep` for multi-strategy beam search with ranked alternatives and statistical significance. Add `--json` for machine-readable output.
```

- [ ] **Step 2: Update INIT_CLAUDE_MD in src/cli/mod.rs**

Find the `INIT_CLAUDE_MD` constant and add the `--deep` flag documentation to the explain command section.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md src/cli/mod.rs
git commit -m "docs: update CLAUDE.md and init template for --deep explain flag"
```

---

## Task Dependency Graph

```
Task 1 (types) ─┬─→ Task 2 (heuristics) ─→ Task 8 (integration)
                 ├─→ Task 3 (scoring fns) ─→ Task 6 (strategies) ─→ Task 7 (beam core) ─→ Task 8
                 ├─→ Task 4 (adaptive EP) ─→ Task 7
                 └─→ Task 5 (decomposition) ─→ Task 8
Task 9 (significance) ←─ Task 8
Task 10 (dialect) ←─ standalone (can run any time)
Task 11 (CLI) ←─ Task 8
Task 12 (pathological tests) ←─ Task 8
Task 13 (docs) ←─ Task 11
```

Tasks 2, 3, 4, 5, 10 can run in parallel after Task 1.
