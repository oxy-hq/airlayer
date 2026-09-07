# Peer Cohorts (Cross-Sectional Comparability) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a `.view.yml` declare which instances of an entity may be benchmarked against each other at one point in time, and resolve those peer cohorts into per-subject baselines and gaps.

**Architecture:** Named cohorts live on the entity beside `lifespan`, because "who are my peers" is intrinsic to the entity. Resolution is **not** SQL: one entity-grain query pulls the universe, and a Rust loop does the correlated per-subject band match and the R-7 median. The result is a **sibling** of `OpportunityResult`, never folded into it — `opportunity`'s result types are rigidly one-scalar-per-dimension and a cohort baseline is per-subject.

**Tech Stack:** Rust, serde/serde_yaml, clap (CLI), DuckDB (tier-1 integration tests). No new dependencies.

**Spec:** [`docs/superpowers/specs/2026-09-05-cross-sectional-comparability-design.md`](../specs/2026-09-05-cross-sectional-comparability-design.md) (verified against main @ `dc3f209`, 2026-09-07). Read §2 before Task 5 — it is the reason `resolve_cohort` exists at all instead of a `--cohort` flag on `opportunity`.

**Branch:** `haitrr/peer-cohorts`, based on `main` @ `dc3f209`. This plan and its spec are
the only commit on it; create a worktree from this branch to implement.

## Global Constraints

- **Scope is phases 1-3 (airlayer only).** No oxy endpoint, no TS SDK. Do not touch the `oxy` repo.
- **`opportunity` and `opportunity_drill` are not modified.** Not their signatures, not their result types, not `select_benchmark`, not `benchmark_filter`. If a task seems to require it, stop and report — that means the sibling-result architecture has sprung a leak.
- **Do NOT run `cargo check --all-features`.** It enables `wasm` alongside `exec-*` and `pub mod executor` is `#[cfg(not(feature = "wasm"))]`. Use `cargo test --lib` and `cargo check --features cli`.
- **No commit trailers.** No `Co-Authored-By`, no "Generated with" footer, no bot attribution of any kind.
- **TDD, strictly.** Write the failing test, run it, confirm it fails *for the stated reason* (not a compile error in the test harness itself), then implement. If a test passes before you write the implementation, the test is wrong — fix the test, do not proceed.
- **Never edit an existing passing test to accommodate new code** without reporting it. Two bugs in the companion PR were caught precisely because existing tests were treated as ground truth.
- Cohort membership is **non-reciprocal by design** (§9). Never "optimise" the peer loop into bucketing or `NTILE` — that silently changes every answer.

---

### Task 1: `Cohort` / `CohortBand` schema types on `Entity`

**Files:**
- Modify: `src/schema/models.rs` (add types near `Lifespan` at :44-56; add field to `Entity` at :62-95)
- Test: `src/schema/parser.rs` (tests module, alongside `test_shift_measure_may_omit_type`)

**Interfaces:**
- Consumes: nothing.
- Produces: `Cohort { band: Option<CohortBand>, require: Vec<String>, min_peers: Option<usize>, exclude_self: bool }`, `CohortBand { measure: String, per: Option<String>, tolerance: f64 }`, `Entity.cohorts: Option<BTreeMap<String, Cohort>>`. Tasks 3-8 all read these.

- [ ] **Step 1: Write the failing parse test**

In `src/schema/parser.rs` tests module:

```rust
#[test]
fn test_parse_entity_cohorts() {
    let yaml = r#"
name: stores
table: stores
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
        require: [stores.accounting_basis]
        min_peers: 3
      basis_only:
        require: [stores.accounting_basis]
dimensions:
  - name: store_id
    type: number
    expr: store_id
"#;
    let view: View = serde_yaml::from_str(yaml).expect("parse view with cohorts");
    let ent = &view.entities[0];
    let cohorts = ent.cohorts.as_ref().expect("cohorts present");
    assert_eq!(cohorts.len(), 2);

    let sized = &cohorts["size_matched"];
    let band = sized.band.as_ref().expect("band present");
    assert_eq!(band.measure, "sales.net_sales");
    assert_eq!(band.per.as_deref(), Some("sales.trading_days"));
    assert!((band.tolerance - 0.35).abs() < 1e-9);
    assert_eq!(sized.require, vec!["stores.accounting_basis".to_string()]);
    assert_eq!(sized.min_peers, Some(3));
    // exclude_self defaults to true: a subject is never its own peer.
    assert!(sized.exclude_self);

    // A cohort with no band is legitimate — exact-match only.
    let basis = &cohorts["basis_only"];
    assert!(basis.band.is_none());
    assert_eq!(basis.min_peers, None);

    // BTreeMap ordering is deterministic, so inspect output is stable.
    let names: Vec<&String> = cohorts.keys().collect();
    assert_eq!(names, vec!["basis_only", "size_matched"]);
}

#[test]
fn test_cohort_rejects_unknown_field() {
    // `deny_unknown_fields` so a typo cannot silently disable a rule — the
    // same reasoning as DimensionAnalysis in the companion PR.
    let yaml = r#"
name: stores
table: stores
entities:
  - name: store_id
    type: primary
    key: store_id
    cohorts:
      typo:
        requires: [stores.basis]
dimensions: []
"#;
    let err = serde_yaml::from_str::<View>(yaml).unwrap_err().to_string();
    assert!(
        err.contains("requires") || err.contains("unknown field"),
        "expected unknown-field rejection, got: {err}"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib test_parse_entity_cohorts test_cohort_rejects_unknown_field 2>&1 | tail -20`
Expected: FAIL — `no field 'cohorts' on type Entity` (compile error is acceptable here; the type does not exist yet).

- [ ] **Step 3: Add the types**

In `src/schema/models.rs`, immediately after the `Lifespan` struct:

```rust
/// A named peer cohort on an entity: the declaration of which *other*
/// instances of this entity may be benchmarked against a given subject at one
/// point in time.
///
/// Cross-sectional sibling of [`Lifespan`], which answers the temporal
/// question ("was this entity alive in both windows?"). Declared on the
/// entity, not the view, for the same reason `parent:` is: "who are my peers"
/// is intrinsic to the entity, so any view using it inherits the rule.
///
/// **Named**, plural, because comparability varies per *measure*, not per
/// entity. In the reference implementation only wage cost and giveaway are
/// size-banded; food cost, voids and review rating deliberately are not,
/// justified by measured slope/R². One `comparable:` block on the entity
/// cannot say that.
///
/// ```yaml
/// entities:
///   - name: restaurant_id
///     type: primary
///     key: restaurant_id
///     cohorts:
///       size_matched:
///         band:
///           measure: sales.net_sales
///           per: sales.trading_days   # a MEASURE, never a calendar unit
///           tolerance: 0.35           # multiplicative, subject-centred
///         require: [restaurants.accounting_basis]
///         min_peers: 3
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Cohort {
    /// Size band. Omit for an exact-match-only cohort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub band: Option<CohortBand>,
    /// Dimensions that must match EXACTLY between subject and peer, applied
    /// before the band. A subject whose value here is NULL joins nothing and
    /// is reported as excluded rather than silently vanishing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub require: Vec<String>,
    /// Minimum peers for the subject's baseline to be marked `sufficient`.
    /// Deliberately NOT a gate: a subject below the floor is still returned,
    /// with `sufficient: false` and its peer count. Whether that is usable is
    /// the client's judgement, not the platform's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_peers: Option<usize>,
    /// Whether a subject is excluded from its own peer set. Defaults true.
    #[serde(default = "default_true")]
    pub exclude_self: bool,
}

/// The size band of a [`Cohort`]: multiplicative, symmetric, and centred on
/// **the subject**, so membership is deliberately non-reciprocal — A can be
/// inside B's band while B is outside A's.
///
/// That asymmetry is measured and accepted, not a defect to optimise away
/// (7 of 55 food pairs and 35 of 210 labor pairs in the reference, July 2026).
/// It is why a cohort is a correlated self-join per subject and never a
/// bucketing or `NTILE` partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CohortBand {
    /// The measure whose magnitude defines "similar size".
    pub measure: String,
    /// Divisor measure. The band compares `measure / per`, never a raw total.
    ///
    /// This is a MEASURE, not a calendar unit. Dividing a window's total by a
    /// constant number of days orders entities identically to the raw total,
    /// leaving the band mathematically unchanged — and banding on the total is
    /// a known real bug: trailing totals conflate size with tenure, so new
    /// stores' 90-day totals read as small stores'. In the reference one store
    /// went from 0 peers to 6 once the divisor became per-entity trading days.
    ///
    /// Omit to band on the raw measure — the caller's explicit choice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per: Option<String>,
    /// Multiplicative half-width. `0.35` means a peer's normalised value must
    /// fall within `[subject * 0.65, subject * 1.35]`. No upper bound —
    /// `1.0` ("up to 2×") is legitimate.
    pub tolerance: f64,
}

fn default_true() -> bool {
    true
}
```

Then add to `Entity`, after the `lifespan` field:

```rust
    /// Named peer cohorts: which other instances of this entity may be
    /// benchmarked against a given subject. See [`Cohort`]. `BTreeMap` for
    /// deterministic ordering, so `inspect --json` output is stable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cohorts: Option<BTreeMap<String, Cohort>>,
```

Add `use std::collections::BTreeMap;` to the imports at the top of `models.rs` if not already present (check first — `HashMap` is imported; `BTreeMap` may not be).

**Note:** `default_true` may already exist in this file — grep before adding, and reuse rather than duplicating.

- [ ] **Step 4: Fix every `Entity { .. }` literal**

`Entity` is constructed by struct literal in the foreign-model parsers and tests. Adding a field breaks them all. Find them:

```bash
grep -rn "Entity {" src/ tests/ | grep -v "^src/schema/models.rs"
```

Add `cohorts: None,` to each. Do NOT add `#[serde(default)]`-style shortcuts by making the struct `Default` — follow whatever the neighbouring fields do at each site.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib test_parse_entity_cohorts test_cohort_rejects_unknown_field 2>&1 | tail -20`
Expected: PASS, 2 tests.

Then the full suite, to catch the struct-literal breakage: `cargo test --lib 2>&1 | tail -5`
Expected: PASS, 836+ tests.

- [ ] **Step 6: Commit**

```bash
git add src/schema/models.rs src/schema/parser.rs
git commit -m "Add named peer cohorts to the entity schema"
```

---

### Task 2: `Measure.default_cohort`

**Files:**
- Modify: `src/schema/models.rs` — `Measure` struct (:846-890), its hand-written `Deserialize` `Repr` (:895-920) **and** the `Ok(Measure { .. })` construction (:930-947)
- Test: `src/schema/parser.rs` tests; `src/engine/preagg.rs` tests (fingerprint immunity)

**Interfaces:**
- Consumes: nothing.
- Produces: `Measure.default_cohort: Option<String>` holding `"entity.cohort_name"`. Task 7 reads it to default the CLI's `--cohort`.

**Why this field exists:** the reference binds cohort↔metric statically because a query-time-only choice creates the one bug class a reader can neither see nor check — the screen claims "stores its size" while the query compared everyone. The decision (spec §4) is *both*: `default_cohort` on the measure, `--cohort` overrides it, and `PeerCohortResult.cohort` always names what was actually used. The result being self-describing is what closes the bug class.

- [ ] **Step 1: Write the failing tests**

In `src/schema/parser.rs` tests:

```rust
#[test]
fn test_measure_default_cohort_parses() {
    let yaml = r#"
name: sales
table: sales
measures:
  - name: wage_cost_pct
    type: number
    expr: "{{sales.wage_cost}} / NULLIF({{sales.net_sales}}, 0)"
    direction: lower_is_better
    default_cohort: restaurant_id.size_matched
  - name: net_sales
    type: sum
    expr: net_sales
dimensions: []
"#;
    let view: View = serde_yaml::from_str(yaml).expect("parse");
    let m = view.measures_list();
    let wage = m.iter().find(|m| m.name == "wage_cost_pct").unwrap();
    assert_eq!(wage.default_cohort.as_deref(), Some("restaurant_id.size_matched"));
    // Absent stays absent — never inferred.
    let net = m.iter().find(|m| m.name == "net_sales").unwrap();
    assert_eq!(net.default_cohort, None);
}
```

In `src/engine/preagg.rs` tests, mirroring the existing `definition_fingerprint_ignores_measure_direction`:

```rust
#[test]
fn definition_fingerprint_ignores_default_cohort() {
    // `default_cohort` is comparability metadata, not part of what a rollup
    // stores. A rollup built before the field was set must stay valid after,
    // or every existing cached rollup silently invalidates on upgrade.
    let mut view = fingerprint_test_view();
    let before = definition_fingerprint(&view, &[], &["total".to_string()], None);
    for m in view.measures.iter_mut() {
        if let MeasureItem::Inline(m) = m {
            m.default_cohort = Some("store_id.size_matched".into());
        }
    }
    let after = definition_fingerprint(&view, &[], &["total".to_string()], None);
    assert_eq!(before, after, "default_cohort must not move the fingerprint");
}
```

**Note:** read the existing `definition_fingerprint_ignores_measure_direction` test (around `preagg.rs:4820`) first and mirror its exact fixture-construction style — the helper name `fingerprint_test_view` above is a placeholder for whatever that test actually uses.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib default_cohort 2>&1 | tail -20`
Expected: FAIL — `no field 'default_cohort' on type Measure`.

- [ ] **Step 3: Add the field in all three places**

`Measure` struct, after `direction`:

```rust
    /// The cohort this measure is compared within by default, as
    /// `"entity.cohort_name"`.
    ///
    /// Comparability varies per measure, not per entity — size matters for
    /// labour cost and giveaway, and deliberately does not for food cost. A
    /// measure that names its cohort here cannot be accidentally compared
    /// against the wrong peer group by a caller who forgot the flag.
    ///
    /// A caller may still override with an explicit cohort; the result always
    /// reports which cohort was actually used, so a consumer rendering that
    /// name cannot drift from the query behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_cohort: Option<String>,
```

`Repr` (inside the hand-written `Deserialize`):

```rust
            #[serde(default)]
            default_cohort: Option<String>,
```

`Ok(Measure { .. })` construction:

```rust
            default_cohort: r.default_cohort,
```

**All three are required.** Adding only the struct field compiles and silently drops the YAML value — the hand-written deserializer is the actual parser.

- [ ] **Step 4: Fix every `Measure { .. }` literal**

```bash
grep -rn "Measure {" src/ tests/ | grep -v "^src/schema/models.rs"
```

Add `default_cohort: None,` to each.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib default_cohort 2>&1 | tail -10` → PASS, 2 tests.
Run: `cargo test --lib 2>&1 | tail -5` → PASS, full suite.

- [ ] **Step 6: Commit**

```bash
git add src/schema/models.rs src/schema/parser.rs src/engine/preagg.rs
git commit -m "Let a measure name its default cohort"
```

---

### Task 3: Validator rules for cohorts

**Files:**
- Modify: `src/schema/validator.rs` — new `validate_cohorts`, called from the same place `validate_promotions` is called
- Test: `src/schema/validator.rs` tests module

**Interfaces:**
- Consumes: `Cohort`, `CohortBand`, `Entity.cohorts` (Task 1); `Measure.default_cohort` (Task 2).
- Produces: nothing consumed by later tasks — this is a leaf. Task 5 may *assume* validated input.

**Rules (spec §4.2), all hard errors:**
1. Cohorts only on `type: primary` entities — a cohort needs a row identity. Mirrors the existing `parent:` rule.
2. `band.measure`, `band.per`, and every `require` member resolve to a real member.
3. `tolerance` is finite and `> 0`. No upper bound.
4. `min_peers >= 1`.
5. The entity's key has a backing dimension (name match, then `expr` match). Without it the entity-grain pull in Task 5 cannot be expressed, and this must fail at validation with an actionable message rather than at query time.
6. `Measure.default_cohort` resolves to a declared `entity.cohort_name`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn test_cohort_on_foreign_entity_errors() {
    let layer = layer_with_cohort_on_foreign();
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(
        errs.iter().any(|e| e.contains("not a primary entity")),
        "expected primary-entity rejection, got: {errs:?}"
    );
}

#[test]
fn test_cohort_unknown_band_measure_errors() {
    let layer = layer_with_cohort_band_measure("sales.no_such_measure");
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(errs.iter().any(|e| e.contains("no_such_measure")));
}

#[test]
fn test_cohort_zero_tolerance_errors() {
    let layer = layer_with_cohort_tolerance(0.0);
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(errs.iter().any(|e| e.contains("tolerance")));
}

#[test]
fn test_cohort_large_tolerance_is_valid() {
    // `1.0` means "up to 2x" and is legitimate. The first draft's (0,1)
    // upper bound was arbitrary; do not reintroduce it.
    let layer = layer_with_cohort_tolerance(1.5);
    assert!(SchemaValidator::validate(&layer).is_ok());
}

#[test]
fn test_cohort_min_peers_zero_errors() {
    let layer = layer_with_cohort_min_peers(0);
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(errs.iter().any(|e| e.contains("min_peers")));
}

#[test]
fn test_cohort_key_without_backing_dimension_errors() {
    // The entity-grain pull selects the key AS A DIMENSION. If no dimension
    // answers to the key by name or by expr, the pull cannot be built — say
    // so here, not at query time.
    let layer = layer_with_cohort_but_no_key_dimension();
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(
        errs.iter().any(|e| e.contains("no dimension")),
        "expected backing-dimension error, got: {errs:?}"
    );
}

#[test]
fn test_measure_default_cohort_unknown_errors() {
    let layer = layer_with_default_cohort("store_id.no_such_cohort");
    let errs = SchemaValidator::validate(&layer).unwrap_err();
    assert!(errs.iter().any(|e| e.contains("no_such_cohort")));
}
```

Write the `layer_with_*` fixture helpers in the tests module. Follow the construction style of the existing `test_shift_comparable_by_unknown_entity_errors` fixtures in this same file — read those first and match them.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib validator::tests::test_cohort 2>&1 | tail -20`
Expected: FAIL — all assertions fail because no cohort validation exists yet (the layers validate clean).

- [ ] **Step 3: Implement `validate_cohorts`**

```rust
    /// Validate `cohorts:` declarations on entities, and `default_cohort:` on
    /// measures.
    ///
    /// All hard errors. A cohort that cannot be resolved is not a degraded
    /// cohort — it is a silently wrong benchmark, which is the failure mode
    /// this whole feature exists to prevent.
    fn validate_cohorts(layer: &SemanticLayer, errors: &mut Vec<String>) {
        // Every declared cohort, as "entity.name", for default_cohort checks.
        let mut declared: HashSet<String> = HashSet::new();

        for view in &layer.views {
            for entity in &view.entities {
                let Some(cohorts) = entity.cohorts.as_ref() else {
                    continue;
                };
                if entity.entity_type != EntityType::Primary {
                    errors.push(format!(
                        "[{}] entity '{}' declares `cohorts:` but is not a primary entity. \
                         A cohort compares instances of an entity, which needs a row identity; \
                         foreign declarations are usages and cannot carry cohorts.",
                        view.name, entity.name
                    ));
                    continue;
                }

                // The entity-grain pull selects the key as a dimension.
                let keys = entity.get_keys();
                for key in &keys {
                    let backed = view
                        .dimensions_list()
                        .iter()
                        .any(|d| &d.name == key || d.expr.as_deref() == Some(key.as_str()));
                    if !backed {
                        errors.push(format!(
                            "[{}] entity '{}' declares `cohorts:` but its key '{}' has no \
                             dimension backing it (matched by name, then by expr). Cohort \
                             resolution selects the key as a dimension to group the entity \
                             universe; declare a dimension for '{}'.",
                            view.name, entity.name, key, key
                        ));
                    }
                }

                for (cohort_name, cohort) in cohorts {
                    declared.insert(format!("{}.{}", entity.name, cohort_name));

                    if let Some(band) = &cohort.band {
                        if !band.tolerance.is_finite() || band.tolerance <= 0.0 {
                            errors.push(format!(
                                "[{}] cohort '{}.{}' has tolerance {} — must be finite and > 0. \
                                 (There is deliberately no upper bound: 1.0 means 'up to 2x' \
                                 and is legitimate.)",
                                view.name, entity.name, cohort_name, band.tolerance
                            ));
                        }
                        Self::require_member(
                            layer, &band.measure, view, entity, cohort_name, "band.measure", errors,
                        );
                        if let Some(per) = &band.per {
                            Self::require_member(
                                layer, per, view, entity, cohort_name, "band.per", errors,
                            );
                        }
                    }

                    for req in &cohort.require {
                        Self::require_member(
                            layer, req, view, entity, cohort_name, "require", errors,
                        );
                    }

                    if let Some(mp) = cohort.min_peers {
                        if mp < 1 {
                            errors.push(format!(
                                "[{}] cohort '{}.{}' has min_peers: 0 — a cohort needs at least \
                                 one peer to have a baseline at all.",
                                view.name, entity.name, cohort_name
                            ));
                        }
                    }
                }
            }
        }

        for view in &layer.views {
            for m in view.measures_list() {
                let Some(dc) = m.default_cohort.as_deref() else {
                    continue;
                };
                if !declared.contains(dc) {
                    errors.push(format!(
                        "[{}] measure '{}' declares `default_cohort: {}` but no entity declares \
                         that cohort. Expected 'entity_name.cohort_name'; declared cohorts are: {}.",
                        view.name,
                        m.name,
                        dc,
                        if declared.is_empty() {
                            "(none)".to_string()
                        } else {
                            let mut d: Vec<&String> = declared.iter().collect();
                            d.sort();
                            d.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                        }
                    ));
                }
            }
        }
    }

    /// Check that `member` ("view.member") resolves to a dimension or measure.
    fn require_member(
        layer: &SemanticLayer,
        member: &str,
        view: &View,
        entity: &Entity,
        cohort_name: &str,
        field: &str,
        errors: &mut Vec<String>,
    ) {
        let ok = member.split_once('.').is_some_and(|(v, name)| {
            layer.views.iter().any(|target| {
                target.name == v
                    && (target.dimensions_list().iter().any(|d| d.name == name)
                        || target.measures_list().iter().any(|m| m.name == name))
            })
        });
        if !ok {
            errors.push(format!(
                "[{}] cohort '{}.{}' field `{}` references '{}', which does not resolve to a \
                 dimension or measure. Expected 'view.member'.",
                view.name, entity.name, cohort_name, field, member
            ));
        }
    }
```

Call `Self::validate_cohorts(layer, &mut errors);` from the same function that calls `validate_promotions` — find it with `grep -n "validate_promotions(" src/schema/validator.rs`.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib validator::tests::test_cohort test_measure_default_cohort 2>&1 | tail -10` → PASS, 7 tests.
Run: `cargo test --lib 2>&1 | tail -5` → PASS, full suite. **If an existing validator test now fails, stop and report** — it means a fixture in the repo declares something these rules reject, which is a finding about the rules, not a licence to edit the fixture.

- [ ] **Step 5: Commit**

```bash
git add src/schema/validator.rs
git commit -m "Validate cohort declarations"
```

---

### Task 4: Surface cohorts in `inspect --json`

**Files:**
- Modify: `src/cli/mod.rs` — the entity serialisation in `inspect --json`, and the `ontology` block (~:1707)
- Test: `tests/integration_tests.rs`

**Interfaces:**
- Consumes: `Entity.cohorts`, `Measure.default_cohort`.
- Produces: JSON keys `entities[].cohorts` and `ontology.comparability`. No Rust API.

A cohort is a **new edge kind** in the ontology, beside the existing containment and categorical promotions: a symmetric-in-intent, asymmetric-in-fact relation over one entity's own instances. Surface it as such rather than squeezing it into `promotions`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn test_inspect_json_surfaces_cohorts() {
    // Build a layer with a cohort, run inspect --json, assert the shape.
    let out = run_inspect_json("tests/integration/views-cohort/");
    let ent = &out["views"][0]["entities"][0];
    assert_eq!(ent["cohorts"]["size_matched"]["band"]["measure"], "sales.net_sales");
    assert_eq!(ent["cohorts"]["size_matched"]["band"]["per"], "sales.trading_days");
    assert_eq!(ent["cohorts"]["size_matched"]["min_peers"], 3);

    // The ontology block names comparability as its own edge kind.
    let comp = &out["ontology"]["comparability"];
    assert_eq!(comp[0]["entity"], "store_id");
    assert_eq!(comp[0]["cohort"], "size_matched");
    assert_eq!(comp[0]["id"], "c_store_id_size_matched");
    assert_eq!(comp[0]["reciprocal"], false);
}
```

Match the existing inspect-JSON test style in `tests/integration_tests.rs` — read the nearest one (`grep -n "inspect --json\|inspect_json" tests/integration_tests.rs`) and mirror how it invokes the CLI and parses output.

Create the fixture `tests/integration/views-cohort/stores.view.yml` and `sales.view.yml` — a `stores` view with a `store_id` primary entity carrying both cohorts from Task 1's YAML, and a `sales` view supplying `net_sales` and `trading_days`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features cli test_inspect_json_surfaces_cohorts 2>&1 | tail -20`
Expected: FAIL — `cohorts` key absent (null).

- [ ] **Step 3: Implement the surfacing**

In the entity JSON construction, add `cohorts` when present (serde already serialises `Cohort` correctly, so `serde_json::to_value(&entity.cohorts)` suffices).

In the `ontology` block, add:

```rust
    // Comparability: a relation over ONE entity's own instances, distinct from
    // the containment and categorical promotions above. Symmetric in intent,
    // asymmetric in fact — a subject-centred band means A can be in B's cohort
    // while B is outside A's, which is why `reciprocal` is stated explicitly
    // rather than assumed.
    let mut comparability = Vec::new();
    for view in &layer.views {
        for entity in &view.entities {
            let Some(cohorts) = entity.cohorts.as_ref() else { continue };
            for (name, cohort) in cohorts {
                comparability.push(serde_json::json!({
                    "id": format!("c_{}_{}", entity.name, name),
                    "entity": entity.name,
                    "cohort": name,
                    "view": view.name,
                    "banded": cohort.band.is_some(),
                    "band_measure": cohort.band.as_ref().map(|b| &b.measure),
                    "band_per": cohort.band.as_ref().and_then(|b| b.per.as_ref()),
                    "tolerance": cohort.band.as_ref().map(|b| b.tolerance),
                    "require": cohort.require,
                    "min_peers": cohort.min_peers,
                    "exclude_self": cohort.exclude_self,
                    "reciprocal": false,
                }));
            }
        }
    }
    if !comparability.is_empty() {
        ontology.insert("comparability".to_string(), serde_json::Value::Array(comparability));
    }
```

- [ ] **Step 4: Run the test**

Run: `cargo test --features cli test_inspect_json_surfaces_cohorts 2>&1 | tail -10` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cli/mod.rs tests/integration_tests.rs tests/integration/views-cohort/
git commit -m "Surface cohorts in inspect --json and the ontology block"
```

---

### Task 5: `resolve_cohort` — the entity-grain pull and its guards

**Files:**
- Create: `src/engine/cohort.rs`
- Modify: `src/engine/mod.rs` (add `pub mod cohort;` in alphabetical position, before `evaluator`)
- Modify: `src/engine/metric_tree_ops.rs` — change `fn quantile_r7` to `pub(crate) fn quantile_r7`; same for `BenchmarkStatistic` if not already `pub` (it is)
- Test: `src/engine/cohort.rs` tests module

**Why a new module:** `metric_tree_ops.rs` is 22k lines. `src/engine/shift.rs` (321 lines) is the established precedent for a focused engine module holding one comparison primitive. Cohorts are the cross-sectional sibling of shift; they belong beside it, not inside the opportunity file.

**Interfaces:**
- Consumes: `Cohort`/`CohortBand` (Task 1); `QueryExecutor` (`metric_tree_ops.rs:4218`); `BenchmarkStatistic` and `quantile_r7` (`metric_tree_ops.rs:3662`, `:3690`); `DEFAULT_QUERY_LIMIT`/`UNBOUNDED_QUERY_LIMIT` (`engine/mod.rs:28`, `:39`).
- Produces:

```rust
pub const MAX_COHORT_ENTITIES: usize = 5_000;

pub struct PeerCohortResult {
    pub entity: String,
    pub cohort: String,
    pub measure: String,
    pub statistic: BenchmarkStatistic,
    pub period: (String, String),
    pub subjects: Vec<CohortSubject>,
    pub excluded: Vec<ExcludedSubject>,
}
pub struct CohortSubject {
    pub key: String,
    pub value: f64,
    pub baseline: f64,
    pub gap: f64,
    pub peers: Vec<String>,
    pub peer_count: usize,
    pub sufficient: bool,
}
pub struct ExcludedSubject { pub key: String, pub reason: String }

pub fn augment_layer_for_cohort(layer: &mut SemanticLayer, entity: &str) -> bool;
pub fn resolve_cohort(
    layer: &SemanticLayer,
    entity: &str,
    cohort: &str,
    measure: &str,
    time_dimension: &str,
    period: (&str, &str),
    statistic: BenchmarkStatistic,
    executor: &QueryExecutor,
) -> Result<PeerCohortResult, EngineError>;
```

Task 6 fills in the peer loop; Task 7 calls `resolve_cohort` from the CLI.

**Deviation from spec §5, deliberate:** the spec's signature omits `measure` and `time_dimension`. Both are required — the pull needs a target measure to compare and a time dimension to filter the period. Recorded here rather than silently changed.

- [ ] **Step 1: Write the failing guard tests**

```rust
#[test]
fn resolve_cohort_refuses_a_truncated_universe() {
    // DEFAULT_QUERY_LIMIT is 10_000. A pull that comes back exactly at a cap
    // is indistinguishable from a complete one by inspection, so the count
    // cross-check is the only thing standing between us and a median computed
    // over an arbitrary 10k slice — a wrong number with no error.
    let layer = cohort_test_layer();
    let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
        if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
            return Ok(vec![row(&[("__cohort_total__", json!(12_000.0))])]);
        }
        Ok((0..10_000).map(|i| entity_row(i)).collect())
    };
    let err = resolve_cohort(
        &layer, "store_id", "size_matched", "sales.wage_pct",
        "sales.sale_date", ("2024-01-01", "2024-12-31"),
        BenchmarkStatistic::Median, &executor,
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("10000") && msg.contains("12000"), "got: {msg}");
}

#[test]
fn resolve_cohort_sets_an_explicit_unbounded_limit() {
    // Both halves of the guard matter: the explicit limit prevents the common
    // case, the count assertion catches a warehouse-side cap we don't control.
    let layer = cohort_test_layer();
    let seen = std::sync::Mutex::new(Vec::new());
    let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
        seen.lock().unwrap().push(q.limit);
        if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
            return Ok(vec![row(&[("__cohort_total__", json!(2.0))])]);
        }
        Ok(vec![entity_row(0), entity_row(1)])
    };
    let _ = resolve_cohort(
        &layer, "store_id", "size_matched", "sales.wage_pct",
        "sales.sale_date", ("2024-01-01", "2024-12-31"),
        BenchmarkStatistic::Median, &executor,
    );
    let limits = seen.lock().unwrap();
    assert!(
        limits.iter().any(|l| *l == Some(crate::engine::UNBOUNDED_QUERY_LIMIT)),
        "the entity-grain pull must set an explicit unbounded limit, saw {limits:?}"
    );
}

#[test]
fn resolve_cohort_refuses_an_unbounded_entity_grain() {
    // A cohort self-join is O(N^2): 23 restaurants is 529 pairs, 2M customers
    // is 4e12. Refuse with the count in the message rather than hanging.
    let layer = cohort_test_layer();
    let executor = |q: &QueryRequest| -> Result<Vec<Row>, EngineError> {
        if q.measures.iter().any(|m| m.contains("__cohort_total__")) {
            return Ok(vec![row(&[("__cohort_total__", json!(2_000_000.0))])]);
        }
        Ok(vec![])
    };
    let err = resolve_cohort(
        &layer, "store_id", "size_matched", "sales.wage_pct",
        "sales.sale_date", ("2024-01-01", "2024-12-31"),
        BenchmarkStatistic::Median, &executor,
    )
    .unwrap_err();
    assert!(err.to_string().contains("2000000"));
    assert!(err.to_string().contains("5000"));
}

#[test]
fn resolve_cohort_rejects_an_unknown_cohort_name() {
    let layer = cohort_test_layer();
    let executor = |_: &QueryRequest| -> Result<Vec<Row>, EngineError> { Ok(vec![]) };
    let err = resolve_cohort(
        &layer, "store_id", "no_such", "sales.wage_pct",
        "sales.sale_date", ("2024-01-01", "2024-12-31"),
        BenchmarkStatistic::Median, &executor,
    )
    .unwrap_err();
    assert!(err.to_string().contains("no_such"));
}
```

Write `cohort_test_layer()`, `entity_row(i)`, and `row(&[..])` helpers in the same tests module. `cohort_test_layer` builds a `stores` view (primary `store_id` entity with the `size_matched` cohort) and a `sales` view (`net_sales`, `trading_days`, `wage_pct`).

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib cohort:: 2>&1 | tail -20`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement the module skeleton, the pull, and the guards**

Create `src/engine/cohort.rs`. Implement, in order:

1. The result types above (all `#[derive(Debug, Clone, Serialize)]`; `ExcludedSubject` and `CohortSubject` also `Deserialize`).
2. `augment_layer_for_cohort(layer, entity)` — install `__cohort_total__` as a `count_distinct` measure over the entity's key on the entity's owning view. Follow `augment_layer_for_opportunity`'s clone-and-install shape (`metric_tree_ops.rs:2583`); it is the established precedent, including the requirement that the *same* augmented layer reach both the engine and this function.
3. Resolution helpers: find the entity's owning view (the view declaring it `Primary`), resolve the key to its backing dimension (name match, then `expr` match — mirror `identifier_dimensions` at `:6077`), look up the named cohort.
4. `resolve_cohort`:
   - Resolve entity → owning view → cohort. Error naming the cohort if absent.
   - Run the count query first (`measures: ["<view>.__cohort_total__"]`, no dimensions, period filter). Refuse if `> MAX_COHORT_ENTITIES`, message carrying both the count and the ceiling.
   - Build the pull: `dimensions: [key_dim, ...cohort.require]`, `measures: [measure, band.measure, band.per]` (dedup; omit band members when there is no band), `time_dimensions: [{dimension: time_dimension, date_range: [start, end]}]`, **`limit: Some(UNBOUNDED_QUERY_LIMIT)`**.
   - Cross-check `rows.len()` against the count. Refuse on mismatch, message carrying both numbers.
   - Return an empty-subjects result for now; Task 6 fills the loop.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib cohort:: 2>&1 | tail -10` → PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/engine/cohort.rs src/engine/mod.rs src/engine/metric_tree_ops.rs
git commit -m "Add the cohort module: entity-grain pull with truncation and cardinality guards"
```

---

### Task 6: The peer loop — band, exact match, R-7 baseline, refusal channels

**Files:**
- Modify: `src/engine/cohort.rs`
- Test: `src/engine/cohort.rs` tests module

**Interfaces:**
- Consumes: everything from Task 5.
- Produces: populated `PeerCohortResult.subjects` and `.excluded`.

**The algorithm, per subject A:**
1. If A's `require` tuple contains a NULL → `excluded` with reason, and A is not a peer for anyone.
2. Candidate peers B: same `require` tuple; `B != A` when `exclude_self`; and, when banded, `norm(B)` within `[norm(A) * (1 - tol), norm(A) * (1 + tol)]` where `norm(X) = band.measure(X) / band.per(X)`, or the raw measure when `per` is absent.
3. `baseline` = the chosen statistic over the peers' **target measure** values (`quantile_r7` for median/p75, min/max for best_peer — reuse the polarity logic; do NOT reimplement it).
4. `gap` is **polarity-aware and positive-means-opportunity**, matching the convention `opportunity` uses: `HigherIsBetter => baseline - value`, `LowerIsBetter => value - baseline`. All five reference metrics are lower-is-better, so getting this backwards inverts every answer.
5. `sufficient = peers.len() >= min_peers.unwrap_or(1)`. **Never a filter** — the subject is returned either way.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn cohort_membership_is_not_reciprocal() {
    // THE property most likely to be "optimised" into a bucketing later.
    // tol=0.35, so a subject at X admits peers in [0.65X, 1.35X]:
    //   a=100 -> [65, 135]   contains c(130), excludes b(200)
    //   b=200 -> [130, 270]  contains c(130), EXCLUDES a(100)
    // c is a peer of both, a and b are not peers of each other, and the
    // relation is directional. Asserting only one side would also pass
    // against a bucketing implementation, which is the thing to catch.
    let layer = cohort_test_layer();
    let rows = vec![
        subject_row("a", 100.0, 1.0, 10.0),
        subject_row("b", 200.0, 1.0, 20.0),
        subject_row("c", 130.0, 1.0, 15.0),
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
    let b = res.subjects.iter().find(|s| s.key == "b").unwrap();
    assert!(a.peers.contains(&"c".to_string()), "c(130) is inside a(100)'s +-35% band");
    assert!(!a.peers.contains(&"b".to_string()), "b(200) is outside a(100)'s band");
    assert!(!b.peers.contains(&"a".to_string()), "a(100) is outside b(200)'s band");
    assert!(b.peers.contains(&"c".to_string()), "c(130) is inside b(200)'s band");
}

#[test]
fn the_band_normalises_by_the_per_measure() {
    // The test that would have caught the `per: Day` mistake. Two entities
    // with EQUAL totals but different trading-day counts must land in
    // different bands — banding on the raw total conflates size with tenure.
    let layer = cohort_test_layer();
    let rows = vec![
        subject_row("mature", 900.0, 90.0, 10.0),  // 10/day
        subject_row("new",     900.0,  9.0, 12.0),  // 100/day
        subject_row("peer",    880.0, 88.0, 11.0),  // 10/day
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let mature = res.subjects.iter().find(|s| s.key == "mature").unwrap();
    assert!(mature.peers.contains(&"peer".to_string()));
    assert!(
        !mature.peers.contains(&"new".to_string()),
        "equal totals, 10x the daily rate — must not be peers"
    );
}

#[test]
fn the_baseline_excludes_the_subject_itself() {
    let layer = cohort_test_layer();
    let rows = vec![
        subject_row("a", 100.0, 1.0, 100.0),  // wild outlier target value
        subject_row("b", 100.0, 1.0, 10.0),
        subject_row("c", 100.0, 1.0, 20.0),
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
    assert_eq!(a.peer_count, 2);
    assert!((a.baseline - 15.0).abs() < 1e-9, "median of [10,20], not of [10,20,100]");
}

#[test]
fn r7_interpolates_an_even_sized_peer_set() {
    // The reference documented that upper-of-two-middle understated a real
    // gap by $933. Interpolation is not cosmetic.
    let layer = cohort_test_layer();
    let rows = vec![
        subject_row("s", 100.0, 1.0, 50.0),
        subject_row("p1", 100.0, 1.0, 10.0),
        subject_row("p2", 100.0, 1.0, 20.0),
        subject_row("p3", 100.0, 1.0, 30.0),
        subject_row("p4", 100.0, 1.0, 44.0),
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let s = res.subjects.iter().find(|s| s.key == "s").unwrap();
    assert_eq!(s.peer_count, 4);
    assert!((s.baseline - 25.0).abs() < 1e-9, "R-7 of [10,20,30,44] is 25, not 30");
}

#[test]
fn a_thin_cohort_is_reported_not_dropped() {
    // min_peers is a REPORTING predicate, never a gate. The reference moved
    // its guards out of the WHERE into a reported column precisely because a
    // store "simply did not appear in the labor list and no screen said why".
    let layer = cohort_test_layer();  // min_peers: 3
    let rows = vec![
        subject_row("lonely", 100.0, 1.0, 10.0),
        subject_row("friend", 105.0, 1.0, 12.0),
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let lonely = res.subjects.iter().find(|s| s.key == "lonely").unwrap();
    assert_eq!(lonely.peer_count, 1);
    assert!(!lonely.sufficient);
    assert!(
        !res.excluded.iter().any(|e| e.key == "lonely"),
        "a thin cohort is insufficient, not excluded — it must not appear in both"
    );
}

#[test]
fn a_null_require_value_is_excluded_and_reported() {
    let layer = cohort_test_layer();
    let mut rows = vec![subject_row("a", 100.0, 1.0, 10.0), subject_row("b", 100.0, 1.0, 12.0)];
    rows.push(subject_row_with_null_basis("orphan", 100.0, 1.0, 15.0));
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    assert!(!res.subjects.iter().any(|s| s.key == "orphan"));
    let ex = res.excluded.iter().find(|e| e.key == "orphan").expect("reported, not vanished");
    assert!(ex.reason.contains("require") || ex.reason.contains("null"));
    // And it contributes to nobody's baseline.
    let a = res.subjects.iter().find(|s| s.key == "a").unwrap();
    assert!(!a.peers.contains(&"orphan".to_string()));
}

#[test]
fn the_gap_is_polarity_aware() {
    // All five reference cohort metrics are lower-is-better. Positive gap
    // means opportunity in BOTH directions.
    let layer = cohort_test_layer_lower_is_better();
    let rows = vec![
        subject_row("spendy", 100.0, 1.0, 30.0),
        subject_row("p1", 100.0, 1.0, 10.0),
        subject_row("p2", 100.0, 1.0, 20.0),
    ];
    let res = resolve_with_rows(&layer, rows, BenchmarkStatistic::Median);
    let s = res.subjects.iter().find(|s| s.key == "spendy").unwrap();
    assert!((s.baseline - 15.0).abs() < 1e-9);
    assert!(s.gap > 0.0, "a cost 2x the peer median is an opportunity, not a credit");
    assert!((s.gap - 15.0).abs() < 1e-9);
}

#[test]
fn the_result_names_the_cohort_actually_used() {
    // The self-describing result is what closes the "screen says one thing,
    // query did another" bug class. Never omit it.
    let layer = cohort_test_layer();
    let res = resolve_with_rows(&layer, vec![subject_row("a", 100.0, 1.0, 10.0)], BenchmarkStatistic::Median);
    assert_eq!(res.cohort, "size_matched");
    assert_eq!(res.entity, "store_id");
    assert_eq!(res.statistic, BenchmarkStatistic::Median);
}
```

Add `resolve_with_rows(layer, rows, statistic)` — a helper wrapping `resolve_cohort` with a canned executor that returns `rows` for the pull and `rows.len()` for the count. Add `subject_row(key, band_measure, per, target)` and `subject_row_with_null_basis(..)`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib cohort:: 2>&1 | tail -20`
Expected: FAIL — `subjects` is empty (Task 5 returns an empty result).

- [ ] **Step 3: Implement the peer loop**

Add to `src/engine/cohort.rs`. Structure it as: parse rows into a `Vec<Subject { key, require_tuple: Option<Vec<String>>, norm: Option<f64>, value: f64 }>`, partition out the NULL-`require` rows into `excluded`, then the O(N²) loop over the remainder. Use `quantile_r7` (now `pub(crate)`) for median/p75 and the same direction logic `select_benchmark` uses for `best_peer` — read `select_benchmark` at `metric_tree_ops.rs:3712` and mirror it rather than inventing a second convention.

A subject whose `band.per` value is zero or NULL cannot be normalised: exclude and report, do not divide by zero.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib cohort:: 2>&1 | tail -10` → PASS, 12 tests.
Run: `cargo test --lib 2>&1 | tail -5` → PASS, full suite.

- [ ] **Step 5: Commit**

```bash
git add src/engine/cohort.rs
git commit -m "Resolve peer cohorts: subject-centred bands, R-7 baselines, reported refusals"
```

---

### Task 7: `airlayer cohort` CLI subcommand

**Files:**
- Modify: `src/cli/mod.rs` — `Commands` enum (after the `Opportunity` variant at :416-461), the `match` arm (~:1290), a new `run_cohort` (beside `run_opportunity` at :2683), and a `print_cohort_result`
- Test: `tests/integration_tests.rs`

**Interfaces:**
- Consumes: `resolve_cohort`, `PeerCohortResult`, `augment_layer_for_cohort` (Tasks 5-6); `Measure.default_cohort` (Task 2); `parse_statistic` (existing, `cli/mod.rs:2731`).
- Produces: the `cohort` subcommand. Terminal surface only.

`--cohort` is optional: when omitted, fall back to the target measure's `default_cohort`, and error actionably when neither is present.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn test_cohort_cli_uses_the_measures_default_cohort() {
    let out = run_cli(&[
        "cohort", "sales.wage_pct",
        "--time", "sales.sale_date",
        "--period", "2024-01-01:2024-12-31",
        "--json",
    ]);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["cohort"], "size_matched");
    assert_eq!(v["entity"], "store_id");
}

#[test]
fn test_cohort_cli_errors_when_no_cohort_is_resolvable() {
    let err = run_cli_expect_failure(&[
        "cohort", "sales.net_sales",
        "--time", "sales.sale_date",
        "--period", "2024-01-01:2024-12-31",
    ]);
    assert!(err.contains("--cohort") && err.contains("default_cohort"));
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --features cli test_cohort_cli 2>&1 | tail -20`
Expected: FAIL — unrecognised subcommand `cohort`.

- [ ] **Step 3: Add the clap variant**

```rust
    Cohort {
        /// Target measure to compare within the cohort (e.g. "sales.wage_pct").
        measure: String,

        /// Cohort to resolve, as "entity.cohort_name". Defaults to the
        /// measure's own `default_cohort:` when omitted.
        #[arg(long)]
        cohort: Option<String>,

        /// Time dimension for period filtering.
        #[arg(long = "time", required = true)]
        time_dimension: String,

        /// Analysis period as start:end.
        #[arg(long, required = true)]
        period: String,

        /// Benchmark statistic each subject is compared against: median,
        /// p75, or best_peer. Note that p75 over a 3-peer cohort is exactly
        /// what the reference implementation warns against — a dollar gap
        /// against the median of a named, listed group is defensible; a
        /// percentile over three peers is not.
        #[arg(long, default_value = "median")]
        statistic: String,

        #[arg(short, long)]
        globals: Option<PathBuf>,
        #[arg(short, long)]
        config: Option<PathBuf>,
        #[arg(short, long)]
        dialect: Option<String>,
        #[arg(long)]
        datasource: Option<String>,
        #[arg(long)]
        json: bool,
    },
```

- [ ] **Step 4: Implement `run_cohort`**

Mirror `run_opportunity` (`:2683`) exactly for config loading, dialect map, connection, and the executor closure — including the clone→`augment_layer_for_cohort`→build-engine-from-the-augmented-copy order, which is load-bearing: the executor resolves measure names against the layer the engine holds, so `__cohort_total__` must exist in both copies.

Cohort resolution order, before any of that:

```rust
    // Explicit flag wins; otherwise the measure's own declaration. The result
    // reports whichever was used, so a consumer rendering the cohort name
    // cannot drift from the query behind it.
    let cohort_ref = match cohort.or_else(|| default_cohort_of(layer, measure)) {
        Some(c) => c,
        None => {
            eprintln!(
                "Error: no cohort to resolve. Pass --cohort <entity>.<name>, or declare \
                 `default_cohort:` on measure '{}'.",
                measure
            );
            std::process::exit(1);
        }
    };
    let Some((entity, cohort_name)) = cohort_ref.split_once('.') else {
        eprintln!("Error: --cohort expects 'entity.cohort_name', got '{}'", cohort_ref);
        std::process::exit(1);
    };
```

- [ ] **Step 5: Implement `print_cohort_result`**

Follow `print_opportunity_result` (`:2795`) for style. Print, in order: the target, the cohort actually used and its entity, the statistic; then per subject `key`, `value`, `baseline`, `gap`, `peer_count`, with insufficient subjects clearly marked; then the excluded list with reasons. **Never silently omit the excluded section** — a subject that vanished with no explanation is the exact failure the reference had to fix.

- [ ] **Step 6: Run the tests**

Run: `cargo test --features cli test_cohort_cli 2>&1 | tail -10` → PASS.
Run: `cargo check --features cli 2>&1 | tail -3` → clean.

- [ ] **Step 7: Commit**

```bash
git add src/cli/mod.rs tests/integration_tests.rs
git commit -m "Add the cohort CLI subcommand"
```

---

### Task 8: DuckDB tier-1 integration test

**Files:**
- Create: `tests/integration/views-cohort/stores.view.yml`, `sales.view.yml` (extend Task 4's fixture), `tests/integration/seed/cohort_duckdb.sql`
- Modify: `tests/integration_tests.rs`

**Interfaces:**
- Consumes: the whole stack. Produces nothing.

End-to-end proof against a real database with **hand-computed** expected values — not values read back from the implementation.

- [ ] **Step 1: Write the seed and the failing test**

Seed a `stores` table (6 stores, 2 accounting bases) and a `sales` table with per-store daily rows engineered so that:
- Two stores have equal 90-day totals but 10× different trading-day counts (the normalisation case).
- One store's band contains a peer whose own band excludes it (the asymmetry case).
- One store has a NULL `accounting_basis` (the exclusion case).
- One store has exactly 1 peer against `min_peers: 3` (the insufficiency case).

```rust
#[test]
fn test_cohort_end_to_end_duckdb() {
    let conn = seed_cohort_duckdb();
    let res = resolve_cohort_via_engine(&conn, "sales.wage_pct", "store_id.size_matched");

    // Hand-computed from the seed, NOT read back from the implementation.
    let s = res.subjects.iter().find(|s| s.key == "store_a").unwrap();
    assert_eq!(s.peer_count, 3);
    assert!((s.baseline - 0.24).abs() < 1e-6, "median of [0.22, 0.24, 0.31] peers");
    assert!((s.value - 0.30).abs() < 1e-6);
    assert!((s.gap - 0.06).abs() < 1e-6, "lower_is_better: value - baseline");
    assert!(s.sufficient);

    let thin = res.subjects.iter().find(|s| s.key == "store_e").unwrap();
    assert!(!thin.sufficient);
    assert_eq!(thin.peer_count, 1);

    let orphan = res.excluded.iter().find(|e| e.key == "store_f").unwrap();
    assert!(orphan.reason.contains("require") || orphan.reason.contains("null"));

    assert_eq!(res.cohort, "size_matched");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features exec-duckdb test_cohort_end_to_end 2>&1 | tail -20`
Expected: FAIL — with a real assertion mismatch or a missing fixture, **not** a compile error. If it fails to compile, fix that first; a compile failure is not a red test.

- [ ] **Step 3: Make it pass**

Fix whatever the end-to-end path reveals. Expect the interesting failures here to be in member resolution across views (`sales.trading_days` referenced from a cohort declared on `stores`) — that path is exercised for the first time in this task.

If a hand-computed expectation disagrees with the implementation, **work out which is right before changing either.** Do not adjust the expected value to match observed output.

- [ ] **Step 4: Run the full suite**

Run: `cargo test --lib 2>&1 | tail -5`
Run: `cargo test --features exec-duckdb 2>&1 | tail -5`
Run: `cargo fmt --check`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "Prove cohort resolution end to end against DuckDB"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §4 schema (`cohorts`, `CohortBand`) | 1 |
| §4 `default_cohort` decision | 2 |
| §4.1 `per:` is a measure | 1 (doc), 6 (normalisation test) |
| §4.2 validation, all five rules | 3 |
| §5 one-query resolution | 5 |
| §5.1 truncation trap | 5 |
| §5.2 R-7 in Rust | 6 (reuses `quantile_r7`) |
| §6 refusal channels, cardinality guard | 5 (guard), 6 (channels) |
| §7 gap pricing per subject | 6 |
| §8 CLI + inspect | 4, 7 |
| §8 pre-agg fingerprint immunity | 2 |
| §10 every listed test | 1, 5, 6, 8 |

**Deliberately not covered** (spec §9, §12): the tier ladder, all threshold constants, period-completeness guards, the basis literal, bridge tables, composition with `Shift.comparable_by`, and the entire oxy surface. Each is a stated boundary, not a gap.

**Known deviation:** `resolve_cohort` takes `measure` and `time_dimension`, which the spec's §5 signature omits. Both are required for the pull. Recorded in Task 5.
