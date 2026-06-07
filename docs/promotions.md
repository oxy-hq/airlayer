# Promotions (induced measures)

A *promotion* is a functional (many-to-one) relationship between two views: every row of the source view belongs to exactly one row of the target view. Its fibers — the sets of source rows mapping to a single target row — are the collections aggregated when projecting a measure upward.

Concretely: define `sales.net_sales` once at the finest grain, and `stores.net_sales`, `companies.net_sales`, `markets.net_sales` become queryable automatically. You never write the coarser-grain measures by hand.

## The relationship lives on the entity, not the view

The hierarchy that drives promotion is a property of the **entity**, declared on its `Primary` declaration with `parent: <other_entity>`:

```yaml
# stores.view.yml — store_id rolls up to company_id
entities:
  - name: store_id
    type: primary
    key: store_id
    parent: company_id          # ← the relationship is intrinsic to store_id

  - name: company_id            # to join to companies
    type: foreign
    key: company_id
```

```yaml
# companies.view.yml — company_id rolls up to market_id
entities:
  - name: company_id
    type: primary
    key: company_id
    parent: market_id
```

```yaml
# markets.view.yml — top of the chain
entities:
  - name: market_id
    type: primary
    key: market_id
```

```yaml
# sales.view.yml — fact view that uses store_id. Unchanged.
entities:
  - { name: store_id, type: foreign, key: store_id }
measures:
  - { name: net_sales, type: sum, expr: amount }
```

`sales.net_sales` is now induced as `stores.net_sales`, `companies.net_sales`, and `markets.net_sales`. The chain walks `store_id → company_id → market_id` automatically.

### Why the entity, not the view

The relationship "store rolls up to company" is true regardless of which view (sales, returns, inventory) happens to record facts about a store. Declaring it on the entity means every view that uses `store_id` participates in the rollup for free — no duplication, no per-fact maintenance.

It also resolves a directional ambiguity that view-level declarations can't: if both `orders` and `customers` over-declare `Foreign` references to each other's keys, *only* the entity that names a `parent:` participates in the hierarchy. Convention is replaced by a single explicit statement.

## Additivity is read from the measure type

The compiler classifies each measure into one of three additivity classes (`MeasureType::additivity_class()`):

| Class | Measure types | What it means for promotion |
|-------|---------------|------------------------------|
| **Additive** | `sum`, `count`, `min`, `max` | Re-foldable. An intermediate aggregate at any sub-grain can be re-aggregated to the target grain (`SUM(SUM(x))=SUM(x)`, `MIN(MIN(x))=MIN(x)`). |
| **Non-additive** | `average`, `count_distinct`, `count_distinct_approx`, `median` | Must be computed by aggregating source-grain rows directly at the requested target grain. Re-folding an intermediate would silently average averages. |
| **Passthrough** | `number`, `custom` | Expression-typed. The `{{view.measure}}` references in the expression are resolved to aggregated SQL fragments at the target grain. The wrapping expression (typically a ratio) is computed over those aggregates. |

No `additive:` field is required (or allowed). The classification is derived from the existing `type:` declaration.

## How induced queries compile

At engine construction, `Promotions::build` walks the entity hierarchy and produces the closure: for every source view's measure, the set of target views where it's induced, plus the entity path taken.

At query time, `SemanticEngine::compile_query` runs a pre-pass:
- If `target.M` is induced from `source.M`, rewrite the measure to `source.M`.
- Record the original name so the result column metadata still reads as `target.M`.
- Pick the source view as the join base (`pick_base_view` tiebreaks "measure-owning view wins on ties").
- Run the existing SQL generator. With the source view as base, the leaves naturally aggregate at the requested target grain.

For passthrough expressions like `tx.amount_per_tx = SUM(tx.amount) / NULLIF(COUNT(*), 0)`, the SQL generator's `{{view.measure}}` resolution does the rest — the leaves get aggregated at target grain by the natural `GROUP BY`, and the ratio is computed over those aggregates (`SUM(x)/SUM(y)`, not `AVG(per-fiber ratio)`).

## Two source views at a shared target (no chasm trap)

When multiple source views promote to a shared target, the planner pre-aggregates each in its own CTE and joins on the target's primary key. Concretely, for sales and returns both attached to `stores`:

```sql
WITH __dim_spine AS (
  SELECT DISTINCT region, store_id FROM returns LEFT JOIN stores LEFT JOIN sales ...
),
__measures_returns AS (
  SELECT store_id, SUM(amount) AS returns__refund_amount FROM returns GROUP BY 1
),
__measures_sales AS (
  SELECT store_id, SUM(amount) AS sales__total_amount FROM sales GROUP BY 1
)
SELECT
  __dim_spine.region,
  SUM(__measures_returns.returns__refund_amount) AS returns__refund_amount,
  SUM(__measures_sales.sales__total_amount)      AS sales__total_amount
FROM __dim_spine
LEFT JOIN __measures_returns ON __dim_spine.store_id = __measures_returns.store_id
LEFT JOIN __measures_sales   ON __dim_spine.store_id = __measures_sales.store_id
GROUP BY 1
```

No row from `sales` is ever paired with a row from `returns`; each side is aggregated independently. The "chasm trap" of dimensional modeling — where a naive join multiplies both sides by the other's row count per fiber — is structurally avoided.

The detection that triggers this routing is in `detect_multiplied_views` (`src/engine/sql_generator.rs`): when two or more "many" siblings hang off the same hub, all of them get their own CTE.

## Hierarchy-aware root-cause analysis

The RCA `explain` command (and its `--deep` beam search) uses the hierarchy to prune the dimension search space. After picking a dimension that maps to an entity `E`, the next level of investigation only considers dimensions on `E` itself or on its descendants in the `parent:` chain.

For a region → store → shelf hierarchy, after the algorithm isolates "California" (a region instance), the next split is restricted to dims of `store_id` or below — never re-cutting by unrelated axes like `customer_country`. The per-level fanout drops from `O(N_all_dims)` to `O(|subtree(E_picked)|)`.

A dim "maps to" an entity when its local name matches the entity's `key:` (e.g. `stores.store_id` maps to entity `store_id` because the entity's key is `store_id`). Attribute dims (no key match) and dims whose grain isn't derivable are kept conservatively — no false pruning.

The pruner is `prune_dims_after_pick` in `src/engine/metric_tree_ops.rs`, used by both the greedy `recurse` path and the beam-search path.

## Discovery: `inspect --json`

Induced measures appear on the target view's `measures` array with `induced: true`, the additivity class, and full provenance:

```json
{
  "name": "stores.net_sales",
  "induced": true,
  "additivity": "additive",
  "promoted_from": {
    "source_view": "sales",
    "source_measure": "sales.net_sales",
    "path": ["store_id"]
  }
}
```

Each view also gets a `hierarchy` block listing its Primary entities with their `parent:` and `children`. Layer-wide `promotion_collisions` and `promotion_ambiguities` arrays surface (only when non-empty) the cases where:
- An explicit measure on the target view shadows an induced name (explicit wins; the dropped induction is reported).
- The same induced name is reachable from multiple distinct source views (the planner needs `through:` to disambiguate — until that's wired into measure resolution, ambiguous queries error with the candidate list).

## Validation

The validator enforces three rules at schema load time:

1. **`parent:` only on Primary entities.** Foreign declarations are usages of the entity, not its definition; carrying `parent:` there would let it silently disagree across views.
2. **`parent: X` must resolve.** Some view must declare `X` as a Primary entity, or the chain dead-ends and no rollup is possible.
3. **No cycles.** A DFS catches `a parent: b, b parent: a` (and self-loops) at build time.

It also emits warnings (non-fatal) for name collisions and cross-source ambiguities. They're informational — useful when refactoring a model, but not gates.

## Disambiguation: `through:` for ambiguous induced names

When the same induced measure name is reachable from multiple source views (e.g. `sellers.total` from both `gmv` and `takerate` in a marketplace), the planner accepts `request.through` as the disambiguator. A candidate matches when its source view appears in `through`, or when any entity on its hierarchy path does:

```json
{
  "measures": ["sellers.total"],
  "dimensions": ["sellers.tier"],
  "through": ["gmv"]
}
```

Without a hint, the planner errors with the candidate list and suggests both routes — qualify the measure (`gmv.total`) or set `through:`. The validator emits a stderr warning at engine construction for every cross-source ambiguity, so collisions are discoverable before they're queried.

## Non-additive routing across source views (user-grain CTEs)

When at least one multiplied source view has a non-additive measure (`avg`, `count_distinct`, `median`, `count_distinct_approx`) or a passthrough measure (`number`, `custom`), the planner switches from the join-key CTE shape to **user-grain CTEs**. Each source view's CTE joins through the entity chain *inside the CTE* and aggregates directly at the user-dim grain:

```sql
WITH
__measures_gmv AS (
  SELECT sellers.tier, AVG(gmv.amount) AS gmv__avg_amount
  FROM gmv
  LEFT JOIN sellers AS sellers ON gmv.seller_id = sellers.seller_id
  GROUP BY 1
),
__measures_takerate AS (
  SELECT sellers.tier, AVG(takerate.fee) AS takerate__avg_fee
  FROM takerate
  LEFT JOIN sellers AS sellers ON takerate.seller_id = sellers.seller_id
  GROUP BY 1
),
__dim_spine AS (SELECT DISTINCT sellers.tier AS sellers__tier FROM sellers)
SELECT __dim_spine.sellers__tier,
       __measures_gmv.gmv__avg_amount,
       __measures_takerate.takerate__avg_fee
FROM __dim_spine
LEFT JOIN __measures_gmv      ON __dim_spine.sellers__tier = __measures_gmv.sellers__tier
LEFT JOIN __measures_takerate ON __dim_spine.sellers__tier = __measures_takerate.sellers__tier
```

`AVG` is applied to the source rows directly within each tier (the correct semantics — not `AVG` of per-seller `AVG`s). The outer SELECT has no `GROUP BY` because each CTE has already aggregated to the target grain. For ratio passthrough measures, the same shape gives `SUM(x) / SUM(y)` at the user-dim grain, which is the correct semantics for a ratio at a coarser grain.

The join-key CTE shape (the all-additive path) is still used when every multiplied-view measure is additive — it's more efficient (smaller intermediate aggregations) and gives the same answer for `SUM/COUNT/MIN/MAX`.

## World model ontology lift

The same schema, viewed entity-first instead of view-first, is the procedural ontology a world model consumer renders. `inspect --json` includes an `ontology` block that emits the formalism's primitives directly — no separate flag, no consumer-specific tailoring.

The formalism is three primitives:

- **Entity** `E_g` — first-class object at grain `g`.
- **Attribute** `a : E_g → V` — observed (a property of the entity, no finer-grain derivation) or calculated (a pushforward `Σ_p` of another attribute).
- **Promotion** `p : E_g → E_h` — a functional, many-to-one map. Its fibers `p⁻¹(e)` are the collections aggregated by `Σ_p`.

Two adjoint operations along a promotion: pushforward `Σ_p` (the "measure" operation, sends attributes up the grain lattice) and pullback `Δ_p` (broadcast / inherited dimension, sends attributes down). The taxonomy {fully, semi, non}-additive is a *theorem* about which operators compose along chains: `(V, ⊕, 0)` must form a commutative monoid AND `Σ_p` must factor as a monoid homomorphism. SUM / COUNT / MIN / MAX qualify; AVG / MEDIAN / COUNT_DISTINCT don't.

airlayer's YAML expresses all of this:

```json
"ontology": {
  "entities": [
    { "grain": "company_id", "depth": 0, "primary_view": "companies",
      "cardinality": null },
    { "grain": "store_id",   "depth": 1, "primary_view": "stores",
      "parent": "company_id", "cardinality": null }
  ],
  "promotions": [
    { "id": "p_store_id_company_id",
      "from": "store_id", "to": "company_id",
      "functional": true, "kind": "containment" },
    { "id": "p_sale_id_store_id",
      "from": "sale_id", "to": "store_id",
      "functional": true, "kind": "categorical" }
  ],
  "observed_attributes": [
    { "id": "stores.region", "name": "region", "grain": "store_id",
      "type": "string", "primary_key": false }
  ],
  "calculated_attributes": [
    { "id": "sales.net_sales", "grain": "sale_id",
      "operator": "SUM", "taxonomy": "fully-additive",
      "chain": [], "induced": false },
    { "id": "companies.net_sales", "grain": "company_id",
      "operator": "SUM", "taxonomy": "fully-additive",
      "chain": ["p_sale_id_store_id", "p_store_id_company_id"],
      "source_attribute": "sales.net_sales", "induced": true }
  ]
}
```

| Formalism primitive | YAML construct | Surfaced in `ontology` block |
|---|---|---|
| Entity at grain `E_g` | `Primary` entity declaration | `entities[]` with `grain`, `primary_view`, `depth` (from ancestry chain) |
| Promotion `p: E_g → E_h` (functional, many-to-one) | `parent:` on Primary → `containment`; Foreign-only reference → `categorical` | `promotions[]` with `id: p_{from}_{to}`, `functional: true`, `kind` |
| Observed attribute `a : E_g → V` | Dimension on a view (classified to the view's grain) | `observed_attributes[]` with `id`, `grain`, `type`, `primary_key` |
| Calculated attribute `(p, a, ⊕)` | Measure — explicit (declared) or induced (computed via closure) | `calculated_attributes[]` with `operator`, `taxonomy`, `chain` |
| Pushforward `Σ_p` (measure up the lattice) | Induced measure at parent grain | `induced: true`, `source_attribute`, `chain` |
| Pullback `Δ_p` (broadcast down) | Joined dimension projection at child grain | (implicit in the join graph) |
| Monoid taxonomy `{fully, semi, non}-additive` | Derived from `MeasureType::additivity_class()` | `taxonomy` field; labels match the formalism verbatim |
| Composed promotion `p_a ∘ p_b` | Transitive `parent:` chain | Multi-element `chain: ["p_...", "p_..."]` |
| Fan-out (non-functional, broken) | Validator rejects non-Primary parents and missing-Primary references | (declined at load time — never reaches the lift) |
| AVG via `(sum, count)` homomorphism | Non-additive routing — user-grain CTE | (runtime behaviour; visible in the generated SQL) |

The two sides agree on the same theorem: a measure composes along a chain iff its operator is a monoid homomorphism on its carrier. The formalism states it; airlayer enforces it at routing time (additive → join-key CTE; non-additive → user-grain CTE; non-functional fan-out → rejected). The ontology block is the bridge — whatever rendering consumes it, the primitives are the formalism's, not any particular consumer's.

If a renderer wants additional descriptive metadata (`EntityKind` like domain/event/master, or `cardinality` from real data) — those are not part of the formalism, but airlayer can lift them through the existing `meta:` field on entities and views, or compute cardinality at query time via a `COUNT(DISTINCT)` over the entity's primary key.

## Limitations and known follow-ups

- **`time_dimensions` in non-additive fan-out.** The user-grain CTE path doesn't yet thread time granularity through. Single-source non-additive with time dims works; chasm + time dims errors with a clear message.
- **Composite-key entities in promotions.** The closure walks single-key entities only today.

## Where the code lives

| Concept | File |
|---------|------|
| `parent:` field, `AdditivityClass`, `MeasureType::additivity_class()` | `src/schema/models.rs` |
| Closure builder, hierarchy navigation (`parent_of`, `children_of`, `ancestry`, `descendants`) | `src/engine/promotions.rs` |
| Validation rules + collision/ambiguity warnings | `src/schema/validator.rs` |
| Query-time rewrite + column-metadata restoration | `src/engine/mod.rs` (`rewrite_induced_measures`) |
| `pick_base_view` tiebreaker (measure-owning view wins) | `src/engine/sql_generator.rs` |
| Fan-out CTE shape (chasm-safe spine + outer aggregation) | `src/engine/sql_generator.rs` (`generate_with_fanout_protection`, `detect_multiplied_views`) |
| Hierarchy-aware RCA pruner | `src/engine/metric_tree_ops.rs` (`prune_dims_after_pick`) |
| `inspect --json` surface | `src/cli/mod.rs` (`inspect_json`) |

## Tests

| Scenario | Test |
|----------|------|
| Single-hop additive | `duckdb_induced_additive_single_hop` |
| Two-hop transitive additive | `duckdb_induced_additive_two_hop` |
| Non-additive AVG (aggregated directly at target grain) | `duckdb_induced_non_additive_avg_is_direct` |
| Passthrough ratio (aggregate-quotient, not avg-of-ratios) | `duckdb_induced_passthrough_ratio_is_aggregate_quotient` |
| Chasm trap (two source views at shared target) | `duckdb_induced_no_fanout_across_two_source_views` |
| Mixed explicit + induced | `duckdb_induced_mixed_explicit_and_induced` |
| Ambiguity → through: source view | `duckdb_induced_through_picks_source_view`, `duckdb_induced_through_picks_other_source` |
| Ambiguity errors clearly without hint | `induced_ambiguous_errors_with_candidates`, `induced_through_no_match_errors` |
| Non-additive routing across two source views | `duckdb_induced_non_additive_two_sources_at_shared_grain` |
| `parent:` validation (Foreign rejection, dead-end, cycle, well-formed) | `schema::validator::tests` |
| Closure correctness (single-hop, transitive, ambiguity, collision, over-declared) | `engine::promotions::tests` |
| RCA hierarchy pruning | `engine::metric_tree_ops::hierarchy_prune_tests` |
| `inspect --json` surfacing | `cli::tests::inspect_json_surfaces_induced_measures_and_hierarchy` |
