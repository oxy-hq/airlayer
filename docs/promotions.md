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

## Limitations and known follow-ups

- **`through:` for ambiguous induced names.** Today's planner errors when the same name is reachable from multiple sources. Wiring the existing join-graph `through:` hint into measure resolution is a clean follow-up.
- **Non-additive measures in chasm contexts.** When two source views promote to a shared target *and* the measures are non-additive (`avg`, `count_distinct`, etc.), the per-CTE pre-aggregation breaks correctness. The current generator detects this and errors with a clear message rather than producing a wrong number. Routing them via a single-stage per-side join+aggregate to target grain is the right fix.
- **Mixed-grain queries with non-additive measures.** Similar story: if an explicit non-additive measure on the parent appears alongside an induced one, the planner refuses rather than guessing.

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
| `parent:` validation (Foreign rejection, dead-end, cycle, well-formed) | `schema::validator::tests` |
| Closure correctness (single-hop, transitive, ambiguity, collision, over-declared) | `engine::promotions::tests` |
| RCA hierarchy pruning | `engine::metric_tree_ops::hierarchy_prune_tests` |
| `inspect --json` surfacing | `cli::tests::inspect_json_surfaces_induced_measures_and_hierarchy` |
