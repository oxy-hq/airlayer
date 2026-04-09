# Schema Format

airlayer uses `.view.yml` files to define the semantic layer. This is the same format used by [Oxy](https://github.com/oxy-hq/oxy).

## View files (`.view.yml`)

```yaml
name: orders                    # required — unique view name
description: "Order data"       # required
table: public.orders            # table reference (or use sql:)
sql: "SELECT * FROM ..."        # SQL subquery (alternative to table:)
dialect: postgres               # SQL dialect (standalone projects)
datasource: warehouse           # maps to dialect via config.yml (Oxy projects)

entities:                       # entity declarations for auto-joins
  - name: customer
    type: primary               # or foreign (default: primary)
    key: customer_id            # single key
    keys: [col_a, col_b]        # composite key (alternative to key:)

dimensions:
  - name: status
    type: string                # string, number, date, datetime, boolean, geo
    expr: status                # SQL expression
    description: "Order status"
    primary_key: true           # marks as primary key dimension
    samples: ["active", "cancelled"]
    sub_query: true             # generates correlated subquery (for cross-view measures)

measures:
  - name: total_revenue
    type: sum                   # count, sum, avg, min, max, count_distinct, count_distinct_approx, median, number, custom
    expr: amount                # SQL expression (omit for count)
    description: "Total order value"
    filters:                    # measure-level filter (CASE WHEN)
      - expr: "status = 'completed'"
        description: "Completed only"
    rolling_window:             # window function frame
      trailing: "unbounded"     # or "7", "30", etc.
      leading: "current row"   # or "unbounded", "1", etc.

segments:
  - name: active_only
    expr: "status = 'active'"
    description: "Only active orders"
```

## Dialect resolution

Each view can declare its SQL dialect directly:

```yaml
name: orders
table: public.orders
dialect: bigquery
```

For Oxy projects or multi-datasource setups, use `datasource` + `config.yml`:

```yaml
# orders.view.yml
name: orders
table: public.orders
datasource: warehouse

# config.yml
databases:
  - name: warehouse
    type: bigquery
  - name: operational
    type: postgres
```

Resolution priority (highest wins):

| Priority | Method | Use case |
|----------|--------|----------|
| 1 | `-d` CLI flag | One-off override |
| 2 | `-c config.yml` + `datasource` | Oxy projects, multi-datasource |
| 3 | View-level `dialect` field | Standalone projects (default) |
| 4 | Postgres fallback | When nothing is specified |

## Dimension types

| Type | Description |
|------|-------------|
| `string` | Text/categorical values |
| `number` | Numeric values |
| `date` | Date values |
| `datetime` | Timestamp values |
| `boolean` | True/false values |
| `geo` | Geographic/spatial values (treated as string for SQL) |

## Measure types

| Type | SQL output |
|------|-----------|
| `count` | `COUNT(*)` or `COUNT(expr)` |
| `sum` | `SUM(expr)` |
| `avg` / `average` | `AVG(expr)` |
| `min` | `MIN(expr)` |
| `max` | `MAX(expr)` |
| `count_distinct` | `COUNT(DISTINCT expr)` |
| `count_distinct_approx` | Dialect-specific approximate count distinct |
| `median` | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expr)` |
| `number` | Pass-through — expression used as-is (must contain its own aggregation) |
| `custom` | Raw expression used as-is |

### Measure-to-measure references

Measures can reference other measures using `{{view.measure_name}}`:

```yaml
measures:
  - name: total_revenue
    type: sum
    expr: revenue
  - name: total_cost
    type: sum
    expr: cost
  - name: profit
    type: number
    expr: "{{financials.total_revenue}} - {{financials.total_cost}}"
```

The `{{view.measure}}` patterns are resolved to the referenced measure's aggregate expression at compile time.

### Rolling windows

Measures can use window functions for cumulative or rolling aggregations:

```yaml
measures:
  - name: cumulative_revenue
    type: sum
    expr: revenue
    rolling_window:
      trailing: "unbounded"    # UNBOUNDED PRECEDING
      leading: "current row"   # CURRENT ROW (default)

  - name: rolling_7day_revenue
    type: sum
    expr: revenue
    rolling_window:
      trailing: "7"            # 7 PRECEDING
      leading: "current row"
```

### Subquery dimensions

Dimensions with `sub_query: true` generate correlated subqueries referencing measures from related views:

```yaml
# customers.view.yml
dimensions:
  - name: order_count
    type: number
    expr: "orders.count"      # references orders view's count measure
    sub_query: true
```

Generates:

```sql
(SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id)
```

## Entities and auto-joins

Views declare entities to enable automatic JOIN generation:

```yaml
# customers.view.yml
entities:
  - name: customer
    type: primary
    key: id

# orders.view.yml
entities:
  - name: customer
    type: foreign
    key: customer_id
```

When a query references members from both views, airlayer matches the foreign `customer` entity to the primary `customer` entity and generates:

```sql
FROM public.orders AS "orders"
LEFT JOIN public.customers AS "customers"
  ON "orders".customer_id = "customers".id
```

### Join relationships

| Relationship | Join type | Description |
|-------------|-----------|-------------|
| OneToOne | INNER JOIN | 1:1 relationship, no row multiplication |
| ManyToOne | LEFT JOIN | Many rows in child map to one parent |
| OneToMany | LEFT JOIN | One parent has many children (triggers fan-out protection) |

### Multi-hop joins

Transitive joins (A -> B -> C) are resolved via BFS on the entity graph. airlayer finds the shortest path and generates all intermediate JOINs.

### Join hints

When multiple join paths exist (e.g., A -> B -> D and A -> C -> D), use `--through` to disambiguate:

```bash
airlayer query --through warehouse_order \
  --dimension orders.order_id \
  --measure shipments.shipment_count
```

### Composite keys

```yaml
entities:
  - name: order_item
    type: primary
    keys: [order_id, line_number]
```

## Cross-entity references

Expressions can reference fields from related entities:

```yaml
dimensions:
  - name: customer_name
    type: string
    expr: "{{customer.name}}"
```

The `{{customer.name}}` is resolved by finding the view with primary entity `customer` and its `name` dimension, then qualifying it with the appropriate table alias.

## Segments

Segments are predefined reusable filter conditions:

```yaml
segments:
  - name: active_only
    expr: "status = 'active'"
```

Applied via the query's `segments` field:

```json
{
  "segments": ["orders.active_only"]
}
```

Segments are added to the WHERE clause.

## Globals inheritance

Shared definitions can be declared in a globals file and inherited by views:

```yaml
# globals.yml
dimensions:
  - created_at:
    name: created_at
    type: time
    expr: created_at

entities:
  - customer:
    name: customer
    type: primary
    key: id
```

Views inherit with:

```yaml
dimensions:
  - name: created_at
    inherits_from: globals.semantics.dimensions.created_at
```

## Variables

`{{variables.X}}` references are preserved as-is in the output SQL for runtime substitution by the host application.

## Self-referencing expressions

`{{TABLE}}` in expressions resolves to the view's table alias:

```yaml
dimensions:
  - name: full_name
    type: string
    expr: "CONCAT({{TABLE}}.first_name, ' ', {{TABLE}}.last_name)"
```

## Motif files (`.motif.yml`)

Custom motifs define reusable post-aggregation analytical patterns. Place them in a `motifs/` directory.

```yaml
name: margin_analysis
description: "Compute gross margin percentage"
params:
  measure:
    type: measure
    constraints: [numeric]
    description: "Measure to analyze"
outputs:
  - name: total
    expr: "SUM({{ measure }}) OVER ()"
  - name: margin_pct
    expr: "{{ measure }} * 100.0 / NULLIF(SUM({{ measure }}) OVER (), 0)"
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique motif name |
| `description` | string | No | Human-readable description |
| `params` | map | No | Parameter declarations (auto-bound from query columns) |
| `outputs` | list | Yes | Output columns added to the query (each has `name` and `expr`) |

### Parameter types

| Type | Description |
|------|-------------|
| `measure` | Auto-bound to first measure column |
| `dimension` | Auto-bound to first matching dimension |
| `number` | Numeric parameter with optional default |

### `{{ param }}` substitution

Expressions in `outputs[].expr` use `{{ param_name }}` references. Standard auto-bindings:

- `{{ measure }}` → first Measure column (aliased as `b.<alias>`)
- `{{ time }}` → first TimeDimension column
- `{{ dimensions }}` → comma-separated Dimension columns
- Custom params are passed via `motif_params` in the query

Custom motifs are always single-stage (no intermediate CTEs). For multi-stage patterns, see the builtin motifs (anomaly, trend) in `src/engine/motifs.rs`.

### Why motif expressions use window functions (`OVER`)

Motifs wrap the base query as a CTE — by the time motif expressions run, the data is already aggregated (one row per group). Consider a base query that groups revenue by region:

```
region  | total_revenue
--------|-------------
North   | 131,500
South   | 87,200
```

`MIN(b.total_revenue) OVER ()` computes the global min but **keeps both rows**:

```
region  | total_revenue | min_value
--------|---------------|----------
North   | 131,500       | 87,200
South   | 87,200        | 87,200
```

Plain `MIN(b.total_revenue)` (without `OVER`) would require a `GROUP BY`, collapsing everything into a single row — losing the per-region breakdown. Motifs need to add analytical columns alongside the existing rows, which is exactly what window functions do.

This is why the `normalized` motif uses `MIN({{ measure }}) OVER ()` and `MAX({{ measure }}) OVER ()` — it can then compute `(value - min) / (max - min)` per row without losing any rows.

### Builtin motifs

airlayer ships with 12 builtin motifs that don't need `.motif.yml` files:

| Motif | Output columns | Requires time dim | Description |
|-------|---------------|-------------------|-------------|
| `yoy` | `previous_value`, `growth_rate` | Yes | Year-over-year comparison |
| `qoq` | `previous_value`, `growth_rate` | Yes | Quarter-over-quarter |
| `mom` | `previous_value`, `growth_rate` | Yes | Month-over-month |
| `wow` | `previous_value`, `growth_rate` | Yes | Week-over-week |
| `dod` | `previous_value`, `growth_rate` | Yes | Day-over-day |
| `anomaly` | `mean_value`, `stddev_value`, `z_score`, `is_anomaly` | No | Z-score anomaly detection |
| `contribution` | `total`, `share` | No | Share of total |
| `trend` | `row_n`, `slope`, `intercept`, `trend_value` | Yes | Linear regression |
| `moving_average` | `moving_avg` | Yes | Rolling average (default 7-period) |
| `rank` | `rank` | No | RANK() by measure DESC |
| `percent_of_total` | `percent_of_total` | No | 100 * measure / total |
| `cumulative` | `cumulative_value` | Yes | Running sum |

## Saved query files (`.query.yml`)

Saved queries define multi-step analytical workflows. Place them in a `queries/` directory.

```yaml
name: revenue_investigation
description: "Investigate revenue trends and anomalies"
params:
  time_range:
    type: date_range
    default: ["2024-01-01", "2024-12-31"]
    description: "Period to analyze"
  metric:
    type: string
    values: ["total_revenue", "order_count"]
    default: "total_revenue"

steps:
  - name: overall_trend
    description: "Get the overall trend"
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: trend

  - name: anomaly_check
    description: "Find anomalous months"
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: anomaly
```

### Saved query fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Display name for the saved query |
| `description` | string | No | Human-readable description |
| `params` | map | No | Saved query-level parameters |
| `steps` | list | Yes | Ordered list of steps (at least one) |

### Step fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique step name (within the saved query) |
| `query` | object | Yes | Structured `QueryRequest` (same as `-q` JSON) |
| `description` | string | No | What this step does |

### Param fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Parameter type (string, number, date_range, etc.) |
| `values` | list | No | Allowed values (enum constraint) |
| `default` | any | No | Default value |
| `description` | string | No | Human-readable description |

### Validation rules

- Each saved query must have at least one step (or inline query fields)
- Step names must be unique within a saved query

## Topic files (`.topic.yml`)

Topics group views and provide descriptions for semantic search:

```yaml
name: sales
description: "Sales and revenue analysis"
views:
  - orders
  - customers
  - products
```
