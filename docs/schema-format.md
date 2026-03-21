# Schema Format

o3 uses `.view.yml` files to define the semantic layer. This is the same format used by [Oxy](https://github.com/oxy-hq/oxy).

## View files (`.view.yml`)

```yaml
name: orders                    # required — unique view name
description: "Order data"       # required
table: public.orders            # table reference (or use sql:)
sql: "SELECT * FROM ..."        # SQL subquery (alternative to table:)
datasource: warehouse           # maps to dialect via config.yml

entities:                       # entity declarations for auto-joins
  - name: customer
    type: primary               # or foreign (default: primary)
    key: customer_id            # single key
    keys: [col_a, col_b]        # composite key (alternative to key:)

dimensions:
  - name: status
    type: string                # string, number, time, date, boolean
    expr: status                # SQL expression
    description: "Order status"
    primary_key: true           # marks as primary key dimension
    samples: ["active", "cancelled"]

measures:
  - name: total_revenue
    type: sum                   # count, sum, avg, min, max, count_distinct, median, custom
    expr: amount                # SQL expression (omit for count)
    description: "Total order value"
    filters:                    # measure-level filter (CASE WHEN)
      - member: orders.status
        operator: equals
        values: ["completed"]

segments:
  - name: active_only
    expr: "status = 'active'"
    description: "Only active orders"
```

## Dimension types

| Type | Description |
|------|-------------|
| `string` | Text/categorical values |
| `number` | Numeric values |
| `time` | Timestamp with optional granularity support |
| `date` | Date values |
| `boolean` | True/false values |

## Measure types

| Type | SQL output |
|------|-----------|
| `count` | `COUNT(*)` or `COUNT(expr)` |
| `sum` | `SUM(expr)` |
| `avg` / `average` | `AVG(expr)` |
| `min` | `MIN(expr)` |
| `max` | `MAX(expr)` |
| `count_distinct` | `COUNT(DISTINCT expr)` |
| `median` | Dialect-specific median |
| `custom` | Raw expression used as-is |

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

When a query references members from both views, o3 matches the foreign `customer` entity to the primary `customer` entity and generates:

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

Transitive joins (A -> B -> C) are resolved via BFS on the entity graph. o3 finds the shortest path and generates all intermediate JOINs.

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

`{TABLE}` in expressions resolves to the view's table alias:

```yaml
dimensions:
  - name: full_name
    type: string
    expr: "CONCAT({TABLE}.first_name, ' ', {TABLE}.last_name)"
```

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
