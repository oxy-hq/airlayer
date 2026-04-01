---
name: migrate-from-cube
description: Migrate a Cube.js semantic layer to airlayer .view.yml files. Use when the user has existing Cube.js schema files (.js or .yml) they want to convert to airlayer format.
---

# Migrate from Cube.js

You are converting Cube.js schema files into airlayer `.view.yml` files. Each Cube.js cube becomes one `.view.yml` file.

## Step 1: Identify the schema files

Cube.js schemas can be in two formats. Read all the cube files and identify which format they use:

**JavaScript format** (`.js` files in `schema/` or `model/cubes/`):

```js
cube(`orders`, {
  sql_table: `public.orders`,
  dimensions: { ... },
  measures: { ... },
  joins: { ... },
})
```

**YAML format** (`.yml` files in `model/cubes/`):

```yaml
cubes:
  - name: orders
    sql_table: public.orders
    dimensions: ...
    measures: ...
    joins: ...
```

> **YAML vs JS naming:** Cube.js YAML uses camelCase measure types (`countDistinct`, `countDistinctApprox`, `avg`) while JS uses snake_case (`count_distinct`, `avg`). Both mean the same thing — map them identically.

Read ALL cube files before starting to write any `.view.yml` — you need the full picture to correctly translate joins into entity pairs.

## Step 2: Plan entity translations

Joins are the most important thing to get right. airlayer uses **entities** on both sides of a join — the entity `name` must match on both views for airlayer to auto-generate the JOIN.

For each join in the Cube.js schema, parse the join SQL to find the FK and PK columns:

**Cube.js join (declared on the FK side / "owning" cube):**

```js
joins: {
  customers: {
    relationship: `many_to_one`,
    sql: `${CUBE}.customer_id = ${customers}.id`,
  },
},
```

**Translation:**

- The LHS column (`customer_id`) → `foreign` entity on `orders`
- The RHS column (`id`) → `primary` entity on `customers`
- Pick a shared entity name based on the concept: use the joined cube name (`customer`)

```yaml
# orders.view.yml
entities:
  - name: customer
    type: foreign
    key: customer_id

# customers.view.yml
entities:
  - name: customer
    type: primary
    key: id
```

**Entity naming rules:**

- Use the joined cube name as the entity name (e.g., join to `customers` → entity name `customer`)
- For `one_to_many` joins (the owning side has the PK): entity is `primary` on the owning cube, `foreign` on the target
- For `many_to_one` joins (the owning side has the FK): entity is `foreign` on the owning cube, `primary` on the target
- Entity names MUST match exactly across views — `customer` ≠ `Customer`
- A cube can have multiple entity declarations (one per relationship)

**Critical: `key`/`keys` must reference dimension names, not column names.**

airlayer validates that entity keys match the `name:` of a dimension in the same view — NOT the raw column name in `expr:`. If your dimension is named `store_id` with `expr: Store`, the entity key must be `store_id`, not `Store`.

```yaml
# WRONG — 'Store' is a column name, not a dimension name
entities:
  - name: store
    type: primary
    keys: ["Store", "Date"]       # ✗ validator error

# CORRECT — use the dimension names defined in the same view
entities:
  - name: store
    type: primary
    keys: ["store_id", "week_date"]   # ✓ matches dimension name fields

# For single-column keys, use key: (not keys:)
entities:
  - name: store
    type: primary
    key: store_id                 # ✓
```

Build a table mapping `(cube_name, entity_name) → (type, key)` for all joins before writing any files.

## Step 3: Translate each cube

For each cube, create a `.view.yml` file in the `views/` directory.

### Table name

```js
sql_table: `public.orders`   →   table: public.orders
sql: `SELECT * FROM orders WHERE active = true`  →  table: "(SELECT * FROM orders WHERE active = true)"
```

For cubes using `sql:` (a subquery), wrap the value in quotes as the `table:` value.

### Dimensions

**Type mapping:**

| Cube.js type | airlayer type | Notes                                                                          |
| ------------ | ------------- | ------------------------------------------------------------------------------ |
| `string`     | `string`      |                                                                                |
| `number`     | `number`      |                                                                                |
| `boolean`    | `boolean`     |                                                                                |
| `time`       | `date`        | Use `datetime` only if the column includes a time component (e.g., timestamps) |
| `geo`        | `string`      | Note the approximation in a comment                                            |

**Expression rewriting:**

- `sql: \`column_name\``→`expr: column_name`
- `sql: \`${CUBE}.column_name\`` → `expr: column_name` (strip `${CUBE}.`)
- `sql: \`${CUBE}.qty * ${CUBE}.price\`` → `expr: "qty * price"` (strip all `${CUBE}.` prefixes)
- `sql: \`${OtherCube}.column\``→ **cannot go in`expr`\*\* directly; see cross-view references below

**Primary key dimensions:**

- Dimensions with `primary_key: true` → declare as a dimension AND add a `primary` entity
- Keep the dimension itself (for grouping/filtering), but also add the entity declaration

**Example — JS:**

```js
dimensions: {
  order_id: { sql: `id`, type: `number`, primary_key: true },
  status:   { sql: `status`, type: `string` },
  created_at: { sql: `created_at`, type: `time` },
  line_total: { sql: `${CUBE}.qty * ${CUBE}.unit_price`, type: `number` },
}
```

**→ YAML:**

```yaml
entities:
  - name: order
    type: primary
    key: id

dimensions:
  - name: order_id
    type: number
    expr: id

  - name: status
    type: string
    expr: status

  - name: created_at
    type: datetime
    expr: created_at

  - name: line_total
    type: number
    expr: "qty * unit_price"
```

### Measures

**Type mapping:**

| Cube.js type            | airlayer type     | Notes                                                           |
| ----------------------- | ----------------- | --------------------------------------------------------------- |
| `count`                 | `count`           | No `expr` needed                                                |
| `count_distinct`        | `count_distinct`  |                                                                 |
| `count_distinct_approx` | `count_distinct`  | Becomes exact; note the change                                  |
| `sum`                   | `sum`             |                                                                 |
| `avg`                   | `average`         | Cube.js uses `avg`, airlayer uses `average`                     |
| `min`                   | `min`             |                                                                 |
| `max`                   | `max`             |                                                                 |
| `number`                | `number`          | Computed/derived — use `expr` with the formula (see note below) |
| `running_total`         | _(no equivalent)_ | Use the `cumulative` motif at query time instead                |

**`number` vs `custom` — picking the right type:**

- Use `type: number` when the expression contains **row-level math** (no aggregate function) and airlayer wraps it: `expr: "qty * unit_price"`
- Use `type: custom` when the expression is **already a full aggregation** and should be passed through verbatim: `VARIANCE(Weekly_Sales)`, `CORR(Temperature, Sales)`, `STDDEV(x) / AVG(x) * 100`

```yaml
# number — row-level formula, airlayer adds no wrapper
- name: profit_margin
  type: number
  expr: "total_profit / NULLIF(total_revenue, 0)"

# custom — expression already contains its own aggregate function(s)
- name: sales_stddev
  type: custom
  expr: "STDDEV(Weekly_Sales)"

- name: temp_correlation
  type: custom
  expr: "CORR(Temperature, Weekly_Sales)"
```

**Measure filters:**

```js
completed_count: {
  type: `count`,
  filters: [{ sql: `${CUBE}.status = 'completed'` }],
}
```

→

```yaml
- name: completed_count
  type: count
  filters:
    - expr: "status = 'completed'"
```

Filter expression rewriting rules:

- `${CUBE}.column = 'value'` → strip `${CUBE}.` → `column = 'value'`
- `column = 'value'` (no `${CUBE}.` prefix) → use as-is → `column = 'value'`
- Both forms appear in Cube.js YAML schemas; bare column references need no change.

**Derived measures (`type: number`):**

```js
profit_margin: {
  sql: `${total_profit} / NULLIF(${total_revenue}, 0)`,
  type: `number`,
}
```

→

```yaml
- name: profit_margin
  type: number
  expr: "total_profit / NULLIF(total_revenue, 0)"
```

Replace `${measure_name}` references with the measure name directly (airlayer resolves measure refs by name).

### Segments

```js
segments: {
  completed: { sql: `${CUBE}.status = 'completed'` },
  high_value: { sql: `${CUBE}.amount > 1000` },
}
```

→

```yaml
segments:
  - name: completed
    expr: "status = 'completed'"

  - name: high_value
    expr: "amount > 1000"
```

## Step 4: Handle cross-view references

When a dimension `sql` references another cube — `${OtherCube}.column` — it's a subquery dimension. Use `sub_query: true`:

```js
// In customers cube
total_orders: {
  sql: `${orders.count}`,
  type: `number`,
  sub_query: true,
}
```

→

```yaml
- name: total_orders
  type: number
  expr: "orders.order_count"
  sub_query: true
```

The `expr` for a sub_query dimension is `view_name.measure_name` referencing the measure from the related view.

## Step 5: Required view fields

Every `.view.yml` must have:

```yaml
name: <cube_name> # snake_case
table: <sql_table value> # actual DB table/schema.table
```

Optional but recommended:

```yaml
description: "..." # from cube's title/description field if present
dialect: postgres # match the database type
datasource: warehouse # match the database name from config.yml
```

## Step 6: Validate

After writing all `.view.yml` files, validate them:

```bash
airlayer validate
```

Fix any errors reported. Common issues:

- Entity name mismatch across views (must be identical strings)
- Missing `expr` on a measure that requires one
- Invalid type value

## Step 7: Test joins

For any views connected by entities, test a cross-view query to confirm joins work:

```bash
airlayer query \
  --dimension customers.name \
  --measure orders.total_revenue
```

If the join fails, check that entity names match exactly across both views.

## What doesn't translate

The following Cube.js features have no equivalent in airlayer. Skip them during migration:

| Cube.js feature                   | Action                                                   |
| --------------------------------- | -------------------------------------------------------- |
| `pre_aggregations`                | Skip — airlayer has no caching layer                     |
| `refresh_key`                     | Skip — caching directive                                 |
| `rolling_window` on measures      | Use `moving_average` or `cumulative` motif at query time |
| `access_policy` / `public: false` | Skip — airlayer has no row-level security                |
| `data_source` override per cube   | Set `datasource:` at the view level instead              |
| `extends` (cube inheritance)      | Inline the inherited fields manually                     |
| `shown: false` on members         | Skip — airlayer shows all members                        |

## Full example

**Input (`orders.js`):**

```js
cube(`orders`, {
  sql_table: `orders`,
  joins: {
    customers: {
      relationship: `many_to_one`,
      sql: `${CUBE}.customer_id = ${customers}.id`,
    },
  },
  dimensions: {
    order_id: { sql: `id`, type: `number`, primary_key: true },
    status: { sql: `status`, type: `string` },
    created_at: { sql: `created_at`, type: `time` },
  },
  measures: {
    count: { type: `count` },
    total_revenue: { sql: `amount`, type: `sum` },
    avg_order_value: { sql: `amount`, type: `avg` },
    completed_count: {
      type: `count`,
      filters: [{ sql: `${CUBE}.status = 'completed'` }],
    },
  },
});
```

**Output (`orders.view.yml`):**

```yaml
name: orders
table: orders

entities:
  - name: order
    type: primary
    key: id
  - name: customer
    type: foreign
    key: customer_id

dimensions:
  - name: order_id
    type: number
    expr: id

  - name: status
    type: string
    expr: status

  - name: created_at
    type: datetime
    expr: created_at

measures:
  - name: count
    type: count

  - name: total_revenue
    type: sum
    expr: amount

  - name: avg_order_value
    type: average
    expr: amount

  - name: completed_count
    type: count
    filters:
      - expr: "status = 'completed'"
```

**Output (`customers.view.yml`):**

```yaml
name: customers
table: customers

entities:
  - name: customer
    type: primary
    key: id

dimensions:
  - name: id
    type: number
    expr: id
  # ... other dimensions
```
