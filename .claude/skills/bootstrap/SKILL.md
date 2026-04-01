---
name: bootstrap
description: Bootstrap a semantic layer from a database. Use when the user wants to create .view.yml files from their warehouse schema, or when starting a new airlayer project from scratch.
---

# Bootstrap a Semantic Layer

You are bootstrapping a semantic layer for airlayer. This means discovering what's in the user's database and generating `.view.yml` files that define dimensions, measures, and entities.

## Prerequisites

The user needs a `config.yml` with database connection details. If they don't have one, help them create it. The format is:

```yaml
databases:
  - name: <name>
    type: <postgres|snowflake|bigquery|duckdb|motherduck|mysql|clickhouse|databricks|sqlite>
    # ... connection fields vary by type (see docs/agent-execution.md)
```

MotherDuck example:
```yaml
databases:
  - name: cloud
    type: motherduck
    token_var: MOTHERDUCK_TOKEN
    database: my_db
```

airlayer must be built with executor support: `cargo build --features exec` (or a specific driver like `exec-postgres`).

## Workflow

### Step 1: Introspect the schema

Run schema introspection to discover all tables, columns, and types:

```bash
airlayer inspect --schema --config <config.yml>
```

Optionally filter to a specific schema/dataset:
```bash
airlayer inspect --schema <schema_name> --config <config.yml>
```

This returns JSON with every table and column. Read the output carefully — it's your source of truth for what's available.

### Step 2: Ask the user which tables to model

Present the discovered tables to the user and ask which ones they want in the semantic layer. Don't model everything — focus on the tables they care about for analytics.

### Step 3: Generate .view.yml files

For each selected table, create a `.view.yml` file in a `views/` directory. Follow these rules:

**Dimensions** (attributes to group/filter by):
- String columns → `type: string`
- Date columns → `type: date`
- Datetime/timestamp columns → `type: datetime`
- Boolean columns → `type: boolean`
- Numeric columns used for grouping (IDs, codes) → `type: string` or `type: number`

**Measures** (aggregations):
- Row count → `type: count` (no expr needed)
- Unique counts → `type: count_distinct` with `expr: <column>`
- Sums → `type: sum` with `expr: <column>`
- Averages → `type: average` with `expr: <column>`
- Computed measures → `type: sum` with `expr: "quantity * price"` etc.

**Entities** (join keys):
- Primary keys → `type: primary`, `key: <column>`
- Foreign keys → `type: foreign`, `key: <column>`
- Name entities after the concept they represent (e.g., `customer`, `order`), not the column name

**Naming conventions**:
- `name:` should be snake_case, semantic (e.g., `total_revenue` not `sum_amount`)
- `expr:` is the raw SQL expression — reference actual column names from the schema
- `description:` add for any non-obvious measures or computed fields

Example view:
```yaml
name: orders
description: "Sales orders with customer and product data"
dialect: postgres
datasource: warehouse
table: public.orders

entities:
  - name: customer
    type: foreign
    key: customer_id

dimensions:
  - name: order_id
    type: string
    expr: order_id

  - name: status
    type: string
    expr: status

  - name: created_at
    type: datetime
    expr: created_at

measures:
  - name: order_count
    type: count

  - name: total_revenue
    type: sum
    expr: amount
```

### Step 4: Profile dimensions

After creating views, profile them to verify the data looks right:

```bash
# Profile all dimensions in a view
airlayer inspect --profile <view_name> --config <config.yml> --path <dir>

# Profile a single dimension
airlayer inspect --profile <view_name>.<dimension_name> --config <config.yml> --path <dir>
```

Review the profile output:
- **String dimensions**: Check cardinality and values — are they what you'd expect?
- **Number dimensions**: Check min/max/mean — do the ranges make sense?
- **Date dimensions**: Check the date range — is it current data?

### Step 5: Test with queries

Run a few test queries to validate the semantic layer:

```bash
airlayer query --execute --config <config.yml> --path <dir> \
  --dimension <view>.<dim> --measure <view>.<measure>
```

Check the envelope:
- `status: "success"` → the view works
- `sql` → does the generated SQL look correct?
- `data` → are the values reasonable?

### Step 6: Iterate

If something's wrong:
- Wrong column name in `expr` → fix the expr, re-run
- Missing measure → add it to the view, re-run
- Bad aggregation type → change the measure type
- Need joins → add entities to both views, airlayer infers JOINs automatically

## Important notes

- The `dialect` field must match the database type (postgres, snowflake, bigquery, duckdb, motherduck, mysql, clickhouse, databricks, redshift, sqlite)
- MotherDuck uses the `duckdb` dialect — set `dialect: duckdb` in views that target MotherDuck
- The `datasource` field must match a database `name` in config.yml
- The `table` field is the actual table name in the database (can be schema-qualified like `public.orders`)
- All views in a query must use the same dialect
- Entity names must match across views for joins to work (e.g., both views declare entity `customer`)
