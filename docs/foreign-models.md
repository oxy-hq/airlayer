# Foreign Semantic Model Support

airlayer can convert semantic models from other platforms into its native `.view.yml` format, enabling you to compile SQL queries from Cube.js, Looker LookML, dbt MetricFlow, and Omni definitions.

## Supported Formats

| Format | File Extensions | CLI Flag | Description |
|--------|----------------|----------|-------------|
| Cube.js | `.yml`, `.yaml` | `--format cube` | Cube.js YAML schema (cubes, dimensions, measures, joins) |
| LookML | `.lkml` | `--format lookml` | Looker LookML (views, explores, dimension_groups) |
| dbt MetricFlow | `.yml`, `.yaml` | `--format dbt` | dbt semantic_models + metrics |
| Omni | `.yml`, `.yaml` | `--format omni` | Omni Analytics YAML (views, topics, dimension_groups) |

## Quick Start

### Convert and query

```bash
# Convert a Cube.js schema to airlayer views
airlayer convert --format cube ./cube_schema/ --output ./views/

# Convert a LookML file
airlayer convert --format lookml ./models/orders.lkml --output ./views/

# Convert dbt semantic models
airlayer convert --format dbt ./models/semantic.yml --output ./views/

# Convert Omni models
airlayer convert --format omni ./models/analytics.yml --output ./views/

# Then query normally
airlayer query --measure orders.total_revenue --dimension orders.status
```

### Print converted YAML to stdout

```bash
airlayer convert --format cube ./cube_schema/ --stdout
```

### Set dialect on converted views

```bash
airlayer convert --format lookml ./models/ --output ./views/ --dialect bigquery
```

## Format Details

### Cube.js

Cube.js schemas define `cubes` with SQL table references, dimensions, measures, joins, and segments.

**Supported features:**
- `sql_table` / `sql` (derived tables)
- Dimension types: `string`, `number`, `time`, `boolean`, `geo`
- Measure types: `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `count_distinct_approx`, `number`, `running_total`
- Measure filters (`filters` array)
- Rolling window measures
- Joins: `belongs_to` (many_to_one), `has_many` (one_to_many), `has_one` (one_to_one)
- Segments (boolean SQL conditions)
- `primary_key` on dimensions
- Sub-query dimensions (`sub_query: true`)
- `{CUBE}` / `{TABLE}` reference rewriting
- Cross-cube references (`{other_cube}.column`)
- `data_source` mapping to airlayer `datasource`

**Type mapping:**

| Cube.js | airlayer |
|---------|----------|
| `string` | `string` |
| `number` | `number` |
| `time` | `datetime` |
| `boolean` | `boolean` |
| `geo` | `geo` |

**Reference rewriting:**
- `{CUBE}.column` → `column` (self-reference)
- `{TABLE}.column` → `column` (self-reference)
- `{other_cube}.column` → `{{other_cube.column}}` (cross-view reference)

**Example input:**
```yaml
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: status
        sql: "{CUBE}.status"
        type: string
    measures:
      - name: count
        type: count
      - name: total_amount
        type: sum
        sql: "{CUBE}.amount"
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users}.id"
        relationship: belongs_to
```

### LookML (Looker)

LookML files define `view` blocks with dimensions, measures, and `explore` blocks with joins.

**Supported features:**
- `sql_table_name` / `derived_table` (with SQL)
- Dimension types: `string`, `number`, `yesno` (→ boolean), `date`, `time`, `datetime`, `tier`, `zipcode`, `location`
- `dimension_group` with timeframes (generates one dimension per timeframe)
- `dimension_group` with `type: duration` (generates interval dimensions)
- Measure types: `count`, `sum`, `average`, `min`, `max`, `count_distinct`, `median`, `number`, `running_total`
- Measure filters
- `primary_key: yes`
- `explore` blocks with join relationships
- `${TABLE}.column` and `${view.field}` reference rewriting
- LookML comments (`#` to end of line)
- `;;` statement terminators

**Dimension group expansion:**

A `dimension_group: created` with `timeframes: [raw, date, month, year]` generates:
- `created_raw` (datetime)
- `created_date` (date)
- `created_month` (date)
- `created_year` (date)

**Type mapping:**

| LookML | airlayer |
|--------|----------|
| `string` | `string` |
| `number` | `number` |
| `yesno` | `boolean` |
| `date` | `date` |
| `time` / `datetime` | `datetime` |
| `tier` | `number` |
| `zipcode` / `location` | `string` |

### dbt MetricFlow

dbt semantic models define entities, dimensions, and measures. Metrics are defined separately and converted to derived measures.

**Supported features:**
- `model: ref('model_name')` → table name resolution
- `source('source', 'table')` → `source.table`
- Entity types: `primary`, `unique`, `natural` (→ Primary), `foreign` (→ Foreign)
- Dimension types: `categorical` (→ string), `time` (→ date/datetime based on granularity)
- Measure aggregations: `sum`, `count`, `count_distinct`, `average`, `min`, `max`, `median`, `sum_boolean`, `percentile`
- Measure filters (`filter_expr` / `where` clauses)
- `agg_time_dimension` (default time dimension)
- Metrics:
  - `simple` — alias for a measure (no-op)
  - `ratio` — generates a Number measure with `CAST(numerator AS DOUBLE) / NULLIF(denominator, 0)`
  - `cumulative` — generates a measure with rolling window
  - `derived` — generates a Number measure with the expression
- Jinja filter rewriting: `{{ Dimension('entity__dim') }}` → `dim`

**Type mapping:**

| dbt | airlayer |
|-----|----------|
| `categorical` | `string` |
| `time` (day/week/month/quarter/year granularity) | `date` |
| `time` (other) | `datetime` |
| `boolean` | `boolean` |

### Omni

Omni uses YAML-based modeling with a syntax inspired by LookML but cleaner.

**Supported features:**
- `sql_table_name` / `derived_table` (with SQL)
- Dimension types: same as LookML (`string`, `number`, `yesno`/`boolean`, `date`, `time`, etc.)
- `dimension_groups` with timeframes (same expansion as LookML)
- `dimension_groups` with `type: duration` and intervals
- Measure types: `count`, `sum`, `average`, `min`, `max`, `count_distinct`, `median`, `number`, `running_total`
- Measure filters (field→value map)
- `primary_key: true`
- Topics with joins and relationships
- `${TABLE}.column` and `${view.field}` reference rewriting
- Filter fields → airlayer segments

**Note:** Omni uses a map-based syntax (dimensions/measures as named maps) rather than Cube.js/LookML's list-based syntax.

## Entity/Join Mapping

All formats map their join/relationship concepts to airlayer's entity system:

| Foreign Concept | airlayer Entity |
|----------------|----------------|
| Cube `belongs_to` / LookML `many_to_one` | Foreign entity |
| Cube `has_many` / LookML `one_to_many` | Primary entity |
| Cube `has_one` / LookML `one_to_one` | Foreign entity |
| dbt `primary` / `unique` / `natural` entity | Primary entity |
| dbt `foreign` entity | Foreign entity |

Join keys are automatically extracted from SQL join conditions when possible.

## Testing

### Unit tests

```bash
cargo test --lib schema::foreign    # All foreign parser tests
```

### Cube.js parity tests (tier 2)

The Cube.js parity tests verify that airlayer generates correct SQL from converted Cube schemas by running queries against a real PostgreSQL database:

```bash
just cube-up           # Start Postgres + Cube.js containers
just test-cube-parity  # Run parity tests
just cube-down         # Stop containers
```

These tests:
1. Convert Cube.js YAML schemas to airlayer views
2. Compile queries to SQL using airlayer's engine
3. Execute the SQL against the same Postgres database
4. Verify results match expected hand-written SQL queries

## Limitations

- **LookML Liquid templating** is not supported. Liquid `{% %}` / `{{ }}` blocks in SQL expressions are passed through as-is.
- **Cube.js JavaScript schemas** (`.js` files) are not supported — only YAML schemas.
- **dbt Jinja** is partially supported (Dimension/TimeDimension references are rewritten), but complex Jinja logic is not evaluated.
- **Pre-aggregations** (Cube.js) are not converted — airlayer doesn't have an equivalent concept.
- **Drill fields** (LookML/Cube.js) are noted in warnings but not preserved.
- **Cross-view references** are mapped to airlayer's `{{ view.field }}` syntax but may require manual review for complex expressions.
