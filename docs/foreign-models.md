# Foreign Semantic Model Support

airlayer works out of the box with Cube.js, Looker LookML, dbt MetricFlow, and Omni repositories — no conversion step required. Run `airlayer init` inside an existing repo to set up your database connection, then query directly.

## Supported Formats

| Format | File Extensions | Auto-detected by | Description |
|--------|----------------|-----------------|-------------|
| Cube.js | `.yml`, `.yaml` | `cubes:` key | Cube.js YAML schema (cubes, dimensions, measures, joins) |
| LookML | `.lkml` | File extension | Looker LookML (views, explores, dimension_groups) |
| dbt MetricFlow | `.yml`, `.yaml` | `semantic_models:` key | dbt semantic_models + metrics |
| Omni | `.yml`, `.yaml` | `views:` + `topics:` keys | Omni Analytics YAML (views, topics, dimension_groups) |

## Quick Start

### Setup

Run `airlayer init` inside an existing foreign model repo. It auto-detects the format, extracts connection details from repo-specific files (dbt `profiles.yml`, Cube `.env`), and generates a `config.yml` with your database connection:

```bash
cd /path/to/lookml-project
airlayer init
# → Detected LookML project with 24 view files
# → Generated config.yml
```

### Query directly (no conversion needed)

Once initialized, query directly — airlayer auto-detects foreign model files and loads them natively:

```bash
# SQL compilation (no database connection needed, just --dialect)
airlayer query --measure orders.count --dimension orders.status -d postgres

# SQL execution (requires config.yml from airlayer init)
airlayer query --measure orders.count --dimension orders.status -x

# Inspect available views
airlayer inspect
```

airlayer auto-detects the format by scanning files in the directory. If `.view.yml` files exist, they take priority. Otherwise, airlayer checks for foreign formats in this order: LookML (`.lkml` extension) → Cube.js (`cubes:` key) → dbt (`semantic_models:` key) → Omni (`views:` key).

### Explicit conversion (optional)

You can also explicitly convert foreign models to `.view.yml` files:

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

## Feature Parity

The table below summarizes which features each parser supports. All four formats support native loading (query directly, no conversion step) and directory-level aggregation across multiple files.

### Dimensions

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| string | Y | Y | Y | Y |
| number | Y | Y | Y | Y |
| boolean | Y | Y (yesno) | Y | Y (yesno) |
| date / datetime | Y | Y | Y | Y |
| geo | Y | — | — | — |
| tier / zipcode / location | — | Y | — | — |
| Primary key | Y | Y | Y (entity) | Y |
| Labels / descriptions | Y | Y | Y | Y |
| Hidden fields | — | — | — | — |

### Dimension Groups

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| Time groups (→ multiple dims) | — | Y | — | Y |
| Duration groups (intervals) | — | Y | — | Y |
| Custom timeframes | — | Y | — | Y |

### Measures

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| count | Y | Y | Y | Y |
| sum | Y | Y | Y | Y |
| average | Y | Y | Y | Y |
| min / max | Y | Y | Y | Y |
| count_distinct | Y | Y | Y | Y |
| count_distinct_approx | Y | — | — | — |
| median | Y | — | Y | — |
| number (custom expr) | Y | Y | — | Y |
| running_total | Y | Y | — | Y |
| Measure filters | Y | Y | Y | Y |

### Joins & Relationships

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| many_to_one | Y | Y | Y | Y |
| one_to_many | Y | Y | Y | Y |
| one_to_one | Y | Y | — | — |
| Join key extraction from SQL | Y | Y | — | Y |
| Auto-create implicit FK dims | — | Y | — | Y |
| Cross-view SQL references | Y | Y | Y | Y |

### SQL References & Rewriting

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| `{CUBE}` / `{TABLE}` | Y | — | — | — |
| `${TABLE}` / `${view.field}` | — | Y | — | Y |
| `@{CONSTANT}` passthrough | — | Y | — | — |
| `ref()` / `source()` | — | — | Y | — |
| Jinja `{{ Dimension() }}` | — | — | Y | — |

### Segments & Filters

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| Named segments / filters | Y | Y | — | Y |

### Special Features

| Feature | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| Rolling window measures | Y | — | — | — |
| Explores (cross-file joins) | — | Y | — | — |
| Explore `from:` / `view_name:` | — | Y | — | — |
| Metrics → measures | — | — | Y | — |
| Topics (field curation) | — | — | — | Y |
| Directory format (multi-file) | — | — | — | Y |
| `relationships.yaml` | — | — | — | Y |
| Type inference from name/format | — | — | — | Y |

### Known Limitations

| Limitation | Cube.js | LookML | dbt | Omni |
|---------|:-------:|:------:|:---:|:----:|
| No JS schema support | X | — | — | — |
| No Liquid templates | — | X | — | — |
| No complex Jinja | — | — | X | — |
| Pre-aggregations ignored | X | — | — | — |
| Drill fields ignored | X | X | — | — |
| View refinements (`+name`) skipped | — | X | — | — |

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

## WASM / Library Usage

Foreign model support is available in the WASM package via feature flags. Each format can be included independently to minimize binary size:

```bash
# LookML only
wasm-pack build --target web --no-default-features --features wasm,foreign-lookml

# All formats
wasm-pack build --target web --no-default-features --features wasm,foreign
```

The `compile_foreign` function compiles queries directly from foreign model files:

```js
import init, { compile_foreign } from 'airlayer';
await init();

const result = compile_foreign(
  'lookml',                           // format
  [viewLkml, exploreLkml],            // array of file content strings
  JSON.stringify({                    // query JSON
    measures: ['orders.count'],
    dimensions: ['orders.status'],
  }),
  'postgres'                          // dialect
);
console.log(result.sql);
```

Available feature flags:

| Feature | Format | Adds |
|---------|--------|------|
| `foreign-cube` | Cube.js | `cube.rs` parser |
| `foreign-lookml` | LookML | `lookml.rs` parser (custom DSL parser) |
| `foreign-dbt` | dbt MetricFlow | `dbt.rs` parser |
| `foreign-omni` | Omni | `omni.rs` parser |
| `foreign` | All formats | All of the above |

The CLI includes all formats by default (`cli` depends on `foreign`).

## Limitations

- **LookML Liquid templating** is not supported. Liquid `{% %}` / `{{ }}` blocks in SQL expressions are passed through as-is.
- **Cube.js JavaScript schemas** (`.js` files) are not supported — only YAML schemas.
- **dbt Jinja** is partially supported (Dimension/TimeDimension references are rewritten), but complex Jinja logic is not evaluated.
- **Pre-aggregations** (Cube.js) are not converted — airlayer doesn't have an equivalent concept.
- **Drill fields** (LookML/Cube.js) are noted in warnings but not preserved.
- **Cross-view references** are mapped to airlayer's `{{ view.field }}` syntax but may require manual review for complex expressions.
