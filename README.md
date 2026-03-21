# o3 (ozone)

An in-process semantic engine that compiles `.view.yml` definitions into SQL. Built in Rust as both a library and CLI tool.

o3 reads `.view.yml` schema files (the same format used by [Oxy](https://github.com/oxy-hq/oxy)), resolves entity relationships, and generates dialect-specific SQL from structured query requests.

## Install

```bash
cargo install --path .
```

## Quick start

Given a `views/orders.view.yml`:

```yaml
name: orders
table: public.orders
datasource: warehouse

dimensions:
  - name: status
    type: string
    expr: status
  - name: order_date
    type: time
    expr: order_date

measures:
  - name: count
    type: count
  - name: total_revenue
    type: sum
    expr: amount
```

Query it:

```bash
# Shorthand flags
o3 query --path views/ -d postgres \
  --dimensions orders.status \
  --measures orders.total_revenue \
  --filter orders.status:equals:active \
  --order orders.total_revenue:desc \
  --limit 10

# Or with JSON
o3 query --path views/ -d postgres -q '{
  "dimensions": ["orders.status"],
  "measures": ["orders.total_revenue"],
  "filters": [{"member": "orders.status", "operator": "equals", "values": ["active"]}],
  "order": [{"id": "orders.total_revenue", "desc": true}],
  "limit": 10
}'
```

Both produce:

```sql
SELECT
  "orders".status AS "orders__status",
  SUM("orders".amount) AS "orders__total_revenue"
FROM public.orders AS "orders"
WHERE ("orders".status = 'active')
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

## CLI

```
o3 <COMMAND>

Commands:
  query     Compile a query to SQL
  validate  Validate .view.yml files
  inspect   List views, dimensions, and measures
```

### `o3 query`

`--path` accepts a base directory containing `views/` and/or `topics/` subdirectories (defaults to current directory):

```bash
o3 query --path myproject/           # directory with views/ and topics/
o3 query                             # uses current directory
```

**Query input** — use either shorthand flags or `-q` JSON (not both):

| Flag | Description |
|---|---|
| `--dimensions <member>` | Dimension to select (repeatable) |
| `--measures <member>` | Measure to select (repeatable) |
| `-f, --filter <expr>` | Filter as `member:operator:value` (repeatable) |
| `--order <expr>` | Order as `member:asc` or `member:desc` (repeatable) |
| `--limit <n>` | Row limit |
| `--offset <n>` | Row offset |
| `-q, --query <json>` | Full query as JSON (or `-` for stdin) |

**Filter syntax:**

```
member:operator:value
member:operator:val1,val2,val3   # multiple values
member:set                       # no value needed
member:notSet                    # no value needed
```

Operators: `equals`, `notEquals`, `contains`, `notContains`, `startsWith`, `notStartsWith`, `endsWith`, `notEndsWith`, `gt`, `gte`, `lt`, `lte`, `set`, `notSet`, `inDateRange`, `notInDateRange`, `beforeDate`, `beforeOrOnDate`, `afterDate`, `afterOrOnDate`

**Dialect resolution:**

```bash
o3 query -d bigquery ...                    # explicit dialect
o3 query -c config.yml ...                  # from config.yml
o3 query ...                                # defaults to postgres
```

With `-c`, dialect is resolved from each view's `datasource` field mapped through `config.yml`:

```yaml
# config.yml
databases:
  - name: warehouse
    type: bigquery
  - name: operational
    type: postgres
```

### `o3 validate`

```bash
o3 validate --path views/
```

### `o3 inspect`

```bash
o3 inspect --path views/
```

## Dialects

Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Databricks, Redshift, SQLite.

Each dialect handles identifier quoting, `DATE_TRUNC`, timezone conversion, parameter placeholders, and type casting according to its conventions.

## Features

- **Entity-based auto-joins**: Primary/foreign entity declarations drive automatic JOIN generation via petgraph with BFS pathfinding. Multi-hop transitive joins (A -> B -> C) are supported.
- **Fan-out protection**: When OneToMany joins would multiply rows, measures are pre-aggregated in CTEs to prevent incorrect results.
- **Segments**: Predefined reusable filter conditions declared in view files, applied as WHERE clauses.
- **HAVING routing**: Filters on measures are automatically routed to HAVING instead of WHERE.
- **Parameterized queries**: Filter values use dialect-specific parameter placeholders ($1, ?, @p0).
- **Expression qualification**: Bare column references in expressions are automatically table-qualified.
- **Self-referencing expressions**: `{TABLE}` in dimension/measure expressions resolves to the view's table alias.
- **Cost-based base view selection**: When multiple views are referenced, the view minimizing join tree cost is selected as the base.
- **Join type from relationship**: OneToOne relationships use INNER JOIN; ManyToOne and OneToMany use LEFT JOIN.
- **Globals inheritance**: Shared dimension/measure/entity definitions via a globals file.
- **Variable passthrough**: `{{variables.X}}` patterns are preserved in output SQL for runtime substitution.

## Schema format

### Views (`.view.yml`)

```yaml
name: orders
table: public.orders       # or sql: "SELECT * FROM ..."
datasource: warehouse      # maps to dialect via config.yml
description: Order data

entities:
  - name: customer
    type: primary           # or foreign
    key: customer_id        # or keys: [col_a, col_b]

dimensions:
  - name: status
    type: string            # string, number, time, boolean
    expr: status

measures:
  - name: total_revenue
    type: sum               # count, sum, avg, min, max, count_distinct, median, custom
    expr: amount
    filters:                # optional CASE WHEN filter
      - member: orders.status
        operator: equals
        values: ["completed"]

segments:
  - name: active_only
    expr: "status = 'active'"
    description: "Only active orders"
```

### Entities and auto-joins

Views declare primary and foreign entities. When a query references members from multiple views, o3 automatically generates JOINs by matching foreign entities to primary entities across views:

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

Querying `customers.name` and `orders.total_revenue` together auto-generates:

```sql
FROM public.orders AS "orders"
LEFT JOIN public.customers AS "customers"
  ON "orders".customer_id = "customers".id
```

Multi-hop transitive joins (A -> B -> C) are supported via BFS pathfinding on the entity graph.

### Cross-entity references

Expressions can reference fields from related entities using `{{entity.field}}`:

```yaml
dimensions:
  - name: customer_name
    type: string
    expr: "{{customer.name}}"
```

### Globals inheritance

Shared definitions via a globals file (`-g globals.yml`):

```yaml
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

Views inherit with `inherits_from`:

```yaml
dimensions:
  - name: created_at
    inherits_from: globals.semantics.dimensions.created_at
```

### Variables

`{{variables.X}}` references are preserved as-is in the output SQL for runtime substitution.

## Examples

The `examples/` directory contains example queries across different dialects:

| Directory | Domain | Dialect | Features demonstrated |
|-----------|--------|---------|----------------------|
| `bigquery/` | IoT sensor telemetry | BigQuery | Single-view queries, filters, custom measures |
| `clickhouse/` | Logistics & shipping | ClickHouse | Auto-joins, measure-level filters, JSON query input |
| `duckdb/` | Course enrollments | DuckDB | CSV tables, filtered measures, status filters |
| `snowflake/` | Subscription revenue | Snowflake | Multi-view joins, CASE expressions, self-referencing dimensions |
| `multi-dialect/` | Product events | All 9 dialects | Same query compiled to every supported dialect |

Each example directory contains `views/`, `topics/`, and numbered shell scripts. Run from the example directory:

```bash
cd examples/clickhouse
bash 01_shipments_by_channel.sh
```

## Library usage

```rust
use o3::{SemanticEngine, DatasourceDialectMap, Dialect};
use o3::engine::query::QueryRequest;

// Load views
let dialects = DatasourceDialectMap::with_default(Dialect::Postgres);
let engine = SemanticEngine::load(
    Path::new("."),   // base directory containing views/ and topics/
    None,
    dialects,
)?;

// Compile a query
let request: QueryRequest = serde_json::from_str(r#"{
    "dimensions": ["orders.status"],
    "measures": ["orders.total_revenue"]
}"#)?;

let result = engine.compile_query(&request)?;
println!("{}", result.sql);
```

## Architecture

```
src/
├── cli/mod.rs              CLI (clap)
├── dialect/
│   ├── mod.rs              Dialect enum, per-dialect SQL functions
│   └── templates.rs        minijinja SQL templates
├── engine/
│   ├── mod.rs              SemanticEngine, DatasourceDialectMap
│   ├── evaluator.rs        Schema member lookup and resolution
│   ├── join_graph.rs       petgraph-based entity join graph with BFS
│   ├── member_sql.rs       {{entity.field}} and {{variables.X}} resolution
│   ├── query.rs            QueryRequest/QueryResult types, filter operators
│   ├── sql_generator.rs    SQL generation (SELECT/JOIN/WHERE/GROUP BY/...)
│   └── error.rs            EngineError types
├── schema/
│   ├── models.rs           View, Dimension, Measure, Entity, SemanticLayer
│   ├── parser.rs           .view.yml parser with globals resolution
│   ├── validator.rs        Schema validation
│   └── globals.rs          Globals file parsing
├── lib.rs                  Public API
└── main.rs                 CLI entry point
```

## Testing

```bash
cargo test              # 28 unit tests + 17 integration tests
```

Integration tests are in `tests/`. See [tests/README.md](tests/README.md) for the two-tier testing strategy (in-process DuckDB/SQLite + Docker-based Postgres/MySQL/ClickHouse).

## Development

```bash
cargo build
cargo test
cargo run -- --help
```
