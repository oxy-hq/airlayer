<p align="center">
  <img src="assets/splash.svg" alt="airlayer — semantic engine" width="100%">
</p>

# airlayer

An in-process semantic engine that compiles `.view.yml` definitions into SQL — and optionally executes queries against real databases. Built in Rust as both a library and CLI tool.

airlayer reads `.view.yml` schema files (the same format used by [Oxy](https://github.com/oxy-hq/oxy)), resolves entity relationships, and generates dialect-specific SQL from structured query requests. With `--execute`, it also runs queries and returns structured JSON envelopes designed for AI agent consumption.

See [PHILOSOPHY.md](PHILOSOPHY.md) for the design principles behind airlayer.

## Install

```bash
# Core (compile-only, no database drivers)
cargo install --path .

# With database execution support
cargo install --path . --features exec              # all drivers
cargo install --path . --features exec-postgres      # postgres/redshift only
cargo install --path . --features exec-snowflake     # snowflake only
cargo install --path . --features exec-duckdb        # duckdb only
```

## Quick start

Given a `views/orders.view.yml`:

```yaml
name: orders
description: Order data
table: public.orders
dialect: postgres

dimensions:
  - name: status
    type: string
    expr: status
  - name: order_date
    type: date
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
# Shorthand flags — dialect is inferred from the view file
airlayer query \
  --dimensions orders.status \
  --measures orders.total_revenue \
  --filter orders.status:equals:active \
  --order orders.total_revenue:desc \
  --limit 10

# Or with JSON
airlayer query -q '{
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

## Execution (agent interface)

With `--execute`, airlayer compiles the query **and** runs it against a configured database, returning a structured JSON envelope:

```bash
airlayer query --execute -c config.yml \
  --dimensions orders.status \
  --measures orders.total_revenue
```

```json
{
  "status": "success",
  "sql": "SELECT \"orders\".status AS \"orders__status\", SUM(\"orders\".amount) AS \"orders__total_revenue\" FROM public.orders AS \"orders\" GROUP BY 1",
  "columns": [
    {"name": "orders__status", "member": "orders.status", "kind": "dimension"},
    {"name": "orders__total_revenue", "member": "orders.total_revenue", "kind": "measure"}
  ],
  "data": [
    {"orders__status": "active", "orders__total_revenue": 15000},
    {"orders__status": "completed", "orders__total_revenue": 42000}
  ],
  "row_count": 2,
  "views_used": ["orders"]
}
```

The envelope is designed for AI agents iterating on `.view.yml` accuracy. `status` encodes where failures occur (`parse_error`, `compile_error`, `execution_error`), `views_used` tells the agent which files to edit, and `data` is capped at 50 rows to respect context window budgets.

Requires a `config.yml` with database connection details and an `exec-*` feature flag. See [docs/agent-execution.md](docs/agent-execution.md) for the full envelope spec and [PHILOSOPHY.md](PHILOSOPHY.md) for the design rationale.

### Schema introspection

Agents discover the semantic vocabulary at runtime — no docs needed:

```bash
airlayer inspect --json --path views/
```

Returns machine-readable JSON with all views, dimensions, measures, types, expressions, and descriptions.

## Dialect resolution

Each view declares its own SQL dialect via the `dialect` field. This is the primary way dialect is determined — the view file is self-describing.

```yaml
# views/orders.view.yml
name: orders
table: public.orders
dialect: bigquery      # ← this view generates BigQuery SQL
```

For Oxy projects or multi-datasource setups, views use `datasource` with a `config.yml` instead:

```yaml
# views/orders.view.yml
name: orders
table: public.orders
datasource: warehouse  # ← resolved via config.yml
```

```yaml
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
| 1 | `-d` CLI flag | One-off override, multi-dialect examples |
| 2 | `-c config.yml` + `datasource` | Oxy projects, multi-datasource |
| 3 | View-level `dialect` field | Standalone projects (default) |
| 4 | Postgres fallback | When nothing is specified |

## CLI

```
airlayer <COMMAND>

Commands:
  query     Compile a query to SQL (or compile + execute with --execute)
  validate  Validate .view.yml files
  inspect   List views, dimensions, and measures (--json for machine-readable)
```

### `airlayer query`

`--path` accepts a base directory containing `views/` and/or `topics/` subdirectories (defaults to current directory):

```bash
airlayer query --path myproject/           # directory with views/ and topics/
airlayer query                             # uses current directory
```

**Query input** — use either shorthand flags or `-q` JSON (not both):

| Flag | Description |
|---|---|
| `--dimensions <member>` | Dimension to select (repeatable) |
| `--measures <member>` | Measure to select (repeatable) |
| `-f, --filter <expr>` | Filter as `member:operator:value` (repeatable) |
| `--segments <member>` | Segment to apply (repeatable) |
| `--order <expr>` | Order as `member:asc` or `member:desc` (repeatable) |
| `--through <entity>` | Route joins through entity (repeatable) |
| `--limit <n>` | Row limit |
| `--offset <n>` | Row offset |
| `-q, --query <json>` | Full query as JSON (or `-` for stdin) |
| `-d <dialect>` | Override dialect |
| `-c <config.yml>` | Datasource→dialect config (for Oxy projects) |
| `-x, --execute` | Execute against database, return JSON envelope |
| `--datasource <name>` | Target a specific database from config.yml |

**Filter syntax:**

```
member:operator:value
member:operator:val1,val2,val3   # multiple values
member:set                       # no value needed
member:notSet                    # no value needed
```

Operators: `equals`, `notEquals`, `contains`, `notContains`, `startsWith`, `notStartsWith`, `endsWith`, `notEndsWith`, `gt`, `gte`, `lt`, `lte`, `set`, `notSet`, `inDateRange`, `notInDateRange`, `beforeDate`, `beforeOrOnDate`, `afterDate`, `afterOrOnDate`, `onTheDate`

### `airlayer validate`

```bash
airlayer validate --path views/
```

### `airlayer inspect`

```bash
airlayer inspect --path views/              # human-readable table
airlayer inspect --path views/ --json       # machine-readable JSON (for agents)
```

## Dialects

Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Databricks, Redshift, SQLite, Domo.

Each dialect handles identifier quoting, `DATE_TRUNC`, timezone conversion, parameter placeholders, and type casting according to its conventions.

## Features

- **Entity-based auto-joins**: Primary/foreign entity declarations drive automatic JOIN generation via petgraph with BFS pathfinding. Multi-hop transitive joins (A -> B -> C) are supported.
- **Join hints (`--through`)**: Disambiguate join paths by specifying which entities to route through when multiple paths exist.
- **Fan-out protection**: When OneToMany joins would multiply rows, measures are pre-aggregated in CTEs to prevent incorrect results.
- **Segments**: Predefined reusable filter conditions declared in view files, applied as WHERE clauses.
- **HAVING routing**: Filters on measures are automatically routed to HAVING instead of WHERE.
- **Parameterized queries**: Filter values use dialect-specific parameter placeholders ($1, ?, @p0).
- **Measure-to-measure references**: Use `{{view.measure_name}}` in measure expressions to reference other measures. The references are resolved to their aggregate SQL expressions.
- **Subquery dimensions**: Dimensions with `sub_query: true` generate correlated subqueries referencing measures from related views.
- **Rolling windows**: Measures with `rolling_window` configuration generate window function frames for cumulative/running aggregations.
- **Relative date ranges**: Time dimensions support relative dates like `"last 7 days"`, `"this month"`, `"yesterday"`, etc.
- **Approximate count distinct**: `count_distinct_approx` measure type uses dialect-specific functions (APPROX_COUNT_DISTINCT, uniqHLL12, etc.).
- **Pass-through measures**: `number` measure type passes the expression as-is (for pre-aggregated expressions).
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
description: Order data
table: public.orders       # or sql: "SELECT * FROM ..."
dialect: postgres           # SQL dialect for this view
datasource: warehouse      # alternative: maps to dialect via config.yml

entities:
  - name: customer
    type: primary           # or foreign
    key: customer_id        # or keys: [col_a, col_b]

dimensions:
  - name: status
    type: string            # string, number, date, datetime, boolean, geo
    expr: status

measures:
  - name: total_revenue
    type: sum               # count, sum, avg, min, max, count_distinct, count_distinct_approx, median, number, custom
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

Views declare primary and foreign entities. When a query references members from multiple views, airlayer automatically generates JOINs by matching foreign entities to primary entities across views:

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

## Oxy interoperability

airlayer uses the same `.view.yml` format as [Oxy](https://github.com/oxy-hq/oxy). Oxy projects use `datasource` + `config.yml` for dialect resolution:

```yaml
# config.yml (Oxy format)
databases:
  - name: warehouse
    type: bigquery
  - name: operational
    type: postgres
```

```bash
# Use with Oxy's config
airlayer query -c config.yml \
  --dimensions orders.status \
  --measures orders.total_revenue
```

When `datasource` and `config.yml` are present, they take precedence over the view-level `dialect` field. This lets the same views work in both standalone airlayer projects (using `dialect:`) and Oxy projects (using `datasource:` + `config.yml`).

## Examples

The `examples/` directory contains example queries across different dialects. Each view declares its dialect directly — no `-d` flag needed:

| Directory | Domain | Dialect | Features demonstrated |
|-----------|--------|---------|----------------------|
| `bigquery/` | IoT sensor telemetry | BigQuery | Single-view queries, filters, custom measures |
| `clickhouse/` | Logistics & shipping | ClickHouse | Auto-joins, measure-level filters, JSON query input |
| `duckdb/` | Course enrollments | DuckDB | CSV tables, filtered measures, status filters |
| `snowflake/` | Subscription revenue | Snowflake | Multi-view joins, CASE expressions, self-referencing dimensions |
| `domo/` | Content performance | Domo | Dataset UUID tables, custom measures, backtick quoting |
| `rolling-windows/` | Daily sales | Postgres | Cumulative revenue, trailing 7-day rolling windows |
| `subquery-dims/` | Customers/orders | Postgres | Correlated subquery dimensions (sub_query: true) |
| `measure-refs/` | Financial metrics | Postgres | Measure-to-measure references (profit, margin, avg) |
| `segments/` | User analytics | Postgres | Segments, filtered measures, multiple segments |
| `join-hints/` | Order fulfillment | Postgres | Diamond join disambiguation with --through |
| `config-yml/` | Multi-datasource | Postgres + BigQuery | Oxy-compatible config.yml with datasource mapping |
| `multi-dialect/` | Product events | All 10 dialects | Same view compiled to every dialect via `-d` override |

Each example directory contains `views/`, `topics/`, and numbered shell scripts:

```bash
cd examples/clickhouse
bash 01_shipments_by_channel.sh
```

## Library usage

```rust
use airlayer::{SemanticEngine, DatasourceDialectMap, Dialect};
use airlayer::engine::query::QueryRequest;

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

### Feature flags

| Feature | Description | Dependencies |
|---------|-------------|-------------|
| *(none)* | Semantic engine only — compile queries to SQL | Zero extra deps |
| `exec-postgres` | Execute against Postgres/Redshift | `postgres` crate |
| `exec-snowflake` | Execute against Snowflake (REST API) | `ureq` |
| `exec-duckdb` | Execute against DuckDB (in-process) | `duckdb` crate |
| `exec` | All execution drivers | All of the above |

```toml
# Cargo.toml — library consumer, compile-only
airlayer = { version = "0.1", default-features = false }

# CLI with all drivers
airlayer = { version = "0.1", features = ["exec"] }
```

## Architecture

```
src/
├── cli/mod.rs              CLI (clap) — query, inspect, validate
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
├── executor/               Database executors (gated behind exec-* features)
│   ├── mod.rs              QueryEnvelope, DatabaseConnection, dispatch
│   ├── postgres.rs         Postgres/Redshift via libpq
│   ├── snowflake.rs        Snowflake via REST API
│   └── duckdb.rs           DuckDB in-process
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
cargo test              # 88 unit tests + 17 integration tests
```

Integration tests are in `tests/`. See [tests/README.md](tests/README.md) for the two-tier testing strategy (in-process DuckDB/SQLite + Docker-based Postgres/MySQL/ClickHouse).

## Documentation

| Document | Description |
|----------|-------------|
| [PHILOSOPHY.md](PHILOSOPHY.md) | Design principles — why the semantic layer is the contract layer |
| [docs/agent-execution.md](docs/agent-execution.md) | Execution envelope spec, config format, agent iteration loop |
| [docs/architecture.md](docs/architecture.md) | Pipeline stages: parse → resolve → plan → generate |
| [docs/query-api.md](docs/query-api.md) | QueryRequest format, filter operators, time dimensions |
| [docs/schema-format.md](docs/schema-format.md) | `.view.yml` reference — dimensions, measures, entities, segments |
| [docs/dialects.md](docs/dialects.md) | Per-dialect SQL behavior (quoting, date_trunc, timezone, params) |
| [docs/testing.md](docs/testing.md) | Two-tier testing strategy |

## Development

```bash
cargo build
cargo test
cargo run -- --help
```
