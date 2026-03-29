<p align="center">
  <img src="assets/splash.svg" alt="airlayer — semantic engine" width="100%">
</p>

# airlayer

An in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL — and optionally executes queries against real databases. Built in Rust as both a library and CLI tool.

airlayer reads `.view.yml` schema files (the same format used by [Oxy](https://github.com/oxy-hq/oxy)), resolves entity relationships, and generates SQL from structured query requests. With `--execute`, it runs queries and returns structured JSON envelopes designed for AI agent consumption.

## Install

```bash
bash <(curl -sSfL https://raw.githubusercontent.com/oxy-hq/airlayer/main/install_airlayer.sh)
```

Pin a version with `AIRLAYER_VERSION=v0.1.0`. Or install from source:

```bash
cargo install --path . --features exec
```

## Quick start

Given a `views/orders.view.yml`:

```yaml
name: orders
table: public.orders
dialect: postgres

dimensions:
  - name: status
    type: string
    expr: status

measures:
  - name: total_revenue
    type: sum
    expr: amount
```

Query it:

```bash
# add --execute -c config.yml to run against a database
airlayer query \
  --dimensions orders.status \
  --measures orders.total_revenue \
  --filter orders.status:equals:active \
  --limit 10
```

```sql
SELECT
  "orders".status AS "orders__status",
  SUM("orders".amount) AS "orders__total_revenue"
FROM public.orders AS "orders"
WHERE ("orders".status = 'active')
GROUP BY 1
LIMIT 10
```

## Getting started

```bash
airlayer init
```

The interactive setup walks you through:

1. **Connect** — select your database type and enter credentials
2. **Discover** — airlayer connects to your warehouse, lists databases/schemas, and lets you pick tables to model
3. **Generate** — creates `config.yml`, `views/` with `.view.yml` files, and [Claude Code](https://docs.anthropic.com/en/docs/claude-code) sub-agents and skills
4. **Enrich** *(optional)* — if Claude Code is installed, offers to review and improve the generated views (adds descriptions, detects joins, refines types)

Then talk to your data directly — `@analyst` answers questions through the semantic layer, and `@builder` creates or modifies views.

## Supported databases

Postgres, MySQL, BigQuery, Snowflake, DuckDB, MotherDuck, ClickHouse, Databricks, Redshift, SQLite, Domo.

## Development

This project uses [`just`](https://github.com/casey/just) as a task runner. Install with `cargo install just`, then run `just` to see all available recipes.

```bash
just build          # core only (no database drivers)
just build-all      # with all database drivers
just test           # tier 1: unit tests + in-process integration (DuckDB, SQLite)
just test-docker    # tier 2: starts Docker DBs + runs tests
just test-cloud     # tier 3: Snowflake, BigQuery, MotherDuck
just test-all       # all tiers
just lint           # clippy lints
just fmt            # format code
```

See [docs/testing.md](docs/testing.md) for the full three-tier testing strategy.

## Documentation

| Document | Description |
|----------|-------------|
| [PHILOSOPHY.md](PHILOSOPHY.md) | Design principles |
| [docs/schema-format.md](docs/schema-format.md) | `.view.yml` reference — dimensions, measures, entities, segments |
| [docs/query-api.md](docs/query-api.md) | Query format, filter operators, time dimensions |
| [docs/agent-execution.md](docs/agent-execution.md) | Execution envelope spec, config format |
| [docs/architecture.md](docs/architecture.md) | Pipeline stages: parse → resolve → plan → generate |
| [docs/dialects.md](docs/dialects.md) | Per-dialect SQL behavior |
| [docs/testing.md](docs/testing.md) | Three-tier testing strategy |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributing and release workflow |

## License

[Apache 2.0](LICENSE)
