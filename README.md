<p align="center">
  <img src="assets/splash.svg" alt="airlayer — semantic engine" width="100%">
</p>

# airlayer

An in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL — and optionally executes queries against real databases. Built in Rust as both a library and CLI tool.

## Quick start

```bash
bash <(curl -sSfL https://raw.githubusercontent.com/oxy-hq/airlayer/main/install_airlayer.sh)
```

Then initialize a project within an empty directory:

```bash
mkdir my-project && cd my-project
airlayer init
```

This connects to your database, discovers your schema, and generates `config.yml`, `.view.yml` files, and [Claude Code](https://docs.anthropic.com/en/docs/claude-code) sub-agents for querying and building your semantic layer.

## Example

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

You can query it with the CLI as follows:

```bash
# add --execute -c config.yml to run against a database
airlayer query \
  --dimension orders.status \
  --measure orders.total_revenue \
  --filter orders.status:equals:active \
  --limit 10
```

Which will compile to the following SQL, returned to stdout:

```sql
SELECT
  "orders".status AS "orders__status",
  SUM("orders".amount) AS "orders__total_revenue"
FROM public.orders AS "orders"
WHERE ("orders".status = 'active')
GROUP BY 1
LIMIT 10
```

## Development

This project uses [`just`](https://github.com/casey/just) as a task runner. Install with `cargo install just`, then run `just` to see all available recipes.

```bash
just build          # core only (no database drivers)
just build-all      # with all database drivers
just build-wasm     # WebAssembly package (output in pkg/)
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
| [npm package](https://www.npmjs.com/package/airlayer) | WebAssembly build for browsers and Node.js |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Contributing and release workflow |
