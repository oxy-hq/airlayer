# airlayer — Claude Code instructions

## What this is

airlayer is an in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL, and optionally executes queries against real databases. It's a Rust crate (library + CLI binary).

The `.view.yml` format is the same schema format used in [oxy-internal](~/repos/oxy-internal). airlayer is a standalone reimplementation — it does NOT depend on Cube.js at runtime (the `cube/` directory is reference only).

## Build & test

```bash
cargo build                                          # core only
cargo build --features exec                          # with all database drivers
cargo test                                           # tier 1 unit tests only (112 tests)
cargo test --features exec                           # tier 1 + executor compilation check
```

### Running integration tests

```bash
# Tier 2: Docker-based (Postgres, MySQL, ClickHouse)
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored      # tier 1 + 2

# Tier 3: Live warehouses (needs .env with credentials)
cargo test --features exec -- --include-ignored tier3       # Snowflake + BigQuery
cargo test --features exec -- --include-ignored motherduck  # MotherDuck

# All tiers at once
cargo test --features exec -- --include-ignored              # tier 1 + 2 + 3 + MotherDuck

# Single warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
```

### BigQuery token refresh

The BigQuery access token expires after ~1 hour. Refresh before running BQ tests:

```bash
sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token)|" .env
```

Full testing guide: **[docs/testing.md](docs/testing.md)**

### Current test counts (146 total)

| Category | Count | What |
|----------|-------|------|
| Unit tests | 112 | SQL generation, profiling, joins, parsing, inline_params escaping |
| Tier 1 integration | 12 | DuckDB (4), SQLite (4), parse validation (4) |
| Tier 2 integration | 5 | Postgres (2), MySQL (1), ClickHouse (2) |
| Tier 3 integration | 17 | Snowflake (5), BigQuery (6), MotherDuck (6) |

## Project structure

```
src/
├── cli/mod.rs              CLI entry (clap). Query, validate, inspect subcommands.
├── dialect/
│   ├── mod.rs              Dialect enum (10 variants), quoting, date_trunc, tz, etc.
│   └── templates.rs        minijinja SQL templates (lightly used)
├── engine/
│   ├── mod.rs              SemanticEngine, DatasourceDialectMap, DatabaseConfig
│   ├── evaluator.rs        SchemaEvaluator — member lookups, path resolution
│   ├── join_graph.rs       petgraph-based entity relationship graph, BFS pathfinding
│   ├── member_sql.rs       {{entity.field}} and {{variables.X}} pattern resolution
│   ├── profiler.rs         Type-aware dimension profiling (string/number/date/boolean)
│   ├── query.rs            QueryRequest, QueryFilter, FilterOperator (20 operators), OrderBy, ColumnMeta
│   ├── sql_generator.rs    Main SQL generation — SELECT/JOIN/WHERE/GROUP BY/HAVING/ORDER/LIMIT
│   └── error.rs            EngineError enum
├── executor/               Gated behind exec-* feature flags
│   ├── mod.rs              DatabaseConnection enum, QueryEnvelope, ExecutionConfig, dispatch
│   ├── introspect.rs       Schema introspection (tables/columns/types from information_schema)
│   ├── postgres.rs         Postgres/Redshift (postgres crate + rust_decimal)
│   ├── mysql.rs            MySQL (mysql crate)
│   ├── snowflake.rs        Snowflake REST API (ureq, session-based auth)
│   ├── bigquery.rs         BigQuery REST API (ureq, OAuth2 token)
│   ├── clickhouse.rs       ClickHouse HTTP API (ureq, JSONCompact format)
│   ├── databricks.rs       Databricks SQL Statement API (ureq)
│   ├── duckdb.rs           DuckDB (duckdb crate, in-process). Shared helpers: rewrite_params, duckdb_value_to_json
│   ├── motherduck.rs       MotherDuck (duckdb crate, md: protocol). Reuses duckdb.rs helpers via pub(crate)
│   ├── sqlite.rs           SQLite (rusqlite crate, in-process)
│   └── domo.rs             Domo REST API (ureq)
├── schema/
│   ├── models.rs           Core types: View, Dimension, Measure, Entity, SemanticLayer, etc.
│   ├── parser.rs           YAML parser for .view.yml, handles globals inheritance
│   ├── validator.rs        Schema validation rules
│   └── globals.rs          Globals file parsing (custom measure deserialization)
├── lib.rs                  Public re-exports
└── main.rs                 CLI main()
tests/
├── integration_tests.rs    All integration tests (tier 1-3)
└── integration/
    ├── views/              Test .view.yml files (unqualified table names)
    ├── views-motherduck/   MotherDuck-specific views (table: analytics.events)
    └── seed/               Per-database seed SQL files (12-row events table)
.claude/
└── skills/                 Claude Code agent skills (bootstrap, query, profile)
examples/
└── bootstrapping/          End-to-end bootstrapping workflow example
```

## Feature flags

```
exec-postgres   = [postgres, rust_decimal]
exec-mysql      = [mysql]
exec-snowflake  = [ureq]
exec-bigquery   = [ureq]
exec-clickhouse = [ureq]
exec-databricks = [ureq]
exec-duckdb     = [duckdb]
exec-sqlite     = [rusqlite]
exec-domo       = [ureq]
exec-motherduck = [duckdb, exec-duckdb]   # ← depends on exec-duckdb for shared helpers
exec            = all of the above
```

## Key design decisions

- **Dialect from datasource**: Dialect is NOT a standalone property. Each view has a `datasource` field that maps to a database config entry, which determines the SQL dialect. `DatasourceDialectMap` handles this resolution. All views in a single query must agree on dialect.
- **Entity-based auto-joins**: Primary/foreign entity declarations on views drive automatic JOIN generation. JoinGraph uses petgraph with BFS for multi-hop paths.
- **Globals inheritance**: `inherits_from: globals.semantics.dimensions.X` resolves fields from a globals YAML file. Entity inheritance merges global fields into inline entities.
- **`#[serde(untagged)]` ordering matters**: In `DimensionItem`/`MeasureItem`/`EntityItem` enums, the `Inline` variant MUST come before `Inherit` for serde to try it first.
- **EntityType defaults to Primary**: `#[serde(default)]` on `entity_type` field, with `Default` impl returning `Primary`.
- **Variable passthrough**: `{{variables.X}}` patterns are preserved in output SQL, not resolved.
- **MotherDuck shares DuckDB internals**: `motherduck.rs` reuses `duckdb::rewrite_params()` and `duckdb::duckdb_value_to_json()` via `pub(crate)`. The `exec-motherduck` feature MUST depend on `exec-duckdb`.
- **Envelope-driven execution**: `--execute` always returns a `QueryEnvelope` JSON — even on errors. The `run_execute` inner function returns `Result<QueryEnvelope, QueryEnvelope>` so all error paths produce valid envelopes.
- **SQL param escaping**: All `inline_params` functions escape `'` as `''` (SQL standard doubled-quote). Never use `\'` (non-standard backslash).

## CLI conventions

- `--path` accepts a base directory containing `views/` and/or `topics/` subdirectories
- Query input: either `-q` (JSON) or `--dimensions`/`--measures`/`--filter` flags (not both)
- Filter flag format: `member:operator:value` with comma-separated multiple values
- Dialect: `-d` flag as default/override, `-c config.yml` for datasource mapping, falls back to postgres
- `--execute` (`-x`): compile + run against database, returns JSON envelope
- `inspect --schema`: introspect database catalog (requires `--config`)
- `inspect --profile`: type-aware dimension profiling (requires `--config`)
- `inspect --json`: machine-readable output for agent consumption

## Reference material

- `cube/` directory contains the full Cube.js repo for reference (don't modify)
- `~/repos/oxy-internal` has the canonical `.view.yml` format and example files
- The `cube_bridge` traits in cube's Rust code inspired the design but airlayer is standalone

## Gotchas

- Globals measures use a quirky YAML list format: `[{total_sales: null, name: "total_sales", type: "sum", ...}]`. Custom `deserialize_measures` in `globals.rs` handles this.
- `petgraph::visit::EdgeRef` must be imported to call `.target()` / `.id()` on edges.
- The `SchemaParser::parse_view_file()` method parses a single file; `parse_views()` scans a directory; `parse_directory()` does views + topics.
- BigQuery access tokens expire after ~1 hour. Always refresh before running BQ tier 3 tests.
- MotherDuck tests use a two-connection pattern: `try_connect_root()` (no database, for seeding) and `try_connect()` (connects to `airlayer_test` database).
- Introspection queries all include `LIMIT 50000` as a safety guard against very large catalogs.
