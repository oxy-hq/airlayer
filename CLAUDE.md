# airlayer — Claude Code instructions

## What this is

airlayer is an in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL. It's a Rust crate (library + CLI binary) at `/Users/robertyi/repos/cewb/airlayer/`.

The `.view.yml` format is the same schema format used in [oxy-internal](~/repos/oxy-internal). airlayer is a standalone reimplementation — it does NOT depend on Cube.js at runtime (the `cube/` directory is reference only).

## Build & test

```bash
cargo build
cargo test                                           # tier 1 unit tests only
cargo test --features exec -- --include-ignored tier3 # tier 3 (live warehouses, needs .env)
```

Full testing guide (three tiers, credentials, seed scripts, docker compose, `.env` setup): **[docs/testing.md](docs/testing.md)**

## Project structure

```
src/
├── cli/mod.rs              CLI entry (clap). Query supports both -q JSON and shorthand flags.
├── dialect/
│   ├── mod.rs              Dialect enum (9 variants), quoting, date_trunc, tz, etc.
│   └── templates.rs        minijinja SQL templates (lightly used)
├── engine/
│   ├── mod.rs              SemanticEngine, DatasourceDialectMap, DatabaseConfig
│   ├── evaluator.rs        SchemaEvaluator — member lookups, path resolution
│   ├── join_graph.rs       petgraph-based entity relationship graph, BFS pathfinding
│   ├── member_sql.rs       {{entity.field}} and {{variables.X}} pattern resolution
│   ├── query.rs            QueryRequest, QueryFilter, FilterOperator (20 operators), OrderBy
│   ├── sql_generator.rs    Main SQL generation — SELECT/JOIN/WHERE/GROUP BY/HAVING/ORDER/LIMIT
│   └── error.rs            EngineError enum
├── executor/               Gated behind exec-* feature flags
│   ├── mod.rs              DatabaseConnection enum, QueryEnvelope, dispatch
│   ├── postgres.rs         Postgres/Redshift (postgres crate + rust_decimal)
│   ├── mysql.rs            MySQL (mysql crate)
│   ├── snowflake.rs        Snowflake REST API (ureq)
│   ├── bigquery.rs         BigQuery REST API (ureq)
│   ├── clickhouse.rs       ClickHouse HTTP API (ureq)
│   ├── databricks.rs       Databricks SQL Statement API (ureq)
│   ├── duckdb.rs           DuckDB (duckdb crate, in-process)
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
    ├── views/              Test .view.yml files
    └── seed/               Per-database seed SQL files
```

## Key design decisions

- **Dialect from datasource**: Dialect is NOT a standalone property. Each view has a `datasource` field that maps to a database config entry, which determines the SQL dialect. `DatasourceDialectMap` handles this resolution. All views in a single query must agree on dialect.
- **Entity-based auto-joins**: Primary/foreign entity declarations on views drive automatic JOIN generation. JoinGraph uses petgraph with BFS for multi-hop paths.
- **Globals inheritance**: `inherits_from: globals.semantics.dimensions.X` resolves fields from a globals YAML file. Entity inheritance merges global fields into inline entities.
- **`#[serde(untagged)]` ordering matters**: In `DimensionItem`/`MeasureItem`/`EntityItem` enums, the `Inline` variant MUST come before `Inherit` for serde to try it first.
- **EntityType defaults to Primary**: `#[serde(default)]` on `entity_type` field, with `Default` impl returning `Primary`.
- **Variable passthrough**: `{{variables.X}}` patterns are preserved in output SQL, not resolved.

## CLI conventions

- `-v` accepts files OR directories (or a mix), can be repeated
- Query input: either `-q` (JSON) or `--dimensions`/`--measures`/`--filter` flags (not both)
- Filter flag format: `member:operator:value` with comma-separated multiple values
- Dialect: `-d` flag as default/override, `-c config.yml` for datasource mapping, falls back to postgres

## Reference material

- `cube/` directory contains the full Cube.js repo for reference (don't modify)
- `~/repos/oxy-internal` has the canonical `.view.yml` format and example files
- The `cube_bridge` traits in cube's Rust code inspired the design but airlayer is standalone

## Gotchas

- Globals measures use a quirky YAML list format: `[{total_sales: null, name: "total_sales", type: "sum", ...}]`. Custom `deserialize_measures` in `globals.rs` handles this.
- `petgraph::visit::EdgeRef` must be imported to call `.target()` / `.id()` on edges.
- The `SchemaParser::parse_view_file()` method parses a single file; `parse_views()` scans a directory; `parse_directory()` does views + topics.
