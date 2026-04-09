# airlayer — Claude Code instructions

## What this is

airlayer is an in-process semantic engine that compiles `.view.yml` definitions into dialect-specific SQL, and optionally executes queries against real databases. It's a Rust crate (library + CLI binary).

The `.view.yml` format is the same schema format used in [oxy-internal](~/repos/oxy-internal). airlayer is a standalone reimplementation — it does NOT depend on Cube.js at runtime (the `cube/` directory is reference only).

## Build & test

This project uses [`just`](https://github.com/casey/just) as a task runner. Install with `cargo install just`. Run `just` to see all available recipes.

```bash
just build                # core only
just build-all            # with all database drivers
just test                 # tier 1 unit tests (136 tests)
just test-exec            # tier 1 + executor compilation check
```

### Running integration tests

```bash
just test-docker          # tier 2: starts Docker DBs (auto-selects free ports) + runs tests
just db-down              # stop Docker DBs

just test-snowflake       # tier 3: Snowflake
just test-bigquery        # tier 3: BigQuery (auto-refreshes token)
just test-databricks      # tier 3: Databricks
just test-motherduck      # tier 3: MotherDuck
just test-cloud           # tier 3: all cloud warehouses

just test-all             # all tiers (Docker + cloud)
```

### Raw cargo commands (equivalent)

```bash
cargo test                                           # tier 1
cargo test --features exec                           # tier 1 + executor compilation
./scripts/test-db-up.sh                              # start Docker DBs (auto port selection)
cargo test --features exec -- --include-ignored      # tier 1 + 2 + 3
```

Full testing guide: **[docs/testing.md](docs/testing.md)**

### Current test counts (222 total)

| Category | Count | What |
|----------|-------|------|
| Unit tests | 140 | SQL generation, profiling, joins, parsing, motifs, inline_params escaping |
| Tier 1 integration | 32 | DuckDB (12), SQLite (7), parse validation (4), motif compile (4), custom motif (2), saved query (3) |
| Tier 2 integration | 21 | Postgres (5), MySQL (2), ClickHouse (5), Presto (9) — all self-seeding |
| Tier 3 integration | 29 | Snowflake (6), BigQuery (7), Databricks (8), MotherDuck (8) — all self-seeding |

## Project structure

```
src/
├── cli/mod.rs              CLI entry (clap). Query, validate, inspect subcommands.
├── dialect/
│   └── mod.rs              Dialect enum (11 variants), quoting, date_trunc, tz, etc.
├── engine/
│   ├── mod.rs              SemanticEngine, DatasourceDialectMap, DatabaseConfig
│   ├── evaluator.rs        SchemaEvaluator — member lookups, path resolution
│   ├── join_graph.rs       petgraph-based entity relationship graph, BFS pathfinding
│   ├── member_sql.rs       {{entity.field}}, {{TABLE}}, {{variables.X}} resolution + shared regex patterns
│   ├── profiler.rs         Type-aware dimension profiling (string/number/date/boolean)
│   ├── motifs.rs           Builtin motif catalog, param resolution, CTE wrapping. Also supports custom motifs via .motif.yml.
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
│   ├── presto.rs           Presto/Trino REST API (ureq, polling nextUri)
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
    ├── views-databricks/   Databricks-specific views (table: workspace.airlayer_test.events)
    ├── views-motherduck/   MotherDuck-specific views (table: analytics.events)
    └── seed/               Per-database seed SQL files (12-row events table)
.claude/
├── agents/                 Sub-agent specs (analyst, builder)
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
exec-presto     = [ureq]
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
- **Motif CTE wrapping**: Motifs compile the base query as `WITH __base AS (...)`, then add window-function columns in the outer SELECT. Complex motifs (anomaly, trend) use multi-stage CTEs (`__base → __stage1 → final`). Params of type `measure`/`dimension` auto-bind only when unambiguous (exactly one column of that kind); with multiple measures, the user must pass explicit `motif_params` using semantic member names. In multi-stage CTEs, final-stage expressions reference the `s.` alias (stage), not `b.` (base).
- **Saved queries are referenced by filepath**: Saved queries are defined as `.query.yml` files in the `queries/` directory. They support both single-step (inline query fields) and multi-step (with `steps`) formats. Saved queries are referenced by their file path (e.g., `airlayer query queries/revenue.query.yml`), not by a global name. The `name` field is a display label only. Saved queries are parsed and validated at load time; each step can be compiled to SQL independently.

## Motifs

Motifs are reusable post-aggregation analytical patterns. They wrap a base query as a CTE and add window-function columns in the outer SELECT. Use `--motif <name>` on the CLI or `"motif": "<name>"` in JSON queries.

### Builtin motifs (12)

| Motif | Output columns | Requires time dim | Description |
|-------|---------------|-------------------|-------------|
| `contribution` | `total`, `share` | No | Share of each row's measure vs total |
| `rank` | `rank` | No | RANK() ordered by measure DESC |
| `percent_of_total` | `percent_of_total` | No | 100 * measure / total |
| `anomaly` | `mean_value`, `stddev_value`, `z_score`, `is_anomaly` | No | Z-score anomaly detection (two-stage CTE, default threshold: 2) |
| `yoy` | `previous_value`, `growth_rate` | Yes (`year`) | Year-over-year via LAG(1) — granularity must be `year` |
| `qoq` | `previous_value`, `growth_rate` | Yes (`quarter`) | Quarter-over-quarter — granularity must be `quarter` |
| `mom` | `previous_value`, `growth_rate` | Yes (`month`) | Month-over-month — granularity must be `month` |
| `wow` | `previous_value`, `growth_rate` | Yes (`week`) | Week-over-week — granularity must be `week` |
| `dod` | `previous_value`, `growth_rate` | Yes (`day`) | Day-over-day — granularity must be `day` |
| `trend` | `row_n`, `slope`, `intercept`, `trend_value` | Yes | Linear regression (two-stage CTE, uses REGR_SLOPE/INTERCEPT) |
| `moving_average` | `moving_avg` | Yes | Rolling average (default 7-period window, configurable via `window` param) |
| `cumulative` | `cumulative_value` | Yes | Running SUM over time |

**PoP granularity rule:** All period-over-period motifs use `LAG(1)`, so the time dimension's `granularity` must match the motif's period. Using `yoy` with `granularity: month` compares to the previous month, not the previous year.

### CTE architecture

- **Single-stage** (most motifs): `WITH __base AS (<sql>) SELECT b.*, <outputs> FROM __base b`
- **Two-stage** (anomaly, trend): `WITH __base AS (<sql>), __stage1 AS (SELECT b.*, <intermediates> FROM __base b) SELECT s.*, <final> FROM __stage1 s`

### Custom motifs (`.motif.yml`)

Custom motifs are defined in `motifs/` directory as `.motif.yml` files. They can declare multiple `type: measure` params for different roles:

```yaml
name: ratio
description: "Ratio of two measures"
params:
  numerator:
    type: measure
  denominator:
    type: measure
outputs:
  - name: ratio
    expr: "CAST({{ numerator }} AS DOUBLE) / NULLIF({{ denominator }}, 0)"
```

Custom motifs are always single-stage. The `{{ param }}` syntax references resolved params (consistent with `{{ entity.field }}` and `{{ variables.X }}` patterns). These are resolved by airlayer's regex-based resolver (`MemberSqlResolver`), not a template engine.

### Parameter resolution

**Unambiguous auto-binding:** When a query has exactly one measure, `{{ measure }}` auto-binds to it. Same for `{{ time }}` with one time dimension. `{{ dimensions }}` always auto-binds to all dimension columns.

**Explicit params required when ambiguous:** With multiple measures, the user must specify which one via `motif_params` using semantic member names (e.g., `"motif_params": {"measure": "orders.total_revenue"}`). The CLI equivalent is `--motif-param measure=orders.total_revenue`. Member names are resolved internally to CTE column aliases.

**Defaults for non-member params:**
- `{{ threshold }}` → default `2` (anomaly z-score threshold)
- `{{ window }}` → default `6` (moving_average window size, meaning 7-period)
- Explicit `motif_params` override defaults

## Saved queries

Saved queries are reusable named queries defined as `.query.yml` files in the `queries/` directory. They support a single-step inline format for simple queries and a multi-step format for analytical workflows.

### Single-step format (`.query.yml`)

```yaml
name: revenue_by_region
description: "Revenue contribution by region"
measures: [orders.total_revenue]
dimensions: [orders.region]
motif: contribution
```

### Multi-step format (`.query.yml`)

```yaml
name: revenue_investigation
description: "Investigate revenue trends and anomalies"
steps:
  - name: trend
    description: "Get the overall trend"
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: trend

  - name: anomaly_check
    description: "Find anomalous months"
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: anomaly
```

### Key concepts

- **Single-step**: Inline query fields at the top level (no `steps` key). Compiled as a single `QueryRequest`.
- **Multi-step**: `steps` is an ordered list. Each step has a `name`, `query` (structured `QueryRequest`, same as `-q` JSON), and optional `description`.

### Validation rules

- Multi-step saved queries must have at least one step
- Step names must be unique within a saved query

## CLI conventions

- **Project root auto-detection** (project mode): `config.yml` anchors the project. All CLI commands walk up from cwd to find it, then scan for `.view.yml`, `.motif.yml`, and `.query.yml` files in the project directory (or in `views/`, `motifs/`, `queries/` subdirectories if they exist). No `--config` needed from anywhere inside the project. In library mode (Rust crate / WASM), everything is passed programmatically — no filesystem detection.
- Query input: either `-q` (JSON) or `--dimension`/`--measure`/`--filter` flags (not both)
- Filter flag format: `member:operator:value` with comma-separated multiple values
- Dialect: `-d` flag as default/override, `-c config.yml` for datasource mapping, falls back to postgres
- `--motif`: apply a post-aggregation motif (contribution, rank, anomaly, yoy, etc.)
- `--motif-param key=value`: pass motif parameters (e.g., `--motif-param measure=orders.total_revenue`). Required when query has multiple measures.
- `--execute` (`-x`): compile + run against database, returns JSON envelope
- `inspect --schema`: introspect database catalog
- `inspect --profile`: type-aware dimension profiling
- `inspect --motifs`: list all motifs (builtins + custom) with params and outputs
- `inspect --queries`: list all saved queries with steps
- `inspect --json`: machine-readable output for agent consumption
- `query <file>`: compile a saved query file (all steps to SQL), e.g. `airlayer query queries/revenue.query.yml`
- `query <file> -x`: execute a saved query file against the database

## Reference material

- `cube/` directory contains the full Cube.js repo for reference (don't modify)
- `~/repos/oxy-internal` has the canonical `.view.yml` format and example files
- The `cube_bridge` traits in cube's Rust code inspired the design but airlayer is standalone

## Keeping init artifacts in sync

When adding features to airlayer (new CLI flags, schema types, etc.), always update these files so that LLMs using the `init` output know about the feature:

1. **`INIT_CLAUDE_MD`** in `src/cli/mod.rs` — the CLAUDE.md template generated by `airlayer init`
2. **`.claude/skills/*/SKILL.md`** — the skill files embedded into the init output via `include_str!`
3. **`CLAUDE.md`** (repo root) — the development-time instructions (this file)

The init command embeds skills at compile time via `include_str!("../../.claude/skills/...")`, so changes to skill files automatically propagate to the binary.

## Workflow

- **Always run `/review` after completing any non-trivial code change** (new features, refactors, bug fixes, test additions). Address all issues found by the review without asking for confirmation — just fix them.

## Gotchas

- Globals measures use a quirky YAML list format: `[{total_sales: null, name: "total_sales", type: "sum", ...}]`. Custom `deserialize_measures` in `globals.rs` handles this.
- `petgraph::visit::EdgeRef` must be imported to call `.target()` / `.id()` on edges.
- The `SchemaParser::parse_view_file()` method parses a single file; `parse_views()` scans a directory; `parse_directory()` does views + topics.
- BigQuery access tokens expire after ~1 hour. Always refresh before running BQ tier 3 tests.
- MotherDuck tests use a two-connection pattern: `try_connect_root()` (no database, for seeding) and `try_connect()` (connects to `airlayer_test` database).
- Databricks uses backtick identifier quoting (like MySQL/BigQuery), not double-quotes. This is handled in `quote_identifier()` in `dialect/mod.rs`.
- Databricks tier 3 tests require a running SQL warehouse. The warehouse auto-stops after 10 minutes of inactivity, so first test run may take longer while the warehouse starts up.
- Introspection queries all include `LIMIT 50000` as a safety guard against very large catalogs.
