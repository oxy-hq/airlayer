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
| Unit tests | 131 | SQL generation, profiling, joins, parsing, motifs, inline_params escaping |
| Tier 1 integration | 22 | DuckDB (11), SQLite (7), parse validation (4) |
| Tier 2 integration | 9 | Postgres (4), MySQL (1), ClickHouse (4) |
| Tier 3 integration | 20 | Snowflake (6), BigQuery (7), MotherDuck (8) |

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
- **Motif CTE wrapping**: Motifs compile the base query as `WITH __base AS (...)`, then add window-function columns in the outer SELECT. Complex motifs (anomaly, trend) use multi-stage CTEs (`__base → __stage1 → final`). The `$measure`/`$time`/`$dimensions` params auto-bind to base columns; explicit `motif_params` override auto-bindings. In multi-stage CTEs, final-stage expressions reference the `s.` alias (stage), not `b.` (base).
- **Sequences are agent-driven**: Sequences define multi-step analytical workflows in `.sequence.yml` files. Steps can contain structured `QueryRequest` objects or natural-language prompts. The sequence schema is parsed and validated but execution is delegated to the analyst agent (not compiled to SQL). Sequences support parameterization, step-to-step context passing, and an optional `synthesize` block for LLM-generated summaries.

## Motifs

Motifs are reusable post-aggregation analytical patterns. They wrap a base query as a CTE and add window-function columns in the outer SELECT. Use `--motif <name>` on the CLI or `"motif": "<name>"` in JSON queries.

### Builtin motifs (12)

| Motif | Output columns | Requires time dim | Description |
|-------|---------------|-------------------|-------------|
| `yoy` | `previous_value`, `growth_rate` | Yes | Year-over-year comparison via LAG |
| `qoq` | `previous_value`, `growth_rate` | Yes | Quarter-over-quarter |
| `mom` | `previous_value`, `growth_rate` | Yes | Month-over-month |
| `wow` | `previous_value`, `growth_rate` | Yes | Week-over-week |
| `dod` | `previous_value`, `growth_rate` | Yes | Day-over-day |
| `anomaly` | `mean_value`, `stddev_value`, `z_score`, `is_anomaly` | No | Z-score anomaly detection (two-stage CTE) |
| `contribution` | `total`, `share` | No | Share of each row's measure vs total |
| `trend` | `row_n`, `slope`, `intercept`, `trend_value` | Yes | Linear regression (two-stage CTE, uses REGR_SLOPE/INTERCEPT) |
| `moving_average` | `moving_avg` | Yes | Rolling average (default 7-period window) |
| `rank` | `rank` | No | RANK() ordered by measure DESC |
| `percent_of_total` | `percent_of_total` | No | 100 * measure / total |
| `cumulative` | `cumulative_value` | Yes | Running SUM over time |

### CTE architecture

- **Single-stage** (most motifs): `WITH __base AS (<sql>) SELECT b.*, <adds> FROM __base b`
- **Two-stage** (anomaly, trend): `WITH __base AS (<sql>), __stage1 AS (SELECT b.*, <intermediates> FROM __base b) SELECT s.*, <final> FROM __stage1 s`

### Multi-measure expansion

When a query has multiple measures, motif columns are emitted per-measure with `{measure_short}__{motif_col}` naming (e.g., `total_revenue__share`, `total_orders__share`).

### Custom motifs (`.motif.yml`)

Custom motifs are defined in `motifs/` directory as `.motif.yml` files:

```yaml
name: my_custom_motif
description: "Custom analytical pattern"
params:
  measure:
    type: measure
    constraints: [numeric]
adds:
  - name: doubled
    expr: "$measure * 2"
```

Custom motifs are always single-stage. The `$param` syntax references resolved params.

### Parameter auto-binding

- `$measure` → first Measure column (prefixed with `b.` alias)
- `$time` → first TimeDimension column
- `$dimensions` → comma-separated Dimension columns
- `$threshold` → default `2` (anomaly z-score threshold)
- `$window` → default `6` (moving_average window size, meaning 7-period)
- Explicit `motif_params` in the query override auto-bindings

## Sequences

Sequences define multi-step analytical workflows as `.sequence.yml` files in the `sequences/` directory. They are parsed and validated at load time but executed by the analyst agent (not compiled to SQL directly).

### Sequence file format (`.sequence.yml`)

```yaml
name: revenue_investigation
description: "Investigate revenue trends and anomalies"
params:
  time_range:
    type: date_range
    default: ["2024-01-01", "2024-12-31"]
    description: "Period to analyze"
  metric:
    type: string
    values: ["total_revenue", "order_count"]
    default: "total_revenue"
steps:
  - name: overall_trend
    description: "Get the overall trend"
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: trend

  - name: anomaly_check
    description: "Find anomalous months"
    context: [overall_trend]      # can reference prior steps
    query:
      measures: ["orders.total_revenue"]
      time_dimensions:
        - dimension: orders.created_at
          granularity: month
      motif: anomaly

  - name: breakdown
    description: "Break down by category for anomalous periods"
    context: [overall_trend, anomaly_check]
    query: "Break down revenue by category for months flagged as anomalies"

synthesize:
  prompt: "Summarize the revenue investigation findings"
  output_format: markdown
```

### Key concepts

- **Steps** execute in order. Each step has a `name`, `query`, optional `description`, and optional `context` (list of prior step names whose results inform this step).
- **Query** can be either a structured `QueryRequest` object (same as `-q` JSON) or a natural-language string (for the analyst agent to interpret).
- **Context** references must point to prior steps only (validated as a DAG — no forward references).
- **Params** are sequence-level parameters that can be substituted into step queries.
- **Synthesize** is an optional final block that asks the LLM to produce a summary from all step results.

### Validation rules

- Sequence names must be unique across all `.sequence.yml` files
- Each sequence must have at least one step
- Step names must be unique within a sequence
- Context references must refer to earlier steps (no forward or circular references)

## CLI conventions

- `--path` accepts a base directory containing `views/` and/or `topics/` subdirectories
- Query input: either `-q` (JSON) or `--dimensions`/`--measures`/`--filter` flags (not both)
- Filter flag format: `member:operator:value` with comma-separated multiple values
- Dialect: `-d` flag as default/override, `-c config.yml` for datasource mapping, falls back to postgres
- `--motif`: apply a post-aggregation motif (contribution, rank, anomaly, yoy, etc.)
- `--execute` (`-x`): compile + run against database, returns JSON envelope
- `inspect --schema`: introspect database catalog (requires `--config`)
- `inspect --profile`: type-aware dimension profiling (requires `--config`)
- `inspect --json`: machine-readable output for agent consumption

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

## Gotchas

- Globals measures use a quirky YAML list format: `[{total_sales: null, name: "total_sales", type: "sum", ...}]`. Custom `deserialize_measures` in `globals.rs` handles this.
- `petgraph::visit::EdgeRef` must be imported to call `.target()` / `.id()` on edges.
- The `SchemaParser::parse_view_file()` method parses a single file; `parse_views()` scans a directory; `parse_directory()` does views + topics.
- BigQuery access tokens expire after ~1 hour. Always refresh before running BQ tier 3 tests.
- MotherDuck tests use a two-connection pattern: `try_connect_root()` (no database, for seeding) and `try_connect()` (connects to `airlayer_test` database).
- Introspection queries all include `LIMIT 50000` as a safety guard against very large catalogs.
