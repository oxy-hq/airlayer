# Testing

airlayer uses a three-tier testing strategy.

## Quick reference

```bash
cargo test                                           # tier 1 only (no external deps)
cargo test --features exec                           # tier 1 + executor compilation check (136 unit tests)

# Start tier 2 databases
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored      # all tiers (tier 1 + 2 + 3)

# Tier 3 only: requires credentials in .env (see below)
cargo test --features exec -- --include-ignored tier3       # Snowflake + BigQuery
cargo test --features exec -- --include-ignored motherduck  # MotherDuck

# Single warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
```

## Credentials (.env)

Tier 3 tests load credentials from a `.env` file at the repo root via [dotenvy](https://crates.io/crates/dotenvy). This file is gitignored — never commit it.

Copy the template and fill in values:

```bash
cp .env.example .env
```

`.env.example` contains:

```
# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# BigQuery
BIGQUERY_PROJECT=
BIGQUERY_ACCESS_TOKEN=

# MotherDuck
MOTHERDUCK_TOKEN=
```

For BigQuery, the access token expires after ~1 hour. Refresh it with:

```bash
# macOS/Linux one-liner to update .env in place
sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token)|" .env

# Or just re-export and run inline
BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token) cargo test --features exec -- --include-ignored bigquery
```

## Tier 1: Unit + in-process tests

**136 unit tests** across `src/engine/sql_generator.rs`, `src/engine/join_graph.rs`, `src/schema/parser.rs`, `src/engine/profiler.rs`, and `src/executor/` cover SQL generation and execution logic:

- Basic SELECT/FROM/GROUP BY generation
- All filter operators (equals, contains, gt, set, date ranges, etc.)
- Nested AND/OR filter compilation
- WHERE vs HAVING routing (dimension vs measure filters)
- Cross-view auto-joins
- Multi-hop transitive joins (A -> B -> C)
- Fan-out protection with CTE pre-aggregation
- Dialect-specific quoting (Postgres, MySQL, BigQuery, Domo)
- Parameter placeholders per dialect
- Time dimensions with granularity
- Segments
- Custom measures
- Ungrouped mode
- Error cases (nonexistent members, empty queries)
- Count distinct approx (dialect-specific functions)
- Number (pass-through) measures
- onTheDate filter operator
- Rolling window / cumulative measures
- Measure-to-measure references ({{view.measure}})
- Subquery dimensions (correlated subqueries)
- Relative date range parsing
- Join hints (through parameter for path disambiguation)
- Geo dimension type
- Data profiling SQL generation (string/number/date/boolean dimension types)
- Cardinality-based enumeration thresholds
- Dialect-specific profiling (BigQuery FLOAT64 casting)
- Inline parameter escaping (BigQuery @p, ClickHouse $N, single-quote handling)
- Introspection result grouping and nullable parsing variants
- MotherDuck config deserialization, connection strings, token validation

**In-process integration tests** (`tests/integration_tests.rs`) run generated SQL against embedded databases:

- **DuckDB** (4 tests): Standard query, segment, unfiltered, measure value correctness
- **SQLite** (4 tests): Standard query, segment, unfiltered, measure value correctness
- **Parse-validation** (4 tests): Validates generated SQL parses correctly for BigQuery, Snowflake, Databricks, Redshift

## Tier 2: Docker-based integration tests

These require running database containers and are marked `#[ignore = "tier2"]`.

### Setup

```bash
docker compose -f docker-compose.test.yml up -d
```

The compose file is at the repo root: `docker-compose.test.yml`. It starts three services:

| Service | Default port | Env var | Database | Seed script |
|---------|-------------|---------|----------|-------------|
| postgres | 15432 | `AIRLAYER_PG_PORT` | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/postgres.sql` |
| mysql | 13306 | `AIRLAYER_MYSQL_PORT` | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/mysql.sql` |
| clickhouse | 18123 | `AIRLAYER_CH_HTTP_PORT` | `analytics` (no auth) | `tests/integration/seed/clickhouse.sql` |

Each service auto-seeds data on startup via init scripts mounted from `tests/integration/seed/`.

**Port conflicts:** If a default port is already in use, set the env var for both Docker and the tests:

```bash
AIRLAYER_PG_PORT=25432 docker compose -f docker-compose.test.yml up -d
AIRLAYER_PG_PORT=25432 cargo test --features exec -- --include-ignored
```

### Running

```bash
cargo test --features exec -- --include-ignored
```

### Tests

- **Postgres** (2 tests): Standard and unfiltered queries
- **MySQL** (1 test): Standard query
- **ClickHouse** (2 tests): Standard and unfiltered queries

### Teardown

```bash
docker compose -f docker-compose.test.yml down
```

## Tier 3: Live warehouses (Snowflake, BigQuery, MotherDuck)

These require live cloud credentials and are marked `#[ignore = "tier3"]` or `#[ignore = "tier3_motherduck"]`. Credentials are read from `.env` at the repo root (see [Credentials](#credentials-env) above).

All tier 3 tests **auto-seed** on first run — the seed SQL from `tests/integration/seed/` is executed via the test's `try_connect` + `seed` functions. You don't need to seed manually unless debugging.

### Snowflake

Required `.env` values:

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g., `jla01554`) |
| `SNOWFLAKE_USER` | Login name |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name (default: `COMPUTE_WH`) |

Seed script: `tests/integration/seed/snowflake.sql` — creates `AIRLAYER_TEST.ANALYTICS.EVENTS`.

### BigQuery

Required `.env` values:

| Variable | Description |
|----------|-------------|
| `BIGQUERY_PROJECT` | GCP project ID (currently `oxy-tech`) |
| `BIGQUERY_ACCESS_TOKEN` | OAuth2 token from `gcloud auth print-access-token` (~1hr expiry) |

Seed script: `tests/integration/seed/bigquery.sql` — creates `analytics.events` dataset/table.

The view files use `table: analytics.events`, which resolves correctly because BigQuery's default dataset is set to `analytics` in the test config.

### MotherDuck

Required `.env` values:

| Variable | Description |
|----------|-------------|
| `MOTHERDUCK_TOKEN` | MotherDuck authentication token |
| `MOTHERDUCK_DATABASE` | Database name (optional, uses default if omitted) |

Seed script: `tests/integration/seed/motherduck.sql` — creates `airlayer_test.events` schema/table.

View files are in `tests/integration/views-motherduck/` (uses `table: analytics.events`).

MotherDuck tests use a **two-connection pattern**: `try_connect_root()` opens a root connection (no database) for seeding, while `try_connect()` connects to the `airlayer_test` database for queries. This matches how MotherDuck requires database context for schema operations.

### Running tier 3

```bash
# Snowflake + BigQuery tests
cargo test --features exec -- --include-ignored tier3

# MotherDuck tests
cargo test --features exec -- --include-ignored motherduck

# Only one warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
```

### Tests per warehouse

| Warehouse | Tests | What they verify |
|-----------|-------|-----------------|
| Snowflake | 6 | seed, standard query, unfiltered, segment, motif contribution, measure values |
| BigQuery | 7 | seed, standard query, unfiltered, motif contribution, measure values, profile (string + number) |
| MotherDuck | 8 | seed, standard query, unfiltered, segment, measure values, motif contribution, motif rank, schema introspection |

## Test data

All tiers use the same 12-row `events` table with consistent values:

| Platform | Events | Revenue |
|----------|--------|---------|
| web | 7 | 164.98 |
| ios | 3 | 25.00 |
| android | 2 | 0.00 |

Test views are in `tests/integration/views/events.view.yml` (unqualified `table: events`) and `examples/multi-dialect/views/events.view.yml` (qualified `table: analytics.events`). Seed scripts for each database are in `tests/integration/seed/`:

| File | Target | Notes |
|------|--------|-------|
| `postgres.sql` | Postgres (tier 2) | Auto-mounted by docker compose |
| `mysql.sql` | MySQL (tier 2) | Auto-mounted by docker compose |
| `clickhouse.sql` | ClickHouse (tier 2) | Auto-mounted by docker compose |
| `snowflake.sql` | Snowflake (tier 3) | Auto-run by test on first execution |
| `bigquery.sql` | BigQuery (tier 3) | Auto-run by test on first execution |
| `motherduck.sql` | MotherDuck (tier 3) | Auto-run by test on first execution |
| `sqlite.sql` | SQLite (tier 1) | Loaded in-process by test |

## Manual executor testing

You can also test executors directly via the CLI:

```bash
# Create a config.yml for your database
# Then run:
cargo run --features exec -- query --execute \
  -c config.yml \
  --dimension events.platform \
  --measure events.total_events --measure events.total_revenue
```

This returns a structured JSON envelope. See [agent-execution.md](agent-execution.md) for the envelope spec.

## Adding tests

### Unit tests

Add to the `tests` module in `src/engine/sql_generator.rs`. Use `make_test_engine()` to get a pre-configured evaluator and join graph with orders/customers/products views.

### Integration tests

Add to `tests/integration_tests.rs`. Use the existing view files in `tests/integration/views/` and seed data in `tests/integration/seed/`. Follow the pattern of loading views, compiling a query, and executing against a database.
