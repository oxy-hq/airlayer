# Testing

airlayer uses a three-tier testing strategy.

## Quick reference

```bash
cargo test                                           # tier 1 only (no external deps)
cargo test --features exec                           # tier 1 + executor compilation check

# Start tier 2 databases
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored      # tier 1 + 2

# Tier 3: requires credentials in .env (see below)
cargo test --features exec -- --include-ignored tier3
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
```

For BigQuery, the access token expires after ~1 hour. Refresh it with:

```bash
# macOS/Linux one-liner to update .env in place
sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token)|" .env

# Or just re-export and run inline
BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token) cargo test --features exec -- --include-ignored bigquery
```

## Tier 1: Unit + in-process tests

**89 unit tests** across `src/engine/sql_generator.rs`, `src/engine/join_graph.rs`, and `src/schema/parser.rs` cover SQL generation logic:

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

**In-process integration tests** (`tests/integration_tests.rs`) run generated SQL against embedded databases:

- **DuckDB** (4 tests): Standard query, filtered, unfiltered, measure value correctness
- **SQLite** (4 tests): Standard query, segment, filtered, measure value correctness
- **Parse-validation** (4 tests): Validates generated SQL parses correctly for BigQuery, Snowflake, Databricks, Redshift

## Tier 2: Docker-based integration tests

These require running database containers and are marked `#[ignore = "tier2"]`.

### Setup

```bash
docker compose -f docker-compose.test.yml up -d
```

The compose file is at the repo root: `docker-compose.test.yml`. It starts three services:

| Service | Port | Database | Seed script |
|---------|------|----------|-------------|
| postgres | 15432 | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/postgres.sql` |
| mysql | 13306 | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/mysql.sql` |
| clickhouse | 18123 | `analytics` (no auth) | `tests/integration/seed/clickhouse.sql` |

Each service auto-seeds data on startup via init scripts mounted from `tests/integration/seed/`.

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

## Tier 3: Live warehouses (Snowflake, BigQuery)

These require live cloud credentials and are marked `#[ignore = "tier3"]`. Credentials are read from `.env` at the repo root (see [Credentials](#credentials-env) above).

Both Snowflake and BigQuery tests **auto-seed** on first run — the seed SQL from `tests/integration/seed/` is executed via the test's `try_connect` + `seed` functions. You don't need to seed manually unless debugging.

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

### Running tier 3

```bash
# All tier 3 tests
cargo test --features exec -- --include-ignored tier3

# Only one warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
```

### Tests per warehouse

| Warehouse | Tests | What they verify |
|-----------|-------|-----------------|
| Snowflake | 5 | seed, standard query, unfiltered, segment, measure values |
| BigQuery | 6 | seed, standard query, unfiltered, measure values, profile (string + number) |

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
| `sqlite.sql` | SQLite (tier 1) | Loaded in-process by test |

## Manual executor testing

You can also test executors directly via the CLI:

```bash
# Create a config.yml for your database
# Then run:
cargo run --features exec -- query --execute \
  -c config.yml \
  --path tests/integration/ \
  --dimensions events.platform \
  --measures events.total_events --measures events.total_revenue
```

This returns a structured JSON envelope. See [agent-execution.md](agent-execution.md) for the envelope spec.

## Adding tests

### Unit tests

Add to the `tests` module in `src/engine/sql_generator.rs`. Use `make_test_engine()` to get a pre-configured evaluator and join graph with orders/customers/products views.

### Integration tests

Add to `tests/integration_tests.rs`. Use the existing view files in `tests/integration/views/` and seed data in `tests/integration/seed/`. Follow the pattern of loading views, compiling a query, and executing against a database.
