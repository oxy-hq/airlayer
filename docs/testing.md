# Testing

airlayer uses a three-tier testing strategy.

## Quick reference

```bash
cargo test                                           # tier 1 only (no external deps)
cargo test --features exec                           # tier 1 + executor compilation check (211 unit tests)

# Start tier 2 databases
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored      # all tiers (tier 1 + 2 + 3)

# Tier 3 only: requires credentials in .env (see below). The positional argument
# is a substring filter on test *names*, so select warehouses by name — there is
# no test whose name contains "tier3".
cargo test --features exec -- --include-ignored snowflake bigquery databricks motherduck

# Single warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
cargo test --features exec -- --include-ignored databricks
cargo test --features exec -- --include-ignored motherduck
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

# Databricks
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_WAREHOUSE_ID=

# Credentials are read per warehouse. A warehouse with none of its variables set
# skips; one with some set and some empty is a misconfiguration. Set this to 1
# (how CI runs tier 3) to turn that misconfiguration — and any connection that
# fails with credentials present — into a failure instead of a skip. A warehouse
# you have no credentials for still skips.
# AIRLAYER_REQUIRE_CLOUD_TESTS=1
```

For BigQuery, the access token expires after ~1 hour. Refresh it with:

```bash
# macOS/Linux one-liner to update .env in place
sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token)|" .env

# Or just re-export and run inline
BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token) cargo test --features exec -- --include-ignored bigquery
```

## Tier 1: Unit + in-process tests

**211 unit tests** across `src/engine/sql_generator.rs`, `src/engine/preagg.rs`, `src/engine/join_graph.rs`, `src/schema/parser.rs`, `src/engine/profiler.rs`, and `src/executor/` cover SQL generation, pre-aggregation, and execution logic:

- Basic SELECT/FROM/GROUP BY generation
- All filter operators (equals, contains, gt, set, date ranges, etc.)
- Nested AND/OR filter compilation
- WHERE vs HAVING routing (dimension vs measure filters)
- Cross-view auto-joins
- Multi-hop transitive joins (A -> B -> C)
- Fan-out protection with CTE pre-aggregation
- Dialect-specific quoting (Postgres, MySQL, BigQuery, Databricks, Domo)
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

- **DuckDB** (12 tests): Standard query, segment, unfiltered, measure value correctness, motifs, time dimensions
- **SQLite** (7 tests): Standard query, segment, unfiltered, measure value correctness, motifs
- **Parse-validation** (4 tests): Validates generated SQL parses correctly for BigQuery, Snowflake, Databricks, Redshift
- **Pre-aggregation** (9 tests): Rollup resolution, CTAS build, manifest roundtrip, re-aggregation correctness (sum/count by platform, count_distinct, time dimension), idempotent rebuild, coverage checking

## Tier 2: Docker-based integration tests

These require running database containers and are marked `#[ignore = "tier2"]`.

### Setup

```bash
docker compose -f docker-compose.test.yml up -d
```

The compose file is at the repo root: `docker-compose.test.yml`. It starts four services:

| Service | Default port | Env var | Database | Seed script |
|---------|-------------|---------|----------|-------------|
| postgres | 15432 | `AIRLAYER_PG_PORT` | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/postgres.sql` |
| mysql | 13306 | `AIRLAYER_MYSQL_PORT` | `airlayer_test` (user: `airlayer`, pass: `airlayertest`) | `tests/integration/seed/mysql.sql` |
| clickhouse | 18123 | `AIRLAYER_CH_HTTP_PORT` | `analytics` (no auth) | `tests/integration/seed/clickhouse.sql` |
| presto | 18080 | `AIRLAYER_PRESTO_PORT` | Trino memory connector (no auth) | `tests/integration/seed/presto.sql` (seeded programmatically) |

Postgres, MySQL, and ClickHouse auto-seed on startup via init scripts mounted from `tests/integration/seed/`. Presto (Trino) uses an in-memory connector and is seeded programmatically by the test harness via the REST API — the seed SQL in `tests/integration/seed/presto.sql` is sent as statements through the executor on first test run.

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
- **Pre-aggregation / ClickHouse** (7 tests): Build rollup table, manifest roundtrip, coverage check, re-aggregation (sum/count by platform, count_distinct, time dimension), rollup data correctness
- **Presto/Trino** (9 tests): Seed, standard query, unfiltered, contribution motif, rank motif, time dimension (DATE_TRUNC), anomaly motif (STDDEV_POP), error handling, config deserialization

### Teardown

```bash
docker compose -f docker-compose.test.yml down
```

## Tier 3: Live warehouses (Snowflake, BigQuery, Databricks, MotherDuck)

These require live cloud credentials and are marked `#[ignore = "tier3"]` or `#[ignore = "tier3_motherduck"]`. Credentials are read from `.env` at the repo root (see [Credentials](#credentials-env) above).

All tier 3 tests **auto-seed** on first run — the seed SQL from `tests/integration/seed/` is executed via the test's `try_connect` + `seed` functions. You don't need to seed manually unless debugging.

**Missing credentials skip, they don't fail.** A `try_connect` with no credentials returns `None` and the test returns early, reporting `ok`. That is what you want locally; in CI it is indistinguishable from a real pass, so `AIRLAYER_REQUIRE_CLOUD_TESTS=1` tightens it. The gate is **per warehouse**, decided from that warehouse's whole credential set (`SNOWFLAKE_ACCOUNT`/`USER`/`PASSWORD`; `BIGQUERY_PROJECT`/`ACCESS_TOKEN`; `MOTHERDUCK_TOKEN`; `DATABRICKS_HOST`/`TOKEN`/`WAREHOUSE_ID`):

| That warehouse's variables | Without the flag | With `AIRLAYER_REQUIRE_CLOUD_TESTS=1` |
|---|---|---|
| all set | run; a failed login or connection skips | run; a failed login or connection **panics** (a failed query or seed already fails either way) |
| none set | skip | **skip** — an unconfigured warehouse is a legitimate state, and must not stop the configured ones from running |
| some set, some empty | skip | **panics**, naming the unset variables — a renamed secret or a token that expired out of `.env`, not an opt-out |

So the flag does not make an unconfigured warehouse fail. What it guarantees is narrower: once a warehouse's credentials are present, its tests can no longer pass by skipping.

```bash
# Reproduce CI's strictness locally, for the warehouses you have configured.
# Select tier 3 by warehouse name: the filter matches test names, and no test
# is named "tier3".
AIRLAYER_REQUIRE_CLOUD_TESTS=1 cargo test --features exec -- --include-ignored \
  snowflake bigquery databricks motherduck
```

Any value other than empty, `0`, or `false` enables it.

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
| `BIGQUERY_ACCESS_TOKEN` | OAuth2 token from `gcloud auth print-access-token` (~1hr expiry). Local dev only — CI mints a fresh token per run, see [CI](#ci-github-actions) |

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

### Databricks

Required `.env` values:

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace host (e.g., `dbc-abc123.cloud.databricks.com`) — without `https://` prefix |
| `DATABRICKS_TOKEN` | Personal access token |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID |

Seed script: `tests/integration/seed/databricks.sql` — creates `workspace.airlayer_test.events`.

View files are in `tests/integration/views-databricks/` (uses `table: workspace.airlayer_test.events`).

The Databricks executor uses the SQL Statement Execution API (`/api/2.0/sql/statements`) with inline disposition (synchronous, 30s timeout). Databricks uses backtick identifier quoting (like MySQL/BigQuery).

### Running tier 3

```bash
# Every warehouse (the filter matches test names; nothing is named "tier3")
cargo test --features exec -- --include-ignored snowflake bigquery databricks motherduck

# Only one warehouse
cargo test --features exec -- --include-ignored snowflake
cargo test --features exec -- --include-ignored bigquery
cargo test --features exec -- --include-ignored databricks
cargo test --features exec -- --include-ignored motherduck
```

### CI (GitHub Actions)

The `Tier 3: Cloud warehouses` job runs only on push to `main`, from the `cloud-tests` environment, with `AIRLAYER_REQUIRE_CLOUD_TESTS=1`.

Before the tests, the `Report - tier-3 warehouse credentials` step writes a roster to the job summary — one row per warehouse, `runs` / `skipped` / `misconfigured` — and emits a notice for each skipped warehouse and a warning for each misconfigured one. **No warehouse fails the roster for being unconfigured**: that is a legitimate state, and one warehouse must not stop another's tests from running. Two cases do fail it:

| Case | Why |
|------|-----|
| Every warehouse `skipped` | The run would contact nothing at all, and pass. |
| `BIGQUERY_SERVICE_ACCOUNT_KEY` set but no access token minted | You asked for BigQuery and did not get it. With no `BIGQUERY_PROJECT_ID` secret this is otherwise indistinguishable from an unconfigured warehouse, so it would skip in silence — `continue-on-error` on the auth step means the 403 does not stop the job by itself. Check that step's log for the underlying error. |

Both checks report before either exits, so a run with both problems names both. The roster writes its whole table and every annotation before failing, so the log still shows which warehouse was in which state. A `misconfigured` warehouse does not trip either check: it has credentials, and its own tests fail below under `AIRLAYER_REQUIRE_CLOUD_TESTS=1`.

So a green tier-3 job contacted at least one warehouse, and every warehouse whose secrets are configured either really ran or turned the job red. The roster is still what tells you *which* ones — read it rather than the job's colour when you want to know how much was covered.

Required secrets in the `cloud-tests` environment:

| Secret | Notes |
|--------|-------|
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` | Same values as `.env` |
| `BIGQUERY_PROJECT_ID` | Passed to the tests as `BIGQUERY_PROJECT`, but only when `BIGQUERY_SERVICE_ACCOUNT_KEY` is also configured — the key is what decides whether CI tests BigQuery at all, and a project without a token would read as a half-configured warehouse |
| `BIGQUERY_SERVICE_ACCOUNT_KEY` | Full JSON key of a service account with BigQuery Job User + Data Editor on the test project |
| `MOTHERDUCK_TOKEN` | Same value as `.env` |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID` | Same values as `.env` |

There is deliberately no `BIGQUERY_ACCESS_TOKEN` secret: a stored token is expired within the hour. `google-github-actions/auth@v2` mints a fresh one from `BIGQUERY_SERVICE_ACCOUNT_KEY` on every run, after the test binaries are compiled so the token's ~1h lifetime is spent on tests rather than on a cold build.

### Tests per warehouse

| Warehouse | Tests | What they verify |
|-----------|-------|-----------------|
| Snowflake | 7 | seed, standard query, unfiltered, segment, motif contribution, measure values, expr-ref joins (#55) |
| BigQuery | 10 | seed, standard query, unfiltered, motif contribution, measure values, profile (string + number), literal escaping (apostrophe, backslash, rollup reagg) |
| Databricks | 8 | seed, standard query, unfiltered, motif contribution, measure values, time dimension, error handling, config deserialization |
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
| `presto.sql` | Presto/Trino (tier 2) | Sent programmatically via REST API by test harness |
| `snowflake.sql` | Snowflake (tier 3) | Auto-run by test on first execution |
| `bigquery.sql` | BigQuery (tier 3) | Auto-run by test on first execution |
| `databricks.sql` | Databricks (tier 3) | Auto-run by test on first execution |
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
