# airlayer Integration Tests

## Philosophy

airlayer generates SQL for 10 dialects, but only a few have freely available database engines. The test strategy uses three tiers to balance coverage with practicality.

### Tier 1 — In-process (no external dependencies)

These tests run in CI and locally with zero setup:

- **DuckDB & SQLite**: Spin up in-memory databases, seed them with test data, generate SQL via airlayer, execute it, and assert on actual query results. This validates the full pipeline — YAML parsing, join graph resolution, SQL generation, and correctness of the output.

- **Parse validation (BigQuery, Databricks, Redshift)**: For cloud-only dialects where we can't run a real instance, we normalize the generated SQL (e.g. backtick-quoting to double-quoting, dialect-specific parameter placeholders to `$N`) and attempt `EXPLAIN` in DuckDB. This catches syntax errors and malformed SQL without requiring cloud credentials.

Run tier 1 tests:
```bash
cargo test --test integration_tests -- --include-ignored duckdb sqlite parse
```

### Tier 2 — Docker (Postgres, MySQL, ClickHouse)

These require `docker-compose.test.yml` to be running:

```bash
docker compose -f docker-compose.test.yml up -d
cargo test --test integration_tests -- --include-ignored postgres mysql clickhouse
docker compose -f docker-compose.test.yml down
```

Each service mounts its seed SQL from `tests/integration/seed/` via Docker's `docker-entrypoint-initdb.d`. Tests use a `try_connect()` pattern — if the database isn't reachable, the test is skipped rather than failed, so tier 2 tests are safe to run anywhere.

### Tier 3 — Cloud warehouses (Snowflake, BigQuery, MotherDuck)

These require live credentials and incur costs. Tests authenticate via the Snowflake session REST API (`/session/v1/login-request`), seed data idempotently on first run, then execute compiled queries against the live warehouse.

**Setup:**

```bash
export SNOWFLAKE_ACCOUNT=jla01554
export SNOWFLAKE_USER=ryi
export SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD_JLA01554
# optional: SNOWFLAKE_WAREHOUSE (defaults to COMPUTE_WH)
```

**Run:**

```bash
cargo test --test integration_tests -- --include-ignored snowflake
```

**How it works:**

1. `try_connect()` reads env vars and calls `/session/v1/login-request` to get a session token. If credentials aren't set or login fails, tests skip gracefully.
2. `seed()` runs once per test session (via `std::sync::Once`) — creates `AIRLAYER_TEST.ANALYTICS.EVENTS` with 12 rows of event data. Uses `CREATE OR REPLACE TABLE` so it's idempotent.
3. Each test compiles a query via `SemanticEngine`, then executes the SQL against Snowflake via `/queries/v1/query-request` with the session token.
4. Assertions check row counts and exact measure values.

**Snowflake identifier quoting:** Snowflake stores unquoted identifiers as UPPERCASE. The `quote_identifier` method for the Snowflake dialect uppercases names (e.g. `platform` → `"PLATFORM"`) so quoted refs match the default convention. Seed SQL uses unquoted column names (stored uppercase), and user-written expressions (e.g. `revenue_cents / 100.0`) are also resolved as uppercase by Snowflake. This means the seed table, generated SQL, and inline expressions all agree on case.

**Tests:**

| Test | What it validates |
|------|-------------------|
| `snowflake_seed` | Seeding works, table has 12 rows |
| `snowflake_standard_query` | Filtered query (web platform), uses `analytics.events` table |
| `snowflake_unfiltered_query` | Groups by platform, expects 3 rows |
| `snowflake_segment_query` | `web_only` segment filter (uses integration views) |
| `snowflake_measure_values_correct` | Exact values: 12 total events, 4 purchases |

## Test data

All tiers share the same logical dataset (product analytics events) defined in `tests/integration/seed/`:

| File | Engine | Notes |
|------|--------|-------|
| `sqlite.sql` | SQLite | 12 events, flat table |
| `workforce_assignments.csv` | DuckDB | CSV loaded via `read_csv_auto` |
| `postgres.sql` | Postgres | `analytics` schema, multi-table |
| `mysql.sql` | MySQL | Flat table, no schema prefix |
| `clickhouse.sql` | ClickHouse | `analytics` database, MergeTree engine |
| `snowflake.sql` | Snowflake | `AIRLAYER_TEST.ANALYTICS` database/schema, unquoted (uppercase) columns |

## View definitions

`tests/integration/views/events.view.yml` is the primary test view — a simple events table with dimensions, measures, and a `web_only` segment. It uses no schema prefix so it works across SQLite and DuckDB without modification.

`examples/multi-dialect/views/events.view.yml` is the schema-qualified variant (`analytics.events`) used by Postgres, ClickHouse, and Snowflake tier 2/3 tests. It does not define segments.

## Adding tests

1. Add seed data to the appropriate file in `tests/integration/seed/`
2. Add or modify view definitions in `tests/integration/views/`
3. Write tests in `tests/integration_tests.rs` using the helper functions (`load_engine`, `standard_query`, etc.)
4. Mark with `#[ignore = "tier1"]`, `#[ignore = "tier2"]`, or `#[ignore = "tier3"]` as appropriate
5. For cloud warehouses (tier 3): use the `try_connect()` skip pattern so tests pass gracefully without credentials
