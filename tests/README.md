# airlayer Integration Tests

## Philosophy

airlayer generates SQL for 9 dialects, but only a few have freely available database engines. The test strategy uses two tiers to balance coverage with practicality.

### Tier 1 — In-process (no external dependencies)

These tests run in CI and locally with zero setup:

- **DuckDB & SQLite**: Spin up in-memory databases, seed them with test data, generate SQL via airlayer, execute it, and assert on actual query results. This validates the full pipeline — YAML parsing, join graph resolution, SQL generation, and correctness of the output.

- **Parse validation (Snowflake, BigQuery, Databricks, Redshift)**: For cloud-only dialects where we can't run a real instance, we normalize the generated SQL (e.g. backtick-quoting to double-quoting, dialect-specific parameter placeholders to `$N`) and attempt `EXPLAIN` in DuckDB. This catches syntax errors and malformed SQL without requiring cloud credentials.

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

## Test data

All tiers share the same logical dataset (product analytics events) defined in `tests/integration/seed/`:

| File | Engine | Notes |
|------|--------|-------|
| `sqlite.sql` | SQLite | 12 events, flat table |
| `workforce_assignments.csv` | DuckDB | CSV loaded via `read_csv_auto` |
| `postgres.sql` | Postgres | `analytics` schema, multi-table |
| `mysql.sql` | MySQL | Flat table, no schema prefix |
| `clickhouse.sql` | ClickHouse | `analytics` database, MergeTree engine |

## View definitions

`tests/integration/views/events.view.yml` is the primary test view — a simple events table with dimensions, measures, and a `web_only` segment. It uses no schema prefix so it works across SQLite and DuckDB without modification.

## Adding tests

1. Add seed data to the appropriate file in `tests/integration/seed/`
2. Add or modify view definitions in `tests/integration/views/`
3. Write tests in `tests/integration_tests.rs` using the helper functions (`load_engine`, `standard_query`, etc.)
4. Mark with `#[ignore = "tier1"]` or `#[ignore = "tier2"]` as appropriate
