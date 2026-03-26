# Testing

airlayer uses a three-tier testing strategy.

## Quick reference

```bash
cargo test                                  # tier 1 only (no external deps)
cargo test --features exec                  # tier 1 + executor compilation check

# Start tier 2 databases
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored   # tier 1 + 2

# Tier 3 requires live Snowflake credentials (see below)
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

## Tier 3: Snowflake (live warehouse)

These require live Snowflake credentials and are marked `#[ignore = "tier3"]`.

### Setup

Set environment variables:

```bash
export SNOWFLAKE_ACCOUNT=<account>
export SNOWFLAKE_USER=<username>
export SNOWFLAKE_PASSWORD=<password>
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH  # optional, defaults to COMPUTE_WH
```

Seed the test database (one-time):

```bash
# Run tests/integration/seed/snowflake.sql against your Snowflake instance
# This creates AIRLAYER_TEST.ANALYTICS schema with events table
```

### Running

```bash
cargo test --features exec -- --include-ignored tier3
```

## Test data

All tiers use the same 12-row `events` table with consistent values:

| Platform | Events | Revenue |
|----------|--------|---------|
| web | 7 | 164.98 |
| ios | 3 | 25.00 |
| android | 2 | 0.00 |

Test views are in `tests/integration/views/events.view.yml`. Seed scripts for each database are in `tests/integration/seed/`.

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
