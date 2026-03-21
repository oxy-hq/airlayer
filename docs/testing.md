# Testing

o3 uses a two-tier testing strategy.

## Tier 1: Unit + in-process tests

**82 unit tests** across `src/engine/sql_generator.rs`, `src/engine/join_graph.rs`, and `src/schema/parser.rs` cover SQL generation logic:

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

```bash
cargo test                      # all tier 1 tests
cargo test -- --include-ignored  # include tier 2
```

## Tier 2: Docker-based integration tests

These require running database containers and are marked `#[ignore]`:

- **Postgres** (2 tests): Standard and unfiltered queries
- **MySQL** (1 test): Standard query
- **ClickHouse** (2 tests): Standard and unfiltered queries

### Running tier 2 tests

```bash
# Start databases
docker compose -f tests/docker-compose.yml up -d

# Wait for readiness, then run
cargo test -- --include-ignored
```

### Docker Compose services

| Service | Port | Database |
|---------|------|----------|
| postgres | 5433 | `testdb` |
| mysql | 3307 | `testdb` |
| clickhouse | 8124 | `default` |

Each service creates test tables and seeds data on startup via init scripts in `tests/init/`.

## Adding tests

### Unit tests

Add to the `tests` module in `src/engine/sql_generator.rs`. Use `make_test_engine()` to get a pre-configured evaluator and join graph with orders/customers/products views.

### Integration tests

Add to `tests/integration_tests.rs`. Use the existing view files in `tests/fixtures/views/` and follow the pattern of loading views, compiling a query, and executing against a database.
