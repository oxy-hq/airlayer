# Pre-Aggregation

Pre-aggregation materializes rollup tables from your views, dramatically reducing query times by scanning thousands of rows instead of millions.

## How it works

1. **Build** — `airlayer build` reads `pre_aggregations` from your `.view.yml` files and creates rollup tables in a dedicated schema (default: `AIRLAYER`) in your warehouse.
2. **Pull** — `airlayer pull` downloads those rollup tables to local Parquet files in `.airlayer/cache/`.
3. **Query** — `airlayer query -x` automatically checks for a matching rollup before hitting the raw table. Resolution order:
   1. Local Parquet cache (via DuckDB, instant)
   2. Warehouse rollup tables (via the original database connection)
   3. Raw SQL against the source table (fallback)

Use `--no-cache` to bypass both cache layers and always hit the raw table.

## Defining rollups

Pre-aggregation is **opt-in**. A view without a `pre_aggregations` block produces
no rollups and is skipped by `build` — there is no implicit default rollup. (An
all-dimensions rollup on a wide view is typically as large as the base table and
buys nothing, so the choice of grain is left to you.) If no view in the project
declares a block, `build` exits with an error saying so.

Add a `pre_aggregations` section to any `.view.yml` file:

```yaml
name: events
table: events
datasource: warehouse

dimensions:
  - name: platform
    type: string
    expr: platform
  - name: country
    type: string
    expr: country
  - name: created_at
    type: datetime
    expr: created_at

measures:
  - name: event_count
    type: count
  - name: total_revenue
    type: sum
    expr: revenue_cents / 100.0
  - name: avg_revenue
    type: average
    expr: revenue_cents / 100.0

pre_aggregations:
  - name: by_platform_daily
    dimensions: [platform]
    measures: [event_count, total_revenue, avg_revenue]
    time_dimension: created_at
    granularity: day

  - name: by_country_monthly
    dimensions: [country]
    measures: [event_count, total_revenue]
    time_dimension: created_at
    granularity: month
```

### Rollup fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique rollup name within the view |
| `dimensions` | string[] | No | Dimensions to GROUP BY in the rollup |
| `measures` | string[] | No | Measures to include (omitted = none) |
| `time_dimension` | string | No | Time dimension for date-based grouping |
| `granularity` | string | No | Time granularity: `day`, `week`, `month`, `quarter`, `year` |

### Omitted fields

`measures` and `dimensions` are both listed literally — there is no "all of them"
shorthand. Omitting `measures` produces a rollup with no measures (it covers no
measure query, so it is almost never what you want). Omitting `dimensions` rolls
the measures up over the time dimension alone.

### Eligible measure types

Not all measure types can be pre-aggregated:

| Type | Pre-aggregable | Rollup columns |
|------|---------------|----------------|
| `sum` | Yes | `measure__sum` |
| `count` | Yes | `measure__count` |
| `avg` / `average` | Yes | `measure__sum` + `measure__count` (recomputed as sum/count) |
| `min` | Yes | `measure__min` |
| `max` | Yes | `measure__max` |
| `count_distinct` | Yes | Raw expression column (re-aggregated with GROUP BY) |
| `median` | No | Requires the full dataset — a rollup never serves it |
| `number` | No | Pass-through expressions can't be re-aggregated |
| `custom` | No | Arbitrary expressions can't be re-aggregated |

Listing a non-pre-aggregable measure in `measures:` is not an error, but the
rollup will never satisfy a query for it — coverage checking rejects `median`,
`number`, and `custom` and falls back to raw SQL. Leave them out.

If a query references a non-pre-aggregable measure, it falls through to raw SQL.

## Config

Add a `pre_aggregations` section to your `config.yml`:

```yaml
databases:
  - name: warehouse
    type: duckdb
    path: ./data/warehouse.duckdb

pre_aggregations:
  schema: preagg          # schema/dataset name for rollup tables (default: AIRLAYER)
  database: warehouse     # which database to build into (default: first in config)
```

## CLI commands

### `airlayer build`

Creates rollup tables in the warehouse.

```bash
airlayer build --config config.yml                    # build all views
airlayer build --config config.yml --view events      # build one view
airlayer build --config config.yml --dry-run           # print SQL without executing
airlayer build --config config.yml --schema my_schema  # custom schema name
```

For each rollup, `build` creates:
- A schema (if it doesn't exist): `CREATE SCHEMA IF NOT EXISTS "preagg"`
- A manifest table: `"preagg"."__manifest"` tracking all rollups
- A rollup table via CTAS: `"preagg"."events__by_platform_daily__abc123__20260415"`

After all new tables are created and the manifest is updated, `build` automatically drops old rollup tables that were replaced. This prevents stale tables from accumulating across rebuilds. Cleanup only runs after the new table and manifest upsert succeed, so there is no downtime.

The same pass prunes **orphaned** rollups: a manifest row for a view in the build's scope whose rollup the view no longer declares (renamed, deleted, or a `default` rollup left over from a version before pre-aggregation became opt-in). Its table is dropped and its manifest row deleted — otherwise it would keep serving data frozen at its last build date that no `build` could refresh. Rollups skipped as fresh, and rollups belonging to views outside the build's scope (e.g. under `--view`), are never pruned. Pruned rollups are listed in `BuildPlan.pruned` and in the `pruned` array of `build`'s JSON summary.

The manifest stores metadata (view name, rollup name, hash, columns, build date) so that `pull` and `query` can discover available rollups.

### `airlayer pull`

Downloads rollup data from the warehouse to local Parquet files.

```bash
airlayer pull --config config.yml                     # pull all rollups
airlayer pull --config config.yml --view events       # pull one view
```

Pull reads the `__manifest` table, queries each rollup table, and writes the results to `.airlayer/cache/` as Parquet files alongside a `manifest.json` index.

### `airlayer query -x --no-cache`

Bypass all pre-aggregation layers:

```bash
airlayer query -x --config config.yml --no-cache \
  --dimension events.platform \
  --measure events.total_revenue
```

## Coverage resolution

When a query is executed (`-x`), airlayer checks whether any available rollup "covers" the query:

- All requested dimensions must be present in the rollup's dimensions
- All requested measures must be present and pre-aggregable
- If the query has filters, the filter dimensions must be in the rollup
- Time dimensions are covered if the rollup's granularity is equal to or finer than the requested granularity (e.g., a `day` rollup covers a `month` query via re-truncation)

If coverage is found, airlayer generates a re-aggregation query against the rollup instead of the raw table:

- SUM columns are re-aggregated with `SUM()`
- COUNT columns are re-aggregated with `SUM()` (summing pre-counted values)
- AVG is recomputed as `SUM(measure__sum) / SUM(measure__count)`, with dialect-appropriate casting
- MIN/MAX are re-aggregated with `MIN()`/`MAX()`
- COUNT_DISTINCT columns are re-aggregated with `COUNT(DISTINCT ...)`
- Time dimensions at coarser granularity use dialect-specific `DATE_TRUNC` to re-truncate

## Dialect support

Pre-aggregation generates dialect-aware SQL for all 11 supported databases:

| Dialect | Identifier quoting | Date truncation | Schema DDL |
|---------|-------------------|-----------------|------------|
| Postgres | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| MySQL | `` `col` `` | `DATE_FORMAT(col, '%Y-%m-01')` | `CREATE SCHEMA IF NOT EXISTS` |
| BigQuery | `` `col` `` | `TIMESTAMP_TRUNC(col, MONTH)` | None (datasets created externally) |
| Snowflake | `"COL"` (uppercase) | `DATE_TRUNC('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| DuckDB | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| ClickHouse | `"col"` | `toStartOfMonth(col)` | `CREATE DATABASE IF NOT EXISTS` |
| Databricks | `` `col` `` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| Redshift | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| SQLite | `"col"` | `date_trunc('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |
| Domo | `` `col` `` | `DATE_FORMAT(col, '%Y-%m-01')` | `CREATE SCHEMA IF NOT EXISTS` |
| Presto | `"col"` | `DATE_TRUNC('month', col)` | `CREATE SCHEMA IF NOT EXISTS` |

### ClickHouse specifics

- Uses `ReplacingMergeTree` for the manifest table (deduplication by `rollup_hash`)
- Uses `MergeTree` for rollup tables
- Manifest upsert is a plain `INSERT INTO` (ReplacingMergeTree handles dedup)
- Queries use `FINAL` when reading the manifest to get deduplicated results

### BigQuery specifics

- No `PRIMARY KEY` in DDL
- Uses `FLOAT64` for double-precision casts
- Uses `STRING` type for text columns in manifest
- Datasets must be created externally before building

### Snowflake specifics

- All identifiers are uppercased when quoted (e.g., `"PLATFORM"`, `"__MANIFEST"`)

## Library API

All pre-aggregation logic is available as pure functions in `airlayer::engine::preagg` for use as a library (e.g., from oxy-internal). These functions perform no I/O — the caller handles database execution.

### Query resolution

```rust
use airlayer::engine::preagg::{self, PreaggResolution};

// Layer 1: local Parquet cache
if let Some(PreaggResolution::LocalParquet { reagg_sql, parquet_path }) =
    preagg::resolve_local(&request, &local_manifest, &cache_dir)
{
    // Execute reagg_sql against in-memory DuckDB
}

// Layer 2: warehouse rollup tables
let manifest_sql = preagg::manifest_query_sql(&schema, &dialect);
// ... execute manifest_sql, get rows ...
let entries = preagg::parse_manifest_rows(&rows);
if let Some(PreaggResolution::WarehouseRollup { reagg_sql, table_name }) =
    preagg::resolve_warehouse(&request, &entries, &schema, &dialect)
{
    // Execute reagg_sql against the warehouse
}
```

### Build planning

```rust
use airlayer::engine::preagg;

let plan = preagg::collect_build_sql(&views, &schema, &date_str, &dialect);
for stmt in &plan.statements {
    // Execute each statement sequentially
}
// plan.manifest_entries contains metadata for reporting
```

### Key types

| Type | Description |
|------|-------------|
| `PreaggResolution` | Enum: `LocalParquet { reagg_sql, parquet_path }` or `WarehouseRollup { reagg_sql, table_name }` |
| `CachedResolution` | Struct: `reagg_sql` (FROM `"__cache"`), `cache_key`, `entry` — filesystem-independent variant |
| `WarehouseRollupEntry` | A rollup entry from the warehouse `__manifest` table |
| `BuildPlan` | All SQL statements + manifest entries for a build operation, plus `skipped` (fresh) and `pruned` (no longer declared) |
| `LocalManifest` | The local `manifest.json` structure (from `pull`) |

## WASM / Browser cache API

The WASM module exposes pre-aggregation cache functions for browser use. The Rust side handles pure computation (coverage checking, SQL generation); the JavaScript caller handles I/O (IndexedDB storage, duckdb-wasm execution).

### Functions

| Function | Description |
|----------|-------------|
| `cache_resolve(manifest_json, query_json)` | Check if a cached rollup covers a query. Returns `{ reagg_sql, cache_key, entry }` or `null`. |
| `cache_build_manifest(rows_json, source_database)` | Parse warehouse manifest rows into a `LocalManifest` JSON string for IndexedDB storage. |
| `cache_key(view_name, rollup_hash)` | Get the IndexedDB key for a rollup (e.g., `"events__a1b2c3d4"`). |
| `cache_resolve_warehouse(rows_json, query_json, schema, dialect)` | Resolve against warehouse rollup entries. Returns `{ reagg_sql, table_name }` or `null`. |

### Typical browser flow

```javascript
import init, { cache_build_manifest, cache_resolve, cache_key } from './airlayer_bg.wasm';

// 1. Fetch manifest rows from warehouse and build local manifest
const manifestJson = cache_build_manifest(JSON.stringify(warehouseRows), "my_warehouse");

// 2. Store manifest + rollup data in IndexedDB
await idb.put('manifest', manifestJson);
for (const entry of warehouseRows) {
  const key = cache_key(entry.view_name, entry.rollup_hash);
  const data = await fetchRollupData(entry.table_name);
  await idb.put(key, data);
}

// 3. On query, check cache coverage
const resolution = cache_resolve(manifestJson, JSON.stringify(query));
if (resolution) {
  // Load cached data into duckdb-wasm table named "__cache"
  const data = await idb.get(resolution.cache_key);
  await duckdb.exec(`CREATE TABLE "__cache" AS SELECT * FROM '${dataUrl}'`);
  const result = await duckdb.exec(resolution.reagg_sql);
  await duckdb.exec('DROP TABLE "__cache"');
}
```

The `reagg_sql` reads from a table named `"__cache"` — the JS caller must create this table in duckdb-wasm with the cached rollup data before executing.

## Example

The `examples/pre-aggregation/` directory contains a complete working demo with a 500M-row DuckDB database:

```bash
cd examples/pre-aggregation
./demo.sh     # seeds data, builds, pulls, and queries
```

The demo shows the speedup from scanning ~1,000 cached rows instead of 500,000,000 raw rows.
