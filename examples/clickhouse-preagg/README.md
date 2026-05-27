# ClickHouse Pre-aggregation Demo

A browser-based demo that compares querying 10M rows directly in ClickHouse against reading from tiny pre-aggregated Parquet files served entirely in the browser via DuckDB WASM — no warehouse round-trip needed.

Same demo as [`../snowflake-preagg`](../snowflake-preagg), but the source warehouse is a **local ClickHouse instance running in Docker** instead of Snowflake — no cloud account or credentials required.

## What this demonstrates

airlayer can pre-aggregate semantic layer queries into rollup tables, pull them as local Parquet files, and resolve queries against them at read time. This demo puts the entire cached path in the browser:

1. **airlayer WASM** compiles semantic queries to SQL and resolves them against a pre-aggregate manifest
2. **DuckDB WASM** reads the matching Parquet file (~3KB) and runs a re-aggregation query
3. The same query is also sent to ClickHouse (scanning all 10M rows) for comparison

## Architecture

```
Browser
├── airlayer WASM           Compile queries, resolve pre-agg cache
├── DuckDB WASM             Read Parquet, run re-aggregation SQL
├── Parquet files (~3KB)    Pre-aggregated rollup data
└── manifest.json           Maps queries → rollup files

Express server (localhost:3456)
├── GET /                   Query comparison page
├── GET /slider             Interactive filter + cost simulator
├── GET /public/*           Static assets (WASM, Parquet, DuckDB)
└── POST /api/query         Proxy — sends raw SQL to ClickHouse

ClickHouse (docker compose, localhost:8123)
└── ramen_demo.daily_sales  10M-row source table
```

## Dataset

10M rows of synthetic ramen shop daily sales, generated entirely inside ClickHouse from a 5,000-row × 2,000-day cross join:

- **5,000 stores** × **2,000 days** (Jul 2019 – Dec 2024)
- Columns: `store_id`, `region` (5), `store_format` (3), `city` (15), `date_key`, `daily_revenue`, `order_count`, `customer_count`, `avg_order_value`, `satisfaction_score`
- Revenue has realistic seasonal patterns (winter ramen boost) and day-of-week effects

Two pre-aggregations are defined in `views/events.view.yml`:

| Rollup | Dimensions | Rows |
|--------|-----------|------|
| `by_region_monthly` | region × month | ~300 |
| `by_format_monthly` | store_format × month | ~180 |

## Prerequisites

- Docker (for ClickHouse — no credentials needed)
- Rust toolchain with `wasm32-unknown-unknown` target
- `wasm-bindgen` CLI (`cargo install wasm-bindgen-cli`)
- Node.js and pnpm

No `.env` file required. Defaults assume ClickHouse at `http://localhost:8123` with user `default` and no password — overrideable via `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`.

## Quick start

The `seed.sh` script handles everything — starting ClickHouse, generating 10M rows, building rollups, pulling Parquet, and setting up the app:

```bash
# 1. Build the release binary and WASM from the repo root
cargo build --features exec --release
cargo build --target wasm32-unknown-unknown --no-default-features --features wasm --release
wasm-bindgen target/wasm32-unknown-unknown/release/airlayer.wasm --out-dir sdk/wasm --target web

# 2. Install app dependencies
cd examples/clickhouse-preagg/app
pnpm install
cd ..

# 3. Start ClickHouse + seed 10M rows + build + pull + setup (takes ~1 minute)
./seed.sh

# 4. Start the app
cd app
pnpm run dev
# → http://localhost:3456
```

## Step-by-step (manual)

If you want to run each step individually:

```bash
cd examples/clickhouse-preagg

# Bring up ClickHouse
docker compose up -d

# Seed 10M rows (~30-60s)
./seed.sh

# Or, if data is already seeded, just build + pull:
airlayer build --config config.yml     # Create rollup tables in ClickHouse
airlayer pull --config config.yml      # Download Parquet files to .airlayer/cache/

# Copy WASM + Parquet + DuckDB assets into app/public/
cd app
pnpm run setup

# Start the server
pnpm run dev
```

## The two demo pages

### `/` — Query comparison

Side-by-side comparison of the same query run two ways:
- **Left:** Cached path (airlayer WASM → DuckDB WASM → Parquet)
- **Right:** Raw path (SQL → ClickHouse proxy → 10M row scan)

Select "Revenue by Region" or "Revenue by Store Format" and click "Run Both Queries".

### `/slider` — Interactive filter + cost simulator

Drag a date slider to filter revenue by month. The cached side updates instantly; the raw side sends each query to ClickHouse with a loading spinner.

A cost simulation panel projects annual ClickHouse Cloud spend based on:
- Number of daily active users (adjustable slider)
- Cluster size (Small through XLarge — approximate ClickHouse Cloud $/hr)
- Observed query latency from the demo

> **Note:** the cost numbers are illustrative — ClickHouse Cloud and self-hosted pricing vary by region, replication, commitment, and data volume. They're meant to show *order-of-magnitude* differences between raw scans and pre-aggregated cache hits.

## CLI demo (no browser)

`demo.sh` runs the same comparison from the command line:

```bash
./demo.sh
```

This runs 6 steps: raw query, build, pull, cached query, format query, and `--no-cache` bypass — all with timing.

## Cleanup

```bash
./teardown.sh         # drops databases, keeps container running
./teardown.sh --all   # also stops the container (docker compose down -v)
```

## Files

```
docker-compose.yml   ClickHouse server (image: clickhouse/clickhouse-server:24-alpine)
seed.sh              Start container + seed 10M rows + build rollups + pull Parquet + setup app
demo.sh              CLI-only speed comparison (no browser)
teardown.sh          Drop ClickHouse databases + clear local cache
config.yml           Database connection + pre-aggregation schema config
views/
  events.view.yml    Semantic view definition (10 columns, 2 pre-aggregations)
app/
  server.js          Express server (static files + ClickHouse HTTP proxy)
  setup.js           Copy WASM/Parquet/DuckDB assets into public/
  index.html         Query comparison page
  slider.html        Interactive filter + cost simulator
  package.json       Dependencies (express, duckdb-wasm, idb-keyval)
```
