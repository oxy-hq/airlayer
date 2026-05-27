#!/usr/bin/env bash
# Seed local ClickHouse (in Docker) with 10M ramen shop daily sales records.
#
# Generates a denormalized daily_sales table with store attributes baked in,
# modeled after the da-bench-internal San Mateo ramen shop dataset.
#
# 5,000 stores × 2,000 days = 10,000,000 rows
#
# Requires: docker compose (or docker-compose)
#
# Usage: ./seed.sh
(
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"

CH_HOST="http://localhost:8123"

# ── Start ClickHouse if not already running ──────────────────────────────
if ! curl -fs "${CH_HOST}/ping" > /dev/null 2>&1; then
    echo "Starting ClickHouse via docker compose..."
    docker compose up -d
    echo -n "Waiting for ClickHouse to become healthy"
    for _ in $(seq 1 60); do
        if curl -fs "${CH_HOST}/ping" > /dev/null 2>&1; then
            echo " ready."
            break
        fi
        echo -n "."
        sleep 1
    done
    if ! curl -fs "${CH_HOST}/ping" > /dev/null 2>&1; then
        echo "" >&2
        echo "ClickHouse did not become healthy in 60s." >&2
        echo "Check logs with: docker compose logs clickhouse" >&2
        exit 1
    fi
else
    echo "ClickHouse already running at ${CH_HOST}."
fi

run_sql() {
    local sql="$1"
    local desc="${2:-}"
    [ -n "$desc" ] && echo "  $desc"
    if ! curl -fs "${CH_HOST}/" --data-binary "$sql" > /dev/null; then
        echo "SQL failed:" >&2
        echo "  $sql" >&2
        echo "" >&2
        curl -s "${CH_HOST}/" --data-binary "$sql" >&2 || true
        return 1
    fi
}

echo ""
echo "Seeding 10M ramen shop daily sales into ramen_demo.daily_sales ..."
echo ""

run_sql "DROP DATABASE IF EXISTS ramen_demo"                    "Dropping any previous ramen_demo database..."
run_sql "DROP DATABASE IF EXISTS preagg_rollups"                "Dropping any previous preagg_rollups database..."
run_sql "CREATE DATABASE ramen_demo"                            "Creating database ramen_demo..."

# Build a 10M-row table via cross join of 5K stores × 2K days.
# ClickHouse note: numbers(N) yields rows with column 'number' UInt64.
# Date math uses addDays(date, n) and date_add for granularity ops.
run_sql "
CREATE TABLE ramen_demo.daily_sales
ENGINE = MergeTree()
ORDER BY (date_key, region, store_format)
AS
WITH stores AS (
    SELECT
        number + 1 AS store_id,
        multiIf(
            number % 5 = 0, 'northeast',
            number % 5 = 1, 'southeast',
            number % 5 = 2, 'midwest',
            number % 5 = 3, 'southwest',
            'west'
        ) AS region,
        multiIf(
            number % 3 = 0, 'full_service',
            number % 3 = 1, 'fast_casual',
            'counter_service'
        ) AS store_format,
        arrayElement(
            ['San Mateo','Brooklyn','Austin','Portland','Chicago','Denver',
             'Seattle','Miami','Nashville','Boston','Phoenix','Atlanta',
             'Minneapolis','Dallas','San Diego'],
            (number % 15) + 1
        ) AS city,
        multiIf(
            number % 3 = 0, toFloat64(450),
            number % 3 = 1, toFloat64(350),
            toFloat64(250)
        ) AS base_revenue,
        multiIf(
            number % 3 = 0, 60,
            number % 3 = 1, 40,
            20
        ) AS seating_capacity
    FROM numbers(5000)
),
days AS (
    SELECT
        addDays(toDate('2019-07-01'), toInt32(number)) AS date_key
    FROM numbers(2000)
)
SELECT
    s.store_id,
    s.region,
    s.store_format,
    s.city,
    d.date_key,
    round(
        s.base_revenue
        * arrayElement(
            [1.15, 1.10, 1.00, 0.90, 0.85, 0.80,
             0.78, 0.80, 0.90, 1.00, 1.10, 1.20],
            toMonth(d.date_key)
        )
        * arrayElement(
            -- toDayOfWeek: Mon=1 .. Sun=7
            [0.75, 0.80, 0.85, 0.95, 1.15, 1.20, 0.85],
            toDayOfWeek(d.date_key)
        )
        * (0.8 + (rand(d.date_key + s.store_id) / 4294967295.0) * 0.4)
    , 2) AS daily_revenue,
    toInt32(
        floor(
            (s.seating_capacity * 0.6)
            * multiIf(
                toDayOfWeek(d.date_key) = 5, 1.15,
                toDayOfWeek(d.date_key) = 6, 1.20,
                toDayOfWeek(d.date_key) = 1, 0.75,
                1.0
              )
            * (0.7 + (rand(d.date_key + s.store_id + 1) / 4294967295.0) * 0.6)
        )
    ) AS order_count,
    toInt32(
        floor(
            (s.seating_capacity * 0.5)
            * multiIf(
                toDayOfWeek(d.date_key) = 5, 1.15,
                toDayOfWeek(d.date_key) = 6, 1.20,
                toDayOfWeek(d.date_key) = 1, 0.75,
                1.0
              )
            * (0.7 + (rand(d.date_key + s.store_id + 2) / 4294967295.0) * 0.6)
        )
    ) AS customer_count,
    round(
        12.0 + (rand(d.date_key + s.store_id + 3) / 4294967295.0) * 8.0
        * multiIf(
            s.store_format = 'full_service', 1.4,
            s.store_format = 'fast_casual', 1.1,
            1.0
        )
    , 2) AS avg_order_value,
    round(
        3.5 + (rand(d.date_key + s.store_id + 4) / 4294967295.0) * 1.2
        - if((rand(d.date_key + s.store_id + 5) / 4294967295.0) > 0.85, 0.8, 0)
    , 2) AS satisfaction_score
FROM stores s
CROSS JOIN days d
" "Creating 10M-row daily sales table (5K stores × 2K days, may take 30-60s)..."

ROW_COUNT=$(curl -fs "${CH_HOST}/" --data-binary "SELECT count() FROM ramen_demo.daily_sales")

echo ""
echo "Done. Table: ramen_demo.daily_sales (${ROW_COUNT} rows)"
echo "  5,000 stores × 2,000 days (Jul 2019 – Dec 2024)"
echo "  Columns: store_id, region, store_format, city, date_key,"
echo "           daily_revenue, order_count, customer_count, avg_order_value, satisfaction_score"

# ── Build pre-aggregations + pull Parquet + copy to app ───────────────────────

AIRLAYER="${REPO_ROOT}/target/release/airlayer"
if [ ! -x "$AIRLAYER" ]; then
    AIRLAYER="airlayer"
fi

# Clear local cache
rm -rf .airlayer

echo ""
echo "Building pre-aggregations in ClickHouse..."
"$AIRLAYER" build --config config.yml

echo ""
echo "Pulling pre-aggregated Parquet files to local cache..."
"$AIRLAYER" pull --config config.yml

echo ""
echo "Copying artifacts to app/public/..."
cd app && node setup.js && cd ..

echo ""
echo "All done! Start the demo with:"
echo "  cd app && pnpm run dev"
)
