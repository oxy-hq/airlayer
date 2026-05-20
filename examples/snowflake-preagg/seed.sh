#!/usr/bin/env bash
# Seed Snowflake with 10M ramen shop daily sales records.
#
# Generates a denormalized DAILY_SALES table with store attributes baked in,
# modeled after the da-bench-internal San Mateo ramen shop dataset.
#
# 5,000 stores × 2,000 days = 10,000,000 rows
#
# Requires: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE
#
# Usage: ./seed.sh
(
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"

if [ -f "$REPO_ROOT/.env" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        key="${line%%=*}"
        value="${line#*=}"
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        export "$key=$value" 2>/dev/null || true
    done < "$REPO_ROOT/.env"
fi

: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT}"
: "${SNOWFLAKE_USER:?Set SNOWFLAKE_USER}"
: "${SNOWFLAKE_PASSWORD:?Set SNOWFLAKE_PASSWORD}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"

BASE_URL="https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"

echo "Authenticating to Snowflake (${SNOWFLAKE_ACCOUNT})..."

LOGIN_RESP=$(curl -s -X POST "${BASE_URL}/session/v1/login-request" \
    -H 'Content-Type: application/json' \
    -d "{
        \"data\": {
            \"LOGIN_NAME\": \"${SNOWFLAKE_USER}\",
            \"PASSWORD\": \"${SNOWFLAKE_PASSWORD}\",
            \"ACCOUNT_NAME\": \"${SNOWFLAKE_ACCOUNT}\"
        }
    }")

TOKEN=$(echo "$LOGIN_RESP" | python3 -c "
import sys, json
resp = json.load(sys.stdin)
if not resp.get('success'):
    msg = resp.get('message', 'Unknown error')
    print(f'Authentication failed: {msg}', file=sys.stderr)
    sys.exit(1)
print(resp['data']['token'])
")

echo "Authenticated."

SEQ=0
run_sql() {
    local sql="$1"
    local desc="${2:-}"
    [ -n "$desc" ] && echo "  $desc"
    SEQ=$((SEQ + 1))

    local body
    body=$(python3 -c "
import json, sys
sql = sys.stdin.read()
print(json.dumps({
    'sqlText': sql.strip(),
    'asyncExec': False,
    'sequenceId': ${SEQ},
}))
" <<< "$sql")

    local resp
    resp=$(curl -s -X POST \
        "${BASE_URL}/queries/v1/query-request?requestId=$(python3 -c 'import uuid; print(uuid.uuid4())')" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/snowflake' \
        -H "Authorization: Snowflake Token=\"${TOKEN}\"" \
        -d "$body")

    local success
    success=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success', False))" 2>/dev/null || echo "False")
    if [ "$success" != "True" ]; then
        echo "SQL failed:" >&2
        echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message','Unknown error'))" >&2
        return 1
    fi
}

echo ""
echo "Seeding 10M ramen shop daily sales into AIRLAYER_TEST.RAMEN_DEMO.DAILY_SALES ..."
echo ""

run_sql "USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE}" "Using warehouse ${SNOWFLAKE_WAREHOUSE}..."
run_sql "CREATE DATABASE IF NOT EXISTS AIRLAYER_TEST" "Creating database..."
run_sql "CREATE SCHEMA IF NOT EXISTS AIRLAYER_TEST.RAMEN_DEMO" "Creating schema..."
run_sql "USE DATABASE AIRLAYER_TEST" ""

run_sql "
CREATE OR REPLACE TABLE RAMEN_DEMO.DAILY_SALES AS
WITH stores AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS STORE_ID,
        CASE MOD(SEQ4(), 5)
            WHEN 0 THEN 'northeast'
            WHEN 1 THEN 'southeast'
            WHEN 2 THEN 'midwest'
            WHEN 3 THEN 'southwest'
            WHEN 4 THEN 'west'
        END AS REGION,
        CASE MOD(SEQ4(), 3)
            WHEN 0 THEN 'full_service'
            WHEN 1 THEN 'fast_casual'
            WHEN 2 THEN 'counter_service'
        END AS STORE_FORMAT,
        CASE MOD(SEQ4(), 15)
            WHEN 0 THEN 'San Mateo'    WHEN 1 THEN 'Brooklyn'
            WHEN 2 THEN 'Austin'       WHEN 3 THEN 'Portland'
            WHEN 4 THEN 'Chicago'      WHEN 5 THEN 'Denver'
            WHEN 6 THEN 'Seattle'      WHEN 7 THEN 'Miami'
            WHEN 8 THEN 'Nashville'    WHEN 9 THEN 'Boston'
            WHEN 10 THEN 'Phoenix'     WHEN 11 THEN 'Atlanta'
            WHEN 12 THEN 'Minneapolis' WHEN 13 THEN 'Dallas'
            WHEN 14 THEN 'San Diego'
        END AS CITY,
        -- Base revenue varies by format
        CASE MOD(SEQ4(), 3)
            WHEN 0 THEN 450  -- full_service: higher revenue
            WHEN 1 THEN 350  -- fast_casual: medium
            WHEN 2 THEN 250  -- counter_service: lower
        END AS BASE_REVENUE,
        -- Seating capacity
        CASE MOD(SEQ4(), 3)
            WHEN 0 THEN 60
            WHEN 1 THEN 40
            WHEN 2 THEN 20
        END AS SEATING_CAPACITY
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
),
days AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2019-07-01'::DATE) AS DATE_KEY
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
)
SELECT
    s.STORE_ID,
    s.REGION,
    s.STORE_FORMAT,
    s.CITY,
    d.DATE_KEY,
    -- Daily revenue: base + seasonal + day-of-week + noise
    ROUND(
        s.BASE_REVENUE
        * (CASE MONTH(d.DATE_KEY)
            WHEN 1 THEN 1.15 WHEN 2 THEN 1.10  -- winter: ramen season
            WHEN 3 THEN 1.00 WHEN 4 THEN 0.90
            WHEN 5 THEN 0.85 WHEN 6 THEN 0.80   -- summer: lower demand
            WHEN 7 THEN 0.78 WHEN 8 THEN 0.80
            WHEN 9 THEN 0.90 WHEN 10 THEN 1.00
            WHEN 11 THEN 1.10 WHEN 12 THEN 1.20  -- holiday boost
        END)
        * (CASE DAYOFWEEK(d.DATE_KEY)
            WHEN 0 THEN 0.85  -- Sunday
            WHEN 1 THEN 0.75  -- Monday (slowest)
            WHEN 2 THEN 0.80  WHEN 3 THEN 0.85
            WHEN 4 THEN 0.95  -- Thursday
            WHEN 5 THEN 1.15  -- Friday
            WHEN 6 THEN 1.20  -- Saturday (busiest)
        END)
        * (0.8 + UNIFORM(0::FLOAT, 0.4::FLOAT, RANDOM()))
    , 2) AS DAILY_REVENUE,
    -- Order count
    FLOOR(
        (s.SEATING_CAPACITY * 0.6)
        * (CASE DAYOFWEEK(d.DATE_KEY) WHEN 5 THEN 1.15 WHEN 6 THEN 1.20 WHEN 1 THEN 0.75 ELSE 1.0 END)
        * (0.7 + UNIFORM(0::FLOAT, 0.6::FLOAT, RANDOM()))
    ) AS ORDER_COUNT,
    -- Customer count (slightly less than orders due to groups)
    FLOOR(
        (s.SEATING_CAPACITY * 0.5)
        * (CASE DAYOFWEEK(d.DATE_KEY) WHEN 5 THEN 1.15 WHEN 6 THEN 1.20 WHEN 1 THEN 0.75 ELSE 1.0 END)
        * (0.7 + UNIFORM(0::FLOAT, 0.6::FLOAT, RANDOM()))
    ) AS CUSTOMER_COUNT,
    -- Average order value
    ROUND(12.0 + UNIFORM(0::FLOAT, 8::FLOAT, RANDOM())
        * (CASE s.STORE_FORMAT
            WHEN 'full_service' THEN 1.4
            WHEN 'fast_casual' THEN 1.1
            ELSE 1.0
        END)
    , 2) AS AVG_ORDER_VALUE,
    -- Customer satisfaction (1-5 scale)
    ROUND(3.5 + UNIFORM(0::FLOAT, 1.2::FLOAT, RANDOM())
        - (CASE WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) > 0.85 THEN 0.8 ELSE 0 END)
    , 2) AS SATISFACTION_SCORE
FROM stores s
CROSS JOIN days d
" "Creating 10M-row daily sales table (5K stores × 2K days, may take 60-90s)..."

echo ""
echo "Done. Table: AIRLAYER_TEST.RAMEN_DEMO.DAILY_SALES (10M rows)"
echo "  5,000 stores × 2,000 days (Jul 2019 – Dec 2024)"
echo "  Columns: STORE_ID, REGION, STORE_FORMAT, CITY, DATE_KEY,"
echo "           DAILY_REVENUE, ORDER_COUNT, CUSTOMER_COUNT, AVG_ORDER_VALUE, SATISFACTION_SCORE"

# ── Build pre-aggregations + pull Parquet + copy to app ───────────────────────

# Use locally-built binary (has latest fixes)
AIRLAYER="${REPO_ROOT}/target/release/airlayer"
if [ ! -x "$AIRLAYER" ]; then
    # Fall back to PATH
    AIRLAYER="airlayer"
fi

# Drop any previous rollup schema to start clean
run_sql "DROP SCHEMA IF EXISTS AIRLAYER_TEST.PREAGG_ROLLUPS CASCADE" "Cleaning previous rollups..."

# Clear local cache
rm -rf .airlayer

echo ""
echo "Building pre-aggregations in Snowflake..."
"$AIRLAYER" build --config config.yml

echo ""
echo "Pulling pre-aggregated Parquet files to local cache..."
"$AIRLAYER" pull --config config.yml

echo ""
echo "Copying artifacts to app/public/..."
cd app && node setup.js && cd ..

echo ""
echo "All done! Start the demo with:"
echo "  cd app && npm run dev"
)
