#!/usr/bin/env bash
# Pre-aggregation speed demo: Snowflake (10M rows)
#
# Compares query speed between:
#   - Raw Snowflake query (scans 10M rows in the warehouse)
#   - Pre-aggregated query (reads from local Parquet cache via DuckDB)
#
# Prerequisites:
#   1. Build airlayer: cargo build --features exec --release
#   2. Set env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#   3. Seed data: ./seed.sh
#
# Usage: ./demo.sh
(
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"

# ── load env ──────────────────────────────────────────────────────────────
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

# ── resolve airlayer binary ──────────────────────────────────────────────
if [ -x "$REPO_ROOT/target/release/airlayer" ]; then
    AL="$REPO_ROOT/target/release/airlayer"
elif [ -x "$REPO_ROOT/target/debug/airlayer" ]; then
    AL="$REPO_ROOT/target/debug/airlayer"
elif command -v airlayer &>/dev/null; then
    AL=airlayer
else
    echo "airlayer binary not found. Build with: cargo build --features exec --release"
    exit 1
fi

# ── helpers ──────────────────────────────────────────────────────────────
bold=$(tput bold)
dim=$(tput setaf 8)
green=$(tput setaf 2)
red=$(tput setaf 1)
reset=$(tput sgr0)

run_query() {
    local start end elapsed
    start=$(python3 -c 'import time; print(time.time())')
    "$@" > /tmp/airlayer_sf_demo_out.json 2>/dev/null
    end=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{($end - $start)*1000:.0f}')")

    # Print results as a table
    python3 -c "
import json, sys
d = json.load(open('/tmp/airlayer_sf_demo_out.json'))
rows = d.get('data', [])
if not rows:
    print('  (no data)')
    sys.exit()
cols = list(rows[0].keys())
short = [c.split('__',1)[-1] if '__' in c else c for c in cols]
widths = [max(len(s), max(len(str(r.get(c,''))) for r in rows)) for s, c in zip(short, cols)]
header = '  '.join(s.ljust(w) for s, w in zip(short, widths))
print(f'  {header}')
print(f'  ' + '  '.join('-'*w for w in widths))
for r in rows:
    vals = []
    for c, w in zip(cols, widths):
        v = r.get(c, '')
        if isinstance(v, float) and v == int(v):
            v = int(v)
        vals.append(str(v).ljust(w))
    print(f'  ' + '  '.join(vals))
"
    local rows
    rows=$(python3 -c "import json; print(len(json.load(open('/tmp/airlayer_sf_demo_out.json')).get('data',[])))")
    echo ""
    echo "${dim}  ${elapsed} ms  (${rows} rows)${reset}"
}

run_quiet() {
    local start end elapsed
    start=$(python3 -c 'import time; print(time.time())')
    "$@" > /tmp/airlayer_sf_demo_out.json 2>/dev/null
    end=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{($end - $start)*1000:.0f}')")
    echo "${dim}  ${elapsed} ms${reset}"
}

# ── clean prior state ────────────────────────────────────────────────────
rm -rf .airlayer

# Drop stale rollup schema in Snowflake (ensures clean manifest)
: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT}"
BASE_URL="https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
SF_TOKEN=$(curl -s -X POST "${BASE_URL}/session/v1/login-request" \
    -H 'Content-Type: application/json' \
    -d "{\"data\":{\"LOGIN_NAME\":\"${SNOWFLAKE_USER}\",\"PASSWORD\":\"${SNOWFLAKE_PASSWORD}\",\"ACCOUNT_NAME\":\"${SNOWFLAKE_ACCOUNT}\"}}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['token'])")
curl -s -X POST \
    "${BASE_URL}/queries/v1/query-request?requestId=$(python3 -c 'import uuid; print(uuid.uuid4())')" \
    -H 'Content-Type: application/json' -H 'Accept: application/snowflake' \
    -H "Authorization: Snowflake Token=\"${SF_TOKEN}\"" \
    -d '{"sqlText":"DROP SCHEMA IF EXISTS AIRLAYER_TEST.PREAGG_ROLLUPS CASCADE","asyncExec":false,"sequenceId":1}' > /dev/null 2>&1 || true

echo ""
echo "${bold}================================================================${reset}"
echo "${bold}  Pre-aggregation Speed Demo  (10M rows in Snowflake)${reset}"
echo "${bold}================================================================${reset}"
echo ""

# ── 1. Raw query ─────────────────────────────────────────────────────────
echo "${bold}1) Raw query — scans all 10,000,000 rows in Snowflake${reset}"
echo "   Revenue + event count by platform"
echo ""
run_query $AL query -x --config config.yml --no-cache \
    --dimension events.platform \
    --measure events.total_revenue \
    --measure events.event_count \
    --order events.total_revenue:desc
RAW_TIME=$(python3 -c "
import json
d = json.load(open('/tmp/airlayer_sf_demo_out.json'))
# save for later comparison
import os; os.makedirs('/tmp', exist_ok=True)
json.dump(d, open('/tmp/airlayer_sf_raw.json','w'))
")
echo ""

# ── 2. Build ─────────────────────────────────────────────────────────────
echo "${bold}2) Build rollup tables in Snowflake${reset}"
echo "   by_platform_monthly: 3 platforms x 12 months = ~36 rows"
echo "   by_country_monthly:  6 countries x 12 months = ~72 rows"
echo ""
run_quiet $AL build --config config.yml
echo ""

# ── 3. Pull ──────────────────────────────────────────────────────────────
echo "${bold}3) Pull rollups to local Parquet cache${reset}"
echo ""
run_quiet $AL pull --config config.yml
echo ""
echo "   Cache files:"
ls -lh .airlayer/cache/*.parquet 2>/dev/null | awk '{print "   "$NF" ("$5")"}'
echo ""

# ── 4. Cached query ──────────────────────────────────────────────────────
echo "${bold}4) Same query — now served from local Parquet cache${reset}"
echo "   Re-aggregated from ~36-row rollup (no Snowflake round-trip)"
echo ""
run_query $AL query -x --config config.yml \
    --dimension events.platform \
    --measure events.total_revenue \
    --measure events.event_count \
    --order events.total_revenue:desc
echo ""

# ── 5. Another cached query ──────────────────────────────────────────────
echo "${bold}5) Country query — served from 72-row Parquet cache${reset}"
echo ""
run_query $AL query -x --config config.yml \
    --dimension events.country \
    --measure events.total_revenue \
    --measure events.event_count \
    --order events.total_revenue:desc
echo ""

# ── 6. --no-cache comparison ────────────────────────────────────────────
echo "${bold}6) --no-cache bypasses cache, scans raw table again${reset}"
echo ""
run_query $AL query -x --config config.yml --no-cache \
    --dimension events.platform \
    --measure events.total_revenue \
    --measure events.event_count \
    --order events.total_revenue:desc
echo ""

echo "${green}${bold}Pre-aggregated queries skip the Snowflake round-trip entirely,${reset}"
echo "${green}${bold}reading from tiny local Parquet files instead of scanning 10M rows.${reset}"
echo ""
echo "Cleanup: ./teardown.sh  (drops PREAGG_DEMO schema + local cache)"
)
