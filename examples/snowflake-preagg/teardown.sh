#!/usr/bin/env bash
# Clean up the Snowflake demo schema.
#
# Usage: ./teardown.sh
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

echo "Authenticating..."
TOKEN=$(curl -s -X POST "${BASE_URL}/session/v1/login-request" \
    -H 'Content-Type: application/json' \
    -d "{
        \"data\": {
            \"LOGIN_NAME\": \"${SNOWFLAKE_USER}\",
            \"PASSWORD\": \"${SNOWFLAKE_PASSWORD}\",
            \"ACCOUNT_NAME\": \"${SNOWFLAKE_ACCOUNT}\"
        }
    }" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['token'])")

echo "Dropping schema AIRLAYER_TEST.RAMEN_DEMO..."
curl -s -X POST \
    "${BASE_URL}/queries/v1/query-request?requestId=$(python3 -c 'import uuid; print(uuid.uuid4())')" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/snowflake' \
    -H "Authorization: Snowflake Token=\"${TOKEN}\"" \
    -d "{
        \"sqlText\": \"DROP SCHEMA IF EXISTS AIRLAYER_TEST.RAMEN_DEMO CASCADE\",
        \"asyncExec\": false,
        \"sequenceId\": 1
    }" > /dev/null

echo "Dropping schema AIRLAYER_TEST.PREAGG_ROLLUPS..."
curl -s -X POST \
    "${BASE_URL}/queries/v1/query-request?requestId=$(python3 -c 'import uuid; print(uuid.uuid4())')" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/snowflake' \
    -H "Authorization: Snowflake Token=\"${TOKEN}\"" \
    -d "{
        \"sqlText\": \"DROP SCHEMA IF EXISTS AIRLAYER_TEST.PREAGG_ROLLUPS CASCADE\",
        \"asyncExec\": false,
        \"sequenceId\": 2
    }" > /dev/null

rm -rf .airlayer

echo "Done. Schemas dropped (RAMEN_DEMO, PREAGG_ROLLUPS), local cache cleared."
)
