#!/usr/bin/env bash
# Tear down the ClickHouse demo: drop databases, stop the container, clear cache.
#
# Usage:
#   ./teardown.sh         # drops data, keeps container running
#   ./teardown.sh --all   # also stops the container (docker compose down -v)
(
set -euo pipefail
cd "$(dirname "$0")"

CH_HOST="http://localhost:8123"

if curl -fs "${CH_HOST}/ping" > /dev/null 2>&1; then
    echo "Dropping databases ramen_demo and preagg_rollups..."
    curl -fs "${CH_HOST}/" --data-binary "DROP DATABASE IF EXISTS ramen_demo" > /dev/null
    curl -fs "${CH_HOST}/" --data-binary "DROP DATABASE IF EXISTS preagg_rollups" > /dev/null
else
    echo "ClickHouse not running — nothing to drop."
fi

rm -rf .airlayer
echo "Cleared local cache (.airlayer/)."

if [ "${1:-}" = "--all" ]; then
    echo "Stopping container..."
    docker compose down -v
    echo "Done."
else
    echo ""
    echo "Container still running. Stop it with: docker compose down"
fi
)
