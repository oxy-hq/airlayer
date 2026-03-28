#!/usr/bin/env bash
# Start tier-2 test databases with automatic port selection.
# If the default port is occupied, finds the next free one.
# Writes chosen ports to .test-ports.env (read by tests via dotenv).
set -euo pipefail
cd "$(dirname "$0")/.."

find_free_port() {
  local port=$1
  while lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; do
    echo "  Port $port occupied, trying $((port + 1))..." >&2
    port=$((port + 1))
  done
  echo "$port"
}

PG_PORT=$(find_free_port "${AIRLAYER_PG_PORT:-15432}")
MYSQL_PORT=$(find_free_port "${AIRLAYER_MYSQL_PORT:-13306}")
CH_HTTP_PORT=$(find_free_port "${AIRLAYER_CH_HTTP_PORT:-18123}")
CH_NATIVE_PORT=$(find_free_port "${AIRLAYER_CH_NATIVE_PORT:-19000}")

cat > .test-ports.env <<EOF
AIRLAYER_PG_PORT=$PG_PORT
AIRLAYER_MYSQL_PORT=$MYSQL_PORT
AIRLAYER_CH_HTTP_PORT=$CH_HTTP_PORT
AIRLAYER_CH_NATIVE_PORT=$CH_NATIVE_PORT
EOF

echo "Ports: pg=$PG_PORT mysql=$MYSQL_PORT clickhouse=$CH_HTTP_PORT"

export AIRLAYER_PG_PORT=$PG_PORT
export AIRLAYER_MYSQL_PORT=$MYSQL_PORT
export AIRLAYER_CH_HTTP_PORT=$CH_HTTP_PORT
export AIRLAYER_CH_NATIVE_PORT=$CH_NATIVE_PORT

docker compose -f docker-compose.test.yml up -d "$@"

echo ""
echo "To run tests:  set -a && source .test-ports.env && set +a && cargo test --features exec -- --include-ignored"
