#!/usr/bin/env bash
# Pre-aggregation speed demo: ClickHouse (10M rows, local Docker)
#
# Compares query speed between:
#   - Raw ClickHouse query (scans 10M rows in the local container)
#   - Pre-aggregated query (reads from local Parquet cache via DuckDB)
#
# Prerequisites:
#   1. Build airlayer: cargo build --features exec --release
#   2. Seed data:      ./seed.sh   (starts Docker + populates 10M rows)
#
# Usage: ./demo.sh
(
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"
CH_HOST="http://localhost:8123"

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

if ! curl -fs "${CH_HOST}/ping" > /dev/null 2>&1; then
    echo "ClickHouse not reachable at ${CH_HOST}." >&2
    echo "Run ./seed.sh first (it brings up the container and seeds 10M rows)." >&2
    exit 1
fi

# ── helpers ──────────────────────────────────────────────────────────────
bold=$(tput bold)
dim=$(tput setaf 8)
green=$(tput setaf 2)
reset=$(tput sgr0)

run_query() {
    local start end elapsed
    start=$(python3 -c 'import time; print(time.time())')
    "$@" > /tmp/airlayer_ch_demo_out.json 2>/dev/null
    end=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{($end - $start)*1000:.0f}')")

    python3 -c "
import json, sys
d = json.load(open('/tmp/airlayer_ch_demo_out.json'))
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
    rows=$(python3 -c "import json; print(len(json.load(open('/tmp/airlayer_ch_demo_out.json')).get('data',[])))")
    echo ""
    echo "${dim}  ${elapsed} ms  (${rows} rows)${reset}"
}

run_quiet() {
    local start end elapsed
    start=$(python3 -c 'import time; print(time.time())')
    "$@" > /tmp/airlayer_ch_demo_out.json 2>/dev/null
    end=$(python3 -c 'import time; print(time.time())')
    elapsed=$(python3 -c "print(f'{($end - $start)*1000:.0f}')")
    echo "${dim}  ${elapsed} ms${reset}"
}

# ── clean prior cache + rollup tables ────────────────────────────────────
rm -rf .airlayer
curl -fs "${CH_HOST}/" --data-binary "DROP DATABASE IF EXISTS preagg_rollups" > /dev/null

echo ""
echo "${bold}================================================================${reset}"
echo "${bold}  Pre-aggregation Speed Demo  (10M rows in ClickHouse, Docker)${reset}"
echo "${bold}================================================================${reset}"
echo ""

# ── 1. Raw query ─────────────────────────────────────────────────────────
echo "${bold}1) Raw query — scans all 10,000,000 rows in ClickHouse${reset}"
echo "   Revenue + orders by region"
echo ""
run_query $AL query -x --config config.yml --no-cache \
    --dimension sales.region \
    --measure sales.total_revenue \
    --measure sales.total_orders \
    --order sales.total_revenue:desc
echo ""

# ── 2. Build ─────────────────────────────────────────────────────────────
echo "${bold}2) Build rollup tables in ClickHouse${reset}"
echo "   by_region_monthly: 5 regions x 60 months = ~300 rows"
echo "   by_format_monthly: 3 formats x 60 months = ~180 rows"
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
echo "   Re-aggregated from ~300-row rollup (no ClickHouse round-trip)"
echo ""
run_query $AL query -x --config config.yml \
    --dimension sales.region \
    --measure sales.total_revenue \
    --measure sales.total_orders \
    --order sales.total_revenue:desc
echo ""

# ── 5. Another cached query ──────────────────────────────────────────────
echo "${bold}5) Format query — served from 180-row Parquet cache${reset}"
echo ""
run_query $AL query -x --config config.yml \
    --dimension sales.store_format \
    --measure sales.total_revenue \
    --measure sales.total_orders \
    --order sales.total_revenue:desc
echo ""

# ── 6. --no-cache comparison ────────────────────────────────────────────
echo "${bold}6) --no-cache bypasses cache, scans raw table again${reset}"
echo ""
run_query $AL query -x --config config.yml --no-cache \
    --dimension sales.region \
    --measure sales.total_revenue \
    --measure sales.total_orders \
    --order sales.total_revenue:desc
echo ""

echo "${green}${bold}Pre-aggregated queries skip the ClickHouse round-trip entirely,${reset}"
echo "${green}${bold}reading from tiny local Parquet files instead of scanning 10M rows.${reset}"
echo ""
echo "Cleanup: ./teardown.sh  (drops ClickHouse databases + local cache)"
)
