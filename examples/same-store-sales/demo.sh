#!/usr/bin/env bash
# Same-store-sales demo: shows lifespan + shift compiling a declarative comp.
#
#   same_store_sales = net_sales / net_sales_prior - 1
#
# `net_sales_prior` re-evaluates net_sales one year earlier, and `comparable_by:
# store_id` restricts the whole query to stores live in BOTH years. The expected
# answer is -3.18%: only stores A and B are comparable (C and D are too new, E
# closed mid-2026), so it compares 980+1150 against 1000+1200.
#
# Usage: ./demo.sh   (seeds the DuckDB file on first run; needs the `duckdb` CLI)
(
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"

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

# ── seed the DuckDB file on first run ────────────────────────────────────
if [ ! -f same-store-sales.duckdb ]; then
    if ! command -v duckdb &>/dev/null; then
        echo "The 'duckdb' CLI is required to seed the demo database."
        echo "Install it (https://duckdb.org/docs/installation/) and re-run, or seed manually:"
        echo "    duckdb same-store-sales.duckdb < seed.sql"
        exit 1
    fi
    echo "Seeding same-store-sales.duckdb ..."
    duckdb same-store-sales.duckdb < seed.sql
    echo ""
fi

# The comp query, as plain CLI flags. `--time-dimension member:granularity:from,to`
# supplies the current window the shift compares against.
ARGS=(
  --config config.yml
  --measure sales.same_store_sales
  --measure sales.net_sales
  --measure sales.net_sales_prior
  --time-dimension sales.sale_date:year:2026-01-01,2026-12-31
)

echo "Compiled SQL (multi-stage: cohort → shifted self-join → ratio):"
echo "------------------------------------------------------------------"
"$AL" query "${ARGS[@]}" -d duckdb | sed 's/^/  /'
echo ""

echo "Executed result (same_store_sales ≈ -0.0318 = -3.18%):"
echo "------------------------------------------------------------------"
"$AL" query -x "${ARGS[@]}"
echo ""
echo "Only stores A and B are comparable across both years; C and D are too new"
echo "and E closed mid-2026, so the comp is 2130/2200 - 1 = -3.18%."
)
