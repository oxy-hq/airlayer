# airlayer task runner
# Install: cargo install just
# List all recipes: just --list

# Default recipe
default:
    @just --list

# ── Build ────────────────────────────────────────────────

# Build core (no database drivers)
build:
    cargo build

# Build with all database drivers
build-all:
    cargo build --features exec

# ── Test ─────────────────────────────────────────────────

# Tier 1: unit tests + in-process integration (DuckDB, SQLite)
test:
    cargo test

# Tier 1 with executor compilation check
test-exec:
    cargo test --features exec

# Tier 2: start Docker databases (auto-selects free ports)
db-up:
    ./scripts/test-db-up.sh

# Tier 2: stop Docker databases
db-down:
    docker compose -f docker-compose.test.yml down

# Tier 2: run all tier 1 + 2 tests (Docker databases must be running)
test-docker: db-up
    @set -a && [ -f .test-ports.env ] && . ./.test-ports.env; set +a; \
    cargo test --features exec -- --include-ignored

# Tier 2: run Presto/Trino tests
test-presto: db-up
    @set -a && [ -f .test-ports.env ] && . ./.test-ports.env; set +a; \
    cargo test --features exec -- --include-ignored presto

# Tier 3: refresh BigQuery access token
bq-refresh:
    sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$$(gcloud auth print-access-token)|" .env

# Tier 3: run Snowflake tests
test-snowflake:
    cargo test --features exec -- --include-ignored snowflake

# Tier 3: run BigQuery tests (refreshes token first)
test-bigquery: bq-refresh
    cargo test --features exec -- --include-ignored bigquery

# Tier 3: run Databricks tests
test-databricks:
    cargo test --features exec -- --include-ignored databricks

# Tier 3: run MotherDuck tests
test-motherduck:
    cargo test --features exec -- --include-ignored motherduck

# Tier 3: run all cloud warehouse tests
test-cloud: bq-refresh
    cargo test --features exec -- --include-ignored tier3
    cargo test --features exec -- --include-ignored motherduck

# All tiers: Docker + cloud (the works)
test-all: db-up bq-refresh
    @set -a && [ -f .test-ports.env ] && . ./.test-ports.env; set +a; \
    cargo test --features exec -- --include-ignored

# ── Validate ─────────────────────────────────────────────

# Run clippy lints
lint:
    cargo clippy --features exec -- -D warnings

# Check formatting
fmt-check:
    cargo fmt -- --check

# Format code
fmt:
    cargo fmt

# ── WASM ─────────────────────────────────────────────────

# Build WASM package for web (output in pkg/)
build-wasm:
    wasm-pack build --target web -- --no-default-features --features wasm
    wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int pkg/airlayer_bg.wasm -o pkg/airlayer_bg.wasm.opt && mv pkg/airlayer_bg.wasm.opt pkg/airlayer_bg.wasm
    cp wasm-readme.md pkg/README.md

# Build WASM package for Node.js (output in pkg/)
build-wasm-node:
    wasm-pack build --target nodejs -- --no-default-features --features wasm
    wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int pkg/airlayer_bg.wasm -o pkg/airlayer_bg.wasm.opt && mv pkg/airlayer_bg.wasm.opt pkg/airlayer_bg.wasm
    cp wasm-readme.md pkg/README.md

# ── Python ──────────────────────────────────────────────

# Build Python package (dev install into current venv)
build-python:
    maturin develop --no-default-features --features python

# Build Python wheel (release)
build-python-release:
    maturin build --release --no-default-features --features python

# ── Utilities ────────────────────────────────────────────

# Validate semantic layer files in a directory
validate path='.':
    cargo run -- validate --path {{path}}

# Run a query (compile only)
query *args:
    cargo run --features exec -- query {{args}}
