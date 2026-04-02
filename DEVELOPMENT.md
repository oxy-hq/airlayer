# Development Guide

This document covers patterns and workflows for developing airlayer itself. For build/test commands, see [CLAUDE.md](CLAUDE.md). For schema and query docs, see the [docs/](docs/) directory.

## Releasing

Releases are built automatically by GitHub Actions when you push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

This triggers `.github/workflows/release.yaml`, which:

1. Builds `airlayer` with `--features exec` on 4 runners in parallel (macOS ARM, macOS Intel, Linux x86_64, Linux ARM64)
2. Generates SHA256 checksums per binary
3. Creates a GitHub Release with all binaries and a combined `SHA256SUMS.txt`

You can also trigger a release manually from the GitHub Actions tab using "Run workflow" and specifying a tag.

### Install script

Users install pre-built binaries via `install_airlayer.sh`:

```bash
bash <(curl -sSfL https://raw.githubusercontent.com/oxy-hq/airlayer/main/install_airlayer.sh)
```

The script detects OS and architecture, downloads the matching binary from GitHub Releases, and installs to `/usr/local/bin` or `~/.local/bin`. Set `AIRLAYER_VERSION=v0.1.0` to pin a version (defaults to `latest`).

## Init artifact pipeline

`airlayer init` runs an interactive discovery flow that connects to your database, lets you select tables, and generates a fully configured project. `airlayer update` refreshes agents and skills to the latest version.

### Interactive init flow

`airlayer init` walks the user through four stages:

1. **Connect** — select database type, enter credentials interactively (host, port, user, password env var, etc.)
2. **Discover** — connects to the warehouse, lists databases and schemas, presents a custom multi-select for table selection (with vim keybindings, viewport scrolling for large lists, toggle-all)
3. **Generate** — writes `config.yml` from prompted values, generates `.view.yml` files from discovered schema (one per table), installs Claude Code agents and skills
4. **Enrich** *(optional)* — if Claude Code is installed, offers to review and improve views via `claude -p ... --dangerously-skip-permissions`. Shows streaming progress with per-file checkmarks, elapsed time, and ETA

If the connection fails, the user is re-prompted with their existing values as defaults (in logical field order, not alphabetical).

### Implementation

The init flow lives in `src/cli/mod.rs` (`run_init()`, `run_init_discovery()`, `run_ai_enrichment()`) with interactive prompts in `src/cli/prompts.rs` and view YAML generation in `src/cli/bootstrap.rs`.

Key implementation details:

- **Process group isolation**: AI enrichment spawns Claude CLI in its own process group via `setpgid(0, 0)`, so ctrl+c kills the parent cleanly and the child dies from SIGPIPE
- **Stream-json parsing**: Reads Claude CLI's `--output-format stream-json` events to track file edits in real-time
- **Compiled-in templates**: Agent specs and skill files are embedded at compile time via `include_str!` — no runtime template files

```
Source (compile time)                        Output (user's project)
─────────────────────                        ───────────────────────
src/cli/mod.rs::INIT_CLAUDE_MD           →   CLAUDE.md
src/cli/mod.rs (prompted values)         →   config.yml
src/cli/bootstrap.rs (from schema)       →   views/*.view.yml
.claude/agents/analyst.md                →   .claude/agents/analyst.md
.claude/agents/builder.md               →   .claude/agents/builder.md
.claude/skills/bootstrap/SKILL.md       →   .claude/skills/bootstrap/SKILL.md
.claude/skills/profile/SKILL.md         →   .claude/skills/profile/SKILL.md
.claude/skills/query/SKILL.md           →   .claude/skills/query/SKILL.md
```

The agent and skill files are embedded at compile time via `include_str!`:

```rust
let agents: &[(&str, &str)] = &[
    ("analyst", include_str!("../../.claude/agents/analyst.md")),
    ("builder", include_str!("../../.claude/agents/builder.md")),
];

let skills: &[(&str, &str)] = &[
    ("bootstrap", include_str!("../../.claude/skills/bootstrap/SKILL.md")),
    ("profile", include_str!("../../.claude/skills/profile/SKILL.md")),
    ("query", include_str!("../../.claude/skills/query/SKILL.md")),
];
```

### init vs update behavior

| Command | `CLAUDE.md` | `config.yml` | `views/` | Agents & Skills |
|---------|-------------|-------------|----------|-----------------|
| `init` | Write if absent | Generated from prompts | Generated from schema | Write if absent |
| `update` | Overwrite (if changed) | Skip | Skip | Overwrite (if changed) |

`init` is interactive and generates config/views from the database. `update` is non-interactive — it only refreshes agents and skills to match the current binary version.

### What to update when adding features

When you add a new capability to airlayer (new CLI flag, schema type, motif, etc.), you must update **all** of these so that LLMs using the `init` output know about it:

| File | What it is | Who reads it |
|------|-----------|--------------|
| `CLAUDE.md` | Dev instructions (this repo) | Developers working on airlayer |
| `src/cli/mod.rs` → `INIT_CLAUDE_MD` | Generated project CLAUDE.md | LLMs in user projects |
| `.claude/agents/analyst.md` | Analyst sub-agent spec | The analyst agent (query execution) |
| `.claude/agents/builder.md` | Builder sub-agent spec | The builder agent (schema editing) |
| `.claude/skills/*/SKILL.md` | Skill instructions | Both agents + direct skill invocation |
| `docs/*.md` | Reference documentation | Developers and advanced users |

The key insight: editing `.claude/agents/analyst.md` in the repo automatically updates what `airlayer init` and `airlayer update` emit, because `include_str!` resolves at compile time. But you must rebuild the binary for changes to take effect.

### Adding a new agent or skill

**New agent:**

1. Create `.claude/agents/<name>.md` with frontmatter (name, description, tools, model, skills)
2. Add an `include_str!` entry in the `agents` array in `install_agents_and_skills()`
3. Document it in `INIT_CLAUDE_MD` under the "Sub-agents" section
4. Document it in `CLAUDE.md`

**New skill:**

1. Create `.claude/skills/<name>/SKILL.md` with frontmatter (name, description)
2. Add an `include_str!` entry in the `skills` array in `install_agents_and_skills()`
3. Reference it in the relevant agent's frontmatter (`skills: [<name>]`)
4. Document it in `INIT_CLAUDE_MD` under the "Skills" section

## Project directory layout

```
src/cli/mod.rs          CLI, init/update flow, AI enrichment, INIT_CLAUDE_MD const
src/cli/prompts.rs      Interactive prompts (credentials, database/table selection)
src/cli/bootstrap.rs    View YAML generation from discovered schema
src/engine/motifs.rs    Builtin motif catalog + CTE wrapping
src/schema/models.rs    All data types (View, Motif, SavedQuery, etc.)
src/schema/parser.rs    YAML parsing (.view.yml, .motif.yml, .query.yml)
src/schema/validator.rs Schema validation rules

.claude/agents/         Sub-agent specs (compiled into binary)
.claude/skills/         Skill files (compiled into binary)
docs/                   Reference documentation (not compiled in)
```

## Motifs architecture

Motifs are post-aggregation analytical patterns that wrap a base query as a CTE. See [docs/schema-format.md](docs/schema-format.md#motif-files-motifyml) for the user-facing format.

### Builtin vs custom

- **Builtin motifs** (12) are defined in `src/engine/motifs.rs` via `builtin_motifs()`. They have no `.motif.yml` file — the plan (output columns + expressions) is generated by `builtin_plan()`.
- **Custom motifs** are defined as `.motif.yml` files in `motifs/`. They are always single-stage and use the `outputs` field directly.

### CTE generation

`wrap_with_motif()` in `src/engine/motifs.rs` handles all SQL generation:

1. **Single-stage** (most motifs): `WITH __base AS (<sql>) SELECT b.*, <outputs> FROM __base b`
2. **Two-stage** (anomaly, trend): `WITH __base AS (<sql>), __stage1 AS (...) SELECT s.*, <final> FROM __stage1 s`

Multi-measure queries expand motif columns per-measure (e.g., `total_revenue__share`, `order_count__share`).

### Adding a new builtin motif

1. Add a constructor function (e.g., `my_motif()`) in `src/engine/motifs.rs`
2. Add it to the `builtin_motifs()` vec and the `is_builtin()` match
3. Add its plan to `builtin_plan()`
4. Add unit tests
5. Update documentation in all the init artifact files (see table above)

## Saved queries architecture

Saved queries define multi-step analytical workflows. See [docs/schema-format.md](docs/schema-format.md#saved-query-files-queryyml) for the user-facing format.

### Data flow

```
.query.yml  →  parser.rs::parse_saved_queries()  →  SavedQuery struct  →  validator.rs::validate_saved_queries()
```

Saved queries are deterministic lists of structured semantic queries. Each step contains a `QueryRequest` (same as `-q` JSON) that can be compiled to SQL independently.

### Validation rules (in `validator.rs`)

- At least one step per saved query
- Unique step names within a saved query

## Column qualification in expressions

When generating SQL for multi-view joins, bare column names in dimension/measure expressions must be qualified with the view's table alias. For example, if two views both have a `"Date"` column, the generated SQL needs `"macro"."Date"` and `"cardio"."Date"` — not ambiguous bare `"Date"`.

This is handled by `qualify_bare_columns()` in `sql_generator.rs`. See [docs/architecture.md](docs/architecture.md#column-qualification) for the full design rationale and comparison to Cube.js.

### Known limitations

- **Double-quoted identifiers are qualified unconditionally.** Unlike unquoted identifiers (which are only qualified if they match a known dimension name), any `"Identifier"` that isn't part of a dotted reference gets qualified. This is correct for column references but would misfire if someone put a non-column double-quoted identifier in an expression. In practice this doesn't happen — double-quoted identifiers in SQL expressions are column references.
- **Escaped single quotes** (`'it''s'`) in string literals are not handled by the string-skipping logic. The parser would see the middle `''` as end-of-string + start-of-new-string. This hasn't been a reported issue since dimension expressions rarely contain escaped string literals.

### Modifying the qualifier

If you need to change `qualify_bare_columns()`:

1. The function is a single-pass character tokenizer — understand the state machine before editing
2. Run `cargo test test_qualify_` to exercise the targeted tests
3. Run `cargo test test_complex_expression` for the COALESCE qualification test
4. Run the full suite (`cargo test`) to catch regressions in SQL generation

## Testing

airlayer uses a three-tier testing strategy. Each tier adds infrastructure requirements. See [docs/testing.md](docs/testing.md) for full details including credentials setup and seed data.

### Tier 1: Unit + in-process (no external deps)

```bash
cargo test                        # core only (no executor code)
cargo test --features exec        # includes executor compilation check
```

**What runs:** ~136 unit tests + in-process integration tests against DuckDB and SQLite.

- Unit tests are in `#[cfg(test)]` modules throughout `src/` — SQL generation, filter compilation, join resolution, motif CTE wrapping, profiling, param escaping, parsing, validation
- In-process integration tests (`tests/integration_tests.rs`) compile queries and execute against embedded DuckDB/SQLite using seed data from `tests/integration/seed/`
- Parse-validation tests verify generated SQL parses correctly for BigQuery, Snowflake, Databricks, and Redshift dialects (no database needed — just syntax checking)

**When to run:** Always. This is the minimum before any PR.

### Tier 2: Docker-based databases

```bash
docker compose -f docker-compose.test.yml up -d
cargo test --features exec -- --include-ignored
```

**What runs:** ~9 tests against Postgres, MySQL, and ClickHouse running in Docker containers.

- Tests are marked `#[ignore = "tier2"]` — `--include-ignored` includes them
- The compose file (`docker-compose.test.yml`) starts three services with auto-seeded data
- Seed scripts are mounted from `tests/integration/seed/{postgres,mysql,clickhouse}.sql`

| Service | Port env var | Default | Database |
|---------|-------------|---------|----------|
| Postgres | `AIRLAYER_PG_PORT` | 15432 | `airlayer_test` |
| MySQL | `AIRLAYER_MYSQL_PORT` | 13306 | `airlayer_test` |
| ClickHouse | `AIRLAYER_CH_HTTP_PORT` | 18123 | `analytics` |

Override ports if you have conflicts (e.g., `AIRLAYER_PG_PORT=25432 docker compose -f docker-compose.test.yml up -d`). The same env vars are read by both the compose file and the test code.

**When to run:** When changing executor code, SQL generation, or dialect-specific behavior. Docker must be running.

### Tier 3: Live cloud warehouses

```bash
cargo test --features exec -- --include-ignored tier3        # Snowflake + BigQuery
cargo test --features exec -- --include-ignored motherduck   # MotherDuck
cargo test --features exec -- --include-ignored snowflake    # just Snowflake
cargo test --features exec -- --include-ignored bigquery     # just BigQuery
```

**What runs:** ~20 tests against live Snowflake, BigQuery, and MotherDuck instances.

- Tests are marked `#[ignore = "tier3"]` or `#[ignore = "tier3_motherduck"]`
- Credentials are loaded from `.env` at the repo root (copy from `.env.example`)
- Tests auto-seed on first run — no manual setup needed
- BigQuery access tokens expire after ~1 hour; refresh with `gcloud auth print-access-token`

| Warehouse | Tests | Key things tested |
|-----------|-------|-------------------|
| Snowflake | ~6 | Standard query, segments, unfiltered, measure values, motifs |
| BigQuery | ~7 | Standard query, unfiltered, measure values, profiling, motifs |
| MotherDuck | ~8 | Standard query, segments, unfiltered, measure values, schema introspection, motifs |

**When to run:** When changing executor code for a specific warehouse, or before releases. Requires cloud credentials.

### Running everything

```bash
# Start Docker databases
docker compose -f docker-compose.test.yml up -d

# Refresh BigQuery token
sed -i '' "s|^BIGQUERY_ACCESS_TOKEN=.*|BIGQUERY_ACCESS_TOKEN=$(gcloud auth print-access-token)|" .env

# All tiers
cargo test --features exec -- --include-ignored
```

### Self-seeding pattern

All tiers seed their data programmatically at test time — tests don't rely on external setup:

- **Tier 1** (DuckDB/SQLite): Seed SQL loaded in-process via `include_str!`
- **Tier 2** (Postgres/MySQL/ClickHouse): Each test module has a `seed()` function that drops and recreates tables from `tests/integration/seed/*.sql` via `include_str!`. Uses `std::sync::Once` to run only once per test suite execution (avoids races when tests run in parallel).
- **Tier 3** (Snowflake/BigQuery/MotherDuck): Each has an explicit `_seed()` test that runs the seed SQL programmatically via the database's REST/driver API.

Docker compose still mounts seed scripts to `/docker-entrypoint-initdb.d/` for initial container creation, but the programmatic seeding means tests work correctly even if the Docker volumes are stale or the init scripts didn't run.

When writing SQL seed files, note that the statement-splitting logic strips `--` comment lines before checking if a statement is empty. This means SQL comments can appear anywhere in the file, including before `CREATE TABLE` statements.

### Test data

All tiers use the same 12-row `events` table with consistent expected values (7 web events / $164.98, 3 ios / $25.00, 2 android / $0.00). This makes it easy to assert exact results across databases. Seed scripts are in `tests/integration/seed/`.

### Adding tests

- **Unit tests:** Add to the `#[cfg(test)]` module in the relevant `src/` file. Use `make_test_engine()` in `sql_generator.rs` for a pre-configured evaluator.
- **Integration tests:** Add to `tests/integration_tests.rs`. Mark with `#[ignore = "tier2"]` or `#[ignore = "tier3"]` as appropriate. Use existing view files and seed data.
