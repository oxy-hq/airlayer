# Agent Execution Interface

## Philosophy

airlayer's `--execute` flag is designed for AI agent consumption. The core principle: **the semantic layer is the contract layer between the agent and the database**. An agent never writes or executes arbitrary SQL. Instead, it expresses intent through the semantic vocabulary — dimensions, measures, filters, segments — and airlayer compiles and executes that intent against the configured database.

This design draws from Justin Poehnelt's ["You Need to Rewrite Your CLI for AI Agents"](https://justin.poehnelt.com/posts/rewrite-your-cli-for-ai-agents/), which argues that agent-first interfaces need fundamentally different properties than human-first ones. The key ideas applied here:

- **Schema introspection replaces documentation.** The agent discovers available dimensions and measures at runtime via `airlayer inspect --json`, not by reading docs.
- **Structured envelopes over raw output.** Every `--execute` invocation returns a single JSON object with status, metadata, data, and error context. The agent never parses free-form text.
- **Context window discipline.** Result data is capped at 50 rows. The `row_count` field always reflects the true total, so the agent knows cardinality without consuming its context budget.
- **Input hardening.** The agent can only reference member paths defined in `.view.yml` files. Invalid paths produce clear `compile_error` responses. There is no path to SQL injection or arbitrary query execution.
- **The agent is not a trusted operator.** The semantic layer constrains what questions can be asked. This is a feature, not a limitation — it means the agent can iterate freely without risk of destructive or nonsensical queries.

## The Iteration Loop

The intended workflow for an agent iterating on semantic layer accuracy:

```
1. Read .view.yml files to understand current definitions
2. Run: airlayer query --execute --config config.yml \
     --dimensions events.platform --measures events.total_revenue
3. Inspect the envelope:
   - status: did it work?
   - sql: what did the semantic layer compile?
   - data: are the values correct?
   - error: what went wrong?
   - views_used: which files need editing?
4. Edit the .view.yml file(s)
5. Repeat from step 2
```

The agent never sees or writes SQL directly. It reads the compiled SQL in the envelope for debugging purposes, but its edits are always to the semantic layer definitions.

## Envelope Format

Every `--execute` invocation outputs exactly one JSON object to stdout:

```json
{
  "status": "success",
  "sql": "SELECT \"events\".\"platform\" AS \"events__platform\", COUNT(*) AS \"events__total_events\" FROM \"events\" GROUP BY 1",
  "columns": [
    {"name": "events__platform", "member": "events.platform", "kind": "dimension"},
    {"name": "events__total_events", "member": "events.total_events", "kind": "measure"}
  ],
  "data": [
    {"events__platform": "web", "events__total_events": 8},
    {"events__platform": "ios", "events__total_events": 3},
    {"events__platform": "android", "events__total_events": 1}
  ],
  "row_count": 3,
  "views_used": ["events"],
  "error": null
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `status` | string | `"success"`, `"parse_error"`, `"compile_error"`, or `"execution_error"` |
| `sql` | string? | The compiled SQL. Present when compilation succeeded, even if execution failed. Null on parse errors. |
| `columns` | array | Column metadata: SQL alias (`name`), semantic path (`member`), and `kind` (dimension/measure/time_dimension). |
| `data` | array | Result rows as JSON objects, keyed by column name. Capped at 50 rows. |
| `row_count` | int | Total rows returned by the database. May exceed `data.length`. |
| `views_used` | array | Names of `.view.yml` views referenced by this query. Tells the agent which files to edit. |
| `error` | string? | Error message when `status` is not `"success"`. |

### Error Stages

The `status` field encodes where the failure occurred:

- **`parse_error`** — The `.view.yml` files are malformed, the globals file can't be loaded, or the query input is invalid. The agent should fix the YAML syntax or structure.
- **`compile_error`** — The query references a member path that doesn't exist, the join graph can't be resolved, or views have conflicting dialects. The agent should check member names and entity relationships.
- **`execution_error`** — The SQL was generated but the database rejected it. Common causes: wrong column names in `expr`, type mismatches, missing tables. The `sql` field is populated so the agent can see exactly what was sent. The agent should fix the `expr` fields in the view.

## Why Not Arbitrary SQL?

The semantic layer exists precisely to prevent this. Arbitrary SQL execution:

- Breaks the contract between business logic and database schema
- Makes the agent's work non-reproducible (SQL is tied to a specific schema, the semantic layer is not)
- Opens the door to destructive queries (DROP, DELETE, UPDATE)
- Produces results without semantic context (no column-to-member mapping, no dimension/measure classification)

The `sql` field in the envelope is read-only context for debugging. The agent's writes go to `.view.yml`, never to SQL.

## Feature Flags

The execution capability is opt-in via Cargo feature flags:

```toml
# Library users — zero driver deps, just the semantic engine
airlayer = { version = "0.1", default-features = false }

# CLI with specific drivers
airlayer = { version = "0.1", features = ["exec-postgres", "exec-snowflake"] }

# CLI with all drivers
airlayer = { version = "0.1", features = ["exec"] }
```

Available flags: `exec-postgres`, `exec-snowflake`, `exec-duckdb`, `exec` (all).

## Config Format

Database connections are defined in `config.yml`:

```yaml
databases:
  - name: warehouse
    type: postgres
    host: localhost
    port: "5432"
    user: analytics
    password_var: PG_PASSWORD    # resolved from environment variable
    database: prod

  - name: snowflake_wh
    type: snowflake
    account: abc12345
    username: analyst
    password_var: SNOWFLAKE_PASSWORD
    warehouse: COMPUTE_WH
    database: PROD_DB
    schema: PUBLIC
    role: ANALYST

  - name: local
    type: duckdb
    file_search_path: ./data/    # auto-loads CSV/Parquet as tables
```

Sensitive values support `_var` suffix for environment variable indirection (e.g., `password_var: PG_PASSWORD` reads from `$PG_PASSWORD`). Direct `password` values are also accepted but discouraged in committed config files.

## Data Profiling

`airlayer inspect --profile` runs type-aware data profiling against the database. This lets agents discover valid filter values and data ranges without hardcoding enums in `.view.yml` files. See [PHILOSOPHY.md](../PHILOSOPHY.md#data-profiling-over-hardcoded-enums) for the rationale.

```bash
# Profile a single dimension
airlayer inspect --profile events.platform \
  --config config.yml --dialect bigquery

# Profile all dimensions in a view
airlayer inspect --profile events \
  --config config.yml --dialect bigquery
```

Profile output varies by dimension type:

**String** (cardinality ≤ 100 → full value list; >100 → top 20):
```json
{
  "member": "events.platform",
  "type": "string",
  "profile": {
    "cardinality": 3,
    "total_rows": 12,
    "null_count": 0,
    "values": ["web", "ios", "android"],
    "top_values": [
      {"value": "web", "count": 7},
      {"value": "ios", "count": 3},
      {"value": "android", "count": 2}
    ]
  }
}
```

**Number**:
```json
{
  "member": "events.revenue",
  "type": "number",
  "profile": {
    "min": 0, "max": 99.99, "mean": 15.83,
    "distinct_count": 5, "null_count": 0, "total_rows": 12
  }
}
```

**Date/Datetime**:
```json
{
  "member": "events.created_at",
  "type": "datetime",
  "profile": {
    "min": "2025-01-15T10:00:00Z", "max": "2025-01-17T16:00:00Z",
    "null_count": 0, "total_rows": 12
  }
}
```

**Boolean**:
```json
{
  "member": "events.is_active",
  "type": "boolean",
  "profile": {
    "true_count": 10, "false_count": 2,
    "null_count": 0, "total_rows": 12
  }
}
```

## CLI Usage

```bash
# Compile only (human use, pipe to psql, etc.)
airlayer query --config config.yml \
  --dimensions events.platform --measures events.total_revenue

# Compile + execute (agent use, structured envelope)
airlayer query --execute --config config.yml \
  --dimensions events.platform --measures events.total_revenue

# Target a specific datasource
airlayer query --execute --config config.yml --datasource snowflake_wh \
  --dimensions events.platform --measures events.total_revenue

# Profile dimensions before querying
airlayer inspect --profile events --config config.yml

# Full query JSON (agent use)
airlayer query --execute --config config.yml -q '{
  "dimensions": ["events.platform"],
  "measures": ["events.total_revenue"],
  "filters": [{"member": "events.platform", "operator": "equals", "values": ["web"]}]
}'
```
