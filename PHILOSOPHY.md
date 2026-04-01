# Philosophy

airlayer is a semantic engine — it sits between the question and the database. This document describes the design principles that drive the project.

## The semantic layer is the contract layer

The central idea: **the semantic layer is the only interface between consumers and the database**. No one — human or machine — writes or executes arbitrary SQL through airlayer. Instead, intent is expressed through the semantic vocabulary: dimensions, measures, filters, segments. airlayer compiles that intent into dialect-specific SQL.

This is a constraint, not a limitation. It means:

- **Business logic lives in one place.** Revenue is defined once in a `.view.yml` file, not reinvented in every query. Change the definition, and every consumer gets the updated logic.
- **The database schema is hidden.** Consumers reference `orders.total_revenue`, not `SUM(CASE WHEN status = 'completed' THEN amount END)`. Schema migrations don't break consumers.
- **Queries are reproducible.** A semantic query is portable across databases and dialects. The same `orders.total_revenue` compiles to Postgres, BigQuery, or Snowflake SQL depending on the view's datasource.
- **Destructive operations are impossible.** There is no path from the semantic vocabulary to DROP, DELETE, or UPDATE. The query surface is read-only by construction.

## Agents are not trusted operators

airlayer is designed to be operated by AI agents. The design draws from Justin Poehnelt's ["You Need to Rewrite Your CLI for AI Agents"](https://justin.poehnelt.com/posts/rewrite-your-cli-for-ai-agents/), which argues that agent-first interfaces need fundamentally different properties than human-first ones:

**Schema introspection replaces documentation.** An agent discovers dimensions, measures, and their types at runtime via `airlayer inspect --json`. It doesn't read docs or guess at field names — it queries the tool itself for its vocabulary.

**Structured envelopes over raw output.** Every `--execute` invocation returns a single JSON object with status, SQL, column metadata, data, and error context. The agent never parses free-form text. It reads `status` to know if the query worked, `sql` to understand what was compiled, `data` to verify results, and `views_used` to know which files to edit.

**Context window discipline.** Result data is capped at 50 rows. The `row_count` field always reflects the true total, so the agent knows cardinality without consuming its context budget on raw data.

**Input hardening.** The agent can only reference member paths defined in `.view.yml` files. Invalid paths produce clear `compile_error` responses with the specific member that failed. There is no path to SQL injection or arbitrary query execution — the semantic layer constrains the question space.

**Error stages encode failure location.** The `status` field tells the agent exactly which layer failed: `parse_error` (bad YAML), `compile_error` (bad member path), or `execution_error` (database rejected the SQL). Each stage implies a different fix — edit YAML structure, check member names, or fix `expr` fields.

## The iteration loop

The intended workflow for an agent iterating on semantic layer accuracy:

```
1. airlayer inspect --json                    → discover the vocabulary
1b. airlayer inspect --profile events.platform \
      --config config.yml --dialect bigquery   → profile dimensions for valid values/ranges
2. Read .view.yml files                        → understand current definitions
3. airlayer query --execute -c config.yml \
     --dimension X --measure Y               → compile + execute
4. Inspect the envelope:
   - status: did it work?
   - sql: what did the semantic layer compile?
   - data: are the values correct?
   - error: what went wrong?
   - views_used: which files need editing?
5. Edit the .view.yml file(s)
6. Repeat from step 3
```

The agent never sees or writes SQL directly. It reads the compiled SQL in the envelope for debugging, but its edits are always to the semantic layer definitions. The semantic layer is both the input and the output of the agent's work.

## Why not arbitrary SQL?

It's tempting to add `airlayer exec "SELECT ..."` as a convenience. We deliberately don't:

- **It breaks the contract.** The semantic layer exists to define business logic once. Arbitrary SQL bypasses that contract and produces results without semantic context — no column-to-member mapping, no dimension/measure classification, no `views_used`.
- **It's not reproducible.** SQL is tied to a specific schema. Semantic queries are not. An agent iterating on `.view.yml` files produces work that transfers across databases.
- **It opens destructive paths.** Even with read-only restrictions, the surface area of raw SQL is large. The semantic vocabulary is small and safe by construction.
- **The `sql` field is already there.** The envelope includes the compiled SQL for debugging. An agent (or human) that needs to see exactly what ran can read it. But the edits go to `.view.yml`, not to SQL strings.

## Compilation is the default, execution is opt-in

airlayer is first a compiler. `airlayer query` compiles semantic queries to SQL and prints the result. No database connection required, no driver dependencies, no network calls.

Execution (`--execute`) is a separate capability, gated behind feature flags:

```toml
# Library — zero driver deps, just the semantic engine
airlayer = { version = "0.1", default-features = false }

# CLI with specific drivers
airlayer = { version = "0.1", features = ["exec-postgres", "exec-snowflake"] }
```

This keeps the core crate light. A library consumer embedding airlayer for SQL generation pays zero cost for database drivers. The `exec-*` flags are for the CLI's agent-facing execution mode.

## Single tool, not a pipeline

The agent-facing interface is a single command: `airlayer query --execute`. It compiles and executes in one step, returning a self-contained envelope. We considered separating compilation and execution into pipeable utilities (`airlayer compile | airlayer exec`), but the single-tool design is better for agents:

- **One tool call, one envelope.** The agent gets everything it needs — SQL, metadata, data, errors — in a single invocation. No orchestration between separate tools.
- **Error context is unified.** A `compile_error` and an `execution_error` use the same envelope shape. The agent doesn't need different error-handling paths for different tools.
- **The compile-only path still exists.** `airlayer query` (without `--execute`) prints raw SQL for humans to pipe to `psql` or inspect directly. The agent path and the human path are the same command with a flag difference.

## Dialect from datasource, not from flag

Each view declares which database it targets via `datasource` (resolved through `config.yml`) or `dialect` (inline). airlayer doesn't have a global dialect setting — it's always per-view, because the semantic layer should be self-describing.

When a query spans multiple views, all referenced views must agree on dialect. This is enforced at compile time. The `-d` CLI flag exists as an override for testing, not as the primary resolution mechanism.

## Data profiling over hardcoded enums

A common question for semantic layers: how does the agent know valid filter values? You could hardcode enum lists in `.view.yml` — but those go stale as data changes, and maintaining them is error-prone.

airlayer takes a different approach: **the agent profiles the data at runtime.** `airlayer inspect --profile events.platform` runs type-aware SQL against the actual database and returns:

- **String dimensions**: cardinality, distinct values (if cardinality ≤ 100), top values by frequency
- **Number dimensions**: min, max, mean, distinct count
- **Date/datetime dimensions**: min (earliest), max (latest), null count
- **Boolean dimensions**: true/false/null counts

This is better than hardcoded enums because:

- **Values are always fresh.** The profile reflects the current state of the data, not what someone remembered to type into a YAML file.
- **Type-appropriate summaries.** A number dimension gets min/max/mean — telling the agent the range. A date dimension gets the data's time span. These help the agent construct sensible filters without trial and error.
- **Cardinality awareness.** High-cardinality string dimensions (>100 values) return only the top-N by frequency, not the full list. The agent learns "this is a high-cardinality field, don't enumerate it" — which is actionable information a static enum can't convey.
- **The `samples` field still exists.** For dimensions where the author wants to give the agent illustrative examples without hitting the database, `samples` in `.view.yml` serves that purpose. Profile is the dynamic complement.

The tradeoff is that profiling requires a database connection (`--config`) and is slower than reading a YAML field. This is acceptable because the agent profiles once per session, not per query — and the information it gets is accurate.

## Entity-based joins, not explicit SQL

Views declare entities (primary/foreign), and airlayer infers JOINs automatically via BFS on the entity graph. This is deliberate:

- **Joins are derivable.** If `orders` has a foreign `customer` entity and `customers` has a primary `customer` entity, the join condition is fully determined. Declaring it again in SQL is redundant.
- **Multi-hop is free.** Once entities are declared, transitive joins (A → B → C) work without additional configuration.
- **The agent doesn't need to understand joins.** It references `customers.name` and `orders.total_revenue` in the same query, and airlayer figures out how to connect them.
