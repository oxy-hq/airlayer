# Architecture

airlayer is structured as a pipeline: **parse** -> **resolve** -> **plan** -> **generate**.

## Pipeline stages

### 1. Parse (`schema/parser.rs`)

YAML `.view.yml` files are deserialized into `View` structs. The parser handles:

- Single file parsing (`parse_view_file`)
- Directory scanning (`parse_views`, `parse_directory`)
- Globals inheritance resolution — `inherits_from: globals.semantics.dimensions.X` merges fields from a shared globals file
- Topic file parsing (`.topic.yml`)

The result is a `SemanticLayer` containing all views and topics.

### 2. Resolve (`engine/evaluator.rs`, `engine/join_graph.rs`)

**SchemaEvaluator** indexes the semantic layer for fast member lookups:

- `view_name.member_name` path resolution
- Distinguishes dimensions vs measures
- Provides view metadata (table, datasource, entities)

**JoinGraph** builds a petgraph from entity declarations across views:

- Primary entities become nodes
- Foreign-to-primary matches create edges
- BFS pathfinding finds shortest join paths between any two views
- Edge metadata carries relationship type (OneToOne, ManyToOne, OneToMany) and join keys

### 3. Plan (`engine/sql_generator.rs` — first half)

The `SqlGenerator` determines:

- **Base view selection**: Cost-based — picks the view that minimizes total join tree depth
- **Join planning**: Uses BFS on the JoinGraph to find paths between referenced views
- **Fan-out detection**: Identifies OneToMany joins that would multiply rows
- **CTE restructuring**: When fan-out is detected, measures from multiplied views are pre-aggregated in CTEs
- **Filter routing**: Dimension filters go to WHERE; measure filters go to HAVING

### 4. Generate (`engine/sql_generator.rs` — second half)

Produces the final SQL string:

- SELECT clause with dialect-specific quoting and aliasing
- FROM with table alias
- JOIN clauses with ON conditions (composite key support)
- WHERE / HAVING with parameterized values
- GROUP BY (positional references)
- ORDER BY
- LIMIT / OFFSET

Expression processing (`engine/member_sql.rs`, `engine/sql_generator.rs`) handles:

- `{{ entity.field }}` cross-entity references resolved to qualified column expressions
- `{{ variables.X }}` preserved as-is for runtime substitution
- `{{ TABLE }}` resolved to the view's table alias
- Column auto-qualification with table alias (see [Column qualification](#column-qualification) below)

The `{{ }}` syntax is Jinja-inspired but is **not** Jinja — there is no template engine. References are resolved by airlayer's own regex-based resolver (`MemberSqlResolver`) with recursive resolution (a measure can reference other measures) and priority-based lookups (variables → measures → dimensions → entities).

## Module map

```
src/
├── cli/mod.rs              CLI entry (clap)
├── dialect/
│   └── mod.rs              Dialect enum + per-dialect SQL functions
├── engine/
│   ├── mod.rs              SemanticEngine orchestrator
│   ├── evaluator.rs        Schema indexing and member lookup
│   ├── join_graph.rs       Entity relationship graph (petgraph + BFS)
│   ├── member_sql.rs       Expression reference resolution ({{entity.field}}, {{TABLE}}, etc.)
│   ├── query.rs            Request/response types, filter operators
│   ├── sql_generator.rs    SQL generation pipeline
│   └── error.rs            Error types
├── schema/
│   ├── models.rs           Core data model types
│   ├── parser.rs           YAML parser with globals resolution
│   ├── validator.rs        Schema validation
│   └── globals.rs          Globals file parsing
├── lib.rs                  Public API exports
└── main.rs                 CLI main
```

### 5. Execute (`executor/` — optional)

When `--execute` is passed, the compiled SQL is dispatched to a real database via the `executor` module. This is gated behind `exec-*` feature flags so the core engine has zero driver dependencies.

- **Postgres/Redshift** — `libpq` via the `postgres` crate
- **Snowflake** — REST API via `ureq` (session auth + query submission)
- **DuckDB** — in-process via the `duckdb` crate, with auto-loading of CSV/Parquet files

Results are wrapped in a `QueryEnvelope` — a structured JSON object with status, SQL, column metadata, data (capped at 50 rows), and error context. See [agent-execution.md](agent-execution.md) for the full spec.

## Module map

```
src/
├── cli/mod.rs              CLI entry (clap)
├── dialect/
│   └── mod.rs              Dialect enum + per-dialect SQL functions
├── engine/
│   ├── mod.rs              SemanticEngine orchestrator
│   ├── evaluator.rs        Schema indexing and member lookup
│   ├── join_graph.rs       Entity relationship graph (petgraph + BFS)
│   ├── member_sql.rs       Expression reference resolution ({{entity.field}}, {{TABLE}}, etc.)
│   ├── query.rs            Request/response types, filter operators
│   ├── sql_generator.rs    SQL generation pipeline
│   └── error.rs            Error types
├── executor/               Database executors (feature-gated)
│   ├── mod.rs              QueryEnvelope, DatabaseConnection, dispatch
│   ├── postgres.rs         Postgres/Redshift executor
│   ├── snowflake.rs        Snowflake REST API executor
│   └── duckdb.rs           DuckDB in-process executor
├── schema/
│   ├── models.rs           Core data model types
│   ├── parser.rs           YAML parser with globals resolution
│   ├── validator.rs        Schema validation
│   └── globals.rs          Globals file parsing
├── lib.rs                  Public API exports
└── main.rs                 CLI main
```

## Column qualification

When a dimension or measure expression references bare column names, the SQL generator must qualify them with the view's table alias to avoid ambiguity in multi-view joins. This is handled by `qualify_bare_columns()` in `sql_generator.rs`.

### Why not a SQL parser?

We considered using the `sqlparser` Rust crate but chose a hand-rolled single-pass tokenizer instead. The reasons:

1. **Expressions aren't valid SQL statements.** A dimension expr like `amount * 2` isn't a standalone SELECT — sqlparser would reject it without wrapping hacks (`SELECT amount * 2` → parse → extract → unparse).
2. **Template patterns aren't SQL.** Expressions can contain `{{entity.field}}`, `{{TABLE}}`, and `{{variables.X}}` — these must be resolved before any SQL parser could handle them, so you'd need custom pre-processing regardless.
3. **Cube.js does the same thing.** Cube's `autoPrefixWithCubeName` uses a simple regex (`/^[_a-zA-Z][_a-zA-Z0-9]*$/`) to qualify plain column names. airlayer's approach is actually more capable — it qualifies individual tokens within complex expressions, not just bare single-identifier expressions.

### How it works

`qualify_bare_columns(expr, view_alias)` makes a single left-to-right pass over the expression string:

1. **Single-quoted strings** (`'...'`) — skipped entirely (these are string literals, not identifiers)
2. **Double-quoted identifiers** (`"Column"`) — qualified with the view alias unless already part of a dotted reference:
   - `"Date"` → `"view"."Date"` (bare identifier, needs qualification)
   - `"schema"."col"` → left as-is (`"schema"` is followed by `.`, `"col"` is preceded by `.`)
3. **Unquoted identifiers** — qualified only if they match a known dimension name for the view, AND are not preceded by `.` (already qualified) or followed by `(` (function call)

This means SQL keywords like `COALESCE`, function names like `UPPER`, and unknown tokens pass through unqualified, while actual column references get the table alias prepended.

### Dialect-aware quoting

All qualification uses `dialect.quote_identifier()`, so the output uses the correct quoting style for the target database — double quotes for Postgres/DuckDB/Snowflake, backticks for MySQL, square brackets for SQL Server.

## Key design decisions

- **Compilation is the default, execution is opt-in**: The core engine is a pure compiler — schema + query → SQL + params. Database execution is a separate layer behind feature flags, so library consumers get zero driver dependencies.
- **Dialect from datasource**: Each view declares a `datasource` that maps to a database config entry. All views in a query must agree on dialect.
- **Entity-based joins**: Rather than explicit JOIN declarations, views declare entities (primary/foreign) and airlayer infers joins automatically.
- **Fan-out protection**: OneToMany joins are detected and handled with CTE pre-aggregation to prevent incorrect measure values.
- **Parameterized output**: Filter values are extracted as parameters, not inlined — preventing SQL injection and enabling prepared statements.
- **Structured envelopes**: Execution results are wrapped in a self-describing JSON envelope designed for machine consumption, not raw query output.

See [PHILOSOPHY.md](../PHILOSOPHY.md) for the full design rationale.
