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

Expression processing (`engine/member_sql.rs`) handles:

- `{{entity.field}}` cross-entity references resolved to qualified column expressions
- `{{variables.X}}` preserved as-is for runtime substitution
- `{TABLE}` resolved to the view's table alias
- Bare column auto-qualification with table alias

## Module map

```
src/
├── cli/mod.rs              CLI entry (clap)
├── dialect/
│   ├── mod.rs              Dialect enum + per-dialect SQL functions
│   └── templates.rs        SQL templates (minijinja)
├── engine/
│   ├── mod.rs              SemanticEngine orchestrator
│   ├── evaluator.rs        Schema indexing and member lookup
│   ├── join_graph.rs       Entity relationship graph (petgraph + BFS)
│   ├── member_sql.rs       Expression template resolution
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
│   ├── mod.rs              Dialect enum + per-dialect SQL functions
│   └── templates.rs        SQL templates (minijinja)
├── engine/
│   ├── mod.rs              SemanticEngine orchestrator
│   ├── evaluator.rs        Schema indexing and member lookup
│   ├── join_graph.rs       Entity relationship graph (petgraph + BFS)
│   ├── member_sql.rs       Expression template resolution
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

## Key design decisions

- **Compilation is the default, execution is opt-in**: The core engine is a pure compiler — schema + query → SQL + params. Database execution is a separate layer behind feature flags, so library consumers get zero driver dependencies.
- **Dialect from datasource**: Each view declares a `datasource` that maps to a database config entry. All views in a query must agree on dialect.
- **Entity-based joins**: Rather than explicit JOIN declarations, views declare entities (primary/foreign) and airlayer infers joins automatically.
- **Fan-out protection**: OneToMany joins are detected and handled with CTE pre-aggregation to prevent incorrect measure values.
- **Parameterized output**: Filter values are extracted as parameters, not inlined — preventing SQL injection and enabling prepared statements.
- **Structured envelopes**: Execution results are wrapped in a self-describing JSON envelope designed for machine consumption, not raw query output.

See [PHILOSOPHY.md](../PHILOSOPHY.md) for the full design rationale.
