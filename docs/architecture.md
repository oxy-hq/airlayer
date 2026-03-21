# Architecture

o3 is structured as a pipeline: **parse** -> **resolve** -> **plan** -> **generate**.

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

## Key design decisions

- **No runtime dependencies**: o3 is a pure compiler — it takes schema + query and produces SQL + params. No database connections, no caching, no HTTP server.
- **Dialect from datasource**: Each view declares a `datasource` that maps to a database config entry. All views in a query must agree on dialect.
- **Entity-based joins**: Rather than explicit JOIN declarations, views declare entities (primary/foreign) and o3 infers joins automatically.
- **Fan-out protection**: OneToMany joins are detected and handled with CTE pre-aggregation to prevent incorrect measure values.
- **Parameterized output**: Filter values are extracted as parameters, not inlined — preventing SQL injection and enabling prepared statements.
