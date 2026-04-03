# Library Usage

airlayer can be used as a library in three languages: Python, JavaScript/TypeScript (via WASM), and Rust. All three expose the same core API — `compile()` and `validate()`.

## Python

### Install

```bash
pip install airlayer
```

### compile()

Compiles a semantic query to SQL.

```python
import airlayer

result = airlayer.compile(
    views_yaml=[open("orders.view.yml").read()],
    query_json='{"measures": ["orders.total_revenue"], "dimensions": ["orders.status"]}',
    dialect="postgres",
)

result["sql"]      # SELECT ... FROM ...
result["params"]   # ["active"] — bind parameters
result["columns"]  # [{"member": "orders.status", "alias": "orders__status", "kind": "Dimension"}, ...]
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `views_yaml` | `list[str]` | `.view.yml` file contents (YAML strings) |
| `query_json` | `str` | Query as JSON (same format as `airlayer query -q`) |
| `dialect` | `str` | SQL dialect: `postgres`, `bigquery`, `snowflake`, `duckdb`, `mysql`, `clickhouse`, `redshift`, `databricks`, `sqlite`, `domo` |
| `topics_yaml` | `list[str]` | Optional `.topic.yml` file contents |
| `motifs_yaml` | `list[str]` | Optional `.motif.yml` file contents (custom motifs) |
| `queries_yaml` | `list[str]` | Optional `.query.yml` file contents (saved queries) |

Returns a `dict` with `sql`, `params`, and `columns` keys. Raises `ValueError` on invalid input.

### validate()

Validates view YAML without compiling a query.

```python
airlayer.validate(views_yaml=[open("orders.view.yml").read()])  # True
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `views_yaml` | `list[str]` | `.view.yml` file contents (YAML strings) |
| `topics_yaml` | `list[str]` | Optional `.topic.yml` file contents |

Returns `True` on success. Raises `ValueError` on error.

### Full example

```python
import airlayer
import json

view = """
name: orders
table: public.orders
dimensions:
  - name: status
    type: string
    expr: status
  - name: created_at
    type: datetime
    expr: created_at
measures:
  - name: total_revenue
    type: sum
    expr: amount
  - name: count
    type: count
"""

# Simple query
result = airlayer.compile(
    views_yaml=[view],
    query_json=json.dumps({
        "measures": ["orders.total_revenue"],
        "dimensions": ["orders.status"],
        "filters": [{"member": "orders.status", "operator": "equals", "values": ["active"]}],
        "limit": 10,
    }),
    dialect="postgres",
)
print(result["sql"])

# With time dimension and motif
result = airlayer.compile(
    views_yaml=[view],
    query_json=json.dumps({
        "measures": ["orders.total_revenue"],
        "time_dimensions": [{"dimension": "orders.created_at", "granularity": "month"}],
        "motif": "mom",
    }),
    dialect="bigquery",
)
print(result["sql"])
```

## JavaScript / TypeScript (WASM)

### Install

```bash
npm install airlayer
```

### Browser

```js
import init, { compile, validate } from 'airlayer';

await init();

const ordersView = `
name: orders
table: public.orders
dimensions:
  - name: status
    type: string
    expr: status
measures:
  - name: total_revenue
    type: sum
    expr: amount
`;

const result = compile(
  [ordersView],
  JSON.stringify({
    measures: ['orders.total_revenue'],
    dimensions: ['orders.status'],
  }),
  'postgres'
);

console.log(result.sql);
console.log(result.columns);
```

### Node.js

Build with `just build-wasm-node`, then:

```js
const { compile, validate } = require('airlayer');

const result = compile([viewYaml], queryJson, 'postgres');
```

### API

**`compile(views, query, dialect, topics?, motifs?, queries?)`**

| Parameter | Type | Description |
|-----------|------|-------------|
| `views` | `string[]` | `.view.yml` file contents (YAML strings) |
| `query` | `string` | Query as JSON |
| `dialect` | `string` | SQL dialect |
| `topics` | `string[]?` | Optional `.topic.yml` contents |
| `motifs` | `string[]?` | Optional `.motif.yml` contents |
| `queries` | `string[]?` | Optional `.query.yml` contents |

Returns `{ sql: string, params: string[], columns: Column[] }`. Throws on error.

**`validate(views, topics?)`**

Returns `true` on success. Throws on error.

## Rust

Add to `Cargo.toml`:

```toml
[dependencies]
airlayer = { git = "https://github.com/oxy-hq/airlayer" }
```

### Example

```rust
use airlayer::{SemanticEngine, DatasourceDialectMap, Dialect};
use airlayer::schema::parser::SchemaParser;
use airlayer::schema::models::SemanticLayer;
use airlayer::engine::query::QueryRequest;

let parser = SchemaParser::new();
let view = parser.parse_view_str(include_str!("orders.view.yml"), "orders").unwrap();
let layer = SemanticLayer::new(vec![view], None);

let mut dialect_map = DatasourceDialectMap::new();
dialect_map.set_default(Dialect::Postgres);

let engine = SemanticEngine::from_semantic_layer(layer, dialect_map).unwrap();

let request: QueryRequest = serde_json::from_str(r#"{
    "measures": ["orders.total_revenue"],
    "dimensions": ["orders.status"]
}"#).unwrap();

let result = engine.compile_query(&request).unwrap();
println!("{}", result.sql);
```

## Query format

All three interfaces accept the same query JSON format. See [query-api.md](query-api.md) for the full reference.

```json
{
  "measures": ["orders.total_revenue"],
  "dimensions": ["orders.status"],
  "time_dimensions": [
    { "dimension": "orders.created_at", "granularity": "month" }
  ],
  "filters": [
    { "member": "orders.status", "operator": "equals", "values": ["active"] }
  ],
  "order": [{ "id": "orders.total_revenue", "desc": true }],
  "limit": 100,
  "motif": "contribution"
}
```

## Supported dialects

`postgres`, `bigquery`, `snowflake`, `duckdb`, `mysql`, `clickhouse`, `redshift`, `databricks`, `sqlite`, `domo`
