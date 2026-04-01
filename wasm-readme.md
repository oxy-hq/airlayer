# airlayer

Compile [`.view.yml`](https://github.com/oxy-hq/airlayer) semantic layer definitions into dialect-specific SQL — in the browser or Node.js via WebAssembly.

## Install

```bash
npm install airlayer
```

## Usage (browser)

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
    filters: [{ member: 'orders.status', operator: 'equals', values: ['active'] }],
    limit: 10,
  }),
  'postgres'
);

console.log(result.sql);
// SELECT
//   "orders".status AS "orders__status",
//   SUM("orders".amount) AS "orders__total_revenue"
// FROM public.orders AS "orders"
// WHERE ("orders".status = 'active')
// GROUP BY 1
// LIMIT 10

console.log(result.columns);
// [{ member: 'orders.status', alias: 'orders__status', kind: 'Dimension' },
//  { member: 'orders.total_revenue', alias: 'orders__total_revenue', kind: 'Measure' }]
```

## Usage (Node.js)

Build the package with the `nodejs` target:

```bash
# from the airlayer repo
just build-wasm-node
```

Then:

```js
const { compile, validate } = require('airlayer');

const result = compile([ordersViewYaml], queryJson, 'postgres');
```

## API

### `compile(views, query, dialect, topics?, motifs?, queries?)`

Compiles a semantic query to SQL.

| Parameter | Type | Description |
|-----------|------|-------------|
| `views` | `string[]` | Array of `.view.yml` file contents (YAML strings) |
| `query` | `string` | Query as JSON (same format as `airlayer query -q`) |
| `dialect` | `string` | SQL dialect: `postgres`, `bigquery`, `snowflake`, `duckdb`, `mysql`, `clickhouse`, `redshift`, `databricks`, `sqlite`, `domo` |
| `topics` | `string[]?` | Optional array of `.topic.yml` file contents |
| `motifs` | `string[]?` | Optional array of `.motif.yml` file contents (custom motifs) |
| `queries` | `string[]?` | Optional array of `.query.yml` file contents |

**Returns** `{ sql: string, params: string[], columns: Column[] }`

### `validate(views, topics?)`

Validates view YAML without compiling a query.

| Parameter | Type | Description |
|-----------|------|-------------|
| `views` | `string[]` | Array of `.view.yml` file contents (YAML strings) |
| `topics` | `string[]?` | Optional array of `.topic.yml` file contents |

**Returns** `true` on success, throws on error.

## Query format

The `query` JSON supports the same fields as the airlayer CLI `-q` flag:

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
  "order": [{ "member": "orders.total_revenue", "direction": "desc" }],
  "limit": 100
}
```

See the [query format docs](https://github.com/oxy-hq/airlayer/blob/main/docs/query-api.md) and [schema format docs](https://github.com/oxy-hq/airlayer/blob/main/docs/schema-format.md) for full reference.

## License

Apache-2.0
