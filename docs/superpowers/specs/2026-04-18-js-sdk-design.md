# airlayer JS SDK — Design Spec

## Problem

airlayer compiles semantic queries to SQL and has pre-aggregation cache logic, all available as WASM. But there's no cohesive JavaScript SDK that ties these together into a usable data plane for browser apps. Today, building a data app requires manually wiring WASM initialization, DuckDB WASM setup, cache management, and proxy fallback. This is the missing layer between "airlayer can compile queries" and "I can build a data app."

## Positioning

- **airlayer** is the platform — semantic layer, query compilation, pre-aggregation, execution. Everything needed to build a data app, as a library.
- **airapp** is one opinionated consumer — a no-code YAML-to-dashboard tool that imports airlayer. It is not the only way to build apps.
- **The JS SDK** is the browser interface to the platform. It wraps the WASM exports into a single `AirlayerClient` class with a simple `query()` API.

## Design

### Public API

```typescript
import { AirlayerClient } from 'airlayer';

const client = new AirlayerClient({
  // Semantic layer — one of:
  views: [viewYaml, viewYaml2],
  // OR
  foreign: { format: 'lookml', files: [viewLkml, exploreLkml] },

  dialect: 'postgres',

  // Pre-aggregated data (primary execution path)
  preAggregates: {
    manifest: '/data/manifest.json',  // URL to manifest
    baseUrl: '/data/aggregates/',     // URL prefix for Parquet files
  },

  // Live query proxy (fallback for uncovered queries)
  proxy: '/api/query',

  // Custom fetch (optional — for auth headers, credentials, etc.)
  fetch: (url, init) => fetch(url, { ...init, headers: { Authorization: `Bearer ${token}` } }),
});

await client.init();
```

#### `client.query(request)`

Compile and execute a semantic query. Returns rows.

```typescript
const result = await client.query({
  measures: ['orders.total_revenue'],
  dimensions: ['orders.status'],
  filters: [{ member: 'orders.region', operator: 'equals', values: ['US'] }],
});

// result.ok      → true if data was returned, false if query couldn't execute
// result.rows    → [{ orders__status: 'active', orders__total_revenue: 1234 }, ...]
// result.columns → [{ key: 'orders__status', type: 'string' }, ...]
// result.sql     → the compiled SQL (always present, even on failure)
// result.source  → 'pre_aggregate' | 'proxy' | null (if not executed)
// result.warning → string | null (e.g., 'Query not covered by pre-aggregates. Configure a proxy...')
```

**Query flow:**

1. Compile query to SQL via WASM (instant, in-browser)
2. Check pre-aggregates — call `cache_resolve(manifest, query)`. If a rollup covers the query:
   a. Fetch the Parquet file from `baseUrl + cache_key + '.parquet'` (if not already in IndexedDB)
   b. Load into DuckDB WASM as `"__cache"` table
   c. Execute the reagg SQL
   d. Return `{ ok: true, source: 'pre_aggregate', rows, columns, sql }`
3. If no pre-aggregate covers the query and proxy is configured:
   a. POST `{ sql, dialect, datasource }` to the proxy URL
   b. Proxy executes against the warehouse, returns `{ rows, columns }`
   c. Return `{ ok: true, source: 'proxy', rows, columns, sql }`
4. If no pre-aggregate and no proxy: return `{ ok: false, source: null, rows: [], columns: [], sql, warning }` — the warning explains what happened and includes the compiled SQL so the caller can display it or handle it gracefully

#### `client.inspect()`

List available views, dimensions, measures.

```typescript
const catalog = client.inspect();
// → [{ view: 'orders', dimensions: [...], measures: [...] }, ...]
```

Uses the existing `catalog_list` WASM export.

#### `client.sql(request)`

Compile a query to SQL without executing. For debugging or when the caller wants to handle execution themselves.

```typescript
const { sql, params } = client.sql({
  measures: ['orders.total_revenue'],
  dimensions: ['orders.status'],
});
```

### Initialization

`client.init()` does the following in order:

1. Initialize airlayer WASM module
2. Initialize DuckDB WASM (shared singleton)
3. If `preAggregates.manifest` is set, fetch the manifest JSON and store it in memory
4. Parse and validate the semantic layer (views or foreign files)

WASM modules are loaded lazily on first `init()` call. Subsequent `AirlayerClient` instances reuse the same WASM modules.

### Pre-Aggregates

Pre-aggregated data is the primary execution path. The SDK treats pre-aggregates as the default and the proxy as a fallback.

**Deployment workflow:**

```bash
# 1. Build rollup tables in your warehouse
airlayer build

# 2. Download as Parquet files
airlayer pull --output ./public/data/

# 3. Build and deploy your app
npm run build && deploy ./public
```

This produces:
```
public/data/
├── manifest.json
├── orders__a1b2c3d4.parquet
├── orders__e5f6g7h8.parquet
└── events__i9j0k1l2.parquet
```

The SDK fetches `manifest.json` on init. Individual Parquet files are fetched lazily on first query that needs them, then stored in IndexedDB for subsequent queries.

**Manifest format:** The existing `LocalManifest` JSON structure, as produced by `airlayer pull` and understood by the `cache_resolve` WASM function.

**Parquet file lifecycle:**
- Fetched from `baseUrl` via standard HTTP GET
- Stored in IndexedDB keyed by `cache_key` (e.g., `orders__a1b2c3d4`)
- Loaded into DuckDB WASM as a temporary `"__cache"` table for reagg SQL execution
- Table dropped after query completes
- IndexedDB entry persists across page loads

### Proxy Protocol

The proxy is a thin HTTP endpoint that executes SQL against a warehouse. The SDK defines the protocol; users implement it however they want (Express, serverless function, etc.).

**Request:** `POST <proxy_url>`
```json
{
  "sql": "SELECT status, SUM(amount) FROM orders GROUP BY 1",
  "dialect": "postgres",
  "datasource": "default"
}
```

**Response:** `200 OK`
```json
{
  "rows": [
    { "status": "active", "orders__total_revenue": 1234.56 }
  ],
  "columns": [
    { "key": "status", "type": "string" },
    { "key": "orders__total_revenue", "type": "number" }
  ]
}
```

**Error:** `4xx/5xx`
```json
{
  "error": "Connection refused"
}
```

The proxy is optional. If not configured and a query can't be served from pre-aggregates, `client.query()` throws an error that includes the compiled SQL, so the caller can handle it (e.g., show an error UI with "this query requires a live connection").

Documentation should emphasize: the proxy is a fallback for uncovered queries. In a well-configured deployment with good pre-aggregate coverage, the proxy should rarely be hit.

### Internal Architecture

```
AirlayerClient
├── WasmManager (singleton)
│   ├── airlayer-wasm module
│   └── init / compile / compile_foreign / cache_resolve / catalog_list
│
├── DuckDBManager (singleton)
│   ├── @duckdb/duckdb-wasm instance
│   ├── loadParquet(url) → register as table
│   └── execute(sql) → rows
│
├── PreAggregateStore
│   ├── manifest (in-memory, from init)
│   ├── IndexedDB (idb-keyval or raw)
│   ├── resolve(query) → { reaggSql, cacheKey } | null
│   ├── fetchAndStore(cacheKey) → Parquet bytes in IDB
│   └── execute(reaggSql, cacheKey) → rows (via DuckDBManager)
│
├── ProxyClient
│   ├── post(sql, dialect) → { rows, columns }
│   └── handles errors, timeouts
│
└── SemanticLayer
    ├── views (parsed YAML strings)
    ├── foreign files (if using foreign format)
    └── compile(query) → { sql, params }
```

### Package Structure

The npm package is `airlayer`. It ships:

```
airlayer/
├── index.js          — AirlayerClient class, TypeScript types
├── wasm/
│   ├── airlayer_bg.wasm
│   └── airlayer.js   — wasm-bindgen glue
└── package.json
```

**Dependencies:**
- `@duckdb/duckdb-wasm` — client-side SQL execution
- `idb-keyval` (or similar) — IndexedDB wrapper for Parquet storage

**No dependency on:** React, airapp, or any UI framework. This is a pure data layer.

**Feature-gated WASM builds:** The WASM binary can include only the foreign parsers needed:
- `airlayer` — core only (compile from .view.yml)
- `airlayer/lookml` — includes LookML parser
- `airlayer/cube` — includes Cube.js parser
- etc.

The exact packaging (subpath exports vs. separate packages) is an implementation detail. The key constraint: users who don't need foreign format support shouldn't pay the binary size cost.

### Error Handling

| Scenario | Behavior |
|----------|----------|
| WASM fails to load | `init()` rejects with error |
| Invalid view YAML | `init()` rejects with parse error |
| Query references unknown member | `query()` rejects with compilation error |
| Pre-aggregate miss, proxy configured | Falls through to proxy silently |
| Pre-aggregate miss, no proxy | `query()` rejects with error including compiled SQL |
| Proxy returns error | `query()` rejects with proxy error |
| Parquet fetch fails (404, network) | Falls through to proxy; if no proxy, rejects |
| DuckDB execution error | `query()` rejects with SQL error |

### What This Spec Does NOT Cover

- **UI components** — no React components, charts, or controls. That's airapp's domain.
- **Proxy implementation** — we define the protocol, not the server. Users bring their own.
- **S3/cloud storage integration** — the SDK fetches URLs. If files are on S3, point `baseUrl` at the bucket URL.
- **Authentication** — the SDK passes through to `fetch()`. Users can provide a custom `fetch` for auth headers.
- **Real-time / streaming** — queries are request/response. No WebSocket or polling.

### Future Considerations (Not In Scope)

- **airapp migration** — airapp should eventually consume this SDK instead of wiring its own WASM + DuckDB. This is a follow-up.
- **React hooks** — `useAirlayerQuery(request)` hook that returns `{ data, loading, error }`. Natural follow-up but not part of the core SDK.
- **Proxy reference implementation** — `airlayer serve --api` that implements the proxy protocol. Useful but not required for v1.
- **Incremental cache updates** — re-fetching only changed Parquet files. v1 fetches everything fresh on `init()`.
