// ---------------------------------------------------------------------------
// Public types — exported to consumers
// ---------------------------------------------------------------------------

/** Options for creating an AirlayerClient. */
export interface AirlayerClientOptions {
  /** Array of .view.yml file contents (YAML strings). Mutually exclusive with `foreign`. */
  views?: string[];
  /** Foreign semantic model files. Mutually exclusive with `views`. */
  foreign?: { format: string; files: string[] };
  /** SQL dialect (e.g., "postgres", "bigquery", "duckdb"). */
  dialect: string;
  /** Pre-aggregated data config (primary execution path). */
  preAggregates?: {
    /** URL to the manifest.json produced by `airlayer pull`. */
    manifest: string;
    /** URL prefix for Parquet files (e.g., "/data/aggregates/"). */
    baseUrl: string;
  };
  /** Proxy URL for live query fallback. Optional. */
  proxy?: string;
  /** Custom fetch function for auth headers, credentials, etc. */
  fetch?: typeof globalThis.fetch;
  /** Explicit URL to the airlayer WASM binary. Auto-detected if omitted. */
  wasmUrl?: string;
}

/** Semantic query request (mirrors the Rust QueryRequest). */
export interface QueryRequest {
  measures?: string[];
  dimensions?: string[];
  filters?: QueryFilter[];
  segments?: string[];
  time_dimensions?: TimeDimensionQuery[];
  order?: OrderBy[];
  limit?: number;
  offset?: number;
  timezone?: string;
  ungrouped?: boolean;
  motif?: string;
  motif_params?: Record<string, unknown>;
}

export interface QueryFilter {
  member?: string;
  operator?: string;
  values?: string[];
  and?: QueryFilter[];
  or?: QueryFilter[];
}

export interface TimeDimensionQuery {
  dimension: string;
  granularity?: string;
  date_range?: string[];
}

export interface OrderBy {
  id: string;
  desc: boolean;
}

/** Result of `client.query()`. Always has `sql`; has rows only when `ok` is true. */
export interface QueryResult {
  ok: boolean;
  rows: Record<string, unknown>[];
  columns: ColumnMeta[];
  sql: string;
  source: 'pre_aggregate' | 'proxy' | null;
  warning: string | null;
}

export interface ColumnMeta {
  key: string;
  type: string;
}

/** Result of `client.sql()`. Compile-only, no execution. */
export interface CompileResult {
  sql: string;
  params: string[];
  columns: ColumnMeta[];
}

/** Entry from `client.inspect()`. */
export interface CatalogEntry {
  kind: string;
  name: string;
  description?: string;
  view?: string;
  member_type?: string;
  meta?: Record<string, string[]>;
}

// ---------------------------------------------------------------------------
// Internal types — used between SDK components, not exported to consumers
// ---------------------------------------------------------------------------

/** Abstraction over the airlayer WASM module. */
export interface WasmModule {
  compile(
    viewsYaml: string[],
    queryJson: string,
    dialect: string,
    topicsYaml?: string[],
    motifsYaml?: string[],
    queriesYaml?: string[],
  ): WasmCompileResult;
  compile_foreign?(
    format: string,
    files: string[],
    queryJson: string,
    dialect: string,
  ): WasmCompileResult;
  cache_resolve(manifestJson: string, queryJson: string): WasmCacheResolution | null;
  catalog_list(
    viewsYaml: string[],
    topicsYaml?: string[],
    motifsYaml?: string[],
    queriesYaml?: string[],
  ): WasmCatalogEntry[];
}

/** Raw compile result from WASM (before SDK transforms it). */
export interface WasmCompileResult {
  sql: string;
  params: string[];
  columns: { member: string; alias: string; kind: string }[];
}

/** Cache resolution result from WASM. */
export interface WasmCacheResolution {
  reagg_sql: string;
  cache_key: string;
  entry: Record<string, unknown>;
}

/** Catalog entry from WASM. */
export interface WasmCatalogEntry {
  kind: string;
  name: string;
  description?: string;
  view?: string;
  member_type?: string;
}

/** Abstraction over DuckDB WASM for testability. */
export interface DuckDBEngine {
  init(): Promise<void>;
  loadParquet(tableName: string, data: Uint8Array): Promise<void>;
  execute(sql: string): Promise<{ rows: Record<string, unknown>[]; columns: ColumnMeta[] }>;
  dropTable(tableName: string): Promise<void>;
}

/** Abstraction over IndexedDB for testability. */
export interface StorageEngine {
  get(key: string): Promise<Uint8Array | undefined>;
  set(key: string, value: Uint8Array): Promise<void>;
}

/** Proxy response shape (matches the spec protocol). */
export interface ProxyResponse {
  rows: Record<string, unknown>[];
  columns: ColumnMeta[];
}

/** Injectable dependencies for testing. */
export interface ClientDeps {
  wasm: WasmModule;
  duckdb?: DuckDBEngine;
  storage?: StorageEngine;
  fetchFn?: typeof globalThis.fetch;
}
