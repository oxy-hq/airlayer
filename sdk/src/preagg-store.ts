import type {
  ColumnMeta,
  DuckDBEngine,
  StorageEngine,
  WasmCacheResolution,
} from './types';

interface PreAggregateStoreOptions {
  manifest: string; // manifest JSON string
  baseUrl: string;
  duckdb: DuckDBEngine;
  storage: StorageEngine;
  fetchFn: typeof globalThis.fetch;
  cacheResolve: (manifestJson: string, queryJson: string) => WasmCacheResolution | null;
}

export interface PreAggResult {
  rows: Record<string, unknown>[];
  columns: ColumnMeta[];
}

export class PreAggregateStore {
  private manifest: string;
  private baseUrl: string;
  private duckdb: DuckDBEngine;
  private storage: StorageEngine;
  private fetchFn: typeof globalThis.fetch;
  private cacheResolve: (m: string, q: string) => WasmCacheResolution | null;

  constructor(options: PreAggregateStoreOptions) {
    this.manifest = options.manifest;
    this.baseUrl = options.baseUrl;
    this.duckdb = options.duckdb;
    this.storage = options.storage;
    this.fetchFn = options.fetchFn;
    this.cacheResolve = options.cacheResolve;
  }

  /**
   * Try to serve a query from pre-aggregated data.
   * Returns null if no rollup covers the query.
   */
  async execute(queryJson: string): Promise<PreAggResult | null> {
    const resolution = this.cacheResolve(this.manifest, queryJson);
    if (!resolution) return null;

    const parquetData = await this.getParquet(resolution.cache_key);
    try {
      await this.duckdb.loadParquet('__cache', parquetData);
      const result = await this.duckdb.execute(resolution.reagg_sql);
      return result;
    } finally {
      await this.duckdb.dropTable('__cache');
    }
  }

  private async getParquet(cacheKey: string): Promise<Uint8Array> {
    // Check IndexedDB first
    const cached = await this.storage.get(cacheKey);
    if (cached) return cached;

    // Fetch from network
    const url = `${this.baseUrl}${cacheKey}.parquet`;
    const response = await this.fetchFn(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch pre-aggregate: ${url} (${response.status})`);
    }

    const buffer = await response.arrayBuffer();
    const data = new Uint8Array(buffer);

    // Store in IndexedDB for next time
    await this.storage.set(cacheKey, data);

    return data;
  }
}
