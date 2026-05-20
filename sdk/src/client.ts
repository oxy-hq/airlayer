import type {
  AirlayerClientOptions,
  QueryRequest,
  QueryResult,
  CompileResult,
  ColumnMeta,
  CatalogEntry,
  ClientDeps,
  WasmModule,
  DuckDBEngine,
  StorageEngine,
} from './types';
import { SemanticLayer } from './semantic-layer';
import { PreAggregateStore } from './preagg-store';
import { ProxyClient } from './proxy-client';
import { loadWasm } from './wasm-manager';

export class AirlayerClient {
  private options: AirlayerClientOptions;
  private deps?: ClientDeps;

  private semanticLayer: SemanticLayer | null = null;
  private preaggStore: PreAggregateStore | null = null;
  private proxyClient: ProxyClient | null = null;
  private initialized = false;

  constructor(options: AirlayerClientOptions, deps?: ClientDeps) {
    this.options = options;
    this.deps = deps;
  }

  async init(): Promise<void> {
    if (this.initialized) return;

    // 1. Load WASM
    const wasm: WasmModule = this.deps?.wasm ?? (await loadWasm(this.options.wasmUrl));

    // 2. Build semantic layer
    this.semanticLayer = new SemanticLayer(wasm, {
      views: this.options.views,
      foreign: this.options.foreign,
    });

    // 3. Set up pre-aggregate store (if configured)
    if (this.options.preAggregates) {
      const fetchFn = this.deps?.fetchFn ?? this.options.fetch ?? globalThis.fetch;

      // Fetch manifest
      const manifestResponse = await fetchFn(this.options.preAggregates.manifest);
      if (!manifestResponse.ok) {
        throw new Error(
          `Failed to fetch pre-aggregate manifest: ${this.options.preAggregates.manifest} (${manifestResponse.status})`,
        );
      }
      const manifestJson = await manifestResponse.text();

      // Initialize DuckDB (lazy — only when pre-aggregates are configured)
      let duckdb: DuckDBEngine;
      if (this.deps?.duckdb) {
        duckdb = this.deps.duckdb;
      } else {
        const { DuckDBManager } = await import('./duckdb-manager');
        duckdb = await DuckDBManager.create();
      }

      const { IDBStorage } = await import('./idb-storage');
      const storage: StorageEngine = this.deps?.storage ?? new IDBStorage();

      this.preaggStore = new PreAggregateStore({
        manifest: manifestJson,
        baseUrl: this.options.preAggregates.baseUrl,
        duckdb,
        storage,
        fetchFn,
        cacheResolve: (m, q) => this.semanticLayer!.cacheResolve(m, q),
      });
    }

    // 4. Set up proxy client (if configured)
    if (this.options.proxy) {
      const fetchFn = this.deps?.fetchFn ?? this.options.fetch ?? globalThis.fetch;
      this.proxyClient = new ProxyClient(this.options.proxy, fetchFn);
    }

    this.initialized = true;
  }

  async query(request: QueryRequest): Promise<QueryResult> {
    this.assertInitialized();

    const queryJson = JSON.stringify(request);
    const compiled = this.semanticLayer!.compile(queryJson, this.options.dialect);
    const sql = compiled.sql;

    // 1. Try pre-aggregates
    if (this.preaggStore) {
      try {
        const result = await this.preaggStore.execute(queryJson);
        if (result) {
          return {
            ok: true,
            rows: result.rows,
            columns: result.columns,
            sql,
            source: 'pre_aggregate',
            warning: null,
          };
        }
      } catch (e) {
        // Pre-aggregate failed — fall through to proxy
        if (!this.proxyClient) {
          throw e;
        }
      }
    }

    // 2. Try proxy
    if (this.proxyClient) {
      const proxyResult = await this.proxyClient.post(sql, this.options.dialect);
      return {
        ok: true,
        rows: proxyResult.rows,
        columns: proxyResult.columns,
        sql,
        source: 'proxy',
        warning: null,
      };
    }

    // 3. Graceful failure
    return {
      ok: false,
      rows: [],
      columns: [],
      sql,
      source: null,
      warning:
        'Query not covered by pre-aggregates. Configure a proxy to execute uncovered queries, ' +
        'or add pre-aggregate coverage with `airlayer build`.',
    };
  }

  sql(request: QueryRequest): CompileResult {
    this.assertInitialized();

    const queryJson = JSON.stringify(request);
    const compiled = this.semanticLayer!.compile(queryJson, this.options.dialect);

    return {
      sql: compiled.sql,
      params: compiled.params,
      columns: compiled.columns.map(
        (c): ColumnMeta => ({ key: c.alias, type: kindToType(c.kind) }),
      ),
    };
  }

  inspect(): CatalogEntry[] {
    this.assertInitialized();
    return this.semanticLayer!.catalog();
  }

  private assertInitialized(): void {
    if (!this.initialized) {
      throw new Error('AirlayerClient not initialized. Call await client.init() first.');
    }
  }
}

function kindToType(kind: string): string {
  switch (kind) {
    case 'Dimension':
      return 'string';
    case 'Measure':
      return 'number';
    case 'TimeDimension':
      return 'datetime';
    default:
      return 'string';
  }
}

