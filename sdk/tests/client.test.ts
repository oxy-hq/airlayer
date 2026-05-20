import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AirlayerClient } from '../src/client';
import type {
  WasmModule,
  WasmCompileResult,
  DuckDBEngine,
  StorageEngine,
  ColumnMeta,
  ClientDeps,
} from '../src/types';

const COMPILE_RESULT: WasmCompileResult = {
  sql: 'SELECT status, COUNT(*) AS count FROM orders GROUP BY 1',
  params: [],
  columns: [
    { member: 'orders.status', alias: 'orders__status', kind: 'Dimension' },
    { member: 'orders.count', alias: 'orders__count', kind: 'Measure' },
  ],
};

function mockWasm(overrides: Partial<WasmModule> = {}): WasmModule {
  return {
    compile: vi.fn().mockReturnValue(COMPILE_RESULT),
    cache_resolve: vi.fn().mockReturnValue(null),
    catalog_list: vi.fn().mockReturnValue([
      { kind: 'View', name: 'orders' },
      { kind: 'Dimension', name: 'orders.status', view: 'orders', member_type: 'string' },
    ]),
    ...overrides,
  };
}

function mockDuckDB(): DuckDBEngine {
  return {
    init: vi.fn().mockResolvedValue(undefined),
    loadParquet: vi.fn().mockResolvedValue(undefined),
    execute: vi.fn().mockResolvedValue({
      rows: [{ orders__status: 'active', orders__count: 42 }],
      columns: [
        { key: 'orders__status', type: 'string' },
        { key: 'orders__count', type: 'number' },
      ] satisfies ColumnMeta[],
    }),
    dropTable: vi.fn().mockResolvedValue(undefined),
  };
}

function mockStorage(): StorageEngine {
  const data = new Map<string, Uint8Array>();
  return {
    get: vi.fn((key: string) => Promise.resolve(data.get(key))),
    set: vi.fn((key: string, value: Uint8Array) => {
      data.set(key, value);
      return Promise.resolve();
    }),
  };
}

describe('AirlayerClient', () => {
  describe('query() — pre-aggregate path', () => {
    it('serves query from pre-aggregates when covered', async () => {
      const wasm = mockWasm({
        cache_resolve: vi.fn().mockReturnValue({
          reagg_sql: 'SELECT * FROM "__cache"',
          cache_key: 'orders__abc123',
          entry: {},
        }),
      });
      const duckdb = mockDuckDB();
      const storage = mockStorage();

      const parquetBytes = new Uint8Array([1, 2, 3]);
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        text: () => Promise.resolve(JSON.stringify({ pulled_at: '', source_database: '', rollups: [] })),
        json: () => Promise.resolve({}),
        arrayBuffer: () => Promise.resolve(parquetBytes.buffer),
      });

      const client = new AirlayerClient(
        {
          views: ['view_yaml'],
          dialect: 'postgres',
          preAggregates: { manifest: '/data/manifest.json', baseUrl: '/data/' },
        },
        { wasm, duckdb, storage, fetchFn: mockFetch as unknown as typeof fetch },
      );

      await client.init();

      const result = await client.query({ measures: ['orders.count'] });

      expect(result.ok).toBe(true);
      expect(result.source).toBe('pre_aggregate');
      expect(result.sql).toContain('SELECT');
      expect(result.warning).toBeNull();
    });
  });

  describe('query() — proxy path', () => {
    it('falls through to proxy when pre-aggregates miss', async () => {
      const wasm = mockWasm(); // cache_resolve returns null
      const proxyResponse = {
        rows: [{ orders__status: 'shipped', orders__count: 10 }],
        columns: [
          { key: 'orders__status', type: 'string' },
          { key: 'orders__count', type: 'number' },
        ],
      };

      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(proxyResponse),
      });

      const client = new AirlayerClient(
        {
          views: ['view_yaml'],
          dialect: 'postgres',
          proxy: '/api/query',
        },
        { wasm, fetchFn: mockFetch as unknown as typeof fetch },
      );
      await client.init();

      const result = await client.query({ measures: ['orders.count'] });

      expect(result.ok).toBe(true);
      expect(result.source).toBe('proxy');
      expect(result.rows).toEqual(proxyResponse.rows);
    });
  });

  describe('query() — graceful failure', () => {
    it('returns ok:false when no pre-agg and no proxy', async () => {
      const wasm = mockWasm(); // cache_resolve returns null

      const client = new AirlayerClient(
        { views: ['view_yaml'], dialect: 'postgres' },
        { wasm },
      );
      await client.init();

      const result = await client.query({ measures: ['orders.count'] });

      expect(result.ok).toBe(false);
      expect(result.source).toBeNull();
      expect(result.rows).toEqual([]);
      expect(result.columns).toEqual([]);
      expect(result.sql).toContain('SELECT');
      expect(result.warning).toContain('not covered by pre-aggregates');
    });
  });

  describe('sql()', () => {
    it('compiles query to SQL without executing', async () => {
      const wasm = mockWasm();
      const client = new AirlayerClient(
        { views: ['view_yaml'], dialect: 'postgres' },
        { wasm },
      );
      await client.init();

      const result = client.sql({ measures: ['orders.count'] });

      expect(result.sql).toContain('SELECT');
      expect(result.params).toEqual([]);
      expect(result.columns).toHaveLength(2);
    });
  });

  describe('inspect()', () => {
    it('lists catalog entries', async () => {
      const wasm = mockWasm();
      const client = new AirlayerClient(
        { views: ['view_yaml'], dialect: 'postgres' },
        { wasm },
      );
      await client.init();

      const entries = client.inspect();

      expect(entries).toHaveLength(2);
      expect(entries[0].name).toBe('orders');
    });
  });
});
