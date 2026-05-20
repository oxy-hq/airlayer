import { describe, it, expect, vi, beforeEach } from 'vitest';
import { PreAggregateStore } from '../src/preagg-store';
import type { DuckDBEngine, ColumnMeta, StorageEngine, WasmCacheResolution } from '../src/types';

function mockDuckDB(): DuckDBEngine {
  return {
    init: vi.fn().mockResolvedValue(undefined),
    loadParquet: vi.fn().mockResolvedValue(undefined),
    execute: vi.fn().mockResolvedValue({
      rows: [{ orders__status: 'active', orders__count: 42 }],
      columns: [
        { key: 'orders__status', type: 'string' },
        { key: 'orders__count', type: 'number' },
      ],
    }),
    dropTable: vi.fn().mockResolvedValue(undefined),
  };
}

function mockStorage(stored?: Record<string, Uint8Array>): StorageEngine {
  const data = new Map(Object.entries(stored ?? {}));
  return {
    get: vi.fn((key: string) => Promise.resolve(data.get(key))),
    set: vi.fn((key: string, value: Uint8Array) => {
      data.set(key, value);
      return Promise.resolve();
    }),
  };
}

const MANIFEST_JSON = JSON.stringify({
  pulled_at: '2024-01-15T10:30:00Z',
  source_database: 'test_db',
  rollups: [
    {
      view_name: 'orders',
      rollup_name: 'default',
      rollup_hash: 'a1b2c3d4',
      file: 'orders__a1b2c3d4',
      dimensions: ['status'],
      measures: [{ name: 'count', type: 'count', columns: ['count'] }],
      time_dimension: null,
      granularity: null,
      build_date: '20240115',
    },
  ],
});

describe('PreAggregateStore', () => {
  it('resolves a cache hit and executes reagg SQL', async () => {
    const resolution: WasmCacheResolution = {
      reagg_sql: 'SELECT status, SUM(count) FROM "__cache" GROUP BY 1',
      cache_key: 'orders__a1b2c3d4',
      entry: {},
    };
    const cacheResolve = vi.fn().mockReturnValue(resolution);

    const parquetData = new Uint8Array([1, 2, 3]);
    const storage = mockStorage({ orders__a1b2c3d4: parquetData });
    const duckdb = mockDuckDB();

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: () => Promise.resolve(parquetData.buffer),
    });

    const store = new PreAggregateStore({
      manifest: MANIFEST_JSON,
      baseUrl: '/data/',
      duckdb,
      storage,
      fetchFn: mockFetch as unknown as typeof fetch,
      cacheResolve,
    });

    const result = await store.execute('{"measures":["orders.count"]}');

    expect(result).not.toBeNull();
    expect(duckdb.loadParquet).toHaveBeenCalledWith('__cache', parquetData);
    expect(duckdb.execute).toHaveBeenCalledWith(resolution.reagg_sql);
    expect(duckdb.dropTable).toHaveBeenCalledWith('__cache');
    expect(result!.rows).toEqual([{ orders__status: 'active', orders__count: 42 }]);
  });

  it('fetches Parquet from network when not in IndexedDB', async () => {
    const resolution: WasmCacheResolution = {
      reagg_sql: 'SELECT 1',
      cache_key: 'orders__a1b2c3d4',
      entry: {},
    };
    const cacheResolve = vi.fn().mockReturnValue(resolution);

    const storage = mockStorage(); // empty storage
    const duckdb = mockDuckDB();
    const parquetBytes = new Uint8Array([10, 20, 30]);

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: () => Promise.resolve(parquetBytes.buffer),
    });

    const store = new PreAggregateStore({
      manifest: MANIFEST_JSON,
      baseUrl: '/data/',
      duckdb,
      storage,
      fetchFn: mockFetch as unknown as typeof fetch,
      cacheResolve,
    });

    await store.execute('{}');

    // Should fetch from network
    expect(mockFetch).toHaveBeenCalledWith('/data/orders__a1b2c3d4.parquet');
    // Should store in IndexedDB for next time
    expect(storage.set).toHaveBeenCalledWith('orders__a1b2c3d4', parquetBytes);
  });

  it('returns null when no rollup covers the query', async () => {
    const cacheResolve = vi.fn().mockReturnValue(null);

    const store = new PreAggregateStore({
      manifest: MANIFEST_JSON,
      baseUrl: '/data/',
      duckdb: mockDuckDB(),
      storage: mockStorage(),
      fetchFn: vi.fn() as unknown as typeof fetch,
      cacheResolve,
    });

    const result = await store.execute('{"measures":["orders.revenue"]}');
    expect(result).toBeNull();
  });
});
