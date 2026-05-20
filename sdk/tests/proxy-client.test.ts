import { describe, it, expect, vi } from 'vitest';
import { ProxyClient } from '../src/proxy-client';

describe('ProxyClient', () => {
  it('posts SQL to proxy and returns rows', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          rows: [{ status: 'active', total: 100 }],
          columns: [
            { key: 'status', type: 'string' },
            { key: 'total', type: 'number' },
          ],
        }),
    });

    const client = new ProxyClient('/api/query', mockFetch as unknown as typeof fetch);
    const result = await client.post('SELECT 1', 'postgres');

    expect(mockFetch).toHaveBeenCalledWith('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1', dialect: 'postgres', datasource: 'default' }),
    });
    expect(result.rows).toEqual([{ status: 'active', total: 100 }]);
    expect(result.columns).toHaveLength(2);
  });

  it('includes datasource when provided', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ rows: [], columns: [] }),
    });

    const client = new ProxyClient('/api/query', mockFetch as unknown as typeof fetch);
    await client.post('SELECT 1', 'postgres', 'warehouse_a');

    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.datasource).toBe('warehouse_a');
  });

  it('throws on HTTP error with server message', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      json: () => Promise.resolve({ error: 'Connection refused' }),
    });

    const client = new ProxyClient('/api/query', mockFetch as unknown as typeof fetch);
    await expect(client.post('SELECT 1', 'postgres')).rejects.toThrow('Connection refused');
  });

  it('throws with status code when no error message', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 502,
      json: () => Promise.reject(new Error('not JSON')),
    });

    const client = new ProxyClient('/api/query', mockFetch as unknown as typeof fetch);
    await expect(client.post('SELECT 1', 'postgres')).rejects.toThrow('Proxy returned 502');
  });
});
