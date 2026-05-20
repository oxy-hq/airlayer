import { describe, it, expect, vi } from 'vitest';
import { SemanticLayer } from '../src/semantic-layer';
import type { WasmModule, WasmCompileResult } from '../src/types';

function mockWasm(overrides: Partial<WasmModule> = {}): WasmModule {
  return {
    compile: vi.fn().mockReturnValue({
      sql: 'SELECT status, COUNT(*) FROM orders GROUP BY 1',
      params: [],
      columns: [
        { member: 'orders.status', alias: 'orders__status', kind: 'Dimension' },
        { member: 'orders.count', alias: 'orders__count', kind: 'Measure' },
      ],
    } satisfies WasmCompileResult),
    cache_resolve: vi.fn().mockReturnValue(null),
    catalog_list: vi.fn().mockReturnValue([]),
    ...overrides,
  };
}

describe('SemanticLayer', () => {
  it('compiles native views', () => {
    const wasm = mockWasm();
    const layer = new SemanticLayer(wasm, { views: ['view_yaml_1', 'view_yaml_2'] });

    const result = layer.compile('{"measures":["orders.count"]}', 'postgres');

    expect(wasm.compile).toHaveBeenCalledWith(
      ['view_yaml_1', 'view_yaml_2'],
      '{"measures":["orders.count"]}',
      'postgres',
      undefined,
      undefined,
      undefined,
    );
    expect(result.sql).toContain('SELECT');
  });

  it('compiles foreign views', () => {
    const compileForeign = vi.fn().mockReturnValue({
      sql: 'SELECT 1',
      params: [],
      columns: [],
    } satisfies WasmCompileResult);
    const wasm = mockWasm({ compile_foreign: compileForeign });
    const layer = new SemanticLayer(wasm, {
      foreign: { format: 'lookml', files: ['file1.lkml'] },
    });

    layer.compile('{"measures":["orders.count"]}', 'postgres');

    expect(compileForeign).toHaveBeenCalledWith(
      'lookml',
      ['file1.lkml'],
      '{"measures":["orders.count"]}',
      'postgres',
    );
  });

  it('throws if foreign compile not available', () => {
    const wasm = mockWasm(); // no compile_foreign
    const layer = new SemanticLayer(wasm, {
      foreign: { format: 'lookml', files: ['file.lkml'] },
    });

    expect(() => layer.compile('{}', 'postgres')).toThrow(
      'compile_foreign not available',
    );
  });

  it('lists catalog entries', () => {
    const wasm = mockWasm({
      catalog_list: vi.fn().mockReturnValue([
        { kind: 'View', name: 'orders' },
        { kind: 'Dimension', name: 'orders.status', view: 'orders', member_type: 'string' },
      ]),
    });
    const layer = new SemanticLayer(wasm, { views: ['yaml'] });
    const entries = layer.catalog();
    expect(entries).toHaveLength(2);
    expect(entries[0].name).toBe('orders');
  });
});
