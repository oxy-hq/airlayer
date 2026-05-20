import type {
  WasmModule,
  WasmCompileResult,
  WasmCacheResolution,
  WasmCatalogEntry,
} from './types';

interface SemanticLayerOptions {
  views?: string[];
  foreign?: { format: string; files: string[] };
}

export class SemanticLayer {
  private wasm: WasmModule;
  private views?: string[];
  private foreign?: { format: string; files: string[] };

  constructor(wasm: WasmModule, options: SemanticLayerOptions) {
    this.wasm = wasm;
    this.views = options.views;
    this.foreign = options.foreign;
  }

  compile(queryJson: string, dialect: string): WasmCompileResult {
    if (this.foreign) {
      if (!this.wasm.compile_foreign) {
        throw new Error(
          'compile_foreign not available — WASM was built without foreign format support. ' +
            'Rebuild with --features wasm,foreign-' + this.foreign.format,
        );
      }
      return this.wasm.compile_foreign(
        this.foreign.format,
        this.foreign.files,
        queryJson,
        dialect,
      );
    }
    return this.wasm.compile(
      this.views!,
      queryJson,
      dialect,
      undefined,
      undefined,
      undefined,
    );
  }

  cacheResolve(manifestJson: string, queryJson: string): WasmCacheResolution | null {
    return this.wasm.cache_resolve(manifestJson, queryJson);
  }

  catalog(): WasmCatalogEntry[] {
    if (this.foreign) {
      return [];
    }
    return this.wasm.catalog_list(this.views!);
  }
}
