import type { WasmModule } from './types';

let singleton: WasmModule | null = null;

/**
 * Load the airlayer WASM module. Returns a singleton — subsequent calls
 * return the same module instance.
 */
export async function loadWasm(wasmUrl?: string): Promise<WasmModule> {
  if (singleton) return singleton;

  // Dynamic import so the WASM glue code is only loaded when needed.
  // The actual path is resolved at bundle time. Consumers must ensure
  // the WASM artifacts are available at the expected location.
  /* eslint-disable @typescript-eslint/ban-ts-comment */
  // @ts-ignore — WASM artifact is generated at build time; not resolvable at type-check time.
  const wasm = await import(/* webpackIgnore: true */ /* @vite-ignore */ '../wasm/airlayer.js');
  /* eslint-enable @typescript-eslint/ban-ts-comment */
  await wasm.default(wasmUrl);
  singleton = wasm as unknown as WasmModule;
  return singleton;
}

/** Reset the singleton (for testing). */
export function _resetWasm(): void {
  singleton = null;
}
