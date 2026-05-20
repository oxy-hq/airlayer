import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  // WASM artifacts are generated separately by build:wasm; mark as external
  // so the dynamic import is preserved at runtime rather than bundled.
  external: ['../wasm/airlayer.js'],
});
