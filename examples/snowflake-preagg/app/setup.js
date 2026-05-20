#!/usr/bin/env node
// Copies pre-built artifacts into public/ so the frontend can load them.
// Run: npm run setup (after `airlayer build && airlayer pull` in parent dir)

import { cpSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '../../..');
const demoDir = resolve(__dirname, '..');
const publicDir = resolve(__dirname, 'public');

// 1. Copy WASM artifacts
const wasmSrc = resolve(root, 'sdk/wasm');
const wasmDst = resolve(publicDir, 'wasm');
if (!existsSync(resolve(wasmSrc, 'airlayer_bg.wasm'))) {
  console.error('WASM not built. Run from repo root: cargo build --target wasm32-unknown-unknown --no-default-features --features wasm --release && wasm-bindgen target/wasm32-unknown-unknown/release/airlayer.wasm --out-dir sdk/wasm --target web');
  process.exit(1);
}
mkdirSync(wasmDst, { recursive: true });
for (const f of ['airlayer.js', 'airlayer_bg.wasm']) {
  cpSync(resolve(wasmSrc, f), resolve(wasmDst, f));
}
console.log('  Copied WASM artifacts');

// 2. Copy pre-aggregate data (manifest + parquet)
const cacheSrc = resolve(demoDir, '.airlayer/cache');
const dataDst = resolve(publicDir, 'data');
if (!existsSync(resolve(cacheSrc, 'manifest.json'))) {
  console.error('Pre-aggregate cache not found. Run from examples/snowflake-preagg:\n  airlayer build --config config.yml\n  airlayer pull --config config.yml');
  process.exit(1);
}
mkdirSync(dataDst, { recursive: true });
cpSync(cacheSrc, dataDst, { recursive: true });
console.log('  Copied pre-aggregate data (manifest + parquet files)');

// 3. Copy the view YAML (so the frontend can load it)
const viewSrc = resolve(demoDir, 'views/events.view.yml');
cpSync(viewSrc, resolve(publicDir, 'data/events.view.yml'));
console.log('  Copied view definition');

// 4. Copy DuckDB WASM from node_modules
const duckdbDir = resolve(__dirname, 'node_modules/@duckdb/duckdb-wasm/dist');
const duckdbDst = resolve(publicDir, 'duckdb');
mkdirSync(duckdbDst, { recursive: true });
for (const f of ['duckdb-mvp.wasm', 'duckdb-browser-mvp.worker.js']) {
  const src = resolve(duckdbDir, f);
  if (existsSync(src)) {
    cpSync(src, resolve(duckdbDst, f));
  }
}
// Also copy the eh bundle as fallback
for (const f of ['duckdb-eh.wasm', 'duckdb-browser-eh.worker.js']) {
  const src = resolve(duckdbDir, f);
  if (existsSync(src)) {
    cpSync(src, resolve(duckdbDst, f));
  }
}
console.log('  Copied DuckDB WASM');

console.log('\nSetup complete. Run: npm run dev');
