#!/usr/bin/env node
// Express server: serves the frontend + proxies raw SQL to a local ClickHouse.
//
// Endpoints:
//   GET  /             → index.html
//   GET  /public/*     → static assets (WASM, parquet, DuckDB)
//   POST /api/query    → execute raw SQL against ClickHouse, returns { rows, columns }

import express from 'express';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || 'http://localhost:8123';
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER || 'default';
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD || '';
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE || 'ramen_demo';

// ── ClickHouse helper ────────────────────────────────────────────────────────
async function chQuery(sql) {
  const headers = {
    'X-ClickHouse-User': CLICKHOUSE_USER,
    'X-ClickHouse-Database': CLICKHOUSE_DATABASE,
  };
  if (CLICKHOUSE_PASSWORD) headers['X-ClickHouse-Key'] = CLICKHOUSE_PASSWORD;

  const withFormat = / FORMAT /i.test(sql) ? sql : `${sql} FORMAT JSONCompact`;

  const res = await fetch(`${CLICKHOUSE_URL}/`, {
    method: 'POST',
    headers,
    body: withFormat,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`ClickHouse HTTP ${res.status}: ${text.slice(0, 500)}`);

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`ClickHouse returned non-JSON response: ${text.slice(0, 500)}`);
  }

  const meta = json.meta || [];
  const data = json.data || [];
  const columns = meta.map((m) => ({ key: m.name, type: m.type }));

  const rows = data.map((row) => {
    const obj = {};
    meta.forEach((m, i) => {
      let val = row[i];
      const innerType = String(m.type || '').replace(/^Nullable\((.*)\)$/, '$1');
      if (val !== null && typeof val === 'string') {
        if (/Int|Float|Decimal/.test(innerType)) {
          const n = Number(val);
          if (!Number.isNaN(n)) val = n;
        }
      }
      obj[m.name] = val;
    });
    return obj;
  });

  return { columns, rows };
}

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

app.use('/public', express.static(resolve(__dirname, 'public'), {
  setHeaders(res, path) {
    if (path.endsWith('.wasm')) res.setHeader('Content-Type', 'application/wasm');
  },
}));

app.get('/', (_req, res) => res.sendFile(resolve(__dirname, 'index.html')));
app.get('/slider', (_req, res) => res.sendFile(resolve(__dirname, 'slider.html')));

app.post('/api/query', async (req, res) => {
  try {
    const { sql } = req.body;
    if (!sql) return res.status(400).json({ error: 'Missing sql' });
    const result = await chQuery(sql);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3456;
app.listen(PORT, () => {
  console.log(`\n  Pre-aggregation Demo (ClickHouse)`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  ClickHouse → ${CLICKHOUSE_URL} (db: ${CLICKHOUSE_DATABASE})\n`);
});
