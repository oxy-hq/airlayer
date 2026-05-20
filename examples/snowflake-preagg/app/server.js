#!/usr/bin/env node
// Express server: serves the frontend + proxies raw SQL to Snowflake.
//
// Endpoints:
//   GET  /             → index.html
//   GET  /public/*     → static assets (WASM, parquet, DuckDB)
//   POST /api/query    → execute raw SQL against Snowflake, returns { rows, columns }

import express from 'express';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '../../..');

// ── Load .env ────────────────────────────────────────────────────────────────
if (existsSync(resolve(repoRoot, '.env'))) {
  for (const line of readFileSync(resolve(repoRoot, '.env'), 'utf8').split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq < 0) continue;
    const key = line.slice(0, eq).trim();
    const value = line.slice(eq + 1).trim();
    if (key && !process.env[key]) process.env[key] = value;
  }
}

const { SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD } = process.env;
const SNOWFLAKE_WAREHOUSE = process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH';

if (!SNOWFLAKE_ACCOUNT || !SNOWFLAKE_USER || !SNOWFLAKE_PASSWORD) {
  console.error('Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env or environment');
  process.exit(1);
}

// ── Snowflake REST helpers ───────────────────────────────────────────────────
const BASE_URL = `https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com`;
let sfToken = null;

async function sfAuth() {
  const res = await fetch(`${BASE_URL}/session/v1/login-request`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      data: { LOGIN_NAME: SNOWFLAKE_USER, PASSWORD: SNOWFLAKE_PASSWORD, ACCOUNT_NAME: SNOWFLAKE_ACCOUNT },
    }),
  });
  const json = await res.json();
  if (!json.success) throw new Error(`Snowflake auth failed: ${json.message}`);
  sfToken = json.data.token;
}

let seqId = 0;
async function sfQuery(sql, _retry = true) {
  if (!sfToken) await sfAuth();
  seqId++;
  const requestId = crypto.randomUUID();
  const res = await fetch(`${BASE_URL}/queries/v1/query-request?requestId=${requestId}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/snowflake',
      Authorization: `Snowflake Token="${sfToken}"`,
    },
    body: JSON.stringify({ sqlText: sql, asyncExec: false, sequenceId: seqId }),
  });
  const json = await res.json();
  if (!json.success) {
    const msg = json.message || '';
    if (_retry && /session.*expired|token.*invalid/i.test(msg)) {
      sfToken = null;
      return sfQuery(sql, false);
    }
    throw new Error(`Snowflake query failed: ${msg}`);
  }

  const rowTypes = json.data?.rowtype || [];
  const rowset = json.data?.rowset || [];
  const columns = rowTypes.map((rt) => ({ key: rt.name, type: rt.type }));
  const rows = rowset.map((row) => {
    const obj = {};
    rowTypes.forEach((rt, i) => {
      let val = row[i];
      if (val !== null && (rt.type === 'fixed' || rt.type === 'real')) {
        val = Number(val);
      }
      obj[rt.name] = val;
    });
    return obj;
  });
  return { columns, rows };
}

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

// Serve static files
app.use('/public', express.static(resolve(__dirname, 'public'), {
  setHeaders(res, path) {
    if (path.endsWith('.wasm')) res.setHeader('Content-Type', 'application/wasm');
  },
}));

// Serve pages
app.get('/', (_req, res) => res.sendFile(resolve(__dirname, 'index.html')));
app.get('/slider', (_req, res) => res.sendFile(resolve(__dirname, 'slider.html')));

// Proxy endpoint: execute SQL against Snowflake
app.post('/api/query', async (req, res) => {
  try {
    const { sql, dialect, datasource } = req.body;
    if (!sql) return res.status(400).json({ error: 'Missing sql' });

    // Set context
    await sfQuery(`USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE}`);
    await sfQuery('USE DATABASE AIRLAYER_TEST');

    const result = await sfQuery(sql);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3456;
app.listen(PORT, () => {
  console.log(`\n  Pre-aggregation Demo`);
  console.log(`  http://localhost:${PORT}\n`);
});
