import type { ColumnMeta, DuckDBEngine } from './types';

let singleton: DuckDBManager | null = null;

export class DuckDBManager implements DuckDBEngine {
  private db: any;
  private conn: any;

  private constructor(db: any, conn: any) {
    this.db = db;
    this.conn = conn;
  }

  static async create(): Promise<DuckDBManager> {
    if (singleton) return singleton;

    const duckdb = await import('@duckdb/duckdb-wasm');

    const DUCKDB_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);

    const worker = new Worker(bundle.mainWorker!);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    const conn = await db.connect();
    singleton = new DuckDBManager(db, conn);
    return singleton;
  }

  async init(): Promise<void> {
    // Already initialized in create()
  }

  async loadParquet(tableName: string, data: Uint8Array): Promise<void> {
    const safeName = tableName.replace(/"/g, '""');
    const safeFile = tableName.replace(/'/g, "''");
    await this.db.registerFileBuffer(`${tableName}.parquet`, data);
    await this.conn.query(
      `CREATE OR REPLACE TABLE "${safeName}" AS SELECT * FROM read_parquet('${safeFile}.parquet')`,
    );
  }

  async execute(
    sql: string,
  ): Promise<{ rows: Record<string, unknown>[]; columns: ColumnMeta[] }> {
    const result = await this.conn.query(sql);

    const columns: ColumnMeta[] = result.schema.fields.map((f: any) => ({
      key: f.name,
      type: arrowTypeToString(f.type),
    }));

    // Pre-fetch column vectors (Arrow is column-oriented)
    const vectors = columns.map((_: ColumnMeta, idx: number) => result.getChildAt(idx));

    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < result.numRows; i++) {
      const row: Record<string, unknown> = {};
      for (let colIdx = 0; colIdx < columns.length; colIdx++) {
        row[columns[colIdx].key] = vectors[colIdx]?.get(i);
      }
      rows.push(row);
    }

    return { rows, columns };
  }

  async dropTable(tableName: string): Promise<void> {
    await this.conn.query(`DROP TABLE IF EXISTS "${tableName}"`);
  }
}

function arrowTypeToString(type: any): string {
  const id = type?.typeId;
  if (id === 2 || id === 3) return 'number';
  if (id === 5) return 'string';
  if (id === 6) return 'boolean';
  if (id === 8 || id === 10) return 'datetime';
  return 'string';
}

/** Reset the singleton (for testing). */
export function _resetDuckDB(): void {
  singleton = null;
}
