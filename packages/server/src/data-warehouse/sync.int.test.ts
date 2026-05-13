// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/** Integration: Postgres (medplum_test) → syncData (sink: local) → Parquet on disk. */

import { DuckDBInstance } from '@duckdb/node-api';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { loadTestConfig } from '../config/loader';
import { DEFAULT_DATABASE_STATEMENT_TIMEOUT, resolveWarehouseSourcesFromPostgresTableNames } from './config';
import { LocalParquetWarehouseSink } from './sink';
import { syncData } from './sync';

/** Isolated history table in medplum_test so we do not collide with real FHIR history tables. */
const HISTORY_TABLE = 'DwSyncIntTest_history';

function assertParquetMagic(bytes: Buffer): void {
  expect(bytes.subarray(0, 4).toString('ascii')).toBe('PAR1');
  expect(bytes.subarray(bytes.length - 4).toString('ascii')).toBe('PAR1');
}

/**
 * Exercises the local Parquet sink by writing a single row to a dedicated Postgres history table,
 * then syncing it to a local Parquet file.
 */
describe('syncData local sink (integration)', () => {
  let host: string;
  let port: number;
  let database: string;
  let username: string;
  let password: string;
  let outDir: string | undefined;

  beforeAll(async () => {
    const config = await loadTestConfig();
    const db = config.database;
    host = db.host ?? '';
    port = db.port ?? 5432;
    database = db.dbname ?? '';
    username = db.username ?? '';
    password = db.password ?? '';

    const client = new pg.Client({
      host,
      port,
      database,
      user: username,
      password,
    });
    await client.connect();
    try {
      await client.query(`DROP TABLE IF EXISTS "${HISTORY_TABLE}"`);
      await client.query(`
        CREATE TABLE "${HISTORY_TABLE}" (
          id TEXT NOT NULL,
          "versionId" TEXT NOT NULL,
          content TEXT NOT NULL,
          "lastUpdated" TIMESTAMPTZ NOT NULL
        );
      `);
      await client.query(
        `INSERT INTO "${HISTORY_TABLE}" (id, "versionId", content, "lastUpdated") VALUES ($1, $2, $3, $4)`,
        [
          'patient-int-1',
          '1',
          JSON.stringify({
            resourceType: 'Patient',
            id: 'patient-int-1',
            meta: { project: 'project-from-json' },
          }),
          '2024-06-01T12:00:00.000Z',
        ]
      );
    } finally {
      await client.end();
    }
  }, 120_000);

  afterAll(async () => {
    const client = new pg.Client({
      host,
      port,
      database,
      user: username,
      password,
    });
    await client.connect();
    try {
      await client.query(`DROP TABLE IF EXISTS "${HISTORY_TABLE}"`);
    } finally {
      await client.end();
    }
    if (outDir) {
      rmSync(outDir, { recursive: true, force: true });
    }
  }, 30_000);

  beforeEach(() => {
    outDir = mkdtempSync(join(tmpdir(), 'medplum-dw-parquet-'));
  });

  afterEach(() => {
    if (outDir) {
      rmSync(outDir, { recursive: true, force: true });
    }
  });

  it('exports projected history rows to a Parquet file via local sink', async () => {
    const sources = resolveWarehouseSourcesFromPostgresTableNames([HISTORY_TABLE]);
    const sink = new LocalParquetWarehouseSink(outDir as string);
    const result = await syncData({
      database: { host, port, dbname: database, username, password },
      databaseStatementTimeout: DEFAULT_DATABASE_STATEMENT_TIMEOUT,
      warehouseSources: sources,
      sink,
    });
    expect(result.resources).toHaveLength(1);
    expect(result.resources[0]?.action).toBe('insert');
    expect(result.resources[0]?.table).toContain(`${sources[0]?.icebergTable}.parquet`);

    const parquetPath = result.resources[0]?.table;
    assertParquetMagic(readFileSync(parquetPath));

    const instance = await DuckDBInstance.create(':memory:');
    const c = await instance.connect();
    try {
      const pqEsc = parquetPath.replace(/\\/g, '/').replaceAll("'", "''");
      const res = await c.runAndReadAll(
        `SELECT id::VARCHAR AS id, project_id::VARCHAR AS project_id FROM read_parquet('${pqEsc}') LIMIT 1`
      );
      const row = res.getRowObjectsJson()[0] as { id: string; project_id: string | null };
      expect(row.id).toBe('patient-int-1');
      expect(row.project_id).toBe('project-from-json');
    } finally {
      c.closeSync();
    }
  }, 120_000);
});
