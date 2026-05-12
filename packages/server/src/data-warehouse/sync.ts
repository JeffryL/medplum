// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import type { MedplumDatabaseConfig } from '../config/types';
import { buildPostgresUrlFromMedplumDatabaseConfig } from './config';
import type { WarehouseSourceTable } from './config';
import type { DataWarehouseSink, DuckdbConnectionForSink } from './sink';
import {
  asSqlIdentifier,
  DEFAULT_NAMESPACE,
} from './warehouse-sql';

export interface SyncOptions {
  /** Same shape as server `database` / `readonlyDatabase` in `MedplumServerConfig`; host, dbname, username, and password are required when running sync. */
  database: MedplumDatabaseConfig;
  /**
   * Postgres `statement_timeout` for the DuckDB source URL (PostgreSQL duration syntax, e.g. `15min`, `45s`).
   * When invoked from the server worker, use `dataWarehouse.databaseStatementTimeout` when set, otherwise derive from `MedplumServerConfig.database.queryTimeout` or the data-warehouse default.
   */
  databaseStatementTimeout: string;
  warehouseSources: WarehouseSourceTable[];
  sink: DataWarehouseSink;
  namespace?: string;
  onProgress?: (message: string, metadata?: Record<string, string | number>) => void;
}

export interface SyncResourceResult {
  tableKey: string;
  table: string;
  count: number;
  action: SyncAction;
}

export interface SyncResult {
  resources: SyncResourceResult[];
}

export type SyncAction = 'skip-empty' | 'insert';

export function getSyncAction(count: number): SyncAction {
  if (count === 0) {
    return 'skip-empty';
  }

  return 'insert';
}

function logSyncProgress(
  options: SyncOptions,
  message: string,
  metadata: Record<string, string | number> | undefined
): void {
  if (options.onProgress) {
    options.onProgress(message, metadata);
    return;
  }

  console.log(message);
}

function getSyncSourceConnectionUrl(options: SyncOptions): string {
  return buildPostgresUrlFromMedplumDatabaseConfig(options.database, options.databaseStatementTimeout);
}

type WarehouseSyncDuckdbConnection = DuckdbConnectionForSink & {
  runAndReadAll(query: string): Promise<{ getRowObjectsJson(): unknown[] }>;
  closeSync(): void;
};

async function runWarehouseTableSync(
  connection: WarehouseSyncDuckdbConnection,
  options: SyncOptions,
  namespace: string
): Promise<SyncResourceResult[]> {
  const results: SyncResourceResult[] = [];

  for (const spec of options.warehouseSources) {
    const { postgresTable, tableKey } = spec;
    const sourcePredicate = options.sink.buildSourcePredicate(spec, namespace);
    const resultTableName = options.sink.getResultTableName(spec);
    await options.sink.ensureTargetExists(spec, namespace);

    const countReader = await connection.runAndReadAll(
      `SELECT COUNT(*) AS count FROM pg_db."${postgresTable}" WHERE content IS NOT NULL AND content != '' AND (${sourcePredicate});`
    );
    const count = Number((countReader.getRowObjectsJson() as { count: number }[])[0]?.count ?? 0);
    const action = getSyncAction(count);

    if (action === 'insert') {
      logSyncProgress(options, `Syncing ${tableKey}: ${count} rows`, {
        table: resultTableName,
        tableKey,
        count,
        action,
      });
      await options.sink.writeRows(connection, { tableSpec: spec, namespace, sourcePredicate });
    } else {
      logSyncProgress(options, `Skipping ${tableKey}: no new rows`, {
        table: resultTableName,
        tableKey,
        count,
        action,
      });
    }

    results.push({
      tableKey,
      table: resultTableName,
      count,
      action,
    });
  }

  return results;
}

export async function syncData(options: SyncOptions): Promise<SyncResult> {

  const sourceConnectionUrl = getSyncSourceConnectionUrl(options);
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);

  let connection: WarehouseSyncDuckdbConnection | undefined;
  let duckdbTempDir: string | undefined;
  try {
    duckdbTempDir = mkdtempSync(join(tmpdir(), `medplum-dw-sync-${Date.now()}-`));
    const duckdbDatabasePath = join(duckdbTempDir, 'warehouse.duckdb');
    const instance = await DuckDBInstance.create(duckdbDatabasePath);
    connection = await instance.connect();
    for (const q of options.sink.getSetupQueries(sourceConnectionUrl)) {
      await connection.run(q);
    }

    if (!options.warehouseSources.length) {
      throw new Error('warehouseSources is required: use --table with at least one Postgres table name.');
    }

    const resources = await runWarehouseTableSync(connection, options, namespace);
    return { resources };
  } finally {
    connection?.closeSync();
    /*
     * DuckDB often creates companion files next to the database, so
     * we're gonna delete the whole directory.
     */
    if (duckdbTempDir) {
      rmSync(duckdbTempDir, { recursive: true, force: true });
    }
  }
}
