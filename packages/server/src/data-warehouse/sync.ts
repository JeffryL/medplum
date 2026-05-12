// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import type { MedplumDatabaseConfig } from '../config/types';
import { buildPostgresUrlFromMedplumDatabaseConfig } from './config';
import type { WarehouseSourceTable } from './config';
import type { DataWarehouseSyncSink, DuckdbConnectionForSink } from './sink';
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
  sink: DataWarehouseSyncSink;
  namespace?: string;
  rowThresholdOverrides?: Record<string, number>;
  onProgress?: (message: string, metadata?: Record<string, string | number>) => void;
}

export interface SyncResourceResult {
  tableKey: string;
  table: string;
  count: number;
  threshold: number;
  action: SyncAction;
}

export interface SyncResult {
  resources: SyncResourceResult[];
}

export type SyncAction = 'skip-empty' | 'skip-threshold' | 'insert';

export function getSyncAction(count: number, threshold: number): SyncAction {
  if (count === 0) {
    return 'skip-empty';
  }

  if (count < threshold) {
    return 'skip-threshold';
  }

  return 'insert';
}

function getThresholdForTable(overrides: Record<string, number>, tableKey: string): number {
  const thresholdCandidate = overrides[tableKey] ?? overrides.default;
  if (thresholdCandidate !== undefined && Number.isFinite(thresholdCandidate) && thresholdCandidate > 0) {
    return Math.floor(thresholdCandidate);
  }

  // No default threshold configured: insert whenever at least one row exists.
  return 1;
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

async function runWarehouseTableSync(
  connection: DuckdbConnectionForSink & { runAndReadAll(query: string): Promise<{ getRowObjectsJson(): unknown[] }> },
  options: SyncOptions,
  namespace: string,
  overrides: Record<string, number>
): Promise<SyncResourceResult[]> {
  const results: SyncResourceResult[] = [];

  for (const spec of options.warehouseSources) {
    const { postgresTable, tableKey } = spec;
    const threshold = getThresholdForTable(overrides, tableKey);
    const sourcePredicate = options.sink.buildSourcePredicate(spec, namespace);
    const resultTableName = options.sink.getResultTableName(spec);
    await options.sink.ensureTargetExists(spec, namespace);

    const countReader = await connection.runAndReadAll(
      `SELECT COUNT(*) AS count FROM pg_db."${postgresTable}" WHERE content IS NOT NULL AND content != '' AND (${sourcePredicate});`
    );
    const count = Number((countReader.getRowObjectsJson() as { count: number }[])[0]?.count ?? 0);
    const action = getSyncAction(count, threshold);

    if (action === 'insert') {
      logSyncProgress(options, `Syncing ${tableKey}: ${count} rows (threshold ${threshold})`, {
        table: resultTableName,
        tableKey,
        count,
        threshold,
        action,
      });
      await options.sink.writeRows(connection, { tableSpec: spec, namespace, sourcePredicate });
    } else if (action === 'skip-threshold') {
      logSyncProgress(options, `Skipping ${tableKey}: ${count} rows is below threshold ${threshold}`, {
        table: resultTableName,
        tableKey,
        count,
        threshold,
        action,
      });
    } else {
      logSyncProgress(options, `Skipping ${tableKey}: no new rows`, {
        table: resultTableName,
        tableKey,
        count,
        threshold,
        action,
      });
    }

    results.push({
      tableKey,
      table: resultTableName,
      count,
      threshold,
      action,
    });
  }

  return results;
}

export async function syncData(options: SyncOptions): Promise<SyncResult> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const sourceConnectionUrl = getSyncSourceConnectionUrl(options);
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const overrides = options.rowThresholdOverrides ?? {};

  try {
    for (const q of options.sink.getSetupQueries(sourceConnectionUrl)) {
      await connection.run(q);
    }

    if (!options.warehouseSources.length) {
      throw new Error('warehouseSources is required: use --table with at least one Postgres table name.');
    }

    const resources = await runWarehouseTableSync(connection, options, namespace, overrides);
    return { resources };
  } finally {
    connection.closeSync();
  }
}
