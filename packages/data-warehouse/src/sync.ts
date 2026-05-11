// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import { DataWarehouseAwsClient } from './aws.ts';
import { resolveDatabaseUrl } from './config.ts';
import type { WarehouseSourceTable } from './export.ts';
import {
  buildManagedIcebergSetupQueries,
  buildProjectedSelectFromHistoryTable,
  WAREHOUSE_HISTORY_COLUMN_NAMES,
} from './export.ts';
import {
  asSqlIdentifier,
  buildInsertIntoSelectQuery,
  buildManagedIcebergQualifiedTable,
  DEFAULT_NAMESPACE,
} from './warehouse-sql.ts';

export interface SyncOptions {
  /**
   * Explicit Postgres connection parameters for constructing the source connection URL.
   */
  database: SyncDatabaseConnectionOptions;
  s3Region: string;
  awsS3TableArn: string;
  s3Bucket?: string;
  warehouseSources: WarehouseSourceTable[];
  namespace?: string;
  defaultRowThreshold?: number;
  rowThresholdOverrides?: Record<string, number>;
  onProgress?: (message: string, metadata?: Record<string, string | number>) => void;
}

export interface SyncDatabaseConnectionOptions {
  host: string;
  port?: number;
  dbname: string;
  username: string;
  password: string;
  statementTimeout?: string;
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

function getThresholdForTable(
  defaultThreshold: number | undefined,
  overrides: Record<string, number>,
  tableKey: string
): number {
  const thresholdCandidate = overrides[tableKey] ?? overrides.default ?? defaultThreshold;
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
  return resolveDatabaseUrl({
    dbHost: options.database.host,
    dbPort: String(options.database.port ?? 5432),
    dbName: options.database.dbname,
    dbUsername: options.database.username,
    dbPassword: options.database.password,
    databaseStatementTimeout: options.database.statementTimeout,
  });
}

export async function syncData(options: SyncOptions): Promise<SyncResult> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const sourceConnectionUrl = getSyncSourceConnectionUrl(options);
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const dwClient = new DataWarehouseAwsClient({
    region: options.s3Region,
  });
  const overrides = options.rowThresholdOverrides ?? {};
  const results: SyncResourceResult[] = [];

  try {
    for (const q of buildManagedIcebergSetupQueries({
      databaseUrl: sourceConnectionUrl,
      s3Region: options.s3Region,
      awsS3TableArn: options.awsS3TableArn,
    })) {
      await connection.run(q);
    }

    if (!options.warehouseSources.length) {
      throw new Error('warehouseSources is required: use --table with at least one Postgres table name.');
    }

    for (const spec of options.warehouseSources) {
      const { postgresTable, icebergTable, tableKey } = spec;
      const threshold = getThresholdForTable(options.defaultRowThreshold, overrides, tableKey);
      const qualifiedIceberg = buildManagedIcebergQualifiedTable(namespace, icebergTable);
      const watermarkSubquery = `(SELECT MAX(last_updated) FROM ${qualifiedIceberg})`;
      // Incremental sync: only Postgres rows newer than the latest row already in Iceberg.
      // When the Iceberg table is empty (or MAX is NULL), `lastUpdated > NULL` would be unknown for every row,
      // so we treat a NULL watermark as "no high-water mark" and include all source rows instead of a sentinel timestamp.
      const sourcePredicate = `(${watermarkSubquery} IS NULL OR "lastUpdated" > ${watermarkSubquery})`;
      const tableExists = await dwClient.tableExists(options.awsS3TableArn, namespace, icebergTable);
      if (!tableExists) {
        throw new Error(
          `Managed Iceberg table does not exist: ${namespace}.${icebergTable}. Run the migrate command before sync.`
        );
      }

      const countReader = await connection.runAndReadAll(
        `SELECT COUNT(*) AS count FROM pg_db."${postgresTable}" WHERE content IS NOT NULL AND content != '' AND (${sourcePredicate});`
      );
      const count = Number((countReader.getRowObjectsJson() as { count: number }[])[0]?.count ?? 0);
      const action = getSyncAction(count, threshold);

      if (action === 'insert') {
        const insertColumns = WAREHOUSE_HISTORY_COLUMN_NAMES.join(', ');
        const insertQuery = buildInsertIntoSelectQuery(
          qualifiedIceberg,
          insertColumns,
          buildProjectedSelectFromHistoryTable(postgresTable, sourcePredicate)
        );
        logSyncProgress(options, `Syncing ${tableKey}: ${count} rows (threshold ${threshold})`, {
          table: icebergTable,
          tableKey,
          count,
          threshold,
          action,
        });
        await connection.run(insertQuery);
      } else if (action === 'skip-threshold') {
        logSyncProgress(options, `Skipping ${tableKey}: ${count} rows is below threshold ${threshold}`, {
          table: icebergTable,
          tableKey,
          count,
          threshold,
          action,
        });
      } else {
        logSyncProgress(options, `Skipping ${tableKey}: no new rows`, {
          table: icebergTable,
          tableKey,
          count,
          threshold,
          action,
        });
      }

      results.push({
        tableKey,
        table: icebergTable,
        count,
        threshold,
        action,
      });
    }

    return { resources: results };
  } finally {
    connection.closeSync();
  }
}
