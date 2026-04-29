// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import { DataWarehouseAwsClient } from './aws.ts';
import {
  buildManagedIcebergSetupQueries,
  buildProjectedSelectFromHistoryTable,
  WAREHOUSE_HISTORY_COLUMN_NAMES,
} from './export.ts';
import type { WarehouseSourceTable } from './export.ts';
import {
  asSqlIdentifier,
  buildInsertIntoSelectQuery,
  buildManagedIcebergQualifiedTable,
  DEFAULT_NAMESPACE,
} from './warehouse-sql.ts';

export interface SyncOptions {
  databaseUrl: string;
  s3Region: string;
  awsS3TableArn: string;
  s3Bucket?: string;
  athenaOutputLocation?: string;
  athenaWorkGroup?: string;
  athenaCatalogName?: string;
  warehouseSources: WarehouseSourceTable[];
  namespace?: string;
  defaultRowThreshold: number;
  rowThresholdOverrides?: Record<string, number>;
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

const MIN_WATERMARK_TIMESTAMP = '0001-01-01T00:00:00Z';

export function getSyncAction(count: number, threshold: number): SyncAction {
  if (count === 0) {
    return 'skip-empty';
  }

  if (count < threshold) {
    return 'skip-threshold';
  }

  return 'insert';
}

export async function syncData(options: SyncOptions): Promise<SyncResult> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const athenaClient = new DataWarehouseAwsClient({
    region: options.s3Region,
    outputLocation: options.athenaOutputLocation,
    workGroup: options.athenaWorkGroup,
    catalogName: options.athenaCatalogName,
  });
  const overrides = options.rowThresholdOverrides ?? {};
  const results: SyncResourceResult[] = [];

  try {
    for (const q of buildManagedIcebergSetupQueries({
      databaseUrl: options.databaseUrl,
      s3Region: options.s3Region,
      awsS3TableArn: options.awsS3TableArn,
      namespace: options.namespace,
    })) {
      await connection.run(q);
    }

    if (!options.warehouseSources.length) {
      throw new Error('warehouseSources is required: use --table with at least one Postgres table name.');
    }

    for (const spec of options.warehouseSources) {
      const { postgresTable, icebergTable, tableKey } = spec;
      const threshold = overrides[tableKey] ?? overrides.default ?? options.defaultRowThreshold;
      const qualifiedIceberg = buildManagedIcebergQualifiedTable(namespace, icebergTable);
      const watermarkSubquery = `(SELECT MAX(last_updated) FROM ${qualifiedIceberg})`;
      const sourcePredicate = `"lastUpdated" > COALESCE(${watermarkSubquery}, TIMESTAMPTZ '${MIN_WATERMARK_TIMESTAMP}')`;
      const tableExists = await athenaClient.tableExists(options.awsS3TableArn, namespace, icebergTable);
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
        console.log(`Syncing ${tableKey}: ${count} rows (threshold ${threshold})`);
        await connection.run(insertQuery);
      } else if (action === 'skip-threshold') {
        console.log(`Skipping ${tableKey}: ${count} rows is below threshold ${threshold}`);
      } else {
        console.log(`Skipping ${tableKey}: no new rows`);
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
