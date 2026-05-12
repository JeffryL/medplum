// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { DataWarehouseAwsClient } from './aws';
import type { WarehouseSourceTable } from './config';
import {
  buildInsertIntoSelectQuery,
  buildManagedIcebergQualifiedTable,
  buildManagedIcebergSetupQueries,
  buildProjectedSelectFromHistoryTable,
  escapeSqlLiteral,
  WAREHOUSE_HISTORY_COLUMN_NAMES,
} from './warehouse-sql';

export type DataWarehouseSinkType = 's3tables' | 'local';

export interface DuckdbConnectionForSink {
  run(query: string): Promise<unknown>;
}

interface SinkQueryContext {
  tableSpec: WarehouseSourceTable;
  namespace: string;
  sourcePredicate: string;
}

export interface DataWarehouseSink {
  readonly type: DataWarehouseSinkType;
  getSetupQueries(databaseUrl: string): string[];
  ensureTargetExists(tableSpec: WarehouseSourceTable, namespace: string): Promise<void>;
  buildSourcePredicate(tableSpec: WarehouseSourceTable, namespace: string): string;
  writeRows(connection: DuckdbConnectionForSink, context: SinkQueryContext): Promise<void>;
  getResultTableName(tableSpec: WarehouseSourceTable): string;
}

export class S3TablesWarehouseSink implements DataWarehouseSink {
  type: DataWarehouseSinkType = 's3tables';

  private readonly dwClient: DataWarehouseAwsClient;
  private readonly s3Region: string;
  private readonly awsS3TableArn: string;

  constructor(s3Region: string, awsS3TableArn: string) {
    this.s3Region = s3Region;
    this.awsS3TableArn = awsS3TableArn;
    this.dwClient = new DataWarehouseAwsClient({ region: s3Region });
  }

  getSetupQueries(databaseUrl: string): string[] {
    return buildManagedIcebergSetupQueries({
      databaseUrl,
      s3Region: this.s3Region,
      awsS3TableArn: this.awsS3TableArn,
    });
  }

  async ensureTargetExists(tableSpec: WarehouseSourceTable, namespace: string): Promise<void> {
    const exists = await this.dwClient.tableExists(this.awsS3TableArn, namespace, tableSpec.icebergTable);
    if (!exists) {
      throw new Error(
        `Managed Iceberg table does not exist: ${namespace}.${tableSpec.icebergTable}. Run the migrate command before sync.`
      );
    }
  }

  buildSourcePredicate(tableSpec: WarehouseSourceTable, namespace: string): string {
    const qualifiedIceberg = buildManagedIcebergQualifiedTable(namespace, tableSpec.icebergTable);
    const watermarkSubquery = `(SELECT MAX(last_updated) FROM ${qualifiedIceberg})`;
    // Incremental sync: only Postgres rows newer than the latest row already in Iceberg.
    // When the Iceberg table is empty (or MAX is NULL), `lastUpdated > NULL` would be unknown for every row,
    // so we treat a NULL watermark as "no high-water mark" and include all source rows instead of a sentinel timestamp.
    return `(${watermarkSubquery} IS NULL OR "lastUpdated" > ${watermarkSubquery})`;
  }

  async writeRows(connection: DuckdbConnectionForSink, context: SinkQueryContext): Promise<void> {
    const qualifiedIceberg = buildManagedIcebergQualifiedTable(context.namespace, context.tableSpec.icebergTable);
    const insertColumns = WAREHOUSE_HISTORY_COLUMN_NAMES.join(', ');
    const insertQuery = buildInsertIntoSelectQuery(
      qualifiedIceberg,
      insertColumns,
      buildProjectedSelectFromHistoryTable(context.tableSpec.postgresTable, context.sourcePredicate)
    );
    await connection.run(insertQuery);
  }

  getResultTableName(tableSpec: WarehouseSourceTable): string {
    return tableSpec.icebergTable;
  }
}

export class LocalParquetWarehouseSink implements DataWarehouseSink {
  readonly type: DataWarehouseSinkType = 'local';
  private readonly basePath: string;

  constructor(basePath: string) {
    this.basePath = basePath;
  }

  getSetupQueries(databaseUrl: string): string[] {
    return [
      'INSTALL postgres;',
      'LOAD postgres;',
      `ATTACH '${escapeSqlLiteral(databaseUrl)}' AS pg_db (TYPE postgres);`,
    ];
  }

  async ensureTargetExists(_tableSpec: WarehouseSourceTable, _namespace: string): Promise<void> {
    mkdirSync(this.basePath, { recursive: true });
  }

  buildSourcePredicate(_tableSpec: WarehouseSourceTable, _namespace: string): string {
    return 'TRUE';
  }

  async writeRows(connection: DuckdbConnectionForSink, context: SinkQueryContext): Promise<void> {
    const parquetPath = this.getParquetPathForTable(context.tableSpec);
    const escapedPath = escapeSqlLiteral(parquetPath);
    const projectedSelect = buildProjectedSelectFromHistoryTable(
      context.tableSpec.postgresTable,
      context.sourcePredicate
    );
    await connection.run(`COPY (${projectedSelect}) TO '${escapedPath}' (FORMAT PARQUET, COMPRESSION zstd);`);
  }

  getResultTableName(tableSpec: WarehouseSourceTable): string {
    return this.getParquetPathForTable(tableSpec);
  }

  private getParquetPathForTable(tableSpec: WarehouseSourceTable): string {
    return join(this.basePath, `${tableSpec.tableKey}.parquet`).replace(/\\/g, '/');
  }
}
