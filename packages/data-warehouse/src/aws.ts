// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import {
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  GetTableCommand,
  IcebergNullOrder,
  IcebergSortDirection,
  S3TablesClient,
} from '@aws-sdk/client-s3tables';
import type { IcebergMetadata, IcebergSortOrder } from '@aws-sdk/client-s3tables';
import { getWarehousePartitionSpec } from './resource-types.ts';
import { asSqlIdentifier } from './warehouse-sql.ts';

export interface DataWarehouseAwsClientOptions {
  region: string;
  workGroup?: string;
  outputLocation?: string;
  catalogName?: string;
}

export interface EnsureIcebergTableOptions {
  tableBucketArn: string;
  namespace: string;
  tableName: string;
}

export function getS3BucketNameFromS3TableArn(arn: string): string {
  const match = /:bucket\/([^/]+)$/.exec(arn.trim());
  if (!match?.[1]) {
    throw new Error(`Invalid AWS S3 Table ARN: ${arn}`);
  }
  return match[1];
}

export function getIcebergTableS3LocationFromBucket(bucket: string, namespace: string, table: string): string {
  const safeNamespace = asSqlIdentifier(namespace);
  const safeTable = asSqlIdentifier(table);
  return `s3://${bucket}/${safeNamespace}/${safeTable}/`;
}

/**
 * Iceberg `writeOrder`: ascending `project_id`, then `last_updated` (nulls last).
 *
 * @param sourceIdByColumnName - Map of Iceberg schema column name to field id (must include `project_id` and `last_updated`).
 * @returns Iceberg sort order with `orderId` 1 and two identity transform fields.
 */
export function buildWarehouseIcebergWriteOrder(sourceIdByColumnName: Record<string, number>): IcebergSortOrder {
  const projectId = sourceIdByColumnName.project_id;
  const lastUpdated = sourceIdByColumnName.last_updated;
  if (!projectId) {
    throw new Error('Iceberg write order requires schema field project_id');
  }
  if (!lastUpdated) {
    throw new Error('Iceberg write order requires schema field last_updated');
  }
  return {
    orderId: 1,
    fields: [
      {
        sourceId: projectId,
        transform: 'identity',
        direction: IcebergSortDirection.ASC,
        nullOrder: IcebergNullOrder.NULLS_LAST,
      },
      {
        sourceId: lastUpdated,
        transform: 'identity',
        direction: IcebergSortDirection.ASC,
        nullOrder: IcebergNullOrder.NULLS_LAST,
      },
    ],
  };
}

export class DataWarehouseAwsClient {
  private readonly s3TablesClient: S3TablesClient;

  constructor(options: DataWarehouseAwsClientOptions) {
    this.s3TablesClient = new S3TablesClient({ region: options.region });
  }

  async tableExists(tableBucketArn: string, namespace: string, tableName: string): Promise<boolean> {
    const safeNamespace = asSqlIdentifier(namespace);
    const safeTable = asSqlIdentifier(tableName);
    try {
      await this.s3TablesClient.send(
        new GetTableCommand({
          tableBucketARN: tableBucketArn,
          namespace: safeNamespace,
          name: safeTable,
        })
      );
      return true;
    } catch (error: any) {
      if (typeof error?.name === 'string' && error.name === 'NotFoundException') {
        return false;
      } else {
        throw error;
      }
    }
  }

  /**
   * Deletes a managed Iceberg table in S3 Tables. Returns `missing` when the table is not found (idempotent).
   *
   * @param options - Table bucket ARN, namespace, and Iceberg table name (SQL-sanitized identifiers).
   * @returns Whether the table existed and was deleted, or was already absent.
   */
  async deleteIcebergTable(options: EnsureIcebergTableOptions): Promise<'deleted' | 'missing'> {
    const safeNamespace = asSqlIdentifier(options.namespace);
    const safeTable = asSqlIdentifier(options.tableName);
    try {
      await this.s3TablesClient.send(
        new DeleteTableCommand({
          tableBucketARN: options.tableBucketArn,
          namespace: safeNamespace,
          name: safeTable,
        })
      );
      return 'deleted';
    } catch (error: any) {
      if (typeof error?.name === 'string' && error.name === 'NotFoundException') {
        return 'missing';
      }
      throw error;
    }
  }

  async ensureNamespaceExists(tableBucketArn: string, namespace: string): Promise<void> {
    const safeNamespace = asSqlIdentifier(namespace);
    try {
      await this.s3TablesClient.send(
        new CreateNamespaceCommand({
          tableBucketARN: tableBucketArn,
          namespace: [safeNamespace],
        })
      );
    } catch (error: any) {
      if (typeof error?.name === 'string' && (error.name.includes('Conflict') || error.name.includes('AlreadyExists'))) {
        return;
      }
      throw error;
    }
  }

  /**
   * Creates a managed Iceberg table with partition spec, {@link buildWarehouseIcebergWriteOrder},
   * and S3 Tables compaction strategy **sort** (aligned with the sort order).
   *
   * **Existing tables:** Adding or changing `writeOrder` / compaction is not an in-place update in
   * this helper; evolve or replace the table in AWS if you need new physical layout.
   *
   * @param options - Table bucket ARN, namespace, and Iceberg table name (SQL-sanitized identifiers).
   */
  async createIcebergTable(options: EnsureIcebergTableOptions): Promise<void> {
    const safeNamespace = asSqlIdentifier(options.namespace);
    const safeTable = asSqlIdentifier(options.tableName);
    const partitionSpec = getWarehousePartitionSpec({ icebergTableName: safeTable });
    const schemaFields = [
      { id: 1, name: 'id', type: 'string' },
      { id: 2, name: 'version_id', type: 'string' },
      { id: 3, name: 'content', type: 'string' },
      { id: 4, name: 'last_updated', type: 'timestamp' },
      { id: 5, name: 'project_id', type: 'string' },
    ];
    const sourceIdByColumnName = Object.fromEntries(schemaFields.map((f) => [f.name, f.id]));
    const iceberg: IcebergMetadata = {
      schema: {
        fields: schemaFields,
      },
      partitionSpec: {
        fields: partitionSpec.fields.map((field, index) => {
          const sourceId = sourceIdByColumnName[field.sourceColumn];
          if (!sourceId) {
            throw new Error(`Partition source column is not present in Iceberg schema: ${field.sourceColumn}`);
          }
          return {
            sourceId,
            transform: field.transform,
            name: field.name,
            fieldId: 1000 + index,
          };
        }),
      },
      // [Error: Not implemented Error: INSERT into a sorted iceberg table is not supported yet]
      // https://github.com/duckdb/duckdb-iceberg/issues/851
      // writeOrder: buildWarehouseIcebergWriteOrder(sourceIdByColumnName),
      properties: {
        table_type: 'ICEBERG',
        format: 'parquet',
        write_compression: 'zstd',
        // iceberg v3 isn't supported by Athena yet
        // 'format-version': '3',
      },
    };

    await this.s3TablesClient.send(
      new CreateTableCommand({
        tableBucketARN: options.tableBucketArn,
        namespace: safeNamespace,
        name: safeTable,
        format: 'ICEBERG',
        metadata: { iceberg },
      })
    );

  }
}
