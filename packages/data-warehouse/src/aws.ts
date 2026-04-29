// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { CreateNamespaceCommand, CreateTableCommand, GetTableCommand, S3TablesClient } from '@aws-sdk/client-s3tables';
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

export class DataWarehouseAwsClient {
  private readonly s3TablesClient: S3TablesClient;

  constructor(options: DataWarehouseAwsClientOptions) {
    this.s3TablesClient = new S3TablesClient({ region: options.region });
  }

  /**
   * Throws if the named Iceberg table is not registered in the S3 Tables bucket.
   * Does not create tables; use {@link createIcebergTable} for programmatic provisioning.
   *
   * @param tableBucketArn - S3 Tables bucket ARN.
   * @param namespace - Iceberg namespace (validated as a SQL identifier).
   * @param tableName - Iceberg table name (validated as a SQL identifier).
   */
  async assertIcebergTableExists(tableBucketArn: string, namespace: string, tableName: string): Promise<void> {
    if (!(await this.tableExists(tableBucketArn, namespace, tableName))) {
      const safeNamespace = asSqlIdentifier(namespace);
      const safeTable = asSqlIdentifier(tableName);
      throw new Error(
        `Managed Iceberg table does not exist: ${safeNamespace}.${safeTable}. Create it in AWS S3 Tables before running migrate.`
      );
    }
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
    const sourceIdByColumnName = Object.fromEntries(schemaFields.map((f) => [f.name, f.id])) as Record<string, number>;
    // eslint-disable-next-line no-useless-catch
    try {
      await this.s3TablesClient.send(
        new CreateTableCommand({
          tableBucketARN: options.tableBucketArn,
          namespace: safeNamespace,
          name: safeTable,
          format: 'ICEBERG',
          metadata: {
            iceberg: {
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
              properties: {
                table_type: 'ICEBERG',
                format: 'parquet',
                write_compression: 'zstd',
                'format-version': '3',
              },
            },
          } as any,
        })
      );
    } catch (error: any) {
      throw error;
    }
  }
}
