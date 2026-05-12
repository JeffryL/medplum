// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { CreateNamespaceCommand, GetTableCommand, S3TablesClient } from '@aws-sdk/client-s3tables';
import { asSqlIdentifier } from './warehouse-sql';

export interface DataWarehouseAwsClientOptions {
  region: string;
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
      // ignore if already exists so it's idempotent
      if (
        typeof error?.name === 'string' &&
        (error.name.includes('Conflict') || error.name.includes('AlreadyExists'))
      ) {
        return;
      }
      throw error;
    }
  }
}
