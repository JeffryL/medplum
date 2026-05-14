// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { CreateNamespaceCommand, GetTableCommand, S3TablesClient } from '@aws-sdk/client-s3tables';

export interface DataWarehouseAwsClientOptions {
  region: string;
}

export class DataWarehouseAwsClient {
  private readonly s3TablesClient: S3TablesClient;

  constructor(options: DataWarehouseAwsClientOptions) {
    this.s3TablesClient = new S3TablesClient({ region: options.region });
  }

  async tableExists(tableBucketArn: string, namespace: string, tableName: string): Promise<boolean> {
    try {
      await this.s3TablesClient.send(
        new GetTableCommand({
          tableBucketARN: tableBucketArn,
          namespace,
          name: tableName,
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
    try {
      await this.s3TablesClient.send(
        new CreateNamespaceCommand({
          tableBucketARN: tableBucketArn,
          namespace: [namespace],
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
