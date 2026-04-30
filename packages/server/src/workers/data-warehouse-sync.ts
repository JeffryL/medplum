// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import type { Job, QueueBaseOptions } from 'bullmq';
import { Queue, Worker } from 'bullmq';
import type { MedplumDataWarehouseSyncConfig, MedplumServerConfig } from '../config/types';
import { globalLogger } from '../logger';
import type { WorkerInitializer, WorkerInitializerOptions } from './utils';
import { getBullmqRedisConnectionOptions, getWorkerBullmqConfig, queueRegistry } from './utils';
import {
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  resolveWarehouseSourcesFromPostgresTableNames,
  syncData
} from '@medplum/data-warehouse';
import type { SyncOptions } from '@medplum/data-warehouse';

export interface DataWarehouseSyncJobData {
  trigger: 'scheduler';
}

export const DataWarehouseSyncQueueName = 'DataWarehouseSyncQueue';
export const DataWarehouseSyncSchedulerId = 'data-warehouse-sync';

export const initDataWarehouseSyncWorker: WorkerInitializer = (config, options?: WorkerInitializerOptions) => {
  const defaultOptions: QueueBaseOptions = {
    connection: getBullmqRedisConnectionOptions(config),
  };

  const queue = new Queue<DataWarehouseSyncJobData>(DataWarehouseSyncQueueName, {
    ...defaultOptions,
    defaultJobOptions: {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 1000,
      },
    },
  });

  let worker: Worker<DataWarehouseSyncJobData> | undefined;
  if (options?.workerEnabled !== false) {
    const workerBullmq = getWorkerBullmqConfig(config, 'data-warehouse-sync') ?? {};
    const perWorkerConcurrency = config.workers?.bullmq?.['data-warehouse-sync']?.concurrency;
    worker = new Worker<DataWarehouseSyncJobData>(
      DataWarehouseSyncQueueName,
      async (job) => processDataWarehouseSyncJob(config, job),
      {
        ...defaultOptions,
        ...workerBullmq,
        concurrency: perWorkerConcurrency ?? 1,
      }
    );
  }

  if (options?.workerEnabled !== false) {
    refreshDataWarehouseSyncScheduler(config, queue).catch((err) => {
      globalLogger.error('Failed to refresh data warehouse sync scheduler', { err });
    });
  }

  return { queue, worker, name: DataWarehouseSyncQueueName };
};

export function getDataWarehouseSyncQueue(): Queue<DataWarehouseSyncJobData> | undefined {
  return queueRegistry.get(DataWarehouseSyncQueueName);
}

export async function refreshDataWarehouseSyncScheduler(
  config: MedplumServerConfig,
  queue: Queue<DataWarehouseSyncJobData>
): Promise<void> {
  const syncConfig = config.dataWarehouseSync;
  if (!syncConfig?.enabled) {
    try {
      await queue.removeJobScheduler(DataWarehouseSyncSchedulerId);
    } catch (err) {
      globalLogger.warn('Failed removing disabled data warehouse sync scheduler', { err });
    }
    return;
  }

  if (!syncConfig.cron) {
    throw new Error('dataWarehouseSync.cron is required when dataWarehouseSync.enabled is true');
  }

  await queue.upsertJobScheduler(
    DataWarehouseSyncSchedulerId,
    {
      pattern: syncConfig.cron,
    },
    {
      data: { trigger: 'scheduler' },
    }
  );
}

export async function processDataWarehouseSyncJob(
  config: MedplumServerConfig,
  _job: Job<DataWarehouseSyncJobData>
): Promise<void> {
  const syncOptions = getDataWarehouseSyncOptions(config);

  const result = await syncData({
    ...syncOptions,
    onProgress: (message, metadata) => {
      globalLogger.info(message, metadata);
    },
  });

  const inserted = result.resources.filter((resource) => resource.action === 'insert').length;
  const skipped = result.resources.length - inserted;
  globalLogger.info('Data warehouse sync completed', { inserted, skipped, total: result.resources.length });
}

export function getDataWarehouseSyncOptions(config: MedplumServerConfig): SyncOptions {
  const syncConfig = config.dataWarehouseSync;
  if (!syncConfig?.enabled) {
    throw new Error('dataWarehouseSync.enabled must be true to run scheduled sync');
  }

  const {
    s3Region,
    awsS3TableArn,
    warehouseTables,
    defaultRowThreshold,
    rowThresholdOverrides,
    namespace,
    athenaOutputLocation,
    athenaWorkGroup,
    athenaCatalogName,
  } = syncConfig;

  if (!s3Region) {
    throw new Error('dataWarehouseSync.s3Region is required');
  }
  if (!awsS3TableArn) {
    throw new Error('dataWarehouseSync.awsS3TableArn is required');
  }
  if (!warehouseTables || warehouseTables.length === 0) {
    throw new Error('dataWarehouseSync.warehouseTables must contain at least one table');
  }
  if (defaultRowThreshold !== undefined && defaultRowThreshold !== null && defaultRowThreshold <= 0) {
    throw new Error('dataWarehouseSync.defaultRowThreshold must be a positive integer');
  }

  return {
    database: getDataWarehouseSyncDatabaseConfig(config),
    s3Region,
    awsS3TableArn,
    namespace,
    athenaOutputLocation,
    athenaWorkGroup,
    athenaCatalogName,
    warehouseSources: resolveWarehouseSourcesFromPostgresTableNames(warehouseTables),
    defaultRowThreshold: defaultRowThreshold ?? undefined,
    rowThresholdOverrides,
  };
}

function getDataWarehouseSyncDatabaseConfig(config: MedplumServerConfig): SyncOptions['database'] {
  const syncConfig = config.dataWarehouseSync as MedplumDataWarehouseSyncConfig;
  const dbConfig = config.readonlyDatabase ?? config.database;
  const host = dbConfig.host;
  const dbname = dbConfig.dbname;
  const username = dbConfig.username;
  const password = dbConfig.password;
  const port = dbConfig.port ?? 5432;

  if (!host || !dbname || !username || !password) {
    throw new Error('database host/dbname/username/password are required for data warehouse sync connection');
  }

  return {
    host,
    port,
    dbname,
    username,
    password,
    statementTimeout: syncConfig.databaseStatementTimeout ?? getDatabaseStatementTimeout(config),
  };
}

function getDatabaseStatementTimeout(config: MedplumServerConfig): string {
  const timeoutMs = config.database.queryTimeout;
  if (!timeoutMs || timeoutMs <= 0) {
    return DEFAULT_DATABASE_STATEMENT_TIMEOUT;
  }

  return `${Math.ceil(timeoutMs / 1000)}s`;
}
