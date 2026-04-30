// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

export {
  dataWarehouseCliEnvDefaults,
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  DEFAULT_ROW_THRESHOLD,
  formatPostgresTargetLabel,
  getThresholdForTableKey,
  mergePostgresStatementTimeout,
  parseDefaultRowThreshold,
  parseRowThresholdOverrides,
  resolveAthenaCatalogName,
  resolveAthenaOutputLocation,
  resolveAthenaWorkGroup,
  resolveAwsS3TableArn,
  resolveDataWarehouseServiceOptionsFromCli,
  resolveDatabaseUrl,
} from './config.ts';
export type { DatabaseConfigOptions } from './config.ts';

export {
  WAREHOUSE_HISTORY_COLUMN_NAMES,
  buildManagedIcebergSetupQueries,
  buildProjectedSelectFromHistoryTable,
  resolveWarehouseSourcesFromPostgresTableNames,
  toIcebergTableName,
} from './export.ts';
export type { ExportOptions, WarehouseSourceTable } from './export.ts';

export { getSyncAction, syncData } from './sync.ts';
export type { SyncAction, SyncOptions, SyncResourceResult, SyncResult } from './sync.ts';
