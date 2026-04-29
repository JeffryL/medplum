// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import fs from 'node:fs';
import { DuckDBInstance } from '@duckdb/node-api';
import { DataWarehouseAwsClient } from './aws.ts';
import {
  asSqlIdentifier,
  buildInsertIntoSelectQuery,
  buildManagedIcebergQualifiedTable,
  DEFAULT_NAMESPACE,
  escapeSqlLiteral,
} from './warehouse-sql.ts';

/**
 * One Postgres source and its managed Iceberg table name.
 * {@link postgresTable} is used verbatim in SQL; {@link icebergTable} is used for S3 Tables / DuckDB paths.
 */
export interface WarehouseSourceTable {
  /** PostgreSQL table identifier as stored (double-quoted in SQL). */
  readonly postgresTable: string;
  /** Managed Iceberg / S3 Tables name: {@link toIcebergTableName} of {@link postgresTable}. */
  readonly icebergTable: string;
  /** Keys row-threshold overrides and sync log lines (same as {@link icebergTable}). */
  readonly tableKey: string;
}

export interface ExportOptions {
  databaseUrl: string;
  s3Bucket?: string;
  s3Region: string;
  athenaOutputLocation?: string;
  athenaWorkGroup?: string;
  athenaCatalogName?: string;
  startWindow: string;
  /** If omitted, export all rows with lastUpdated >= startWindow (no upper bound). */
  endWindow?: string;
  /**
   * Postgres source tables and derived Iceberg names, typically from `--table` (one or more).
   * {@link WarehouseSourceTable.postgresTable} is never altered for SQL.
   */
  warehouseSources: WarehouseSourceTable[];
  namespace?: string;
  awsS3TableArn?: string; // Optional AWS S3 Table ARN for managed Iceberg
  localPath?: string; // Write Parquet files to local directory instead of S3 (skips AWS auth)
  /** When true with {@link ExportOptions.awsS3TableArn}, run `DROP TABLE IF EXISTS` per target table before create/export. */
  clean?: boolean;
}

const PROJECT_ID_JSON_PATH = '$.meta.project';

/**
 * Normalize a Postgres table identifier to the managed Iceberg / Parquet path segment: insert underscores at camelCase
 * breaks, then lowercase (non-alphanumeric except underscore → underscore).
 *
 * @param tableIdentifier - Postgres `relname`-style identifier (e.g. `AuditEvent_history`).
 * @returns Normalized name (e.g. `audit_event_history`).
 */
export function toIcebergTableName(tableIdentifier: string): string {
  return tableIdentifier
    .replaceAll(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replaceAll(/[^a-zA-Z0-9_]/g, '_')
    .toLowerCase();
}

const POSTGRES_WAREHOUSE_TABLE_SAFE = /^[A-Za-z][A-Za-z0-9_]*$/;

/**
 * Map CLI `--table` to warehouse sources. Postgres names are used verbatim; Iceberg names are {@link toIcebergTableName}(postgres).
 * The `migrate` command checks these exist in Postgres before provisioning S3 Tables.
 *
 * @param tableNames - Raw CLI tokens (trimmed per entry); each non-empty Postgres identifier must be `[A-Za-z][A-Za-z0-9_]*`.
 * @returns Deduplicated sources in first-seen order (by {@link WarehouseSourceTable.postgresTable}).
 */
export function resolveWarehouseSourcesFromPostgresTableNames(tableNames: string[]): WarehouseSourceTable[] {
  const resolved: WarehouseSourceTable[] = [];
  for (const raw of tableNames) {
    const postgresTable = raw.trim();
    if (!postgresTable) {
      continue;
    }

    if (!POSTGRES_WAREHOUSE_TABLE_SAFE.test(postgresTable)) {
      throw new Error(
        `Invalid Postgres table name ${JSON.stringify(postgresTable)}: use only ASCII letters, digits, and underscore, starting with a letter (exact identifier for your database)`
      );
    }

    const icebergTable = toIcebergTableName(postgresTable);
    resolved.push({ postgresTable, icebergTable, tableKey: icebergTable });
  }

  if (resolved.length === 0) {
    throw new Error('At least one Postgres table name is required when using --table');
  }

  return [...new Map(resolved.map((s) => [s.postgresTable, s])).values()];
}

/**
 * @param warehouseSources - Optional list from {@link ExportOptions.warehouseSources}.
 * @returns Non-empty {@link WarehouseSourceTable} list.
 * @throws Error if {@link ExportOptions.warehouseSources} is missing or empty.
 */
function requireWarehouseSources(warehouseSources: WarehouseSourceTable[] | undefined): WarehouseSourceTable[] {
  if (warehouseSources && warehouseSources.length > 0) {
    return warehouseSources;
  }
  throw new Error('At least one Postgres table is required: pass --table with a comma-separated list of table names.');
}

/** DuckDB connection shape used for Iceberg probes and export execution. */
export type DuckdbRunnable = { run(sql: string): Promise<unknown> };

/**
 * Returns whether the managed Iceberg table was readable before this export mutates it.
 * Used to decide if the table is new (create-from-scratch) vs an incremental re-export.
 *
 * @param connection - DuckDB connection with `run`.
 * @param qualifiedTable - Trusted `catalog.schema.table` identifier string.
 * @returns True if `SELECT 1 FROM … LIMIT 1` succeeds.
 */
export async function probeManagedIcebergTableExists(connection: DuckdbRunnable, qualifiedTable: string): Promise<boolean> {
  try {
    await connection.run(`SELECT 1 FROM ${qualifiedTable} LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

/**
 * Whether to run `CALL set_iceberg_table_properties` after INSERT: after `--clean` (drop + recreate)
 * or when the table did not exist before this run (first-time create).
 *
 * @param clean - Export `--clean` flag.
 * @param tableExistedBeforeMutate - Result of {@link probeManagedIcebergTableExists} before DROP/CREATE for this table.
 * @returns Whether to run `CALL set_iceberg_table_properties` after INSERT.
 */
export function shouldApplyIcebergTablePropertiesAfterInsert(
  clean: boolean | undefined,
  tableExistedBeforeMutate: boolean
): boolean {
  return Boolean(clean) || !tableExistedBeforeMutate;
}

/**
 * Predicate on source Postgres "lastUpdated" for [start, end) or [start, ∞).
 * @param startWindow - Inclusive lower bound (ISO 8601).
 * @param endWindow - Exclusive upper bound, or undefined for no upper bound.
 * @returns SQL expression using double-quoted lastUpdated (Postgres column name).
 */
function getSourceLastUpdatedWindowWhereClause(startWindow: string, endWindow?: string): string {
  const escapedStart = escapeSqlLiteral(startWindow);
  if (endWindow === undefined) {
    return `"lastUpdated" >= '${escapedStart}'`;
  }
  const escapedEnd = escapeSqlLiteral(endWindow);
  return `"lastUpdated" >= '${escapedStart}' AND "lastUpdated" < '${escapedEnd}'`;
}

/**
 * Predicate on destination "last_updated" for [start, end) or [start, ∞).
 * @param startWindow - Inclusive lower bound (ISO 8601).
 * @param endWindow - Exclusive upper bound, or undefined for no upper bound.
 * @returns SQL expression using lowercase snake_case last_updated column.
 */
function getTargetLastUpdatedWindowWhereClause(startWindow: string, endWindow?: string): string {
  const escapedStart = escapeSqlLiteral(startWindow);
  if (endWindow === undefined) {
    return `last_updated >= '${escapedStart}'`;
  }
  const escapedEnd = escapeSqlLiteral(endWindow);
  return `last_updated >= '${escapedStart}' AND last_updated < '${escapedEnd}'`;
}

function resolveWarehouseSources(options: ExportOptions): WarehouseSourceTable[] {
  return requireWarehouseSources(options.warehouseSources);
}

/** Options required to build managed Iceberg attach/setup SQL (extensions, secrets, attach). */
export interface ManagedIcebergAttachOptions {
  databaseUrl: string;
  s3Region: string;
  awsS3TableArn: string;
  namespace?: string;
  localPath?: string;
}

function toManagedIcebergAttachOptions(options: ExportOptions): ManagedIcebergAttachOptions {
  return {
    databaseUrl: options.databaseUrl,
    s3Region: options.s3Region,
    awsS3TableArn: options.awsS3TableArn as string,
    namespace: options.namespace,
    localPath: options.localPath,
  };
}

/** Iceberg / Parquet column names written for each resource history row (order matters for INSERT). */
export const WAREHOUSE_HISTORY_COLUMN_NAMES = ['id', 'version_id', 'content', 'last_updated', 'project_id'] as const;

/**
 * Projects a Medplum history Postgres table into the warehouse column layout.
 *
 * @param sourceHistoryTable - Postgres table identifier exactly as stored (e.g. `Patient_history` or `Patient_History`).
 * @param whereClause - SQL boolean expression (joined with `AND` after non-empty content filter).
 * @returns DuckDB `SELECT` statement text (no trailing semicolon).
 */
export function buildProjectedSelectFromHistoryTable(sourceHistoryTable: string, whereClause: string): string {
  return `SELECT id, "versionId" AS version_id, content, "lastUpdated" AS last_updated, json_extract_string(content, '${PROJECT_ID_JSON_PATH}') AS project_id FROM pg_db."${sourceHistoryTable}" WHERE content IS NOT NULL AND content != '' AND (${whereClause})`;
}

/**
 * Queries through `CREATE SCHEMA` for a managed Iceberg export (extensions, secrets, attach).
 *
 * @param options - Attach options; requires `awsS3TableArn`.
 * @returns SQL strings to run in order before per-table mutations.
 */
export function buildManagedIcebergSetupQueries(options: ManagedIcebergAttachOptions): string[] {
  const queries: string[] = [];
  queries.push(`INSTALL aws;`);
  queries.push(`LOAD aws;`);
  queries.push(`INSTALL postgres;`);
  queries.push(`LOAD postgres;`);
  queries.push(`INSTALL httpfs;`);
  queries.push(`LOAD httpfs;`);
  queries.push(`INSTALL iceberg;`);
  queries.push(`LOAD iceberg;`);

  if (!options.localPath) {
    queries.push(`CREATE SECRET ( TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION '${escapeSqlLiteral(options.s3Region)}' );`);
  }

  const escapedDatabaseUrl = escapeSqlLiteral(options.databaseUrl);
  queries.push(`ATTACH '${escapedDatabaseUrl}' AS pg_db (TYPE postgres);`);

  const escapedS3TableArn = escapeSqlLiteral(options.awsS3TableArn);
  queries.push(`ATTACH '${escapedS3TableArn}' AS s3_tables_db ( TYPE iceberg, ENDPOINT_TYPE s3_tables );`);

  return queries;
}

/**
 * Per-resource DELETE window + INSERT for an already-provisioned managed Iceberg table.
 *
 * @param options - Export options.
 * @param spec - Postgres source and target Iceberg identifier.
 * @returns SQL strings for one Iceberg table.
 */
export function buildManagedIcebergTableMutationQueries(options: ExportOptions, spec: WarehouseSourceTable): string[] {
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const sourceLastUpdatedWhere = getSourceLastUpdatedWindowWhereClause(options.startWindow, options.endWindow);
  const targetLastUpdatedWhere = getTargetLastUpdatedWindowWhereClause(options.startWindow, options.endWindow);
  const projectedSourceSelect = buildProjectedSelectFromHistoryTable(spec.postgresTable, sourceLastUpdatedWhere);
  const insertColumns = WAREHOUSE_HISTORY_COLUMN_NAMES.join(', ');
  const qualifiedTable = buildManagedIcebergQualifiedTable(namespace, spec.icebergTable);
  const queries: string[] = [];
  queries.push(`DELETE FROM ${qualifiedTable} WHERE ${targetLastUpdatedWhere};`);
  queries.push(buildInsertIntoSelectQuery(qualifiedTable, insertColumns, projectedSourceSelect));
  return queries;
}

export function buildExportQueries(options: ExportOptions): string[] {
  const queries: string[] = [];
  const sources = resolveWarehouseSources(options);

  if (options.awsS3TableArn) {
    queries.push(...buildManagedIcebergSetupQueries(toManagedIcebergAttachOptions(options)));
    for (const spec of sources) {
      queries.push(...buildManagedIcebergTableMutationQueries(options, spec));
    }
    return queries;
  }

  const escapedDatabaseUrl = escapeSqlLiteral(options.databaseUrl);
  const sourceLastUpdatedWhere = getSourceLastUpdatedWindowWhereClause(options.startWindow, options.endWindow);

  queries.push(`INSTALL postgres;`);
  queries.push(`LOAD postgres;`);
  queries.push(`INSTALL httpfs;`);
  queries.push(`LOAD httpfs;`);

  if (!options.localPath) {
    queries.push(`CREATE SECRET ( TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION '${escapeSqlLiteral(options.s3Region)}' );`);
  }

  queries.push(`ATTACH '${escapedDatabaseUrl}' AS pg_db (TYPE postgres);`);

  // Fallback: Write unmanaged partitioned Parquet files
  const s3Path = options.localPath || `s3://${options.s3Bucket}`;
  const safeStart = options.startWindow.replace(/[:.T]/g, '-').replace('Z', '');
  const safeEnd = options.endWindow?.replace(/[:.T]/g, '-').replace('Z', '') ?? 'open';

  for (const spec of sources) {
    const parquetFile = `${s3Path}/${spec.icebergTable}/window_${safeStart}_${safeEnd}.parquet`;

    queries.push(
      `COPY (${buildProjectedSelectFromHistoryTable(spec.postgresTable, sourceLastUpdatedWhere)}) TO '${escapeSqlLiteral(parquetFile)}' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6);`
    );
  }

  return queries;
}

async function runExportQuery(connection: DuckdbRunnable, query: string): Promise<void> {
  if (
    query.startsWith('DROP TABLE') ||
    query.startsWith('CREATE TABLE') ||
    query.startsWith('DELETE') ||
    query.startsWith('INSERT') ||
    query.trimStart().startsWith('CALL set_iceberg')
  ) {
    console.log(`Executing: ${query}`);
  }
  await connection.run(query);
}

async function executeManagedIcebergExport(connection: DuckdbRunnable, options: ExportOptions): Promise<void> {
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const sources = resolveWarehouseSources(options);
  const athenaClient = new DataWarehouseAwsClient({
    region: options.s3Region,
    outputLocation: options.athenaOutputLocation,
    workGroup: options.athenaWorkGroup,
    catalogName: options.athenaCatalogName,
  });

  for (const q of buildManagedIcebergSetupQueries(toManagedIcebergAttachOptions(options))) {
    await runExportQuery(connection, q);
  }

  for (const spec of sources) {
    const { icebergTable } = spec;
    const exists = await athenaClient.tableExists(options.awsS3TableArn as string, namespace, icebergTable);
    if (!exists) {
      throw new Error(
        `Managed Iceberg table does not exist: ${namespace}.${icebergTable}. Run the migrate command before export.`
      );
    }
    const mutations = buildManagedIcebergTableMutationQueries(options, spec);
    for (const q of mutations) {
      await runExportQuery(connection, q);
    }
  }
}

export async function exportData(options: ExportOptions): Promise<void> {
  if (options.localPath) {
    for (const spec of resolveWarehouseSources(options)) {
      fs.mkdirSync(`${options.localPath}/${spec.icebergTable}`, { recursive: true });
    }
  }

  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();

  try {
    if (options.awsS3TableArn) {
      await executeManagedIcebergExport(connection, options);
    } else {
      const queries = buildExportQueries(options);
      for (const query of queries) {
        await runExportQuery(connection, query);
      }
    }
  } finally {
    connection.closeSync();
  }
}
