// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/** Default Iceberg catalog schema when {@link asSqlIdentifier} is applied to namespace. */
export const DEFAULT_NAMESPACE = 'default';

export function escapeSqlLiteral(value: string): string {
  return value.replaceAll("'", "''");
}

export function asSqlIdentifier(value: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }
  return value;
}

/**
 * DuckDB `ATTACH` for a PostgreSQL server (postgres extension), using the same alias as other data-warehouse DuckDB flows (`pg_db`).
 *
 * @param databaseUrl - Full Postgres connection URI (including any `options` for session GUCs such as `statement_timeout`).
 * @param alias - Unquoted DuckDB catalog name (default `pg_db`).
 * @returns SQL to run after `INSTALL postgres; LOAD postgres;`
 */
export function buildDuckdbPostgresAttachQuery(databaseUrl: string, alias = 'pg_db'): string {
  const name = asSqlIdentifier(alias);
  return `ATTACH '${escapeSqlLiteral(databaseUrl)}' AS ${name} (TYPE postgres);`;
}

export function buildCreateTableIfNotExistsAsQuery(qualifiedTable: string, selectQuery: string): string {
  return `CREATE TABLE IF NOT EXISTS ${qualifiedTable} AS ${selectQuery};`;
}

export function buildManagedIcebergQualifiedTable(namespace: string, icebergTable: string): string {
  return `s3_tables_db.${namespace}.${icebergTable}`;
}

export function buildInsertIntoSelectQuery(qualifiedTable: string, columns: string, selectQuery: string): string {
  return `INSERT INTO ${qualifiedTable} (${columns}) ${selectQuery};`;
}

const PROJECT_ID_JSON_PATH = '$.meta.project';

/** Iceberg / Parquet column names written for each resource history row (order matters for INSERT). */
export const WAREHOUSE_HISTORY_COLUMN_NAMES = ['id', 'version_id', 'content', 'last_updated', 'project_id'] as const;

/**
 * Projects a Medplum history Postgres table into the warehouse column layout.
 *
 * Rows are ordered by source `"lastUpdated"` so writers (Iceberg INSERT, Parquet COPY) emit
 * physically sorted data for time-range locality within files.
 *
 * @param sourceHistoryTable - Postgres table identifier exactly as stored (e.g. `Patient_history` or `Patient_History`).
 * @param whereClause - SQL boolean expression (joined with `AND` after non-empty content filter).
 * @returns DuckDB `SELECT` statement text (no trailing semicolon).
 */
export function buildProjectedSelectFromHistoryTable(sourceHistoryTable: string, whereClause: string): string {
  return `SELECT id, "versionId" AS version_id, content, "lastUpdated" AS last_updated, json_extract_string(content, '${PROJECT_ID_JSON_PATH}') AS project_id FROM pg_db."${sourceHistoryTable}" WHERE content IS NOT NULL AND content != '' AND (${whereClause}) ORDER BY "lastUpdated"`;
}

/** Options required to build managed Iceberg attach/setup SQL (extensions, secrets, attach). */
export interface ManagedIcebergAttachOptions {
  databaseUrl: string;
  s3Region: string;
  awsS3TableArn: string;
  namespace?: string;
  localPath?: string;
}

/**
 * DuckDB setup for managed Iceberg (extensions, optional S3 secret, Postgres attach, S3 Tables attach).
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
    queries.push(
      `CREATE SECRET ( TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION '${escapeSqlLiteral(options.s3Region)}' );`
    );
  }

  queries.push(buildDuckdbPostgresAttachQuery(options.databaseUrl));

  const escapedS3TableArn = escapeSqlLiteral(options.awsS3TableArn);
  queries.push(`ATTACH '${escapedS3TableArn}' AS s3_tables_db ( TYPE iceberg, ENDPOINT_TYPE s3_tables );`);

  return queries;
}
