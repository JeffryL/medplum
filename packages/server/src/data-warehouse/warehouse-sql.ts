// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import type { Expression } from '../fhir/sql';
import { Column, Constant, InsertQuery, SelectQuery, SqlBuilder } from '../fhir/sql';

/** Default Iceberg catalog schema for warehouse objects. */
export const DEFAULT_NAMESPACE = 'default';

/** DuckDB catalog alias for attached S3 Tables Iceberg; not a Postgres schema/catalog name. */
export const S3_TABLES_CATALOG = 's3_tables_db';

/** DuckDB catalog alias for attached Postgres source tables. */
export const POSTGRES_CATALOG = 'pg_db';

const PROJECT_ID_JSON_PATH = '$.meta.project';

/** Iceberg / Parquet column names written for each resource history row (order matters for INSERT). */
export const WAREHOUSE_HISTORY_COLUMN_NAMES = ['id', 'version_id', 'content', 'last_updated', 'project_id'] as const;

function buildSql(buildFn: (sql: SqlBuilder) => void): string {
  const sql = new SqlBuilder();
  buildFn(sql);
  return sql.toString();
}

function buildCatalogQualifiedTableIdentifier(catalog: string, table: string): string {
  return `${catalog}"."${table}`;
}

function getQualifiedTableIdentifierParts(qualifiedTable: string, minParts = 1): string[] {
  const parts = qualifiedTable.split('.');
  if (parts.length < minParts || parts.some((part) => !part)) {
    throw new Error(`Invalid qualified table identifier: ${qualifiedTable}`);
  }
  return parts;
}

function buildQualifiedTableIdentifier(qualifiedTable: string, minParts = 1): string {
  return getQualifiedTableIdentifierParts(qualifiedTable, minParts).join('"."');
}

function appendQualifiedTableIdentifier(sql: SqlBuilder, qualifiedTable: string, minParts = 1): void {
  const parts = getQualifiedTableIdentifierParts(qualifiedTable, minParts);
  for (let i = 0; i < parts.length; i++) {
    if (i > 0) {
      sql.append('.');
    }
    sql.appendIdentifier(parts[i]);
  }
}

/**
 * DuckDB `ATTACH` for a PostgreSQL server (postgres extension), using the same alias as other data-warehouse DuckDB flows (`pg_db`).
 *
 * @param databaseUrl - Full Postgres connection URI (including any `options` for session GUCs such as `statement_timeout`).
 * @param alias - Unquoted DuckDB catalog name (default `pg_db`).
 * @returns SQL to run after `INSTALL postgres; LOAD postgres;`
 */
export function buildDuckdbPostgresAttachQuery(databaseUrl: string, alias = POSTGRES_CATALOG): string {
  return buildSql((sql) => {
    sql.append('ATTACH ');
    sql.appendLiteral(databaseUrl);
    sql.append(' AS ');
    sql.appendIdentifier(alias);
    sql.append(' (TYPE postgres, READ_ONLY);');
  });
}

export function buildCreateTableIfNotExistsAsQuery(qualifiedTable: string, selectQuery: string | Expression): string {
  return buildSql((sql) => {
    sql.append('CREATE TABLE IF NOT EXISTS ');
    appendQualifiedTableIdentifier(sql, qualifiedTable);
    sql.append(' AS ');
    if (typeof selectQuery === 'string') {
      sql.append(selectQuery);
    } else {
      sql.appendExpression(selectQuery);
    }
    sql.append(';');
  });
}

export function buildManagedIcebergQualifiedTable(namespace: string, icebergTable: string): string {
  return `${S3_TABLES_CATALOG}.${namespace}.${icebergTable}`;
}

export function buildInsertIntoSelectQuery(
  qualifiedTable: string,
  selectQuery: SelectQuery,
  insertColumns: string[] = [...WAREHOUSE_HISTORY_COLUMN_NAMES]
): string {
  const safeQualifiedTableName = buildQualifiedTableIdentifier(qualifiedTable);
  const query = new InsertQuery(safeQualifiedTableName, selectQuery, insertColumns);
  return buildSql((sql) => {
    sql.appendExpression(query);
    sql.append(';');
  });
}

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
export function buildProjectedSelectFromHistoryTableQuery(
  sourceHistoryTable: string,
  whereClause: string
): SelectQuery {
  const catalogQualifiedTableName = buildCatalogQualifiedTableIdentifier(POSTGRES_CATALOG, sourceHistoryTable);
  const escapedProjectIdPath = PROJECT_ID_JSON_PATH;
  const query = new SelectQuery(catalogQualifiedTableName);
  query
    .column('id')
    .column(new Column(catalogQualifiedTableName, 'versionId', false, 'version_id'))
    .column('content')
    .column(new Column(catalogQualifiedTableName, 'lastUpdated', false, 'last_updated'))
    // duckdb only allows json_extract_string() on JSON content
    .raw(`json_extract_string(content, '${escapedProjectIdPath}') AS project_id`)
    // ..and you can't call json_extract_string() on non-JSON content
    .where('content', '!=', null)
    .where('content', '!=', new Constant(`''`))
    .whereExpr(new Constant(`(${whereClause})`))
    .orderBy('lastUpdated');
  return query;
}

export function buildProjectedSelectFromHistoryTable(sourceHistoryTable: string, whereClause: string): string {
  return buildSql((sql) =>
    sql.appendExpression(buildProjectedSelectFromHistoryTableQuery(sourceHistoryTable, whereClause))
  );
}

export function buildCountFromHistoryTableQuery(sourceHistoryTable: string, whereClause: string): string {
  const catalogQualifiedTableName = buildCatalogQualifiedTableIdentifier(POSTGRES_CATALOG, sourceHistoryTable);
  const emptyString = new Constant(`''`);
  const query = new SelectQuery(catalogQualifiedTableName)
    .raw('COUNT(*) AS count')
    .where('content', '!=', null)
    .where('content', '!=', emptyString)
    .whereExpr(new Constant(`(${whereClause})`));
  return buildSql((sql) => {
    sql.appendExpression(query);
    sql.append(';');
  });
}

/**
 * Constructs a SQL predicate for selecting rows with a "lastUpdated" value greater than the current high-watermark.
 *
 * This predicate is used for incremental syncs, ensuring only new rows are considered. The query is intentionally written as
 *   (MAX(last_updated)) IS NULL OR lastUpdated > (MAX(last_updated))
 * and not just lastUpdated > (MAX(...)), for the following reasons:
 *
 * 1. The target table may be empty. In this case, MAX(last_updated) returns NULL. SQL `NULL > value` is unknown (not true!),
 *    so we must explicitly check for the NULL case. Only by adding IS NULL do we guarantee that when the table is empty,
 *    the predicate evaluates as true for all source rows.
 *
 * 2. DuckDB and Postgres strictly follow SQL three-valued logic; without this clause, no rows would be selected if there
 *    is not yet a high-watermark value.
 *
 * 3. Users may set up a table with no prior sync artifacts, and in that case, we need to ensure the initial sync (bootstrap)
 *    includes all data.
 *
 * @param qualifiedTable - Fully qualified table name (may include schema, etc.)
 * @returns SQL boolean expression for use in WHERE clauses to filter records for incremental sync based on lastUpdated.
 */
export function buildMaxLastUpdatedWatermarkPredicate(qualifiedTable: string): string {
  const safeQualifiedTableName = buildQualifiedTableIdentifier(qualifiedTable, 2);
  const maxLastUpdatedSubquery = buildSql((sql) =>
    sql.appendExpression(new SelectQuery(safeQualifiedTableName).raw('MAX(last_updated)'))
  );

  return buildSql((sql) => {
    sql.append('((');
    sql.append(maxLastUpdatedSubquery);
    sql.append(') IS NULL OR ');
    sql.appendIdentifier('lastUpdated');
    sql.append(' > (');
    sql.append(maxLastUpdatedSubquery);
    sql.append('))');
  });
}

export function buildCopySelectToParquetQuery(selectQuery: string, parquetPath: string): string {
  return buildSql((sql) => {
    sql.append('COPY (');
    sql.append(selectQuery);
    sql.append(') TO ');
    sql.appendLiteral(parquetPath);
    sql.append(' (FORMAT PARQUET, COMPRESSION zstd);');
  });
}

/**
 * `INSTALL` / `LOAD` lines for the managed Iceberg / S3 Tables DuckDB stack (extensions used by {@link buildManagedIcebergSetupQueries}).
 *
 * @see https://duckdb.org/docs/current/core_extensions/postgres
 * @returns SQL statements in execution order.
 */
export function buildManagedIcebergExtensionQueries(): string[] {
  return [
    `INSTALL aws;`,
    `LOAD aws;`,
    `INSTALL postgres;`,
    `LOAD postgres;`,
    `INSTALL httpfs;`,
    `LOAD httpfs;`,
    `INSTALL iceberg;`,
    `LOAD iceberg;`,
  ];
}

/**
 * DuckDB default secret for S3-backed Iceberg (managed S3 Tables path).
 * @see https://duckdb.org/docs/current/core_extensions/postgres
 * @see https://duckdb.org/docs/current/core_extensions/aws#credential_chain-provider
 * @param s3Region - AWS region passed to the secret.
 * @returns Single `CREATE SECRET` statement.
 */
export function buildManagedS3CredentialSecretQuery(s3Region: string): string {
  return buildSql((sql) => {
    sql.append('CREATE SECRET ( TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION ');
    sql.appendLiteral(s3Region);
    sql.append(' );');
  });
}

/**
 * DuckDB `ATTACH` for AWS S3 Tables as the `s3_tables_db` Iceberg catalog.
 *
 * @see https://duckdb.org/docs/current/core_extensions/iceberg/amazon_s3_tables
 * @param awsS3TableArn - S3 Tables bucket ARN.
 * @returns Single `ATTACH` statement.
 */
export function buildManagedS3TablesIcebergAttachQuery(awsS3TableArn: string): string {
  return buildSql((sql) => {
    sql.append('ATTACH ');
    sql.appendLiteral(awsS3TableArn);
    sql.append(' AS ');
    sql.appendIdentifier(S3_TABLES_CATALOG);
    sql.append(' ( TYPE iceberg, ENDPOINT_TYPE s3_tables );');
  });
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
  const queries: string[] = [...buildManagedIcebergExtensionQueries()];

  if (!options.localPath) {
    queries.push(buildManagedS3CredentialSecretQuery(options.s3Region));
  }

  queries.push(buildDuckdbPostgresAttachQuery(options.databaseUrl));
  queries.push(buildManagedS3TablesIcebergAttachQuery(options.awsS3TableArn));

  return queries;
}
