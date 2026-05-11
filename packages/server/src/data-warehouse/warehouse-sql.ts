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
 * DuckDB `ATTACH` for a PostgreSQL server (postgres extension), using the same alias as export (`pg_db`).
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

export interface AthenaIcebergCreateTableOptions {
  qualifiedTable: string;
  columns: readonly { name: string; type: string }[];
  partitionedBy: readonly string[];
  location: string;
  tableProperties?: Readonly<Record<string, string>>;
}

/**
 * Builds an Athena Iceberg CREATE TABLE statement using the documented
 * PARTITIONED BY + LOCATION + TBLPROPERTIES shape.
 *
 * @param options - Athena table name, columns, partition transforms, location, and table properties.
 * @returns SQL `CREATE TABLE` statement using Athena Iceberg syntax.
 * @see https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html
 */
export function buildAthenaCreateIcebergTableQuery(options: AthenaIcebergCreateTableOptions): string {
  if (options.columns.length === 0) {
    throw new Error('Athena Iceberg CREATE TABLE requires at least one column');
  }
  if (options.partitionedBy.length === 0) {
    throw new Error('Athena Iceberg CREATE TABLE requires at least one PARTITIONED BY expression');
  }

  const columnsSql = options.columns.map((column) => `${asSqlIdentifier(column.name)} ${column.type}`).join(', ');
  const partitionedBySql = options.partitionedBy.join(', ');
  const tableProperties = {
    table_type: 'ICEBERG',
    ...(options.tableProperties ?? {}),
  };
  const tablePropertiesSql = Object.entries(tableProperties)
    .map(([propertyName, propertyValue]) => `'${escapeSqlLiteral(propertyName)}'='${escapeSqlLiteral(propertyValue)}'`)
    .join(', ');
  const location = escapeSqlLiteral(options.location);

  return `CREATE TABLE ${options.qualifiedTable} (${columnsSql}) PARTITIONED BY (${partitionedBySql}) LOCATION '${location}' TBLPROPERTIES (${tablePropertiesSql});`;
}
