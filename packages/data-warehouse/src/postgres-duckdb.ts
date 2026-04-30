// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { asSqlIdentifier, escapeSqlLiteral } from './warehouse-sql.ts';

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
