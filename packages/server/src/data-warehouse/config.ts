// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { getResourceTypes } from '@medplum/core';
import type { MedplumDatabaseConfig, MedplumDatabaseSslConfig } from '../config/types';

/** Default Postgres `statement_timeout` applied to DuckDB-attached connections (PostgreSQL duration syntax). */
export const DEFAULT_DATABASE_STATEMENT_TIMEOUT = '15min';

/**
 * Sets libpq URI query parameters (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`) from {@link MedplumDatabaseSslConfig}.
 *
 * Unfortunately, we need to construct this file for DuckDB/libpq support
 * @param url - Parsed `postgresql:` URL (mutated).
 * @param ssl - Optional SSL options from server database config.
 */
function applyMedplumDatabaseSslToPostgresUrl(url: URL, ssl: MedplumDatabaseSslConfig | undefined): void {
  if (!ssl) {
    return;
  }

  const requireTls = ssl.require;
  const rejectUnauthorized = ssl.rejectUnauthorized;

  if (requireTls === false && !ssl.ca && !ssl.cert && !ssl.key) {
    url.searchParams.set('sslmode', 'disable');
    return;
  }

  const setPathParam = (param: 'sslrootcert' | 'sslcert' | 'sslkey', value: string | undefined): void => {
    if (value === undefined) {
      return;
    }
    url.searchParams.set(param, value);
  };

  setPathParam('sslrootcert', ssl.ca);
  setPathParam('sslcert', ssl.cert);
  setPathParam('sslkey', ssl.key);

  const hasRootCert = url.searchParams.has('sslrootcert');
  const hasClientCert = url.searchParams.has('sslcert') || url.searchParams.has('sslkey');

  if (rejectUnauthorized === false) {
    url.searchParams.set('sslmode', 'require');
    return;
  }

  if (hasRootCert) {
    url.searchParams.set('sslmode', 'verify-ca');
    return;
  }

  if (requireTls === true || rejectUnauthorized === true || hasClientCert) {
    url.searchParams.set('sslmode', 'verify-full');
  }
}

/**
 * @param databaseUrl - Postgres connection URI (may include other query parameters; any prior `options` is replaced).
 * @param statementTimeout - Postgres `statement_timeout` value (e.g. `15min`, `900s`).
 * @returns The same URL with `options` set to `-c statement_timeout=...`, percent-encoded for libpq.
 */
export function mergePostgresStatementTimeout(databaseUrl: string, statementTimeout: string): string {
  const trimmed = statementTimeout.trim();
  if (!trimmed) {
    return databaseUrl;
  }

  let url: URL;
  try {
    url = new URL(databaseUrl);
  } catch {
    throw new Error(`Invalid database URL: ${databaseUrl}`);
  }

  const optionsValue = `-c statement_timeout=${trimmed}`;
  const params = new URLSearchParams(url.search);
  params.set('options', optionsValue);
  const query = [...params.entries()].map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join('&');
  url.search = query ? `?${query}` : '';
  return url.toString();
}

/**
 * Builds a Postgres connection URI for DuckDB / libpq from {@link MedplumDatabaseConfig}, including
 * `statement_timeout` via the URL `options` query parameter and TLS via `sslmode` / `sslrootcert` / `sslcert` / `sslkey`
 * derived from {@link MedplumDatabaseConfig.ssl} in line with how `pg` uses the same object (encryption vs verify, CA paths).
 * When using an RDS proxy, callers should set `host` to the proxy endpoint and `ssl.require` to true (same as `database.ts` pool config).
 *
 * @param db - Medplum database settings; host, dbname, username, and password must be set.
 * @param statementTimeout - Postgres `statement_timeout` duration (e.g. `15min`); empty uses {@link DEFAULT_DATABASE_STATEMENT_TIMEOUT}.
 * @returns A `postgresql:` URI with `options=-c statement_timeout=...` encoded for libpq.
 */
export function buildPostgresUrlFromMedplumDatabaseConfig(db: MedplumDatabaseConfig, statementTimeout: string): string {
  const host = db.host;
  const dbname = db.dbname;
  const username = db.username;
  const password = db.password;
  const port = db.port ?? 5432;

  if (!host || !dbname || !username || !password) {
    throw new Error('Missing required database configuration: host, dbname, username, and password are required.');
  }

  const baseUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${dbname}`;
  const url = new URL(baseUrl);
  applyMedplumDatabaseSslToPostgresUrl(url, db.ssl);
  const trimmed = statementTimeout.trim();
  return mergePostgresStatementTimeout(url.toString(), trimmed || DEFAULT_DATABASE_STATEMENT_TIMEOUT);
}

/**
 * One Postgres source and its managed Iceberg table name.
 * `postgresTable` is used verbatim in SQL; `icebergTable` is used for S3 Tables / DuckDB paths.
 */
export interface WarehouseSourceTable {
  /** PostgreSQL table identifier as stored (double-quoted in SQL). */
  readonly postgresTable: string;
  /** Managed Iceberg / S3 Tables name: result of `toIcebergTableName(postgresTable)`. */
  readonly icebergTable: string;
  /** Stable key for sync logs and metadata (same as `icebergTable`). */
  readonly tableKey: string;
}

/**
 * Postgres history table names for all indexed repository resource types (`{ResourceType}_History`),
 * matching migrations (`resourceType + '_History'`).
 * Used by the scheduled data warehouse sync worker.
 *
 * @returns The list of Postgres table names.
 */
export function getWarehouseSyncPostgresTableNames(): string[] {
  return getResourceTypes().map((resourceType) => `${resourceType}_History`);
}

/**
 * Normalize a Postgres table identifier to the managed Iceberg / Parquet path segment: insert underscores at camelCase
 * breaks, then lowercase (non-alphanumeric except underscore → underscore).
 *
 * @param tableIdentifier - Postgres `relname`-style identifier (e.g. `AuditEvent_history`).
 * @returns Normalized name (e.g. `auditevent_history`).
 */
function toIcebergTableName(tableIdentifier: string): string {
  return tableIdentifier
    .replaceAll(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replaceAll(/[^a-zA-Z0-9_]/g, '_')
    .toLowerCase();
}

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

    const icebergTable = toIcebergTableName(postgresTable);
    resolved.push({ postgresTable, icebergTable, tableKey: icebergTable });
  }

  if (resolved.length === 0) {
    throw new Error('At least one Postgres table name is required when using --table');
  }

  return [...new Map(resolved.map((s) => [s.postgresTable, s])).values()];
}
