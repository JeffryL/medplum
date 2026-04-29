// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

export const DEFAULT_ROW_THRESHOLD = 1000;

/** Default Postgres `statement_timeout` applied to DuckDB-attached connections (PostgreSQL duration syntax). */
export const DEFAULT_DATABASE_STATEMENT_TIMEOUT = '15min';

export interface DatabaseConfigOptions {
  databaseUrl?: string;
  dbHost?: string;
  dbPort?: string;
  dbName?: string;
  dbUsername?: string;
  dbPassword?: string;
  /** Postgres `statement_timeout` (e.g. `15min`, `900s`). Overrides `MEDPLUM_DATABASE_STATEMENT_TIMEOUT` when set. */
  databaseStatementTimeout?: string;
}

/**
 * Returns a copy of the Postgres connection URL with `statement_timeout` set for the session,
 * using libpq's `options` parameter (`-c statement_timeout=...`), merged with any existing `options` value.
 *
 * @param databaseUrl - Postgres connection URI.
 * @param statementTimeout - Value for Postgres `statement_timeout` session setting (duration or ms).
 * @returns URI including merged `options` query parameter.
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

  const existing = url.searchParams.get('options')?.replace(/\s+/g, ' ').trim() ?? '';
  const stripped = existing.replace(/\s*-c\s+statement_timeout=\S+/gi, '').trim();
  const clause = `-c statement_timeout=${trimmed}`;
  const mergedOptions = stripped ? `${stripped} ${clause}` : clause;

  // URLSearchParams serializes spaces as "+"; libpq leaves "+" literal in `options`, so Postgres
  // sees an invalid GUC name (e.g. "+statement_timeout"). Rebuild query with encodeURIComponent (%20).
  url.searchParams.delete('options');
  const pairs: string[] = [];
  url.searchParams.forEach((value, key) => {
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
  });
  pairs.push(`options=${encodeURIComponent(mergedOptions)}`);
  url.search = pairs.length > 0 ? `?${pairs.join('&')}` : `?options=${encodeURIComponent(mergedOptions)}`;
  return url.toString();
}

export function resolveDatabaseUrl(options: DatabaseConfigOptions): string {
  let baseUrl: string;
  if (options.databaseUrl) {
    baseUrl = options.databaseUrl;
  } else {
    const host = options.dbHost ?? process.env.MEDPLUM_DATABASE_HOST ?? process.env.DATABASE_HOST;
    const port = options.dbPort ?? process.env.MEDPLUM_DATABASE_PORT ?? process.env.DATABASE_PORT ?? '5432';
    const dbName = options.dbName ?? process.env.MEDPLUM_DATABASE_DBNAME ?? process.env.DATABASE_DBNAME;
    const username = options.dbUsername ?? process.env.MEDPLUM_DATABASE_USERNAME ?? process.env.DATABASE_USERNAME;
    const password = options.dbPassword ?? process.env.MEDPLUM_DATABASE_PASSWORD ?? process.env.DATABASE_PASSWORD;

    if (!host || !dbName || !username || !password) {
      throw new Error(
        'Missing required database configuration. Provide --database-url or set host/dbname/username/password options (or MEDPLUM_DATABASE_* env vars).'
      );
    }

    baseUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${dbName}`;
  }

  const statementTimeout =
    options.databaseStatementTimeout?.trim() ||
    process.env.MEDPLUM_DATABASE_STATEMENT_TIMEOUT?.trim() ||
    DEFAULT_DATABASE_STATEMENT_TIMEOUT;

  return mergePostgresStatementTimeout(baseUrl, statementTimeout);
}

/**
 * Host, port, and database segment for logs. Omits credentials and query parameters.
 * @param databaseUrl - Postgres connection URL string.
 * @returns Human-readable host/port/database label, or "(invalid database URL)" if parsing fails.
 */
export function formatPostgresTargetLabel(databaseUrl: string): string {
  try {
    const url = new URL(databaseUrl);
    const db = decodeURIComponent(url.pathname.replace(/^\//, '') || '(default)');
    const host = url.hostname || 'localhost';
    return url.port ? `${host}:${url.port}/${db}` : `${host}/${db}`;
  } catch {
    return '(invalid database URL)';
  }
}

export function resolveAwsS3TableArn(value: string | undefined): string | undefined {
  return value ?? process.env.MEDPLUM_AWS_S3_TABLE_ARN ?? process.env.AWS_S3_TABLE_ARN;
}

export function resolveAthenaOutputLocation(value: string | undefined): string | undefined {
  return value ?? process.env.MEDPLUM_ATHENA_OUTPUT_LOCATION ?? process.env.ATHENA_OUTPUT_LOCATION;
}

export function resolveAthenaWorkGroup(value: string | undefined): string | undefined {
  return value ?? process.env.MEDPLUM_ATHENA_WORKGROUP ?? process.env.ATHENA_WORKGROUP;
}

export function resolveAthenaCatalogName(value: string | undefined): string | undefined {
  return value ?? process.env.MEDPLUM_ATHENA_CATALOG_NAME ?? process.env.ATHENA_CATALOG_NAME;
}

export function parseDefaultRowThreshold(value: string | undefined): number {
  if (!value) {
    return DEFAULT_ROW_THRESHOLD;
  }

  const threshold = Number.parseInt(value, 10);
  if (!Number.isFinite(threshold) || threshold <= 0) {
    throw new Error(`Invalid default row threshold: ${value}`);
  }

  return threshold;
}

export function parseRowThresholdOverrides(value: string | undefined): Record<string, number> {
  if (!value) {
    return {};
  }

  const parsed = JSON.parse(value) as Record<string, number>;
  const overrides: Record<string, number> = {};

  for (const [tableKey, threshold] of Object.entries(parsed)) {
    if (!Number.isFinite(threshold) || threshold <= 0) {
      throw new Error(`Invalid row threshold override for ${tableKey}: ${threshold}`);
    }
    overrides[tableKey] = Math.floor(threshold);
  }

  return overrides;
}

export function getThresholdForTableKey(
  tableKey: string,
  defaultThreshold: number,
  overrides: Record<string, number>
): number {
  return overrides[tableKey] ?? overrides.default ?? defaultThreshold;
}
