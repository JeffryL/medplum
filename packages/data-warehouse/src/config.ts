// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * Default third arguments for commander `.option(…, …, default)` (from the process environment at module load).
 */
export const dataWarehouseCliEnvDefaults = {
  dbHost: process.env.MEDPLUM_DATABASE_HOST,
  dbPort: process.env.MEDPLUM_DATABASE_PORT || '5432',
  dbName: process.env.MEDPLUM_DATABASE_DBNAME,
  dbUsername: process.env.MEDPLUM_DATABASE_USERNAME,
  dbPassword: process.env.MEDPLUM_DATABASE_PASSWORD,
  databaseStatementTimeout: process.env.MEDPLUM_DATABASE_STATEMENT_TIMEOUT ?? '15min',
  s3Bucket: process.env.S3_BUCKET,
  s3Region: process.env.AWS_REGION || 'us-east-1',
  /** Primary env for S3 Table ARN (all commands that use managed Iceberg). */
  awsS3TableArn: process.env.MEDPLUM_AWS_S3_TABLE_ARN ?? process.env.AWS_S3_TABLE_ARN,
  athenaOutputLocation: process.env.MEDPLUM_ATHENA_OUTPUT_LOCATION ?? process.env.ATHENA_OUTPUT_LOCATION,
  athenaWorkGroup: process.env.MEDPLUM_ATHENA_WORKGROUP ?? process.env.ATHENA_WORKGROUP,
  athenaCatalogName: process.env.MEDPLUM_ATHENA_CATALOG_NAME ?? process.env.ATHENA_CATALOG_NAME,
  /** Comma-separated table names (export / migrate / sync / delete-table). */
  warehouseTableNames: process.env.MEDPLUM_DATA_WAREHOUSE_TABLES,
  defaultRowThreshold: process.env.MEDPLUM_DATA_WAREHOUSE_DEFAULT_ROW_THRESHOLD,
  rowThresholdsJson: process.env.MEDPLUM_DATA_WAREHOUSE_ROW_THRESHOLDS_JSON,
  /** `download` command: required S3 table ARN (env-only default). */
  downloadAwsS3TableArn: process.env.AWS_S3_TABLE_ARN,
} as const;

export const DEFAULT_ROW_THRESHOLD = 1000;

/** Default Postgres `statement_timeout` applied to DuckDB-attached connections (PostgreSQL duration syntax). */
export const DEFAULT_DATABASE_STATEMENT_TIMEOUT = '15min';

export interface DatabaseConfigOptions {
  dbHost?: string;
  dbPort?: string;
  dbName?: string;
  dbUsername?: string;
  dbPassword?: string;
  /** Postgres `statement_timeout` (e.g. `15min`, `900s`). Overrides `MEDPLUM_DATABASE_STATEMENT_TIMEOUT` when set. */
  databaseStatementTimeout?: string;
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
  const query = [...params.entries()]
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  url.search = query ? `?${query}` : '';
  return url.toString();
}

export function resolveDatabaseUrl(options: DatabaseConfigOptions): string {
  const host = options.dbHost ?? process.env.MEDPLUM_DATABASE_HOST ?? process.env.DATABASE_HOST;
  const port = options.dbPort ?? process.env.MEDPLUM_DATABASE_PORT ?? process.env.DATABASE_PORT ?? '5432';
  const dbName = options.dbName ?? process.env.MEDPLUM_DATABASE_DBNAME ?? process.env.DATABASE_DBNAME;
  const username = options.dbUsername ?? process.env.MEDPLUM_DATABASE_USERNAME ?? process.env.DATABASE_USERNAME;
  const password = options.dbPassword ?? process.env.MEDPLUM_DATABASE_PASSWORD ?? process.env.DATABASE_PASSWORD;

  if (!host || !dbName || !username || !password) {
    throw new Error(
      'Missing required database configuration. Set --db-host, --db-name, --db-username, and --db-password (or MEDPLUM_DATABASE_HOST, MEDPLUM_DATABASE_DBNAME, MEDPLUM_DATABASE_USERNAME, MEDPLUM_DATABASE_PASSWORD).'
    );
  }

  const baseUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/${dbName}`;

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

/**
 * @param options - Optional CLI flags for AWS S3 Tables and Athena.
 * @param options.awsS3TableArn - S3 table ARN (falls back to env when omitted).
 * @param options.athenaOutputLocation - Athena output S3 URI.
 * @param options.athenaWorkgroup - Athena workgroup name.
 * @param options.athenaCatalogName - Glue/Athena catalog name.
 * @returns Resolved values with the same env fallbacks as the individual `resolve*` helpers.
 */
export function resolveDataWarehouseServiceOptionsFromCli(options: {
  readonly awsS3TableArn?: string;
  readonly athenaOutputLocation?: string;
  readonly athenaWorkgroup?: string;
  readonly athenaCatalogName?: string;
}): {
  readonly awsS3TableArn: string | undefined;
  readonly athenaOutputLocation: string | undefined;
  readonly athenaWorkGroup: string | undefined;
  readonly athenaCatalogName: string | undefined;
} {
  return {
    awsS3TableArn: resolveAwsS3TableArn(options.awsS3TableArn),
    athenaOutputLocation: resolveAthenaOutputLocation(options.athenaOutputLocation),
    athenaWorkGroup: resolveAthenaWorkGroup(options.athenaWorkgroup),
    athenaCatalogName: resolveAthenaCatalogName(options.athenaCatalogName),
  };
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
