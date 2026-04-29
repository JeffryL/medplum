// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { Client } from 'pg';
import { DataWarehouseAwsClient } from './aws.ts';
import type { WarehouseSourceTable } from './export.ts';
import { asSqlIdentifier, DEFAULT_NAMESPACE } from './warehouse-sql.ts';

/** Summary returned after namespace and Iceberg tables are ensured in S3 Tables. */
export interface MigrateTablesSummary {
  /** Iceberg namespace passed through {@link asSqlIdentifier} (same string when valid). */
  namespace: string;
  /** Number of Iceberg tables created or verified. */
  tableCount: number;
}

export interface MigrateOptions {
  /** Postgres connection URI; used to verify each source table exists before S3 Tables provisioning. */
  databaseUrl: string;
  awsS3TableArn: string;
  s3Region: string;
  /** Tables to provision in S3 Tables (same shape as `export` / `sync`, from `--table` Postgres names). */
  warehouseSources: WarehouseSourceTable[];
  namespace?: string;
  athenaWorkGroup?: string;
  athenaCatalogName?: string;
}

/**
 * Verifies each {@link WarehouseSourceTable.postgresTable} exists as a visible base or partitioned table.
 * Matching is exact on `pg_class.relname` (same as double-quoted identifiers in `FROM pg_db."Name"`).
 *
 * @param databaseUrl - Postgres connection URI.
 * @param postgresTables - Distinct table names to check.
 * @throws Error listing any names that are not found or not visible on the session search_path.
 */
export async function verifyWarehousePostgresTablesExist(databaseUrl: string, postgresTables: string[]): Promise<void> {
  const unique = [...new Set(postgresTables)];
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    const missing: string[] = [];
    for (const relname of unique) {
      const { rows } = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_class c
          WHERE c.relkind IN ('r', 'p')
            AND c.relname = $1
            AND pg_catalog.pg_table_is_visible(c.oid)
        ) AS exists`,
        [relname]
      );
      if (!rows[0]?.exists) {
        missing.push(relname);
      }
    }
    if (missing.length > 0) {
      throw new Error(
        `Postgres table(s) not found (must exist as a base/partitioned table and be visible on search_path): ${missing.map((t) => JSON.stringify(t)).join(', ')}`
      );
    }
  } finally {
    await client.end();
  }
}

/**
 * Ensures S3 Tables namespace and Iceberg table definitions exist before data export/sync.
 * Table creation is performed via AWS S3 Tables API, not SQL DDL. New tables get a
 * `project_id` then `last_updated` write order and **sort** compaction; changing that on an
 * already-created table is an AWS replacement / evolution concern, not handled here.
 *
 * @param options - The options for the migrate tables operation.
 * @returns The summary of the migrate tables operation.
 */
export async function migrateTables(options: MigrateOptions): Promise<MigrateTablesSummary> {
  await verifyWarehousePostgresTablesExist(
    options.databaseUrl,
    options.warehouseSources.map((s) => s.postgresTable)
  );

  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const dwClient = new DataWarehouseAwsClient({
    region: options.s3Region,
    workGroup: options.athenaWorkGroup,
    catalogName: options.athenaCatalogName,
  });

  await dwClient.ensureNamespaceExists(options.awsS3TableArn, namespace);

  for (const spec of options.warehouseSources) {
    await dwClient.createIcebergTable({
      tableBucketArn: options.awsS3TableArn,
      namespace,
      tableName: spec.icebergTable,
    });
  }

  return { namespace, tableCount: options.warehouseSources.length };
}
