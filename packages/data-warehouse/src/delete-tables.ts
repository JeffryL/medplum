// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DataWarehouseAwsClient } from './aws.ts';
import type { WarehouseSourceTable } from './export.ts';
import { asSqlIdentifier, DEFAULT_NAMESPACE } from './warehouse-sql.ts';

export interface DeleteWarehouseTablesOptions {
  awsS3TableArn: string;
  s3Region: string;
  warehouseSources: WarehouseSourceTable[];
  namespace?: string;
}

export interface DeleteWarehouseTablesSummary {
  /** Iceberg table names that were deleted. */
  deleted: string[];
  /** Iceberg table names that were not found (no-op). */
  missing: string[];
}

/**
 * Deletes managed Iceberg tables in S3 Tables for each warehouse source (by derived Iceberg name).
 * Missing tables are reported in {@link DeleteWarehouseTablesSummary.missing} and do not fail the run.
 *
 * @param options - S3 table bucket ARN, region, namespace, and warehouse sources to delete.
 * @returns Lists of Iceberg table names that were deleted vs not found.
 */
export async function deleteWarehouseIcebergTables(
  options: DeleteWarehouseTablesOptions
): Promise<DeleteWarehouseTablesSummary> {
  const namespace = asSqlIdentifier(options.namespace ?? DEFAULT_NAMESPACE);
  const client = new DataWarehouseAwsClient({ region: options.s3Region });
  const deleted: string[] = [];
  const missing: string[] = [];
  for (const spec of options.warehouseSources) {
    const outcome = await client.deleteIcebergTable({
      tableBucketArn: options.awsS3TableArn,
      namespace,
      tableName: spec.icebergTable,
    });
    if (outcome === 'deleted') {
      deleted.push(spec.icebergTable);
    } else {
      missing.push(spec.icebergTable);
    }
  }
  return { deleted, missing };
}
