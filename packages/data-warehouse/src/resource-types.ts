// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * Parse a comma-separated CLI list of Postgres table names (trimmed, empty entries dropped).
 */
export function parseCommaSeparatedTableNames(value: string | undefined): string[] | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = value
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);

  return parsed.length > 0 ? parsed : undefined;
}

export type WarehousePartitionTransform = 'identity' | 'year' | 'month' | 'day' | 'hour' | 'week';

export interface WarehousePartitionField {
  sourceColumn: 'project_id' | 'last_updated';
  transform: WarehousePartitionTransform;
  name: string;
}

export interface WarehousePartitionSpec {
  key: string;
  fields: readonly WarehousePartitionField[];
}

const PROJECT_ONLY_PARTITION_SPEC: WarehousePartitionSpec = {
  key: 'project-only',
  fields: [{ sourceColumn: 'project_id', transform: 'identity', name: 'project_id' }],
};

const PROJECT_AND_LAST_UPDATED_WEEK_PARTITION_SPEC: WarehousePartitionSpec = {
  key: 'project-and-last-updated-week',
  fields: [
    { sourceColumn: 'project_id', transform: 'identity', name: 'project_id' },
    { sourceColumn: 'last_updated', transform: 'week', name: 'last_updated_week' },
  ],
};

const WAREHOUSE_PARTITION_SPECS: Readonly<Record<string, WarehousePartitionSpec>> = {
  [PROJECT_ONLY_PARTITION_SPEC.key]: PROJECT_ONLY_PARTITION_SPEC,
  [PROJECT_AND_LAST_UPDATED_WEEK_PARTITION_SPEC.key]: PROJECT_AND_LAST_UPDATED_WEEK_PARTITION_SPEC,
};

/**
 * Iceberg table name (lowercase) → partition strategy. Unlisted tables use project-only.
 */
const PARTITION_SPEC_KEY_BY_ICEBERG_TABLE: Readonly<Record<string, string>> = {
  encounter_history: PROJECT_AND_LAST_UPDATED_WEEK_PARTITION_SPEC.key,
  observation_history: PROJECT_AND_LAST_UPDATED_WEEK_PARTITION_SPEC.key,
};

export function getWarehousePartitionSpec(options: { icebergTableName: string }): WarehousePartitionSpec {
  const strategyKey =
    PARTITION_SPEC_KEY_BY_ICEBERG_TABLE[options.icebergTableName.toLowerCase()] ?? PROJECT_ONLY_PARTITION_SPEC.key;
  const partitionSpec = WAREHOUSE_PARTITION_SPECS[strategyKey];
  if (!partitionSpec) {
    throw new Error(`Unknown warehouse partition strategy: ${strategyKey}`);
  }
  return partitionSpec;
}
