// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * Parse a comma-separated CLI list of Postgres table names (trimmed, empty entries dropped).
 * @param value - Raw comma-separated string from CLI or env, or undefined.
 * @returns Non-empty array of table names, or undefined if input is missing or yields no names.
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

export type WarehousePartitionTransform = 'identity' | 'year' | 'month' | 'day' | 'hour';

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

const PROJECT_AND_LAST_UPDATED_MONTH_PARTITION_SPEC: WarehousePartitionSpec = {
  key: 'project-and-last-updated-month',
  fields: [
    { sourceColumn: 'project_id', transform: 'identity', name: 'project_id' },
    { sourceColumn: 'last_updated', transform: 'month', name: 'last_updated_month' },
  ],
};

const WAREHOUSE_PARTITION_SPECS: Readonly<Record<string, WarehousePartitionSpec>> = {
  [PROJECT_ONLY_PARTITION_SPEC.key]: PROJECT_ONLY_PARTITION_SPEC,
  [PROJECT_AND_LAST_UPDATED_MONTH_PARTITION_SPEC.key]: PROJECT_AND_LAST_UPDATED_MONTH_PARTITION_SPEC,
};

/** Default partition strategy for Iceberg tables (override per table name when needed). */
const DEFAULT_PARTITION_STRATEGY_KEY = PROJECT_AND_LAST_UPDATED_MONTH_PARTITION_SPEC.key;

/**
 * Iceberg table name (lowercase) → partition strategy key. Unlisted tables use
 * {@link DEFAULT_PARTITION_STRATEGY_KEY} (`project_id` + `month(last_updated)`).
 */
const PARTITION_SPEC_KEY_BY_ICEBERG_TABLE: Readonly<Record<string, string>> = {};

export function getWarehousePartitionSpec(options: { icebergTableName: string }): WarehousePartitionSpec {
  const strategyKey =
    PARTITION_SPEC_KEY_BY_ICEBERG_TABLE[options.icebergTableName.toLowerCase()] ?? DEFAULT_PARTITION_STRATEGY_KEY;
  const partitionSpec = WAREHOUSE_PARTITION_SPECS[strategyKey];
  if (!partitionSpec) {
    throw new Error(`Unknown warehouse partition strategy: ${strategyKey}`);
  }
  return partitionSpec;
}
