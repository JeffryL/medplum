// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { getWarehousePartitionSpec, parseCommaSeparatedTableNames } from './resource-types.ts';

describe('parseCommaSeparatedTableNames', () => {
  it('splits, trims, and drops empty segments', () => {
    expect(parseCommaSeparatedTableNames('a, b ,c')).toEqual(['a', 'b', 'c']);
  });

  it('returns undefined for undefined, empty, or all-blank input', () => {
    expect(parseCommaSeparatedTableNames(undefined)).toBeUndefined();
    expect(parseCommaSeparatedTableNames('')).toBeUndefined();
    expect(parseCommaSeparatedTableNames('  ,  ')).toBeUndefined();
  });
});

describe('warehouse partition strategy', () => {
  it('defaults to project-only partitioning', () => {
    const partitionSpec = getWarehousePartitionSpec({ icebergTableName: 'patient' });
    expect(partitionSpec.key).toBe('project-only');
    expect(partitionSpec.fields).toEqual([{ sourceColumn: 'project_id', transform: 'identity', name: 'project_id' }]);
  });

  it('uses project + weekly last_updated for known Iceberg table names', () => {
    for (const name of ['encounter_history', 'EncounteR_History']) {
      const partitionSpec = getWarehousePartitionSpec({ icebergTableName: name });
      expect(partitionSpec.key).toBe('project-and-last-updated-week');
      expect(partitionSpec.fields).toEqual([
        { sourceColumn: 'project_id', transform: 'identity', name: 'project_id' },
        { sourceColumn: 'last_updated', transform: 'week', name: 'last_updated_week' },
      ]);
    }
  });
});
