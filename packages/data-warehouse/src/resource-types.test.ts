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
  const monthSpecFields = [
    { sourceColumn: 'project_id' as const, transform: 'identity' as const, name: 'project_id' },
    { sourceColumn: 'last_updated' as const, transform: 'month' as const, name: 'last_updated_month' },
  ];

  it('defaults to project + monthly last_updated for any Iceberg table name', () => {
    for (const name of ['patient', 'audit_event_history', 'encounter_history', 'EncounteR_History']) {
      const partitionSpec = getWarehousePartitionSpec({ icebergTableName: name });
      expect(partitionSpec.key).toBe('project-and-last-updated-month');
      expect(partitionSpec.fields).toEqual(monthSpecFields);
    }
  });
});
