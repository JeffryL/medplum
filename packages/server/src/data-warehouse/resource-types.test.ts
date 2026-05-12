// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { parseCommaSeparatedTableNames, WAREHOUSE_ICEBERG_PARTITION_FIELDS } from './resource-types';

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
  const daySpecFields = [
    { sourceColumn: 'project_id' as const, transform: 'identity' as const, name: 'project_id' },
    { sourceColumn: 'last_updated' as const, transform: 'day' as const, name: 'last_updated_day' },
  ];

  it('always partitions by project + daily last_updated', () => {
    expect(WAREHOUSE_ICEBERG_PARTITION_FIELDS).toEqual(daySpecFields);
  });
});
