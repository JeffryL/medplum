// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LocalParquetWarehouseSink, S3TablesWarehouseSink } from './sink';

describe('data warehouse sinks', () => {
  test('s3tables sink builds managed setup queries', () => {
    const sink = new S3TablesWarehouseSink('us-east-1', 'arn:aws:s3tables:us-east-1:123456789012:bucket/test');
    const queries = sink.getSetupQueries('postgresql://user:pass@localhost/db');
    expect(queries.join('\n')).toContain('ATTACH \'arn:aws:s3tables:us-east-1:123456789012:bucket/test\'');
    expect(queries.join('\n')).toContain('ENDPOINT_TYPE s3_tables');
  });

  test('local sink returns parquet file result path', async () => {
    const basePath = mkdtempSync(join(tmpdir(), 'dw-local-sink-'));
    try {
      const sink = new LocalParquetWarehouseSink(basePath);
      const table = sink.getResultTableName({
        postgresTable: 'Patient_history',
        icebergTable: 'patient_history',
        tableKey: 'patient_history',
      });
      expect(table).toContain('patient_history.parquet');
      expect(sink.buildSourcePredicate({ postgresTable: 'a', icebergTable: 'a', tableKey: 'a' }, 'default')).toBe(
        'TRUE'
      );
    } finally {
      rmSync(basePath, { recursive: true, force: true });
    }
  });
});
