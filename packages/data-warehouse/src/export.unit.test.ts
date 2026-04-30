// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  buildExportQueries,
  resolveWarehouseSourcesFromPostgresTableNames,
  shouldApplyIcebergTablePropertiesAfterInsert,
} from './export.ts';

const auditEventHistorySources = resolveWarehouseSourcesFromPostgresTableNames(['AuditEvent_history']);

describe('shouldApplyIcebergTablePropertiesAfterInsert', () => {
  it('is true when --clean (drop + recreate)', () => {
    expect(shouldApplyIcebergTablePropertiesAfterInsert(true, true)).toBe(true);
    expect(shouldApplyIcebergTablePropertiesAfterInsert(true, false)).toBe(true);
  });

  it('is true when the table did not exist before this run', () => {
    expect(shouldApplyIcebergTablePropertiesAfterInsert(undefined, false)).toBe(true);
    expect(shouldApplyIcebergTablePropertiesAfterInsert(false, false)).toBe(true);
  });

  it('is false for incremental export onto an existing table', () => {
    expect(shouldApplyIcebergTablePropertiesAfterInsert(undefined, true)).toBe(false);
    expect(shouldApplyIcebergTablePropertiesAfterInsert(false, true)).toBe(false);
  });
});

describe('resolveWarehouseSourcesFromPostgresTableNames', () => {
  it('uses toIcebergTableName on the Postgres identifier (distinction between patient and patient_history is preserved)', () => {
    expect(resolveWarehouseSourcesFromPostgresTableNames(['Patient_History', 'serviceRequest_history'])).toEqual([
      {
        postgresTable: 'Patient_History',
        icebergTable: 'patient_history',
        tableKey: 'patient_history',
      },
      {
        postgresTable: 'serviceRequest_history',
        icebergTable: 'service_request_history',
        tableKey: 'service_request_history',
      },
    ]);
  });

  it('dedupes by postgres table name', () => {
    expect(resolveWarehouseSourcesFromPostgresTableNames(['Patient_history', 'Patient_history'])).toEqual([
      {
        postgresTable: 'Patient_history',
        icebergTable: 'patient_history',
        tableKey: 'patient_history',
      },
    ]);
  });

  it('accepts arbitrary Postgres tables', () => {
    expect(resolveWarehouseSourcesFromPostgresTableNames(['Patient', 'my_custom_events'])).toEqual([
      {
        postgresTable: 'Patient',
        icebergTable: 'patient',
        tableKey: 'patient',
      },
      {
        postgresTable: 'my_custom_events',
        icebergTable: 'my_custom_events',
        tableKey: 'my_custom_events',
      },
    ]);
  });

  it('maps odd identifiers with the same toIcebergTableName rules as elsewhere', () => {
    expect(resolveWarehouseSourcesFromPostgresTableNames(['NotAType_history'])).toEqual([
      {
        postgresTable: 'NotAType_history',
        icebergTable: 'not_atype_history',
        tableKey: 'not_atype_history',
      },
    ]);
  });
});

describe('Data Warehouse Export - Unit Tests', () => {
  it('should build correct SQL queries for production environment (Parquet fallback)', () => {
    const queries = buildExportQueries({
      databaseUrl: 'postgresql://user:pass@localhost/db',
      s3Bucket: 'my-bucket',
      s3Region: 'us-west-2',
      startWindow: '2026-01-01T00:00:00Z',
      endWindow: '2026-01-02T00:00:00Z',
      warehouseSources: auditEventHistorySources,
    });

    expect(queries).toContain('INSTALL postgres;');
    expect(queries).toContain('LOAD postgres;');
    expect(queries).toContain('INSTALL httpfs;');
    expect(queries).toContain('LOAD httpfs;');

    // S3 secret should be created
    expect(queries.find((q: string) => q.includes('CREATE SECRET') && q.includes("REGION 'us-west-2'"))).toBeDefined();

    // Postgres attach
    expect(queries).toContain("ATTACH 'postgresql://user:pass@localhost/db' AS pg_db (TYPE postgres);");
  });

  it('should build correct SQL queries for AWS S3 Tables managed Iceberg', () => {
    const queries = buildExportQueries({
      databaseUrl: 'postgresql://user:pass@localhost/db',
      s3Bucket: '', // omitted
      s3Region: 'us-west-2',
      startWindow: '2026-01-01T00:00:00Z',
      endWindow: '2026-01-02T00:00:00Z',
      awsS3TableArn: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-s3-tables-bucket',
      warehouseSources: auditEventHistorySources,
    });

    expect(queries).toContain('INSTALL aws;');
    expect(queries).toContain('LOAD aws;');
    expect(queries).toContain('INSTALL postgres;');
    expect(queries).toContain('LOAD postgres;');
    expect(queries).toContain('INSTALL httpfs;');
    expect(queries).toContain('LOAD httpfs;');
    expect(queries).toContain('INSTALL iceberg;');
    expect(queries).toContain('LOAD iceberg;');

    // S3 secret should be created
    expect(queries.find((q: string) => q.includes('CREATE SECRET') && q.includes("REGION 'us-west-2'"))).toBeDefined();

    // Postgres attach
    expect(queries).toContain("ATTACH 'postgresql://user:pass@localhost/db' AS pg_db (TYPE postgres);");

    // S3 Tables attach
    expect(queries).toContain(
      "ATTACH 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-s3-tables-bucket' AS s3_tables_db ( TYPE iceberg, ENDPOINT_TYPE s3_tables );"
    );
  });

  it('does not emit DDL for managed Iceberg mutations', () => {
    const queries = buildExportQueries({
      databaseUrl: 'postgresql://user:pass@localhost/db',
      s3Bucket: '',
      s3Region: 'us-west-2',
      startWindow: '2026-01-01T00:00:00Z',
      endWindow: '2026-01-02T00:00:00Z',
      awsS3TableArn: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-s3-tables-bucket',
      warehouseSources: auditEventHistorySources,
      clean: true,
    });

    expect(queries.some((q) => q.startsWith('DROP TABLE IF EXISTS s3_tables_db.default.audit_event_history'))).toBe(
      false
    );
    expect(
      queries.some((q) => q.startsWith('CREATE TABLE IF NOT EXISTS s3_tables_db.default.audit_event_history'))
    ).toBe(false);
    expect(queries.some((q) => q.includes('set_iceberg_table_properties'))).toBe(false);
  });

  it('omits end window in SQL for Iceberg when endWindow is not set', () => {
    const queries = buildExportQueries({
      databaseUrl: 'postgresql://user:pass@localhost/db',
      s3Bucket: '',
      s3Region: 'us-west-2',
      startWindow: '2026-01-01T00:00:00Z',
      awsS3TableArn: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-s3-tables-bucket',
      warehouseSources: auditEventHistorySources,
    });

    expect(queries.some((q) => q.includes('set_iceberg_table_properties'))).toBe(false);
    expect(queries).toContain(
      "DELETE FROM s3_tables_db.default.audit_event_history WHERE last_updated >= '2026-01-01T00:00:00Z';"
    );
  });

  it('should build correct SQL queries for test environment (mocking S3)', () => {
    const queries = buildExportQueries({
      databaseUrl: 'postgresql://user:pass@localhost/db',
      s3Bucket: 'my-bucket',
      s3Region: 'us-west-2',
      startWindow: '2026-01-01T00:00:00Z',
      endWindow: '2026-01-02T00:00:00Z',
      localPath: '/tmp/mock-s3-path',
      warehouseSources: auditEventHistorySources,
    });

    // S3 secret should NOT be created
    expect(queries.find((q: string) => q.includes('CREATE SECRET'))).toBeUndefined();
  });
});
