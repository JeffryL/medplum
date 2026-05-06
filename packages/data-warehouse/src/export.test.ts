// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { exportData, resolveWarehouseSourcesFromPostgresTableNames } from './export.ts';
import { startPostgresTestContainer } from './postgres-testcontainer.util.ts';
import { buildAthenaCreateIcebergTableQuery } from './warehouse-sql.ts';

describe('Data Warehouse Export', () => {
  let pgContainer: any;
  let databaseUrl: string;
  /** Set only after successful setup; undefined if `beforeAll` throws (e.g. no Docker). */
  let tempDir: string | undefined;

  beforeAll(async () => {
    // Spin up Postgres
    const postgres = await startPostgresTestContainer();
    pgContainer = postgres.container;

    // Connect and create table + insert data
    databaseUrl = postgres.connectionUri;

    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    await connection.run(`INSTALL postgres; LOAD postgres;`);
    await connection.run(`ATTACH '${databaseUrl}' AS pg_db (TYPE postgres);`);

    // Create AuditEvent_history table and insert sample data
    await connection.run(`
      CREATE TABLE pg_db."AuditEvent_history" (
        "versionId" UUID PRIMARY KEY,
        id UUID NOT NULL,
        content TEXT NOT NULL,
        "lastUpdated" TIMESTAMP WITH TIME ZONE NOT NULL,
        "projectId" UUID NOT NULL
      );
    `);

    const projectId = '123e4567-e89b-12d3-a456-426614174000';
    const resourceType = 'AuditEvent';
    const sql = `
      INSERT INTO pg_db."${resourceType}_history" VALUES
        ('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', '{"resourceType":"${resourceType}","meta":{"project":"${projectId}"},"event":1}', '2026-04-11 10:00:00+00', '${projectId}'),
        ('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000002', '{"resourceType":"${resourceType}","meta":{"project":"${projectId}"},"event":2}', '2026-04-11 10:15:00+00', '${projectId}'),
        ('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000003', '{"resourceType":"${resourceType}","meta":{"project":"${projectId}"},"event":3}', '2026-04-11 10:30:00+00', '${projectId}');
    `;
    await connection.run(sql);

    connection.closeSync();

    // Create temp dir for mock S3 Iceberg catalog
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'iceberg-test-'));
    fs.mkdirSync(path.join(tempDir, 'audit_event_history'));
  }, 60_000);

  afterAll(async () => {
    if (pgContainer) {
      await pgContainer.stop();
    }
    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  }, 10000);

  it('should incrementally export data to parquet files', async () => {
    // 1. Initial export: 10:00 to 10:20 (Should export 2 rows)
    await exportData({
      databaseUrl,
      s3Bucket: 'test-bucket', // ignored because of localPath
      s3Region: 'us-east-1',
      startWindow: '2026-04-11T10:00:00Z',
      endWindow: '2026-04-11T10:20:00Z',
      localPath: tempDir,
      warehouseSources: resolveWarehouseSourcesFromPostgresTableNames(['AuditEvent_history']),
    });

    // Verify
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    const res1 = (
      await connection.runAndReadAll(`
      SELECT * FROM read_parquet('${tempDir}/audit_event_history/*.parquet') ORDER BY last_updated;
    `)
    ).getRowObjectsJson() as { id: string }[];

    expect(res1.length).toBe(2);
    expect(res1[0].id).toBe('00000000-0000-0000-0000-000000000001');

    // 2. Idempotent retry: 10:00 to 10:20 (Should still have 2 rows total)
    await exportData({
      databaseUrl,
      s3Bucket: 'test-bucket',
      s3Region: 'us-east-1',
      startWindow: '2026-04-11T10:00:00Z',
      endWindow: '2026-04-11T10:20:00Z',
      localPath: tempDir,
      warehouseSources: resolveWarehouseSourcesFromPostgresTableNames(['AuditEvent_history']),
    });

    const res2 = (
      await connection.runAndReadAll(`
      SELECT * FROM read_parquet('${tempDir}/audit_event_history/*.parquet') ORDER BY last_updated;
    `)
    ).getRowObjectsJson();
    expect(res2.length).toBe(2);

    // 3. Incremental next window: 10:20 to 10:40 (Should add 1 row)
    await exportData({
      databaseUrl,
      s3Bucket: 'test-bucket',
      s3Region: 'us-east-1',
      startWindow: '2026-04-11T10:20:00Z',
      endWindow: '2026-04-11T10:40:00Z',
      localPath: tempDir,
      warehouseSources: resolveWarehouseSourcesFromPostgresTableNames(['AuditEvent_history']),
    });

    const res3 = (
      await connection.runAndReadAll(`
      SELECT * FROM read_parquet('${tempDir}/audit_event_history/*.parquet') ORDER BY last_updated;
    `)
    ).getRowObjectsJson();
    expect(res3.length).toBe(3);

    connection.closeSync();
  }, 10000);

  it('builds Athena Iceberg CREATE TABLE SQL with partition transforms', () => {
    const query = buildAthenaCreateIcebergTableQuery({
      qualifiedTable: 'warehouse.audit_event',
      columns: [
        { name: 'id', type: 'string' },
        { name: 'last_updated', type: 'timestamp' },
        { name: 'project_id', type: 'string' },
        { name: 'content', type: 'string' },
      ],
      partitionedBy: ['project_id', 'day(last_updated)', 'bucket(16, id)'],
      location: 's3://example-bucket/audit_event/',
      tableProperties: {
        format: 'parquet',
      },
    });

    expect(query).toContain('CREATE TABLE warehouse.audit_event');
    expect(query).toContain('PARTITIONED BY (project_id, day(last_updated), bucket(16, id))');
    expect(query).toContain("LOCATION 's3://example-bucket/audit_event/'");
    expect(query).toContain("TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')");
  });

  it('uses DuckDB to create Athena-like partitioned parquet layout', async () => {
    const localPartitionedDir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-partitioned-'));
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    const createPartitionedTableQuery = buildAthenaCreateIcebergTableQuery({
      qualifiedTable: 'warehouse.audit_event',
      columns: [
        { name: 'id', type: 'string' },
        { name: 'last_updated', type: 'timestamp' },
        { name: 'project_id', type: 'string' },
        { name: 'id_bucket', type: 'int' },
      ],
      partitionedBy: ['project_id', 'day(last_updated)', 'bucket(16, id)'],
      location: 's3://example-bucket/audit_event/',
      tableProperties: {
        format: 'parquet',
      },
    });

    try {
      expect(createPartitionedTableQuery).toContain('PARTITIONED BY (project_id, day(last_updated), bucket(16, id))');
      await connection.run(`
        CREATE TABLE audit_event_source (
          id VARCHAR,
          last_updated TIMESTAMP,
          project_id VARCHAR,
          id_bucket INTEGER
        );
      `);
      await connection.run(`
        INSERT INTO audit_event_source VALUES
          ('a1', '2026-04-11 10:00:00', 'project-1', 3),
          ('a2', '2026-04-11 11:00:00', 'project-1', 3),
          ('b1', '2026-04-12 09:00:00', 'project-2', 7);
      `);

      await connection.run(`
        COPY (
          SELECT
            id,
            last_updated,
            project_id,
            CAST(last_updated AS DATE) AS partition_day,
            id_bucket
          FROM audit_event_source
        ) TO '${localPartitionedDir}' (
          FORMAT PARQUET,
          PARTITION_BY (project_id, partition_day, id_bucket)
        );
      `);
    } finally {
      connection.closeSync();
    }

    const project1Partition = path.join(
      localPartitionedDir,
      'project_id=project-1',
      'partition_day=2026-04-11',
      'id_bucket=3'
    );
    const project2Partition = path.join(
      localPartitionedDir,
      'project_id=project-2',
      'partition_day=2026-04-12',
      'id_bucket=7'
    );

    expect(fs.existsSync(project1Partition)).toBe(true);
    expect(fs.existsSync(project2Partition)).toBe(true);
    expect(fs.readdirSync(project1Partition).some((name) => name.endsWith('.parquet'))).toBe(true);
    expect(fs.readdirSync(project2Partition).some((name) => name.endsWith('.parquet'))).toBe(true);

    fs.rmSync(localPartitionedDir, { recursive: true, force: true });
  });
});
