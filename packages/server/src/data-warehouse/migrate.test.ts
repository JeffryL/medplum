// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { Client } from 'pg';
import { verifyWarehousePostgresTablesExist } from './migrate';
import { startPostgresTestContainer } from './postgres-testcontainer.util';

describe('migrate Postgres verification', () => {
  let pgContainer: { stop(): Promise<unknown> } | undefined;
  let databaseUrl: string;

  beforeAll(async () => {
    const postgres = await startPostgresTestContainer();
    pgContainer = postgres.container;
    databaseUrl = postgres.connectionUri;

    const client = new Client({ connectionString: databaseUrl });
    await client.connect();
    try {
      await client.query(
        'CREATE TABLE "MigrateVerify_Patient_History" ("versionId" UUID PRIMARY KEY, id UUID NOT NULL, content TEXT NOT NULL, "lastUpdated" TIMESTAMPTZ NOT NULL)'
      );
    } finally {
      await client.end();
    }
  }, 30_000);

  afterAll(async () => {
    if (pgContainer) {
      await pgContainer.stop();
    }
  }, 30_000);

  it('accepts existing table names with exact relname match', async () => {
    await expect(
      verifyWarehousePostgresTablesExist(databaseUrl, ['MigrateVerify_Patient_History'])
    ).resolves.toBeUndefined();
  });

  it('rejects missing tables', async () => {
    await expect(
      verifyWarehousePostgresTablesExist(databaseUrl, ['MigrateVerify_Patient_History', 'MigrateVerify_missing_xyz'])
    ).rejects.toThrow(/MigrateVerify_missing_xyz/);
  });

  it('rejects wrong-case when relname is mixed', async () => {
    await expect(verifyWarehousePostgresTablesExist(databaseUrl, ['migrateverify_patient_history'])).rejects.toThrow(
      /migrateverify_patient_history/
    );
  });
});
