// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DEFAULT_DATABASE_STATEMENT_TIMEOUT, mergePostgresStatementTimeout, resolveDatabaseUrl } from './config.js';
import { buildDuckdbPostgresAttachQuery } from './postgres-duckdb.ts';
import { startPostgresTestContainer } from './postgres-testcontainer.ts';

/**
 * Exercises `mergePostgresStatementTimeout` and `resolveDatabaseUrl` against a real Postgres
 * and DuckDB’s postgres `ATTACH` (same path as `exportData`), so URL `options` encoding matches
 * what libpq/DuckDB expect (spaces must not be `+` in the `options` token).
 */
describe('config (integration)', () => {
  let container: { stop(): Promise<unknown> } | undefined;
  let connectionUri: string;
  let host: string;
  let port: number;
  let database: string;
  let username: string;
  let password: string;

  beforeAll(async () => {
    const started = await startPostgresTestContainer();
    container = started.container;
    connectionUri = started.connectionUri;
    host = started.host;
    port = started.port;
    database = started.database;
    username = started.username;
    password = started.password;
  }, 60_000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  }, 30_000);

  it('sets statement_timeout for direct libpq (pg) and for DuckDB ATTACH + postgres_query', async () => {
    const want = '4s';
    const merged = mergePostgresStatementTimeout(connectionUri, want);

    const direct = new pg.Client({ connectionString: merged });
    await direct.connect();
    try {
      const { rows } = await direct.query(`select current_setting('statement_timeout') as t`);
      expect(rows[0]?.t).toBe(want);
    } finally {
      await direct.end();
    }

    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    try {
      await connection.run('INSTALL postgres; LOAD postgres;');
      await connection.run(buildDuckdbPostgresAttachQuery(merged, 'pgprobe'));
      const res = await connection.runAndReadAll(
        "SELECT * FROM postgres_query('pgprobe', 'SELECT current_setting(''statement_timeout'') AS t');"
      );
      const row = res.getRowObjectsJson()[0] as { t: string };
      expect(row.t).toBe(want);
    } finally {
      connection.closeSync();
    }
  }, 30_000);

  it('resolveDatabaseUrl applies default and custom statement_timeout to the live URL', async () => {
    const withDefault = resolveDatabaseUrl({
      dbHost: host,
      dbPort: String(port),
      dbName: database,
      dbUsername: username,
      dbPassword: password,
    });
    expect(withDefault).toContain(`statement_timeout%3D${DEFAULT_DATABASE_STATEMENT_TIMEOUT}`);

    const withOverride = resolveDatabaseUrl({
      dbHost: host,
      dbPort: String(port),
      dbName: database,
      dbUsername: username,
      dbPassword: password,
      databaseStatementTimeout: '6s',
    });
    const c = new pg.Client({ connectionString: withOverride });
    await c.connect();
    try {
      const { rows } = await c.query(`select current_setting('statement_timeout') as t`);
      expect(rows[0]?.t).toBe('6s');
    } finally {
      await c.end();
    }
  }, 30_000);

  it('resolveDatabaseUrl from discrete fields matches merge + connect', async () => {
    const url = resolveDatabaseUrl({
      dbHost: host,
      dbPort: String(port),
      dbName: database,
      dbUsername: username,
      dbPassword: password,
      databaseStatementTimeout: '3s',
    });
    const client = new pg.Client({ connectionString: url });
    await client.connect();
    try {
      const { rows } = await client.query(`select current_setting('statement_timeout') as t`);
      expect(rows[0]?.t).toBe('3s');
    } finally {
      await client.end();
    }
  }, 30_000);
});
