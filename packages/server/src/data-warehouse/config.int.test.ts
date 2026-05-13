// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import { rmSync } from 'node:fs';
import pg from 'pg';
import {
  buildPostgresUrlFromMedplumDatabaseConfig,
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  mergePostgresStatementTimeout,
} from './config';
import { startPostgresSslTestContainer, startPostgresTestContainer } from './postgres-testcontainer.util';
import { buildDuckdbPostgresAttachQuery } from './warehouse-sql';

/**
 * Exercises `mergePostgresStatementTimeout` and `buildPostgresUrlFromMedplumDatabaseConfig` against a real Postgres
 * and DuckDB’s postgres `ATTACH` (same path as data-warehouse sync), so URL `options` encoding matches
 * what libpq/DuckDB expect (spaces must not be `+` in the `options` token).
 * 
 * This is unfortunately necessary because while our config provides db host, name, ssl, etc. individually,
 * we need to construct the full connection URI for DuckDB/libpq.
 * 
 * TODO: figure out if there's a better way to do this using DuckDB APIs directly
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

  it('buildPostgresUrlFromMedplumDatabaseConfig applies default and custom statement_timeout to the live URL', async () => {
    const withDefault = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host,
        port,
        dbname: database,
        username,
        password,
      },
      ''
    );
    expect(withDefault).toContain(`statement_timeout%3D${DEFAULT_DATABASE_STATEMENT_TIMEOUT}`);

    const withOverride = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host,
        port,
        dbname: database,
        username,
        password,
      },
      '6s'
    );
    const c = new pg.Client({ connectionString: withOverride });
    await c.connect();
    try {
      const { rows } = await c.query(`select current_setting('statement_timeout') as t`);
      expect(rows[0]?.t).toBe('6s');
    } finally {
      await c.end();
    }
  }, 30_000);

  it('buildPostgresUrlFromMedplumDatabaseConfig connects with expected statement_timeout', async () => {
    const url = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host,
        port,
        dbname: database,
        username,
        password,
      },
      '3s'
    );
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

describe('buildPostgresUrlFromMedplumDatabaseConfig (SSL, integration)', () => {
  let container: { stop(): Promise<unknown> } | undefined;
  let sslDir: string | undefined;
  let host: string;
  let port: number;
  let database: string;
  let username: string;
  let password: string;
  let caCertPath: string;

  beforeAll(async () => {
    const started = await startPostgresSslTestContainer();
    container = started.container;
    sslDir = started.sslDir;
    host = started.host;
    port = started.port;
    database = started.database;
    username = started.username;
    password = started.password;
    caCertPath = started.caCertPath;
  }, 120_000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
    if (sslDir) {
      rmSync(sslDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('connects when ssl.rejectUnauthorized is false (sslmode=require)', async () => {
    const url = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host,
        port,
        dbname: database,
        username,
        password,
        ssl: { require: true, rejectUnauthorized: false },
      },
      '5s'
    );
    expect(new URL(url).searchParams.get('sslmode')).toBe('require');

    // Node pg currently maps sslmode=require to verify-full unless uselibpqcompat=true (see pg-connection-string warning).
    const client = new pg.Client({ connectionString: `${url}&uselibpqcompat=true` });
    await client.connect();
    try {
      const { rows } = await client.query(`select 1 as ok`);
      expect(rows[0]?.ok).toBe(1);
    } finally {
      await client.end();
    }
  }, 30_000);

  it('connects when ssl.rejectUnauthorized is true and ca is a filesystem path (verify-ca)', async () => {
    const url = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host,
        port,
        dbname: database,
        username,
        password,
        ssl: { require: true, rejectUnauthorized: true, ca: caCertPath },
      },
      '5s'
    );
    const parsed = new URL(url);
    expect(parsed.searchParams.get('sslmode')).toBe('verify-ca');
    expect(parsed.searchParams.get('sslrootcert')).toBe(caCertPath);

    const client = new pg.Client({ connectionString: `${url}&uselibpqcompat=true` });
    await client.connect();
    try {
      const { rows } = await client.query(`select 1 as ok`);
      expect(rows[0]?.ok).toBe(1);
    } finally {
      await client.end();
    }
  }, 30_000);
});
