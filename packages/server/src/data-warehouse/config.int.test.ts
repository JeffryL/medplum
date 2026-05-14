// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { DuckDBInstance } from '@duckdb/node-api';
import pg from 'pg';
import { loadTestConfig } from '../config/loader';
import {
  buildPostgresUrlFromMedplumDatabaseConfig,
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  mergePostgresStatementTimeout,
} from './config';
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
  let connectionUri: string;
  let host: string;
  let port: number;
  let database: string;
  let username: string;
  let password: string;

  beforeAll(async () => {
    const config = await loadTestConfig();
    const db = config.database;
    host = db.host ?? '';
    port = db.port ?? 5432;
    database = db.dbname ?? '';
    username = db.username ?? '';
    password = db.password ?? '';
    connectionUri = buildPostgresUrlFromMedplumDatabaseConfig(db, '');
  }, 60_000);

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
