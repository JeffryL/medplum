// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import {
  buildPostgresUrlFromMedplumDatabaseConfig,
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  mergePostgresStatementTimeout,
  resolveWarehouseSourcesFromPostgresTableNames,
} from './config';

describe('config helpers', () => {
  it('mergePostgresStatementTimeout adds libpq options for statement_timeout', () => {
    const out = mergePostgresStatementTimeout('postgresql://u:p@localhost:5432/db', '15min');
    const url = new URL(out);
    expect(url.searchParams.get('options')).toBe('-c statement_timeout=15min');
    // "+" for space breaks libpq/Postgres `options`; must use %20 in the wire query string.
    expect(out).toContain('options=-c%20statement_timeout%3D15min');
    expect(out).not.toMatch(/options=-c\+statement_timeout/);
  });

  it('mergePostgresStatementTimeout sets options and keeps other query parameters', () => {
    const out = mergePostgresStatementTimeout('postgresql://u:p@h/db?sslmode=disable', '15min');
    const url = new URL(out);
    expect(url.searchParams.get('sslmode')).toBe('disable');
    expect(url.searchParams.get('options')).toBe('-c statement_timeout=15min');
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig applies default statement_timeout when empty', () => {
    const out = buildPostgresUrlFromMedplumDatabaseConfig(
      { host: 'localhost', dbname: 'db', username: 'u', password: 'p' },
      ''
    );
    const url = new URL(out);
    expect(url.searchParams.get('options')).toBe(`-c statement_timeout=${DEFAULT_DATABASE_STATEMENT_TIMEOUT}`);
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig respects statementTimeout override', () => {
    const out = buildPostgresUrlFromMedplumDatabaseConfig(
      { host: 'localhost', dbname: 'db', username: 'u', password: 'p' },
      '900s'
    );
    expect(new URL(out).searchParams.get('options')).toBe('-c statement_timeout=900s');
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig maps ssl.require + rejectUnauthorized false to sslmode=require', () => {
    const out = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host: 'localhost',
        dbname: 'db',
        username: 'u',
        password: 'p',
        ssl: { require: true, rejectUnauthorized: false },
      },
      ''
    );
    const url = new URL(out);
    expect(url.searchParams.get('sslmode')).toBe('require');
    expect(url.searchParams.get('options')).toBe(`-c statement_timeout=${DEFAULT_DATABASE_STATEMENT_TIMEOUT}`);
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig maps ssl root cert path to verify-ca and sslrootcert', () => {
    const out = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host: 'localhost',
        dbname: 'db',
        username: 'u',
        password: 'p',
        ssl: { ca: '/path/to/ca.pem' },
      },
      ''
    );
    const url = new URL(out);
    expect(url.searchParams.get('sslmode')).toBe('verify-ca');
    expect(url.searchParams.get('sslrootcert')).toBe('/path/to/ca.pem');
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig maps ssl.require without ca to sslmode=verify-full', () => {
    const out = buildPostgresUrlFromMedplumDatabaseConfig(
      {
        host: 'localhost',
        dbname: 'db',
        username: 'u',
        password: 'p',
        ssl: { require: true },
      },
      ''
    );
    expect(new URL(out).searchParams.get('sslmode')).toBe('verify-full');
  });

  it('buildPostgresUrlFromMedplumDatabaseConfig rejects inline PEM for ssl.ca', () => {
    expect(() =>
      buildPostgresUrlFromMedplumDatabaseConfig(
        {
          host: 'localhost',
          dbname: 'db',
          username: 'u',
          password: 'p',
          ssl: { ca: '-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----' },
        },
        ''
      )
    ).toThrow('inline PEM');
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
