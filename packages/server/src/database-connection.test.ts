// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import type { MedplumDatabaseConfig } from './config/types';
import { resolveMedplumDatabaseTcpConnection } from './database-connection';

describe('resolveMedplumDatabaseTcpConnection', () => {
  it('returns the same object reference when proxy is unset', () => {
    const db: MedplumDatabaseConfig = { host: 'db.example.com', dbname: 'x', username: 'u', password: 'p' };
    expect(resolveMedplumDatabaseTcpConnection(db, undefined)).toBe(db);
    expect(resolveMedplumDatabaseTcpConnection(db, '')).toBe(db);
    expect(resolveMedplumDatabaseTcpConnection(db, '   ')).toBe(db);
  });

  it('rewrites host and sets ssl.require when proxy is set', () => {
    const db: MedplumDatabaseConfig = {
      host: 'db.example.com',
      port: 5432,
      dbname: 'medplum',
      username: 'u',
      password: 'p',
      ssl: { rejectUnauthorized: false },
    };
    const out = resolveMedplumDatabaseTcpConnection(db, 'proxy.example.com');
    expect(out.host).toStrictEqual('proxy.example.com');
    expect(out.ssl).toStrictEqual({ rejectUnauthorized: false, require: true });
    expect(db.host).toStrictEqual('db.example.com');
    expect(db.ssl).toStrictEqual({ rejectUnauthorized: false });
  });

  it('sets ssl.require when proxy is set and ssl was undefined', () => {
    const db: MedplumDatabaseConfig = { host: 'db.example.com', dbname: 'x', username: 'u', password: 'p' };
    const out = resolveMedplumDatabaseTcpConnection(db, 'proxy.example.com');
    expect(out.ssl).toStrictEqual({ require: true });
  });
});
