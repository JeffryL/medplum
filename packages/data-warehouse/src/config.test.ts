// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  DEFAULT_DATABASE_STATEMENT_TIMEOUT,
  DEFAULT_ROW_THRESHOLD,
  formatPostgresTargetLabel,
  getThresholdForTableKey,
  mergePostgresStatementTimeout,
  parseDefaultRowThreshold,
  parseRowThresholdOverrides,
  resolveDatabaseUrl,
} from './config.ts';

describe('config helpers', () => {
  it('formatPostgresTargetLabel omits credentials', () => {
    expect(formatPostgresTargetLabel('postgresql://user:secret@db.example.com:5432/medplum')).toBe(
      'db.example.com:5432/medplum'
    );
  });

  it('parses threshold overrides', () => {
    const overrides = parseRowThresholdOverrides('{"default":1000,"Patient":5000}');
    expect(overrides).toEqual({ default: 1000, Patient: 5000 });
  });

  it('falls back to default row threshold', () => {
    expect(parseDefaultRowThreshold(undefined)).toBe(DEFAULT_ROW_THRESHOLD);
  });

  it('resolves per-table threshold with fallback order', () => {
    const threshold = getThresholdForTableKey('observation', 1000, {
      default: 2000,
      patient: 5000,
    });
    expect(threshold).toBe(2000);
  });

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

  it('resolveDatabaseUrl applies default statement_timeout', () => {
    const out = resolveDatabaseUrl({
      dbHost: 'localhost',
      dbName: 'db',
      dbUsername: 'u',
      dbPassword: 'p',
    });
    const url = new URL(out);
    expect(url.searchParams.get('options')).toBe(`-c statement_timeout=${DEFAULT_DATABASE_STATEMENT_TIMEOUT}`);
  });

  it('resolveDatabaseUrl respects databaseStatementTimeout override', () => {
    const out = resolveDatabaseUrl({
      dbHost: 'localhost',
      dbName: 'db',
      dbUsername: 'u',
      dbPassword: 'p',
      databaseStatementTimeout: '900s',
    });
    expect(new URL(out).searchParams.get('options')).toBe('-c statement_timeout=900s');
  });
});
