// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import type { MedplumDatabaseConfig, MedplumDatabaseSslConfig } from './config/types';

/**
 * Applies the same host and TLS rules as `initPool` in `database.ts` when a database proxy endpoint is configured
 * (`host` → proxy, `ssl.require` → `true`).
 * Use before `buildPostgresUrlFromMedplumDatabaseConfig` so DuckDB/libpq matches the main server's `pg` pool profile.
 *
 * @param config - `database` or `readonlyDatabase` from server config.
 * @param proxyEndpoint - `databaseProxyEndpoint` or `readonlyDatabaseProxyEndpoint`, when set.
 * @returns A shallow-cloned config with effective `host` and `ssl`; does not mutate `config`.
 */
export function resolveMedplumDatabaseTcpConnection(
  config: MedplumDatabaseConfig,
  proxyEndpoint?: string
): MedplumDatabaseConfig {
  const trimmed = proxyEndpoint?.trim();
  if (!trimmed) {
    return config;
  }

  const ssl: MedplumDatabaseSslConfig = {
    ...(config.ssl ?? {}),
    require: true,
  };

  return {
    ...config,
    host: trimmed,
    ssl,
  };
}
