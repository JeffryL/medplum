// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { execSync } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

type StartedContainer = Awaited<ReturnType<PostgreSqlContainer['start']>>;

export async function startPostgresTestContainer(image = 'postgres:17'): Promise<{
  container: StartedContainer;
  connectionUri: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
}> {
  const container = await new PostgreSqlContainer(image).start();
  return {
    container,
    connectionUri: container.getConnectionUri(),
    host: container.getHost(),
    port: container.getPort(),
    database: container.getDatabase(),
    username: container.getUsername(),
    password: container.getPassword(),
  };
}

/**
 * Writes a minimal CA + server cert chain for {@link PostgreSqlContainer.withSSLCert} (requires OpenSSL on PATH).
 *
 * @returns Absolute paths to temp cert material under a new directory {@link sslDir}.
 */
function createTempPostgresSslMaterial(): {
  sslDir: string;
  caCertPath: string;
  serverCertPath: string;
  serverKeyPath: string;
} {
  const sslDir = mkdtempSync(join(tmpdir(), 'pgssl-'));
  const caKey = join(sslDir, 'ca.key');
  const caCert = join(sslDir, 'ca.crt');
  const serverKey = join(sslDir, 'server.key');
  const serverCsr = join(sslDir, 'server.csr');
  const serverCert = join(sslDir, 'server.crt');

  execSync(
    `openssl req -new -x509 -days 2 -nodes -keyout "${caKey}" -out "${caCert}" -subj "/CN=integration-test-ca"`,
    { stdio: 'pipe' }
  );
  execSync(`openssl req -new -nodes -keyout "${serverKey}" -out "${serverCsr}" -subj "/CN=localhost"`, {
    stdio: 'pipe',
  });
  execSync(
    `openssl x509 -req -in "${serverCsr}" -CA "${caCert}" -CAkey "${caKey}" -CAcreateserial -out "${serverCert}" -days 2`,
    { stdio: 'pipe' }
  );

  return { sslDir, caCertPath: caCert, serverCertPath: serverCert, serverKeyPath: serverKey };
}

/**
 * Postgres with `ssl=on` (Testcontainers copies cert material into the container).
 *
 * @param image - Docker image ref (default `postgres:17`).
 * @returns Started container, JDBC-style connection fields, host CA path, and temp ssl directory to remove after stop.
 */
export async function startPostgresSslTestContainer(image = 'postgres:17'): Promise<{
  container: StartedContainer;
  connectionUri: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  /** Trust anchor path on the host; pass as `database.ssl.ca` for `buildPostgresUrlFromMedplumDatabaseConfig`. */
  caCertPath: string;
  /** Temp directory holding cert files; remove after {@link StartedContainer.stop} if desired. */
  sslDir: string;
}> {
  const { sslDir, caCertPath, serverCertPath, serverKeyPath } = createTempPostgresSslMaterial();
  const container = await new PostgreSqlContainer(image).withSSLCert(caCertPath, serverCertPath, serverKeyPath).start();
  return {
    container,
    connectionUri: container.getConnectionUri(),
    host: container.getHost(),
    port: container.getPort(),
    database: container.getDatabase(),
    username: container.getUsername(),
    password: container.getPassword(),
    caCertPath,
    sslDir,
  };
}
