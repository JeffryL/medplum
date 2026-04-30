// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { PostgreSqlContainer } from '@testcontainers/postgresql';

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
