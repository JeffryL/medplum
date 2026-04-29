// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { DataWarehouseAwsClient } from './aws.ts';

describe('DataWarehouseAwsClient.assertIcebergTableExists', () => {
  it('throws when tableExists is false', async () => {
    const client = new DataWarehouseAwsClient({ region: 'us-east-1' });
    vi.spyOn(client, 'tableExists').mockResolvedValue(false);
    await expect(client.assertIcebergTableExists('arn:aws:s3tables:us-east-1:123:bucket/foo', 'default', 'observation')).rejects.toThrow(
      /Managed Iceberg table does not exist: default\.observation/
    );
  });

  it('resolves when tableExists is true', async () => {
    const client = new DataWarehouseAwsClient({ region: 'us-east-1' });
    vi.spyOn(client, 'tableExists').mockResolvedValue(true);
    await expect(
      client.assertIcebergTableExists('arn:aws:s3tables:us-east-1:123:bucket/foo', 'default', 'observation')
    ).resolves.toBeUndefined();
  });
});
