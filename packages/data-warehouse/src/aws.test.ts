// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { IcebergNullOrder, IcebergSortDirection } from '@aws-sdk/client-s3tables';
import { describe, expect, it } from 'vitest';
import { buildWarehouseIcebergWriteOrder } from './aws.ts';

describe('buildWarehouseIcebergWriteOrder', () => {
  it('orders project_id then last_updated ascending with nulls last', () => {
    const sourceIdByColumnName = { project_id: 5, last_updated: 4 };
    const order = buildWarehouseIcebergWriteOrder(sourceIdByColumnName);
    expect(order.orderId).toBe(1);
    expect(order.fields).toHaveLength(2);
    expect(order.fields?.[0]).toMatchObject({
      sourceId: 5,
      transform: 'identity',
      direction: IcebergSortDirection.ASC,
      nullOrder: IcebergNullOrder.NULLS_LAST,
    });
    expect(order.fields?.[1]).toMatchObject({
      sourceId: 4,
      transform: 'identity',
      direction: IcebergSortDirection.ASC,
      nullOrder: IcebergNullOrder.NULLS_LAST,
    });
  });
});
