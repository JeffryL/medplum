// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import type { MedplumDataWarehouseConfig, MedplumServerConfig } from './types';

/**
 * Ensures `dataWarehouse` has every field required when `enabled` is true.
 * Call after `addDefaults` so `sink` defaulting matches runtime sync behavior.
 *
 * @param dw - Data warehouse subsection of server config (caller must ensure `enabled` is true).
 */
export function assertEnabledDataWarehouseComplete(dw: MedplumDataWarehouseConfig): void {
  if (!dw.cron?.trim()) {
    throw new Error('dataWarehouse.cron is required when dataWarehouse.enabled is true');
  }

  const sink = dw.sink;
  if (sink === 'local') {
    if (!dw.localBasePath?.trim()) {
      throw new Error('dataWarehouse.localBasePath is required when dataWarehouse.sink is "local"');
    }
  } else if (sink === 's3tables') {
    if (!dw.awsS3TableArn?.trim()) {
      throw new Error('dataWarehouse.awsS3TableArn is required when dataWarehouse.sink is "s3tables"');
    }
  } else {
    throw new Error(`dataWarehouse.sink must be "s3tables" or "local"`);
  }
}

/**
 * Validates `config.dataWarehouse` when the feature is enabled.
 * Intended to run once after `loadConfig` merges sources and applies `addDefaults`.
 *
 * @param config - Full server configuration after defaults.
 */
export function validateDataWarehouseConfig(config: MedplumServerConfig): void {
  const dw = config.dataWarehouse;
  if (dw?.enabled) {
    assertEnabledDataWarehouseComplete(dw);
  }
}
