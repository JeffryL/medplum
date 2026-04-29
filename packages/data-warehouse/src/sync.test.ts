// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { getSyncAction } from './sync.js';

describe('getSyncAction', () => {
  it('skips when no rows are available', () => {
    expect(getSyncAction(0, 1000)).toBe('skip-empty');
  });

  it('skips when count is below threshold', () => {
    expect(getSyncAction(250, 1000)).toBe('skip-threshold');
  });

  it('inserts when count reaches threshold', () => {
    expect(getSyncAction(1000, 1000)).toBe('insert');
  });
});
