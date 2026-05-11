// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

import { getSyncAction } from './sync';

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

  it('inserts for a single row when threshold is 1', () => {
    expect(getSyncAction(1, 1)).toBe('insert');
  });
});
