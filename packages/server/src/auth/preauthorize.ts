// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0
import { createReference, parseReference } from '@medplum/core';
import type { Login } from '@medplum/fhirtypes';
import type { Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import { getAuthenticatedContext } from '../context';
import { getGlobalSystemRepo } from '../fhir/repo';
import { generateSecret } from '../oauth/keys';
import { makeValidationMiddleware } from '../util/validator';
import { sendLoginResult } from './utils';

export const preAuthorizeValidator = makeValidationMiddleware([
  // body('membership').isUUID().withMessage('Membership ID is required'),
  // body('password').isLength({ min: 8 }).withMessage('Invalid password, must be at least 8 characters'),
]);

export async function preAuthorizeHandler(req: Request, res: Response): Promise<void> {
  const { project, membership, profile } = getAuthenticatedContext();
  const systemRepo = getGlobalSystemRepo();
  const [resourceType, _id] = parseReference(profile);
  const login = await systemRepo.createResource<Login>({
    resourceType: 'Login',
    authMethod: 'pre-authorized',
    project: createReference(project),
    profileType: resourceType,
    membership: createReference(membership),
    user: membership.user,
    authTime: new Date().toISOString(),
    code: generateSecret(16),
    scope: req.body.scope || 'openid',
    nonce: req.body.nonce || randomUUID(),
    remoteAddress: req.ip,
    userAgent: req.get('User-Agent'),
  });
  await sendLoginResult(res, login);
}
