// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0

/* global process */
/* global console */

import esbuild from 'esbuild';
import packageJson from './package.json' with { type: 'json' };

const external = Object.keys(packageJson.dependencies ?? {});

const commonOptions = {
  bundle: true,
  platform: 'node',
  loader: { '.ts': 'ts' },
  resolveExtensions: ['.ts'],
  target: 'es2022',
  tsconfig: 'tsconfig.json',
  format: 'esm',
  minify: true,
  sourcemap: true,
  external,
};

const buildLibrary = esbuild.build({
  ...commonOptions,
  entryPoints: ['./src/index.ts'],
  outfile: './dist/index.mjs',
});

const buildCli = esbuild.build({
  ...commonOptions,
  entryPoints: ['./src/main.ts'],
  outfile: './dist/main.mjs',
  banner: {
    js: '#!/usr/bin/env node',
  },
});

Promise.all([buildLibrary, buildCli]).catch((err) => {
  console.error(err);
  process.exit(1);
});
