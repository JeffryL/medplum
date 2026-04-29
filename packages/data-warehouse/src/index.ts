// SPDX-FileCopyrightText: Copyright Orangebot, Inc. and Medplum contributors
// SPDX-License-Identifier: Apache-2.0
import { Command } from 'commander';
import { config } from 'dotenv';
import {
  formatPostgresTargetLabel,
  parseDefaultRowThreshold,
  parseRowThresholdOverrides,
  resolveAthenaCatalogName,
  resolveAthenaOutputLocation,
  resolveAthenaWorkGroup,
  resolveAwsS3TableArn,
  resolveDatabaseUrl,
} from './config.ts';
import { parseCommaSeparatedTableNames } from './resource-types.ts';
import { downloadParquetFiles } from './download.ts';
import { exportData, resolveWarehouseSourcesFromPostgresTableNames } from './export.ts';
import { migrateTables } from './migrate.ts';
import { syncData } from './sync.ts';

config();

export async function main(args: string[]): Promise<void> {
  const program = new Command();

  program
    .name('medplum-data-warehouse')
    .description('Export Medplum Postgres data to S3 via DuckDB Iceberg tables');

  program
    .command('export')
    .description('Export Postgres tables to Iceberg or Parquet using explicit time windows')
    .option('-d, --database-url <url>', 'Postgres Database URL', process.env.MEDPLUM_DATABASE_URL)
    .option('--db-host <host>', 'Postgres Database Host', process.env.MEDPLUM_DATABASE_HOST)
    .option('--db-port <port>', 'Postgres Database Port', process.env.MEDPLUM_DATABASE_PORT || '5432')
    .option('--db-name <dbname>', 'Postgres Database Name', process.env.MEDPLUM_DATABASE_DBNAME)
    .option('--db-username <username>', 'Postgres Database Username', process.env.MEDPLUM_DATABASE_USERNAME)
    .option('--db-password <password>', 'Postgres Database Password', process.env.MEDPLUM_DATABASE_PASSWORD)
    .option(
      '--database-statement-timeout <duration>',
      'Postgres statement_timeout for DuckDB-attached connections (PostgreSQL duration, e.g. 15min, 900s; use 0 to disable)',
      process.env.MEDPLUM_DATABASE_STATEMENT_TIMEOUT ?? '15min'
    )
    .option('-s, --s3-bucket <bucket>', 'S3 Bucket name', process.env.S3_BUCKET)
    .option('-r, --s3-region <region>', 'S3 Region', process.env.AWS_REGION || 'us-east-1')
    .option(
      '-a, --aws-s3-table-arn <arn>',
      'AWS S3 Table ARN (optional)',
      process.env.MEDPLUM_AWS_S3_TABLE_ARN ?? process.env.AWS_S3_TABLE_ARN
    )
    .option('-l, --local-path <path>', 'Write Parquet files to local directory instead of S3 (no AWS credentials needed)')
    .option('-n, --namespace <namespace>', 'Iceberg namespace', 'default')
    .option(
      '--athena-output-location <s3-uri>',
      'Athena query result output location (optional if workgroup enforces output location)',
      process.env.MEDPLUM_ATHENA_OUTPUT_LOCATION ?? process.env.ATHENA_OUTPUT_LOCATION
    )
    .option(
      '--athena-workgroup <name>',
      'Athena workgroup for DDL operations',
      process.env.MEDPLUM_ATHENA_WORKGROUP ?? process.env.ATHENA_WORKGROUP
    )
    .option(
      '--athena-catalog-name <name>',
      'Athena catalog name for table metadata and DDL',
      process.env.MEDPLUM_ATHENA_CATALOG_NAME ?? process.env.ATHENA_CATALOG_NAME
    )
    .requiredOption(
      '-t, --table <names>',
      'Comma-separated Postgres table names exactly as stored (default: MEDPLUM_DATA_WAREHOUSE_TABLES). Managed Iceberg names are derived from each identifier (snake_case, lowercased).',
      process.env.MEDPLUM_DATA_WAREHOUSE_TABLES
    )
    .option('--start-window <start>', 'Start window timestamp (ISO 8601)')
    .option(
      '--end-window <end>',
      'End window timestamp (ISO 8601, exclusive). If omitted, export from --start-window with no upper bound on lastUpdated'
    )
    .option(
      '--clean',
      'Drop managed Iceberg tables in the target namespace before export (requires --aws-s3-table-arn). ZSTD Parquet write properties are applied after load whenever a table is created from scratch (`--clean` or first-time table); incremental runs onto existing tables skip that CALL.'
    )
    .action(async (options) => {
      const {
        databaseUrl,
        dbHost,
        dbPort,
        dbName,
        dbUsername,
        dbPassword,
        databaseStatementTimeout,
        s3Bucket,
        s3Region,
        awsS3TableArn,
        localPath,
        namespace,
        athenaOutputLocation,
        athenaWorkgroup,
        athenaCatalogName,
        table,
        startWindow,
        endWindow,
        clean,
      } = options;

      const resolvedAwsS3TableArn = resolveAwsS3TableArn(awsS3TableArn);
      const resolvedAthenaOutputLocation = resolveAthenaOutputLocation(athenaOutputLocation);
      const resolvedAthenaWorkGroup = resolveAthenaWorkGroup(athenaWorkgroup);
      const resolvedAthenaCatalogName = resolveAthenaCatalogName(athenaCatalogName);

      if (clean && !resolvedAwsS3TableArn) {
        console.error('Invalid options: --clean requires --aws-s3-table-arn (managed Iceberg)');
        process.exit(1);
      }
      if (clean && resolvedAwsS3TableArn) {
        console.error('Invalid options: --clean is no longer supported. Run the migrate command to create/replace tables.');
        process.exit(1);
      }

      if (!s3Bucket && !resolvedAwsS3TableArn && !localPath) {
        console.error('Missing required option: --s3-bucket, --aws-s3-table-arn, or --local-path');
        process.exit(1);
      }

      if (!startWindow) {
        console.error('Missing required option: --start-window');
        process.exit(1);
      }

      const resolvedDatabaseUrl = resolveDatabaseUrl({
        databaseUrl,
        dbHost,
        dbPort,
        dbName,
        dbUsername,
        dbPassword,
        databaseStatementTimeout,
      });
      const tableNames = parseCommaSeparatedTableNames(table);
      if (!tableNames?.length) {
        console.error('Missing or empty: --table (comma-separated Postgres table names, or set MEDPLUM_DATA_WAREHOUSE_TABLES)');
        process.exit(1);
      }
      const warehouseSources = resolveWarehouseSourcesFromPostgresTableNames(tableNames);

      await exportData({
        databaseUrl: resolvedDatabaseUrl,
        s3Bucket,
        s3Region,
        startWindow,
        ...(endWindow ? { endWindow } : {}),
        awsS3TableArn: resolvedAwsS3TableArn,
        namespace,
        warehouseSources,
        localPath,
        athenaOutputLocation: resolvedAthenaOutputLocation,
        athenaWorkGroup: resolvedAthenaWorkGroup,
        athenaCatalogName: resolvedAthenaCatalogName,
        ...(clean ? { clean: true } : {}),
      });
      console.log('Export completed successfully');
    });

  program
    .command('migrate')
    .description(
      'Create missing S3 Tables Iceberg tables and namespace metadata (verifies each Postgres source table exists first)'
    )
    .option('-d, --database-url <url>', 'Postgres Database URL', process.env.MEDPLUM_DATABASE_URL)
    .option('--db-host <host>', 'Postgres Database Host', process.env.MEDPLUM_DATABASE_HOST)
    .option('--db-port <port>', 'Postgres Database Port', process.env.MEDPLUM_DATABASE_PORT || '5432')
    .option('--db-name <dbname>', 'Postgres Database Name', process.env.MEDPLUM_DATABASE_DBNAME)
    .option('--db-username <username>', 'Postgres Database Username', process.env.MEDPLUM_DATABASE_USERNAME)
    .option('--db-password <password>', 'Postgres Database Password', process.env.MEDPLUM_DATABASE_PASSWORD)
    .option(
      '--database-statement-timeout <duration>',
      'Postgres statement_timeout merged into the connection URI for migrate checks (PostgreSQL duration, e.g. 15min, 900s; use 0 to disable)',
      process.env.MEDPLUM_DATABASE_STATEMENT_TIMEOUT ?? '15min'
    )
    .requiredOption(
      '-a, --aws-s3-table-arn <arn>',
      'AWS S3 Table ARN',
      process.env.MEDPLUM_AWS_S3_TABLE_ARN ?? process.env.AWS_S3_TABLE_ARN
    )
    .option('-r, --s3-region <region>', 'S3 Region', process.env.AWS_REGION || 'us-east-1')
    .option('-n, --namespace <namespace>', 'Iceberg namespace', 'default')
    .option(
      '--athena-workgroup <name>',
      'Athena workgroup for metadata checks',
      process.env.MEDPLUM_ATHENA_WORKGROUP ?? process.env.ATHENA_WORKGROUP
    )
    .option(
      '--athena-catalog-name <name>',
      'Athena catalog name for metadata checks',
      process.env.MEDPLUM_ATHENA_CATALOG_NAME ?? process.env.ATHENA_CATALOG_NAME
    )
    .requiredOption(
      '-t, --table <names>',
      'Comma-separated Postgres table names exactly as stored (default: MEDPLUM_DATA_WAREHOUSE_TABLES). Provisions matching Iceberg tables in S3 Tables.',
      process.env.MEDPLUM_DATA_WAREHOUSE_TABLES
    )
    .action(async (options) => {
      const {
        databaseUrl,
        dbHost,
        dbPort,
        dbName,
        dbUsername,
        dbPassword,
        databaseStatementTimeout,
        awsS3TableArn,
        s3Region,
        namespace,
        athenaWorkgroup,
        athenaCatalogName,
        table,
      } = options;

      try {
        const resolvedDatabaseUrl = resolveDatabaseUrl({
          databaseUrl,
          dbHost,
          dbPort,
          dbName,
          dbUsername,
          dbPassword,
          databaseStatementTimeout,
        });
        const resolvedAwsS3TableArn = resolveAwsS3TableArn(awsS3TableArn);
        const resolvedAthenaWorkGroup = resolveAthenaWorkGroup(athenaWorkgroup);
        const resolvedAthenaCatalogName = resolveAthenaCatalogName(athenaCatalogName);
        if (!resolvedAwsS3TableArn) {
          throw new Error('Missing required option: --aws-s3-table-arn');
        }

        const tableNames = parseCommaSeparatedTableNames(table);
        if (!tableNames?.length) {
          throw new Error('Missing or empty: --table (comma-separated Postgres table names, or set MEDPLUM_DATA_WAREHOUSE_TABLES)');
        }
        const warehouseSources = resolveWarehouseSourcesFromPostgresTableNames(tableNames);

        const migrateStartedAt = Date.now();
        const { namespace: icebergNamespace, tableCount } = await migrateTables({
          databaseUrl: resolvedDatabaseUrl,
          awsS3TableArn: resolvedAwsS3TableArn,
          s3Region,
          namespace,
          warehouseSources,
          athenaWorkGroup: resolvedAthenaWorkGroup,
          athenaCatalogName: resolvedAthenaCatalogName,
        });
        const migrateElapsedSec = ((Date.now() - migrateStartedAt) / 1000).toFixed(1);
        const sourceSummary = `${tableCount} Postgres source table(s) from --table`;
        const athenaBits = [
          resolvedAthenaCatalogName && `Athena catalog ${resolvedAthenaCatalogName}`,
          resolvedAthenaWorkGroup && `workgroup ${resolvedAthenaWorkGroup}`,
        ].filter(Boolean);
        console.log(
          `Migrate completed in ${migrateElapsedSec}s: ${sourceSummary}; Postgres ${formatPostgresTargetLabel(
            resolvedDatabaseUrl
          )}; Iceberg namespace ${JSON.stringify(icebergNamespace)}; S3 Tables ${resolvedAwsS3TableArn}; region ${s3Region}` +
            (athenaBits.length > 0 ? `; ${athenaBits.join('; ')}` : '')
        );
      } catch (err) {
        console.error('Migrate failed:', err);
        process.exit(1);
      }
    });

  program
    .command('sync')
    .description('Incrementally sync Postgres tables to AWS S3 Tables using a watermark and per-table row threshold')
    .option('-d, --database-url <url>', 'Postgres Database URL', process.env.MEDPLUM_DATABASE_URL)
    .option('--db-host <host>', 'Postgres Database Host', process.env.MEDPLUM_DATABASE_HOST)
    .option('--db-port <port>', 'Postgres Database Port', process.env.MEDPLUM_DATABASE_PORT || '5432')
    .option('--db-name <dbname>', 'Postgres Database Name', process.env.MEDPLUM_DATABASE_DBNAME)
    .option('--db-username <username>', 'Postgres Database Username', process.env.MEDPLUM_DATABASE_USERNAME)
    .option('--db-password <password>', 'Postgres Database Password', process.env.MEDPLUM_DATABASE_PASSWORD)
    .option(
      '--database-statement-timeout <duration>',
      'Postgres statement_timeout for DuckDB-attached connections (PostgreSQL duration, e.g. 15min, 900s; use 0 to disable)',
      process.env.MEDPLUM_DATABASE_STATEMENT_TIMEOUT ?? '15min'
    )
    .requiredOption(
      '-a, --aws-s3-table-arn <arn>',
      'AWS S3 Table ARN',
      process.env.MEDPLUM_AWS_S3_TABLE_ARN ?? process.env.AWS_S3_TABLE_ARN
    )
    .option('-r, --s3-region <region>', 'S3 Region', process.env.AWS_REGION || 'us-east-1')
    .option('-n, --namespace <namespace>', 'Iceberg namespace', 'default')
    .option(
      '--athena-output-location <s3-uri>',
      'Athena query result output location (optional if workgroup enforces output location)',
      process.env.MEDPLUM_ATHENA_OUTPUT_LOCATION ?? process.env.ATHENA_OUTPUT_LOCATION
    )
    .option(
      '--athena-workgroup <name>',
      'Athena workgroup for DDL operations',
      process.env.MEDPLUM_ATHENA_WORKGROUP ?? process.env.ATHENA_WORKGROUP
    )
    .option(
      '--athena-catalog-name <name>',
      'Athena catalog name for table metadata and DDL',
      process.env.MEDPLUM_ATHENA_CATALOG_NAME ?? process.env.ATHENA_CATALOG_NAME
    )
    .requiredOption(
      '-t, --table <names>',
      'Comma-separated Postgres table names exactly as stored (default: MEDPLUM_DATA_WAREHOUSE_TABLES).',
      process.env.MEDPLUM_DATA_WAREHOUSE_TABLES
    )
    .option(
      '--default-row-threshold <count>',
      'Default minimum row count required before syncing a table',
      process.env.MEDPLUM_DATA_WAREHOUSE_DEFAULT_ROW_THRESHOLD
    )
    .option(
      '--row-thresholds-json <json>',
      'Per-table row thresholds JSON (table keys are derived identifiers, e.g. patient_history), e.g. {"default":1000,"patient_history":5000}',
      process.env.MEDPLUM_DATA_WAREHOUSE_ROW_THRESHOLDS_JSON
    )
    .action(async (options) => {
      const {
        databaseUrl,
        dbHost,
        dbPort,
        dbName,
        dbUsername,
        dbPassword,
        databaseStatementTimeout,
        awsS3TableArn,
        s3Region,
        namespace,
        athenaOutputLocation,
        athenaWorkgroup,
        athenaCatalogName,
        table,
        defaultRowThreshold,
        rowThresholdsJson,
      } = options;

      try {
        const resolvedDatabaseUrl = resolveDatabaseUrl({
          databaseUrl,
          dbHost,
          dbPort,
          dbName,
          dbUsername,
          dbPassword,
          databaseStatementTimeout,
        });
        const resolvedAwsS3TableArn = resolveAwsS3TableArn(awsS3TableArn);
        const resolvedAthenaOutputLocation = resolveAthenaOutputLocation(athenaOutputLocation);
        const resolvedAthenaWorkGroup = resolveAthenaWorkGroup(athenaWorkgroup);
        const resolvedAthenaCatalogName = resolveAthenaCatalogName(athenaCatalogName);
        if (!resolvedAwsS3TableArn) {
          throw new Error('Missing required option: --aws-s3-table-arn');
        }

        const tableNames = parseCommaSeparatedTableNames(table);
        if (!tableNames?.length) {
          throw new Error('Missing or empty: --table (comma-separated Postgres table names, or set MEDPLUM_DATA_WAREHOUSE_TABLES)');
        }
        const warehouseSources = resolveWarehouseSourcesFromPostgresTableNames(tableNames);

        const result = await syncData({
          databaseUrl: resolvedDatabaseUrl,
          s3Region,
          awsS3TableArn: resolvedAwsS3TableArn,
          athenaOutputLocation: resolvedAthenaOutputLocation,
          athenaWorkGroup: resolvedAthenaWorkGroup,
          athenaCatalogName: resolvedAthenaCatalogName,
          namespace,
          warehouseSources,
          defaultRowThreshold: parseDefaultRowThreshold(defaultRowThreshold),
          rowThresholdOverrides: parseRowThresholdOverrides(rowThresholdsJson),
        });

        const inserted = result.resources.filter((resource) => resource.action === 'insert').length;
        const skipped = result.resources.length - inserted;
        console.log(`Sync completed successfully: ${inserted} inserted, ${skipped} skipped`);
      } catch (err) {
        console.error('Sync failed:', err);
        process.exit(1);
      }
    });

  program
    .command('download')
    .description('Download raw parquet files from an AWS S3 Table')
    .requiredOption('-a, --aws-s3-table-arn <arn>', 'AWS S3 Table ARN', process.env.AWS_S3_TABLE_ARN)
    .option('-r, --s3-region <region>', 'S3 Region', process.env.AWS_REGION || 'us-east-1')
    .option('-n, --namespace <namespace>', 'Iceberg namespace', 'default')
    .option('-t, --table <table>', 'Iceberg table name', 'audit_events')
    .option('-o, --output-dir <path>', 'Output directory', 'parquet-download')
    .action(async (options) => {
      const { awsS3TableArn, s3Region, namespace, table, outputDir } = options;

      try {
        const count = await downloadParquetFiles({
          awsS3TableArn,
          s3Region,
          namespace,
          table,
          outputDir,
        });

        if (count === 0) {
          console.log('No Parquet files found for the selected table');
        } else {
          console.log(`Downloaded ${count} Parquet file(s)`);
        }
      } catch (err) {
        console.error('Download failed:', err);
        process.exit(1);
      }
    });

  await program.parseAsync(args);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main(process.argv).catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
