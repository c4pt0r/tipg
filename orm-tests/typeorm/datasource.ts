import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';
import pg from 'pg';
import { User, Post, Tag } from './entities/index.js';
import { defaultConfig } from '../shared/config.js';

const { types } = pg;

function restoreDefaultTypeParsers() {
  types.setTypeParser(types.builtins.TIMESTAMPTZ, (val: string) => new Date(val));
  types.setTypeParser(types.builtins.TIMESTAMP, (val: string) => new Date(val));
  types.setTypeParser(types.builtins.DATE, (val: string) => new Date(val));
}

export function createDataSourceOptions(
  options: Partial<DataSourceOptions> = {}
): DataSourceOptions {
  return {
    type: 'postgres',
    host: defaultConfig.host,
    port: defaultConfig.port,
    database: defaultConfig.database,
    username: defaultConfig.user,
    password: defaultConfig.password,
    ssl: defaultConfig.ssl ? { rejectUnauthorized: false } : false,
    entities: [User, Post, Tag],
    synchronize: false,
    logging: process.env.DEBUG === 'true' ? ['query', 'error'] : false,
    ...options,
  } as DataSourceOptions;
}

export function createDataSource(options: Partial<DataSourceOptions> = {}): DataSource {
  restoreDefaultTypeParsers();
  return new DataSource(createDataSourceOptions(options));
}

let sharedDataSource: DataSource | null = null;

export async function getSharedDataSource(): Promise<DataSource> {
  if (!sharedDataSource || !sharedDataSource.isInitialized) {
    sharedDataSource = createDataSource();
    await sharedDataSource.initialize();
  }
  return sharedDataSource;
}

export async function closeSharedDataSource(): Promise<void> {
  if (sharedDataSource?.isInitialized) {
    await sharedDataSource.destroy();
    sharedDataSource = null;
  }
}
