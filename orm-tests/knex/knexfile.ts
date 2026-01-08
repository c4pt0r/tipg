import type { Knex } from 'knex';
import { defaultConfig } from '../shared/config.js';

const config: Knex.Config = {
  client: 'pg',
  connection: {
    host: defaultConfig.host,
    port: defaultConfig.port,
    database: defaultConfig.database,
    user: defaultConfig.user,
    password: defaultConfig.password,
    ssl: defaultConfig.ssl ? { rejectUnauthorized: false } : false,
  },
  pool: {
    min: 0,
    max: 10,
  },
  migrations: {
    tableName: 'knex_migrations',
    directory: './migrations',
  },
  seeds: {
    directory: './seeds',
  },
};

export default config;
