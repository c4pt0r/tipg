export interface DatabaseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl: boolean;
}

export const defaultConfig: DatabaseConfig = {
  host: process.env.PG_HOST ?? '127.0.0.1',
  port: parseInt(process.env.PG_PORT ?? '5433', 10),
  database: process.env.PG_DATABASE ?? 'postgres',
  user: process.env.PG_USER ?? 'postgres',
  password: process.env.PG_PASSWORD ?? 'postgres',
  ssl: process.env.PG_SSL === 'true',
};

export function getConnectionString(config: DatabaseConfig = defaultConfig): string {
  const { host, port, database, user, password, ssl } = config;
  const sslParam = ssl ? '?sslmode=require' : '';
  return `postgresql://${user}:${password}@${host}:${port}/${database}${sslParam}`;
}

export function getPgConfig(config: DatabaseConfig = defaultConfig) {
  return {
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    ssl: config.ssl ? { rejectUnauthorized: false } : false,
  };
}
