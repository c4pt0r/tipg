import { Sequelize, Options } from 'sequelize';
import { defaultConfig } from '../shared/config.js';
import { initModels } from './models.js';

export function createSequelizeOptions(options: Partial<Options> = {}): Options {
  return {
    dialect: 'postgres',
    host: defaultConfig.host,
    port: defaultConfig.port,
    database: defaultConfig.database,
    username: defaultConfig.user,
    password: defaultConfig.password,
    logging: process.env.DEBUG === 'true' ? console.log : false,
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
    ...options,
  };
}

export function createSequelize(options: Partial<Options> = {}): Sequelize {
  const sequelize = new Sequelize(createSequelizeOptions(options));
  initModels(sequelize);
  return sequelize;
}

let sharedSequelize: Sequelize | null = null;

export async function getSharedSequelize(): Promise<Sequelize> {
  if (!sharedSequelize) {
    sharedSequelize = createSequelize();
    await sharedSequelize.authenticate();
  }
  return sharedSequelize;
}

export async function closeSharedSequelize(): Promise<void> {
  if (sharedSequelize) {
    await sharedSequelize.close();
    sharedSequelize = null;
  }
}
