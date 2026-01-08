import knex, { Knex } from 'knex';
import { defaultConfig } from '../shared/config.js';

export function createKnexClient(options: Partial<Knex.Config> = {}): Knex {
  return knex({
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
    debug: process.env.DEBUG === 'true',
    ...options,
  });
}

let sharedKnex: Knex | null = null;

export function getSharedKnex(): Knex {
  if (!sharedKnex) {
    sharedKnex = createKnexClient();
  }
  return sharedKnex;
}

export async function closeSharedKnex(): Promise<void> {
  if (sharedKnex) {
    await sharedKnex.destroy();
    sharedKnex = null;
  }
}

export async function setupKnexTables(db: Knex): Promise<void> {
  await db.schema.dropTableIfExists('knex_post_tags');
  await db.schema.dropTableIfExists('knex_posts');
  await db.schema.dropTableIfExists('knex_tags');
  await db.schema.dropTableIfExists('knex_users');

  await db.schema.createTable('knex_users', (table) => {
    table.increments('id').primary();
    table.string('email', 255).notNullable().unique();
    table.string('name', 100).notNullable();
    table.integer('age').defaultTo(0);
    table.boolean('is_active').defaultTo(true);
    table.text('bio');
    table.jsonb('metadata');
    table.uuid('external_id');
    table.timestamp('created_at', { useTz: true }).defaultTo(db.fn.now());
    table.timestamp('updated_at', { useTz: true }).defaultTo(db.fn.now());
    table.index('email');
  });

  await db.schema.createTable('knex_tags', (table) => {
    table.increments('id').primary();
    table.string('name', 100).notNullable().unique();
    table.string('color', 7).defaultTo('#000000');
    table.index('name');
  });

  await db.schema.createTable('knex_posts', (table) => {
    table.increments('id').primary();
    table.string('title', 500).notNullable();
    table.text('content').notNullable();
    table.boolean('published').defaultTo(false);
    table.integer('view_count').defaultTo(0);
    table.jsonb('settings');
    table.timestamp('created_at', { useTz: true }).defaultTo(db.fn.now());
    table.integer('author_id').notNullable().references('id').inTable('knex_users').onDelete('CASCADE');
    table.index('title');
  });

  await db.schema.createTable('knex_post_tags', (table) => {
    table.integer('post_id').notNullable().references('id').inTable('knex_posts').onDelete('CASCADE');
    table.integer('tag_id').notNullable().references('id').inTable('knex_tags').onDelete('CASCADE');
    table.primary(['post_id', 'tag_id']);
  });
}

export async function cleanupKnexTables(db: Knex): Promise<void> {
  await db('knex_post_tags').del();
  await db('knex_posts').del();
  await db('knex_tags').del();
  await db('knex_users').del();
}
