import { drizzle } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import { getPgConfig } from '../shared/config.js';
import * as schema from './schema.js';

const { Pool } = pg;

export function createDrizzleClient() {
  const pool = new Pool(getPgConfig());
  return {
    db: drizzle(pool, { schema }),
    pool,
  };
}

let sharedClient: ReturnType<typeof createDrizzleClient> | null = null;

export function getSharedDrizzle() {
  if (!sharedClient) {
    sharedClient = createDrizzleClient();
  }
  return sharedClient;
}

export async function closeSharedDrizzle(): Promise<void> {
  if (sharedClient) {
    await sharedClient.pool.end();
    sharedClient = null;
  }
}

export async function setupDrizzleTables(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('DROP TABLE IF EXISTS drizzle_post_tags CASCADE');
    await client.query('DROP TABLE IF EXISTS drizzle_posts CASCADE');
    await client.query('DROP TABLE IF EXISTS drizzle_tags CASCADE');
    await client.query('DROP TABLE IF EXISTS drizzle_users CASCADE');

    await client.query(`
      CREATE TABLE drizzle_users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(100) NOT NULL,
        age INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT true,
        bio TEXT,
        metadata JSONB,
        external_id UUID,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE drizzle_tags (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL UNIQUE,
        color VARCHAR(7) DEFAULT '#000000'
      )
    `);

    await client.query(`
      CREATE TABLE drizzle_posts (
        id SERIAL PRIMARY KEY,
        title VARCHAR(500) NOT NULL,
        content TEXT NOT NULL,
        published BOOLEAN DEFAULT false,
        view_count INTEGER DEFAULT 0,
        settings JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        author_id INTEGER NOT NULL REFERENCES drizzle_users(id) ON DELETE CASCADE
      )
    `);

    await client.query(`
      CREATE TABLE drizzle_post_tags (
        post_id INTEGER NOT NULL REFERENCES drizzle_posts(id) ON DELETE CASCADE,
        tag_id INTEGER NOT NULL REFERENCES drizzle_tags(id) ON DELETE CASCADE,
        PRIMARY KEY (post_id, tag_id)
      )
    `);

    await client.query('CREATE INDEX drizzle_users_email_idx ON drizzle_users(email)');
    await client.query('CREATE INDEX drizzle_posts_title_idx ON drizzle_posts(title)');
    await client.query('CREATE INDEX drizzle_tags_name_idx ON drizzle_tags(name)');
  } finally {
    client.release();
  }
}

export async function cleanupDrizzleTables(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('DELETE FROM drizzle_post_tags');
    await client.query('DELETE FROM drizzle_posts');
    await client.query('DELETE FROM drizzle_tags');
    await client.query('DELETE FROM drizzle_users');
  } finally {
    client.release();
  }
}
