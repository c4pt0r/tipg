import { describe, it, expect, afterAll } from 'vitest';
import knex, { Knex } from 'knex';
import { createKnexClient } from './client.js';
import { defaultConfig } from '../shared/config.js';

describe('Knex Connection & Protocol Compatibility [pg-tikv]', () => {
  const clients: Knex[] = [];

  afterAll(async () => {
    await Promise.all(clients.map((c) => c.destroy()));
  });

  describe('connection establishment', () => {
    it('should establish connection via pg driver', async () => {
      const db = createKnexClient();
      clients.push(db);

      const result = await db.raw('SELECT 1 as value');
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].value).toBe(1);
    });

    it('should execute query after connection', async () => {
      const db = createKnexClient();
      clients.push(db);

      const result = await db.select(db.raw('1 as value'));
      expect(result).toHaveLength(1);
      expect(result[0].value).toBe(1);
    });

    it('should handle connection with explicit database', async () => {
      const db = knex({
        client: 'pg',
        connection: {
          host: defaultConfig.host,
          port: defaultConfig.port,
          database: 'postgres',
          user: defaultConfig.user,
          password: defaultConfig.password,
        },
      });
      clients.push(db);

      const result = await db.raw('SELECT current_database()');
      expect(result.rows[0].current_database).toBe('postgres');
    });
  });

  describe('connection pool', () => {
    it('should reuse connections from pool', async () => {
      const db = createKnexClient({
        pool: { min: 1, max: 5 },
      });
      clients.push(db);

      const queries = Array.from({ length: 10 }, (_, i) =>
        db.raw(`SELECT ${i + 1} as num`)
      );
      const results = await Promise.all(queries);

      expect(results).toHaveLength(10);
      results.forEach((r, i) => {
        expect(r.rows[0].num).toBe(i + 1);
      });
    });

    it('should handle concurrent connections', async () => {
      const dbs = Array.from({ length: 3 }, () => {
        const db = createKnexClient();
        clients.push(db);
        return db;
      });

      const results = await Promise.all(
        dbs.map((db) => db.raw('SELECT current_timestamp as ts'))
      );

      expect(results).toHaveLength(3);
      results.forEach((r) => {
        expect(r.rows[0].ts).toBeDefined();
      });
    });
  });

  describe('connection error handling', () => {
    it('should report connection error for invalid host', async () => {
      const db = knex({
        client: 'pg',
        connection: {
          host: 'invalid-host-that-does-not-exist',
          port: 5433,
          database: 'postgres',
          user: 'admin',
          password: 'admin',
          connectionTimeoutMillis: 1000,
        },
      });

      await expect(db.raw('SELECT 1')).rejects.toThrow();
      await db.destroy().catch(() => {});
    });

    it('should report authentication error for invalid credentials', async () => {
      const db = knex({
        client: 'pg',
        connection: {
          host: defaultConfig.host,
          port: defaultConfig.port,
          database: defaultConfig.database,
          user: 'nonexistent_user',
          password: 'wrong_password',
        },
      });

      await expect(db.raw('SELECT 1')).rejects.toThrow();
      await db.destroy().catch(() => {});
    });
  });
});
