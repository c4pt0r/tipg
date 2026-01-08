import { describe, it, expect, afterAll } from 'vitest';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';
import { getPgConfig, defaultConfig } from '../shared/config.js';

const { Pool } = pg;

describe('Drizzle Connection & Protocol Compatibility [pg-tikv]', () => {
  const pools: pg.Pool[] = [];

  afterAll(async () => {
    await Promise.all(pools.map((p) => p.end()));
  });

  describe('connection establishment', () => {
    it('should establish connection via pg driver', async () => {
      const pool = new Pool(getPgConfig());
      pools.push(pool);
      const db = drizzle(pool);

      const result = await db.execute(sql`SELECT 1 as value`);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].value).toBe(1);
    });

    it('should execute query after connection', async () => {
      const pool = new Pool(getPgConfig());
      pools.push(pool);
      const db = drizzle(pool);

      const result = await db.execute(sql`SELECT current_timestamp as ts`);
      expect(result.rows[0].ts).toBeDefined();
    });

    it('should handle connection with explicit database', async () => {
      const pool = new Pool({ ...getPgConfig(), database: 'postgres' });
      pools.push(pool);
      const db = drizzle(pool);

      const result = await db.execute(sql`SELECT current_database() as db`);
      expect(result.rows[0].db).toBe('postgres');
    });
  });

  describe('connection pool', () => {
    it('should reuse connections from pool', async () => {
      const pool = new Pool({ ...getPgConfig(), max: 5, min: 1 });
      pools.push(pool);
      const db = drizzle(pool);

      const queries = Array.from({ length: 10 }, (_, i) =>
        db.execute(sql`SELECT ${i + 1} as num`)
      );
      const results = await Promise.all(queries);

      expect(results).toHaveLength(10);
      results.forEach((r, i) => {
        expect(Number(r.rows[0].num)).toBe(i + 1);
      });
    });

    it('should handle concurrent connections', async () => {
      const poolConfigs = Array.from({ length: 3 }, () => {
        const pool = new Pool(getPgConfig());
        pools.push(pool);
        return drizzle(pool);
      });

      const results = await Promise.all(
        poolConfigs.map((db) => db.execute(sql`SELECT current_timestamp as ts`))
      );

      expect(results).toHaveLength(3);
      results.forEach((r) => {
        expect(r.rows[0].ts).toBeDefined();
      });
    });
  });

  describe('connection error handling', () => {
    it('should report connection error for invalid host', async () => {
      const pool = new Pool({
        host: 'invalid-host-that-does-not-exist',
        port: 5433,
        database: 'postgres',
        user: 'admin',
        password: 'admin',
        connectionTimeoutMillis: 1000,
      });
      const db = drizzle(pool);

      await expect(db.execute(sql`SELECT 1`)).rejects.toThrow();
      await pool.end().catch(() => {});
    });

    it('should report authentication error for invalid credentials', async () => {
      const pool = new Pool({
        host: defaultConfig.host,
        port: defaultConfig.port,
        database: defaultConfig.database,
        user: 'nonexistent_user',
        password: 'wrong_password',
      });
      const db = drizzle(pool);

      await expect(db.execute(sql`SELECT 1`)).rejects.toThrow();
      await pool.end().catch(() => {});
    });
  });
});
