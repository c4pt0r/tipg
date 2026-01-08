import { describe, it, expect, afterAll } from 'vitest';
import { Sequelize } from 'sequelize';
import { createSequelize, createSequelizeOptions } from './connection.js';

describe('Sequelize Connection & Protocol Compatibility [pg-tikv]', () => {
  const instances: Sequelize[] = [];

  afterAll(async () => {
    await Promise.all(instances.map((s) => s.close()));
  });

  describe('connection establishment', () => {
    it('should establish connection via pg driver', async () => {
      const sequelize = createSequelize();
      instances.push(sequelize);

      await sequelize.authenticate();
      expect(sequelize).toBeDefined();
    });

    it('should execute raw query after connection', async () => {
      const sequelize = createSequelize();
      instances.push(sequelize);

      const [results] = await sequelize.query('SELECT 1 as value');
      expect(results).toHaveLength(1);
      expect((results[0] as { value: number }).value).toBe(1);
    });

    it('should handle connection with explicit database', async () => {
      const sequelize = createSequelize({ database: 'postgres' });
      instances.push(sequelize);

      await sequelize.authenticate();
    });
  });

  describe('connection pool', () => {
    it('should reuse connections from pool', async () => {
      const sequelize = createSequelize({
        pool: { max: 5, min: 1, idle: 10000 },
      });
      instances.push(sequelize);

      const queries = Array.from({ length: 10 }, (_, i) =>
        sequelize.query(`SELECT ${i + 1} as num`)
      );
      const results = await Promise.all(queries);

      expect(results).toHaveLength(10);
      results.forEach(([r], i) => {
        expect((r[0] as { num: number }).num).toBe(i + 1);
      });
    });

    it('should handle concurrent connections', async () => {
      const sequelizes = await Promise.all(
        Array.from({ length: 3 }, async () => {
          const s = createSequelize();
          instances.push(s);
          await s.authenticate();
          return s;
        })
      );

      const results = await Promise.all(
        sequelizes.map((s) => s.query('SELECT current_timestamp as ts'))
      );

      expect(results).toHaveLength(3);
      results.forEach(([r]) => {
        expect((r[0] as { ts: unknown }).ts).toBeDefined();
      });
    });
  });

  describe('connection error handling', () => {
    it('should report connection error for invalid host', async () => {
      const sequelize = new Sequelize({
        ...createSequelizeOptions(),
        host: 'invalid-host-that-does-not-exist',
        dialectOptions: { connectTimeout: 1000 },
      });

      await expect(sequelize.authenticate()).rejects.toThrow();
      await sequelize.close().catch(() => {});
    });

    it('should report authentication error for invalid credentials', async () => {
      const sequelize = new Sequelize({
        ...createSequelizeOptions(),
        username: 'nonexistent_user',
        password: 'wrong_password',
      });

      await expect(sequelize.authenticate()).rejects.toThrow();
      await sequelize.close().catch(() => {});
    });
  });
});
