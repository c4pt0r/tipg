import { describe, it, expect, afterAll, beforeAll } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource, closeSharedDataSource } from './datasource.js';

describe('TypeORM Connection & Protocol Compatibility [pg-tikv]', () => {
  describe('connection establishment', () => {
    it('should establish connection via pg driver', async () => {
      const ds = createDataSource();
      await ds.initialize();
      expect(ds.isInitialized).toBe(true);
      await ds.destroy();
    });

    it('should execute raw query after connection', async () => {
      const ds = createDataSource();
      await ds.initialize();
      const result = await ds.query('SELECT 1 as value');
      expect(result).toHaveLength(1);
      expect(result[0].value).toBe(1);
      await ds.destroy();
    });

    it('should handle connection with explicit database', async () => {
      const ds = createDataSource({ database: 'postgres' });
      await ds.initialize();
      expect(ds.isInitialized).toBe(true);
      await ds.destroy();
    });
  });

  describe('connection pool', () => {
    it('should reuse connections from pool', async () => {
      const ds = createDataSource({
        extra: {
          max: 5,
          min: 1,
          idleTimeoutMillis: 10000,
        },
      });
      await ds.initialize();

      const queries = Array.from({ length: 10 }, (_, i) =>
        ds.query(`SELECT ${i + 1} as num`)
      );
      const results = await Promise.all(queries);
      
      expect(results).toHaveLength(10);
      results.forEach((r, i) => {
        expect(r[0].num).toBe(i + 1);
      });

      await ds.destroy();
    });

    it('should handle concurrent connections', async () => {
      const dataSources = await Promise.all(
        Array.from({ length: 3 }, () => {
          const ds = createDataSource();
          return ds.initialize().then(() => ds);
        })
      );

      const results = await Promise.all(
        dataSources.map((ds) => ds.query('SELECT current_timestamp as ts'))
      );

      expect(results).toHaveLength(3);
      results.forEach((r) => {
        expect(r[0].ts).toBeDefined();
      });

      await Promise.all(dataSources.map((ds) => ds.destroy()));
    });
  });

  describe('connection error handling', () => {
    it('should report connection error for invalid host', async () => {
      const ds = createDataSource({
        host: 'invalid-host-that-does-not-exist',
        connectTimeoutMS: 1000,
      });

      await expect(ds.initialize()).rejects.toThrow();
    });

    it('should report authentication error for invalid credentials', async () => {
      const ds = createDataSource({
        username: 'nonexistent_user',
        password: 'wrong_password',
      });

      await expect(ds.initialize()).rejects.toThrow();
    });
  });

  afterAll(async () => {
    await closeSharedDataSource();
  });
});
