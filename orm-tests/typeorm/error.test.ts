import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource, QueryFailedError } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User, Post } from './entities/index.js';

describe('TypeORM Error Semantics [pg-tikv]', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = createDataSource({ synchronize: true });
    await dataSource.initialize();
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.query('DROP TABLE IF EXISTS typeorm_post_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_posts CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_users CASCADE');
      await dataSource.destroy();
    }
  });

  beforeEach(async () => {
    await dataSource.query('DELETE FROM typeorm_post_tags');
    await dataSource.query('DELETE FROM typeorm_posts');
    await dataSource.query('DELETE FROM typeorm_tags');
    await dataSource.query('DELETE FROM typeorm_users');
  });

  describe('unique constraint violation', () => {
    it('should throw error on duplicate unique key', async () => {
      const userRepo = dataSource.getRepository(User);

      await userRepo.save({
        email: 'unique@example.com',
        name: 'First User',
        age: 25,
      });

      await expect(
        userRepo.save({
          email: 'unique@example.com',
          name: 'Second User',
          age: 30,
        })
      ).rejects.toThrow();
    });

    it('should have correct error type for unique violation', async () => {
      const userRepo = dataSource.getRepository(User);

      await userRepo.save({
        email: 'error@example.com',
        name: 'First User',
        age: 25,
      });

      try {
        await userRepo.save({
          email: 'error@example.com',
          name: 'Second User',
          age: 30,
        });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(QueryFailedError);
        const qfe = error as QueryFailedError;
        expect(qfe.driverError).toBeDefined();
      }
    });
  });

  describe('foreign key constraint violation', () => {
    it('should throw error on invalid foreign key', async () => {
      const postRepo = dataSource.getRepository(Post);

      await expect(
        postRepo.save({
          title: 'Orphan Post',
          content: 'Content',
          authorId: 99999,
        })
      ).rejects.toThrow();
    });
  });

  describe('NOT NULL constraint violation', () => {
    it('should throw error on NULL in NOT NULL column', async () => {
      await expect(
        dataSource.query(`
          INSERT INTO typeorm_users (email, name) VALUES (NULL, 'Test')
        `)
      ).rejects.toThrow();
    });
  });

  describe('SQL syntax errors', () => {
    it('should throw error on invalid SQL', async () => {
      await expect(
        dataSource.query('SELECT * FORM nonexistent_table')
      ).rejects.toThrow();
    });

    it('should throw error on invalid table', async () => {
      await expect(
        dataSource.query('SELECT * FROM nonexistent_table_xyz')
      ).rejects.toThrow();
    });

    it('should throw error on invalid column', async () => {
      await expect(
        dataSource.query('SELECT nonexistent_column FROM typeorm_users')
      ).rejects.toThrow();
    });
  });

  describe('data type errors', () => {
    it('should throw error on invalid integer', async () => {
      await expect(
        dataSource.query(`
          INSERT INTO typeorm_users (email, name, age) VALUES ('test@test.com', 'Test', 'not-a-number')
        `)
      ).rejects.toThrow();
    });

    it('should throw error on invalid UUID', async () => {
      await expect(
        dataSource.query(`
          INSERT INTO typeorm_users (email, name, age, "externalId") 
          VALUES ('uuid@test.com', 'Test', 25, 'not-a-uuid')
        `)
      ).rejects.toThrow();
    });

    it('should throw error on invalid JSON', async () => {
      await expect(
        dataSource.query(`
          INSERT INTO typeorm_users (email, name, age, metadata) 
          VALUES ('json@test.com', 'Test', 25, 'not-valid-json')
        `)
      ).rejects.toThrow();
    });
  });

  describe('error recovery', () => {
    it('should allow operations after error', async () => {
      const userRepo = dataSource.getRepository(User);

      await userRepo.save({
        email: 'first@example.com',
        name: 'First',
        age: 25,
      });

      try {
        await userRepo.save({
          email: 'first@example.com',
          name: 'Duplicate',
          age: 30,
        });
      } catch {
      }

      const user = await userRepo.save({
        email: 'second@example.com',
        name: 'Second',
        age: 35,
      });

      expect(user.id).toBeDefined();
      expect(user.email).toBe('second@example.com');
    });
  });
});
