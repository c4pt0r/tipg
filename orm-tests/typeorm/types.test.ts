import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User } from './entities/index.js';

describe('TypeORM TypeScript Type Fidelity [pg-tikv]', () => {
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
    await dataSource.query('DELETE FROM typeorm_users');
  });

  describe('number types', () => {
    it('should preserve integer precision', async () => {
      const userRepo = dataSource.getRepository(User);

      const saved = await userRepo.save({
        email: 'int@example.com',
        name: 'Int Test',
        age: 2147483647,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.age).toBe(2147483647);
      expect(typeof found?.age).toBe('number');
    });

    it('should return number type for id', async () => {
      const userRepo = dataSource.getRepository(User);

      const saved = await userRepo.save({
        email: 'id@example.com',
        name: 'ID Test',
        age: 25,
      });

      expect(typeof saved.id).toBe('number');
    });
  });

  describe('boolean types', () => {
    it('should preserve boolean true', async () => {
      const userRepo = dataSource.getRepository(User);

      const saved = await userRepo.save({
        email: 'bool1@example.com',
        name: 'Bool Test',
        age: 25,
        isActive: true,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.isActive).toBe(true);
      expect(typeof found?.isActive).toBe('boolean');
    });

    it('should preserve boolean false', async () => {
      const userRepo = dataSource.getRepository(User);

      const saved = await userRepo.save({
        email: 'bool2@example.com',
        name: 'Bool Test',
        age: 25,
        isActive: false,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.isActive).toBe(false);
      expect(typeof found?.isActive).toBe('boolean');
    });
  });

  describe('string types', () => {
    it('should preserve varchar', async () => {
      const userRepo = dataSource.getRepository(User);
      const longName = 'A'.repeat(100);

      const saved = await userRepo.save({
        email: 'varchar@example.com',
        name: longName,
        age: 25,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.name).toBe(longName);
      expect(found?.name.length).toBe(100);
    });

    it('should preserve text with unicode', async () => {
      const userRepo = dataSource.getRepository(User);
      const unicodeBio = '你好世界 🌍 مرحبا العالم';

      const saved = await userRepo.save({
        email: 'unicode@example.com',
        name: 'Unicode Test',
        age: 25,
        bio: unicodeBio,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.bio).toBe(unicodeBio);
    });
  });

  describe('Date types', () => {
    it('should preserve timestamp with timezone', async () => {
      const userRepo = dataSource.getRepository(User);
      const testDate = new Date('2024-06-15T10:30:00.000Z');

      const saved = await userRepo.save({
        email: 'date@example.com',
        name: 'Date Test',
        age: 25,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.createdAt).toBeInstanceOf(Date);
      expect(found?.updatedAt).toBeInstanceOf(Date);
    });

    it('should handle date comparison', async () => {
      const userRepo = dataSource.getRepository(User);

      await userRepo.save({
        email: 'datecomp@example.com',
        name: 'Date Comp Test',
        age: 25,
      });

      const now = new Date();
      const users = await userRepo
        .createQueryBuilder('user')
        .where('user.createdAt <= :now', { now })
        .getMany();

      expect(users.length).toBeGreaterThan(0);
    });
  });

  describe('JSON types', () => {
    it('should preserve object structure', async () => {
      const userRepo = dataSource.getRepository(User);
      const metadata = {
        settings: {
          theme: 'dark',
          notifications: true,
        },
        tags: ['premium', 'verified'],
        score: 95.5,
      };

      const saved = await userRepo.save({
        email: 'json@example.com',
        name: 'JSON Test',
        age: 25,
        metadata,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.metadata).toEqual(metadata);
    });

    it('should preserve array in JSON', async () => {
      const userRepo = dataSource.getRepository(User);
      const metadata = { items: [1, 2, 3, 4, 5] };

      const saved = await userRepo.save({
        email: 'jsonarray@example.com',
        name: 'JSON Array Test',
        age: 25,
        metadata,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.metadata?.items).toEqual([1, 2, 3, 4, 5]);
    });

    it('should preserve null in JSON', async () => {
      const userRepo = dataSource.getRepository(User);
      const metadata = { nullValue: null, nested: { also: null } };

      const saved = await userRepo.save({
        email: 'jsonnull@example.com',
        name: 'JSON Null Test',
        age: 25,
        metadata: metadata as Record<string, unknown>,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.metadata?.nullValue).toBeNull();
    });
  });

  describe('UUID types', () => {
    it('should preserve UUID format', async () => {
      const userRepo = dataSource.getRepository(User);
      const uuid = '550e8400-e29b-41d4-a716-446655440000';

      const saved = await userRepo.save({
        email: 'uuid@example.com',
        name: 'UUID Test',
        age: 25,
        externalId: uuid,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.externalId).toBe(uuid);
      expect(typeof found?.externalId).toBe('string');
    });
  });

  describe('nullable types', () => {
    it('should preserve null values', async () => {
      const userRepo = dataSource.getRepository(User);

      const saved = await userRepo.save({
        email: 'nullable@example.com',
        name: 'Nullable Test',
        age: 25,
        bio: null,
        metadata: null,
        externalId: null,
      });

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.bio).toBeNull();
      expect(found?.metadata).toBeNull();
      expect(found?.externalId).toBeNull();
    });
  });
});
