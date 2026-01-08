import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User, Post, Tag } from './entities/index.js';

describe('TypeORM CRUD Semantics [pg-tikv]', () => {
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

  describe('INSERT operations', () => {
    it('should insert single row', async () => {
      const userRepo = dataSource.getRepository(User);
      const user = userRepo.create({
        email: 'test@example.com',
        name: 'Test User',
        age: 25,
      });
      const saved = await userRepo.save(user);

      expect(saved.id).toBeDefined();
      expect(saved.id).toBeGreaterThan(0);
      expect(saved.email).toBe('test@example.com');
    });

    it('should insert batch rows', async () => {
      const userRepo = dataSource.getRepository(User);
      const users = [
        { email: 'user1@example.com', name: 'User 1', age: 20 },
        { email: 'user2@example.com', name: 'User 2', age: 30 },
        { email: 'user3@example.com', name: 'User 3', age: 40 },
      ];

      const saved = await userRepo.save(users.map((u) => userRepo.create(u)));

      expect(saved).toHaveLength(3);
      saved.forEach((u, i) => {
        expect(u.id).toBeGreaterThan(0);
        expect(u.email).toBe(users[i].email);
      });
    });

    it('should return inserted row with RETURNING', async () => {
      const result = await dataSource
        .createQueryBuilder()
        .insert()
        .into(User)
        .values({
          email: 'returning@example.com',
          name: 'Returning Test',
          age: 35,
        })
        .returning(['id', 'email', 'createdAt'])
        .execute();

      expect(result.raw).toHaveLength(1);
      expect(result.raw[0].id).toBeGreaterThan(0);
      expect(result.raw[0].email).toBe('returning@example.com');
      expect(result.raw[0].createdAt || result.raw[0].createdat).toBeDefined();
    });

    it('should apply default values', async () => {
      const userRepo = dataSource.getRepository(User);
      const user = userRepo.create({
        email: 'defaults@example.com',
        name: 'Default Test',
      });
      const saved = await userRepo.save(user);

      expect(saved.age).toBe(0);
      expect(saved.isActive).toBe(true);
    });

    it('should handle ON CONFLICT (upsert)', async () => {
      const userRepo = dataSource.getRepository(User);
      
      await userRepo.save({
        email: 'upsert@example.com',
        name: 'Original',
        age: 20,
      });

      await dataSource
        .createQueryBuilder()
        .insert()
        .into(User)
        .values({
          email: 'upsert@example.com',
          name: 'Updated',
          age: 30,
        })
        .orUpdate(['name', 'age'], ['email'])
        .execute();

      const user = await userRepo.findOneBy({ email: 'upsert@example.com' });
      expect(user?.name).toBe('Updated');
      expect(user?.age).toBe(30);
    });

    it('should handle JSONB insert', async () => {
      const userRepo = dataSource.getRepository(User);
      const user = userRepo.create({
        email: 'jsonb@example.com',
        name: 'JSONB Test',
        metadata: { role: 'admin', permissions: ['read', 'write'] },
      });
      const saved = await userRepo.save(user);

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    });

    it('should handle UUID insert', async () => {
      const userRepo = dataSource.getRepository(User);
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const user = userRepo.create({
        email: 'uuid@example.com',
        name: 'UUID Test',
        externalId: uuid,
      });
      const saved = await userRepo.save(user);

      const found = await userRepo.findOneBy({ id: saved.id });
      expect(found?.externalId).toBe(uuid);
    });
  });

  describe('SELECT operations', () => {
    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save([
        { email: 'alice@example.com', name: 'Alice', age: 25 },
        { email: 'bob@example.com', name: 'Bob', age: 30 },
        { email: 'charlie@example.com', name: 'Charlie', age: 35 },
        { email: 'diana@example.com', name: 'Diana', age: 25 },
        { email: 'eve@example.com', name: 'Eve', age: 40 },
      ]);
    });

    it('should select with WHERE condition', async () => {
      const userRepo = dataSource.getRepository(User);
      const users = await userRepo.findBy({ age: 25 });

      expect(users).toHaveLength(2);
      users.forEach((u) => expect(u.age).toBe(25));
    });

    it('should select with LIMIT and OFFSET', async () => {
      const userRepo = dataSource.getRepository(User);
      const users = await userRepo.find({
        order: { name: 'ASC' },
        skip: 1,
        take: 2,
      });

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Bob');
      expect(users[1].name).toBe('Charlie');
    });

    it('should select with ORDER BY', async () => {
      const userRepo = dataSource.getRepository(User);
      const users = await userRepo.find({
        order: { age: 'DESC', name: 'ASC' },
      });

      expect(users[0].age).toBe(40);
      expect(users[users.length - 1].age).toBe(25);
    });

    it('should select with COUNT aggregate', async () => {
      const count = await dataSource.getRepository(User).count();
      expect(count).toBe(5);
    });

    it('should select with SUM aggregate', async () => {
      const result = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .select('SUM(user.age)', 'totalAge')
        .getRawOne();

      expect(parseInt(result.totalAge, 10)).toBe(155);
    });

    it('should select with GROUP BY', async () => {
      const results = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .select('user.age', 'age')
        .addSelect('COUNT(*)', 'count')
        .groupBy('user.age')
        .orderBy('user.age', 'ASC')
        .getRawMany();

      expect(results).toHaveLength(4);
      const age25 = results.find((r) => parseInt(r.age, 10) === 25);
      expect(parseInt(age25.count, 10)).toBe(2);
    });

    it('should select specific columns', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .select(['user.id', 'user.name'])
        .getMany();

      expect(users).toHaveLength(5);
      users.forEach((u) => {
        expect(u.id).toBeDefined();
        expect(u.name).toBeDefined();
        expect(u.email).toBeUndefined();
      });
    });
  });

  describe('UPDATE operations', () => {
    let userId: number;

    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      const user = await userRepo.save({
        email: 'update@example.com',
        name: 'Update Test',
        age: 25,
      });
      userId = user.id;
    });

    it('should update partial fields', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.update(userId, { name: 'Updated Name' });

      const user = await userRepo.findOneBy({ id: userId });
      expect(user?.name).toBe('Updated Name');
      expect(user?.age).toBe(25);
    });

    it('should update with condition', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save({ email: 'another@example.com', name: 'Another', age: 25 });

      const result = await userRepo.update({ age: 25 }, { isActive: false });

      expect(result.affected).toBe(2);
    });

    it('should update with RETURNING', async () => {
      const result = await dataSource
        .createQueryBuilder()
        .update(User)
        .set({ name: 'Returned Update' })
        .where('id = :id', { id: userId })
        .returning(['id', 'name', 'updatedAt'])
        .execute();

      expect(result.raw).toHaveLength(1);
      expect(result.raw[0].name).toBe('Returned Update');
    });

    it('should update JSONB field', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.update(userId, {
        metadata: { updated: true, timestamp: Date.now() },
      });

      const user = await userRepo.findOneBy({ id: userId });
      expect(user?.metadata?.updated).toBe(true);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save([
        { email: 'del1@example.com', name: 'Delete 1', age: 20 },
        { email: 'del2@example.com', name: 'Delete 2', age: 20 },
        { email: 'del3@example.com', name: 'Delete 3', age: 30 },
      ]);
    });

    it('should delete with condition', async () => {
      const userRepo = dataSource.getRepository(User);
      const result = await userRepo.delete({ age: 20 });

      expect(result.affected).toBe(2);

      const remaining = await userRepo.count();
      expect(remaining).toBe(1);
    });

    it('should delete by id', async () => {
      const userRepo = dataSource.getRepository(User);
      const user = await userRepo.findOneBy({ email: 'del1@example.com' });

      await userRepo.delete(user!.id);

      const found = await userRepo.findOneBy({ id: user!.id });
      expect(found).toBeNull();
    });

    it('should cascade delete related entities', async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);

      const user = await userRepo.save({
        email: 'cascade@example.com',
        name: 'Cascade Test',
        age: 25,
      });

      await postRepo.save({
        title: 'Test Post',
        content: 'Content',
        authorId: user.id,
      });

      await userRepo.delete(user.id);

      const posts = await postRepo.findBy({ authorId: user.id });
      expect(posts).toHaveLength(0);
    });
  });
});
