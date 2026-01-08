import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Sequelize, Op } from 'sequelize';
import { createSequelize } from './connection.js';
import { User, Post, Tag } from './models.js';

describe('Sequelize CRUD Semantics [pg-tikv]', () => {
  let sequelize: Sequelize;

  beforeAll(async () => {
    sequelize = createSequelize();
    await sequelize.sync({ force: true });
  });

  afterAll(async () => {
    await sequelize.drop();
    await sequelize.close();
  });

  beforeEach(async () => {
    await Post.destroy({ where: {}, force: true });
    await Tag.destroy({ where: {}, force: true });
    await User.destroy({ where: {}, force: true });
  });

  describe('INSERT operations', () => {
    it('should insert single row with create', async () => {
      const user = await User.create({
        email: 'test@example.com',
        name: 'Test User',
        age: 25,
      });

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('test@example.com');
    });

    it('should insert batch rows with bulkCreate', async () => {
      const users = await User.bulkCreate([
        { email: 'user1@example.com', name: 'User 1', age: 20 },
        { email: 'user2@example.com', name: 'User 2', age: 30 },
        { email: 'user3@example.com', name: 'User 3', age: 40 },
      ]);

      expect(users).toHaveLength(3);
      users.forEach((u, i) => {
        expect(u.id).toBeGreaterThan(0);
        expect(u.email).toBe(`user${i + 1}@example.com`);
      });
    });

    it('should apply default values', async () => {
      const user = await User.create({
        email: 'defaults@example.com',
        name: 'Default Test',
      });

      expect(user.age).toBe(0);
      expect(user.isActive).toBe(true);
    });

    it('should handle upsert', async () => {
      await User.create({
        email: 'upsert@example.com',
        name: 'Original',
        age: 20,
      });

      const [user, created] = await User.upsert({
        email: 'upsert@example.com',
        name: 'Updated',
        age: 30,
      });

      expect(created === false || created === null).toBe(true);
      expect(user.name).toBe('Updated');
      expect(user.age).toBe(30);
    });

    it('should handle JSONB insert', async () => {
      const user = await User.create({
        email: 'jsonb@example.com',
        name: 'JSONB Test',
        metadata: { role: 'admin', permissions: ['read', 'write'] },
      });

      const found = await User.findByPk(user.id);
      expect(found?.metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    });

    it('should handle UUID insert', async () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const user = await User.create({
        email: 'uuid@example.com',
        name: 'UUID Test',
        externalId: uuid,
      });

      const found = await User.findByPk(user.id);
      expect(found?.externalId).toBe(uuid);
    });
  });

  describe('SELECT operations', () => {
    beforeEach(async () => {
      await User.bulkCreate([
        { email: 'alice@example.com', name: 'Alice', age: 25 },
        { email: 'bob@example.com', name: 'Bob', age: 30 },
        { email: 'charlie@example.com', name: 'Charlie', age: 35 },
        { email: 'diana@example.com', name: 'Diana', age: 25 },
        { email: 'eve@example.com', name: 'Eve', age: 40 },
      ]);
    });

    it('should select with WHERE condition', async () => {
      const users = await User.findAll({ where: { age: 25 } });

      expect(users).toHaveLength(2);
      users.forEach((u) => expect(u.age).toBe(25));
    });

    it('should select with LIMIT and OFFSET', async () => {
      const users = await User.findAll({
        order: [['name', 'ASC']],
        offset: 1,
        limit: 2,
      });

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Bob');
      expect(users[1].name).toBe('Charlie');
    });

    it('should select with ORDER BY', async () => {
      const users = await User.findAll({
        order: [
          ['age', 'DESC'],
          ['name', 'ASC'],
        ],
      });

      expect(users[0].age).toBe(40);
      expect(users[users.length - 1].age).toBe(25);
    });

    it('should select with COUNT aggregate', async () => {
      const count = await User.count();
      expect(count).toBe(5);
    });

    it('should select with SUM aggregate', async () => {
      const sum = await User.sum('age');
      expect(sum).toBe(155);
    });

    it('should select with GROUP BY', async () => {
      const results = await User.findAll({
        attributes: ['age', [sequelize.fn('COUNT', '*'), 'count']],
        group: ['age'],
        order: [['age', 'ASC']],
        raw: true,
      });

      expect(results).toHaveLength(4);
      const age25 = results.find((r) => r.age === 25) as { age: number; count: string };
      expect(parseInt(age25.count, 10)).toBe(2);
    });

    it('should select specific attributes', async () => {
      const users = await User.findAll({
        attributes: ['id', 'name'],
      });

      expect(users).toHaveLength(5);
      users.forEach((u) => {
        expect(u.id).toBeDefined();
        expect(u.name).toBeDefined();
        expect(u.getDataValue('email')).toBeUndefined();
      });
    });
  });

  describe('UPDATE operations', () => {
    let userId: number;

    beforeEach(async () => {
      const user = await User.create({
        email: 'update@example.com',
        name: 'Update Test',
        age: 25,
      });
      userId = user.id;
    });

    it('should update partial fields', async () => {
      await User.update({ name: 'Updated Name' }, { where: { id: userId } });

      const user = await User.findByPk(userId);
      expect(user?.name).toBe('Updated Name');
      expect(user?.age).toBe(25);
    });

    it('should update with condition', async () => {
      await User.create({ email: 'another@example.com', name: 'Another', age: 25 });

      const [affected] = await User.update(
        { isActive: false },
        { where: { age: 25 } }
      );

      expect(affected).toBe(2);
    });

    it('should update instance', async () => {
      const user = await User.findByPk(userId);
      user!.name = 'Instance Updated';
      await user!.save();

      const found = await User.findByPk(userId);
      expect(found?.name).toBe('Instance Updated');
    });

    it('should update JSONB field', async () => {
      await User.update(
        { metadata: { updated: true, timestamp: Date.now() } },
        { where: { id: userId } }
      );

      const user = await User.findByPk(userId);
      expect(user?.metadata?.updated).toBe(true);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await User.bulkCreate([
        { email: 'del1@example.com', name: 'Delete 1', age: 20 },
        { email: 'del2@example.com', name: 'Delete 2', age: 20 },
        { email: 'del3@example.com', name: 'Delete 3', age: 30 },
      ]);
    });

    it('should delete with condition', async () => {
      const affected = await User.destroy({ where: { age: 20 } });

      expect(affected).toBe(2);

      const remaining = await User.count();
      expect(remaining).toBe(1);
    });

    it('should delete by id', async () => {
      const user = await User.findOne({ where: { email: 'del1@example.com' } });

      await user!.destroy();

      const found = await User.findByPk(user!.id);
      expect(found).toBeNull();
    });

    it('should cascade delete related entities', async () => {
      const user = await User.create({
        email: 'cascade@example.com',
        name: 'Cascade Test',
        age: 25,
      });

      await Post.create({
        title: 'Test Post',
        content: 'Content',
        authorId: user.id,
      });

      await user.destroy();

      const posts = await Post.findAll({ where: { authorId: user.id } });
      expect(posts).toHaveLength(0);
    });
  });

  describe('operator queries', () => {
    beforeEach(async () => {
      await User.bulkCreate([
        { email: 'op1@example.com', name: 'Alice', age: 25, bio: 'Developer' },
        { email: 'op2@example.com', name: 'Bob', age: 30, bio: 'Designer' },
        { email: 'op3@example.com', name: 'CHARLIE', age: 35, bio: null },
      ]);
    });

    it('should handle Op.gt, Op.gte, Op.lt, Op.lte', async () => {
      const gt = await User.findAll({ where: { age: { [Op.gt]: 25 } } });
      expect(gt).toHaveLength(2);

      const lte = await User.findAll({ where: { age: { [Op.lte]: 30 } } });
      expect(lte).toHaveLength(2);
    });

    it('should handle Op.in', async () => {
      const users = await User.findAll({
        where: { name: { [Op.in]: ['Alice', 'Bob'] } },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle Op.like', async () => {
      const users = await User.findAll({
        where: { name: { [Op.like]: '%li%' } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle Op.iLike', async () => {
      const users = await User.findAll({
        where: { name: { [Op.iLike]: '%CHAR%' } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle Op.is (NULL check)', async () => {
      const users = await User.findAll({
        where: { bio: { [Op.is]: null } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle Op.and and Op.or', async () => {
      const users = await User.findAll({
        where: {
          [Op.or]: [
            { [Op.and]: [{ age: { [Op.gte]: 30 } }, { bio: { [Op.not]: null } }] },
            { name: 'Alice' },
          ],
        },
      });
      expect(users).toHaveLength(2);
    });
  });
});
