import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { getPrismaClient, disconnectPrisma, cleanupPrismaTables } from './client.js';

describe('Prisma CRUD Semantics [pg-tikv]', () => {
  let prisma: PrismaClient;

  beforeAll(async () => {
    prisma = getPrismaClient();
    await prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS prisma_users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(100) NOT NULL,
        age INT DEFAULT 0,
        "isActive" BOOLEAN DEFAULT true,
        bio TEXT,
        metadata JSONB,
        "externalId" UUID,
        "createdAt" TIMESTAMPTZ DEFAULT NOW(),
        "updatedAt" TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS prisma_tags (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE NOT NULL,
        color VARCHAR(7) DEFAULT '#000000'
      )
    `);
    await prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS prisma_posts (
        id SERIAL PRIMARY KEY,
        title VARCHAR(500) NOT NULL,
        content TEXT NOT NULL,
        published BOOLEAN DEFAULT false,
        "viewCount" INT DEFAULT 0,
        settings JSONB,
        "createdAt" TIMESTAMPTZ DEFAULT NOW(),
        "authorId" INT NOT NULL REFERENCES prisma_users(id) ON DELETE CASCADE
      )
    `);
    await prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "_PrismaPostToPrismaTag" (
        "A" INT NOT NULL REFERENCES prisma_posts(id) ON DELETE CASCADE,
        "B" INT NOT NULL REFERENCES prisma_tags(id) ON DELETE CASCADE,
        PRIMARY KEY ("A", "B")
      )
    `);
  });

  afterAll(async () => {
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS "_PrismaPostToPrismaTag" CASCADE');
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS prisma_posts CASCADE');
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS prisma_tags CASCADE');
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS prisma_users CASCADE');
    await disconnectPrisma();
  });

  beforeEach(async () => {
    await prisma.$executeRawUnsafe('DELETE FROM "_PrismaPostToPrismaTag"');
    await prisma.$executeRawUnsafe('DELETE FROM prisma_posts');
    await prisma.$executeRawUnsafe('DELETE FROM prisma_tags');
    await prisma.$executeRawUnsafe('DELETE FROM prisma_users');
  });

  describe('INSERT operations', () => {
    it('should insert single row with create', async () => {
      const user = await prisma.prismaUser.create({
        data: {
          email: 'test@example.com',
          name: 'Test User',
          age: 25,
        },
      });

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('test@example.com');
      expect(user.name).toBe('Test User');
    });

    it('should insert batch rows with createMany', async () => {
      const result = await prisma.prismaUser.createMany({
        data: [
          { email: 'user1@example.com', name: 'User 1', age: 20 },
          { email: 'user2@example.com', name: 'User 2', age: 30 },
          { email: 'user3@example.com', name: 'User 3', age: 40 },
        ],
      });

      expect(result.count).toBe(3);
    });

    it('should apply default values', async () => {
      const user = await prisma.prismaUser.create({
        data: {
          email: 'defaults@example.com',
          name: 'Default Test',
        },
      });

      expect(user.age).toBe(0);
      expect(user.isActive).toBe(true);
    });

    it('should handle upsert', async () => {
      await prisma.prismaUser.create({
        data: {
          email: 'upsert@example.com',
          name: 'Original',
          age: 20,
        },
      });

      const upserted = await prisma.prismaUser.upsert({
        where: { email: 'upsert@example.com' },
        update: { name: 'Updated', age: 30 },
        create: { email: 'upsert@example.com', name: 'Created', age: 25 },
      });

      expect(upserted.name).toBe('Updated');
      expect(upserted.age).toBe(30);
    });

    it('should handle JSONB insert', async () => {
      const user = await prisma.prismaUser.create({
        data: {
          email: 'jsonb@example.com',
          name: 'JSONB Test',
          metadata: { role: 'admin', permissions: ['read', 'write'] },
        },
      });

      const found = await prisma.prismaUser.findUnique({ where: { id: user.id } });
      expect(found?.metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    });

    it('should handle UUID insert', async () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const user = await prisma.prismaUser.create({
        data: {
          email: 'uuid@example.com',
          name: 'UUID Test',
          externalId: uuid,
        },
      });

      const found = await prisma.prismaUser.findUnique({ where: { id: user.id } });
      expect(found?.externalId).toBe(uuid);
    });
  });

  describe('SELECT operations', () => {
    beforeEach(async () => {
      await prisma.prismaUser.createMany({
        data: [
          { email: 'alice@example.com', name: 'Alice', age: 25 },
          { email: 'bob@example.com', name: 'Bob', age: 30 },
          { email: 'charlie@example.com', name: 'Charlie', age: 35 },
          { email: 'diana@example.com', name: 'Diana', age: 25 },
          { email: 'eve@example.com', name: 'Eve', age: 40 },
        ],
      });
    });

    it('should select with WHERE condition', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { age: 25 },
      });

      expect(users).toHaveLength(2);
      users.forEach((u) => expect(u.age).toBe(25));
    });

    it('should select with take and skip (LIMIT/OFFSET)', async () => {
      const users = await prisma.prismaUser.findMany({
        orderBy: { name: 'asc' },
        skip: 1,
        take: 2,
      });

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Bob');
      expect(users[1].name).toBe('Charlie');
    });

    it('should select with orderBy', async () => {
      const users = await prisma.prismaUser.findMany({
        orderBy: [{ age: 'desc' }, { name: 'asc' }],
      });

      expect(users[0].age).toBe(40);
      expect(users[users.length - 1].age).toBe(25);
    });

    it('should select with count aggregate', async () => {
      const count = await prisma.prismaUser.count();
      expect(count).toBe(5);
    });

    it('should select with aggregate functions', async () => {
      const result = await prisma.prismaUser.aggregate({
        _sum: { age: true },
        _avg: { age: true },
        _min: { age: true },
        _max: { age: true },
      });

      expect(result._sum.age).toBe(155);
      expect(result._min.age).toBe(25);
      expect(result._max.age).toBe(40);
    });

    it('should select with groupBy', async () => {
      const results = await prisma.prismaUser.groupBy({
        by: ['age'],
        _count: true,
        orderBy: { age: 'asc' },
      });

      expect(results).toHaveLength(4);
      const age25 = results.find((r) => r.age === 25);
      expect(age25?._count).toBe(2);
    });

    it('should select specific fields', async () => {
      const users = await prisma.prismaUser.findMany({
        select: { id: true, name: true },
      });

      expect(users).toHaveLength(5);
      users.forEach((u) => {
        expect(u.id).toBeDefined();
        expect(u.name).toBeDefined();
        expect((u as Record<string, unknown>).email).toBeUndefined();
      });
    });
  });

  describe('UPDATE operations', () => {
    let userId: number;

    beforeEach(async () => {
      const user = await prisma.prismaUser.create({
        data: {
          email: 'update@example.com',
          name: 'Update Test',
          age: 25,
        },
      });
      userId = user.id;
    });

    it('should update partial fields', async () => {
      await prisma.prismaUser.update({
        where: { id: userId },
        data: { name: 'Updated Name' },
      });

      const user = await prisma.prismaUser.findUnique({ where: { id: userId } });
      expect(user?.name).toBe('Updated Name');
      expect(user?.age).toBe(25);
    });

    it('should update many with condition', async () => {
      await prisma.prismaUser.create({
        data: { email: 'another@example.com', name: 'Another', age: 25 },
      });

      const result = await prisma.prismaUser.updateMany({
        where: { age: 25 },
        data: { isActive: false },
      });

      expect(result.count).toBe(2);
    });

    it('should update JSONB field', async () => {
      await prisma.prismaUser.update({
        where: { id: userId },
        data: { metadata: { updated: true, timestamp: Date.now() } },
      });

      const user = await prisma.prismaUser.findUnique({ where: { id: userId } });
      expect((user?.metadata as Record<string, unknown>)?.updated).toBe(true);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await prisma.prismaUser.createMany({
        data: [
          { email: 'del1@example.com', name: 'Delete 1', age: 20 },
          { email: 'del2@example.com', name: 'Delete 2', age: 20 },
          { email: 'del3@example.com', name: 'Delete 3', age: 30 },
        ],
      });
    });

    it('should delete with condition', async () => {
      const result = await prisma.prismaUser.deleteMany({
        where: { age: 20 },
      });

      expect(result.count).toBe(2);

      const remaining = await prisma.prismaUser.count();
      expect(remaining).toBe(1);
    });

    it('should delete by id', async () => {
      const user = await prisma.prismaUser.findFirst({
        where: { email: 'del1@example.com' },
      });

      await prisma.prismaUser.delete({ where: { id: user!.id } });

      const found = await prisma.prismaUser.findUnique({ where: { id: user!.id } });
      expect(found).toBeNull();
    });

    it('should cascade delete related entities', async () => {
      const user = await prisma.prismaUser.create({
        data: {
          email: 'cascade@example.com',
          name: 'Cascade Test',
          age: 25,
        },
      });

      await prisma.prismaPost.create({
        data: {
          title: 'Test Post',
          content: 'Content',
          authorId: user.id,
        },
      });

      await prisma.prismaUser.delete({ where: { id: user.id } });

      const posts = await prisma.prismaPost.findMany({
        where: { authorId: user.id },
      });
      expect(posts).toHaveLength(0);
    });
  });
});
