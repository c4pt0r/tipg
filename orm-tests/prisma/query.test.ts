import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PrismaClient, Prisma } from '@prisma/client';
import { getPrismaClient, disconnectPrisma } from './client.js';

describe('Prisma Query Generation & SQL Compatibility [pg-tikv]', () => {
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
  });

  afterAll(async () => {
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS prisma_users CASCADE');
    await disconnectPrisma();
  });

  beforeEach(async () => {
    await prisma.$executeRawUnsafe('DELETE FROM prisma_users');
    await prisma.prismaUser.createMany({
      data: [
        { email: 'alice@example.com', name: 'Alice', age: 25, bio: 'Developer' },
        { email: 'bob@example.com', name: 'Bob', age: 30, bio: 'Designer' },
        { email: 'charlie@example.com', name: 'CHARLIE', age: 35, bio: null },
      ],
    });
  });

  describe('raw query with parameters', () => {
    it('should bind parameters in raw query', async () => {
      const minAge = 25;
      const maxAge = 35;
      const result = await prisma.$queryRaw<{ name: string }[]>`
        SELECT name FROM prisma_users WHERE age >= ${minAge} AND age < ${maxAge}
      `;
      expect(result).toHaveLength(2);
    });

    it('should handle Prisma.sql for dynamic queries', async () => {
      const column = 'age';
      const value = 30;
      const result = await prisma.$queryRaw<{ name: string }[]>(
        Prisma.sql`SELECT name FROM prisma_users WHERE ${Prisma.raw(column)} >= ${value}`
      );
      expect(result).toHaveLength(2);
    });
  });

  describe('filter operations', () => {
    it('should handle equals filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { age: { equals: 25 } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle not equals filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { age: { not: 25 } },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle gt/gte/lt/lte filters', async () => {
      const gt = await prisma.prismaUser.findMany({ where: { age: { gt: 25 } } });
      expect(gt).toHaveLength(2);

      const gte = await prisma.prismaUser.findMany({ where: { age: { gte: 25 } } });
      expect(gte).toHaveLength(3);

      const lt = await prisma.prismaUser.findMany({ where: { age: { lt: 35 } } });
      expect(lt).toHaveLength(2);

      const lte = await prisma.prismaUser.findMany({ where: { age: { lte: 35 } } });
      expect(lte).toHaveLength(3);
    });

    it('should handle in filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { in: ['Alice', 'Bob'] } },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle notIn filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { notIn: ['Alice', 'Bob'] } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle contains filter (LIKE)', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { contains: 'li' } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle contains with mode insensitive (ILIKE)', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { contains: 'CHAR', mode: 'insensitive' } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle startsWith filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { startsWith: 'Al' } },
      });
      expect(users).toHaveLength(1);
    });

    it('should handle endsWith filter', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { name: { endsWith: 'ob' } },
      });
      expect(users).toHaveLength(1);
    });
  });

  describe('logical operators', () => {
    it('should handle AND', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          AND: [{ age: { gte: 25 } }, { age: { lte: 30 } }],
        },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle OR', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          OR: [{ name: 'Alice' }, { name: 'Bob' }],
        },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle NOT', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          NOT: { name: 'Alice' },
        },
      });
      expect(users).toHaveLength(2);
    });

    it('should handle nested logical operators', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          OR: [
            { AND: [{ age: { gte: 30 } }, { bio: { not: null } }] },
            { name: 'Alice' },
          ],
        },
      });
      expect(users).toHaveLength(2);
    });
  });

  describe('NULL handling', () => {
    it('should handle IS NULL', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { bio: null },
      });
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle IS NOT NULL', async () => {
      const users = await prisma.prismaUser.findMany({
        where: { bio: { not: null } },
      });
      expect(users).toHaveLength(2);
    });
  });

  describe('JSONB operations', () => {
    beforeEach(async () => {
      await prisma.prismaUser.update({
        where: { email: 'alice@example.com' },
        data: { metadata: { role: 'admin', level: 5, tags: ['dev', 'lead'] } },
      });
      await prisma.prismaUser.update({
        where: { email: 'bob@example.com' },
        data: { metadata: { role: 'user', level: 2, tags: ['design'] } },
      });
    });

    it('should filter by JSON path', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          metadata: {
            path: ['role'],
            equals: 'admin',
          },
        },
      });
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Alice');
    });

    it('should filter by nested JSON path', async () => {
      const users = await prisma.prismaUser.findMany({
        where: {
          metadata: {
            path: ['level'],
            gte: 3,
          },
        },
      });
      expect(users).toHaveLength(1);
    });
  });

  describe('distinct', () => {
    beforeEach(async () => {
      await prisma.prismaUser.create({
        data: { email: 'diana@example.com', name: 'Diana', age: 40 },
      });
    });

    it('should handle distinct', async () => {
      const ages = await prisma.prismaUser.findMany({
        distinct: ['age'],
        select: { age: true },
        orderBy: { age: 'asc' },
      });
      expect(ages).toHaveLength(4);
    });
  });

  describe('cursor pagination', () => {
    it('should handle cursor-based pagination', async () => {
      const firstPage = await prisma.prismaUser.findMany({
        take: 2,
        orderBy: { id: 'asc' },
      });

      expect(firstPage).toHaveLength(2);

      const secondPage = await prisma.prismaUser.findMany({
        take: 2,
        skip: 1,
        cursor: { id: firstPage[1].id },
        orderBy: { id: 'asc' },
      });

      expect(secondPage).toHaveLength(1);
      expect(secondPage[0].id).toBeGreaterThan(firstPage[1].id);
    });
  });
});
