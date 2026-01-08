import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { getPrismaClient, disconnectPrisma } from './client.js';

describe('Prisma Transactions & Isolation [pg-tikv]', () => {
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
  });

  describe('interactive transactions', () => {
    it('should commit interactive transaction', async () => {
      await prisma.$transaction(async (tx) => {
        await tx.prismaUser.create({
          data: {
            email: 'txn@example.com',
            name: 'Transaction User',
            age: 30,
          },
        });
      });

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'txn@example.com' },
      });
      expect(user).not.toBeNull();
      expect(user?.name).toBe('Transaction User');
    });

    it('should rollback interactive transaction on error', async () => {
      try {
        await prisma.$transaction(async (tx) => {
          await tx.prismaUser.create({
            data: {
              email: 'rollback@example.com',
              name: 'Rollback User',
              age: 30,
            },
          });
          throw new Error('Intentional error');
        });
      } catch {
      }

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'rollback@example.com' },
      });
      expect(user).toBeNull();
    });

    it('should handle multiple operations in transaction', async () => {
      await prisma.$transaction(async (tx) => {
        const user1 = await tx.prismaUser.create({
          data: {
            email: 'multi1@example.com',
            name: 'Multi User 1',
            age: 25,
          },
        });

        await tx.prismaUser.create({
          data: {
            email: 'multi2@example.com',
            name: 'Multi User 2',
            age: 30,
          },
        });

        await tx.prismaUser.update({
          where: { id: user1.id },
          data: { age: 26 },
        });
      });

      const users = await prisma.prismaUser.findMany();
      expect(users).toHaveLength(2);
    });
  });

  describe('batch transactions', () => {
    it('should execute batch transaction', async () => {
      const [user1, user2] = await prisma.$transaction([
        prisma.prismaUser.create({
          data: { email: 'batch1@example.com', name: 'Batch 1', age: 25 },
        }),
        prisma.prismaUser.create({
          data: { email: 'batch2@example.com', name: 'Batch 2', age: 30 },
        }),
      ]);

      expect(user1.email).toBe('batch1@example.com');
      expect(user2.email).toBe('batch2@example.com');

      const count = await prisma.prismaUser.count();
      expect(count).toBe(2);
    });

    it('should rollback batch transaction on any failure', async () => {
      try {
        await prisma.$transaction([
          prisma.prismaUser.create({
            data: { email: 'batchfail1@example.com', name: 'Batch Fail 1', age: 25 },
          }),
          prisma.prismaUser.create({
            data: { email: 'batchfail1@example.com', name: 'Duplicate', age: 30 },
          }),
        ]);
      } catch {
      }

      const count = await prisma.prismaUser.count();
      expect(count).toBe(0);
    });
  });

  describe('transaction options', () => {
    it('should handle transaction with timeout', async () => {
      await prisma.$transaction(
        async (tx) => {
          await tx.prismaUser.create({
            data: { email: 'timeout@example.com', name: 'Timeout Test', age: 25 },
          });
        },
        { timeout: 10000 }
      );

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'timeout@example.com' },
      });
      expect(user).not.toBeNull();
    });

    it('should handle transaction with max wait', async () => {
      await prisma.$transaction(
        async (tx) => {
          await tx.prismaUser.create({
            data: { email: 'maxwait@example.com', name: 'Max Wait Test', age: 25 },
          });
        },
        { maxWait: 5000 }
      );

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'maxwait@example.com' },
      });
      expect(user).not.toBeNull();
    });

    it('should handle READ COMMITTED isolation', async () => {
      await prisma.$transaction(
        async (tx) => {
          await tx.prismaUser.create({
            data: { email: 'readcommit@example.com', name: 'Read Commit', age: 25 },
          });
        },
        { isolationLevel: 'ReadCommitted' }
      );

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'readcommit@example.com' },
      });
      expect(user).not.toBeNull();
    });

    it('should handle REPEATABLE READ isolation', async () => {
      await prisma.$transaction(
        async (tx) => {
          await tx.prismaUser.create({
            data: { email: 'repeatread@example.com', name: 'Repeat Read', age: 25 },
          });
        },
        { isolationLevel: 'RepeatableRead' }
      );

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'repeatread@example.com' },
      });
      expect(user).not.toBeNull();
    });

    it('should handle SERIALIZABLE isolation', async () => {
      await prisma.$transaction(
        async (tx) => {
          await tx.prismaUser.create({
            data: { email: 'serial@example.com', name: 'Serializable', age: 25 },
          });
        },
        { isolationLevel: 'Serializable' }
      );

      const user = await prisma.prismaUser.findUnique({
        where: { email: 'serial@example.com' },
      });
      expect(user).not.toBeNull();
    });
  });

  describe('nested writes (implicit transactions)', () => {
    it('should handle nested create', async () => {
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

      const user = await prisma.prismaUser.create({
        data: {
          email: 'nested@example.com',
          name: 'Nested Create',
          age: 30,
          posts: {
            create: [
              { title: 'Post 1', content: 'Content 1' },
              { title: 'Post 2', content: 'Content 2' },
            ],
          },
        },
        include: { posts: true },
      });

      expect(user.posts).toHaveLength(2);

      await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS prisma_posts CASCADE');
    });
  });
});
