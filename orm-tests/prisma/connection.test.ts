import { describe, it, expect, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { getConnectionString } from '../shared/config.js';

describe('Prisma Connection & Protocol Compatibility [pg-tikv]', () => {
  const clients: PrismaClient[] = [];

  afterAll(async () => {
    await Promise.all(clients.map((c) => c.$disconnect()));
  });

  describe('connection establishment', () => {
    it('should establish connection via pg driver', async () => {
      process.env.DATABASE_URL = getConnectionString();
      const prisma = new PrismaClient();
      clients.push(prisma);

      await prisma.$connect();
      const result = await prisma.$queryRaw`SELECT 1 as value`;
      expect(result).toHaveLength(1);
    });

    it('should execute raw query after connection', async () => {
      process.env.DATABASE_URL = getConnectionString();
      const prisma = new PrismaClient();
      clients.push(prisma);

      const result = await prisma.$queryRaw<{ value: number }[]>`SELECT 1 as value`;
      expect(result).toHaveLength(1);
      expect(result[0].value).toBe(1);
    });

    it('should handle multiple sequential connections', async () => {
      for (let i = 0; i < 3; i++) {
        process.env.DATABASE_URL = getConnectionString();
        const prisma = new PrismaClient();
        clients.push(prisma);

        const result = await prisma.$queryRaw<{ num: bigint }[]>`SELECT ${i + 1} as num`;
        expect(Number(result[0].num)).toBe(i + 1);
      }
    });
  });

  describe('connection pool', () => {
    it('should handle concurrent queries', async () => {
      process.env.DATABASE_URL = getConnectionString();
      const prisma = new PrismaClient({
        datasources: {
          db: {
            url: getConnectionString(),
          },
        },
      });
      clients.push(prisma);

      const queries = Array.from({ length: 10 }, (_, i) =>
        prisma.$queryRaw<{ num: bigint }[]>`SELECT ${i + 1} as num`
      );
      const results = await Promise.all(queries);

      expect(results).toHaveLength(10);
      results.forEach((r, i) => {
        expect(Number(r[0].num)).toBe(i + 1);
      });
    });
  });

  describe('connection error handling', () => {
    it('should report error for invalid credentials', async () => {
      const invalidUrl = 'postgresql://invalid:wrong@127.0.0.1:5433/postgres';
      process.env.DATABASE_URL = invalidUrl;
      const prisma = new PrismaClient({
        datasources: {
          db: { url: invalidUrl },
        },
      });

      await expect(prisma.$connect()).rejects.toThrow();
      await prisma.$disconnect().catch(() => {});
    });
  });
});
