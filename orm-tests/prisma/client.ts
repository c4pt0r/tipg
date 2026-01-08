import { PrismaClient } from '@prisma/client';
import { getConnectionString } from '../shared/config.js';

let prisma: PrismaClient | null = null;

export function getPrismaClient(): PrismaClient {
  if (!prisma) {
    process.env.DATABASE_URL = getConnectionString();
    prisma = new PrismaClient({
      log: process.env.DEBUG === 'true' ? ['query', 'error', 'warn'] : ['error'],
    });
  }
  return prisma;
}

export async function disconnectPrisma(): Promise<void> {
  if (prisma) {
    await prisma.$disconnect();
    prisma = null;
  }
}

export async function cleanupPrismaTables(client: PrismaClient): Promise<void> {
  await client.$executeRawUnsafe('DELETE FROM prisma_posts WHERE true');
  await client.$executeRawUnsafe('DELETE FROM prisma_tags WHERE true');
  await client.$executeRawUnsafe('DELETE FROM prisma_users WHERE true');
}
