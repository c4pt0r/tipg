import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { eq, sql } from 'drizzle-orm';
import pg from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { createDrizzleClient, setupDrizzleTables, cleanupDrizzleTables } from './client.js';
import { drizzleUsers } from './schema.js';
import * as schema from './schema.js';

describe('Drizzle Transactions & Isolation [pg-tikv]', () => {
  let pool: pg.Pool;
  let db: ReturnType<typeof drizzle<typeof schema>>;

  beforeAll(async () => {
    const client = createDrizzleClient();
    pool = client.pool;
    db = client.db;
    await setupDrizzleTables(pool);
  });

  afterAll(async () => {
    const client = await pool.connect();
    try {
      await client.query('DROP TABLE IF EXISTS drizzle_post_tags CASCADE');
      await client.query('DROP TABLE IF EXISTS drizzle_posts CASCADE');
      await client.query('DROP TABLE IF EXISTS drizzle_tags CASCADE');
      await client.query('DROP TABLE IF EXISTS drizzle_users CASCADE');
    } finally {
      client.release();
    }
    await pool.end();
  });

  beforeEach(async () => {
    await cleanupDrizzleTables(pool);
  });

  describe('transaction callback', () => {
    it('should commit transaction', async () => {
      await db.transaction(async (tx) => {
        await tx.insert(drizzleUsers).values({
          email: 'txn@example.com',
          name: 'Transaction User',
          age: 30,
        });
      });

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'txn@example.com'));

      expect(user).not.toBeUndefined();
      expect(user.name).toBe('Transaction User');
    });

    it('should rollback transaction on error', async () => {
      try {
        await db.transaction(async (tx) => {
          await tx.insert(drizzleUsers).values({
            email: 'rollback@example.com',
            name: 'Rollback User',
            age: 30,
          });
          throw new Error('Intentional error');
        });
      } catch {
      }

      const users = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'rollback@example.com'));

      expect(users).toHaveLength(0);
    });

    it('should handle multiple operations in transaction', async () => {
      await db.transaction(async (tx) => {
        const [user1] = await tx
          .insert(drizzleUsers)
          .values({ email: 'multi1@example.com', name: 'Multi User 1', age: 25 })
          .returning();

        await tx.insert(drizzleUsers).values({
          email: 'multi2@example.com',
          name: 'Multi User 2',
          age: 30,
        });

        await tx
          .update(drizzleUsers)
          .set({ age: 26 })
          .where(eq(drizzleUsers.id, user1.id));
      });

      const users = await db.select().from(drizzleUsers);
      expect(users).toHaveLength(2);
    });
  });

  describe('nested transactions (savepoints)', () => {
    it('should handle nested transaction', async () => {
      await db.transaction(async (tx) => {
        await tx.insert(drizzleUsers).values({
          email: 'outer@example.com',
          name: 'Outer User',
          age: 30,
        });

        try {
          await tx.transaction(async (nested) => {
            await nested.insert(drizzleUsers).values({
              email: 'inner@example.com',
              name: 'Inner User',
              age: 25,
            });
            throw new Error('Rollback nested');
          });
        } catch {
        }
      });

      const outer = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'outer@example.com'));

      const inner = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'inner@example.com'));

      expect(outer).toHaveLength(1);
      expect(inner).toHaveLength(0);
    });
  });

  describe('isolation levels', () => {
    it('should handle read committed isolation', async () => {
      await db.transaction(
        async (tx) => {
          await tx.insert(drizzleUsers).values({
            email: 'readcommit@example.com',
            name: 'Read Commit',
            age: 25,
          });
        },
        { isolationLevel: 'read committed' }
      );

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'readcommit@example.com'));

      expect(user).not.toBeUndefined();
    });

    it('should handle repeatable read isolation', async () => {
      await db.insert(drizzleUsers).values({
        email: 'rr@example.com',
        name: 'RR User',
        age: 30,
      });

      await db.transaction(
        async (tx) => {
          const [first] = await tx
            .select()
            .from(drizzleUsers)
            .where(eq(drizzleUsers.email, 'rr@example.com'));

          await db
            .update(drizzleUsers)
            .set({ age: 40 })
            .where(eq(drizzleUsers.email, 'rr@example.com'));

          const [second] = await tx
            .select()
            .from(drizzleUsers)
            .where(eq(drizzleUsers.email, 'rr@example.com'));

          expect(first.age).toBe(second.age);
        },
        { isolationLevel: 'repeatable read' }
      );
    });

    it('should handle serializable isolation', async () => {
      await db.transaction(
        async (tx) => {
          await tx.insert(drizzleUsers).values({
            email: 'serial@example.com',
            name: 'Serializable',
            age: 25,
          });
        },
        { isolationLevel: 'serializable' }
      );

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'serial@example.com'));

      expect(user).not.toBeUndefined();
    });
  });

  describe('transaction visibility', () => {
    it('should isolate uncommitted changes', async () => {
      const pgClient = await pool.connect();

      try {
        await pgClient.query('BEGIN');

        await pgClient.query(
          `INSERT INTO drizzle_users (email, name, age) VALUES ('visibility@example.com', 'Visibility Test', 30)`
        );

        const outsideView = await db
          .select()
          .from(drizzleUsers)
          .where(eq(drizzleUsers.email, 'visibility@example.com'));

        expect(outsideView).toHaveLength(0);

        await pgClient.query('COMMIT');

        const afterCommit = await db
          .select()
          .from(drizzleUsers)
          .where(eq(drizzleUsers.email, 'visibility@example.com'));

        expect(afterCommit).toHaveLength(1);
      } finally {
        pgClient.release();
      }
    });
  });
});
