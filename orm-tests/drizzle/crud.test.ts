import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { eq, and, or, gt, gte, lt, lte, ne, inArray, like, ilike, isNull, isNotNull, sql, count, sum, avg } from 'drizzle-orm';
import pg from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { createDrizzleClient, setupDrizzleTables, cleanupDrizzleTables } from './client.js';
import { drizzleUsers, drizzlePosts, drizzleTags } from './schema.js';
import * as schema from './schema.js';

const { Pool } = pg;

describe('Drizzle CRUD Semantics [pg-tikv]', () => {
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

  describe('INSERT operations', () => {
    it('should insert single row', async () => {
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'test@example.com', name: 'Test User', age: 25 })
        .returning();

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('test@example.com');
    });

    it('should insert batch rows', async () => {
      const users = await db
        .insert(drizzleUsers)
        .values([
          { email: 'user1@example.com', name: 'User 1', age: 20 },
          { email: 'user2@example.com', name: 'User 2', age: 30 },
          { email: 'user3@example.com', name: 'User 3', age: 40 },
        ])
        .returning();

      expect(users).toHaveLength(3);
      users.forEach((u, i) => {
        expect(u.id).toBeGreaterThan(0);
        expect(u.email).toBe(`user${i + 1}@example.com`);
      });
    });

    it('should return inserted row with RETURNING', async () => {
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'returning@example.com', name: 'Returning Test', age: 35 })
        .returning({ id: drizzleUsers.id, email: drizzleUsers.email, createdAt: drizzleUsers.createdAt });

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('returning@example.com');
      expect(user.createdAt).toBeDefined();
    });

    it('should apply default values', async () => {
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'defaults@example.com', name: 'Default Test' })
        .returning();

      expect(user.age).toBe(0);
      expect(user.isActive).toBe(true);
    });

    it('should handle ON CONFLICT (upsert)', async () => {
      await db
        .insert(drizzleUsers)
        .values({ email: 'upsert@example.com', name: 'Original', age: 20 });

      await db
        .insert(drizzleUsers)
        .values({ email: 'upsert@example.com', name: 'Updated', age: 30 })
        .onConflictDoUpdate({
          target: drizzleUsers.email,
          set: { name: sql`excluded.name`, age: sql`excluded.age` },
        });

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'upsert@example.com'));

      expect(user.name).toBe('Updated');
      expect(user.age).toBe(30);
    });

    it('should handle JSONB insert', async () => {
      const [user] = await db
        .insert(drizzleUsers)
        .values({
          email: 'jsonb@example.com',
          name: 'JSONB Test',
          metadata: { role: 'admin', permissions: ['read', 'write'] },
        })
        .returning();

      const [found] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.id, user.id));

      expect(found.metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    });

    it('should handle UUID insert', async () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'uuid@example.com', name: 'UUID Test', externalId: uuid })
        .returning();

      const [found] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.id, user.id));

      expect(found.externalId).toBe(uuid);
    });
  });

  describe('SELECT operations', () => {
    beforeEach(async () => {
      await db.insert(drizzleUsers).values([
        { email: 'alice@example.com', name: 'Alice', age: 25 },
        { email: 'bob@example.com', name: 'Bob', age: 30 },
        { email: 'charlie@example.com', name: 'Charlie', age: 35 },
        { email: 'diana@example.com', name: 'Diana', age: 25 },
        { email: 'eve@example.com', name: 'Eve', age: 40 },
      ]);
    });

    it('should select with WHERE condition', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.age, 25));

      expect(users).toHaveLength(2);
      users.forEach((u) => expect(u.age).toBe(25));
    });

    it('should select with LIMIT and OFFSET', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .orderBy(drizzleUsers.name)
        .offset(1)
        .limit(2);

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Bob');
      expect(users[1].name).toBe('Charlie');
    });

    it('should select with ORDER BY', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .orderBy(sql`${drizzleUsers.age} DESC`, drizzleUsers.name);

      expect(users[0].age).toBe(40);
      expect(users[users.length - 1].age).toBe(25);
    });

    it('should select with COUNT aggregate', async () => {
      const [result] = await db
        .select({ count: count() })
        .from(drizzleUsers);

      expect(Number(result.count)).toBe(5);
    });

    it('should select with SUM aggregate', async () => {
      const [result] = await db
        .select({ total: sum(drizzleUsers.age) })
        .from(drizzleUsers);

      expect(Number(result.total)).toBe(155);
    });

    it('should select with GROUP BY', async () => {
      const results = await db
        .select({ age: drizzleUsers.age, count: count() })
        .from(drizzleUsers)
        .groupBy(drizzleUsers.age)
        .orderBy(drizzleUsers.age);

      expect(results).toHaveLength(4);
      const age25 = results.find((r) => r.age === 25);
      expect(Number(age25?.count)).toBe(2);
    });

    it('should select specific columns', async () => {
      const users = await db
        .select({ id: drizzleUsers.id, name: drizzleUsers.name })
        .from(drizzleUsers);

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
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'update@example.com', name: 'Update Test', age: 25 })
        .returning();
      userId = user.id;
    });

    it('should update partial fields', async () => {
      await db
        .update(drizzleUsers)
        .set({ name: 'Updated Name' })
        .where(eq(drizzleUsers.id, userId));

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.id, userId));

      expect(user.name).toBe('Updated Name');
      expect(user.age).toBe(25);
    });

    it('should update with condition', async () => {
      await db.insert(drizzleUsers).values({
        email: 'another@example.com',
        name: 'Another',
        age: 25,
      });

      const result = await db
        .update(drizzleUsers)
        .set({ isActive: false })
        .where(eq(drizzleUsers.age, 25))
        .returning();

      expect(result).toHaveLength(2);
    });

    it('should update with RETURNING', async () => {
      const [updated] = await db
        .update(drizzleUsers)
        .set({ name: 'Returned Update' })
        .where(eq(drizzleUsers.id, userId))
        .returning({ id: drizzleUsers.id, name: drizzleUsers.name, updatedAt: drizzleUsers.updatedAt });

      expect(updated.name).toBe('Returned Update');
    });

    it('should update JSONB field', async () => {
      await db
        .update(drizzleUsers)
        .set({ metadata: { updated: true, timestamp: Date.now() } })
        .where(eq(drizzleUsers.id, userId));

      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.id, userId));

      expect((user.metadata as Record<string, unknown>)?.updated).toBe(true);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await db.insert(drizzleUsers).values([
        { email: 'del1@example.com', name: 'Delete 1', age: 20 },
        { email: 'del2@example.com', name: 'Delete 2', age: 20 },
        { email: 'del3@example.com', name: 'Delete 3', age: 30 },
      ]);
    });

    it('should delete with condition', async () => {
      const deleted = await db
        .delete(drizzleUsers)
        .where(eq(drizzleUsers.age, 20))
        .returning();

      expect(deleted).toHaveLength(2);

      const [remaining] = await db.select({ count: count() }).from(drizzleUsers);
      expect(Number(remaining.count)).toBe(1);
    });

    it('should delete by id', async () => {
      const [user] = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.email, 'del1@example.com'));

      await db.delete(drizzleUsers).where(eq(drizzleUsers.id, user.id));

      const found = await db
        .select()
        .from(drizzleUsers)
        .where(eq(drizzleUsers.id, user.id));

      expect(found).toHaveLength(0);
    });

    it('should cascade delete related entities', async () => {
      const [user] = await db
        .insert(drizzleUsers)
        .values({ email: 'cascade@example.com', name: 'Cascade Test', age: 25 })
        .returning();

      await db.insert(drizzlePosts).values({
        title: 'Test Post',
        content: 'Content',
        authorId: user.id,
      });

      await db.delete(drizzleUsers).where(eq(drizzleUsers.id, user.id));

      const posts = await db
        .select()
        .from(drizzlePosts)
        .where(eq(drizzlePosts.authorId, user.id));

      expect(posts).toHaveLength(0);
    });
  });

  describe('filter operations', () => {
    beforeEach(async () => {
      await db.insert(drizzleUsers).values([
        { email: 'alice@example.com', name: 'Alice', age: 25, bio: 'Developer' },
        { email: 'bob@example.com', name: 'Bob', age: 30, bio: 'Designer' },
        { email: 'charlie@example.com', name: 'CHARLIE', age: 35, bio: null },
      ]);
    });

    it('should handle gt, gte, lt, lte', async () => {
      const gtResult = await db.select().from(drizzleUsers).where(gt(drizzleUsers.age, 25));
      expect(gtResult).toHaveLength(2);

      const lteResult = await db.select().from(drizzleUsers).where(lte(drizzleUsers.age, 30));
      expect(lteResult).toHaveLength(2);
    });

    it('should handle inArray', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .where(inArray(drizzleUsers.name, ['Alice', 'Bob']));

      expect(users).toHaveLength(2);
    });

    it('should handle like', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .where(like(drizzleUsers.name, '%li%'));

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Alice');
    });

    it('should handle ilike', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .where(ilike(drizzleUsers.name, '%CHAR%'));

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle isNull and isNotNull', async () => {
      const nullBio = await db.select().from(drizzleUsers).where(isNull(drizzleUsers.bio));
      expect(nullBio).toHaveLength(1);

      const notNullBio = await db.select().from(drizzleUsers).where(isNotNull(drizzleUsers.bio));
      expect(notNullBio).toHaveLength(2);
    });

    it('should handle and and or', async () => {
      const users = await db
        .select()
        .from(drizzleUsers)
        .where(
          or(
            and(gte(drizzleUsers.age, 30), isNotNull(drizzleUsers.bio)),
            eq(drizzleUsers.name, 'Alice')
          )
        );

      expect(users).toHaveLength(2);
    });
  });
});
