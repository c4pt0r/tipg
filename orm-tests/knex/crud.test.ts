import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Knex } from 'knex';
import { createKnexClient, setupKnexTables, cleanupKnexTables } from './client.js';

describe('Knex CRUD Semantics [pg-tikv]', () => {
  let db: Knex;

  beforeAll(async () => {
    db = createKnexClient();
    await setupKnexTables(db);
  });

  afterAll(async () => {
    await db.schema.dropTableIfExists('knex_post_tags');
    await db.schema.dropTableIfExists('knex_posts');
    await db.schema.dropTableIfExists('knex_tags');
    await db.schema.dropTableIfExists('knex_users');
    await db.destroy();
  });

  beforeEach(async () => {
    await cleanupKnexTables(db);
  });

  describe('INSERT operations', () => {
    it('should insert single row', async () => {
      const [user] = await db('knex_users')
        .insert({ email: 'test@example.com', name: 'Test User', age: 25 })
        .returning('*');

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('test@example.com');
    });

    it('should insert batch rows', async () => {
      const users = await db('knex_users')
        .insert([
          { email: 'user1@example.com', name: 'User 1', age: 20 },
          { email: 'user2@example.com', name: 'User 2', age: 30 },
          { email: 'user3@example.com', name: 'User 3', age: 40 },
        ])
        .returning('*');

      expect(users).toHaveLength(3);
      users.forEach((u, i) => {
        expect(u.id).toBeGreaterThan(0);
        expect(u.email).toBe(`user${i + 1}@example.com`);
      });
    });

    it('should return inserted row with RETURNING', async () => {
      const [user] = await db('knex_users')
        .insert({ email: 'returning@example.com', name: 'Returning Test', age: 35 })
        .returning(['id', 'email', 'created_at']);

      expect(user.id).toBeGreaterThan(0);
      expect(user.email).toBe('returning@example.com');
      expect(user.created_at).toBeDefined();
    });

    it('should apply default values', async () => {
      const [user] = await db('knex_users')
        .insert({ email: 'defaults@example.com', name: 'Default Test' })
        .returning('*');

      expect(user.age).toBe(0);
      expect(user.is_active).toBe(true);
    });

    it('should handle ON CONFLICT (upsert)', async () => {
      await db('knex_users').insert({
        email: 'upsert@example.com',
        name: 'Original',
        age: 20,
      });

      await db('knex_users')
        .insert({ email: 'upsert@example.com', name: 'Updated', age: 30 })
        .onConflict('email')
        .merge(['name', 'age']);

      const user = await db('knex_users').where({ email: 'upsert@example.com' }).first();
      expect(user.name).toBe('Updated');
      expect(user.age).toBe(30);
    });

    it('should handle JSONB insert', async () => {
      const [user] = await db('knex_users')
        .insert({
          email: 'jsonb@example.com',
          name: 'JSONB Test',
          metadata: JSON.stringify({ role: 'admin', permissions: ['read', 'write'] }),
        })
        .returning('*');

      const found = await db('knex_users').where({ id: user.id }).first();
      expect(found.metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    });

    it('should handle UUID insert', async () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      const [user] = await db('knex_users')
        .insert({ email: 'uuid@example.com', name: 'UUID Test', external_id: uuid })
        .returning('*');

      const found = await db('knex_users').where({ id: user.id }).first();
      expect(found.external_id).toBe(uuid);
    });
  });

  describe('SELECT operations', () => {
    beforeEach(async () => {
      await db('knex_users').insert([
        { email: 'alice@example.com', name: 'Alice', age: 25 },
        { email: 'bob@example.com', name: 'Bob', age: 30 },
        { email: 'charlie@example.com', name: 'Charlie', age: 35 },
        { email: 'diana@example.com', name: 'Diana', age: 25 },
        { email: 'eve@example.com', name: 'Eve', age: 40 },
      ]);
    });

    it('should select with WHERE condition', async () => {
      const users = await db('knex_users').where({ age: 25 });

      expect(users).toHaveLength(2);
      users.forEach((u) => expect(u.age).toBe(25));
    });

    it('should select with LIMIT and OFFSET', async () => {
      const users = await db('knex_users')
        .orderBy('name', 'asc')
        .offset(1)
        .limit(2);

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Bob');
      expect(users[1].name).toBe('Charlie');
    });

    it('should select with ORDER BY', async () => {
      const users = await db('knex_users')
        .orderBy([
          { column: 'age', order: 'desc' },
          { column: 'name', order: 'asc' },
        ]);

      expect(users[0].age).toBe(40);
      expect(users[users.length - 1].age).toBe(25);
    });

    it('should select with COUNT aggregate', async () => {
      const [{ count }] = await db('knex_users').count('* as count');
      expect(parseInt(count as string, 10)).toBe(5);
    });

    it('should select with SUM aggregate', async () => {
      const [{ sum }] = await db('knex_users').sum('age as sum');
      expect(parseInt(sum as string, 10)).toBe(155);
    });

    it('should select with GROUP BY', async () => {
      const results = await db('knex_users')
        .select('age')
        .count('* as count')
        .groupBy('age')
        .orderBy('age', 'asc');

      expect(results).toHaveLength(4);
      const age25 = results.find((r) => r.age === 25);
      expect(parseInt(age25.count as string, 10)).toBe(2);
    });

    it('should select specific columns', async () => {
      const users = await db('knex_users').select('id', 'name');

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
      const [user] = await db('knex_users')
        .insert({ email: 'update@example.com', name: 'Update Test', age: 25 })
        .returning('*');
      userId = user.id;
    });

    it('should update partial fields', async () => {
      await db('knex_users').where({ id: userId }).update({ name: 'Updated Name' });

      const user = await db('knex_users').where({ id: userId }).first();
      expect(user.name).toBe('Updated Name');
      expect(user.age).toBe(25);
    });

    it('should update with condition', async () => {
      await db('knex_users').insert({ email: 'another@example.com', name: 'Another', age: 25 });

      const affected = await db('knex_users').where({ age: 25 }).update({ is_active: false });

      expect(affected).toBe(2);
    });

    it('should update with RETURNING', async () => {
      const [updated] = await db('knex_users')
        .where({ id: userId })
        .update({ name: 'Returned Update' })
        .returning(['id', 'name', 'updated_at']);

      expect(updated.name).toBe('Returned Update');
    });

    it('should update JSONB field', async () => {
      await db('knex_users')
        .where({ id: userId })
        .update({ metadata: JSON.stringify({ updated: true, timestamp: Date.now() }) });

      const user = await db('knex_users').where({ id: userId }).first();
      expect(user.metadata.updated).toBe(true);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await db('knex_users').insert([
        { email: 'del1@example.com', name: 'Delete 1', age: 20 },
        { email: 'del2@example.com', name: 'Delete 2', age: 20 },
        { email: 'del3@example.com', name: 'Delete 3', age: 30 },
      ]);
    });

    it('should delete with condition', async () => {
      const affected = await db('knex_users').where({ age: 20 }).del();

      expect(affected).toBe(2);

      const remaining = await db('knex_users').count('* as count');
      expect(parseInt(remaining[0].count as string, 10)).toBe(1);
    });

    it('should delete by id', async () => {
      const user = await db('knex_users').where({ email: 'del1@example.com' }).first();

      await db('knex_users').where({ id: user.id }).del();

      const found = await db('knex_users').where({ id: user.id }).first();
      expect(found).toBeUndefined();
    });

    it('should cascade delete related entities', async () => {
      const [user] = await db('knex_users')
        .insert({ email: 'cascade@example.com', name: 'Cascade Test', age: 25 })
        .returning('*');

      await db('knex_posts').insert({
        title: 'Test Post',
        content: 'Content',
        author_id: user.id,
      });

      await db('knex_users').where({ id: user.id }).del();

      const posts = await db('knex_posts').where({ author_id: user.id });
      expect(posts).toHaveLength(0);
    });
  });
});
