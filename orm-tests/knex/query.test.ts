import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Knex } from 'knex';
import { createKnexClient, setupKnexTables, cleanupKnexTables } from './client.js';

describe('Knex Query Generation & SQL Compatibility [pg-tikv]', () => {
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
    await db('knex_users').insert([
      { email: 'alice@example.com', name: 'Alice', age: 25, bio: 'Developer' },
      { email: 'bob@example.com', name: 'Bob', age: 30, bio: 'Designer' },
      { email: 'charlie@example.com', name: 'CHARLIE', age: 35, bio: null },
    ]);
  });

  describe('raw queries with bindings', () => {
    it('should bind positional parameters', async () => {
      const result = await db.raw(
        'SELECT * FROM knex_users WHERE age > ? AND age < ?',
        [20, 35]
      );
      expect(result.rows).toHaveLength(2);
    });

    it('should bind named parameters', async () => {
      const result = await db.raw(
        'SELECT * FROM knex_users WHERE age >= :minAge AND name = :name',
        { minAge: 30, name: 'Bob' }
      );
      expect(result.rows).toHaveLength(1);
    });
  });

  describe('subqueries', () => {
    it('should handle subquery in WHERE', async () => {
      const users = await db('knex_users')
        .where('age', '>', db('knex_users').avg('age'));

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle subquery in SELECT', async () => {
      const result = await db('knex_users')
        .select('name')
        .select(
          db('knex_users')
            .count('*')
            .where('age', '<=', db.ref('knex_users.age'))
            .as('rank')
        )
        .orderBy('age', 'asc');

      expect(result).toHaveLength(3);
    });
  });

  describe('CTE (WITH clause)', () => {
    it('should execute CTE query', async () => {
      const result = await db
        .with('older_users', db('knex_users').where('age', '>=', 30))
        .select('name', 'age')
        .from('older_users')
        .orderBy('age');

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Bob');
      expect(result[1].name).toBe('CHARLIE');
    });
  });

  describe('DISTINCT operations', () => {
    beforeEach(async () => {
      await db('knex_users').insert({ email: 'diana@example.com', name: 'Diana', age: 25 });
    });

    it('should handle DISTINCT', async () => {
      const result = await db('knex_users')
        .distinct('age')
        .orderBy('age', 'asc');

      expect(result).toHaveLength(3);
    });

    it('should handle DISTINCT ON', async () => {
      const result = await db.raw(`
        SELECT DISTINCT ON (age) name, age 
        FROM knex_users 
        ORDER BY age, name
      `);

      expect(result.rows).toHaveLength(3);
    });
  });

  describe('LIKE and ILIKE', () => {
    it('should handle LIKE (case-sensitive)', async () => {
      const users = await db('knex_users').where('name', 'like', '%li%');
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Alice');
    });

    it('should handle ILIKE (case-insensitive)', async () => {
      const users = await db('knex_users').where('name', 'ilike', '%CHAR%');
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });
  });

  describe('NULL handling', () => {
    it('should handle IS NULL', async () => {
      const users = await db('knex_users').whereNull('bio');
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle IS NOT NULL', async () => {
      const users = await db('knex_users').whereNotNull('bio');
      expect(users).toHaveLength(2);
    });

    it('should handle COALESCE', async () => {
      const result = await db('knex_users')
        .select('name')
        .select(db.raw("COALESCE(bio, 'No bio') as bio"))
        .orderBy('name');

      const charlie = result.find((r) => r.name === 'CHARLIE');
      expect(charlie.bio).toBe('No bio');
    });
  });

  describe('CASE expressions', () => {
    it('should handle CASE WHEN via raw', async () => {
      const result = await db('knex_users')
        .select('name')
        .select(
          db.raw(`
            CASE 
              WHEN age < 30 THEN 'young'
              WHEN age < 40 THEN 'middle'
              ELSE 'senior'
            END as age_group
          `)
        )
        .orderBy('name');

      expect(result.find((r) => r.name === 'Alice').age_group).toBe('young');
      expect(result.find((r) => r.name === 'Bob').age_group).toBe('middle');
    });
  });

  describe('WHERE clauses', () => {
    it('should handle whereIn', async () => {
      const users = await db('knex_users').whereIn('name', ['Alice', 'Bob']);
      expect(users).toHaveLength(2);
    });

    it('should handle whereNotIn', async () => {
      const users = await db('knex_users').whereNotIn('name', ['Alice', 'Bob']);
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle whereBetween', async () => {
      const users = await db('knex_users').whereBetween('age', [25, 30]);
      expect(users).toHaveLength(2);
    });

    it('should handle orWhere', async () => {
      const users = await db('knex_users')
        .where('name', 'Alice')
        .orWhere('name', 'Bob');
      expect(users).toHaveLength(2);
    });

    it('should handle nested where', async () => {
      const users = await db('knex_users')
        .where(function () {
          this.where('age', '>=', 30).whereNotNull('bio');
        })
        .orWhere('name', 'Alice');
      expect(users).toHaveLength(2);
    });
  });

  describe('JSONB operations', () => {
    beforeEach(async () => {
      await db('knex_users')
        .where({ email: 'alice@example.com' })
        .update({ metadata: JSON.stringify({ role: 'admin', level: 5 }) });
      await db('knex_users')
        .where({ email: 'bob@example.com' })
        .update({ metadata: JSON.stringify({ role: 'user', level: 2 }) });
    });

    it('should query JSONB with -> operator', async () => {
      const result = await db.raw(`
        SELECT name, metadata->'role' as role 
        FROM knex_users 
        WHERE metadata IS NOT NULL
        ORDER BY name
      `);

      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].role).toMatch(/^"?admin"?$/);
    });

    it('should query JSONB with ->> operator', async () => {
      const result = await db.raw(`
        SELECT name, metadata->>'role' as role 
        FROM knex_users 
        WHERE metadata->>'role' = 'admin'
      `);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should query JSONB with @> operator', async () => {
      const result = await db.raw(`
        SELECT name FROM knex_users 
        WHERE metadata @> '{"role": "admin"}'::jsonb
      `);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });
  });

  describe('joins', () => {
    beforeEach(async () => {
      const [user] = await db('knex_users').where({ email: 'alice@example.com' });
      await db('knex_posts').insert([
        { title: 'Post 1', content: 'Content 1', author_id: user.id },
        { title: 'Post 2', content: 'Content 2', author_id: user.id },
      ]);
    });

    it('should handle INNER JOIN', async () => {
      const result = await db('knex_users')
        .innerJoin('knex_posts', 'knex_users.id', 'knex_posts.author_id')
        .select('knex_users.name', 'knex_posts.title');

      expect(result).toHaveLength(2);
    });

    it('should handle LEFT JOIN', async () => {
      const result = await db('knex_users')
        .leftJoin('knex_posts', 'knex_users.id', 'knex_posts.author_id')
        .select('knex_users.name', 'knex_posts.title')
        .orderBy('knex_users.name');

      expect(result).toHaveLength(4);
    });
  });
});
