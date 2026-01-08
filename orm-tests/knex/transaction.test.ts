import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Knex } from 'knex';
import { createKnexClient, setupKnexTables, cleanupKnexTables } from './client.js';

describe('Knex Transactions & Isolation [pg-tikv]', () => {
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

  describe('transaction callback', () => {
    it('should commit transaction', async () => {
      await db.transaction(async (trx) => {
        await trx('knex_users').insert({
          email: 'txn@example.com',
          name: 'Transaction User',
          age: 30,
        });
      });

      const user = await db('knex_users').where({ email: 'txn@example.com' }).first();
      expect(user).not.toBeUndefined();
      expect(user.name).toBe('Transaction User');
    });

    it('should rollback transaction on error', async () => {
      try {
        await db.transaction(async (trx) => {
          await trx('knex_users').insert({
            email: 'rollback@example.com',
            name: 'Rollback User',
            age: 30,
          });
          throw new Error('Intentional error');
        });
      } catch {
      }

      const user = await db('knex_users').where({ email: 'rollback@example.com' }).first();
      expect(user).toBeUndefined();
    });

    it('should handle multiple operations in transaction', async () => {
      await db.transaction(async (trx) => {
        const [user1] = await trx('knex_users')
          .insert({ email: 'multi1@example.com', name: 'Multi User 1', age: 25 })
          .returning('*');

        await trx('knex_users').insert({
          email: 'multi2@example.com',
          name: 'Multi User 2',
          age: 30,
        });

        await trx('knex_users').where({ id: user1.id }).update({ age: 26 });
      });

      const users = await db('knex_users');
      expect(users).toHaveLength(2);
    });
  });

  describe('manual transaction', () => {
    it('should manually commit transaction', async () => {
      const trx = await db.transaction();

      try {
        await trx('knex_users').insert({
          email: 'manual@example.com',
          name: 'Manual User',
          age: 30,
        });
        await trx.commit();
      } catch (err) {
        await trx.rollback();
        throw err;
      }

      const user = await db('knex_users').where({ email: 'manual@example.com' }).first();
      expect(user).not.toBeUndefined();
    });

    it('should manually rollback transaction', async () => {
      const trx = await db.transaction();

      await trx('knex_users').insert({
        email: 'manualroll@example.com',
        name: 'Manual Rollback',
        age: 30,
      });

      await trx.rollback();

      const user = await db('knex_users').where({ email: 'manualroll@example.com' }).first();
      expect(user).toBeUndefined();
    });
  });

  describe('savepoints', () => {
    it('should handle savepoint', async () => {
      await db.transaction(async (trx) => {
        await trx('knex_users').insert({
          email: 'outer@example.com',
          name: 'Outer User',
          age: 30,
        });

        const savepoint = await trx.savepoint(async (sp) => {
          await sp('knex_users').insert({
            email: 'inner@example.com',
            name: 'Inner User',
            age: 25,
          });
          throw new Error('Rollback savepoint');
        }).catch(() => {});
      });

      const outer = await db('knex_users').where({ email: 'outer@example.com' }).first();
      const inner = await db('knex_users').where({ email: 'inner@example.com' }).first();

      expect(outer).not.toBeUndefined();
      expect(inner).toBeUndefined();
    });
  });

  describe('isolation levels', () => {
    it('should handle READ COMMITTED isolation', async () => {
      const trx = await db.transaction();
      try {
        await trx.raw('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
        await trx('knex_users').insert({
          email: 'readcommit@example.com',
          name: 'Read Commit',
          age: 25,
        });
        await trx.commit();
      } catch (err) {
        await trx.rollback();
        throw err;
      }

      const user = await db('knex_users').where({ email: 'readcommit@example.com' }).first();
      expect(user).not.toBeUndefined();
    });

    it('should handle REPEATABLE READ isolation', async () => {
      await db('knex_users').insert({ email: 'rr@example.com', name: 'RR User', age: 30 });

      const trx = await db.transaction();
      try {
        await trx.raw('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
        const first = await trx('knex_users').where({ email: 'rr@example.com' }).first();

        await db('knex_users').where({ email: 'rr@example.com' }).update({ age: 40 });

        const second = await trx('knex_users').where({ email: 'rr@example.com' }).first();

        expect(first.age).toBe(second.age);
        await trx.commit();
      } catch (err) {
        await trx.rollback();
        throw err;
      }
    });

    it('should handle SERIALIZABLE isolation', async () => {
      const trx = await db.transaction();
      try {
        await trx.raw('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
        await trx('knex_users').insert({
          email: 'serial@example.com',
          name: 'Serializable',
          age: 25,
        });
        await trx.commit();
      } catch (err) {
        await trx.rollback();
        throw err;
      }

      const user = await db('knex_users').where({ email: 'serial@example.com' }).first();
      expect(user).not.toBeUndefined();
    });
  });

  describe('transaction visibility', () => {
    it('should not see uncommitted changes outside transaction', async () => {
      const trx = await db.transaction();

      await trx('knex_users').insert({
        email: 'visibility@example.com',
        name: 'Visibility Test',
        age: 30,
      });

      const outsideView = await db('knex_users').where({ email: 'visibility@example.com' }).first();
      expect(outsideView).toBeUndefined();

      const insideView = await trx('knex_users').where({ email: 'visibility@example.com' }).first();
      expect(insideView).not.toBeUndefined();

      await trx.commit();

      const afterCommit = await db('knex_users').where({ email: 'visibility@example.com' }).first();
      expect(afterCommit).not.toBeUndefined();
    });
  });
});
