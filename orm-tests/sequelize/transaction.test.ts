import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Sequelize, Transaction } from 'sequelize';
import { createSequelize } from './connection.js';
import { User } from './models.js';

describe('Sequelize Transactions & Isolation [pg-tikv]', () => {
  let sequelize: Sequelize;

  beforeAll(async () => {
    sequelize = createSequelize();
    await sequelize.sync({ force: true });
  });

  afterAll(async () => {
    await sequelize.drop();
    await sequelize.close();
  });

  beforeEach(async () => {
    await User.destroy({ where: {}, force: true });
  });

  describe('managed transactions', () => {
    it('should commit transaction', async () => {
      await sequelize.transaction(async (t) => {
        await User.create(
          { email: 'txn@example.com', name: 'Transaction User', age: 30 },
          { transaction: t }
        );
      });

      const user = await User.findOne({ where: { email: 'txn@example.com' } });
      expect(user).not.toBeNull();
    });

    it('should rollback transaction on error', async () => {
      try {
        await sequelize.transaction(async (t) => {
          await User.create(
            { email: 'rollback@example.com', name: 'Rollback User', age: 30 },
            { transaction: t }
          );
          throw new Error('Intentional error');
        });
      } catch {
      }

      const user = await User.findOne({ where: { email: 'rollback@example.com' } });
      expect(user).toBeNull();
    });

    it('should handle multiple operations in transaction', async () => {
      await sequelize.transaction(async (t) => {
        const user1 = await User.create(
          { email: 'multi1@example.com', name: 'Multi User 1', age: 25 },
          { transaction: t }
        );

        await User.create(
          { email: 'multi2@example.com', name: 'Multi User 2', age: 30 },
          { transaction: t }
        );

        await user1.update({ age: 26 }, { transaction: t });
      });

      const users = await User.findAll();
      expect(users).toHaveLength(2);
    });
  });

  describe('unmanaged transactions', () => {
    it('should manually commit transaction', async () => {
      const t = await sequelize.transaction();

      try {
        await User.create(
          { email: 'manual@example.com', name: 'Manual User', age: 30 },
          { transaction: t }
        );
        await t.commit();
      } catch (err) {
        await t.rollback();
        throw err;
      }

      const user = await User.findOne({ where: { email: 'manual@example.com' } });
      expect(user).not.toBeNull();
    });

    it('should manually rollback transaction', async () => {
      const t = await sequelize.transaction();

      await User.create(
        { email: 'manualroll@example.com', name: 'Manual Rollback', age: 30 },
        { transaction: t }
      );

      await t.rollback();

      const user = await User.findOne({ where: { email: 'manualroll@example.com' } });
      expect(user).toBeNull();
    });
  });

  describe('savepoints', () => {
    it('should handle nested transaction with savepoint', async () => {
      await sequelize.transaction(async (t1) => {
        await User.create(
          { email: 'outer@example.com', name: 'Outer User', age: 30 },
          { transaction: t1 }
        );

        try {
          await sequelize.transaction({ transaction: t1 }, async (t2) => {
            await User.create(
              { email: 'inner@example.com', name: 'Inner User', age: 25 },
              { transaction: t2 }
            );
            throw new Error('Rollback inner');
          });
        } catch {
        }
      });

      const outer = await User.findOne({ where: { email: 'outer@example.com' } });
      const inner = await User.findOne({ where: { email: 'inner@example.com' } });

      expect(outer).not.toBeNull();
      expect(inner).toBeNull();
    });
  });

  describe('isolation levels', () => {
    it('should handle READ COMMITTED isolation', async () => {
      await sequelize.transaction(
        { isolationLevel: Transaction.ISOLATION_LEVELS.READ_COMMITTED },
        async (t) => {
          await User.create(
            { email: 'readcommit@example.com', name: 'Read Commit', age: 25 },
            { transaction: t }
          );
        }
      );

      const user = await User.findOne({ where: { email: 'readcommit@example.com' } });
      expect(user).not.toBeNull();
    });

    it('should handle REPEATABLE READ isolation', async () => {
      await User.create({ email: 'rr@example.com', name: 'RR User', age: 30 });

      await sequelize.transaction(
        { isolationLevel: Transaction.ISOLATION_LEVELS.REPEATABLE_READ },
        async (t) => {
          const first = await User.findOne({
            where: { email: 'rr@example.com' },
            transaction: t,
          });

          await User.update(
            { age: 40 },
            { where: { email: 'rr@example.com' } }
          );

          const second = await User.findOne({
            where: { email: 'rr@example.com' },
            transaction: t,
          });

          expect(first?.age).toBe(second?.age);
        }
      );
    });

    it('should handle SERIALIZABLE isolation', async () => {
      await sequelize.transaction(
        { isolationLevel: Transaction.ISOLATION_LEVELS.SERIALIZABLE },
        async (t) => {
          await User.create(
            { email: 'serial@example.com', name: 'Serializable', age: 25 },
            { transaction: t }
          );
        }
      );

      const user = await User.findOne({ where: { email: 'serial@example.com' } });
      expect(user).not.toBeNull();
    });
  });

  describe('transaction visibility', () => {
    it('should not see uncommitted changes outside transaction', async () => {
      const t = await sequelize.transaction();

      await User.create(
        { email: 'visibility@example.com', name: 'Visibility Test', age: 30 },
        { transaction: t }
      );

      const outsideView = await User.findOne({
        where: { email: 'visibility@example.com' },
      });
      expect(outsideView).toBeNull();

      const insideView = await User.findOne({
        where: { email: 'visibility@example.com' },
        transaction: t,
      });
      expect(insideView).not.toBeNull();

      await t.commit();

      const afterCommit = await User.findOne({
        where: { email: 'visibility@example.com' },
      });
      expect(afterCommit).not.toBeNull();
    });
  });
});
