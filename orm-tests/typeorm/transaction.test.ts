import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User } from './entities/index.js';

describe('TypeORM Transactions & Isolation [pg-tikv]', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = createDataSource({ synchronize: true });
    await dataSource.initialize();
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.query('DROP TABLE IF EXISTS typeorm_post_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_posts CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_users CASCADE');
      await dataSource.destroy();
    }
  });

  beforeEach(async () => {
    await dataSource.query('DELETE FROM typeorm_post_tags');
    await dataSource.query('DELETE FROM typeorm_posts');
    await dataSource.query('DELETE FROM typeorm_tags');
    await dataSource.query('DELETE FROM typeorm_users');
  });

  describe('explicit transactions', () => {
    it('should commit transaction', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      try {
        await queryRunner.manager.save(User, {
          email: 'txn@example.com',
          name: 'Transaction User',
          age: 30,
        });
        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const user = await dataSource.getRepository(User).findOneBy({ email: 'txn@example.com' });
      expect(user).not.toBeNull();
      expect(user?.name).toBe('Transaction User');
    });

    it('should rollback transaction', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      try {
        await queryRunner.manager.save(User, {
          email: 'rollback@example.com',
          name: 'Rollback User',
          age: 30,
        });
        await queryRunner.rollbackTransaction();
      } finally {
        await queryRunner.release();
      }

      const user = await dataSource.getRepository(User).findOneBy({ email: 'rollback@example.com' });
      expect(user).toBeNull();
    });

    it('should handle transaction with multiple operations', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      try {
        const user1 = await queryRunner.manager.save(User, {
          email: 'multi1@example.com',
          name: 'Multi User 1',
          age: 25,
        });

        const user2 = await queryRunner.manager.save(User, {
          email: 'multi2@example.com',
          name: 'Multi User 2',
          age: 30,
        });

        await queryRunner.manager.update(User, user1.id, { age: 26 });
        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const users = await dataSource.getRepository(User).find();
      expect(users).toHaveLength(2);
    });
  });

  describe('transaction manager', () => {
    it('should use transaction manager wrapper', async () => {
      await dataSource.transaction(async (manager) => {
        await manager.save(User, {
          email: 'manager@example.com',
          name: 'Manager User',
          age: 28,
        });
      });

      const user = await dataSource.getRepository(User).findOneBy({ email: 'manager@example.com' });
      expect(user).not.toBeNull();
    });

    it('should auto-rollback on error in transaction manager', async () => {
      try {
        await dataSource.transaction(async (manager) => {
          await manager.save(User, {
            email: 'error@example.com',
            name: 'Error User',
            age: 28,
          });
          throw new Error('Intentional error');
        });
      } catch {
      }

      const user = await dataSource.getRepository(User).findOneBy({ email: 'error@example.com' });
      expect(user).toBeNull();
    });
  });

  describe('savepoint (nested transactions)', () => {
    it('should handle savepoint rollback', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      try {
        await queryRunner.manager.save(User, {
          email: 'outer@example.com',
          name: 'Outer User',
          age: 30,
        });

        await queryRunner.query('SAVEPOINT sp1');

        await queryRunner.manager.save(User, {
          email: 'inner@example.com',
          name: 'Inner User',
          age: 25,
        });

        await queryRunner.query('ROLLBACK TO SAVEPOINT sp1');

        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const outer = await dataSource.getRepository(User).findOneBy({ email: 'outer@example.com' });
      const inner = await dataSource.getRepository(User).findOneBy({ email: 'inner@example.com' });

      expect(outer).not.toBeNull();
      expect(inner).toBeNull();
    });

    it('should handle multiple savepoints', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      try {
        await queryRunner.manager.save(User, {
          email: 'base@example.com',
          name: 'Base User',
          age: 30,
        });

        await queryRunner.query('SAVEPOINT sp1');
        await queryRunner.manager.save(User, {
          email: 'sp1@example.com',
          name: 'SP1 User',
          age: 25,
        });

        await queryRunner.query('SAVEPOINT sp2');
        await queryRunner.manager.save(User, {
          email: 'sp2@example.com',
          name: 'SP2 User',
          age: 20,
        });

        await queryRunner.query('ROLLBACK TO SAVEPOINT sp2');
        await queryRunner.query('RELEASE SAVEPOINT sp1');

        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const base = await dataSource.getRepository(User).findOneBy({ email: 'base@example.com' });
      const sp1 = await dataSource.getRepository(User).findOneBy({ email: 'sp1@example.com' });
      const sp2 = await dataSource.getRepository(User).findOneBy({ email: 'sp2@example.com' });

      expect(base).not.toBeNull();
      expect(sp1).not.toBeNull();
      expect(sp2).toBeNull();
    });
  });

  describe('transaction isolation', () => {
    it('should handle READ COMMITTED isolation', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction('READ COMMITTED');

      try {
        await queryRunner.manager.save(User, {
          email: 'isolation@example.com',
          name: 'Isolation User',
          age: 30,
        });
        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const user = await dataSource.getRepository(User).findOneBy({ email: 'isolation@example.com' });
      expect(user).not.toBeNull();
    });

    it('should handle REPEATABLE READ isolation', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save({
        email: 'rr@example.com',
        name: 'RR User',
        age: 30,
      });

      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction('REPEATABLE READ');

      try {
        const firstRead = await queryRunner.manager.findOneBy(User, { email: 'rr@example.com' });

        await userRepo.update({ email: 'rr@example.com' }, { age: 40 });

        const secondRead = await queryRunner.manager.findOneBy(User, { email: 'rr@example.com' });

        expect(firstRead?.age).toBe(secondRead?.age);

        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }
    });

    it('should handle SERIALIZABLE isolation', async () => {
      const queryRunner = dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction('SERIALIZABLE');

      try {
        await queryRunner.manager.save(User, {
          email: 'serial@example.com',
          name: 'Serial User',
          age: 30,
        });
        await queryRunner.commitTransaction();
      } catch (err) {
        await queryRunner.rollbackTransaction();
        throw err;
      } finally {
        await queryRunner.release();
      }

      const user = await dataSource.getRepository(User).findOneBy({ email: 'serial@example.com' });
      expect(user).not.toBeNull();
    });
  });
});
