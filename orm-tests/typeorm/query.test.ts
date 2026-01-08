import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User, Post, Tag } from './entities/index.js';

describe('TypeORM Query Generation & SQL Compatibility [pg-tikv]', () => {
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

    const userRepo = dataSource.getRepository(User);
    await userRepo.save([
      { email: 'alice@example.com', name: 'Alice', age: 25, bio: 'Developer' },
      { email: 'bob@example.com', name: 'Bob', age: 30, bio: 'Designer' },
      { email: 'charlie@example.com', name: 'CHARLIE', age: 35, bio: null },
    ]);
  });

  describe('parameter binding', () => {
    it('should bind positional parameters ($1, $2)', async () => {
      const result = await dataSource.query(
        'SELECT * FROM typeorm_users WHERE age > $1 AND age < $2',
        [20, 35]
      );
      expect(result).toHaveLength(2);
    });

    it('should bind named parameters via QueryBuilder', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.age >= :minAge', { minAge: 30 })
        .andWhere('user.name = :name', { name: 'Bob' })
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Bob');
    });
  });

  describe('subqueries', () => {
    it('should handle subquery in WHERE', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where((qb) => {
          const subQuery = qb
            .subQuery()
            .select('AVG(u.age)')
            .from(User, 'u')
            .getQuery();
          return `user.age > (${subQuery})`;
        })
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle subquery in SELECT', async () => {
      const result = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .select('user.name', 'name')
        .addSelect((qb) => {
          return qb
            .subQuery()
            .select('COUNT(*)')
            .from(User, 'u')
            .where('u.age <= user.age');
        }, 'rank')
        .orderBy('user.age', 'ASC')
        .getRawMany();

      expect(result).toHaveLength(3);
    });
  });

  describe('CTE (WITH clause)', () => {
    it('should execute CTE query', async () => {
      const result = await dataSource.query(`
        WITH older_users AS (
          SELECT * FROM typeorm_users WHERE age >= 30
        )
        SELECT name, age FROM older_users ORDER BY age
      `);

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Bob');
      expect(result[1].name).toBe('CHARLIE');
    });
  });

  describe('DISTINCT operations', () => {
    it('should handle DISTINCT', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save({ email: 'diana@example.com', name: 'Diana', age: 25 });

      const result = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .select('DISTINCT user.age', 'age')
        .orderBy('age', 'ASC')
        .getRawMany();

      expect(result).toHaveLength(3);
    });

    it('should handle DISTINCT ON', async () => {
      const userRepo = dataSource.getRepository(User);
      await userRepo.save({ email: 'diana@example.com', name: 'Diana', age: 25 });

      const result = await dataSource.query(`
        SELECT DISTINCT ON (age) name, age 
        FROM typeorm_users 
        ORDER BY age, name
      `);

      expect(result).toHaveLength(3);
    });
  });

  describe('LIKE and ILIKE', () => {
    it('should handle LIKE (case-sensitive)', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.name LIKE :pattern', { pattern: '%li%' })
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Alice');
    });

    it('should handle ILIKE (case-insensitive)', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.name ILIKE :pattern', { pattern: '%CHAR%' })
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });
  });

  describe('NULL handling', () => {
    it('should handle IS NULL', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.bio IS NULL')
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });

    it('should handle IS NOT NULL', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.bio IS NOT NULL')
        .getMany();

      expect(users).toHaveLength(2);
    });

    it('should handle COALESCE', async () => {
      const result = await dataSource.query(`
        SELECT name, COALESCE(bio, 'No bio') as bio FROM typeorm_users ORDER BY name
      `);

      const charlie = result.find((r: { name: string }) => r.name === 'CHARLIE');
      expect(charlie.bio).toBe('No bio');
    });
  });

  describe('CASE expressions', () => {
    it('should handle CASE WHEN', async () => {
      const result = await dataSource.query(`
        SELECT name, 
          CASE 
            WHEN age < 30 THEN 'young'
            WHEN age < 40 THEN 'middle'
            ELSE 'senior'
          END as age_group
        FROM typeorm_users
        ORDER BY name
      `);

      expect(result.find((r: { name: string }) => r.name === 'Alice').age_group).toBe('young');
      expect(result.find((r: { name: string }) => r.name === 'Bob').age_group).toBe('middle');
    });
  });

  describe('array operations', () => {
    it('should handle IN clause', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.name IN (:...names)', { names: ['Alice', 'Bob'] })
        .getMany();

      expect(users).toHaveLength(2);
    });

    it('should handle NOT IN clause', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .where('user.name NOT IN (:...names)', { names: ['Alice', 'Bob'] })
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('CHARLIE');
    });
  });

  describe('JSONB operations', () => {
    beforeEach(async () => {
      await dataSource.getRepository(User).update(
        { email: 'alice@example.com' },
        { metadata: { role: 'admin', level: 5, tags: ['dev', 'lead'] } }
      );
      await dataSource.getRepository(User).update(
        { email: 'bob@example.com' },
        { metadata: { role: 'user', level: 2, tags: ['design'] } }
      );
    });

    it('should query JSONB with -> operator', async () => {
      const result = await dataSource.query(`
        SELECT name, metadata->'role' as role 
        FROM typeorm_users 
        WHERE metadata IS NOT NULL
        ORDER BY name
      `);

      expect(result).toHaveLength(2);
      expect(result[0].role).toMatch(/^"?admin"?$/);
    });

    it('should query JSONB with ->> operator', async () => {
      const result = await dataSource.query(`
        SELECT name, metadata->>'role' as role 
        FROM typeorm_users 
        WHERE metadata->>'role' = 'admin'
      `);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });

    it('should query JSONB with @> operator', async () => {
      const result = await dataSource.query(`
        SELECT name FROM typeorm_users 
        WHERE metadata @> '{"role": "admin"}'::jsonb
      `);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });
});
