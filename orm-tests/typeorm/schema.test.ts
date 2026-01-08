import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User, Post, Tag } from './entities/index.js';

describe('TypeORM Schema & Metadata Compatibility [pg-tikv]', () => {
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

  describe('table and column types', () => {
    it('should create tables with synchronize', async () => {
      const tables = await dataSource.query(`
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'typeorm_%'
      `);
      const tableNames = tables.map((t: { table_name: string }) => t.table_name);
      
      expect(tableNames).toContain('typeorm_users');
      expect(tableNames).toContain('typeorm_posts');
      expect(tableNames).toContain('typeorm_tags');
    });

    it('should create columns with correct data types', async () => {
      const columns = await dataSource.query(`
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'typeorm_users'
        ORDER BY ordinal_position
      `);

      const columnMap = new Map(
        columns.map((c: { column_name: string; data_type: string }) => [c.column_name, c.data_type])
      );

      expect(columnMap.get('id')).toBe('integer');
      expect(columnMap.get('email')).toBe('character varying');
      expect(columnMap.get('name')).toBe('character varying');
      expect(columnMap.get('age')).toBe('integer');
      expect(columnMap.get('isActive')).toBe('boolean');
      expect(columnMap.get('bio')).toBe('text');
      expect(columnMap.get('metadata')).toBe('jsonb');
      expect(columnMap.get('externalId')).toBe('uuid');
    });

    it('should create nullable columns correctly', async () => {
      const columns = await dataSource.query(`
        SELECT column_name, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'typeorm_users'
      `);

      const nullableMap = new Map(
        columns.map((c: { column_name: string; is_nullable: string }) => [
          c.column_name,
          c.is_nullable,
        ])
      );

      expect(nullableMap.get('bio')).toBe('YES');
      expect(nullableMap.get('metadata')).toBe('YES');
      expect(nullableMap.get('email')).toBe('NO');
    });

    it('should create columns with default values', async () => {
      const columns = await dataSource.query(`
        SELECT column_name, column_default
        FROM information_schema.columns 
        WHERE table_name = 'typeorm_users' AND column_default IS NOT NULL
      `);

      const hasAgeDefault = columns.some(
        (c: { column_name: string; column_default: string }) =>
          c.column_name === 'age' && c.column_default.includes('0')
      );
      const hasIsActiveDefault = columns.some(
        (c: { column_name: string; column_default: string }) =>
          c.column_name === 'isActive' && c.column_default.includes('true')
      );

      expect(hasAgeDefault).toBe(true);
      expect(hasIsActiveDefault).toBe(true);
    });
  });

  describe('indexes and constraints', () => {
    it('should create primary key', async () => {
      const pks = await dataSource.query(`
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = 'typeorm_users' AND constraint_type = 'PRIMARY KEY'
      `);

      expect(pks.length).toBeGreaterThan(0);
    });

    it('should create unique constraint', async () => {
      const constraints = await dataSource.query(`
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = 'typeorm_users' AND constraint_type = 'UNIQUE'
      `);

      expect(constraints.length).toBeGreaterThan(0);
    });

    it('should create foreign key constraint', async () => {
      const fks = await dataSource.query(`
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = 'typeorm_posts' AND constraint_type = 'FOREIGN KEY'
      `);

      expect(fks.length).toBeGreaterThan(0);
    });

    it('should create indexes', async () => {
      const indexes = await dataSource.query(`
        SELECT indexname FROM pg_indexes WHERE tablename = 'typeorm_users'
      `);

      expect(indexes.length).toBeGreaterThan(0);
    });
  });

  describe('schema introspection', () => {
    it('should introspect entity metadata correctly', () => {
      const userMetadata = dataSource.getMetadata(User);
      
      expect(userMetadata.tableName).toBe('typeorm_users');
      expect(userMetadata.columns.length).toBeGreaterThan(0);
      
      const emailColumn = userMetadata.columns.find((c) => c.propertyName === 'email');
      expect(emailColumn).toBeDefined();
    });

    it('should introspect relations correctly', () => {
      const postMetadata = dataSource.getMetadata(Post);
      
      const authorRelation = postMetadata.relations.find(
        (r) => r.propertyName === 'author'
      );
      expect(authorRelation).toBeDefined();
      expect(authorRelation?.relationType).toBe('many-to-one');

      const tagsRelation = postMetadata.relations.find(
        (r) => r.propertyName === 'tags'
      );
      expect(tagsRelation).toBeDefined();
      expect(tagsRelation?.relationType).toBe('many-to-many');
    });
  });
});
