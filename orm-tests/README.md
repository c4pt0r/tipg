# pg-tikv ORM Compatibility Tests

This test suite validates pg-tikv's compatibility with popular TypeScript/JavaScript ORMs.

## Prerequisites

- Node.js >= 18.0.0
- npm or yarn
- Running pg-tikv instance (default: `localhost:5433`)
- TiKV cluster (for pg-tikv backend)

## Quick Start

```bash
# 1. Start TiKV cluster
tiup playground --mode tikv-slim

# 2. Start pg-tikv (in another terminal)
cd /path/to/pg-tikv
cargo run

# 3. Run ORM tests
cd orm-tests
npm install
npx prisma generate
npm test
```

## Test Configuration

Default connection settings in `shared/config.ts`:

| Setting | Default | Environment Variable |
|---------|---------|---------------------|
| Host | `127.0.0.1` | `PG_HOST` |
| Port | `5433` | `PG_PORT` |
| Database | `postgres` | `PG_DATABASE` |
| User | `postgres` | `PG_USER` |
| Password | `postgres` | `PG_PASSWORD` |

Override using environment variables:

```bash
PG_HOST=192.168.1.100 PG_PORT=5432 npm test
```

## ORMs Tested

| ORM | Version | Test Files |
|-----|---------|------------|
| TypeORM | ^0.3.17 | `typeorm/*.test.ts` |
| Prisma | ^5.7.0 | `prisma/*.test.ts` |
| Sequelize | ^6.35.0 | `sequelize/*.test.ts` |
| Knex.js | ^3.1.0 | `knex/*.test.ts` |
| Drizzle | ^0.29.0 | `drizzle/*.test.ts` |

## Test Categories

### Basic Tests

| Category | Description |
|----------|-------------|
| `connection.test.ts` | Connection establishment, pooling, error handling |
| `crud.test.ts` | INSERT, SELECT, UPDATE, DELETE operations |
| `transaction.test.ts` | BEGIN, COMMIT, ROLLBACK, isolation levels |
| `relation.test.ts` | Foreign keys, JOINs, eager loading |
| `query.test.ts` | Query builder, raw queries, filtering |

### Advanced Tests (`advanced.test.ts`)

Each ORM has an `advanced.test.ts` file testing:

| Feature | SQL Constructs |
|---------|---------------|
| Window Functions | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LEAD()`, `LAG()`, `SUM() OVER`, `AVG() OVER` |
| CTEs | `WITH ... AS`, multiple CTEs |
| Recursive CTEs | `WITH RECURSIVE` for hierarchies |
| Subqueries | `IN (SELECT ...)`, `EXISTS`, scalar subqueries, derived tables |
| Views | `CREATE VIEW`, `SELECT FROM view` |
| JSONB | `->`, `->>`, `@>` operators |
| String Functions | `UPPER`, `LOWER`, `CONCAT`, `SUBSTRING`, `SPLIT_PART`, `REPLACE` |
| Date Functions | `NOW()`, `DATE_TRUNC`, `EXTRACT`, interval arithmetic |
| Aggregates | `GROUP BY`, `HAVING`, `COUNT`, `SUM`, `AVG` |
| DISTINCT ON | PostgreSQL-specific `DISTINCT ON` |
| CASE | `CASE WHEN ... THEN ... END` |
| NULL Handling | `COALESCE`, `NULLIF`, `IS NULL` |
| Math Functions | `ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POWER`, `MOD` |
| Constraints | Foreign key enforcement |

## Running Tests

### All Tests

```bash
npm test
```

### Single ORM

```bash
npm test -- typeorm/
npm test -- prisma/
npm test -- sequelize/
npm test -- knex/
npm test -- drizzle/
```

### Specific Test File

```bash
npm test -- typeorm/advanced.test.ts
npm test -- knex/crud.test.ts
```

### Specific Test by Name

```bash
npm test -- -t "window functions"
npm test -- -t "should support ROW_NUMBER"
```

### Watch Mode

```bash
npm run test:watch
```

### Verbose Output

```bash
npm test -- --reporter=verbose
```

### Debug Mode

```bash
DEBUG=true npm test
```

## Generating Reports

### Compatibility Report

After running tests, generate a markdown report:

```bash
npm run report
```

Output: `COMPATIBILITY.md`

### JSON Results

Test results are automatically saved to `test-results.json` after each run.

## Project Structure

```
orm-tests/
├── shared/
│   ├── config.ts           # Database connection config
│   └── generate-report.ts  # Report generator
├── typeorm/
│   ├── datasource.ts       # TypeORM DataSource setup
│   ├── entities/           # Entity definitions
│   ├── connection.test.ts
│   ├── crud.test.ts
│   ├── transaction.test.ts
│   ├── relation.test.ts
│   ├── query.test.ts
│   ├── schema.test.ts
│   ├── error.test.ts
│   ├── types.test.ts
│   └── advanced.test.ts    # Window functions, CTEs, etc.
├── prisma/
│   ├── schema.prisma       # Prisma schema
│   ├── client.ts           # Prisma client setup
│   ├── connection.test.ts
│   ├── crud.test.ts
│   ├── query.test.ts
│   ├── transaction.test.ts
│   └── advanced.test.ts
├── sequelize/
│   ├── connection.ts       # Sequelize setup
│   ├── models.ts           # Model definitions
│   ├── connection.test.ts
│   ├── crud.test.ts
│   ├── transaction.test.ts
│   ├── relation.test.ts
│   └── advanced.test.ts
├── knex/
│   ├── client.ts           # Knex client setup
│   ├── connection.test.ts
│   ├── crud.test.ts
│   ├── query.test.ts
│   ├── transaction.test.ts
│   └── advanced.test.ts
├── drizzle/
│   ├── schema.ts           # Drizzle schema
│   ├── client.ts           # Drizzle client setup
│   ├── connection.test.ts
│   ├── crud.test.ts
│   ├── transaction.test.ts
│   └── advanced.test.ts
├── package.json
├── tsconfig.json
├── vitest.config.ts
└── README.md
```

## Known Limitations

### pg-tikv Limitations

1. **information_schema** - Limited support. Some TypeORM schema tests are skipped.

2. **JSONB `->` operator** - Returns text without JSON quotes (differs from PostgreSQL).

### ORM-Specific Notes

1. **Drizzle ORM** - Modifies global `pg` type parsers. TypeORM tests restore defaults.

2. **Sequelize upsert** - Returns `null` instead of `false` for "created" flag.

3. **Prisma** - Uses `$queryRaw` for advanced SQL features.

## Adding New Tests

### 1. Create Test File

```typescript
// orm-name/new-feature.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

describe('ORM New Feature [pg-tikv]', () => {
  // Setup
  beforeAll(async () => {
    // Initialize connection and create test tables
  });

  afterAll(async () => {
    // Cleanup and close connection
  });

  it('should do something', async () => {
    // Test implementation
  });
});
```

### 2. Follow Naming Conventions

- Test files: `*.test.ts`
- Describe blocks: `'ORM Feature [pg-tikv]'`
- Table names: Prefix with ORM name (e.g., `typeorm_users`, `knex_posts`)

### 3. Run and Verify

```bash
npm test -- new-feature.test.ts
```

## Examples: Adding Tests for Each ORM

### TypeORM Example

```typescript
// typeorm/array-ops.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';

describe('TypeORM Array Operations [pg-tikv]', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = createDataSource({ synchronize: false });
    await dataSource.initialize();
    
    // Create test table
    await dataSource.query(`
      DROP TABLE IF EXISTS typeorm_array_test;
      CREATE TABLE typeorm_array_test (
        id SERIAL PRIMARY KEY,
        tags TEXT[],
        scores INTEGER[]
      )
    `);
  });

  afterAll(async () => {
    await dataSource.query('DROP TABLE IF EXISTS typeorm_array_test');
    await dataSource.destroy();
  });

  it('should insert and query arrays', async () => {
    await dataSource.query(
      `INSERT INTO typeorm_array_test (tags, scores) VALUES ($1, $2)`,
      [['rust', 'postgres'], [95, 87, 92]]
    );

    const result = await dataSource.query(
      `SELECT * FROM typeorm_array_test WHERE 'rust' = ANY(tags)`
    );
    
    expect(result).toHaveLength(1);
    expect(result[0].tags).toContain('rust');
  });

  it('should use array functions', async () => {
    const result = await dataSource.query(
      `SELECT array_length(scores, 1) as len FROM typeorm_array_test`
    );
    
    expect(result[0].len).toBe(3);
  });
});
```

### Prisma Example

```typescript
// prisma/json-ops.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { createPrismaClient } from './client.js';

describe('Prisma JSON Operations [pg-tikv]', () => {
  let prisma: PrismaClient;

  beforeAll(async () => {
    prisma = createPrismaClient();
    
    // Create test table using raw SQL
    await prisma.$executeRaw`
      DROP TABLE IF EXISTS prisma_json_test;
      CREATE TABLE prisma_json_test (
        id SERIAL PRIMARY KEY,
        metadata JSONB
      )
    `;
  });

  afterAll(async () => {
    await prisma.$executeRaw`DROP TABLE IF EXISTS prisma_json_test`;
    await prisma.$disconnect();
  });

  it('should query JSONB with operators', async () => {
    await prisma.$executeRaw`
      INSERT INTO prisma_json_test (metadata) 
      VALUES ('{"name": "Alice", "age": 30, "skills": ["rust", "go"]}')
    `;

    const result = await prisma.$queryRaw<any[]>`
      SELECT metadata->>'name' as name, metadata->'age' as age
      FROM prisma_json_test
      WHERE metadata @> '{"name": "Alice"}'
    `;
    
    expect(result[0].name).toBe('Alice');
  });

  it('should use JSONB array access', async () => {
    const result = await prisma.$queryRaw<any[]>`
      SELECT metadata->'skills'->0 as first_skill
      FROM prisma_json_test
    `;
    
    expect(result[0].first_skill).toBe('rust');
  });
});
```

### Knex Example

```typescript
// knex/window-funcs.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createKnexClient } from './client.js';
import type { Knex } from 'knex';

describe('Knex Window Functions [pg-tikv]', () => {
  let knex: Knex;

  beforeAll(async () => {
    knex = createKnexClient();
    
    // Create and populate test table
    await knex.raw(`DROP TABLE IF EXISTS knex_sales`);
    await knex.raw(`
      CREATE TABLE knex_sales (
        id SERIAL PRIMARY KEY,
        region TEXT,
        amount DOUBLE PRECISION,
        sale_date DATE
      )
    `);
    
    await knex.raw(`
      INSERT INTO knex_sales (region, amount, sale_date) VALUES
      ('North', 1000, '2024-01-15'),
      ('North', 1500, '2024-01-20'),
      ('South', 800, '2024-01-10'),
      ('South', 1200, '2024-01-25')
    `);
  });

  afterAll(async () => {
    await knex.raw('DROP TABLE IF EXISTS knex_sales');
    await knex.destroy();
  });

  it('should support ROW_NUMBER', async () => {
    const result = await knex.raw(`
      SELECT region, amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank
      FROM knex_sales
    `);
    
    expect(result.rows).toHaveLength(4);
    // First row of each partition should have rank 1
    const northFirst = result.rows.find(
      (r: any) => r.region === 'North' && r.rank === '1'
    );
    expect(Number(northFirst.amount)).toBe(1500);
  });

  it('should support running totals', async () => {
    const result = await knex.raw(`
      SELECT region, amount,
        SUM(amount) OVER (PARTITION BY region ORDER BY sale_date) as running_total
      FROM knex_sales
      ORDER BY region, sale_date
    `);
    
    expect(result.rows).toHaveLength(4);
  });
});
```

### Sequelize Example

```typescript
// sequelize/cte.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createSequelize } from './connection.js';
import { Sequelize, QueryTypes } from 'sequelize';

describe('Sequelize CTE Support [pg-tikv]', () => {
  let sequelize: Sequelize;

  beforeAll(async () => {
    sequelize = createSequelize();
    
    await sequelize.query(`DROP TABLE IF EXISTS seq_employees`);
    await sequelize.query(`
      CREATE TABLE seq_employees (
        id SERIAL PRIMARY KEY,
        name TEXT,
        manager_id INTEGER,
        department TEXT
      )
    `);
    
    await sequelize.query(`
      INSERT INTO seq_employees (name, manager_id, department) VALUES
      ('CEO', NULL, 'Executive'),
      ('CTO', 1, 'Tech'),
      ('Engineer', 2, 'Tech'),
      ('CFO', 1, 'Finance')
    `);
  });

  afterAll(async () => {
    await sequelize.query('DROP TABLE IF EXISTS seq_employees');
    await sequelize.close();
  });

  it('should support simple CTE', async () => {
    const [results] = await sequelize.query(`
      WITH tech_team AS (
        SELECT * FROM seq_employees WHERE department = 'Tech'
      )
      SELECT name FROM tech_team ORDER BY name
    `);
    
    expect(results).toHaveLength(2);
  });

  it('should support recursive CTE for hierarchy', async () => {
    const [results] = await sequelize.query(`
      WITH RECURSIVE org_tree AS (
        SELECT id, name, manager_id, 1 as level
        FROM seq_employees WHERE manager_id IS NULL
        
        UNION ALL
        
        SELECT e.id, e.name, e.manager_id, t.level + 1
        FROM seq_employees e
        JOIN org_tree t ON e.manager_id = t.id
      )
      SELECT name, level FROM org_tree ORDER BY level, name
    `);
    
    expect(results).toHaveLength(4);
    expect((results[0] as any).level).toBe(1); // CEO at level 1
  });
});
```

### Drizzle Example

```typescript
// drizzle/subquery.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import { createDrizzleClient, pool } from './client.js';

describe('Drizzle Subquery Support [pg-tikv]', () => {
  let db: ReturnType<typeof drizzle>;

  beforeAll(async () => {
    db = createDrizzleClient();
    
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_orders`);
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_products`);
    
    await db.execute(sql`
      CREATE TABLE drizzle_products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        price DOUBLE PRECISION
      )
    `);
    
    await db.execute(sql`
      CREATE TABLE drizzle_orders (
        id SERIAL PRIMARY KEY,
        product_id INTEGER,
        quantity INTEGER
      )
    `);
    
    await db.execute(sql`
      INSERT INTO drizzle_products (name, price) VALUES
      ('Widget', 10.00), ('Gadget', 25.00), ('Gizmo', 15.00)
    `);
    
    await db.execute(sql`
      INSERT INTO drizzle_orders (product_id, quantity) VALUES
      (1, 5), (2, 3), (1, 2)
    `);
  });

  afterAll(async () => {
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_orders`);
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_products`);
    await pool.end();
  });

  it('should support IN subquery', async () => {
    const result = await db.execute(sql`
      SELECT name FROM drizzle_products
      WHERE id IN (SELECT DISTINCT product_id FROM drizzle_orders)
      ORDER BY name
    `);
    
    expect(result.rows).toHaveLength(2);
  });

  it('should support scalar subquery', async () => {
    const result = await db.execute(sql`
      SELECT name,
        (SELECT SUM(quantity) FROM drizzle_orders WHERE product_id = drizzle_products.id) as total_ordered
      FROM drizzle_products
      ORDER BY name
    `);
    
    expect(result.rows).toHaveLength(3);
    const widget = result.rows.find((r: any) => r.name === 'Widget');
    expect(Number(widget.total_ordered)).toBe(7);
  });

  it('should support EXISTS subquery', async () => {
    const result = await db.execute(sql`
      SELECT name FROM drizzle_products p
      WHERE EXISTS (
        SELECT 1 FROM drizzle_orders o WHERE o.product_id = p.id
      )
      ORDER BY name
    `);
    
    expect(result.rows).toHaveLength(2);
  });
});
```

### Best Practices

1. **Isolate test data**: Use ORM-prefixed table names (`typeorm_`, `prisma_`, etc.)

2. **Clean up properly**: Always drop tables in `afterAll` to avoid conflicts

3. **Test both ORM methods and raw SQL**: Some features require `$queryRaw` or equivalent

4. **Check actual values**: Don't just check row counts, verify data correctness

5. **Handle async properly**: Always `await` database operations

6. **Use descriptive test names**: `'should support ROW_NUMBER with PARTITION BY'`

7. **Group related tests**: Use nested `describe` blocks for features

```typescript
describe('Knex Advanced Queries [pg-tikv]', () => {
  describe('window functions', () => {
    it('should support ROW_NUMBER', async () => { ... });
    it('should support RANK', async () => { ... });
  });
  
  describe('CTEs', () => {
    it('should support simple CTE', async () => { ... });
    it('should support recursive CTE', async () => { ... });
  });
});
```

## CI Integration

GitHub Actions workflow is configured in `.github/workflows/orm-tests.yml`.

The workflow:
1. Starts TiKV cluster
2. Builds and starts pg-tikv
3. Runs all ORM tests
4. Generates compatibility report

## Troubleshooting

### Connection Refused

```
Error: connect ECONNREFUSED 127.0.0.1:5433
```

Ensure pg-tikv is running:
```bash
ps aux | grep pg-tikv
```

### TiKV Not Available

```
Error: Failed to connect to TiKV
```

Start TiKV cluster:
```bash
tiup playground --mode tikv-slim
```

### Prisma Client Not Generated

```
Error: @prisma/client did not initialize yet
```

Generate Prisma client:
```bash
npx prisma generate
```

### Port Already in Use

```
Error: Address already in use (os error 98)
```

Find and kill the process:
```bash
lsof -i :5433
kill <PID>
```

Or use a different port:
```bash
PG_PORT=5434 cargo run
PG_PORT=5434 npm test
```

## Contributing

1. Add tests for new pg-tikv features
2. Ensure all ORMs have equivalent tests where possible
3. Update this README if adding new test categories
4. Run full test suite before submitting PR:
   ```bash
   npm test && npm run report
   ```
