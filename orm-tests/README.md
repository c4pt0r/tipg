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
