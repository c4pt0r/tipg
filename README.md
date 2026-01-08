# pg-tikv

A PostgreSQL-compatible distributed SQL database built on TiKV.


<img width="337" height="162" alt="image" src="https://github.com/user-attachments/assets/b1d14c35-efef-4ca7-8f3d-c71451602780" />


## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Clients                       │
│              (psql, pgcli, pg_dump, pg_restore)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ PostgreSQL Wire Protocol (pgwire 0.28)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      pg-tikv Server                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Protocol: Simple Query, Extended Query, COPY         │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │  SQL: Parser (sqlparser-rs) → Executor                │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │  Storage: Key Encoding, Indexes, Transactions         │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ gRPC (TiKV Pessimistic Transactions)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                          TiKV                               │
│                Distributed KV Storage (Raft)                │
└─────────────────────────────────────────────────────────────┘
```

## Features

### SQL Support

| Category | Features |
|----------|----------|
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE ADD COLUMN`, `CREATE INDEX`, `CREATE VIEW`, `DROP VIEW`, `CREATE MATERIALIZED VIEW`, `DROP MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, `SHOW TABLES` |
| **DML** | `INSERT`, `UPDATE`, `DELETE` with `RETURNING`, `SELECT` with full `WHERE` support |
| **Queries** | `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, `GROUP BY`, `HAVING`, `WITH ... AS` (CTEs), `WITH RECURSIVE` (Recursive CTEs) |
| **Joins** | `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN` |
| **Aggregates** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` |
| **Window Functions** | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`, `SUM/AVG/COUNT/MIN/MAX OVER` |
| **Expressions** | `+`, `-`, `*`, `/`, `%`, `\|\|`, `AND`, `OR`, `NOT`, comparisons |
| **Predicates** | `IN (...)`, `IN (SELECT ...)`, `EXISTS`, `BETWEEN`, `LIKE`, `ILIKE`, `IS NULL`, `IS NOT NULL`, Scalar Subqueries |
| **Functions** | String, Math, Date/Time, `CASE WHEN`, `CAST`, `COALESCE`, `NULLIF` |
| **Procedures** | `CREATE PROCEDURE`, `DROP PROCEDURE`, `CALL` |
| **Transactions** | `BEGIN`, `COMMIT`, `ROLLBACK`, `SELECT FOR UPDATE` |
| **COPY** | `COPY FROM stdin` for bulk loading, pg_restore compatible |

### Data Types

| Type | Aliases |
|------|---------|
| `BOOLEAN` | `BOOL` |
| `INTEGER` | `INT`, `INT4`, `SERIAL` |
| `BIGINT` | `INT8`, `BIGSERIAL` |
| `REAL` | `FLOAT4` |
| `DOUBLE PRECISION` | `FLOAT8` |
| `TEXT` | `VARCHAR`, `CHAR` |
| `BYTEA` | - |
| `TIMESTAMP` | `TIMESTAMPTZ` |
| `INTERVAL` | - |
| `UUID` | - |
| `JSON` | `JSONB` |

### PostgreSQL Functions

```sql
-- String
UPPER, LOWER, LENGTH, CONCAT, LEFT, RIGHT, SUBSTRING, TRIM,
LPAD, RPAD, REPLACE, REVERSE, REPEAT, SPLIT_PART, INITCAP, POSITION

-- Math  
ABS, CEIL, FLOOR, ROUND, TRUNC, SQRT, POWER, EXP, LN, LOG, SIGN, MOD, PI, RANDOM

-- Date/Time
NOW, CURRENT_TIMESTAMP, CURRENT_DATE, DATE_TRUNC, EXTRACT, TO_CHAR, AGE

-- Other
COALESCE, NULLIF, GREATEST, LEAST, gen_random_uuid()
```

## Quick Start

```bash
# 1. Start TiKV
tiup playground --mode tikv-slim

# 2. Start pg-tikv
cargo run

# 3. Connect
psql -h 127.0.0.1 -p 5433 -d postgres
```

### Example Session

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');

SELECT * FROM users WHERE name LIKE 'A%';
SELECT COUNT(*), DATE_TRUNC('day', created_at) FROM users GROUP BY DATE_TRUNC('day', created_at);

-- CTE example
WITH active_users AS (
    SELECT * FROM users WHERE created_at > NOW() - INTERVAL '7 days'
)
SELECT * FROM active_users ORDER BY name;

-- Subquery examples
SELECT * FROM users WHERE id IN (SELECT id FROM users WHERE name LIKE 'A%');
SELECT id, name, (SELECT COUNT(*) FROM users) as total_users FROM users;
SELECT * FROM users WHERE id = (SELECT MIN(id) FROM users);

-- View example
CREATE VIEW recent_users AS SELECT * FROM users WHERE created_at > NOW() - INTERVAL '30 days';
SELECT * FROM recent_users WHERE name LIKE 'A%';

-- Window function examples
SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM users;
SELECT id, name, RANK() OVER (ORDER BY name) as rank FROM users;
SELECT id, name, SUM(id) OVER (ORDER BY id) as running_total FROM users;
SELECT id, name, LAG(name) OVER (ORDER BY id) as prev_name FROM users;
SELECT id, name, LEAD(name, 1, 'N/A') OVER (ORDER BY id) as next_name FROM users;

-- RETURNING clause
UPDATE users SET name = 'Robert' WHERE name = 'Bob' RETURNING *;
DELETE FROM users WHERE id = 1 RETURNING id, name;
```

### Restore a PostgreSQL Dump

```bash
pg_restore -h 127.0.0.1 -p 5433 -d postgres --no-owner --no-privileges ./backup/
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD endpoints |
| `PG_PORT` | `5433` | PostgreSQL protocol port |
| `PG_NAMESPACE` | (empty) | Multi-tenant namespace prefix |

## Constraints

| Constraint | Status | Notes |
|------------|--------|-------|
| PRIMARY KEY | ✅ | Single and composite keys |
| NOT NULL | ✅ | Enforced on INSERT/UPDATE |
| UNIQUE | ✅ | With auto-index creation |
| CHECK | ✅ | Column-level constraints |
| FOREIGN KEY | ✅ | Full referential integrity |
| DEFAULT | ✅ | Including expressions like `NOW()` |

### Foreign Key Actions

| Action | ON DELETE | ON UPDATE |
|--------|-----------|-----------|
| CASCADE | ✅ | ✅ |
| SET NULL | ✅ | ✅ |
| SET DEFAULT | ✅ | ✅ |
| RESTRICT | ✅ | ✅ |
| NO ACTION | ✅ | ✅ |

## Stored Procedures

Basic stored procedure support with parameter passing:

```sql
-- Create a procedure
CREATE PROCEDURE update_prices(p_factor INT)
AS BEGIN
UPDATE products SET price = price * p_factor
END;

-- Call the procedure
CALL update_prices(2);

-- Drop the procedure
DROP PROCEDURE update_prices;
```

**Supported**: `CREATE PROCEDURE`, `DROP PROCEDURE`, `CALL`, parameters with type-aware substitution.

**Limitations**: No OUT/INOUT parameters, no control flow (IF/WHILE), no exception handling.

## Project Structure

```
src/
├── main.rs              # TCP server entry point
├── protocol/
│   ├── mod.rs
│   └── handler.rs       # pgwire handlers (query, COPY)
├── sql/
│   ├── mod.rs
│   ├── parser.rs        # SQL parsing
│   ├── executor.rs      # Query execution
│   ├── expr.rs          # Expression evaluation
│   ├── aggregate.rs     # Aggregation functions
│   ├── session.rs       # Transaction management
│   ├── information_schema.rs  # information_schema virtual tables
│   └── result.rs        # Result types
├── storage/
│   ├── mod.rs
│   ├── encoding.rs      # Key/value encoding
│   └── tikv_store.rs    # TiKV client wrapper
└── types/
    └── mod.rs           # Value, Row, Schema types

tests/                   # SQL integration tests
orm-tests/               # ORM compatibility tests (TypeORM, Prisma, etc.)
```

## ORM Compatibility

pg-tikv is tested against popular TypeScript/JavaScript ORMs:

| ORM | Tests | Status |
|-----|-------|--------|
| TypeORM | 147 | ✅ All passing |
| Prisma | 89 | ✅ All passing |
| Sequelize | 87 | ✅ All passing |
| Knex.js | 97 | ✅ All passing |
| Drizzle | 75 | ✅ All passing |

**Total: 498 ORM tests passing**

Features tested include:
- Connection pooling and error handling
- CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- Transactions (BEGIN, COMMIT, ROLLBACK, isolation levels)
- Relations (foreign keys, JOINs, eager loading)
- Advanced queries (window functions, CTEs, subqueries, views)
- JSONB operations
- Schema introspection via `information_schema`

See [orm-tests/README.md](orm-tests/README.md) for details.

## Tests

```bash
# Unit tests (184 tests)
cargo test

# Integration tests (requires running server)
python3 scripts/integration_test.py

# ORM tests (requires running server)
cd orm-tests && npm test
```

| Test Suite | Coverage |
|------------|----------|
| DDL | CREATE, DROP, ALTER, TRUNCATE, Views |
| DML | INSERT, UPDATE, DELETE, SELECT, RETURNING |
| Transactions | BEGIN, COMMIT, ROLLBACK, SELECT FOR UPDATE |
| Queries | WHERE, ORDER BY, LIMIT, GROUP BY, HAVING, JOIN, CTEs |
| Subqueries | IN (SELECT ...), EXISTS, NOT EXISTS, Scalar Subqueries |
| Window Functions | ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, SUM/AVG/COUNT OVER |
| Functions | String, Math, Date, CASE, CAST |
| Indexes | CREATE INDEX, Index Scan optimization |
| Types | UUID, INTERVAL, TIMESTAMP, JSONB |
| Compatibility | COPY protocol, pg_restore, Extended Query, ORMs |

## License

Apache 2.0
