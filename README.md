# pg-tikv

A PostgreSQL-compatible distributed SQL database built on TiKV.

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
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE ADD COLUMN`, `CREATE INDEX`, `SHOW TABLES` |
| **DML** | `INSERT`, `UPDATE`, `DELETE` with `RETURNING`, `SELECT` with full `WHERE` support |
| **Queries** | `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, `GROUP BY`, `HAVING`, `WITH ... AS` (CTEs) |
| **Joins** | `INNER JOIN`, `LEFT JOIN` with `ON` clause |
| **Aggregates** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` |
| **Expressions** | `+`, `-`, `*`, `/`, `%`, `\|\|`, `AND`, `OR`, `NOT`, comparisons |
| **Predicates** | `IN (...)`, `IN (SELECT ...)`, `EXISTS`, `BETWEEN`, `LIKE`, `ILIKE`, `IS NULL`, `IS NOT NULL` |
| **Functions** | String, Math, Date/Time, `CASE WHEN`, `CAST`, `COALESCE`, `NULLIF` |
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

-- Subquery example
SELECT * FROM users WHERE id IN (SELECT id FROM users WHERE name LIKE 'A%');

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

## Limitations

| Feature | Status | Notes |
|---------|--------|-------|
| RIGHT/FULL OUTER JOIN | ❌ | Only INNER and LEFT JOIN |
| Window Functions | ❌ | `ROW_NUMBER()`, `RANK()`, etc. |
| Recursive CTEs | ❌ | `WITH RECURSIVE` not supported |
| Scalar Subqueries | ❌ | `SELECT (SELECT ...)` not supported |
| Foreign Keys | ❌ | Parsed but not enforced |
| CHECK Constraints | ❌ | Parsed but not enforced |
| Views | ❌ | Not implemented |
| Stored Procedures | ❌ | Not implemented |

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
│   └── result.rs        # Result types
├── storage/
│   ├── mod.rs
│   ├── encoding.rs      # Key/value encoding
│   └── tikv_store.rs    # TiKV client wrapper
└── types/
    └── mod.rs           # Value, Row, Schema types

tests/
├── 01_ddl_basic.sql     # DDL tests
├── 02_dml_crud.sql      # CRUD tests
├── ...
├── 17_dvdrental.sql     # Integration tests
└── dvdrental/           # Sample database for pg_restore
```

## Tests

```bash
# Unit tests (67 tests)
cargo test

# Integration tests (requires running server)
./run_tests.sh
```

| Test Suite | Coverage |
|------------|----------|
| DDL | CREATE, DROP, ALTER, TRUNCATE |
| DML | INSERT, UPDATE, DELETE, SELECT, RETURNING |
| Transactions | BEGIN, COMMIT, ROLLBACK, SELECT FOR UPDATE |
| Queries | WHERE, ORDER BY, LIMIT, GROUP BY, HAVING, JOIN, CTEs |
| Subqueries | IN (SELECT ...), EXISTS, NOT EXISTS |
| Functions | String, Math, Date, CASE, CAST |
| Indexes | CREATE INDEX, Index Scan optimization |
| Types | UUID, INTERVAL, TIMESTAMP |
| Compatibility | COPY protocol, pg_restore, Extended Query |

## License

MIT
