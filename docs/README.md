# pg-tikv Documentation

A PostgreSQL-compatible distributed SQL database built on TiKV.

## Table of Contents

- [Quick Start](./quickstart.md)
- [SQL Reference](./sql-reference.md)
- [Multi-Tenancy](./multi-tenancy.md)
- [Authentication & RBAC](./authentication.md)
- [Configuration](./configuration.md)
- [Architecture](./architecture.md)

## Overview

pg-tikv provides a PostgreSQL wire protocol interface on top of TiKV's distributed key-value storage. This allows you to use standard PostgreSQL clients (psql, pgcli, language drivers) while benefiting from TiKV's horizontal scalability and strong consistency.

### Key Features

- **PostgreSQL Compatibility**: Use psql, pg_dump, pg_restore, and standard PostgreSQL drivers
- **Distributed Storage**: Data automatically distributed across TiKV nodes with Raft consensus
- **Multi-Tenancy**: Keyspace-based isolation with username routing (`tenant.user`)
- **RBAC**: Full role-based access control with CREATE USER, GRANT, REVOKE
- **Transactions**: Pessimistic transactions with BEGIN/COMMIT/ROLLBACK
- **Rich SQL**: CTEs, window functions, subqueries, JSON/JSONB support

### Supported SQL Features

| Category | Features |
|----------|----------|
| DDL | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW |
| DML | INSERT, UPDATE, DELETE with RETURNING clause |
| Queries | SELECT with WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT |
| Joins | INNER JOIN, LEFT JOIN |
| Aggregates | COUNT, SUM, AVG, MIN, MAX with GROUP BY, HAVING |
| Window Functions | ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, SUM/AVG/COUNT OVER |
| Subqueries | IN (SELECT ...), EXISTS, scalar subqueries |
| CTEs | WITH ... AS (non-recursive) |
| JSON | JSONB type, ->, ->>, @>, <@ operators |
| Auth | CREATE/ALTER/DROP ROLE, GRANT, REVOKE |

## Getting Started

```bash
# Start TiKV (requires tiup)
tiup playground --mode tikv-slim

# Build and run pg-tikv
cargo build --release
./target/release/pg-tikv

# Connect with psql
psql -h 127.0.0.1 -p 5433 -U admin
# Default password: admin
```

See [Quick Start Guide](./quickstart.md) for detailed instructions.
