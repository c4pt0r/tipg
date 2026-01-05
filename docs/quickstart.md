# Quick Start Guide

## Prerequisites

- Rust 1.70+ with Cargo
- TiUP (TiKV package manager)
- PostgreSQL client (psql)

### Install TiUP

```bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.bashrc
```

### Install PostgreSQL Client

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-client

# macOS
brew install libpq
```

## Starting TiKV

### Option 1: Simple Development Setup

```bash
tiup playground --mode tikv-slim
```

This starts a single-node TiKV cluster. Note the PD endpoint from the output (e.g., `127.0.0.1:2379`).

### Option 2: With Keyspace Support (Multi-Tenancy)

Create a config file `/tmp/tikv.toml`:

```toml
[storage]
api-version = 2
enable-ttl = true
```

Start with config:

```bash
tiup playground --mode tikv-slim --kv.config /tmp/tikv.toml
```

Create keyspaces for tenants:

```bash
# Find PD port from tiup output
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create default
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create tenant_a
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create tenant_b
```

## Building pg-tikv

```bash
git clone https://github.com/c4pt0r/tipg.git
cd tipg
cargo build --release
```

## Running pg-tikv

### Basic Start

```bash
./target/release/pg-tikv
```

### With Custom Configuration

```bash
PD_ENDPOINTS=127.0.0.1:2379 \
PG_PORT=5433 \
PG_PASSWORD=fallback_password \
./target/release/pg-tikv
```

## Connecting

### Basic Connection

```bash
psql -h 127.0.0.1 -p 5433 -U admin
# Password: admin (default)
```

### Multi-Tenant Connection

```bash
# Connect to tenant_a keyspace
psql -h 127.0.0.1 -p 5433 -U tenant_a.admin
# Password: admin

# Connect to tenant_b keyspace
psql -h 127.0.0.1 -p 5433 -U tenant_b.admin
# Password: admin
```

## First Steps

```sql
-- Create a table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO users (name, email) VALUES 
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');

-- Query data
SELECT * FROM users WHERE name LIKE 'A%';

-- Create index
CREATE INDEX idx_users_email ON users (email);

-- Show tables
SHOW TABLES;
```

## Using Transactions

```sql
BEGIN;
INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com');
UPDATE users SET name = 'Charles' WHERE name = 'Charlie';
COMMIT;

-- Or rollback
BEGIN;
DELETE FROM users WHERE id = 1;
ROLLBACK;  -- Changes discarded
```

## Creating Users

```sql
-- Create a read-only user
CREATE ROLE reader WITH PASSWORD 'secret' LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;

-- Create an admin user
CREATE ROLE app_admin WITH PASSWORD 'admin123' LOGIN SUPERUSER;

-- Connect as new user
-- psql -h 127.0.0.1 -p 5433 -U tenant_a.reader
```

## Loading Existing Data

pg-tikv supports pg_restore for loading PostgreSQL dumps:

```bash
pg_restore -h 127.0.0.1 -p 5433 -U admin -d postgres \
    --no-owner --no-privileges ./backup/
```

## Next Steps

- [SQL Reference](./sql-reference.md) - Complete SQL syntax reference
- [Multi-Tenancy](./multi-tenancy.md) - Keyspace isolation and routing
- [Authentication](./authentication.md) - User management and RBAC
- [Configuration](./configuration.md) - Environment variables and options
