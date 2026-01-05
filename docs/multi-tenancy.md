# Multi-Tenancy Guide

pg-tikv supports multi-tenancy through TiKV's Keyspace feature, providing complete data isolation between tenants.

## Overview

Each tenant's data is stored in a separate TiKV keyspace. Tenants are completely isolated:

- Separate data storage
- Separate user databases (each keyspace has its own users)
- Separate table namespaces
- No cross-tenant queries possible

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Clients                       │
│            tenant_a.admin    tenant_b.admin                 │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              ┌──────────┐        ┌──────────┐
              │ Keyspace │        │ Keyspace │
              │ tenant_a │        │ tenant_b │
              └──────────┘        └──────────┘
                    │                   │
                    └─────────┬─────────┘
                              ▼
              ┌───────────────────────────────┐
              │            TiKV               │
              │   (Distributed KV Storage)    │
              └───────────────────────────────┘
```

## Setup

### 1. Enable TiKV API v2

Create `/tmp/tikv.toml`:

```toml
[storage]
api-version = 2
enable-ttl = true
```

Start TiKV with config:

```bash
tiup playground --mode tikv-slim --kv.config /tmp/tikv.toml
```

### 2. Create Keyspaces

Find PD port from tiup output, then create keyspaces:

```bash
# Create default keyspace (for users without tenant prefix)
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create default

# Create tenant keyspaces
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create tenant_a
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create tenant_b
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace create acme_corp

# List all keyspaces
tiup ctl:v8.5.4 pd -u http://127.0.0.1:2379 keyspace list
```

### 3. Start pg-tikv

```bash
PD_ENDPOINTS=127.0.0.1:2379 ./target/release/pg-tikv
```

## Username Routing

pg-tikv uses the username to route connections to the correct keyspace.

### Username Format

```
<keyspace>.<username>
```

or

```
<keyspace>:<username>
```

### Examples

| Connection Username | Keyspace | Database User |
|---------------------|----------|---------------|
| `tenant_a.admin` | `tenant_a` | `admin` |
| `tenant_b.app` | `tenant_b` | `app` |
| `acme_corp:postgres` | `acme_corp` | `postgres` |
| `admin` | `default` | `admin` |
| `postgres` | `default` | `postgres` |

### Connecting

```bash
# Connect to tenant_a
psql -h 127.0.0.1 -p 5433 -U tenant_a.admin
# Password: admin (default)

# Connect to tenant_b
psql -h 127.0.0.1 -p 5433 -U tenant_b.admin

# Connect to default keyspace
psql -h 127.0.0.1 -p 5433 -U admin
```

## Data Isolation Example

### Tenant A

```bash
psql -h 127.0.0.1 -p 5433 -U tenant_a.admin
```

```sql
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO customers (name) VALUES ('Alice'), ('Bob');
SELECT * FROM customers;
-- Returns: Alice, Bob
```

### Tenant B

```bash
psql -h 127.0.0.1 -p 5433 -U tenant_b.admin
```

```sql
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO customers (name) VALUES ('Charlie');
SELECT * FROM customers;
-- Returns: Charlie (completely separate from Tenant A)
```

## User Management Per Tenant

Each keyspace has its own user database. Users created in one keyspace don't exist in others.

### Tenant A Users

```bash
psql -h 127.0.0.1 -p 5433 -U tenant_a.admin
```

```sql
-- Create users for tenant_a
CREATE ROLE reader WITH PASSWORD 'read123' LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;

CREATE ROLE writer WITH PASSWORD 'write123' LOGIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO writer;
```

### Tenant B Users

```bash
psql -h 127.0.0.1 -p 5433 -U tenant_b.admin
```

```sql
-- Create different users for tenant_b
CREATE ROLE analyst WITH PASSWORD 'analyst123' LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
```

### Connecting as Tenant Users

```bash
# Connect as tenant_a's reader
psql -h 127.0.0.1 -p 5433 -U tenant_a.reader
# Password: read123

# Connect as tenant_b's analyst
psql -h 127.0.0.1 -p 5433 -U tenant_b.analyst
# Password: analyst123
```

## Default Keyspace

When a username doesn't contain a separator (`.` or `:`), the connection is routed to the `default` keyspace.

```bash
# These all connect to the default keyspace
psql -h 127.0.0.1 -p 5433 -U admin
psql -h 127.0.0.1 -p 5433 -U postgres
psql -h 127.0.0.1 -p 5433 -U myuser
```

You can also set a different default keyspace via environment variable:

```bash
PG_KEYSPACE=my_default ./target/release/pg-tikv
```

## Bootstrap User

Each keyspace automatically creates a default `admin` user with password `admin` on first connection. This user has superuser privileges and can create other users.

**Security Note**: Change the admin password immediately in production:

```sql
ALTER ROLE admin WITH PASSWORD 'new_secure_password';
```

## Best Practices

### 1. Naming Conventions

Use consistent keyspace naming:

```
prod_tenant_acme
prod_tenant_globex
staging_tenant_acme
dev_shared
```

### 2. Separate Admin Accounts

Create separate admin accounts per tenant rather than using the default:

```sql
-- As admin
CREATE ROLE tenant_admin WITH PASSWORD 'secure_password' LOGIN SUPERUSER;
ALTER ROLE admin WITH PASSWORD 'disabled' NOLOGIN;
```

### 3. Application Connection Strings

Configure your application with the full username:

```python
# Python example
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    user="tenant_a.app_user",
    password="app_password",
    database="postgres"
)
```

```javascript
// Node.js example
const { Client } = require('pg');
const client = new Client({
    host: '127.0.0.1',
    port: 5433,
    user: 'tenant_a.app_user',
    password: 'app_password',
    database: 'postgres'
});
```

### 4. Monitoring

Each keyspace's activity is logged with the keyspace name:

```
INFO Extracted keyspace 'tenant_a' from username 'tenant_a.admin'
INFO Authentication successful for user 'admin' with keyspace Some("tenant_a")
```

## Limitations

1. **No Cross-Keyspace Queries**: Cannot join tables across keyspaces
2. **Keyspace Creation**: Keyspaces must be created via pd-ctl before use
3. **No Dynamic Keyspace**: Keyspace is determined at connection time and cannot be changed
4. **Single Database**: Each keyspace has a single logical database (no CREATE DATABASE support)
