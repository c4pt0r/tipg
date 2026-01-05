# Authentication & RBAC

pg-tikv implements PostgreSQL-compatible authentication and role-based access control (RBAC).

## Overview

- **Password Authentication**: Cleartext password authentication
- **Per-Keyspace Users**: Each keyspace has its own user database
- **RBAC**: Role-based access control with privileges on tables
- **Superuser**: Full access to all operations
- **Bootstrap User**: Default `admin` user created automatically

## Default User

Each keyspace automatically creates a default superuser on first connection:

| Username | Password | Privileges |
|----------|----------|------------|
| `admin` | `admin` | SUPERUSER, LOGIN, CREATEDB, CREATEROLE |

**Important**: Change the default password in production:

```sql
ALTER ROLE admin WITH PASSWORD 'your_secure_password';
```

## Creating Users

### Basic User

```sql
CREATE ROLE username WITH PASSWORD 'password' LOGIN;
```

### User with Options

```sql
CREATE ROLE username WITH 
    PASSWORD 'password' 
    LOGIN 
    CREATEDB 
    CREATEROLE;
```

### Superuser

```sql
CREATE ROLE admin_user WITH 
    PASSWORD 'password' 
    LOGIN 
    SUPERUSER;
```

### Role Options

| Option | Description |
|--------|-------------|
| `PASSWORD 'xxx'` | Set password |
| `LOGIN` | Allow login (required for connecting) |
| `NOLOGIN` | Disallow login (for roles only) |
| `SUPERUSER` | Grant all privileges |
| `NOSUPERUSER` | Normal user (default) |
| `CREATEDB` | Allow creating databases |
| `NOCREATEDB` | Disallow creating databases (default) |
| `CREATEROLE` | Allow creating other roles |
| `NOCREATEROLE` | Disallow creating roles (default) |

## Modifying Users

### Change Password

```sql
ALTER ROLE username WITH PASSWORD 'new_password';
```

### Grant/Revoke Options

```sql
ALTER ROLE username WITH SUPERUSER;
ALTER ROLE username WITH NOSUPERUSER;
ALTER ROLE username WITH CREATEDB;
ALTER ROLE username WITH NOLOGIN;
```

### Rename User

```sql
ALTER ROLE old_name RENAME TO new_name;
```

## Deleting Users

```sql
DROP ROLE username;
DROP ROLE IF EXISTS username;
```

## Privileges

### Privilege Types

| Privilege | Applies To | Description |
|-----------|------------|-------------|
| `SELECT` | Tables | Read data |
| `INSERT` | Tables | Insert rows |
| `UPDATE` | Tables | Modify rows |
| `DELETE` | Tables | Delete rows |
| `TRUNCATE` | Tables | Truncate table |
| `REFERENCES` | Tables | Create foreign keys |
| `TRIGGER` | Tables | Create triggers |
| `CREATE` | Schemas | Create objects |
| `CONNECT` | Databases | Connect to database |
| `USAGE` | Schemas, Sequences | Use schema/sequence |
| `EXECUTE` | Functions | Execute function |
| `ALL` | Any | All applicable privileges |

### Granting Privileges

#### On Specific Table

```sql
GRANT SELECT ON users TO reader;
GRANT SELECT, INSERT ON users TO writer;
GRANT ALL ON users TO admin_user;
```

#### On All Tables in Schema

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
```

#### With Grant Option

```sql
-- User can grant this privilege to others
GRANT SELECT ON users TO manager WITH GRANT OPTION;
```

### Revoking Privileges

```sql
REVOKE DELETE ON users FROM writer;
REVOKE ALL ON users FROM temp_user;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM reader;
```

## Roles (Groups)

Roles can be used as groups to manage privileges for multiple users.

### Create Role Group

```sql
-- Create a role without login (group)
CREATE ROLE readonly NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;

-- Create users and add to group
CREATE ROLE user1 WITH PASSWORD 'pass1' LOGIN;
CREATE ROLE user2 WITH PASSWORD 'pass2' LOGIN;

-- Add users to group (grant role membership)
ALTER ROLE readonly ADD MEMBER user1;
ALTER ROLE readonly ADD MEMBER user2;
```

### Remove from Group

```sql
ALTER ROLE readonly DROP MEMBER user1;
```

## Common Patterns

### Read-Only User

```sql
CREATE ROLE readonly WITH PASSWORD 'readonly_pass' LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
```

### Application User

```sql
CREATE ROLE app_user WITH PASSWORD 'app_pass' LOGIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
```

### Admin User

```sql
CREATE ROLE db_admin WITH PASSWORD 'admin_pass' LOGIN SUPERUSER CREATEDB CREATEROLE;
```

### Analytics User

```sql
CREATE ROLE analyst WITH PASSWORD 'analyst_pass' LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
-- Optionally grant specific tables
GRANT SELECT ON orders TO analyst;
GRANT SELECT ON users TO analyst;
```

## Multi-Tenant User Management

Each keyspace has its own user database. Users must be created per keyspace.

### Example: Two Tenants

```bash
# Connect to tenant_a
psql -h 127.0.0.1 -p 5433 -U tenant_a.admin
```

```sql
-- Create tenant_a users
CREATE ROLE app WITH PASSWORD 'tenant_a_app_pass' LOGIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app;
```

```bash
# Connect to tenant_b
psql -h 127.0.0.1 -p 5433 -U tenant_b.admin
```

```sql
-- Create tenant_b users (completely separate)
CREATE ROLE app WITH PASSWORD 'tenant_b_app_pass' LOGIN;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app;
```

## Fallback Password

For compatibility and testing, you can set a fallback password via environment variable:

```bash
PG_PASSWORD=master_password ./target/release/pg-tikv
```

This password works for any user if database authentication fails. **Do not use in production.**

## Security Best Practices

### 1. Change Default Password

```sql
ALTER ROLE admin WITH PASSWORD 'strong_random_password_here';
```

### 2. Use Strong Passwords

```sql
-- Good: Long, random
CREATE ROLE app WITH PASSWORD 'xK9#mP2$vL5@nQ8&' LOGIN;

-- Bad: Weak
CREATE ROLE app WITH PASSWORD 'password123' LOGIN;
```

### 3. Principle of Least Privilege

```sql
-- Only grant what's needed
GRANT SELECT ON orders TO reporting_user;
-- NOT: GRANT ALL ON ALL TABLES TO reporting_user;
```

### 4. Separate Users Per Application

```sql
-- Each service gets its own user
CREATE ROLE api_service WITH PASSWORD 'pass1' LOGIN;
CREATE ROLE worker_service WITH PASSWORD 'pass2' LOGIN;
CREATE ROLE admin_service WITH PASSWORD 'pass3' LOGIN;
```

### 5. Audit User Creation

Keep track of all users and their privileges:

```sql
-- List all users (future feature)
-- Currently users are stored in TiKV, 
-- use CREATE ROLE statements as documentation
```

## Limitations

1. **Password Storage**: Passwords are hashed with SHA256, but transmitted in cleartext (use TLS in production)
2. **No Row-Level Security**: Cannot restrict access to specific rows
3. **No Column-Level Privileges**: Privileges are at table level
4. **No GRANT ON DATABASE**: All tables are in a single logical database
5. **No pg_catalog**: System catalogs are not implemented
