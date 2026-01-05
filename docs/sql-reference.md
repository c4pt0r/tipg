# SQL Reference

## Data Types

| Type | Aliases | Description |
|------|---------|-------------|
| `BOOLEAN` | `BOOL` | True/false value |
| `INTEGER` | `INT`, `INT4` | 32-bit signed integer |
| `BIGINT` | `INT8` | 64-bit signed integer |
| `SERIAL` | - | Auto-incrementing 32-bit integer |
| `BIGSERIAL` | - | Auto-incrementing 64-bit integer |
| `REAL` | `FLOAT4` | 32-bit floating point |
| `DOUBLE PRECISION` | `FLOAT8` | 64-bit floating point |
| `TEXT` | `VARCHAR`, `CHAR` | Variable-length string |
| `BYTEA` | - | Binary data |
| `TIMESTAMP` | `TIMESTAMPTZ` | Date and time |
| `INTERVAL` | - | Time interval |
| `UUID` | - | Universally unique identifier |
| `JSON` | - | JSON data (text storage) |
| `JSONB` | - | JSON data (binary, normalized) |

## DDL Statements

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column_name data_type [PRIMARY KEY] [NOT NULL] [UNIQUE] [DEFAULT expr],
    ...
    [PRIMARY KEY (col1, col2, ...)]
);

CREATE TABLE IF NOT EXISTS table_name (...);
```

Examples:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    user_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    PRIMARY KEY (user_id, product_id)
);
```

### ALTER TABLE

```sql
ALTER TABLE table_name ADD COLUMN column_name data_type [options];
```

Example:

```sql
ALTER TABLE users ADD COLUMN phone TEXT;
```

### DROP TABLE

```sql
DROP TABLE table_name;
DROP TABLE IF EXISTS table_name;
```

### TRUNCATE TABLE

```sql
TRUNCATE TABLE table_name;
```

### CREATE INDEX

```sql
CREATE INDEX index_name ON table_name (column1, column2, ...);
CREATE UNIQUE INDEX index_name ON table_name (column);
CREATE INDEX IF NOT EXISTS index_name ON table_name (column);
```

### DROP INDEX

```sql
DROP INDEX index_name;
DROP INDEX IF EXISTS index_name;
```

### CREATE VIEW

```sql
CREATE VIEW view_name AS SELECT ...;
CREATE OR REPLACE VIEW view_name AS SELECT ...;
```

Example:

```sql
CREATE VIEW active_users AS
    SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days';
```

### DROP VIEW

```sql
DROP VIEW view_name;
DROP VIEW IF EXISTS view_name;
```

## DML Statements

### INSERT

```sql
INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...) RETURNING *;
INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...) RETURNING col1, col2;
```

Examples:

```sql
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com') RETURNING id, name;
```

### UPDATE

```sql
UPDATE table_name SET col1 = val1, col2 = val2 WHERE condition;
UPDATE table_name SET col1 = val1 WHERE condition RETURNING *;
```

Examples:

```sql
UPDATE users SET age = age + 1 WHERE id = 1;
UPDATE users SET name = 'Robert' WHERE name = 'Bob' RETURNING *;
```

### DELETE

```sql
DELETE FROM table_name WHERE condition;
DELETE FROM table_name WHERE condition RETURNING *;
```

Examples:

```sql
DELETE FROM users WHERE id = 1;
DELETE FROM users WHERE age < 18 RETURNING id, name;
```

## SELECT Queries

### Basic SELECT

```sql
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name;
SELECT DISTINCT col1 FROM table_name;
SELECT col1 AS alias FROM table_name;
```

### WHERE Clause

```sql
SELECT * FROM users WHERE age > 18;
SELECT * FROM users WHERE name = 'Alice' AND age >= 21;
SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob';
SELECT * FROM users WHERE age BETWEEN 18 AND 30;
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name ILIKE 'alice';  -- case-insensitive
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE id IN (1, 2, 3);
```

### ORDER BY, LIMIT, OFFSET

```sql
SELECT * FROM users ORDER BY name ASC;
SELECT * FROM users ORDER BY created_at DESC;
SELECT * FROM users ORDER BY age DESC, name ASC;
SELECT * FROM users ORDER BY name LIMIT 10;
SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 20;
```

### GROUP BY and HAVING

```sql
SELECT department, COUNT(*) FROM employees GROUP BY department;
SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 50000;
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT email) FROM users;
SELECT SUM(amount) FROM orders;
SELECT AVG(age) FROM users;
SELECT MIN(created_at), MAX(created_at) FROM users;
```

### JOINs

```sql
-- INNER JOIN
SELECT u.name, o.product 
FROM users u 
INNER JOIN orders o ON u.id = o.user_id;

-- LEFT JOIN
SELECT u.name, o.product 
FROM users u 
LEFT JOIN orders o ON u.id = o.user_id;
```

### Subqueries

```sql
-- IN subquery
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- EXISTS subquery
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- Scalar subquery
SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count FROM users;
```

### Common Table Expressions (CTEs)

```sql
WITH active_users AS (
    SELECT * FROM users WHERE last_login > NOW() - INTERVAL '30 days'
)
SELECT * FROM active_users WHERE age > 21;

-- Multiple CTEs
WITH 
    recent_orders AS (SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'),
    high_value AS (SELECT * FROM recent_orders WHERE amount > 1000)
SELECT * FROM high_value;
```

### Window Functions

```sql
-- ROW_NUMBER
SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) AS rn FROM users;

-- RANK / DENSE_RANK
SELECT name, score, RANK() OVER (ORDER BY score DESC) FROM players;
SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) FROM players;

-- LEAD / LAG
SELECT name, 
       LAG(name) OVER (ORDER BY id) AS prev_name,
       LEAD(name) OVER (ORDER BY id) AS next_name 
FROM users;

-- Running totals
SELECT name, amount, SUM(amount) OVER (ORDER BY created_at) AS running_total FROM orders;

-- Partitioned windows
SELECT department, name, salary,
       AVG(salary) OVER (PARTITION BY department) AS dept_avg
FROM employees;
```

## JSON Operations

### Creating JSON Data

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    data JSONB
);

INSERT INTO events (data) VALUES ('{"type": "click", "page": "/home"}');
INSERT INTO events (data) VALUES ('{"type": "view", "page": "/products", "count": 5}');
```

### JSON Operators

```sql
-- Extract as JSON
SELECT data->'type' FROM events;

-- Extract as text
SELECT data->>'type' FROM events;

-- Nested extraction
SELECT data->'user'->>'name' FROM events;

-- Array index
SELECT data->'items'->0 FROM events;

-- Containment
SELECT * FROM events WHERE data @> '{"type": "click"}';
SELECT * FROM events WHERE '{"type": "click"}' <@ data;
```

### JSON Casting

```sql
SELECT '{"a": 1}'::json;
SELECT '{"a": 1}'::jsonb;
```

## Transaction Control

```sql
BEGIN;
-- statements
COMMIT;

BEGIN;
-- statements
ROLLBACK;

-- Row locking
SELECT * FROM users WHERE id = 1 FOR UPDATE;
```

## Functions

### String Functions

```sql
SELECT UPPER('hello');                    -- HELLO
SELECT LOWER('HELLO');                    -- hello
SELECT LENGTH('hello');                   -- 5
SELECT CONCAT('hello', ' ', 'world');     -- hello world
SELECT 'hello' || ' ' || 'world';         -- hello world
SELECT LEFT('hello', 2);                  -- he
SELECT RIGHT('hello', 2);                 -- lo
SELECT SUBSTRING('hello', 2, 3);          -- ell
SELECT TRIM('  hello  ');                 -- hello
SELECT LTRIM('  hello');                  -- hello
SELECT RTRIM('hello  ');                  -- hello
SELECT LPAD('42', 5, '0');                -- 00042
SELECT RPAD('42', 5, '0');                -- 42000
SELECT REPLACE('hello', 'l', 'L');        -- heLLo
SELECT REVERSE('hello');                  -- olleh
SELECT REPEAT('ab', 3);                   -- ababab
SELECT SPLIT_PART('a,b,c', ',', 2);       -- b
SELECT INITCAP('hello world');            -- Hello World
SELECT POSITION('lo' IN 'hello');         -- 4
```

### Math Functions

```sql
SELECT ABS(-5);                           -- 5
SELECT CEIL(4.2);                         -- 5
SELECT FLOOR(4.8);                        -- 4
SELECT ROUND(4.567, 2);                   -- 4.57
SELECT TRUNC(4.567, 1);                   -- 4.5
SELECT SQRT(16);                          -- 4
SELECT POWER(2, 10);                      -- 1024
SELECT EXP(1);                            -- 2.718...
SELECT LN(2.718);                         -- ~1
SELECT LOG(100);                          -- 2
SELECT SIGN(-5);                          -- -1
SELECT MOD(10, 3);                        -- 1
SELECT PI();                              -- 3.14159...
SELECT RANDOM();                          -- 0.0 to 1.0
SELECT GREATEST(1, 5, 3);                 -- 5
SELECT LEAST(1, 5, 3);                    -- 1
```

### Date/Time Functions

```sql
SELECT NOW();                             -- Current timestamp
SELECT CURRENT_TIMESTAMP;                 -- Current timestamp
SELECT CURRENT_DATE;                      -- Current date
SELECT DATE_TRUNC('day', NOW());          -- Truncate to day
SELECT DATE_TRUNC('month', NOW());        -- Truncate to month
SELECT EXTRACT(YEAR FROM NOW());          -- Year component
SELECT EXTRACT(MONTH FROM NOW());         -- Month component
SELECT AGE(NOW(), '2020-01-01');          -- Interval between dates
SELECT TO_CHAR(NOW(), 'YYYY-MM-DD');      -- Format timestamp
```

### Conditional Functions

```sql
SELECT COALESCE(null, 'default');         -- default
SELECT NULLIF(a, b);                      -- null if a = b
SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END;
```

### UUID Functions

```sql
SELECT gen_random_uuid();                 -- Generate random UUID
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
```

## User Management

See [Authentication & RBAC](./authentication.md) for complete user management documentation.

```sql
-- Create user
CREATE ROLE username WITH PASSWORD 'password' LOGIN;

-- Grant privileges
GRANT SELECT, INSERT ON table_name TO username;
GRANT ALL ON ALL TABLES IN SCHEMA public TO username;

-- Revoke privileges
REVOKE DELETE ON table_name FROM username;

-- Drop user
DROP ROLE username;
```

## Utility Statements

```sql
SHOW TABLES;                              -- List all tables
```
