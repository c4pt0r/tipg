-- Test information_schema support for ORM compatibility
-- This tests the virtual information_schema tables that ORMs use for introspection

-- Setup: Create tables with various constraints
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    total DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Test information_schema.schemata
SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('public', 'information_schema') ORDER BY schema_name;
-- Expected:
-- information_schema
-- public

-- Test information_schema.tables
SELECT table_name, table_type FROM information_schema.tables WHERE table_name IN ('customers', 'orders') ORDER BY table_name;
-- Expected:
-- customers | BASE TABLE
-- orders    | BASE TABLE

-- Test information_schema.columns
SELECT table_name, column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'customers' 
ORDER BY ordinal_position;
-- Expected:
-- customers | id     | integer | NO  | (serial)
-- customers | name   | text    | NO  | (null)
-- customers | email  | text    | YES | (null)
-- customers | status | text    | YES | 'active'

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_name = 'orders' 
ORDER BY ordinal_position;
-- Expected:
-- orders | id          | integer   | NO
-- orders | customer_id | integer   | NO
-- orders | total       | double... | NO
-- orders | created_at  | timestamp | YES

-- Test information_schema.table_constraints
SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name = 'customers' 
ORDER BY constraint_type, constraint_name;
-- Expected: CHECK, PRIMARY KEY, UNIQUE constraints

SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name = 'orders' 
ORDER BY constraint_type, constraint_name;
-- Expected: FOREIGN KEY, PRIMARY KEY constraints

-- Test information_schema.key_column_usage
SELECT constraint_name, column_name 
FROM information_schema.key_column_usage 
WHERE table_name = 'customers' 
ORDER BY constraint_name;
-- Expected: PK and UNIQUE key columns

SELECT constraint_name, column_name, position_in_unique_constraint
FROM information_schema.key_column_usage 
WHERE table_name = 'orders' AND constraint_name LIKE '%fkey%'
ORDER BY constraint_name;
-- Expected: FK column info

-- Test information_schema.referential_constraints
SELECT constraint_name, unique_constraint_name, delete_rule, update_rule
FROM information_schema.referential_constraints
WHERE constraint_name LIKE '%orders%' OR constraint_name LIKE '%fkey%';
-- Expected: FK rules (CASCADE, etc.)

-- Test information_schema.check_constraints
SELECT constraint_name, check_clause
FROM information_schema.check_constraints
WHERE constraint_name LIKE '%status%';
-- Expected: CHECK constraint expression

-- Cleanup
DROP TABLE orders CASCADE;
DROP TABLE customers CASCADE;

-- Final verification: empty information_schema.tables after cleanup
SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('customers', 'orders');
-- Expected: 0
