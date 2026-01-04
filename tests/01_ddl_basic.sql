-- DDL Basic Tests
-- Purpose: Verify Create, Drop, Truncate, IF EXISTS

-- 1. Clean start
DROP TABLE IF EXISTS t_ddl;

-- 2. Create table
CREATE TABLE t_ddl (id INT PRIMARY KEY, val TEXT);

-- 3. Create duplicate (should fail)
CREATE TABLE t_ddl (id INT PRIMARY KEY);

-- 4. Create IF NOT EXISTS (should succeed/ignore)
CREATE TABLE IF NOT EXISTS t_ddl (id INT PRIMARY KEY);

-- 5. Insert some data for truncate test
INSERT INTO t_ddl (id, val) VALUES (1, 'a');
INSERT INTO t_ddl (id, val) VALUES (2, 'b');
SELECT * FROM t_ddl;

-- 6. Truncate
TRUNCATE TABLE t_ddl;
SELECT * FROM t_ddl; -- Should be empty

-- 7. Drop table
DROP TABLE t_ddl;

-- 8. Drop IF EXISTS
DROP TABLE IF EXISTS t_ddl;
