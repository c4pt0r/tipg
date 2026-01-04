-- DML CRUD Tests
-- Purpose: Verify Insert, Select, Update, Delete, Where, Expressions

DROP TABLE IF EXISTS t_dml;
CREATE TABLE t_dml (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT,
    score FLOAT
);

-- 1. Insert
INSERT INTO t_dml (name, age, score) VALUES ('Alice', 20, 80.5);
INSERT INTO t_dml (name, age, score) VALUES ('Bob', 25, 90.0);
INSERT INTO t_dml (name, age, score) VALUES ('Charlie', 30, 85.5);

-- 2. Select All
SELECT * FROM t_dml;

-- 3. Select Projection
SELECT name, age FROM t_dml;

-- 4. Where Clause (Comparison)
SELECT * FROM t_dml WHERE age > 22;

-- 5. Where Clause (Logic)
SELECT * FROM t_dml WHERE age > 20 AND score < 90.0;

-- 6. Update (Atomic)
UPDATE t_dml SET score = 95.0 WHERE name = 'Alice';
SELECT * FROM t_dml WHERE name = 'Alice';

-- 7. Update with Expression
UPDATE t_dml SET age = age + 1;
SELECT name, age FROM t_dml;

-- 8. Delete
DELETE FROM t_dml WHERE name = 'Bob';
SELECT * FROM t_dml;

-- Clean up
DROP TABLE t_dml;
