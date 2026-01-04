-- Schema Evolution Tests
-- Purpose: Verify ALTER TABLE ADD COLUMN and Lazy Materialization

DROP TABLE IF EXISTS t_evolve;
CREATE TABLE t_evolve (
    id INT PRIMARY KEY,
    v1 TEXT
);

-- 1. Insert initial data
INSERT INTO t_evolve (id, v1) VALUES (1, 'old');

-- 2. Add column with default
ALTER TABLE t_evolve ADD COLUMN v2 TEXT DEFAULT 'new_default';

-- 3. Verify old data has default value (Lazy Check)
SELECT * FROM t_evolve WHERE id = 1;

-- 4. Insert new data (using default)
INSERT INTO t_evolve (id, v1) VALUES (2, 'new_implicit');

-- 5. Insert new data (override default)
INSERT INTO t_evolve (id, v1, v2) VALUES (3, 'new_explicit', 'overridden');

-- 6. Verify all data
SELECT * FROM t_evolve ORDER BY id; -- Note: ORDER BY ignored by server currently but psql might handle? No, server does full scan order.

-- Clean up
DROP TABLE t_evolve;
