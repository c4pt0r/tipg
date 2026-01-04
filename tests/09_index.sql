-- Index Tests
-- Purpose: Verify CREATE INDEX, Backfill, and Unique Constraints

DROP TABLE IF EXISTS idx_test;
CREATE TABLE idx_test (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT
);

INSERT INTO idx_test (name, age) VALUES ('Alice', 20);
INSERT INTO idx_test (name, age) VALUES ('Bob', 30);
INSERT INTO idx_test (name, age) VALUES ('Alice', 25);

-- 1. Create Index (Backfill)
CREATE INDEX idx_name ON idx_test (name);

-- 2. Create Unique Index (Backfill)
CREATE UNIQUE INDEX idx_age ON idx_test (age);

-- 3. Insert duplicate unique value (should fail)
-- 'Alice' has age 20. Inserting Charlie with 20 should fail.
INSERT INTO idx_test (name, age) VALUES ('Charlie', 20);

-- 4. Select
SELECT * FROM idx_test;

DROP TABLE idx_test;
