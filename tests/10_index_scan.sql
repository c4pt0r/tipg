-- Index Scan Optimization Tests
-- Purpose: Verify optimizer chooses Index Scan for point lookups

DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT
);

CREATE INDEX idx_name_age ON users (name, age);

INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);
INSERT INTO users (id, name, age) VALUES (3, 'Alice', 25);

-- 1. Full Match Index Scan (Should return id=1)
SELECT * FROM users WHERE name = 'Alice' AND age = 20;

-- 2. No Match Index Scan (Should return empty)
SELECT * FROM users WHERE name = 'Alice' AND age = 99;

-- 3. Verify data correctness via seq scan fallback
SELECT * FROM users WHERE age > 20 ORDER BY age;

DROP TABLE users;
