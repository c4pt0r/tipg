-- Group By Tests
-- Purpose: Verify Group By Aggregation

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer TEXT,
    amount INT
);

INSERT INTO orders (customer, amount) VALUES ('Alice', 10);
INSERT INTO orders (customer, amount) VALUES ('Bob', 20);
INSERT INTO orders (customer, amount) VALUES ('Alice', 30);
INSERT INTO orders (customer, amount) VALUES ('Bob', 5);
INSERT INTO orders (customer, amount) VALUES ('Charlie', 50);

-- 1. Simple Group By (Alice: 40, Bob: 25, Charlie: 50)
-- Note: Order is not guaranteed without ORDER BY
SELECT customer, SUM(amount) FROM orders GROUP BY customer;

-- 2. Group By with multiple aggregates
SELECT customer, COUNT(*), MAX(amount) FROM orders GROUP BY customer;

DROP TABLE orders;
