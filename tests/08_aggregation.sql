-- Aggregation Tests
-- Purpose: Verify COUNT, SUM, MAX, MIN

DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product TEXT,
    amount INT,
    price FLOAT
);

INSERT INTO sales (product, amount, price) VALUES ('A', 10, 5.5);
INSERT INTO sales (product, amount, price) VALUES ('B', 5, 10.0);
INSERT INTO sales (product, amount, price) VALUES ('A', 20, 5.0);
INSERT INTO sales (product, amount, price) VALUES ('C', 10, 20.0);

-- 1. COUNT (*)
SELECT COUNT(*) FROM sales;

-- 2. SUM (amount) -> 10+5+20+10 = 45
SELECT SUM(amount) FROM sales;

-- 3. MAX/MIN (price) -> 20.0 / 5.0
SELECT MAX(price), MIN(price) FROM sales;

-- 4. Filtered SUM -> 10+20 = 30
SELECT SUM(amount) FROM sales WHERE product = 'A';

DROP TABLE sales;
