-- Subqueries Tests
-- Purpose: Verify IN (SELECT), EXISTS, NOT EXISTS, and scalar subqueries

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT,
    vip BOOLEAN DEFAULT FALSE
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    amount INT,
    status TEXT
);

INSERT INTO customers (name, vip) VALUES ('Alice', TRUE);
INSERT INTO customers (name, vip) VALUES ('Bob', FALSE);
INSERT INTO customers (name, vip) VALUES ('Charlie', TRUE);
INSERT INTO customers (name, vip) VALUES ('Diana', FALSE);

INSERT INTO orders (customer_id, amount, status) VALUES (1, 100, 'completed');
INSERT INTO orders (customer_id, amount, status) VALUES (1, 200, 'completed');
INSERT INTO orders (customer_id, amount, status) VALUES (2, 50, 'pending');
INSERT INTO orders (customer_id, amount, status) VALUES (3, 150, 'completed');

-- 1. IN (SELECT ...) - customers with orders
SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders);

-- 2. IN (SELECT ...) - VIP customers with completed orders
SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE status = 'completed') AND vip = TRUE;

-- 3. NOT IN (SELECT ...) - customers without orders
SELECT * FROM customers WHERE id NOT IN (SELECT customer_id FROM orders);

-- 4. EXISTS - customers who have at least one order
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- 5. NOT EXISTS - customers with no orders
SELECT * FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- 6. EXISTS with additional conditions
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100);

-- 7. Scalar subquery in SELECT - total order count
SELECT id, name, (SELECT COUNT(*) FROM orders) as total_orders FROM customers;

-- 8. Scalar subquery in SELECT - max order amount
SELECT id, name, (SELECT MAX(amount) FROM orders) as max_order FROM customers;

-- 9. Scalar subquery in WHERE - customer with highest order
SELECT * FROM orders WHERE amount = (SELECT MAX(amount) FROM orders);

-- 10. Scalar subquery in WHERE - orders above average
SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders);

-- 11. Scalar subquery returning NULL (empty result)
SELECT id, name, (SELECT amount FROM orders WHERE customer_id = 999) as no_order FROM customers WHERE id = 1;

-- 12. Multiple scalar subqueries
SELECT id, name, 
       (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) as order_count,
       (SELECT SUM(amount) FROM orders WHERE customer_id = customers.id) as total_amount
FROM customers;

-- 13. Nested IN subqueries
SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE vip = TRUE);

-- 14. Subquery with ORDER BY and LIMIT
SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders ORDER BY amount DESC LIMIT 2);

-- 15. IN with multiple columns comparison simulation
SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE name LIKE 'A%' OR name LIKE 'C%');

-- 16. Complex EXISTS with aggregation in outer query
SELECT c.name, COUNT(o.id) as order_count
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
GROUP BY c.name;

DROP TABLE orders;
DROP TABLE customers;

-- Test scalar subquery edge cases
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT,
    price INT,
    category TEXT
);

INSERT INTO products (name, price, category) VALUES ('Widget', 10, 'A');
INSERT INTO products (name, price, category) VALUES ('Gadget', 20, 'A');
INSERT INTO products (name, price, category) VALUES ('Gizmo', 15, 'B');
INSERT INTO products (name, price, category) VALUES ('Thing', 25, 'B');

-- 17. Scalar subquery - min/max in expression
SELECT name, price, price - (SELECT MIN(price) FROM products) as above_min FROM products;

-- 18. Scalar subquery in CASE
SELECT name, price,
       CASE WHEN price > (SELECT AVG(price) FROM products) THEN 'expensive' ELSE 'cheap' END as price_category
FROM products;

-- 19. Correlated scalar subquery - category average
SELECT name, price, category,
       (SELECT AVG(price) FROM products p2 WHERE p2.category = products.category) as category_avg
FROM products;

-- 20. Subquery with DISTINCT
SELECT * FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE price > 15);

DROP TABLE products;
