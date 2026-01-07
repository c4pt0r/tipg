-- Materialized Views Tests

DROP MATERIALIZED VIEW IF EXISTS sales_summary;
DROP MATERIALIZED VIEW IF EXISTS monthly_revenue;
DROP MATERIALIZED VIEW IF EXISTS customer_stats;
DROP MATERIALIZED VIEW IF EXISTS product_ranking;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS mv_orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT,
    region TEXT,
    tier TEXT
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT,
    category TEXT,
    price INT
);

CREATE TABLE mv_orders (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    order_date TEXT
);

INSERT INTO customers (name, region, tier) VALUES ('Acme Corp', 'North', 'Gold');
INSERT INTO customers (name, region, tier) VALUES ('Beta Inc', 'South', 'Silver');
INSERT INTO customers (name, region, tier) VALUES ('Gamma LLC', 'North', 'Bronze');
INSERT INTO customers (name, region, tier) VALUES ('Delta Co', 'East', 'Gold');
INSERT INTO customers (name, region, tier) VALUES ('Epsilon Ltd', 'West', 'Silver');

INSERT INTO products (name, category, price) VALUES ('Widget A', 'Hardware', 100);
INSERT INTO products (name, category, price) VALUES ('Widget B', 'Hardware', 150);
INSERT INTO products (name, category, price) VALUES ('Service X', 'Software', 500);
INSERT INTO products (name, category, price) VALUES ('Service Y', 'Software', 750);
INSERT INTO products (name, category, price) VALUES ('Gadget Z', 'Electronics', 200);

INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (1, 1, 10, '2024-01-15');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (1, 3, 2, '2024-01-20');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (2, 2, 5, '2024-01-18');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (2, 4, 1, '2024-02-01');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (3, 5, 20, '2024-02-10');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (4, 1, 15, '2024-02-15');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (4, 2, 8, '2024-02-20');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (5, 3, 3, '2024-03-01');

-- 1. Basic Materialized View
CREATE MATERIALIZED VIEW sales_summary AS
SELECT 
    p.category,
    COUNT(*) as order_count,
    SUM(o.quantity) as total_quantity,
    SUM(o.quantity * p.price) as total_revenue
FROM mv_orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category;

SELECT * FROM sales_summary ORDER BY category;

-- 2. Materialized View with Multiple Joins
CREATE MATERIALIZED VIEW customer_stats AS
SELECT 
    c.name as customer_name,
    c.region,
    c.tier,
    COUNT(o.id) as order_count,
    SUM(o.quantity * p.price) as total_spent
FROM customers c
LEFT JOIN mv_orders o ON c.id = o.customer_id
LEFT JOIN products p ON o.product_id = p.id
GROUP BY c.id, c.name, c.region, c.tier;

SELECT * FROM customer_stats ORDER BY total_spent DESC;

-- 3. Query Materialized View with WHERE
SELECT * FROM sales_summary WHERE total_revenue > 2000;

-- 4. Query Materialized View with ORDER BY
SELECT * FROM customer_stats ORDER BY customer_name;

-- 5. Aggregation on Materialized View
SELECT SUM(total_revenue) as grand_total FROM sales_summary;
SELECT AVG(total_spent) as avg_customer_spend FROM customer_stats;

-- 6. Join Regular Table with Materialized View
SELECT c.name, cs.order_count, cs.total_spent
FROM customers c
JOIN customer_stats cs ON c.name = cs.customer_name
WHERE c.tier = 'Gold';

-- 7. Verify Materialized View Shows Stale Data
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (1, 4, 5, '2024-03-15');
INSERT INTO mv_orders (customer_id, product_id, quantity, order_date) VALUES (3, 3, 10, '2024-03-20');

SELECT * FROM sales_summary ORDER BY category;

-- 8. REFRESH Materialized View
REFRESH MATERIALIZED VIEW sales_summary;

SELECT * FROM sales_summary ORDER BY category;

-- 9. REFRESH Another Materialized View
REFRESH MATERIALIZED VIEW customer_stats;

SELECT * FROM customer_stats ORDER BY total_spent DESC;

-- 10. Materialized View with Computed Columns
CREATE MATERIALIZED VIEW product_ranking AS
SELECT 
    p.name as product_name,
    p.category,
    p.price,
    COALESCE(SUM(o.quantity), 0) as units_sold,
    COALESCE(SUM(o.quantity * p.price), 0) as revenue,
    CASE 
        WHEN COALESCE(SUM(o.quantity), 0) > 10 THEN 'High'
        WHEN COALESCE(SUM(o.quantity), 0) > 5 THEN 'Medium'
        ELSE 'Low'
    END as demand_level
FROM products p
LEFT JOIN mv_orders o ON p.id = o.product_id
GROUP BY p.id, p.name, p.category, p.price;

SELECT * FROM product_ranking ORDER BY revenue DESC;

-- 11. Subquery on Materialized View
SELECT * FROM product_ranking 
WHERE revenue > (SELECT AVG(revenue) FROM product_ranking);

-- 12. Materialized View in CTE
WITH top_categories AS (
    SELECT category, total_revenue FROM sales_summary ORDER BY total_revenue DESC LIMIT 2
)
SELECT * FROM top_categories;

-- 13. Multiple Materialized Views in Query
SELECT 
    ss.category,
    ss.total_revenue,
    (SELECT COUNT(*) FROM customer_stats cs WHERE cs.total_spent > 1000) as premium_customers
FROM sales_summary ss
ORDER BY ss.total_revenue DESC;

-- 14. Verify Data After Multiple Operations
SELECT 'sales_summary' as view_name, COUNT(*) as rows FROM sales_summary
UNION ALL
SELECT 'customer_stats', COUNT(*) FROM customer_stats
UNION ALL
SELECT 'product_ranking', COUNT(*) FROM product_ranking;

-- 15. DROP MATERIALIZED VIEW
DROP MATERIALIZED VIEW product_ranking;

-- 16. DROP MATERIALIZED VIEW IF EXISTS
DROP MATERIALIZED VIEW IF EXISTS nonexistent_matview;

-- 17. Recreate Dropped View
CREATE MATERIALIZED VIEW product_ranking AS
SELECT p.name, SUM(o.quantity) as qty FROM products p
LEFT JOIN mv_orders o ON p.id = o.product_id
GROUP BY p.id, p.name;

SELECT * FROM product_ranking ORDER BY qty DESC;

-- Cleanup
DROP MATERIALIZED VIEW IF EXISTS product_ranking;
DROP MATERIALIZED VIEW IF EXISTS customer_stats;
DROP MATERIALIZED VIEW IF EXISTS sales_summary;
DROP TABLE mv_orders;
DROP TABLE products;
DROP TABLE customers;
