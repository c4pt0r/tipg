-- Views Tests
-- Purpose: Verify CREATE VIEW, DROP VIEW, and view queries

DROP VIEW IF EXISTS active_users;
DROP VIEW IF EXISTS user_stats;
DROP VIEW IF EXISTS recent_orders;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT,
    amount INT,
    status TEXT DEFAULT 'pending'
);

INSERT INTO users (name, email, active) VALUES ('Alice', 'alice@example.com', TRUE);
INSERT INTO users (name, email, active) VALUES ('Bob', 'bob@example.com', TRUE);
INSERT INTO users (name, email, active) VALUES ('Charlie', 'charlie@example.com', FALSE);
INSERT INTO users (name, email, active) VALUES ('Diana', 'diana@example.com', TRUE);

INSERT INTO orders (user_id, amount, status) VALUES (1, 100, 'completed');
INSERT INTO orders (user_id, amount, status) VALUES (1, 200, 'completed');
INSERT INTO orders (user_id, amount, status) VALUES (2, 150, 'pending');
INSERT INTO orders (user_id, amount, status) VALUES (2, 50, 'completed');
INSERT INTO orders (user_id, amount, status) VALUES (4, 300, 'completed');

-- 1. Simple view creation
CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE;
SELECT * FROM active_users;

-- 2. View with column selection
CREATE VIEW user_emails AS SELECT id, name, email FROM users;
SELECT * FROM user_emails;

-- 3. View with aggregation
CREATE VIEW order_totals AS 
SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount 
FROM orders 
GROUP BY user_id;
SELECT * FROM order_totals;

-- 4. Query view with WHERE
SELECT * FROM active_users WHERE name LIKE 'A%';

-- 5. Query view with ORDER BY
SELECT * FROM active_users ORDER BY name;

-- 6. Query view with LIMIT
SELECT * FROM active_users LIMIT 2;

-- 7. JOIN with view
SELECT u.name, o.order_count, o.total_amount
FROM active_users u
JOIN order_totals o ON u.id = o.user_id;

-- 8. View referencing another view
CREATE VIEW active_user_orders AS
SELECT au.name, ot.order_count, ot.total_amount
FROM active_users au
JOIN order_totals ot ON au.id = ot.user_id;
SELECT * FROM active_user_orders;

-- 9. View with CASE expression
CREATE VIEW user_status AS
SELECT id, name, 
       CASE WHEN active THEN 'Active' ELSE 'Inactive' END as status
FROM users;
SELECT * FROM user_status;

-- 10. View with computed column
CREATE VIEW order_summary AS
SELECT id, user_id, amount, amount * 0.1 as tax, amount * 1.1 as total
FROM orders;
SELECT * FROM order_summary;

-- 11. DROP VIEW
DROP VIEW user_emails;

-- 12. DROP VIEW IF EXISTS (should not error)
DROP VIEW IF EXISTS nonexistent_view;

-- 13. CREATE OR REPLACE VIEW
CREATE OR REPLACE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE;
SELECT * FROM active_users;

-- 14. View with subquery
CREATE VIEW high_value_users AS
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);
SELECT * FROM high_value_users;

-- 15. View with window function
CREATE VIEW user_order_rank AS
SELECT user_id, amount, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) as rank
FROM orders;
SELECT * FROM user_order_rank;

-- 16. Multiple views in single query
SELECT au.name, ao.order_count
FROM active_users au
JOIN active_user_orders ao ON au.name = ao.name;

-- 17. View with NULL handling
CREATE VIEW user_safe AS
SELECT id, COALESCE(name, 'Unknown') as name, COALESCE(email, 'no-email') as email
FROM users;
SELECT * FROM user_safe;

-- 18. Aggregation on view
SELECT COUNT(*) FROM active_users;
SELECT AVG(total_amount) FROM order_totals;

-- 19. View in CTE
WITH top_users AS (SELECT * FROM active_users)
SELECT * FROM top_users WHERE id < 3;

-- 20. Complex nested view query
SELECT * FROM active_user_orders WHERE order_count > 1 ORDER BY total_amount DESC;

DROP VIEW IF EXISTS user_order_rank;
DROP VIEW IF EXISTS high_value_users;
DROP VIEW IF EXISTS user_safe;
DROP VIEW IF EXISTS order_summary;
DROP VIEW IF EXISTS user_status;
DROP VIEW IF EXISTS active_user_orders;
DROP VIEW IF EXISTS order_totals;
DROP VIEW IF EXISTS active_users;
DROP TABLE orders;
DROP TABLE users;
