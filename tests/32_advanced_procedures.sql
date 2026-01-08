-- Advanced Stored Procedures Tests

DROP PROCEDURE IF EXISTS inventory_restock;
DROP PROCEDURE IF EXISTS apply_discount;
DROP PROCEDURE IF EXISTS archive_orders;
DROP PROCEDURE IF EXISTS batch_update_prices;
DROP PROCEDURE IF EXISTS calculate_commission;
DROP PROCEDURE IF EXISTS process_refund;
DROP PROCEDURE IF EXISTS daily_summary;
DROP PROCEDURE IF EXISTS cascade_delete;
DROP PROCEDURE IF EXISTS audit_log;
DROP PROCEDURE IF EXISTS reset_sequence;
DROP PROCEDURE IF EXISTS update_order_status;
DROP PROCEDURE IF EXISTS log_event;
DROP PROCEDURE IF EXISTS cleanup_product;

DROP TABLE IF EXISTS audit_trail;
DROP TABLE IF EXISTS order_archive;
DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS adv_products;
DROP TABLE IF EXISTS adv_customers;
DROP TABLE IF EXISTS sales_reps;

CREATE TABLE adv_products (
    id SERIAL PRIMARY KEY,
    name TEXT,
    category TEXT,
    price INT,
    stock INT,
    min_stock INT
);

CREATE TABLE adv_customers (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    tier TEXT,
    total_spent INT DEFAULT 0
);

CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id INT,
    warehouse TEXT,
    quantity INT,
    last_updated TEXT
);

CREATE TABLE order_details (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price INT,
    discount_pct INT DEFAULT 0,
    status TEXT DEFAULT 'pending',
    order_date TEXT
);

CREATE TABLE order_archive (
    id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount INT,
    archived_date TEXT
);

CREATE TABLE sales_reps (
    id SERIAL PRIMARY KEY,
    name TEXT,
    region TEXT,
    commission_rate INT,
    total_sales INT DEFAULT 0,
    total_commission INT DEFAULT 0
);

CREATE TABLE audit_trail (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    operation TEXT,
    record_id INT,
    old_value TEXT,
    new_value TEXT,
    changed_at TEXT
);

INSERT INTO adv_products (name, category, price, stock, min_stock) VALUES 
    ('Laptop Pro', 'Electronics', 1200, 50, 10),
    ('Wireless Mouse', 'Electronics', 30, 200, 50),
    ('Office Chair', 'Furniture', 250, 30, 5),
    ('Standing Desk', 'Furniture', 450, 15, 3),
    ('USB Hub', 'Electronics', 25, 100, 20);

INSERT INTO adv_customers (name, email, tier, total_spent) VALUES
    ('TechCorp', 'orders@techcorp.com', 'Gold', 50000),
    ('StartupXYZ', 'buy@startupxyz.com', 'Silver', 15000),
    ('BigEnterprise', 'procurement@bigent.com', 'Platinum', 200000),
    ('SmallBiz', 'owner@smallbiz.com', 'Bronze', 3000);

INSERT INTO inventory (product_id, warehouse, quantity, last_updated) VALUES
    (1, 'West', 30, '2024-01-15'),
    (1, 'East', 20, '2024-01-15'),
    (2, 'West', 150, '2024-01-15'),
    (2, 'East', 50, '2024-01-15'),
    (3, 'Central', 30, '2024-01-15');

INSERT INTO order_details (customer_id, product_id, quantity, unit_price, status, order_date) VALUES
    (1, 1, 5, 1200, 'completed', '2024-01-10'),
    (1, 2, 20, 30, 'completed', '2024-01-12'),
    (2, 3, 10, 250, 'pending', '2024-01-15'),
    (3, 1, 10, 1200, 'completed', '2024-01-08'),
    (3, 4, 5, 450, 'shipped', '2024-01-11'),
    (4, 5, 50, 25, 'pending', '2024-01-16');

INSERT INTO sales_reps (name, region, commission_rate) VALUES
    ('John Smith', 'West', 5),
    ('Jane Doe', 'East', 6),
    ('Bob Wilson', 'Central', 4);

-- 1. Inventory Restock Procedure (simplified - updates by product_id only)
CREATE PROCEDURE inventory_restock(p_product_id INT, p_quantity INT)
AS BEGIN
UPDATE inventory SET quantity = quantity + p_quantity, last_updated = '2024-01-20' 
WHERE product_id = p_product_id;
UPDATE adv_products SET stock = stock + p_quantity WHERE id = p_product_id
END;

SELECT p.name, SUM(i.quantity) as total_qty FROM inventory i 
JOIN adv_products p ON i.product_id = p.id WHERE p.id = 1 GROUP BY p.name;

CALL inventory_restock(1, 25);

SELECT p.name, SUM(i.quantity) as total_qty FROM inventory i 
JOIN adv_products p ON i.product_id = p.id WHERE p.id = 1 GROUP BY p.name;

-- 2. Apply Discount to Orders
CREATE PROCEDURE apply_discount(p_customer_tier TEXT, p_discount INT)
AS BEGIN
UPDATE order_details SET discount_pct = p_discount 
WHERE customer_id IN (SELECT id FROM adv_customers WHERE tier = p_customer_tier)
AND status = 'pending'
END;

SELECT od.id, c.name, c.tier, od.unit_price, od.discount_pct 
FROM order_details od
JOIN adv_customers c ON od.customer_id = c.id
WHERE od.status = 'pending';

CALL apply_discount('Silver', 10);
CALL apply_discount('Bronze', 5);

SELECT od.id, c.name, c.tier, od.unit_price, od.discount_pct 
FROM order_details od
JOIN adv_customers c ON od.customer_id = c.id
WHERE od.status = 'pending';

-- 3. Simple Order Status Update (INSERT INTO SELECT not supported)
CREATE PROCEDURE update_order_status(p_old_status TEXT, p_new_status TEXT)
AS BEGIN
UPDATE order_details SET status = p_new_status WHERE status = p_old_status
END;

SELECT status, COUNT(*) as cnt FROM order_details GROUP BY status ORDER BY status;

CALL update_order_status('pending', 'processing');

SELECT status, COUNT(*) as cnt FROM order_details GROUP BY status ORDER BY status;

-- 4. Batch Price Update
CREATE PROCEDURE batch_update_prices(p_category TEXT, p_increase_pct INT)
AS BEGIN
UPDATE adv_products SET price = price * (100 + p_increase_pct) / 100 WHERE category = p_category
END;

SELECT name, category, price FROM adv_products WHERE category = 'Electronics';

CALL batch_update_prices('Electronics', 15);

SELECT name, category, price FROM adv_products WHERE category = 'Electronics';

-- 5. Calculate Sales Rep Commission
CREATE PROCEDURE calculate_commission(p_rep_id INT, p_sales_amount INT)
AS BEGIN
UPDATE sales_reps SET total_sales = total_sales + p_sales_amount,
total_commission = total_commission + (p_sales_amount * commission_rate / 100)
WHERE id = p_rep_id
END;

SELECT name, total_sales, total_commission FROM sales_reps;

CALL calculate_commission(1, 10000);
CALL calculate_commission(2, 15000);
CALL calculate_commission(3, 8000);

SELECT name, total_sales, total_commission FROM sales_reps;

-- 6. Process Refund (simplified)
CREATE PROCEDURE process_refund(p_order_id INT)
AS BEGIN
UPDATE order_details SET status = 'refunded' WHERE id = p_order_id
END;

SELECT id, status FROM order_details WHERE id = 3;

CALL process_refund(3);

SELECT id, status FROM order_details WHERE id = 3;

-- 7. Simple Audit Insert
CREATE PROCEDURE log_event(p_table TEXT, p_operation TEXT, p_record_id INT)
AS BEGIN
INSERT INTO audit_trail (table_name, operation, record_id, old_value, new_value, changed_at)
VALUES (p_table, p_operation, p_record_id, '', '', '2024-01-20')
END;

CALL log_event('order_details', 'VIEW', 1);
CALL log_event('adv_products', 'UPDATE', 2);

SELECT * FROM audit_trail ORDER BY id;

-- 8. Multi-Table Cleanup by Product
CREATE PROCEDURE cleanup_product(p_product_id INT)
AS BEGIN
DELETE FROM order_details WHERE product_id = p_product_id;
DELETE FROM adv_products WHERE id = p_product_id
END;

SELECT COUNT(*) as product_count FROM adv_products;
SELECT COUNT(*) as order_count FROM order_details WHERE product_id = 5;

CALL cleanup_product(5);

SELECT COUNT(*) as product_count FROM adv_products;
SELECT COUNT(*) as order_count FROM order_details WHERE product_id = 5;

-- 9. Audit Logging Procedure
CREATE PROCEDURE audit_log(p_table TEXT, p_op TEXT, p_id INT, p_old TEXT, p_new TEXT)
AS BEGIN
INSERT INTO audit_trail (table_name, operation, record_id, old_value, new_value, changed_at)
VALUES (p_table, p_op, p_id, p_old, p_new, '2024-01-20')
END;

CALL audit_log('adv_customers', 'UPDATE', 1, 'tier=Gold', 'tier=Platinum');
CALL audit_log('adv_products', 'INSERT', 6, '', 'name=NewProduct');
CALL audit_log('inventory', 'DELETE', 99, 'qty=100', '');

SELECT * FROM audit_trail WHERE changed_at = '2024-01-20' ORDER BY id;

-- 10. Chained Procedure Calls Test
SELECT 'Before chained calls' as stage;
SELECT name, stock FROM adv_products WHERE id = 1;
SELECT name, total_sales FROM sales_reps WHERE id = 1;

CALL inventory_restock(1, 10);
CALL calculate_commission(1, 5000);
CALL audit_log('test', 'CHAIN', 1, 'test1', 'test2');

SELECT 'After chained calls' as stage;
SELECT name, stock FROM adv_products WHERE id = 1;
SELECT name, total_sales FROM sales_reps WHERE id = 1;

-- 11. Statistics Verification
SELECT 'Final Statistics' as report;
SELECT COUNT(*) as total_products FROM adv_products;
SELECT COUNT(*) as total_customers FROM adv_customers;
SELECT COUNT(*) as total_orders FROM order_details;
SELECT COUNT(*) as archived_orders FROM order_archive;
SELECT COUNT(*) as audit_entries FROM audit_trail;
SELECT SUM(total_commission) as total_commissions FROM sales_reps;

-- Cleanup
DROP PROCEDURE IF EXISTS inventory_restock;
DROP PROCEDURE IF EXISTS apply_discount;
DROP PROCEDURE IF EXISTS archive_orders;
DROP PROCEDURE IF EXISTS batch_update_prices;
DROP PROCEDURE IF EXISTS calculate_commission;
DROP PROCEDURE IF EXISTS process_refund;
DROP PROCEDURE IF EXISTS daily_summary;
DROP PROCEDURE IF EXISTS cascade_delete;
DROP PROCEDURE IF EXISTS audit_log;

DROP TABLE audit_trail;
DROP TABLE order_archive;
DROP TABLE order_details;
DROP TABLE inventory;
DROP TABLE sales_reps;
DROP TABLE adv_customers;
DROP TABLE adv_products;
