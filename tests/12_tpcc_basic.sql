-- TPC-C like functional test
-- Tests: JOINs, AVG, IN, BETWEEN, IS NULL, complex queries

-- ============================================================
-- PART 1: Schema Creation (TPC-C simplified tables)
-- ============================================================

-- Warehouse table
CREATE TABLE warehouse (
    w_id INT PRIMARY KEY,
    w_name TEXT,
    w_street_1 TEXT,
    w_city TEXT,
    w_state TEXT,
    w_zip TEXT,
    w_tax FLOAT,
    w_ytd FLOAT
);

-- District table
CREATE TABLE district (
    d_id INT,
    d_w_id INT,
    d_name TEXT,
    d_street_1 TEXT,
    d_city TEXT,
    d_state TEXT,
    d_zip TEXT,
    d_tax FLOAT,
    d_ytd FLOAT,
    d_next_o_id INT,
    PRIMARY KEY (d_w_id, d_id)
);

-- Customer table
CREATE TABLE customer (
    c_id INT,
    c_d_id INT,
    c_w_id INT,
    c_first TEXT,
    c_middle TEXT,
    c_last TEXT,
    c_street_1 TEXT,
    c_city TEXT,
    c_state TEXT,
    c_zip TEXT,
    c_phone TEXT,
    c_credit TEXT,
    c_credit_lim FLOAT,
    c_discount FLOAT,
    c_balance FLOAT,
    c_ytd_payment FLOAT,
    c_payment_cnt INT,
    c_delivery_cnt INT,
    c_data TEXT,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

-- Orders table
CREATE TABLE orders (
    o_id INT,
    o_d_id INT,
    o_w_id INT,
    o_c_id INT,
    o_entry_d TIMESTAMP,
    o_carrier_id INT,
    o_ol_cnt INT,
    o_all_local INT,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

-- Order Line table
CREATE TABLE order_line (
    ol_o_id INT,
    ol_d_id INT,
    ol_w_id INT,
    ol_number INT,
    ol_i_id INT,
    ol_supply_w_id INT,
    ol_delivery_d TIMESTAMP,
    ol_quantity INT,
    ol_amount FLOAT,
    ol_dist_info TEXT,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

-- Stock table
CREATE TABLE stock (
    s_i_id INT,
    s_w_id INT,
    s_quantity INT,
    s_dist_01 TEXT,
    s_dist_02 TEXT,
    s_ytd INT,
    s_order_cnt INT,
    s_remote_cnt INT,
    s_data TEXT,
    PRIMARY KEY (s_w_id, s_i_id)
);

-- Item table
CREATE TABLE item (
    i_id INT PRIMARY KEY,
    i_im_id INT,
    i_name TEXT,
    i_price FLOAT,
    i_data TEXT
);

SHOW TABLES;

-- ============================================================
-- PART 2: Data Population
-- ============================================================

-- Insert warehouse
INSERT INTO warehouse VALUES (1, 'Warehouse 1', '123 Main St', 'Springfield', 'IL', '62701', 0.08, 300000.00);
INSERT INTO warehouse VALUES (2, 'Warehouse 2', '456 Oak Ave', 'Chicago', 'IL', '60601', 0.07, 500000.00);

-- Insert districts
INSERT INTO district VALUES (1, 1, 'District 1-1', '100 First St', 'Springfield', 'IL', '62701', 0.05, 30000.00, 3001);
INSERT INTO district VALUES (2, 1, 'District 1-2', '200 Second St', 'Springfield', 'IL', '62702', 0.06, 35000.00, 3002);
INSERT INTO district VALUES (1, 2, 'District 2-1', '300 Third St', 'Chicago', 'IL', '60602', 0.04, 40000.00, 3003);

-- Insert customers
INSERT INTO customer VALUES (1, 1, 1, 'John', 'A', 'Doe', '111 A St', 'Springfield', 'IL', '62701', '555-1001', 'GC', 50000.00, 0.10, -100.00, 100.00, 5, 2, 'VIP customer');
INSERT INTO customer VALUES (2, 1, 1, 'Jane', 'B', 'Smith', '222 B St', 'Springfield', 'IL', '62701', '555-1002', 'BC', 40000.00, 0.05, 200.00, 200.00, 3, 1, 'Regular customer');
INSERT INTO customer VALUES (1, 2, 1, 'Bob', 'C', 'Johnson', '333 C St', 'Springfield', 'IL', '62702', '555-1003', 'GC', 60000.00, 0.15, 500.00, 500.00, 10, 5, 'Premium customer');
INSERT INTO customer VALUES (1, 1, 2, 'Alice', 'D', 'Brown', '444 D St', 'Chicago', 'IL', '60602', '555-2001', 'BC', 30000.00, 0.08, 0.00, 0.00, 0, 0, 'New customer');

-- Insert items
INSERT INTO item VALUES (1, 1001, 'Item 1', 10.50, 'Original product');
INSERT INTO item VALUES (2, 1002, 'Item 2', 25.00, 'Premium product');
INSERT INTO item VALUES (3, 1003, 'Item 3', 5.75, 'Budget product');
INSERT INTO item VALUES (4, 1004, 'Item 4', 100.00, 'Luxury product');
INSERT INTO item VALUES (5, 1005, 'Item 5', 15.25, 'Standard product');

-- Insert stock
INSERT INTO stock VALUES (1, 1, 100, 'DIST01', 'DIST02', 500, 50, 5, 'Stock data 1');
INSERT INTO stock VALUES (2, 1, 50, 'DIST01', 'DIST02', 300, 30, 3, 'Stock data 2');
INSERT INTO stock VALUES (3, 1, 200, 'DIST01', 'DIST02', 800, 80, 8, 'Stock data 3');
INSERT INTO stock VALUES (1, 2, 75, 'DIST01', 'DIST02', 400, 40, 4, 'Stock data 4');

-- Insert orders
INSERT INTO orders VALUES (1, 1, 1, 1, 1704067200000, 1, 3, 1);
INSERT INTO orders VALUES (2, 1, 1, 2, 1704153600000, 2, 2, 1);
INSERT INTO orders VALUES (3, 1, 1, 1, 1704240000000, NULL, 1, 0);
INSERT INTO orders VALUES (1, 2, 1, 1, 1704067200000, 1, 2, 1);

-- Insert order lines
INSERT INTO order_line VALUES (1, 1, 1, 1, 1, 1, 1704067200000, 5, 52.50, 'D1INFO');
INSERT INTO order_line VALUES (1, 1, 1, 2, 2, 1, 1704067200000, 3, 75.00, 'D1INFO');
INSERT INTO order_line VALUES (1, 1, 1, 3, 3, 1, 1704067200000, 10, 57.50, 'D1INFO');
INSERT INTO order_line VALUES (2, 1, 1, 1, 4, 1, 1704153600000, 1, 100.00, 'D1INFO');
INSERT INTO order_line VALUES (2, 1, 1, 2, 5, 1, 1704153600000, 2, 30.50, 'D1INFO');

-- ============================================================
-- PART 3: Basic Queries (should already work)
-- ============================================================

-- Simple select
SELECT * FROM warehouse;
SELECT * FROM item WHERE i_price > 20.00;
SELECT c_first, c_last, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1;

-- Aggregations
SELECT COUNT(*) FROM customer;
SELECT SUM(ol_amount) FROM order_line;
SELECT MAX(i_price), MIN(i_price) FROM item;

-- ============================================================
-- PART 4: New Features to Test
-- ============================================================

-- 4.1 AVG aggregation
SELECT AVG(i_price) FROM item;
SELECT AVG(s_quantity) FROM stock WHERE s_w_id = 1;

-- 4.2 IS NULL / IS NOT NULL  
SELECT * FROM orders WHERE o_carrier_id IS NULL;
SELECT * FROM orders WHERE o_carrier_id IS NOT NULL;
SELECT COUNT(*) FROM orders WHERE o_carrier_id IS NULL;

-- 4.3 IN clause
SELECT * FROM item WHERE i_id IN (1, 3, 5);
SELECT c_first, c_last FROM customer WHERE c_credit IN ('GC', 'BC');

-- 4.4 BETWEEN clause
SELECT * FROM item WHERE i_price BETWEEN 10.00 AND 30.00;
SELECT * FROM stock WHERE s_quantity BETWEEN 50 AND 150;

-- 4.5 COALESCE
SELECT o_id, COALESCE(o_carrier_id, 0) FROM orders;

-- 4.6 Modulo operator
SELECT i_id, i_id % 2 FROM item;

-- ============================================================
-- PART 5: JOIN Queries (TPC-C style)
-- ============================================================

-- 5.1 Simple INNER JOIN - Get order with customer info
SELECT o.o_id, c.c_first, c.c_last
FROM orders o
INNER JOIN customer c ON o.o_c_id = c.c_id AND o.o_d_id = c.c_d_id AND o.o_w_id = c.c_w_id
WHERE o.o_w_id = 1 AND o.o_d_id = 1;

-- 5.2 JOIN with aggregation - Total amount per order
SELECT ol.ol_o_id, SUM(ol.ol_amount)
FROM order_line ol
INNER JOIN orders o ON ol.ol_o_id = o.o_id AND ol.ol_d_id = o.o_d_id AND ol.ol_w_id = o.o_w_id
WHERE ol.ol_w_id = 1 AND ol.ol_d_id = 1
GROUP BY ol.ol_o_id;

-- 5.3 JOIN item and order_line
SELECT ol.ol_o_id, i.i_name, ol.ol_quantity, ol.ol_amount
FROM order_line ol
INNER JOIN item i ON ol.ol_i_id = i.i_id
WHERE ol.ol_w_id = 1 AND ol.ol_d_id = 1 AND ol.ol_o_id = 1;

-- 5.4 Stock level query (TPC-C Transaction 5 simplified)
SELECT COUNT(*) FROM stock s
INNER JOIN order_line ol ON s.s_i_id = ol.ol_i_id AND s.s_w_id = ol.ol_w_id
WHERE s.s_w_id = 1 AND s.s_quantity < 100;

-- ============================================================
-- PART 6: DISTINCT
-- ============================================================

SELECT DISTINCT c_credit FROM customer;
SELECT DISTINCT o_carrier_id FROM orders;

-- ============================================================
-- PART 7: Cleanup
-- ============================================================

DROP TABLE order_line;
DROP TABLE orders;
DROP TABLE stock;
DROP TABLE customer;
DROP TABLE district;
DROP TABLE warehouse;
DROP TABLE item;

SHOW TABLES;
