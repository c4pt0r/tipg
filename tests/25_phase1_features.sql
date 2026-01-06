-- Phase 1 Features Integration Tests
-- Tests: FETCH, NATURAL JOIN, CROSS JOIN, UPDATE FROM, CREATE TABLE AS, SELECT INTO

-- ============================================
-- 1. FETCH FIRST N ROWS (SQL Standard)
-- ============================================
DROP TABLE IF EXISTS fetch_test;
CREATE TABLE fetch_test (id INT PRIMARY KEY, name TEXT);
INSERT INTO fetch_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve');

SELECT * FROM fetch_test ORDER BY id FETCH FIRST ROW ONLY;
SELECT * FROM fetch_test ORDER BY id FETCH FIRST 3 ROWS ONLY;
SELECT * FROM fetch_test ORDER BY id OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY;
SELECT * FROM fetch_test ORDER BY id FETCH NEXT 2 ROWS ONLY;

DROP TABLE fetch_test;

-- ============================================
-- 2. NATURAL JOIN
-- ============================================
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS employees_nj;

CREATE TABLE departments (dept_id INT PRIMARY KEY, dept_name TEXT);
CREATE TABLE employees_nj (emp_id INT PRIMARY KEY, name TEXT, dept_id INT);

INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR');
INSERT INTO employees_nj VALUES (101, 'Alice', 1), (102, 'Bob', 2), (103, 'Charlie', 1), (104, 'David', NULL);

SELECT * FROM employees_nj NATURAL JOIN departments ORDER BY emp_id;
SELECT emp_id, name, dept_name FROM employees_nj NATURAL JOIN departments ORDER BY emp_id;

DROP TABLE employees_nj;
DROP TABLE departments;

-- ============================================
-- 3. CROSS JOIN
-- ============================================
DROP TABLE IF EXISTS colors;
DROP TABLE IF EXISTS sizes;

CREATE TABLE colors (id INT PRIMARY KEY, color TEXT);
CREATE TABLE sizes (id INT PRIMARY KEY, size TEXT);

INSERT INTO colors VALUES (1, 'Red'), (2, 'Blue');
INSERT INTO sizes VALUES (1, 'S'), (2, 'M'), (3, 'L');

SELECT c.color, s.size FROM colors c CROSS JOIN sizes s ORDER BY c.color, s.size;

DROP TABLE colors;
DROP TABLE sizes;

-- ============================================
-- 4. UPDATE ... FROM (UPDATE JOIN)
-- ============================================
DROP TABLE IF EXISTS products_uf;
DROP TABLE IF EXISTS price_updates;

CREATE TABLE products_uf (id INT PRIMARY KEY, name TEXT, price INT);
CREATE TABLE price_updates (product_id INT PRIMARY KEY, new_price INT);

INSERT INTO products_uf VALUES (1, 'Apple', 100), (2, 'Banana', 50), (3, 'Cherry', 200);
INSERT INTO price_updates VALUES (1, 120), (3, 180);

SELECT * FROM products_uf ORDER BY id;

UPDATE products_uf p SET price = pu.new_price FROM price_updates pu WHERE p.id = pu.product_id;

SELECT * FROM products_uf ORDER BY id;

DROP TABLE products_uf;
DROP TABLE price_updates;

SELECT 'Phase 1 tests (FETCH, NATURAL JOIN, CROSS JOIN, UPDATE FROM) completed!' AS status;
