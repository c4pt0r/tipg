-- Phase 1 Features Integration Tests
-- Tests: FETCH, NATURAL JOIN, CROSS JOIN, UPDATE FROM, CREATE TABLE AS, SELECT INTO

-- ============================================
-- 1. FETCH FIRST N ROWS (SQL Standard)
-- ============================================
DROP TABLE IF EXISTS fetch_test;
CREATE TABLE fetch_test (id INT PRIMARY KEY, name TEXT, score INT);
INSERT INTO fetch_test VALUES (1, 'Alice', 95), (2, 'Bob', 87), (3, 'Charlie', 92), (4, 'David', 78), (5, 'Eve', 88), (6, 'Frank', 91), (7, 'Grace', 85);

SELECT * FROM fetch_test ORDER BY id FETCH FIRST ROW ONLY;
SELECT * FROM fetch_test ORDER BY id FETCH FIRST 1 ROW ONLY;
SELECT * FROM fetch_test ORDER BY id FETCH FIRST 3 ROWS ONLY;
SELECT * FROM fetch_test ORDER BY id FETCH NEXT 3 ROWS ONLY;

SELECT * FROM fetch_test ORDER BY id OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY;
SELECT * FROM fetch_test ORDER BY id OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY;
SELECT * FROM fetch_test ORDER BY id OFFSET 0 ROWS FETCH FIRST 2 ROWS ONLY;

SELECT * FROM fetch_test ORDER BY score DESC FETCH FIRST 3 ROWS ONLY;
SELECT name, score FROM fetch_test WHERE score > 85 ORDER BY score FETCH FIRST 2 ROWS ONLY;

DROP TABLE fetch_test;

-- ============================================
-- 2. NATURAL JOIN
-- ============================================
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS employees_nj;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS skills;
DROP TABLE IF EXISTS emp_skills;

-- 2.1 Basic NATURAL JOIN
CREATE TABLE departments (dept_id INT PRIMARY KEY, dept_name TEXT, location TEXT);
CREATE TABLE employees_nj (emp_id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT);

INSERT INTO departments VALUES (1, 'Engineering', 'Building A'), (2, 'Sales', 'Building B'), (3, 'HR', 'Building C'), (4, 'Marketing', 'Building D');
INSERT INTO employees_nj VALUES (101, 'Alice', 1, 75000), (102, 'Bob', 2, 65000), (103, 'Charlie', 1, 80000), (104, 'David', NULL, 55000), (105, 'Eve', 3, 60000);

-- Basic join on common column (dept_id)
SELECT * FROM employees_nj NATURAL JOIN departments ORDER BY emp_id;
SELECT emp_id, name, dept_name, location FROM employees_nj NATURAL JOIN departments ORDER BY emp_id;

-- 2.2 NATURAL JOIN with WHERE clause
SELECT name, dept_name FROM employees_nj NATURAL JOIN departments WHERE salary > 70000;
SELECT * FROM employees_nj NATURAL JOIN departments WHERE dept_name = 'Engineering' ORDER BY emp_id;
SELECT name, location FROM employees_nj NATURAL JOIN departments WHERE salary BETWEEN 60000 AND 75000 ORDER BY name;

-- 2.3 NATURAL JOIN with aggregation
SELECT dept_name, COUNT(*) as emp_count, AVG(salary) as avg_salary FROM employees_nj NATURAL JOIN departments GROUP BY dept_name ORDER BY dept_name;
SELECT dept_name, MAX(salary) as max_sal FROM employees_nj NATURAL JOIN departments GROUP BY dept_name HAVING COUNT(*) > 1;

-- 2.4 Three-table NATURAL JOIN
CREATE TABLE projects (proj_id INT PRIMARY KEY, proj_name TEXT, dept_id INT);
INSERT INTO projects VALUES (1, 'Project Alpha', 1), (2, 'Project Beta', 2), (3, 'Project Gamma', 1);

SELECT d.dept_name, p.proj_name FROM departments d NATURAL JOIN projects p ORDER BY d.dept_name, p.proj_name;

-- 2.5 NATURAL JOIN with ORDER BY and LIMIT
SELECT emp_id, name, dept_name FROM employees_nj NATURAL JOIN departments ORDER BY salary DESC FETCH FIRST 3 ROWS ONLY;
SELECT * FROM employees_nj NATURAL JOIN departments ORDER BY emp_id OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY;

-- 2.6 NATURAL JOIN - employees without matching department are excluded
SELECT COUNT(*) AS matched_count FROM employees_nj NATURAL JOIN departments;
SELECT COUNT(*) AS total_employees FROM employees_nj;

-- 2.7 NATURAL JOIN with multiple common columns
CREATE TABLE skills (skill_id INT PRIMARY KEY, skill_name TEXT, dept_id INT, level INT);
INSERT INTO skills VALUES (1, 'Python', 1, 3), (2, 'Sales', 2, 2), (3, 'Java', 1, 3), (4, 'Excel', 3, 1);

SELECT dept_name, skill_name FROM departments d NATURAL JOIN skills s ORDER BY dept_name, skill_name;

DROP TABLE skills;
DROP TABLE projects;
DROP TABLE employees_nj;
DROP TABLE departments;

-- ============================================
-- 3. CROSS JOIN
-- ============================================
DROP TABLE IF EXISTS colors;
DROP TABLE IF EXISTS sizes;
DROP TABLE IF EXISTS materials;
DROP TABLE IF EXISTS numbers_a;
DROP TABLE IF EXISTS numbers_b;

CREATE TABLE colors (id INT PRIMARY KEY, color TEXT);
CREATE TABLE sizes (id INT PRIMARY KEY, size TEXT);
CREATE TABLE materials (id INT PRIMARY KEY, material TEXT);

INSERT INTO colors VALUES (1, 'Red'), (2, 'Blue'), (3, 'Green');
INSERT INTO sizes VALUES (1, 'S'), (2, 'M'), (3, 'L'), (4, 'XL');
INSERT INTO materials VALUES (1, 'Cotton'), (2, 'Polyester');

-- 3.1 Basic two-table CROSS JOIN (Cartesian product)
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s ORDER BY c.color, s.size;
SELECT COUNT(*) AS combinations FROM colors c CROSS JOIN sizes s;

-- 3.2 Three-table CROSS JOIN
SELECT c.color, s.size, m.material FROM colors c CROSS JOIN sizes s CROSS JOIN materials m WHERE c.id = 1 AND s.id <= 2 ORDER BY m.material;
SELECT COUNT(*) AS total_combos FROM colors c CROSS JOIN sizes s CROSS JOIN materials m;

-- 3.3 CROSS JOIN with WHERE filtering
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s WHERE c.color = 'Red' ORDER BY s.size;
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s WHERE c.id = s.id ORDER BY c.id;
SELECT c.color, s.size, m.material FROM colors c CROSS JOIN sizes s CROSS JOIN materials m WHERE c.color != 'Green' AND s.size IN ('S', 'M') ORDER BY c.color, s.size, m.material;

-- 3.4 CROSS JOIN with aggregation
SELECT c.color, COUNT(*) as size_count FROM colors c CROSS JOIN sizes s GROUP BY c.color ORDER BY c.color;
SELECT s.size, SUM(c.id) as color_id_sum FROM colors c CROSS JOIN sizes s GROUP BY s.size ORDER BY s.size;

-- 3.5 CROSS JOIN with LIMIT/FETCH
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s ORDER BY c.color, s.size FETCH FIRST 5 ROWS ONLY;
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s ORDER BY c.id DESC, s.id DESC OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY;

-- 3.6 Self CROSS JOIN
CREATE TABLE numbers_a (n INT PRIMARY KEY);
INSERT INTO numbers_a VALUES (1), (2), (3);
SELECT a.n as a, b.n as b, a.n * b.n as product FROM numbers_a a CROSS JOIN numbers_a b ORDER BY a.n, b.n;
SELECT a.n as a, b.n as b FROM numbers_a a CROSS JOIN numbers_a b WHERE a.n < b.n ORDER BY a.n, b.n;

-- 3.7 CROSS JOIN with single-row table
CREATE TABLE numbers_b (val INT PRIMARY KEY);
INSERT INTO numbers_b VALUES (100);
SELECT c.color, n.val as multiplier FROM colors c CROSS JOIN numbers_b n ORDER BY c.color;

-- 3.8 CROSS JOIN with empty table result (when filtered)
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s WHERE c.id > 100;

DROP TABLE numbers_b;
DROP TABLE numbers_a;
DROP TABLE materials;
DROP TABLE colors;
DROP TABLE sizes;

-- ============================================
-- 4. UPDATE ... FROM (UPDATE JOIN)
-- ============================================
DROP TABLE IF EXISTS products_uf;
DROP TABLE IF EXISTS price_updates;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS category_discounts;
DROP TABLE IF EXISTS inventory_levels;
DROP TABLE IF EXISTS stock_adjustments;

CREATE TABLE products_uf (id INT PRIMARY KEY, name TEXT, price INT, category_id INT, stock INT);
CREATE TABLE price_updates (product_id INT PRIMARY KEY, new_price INT);

INSERT INTO products_uf VALUES (1, 'Apple', 100, 1, 50), (2, 'Banana', 50, 1, 100), (3, 'Laptop', 1000, 2, 10), (4, 'Phone', 800, 2, 25), (5, 'Book', 25, 3, 200);
INSERT INTO price_updates VALUES (1, 120), (3, 950), (5, 30);

-- 4.1 Basic UPDATE FROM with direct value assignment
SELECT id, name, price FROM products_uf ORDER BY id;
UPDATE products_uf p SET price = pu.new_price FROM price_updates pu WHERE p.id = pu.product_id;
SELECT id, name, price FROM products_uf ORDER BY id;

-- 4.2 UPDATE FROM with calculation using both tables
CREATE TABLE categories (id INT PRIMARY KEY, name TEXT);
CREATE TABLE category_discounts (category_id INT PRIMARY KEY, discount_pct INT);

INSERT INTO categories VALUES (1, 'Food'), (2, 'Electronics'), (3, 'Books');
INSERT INTO category_discounts VALUES (1, 10), (2, 5);

UPDATE products_uf p SET price = p.price * (100 - cd.discount_pct) / 100 
FROM category_discounts cd WHERE p.category_id = cd.category_id;
SELECT id, name, price, category_id FROM products_uf ORDER BY id;

-- 4.3 UPDATE FROM with multiple columns updated
CREATE TABLE stock_adjustments (product_id INT PRIMARY KEY, qty_change INT, new_price INT);
INSERT INTO stock_adjustments VALUES (1, 20, 130), (2, -10, 55), (4, 5, 750);

UPDATE products_uf p SET stock = p.stock + sa.qty_change, price = sa.new_price
FROM stock_adjustments sa WHERE p.id = sa.product_id;
SELECT id, name, price, stock FROM products_uf ORDER BY id;

-- 4.4 UPDATE FROM affecting no rows (no match)
UPDATE products_uf p SET price = 999 FROM price_updates pu WHERE p.id = pu.product_id AND p.id > 100;
SELECT id, name, price FROM products_uf ORDER BY id;

-- 4.5 UPDATE FROM with table alias
UPDATE products_uf AS prod SET price = adj.new_price
FROM stock_adjustments AS adj WHERE prod.id = adj.product_id;
SELECT id, name, price FROM products_uf ORDER BY id;

-- 4.6 UPDATE FROM with complex WHERE condition
UPDATE products_uf p SET price = p.price + 10
FROM categories c WHERE p.category_id = c.id AND c.name = 'Food';
SELECT id, name, price, category_id FROM products_uf WHERE category_id = 1 ORDER BY id;

-- 4.7 Verify unmatched rows unchanged
SELECT id, name, price FROM products_uf WHERE category_id = 3 ORDER BY id;

DROP TABLE stock_adjustments;
DROP TABLE category_discounts;
DROP TABLE categories;
DROP TABLE price_updates;
DROP TABLE products_uf;

-- ============================================
-- 5. CREATE TABLE AS
-- ============================================
DROP TABLE IF EXISTS source_data;
DROP TABLE IF EXISTS derived_table;
DROP TABLE IF EXISTS high_scores;
DROP TABLE IF EXISTS name_list;
DROP TABLE IF EXISTS agg_result;
DROP TABLE IF EXISTS join_result;
DROP TABLE IF EXISTS expr_result;
DROP TABLE IF EXISTS orders_ctas;
DROP TABLE IF EXISTS customers_ctas;

CREATE TABLE source_data (id INT PRIMARY KEY, name TEXT, score INT, grade TEXT);
INSERT INTO source_data VALUES (1, 'Alice', 95, 'A'), (2, 'Bob', 87, 'B'), (3, 'Charlie', 92, 'A'), (4, 'David', 78, 'C'), (5, 'Eve', 88, 'B');

-- 5.1 Basic CREATE TABLE AS with WHERE
CREATE TABLE derived_table AS SELECT id, name FROM source_data WHERE score > 85;
SELECT * FROM derived_table ORDER BY id;

-- 5.2 CREATE TABLE AS with ORDER BY
CREATE TABLE high_scores AS SELECT name, score, grade FROM source_data WHERE grade = 'A' ORDER BY score DESC;
SELECT * FROM high_scores;

-- 5.3 CREATE TABLE AS with DISTINCT
CREATE TABLE name_list AS SELECT DISTINCT grade FROM source_data ORDER BY grade;
SELECT * FROM name_list;

-- 5.4 CREATE TABLE IF NOT EXISTS (table already exists)
CREATE TABLE IF NOT EXISTS derived_table AS SELECT * FROM source_data;
SELECT COUNT(*) FROM derived_table;

-- 5.5 CREATE TABLE AS with aggregation
CREATE TABLE agg_result AS SELECT grade, COUNT(*) as cnt, AVG(score) as avg_score FROM source_data GROUP BY grade;
SELECT * FROM agg_result ORDER BY grade;

-- 5.6 CREATE TABLE AS from JOIN
CREATE TABLE customers_ctas (id INT PRIMARY KEY, name TEXT);
CREATE TABLE orders_ctas (id INT PRIMARY KEY, customer_id INT, amount INT);
INSERT INTO customers_ctas VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO orders_ctas VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150);

CREATE TABLE join_result AS 
SELECT c.name, SUM(o.amount) as total 
FROM customers_ctas c 
INNER JOIN orders_ctas o ON c.id = o.customer_id 
GROUP BY c.id, c.name;
SELECT * FROM join_result ORDER BY name;

-- 5.7 CREATE TABLE AS with expressions
CREATE TABLE expr_result AS SELECT id, name, score * 2 as double_score, score > 90 as high_performer FROM source_data;
SELECT * FROM expr_result ORDER BY id;

-- 5.8 CREATE TABLE AS with LIMIT/FETCH
DROP TABLE IF EXISTS top3;
CREATE TABLE top3 AS SELECT name, score FROM source_data ORDER BY score DESC FETCH FIRST 3 ROWS ONLY;
SELECT * FROM top3;

DROP TABLE top3;
DROP TABLE expr_result;
DROP TABLE join_result;
DROP TABLE orders_ctas;
DROP TABLE customers_ctas;
DROP TABLE agg_result;
DROP TABLE name_list;
DROP TABLE high_scores;
DROP TABLE derived_table;
DROP TABLE source_data;

-- ============================================
-- 6. SELECT INTO
-- ============================================
DROP TABLE IF EXISTS employees_si;
DROP TABLE IF EXISTS high_earners;
DROP TABLE IF EXISTS dept_summary;
DROP TABLE IF EXISTS top_performers;
DROP TABLE IF EXISTS year_summary;
DROP TABLE IF EXISTS expr_into;
DROP TABLE IF EXISTS distinct_depts;

CREATE TABLE employees_si (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, hire_year INT);
INSERT INTO employees_si VALUES 
    (1, 'Alice', 'Engineering', 75000, 2020),
    (2, 'Bob', 'Sales', 65000, 2019),
    (3, 'Charlie', 'Engineering', 80000, 2018),
    (4, 'David', 'HR', 55000, 2021),
    (5, 'Eve', 'Sales', 70000, 2020),
    (6, 'Frank', 'Engineering', 85000, 2017);

-- 6.1 Basic SELECT INTO with WHERE
SELECT id, name, salary INTO high_earners FROM employees_si WHERE salary > 70000;
SELECT * FROM high_earners ORDER BY salary DESC;

-- 6.2 SELECT INTO with GROUP BY aggregation
SELECT dept, COUNT(*) as emp_count, AVG(salary) as avg_salary INTO dept_summary FROM employees_si GROUP BY dept;
SELECT * FROM dept_summary ORDER BY avg_salary DESC;

-- 6.3 SELECT INTO with ORDER BY and FETCH
SELECT name, salary INTO top_performers FROM employees_si WHERE salary > 75000 ORDER BY salary DESC FETCH FIRST 2 ROWS ONLY;
SELECT * FROM top_performers;

-- 6.4 SELECT INTO with DISTINCT
SELECT DISTINCT dept INTO distinct_depts FROM employees_si ORDER BY dept;
SELECT * FROM distinct_depts;

-- 6.5 SELECT INTO with expressions
SELECT id, name, salary / 12 as monthly_salary, salary > 70000 as above_avg INTO expr_into FROM employees_si;
SELECT * FROM expr_into ORDER BY id;

-- 6.6 SELECT INTO with multiple aggregations
SELECT hire_year, COUNT(*) as hires, MIN(salary) as min_sal, MAX(salary) as max_sal INTO year_summary FROM employees_si GROUP BY hire_year;
SELECT * FROM year_summary ORDER BY hire_year;

-- 6.7 Verify row counts
SELECT COUNT(*) as high_earner_count FROM high_earners;
SELECT COUNT(*) as dept_count FROM dept_summary;
SELECT COUNT(*) as distinct_dept_count FROM distinct_depts;

DROP TABLE year_summary;
DROP TABLE expr_into;
DROP TABLE distinct_depts;
DROP TABLE top_performers;
DROP TABLE dept_summary;
DROP TABLE high_earners;
DROP TABLE employees_si;

-- ============================================
-- 7. Combined Feature Tests
-- ============================================
DROP TABLE IF EXISTS orders_combined;
DROP TABLE IF EXISTS customers_combined;
DROP TABLE IF EXISTS order_summary;

CREATE TABLE customers_combined (id INT PRIMARY KEY, name TEXT, region TEXT);
CREATE TABLE orders_combined (id INT PRIMARY KEY, customer_id INT, amount INT, order_date TEXT);

INSERT INTO customers_combined VALUES (1, 'Acme Corp', 'North'), (2, 'Beta Inc', 'South'), (3, 'Gamma LLC', 'North');
INSERT INTO orders_combined VALUES (1, 1, 1000, '2024-01-15'), (2, 1, 1500, '2024-02-20'), (3, 2, 800, '2024-01-10'), (4, 3, 2000, '2024-03-05');

CREATE TABLE order_summary AS 
SELECT c.name, c.region, SUM(o.amount) as total_amount
FROM customers_combined c 
INNER JOIN orders_combined o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.region;

SELECT * FROM order_summary ORDER BY total_amount DESC FETCH FIRST 2 ROWS ONLY;

UPDATE order_summary SET total_amount = total_amount * 1.1 WHERE region = 'North';
SELECT * FROM order_summary ORDER BY name;

DROP TABLE order_summary;
DROP TABLE orders_combined;
DROP TABLE customers_combined;

SELECT 'All Phase 1 tests completed successfully!' AS status;
