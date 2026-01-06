DROP TABLE IF EXISTS unique_test CASCADE;

CREATE TABLE unique_test (
    id INT PRIMARY KEY,
    email TEXT UNIQUE,
    username TEXT,
    code INT,
    CONSTRAINT unique_username UNIQUE (username),
    CONSTRAINT unique_code UNIQUE (code)
);

INSERT INTO unique_test (id, email, username, code) VALUES (1, 'alice@example.com', 'alice', 100);
INSERT INTO unique_test (id, email, username, code) VALUES (2, 'bob@example.com', 'bob', 200);
INSERT INTO unique_test (id, email, username, code) VALUES (3, 'alice@example.com', 'alice2', 300);
INSERT INTO unique_test (id, email, username, code) VALUES (4, 'charlie@example.com', 'alice', 400);
INSERT INTO unique_test (id, email, username, code) VALUES (5, 'david@example.com', 'david', 100);
INSERT INTO unique_test (id, email, username, code) VALUES (6, NULL, 'eve', 600);
INSERT INTO unique_test (id, email, username, code) VALUES (7, NULL, 'frank', 700);

SELECT * FROM unique_test ORDER BY id;

UPDATE unique_test SET email = 'bob@example.com' WHERE id = 1;

SELECT * FROM unique_test ORDER BY id;

DROP TABLE unique_test CASCADE;

DROP TABLE IF EXISTS default_test;

CREATE TABLE default_test (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    counter INT DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    price DOUBLE PRECISION DEFAULT 9.99
);

INSERT INTO default_test (id, name) VALUES (1, 'test1');
INSERT INTO default_test (id, name, status, counter) VALUES (2, 'test2', 'active', 10);
INSERT INTO default_test (id, name, status) VALUES (3, 'test3', NULL);

SELECT id, name, status, counter, is_active, price FROM default_test ORDER BY id;

DROP TABLE default_test;

DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    amount DOUBLE PRECISION,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO customers (id, name, email) VALUES (2, 'Bob', 'bob@example.com');
INSERT INTO orders (id, customer_id, amount) VALUES (101, 1, 99.99);
INSERT INTO orders (id, customer_id, amount) VALUES (102, 2, 149.50);
INSERT INTO orders (id, customer_id, amount) VALUES (103, 999, 50.00);
INSERT INTO orders (id, customer_id, amount) VALUES (104, NULL, 25.00);

SELECT o.id, o.customer_id, c.name, o.amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
ORDER BY o.id;

DELETE FROM customers WHERE id = 1;
UPDATE customers SET id = 999 WHERE id = 1;

SELECT * FROM customers ORDER BY id;

DROP TABLE orders CASCADE;
DROP TABLE customers CASCADE;

DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders_cascade CASCADE;

CREATE TABLE orders_cascade (
    id INT PRIMARY KEY,
    order_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product TEXT,
    FOREIGN KEY (order_id) REFERENCES orders_cascade(id) ON DELETE CASCADE
);

INSERT INTO orders_cascade (id) VALUES (1), (2);
INSERT INTO order_items (id, order_id, product) VALUES (101, 1, 'Widget'), (102, 1, 'Gadget'), (103, 2, 'Doohickey');

SELECT * FROM order_items ORDER BY id;

DELETE FROM orders_cascade WHERE id = 1;

SELECT * FROM order_items ORDER BY id;

DROP TABLE order_items CASCADE;
DROP TABLE orders_cascade CASCADE;

DROP TABLE IF EXISTS composite_unique;

CREATE TABLE composite_unique (
    id INT PRIMARY KEY,
    dept TEXT,
    employee_no INT,
    CONSTRAINT unique_dept_empno UNIQUE (dept, employee_no)
);

INSERT INTO composite_unique (id, dept, employee_no) VALUES (1, 'Sales', 100);
INSERT INTO composite_unique (id, dept, employee_no) VALUES (2, 'Engineering', 100);
INSERT INTO composite_unique (id, dept, employee_no) VALUES (3, 'Sales', 200);
INSERT INTO composite_unique (id, dept, employee_no) VALUES (4, 'Sales', 100);

SELECT * FROM composite_unique ORDER BY id;

DROP TABLE composite_unique;
