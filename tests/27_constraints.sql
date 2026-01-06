DROP TABLE IF EXISTS not_null_test;

CREATE TABLE not_null_test (
    id INT PRIMARY KEY,
    required_col TEXT NOT NULL,
    optional_col TEXT
);

INSERT INTO not_null_test (id, required_col) VALUES (1, 'valid');

INSERT INTO not_null_test (id, required_col, optional_col) VALUES (2, 'also valid', NULL);

INSERT INTO not_null_test (id, required_col) VALUES (3, NULL);

INSERT INTO not_null_test (id, optional_col) VALUES (4, 'missing required');

UPDATE not_null_test SET required_col = NULL WHERE id = 1;

UPDATE not_null_test SET optional_col = NULL WHERE id = 1;

SELECT * FROM not_null_test ORDER BY id;

DROP TABLE not_null_test;

DROP TABLE IF EXISTS check_test;

CREATE TABLE check_test (
    id INT PRIMARY KEY,
    age INT CHECK (age >= 0),
    price DOUBLE PRECISION CONSTRAINT positive_price CHECK (price > 0),
    status TEXT CHECK (status IN ('active', 'inactive', 'pending'))
);

INSERT INTO check_test (id, age, price, status) VALUES (1, 25, 99.99, 'active');
INSERT INTO check_test (id, age, price, status) VALUES (2, 0, 0.01, 'inactive');
INSERT INTO check_test (id, age, price, status) VALUES (3, NULL, NULL, NULL);
INSERT INTO check_test (id, age, price, status) VALUES (4, -1, 10.00, 'active');
INSERT INTO check_test (id, age, price, status) VALUES (5, 30, 0, 'active');
INSERT INTO check_test (id, age, price, status) VALUES (6, 30, 10.00, 'deleted');

SELECT * FROM check_test ORDER BY id;

UPDATE check_test SET age = 30 WHERE id = 1;
UPDATE check_test SET age = -5 WHERE id = 1;

SELECT * FROM check_test ORDER BY id;

DROP TABLE check_test;
