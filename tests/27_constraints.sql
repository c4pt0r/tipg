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
