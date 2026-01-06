DROP TABLE IF EXISTS explain_test;
CREATE TABLE explain_test (
    id INT PRIMARY KEY,
    a INT,
    b TEXT
);

CREATE INDEX idx_a ON explain_test(a);

INSERT INTO explain_test VALUES (1, 10, 'one'), (2, 20, 'two'), (3, 10, 'three');

EXPLAIN SELECT 1;

EXPLAIN SELECT * FROM explain_test WHERE b = 'one';

EXPLAIN SELECT * FROM explain_test WHERE id = 1;

EXPLAIN SELECT * FROM explain_test WHERE a = 10;

EXPLAIN SELECT * FROM explain_test;

EXPLAIN SELECT * FROM explain_test ORDER BY a;

EXPLAIN SELECT * FROM explain_test LIMIT 1;

EXPLAIN SELECT a, COUNT(*) FROM explain_test GROUP BY a;

DROP TABLE explain_test;
