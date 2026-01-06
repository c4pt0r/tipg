DROP TABLE IF EXISTS explain_test;
DROP TABLE IF EXISTS explain_join;

CREATE TABLE explain_test (
    id INT PRIMARY KEY,
    a INT,
    b TEXT,
    c INT
);

CREATE TABLE explain_join (
    id INT PRIMARY KEY,
    test_id INT,
    value TEXT
);

CREATE INDEX idx_a ON explain_test(a);
CREATE INDEX idx_ab ON explain_test(a, b);
CREATE UNIQUE INDEX idx_c_unique ON explain_test(c);
CREATE INDEX idx_join_test_id ON explain_join(test_id);

INSERT INTO explain_test VALUES (1, 10, 'one', 100), (2, 20, 'two', 200), (3, 10, 'three', 300);
INSERT INTO explain_join VALUES (1, 1, 'join1'), (2, 2, 'join2'), (3, 1, 'join3');

EXPLAIN SELECT 1;

EXPLAIN SELECT 1 + 2;

EXPLAIN SELECT NOW();

EXPLAIN SELECT * FROM explain_test WHERE b = 'one';

EXPLAIN SELECT * FROM explain_test WHERE id = 1;

EXPLAIN SELECT * FROM explain_test WHERE a = 10;

EXPLAIN SELECT * FROM explain_test WHERE c = 100;

EXPLAIN SELECT * FROM explain_test WHERE a = 10 AND b = 'one';

EXPLAIN SELECT * FROM explain_test;

EXPLAIN SELECT * FROM explain_test ORDER BY a;

EXPLAIN SELECT * FROM explain_test ORDER BY a DESC;

EXPLAIN SELECT * FROM explain_test ORDER BY a, b;

EXPLAIN SELECT * FROM explain_test LIMIT 1;

EXPLAIN SELECT * FROM explain_test LIMIT 10 OFFSET 5;

EXPLAIN SELECT * FROM explain_test ORDER BY a LIMIT 2;

EXPLAIN SELECT a, COUNT(*) FROM explain_test GROUP BY a;

EXPLAIN SELECT a, SUM(c), AVG(c) FROM explain_test GROUP BY a;

EXPLAIN SELECT a, COUNT(*) FROM explain_test GROUP BY a HAVING COUNT(*) > 1;

EXPLAIN SELECT * FROM explain_test t1 JOIN explain_join t2 ON t1.id = t2.test_id;

EXPLAIN SELECT * FROM explain_test t1 LEFT JOIN explain_join t2 ON t1.id = t2.test_id;

EXPLAIN SELECT * FROM explain_test WHERE a IN (10, 20);

EXPLAIN SELECT * FROM explain_test WHERE a BETWEEN 10 AND 20;

EXPLAIN SELECT * FROM explain_test WHERE b LIKE 'o%';

EXPLAIN SELECT * FROM explain_test WHERE b IS NOT NULL;

EXPLAIN SELECT DISTINCT a FROM explain_test;

EXPLAIN SELECT * FROM explain_test WHERE id IN (SELECT test_id FROM explain_join);

EXPLAIN WITH cte AS (SELECT * FROM explain_test WHERE a = 10) SELECT * FROM cte;

DROP TABLE explain_join;
DROP TABLE explain_test;
